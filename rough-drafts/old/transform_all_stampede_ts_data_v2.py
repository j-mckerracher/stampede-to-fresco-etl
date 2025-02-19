import queue
from enum import Enum
from typing import Set
import subprocess
import threading
import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import shutil
import time
import json
from pathlib import Path
import gc
import psutil
from typing import Dict
import pandas as pd
from helpers.data_version_manager import DataVersionManager
from helpers.processing_tracker import ProcessingTracker
from multiprocessing import Pool, cpu_count
from helpers.node_status import NodeStatus


class DataManager:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.version_manager = DataVersionManager(base_dir)
        self.monthly_data_lock = threading.Lock()
        self.monthly_data: Dict[str, pd.DataFrame] = {}
        self.load_checkpoint()

    def load_checkpoint(self):
        """Load monthly data from checkpoint"""
        checkpoint_path = Path(self.base_dir) / "monthly_data_checkpoint.pkl"
        if checkpoint_path.exists():
            try:
                self.monthly_data = pd.read_pickle(checkpoint_path)
                print("Loaded data checkpoint")
            except Exception as e:
                print(f"Error loading checkpoint: {e}")

    def save_checkpoint(self):
        """Save monthly data checkpoint"""
        checkpoint_path = Path(self.base_dir) / "monthly_data_checkpoint.pkl"
        try:
            with self.monthly_data_lock:
                pd.to_pickle(self.monthly_data, checkpoint_path)
                print("Saved data checkpoint")
        except Exception as e:
            print(f"Error saving checkpoint: {e}")

    def cleanup_incomplete_nodes(self, nodes: Set[str]):
        """Remove data from incomplete nodes"""
        with self.monthly_data_lock:
            for month, df in self.monthly_data.items():
                self.monthly_data[month] = df[~df['Host'].isin(nodes)]
            self.save_checkpoint()

    def update_monthly_data(self, new_data: Dict[str, pd.DataFrame]):
        """Thread-safe update of monthly data"""
        with self.monthly_data_lock:
            for month, new_df in new_data.items():
                if month in self.monthly_data:
                    self.monthly_data[month] = pd.concat(
                        [self.monthly_data[month], new_df]
                    ).drop_duplicates()
                else:
                    self.monthly_data[month] = new_df
            self.save_checkpoint()


def process_node_folder(folder_path):
    """Process a single node folder with memory management"""
    results = []

    # Process block, cpu, and nfs files
    for file_type in ['block', 'cpu', 'nfs']:
        file_path = os.path.join(folder_path, f'{file_type}.csv')
        if os.path.exists(file_path):
            # Process one file at a time
            result = globals()[f'process_{file_type}_file'](file_path)
            results.append(result)
            # Force garbage collection after each file
            del result
            gc.collect()

    # Process memory metrics separately since they produce two dataframes
    mem_file_path = os.path.join(folder_path, 'mem.csv')
    if os.path.exists(mem_file_path):
        memused_df, memused_nocache_df = process_memory_metrics(mem_file_path)
        results.append(memused_df)
        results.append(memused_nocache_df)
        # Force garbage collection
        del memused_df
        del memused_nocache_df
        gc.collect()

    if results:
        final_result = pd.concat(results, ignore_index=True)
        return final_result
    return None


class DataProcessor:
    def __init__(self, base_dir: str, max_download_threads: int = 8, max_process_threads: int = None):
        self.base_dir = base_dir
        self.max_download_threads = max_download_threads
        self.max_process_threads = max_process_threads or max(1, cpu_count() - 1)  # Leave one CPU free
        self.tracker = ProcessingTracker(base_dir)
        self.data_manager = DataManager(base_dir)

        self.download_queue = queue.Queue()
        self.process_queue = queue.Queue()
        self.stop_event = threading.Event()

        # Create process pool for CPU-intensive operations
        self.process_pool = Pool(processes=self.max_process_threads)

    def start(self):
        """Start processing with download-process pipeline"""
        # Cleanup incomplete nodes from previous run
        incomplete_nodes = self.tracker.get_incomplete_nodes()
        if incomplete_nodes:
            print(f"Cleaning up incomplete nodes: {incomplete_nodes}")
            self.data_manager.cleanup_incomplete_nodes(incomplete_nodes)

        # Start downloader threads
        download_threads = []
        for _ in range(self.max_download_threads):
            t = threading.Thread(target=self._downloader_worker)
            t.daemon = True
            t.start()
            download_threads.append(t)

        # Start multiple processor threads
        process_threads = []
        for _ in range(self.max_process_threads):
            t = threading.Thread(target=self._processor_worker)
            t.daemon = True
            t.start()
            process_threads.append(t)

        try:
            self._main_loop()
        except KeyboardInterrupt:
            print("\nShutting down gracefully...")
            self.stop_event.set()

        # Wait for queues to empty and threads to finish
        self.download_queue.join()
        self.process_queue.join()

        # Clean up process pool
        self.process_pool.close()
        self.process_pool.join()

    def _downloader_worker(self):
        """Worker thread for downloading node folders"""
        while not self.stop_event.is_set():
            try:
                node_info = self.download_queue.get(timeout=1)
                if node_info is None:
                    break

                link, node_url = node_info
                node_name = link.text.strip('/')

                self.tracker.update_node_status(node_name, NodeStatus.DOWNLOADING)
                success = self._download_node_folder(node_info)

                if success:
                    self.tracker.update_node_status(node_name, NodeStatus.DOWNLOADED)
                    self.process_queue.put(node_name)
                else:
                    self.tracker.update_node_status(node_name, NodeStatus.FAILED)

                self.download_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Downloader error: {e}")

    def _processor_worker(self):
        """Worker thread for processing node folders"""
        while not self.stop_event.is_set():
            try:
                node_name = self.process_queue.get(timeout=1)
                if node_name is None:
                    break

                self.tracker.update_node_status(node_name, NodeStatus.PROCESSING)
                folder_path = os.path.join(self.base_dir, node_name)

                # Use process pool for CPU-intensive operations
                result = process_node_folder(folder_path, self.process_pool)

                if result is not None:
                    # Use process pool for month splitting
                    df_split = result.copy()
                    df_split['month'] = pd.to_datetime(df_split['Timestamp']).dt.strftime('%Y_%m')

                    # Group by month in parallel
                    def process_month_group(group_data):
                        month, data = group_data
                        return month, data.copy()

                    month_groups = df_split.groupby('month')
                    batch_monthly = dict(
                        self.process_pool.imap_unordered(
                            process_month_group,
                            ((month, group) for month, group in month_groups)
                        )
                    )

                    self.data_manager.update_monthly_data(batch_monthly)
                    self.tracker.update_node_status(node_name, NodeStatus.COMPLETED)
                    cleanup_files([folder_path])
                else:
                    self.tracker.update_node_status(node_name, NodeStatus.FAILED)

                self.process_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Processor error: {e}")
                self.tracker.update_node_status(node_name, NodeStatus.FAILED)

    def _main_loop(self):
        """Main loop for queuing downloads"""
        start_index = self.tracker.current_batch * 3
        while True:
            is_safe, _ = check_critical_disk_space()
            if not is_safe:
                self.data_manager.save_checkpoint()
                break

            next_index, has_more = self._queue_next_batch(start_index)
            if next_index is None or not has_more:
                break

            start_index = next_index
            self.tracker.current_batch += 1
            self.tracker.save_status()

            # Small delay between batches
            time.sleep(1)

    def _queue_next_batch(self, start_index):
        """Queue the next batch of nodes for downloading"""
        try:
            base_url = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/"
            response = requests.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            node_links = [
                link for link in soup.find_all('a')
                if 'NODE' in link.text
                   and self.tracker.node_status.get(link.text.strip('/')) != NodeStatus.COMPLETED
            ]

            if start_index >= len(node_links):
                return None, False

            current_batch = node_links[start_index:start_index + 3]
            for link in current_batch:
                node_url = urljoin(base_url, link['href'])
                self.download_queue.put((link, node_url))

            return start_index + 3, start_index + 3 < len(node_links)
        except Exception as e:
            print(f"Error queuing batch: {e}")
            return None, False


def get_base_dir():
    """Get the current working directory using pwd"""
    try:
        result = subprocess.run(['pwd'], capture_output=True, text=True)
        base_dir = result.stdout.strip()
        return base_dir
    except Exception as e:
        print(f"Error getting base directory: {str(e)}")
        print(f"Using: {os.getcwd()}")
        return os.getcwd()  # Fallback to os.getcwd()


def process_memory_metrics(file_path):
    """Process mem.csv file and transform to FRESCO format for both memory metrics"""
    df = pd.read_csv(file_path)

    # Ensure all memory values are in bytes before conversion
    # Note: Memory values in /proc/meminfo are typically in KB, multiply by 1024
    memory_cols = ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']
    for col in memory_cols:
        if col in df.columns:
            df[col] = df[col] * 1024  # Convert KB to bytes

    # Calculate memused (total physical memory usage)
    # Convert to GB for output
    df['memused'] = df['MemUsed'] / (1024 * 1024 * 1024)

    # Calculate memused_minus_diskcache
    # Memory used minus file cache (FilePages)
    df['memused_minus_diskcache'] = (df['MemUsed'] - df['FilePages']) / (1024 * 1024 * 1024)

    # Ensure values don't go below 0
    df['memused_minus_diskcache'] = df['memused_minus_diskcache'].clip(lower=0)

    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    # Create output dataframes for both metrics
    memused_df = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'memused',
        'Value': df['memused'],
        'Units': 'GB',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    memused_nocache_df = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'memused_minus_diskcache',
        'Value': df['memused_minus_diskcache'],
        'Units': 'GB',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    return memused_df, memused_nocache_df


def cleanup_files(dirs_to_delete):
    """
    Delete specified directories and their contents.
    """
    print("\nCleaning up local files...")
    for dir_path in dirs_to_delete:
        if os.path.exists(dir_path):
            try:
                if os.path.isfile(dir_path):
                    os.remove(dir_path)
                else:
                    shutil.rmtree(dir_path)
                print(f"Deleted: {dir_path}")
            except Exception as e:
                print(f"Error deleting {dir_path}: {str(e)}")


def check_critical_disk_space(warning_gb=50, critical_gb=20):
    """
    Check disk space status
    Returns:
        - (True, True) if space is fine
        - (True, False) if warning level reached
        - (False, False) if critical level reached
    """
    disk_usage = psutil.disk_usage('C:')
    available_gb = disk_usage.free / (1024 ** 3)

    return (
        available_gb > critical_gb,  # is_safe
        available_gb > warning_gb  # is_abundant
    )


def main():
    base_dir = get_base_dir()
    processor = DataProcessor(base_dir)
    processor.start()


if __name__ == "__main__":
    main()
