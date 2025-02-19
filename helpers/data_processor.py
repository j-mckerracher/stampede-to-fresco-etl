import os
import queue
import signal
import threading
import time
from multiprocessing import Pool, cpu_count
from urllib.parse import urljoin
import polars as pl
import requests
from bs4 import BeautifulSoup
from helpers.data_manager import DataManager
from helpers.node_status import NodeStatus
from helpers.processing_tracker import ProcessingTracker
from helpers.utilities import process_node_folder, cleanup_files, download_node_folder, check_critical_disk_space


class DataProcessor:
    def __init__(self, base_dir: str, max_download_threads: int = 8, max_process_threads: int = None):
        self.base_dir = base_dir
        self.max_download_threads = max_download_threads
        self.max_process_threads = max_process_threads or max(1, cpu_count() - 1)
        self.tracker = ProcessingTracker(base_dir)
        self.data_manager = DataManager(base_dir)

        self.download_queue = queue.Queue()
        self.process_queue = queue.Queue()
        self.stop_event = threading.Event()

        # Create process pool for CPU-intensive operations
        self.process_pool = Pool(processes=self.max_process_threads)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\nReceived signal {signum}. Shutting down gracefully...")
        self.stop_event.set()

        # Add None to queues to signal workers to stop
        for _ in range(self.max_download_threads):
            self.download_queue.put(None)
        for _ in range(self.max_process_threads):
            self.process_queue.put(None)

        # Clean up process pool
        if hasattr(self, 'process_pool'):
            self.process_pool.close()
            self.process_pool.terminate()
            self.process_pool.join()

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
                    # Use polars for efficient datetime operations
                    df_split = result.with_columns([
                        pl.col("Timestamp").dt.strftime("%Y_%m").alias("month")
                    ])

                    # Group by month using polars
                    batch_monthly = {}
                    for month in df_split.get_column("month").unique():
                        month_data = df_split.filter(pl.col("month") == month)
                        batch_monthly[month] = month_data.drop("month")

                    self.data_manager.update_monthly_data(batch_monthly)
                    self.tracker.update_node_status(node_name, NodeStatus.COMPLETED)
                    cleanup_files([folder_path])
                else:
                    self.tracker.update_node_status(node_name, NodeStatus.FAILED)

                self.process_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Processor error for {node_name}: {e}")
                self.tracker.update_node_status(node_name, NodeStatus.FAILED)

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
                success = download_node_folder(node_info, self.base_dir, self.tracker)

                if success:
                    self.tracker.update_node_status(node_name, NodeStatus.DOWNLOADED)
                    self.process_queue.put(node_name)
                else:
                    self.tracker.update_node_status(node_name, NodeStatus.FAILED)

                self.download_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Downloader error for {node_name}: {e}")
                self.tracker.update_node_status(node_name, NodeStatus.FAILED)

    def start(self):
        """Start processing with download-process pipeline"""
        try:
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

                # Add None to queues to signal workers to stop
                for _ in range(self.max_download_threads):
                    self.download_queue.put(None)
                for _ in range(self.max_process_threads):
                    self.process_queue.put(None)

                # Wait for queues to empty
                self.download_queue.join()
                self.process_queue.join()

                # Clean up process pool more gracefully
                self.process_pool.close()
                self.process_pool.terminate()
                self.process_pool.join()

        except Exception as e:
            print(f"Error in start: {e}")
        finally:
            # Ensure process pool is cleaned up
            if hasattr(self, 'process_pool'):
                self.process_pool.close()
                self.process_pool.terminate()
                self.process_pool.join()

    def _main_loop(self):
        """Main loop for queuing downloads"""
        start_index = self.tracker.current_batch * 3
        while True:
            is_safe, _ = check_critical_disk_space()
            if not is_safe:
                print("Critical disk space reached. Saving checkpoint...")
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