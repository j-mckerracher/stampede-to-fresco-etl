import gc
import glob
import queue
import shutil
import signal
import threading
import time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from helpers.data_manager import DataManager
from helpers.node_status import NodeStatus
from helpers.processing_tracker import ProcessingTracker
from helpers.utilities import (
    process_node_folder, process_multiple_folders, cleanup_files,
    download_node_folder
)
from multiprocessing import Pool, cpu_count
import os
import logging
import polars as pl
from pathlib import Path


class DataProcessor:
    def __init__(self, base_dir: str, batch_size: int = 10, max_process_threads: int = None):
        self.base_dir = base_dir
        self.batch_size = batch_size
        self.max_download_threads = batch_size
        self.max_process_threads = max_process_threads or max(1, cpu_count() - 1)
        self.tracker = ProcessingTracker(base_dir)
        self.data_manager = DataManager(base_dir)
        self.process_pool = None

        # Get nodes that were incomplete when the script last stopped
        incomplete_nodes = self.tracker.get_incomplete_nodes()
        if incomplete_nodes:
            logging.info(f"Found {len(incomplete_nodes)} incomplete nodes from previous run")
            logging.info(f"Incomplete nodes: {incomplete_nodes}")

            # Clean up any data from these nodes to prevent duplication
            self.data_manager.cleanup_incomplete_nodes(incomplete_nodes)

            # Clean up any leftover folders from these nodes
            for node in incomplete_nodes:
                node_dir = Path(self.base_dir) / node
                if node_dir.exists():
                    logging.info(f"Cleaning up incomplete node directory: {node}")
                    shutil.rmtree(node_dir)

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

        # Mark currently processing nodes as incomplete
        current_nodes = set()

        # Check download queue
        while not self.download_queue.empty():
            try:
                item = self.download_queue.get_nowait()
                if item is not None:
                    link, _ = item
                    node_name = link.text.strip('/')
                    current_nodes.add(node_name)
            except queue.Empty:
                break

        # Check process queue
        while not self.process_queue.empty():
            try:
                node_name = self.process_queue.get_nowait()
                if node_name is not None:
                    current_nodes.add(node_name)
            except queue.Empty:
                break

        # Update node statuses
        for node in current_nodes:
            self.tracker.update_node_status(node, NodeStatus.PROCESSING)

        # Save current state
        self.tracker.save_status()
        self.data_manager.save_checkpoint()

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

                try:
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

                        # Immediately clean up after successful processing
                        cleanup_files([folder_path])

                        # Force garbage collection
                        gc.collect()
                    else:
                        self.tracker.update_node_status(node_name, NodeStatus.FAILED)
                finally:
                    # Clean up the folder even if processing fails
                    if os.path.exists(folder_path):
                        cleanup_files([folder_path])

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
        self.process_pool = None
        try:
            # Initialize process pool
            self.process_pool = Pool(processes=self.max_process_threads)

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
                print("\nReceived KeyboardInterrupt. Initiating graceful shutdown...")
            except Exception as e:
                print(f"\nError in main loop: {e}")
                logging.error(f"Main loop error: {e}")
            finally:
                print("\nShutting down workers...")
                self.stop_event.set()

                # Signal workers to stop
                for _ in range(self.max_download_threads):
                    self.download_queue.put(None)
                for _ in range(self.max_process_threads):
                    self.process_queue.put(None)

                # Wait for queues to empty with timeout
                try:
                    self.download_queue.join()
                    self.process_queue.join()
                except Exception as e:
                    print(f"Error waiting for queues to empty: {e}")

                # Wait for threads to finish
                for thread in download_threads + process_threads:
                    try:
                        thread.join(timeout=2.0)  # 2 second timeout for each thread
                    except Exception as e:
                        print(f"Error joining thread: {e}")

        except Exception as e:
            print(f"Critical error in start: {e}")
            logging.error(f"Critical error in start: {e}")
        finally:
            print("\nCleaning up resources...")
            # Save checkpoint before cleanup
            try:
                self.data_manager.save_checkpoint()
            except Exception as e:
                print(f"Error saving checkpoint: {e}")

            # Clean up process pool
            if self.process_pool:
                try:
                    self.process_pool.close()
                    self.process_pool.terminate()
                    self.process_pool.join(timeout=3.0)  # 3 second timeout for pool cleanup
                except Exception as e:
                    print(f"Error cleaning up process pool: {e}")
                finally:
                    self.process_pool = None

            print("Shutdown complete.")

    def _main_loop(self):
        """Main loop for queuing downloads and processing"""
        # Calculate start_index based on processed nodes
        processed_count = len(self.tracker.node_status)
        start_index = processed_count

        logging.info(f"Resuming from index {start_index} (processed {processed_count} nodes)")
        logging.info(f"Using batch size of {self.batch_size} with {self.max_download_threads} download threads")

        while not self.stop_event.is_set():
            try:
                # Check storage and potentially trigger upload
                if not self.data_manager.manage_storage():
                    print("Storage management failed. Saving checkpoint and stopping...")
                    self.data_manager.save_checkpoint()
                    break

                next_index, has_more = self._queue_next_batch(start_index, self.batch_size)
                if next_index is None or not has_more:
                    print("No more nodes to process")
                    break

                # Wait for downloads to complete
                self.download_queue.join()

                # Get all downloaded folders
                node_folders = glob.glob(os.path.join(self.base_dir, "NODE*"))
                if node_folders:
                    # Process multiple folders in parallel
                    result = process_multiple_folders(node_folders)

                    if result is not None:
                        # Split by month and update data manager
                        df_split = result.with_columns([
                            pl.col("Timestamp").dt.strftime("%Y_%m").alias("month")
                        ])

                        # Group by month
                        batch_monthly = {}
                        for month in df_split.get_column("month").unique():
                            month_data = df_split.filter(pl.col("month") == month)
                            batch_monthly[month] = month_data.drop("month")

                        # Update data manager
                        self.data_manager.update_monthly_data(batch_monthly)

                        # Clean up processed folders
                        for folder in node_folders:
                            cleanup_files([folder])

                start_index = next_index
                self.tracker.current_batch += 1
                self.tracker.save_status()

                # Small delay between batches
                time.sleep(1)

            except Exception as e:
                print(f"Error in main loop: {e}")
                self.data_manager.save_checkpoint()
                break

    def _queue_next_batch(self, start_index, batch_size):
        """Queue the next batch of nodes for downloading"""
        try:
            base_url = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/"
            response = requests.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Get all NODE links and sort them numerically
            node_links = [
                link for link in soup.find_all('a')
                if 'NODE' in link.text
                   and self.tracker.node_status.get(link.text.strip('/')) != NodeStatus.COMPLETED
            ]

            # Sort nodes numerically
            node_links.sort(key=lambda x: int(x.text.strip('/').replace('NODE', '')))

            if start_index >= len(node_links):
                return None, False

            logging.info(f"Queuing nodes starting at index {start_index}")
            current_batch = node_links[start_index:start_index + batch_size]

            # Log the nodes being queued
            node_names = [link.text.strip('/') for link in current_batch]
            logging.info(f"Queuing nodes: {node_names}")

            for link in current_batch:
                node_url = urljoin(base_url, link['href'])
                self.download_queue.put((link, node_url))

            return start_index + batch_size, start_index + batch_size < len(node_links)
        except Exception as e:
            print(f"Error queuing batch: {e}")
            return None, False
