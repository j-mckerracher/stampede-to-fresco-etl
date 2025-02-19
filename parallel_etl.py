import gc
import logging
import queue
import threading
import time
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import polars as pl
from helpers.data_manager import DataManager
from helpers.node_status import NodeStatus
from helpers.processing_tracker import ProcessingTracker
from helpers.utilities import download_node_folder, process_node_folder, cleanup_files, cleanup_temp_files, \
    get_user_disk_usage


class ParallelETL:
    def __init__(self, base_dir: str, max_downloaders: int = 4, max_processors: int = 4):
        self.base_dir = Path(base_dir)
        self.max_downloaders = max_downloaders
        self.max_processors = max_processors
        self.download_queue = queue.Queue(maxsize=20)
        self.process_queue = queue.Queue(maxsize=20)
        self.stop_event = threading.Event()

        # Initialize management components
        self.tracker = ProcessingTracker(base_dir)
        self.data_manager = DataManager(base_dir)
        self.current_version = self.data_manager.version_manager.get_current_version()

        # Disk management parameters
        self.quota_limit = 24512  # 24GB in MB
        self.warning_threshold = 0.7 * self.quota_limit
        self.critical_threshold = 0.8 * self.quota_limit

    def _download_worker(self):
        """Producer thread that downloads node data"""
        while not self.stop_event.is_set():
            try:
                node_info = self.download_queue.get(timeout=1)
                if node_info is None:
                    break

                link, node_url = node_info
                node_name = link.text.strip('/')
                self.tracker.update_node_status(node_name, NodeStatus.DOWNLOADING)

                if download_node_folder(node_info, self.base_dir, self.tracker):
                    self.tracker.update_node_status(node_name, NodeStatus.DOWNLOADED)
                    self.process_queue.put(node_name)
                else:
                    self.tracker.update_node_status(node_name, NodeStatus.FAILED)

                self.download_queue.task_done()
                self._check_disk_usage()

            except queue.Empty:
                continue

    def _process_worker(self):
        """Consumer thread that processes node data"""
        while not self.stop_event.is_set():
            try:
                node_name = self.process_queue.get(timeout=1)
                if node_name is None:
                    break

                self.tracker.update_node_status(node_name, NodeStatus.PROCESSING)
                node_path = self.base_dir / node_name

                try:
                    if df := process_node_folder(str(node_path)):
                        self._handle_processed_data(df, node_name)
                finally:
                    cleanup_files([node_path])
                    self.process_queue.task_done()
                    self._check_disk_usage()

            except queue.Empty:
                continue

    def _handle_processed_data(self, df, node_name):
        """Handle processed data and update monthly grouping"""
        monthly_data = df.with_columns(
            pl.col("Timestamp").dt.strftime("%Y_%m").alias("month")
        ).partition_by("month", as_dict=True)

        with self.data_manager.monthly_data_lock:
            for month, data in monthly_data.items():
                if month in self.data_manager.monthly_data:
                    self.data_manager.monthly_data[month] = pl.concat([
                        self.data_manager.monthly_data[month],
                        data
                    ]).unique()
                else:
                    self.data_manager.monthly_data[month] = data

        self.tracker.update_node_status(node_name, NodeStatus.COMPLETED)
        self.data_manager.save_checkpoint()

    def _check_disk_usage(self):
        """Monitor disk usage and trigger upload if needed"""
        used_mb, quota_mb, _ = get_user_disk_usage()  # Use the function from utilities.py
        if used_mb is None:
            logging.error("Could not determine disk usage")
            return

        if used_mb > self.critical_threshold:
            logging.warning("Critical disk usage reached! Triggering upload...")
            if self.data_manager.upload_to_s3():
                logging.info("Upload successful, incrementing version")
                self.current_version = self.data_manager.version_manager.get_current_version()
            else:
                logging.error("Upload failed! Trying to free space...")
                self._emergency_cleanup()
        elif used_mb > self.warning_threshold:
            logging.info("High disk usage warning - proactive cleanup")
            self._proactive_cleanup()

    def _proactive_cleanup(self):
        """Clean up temporary files to free space"""
        space_freed = cleanup_temp_files(str(self.base_dir))
        logging.info(f"Proactive cleanup freed {space_freed:.2f}MB")

    def _emergency_cleanup(self):
        """Emergency cleanup when upload fails"""
        space_freed = 0
        for month in list(self.data_manager.monthly_data.keys()):
            space_freed += self.data_manager.monthly_data[month].estimated_size('mb')
            del self.data_manager.monthly_data[month]
            gc.collect()
        logging.warning(f"Emergency cleanup freed {space_freed:.2f}MB")

    def _queue_nodes(self):
        """Populate download queue with unprocessed nodes"""
        base_url = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/"
        response = requests.get(base_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        node_links = [
            (link, urljoin(base_url, link['href']))
            for link in soup.find_all('a')
            if 'NODE' in link.text and not self.tracker.is_node_processed(link.text.strip('/'))
        ]

        for node_info in node_links:
            self.download_queue.put(node_info)

    def start(self):
        """Start the ETL pipeline"""
        # Start worker threads
        download_threads = [
            threading.Thread(target=self._download_worker, daemon=True)
            for _ in range(self.max_downloaders)
        ]
        process_threads = [
            threading.Thread(target=self._process_worker, daemon=True)
            for _ in range(self.max_processors)
        ]

        for t in download_threads + process_threads:
            t.start()

        try:
            self._queue_nodes()
            while any(t.is_alive() for t in download_threads + process_threads):
                time.sleep(1)
                if self.stop_event.is_set():
                    break
        except KeyboardInterrupt:
            logging.info("Shutting down gracefully...")
            self.stop_event.set()
        finally:
            # Signal workers to stop
            for _ in range(self.max_downloaders):
                self.download_queue.put(None)
            for _ in range(self.max_processors):
                self.process_queue.put(None)

            # Wait for completion
            for t in download_threads + process_threads:
                t.join(timeout=5)

            self.data_manager.save_checkpoint()
            logging.info("ETL pipeline stopped")


if __name__ == "__main__":
    etl = ParallelETL(base_dir="/data/stampede", max_downloaders=6, max_processors=8)
    etl.start()
