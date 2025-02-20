import multiprocessing
import os
import json
import queue
import time
import shutil
import threading
from queue import Queue
from pathlib import Path
from typing import Dict, List, Set, Tuple
import pandas as pd
import numpy as np
import psutil
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import boto3
import concurrent.futures
from dataclasses import dataclass, asdict
import logging
from datetime import datetime
import polars as pl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_optimal_workers():
    cpu_count = multiprocessing.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)  # Convert to GB

    # Calculate optimal workers based on available CPU cores and memory
    # We leave some headroom for system processes
    cpu_workers = max(1, cpu_count - 1)

    # Assume each worker might need ~2GB RAM for processing large datasets
    memory_workers = max(1, int(memory_gb / 2))

    # Take the minimum of CPU and memory-based worker counts
    return min(cpu_workers, memory_workers)


def get_optimal_chunk_size():
    # Get available memory in bytes
    available_memory = psutil.virtual_memory().available

    # Use 75% of available memory as a safe limit
    safe_memory = int(available_memory * 0.75)

    # Convert to MB for easier handling
    chunk_size_mb = safe_memory / (1024 * 1024)

    return max(1024, int(chunk_size_mb))  # Minimum 1GB chunks


@dataclass
class NodeStatus:
    """Track processing status of a node"""
    name: str
    download_complete: bool = False
    processing_complete: bool = False
    processing_started: bool = False
    last_modified: datetime = None
    size_bytes: int = 0


class DiskQuotaManager:
    """Manage disk space usage to stay under quota"""

    def __init__(self, quota_mb: int, safety_buffer_mb: int = 1024):
        self.quota_mb = quota_mb
        self.safety_buffer_mb = safety_buffer_mb
        self.current_usage_mb = 0
        self._lock = threading.Lock()
        logger.info(f"DiskQuotaManager initialized with quota: {quota_mb} MB, safety buffer: {safety_buffer_mb} MB")

    def get_available_space_mb(self) -> int:
        with self._lock:
            available_space = self.quota_mb - self.safety_buffer_mb - self.current_usage_mb
            logger.debug(f"Available space: {available_space} MB")
            return available_space

    def request_space(self, needed_mb: int) -> bool:
        """Try to allocate space for an operation"""
        with self._lock:
            if self.current_usage_mb + needed_mb <= self.quota_mb - self.safety_buffer_mb:
                self.current_usage_mb += needed_mb
                logger.info(f"Allocated {needed_mb} MB. Current usage: {self.current_usage_mb} MB")
                return True
            logger.warning(f"Failed to allocate {needed_mb} MB. Current usage: {self.current_usage_mb} MB")
            return False

    def release_space(self, released_mb: int):
        """Release allocated space after operation"""
        with self._lock:
            self.current_usage_mb = max(0, self.current_usage_mb - released_mb)
            logger.info(f"Released {released_mb} MB. Current usage: {self.current_usage_mb} MB")


class VersionManager:
    """Manage versioning of monthly data files"""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.version_file = self.base_dir / "version_info.json"
        self._lock = threading.Lock()
        self.load_version_info()
        logger.info(f"VersionManager initialized with base directory: {base_dir}")

    def load_version_info(self):
        if self.version_file.exists():
            with open(self.version_file) as f:
                self.version_info = json.load(f)
            logger.info(f"Loaded version info: {self.version_info}")
        else:
            self.version_info = {
                'current_version': 1,
                'uploaded_versions': []
            }
            self.save_version_info()
            logger.info("Created new version info file")

    def save_version_info(self):
        with self._lock:
            with open(self.version_file, 'w') as f:
                json.dump(self.version_info, f)
            logger.debug("Saved version info")

    def get_current_version(self) -> str:
        version = f"v{self.version_info['current_version']}"
        logger.debug(f"Current version: {version}")
        return version

    def increment_version(self):
        with self._lock:
            self.version_info['uploaded_versions'].append(
                self.version_info['current_version']
            )
            self.version_info['current_version'] += 1
            self.save_version_info()
            logger.info(f"Incremented version to {self.version_info['current_version']}")


class ProcessingStateManager:
    """Manage processing state and recovery"""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.state_file = self.base_dir / "processing_state.json"
        self.node_statuses: Dict[str, NodeStatus] = {}
        self._lock = threading.Lock()
        self.load_state()
        logger.info(f"ProcessingStateManager initialized with base directory: {base_dir}")

    def load_state(self):
        if self.state_file.exists():
            with open(self.state_file) as f:
                state_dict = json.load(f)
                self.node_statuses = {
                    name: NodeStatus(**status)
                    for name, status in state_dict.items()
                }
            logger.info(f"Loaded processing state with {len(self.node_statuses)} nodes")
        else:
            logger.info("No existing processing state file found")

    def save_state(self):
        with self._lock:
            state_dict = {
                name: asdict(status)
                for name, status in self.node_statuses.items()
            }
            with open(self.state_file, 'w') as f:
                json.dump(state_dict, f)
            logger.debug("Saved processing state")

    def update_node_status(self, node_name: str, **kwargs):
        with self._lock:
            if node_name not in self.node_statuses:
                self.node_statuses[node_name] = NodeStatus(name=node_name)
                logger.info(f"Added new node {node_name} to processing state")

            status = self.node_statuses[node_name]
            for key, value in kwargs.items():
                setattr(status, key, value)

            self.save_state()
            logger.debug(f"Updated status for node {node_name}: {kwargs}")

    def get_incomplete_nodes(self) -> List[str]:
        incomplete_nodes = [
            name for name, status in self.node_statuses.items()
            if not status.processing_complete
        ]
        logger.debug(f"Found {len(incomplete_nodes)} incomplete nodes")
        return incomplete_nodes


class MonthlyDataManager:
    """Manage monthly data files"""

    def __init__(self, base_dir: str, version_manager: VersionManager):
        self.base_dir = Path(base_dir)
        self.monthly_data_dir = self.base_dir / "monthly_data"
        self.monthly_data_dir.mkdir(exist_ok=True)
        self.version_manager = version_manager
        self._lock = threading.Lock()
        logger.info(f"MonthlyDataManager initialized with base directory: {base_dir}")

    def get_monthly_file_path(self, year_month: str) -> Path:
        version = self.version_manager.get_current_version()
        file_path = self.monthly_data_dir / f"FRESCO_Stampede_ts_{year_month}_{version}.csv"
        logger.debug(f"Generated monthly file path: {file_path}")
        return file_path

    def save_monthly_data(self, monthly_data: Dict[str, pl.DataFrame]) -> List[Path]:
        saved_files = []
        with self._lock:
            for year_month, df in monthly_data.items():
                file_path = self.get_monthly_file_path(year_month)

                # Convert timestamp to string for storage
                df_to_save = df.with_columns([
                    pl.col('Timestamp').dt.strftime('%Y-%m-%d %H:%M:%S').alias('Timestamp')
                ])

                if file_path.exists():
                    existing_df = pl.read_csv(file_path)

                    # Convert existing timestamp to datetime for comparison
                    existing_df = existing_df.with_columns([
                        pl.col('Timestamp').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S').alias('Timestamp')
                    ])

                    # Convert back to string for storage
                    merged_df = pl.concat([existing_df, df]).unique().with_columns([
                        pl.col('Timestamp').dt.strftime('%Y-%m-%d %H:%M:%S').alias('Timestamp')
                    ])

                    merged_df.write_csv(file_path)
                    logger.info(f"Merged data into existing file: {file_path}")
                else:
                    df_to_save.write_csv(file_path)
                    logger.info(f"Created new monthly file: {file_path}")

                saved_files.append(file_path)

        return saved_files


class S3Uploader:
    """Handle S3 uploads with retries"""

    def __init__(self, bucket_name: str, max_retries: int = 3):
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        self.s3_client = boto3.client('s3')
        logger.info(f"S3Uploader initialized with bucket: {bucket_name}")

    def upload_files(self, file_paths: List[Path]) -> bool:
        success = True
        for file_path in file_paths:
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    logger.info(f"Uploading {file_path} to S3 bucket {self.bucket_name}")
                    self.s3_client.upload_file(
                        str(file_path),
                        self.bucket_name,
                        file_path.name
                    )
                    logger.info(f"Successfully uploaded {file_path}")
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == self.max_retries:
                        logger.error(f"Failed to upload {file_path}: {e}")
                        success = False
                    else:
                        logger.warning(f"Retry {retry_count}/{self.max_retries} uploading {file_path}: {e}")
                    time.sleep(2 ** retry_count)

        return success


class NodeDataProcessor:
    """Process node data files using Polars with optimized parallel processing"""
    def __init__(self):
        self.cpu_count = max(1, multiprocessing.cpu_count() - 1)

    def process_block_file(self, file_path: Path) -> pl.DataFrame:
        logger.info(f"Processing block file: {file_path}")
        # Use streaming for large files
        df = pl.scan_csv(file_path)

        # Optimize calculations using lazy evaluation
        df = df.with_columns([
            pl.col(['rd_sectors', 'wr_sectors', 'rd_ticks', 'wr_ticks'])
            .cast(pl.Float64)
            .suffix('_num')
        ])

        df = df.with_columns([
            (pl.col('rd_sectors_num') + pl.col('wr_sectors_num')).alias('total_sectors'),
            ((pl.col('rd_ticks_num') + pl.col('wr_ticks_num')) / 1000).alias('total_ticks')
        ])

        # Use lazy evaluation for better performance
        df = df.with_columns([
            pl.when(pl.col('total_ticks') > 0)
            .then((pl.col('total_sectors') * 512) / pl.col('total_ticks') / (1024 ** 3))
            .otherwise(0)
            .alias('Value')
        ])

        return df.select([
            pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
            pl.col('node').alias('Host'),
            pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
            pl.lit('block').alias('Event'),
            pl.col('Value'),
            pl.lit('GB/s').alias('Units')
        ]).collect(streaming=True)

    def process_memory_metrics(self, file_path: Path) -> List[pl.DataFrame]:
        logger.info(f"Processing memory file: {file_path}")
        # Use streaming for large files
        df = pl.scan_csv(file_path)

        # Optimize memory column calculations
        memory_cols = ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']
        df = df.with_columns([
            pl.col(col).mul(1024) for col in memory_cols if col in df.columns
        ])

        df = df.with_columns([
            (pl.col('MemUsed') / (1024 ** 3)).alias('memused'),
            ((pl.col('MemUsed') - pl.col('FilePages')) / (1024 ** 3))
            .clip(0, None)
            .alias('memused_minus_diskcache')
        ])

        # Use common base for both metrics to avoid redundant computation
        base_df = df.select([
            pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
            pl.col('node').alias('Host'),
            pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
            pl.col('memused'),
            pl.col('memused_minus_diskcache')
        ]).collect(streaming=True)

        return [
            base_df.with_columns([
                pl.lit('memused').alias('Event'),
                pl.col('memused').alias('Value'),
                pl.lit('GB').alias('Units')
            ]).drop(['memused', 'memused_minus_diskcache']),

            base_df.with_columns([
                pl.lit('memused_minus_diskcache').alias('Event'),
                pl.col('memused_minus_diskcache').alias('Value'),
                pl.lit('GB').alias('Units')
            ]).drop(['memused', 'memused_minus_diskcache'])
        ]


class NodeDownloader:
    """Download node data with improved parallel processing and quota management"""

    def __init__(
            self,
            base_url: str,
            save_dir: Path,
            quota_manager: DiskQuotaManager,
            session: requests.Session,
            process_queue: Queue,
            max_workers: int = 3
    ):
        self.base_url = base_url
        self.save_dir = save_dir
        self.quota_manager = quota_manager
        self.max_workers = max_workers
        self.session = session
        self.process_queue = process_queue
        self.download_semaphore = threading.Semaphore(20)  # Limit concurrent downloads
        logger.info(f"NodeDownloader initialized with base URL: {base_url}, save directory: {save_dir}")

    def _get_file_size(self, url: str) -> int:
        """Pre-check file size before downloading"""
        try:
            with self.download_semaphore:
                response = self.session.head(url, allow_redirects=True)
                return int(response.headers.get('content-length', 0))
        except Exception as e:
            logger.error(f"Error getting file size for {url}: {e}")
            return 0

    def _check_quota_for_node(self, node_name: str) -> bool:
        """Check if we have enough quota for all files of a node"""
        required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']
        total_size_mb = 0

        for file_name in required_files:
            file_url = urljoin(self.base_url, f"{node_name}/{file_name}")
            size_bytes = self._get_file_size(file_url)
            total_size_mb += size_bytes / (1024 * 1024)

        # Add 10% buffer
        total_size_mb *= 1.1

        return self.quota_manager.request_space(int(total_size_mb))

    def download_node_files(self, node_name: str) -> bool:
        """Download all files for a node with improved error handling and quota management"""
        node_dir = self.save_dir / node_name
        node_dir.mkdir(exist_ok=True)
        logger.info(f"Starting download for node: {node_name}")

        # Check quota before starting downloads
        if not self._check_quota_for_node(node_name):
            logger.warning(f"Insufficient quota for node {node_name}")
            if node_dir.exists():
                shutil.rmtree(node_dir)
            return False

        required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']
        download_success = True

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                future_to_file = {
                    executor.submit(
                        self._download_file,
                        urljoin(self.base_url, f"{node_name}/{file_name}"),
                        node_dir / file_name
                    ): file_name
                    for file_name in required_files
                }

                for future in concurrent.futures.as_completed(future_to_file):
                    file_name = future_to_file[future]
                    try:
                        success = future.result()
                        if not success:
                            download_success = False
                            break
                    except Exception as e:
                        logger.error(f"Error downloading {file_name} for {node_name}: {e}")
                        download_success = False
                        break

            if not download_success and node_dir.exists():
                shutil.rmtree(node_dir)
                # Release quota for failed download
                for file_name in required_files:
                    file_path = node_dir / file_name
                    if file_path.exists():
                        self.quota_manager.release_space(
                            file_path.stat().st_size / (1024 * 1024)
                        )
            else:
                self.process_queue.put(node_name)
                logger.info(f"Added {node_name} to process queue")

            return download_success

        except Exception as e:
            logger.error(f"Error downloading {node_name}: {e}")
            if node_dir.exists():
                shutil.rmtree(node_dir)
            return False

    def _download_file(self, url: str, file_path: Path) -> bool:
        """Download a single file with improved error handling"""
        try:
            with self.download_semaphore:
                logger.info(f"Downloading file: {url}")
                response = self.session.get(url, stream=True)
                response.raise_for_status()

                temp_path = file_path.with_suffix('.tmp')
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                # Atomic rename
                temp_path.rename(file_path)
                logger.info(f"Successfully downloaded {url} to {file_path}")
                return True

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            if temp_path.exists():
                temp_path.unlink()
            return False


class ETLPipeline:
    """Main ETL pipeline implementation"""

    def __init__(self, base_url: str, base_dir: str, quota_mb: int, bucket_name: str,
                 max_download_workers: int = 3, max_process_workers: int = 4, max_retries: int = 3):
        self.base_url = base_url
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.max_retries = max_retries  # Added missing attribute

        # Initialize session with optimized connection pool
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max_download_workers * 4,
            pool_maxsize=max_download_workers * 4,
            max_retries=max_retries,
            pool_block=True
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Initialize managers with strict quota
        self.quota_manager = DiskQuotaManager(quota_mb, safety_buffer_mb=1024)
        self.version_manager = VersionManager(base_dir)
        self.state_manager = ProcessingStateManager(base_dir)
        self.monthly_data_manager = MonthlyDataManager(base_dir, self.version_manager)

        # Initialize processor
        self.processor = NodeDataProcessor()  # Added missing processor initialization

        # Initialize downloader
        self.downloader = NodeDownloader(  # Added missing downloader initialization
            base_url=base_url,
            save_dir=self.base_dir,
            quota_manager=self.quota_manager,
            session=self.session,
            process_queue=self.process_queue,
            max_workers=max_download_workers
        )

        # Initialize S3 uploader
        self.s3_uploader = S3Uploader(bucket_name, max_retries=max_retries)

        # Initialize queues with size limits
        self.download_queue = Queue(maxsize=max_download_workers * 2)
        self.process_queue = Queue(maxsize=max_process_workers * 2)

        self.max_download_workers = max_download_workers
        self.max_process_workers = max_process_workers

        # Event for graceful shutdown
        self.should_stop = threading.Event()

        # Track active operations
        self.active_downloads = set()
        self.active_processing = set()
        self._lock = threading.Lock()

    def cleanup_failed_node(self, node_name: str):
        """Remove all data for a failed node to ensure data integrity"""
        try:
            # Get list of all monthly files
            monthly_files = list(self.monthly_data_manager.monthly_data_dir.glob('*.csv'))

            for file_path in monthly_files:
                temp_path = file_path.with_suffix('.tmp')

                # Read existing data
                df = pl.read_csv(file_path)

                # Remove rows for failed node
                df_filtered = df.filter(pl.col('Host') != node_name)

                # Write to temporary file first
                df_filtered.write_csv(temp_path)

                # Atomic rename
                temp_path.rename(file_path)

            logger.info(f"Cleaned up data for failed node: {node_name}")

            # Clean up node directory if it exists
            node_dir = self.base_dir / node_name
            if node_dir.exists():
                shutil.rmtree(node_dir)

        except Exception as e:
            logger.error(f"Error cleaning up node {node_name}: {e}")

    def process_worker(self):
        while not self.should_stop.is_set():
            try:
                node_name = self.process_queue.get(timeout=1)
                with self._lock:
                    self.active_processing.add(node_name)
                logger.info(f"Processing node {node_name}")
                start_time = time.time()

                node_dir = self.base_dir / node_name
                if not node_dir.exists():
                    logger.warning(f"Node directory missing: {node_name}")
                    continue

                try:
                    # Process each file type
                    dfs = []

                    # Process block file
                    block_path = node_dir / 'block.csv'
                    if block_path.exists():
                        dfs.append(self.processor.process_block_file(block_path))

                    # Process CPU file
                    cpu_path = node_dir / 'cpu.csv'
                    if cpu_path.exists():
                        dfs.append(self.processor.process_cpu_file(cpu_path))

                    # Process NFS file
                    nfs_path = node_dir / 'nfs.csv'
                    if nfs_path.exists():
                        dfs.append(self.processor.process_nfs_file(nfs_path))

                    # Process memory file
                    mem_path = node_dir / 'mem.csv'
                    if mem_path.exists():
                        memory_dfs = self.processor.process_memory_metrics(mem_path)
                        dfs.extend(memory_dfs)

                    if dfs:
                        combined_df = pl.concat(dfs)

                        # Add a month column for partitioning
                        combined_df = combined_df.with_columns([
                            pl.col('Timestamp').dt.strftime('%Y-%m').alias('month')
                        ])

                        # Group by the new month column
                        monthly_data = {}
                        for group in combined_df.partition_by('month'):
                            month_key = group[0, 'month']  # Get first row's month value
                            monthly_data[month_key] = group.drop('month')  # Remove temporary month column

                        self.monthly_data_manager.save_monthly_data(monthly_data)

                    self.state_manager.update_node_status(
                        node_name,
                        processing_complete=True,
                        processing_started=False
                    )
                    shutil.rmtree(node_dir)
                    processing_time = time.time() - start_time
                    logger.info(f"Processing complete: {node_name} - Processing time: {processing_time:.2f} seconds")

                except Exception as e:
                    logger.error(f"Error processing {node_name}: {e}", exc_info=True)
                    self.state_manager.update_node_status(
                        node_name,
                        processing_complete=False,
                        processing_started=False
                    )

                with self._lock:
                    self.active_processing.remove(node_name)
                self.process_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in process worker: {e}", exc_info=True)

    def get_node_list(self) -> List[str]:
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                logging.info(f"Fetching node list from {self.base_url}")
                response = self.session.get(self.base_url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                nodes = [link.text.strip('/') for link in soup.find_all('a') if link.text.startswith('NODE')]

                if not nodes:
                    logging.warning(f"No nodes found at {self.base_url}, retrying...")
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                    continue

                logging.info(f"Found {len(nodes)} nodes to process")
                return nodes
            except requests.exceptions.RequestException as e:
                retry_count += 1
                logging.warning(f"Retry {retry_count}/{self.max_retries} getting node list: {e}")
                time.sleep(2 ** retry_count)

        logging.error("Failed to fetch node list after max retries")
        return []

    def download_worker(self):
        while not self.should_stop.is_set():
            try:
                node_name = self.download_queue.get(timeout=1)
                with self._lock:
                    self.active_downloads.add(node_name)
                logging.info(f"Downloading node {node_name}")

                success = self.downloader.download_node_files(node_name)

                if success:
                    self.state_manager.update_node_status(node_name, download_complete=True)
                    self.process_queue.put(node_name)
                    logging.info(f"Download complete: {node_name}, added to process queue")
                else:
                    self.state_manager.update_node_status(node_name, download_complete=False)
                    logging.warning(f"Download failed: {node_name}")

                with self._lock:
                    self.active_downloads.remove(node_name)
                self.download_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error in download worker: {e}", exc_info=True)

    def check_upload_needed(self) -> bool:
        """Check if we should trigger an S3 upload based on data volume"""
        try:
            monthly_files = list(self.monthly_data_manager.monthly_data_dir.glob('*.csv'))
            total_size_mb = sum(f.stat().st_size for f in monthly_files) / (1024 * 1024)

            threshold_mb = 8192  # 8GB

            logger.debug(f"Current monthly data size: {total_size_mb:.2f} MB")
            return total_size_mb > threshold_mb
        except Exception as e:
            logger.error(f"Error checking upload status: {e}")
            return False

    def upload_to_s3(self) -> bool:
        """Upload files to S3 and cleanup after successful upload"""
        try:
            monthly_files = list(self.monthly_data_manager.monthly_data_dir.glob('*.csv'))
            if self.s3_uploader.upload_files(monthly_files):
                # Increment version after successful upload
                self.version_manager.increment_version()

                # Clean up uploaded files to free space
                for file_path in monthly_files:
                    file_path.unlink()

                # Release quota space
                total_size_mb = sum(f.stat().st_size for f in monthly_files) / (1024 * 1024)
                self.quota_manager.release_space(int(total_size_mb))

                logger.info("Successfully uploaded and cleaned up files")
                return True
            return False
        except Exception as e:
            logger.error(f"Error in upload process: {e}")
            return False

    def run(self):
        download_threads = []  # Initialize before try block
        process_threads = []  # Initialize before try block

        try:
            # Get node list
            nodes = self.get_node_list()
            if not nodes:
                logger.error("No nodes found. Exiting.")
                return

            # Clean up incomplete nodes from previous run
            incomplete_nodes = self.state_manager.get_incomplete_nodes()
            for node in incomplete_nodes:
                logger.info(f"Cleaning up incomplete node: {node}")
                self.cleanup_failed_node(node)

            # Start worker threads
            download_threads = [
                threading.Thread(target=self.download_worker)
                for _ in range(self.max_download_workers)
            ]
            process_threads = [
                threading.Thread(target=self.process_worker)
                for _ in range(self.max_process_workers)
            ]

            for thread in download_threads + process_threads:
                thread.daemon = True
                thread.start()

            # Queue nodes for processing
            for node in nodes:
                if node in incomplete_nodes or not self.state_manager.node_statuses.get(node, None):
                    self.download_queue.put(node)
                    logger.info(f"Queued {node} for processing")

            # Monitor processing and handle uploads
            upload_check_interval = 300  # 5 minutes
            last_upload_check = time.time()

            while True:
                if (self.download_queue.empty() and
                        self.process_queue.empty() and
                        not self.active_downloads and
                        not self.active_processing):
                    break

                current_time = time.time()
                if current_time - last_upload_check > upload_check_interval:
                    if self.check_upload_needed():
                        logger.info("Upload threshold reached. Starting upload...")
                        if self.upload_to_s3():
                            self.version_manager.increment_version()
                    last_upload_check = current_time

                time.sleep(10)

            # Final upload and version increment
            logger.info("Processing complete. Performing final upload...")
            if self.upload_to_s3():
                self.version_manager.increment_version()

        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            self.should_stop.set()
        finally:
            self.should_stop.set()
            for thread in download_threads + process_threads:
                thread.join(timeout=60)
            self.session.close()
            logger.info("ETL pipeline completed")


def main():
    optimal_workers = get_optimal_workers()
    chunk_size = get_optimal_chunk_size()

    # Get total system memory in MB
    total_memory_mb = psutil.virtual_memory().total / (1024 * 1024)
    # Use 80% of total memory for quota
    quota_mb = int(total_memory_mb * 0.8)

    base_dir = os.getenv('ETL_BASE_DIR', '/tmp/etl_data')
    base_url = 'https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/'
    bucket_name = os.getenv('ETL_S3_BUCKET', 'data-transform-stampede')

    pipeline = ETLPipeline(
        base_url=base_url,
        base_dir=base_dir,
        quota_mb=24512,  # 24GB quota
        bucket_name=bucket_name,
        max_download_workers=4,
        max_process_workers=12
    )

    pl.Config.set_streaming_chunk_size(chunk_size)

    # Use ThreadPool for IO-bound operations
    pipeline.run()


if __name__ == '__main__':
    main()
