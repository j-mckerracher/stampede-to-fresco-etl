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
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import boto3
import concurrent.futures
from dataclasses import dataclass, asdict
import logging
from datetime import datetime, timedelta
import polars as pl
import gc

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


class ActivityMonitor:
    """Periodically log activity to show the script is still running"""

    def __init__(self, interval_seconds=300):
        self.interval_seconds = interval_seconds
        self.last_activity = datetime.now()
        self.active_nodes = set()
        self.lock = threading.Lock()
        self._stop_event = threading.Event()
        self.monitor_thread = None

    def start(self):
        """Start the activity monitor in a separate thread"""
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info(f"Activity monitor started (interval: {self.interval_seconds} seconds)")

    def stop(self):
        """Stop the activity monitor"""
        if self.monitor_thread:
            self._stop_event.set()
            self.monitor_thread.join(timeout=5)
            logger.info("Activity monitor stopped")

    def update_activity(self, node_name=None, activity_type=None):
        """Update last activity timestamp and optionally record node activity"""
        with self.lock:
            self.last_activity = datetime.now()
            if node_name:
                self.active_nodes.add((node_name, activity_type or "processing"))

    def remove_node(self, node_name):
        """Remove a node from active nodes"""
        with self.lock:
            self.active_nodes = {(node, activity) for node, activity in self.active_nodes if node != node_name}

    def _monitor_loop(self):
        """Background thread that periodically logs activity"""
        while not self._stop_event.is_set():
            time.sleep(min(30, self.interval_seconds))  # First check after 30 seconds max

            with self.lock:
                time_since_last = datetime.now() - self.last_activity
                active_node_count = len(self.active_nodes)

                # Create a summary of active nodes by activity type
                activity_summary = {}
                for node, activity in self.active_nodes:
                    activity_summary[activity] = activity_summary.get(activity, 0) + 1

                # Log current activity
                if time_since_last.total_seconds() > self.interval_seconds:
                    if active_node_count > 0:
                        logger.info(
                            f"Still running - {active_node_count} active nodes - Activity summary: {activity_summary}")
                    else:
                        logger.info(
                            f"Still running - No active nodes - Last activity: {self.last_activity.strftime('%H:%M:%S')}")
                    self.last_activity = datetime.now()


@dataclass
class NodeStatus:
    """Track processing status of a node"""
    name: str
    download_complete: bool = False
    processing_complete: bool = False
    processing_started: bool = False
    uploaded: bool = False
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
            if not status.processing_complete or not status.uploaded
        ]
        logger.debug(f"Found {len(incomplete_nodes)} incomplete nodes")
        return incomplete_nodes

    def is_node_processed_and_uploaded(self, node_name: str) -> bool:
        status = self.node_statuses.get(node_name)
        if not status:
            return False
        return status.processing_complete and status.uploaded


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

    def save_node_data(self, node_name: str, node_data: pl.DataFrame) -> List[Path]:
        """Save data for a single node, partitioned by month"""
        saved_files = []

        # Add a month column for partitioning
        df_with_month = node_data.with_columns([
            pl.col('Timestamp').dt.strftime('%Y_%m').alias('month')
        ])

        # Group by month
        monthly_data = {}
        for month_group in df_with_month.partition_by('month'):
            if len(month_group) == 0:
                continue

            month_key = month_group[0, 'month']  # Get first row's month value
            monthly_data[month_key] = month_group.drop('month')  # Remove temporary month column

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
                    logger.info(f"Merged {node_name} data into existing file: {file_path}")
                else:
                    df_to_save.write_csv(file_path)
                    logger.info(f"Created new monthly file for {node_name}: {file_path}")

                saved_files.append(file_path)

        return saved_files

    def get_current_monthly_files(self) -> List[Path]:
        """Get list of all current monthly data files"""
        return list(self.monthly_data_dir.glob('*.csv'))


class NodeDataProcessor:
    """Process node data files using Polars"""

    def process_block_file(self, file_path: Path) -> pl.DataFrame:
        logger.info(f"Processing block file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Calculate throughput
            df = df.with_columns([
                pl.col('rd_sectors').cast(pl.Float64).alias('rd_sectors_num'),
                pl.col('wr_sectors').cast(pl.Float64).alias('wr_sectors_num'),
                pl.col('rd_ticks').cast(pl.Float64).alias('rd_ticks_num'),
                pl.col('wr_ticks').cast(pl.Float64).alias('wr_ticks_num')
            ])

            df = df.with_columns([
                (pl.col('rd_sectors_num') + pl.col('wr_sectors_num')).alias('total_sectors'),
                ((pl.col('rd_ticks_num') + pl.col('wr_ticks_num')) / 1000).alias('total_ticks')
            ])

            # Convert to GB/s
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
            ])
        except Exception as e:
            logger.error(f"Error processing block file {file_path}: {e}")
            return pl.DataFrame()

    def process_cpu_file(self, file_path: Path) -> pl.DataFrame:
        logger.info(f"Processing CPU file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            df = df.with_columns([
                (pl.col('user') + pl.col('nice') + pl.col('system') +
                 pl.col('idle') + pl.col('iowait') + pl.col('irq') +
                 pl.col('softirq')).alias('total_ticks')
            ])

            df = df.with_columns([
                pl.when(pl.col('total_ticks') > 0)
                .then(((pl.col('user') + pl.col('nice')) / pl.col('total_ticks')) * 100)
                .otherwise(0)
                .clip(0, 100)
                .alias('Value')
            ])

            return df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('cpuuser').alias('Event'),
                pl.col('Value'),
                pl.lit('CPU %').alias('Units')
            ])
        except Exception as e:
            logger.error(f"Error processing CPU file {file_path}: {e}")
            return pl.DataFrame()

    def process_nfs_file(self, file_path: Path) -> pl.DataFrame:
        logger.info(f"Processing NFS file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            df = df.with_columns([
                ((pl.col('READ_bytes_recv') + pl.col('WRITE_bytes_sent')) / (1024 * 1024)).alias('Value'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp')
            ])

            # Calculate time differences
            df = df.with_columns([
                pl.col('Timestamp').diff().cast(pl.Duration).dt.total_seconds().fill_null(600).alias('TimeDiff')
            ])

            df = df.with_columns([
                (pl.col('Value') / pl.col('TimeDiff')).alias('Value')
            ])

            return df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('nfs').alias('Event'),
                pl.col('Value'),
                pl.lit('MB/s').alias('Units')
            ])
        except Exception as e:
            logger.error(f"Error processing NFS file {file_path}: {e}")
            return pl.DataFrame()

    def process_memory_metrics(self, file_path: Path) -> List[pl.DataFrame]:
        logger.info(f"Processing memory file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Convert KB to bytes
            memory_cols = ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']
            df = df.with_columns([
                pl.col(col).mul(1024) for col in memory_cols if col in df.columns
            ])

            # Calculate metrics in GB
            df = df.with_columns([
                (pl.col('MemUsed') / (1024 ** 3)).alias('memused'),
                ((pl.col('MemUsed') - pl.col('FilePages')) / (1024 ** 3))
                .clip(0, None)
                .alias('memused_minus_diskcache')
            ])

            memused_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('memused').alias('Event'),
                pl.col('memused').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            memused_nocache_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('memused_minus_diskcache').alias('Event'),
                pl.col('memused_minus_diskcache').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            return [memused_df, memused_nocache_df]
        except Exception as e:
            logger.error(f"Error processing memory file {file_path}: {e}")
            return []

    def process_node_data(self, node_dir: Path) -> pl.DataFrame:
        """Process all files for a node and return combined dataframe"""
        logger.info(f"Processing all files for node in directory: {node_dir}")

        dfs = []
        try:
            # Process block file
            block_path = node_dir / 'block.csv'
            if block_path.exists() and block_path.stat().st_size > 0:
                block_df = self.process_block_file(block_path)
                if block_df and len(block_df) > 0:
                    dfs.append(block_df)
                # Delete file after processing
                os.remove(block_path)

            # Process CPU file
            cpu_path = node_dir / 'cpu.csv'
            if cpu_path.exists() and cpu_path.stat().st_size > 0:
                cpu_df = self.process_cpu_file(cpu_path)
                if cpu_df and len(cpu_df) > 0:
                    dfs.append(cpu_df)
                # Delete file after processing
                os.remove(cpu_path)

            # Process NFS file
            nfs_path = node_dir / 'nfs.csv'
            if nfs_path.exists() and nfs_path.stat().st_size > 0:
                nfs_df = self.process_nfs_file(nfs_path)
                if nfs_df and len(nfs_df) > 0:
                    dfs.append(nfs_df)
                # Delete file after processing
                os.remove(nfs_path)

            # Process memory file
            mem_path = node_dir / 'mem.csv'
            if mem_path.exists() and mem_path.stat().st_size > 0:
                memory_dfs = self.process_memory_metrics(mem_path)
                if memory_dfs:
                    dfs.extend(memory_dfs)
                # Delete file after processing
                os.remove(mem_path)

            # Combine all dataframes
            if dfs:
                result = pl.concat(dfs)
                logger.info(f"Successfully processed node data with {len(result)} rows")
                return result
            else:
                logger.warning(f"No valid data processed for node in {node_dir}")
                return pl.DataFrame()

        except Exception as e:
            logger.error(f"Error processing node data in {node_dir}: {e}")
            return pl.DataFrame()


class NodeDownloader:
    """Download node data with parallel processing"""

    def __init__(
            self,
            base_url: str,
            save_dir: Path,
            quota_manager: DiskQuotaManager,
            session: requests.Session,
            max_workers: int = 3
    ):
        self.base_url = base_url
        self.save_dir = save_dir
        self.quota_manager = quota_manager
        self.max_workers = max_workers or multiprocessing.cpu_count()
        self.session = session
        logger.info(f"NodeDownloader initialized with base URL: {base_url}, save directory: {save_dir}")

    def download_node_files(self, node_name: str) -> bool:
        node_dir = self.save_dir / node_name
        node_dir.mkdir(exist_ok=True)
        logger.info(f"Starting download for node: {node_name}")

        node_url = urljoin(self.base_url, f"{node_name}/")
        required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']

        try:
            response = self.session.get(node_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            download_tasks = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                for file_name in required_files:
                    file_url = urljoin(node_url, file_name)
                    file_path = node_dir / file_name

                    download_tasks.append(
                        executor.submit(
                            self._download_file,
                            file_url,
                            file_path
                        )
                    )

                success = all(task.result() for task in download_tasks)

            if not success:
                shutil.rmtree(node_dir)
                logger.warning(f"Failed to download all files for node {node_name}")
            else:
                logger.info(f"Successfully downloaded all files for node {node_name}")

            return success

        except Exception as e:
            logger.error(f"Error downloading {node_name}: {e}")
            if node_dir.exists():
                shutil.rmtree(node_dir)
            return False

    def _download_file(self, url: str, file_path: Path) -> bool:
        try:
            logger.info(f"Downloading file: {url}")
            response = self.session.get(url, stream=True)
            response.raise_for_status()

            # Get file size and request quota
            file_size = int(response.headers.get('content-length', 0))
            if not self.quota_manager.request_space(file_size // (1024 * 1024) + 1):
                logger.warning(f"Insufficient space to download {url}")
                return False

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Successfully downloaded {url} to {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return False
        finally:
            # Release quota space if download failed
            if not file_path.exists():
                self.quota_manager.release_space(file_size // (1024 * 1024) + 1)
                logger.info(f"Released quota space for failed download: {url}")


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
                        file_path.name,
                        ExtraArgs={'ContentType': 'text/csv'}
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


class ETLPipeline:
    """Main ETL pipeline with per-node processing and immediate uploads"""

    def __init__(self, base_url: str, base_dir: str, quota_mb: int, bucket_name: str,
                 max_workers: int = 4, max_retries: int = 3,
                 activity_monitor_interval: int = 300):
        self.base_url = base_url
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.max_retries = max_retries

        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=20, max_retries=max_retries, pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.quota_manager = DiskQuotaManager(quota_mb)
        self.version_manager = VersionManager(base_dir)
        self.state_manager = ProcessingStateManager(base_dir)
        self.monthly_data_manager = MonthlyDataManager(base_dir, self.version_manager)
        self.s3_uploader = S3Uploader(bucket_name)
        self.processor = NodeDataProcessor()
        self.downloader = NodeDownloader(
            base_url=base_url,
            save_dir=self.base_dir,
            quota_manager=self.quota_manager,
            session=self.session,
            max_workers=max_workers
        )

        self.max_workers = max_workers

        # Initialize activity monitor
        self.activity_monitor = ActivityMonitor(interval_seconds=activity_monitor_interval)

        logger.info("ETL pipeline initialized with immediate per-node processing and upload")

    def run(self):
        """Run the ETL pipeline with immediate per-node processing and uploads"""
        try:
            # Start activity monitor
            self.activity_monitor.start()
            self.activity_monitor.update_activity(activity_type="initialization")

            # 1. Get list of nodes
            nodes = self.get_node_list()
            if not nodes:
                logger.error("No nodes found. Exiting.")
                return

            self.activity_monitor.update_activity(activity_type="found_nodes")

            # 2. Process nodes with parallel processing
            total_nodes = len(nodes)
            logger.info(f"Starting processing of {total_nodes} nodes")

            # Filter nodes that need processing
            nodes_to_process = [node for node in nodes
                                if not self.state_manager.is_node_processed_and_uploaded(node)]
            logger.info(f"Found {len(nodes_to_process)} nodes that need processing")

            if not nodes_to_process:
                logger.info("All nodes are already processed. Nothing to do.")
                return

            # Process nodes in parallel for better efficiency
            processed_count = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_node = {executor.submit(self.process_and_upload_node, node): node
                                  for node in nodes_to_process}

                logger.info(f"Submitted {len(future_to_node)} nodes for processing")
                self.activity_monitor.update_activity(activity_type="submitted_jobs")

                # Periodically log overall status
                last_status_time = time.time()

                for future in concurrent.futures.as_completed(future_to_node):
                    node = future_to_node[future]
                    try:
                        success = future.result()
                        processed_count += 1

                        # Print progress
                        print_progress(processed_count, len(future_to_node),
                                       prefix='Overall Progress:',
                                       suffix=f'({processed_count}/{len(future_to_node)})')

                        # Log progress every 10 nodes or 5 minutes, whichever comes first
                        current_time = time.time()
                        if processed_count % 10 == 0 or (current_time - last_status_time) > 300:
                            logger.info(f"Progress: {processed_count}/{len(future_to_node)} nodes completed")
                            last_status_time = current_time

                        if success:
                            logger.info(f"Successfully processed node {node} ({processed_count}/{len(future_to_node)})")
                        else:
                            logger.warning(f"Failed to process node {node} ({processed_count}/{len(future_to_node)})")

                        # Update activity monitor
                        self.activity_monitor.remove_node(node)
                        self.activity_monitor.update_activity(activity_type="completed_node")

                    except Exception as e:
                        logger.error(f"Error processing node {node}: {e}")
                        self.activity_monitor.remove_node(node)

            logger.info(f"Processing completed! Processed {processed_count} nodes.")

        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}", exc_info=True)
        finally:
            self.activity_monitor.stop()
            self.session.close()

    def get_node_list(self) -> List[str]:
        """Get list of all nodes from the base URL"""
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

    def process_and_upload_node(self, node_name: str) -> bool:
        """Process and upload a single node's data, following the sequential steps"""
        try:
            # Update activity monitor
            self.activity_monitor.update_activity(node_name, "starting")

            # Skip if already processed
            if self.state_manager.is_node_processed_and_uploaded(node_name):
                logger.info(f"Node {node_name} already processed and uploaded, skipping")
                self.activity_monitor.remove_node(node_name)
                return True

            # 1. Check disk space
            if self.quota_manager.get_available_space_mb() < 500:  # Minimum 500MB required
                logger.error(f"Insufficient disk space to process node {node_name}")
                self.activity_monitor.remove_node(node_name)
                return False

            # 2. Download
            logger.info(f"Downloading node {node_name}")
            self.activity_monitor.update_activity(node_name, "downloading")

            if not self.downloader.download_node_files(node_name):
                logger.error(f"Failed to download files for node {node_name}")
                self.state_manager.update_node_status(node_name, download_complete=False)
                self.activity_monitor.remove_node(node_name)
                return False

            self.state_manager.update_node_status(node_name, download_complete=True)

            # 3. Process
            logger.info(f"Processing node {node_name}")
            self.activity_monitor.update_activity(node_name, "processing")

            node_dir = self.base_dir / node_name
            self.state_manager.update_node_status(node_name, processing_started=True)

            # Add file size logging for debugging
            try:
                for file_path in node_dir.glob('*.csv'):
                    file_size_mb = file_path.stat().st_size / (1024 * 1024)
                    logger.info(f"Processing file {file_path.name} for node {node_name} (size: {file_size_mb:.2f} MB)")
            except Exception as e:
                logger.warning(f"Error logging file sizes for {node_name}: {e}")

            node_df = self.processor.process_node_data(node_dir)

            # Log completion of processing
            self.activity_monitor.update_activity(node_name, "processed_files")

            if node_df.is_empty():
                logger.warning(f"No data processed for node {node_name}")
                self.state_manager.update_node_status(node_name, processing_complete=False)
                shutil.rmtree(node_dir)
                self.activity_monitor.remove_node(node_name)
                return False

            # 4. Save to monthly files
            logger.info(f"Saving node {node_name} data to monthly files")
            self.activity_monitor.update_activity(node_name, "saving")

            saved_files = self.monthly_data_manager.save_node_data(node_name, node_df)
            if not saved_files:
                logger.warning(f"No files saved for node {node_name}")
                self.state_manager.update_node_status(node_name, processing_complete=False)
                shutil.rmtree(node_dir)
                self.activity_monitor.remove_node(node_name)
                return False

            # 5. Upload to S3
            logger.info(f"Uploading node {node_name} data to S3")
            self.activity_monitor.update_activity(node_name, "uploading")

            if self.s3_uploader.upload_files(saved_files):
                self.version_manager.increment_version()
                self.state_manager.update_node_status(
                    node_name,
                    processing_complete=True,
                    uploaded=True
                )
                # Clean up
                for file_path in saved_files:
                    os.remove(file_path)
                    logger.info(f"Removed file after upload: {file_path}")

                # Clean up node directory
                shutil.rmtree(node_dir, ignore_errors=True)
                logger.info(f"Successfully processed and uploaded node {node_name}")

                # Force garbage collection
                del node_df
                gc.collect()

                self.activity_monitor.remove_node(node_name)
                return True
            else:
                logger.error(f"Failed to upload files for node {node_name}")
                self.state_manager.update_node_status(node_name, processing_complete=True, uploaded=False)
                shutil.rmtree(node_dir, ignore_errors=True)
                self.activity_monitor.remove_node(node_name)
                return False

        except Exception as e:
            logger.error(f"Error processing node {node_name}: {e}", exc_info=True)
            self.state_manager.update_node_status(node_name, processing_complete=False)
            # Clean up node directory if it exists
            node_dir = self.base_dir / node_name
            if node_dir.exists():
                shutil.rmtree(node_dir, ignore_errors=True)
            self.activity_monitor.remove_node(node_name)
            return False


def print_progress(current: int, total: int, prefix: str = '', suffix: str = ''):
    """Print progress as a percentage with a progress bar"""
    percent = (current / total) * 100
    bar_length = 50
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    print(f'\r{prefix} |{bar}| {percent:.1f}% {suffix}', end='', flush=True)
    if current == total:
        print()


def get_default_base_dir():
    """Get platform-appropriate default base directory"""
    if os.name == 'nt':  # Windows
        return os.path.join(os.environ.get('TEMP', os.path.expanduser('~')), 'etl_data')
    else:  # Unix-like systems
        return '/tmp/etl_data'


def main():
    """Main entry point"""
    base_dir = os.getenv('ETL_BASE_DIR', get_default_base_dir())
    base_url = 'https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/'
    quota_mb = int(os.getenv('ETL_QUOTA_MB', '24512'))
    bucket_name = os.getenv('ETL_S3_BUCKET', 'data-transform-stampede')
    max_workers = int(os.getenv('ETL_MAX_WORKERS', os.cpu_count() or 4))
    activity_monitor_interval = int(os.getenv('ETL_ACTIVITY_INTERVAL', '300'))

    # Print configuration
    logger.info(f"ETL Configuration:")
    logger.info(f"  - Base URL: {base_url}")
    logger.info(f"  - Base Directory: {base_dir}")
    logger.info(f"  - Quota: {quota_mb} MB")
    logger.info(f"  - S3 Bucket: {bucket_name}")
    logger.info(f"  - Max Workers: {max_workers}")
    logger.info(f"  - Activity Monitor Interval: {activity_monitor_interval} seconds")

    pipeline = ETLPipeline(
        base_url=base_url,
        base_dir=base_dir,
        quota_mb=quota_mb,
        bucket_name=bucket_name,
        max_workers=max_workers,
        activity_monitor_interval=activity_monitor_interval
    )

    pipeline.run()


if __name__ == '__main__':
    main()