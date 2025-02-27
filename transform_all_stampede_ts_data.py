import multiprocessing
import os
import json
import time
import shutil
import threading
from pathlib import Path
from typing import Dict, List, Optional
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
        self.node_progress = {}  # Track progress for each node
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

    def update_activity(self, node_name=None, activity_type=None, progress=None):
        """Update last activity timestamp and optionally record node activity with progress"""
        with self.lock:
            self.last_activity = datetime.now()
            if node_name:
                self.active_nodes.add((node_name, activity_type or "processing"))
                if progress is not None:
                    self.node_progress[node_name] = {
                        'activity': activity_type,
                        'progress': progress,
                        'last_update': datetime.now()
                    }

    def get_stalled_nodes(self, stall_threshold_seconds=600):
        """Return nodes that haven't made progress in the specified time"""
        stalled_nodes = []
        now = datetime.now()
        with self.lock:
            for node, progress_info in self.node_progress.items():
                time_since_update = (now - progress_info['last_update']).total_seconds()
                if time_since_update > stall_threshold_seconds:
                    stalled_nodes.append((node, progress_info['activity'], time_since_update))
        return stalled_nodes

    def remove_node(self, node_name):
        """Remove a node from active nodes"""
        with self.lock:
            self.active_nodes = {(node, activity) for node, activity in self.active_nodes if node != node_name}
            if node_name in self.node_progress:
                del self.node_progress[node_name]

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

                # Check for stalled nodes
                stalled_nodes = self.get_stalled_nodes()
                if stalled_nodes:
                    logger.warning(f"Detected {len(stalled_nodes)} stalled nodes: {stalled_nodes}")

                # Log current activity
                if time_since_last.total_seconds() > self.interval_seconds:
                    if active_node_count > 0:
                        logger.info(
                            f"Still running - {active_node_count} active nodes - Activity summary: {activity_summary}")
                    else:
                        logger.info(
                            f"Still running - No active nodes - Last activity: {self.last_activity.strftime('%H:%M:%S')}")
                    self.last_activity = datetime.now()


class DownloadProgressTracker:
    """Track progress of file downloads to detect stalled downloads"""

    def __init__(self, url, expected_size_bytes, timeout_seconds=300):
        self.url = url
        self.expected_size_bytes = expected_size_bytes
        self.timeout_seconds = timeout_seconds
        self.bytes_downloaded = 0
        self.last_progress_time = datetime.now()
        self.is_stalled = False

    def update_progress(self, chunk_size):
        """Update download progress"""
        self.bytes_downloaded += chunk_size
        self.last_progress_time = datetime.now()

    def check_stalled(self):
        """Check if download is stalled"""
        time_since_progress = (datetime.now() - self.last_progress_time).total_seconds()
        if time_since_progress > self.timeout_seconds:
            self.is_stalled = True
            return True
        return False

    def get_progress_percentage(self):
        """Get download progress as percentage"""
        if self.expected_size_bytes > 0:
            return (self.bytes_downloaded / self.expected_size_bytes) * 100
        return 0


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
    retry_count: int = 0  # Track retry attempts


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

    def increment_retry_count(self, node_name: str) -> int:
        """Increment retry count for a node and return the new count"""
        with self._lock:
            if node_name not in self.node_statuses:
                self.node_statuses[node_name] = NodeStatus(name=node_name)

            status = self.node_statuses[node_name]
            status.retry_count += 1
            self.save_state()
            return status.retry_count


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
            max_workers: int = 3,
            download_timeout: int = 300,  # Added download timeout parameter
            connect_timeout: int = 10  # Added connect timeout parameter
    ):
        self.base_url = base_url
        self.save_dir = save_dir
        self.quota_manager = quota_manager
        self.max_workers = max_workers or multiprocessing.cpu_count()
        self.session = session
        self.download_timeout = download_timeout
        self.connect_timeout = connect_timeout
        logger.info(f"NodeDownloader initialized with base URL: {base_url}, save directory: {save_dir}, "
                    f"download timeout: {download_timeout}s, connect timeout: {connect_timeout}s")

    def download_node_files(self, node_name: str) -> bool:
        node_dir = self.save_dir / node_name
        node_dir.mkdir(exist_ok=True)
        logger.info(f"Starting download for node: {node_name}")

        node_url = urljoin(self.base_url, f"{node_name}/")
        required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']

        try:
            # Use timeout for the initial connection
            logger.debug(f"Attempting to connect to {node_url} with timeout={self.connect_timeout}s")
            start_time = time.time()

            try:
                response = self.session.get(node_url, timeout=(self.connect_timeout, 30))
                elapsed = time.time() - start_time
                logger.info(f"Initial connection to {node_url} took {elapsed:.2f}s (status: {response.status_code})")
                response.raise_for_status()
            except requests.exceptions.Timeout:
                logger.error(f"Initial connection to {node_url} timed out after {self.connect_timeout}s")
                if node_dir.exists():
                    shutil.rmtree(node_dir, ignore_errors=True)
                return False
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error to {node_url}: {e}")
                if node_dir.exists():
                    shutil.rmtree(node_dir, ignore_errors=True)
                return False
            except Exception as e:
                logger.error(f"Error connecting to {node_url}: {e}")
                if node_dir.exists():
                    shutil.rmtree(node_dir, ignore_errors=True)
                return False

            # Test if we can parse the response
            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a')
                logger.debug(f"Found {len(links)} links on {node_url}")
            except Exception as e:
                logger.error(f"Error parsing HTML from {node_url}: {e}")
                if node_dir.exists():
                    shutil.rmtree(node_dir, ignore_errors=True)
                return False

            # First check if files actually exist before attempting download
            file_exists = {}
            for file_name in required_files:
                file_url = urljoin(node_url, file_name)
                try:
                    # Just do a HEAD request to check if file exists
                    head_response = self.session.head(file_url, timeout=(5, 10))
                    file_exists[file_name] = head_response.status_code == 200
                    logger.debug(f"File check: {file_url} - Status: {head_response.status_code}")
                except Exception as e:
                    logger.warning(f"Error checking if file exists {file_url}: {e}")
                    file_exists[file_name] = False

            # Log which files we'll be downloading
            available_files = [f for f, exists in file_exists.items() if exists]
            logger.info(
                f"Found {len(available_files)}/{len(required_files)} available files for {node_name}: {available_files}")

            if not available_files:
                logger.warning(f"No files available to download for {node_name}")
                if node_dir.exists():
                    shutil.rmtree(node_dir, ignore_errors=True)
                return False

            download_tasks = []
            success_flags = []

            # Create a ThreadPoolExecutor with a specific max_workers value
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                for file_name in required_files:
                    if not file_exists.get(file_name, False):
                        logger.debug(f"Skipping unavailable file: {file_name} for node {node_name}")
                        continue

                    file_url = urljoin(node_url, file_name)
                    file_path = node_dir / file_name

                    # Submit the download task
                    download_tasks.append(
                        executor.submit(
                            self._download_file,
                            file_url,
                            file_path
                        )
                    )

                if not download_tasks:
                    logger.warning(f"No download tasks created for node {node_name}")
                    if node_dir.exists():
                        shutil.rmtree(node_dir, ignore_errors=True)
                    return False

                logger.info(f"Started {len(download_tasks)} download tasks for node {node_name}")

                # Wait for tasks with timeout
                for i, task in enumerate(download_tasks):
                    try:
                        task_start = time.time()
                        # Add timeout to prevent hanging tasks
                        result = task.result(timeout=self.download_timeout + 60)  # Add buffer to timeout
                        task_elapsed = time.time() - task_start
                        logger.debug(
                            f"Download task {i + 1}/{len(download_tasks)} for {node_name} completed in {task_elapsed:.2f}s: {result}")
                        success_flags.append(result)
                    except concurrent.futures.TimeoutError:
                        logger.error(
                            f"Download task {i + 1}/{len(download_tasks)} for node {node_name} timed out after {self.download_timeout + 60}s")
                        success_flags.append(False)
                    except Exception as e:
                        logger.error(f"Download task {i + 1}/{len(download_tasks)} for node {node_name} failed: {e}")
                        success_flags.append(False)

            success = all(success_flags) and len(success_flags) > 0
            logger.info(f"Download summary for {node_name}: {sum(success_flags)}/{len(success_flags)} successful")

            if not success:
                shutil.rmtree(node_dir, ignore_errors=True)
                logger.warning(f"Failed to download all files for node {node_name}")
            else:
                logger.info(f"Successfully downloaded all files for node {node_name}")

            return success

        except requests.exceptions.Timeout:
            logger.error(f"Timeout while accessing node URL for {node_name}")
            if node_dir.exists():
                shutil.rmtree(node_dir, ignore_errors=True)
            return False
        except Exception as e:
            logger.error(f"Error downloading {node_name}: {e}")
            if node_dir.exists():
                shutil.rmtree(node_dir, ignore_errors=True)
            return False

    def _download_file(self, url: str, file_path: Path) -> bool:
        progress_tracker = None
        file_size = 0
        start_time = time.time()

        try:
            logger.info(f"Downloading file: {url}")

            # Use timeout for initial connection - with more detailed error handling
            try:
                connect_start = time.time()
                response = self.session.get(url, stream=True, timeout=(self.connect_timeout, 30))
                connect_time = time.time() - connect_start
                logger.debug(f"Connection established to {url} in {connect_time:.2f}s (status: {response.status_code})")
                response.raise_for_status()
            except requests.exceptions.Timeout:
                logger.error(f"Timeout connecting to {url} after {self.connect_timeout}s")
                return False
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error connecting to {url}: {e}")
                return False
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error for {url}: {e}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error connecting to {url}: {e}")
                return False

            # Get file size and request quota
            file_size = int(response.headers.get('content-length', 0))
            if file_size == 0:
                logger.warning(f"Could not determine file size for {url}, headers: {dict(response.headers)}")
            else:
                logger.debug(f"File size for {url}: {file_size} bytes ({file_size / (1024 * 1024):.2f} MB)")

            # Initialize progress tracker
            progress_tracker = DownloadProgressTracker(url, file_size, self.download_timeout)

            # Request space
            space_needed = max(1, file_size // (1024 * 1024))
            if not self.quota_manager.request_space(space_needed):
                logger.warning(f"Insufficient space to download {url} (needed: {space_needed} MB)")
                return False

            bytes_downloaded = 0
            chunk_count = 0
            last_log_time = time.time()

            with open(file_path, 'wb') as f:
                # Use a smaller chunk size for more frequent progress updates
                for chunk in response.iter_content(chunk_size=4096):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        chunk_count += 1
                        progress_tracker.update_progress(len(chunk))

                        # Log progress periodically (every 5 seconds)
                        current_time = time.time()
                        if current_time - last_log_time > 5:
                            elapsed = current_time - start_time
                            progress_pct = (bytes_downloaded / file_size * 100) if file_size > 0 else 0
                            download_rate = bytes_downloaded / (1024 * elapsed) if elapsed > 0 else 0
                            logger.debug(f"Download progress for {url}: {bytes_downloaded}/{file_size} bytes "
                                         f"({progress_pct:.1f}%) at {download_rate:.1f} KB/s, {chunk_count} chunks received")
                            last_log_time = current_time

                        # Check if download is stalled
                        if progress_tracker.check_stalled():
                            logger.warning(f"Download stalled for {url} after {self.download_timeout}s")
                            return False

            download_time = time.time() - start_time
            download_rate = bytes_downloaded / (1024 * download_time) if download_time > 0 else 0

            # Verify file exists and has content
            if not file_path.exists():
                logger.warning(f"Downloaded file {file_path} does not exist after download")
                return False

            actual_size = file_path.stat().st_size
            if actual_size == 0:
                logger.warning(f"Downloaded file {file_path} is empty")
                return False

            # Check if downloaded size matches expected size
            if file_size > 0 and actual_size != file_size:
                logger.warning(f"Size mismatch for {file_path}: expected {file_size} bytes, got {actual_size} bytes")
                return False

            logger.info(
                f"Successfully downloaded {url} to {file_path} ({actual_size} bytes in {download_time:.2f}s, {download_rate:.1f} KB/s)")
            return True

        except requests.exceptions.Timeout:
            logger.error(f"Timeout downloading {url}")
            return False
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error downloading {url}: {e}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error downloading {url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return False
        finally:
            # Clean up partial file if download failed
            if file_path.exists() and (progress_tracker is None or progress_tracker.is_stalled):
                try:
                    os.remove(file_path)
                    logger.info(f"Removed partial download: {file_path}")
                except Exception as e:
                    logger.error(f"Error removing partial file {file_path}: {e}")

            # Release quota space if download failed
            if not file_path.exists() and file_size > 0:
                self.quota_manager.release_space(max(1, file_size // (1024 * 1024)))
                logger.info(f"Released quota space for failed download: {url}")


class S3Uploader:
    """Handle S3 uploads with retries"""

    def __init__(self, bucket_name: str, max_retries: int = 3, timeout: int = 300):
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        self.timeout = timeout

        # Configure S3 client with timeouts - using botocore Config
        try:
            from botocore.config import Config
            config = Config(
                connect_timeout=30,
                read_timeout=timeout,
                retries={'max_attempts': max_retries}
            )
            self.s3_client = boto3.client('s3', config=config)
        except (ImportError, AttributeError):
            # Fallback if Config import fails
            self.s3_client = boto3.client('s3')
            logger.warning("Could not configure S3 client with timeouts, using default configuration")

        logger.info(f"S3Uploader initialized with bucket: {bucket_name}, timeout: {timeout}s")

    def upload_files(self, file_paths: List[Path]) -> bool:
        success = True
        for file_path in file_paths:
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    logger.info(f"Uploading {file_path} to S3 bucket {self.bucket_name}")
                    # Use a thread with timeout to monitor the upload
                    upload_thread = threading.Thread(
                        target=self._upload_file_with_timeout,
                        args=(file_path,)
                    )
                    upload_thread.start()
                    upload_thread.join(timeout=self.timeout)

                    if upload_thread.is_alive():
                        # Upload is taking too long, consider it failed
                        logger.error(f"Upload of {file_path} timed out after {self.timeout}s")
                        retry_count += 1
                        # Wait before retrying
                        time.sleep(min(30, 2 ** retry_count))
                        continue

                    logger.info(f"Successfully uploaded {file_path}")
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == self.max_retries:
                        logger.error(f"Failed to upload {file_path} after {self.max_retries} attempts: {e}")
                        success = False
                    else:
                        logger.warning(f"Retry {retry_count}/{self.max_retries} uploading {file_path}: {e}")
                    time.sleep(min(30, 2 ** retry_count))

        return success

    def _upload_file_with_timeout(self, file_path: Path):
        """Upload a file to S3, used within a thread with timeout monitoring"""
        try:
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                file_path.name,
                ExtraArgs={'ContentType': 'text/csv'}
            )
        except Exception as e:
            logger.error(f"Error in threaded upload of {file_path}: {e}")
            raise


class ETLPipeline:
    """Main ETL pipeline with per-node processing and immediate uploads"""

    def __init__(self, base_url: str, base_dir: str, quota_mb: int, bucket_name: str,
                 max_workers: int = 4, max_retries: int = 3,
                 activity_monitor_interval: int = 300,
                 download_timeout: int = 300,
                 max_node_retries: int = 2,
                 stall_detection_timeout: int = 600,
                 socket_timeout: int = 30):
        self.base_url = base_url
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.max_retries = max_retries
        self.max_node_retries = max_node_retries
        self.stall_detection_timeout = stall_detection_timeout
        self.socket_timeout = socket_timeout

        # Set global socket timeout for all operations
        import socket
        socket.setdefaulttimeout(socket_timeout)
        logger.info(f"Set global socket timeout to {socket_timeout} seconds")

        # Configure session with better connection pooling and timeout settings
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=min(max_workers * 2, 40),  # Cap at reasonable number
            pool_maxsize=min(max_workers * 4, 80),  # Cap at reasonable number
            max_retries=max_retries,
            pool_block=True  # Block when pool is full instead of discarding
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.quota_manager = DiskQuotaManager(quota_mb)
        self.version_manager = VersionManager(base_dir)
        self.state_manager = ProcessingStateManager(base_dir)
        self.monthly_data_manager = MonthlyDataManager(base_dir, self.version_manager)
        self.s3_uploader = S3Uploader(bucket_name, max_retries=max_retries, timeout=download_timeout)
        self.processor = NodeDataProcessor()
        self.downloader = NodeDownloader(
            base_url=base_url,
            save_dir=self.base_dir,
            quota_manager=self.quota_manager,
            session=self.session,
            max_workers=max_workers,
            download_timeout=download_timeout
        )

        self.max_workers = max_workers

        # Initialize activity monitor with stall detection
        self.activity_monitor = ActivityMonitor(interval_seconds=activity_monitor_interval)

        logger.info(f"ETL pipeline initialized with immediate per-node processing and upload. "
                    f"Max workers: {max_workers}, download timeout: {download_timeout}s, "
                    f"max node retries: {max_node_retries}")

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

            # First, validate the data source URL is accessible
            try:
                logger.info(f"Validating data source accessibility: {self.base_url}")
                test_response = self.session.get(self.base_url, timeout=(5, 10))
                test_response.raise_for_status()
                logger.info(f"Data source validation succeeded: status {test_response.status_code}")
            except Exception as e:
                logger.error(f"Data source validation failed: {e}")
                logger.error("Cannot proceed with processing as data source is not accessible")
                return

            # First try with a single node to validate the pipeline
            try:
                first_node = nodes_to_process[0]
                logger.info(f"Testing pipeline with a single node: {first_node}")

                # Use lower level timeouts for the test
                self.downloader.download_timeout = 60
                self.downloader.connect_timeout = 5

                # Try to process one node with detailed logging
                success = self.process_test_node(first_node)

                if not success:
                    logger.error(f"Test node {first_node} failed processing - pipeline may have issues")
                    logger.error("Continuing with batch processing but expect failures")
                else:
                    logger.info(f"Test node {first_node} processed successfully - pipeline is working")

                # Reset timeouts to normal
                self.downloader.download_timeout = 300
                self.downloader.connect_timeout = 10

            except Exception as e:
                logger.error(f"Error testing pipeline with node {first_node}: {e}")
                logger.error("Continuing with batch processing but expect failures")

            # Use a smaller batch size to prevent overwhelming the server
            batch_size = min(self.max_workers, 5)  # Reduced batch size
            total_nodes_to_process = len(nodes_to_process)
            processed_count = 0

            # Process in batches
            for i in range(0, total_nodes_to_process, batch_size):
                batch = nodes_to_process[i:i + batch_size]
                logger.info(f"Processing batch {i // batch_size + 1} with {len(batch)} nodes")

                # Process current batch in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(batch_size, self.max_workers)) as executor:
                    future_to_node = {executor.submit(self.process_and_upload_node, node): node
                                      for node in batch}

                    logger.info(f"Submitted {len(future_to_node)} nodes for processing in current batch")
                    self.activity_monitor.update_activity(activity_type="submitted_batch")

                    # Track active futures
                    active_futures = list(future_to_node.keys())
                    completed_futures = []
                    start_time = time.time()

                    # Process futures with timeout detection
                    while active_futures:
                        # Wait for some futures to complete (with timeout)
                        done, active_futures = concurrent.futures.wait(
                            active_futures,
                            timeout=60,  # Check every minute
                            return_when=concurrent.futures.FIRST_COMPLETED
                        )

                        # Process completed futures
                        for future in done:
                            node = future_to_node[future]
                            try:
                                success = future.result(timeout=10)  # Short timeout to get result
                                processed_count += 1

                                # Update progress
                                print_progress(processed_count, total_nodes_to_process,
                                               prefix='Overall Progress:',
                                               suffix=f'({processed_count}/{total_nodes_to_process})')

                                if success:
                                    logger.info(
                                        f"Successfully processed node {node} ({processed_count}/{total_nodes_to_process})")
                                else:
                                    logger.warning(f"Failed to process node {node}")

                                # Update activity monitor
                                self.activity_monitor.remove_node(node)
                                completed_futures.append(future)

                            except Exception as e:
                                logger.error(f"Error getting result for node {node}: {e}")
                                self.activity_monitor.remove_node(node)
                                completed_futures.append(future)

                        # Check for stalled futures
                        elapsed_time = time.time() - start_time
                        if elapsed_time > self.stall_detection_timeout and active_futures:
                            stalled_nodes = [future_to_node[f] for f in active_futures]
                            logger.warning(f"Detected potential stalled tasks for nodes: {stalled_nodes}")

                            # Check with activity monitor
                            stalled_nodes_info = self.activity_monitor.get_stalled_nodes(self.stall_detection_timeout)
                            if stalled_nodes_info:
                                logger.warning(f"Activity monitor confirms stalled nodes: {stalled_nodes_info}")

                                # Cancel stalled tasks (by adding to completed and removing from active)
                                for future in list(active_futures):
                                    node = future_to_node[future]
                                    if any(node == n for n, _, _ in stalled_nodes_info):
                                        logger.warning(f"Cancelling stalled task for node {node}")
                                        future.cancel()
                                        self.activity_monitor.remove_node(node)
                                        completed_futures.append(future)
                                        active_futures.remove(future)

                        # If everything is stalled for too long, abort the batch
                        if (elapsed_time > self.stall_detection_timeout * 2 and
                                len(active_futures) == len(future_to_node) and
                                len(completed_futures) == 0):
                            logger.error(f"Entire batch appears stalled after {elapsed_time:.0f}s. Aborting batch.")
                            for future in list(active_futures):
                                future.cancel()
                            break

                    # Release session connections after each batch
                    self.session.close()
                    self.session = requests.Session()
                    adapter = requests.adapters.HTTPAdapter(
                        pool_connections=self.max_workers * 2,
                        pool_maxsize=self.max_workers * 4,
                        max_retries=self.max_retries,
                        pool_block=True
                    )
                    self.session.mount('http://', adapter)
                    self.session.mount('https://', adapter)

                    # Force garbage collection between batches
                    gc.collect()

                logger.info(
                    f"Completed batch {i // batch_size + 1} - {processed_count}/{total_nodes_to_process} nodes processed")

                # If the batch was completely unsuccessful, adjust parameters for next batch
                if processed_count == 0 and i > 0:
                    logger.warning("No successful nodes processed. Adjusting batch size and timeouts.")
                    batch_size = max(1, batch_size // 2)
                    self.downloader.download_timeout = max(30, self.downloader.download_timeout // 2)
                    self.downloader.connect_timeout = max(2, self.downloader.connect_timeout // 2)
                    logger.info(f"New batch size: {batch_size}, download timeout: {self.downloader.download_timeout}s")

            logger.info(f"Processing completed! Processed {processed_count} nodes.")
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down gracefully...")
        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}", exc_info=True)
        finally:
            self.activity_monitor.stop()
            self.session.close()

    def process_test_node(self, node_name: str) -> bool:
        """Process a single test node with detailed logging"""
        logger.info(f"TESTING NODE: Beginning detailed test of node {node_name}")
        start_time = time.time()

        try:
            # Test direct URL access
            node_url = urljoin(self.base_url, f"{node_name}/")
            logger.info(f"TESTING NODE: Accessing node directory {node_url}")

            try:
                response = self.session.get(node_url, timeout=(5, 10))
                logger.info(f"TESTING NODE: Response status: {response.status_code}")

                if response.status_code != 200:
                    logger.error(f"TESTING NODE: Failed to access node directory, status: {response.status_code}")
                    return False

                # Try to parse HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a')
                files = [link.text for link in links if not link.text.startswith('/')]
                logger.info(f"TESTING NODE: Found links: {files}")

            except Exception as e:
                logger.error(f"TESTING NODE: Error accessing node directory: {e}")
                return False

            # Test downloading one small file
            if 'cpu.csv' in files or True:  # Try anyway even if not found in directory listing
                file_url = urljoin(node_url, 'cpu.csv')
                logger.info(f"TESTING NODE: Testing download of {file_url}")

                try:
                    with self.session.get(file_url, stream=True, timeout=(5, 30)) as file_response:
                        if file_response.status_code != 200:
                            logger.error(f"TESTING NODE: File download failed, status: {file_response.status_code}")
                            return False

                        content_length = file_response.headers.get('content-length')
                        logger.info(f"TESTING NODE: Content length: {content_length}")

                        # Read first chunk
                        chunk = next(file_response.iter_content(chunk_size=4096), None)
                        if chunk:
                            logger.info(f"TESTING NODE: Successfully received first chunk ({len(chunk)} bytes)")
                        else:
                            logger.error("TESTING NODE: Failed to receive any data")
                            return False

                except Exception as e:
                    logger.error(f"TESTING NODE: Error downloading test file: {e}")
                    return False

            logger.info(f"TESTING NODE: Manual tests passed, trying actual node processing")
            success = self.process_and_upload_node(node_name)

            elapsed = time.time() - start_time
            if success:
                logger.info(f"TESTING NODE: Successfully processed test node in {elapsed:.1f}s")
            else:
                logger.error(f"TESTING NODE: Failed to process test node (took {elapsed:.1f}s)")

            return success

        except Exception as e:
            logger.error(f"TESTING NODE: Unexpected error in test node processing: {e}")
            return False

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down gracefully...")
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
                # Use timeout to prevent hanging
                response = self.session.get(self.base_url, timeout=(10, 30))
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
            except requests.exceptions.Timeout:
                retry_count += 1
                logging.warning(f"Timeout getting node list, retry {retry_count}/{self.max_retries}")
                time.sleep(2 ** retry_count)
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

            # Check retry count
            retry_count = self.state_manager.increment_retry_count(node_name)
            if retry_count > self.max_node_retries:
                logger.warning(f"Node {node_name} has been retried {retry_count} times, skipping")
                self.activity_monitor.remove_node(node_name)
                return False

            # 1. Check disk space
            if self.quota_manager.get_available_space_mb() < 500:  # Minimum 500MB required
                logger.error(f"Insufficient disk space to process node {node_name}")
                self.activity_monitor.remove_node(node_name)
                return False

            # 2. Download
            logger.info(f"Downloading node {node_name}")
            self.activity_monitor.update_activity(node_name, "downloading", 0)

            # Set a timeout for the entire download phase
            download_start = time.time()
            download_timeout = 600  # 10 minutes max for downloads

            download_success = self.downloader.download_node_files(node_name)

            # Check if download timed out
            if time.time() - download_start > download_timeout:
                logger.error(f"Download phase for node {node_name} exceeded timeout of {download_timeout}s")
                self.state_manager.update_node_status(node_name, download_complete=False)
                node_dir = self.base_dir / node_name
                if node_dir.exists():
                    shutil.rmtree(node_dir, ignore_errors=True)
                self.activity_monitor.remove_node(node_name)
                return False

            if not download_success:
                logger.error(f"Failed to download files for node {node_name}")
                self.state_manager.update_node_status(node_name, download_complete=False)
                self.activity_monitor.remove_node(node_name)
                return False

            self.state_manager.update_node_status(node_name, download_complete=True)

            # 3. Process
            logger.info(f"Processing node {node_name}")
            self.activity_monitor.update_activity(node_name, "processing", 50)

            node_dir = self.base_dir / node_name
            self.state_manager.update_node_status(node_name, processing_started=True)

            # Add file size logging for debugging
            try:
                for file_path in node_dir.glob('*.csv'):
                    file_size_mb = file_path.stat().st_size / (1024 * 1024)
                    logger.info(f"Processing file {file_path.name} for node {node_name} (size: {file_size_mb:.2f} MB)")
            except Exception as e:
                logger.warning(f"Error logging file sizes for {node_name}: {e}")

            # Add timeout for processing phase
            processing_start = time.time()
            processing_timeout = 300  # 5 minutes max for processing

            node_df = self.processor.process_node_data(node_dir)

            # Check if processing timed out
            if time.time() - processing_start > processing_timeout:
                logger.error(f"Processing phase for node {node_name} exceeded timeout of {processing_timeout}s")
                self.state_manager.update_node_status(node_name, processing_complete=False)
                shutil.rmtree(node_dir, ignore_errors=True)
                self.activity_monitor.remove_node(node_name)
                return False

            # Log completion of processing
            self.activity_monitor.update_activity(node_name, "processed_files", 70)

            if node_df.is_empty():
                logger.warning(f"No data processed for node {node_name}")
                self.state_manager.update_node_status(node_name, processing_complete=False)
                shutil.rmtree(node_dir, ignore_errors=True)
                self.activity_monitor.remove_node(node_name)
                return False

            # 4. Save to monthly files
            logger.info(f"Saving node {node_name} data to monthly files")
            self.activity_monitor.update_activity(node_name, "saving", 80)

            saved_files = self.monthly_data_manager.save_node_data(node_name, node_df)
            if not saved_files:
                logger.warning(f"No files saved for node {node_name}")
                self.state_manager.update_node_status(node_name, processing_complete=False)
                shutil.rmtree(node_dir, ignore_errors=True)
                self.activity_monitor.remove_node(node_name)
                return False

            # 5. Upload to S3
            logger.info(f"Uploading node {node_name} data to S3")
            self.activity_monitor.update_activity(node_name, "uploading", 90)

            # Add timeout for upload phase
            upload_start = time.time()
            upload_timeout = 300  # 5 minutes max for uploads

            upload_success = self.s3_uploader.upload_files(saved_files)

            # Check if upload timed out
            if time.time() - upload_start > upload_timeout:
                logger.error(f"Upload phase for node {node_name} exceeded timeout of {upload_timeout}s")
                self.state_manager.update_node_status(node_name, uploaded=False)
                # Don't delete the files so they can be uploaded later
                shutil.rmtree(node_dir, ignore_errors=True)
                self.activity_monitor.remove_node(node_name)
                return False

            if upload_success:
                self.version_manager.increment_version()
                self.state_manager.update_node_status(
                    node_name,
                    processing_complete=True,
                    uploaded=True
                )
                # Clean up
                for file_path in saved_files:
                    try:
                        os.remove(file_path)
                        logger.info(f"Removed file after upload: {file_path}")
                    except Exception as e:
                        logger.warning(f"Error removing file {file_path}: {e}")

                # Clean up node directory
                try:
                    shutil.rmtree(node_dir, ignore_errors=True)
                    logger.info(f"Successfully processed and uploaded node {node_name}")
                except Exception as e:
                    logger.warning(f"Error removing node directory {node_dir}: {e}")

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
    download_timeout = int(os.getenv('ETL_DOWNLOAD_TIMEOUT', '300'))
    max_node_retries = int(os.getenv('ETL_MAX_NODE_RETRIES', '2'))
    stall_detection_timeout = int(os.getenv('ETL_STALL_DETECTION_TIMEOUT', '600'))
    socket_timeout = int(os.getenv('ETL_SOCKET_TIMEOUT', '30'))

    # Print configuration
    logger.info(f"ETL Configuration:")
    logger.info(f"  - Base URL: {base_url}")
    logger.info(f"  - Base Directory: {base_dir}")
    logger.info(f"  - Quota: {quota_mb} MB")
    logger.info(f"  - S3 Bucket: {bucket_name}")
    logger.info(f"  - Max Workers: {max_workers}")
    logger.info(f"  - Activity Monitor Interval: {activity_monitor_interval} seconds")
    logger.info(f"  - Download Timeout: {download_timeout} seconds")
    logger.info(f"  - Max Node Retries: {max_node_retries}")
    logger.info(f"  - Stall Detection Timeout: {stall_detection_timeout} seconds")
    logger.info(f"  - Socket Timeout: {socket_timeout} seconds")

    # Reduce max_workers if it's extremely high
    if max_workers > 40:
        original_workers = max_workers
        max_workers = 10
        logger.info(f"Reduced max_workers from {original_workers} to {max_workers} to prevent connection issues")

    pipeline = ETLPipeline(
        base_url=base_url,
        base_dir=base_dir,
        quota_mb=quota_mb,
        bucket_name=bucket_name,
        max_workers=max_workers,
        activity_monitor_interval=activity_monitor_interval,
        download_timeout=download_timeout,
        max_node_retries=max_node_retries,
        stall_detection_timeout=stall_detection_timeout,
        socket_timeout=socket_timeout
    )

    pipeline.run()


if __name__ == '__main__':
    main()