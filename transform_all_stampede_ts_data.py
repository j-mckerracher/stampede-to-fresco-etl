"""
Optimized ETL Script for Node Data Processing

This script downloads node data, processes it into monthly files,
and uploads the results to AWS S3. Features include:
1. Sequential node processing with parallel internal operations
2. Disk quota management to stay under 24GB
3. File versioning
4. Efficient S3 operations with local caching
5. Full resumability with node tracking
6. Optimized performance
"""
import concurrent
import os
import sys
import time
import shutil
import logging
import threading
import multiprocessing
from typing import List, Dict, Set
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import traceback
import json
import boto3
import requests
from bs4 import BeautifulSoup
import polars as pl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('node_etl.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration (replace command line arguments)
CONFIG = {
    'base_url': 'https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/',  # Base URL for node data
    'temp_dir': './temp',                     # Temporary directory for downloads
    'monthly_dir': './monthly_files',         # Directory for monthly files
    'cache_dir': './cache',                   # Cache directory for monthly files
    's3_bucket': 'data-transform-stampede',   # S3 bucket for uploads
    'quota_mb': 20000,                        # Disk quota in MB (20GB)
    'download_workers': 4,                    # Max parallel downloads
    'process_workers': None,                  # Max parallel processing workers (None = auto)
    'download_timeout': 300,                  # Download timeout in seconds
    'connect_timeout': 10,                    # Connection timeout in seconds
    's3_timeout': 300,                        # S3 upload timeout in seconds
    'upload_batch_size': 50,                  # Number of nodes to process before uploading to S3
    'resume_node': None,                      # Node to resume from (None = auto from state file)
    'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),  # AWS credentials
    'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY')
}


class DownloadProgressTracker:
    """Track download progress and detect stalled downloads"""

    def __init__(self, url: str, total_size: int, stall_timeout: int = 300):
        self.url = url
        self.total_size = total_size
        self.bytes_received = 0
        self.last_received_time = time.time()
        self.stall_timeout = stall_timeout
        self.is_stalled = False

    def update_progress(self, bytes_received: int):
        """Update progress with newly received bytes"""
        self.bytes_received += bytes_received
        self.last_received_time = time.time()
        self.is_stalled = False

    def check_stalled(self) -> bool:
        """Check if download has stalled (no progress for timeout period)"""
        if time.time() - self.last_received_time > self.stall_timeout:
            self.is_stalled = True
            return True
        return False


class DiskQuotaManager:
    """Manage disk quota to prevent exceeding limits and enable recovery"""

    def __init__(self, max_quota_mb: int, temp_dir: Path, monthly_dir: Path, cache_dir: Path):
        self.max_quota_mb = max_quota_mb
        self.temp_dir = temp_dir
        self.monthly_dir = monthly_dir
        self.cache_dir = cache_dir
        self.allocated_mb = 0
        self.lock = threading.Lock()
        logger.info(f"DiskQuotaManager initialized with max quota: {max_quota_mb} MB")

    def get_current_disk_usage(self) -> int:
        """Get current disk usage across all managed directories in MB"""
        try:
            total_size = 0

            # Check temp directory
            if self.temp_dir.exists():
                for path in self.temp_dir.glob('**/*'):
                    if path.is_file():
                        total_size += path.stat().st_size

            # Check monthly directory
            if self.monthly_dir.exists():
                for path in self.monthly_dir.glob('**/*'):
                    if path.is_file():
                        total_size += path.stat().st_size

            # Check cache directory
            if self.cache_dir.exists():
                for path in self.cache_dir.glob('**/*'):
                    if path.is_file():
                        total_size += path.stat().st_size

            return total_size // (1024 * 1024)
        except Exception as e:
            logger.error(f"Error calculating disk usage: {e}")
            # Return a conservative estimate
            return self.allocated_mb

    def is_quota_exceeded(self) -> bool:
        """Check if current usage exceeds or is close to quota limit"""
        current_usage = self.get_current_disk_usage()
        # Consider quota exceeded if at 95% or above
        return current_usage >= (self.max_quota_mb * 0.95)

    def request_space(self, mb_needed: int) -> bool:
        """Request space allocation, returns True if space is available"""
        with self.lock:
            current_usage = self.get_current_disk_usage()
            if current_usage + mb_needed <= self.max_quota_mb:
                self.allocated_mb = current_usage + mb_needed
                logger.debug(f"Space allocated: {mb_needed} MB, Total: {self.allocated_mb}/{self.max_quota_mb} MB")
                return True
            else:
                logger.warning(f"Cannot allocate {mb_needed} MB, would exceed quota. "
                               f"Current: {current_usage}/{self.max_quota_mb} MB")
                return False

    def release_space(self, mb_to_release: int):
        """Release allocated space"""
        with self.lock:
            self.allocated_mb = max(0, self.allocated_mb - mb_to_release)
            logger.debug(f"Space released: {mb_to_release} MB, Remaining allocation: {self.allocated_mb} MB")

    def recalculate_usage(self):
        """Recalculate actual disk usage"""
        with self.lock:
            actual_usage = self.get_current_disk_usage()
            logger.info(f"Disk usage recalculated: {actual_usage} MB (was tracking {self.allocated_mb} MB)")
            self.allocated_mb = actual_usage


class MonthlyFileManager:
    """Manage monthly data files with improved tracking, caching, and versioning"""

    def __init__(self, monthly_dir: Path, cache_dir: Path, state_file: Path):
        self.monthly_dir = monthly_dir
        self.cache_dir = cache_dir
        self.state_file = state_file
        self.current_node = None
        self.processed_nodes = set()  # Track processed nodes
        self.modified_files = set()  # Track modified files that need to be uploaded
        self.monthly_files = {}  # Format: {'YYYY-MM': 'file_path'}
        self.version_counter = 0  # File version counter
        self.lock = threading.Lock()

        # Create dirs if they don't exist
        self.monthly_dir.mkdir(exist_ok=True, parents=True)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

        # Load state if it exists
        self._load_state()
        logger.info(
            f"MonthlyFileManager initialized with dir: {monthly_dir}, cache: {cache_dir}, version: {self.version_counter}")
        logger.info(f"Loaded {len(self.processed_nodes)} previously processed nodes")

    def _load_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)

                if 'current_node' in state:
                    self.current_node = state['current_node']
                    logger.info(f"Loaded state: current node: {self.current_node}")

                # Load processed nodes
                if 'processed_nodes' in state:
                    self.processed_nodes = set(state['processed_nodes'])
                    logger.info(f"Loaded {len(self.processed_nodes)} processed nodes from state")

                # Load modified files
                if 'modified_files' in state:
                    self.modified_files = set(state['modified_files'])
                    logger.info(f"Loaded {len(self.modified_files)} modified files from state")

                # Load version counter
                if 'version_counter' in state:
                    self.version_counter = state['version_counter']
                    logger.info(f"Loaded version counter: {self.version_counter}")
            except Exception as e:
                logger.error(f"Error loading state file: {e}")
                self.current_node = None
                self.processed_nodes = set()
                self.modified_files = set()
                self.version_counter = 0
        else:
            logger.info("No state file found, starting fresh")

    def _save_state(self):
        """Save processing state to file"""
        try:
            state = {
                'current_node': self.current_node,
                'processed_nodes': list(self.processed_nodes),
                'modified_files': list(self.modified_files),
                'version_counter': self.version_counter
            }

            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)

            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state file: {e}")

    def increment_version(self):
        """Increment the version counter and update state"""
        with self.lock:
            self.version_counter += 1
            self._save_state()
            logger.info(f"Incremented version counter to {self.version_counter}")
        return self.version_counter

    def release_current_node(self):
        """Release the current node without marking it as processed"""
        with self.lock:
            previous_node = self.current_node
            if previous_node:
                self.current_node = None
                self._save_state()
                logger.info(f"Released current node {previous_node} without marking as processed")
                return previous_node
            return None

    def get_monthly_file_path(self, timestamp: datetime) -> Path:
        """Get the file path for a timestamp with versioning"""
        month_key = timestamp.strftime('%Y-%m')

        # Include version in filename if version_counter > 0
        if self.version_counter > 0:
            return self.cache_dir / f"node_data_v{self.version_counter}_{month_key}.csv"
        else:
            return self.cache_dir / f"node_data_{month_key}.csv"

    def get_all_cached_files(self) -> List[Path]:
        """Get all files in the cache directory"""
        return list(self.cache_dir.glob('*.csv'))


class NodeDiscoverer:
    """Discover available nodes from base URL"""

    def __init__(self, base_url: str, session: requests.Session, connect_timeout: int = 10):
        self.base_url = base_url
        self.session = session
        self.connect_timeout = connect_timeout
        logger.info(f"NodeDiscoverer initialized with base URL: {base_url}")

    def discover_nodes(self) -> List[str]:
        """Discover available nodes"""
        logger.info(f"Discovering nodes from: {self.base_url}")

        try:
            response = self.session.get(self.base_url, timeout=self.connect_timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a')

            node_names = []
            for link in links:
                href = link.get('href', '')
                # Look for links that match NODE pattern with trailing slash
                if href.startswith('NODE') and href.endswith('/'):
                    node_name = href.rstrip('/')
                    node_names.append(node_name)

            logger.info(f"Discovered {len(node_names)} nodes")
            return node_names

        except Exception as e:
            logger.error(f"Error discovering nodes: {e}")
            return []


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
            return None

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
            return None

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
            return None

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
            return None

    def process_node_data(self, node_dir: Path) -> pl.DataFrame:
        """Process all files for a node and return combined dataframe"""
        logger.info(f"Processing all files for node in directory: {node_dir}")

        dfs = []
        try:
            # Process block file
            block_path = node_dir / 'block.csv'
            if block_path.exists() and block_path.stat().st_size > 0:
                block_df = self.process_block_file(block_path)
                if isinstance(block_df, pl.DataFrame) and len(block_df) > 0:
                    dfs.append(block_df)
                # Delete file after processing
                os.remove(block_path)

            # Process CPU file
            cpu_path = node_dir / 'cpu.csv'
            if cpu_path.exists() and cpu_path.stat().st_size > 0:
                cpu_df = self.process_cpu_file(cpu_path)
                if isinstance(cpu_df, pl.DataFrame) and len(cpu_df) > 0:
                    dfs.append(cpu_df)
                # Delete file after processing
                os.remove(cpu_path)

            # Process NFS file
            nfs_path = node_dir / 'nfs.csv'
            if nfs_path.exists() and nfs_path.stat().st_size > 0:
                nfs_df = self.process_nfs_file(nfs_path)
                if isinstance(nfs_df, pl.DataFrame) and len(nfs_df) > 0:
                    dfs.append(nfs_df)
                # Delete file after processing
                os.remove(nfs_path)

            # Process memory file
            mem_path = node_dir / 'mem.csv'
            if mem_path.exists() and mem_path.stat().st_size > 0:
                memory_dfs = self.process_memory_metrics(mem_path)
                if memory_dfs is not None and len(memory_dfs) > 0:
                    dfs.extend(memory_dfs)
                # Delete file after processing
                os.remove(mem_path)

            # Combine all dataframes
            if dfs:
                # Use thread pool to process dataframes in parallel
                with ThreadPoolExecutor(max_workers=max(1, multiprocessing.cpu_count() - 1)) as executor:
                    # No actual parallel work here, but setting up the structure
                    # for potential future parallelization of data transformations
                    futures = [executor.submit(lambda df=df: df) for df in dfs]
                    processed_dfs = [future.result() for future in futures]

                result = pl.concat(processed_dfs)
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
            download_timeout: int = 300,
            connect_timeout: int = 10
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
        """Download node data files with enhanced disk quota handling"""
        node_dir = self.save_dir / node_name
        node_dir.mkdir(exist_ok=True)
        logger.info(f"Starting download for node: {node_name}")

        node_url = urljoin(self.base_url, f"{node_name}/")
        required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']

        try:
            # Check current disk usage before proceeding
            current_usage = self.quota_manager.get_current_disk_usage()
            max_quota = self.quota_manager.max_quota_mb
            available_space = max_quota - current_usage
            logger.info(f"Current disk usage: {current_usage}/{max_quota} MB, Available: {available_space} MB")

            # Abort if we're already at 95% capacity
            if available_space < (max_quota * 0.05):
                logger.error(f"Insufficient disk space available ({available_space} MB) to download node {node_name}")
                if node_dir.exists():
                    shutil.rmtree(node_dir, ignore_errors=True)
                return False

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

            # First check if files actually exist and get their sizes
            file_exists = {}
            file_sizes = {}
            total_size_mb = 0

            for file_name in required_files:
                file_url = urljoin(node_url, file_name)
                try:
                    # Do a HEAD request to check if file exists and get its size
                    head_response = self.session.head(file_url, timeout=(5, 10))
                    file_exists[file_name] = head_response.status_code == 200

                    if file_exists[file_name]:
                        size_bytes = int(head_response.headers.get('content-length', 0))
                        size_mb = max(1, size_bytes // (1024 * 1024))
                        file_sizes[file_name] = size_mb
                        total_size_mb += size_mb
                        logger.debug(f"File check: {file_url} - Size: {size_mb} MB")
                    else:
                        logger.debug(f"File check: {file_url} - Status: {head_response.status_code} (not available)")
                except Exception as e:
                    logger.warning(f"Error checking if file exists {file_url}: {e}")
                    file_exists[file_name] = False

            # Check if we have enough space for all files
            if total_size_mb > 0:
                logger.info(f"Total size of all files for {node_name}: {total_size_mb} MB")
                if total_size_mb > available_space:
                    logger.error(f"Insufficient space to download all files for {node_name} "
                                 f"(need {total_size_mb} MB, have {available_space} MB)")
                    if node_dir.exists():
                        shutil.rmtree(node_dir, ignore_errors=True)
                    return False

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
            disk_quota_exceeded = False

            # Create a ThreadPoolExecutor with a specific max_workers value
            with ThreadPoolExecutor(max_workers=4) as executor:
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
                        # Check for disk quota exceeded errors
                        if isinstance(e, OSError) and e.errno == 122:
                            logger.error(f"Disk quota exceeded during download for node {node_name}")
                            disk_quota_exceeded = True
                            break

                success = all(success_flags) and len(success_flags) > 0
                logger.info(f"Download summary for {node_name}: {sum(success_flags)}/{len(success_flags)} successful")

                if not success:
                    shutil.rmtree(node_dir, ignore_errors=True)
                    logger.warning(f"Failed to download all files for node {node_name}")

                    # If disk quota was exceeded, propagate this information
                    if disk_quota_exceeded:
                        logger.error(f"Download failed due to disk quota exceeded for node {node_name}")
                        # Clean up any partially downloaded files
                        for file_name in required_files:
                            file_path = node_dir / file_name
                            if file_path.exists():
                                try:
                                    os.remove(file_path)
                                    logger.debug(f"Removed partial file {file_path}")
                                except Exception as e:
                                    logger.error(f"Error removing partial file {file_path}: {e}")
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
        """Download a single file with enhanced disk quota handling"""
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

            try:
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
            except OSError as e:
                if e.errno == 122:  # Disk quota exceeded
                    logger.error(f"Disk quota exceeded while writing file for {url}")
                    # Propagate the error up so the calling function knows about the disk quota issue
                    raise
                else:
                    logger.error(f"OS error writing file for {url}: {e}")
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
        except OSError as e:
            if e.errno == 122:  # Disk quota exceeded
                logger.error(f"Disk quota exceeded while downloading {url}")
                # Release the space allocation we requested
                if file_size > 0:
                    space_to_release = max(1, file_size // (1024 * 1024))
                    self.quota_manager.release_space(space_to_release)
                    logger.info(f"Released {space_to_release} MB after disk quota error")
                # Propagate the error up so the calling function knows about the disk quota issue
                raise
            else:
                logger.error(f"OS error downloading {url}: {e}")
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


class S3Manager:
    """Handle S3 uploads and downloads with retries"""

    def __init__(self, bucket_name: str, max_retries: int = 3, timeout: int = 300,
                 aws_access_key_id: str = None, aws_secret_access_key: str = None):
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        self.timeout = timeout
        self.upload_errors = {}  # Track upload errors for reporting
        self.upload_results = {}  # Track thread results for each file

        # Check environment variables for credentials if not provided
        if aws_access_key_id is None:
            aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
            if aws_access_key_id:
                logger.debug("Found AWS_ACCESS_KEY_ID in environment variables")
            else:
                logger.warning("AWS_ACCESS_KEY_ID not found in environment variables")

        if aws_secret_access_key is None:
            aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
            if aws_secret_access_key:
                logger.debug("Found AWS_SECRET_ACCESS_KEY in environment variables")
            else:
                logger.warning("AWS_SECRET_ACCESS_KEY not found in environment variables")

        # Check if we have credentials
        have_credentials = aws_access_key_id is not None and aws_secret_access_key is not None

        # Configure S3 client with timeouts and credentials
        try:
            from botocore.config import Config
            config = Config(
                connect_timeout=30,
                read_timeout=timeout,
                retries={'max_attempts': max_retries}
            )

            # Initialize with or without explicit credentials
            if have_credentials:
                self.s3_client = boto3.client(
                    's3',
                    config=config,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )
                logger.info("Initialized S3 client with explicit credentials")
            else:
                self.s3_client = boto3.client('s3', config=config)
                logger.info("Initialized S3 client with environment credentials")

        except (ImportError, AttributeError) as e:
            logger.warning(f"Error configuring S3 client: {e}")

            # Fallback if Config import fails
            if have_credentials:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )
            else:
                self.s3_client = boto3.client('s3')

            logger.warning("Could not configure S3 client with timeouts, using default configuration")

        # Test connection
        try:
            # Just call a simple API to test credentials
            self.s3_client.list_buckets()
            logger.info("Successfully authenticated with AWS")
        except Exception as e:
            logger.error(f"Failed to authenticate with AWS: {e}")

        logger.info(f"S3Manager initialized with bucket: {bucket_name}, timeout: {timeout}s")

    def list_files(self, prefix: str = None) -> List[str]:
        """List files in the S3 bucket, optionally with a prefix"""
        try:
            if prefix:
                response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            else:
                response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)

            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []

        except Exception as e:
            logger.error(f"Error listing files in S3 bucket {self.bucket_name}: {e}")
            return []

    def download_file(self, s3_key: str, local_path: Path) -> bool:
        """Download a file from S3 with retry logic"""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                logger.info(f"Downloading {s3_key} from S3 bucket {self.bucket_name} to {local_path}")

                # Ensure directory exists
                local_path.parent.mkdir(parents=True, exist_ok=True)

                self.s3_client.download_file(
                    self.bucket_name,
                    s3_key,
                    str(local_path)
                )

                logger.info(f"Successfully downloaded {s3_key} to {local_path}")
                return True

            except Exception as e:
                retry_count += 1
                if retry_count == self.max_retries:
                    logger.error(f"Failed to download {s3_key} after {self.max_retries} attempts: {e}")
                    return False
                else:
                    logger.warning(f"Retry {retry_count}/{self.max_retries} downloading {s3_key}: {e}")
                    time.sleep(min(30, 2 ** retry_count))

        return False

    def upload_files(self, file_paths: List[Path]) -> bool:
        """Upload multiple files to S3 with retry logic"""
        success = True
        self.upload_results = {}  # Clear previous results
        self.upload_errors = {}  # Clear previous errors

        for file_path in file_paths:
            str_path = str(file_path)  # Use string path as dict key
            retry_count = 0
            file_success = False

            while retry_count < self.max_retries and not file_success:
                try:
                    logger.info(f"Uploading {file_path} to S3 bucket {self.bucket_name}")

                    # Create an event to signal when the thread completes
                    self.upload_results[str_path] = {
                        'completed': threading.Event(),
                        'success': False,
                        'error': None
                    }

                    # Use a thread with timeout to monitor the upload
                    upload_thread = threading.Thread(
                        target=self._upload_file_with_timeout,
                        args=(file_path, str_path)
                    )
                    upload_thread.daemon = True  # Mark as daemon to not block program exit
                    upload_thread.start()

                    # Wait for thread to complete with timeout
                    completed = self.upload_results[str_path]['completed'].wait(timeout=self.timeout)

                    if not completed or upload_thread.is_alive():
                        # Upload is taking too long, consider it failed
                        logger.error(f"Upload of {file_path} timed out after {self.timeout}s")
                        # Store the error
                        self.upload_errors[str_path] = f"Upload timed out after {self.timeout}s"
                        retry_count += 1
                        # Wait before retrying
                        time.sleep(min(30, 2 ** retry_count))
                        continue

                    # Check if the thread reported success
                    if self.upload_results[str_path]['success']:
                        logger.info(f"Successfully uploaded {file_path}")
                        file_success = True
                        break
                    else:
                        error = self.upload_results[str_path].get('error', 'Unknown error')
                        logger.error(f"Upload thread for {file_path} reported failure: {error}")
                        self.upload_errors[str_path] = error
                        retry_count += 1
                        time.sleep(min(30, 2 ** retry_count))

                except Exception as e:
                    logger.error(f"Exception in upload control for {file_path}: {e}")
                    self.upload_errors[str_path] = str(e)
                    retry_count += 1
                    time.sleep(min(30, 2 ** retry_count))

            # After all retries, check if this file was successful
            if not file_success:
                logger.error(f"Failed to upload {file_path} after {self.max_retries} attempts")
                success = False

        # Summarize results
        failed_files = [path for path, result in self.upload_results.items()
                        if not result.get('success', False)]

        if failed_files:
            logger.error(f"Failed to upload {len(failed_files)} files: {failed_files}")
            success = False

        return success

    def _upload_file_with_timeout(self, file_path: Path, path_key: str):
        """Upload a file to S3, used within a thread with timeout monitoring"""
        try:
            # Check if file exists and is readable
            if not file_path.exists():
                error_msg = f"File {file_path} does not exist"
                logger.error(error_msg)
                self.upload_results[path_key]['error'] = error_msg
                self.upload_results[path_key]['success'] = False
                self.upload_results[path_key]['completed'].set()
                return

            # Do the actual upload
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                file_path.name,
                ExtraArgs={'ContentType': 'text/csv'}
            )

            # Record success
            self.upload_results[path_key]['success'] = True
            self.upload_results[path_key]['completed'].set()

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error in threaded upload of {file_path}: {error_msg}")

            # Record the error
            self.upload_results[path_key]['error'] = error_msg
            self.upload_results[path_key]['success'] = False
            self.upload_results[path_key]['completed'].set()


class NodeETL:
    """Main ETL class to orchestrate node data processing with optimized S3 operations"""

    def __init__(
            self,
            base_url: str,
            temp_dir: Path,
            monthly_dir: Path,
            cache_dir: Path,  # New cache directory
            s3_bucket: str,
            max_quota_mb: int = 20000,  # 20GB default
            max_download_workers: int = 3,
            max_process_workers: int = None,
            download_timeout: int = 300,
            connect_timeout: int = 10,
            s3_timeout: int = 300,
            upload_batch_size: int = 50,  # New batch size parameter
            resume_node: str = None,  # New resume node parameter
            aws_access_key_id: str = None,
            aws_secret_access_key: str = None
    ):
        self.base_url = base_url
        self.temp_dir = temp_dir
        self.monthly_dir = monthly_dir
        self.cache_dir = cache_dir
        self.s3_bucket = s3_bucket
        self.upload_batch_size = upload_batch_size
        self.resume_node = resume_node

        # Create directories
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        self.monthly_dir.mkdir(exist_ok=True, parents=True)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

        # State file location
        self.state_file = self.monthly_dir / 'etl_state.json'

        # Initialize managers
        self.quota_manager = DiskQuotaManager(max_quota_mb, temp_dir, monthly_dir, cache_dir)
        self.monthly_file_manager = MonthlyFileManager(monthly_dir, cache_dir, self.state_file)

        # Worker settings
        self.max_download_workers = max_download_workers
        self.max_process_workers = max_process_workers or max(1, multiprocessing.cpu_count() - 1)

        # Timeout settings
        self.download_timeout = download_timeout
        self.connect_timeout = connect_timeout
        self.s3_timeout = s3_timeout

        # Create session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NodeETL/1.0',
        })

        # Initialize components
        self.node_discoverer = NodeDiscoverer(base_url, self.session, connect_timeout)
        self.node_downloader = NodeDownloader(
            base_url,
            temp_dir,
            self.quota_manager,
            self.session,
            max_workers=max_download_workers,
            download_timeout=download_timeout,
            connect_timeout=connect_timeout
        )
        self.node_processor = NodeDataProcessor()
        self.s3_manager = S3Manager(
            s3_bucket,
            max_retries=3,
            timeout=s3_timeout,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        logger.info(f"NodeETL initialized. Temp dir: {temp_dir}, Monthly dir: {monthly_dir}, "
                    f"Cache dir: {cache_dir}, S3 bucket: {s3_bucket}, Max quota: {max_quota_mb} MB, "
                    f"Upload batch size: {upload_batch_size}")

    def recover_from_disk_quota_exceeded(self) -> bool:
        """
        Handle disk quota exceeded scenario:
        1. Upload all cached files to S3
        2. Delete cached files after successful upload
        3. Increment version counter
        4. Return success status
        """
        logger.info("Attempting to recover from disk quota exceeded")

        # Get all files in cache
        cached_files = self.monthly_file_manager.get_all_cached_files()

        if not cached_files:
            logger.warning("No cached files found for recovery")
            return False

        logger.info(f"Found {len(cached_files)} cached files to upload")

        # Upload all files to S3
        success = self.s3_manager.upload_files(cached_files)

        if success:
            logger.info("Successfully uploaded all cached files to S3")

            # Delete all files from cache
            files_deleted = 0
            for file_path in cached_files:
                try:
                    os.remove(file_path)
                    files_deleted += 1
                except Exception as e:
                    logger.error(f"Error deleting cached file {file_path}: {e}")

            logger.info(f"Deleted {files_deleted}/{len(cached_files)} cached files")

            # Increment version counter
            self.monthly_file_manager.increment_version()

            # Recalculate disk usage
            self.quota_manager.recalculate_usage()

            # Clear the modified files list since we've uploaded everything
            self.monthly_file_manager.clear_modified_files()

            return True
        else:
            logger.error("Failed to upload cached files during recovery")
            return False

    def process_node_data_to_monthly(self, node_data: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """Split node data into monthly DataFrames"""
        if len(node_data) == 0:
            logger.warning("Empty node data provided for monthly processing")
            return {}

        # Group by month
        try:
            node_data = node_data.with_columns([
                pl.col('Timestamp').dt.strftime('%Y-%m').alias('month')
            ])

            monthly_groups = {}
            for month in node_data['month'].unique():
                month_df = node_data.filter(pl.col('month') == month)
                monthly_groups[month] = month_df.drop('month')
                logger.info(f"Split {len(month_df)} rows for month {month}")

            return monthly_groups
        except Exception as e:
            logger.error(f"Error splitting data into monthly groups: {e}")
            return {}

    def download_all_monthly_files(self) -> Dict[str, Path]:
        """Download all monthly files from S3 at the beginning of the ETL process"""
        logger.info("Initializing local cache by downloading all monthly files from S3")

        # List all files in the bucket
        s3_files = self.s3_manager.list_files()

        # Filter for node_data CSV files
        monthly_files = [f for f in s3_files if f.startswith('node_data_') and f.endswith('.csv')]

        if not monthly_files:
            logger.info("No existing monthly files found in S3 bucket")
            return {}

        logger.info(f"Found {len(monthly_files)} monthly files in S3 bucket")

        # Map of month to downloaded file path
        downloaded_files = {}

        for s3_key in monthly_files:
            try:
                # Extract month from filename (format: node_data_YYYY-MM.csv)
                month = s3_key.replace('node_data_', '').replace('.csv', '')

                # Download to cache directory
                local_path = self.cache_dir / s3_key

                if self.s3_manager.download_file(s3_key, local_path):
                    downloaded_files[month] = local_path
                    logger.info(f"Downloaded {s3_key} from S3 to local cache")
                else:
                    logger.error(f"Failed to download {s3_key} from S3")
            except Exception as e:
                logger.error(f"Error downloading {s3_key}: {e}")

        logger.info(f"Successfully downloaded {len(downloaded_files)} monthly files to local cache")
        return downloaded_files

    def handle_monthly_data(self, monthly_data: Dict[str, pl.DataFrame]) -> Dict[str, Path]:
        """Process monthly data - append to cached files (no immediate upload)"""
        if not monthly_data:
            logger.warning("No monthly data to handle")
            return {}

        # Get mapping of month to file path for all months
        month_file_map = self.monthly_file_manager.get_monthly_files_for_data(monthly_data)

        # Process each month
        processed_files = {}

        for month, df in monthly_data.items():
            try:
                # Convert Timestamp to string format for consistent storage
                if 'Timestamp' in df.columns:
                    df = df.with_columns([
                        pl.col('Timestamp').dt.strftime('%Y-%m-%d %H:%M:%S').alias('Timestamp')
                    ])

                # Get the file path for this month
                file_path = month_file_map[month]

                # Check if we have a cached file to append to
                if file_path.exists() and file_path.stat().st_size > 0:
                    # Read existing file
                    existing_df = pl.read_csv(file_path)

                    # Append new data
                    combined_df = pl.concat([existing_df, df])

                    # Write back to file
                    combined_df.write_csv(file_path)
                    logger.info(f"Appended {len(df)} rows to existing cached file for month {month}")
                else:
                    # Create new file
                    df.write_csv(file_path)
                    logger.info(f"Created new monthly file for month {month}")

                # Mark file as modified
                self.monthly_file_manager.mark_file_modified(file_path)

                # Add to list of processed files
                processed_files[month] = file_path

            except Exception as e:
                logger.error(f"Error processing monthly data for {month}: {e}")

        return processed_files

    def upload_modified_files(self) -> bool:
        """Upload all modified files to S3"""
        modified_files = self.monthly_file_manager.get_modified_files()

        if not modified_files:
            logger.info("No modified files to upload")
            return True

        logger.info(f"Uploading {len(modified_files)} modified files to S3")

        # Upload files
        success = self.s3_manager.upload_files(modified_files)

        if success:
            # Clear the modified files list
            self.monthly_file_manager.clear_modified_files()
            logger.info(f"Successfully uploaded {len(modified_files)} files to S3")
        else:
            logger.error(f"Failed to upload some modified files to S3")

        return success

    def process_node(self, node_name: str) -> bool:
        """Process a single node end-to-end (with disk quota recovery)"""
        logger.info(f"Starting end-to-end processing for node: {node_name}")

        # Check if we're already processing this node (resuming after failure)
        resuming = False
        if self.monthly_file_manager.current_node == node_name:
            logger.info(f"Resuming processing for node {node_name}")
            resuming = True

        # Check if this node has already been processed
        if self.monthly_file_manager.is_node_processed(node_name):
            logger.info(f"Node {node_name} has already been processed, skipping")
            return True

        # Start processing this node
        if not resuming:
            if not self.monthly_file_manager.start_node_processing(node_name):
                logger.warning(f"Cannot start processing {node_name}, already processing another node")
                return False

        # Create node directory in temp
        node_dir = self.temp_dir / node_name
        node_dir.mkdir(exist_ok=True)

        success = False
        processed_files = {}

        # Check if we have enough disk space before proceeding
        if self.quota_manager.is_quota_exceeded():
            logger.warning(f"Disk quota near exceeded before processing node {node_name}, triggering recovery")
            if not self.recover_from_disk_quota_exceeded():
                logger.error(f"Failed to recover disk space, cannot process node {node_name}")
                return False

        try:
            # Download node data
            download_success = self.node_downloader.download_node_files(node_name)

            # If download failed due to disk quota, try recovery and retry
            if not download_success:
                if self.quota_manager.is_quota_exceeded():
                    logger.warning(f"Download failed due to disk quota, attempting recovery")
                    if self.recover_from_disk_quota_exceeded():
                        logger.info(f"Recovery successful, retrying download for node {node_name}")
                        # Retry download after recovery
                        download_success = self.node_downloader.download_node_files(node_name)

            if not download_success:
                logger.error(f"Failed to download data for node {node_name}")
                self.monthly_file_manager.release_current_node()  # Release without marking as processed
                return False

            # Process node data
            node_df = self.node_processor.process_node_data(node_dir)

            # Check if we got any data
            if node_df is None or node_df.shape[0] == 0:
                logger.warning(f"No data processed for node {node_name}")
                self.monthly_file_manager.finish_node_processing(node_name)
                return False

            # Split into monthly datasets
            monthly_data = self.process_node_data_to_monthly(node_df)

            # Handle monthly data - append to cached files (no immediate upload)
            if monthly_data:
                processed_files = self.handle_monthly_data(monthly_data)
                if processed_files:
                    logger.info(f"Updated {len(processed_files)} monthly files for node {node_name}")
                else:
                    logger.warning(f"No monthly files processed for node {node_name}")

            # Finish processing this node
            self.monthly_file_manager.finish_node_processing(node_name)
            success = True

        except Exception as e:
            logger.error(f"Error processing node {node_name}: {e}")
            logger.error(traceback.format_exc())
            success = False

            # If we have processed files, clean up node data from them
            if processed_files:
                self.monthly_file_manager.clean_node_data(node_name, list(processed_files.values()))

        finally:
            # Clean up node directory
            if node_dir.exists():
                try:
                    shutil.rmtree(node_dir)
                    logger.info(f"Cleaned up temp directory for node {node_name}")
                except Exception as e:
                    logger.error(f"Error cleaning up node directory {node_dir}: {e}")

            # Recalculate disk usage
            self.quota_manager.recalculate_usage()

        return success

    def sort_nodes_lexicographic(self, node_names: List[str]) -> List[str]:
        """Sort node names lexicographically (NODE1, NODE10, NODE100, etc.)"""
        return sorted(node_names)

    def process_nodes_sequential(self, node_names: List[str]) -> int:
        """Process nodes sequentially with batched uploads, returning count of successful nodes"""
        logger.info(f"Processing {len(node_names)} nodes sequentially")

        # Sort nodes for consistent processing order (lexicographic)
        sorted_nodes = self.sort_nodes_lexicographic(node_names)

        # Handle resume node logic
        if self.resume_node:
            logger.info(f"Resuming from node {self.resume_node}")
            # Find the index of the resume node
            try:
                resume_idx = sorted_nodes.index(self.resume_node)
                # Skip all nodes up to and including the resume node
                sorted_nodes = sorted_nodes[resume_idx + 1:]
                logger.info(f"Skipping {resume_idx + 1} nodes, processing remaining {len(sorted_nodes)} nodes")
            except ValueError:
                logger.warning(f"Resume node {self.resume_node} not found in node list, starting from beginning")

        # Filter out already processed nodes
        nodes_to_process = [node for node in sorted_nodes
                            if not self.monthly_file_manager.is_node_processed(node)]

        logger.info(f"Processing {len(nodes_to_process)} nodes after filtering already processed nodes")

        # Check if we need to resume an interrupted node first
        current_node = self.monthly_file_manager.current_node
        if current_node and current_node in nodes_to_process:
            logger.info(f"Resuming interrupted node {current_node} first")

            # Process the current node first
            success = self.process_node(current_node)

            # Remove from list if successfully processed
            if success:
                nodes_to_process = [n for n in nodes_to_process if n != current_node]

        successful_nodes = 0
        failed_nodes = []
        nodes_since_upload = 0
        retry_nodes = []  # Track nodes that failed due to disk quota for retry

        # Process each node sequentially
        for node in nodes_to_process:
            try:
                # Check if we need to recover disk space before processing this node
                if self.quota_manager.is_quota_exceeded():
                    logger.warning(f"Disk quota exceeded before processing node {node}, triggering recovery")
                    if not self.recover_from_disk_quota_exceeded():
                        logger.error("Failed to recover disk space, skipping node")
                        failed_nodes.append(node)
                        continue

                success = self.process_node(node)
                if success:
                    successful_nodes += 1
                    nodes_since_upload += 1
                    logger.info(f"Successfully processed node: {node}")
                else:
                    # Check if failure was due to disk quota
                    if self.quota_manager.is_quota_exceeded():
                        logger.warning(
                            f"Node {node} processing may have failed due to disk quota, will retry after recovery")
                        retry_nodes.append(node)

                        # Attempt recovery
                        if self.recover_from_disk_quota_exceeded():
                            logger.info("Successfully recovered disk space")
                        else:
                            logger.error("Failed to recover disk space")
                    else:
                        failed_nodes.append(node)
                        logger.warning(f"Failed to process node: {node}")

                # Upload to S3 in batches
                if nodes_since_upload >= self.upload_batch_size:
                    logger.info(
                        f"Processed {nodes_since_upload} nodes since last upload, uploading modified files to S3")
                    upload_success = self.upload_modified_files()
                    if upload_success:
                        nodes_since_upload = 0
                        logger.info("Successfully uploaded batch of files to S3")
                    else:
                        logger.warning("Failed to upload some files, will retry in next batch")

            except Exception as e:
                logger.error(f"Exception processing node {node}: {e}")
                failed_nodes.append(node)

        # Retry nodes that failed due to disk quota
        if retry_nodes:
            logger.info(f"Retrying {len(retry_nodes)} nodes that may have failed due to disk quota")
            for node in retry_nodes:
                try:
                    success = self.process_node(node)
                    if success:
                        successful_nodes += 1
                        nodes_since_upload += 1
                        logger.info(f"Successfully processed node on retry: {node}")
                        # Remove from failed nodes if it was there
                        if node in failed_nodes:
                            failed_nodes.remove(node)
                    else:
                        logger.warning(f"Failed to process node on retry: {node}")
                        if node not in failed_nodes:
                            failed_nodes.append(node)
                except Exception as e:
                    logger.error(f"Exception processing node {node} on retry: {e}")
                    if node not in failed_nodes:
                        failed_nodes.append(node)

        # Final upload of any remaining modified files
        if nodes_since_upload > 0:
            logger.info(f"Uploading final batch of {nodes_since_upload} nodes")
            self.upload_modified_files()

        if failed_nodes:
            logger.warning(f"Failed to process {len(failed_nodes)} nodes: {failed_nodes}")

        return successful_nodes

    def run_etl_pipeline(self) -> bool:
        """Run the full ETL pipeline"""
        logger.info("Starting ETL pipeline")

        try:
            # First, download all monthly files from S3 to local cache
            logger.info("Initializing cache by downloading all monthly files from S3")
            self.download_all_monthly_files()

            # Discover nodes
            nodes = self.node_discoverer.discover_nodes()
            if not nodes:
                logger.error("No nodes discovered, aborting pipeline")
                return False

            logger.info(f"Discovered {len(nodes)} nodes to process")

            # Process nodes sequentially (one at a time)
            successful_nodes = self.process_nodes_sequential(nodes)
            logger.info(f"Successfully processed {successful_nodes}/{len(nodes)} nodes")

            # Final upload to S3 (may be redundant but ensures we upload everything)
            logger.info("Final upload of all modified files to S3")
            upload_success = self.upload_modified_files()

            if not upload_success:
                logger.error("Failed to upload some files in final upload")

            if successful_nodes > 0:
                logger.info("ETL pipeline completed successfully")
                return True
            else:
                logger.warning("ETL pipeline completed with issues")
                return False

        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}")
            logger.error(traceback.format_exc())
            return False


def main():
    """Main entry point"""
    # Initialize ETL with configuration
    etl = NodeETL(
        base_url=CONFIG['base_url'],
        temp_dir=Path(CONFIG['temp_dir']),
        monthly_dir=Path(CONFIG['monthly_dir']),
        cache_dir=Path(CONFIG['cache_dir']),
        s3_bucket=CONFIG['s3_bucket'],
        max_quota_mb=CONFIG['quota_mb'],
        max_download_workers=CONFIG['download_workers'],
        max_process_workers=CONFIG['process_workers'],
        download_timeout=CONFIG['download_timeout'],
        connect_timeout=CONFIG['connect_timeout'],
        s3_timeout=CONFIG['s3_timeout'],
        upload_batch_size=CONFIG['upload_batch_size'],
        resume_node=CONFIG['resume_node'],
        aws_access_key_id=CONFIG['aws_access_key_id'],
        aws_secret_access_key=CONFIG['aws_secret_access_key']
    )

    # Run pipeline
    success = etl.run_etl_pipeline()

    if success:
        logger.info("ETL pipeline completed successfully")
        return 0
    else:
        logger.error("ETL pipeline failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())