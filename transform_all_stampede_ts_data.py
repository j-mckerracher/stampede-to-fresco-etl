"""
ETL Script for Node Data Processing

This script downloads node data, processes it into monthly files,
and uploads the results to AWS S3. Features include:
1. Sequential node processing with parallel internal operations
2. Disk quota management to stay under 24GB
3. File versioning
4. Resumability in case of failure
5. Optimized performance
"""
import concurrent
import os
import sys
import time
import shutil
import logging
import argparse
import threading
import multiprocessing
from typing import List, Dict
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
    """Manage disk quota to prevent exceeding limits"""

    def __init__(self, max_quota_mb: int, temp_dir: Path):
        self.max_quota_mb = max_quota_mb
        self.temp_dir = temp_dir
        self.allocated_mb = 0
        self.lock = threading.Lock()
        logger.info(f"DiskQuotaManager initialized with max quota: {max_quota_mb} MB")

    def get_current_disk_usage(self) -> int:
        """Get current disk usage of temp directory in MB"""
        try:
            if not self.temp_dir.exists():
                return 0

            total_size = 0
            for path in self.temp_dir.glob('**/*'):
                if path.is_file():
                    total_size += path.stat().st_size

            return total_size // (1024 * 1024)
        except Exception as e:
            logger.error(f"Error calculating disk usage: {e}")
            # Return a conservative estimate
            return self.allocated_mb

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
    """Manage monthly data files with versioning"""

    def __init__(self, monthly_dir: Path, state_file: Path):
        self.monthly_dir = monthly_dir
        self.state_file = state_file
        self.monthly_files = {}  # Format: {'YYYY-MM': {'file_path': Path, 'version': int}}
        self.current_node = None
        self.lock = threading.Lock()

        # Create dirs if they don't exist
        self.monthly_dir.mkdir(exist_ok=True, parents=True)

        # Load state if it exists
        self._load_state()
        logger.info(f"MonthlyFileManager initialized with dir: {monthly_dir}")

    def _load_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)

                if 'monthly_files' in state:
                    # Convert string paths back to Path objects
                    for month, data in state['monthly_files'].items():
                        data['file_path'] = Path(data['file_path'])
                    self.monthly_files = state['monthly_files']

                if 'current_node' in state:
                    self.current_node = state['current_node']

                logger.info(f"Loaded state: {len(self.monthly_files)} monthly files, "
                            f"current node: {self.current_node}")
            except Exception as e:
                logger.error(f"Error loading state file: {e}")
                self.monthly_files = {}
                self.current_node = None
        else:
            logger.info("No state file found, starting fresh")

    def _save_state(self):
        """Save processing state to file"""
        try:
            # Convert Path objects to strings for JSON serialization
            serializable_monthly_files = {}
            for month, data in self.monthly_files.items():
                serializable_monthly_files[month] = {
                    'file_path': str(data['file_path']),
                    'version': data['version']
                }

            state = {
                'monthly_files': serializable_monthly_files,
                'current_node': self.current_node
            }

            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)

            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state file: {e}")

    def start_node_processing(self, node_name: str) -> bool:
        """Mark a node as currently being processed"""
        with self.lock:
            if self.current_node and self.current_node != node_name:
                logger.warning(f"Cannot start processing {node_name}, "
                               f"already processing {self.current_node}")
                return False

            self.current_node = node_name
            self._save_state()
            logger.info(f"Started processing node: {node_name}")
            return True

    def finish_node_processing(self, node_name: str):
        """Mark node processing as complete"""
        with self.lock:
            if self.current_node == node_name:
                self.current_node = None
                self._save_state()
                logger.info(f"Finished processing node: {node_name}")
            else:
                logger.warning(f"Node {node_name} was not marked as being processed")

    def clean_node_data(self, node_name: str):
        """Remove all data for this node from monthly files"""
        with self.lock:
            logger.info(f"Cleaning up data for node: {node_name}")

            # Get all monthly files
            files_to_update = []
            for month_data in self.monthly_files.values():
                files_to_update.append(month_data['file_path'])

            for file_path in files_to_update:
                if not file_path.exists():
                    logger.warning(f"Monthly file {file_path} does not exist, skipping cleanup")
                    continue

                try:
                    # Read file
                    df = pl.read_csv(file_path)

                    # Check if this node exists in the file
                    if 'Host' in df.columns and node_name in df['Host'].unique():
                        # Filter out rows for this node
                        logger.info(f"Removing {node_name} data from {file_path}")
                        df_filtered = df.filter(pl.col('Host') != node_name)

                        # Write back to file
                        df_filtered.write_csv(file_path)
                        logger.info(f"Removed {len(df) - len(df_filtered)} rows for {node_name} from {file_path}")
                except Exception as e:
                    logger.error(f"Error cleaning node data from {file_path}: {e}")

    def get_monthly_file(self, timestamp: datetime) -> Path:
        """Get the appropriate monthly file for a timestamp"""
        month_key = timestamp.strftime('%Y-%m')

        with self.lock:
            if month_key not in self.monthly_files:
                version = 1
                file_path = self.monthly_dir / f"node_data_{month_key}_v{version}.csv"
                self.monthly_files[month_key] = {
                    'file_path': file_path,
                    'version': version
                }
                logger.info(f"Created new monthly file entry: {file_path}")
                self._save_state()

            return self.monthly_files[month_key]['file_path']

    def increment_versions(self) -> Dict[str, Path]:
        """Increment all file versions, returning dict of old->new paths"""
        with self.lock:
            version_mapping = {}

            for month_key, data in self.monthly_files.items():
                old_path = data['file_path']
                new_version = data['version'] + 1

                # Create new file path with incremented version
                new_path = self.monthly_dir / f"node_data_{month_key}_v{new_version}.csv"

                # Update in our tracking dict
                self.monthly_files[month_key]['file_path'] = new_path
                self.monthly_files[month_key]['version'] = new_version

                # Add to mapping
                version_mapping[str(old_path)] = new_path

                logger.info(f"Incremented version for {month_key}: v{new_version}")

            self._save_state()
            return version_mapping

    def get_all_current_files(self) -> List[Path]:
        """Get all current monthly files"""
        with self.lock:
            return [data['file_path'] for data in self.monthly_files.values()]


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


class NodeETL:
    """Main ETL class to orchestrate node data processing"""

    def __init__(
            self,
            base_url: str,
            temp_dir: Path,
            monthly_dir: Path,
            s3_bucket: str,
            max_quota_mb: int = 24512,  # 24GB default
            max_download_workers: int = 3,
            max_process_workers: int = None,
            download_timeout: int = 300,
            connect_timeout: int = 10,
            s3_timeout: int = 300
    ):
        self.base_url = base_url
        self.temp_dir = temp_dir
        self.monthly_dir = monthly_dir
        self.s3_bucket = s3_bucket

        # Create directories
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        self.monthly_dir.mkdir(exist_ok=True, parents=True)

        # State file location
        self.state_file = self.monthly_dir / 'etl_state.json'

        # Initialize managers
        self.quota_manager = DiskQuotaManager(max_quota_mb, temp_dir)
        self.monthly_file_manager = MonthlyFileManager(monthly_dir, self.state_file)

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
        self.s3_uploader = S3Uploader(s3_bucket, max_retries=3, timeout=s3_timeout)

        logger.info(f"NodeETL initialized. Temp dir: {temp_dir}, Monthly dir: {monthly_dir}, "
                    f"S3 bucket: {s3_bucket}, Max quota: {max_quota_mb} MB")

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

    def append_to_monthly_files(self, monthly_data: Dict[str, pl.DataFrame]) -> bool:
        """Append data to appropriate monthly files"""
        if not monthly_data:
            logger.warning("No monthly data to append")
            return False

        success = True
        for month, df in monthly_data.items():
            try:
                # Parse month string to datetime for the file manager
                month_date = datetime.strptime(month, '%Y-%m')

                # Get the appropriate file for this month
                file_path = self.monthly_file_manager.get_monthly_file(month_date)

                # Check if file exists and has content
                if file_path.exists() and file_path.stat().st_size > 0:
                    # Read existing file
                    existing_df = pl.read_csv(file_path)

                    # Append new data
                    combined_df = pl.concat([existing_df, df])

                    # Write back to file
                    combined_df.write_csv(file_path)
                    logger.info(f"Appended {len(df)} rows to existing file {file_path}")
                else:
                    # Create new file
                    df.write_csv(file_path)
                    logger.info(f"Created new monthly file {file_path} with {len(df)} rows")

            except Exception as e:
                logger.error(f"Error appending to monthly file for {month}: {e}")
                success = False

        return success

    def upload_monthly_files_to_s3(self) -> bool:
        """Upload all monthly files to S3 with version increment"""
        logger.info("Starting upload of monthly files to S3")

        # Get all current monthly files
        current_files = self.monthly_file_manager.get_all_current_files()
        if not current_files:
            logger.warning("No monthly files to upload")
            return False

        # Make sure all files exist
        files_to_upload = [f for f in current_files if f.exists()]
        if len(files_to_upload) < len(current_files):
            logger.warning(f"Some monthly files are missing: expected {len(current_files)}, "
                           f"found {len(files_to_upload)}")

        if not files_to_upload:
            logger.warning("No monthly files found for upload")
            return False

        # Upload files
        upload_success = self.s3_uploader.upload_files(files_to_upload)

        if upload_success:
            logger.info(f"Successfully uploaded {len(files_to_upload)} files to S3")

            # Increment versions for next upload
            version_mapping = self.monthly_file_manager.increment_versions()

            # Rename files on disk to match new versions
            for old_path_str, new_path in version_mapping.items():
                old_path = Path(old_path_str)
                if old_path.exists():
                    try:
                        shutil.copy2(old_path, new_path)
                        logger.info(f"Copied {old_path} to {new_path}")
                    except Exception as e:
                        logger.error(f"Error copying {old_path} to {new_path}: {e}")
        else:
            logger.error("Failed to upload monthly files to S3")

        return upload_success

    def process_node(self, node_name: str) -> bool:
        """Process a single node end-to-end"""
        logger.info(f"Starting end-to-end processing for node: {node_name}")

        # Check if we're already processing this node (resuming after failure)
        resuming = False
        if self.monthly_file_manager.current_node == node_name:
            logger.info(f"Resuming processing for node {node_name}")
            resuming = True

            # Clean up any partial data for this node
            self.monthly_file_manager.clean_node_data(node_name)

        # Start processing this node
        if not resuming:
            if not self.monthly_file_manager.start_node_processing(node_name):
                logger.warning(f"Cannot start processing {node_name}, already processing another node")
                return False

        # Create node directory in temp
        node_dir = self.temp_dir / node_name
        node_dir.mkdir(exist_ok=True)

        success = False
        try:
            # Download node data
            download_success = self.node_downloader.download_node_files(node_name)
            if not download_success:
                logger.error(f"Failed to download data for node {node_name}")
                return False

            # Process node data
            node_df = self.node_processor.process_node_data(node_dir)

            # Check if we got any data
            if node_df.shape[0] == 0:
                logger.warning(f"No data processed for node {node_name}")
                self.monthly_file_manager.finish_node_processing(node_name)
                return False

            # Split into monthly datasets
            monthly_data = self.process_node_data_to_monthly(node_df)

            # Append to monthly files
            if monthly_data:
                append_success = self.append_to_monthly_files(monthly_data)
                if not append_success:
                    logger.error(f"Failed to append {node_name} data to monthly files")
                    return False

            # Finish processing this node
            self.monthly_file_manager.finish_node_processing(node_name)
            success = True

        except Exception as e:
            logger.error(f"Error processing node {node_name}: {e}")
            logger.error(traceback.format_exc())
            success = False

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

    def process_nodes_sequential(self, node_names: List[str]) -> int:
        """Process nodes sequentially, returning count of successful nodes"""
        logger.info(f"Processing {len(node_names)} nodes sequentially")

        # Check if we need to resume an interrupted node first
        current_node = self.monthly_file_manager.current_node
        if current_node and current_node in node_names:
            logger.info(f"Resuming interrupted node {current_node} first")

            # Process the current node first
            success = self.process_node(current_node)

            # Remove from list if successfully processed
            if success:
                node_names = [n for n in node_names if n != current_node]

        successful_nodes = 0
        failed_nodes = []

        # Process each node sequentially (to fulfill requirement #1)
        for node in node_names:
            try:
                success = self.process_node(node)
                if success:
                    successful_nodes += 1
                    logger.info(f"Successfully processed node: {node}")
                else:
                    failed_nodes.append(node)
                    logger.warning(f"Failed to process node: {node}")
            except Exception as e:
                logger.error(f"Exception processing node {node}: {e}")
                failed_nodes.append(node)

        if failed_nodes:
            logger.warning(f"Failed to process {len(failed_nodes)} nodes: {failed_nodes}")

        return successful_nodes

    def run_etl_pipeline(self) -> bool:
        """Run the full ETL pipeline"""
        logger.info("Starting ETL pipeline")

        try:
            # Discover nodes
            nodes = self.node_discoverer.discover_nodes()
            if not nodes:
                logger.error("No nodes discovered, aborting pipeline")
                return False

            logger.info(f"Discovered {len(nodes)} nodes to process")

            # Process nodes sequentially (one at a time)
            successful_nodes = self.process_nodes_sequential(nodes)
            logger.info(f"Successfully processed {successful_nodes}/{len(nodes)} nodes")

            # Upload results to S3
            upload_success = self.upload_monthly_files_to_s3()

            if successful_nodes > 0 and upload_success:
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
    parser = argparse.ArgumentParser(description='ETL Script for Node Data Processing')
    parser.add_argument('--base-url', type=str, required=True, help='Base URL for node data')
    parser.add_argument('--temp-dir', type=str, default='./temp', help='Temporary directory for downloads')
    parser.add_argument('--monthly-dir', type=str, default='./monthly_files', help='Directory for monthly files')
    parser.add_argument('--s3-bucket', type=str, required=True, help='S3 bucket for uploads')
    parser.add_argument('--quota-mb', type=int, default=24512, help='Disk quota in MB (default: 24GB)')
    parser.add_argument('--download-workers', type=int, default=3, help='Max parallel downloads')
    parser.add_argument('--process-workers', type=int, default=None, help='Max parallel processing workers')
    parser.add_argument('--download-timeout', type=int, default=300, help='Download timeout in seconds')
    parser.add_argument('--connect-timeout', type=int, default=10, help='Connection timeout in seconds')
    parser.add_argument('--s3-timeout', type=int, default=300, help='S3 upload timeout in seconds')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Create ETL pipeline
    etl = NodeETL(
        base_url=args.base_url,
        temp_dir=Path(args.temp_dir),
        monthly_dir=Path(args.monthly_dir),
        s3_bucket=args.s3_bucket,
        max_quota_mb=args.quota_mb,
        max_download_workers=args.download_workers,
        max_process_workers=args.process_workers,
        download_timeout=args.download_timeout,
        connect_timeout=args.connect_timeout,
        s3_timeout=args.s3_timeout
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