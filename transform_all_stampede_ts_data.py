import os
import json
import time
import shutil
import threading
from queue import Queue
from pathlib import Path
from typing import Dict, List, Set
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import boto3
import concurrent.futures
from dataclasses import dataclass, asdict
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log'),
        logging.StreamHandler()
    ]
)


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

    def get_available_space_mb(self) -> int:
        with self._lock:
            return self.quota_mb - self.safety_buffer_mb - self.current_usage_mb

    def request_space(self, needed_mb: int) -> bool:
        """Try to allocate space for an operation"""
        with self._lock:
            if self.current_usage_mb + needed_mb <= self.quota_mb - self.safety_buffer_mb:
                self.current_usage_mb += needed_mb
                return True
            return False

    def release_space(self, released_mb: int):
        """Release allocated space after operation"""
        with self._lock:
            self.current_usage_mb = max(0, self.current_usage_mb - released_mb)


class VersionManager:
    """Manage versioning of monthly data files"""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.version_file = self.base_dir / "version_info.json"
        self._lock = threading.Lock()
        self.load_version_info()

    def load_version_info(self):
        if self.version_file.exists():
            with open(self.version_file) as f:
                self.version_info = json.load(f)
        else:
            self.version_info = {
                'current_version': 1,
                'uploaded_versions': []
            }
            self.save_version_info()

    def save_version_info(self):
        with self._lock:
            with open(self.version_file, 'w') as f:
                json.dump(self.version_info, f)

    def get_current_version(self) -> str:
        return f"v{self.version_info['current_version']}"

    def increment_version(self):
        with self._lock:
            self.version_info['uploaded_versions'].append(
                self.version_info['current_version']
            )
            self.version_info['current_version'] += 1
            self.save_version_info()


class ProcessingStateManager:
    """Manage processing state and recovery"""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.state_file = self.base_dir / "processing_state.json"
        self.node_statuses: Dict[str, NodeStatus] = {}
        self._lock = threading.Lock()
        self.load_state()

    def load_state(self):
        if self.state_file.exists():
            with open(self.state_file) as f:
                state_dict = json.load(f)
                self.node_statuses = {
                    name: NodeStatus(**status)
                    for name, status in state_dict.items()
                }

    def save_state(self):
        with self._lock:
            state_dict = {
                name: asdict(status)
                for name, status in self.node_statuses.items()
            }
            with open(self.state_file, 'w') as f:
                json.dump(state_dict, f)

    def update_node_status(self, node_name: str, **kwargs):
        with self._lock:
            if node_name not in self.node_statuses:
                self.node_statuses[node_name] = NodeStatus(name=node_name)

            status = self.node_statuses[node_name]
            for key, value in kwargs.items():
                setattr(status, key, value)

            self.save_state()

    def get_incomplete_nodes(self) -> List[str]:
        return [
            name for name, status in self.node_statuses.items()
            if not status.processing_complete
        ]


class MonthlyDataManager:
    """Manage monthly data files"""

    def __init__(self, base_dir: str, version_manager: VersionManager):
        self.base_dir = Path(base_dir)
        self.monthly_data_dir = self.base_dir / "monthly_data"
        self.monthly_data_dir.mkdir(exist_ok=True)
        self.version_manager = version_manager
        self._lock = threading.Lock()

    def get_monthly_file_path(self, year_month: str) -> Path:
        version = self.version_manager.get_current_version()
        return self.monthly_data_dir / f"FRESCO_Stampede_ts_{year_month}_{version}.csv"

    def save_monthly_data(self, monthly_data: Dict[str, pd.DataFrame]) -> List[Path]:
        saved_files = []
        with self._lock:
            for year_month, df in monthly_data.items():
                file_path = self.get_monthly_file_path(year_month)

                if file_path.exists():
                    existing_df = pd.read_csv(file_path)
                    merged_df = pd.concat([existing_df, df]).drop_duplicates()
                    merged_df.to_csv(file_path, index=False)
                else:
                    df.to_csv(file_path, index=False)

                saved_files.append(file_path)

        return saved_files


class S3Uploader:
    """Handle S3 uploads with retries"""

    def __init__(self, bucket_name: str, max_retries: int = 3):
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        self.s3_client = boto3.client('s3')

    def upload_files(self, file_paths: List[Path]) -> bool:
        success = True
        for file_path in file_paths:
            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    self.s3_client.upload_file(
                        str(file_path),
                        self.bucket_name,
                        file_path.name
                    )
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == self.max_retries:
                        logging.error(f"Failed to upload {file_path}: {e}")
                        success = False
                    time.sleep(2 ** retry_count)

        return success


class NodeDataProcessor:
    """Process node data files"""

    @staticmethod
    def process_block_file(file_path: Path) -> pd.DataFrame:
        df = pd.read_csv(file_path)

        # Calculate throughput
        df['total_sectors'] = df['rd_sectors'] + df['wr_sectors']
        df['total_ticks'] = (df['rd_ticks'] + df['wr_ticks']) / 1000

        # Convert to GB/s
        df['Value'] = np.where(
            df['total_ticks'] > 0,
            (df['total_sectors'] * 512) / df['total_ticks'] / (1024 ** 3),
            0
        )

        return pd.DataFrame({
            'Job Id': df['jobID'].str.replace('job', 'JOB', case=False),
            'Host': df['node'],
            'Event': 'block',
            'Value': df['Value'],
            'Units': 'GB/s',
            'Timestamp': pd.to_datetime(df['timestamp'])
        })

    @staticmethod
    def process_cpu_file(file_path: Path) -> pd.DataFrame:
        df = pd.read_csv(file_path)

        df['total_ticks'] = (
                df['user'] + df['nice'] + df['system'] +
                df['idle'] + df['iowait'] + df['irq'] + df['softirq']
        )

        df['Value'] = np.where(
            df['total_ticks'] > 0,
            ((df['user'] + df['nice']) / df['total_ticks']) * 100,
            0
        ).clip(0, 100)

        return pd.DataFrame({
            'Job Id': df['jobID'].str.replace('job', 'JOB', case=False),
            'Host': df['node'],
            'Event': 'cpuuser',
            'Value': df['Value'],
            'Units': 'CPU %',
            'Timestamp': pd.to_datetime(df['timestamp'])
        })

    @staticmethod
    def process_nfs_file(file_path: Path) -> pd.DataFrame:
        df = pd.read_csv(file_path)

        # Calculate MB/s
        df['Value'] = (
                              df['READ_bytes_recv'] + df['WRITE_bytes_sent']
                      ) / (1024 * 1024)

        df['Timestamp'] = pd.to_datetime(df['timestamp'])
        df['TimeDiff'] = df['Timestamp'].diff().dt.total_seconds().fillna(600)
        df['Value'] = df['Value'] / df['TimeDiff']

        return pd.DataFrame({
            'Job Id': df['jobID'].str.replace('job', 'JOB', case=False),
            'Host': df['node'],
            'Event': 'nfs',
            'Value': df['Value'],
            'Units': 'MB/s',
            'Timestamp': df['Timestamp']
        })

    @staticmethod
    def process_memory_metrics(file_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
        df = pd.read_csv(file_path)

        # Convert KB to bytes
        memory_cols = ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']
        for col in memory_cols:
            if col in df.columns:
                df[col] *= 1024

        # Calculate metrics in GB
        df['memused'] = df['MemUsed'] / (1024 ** 3)
        df['memused_minus_diskcache'] = (
                (df['MemUsed'] - df['FilePages']) / (1024 ** 3)
        ).clip(lower=0)

        base_df = {
            'Job Id': df['jobID'].str.replace('job', 'JOB', case=False),
            'Host': df['node'],
            'Timestamp': pd.to_datetime(df['timestamp'])
        }

        memused_df = pd.DataFrame({
            **base_df,
            'Event': 'memused',
            'Value': df['memused'],
            'Units': 'GB'
        })

        memused_nocache_df = pd.DataFrame({
            **base_df,
            'Event': 'memused_minus_diskcache',
            'Value': df['memused_minus_diskcache'],
            'Units': 'GB'
        })

        return memused_df, memused_nocache_df


class NodeDownloader:
    """Download node data with parallel processing"""

    def __init__(
            self,
            base_url: str,
            save_dir: Path,
            quota_manager: DiskQuotaManager,
            max_workers: int = 3
    ):
        self.base_url = base_url
        self.save_dir = save_dir
        self.quota_manager = quota_manager
        self.max_workers = max_workers
        self.session = requests.Session()

    def download_node_files(self, node_name: str) -> bool:
        node_dir = self.save_dir / node_name
        node_dir.mkdir(exist_ok=True)

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

            return success

        except Exception as e:
            logging.error(f"Error downloading {node_name}: {e}")
            if node_dir.exists():
                shutil.rmtree(node_dir)
            return False

    def _download_file(self, url: str, file_path: Path) -> bool:
        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()

            # Get file size and request quota
            file_size = int(response.headers.get('content-length', 0))
            if not self.quota_manager.request_space(file_size // (1024 * 1024) + 1):
                return False

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return True

        except Exception as e:
            logging.error(f"Error downloading {url}: {e}")
            return False
        finally:
            # Release quota space if download failed
            if not file_path.exists():
                self.quota_manager.release_space(file_size // (1024 * 1024) + 1)


def main():
    """Main entry point"""
    base_dir = os.getenv('ETL_BASE_DIR', '/path/to/base/dir')
    base_url = os.getenv(
        'ETL_BASE_URL',
        'https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/'
    )
    quota_mb = int(os.getenv('ETL_QUOTA_MB', '24512'))
    bucket_name = os.getenv('ETL_S3_BUCKET', 'data-transform-stampede')

    pipeline = ETLPipeline(
        base_url=base_url,
        base_dir=base_dir,
        quota_mb=quota_mb,
        bucket_name=bucket_name
    )

    pipeline.run()


if __name__ == '__main__':
    main()


class ETLPipeline:
    """Main ETL pipeline implementation"""

    def __init__(
            self,
            base_url: str,
            base_dir: str,
            quota_mb: int,
            bucket_name: str,
            max_download_workers: int = 3,
            max_process_workers: int = 4
    ):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)

        # Initialize managers
        self.quota_manager = DiskQuotaManager(quota_mb)
        self.version_manager = VersionManager(base_dir)
        self.state_manager = ProcessingStateManager(base_dir)
        self.monthly_data_manager = MonthlyDataManager(base_dir, self.version_manager)

        # Initialize workers
        self.downloader = NodeDownloader(
            base_url,
            self.base_dir,
            self.quota_manager,
            max_download_workers
        )
        self.processor = NodeDataProcessor()
        self.s3_uploader = S3Uploader(bucket_name)

        # Queues for producer-consumer pattern
        self.download_queue = Queue()
        self.process_queue = Queue()

        # Threading controls
        self.max_download_workers = max_download_workers
        self.max_process_workers = max_process_workers
        self.should_stop = threading.Event()

        # Tracking sets for active processing
        self.active_downloads = set()
        self.active_processing = set()
        self._lock = threading.Lock()

    def get_node_list(self) -> List[str]:
        """Get list of nodes to process from base URL"""
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            return [
                link.text.strip('/')
                for link in soup.find_all('a')
                if link.text.startswith('NODE')
            ]
        except Exception as e:
            logging.error(f"Error getting node list: {e}")
            return []

    def download_worker(self):
        """Worker thread for downloading node data"""
        while not self.should_stop.is_set():
            try:
                node_name = self.download_queue.get(timeout=1)

                with self._lock:
                    self.active_downloads.add(node_name)

                success = self.downloader.download_node_files(node_name)

                if success:
                    self.state_manager.update_node_status(
                        node_name,
                        download_complete=True
                    )
                    self.process_queue.put(node_name)
                else:
                    self.state_manager.update_node_status(
                        node_name,
                        download_complete=False
                    )

                with self._lock:
                    self.active_downloads.remove(node_name)

                self.download_queue.task_done()

            except Queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error in download worker: {e}")
                continue

    def process_worker(self):
        """Worker thread for processing node data"""
        while not self.should_stop.is_set():
            try:
                node_name = self.process_queue.get(timeout=1)

                with self._lock:
                    self.active_processing.add(node_name)

                node_dir = self.base_dir / node_name
                if not node_dir.exists():
                    continue

                try:
                    # Process each file type
                    results = []

                    if (node_dir / 'block.csv').exists():
                        results.append(
                            self.processor.process_block_file(node_dir / 'block.csv')
                        )

                    if (node_dir / 'cpu.csv').exists():
                        results.append(
                            self.processor.process_cpu_file(node_dir / 'cpu.csv')
                        )

                    if (node_dir / 'nfs.csv').exists():
                        results.append(
                            self.processor.process_nfs_file(node_dir / 'nfs.csv')
                        )

                    if (node_dir / 'mem.csv').exists():
                        memused_df, memused_nocache_df = (
                            self.processor.process_memory_metrics(node_dir / 'mem.csv')
                        )
                        results.extend([memused_df, memused_nocache_df])

                    # Combine and split by month
                    if results:
                        combined_df = pd.concat(results, ignore_index=True)
                        monthly_data = self._split_by_month(combined_df)

                        # Save to monthly files
                        self.monthly_data_manager.save_monthly_data(monthly_data)

                    # Mark as complete and clean up
                    self.state_manager.update_node_status(
                        node_name,
                        processing_complete=True
                    )
                    shutil.rmtree(node_dir)

                except Exception as e:
                    logging.error(f"Error processing {node_name}: {e}")
                    self.state_manager.update_node_status(
                        node_name,
                        processing_complete=False
                    )

                with self._lock:
                    self.active_processing.remove(node_name)

                self.process_queue.task_done()

            except Queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error in process worker: {e}")
                continue

    def _split_by_month(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Split DataFrame into monthly groups"""
        monthly_data = {}
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])

        for name, group in df.groupby(df['Timestamp'].dt.strftime('%Y_%m')):
            monthly_data[name] = group

        return monthly_data

    def check_upload_needed(self) -> bool:
        """Check if S3 upload is needed based on disk usage"""
        available_space = self.quota_manager.get_available_space_mb()
        return available_space < (self.quota_manager.quota_mb * 0.2)  # 20% threshold

    def upload_to_s3(self) -> bool:
        """Upload current version of monthly files to S3"""
        try:
            monthly_files = list(self.monthly_data_manager.monthly_data_dir.glob(
                f"*_{self.version_manager.get_current_version()}.csv"
            ))

            if not monthly_files:
                return False

            success = self.s3_uploader.upload_files(monthly_files)

            if success:
                self.version_manager.increment_version()

            return success

        except Exception as e:
            logging.error(f"Error uploading to S3: {e}")
            return False

    def run(self):
        """Run the ETL pipeline"""
        try:
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
                thread.start()

            # Get nodes to process
            nodes = self.get_node_list()
            incomplete_nodes = self.state_manager.get_incomplete_nodes()

            # Add nodes to download queue
            for node in nodes:
                if node in incomplete_nodes:
                    # Clean up any partial data
                    node_dir = self.base_dir / node
                    if node_dir.exists():
                        shutil.rmtree(node_dir)

                self.download_queue.put(node)

            # Monitor queues and disk space
            while True:
                if (self.download_queue.empty() and
                        self.process_queue.empty() and
                        not self.active_downloads and
                        not self.active_processing):
                    break

                if self.check_upload_needed():
                    self.upload_to_s3()

                time.sleep(10)

            # Final upload
            self.upload_to_s3()

        except KeyboardInterrupt:
            logging.info("Received shutdown signal")
            self.should_stop.set()

        finally:
            # Wait for threads to complete
            self.should_stop.set()
            for thread in download_threads + process_threads:
                thread.join()

            logging.info("ETL pipeline completed")