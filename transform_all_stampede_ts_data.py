import logging
import subprocess
import sys
import threading
import os
import numpy as np
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
import shutil
import time
import glob
import json
from pathlib import Path
import gc
import psutil
from typing import Dict
import pandas as pd
from helpers.data_version_manager import DataVersionManager
from helpers.utilities import log_disk_usage, cleanup_temp_files, setup_quota_logging, check_disk_space, \
    check_critical_disk_space, get_user_disk_usage


def save_monthly_data_locally(monthly_data, base_dir, version_manager):
    """
    Save monthly data to local files, updating existing files if they exist
    Returns list of saved file paths
    """
    output_dir = Path(base_dir) / "monthly_data"
    output_dir.mkdir(exist_ok=True)

    version_suffix = version_manager.get_current_version()
    saved_files = []

    for month, new_df in monthly_data.items():
        file_path = output_dir / f"FRESCO_Stampede_ts_{month}_{version_suffix}.csv"

        if file_path.exists():
            # Read existing file and merge with new data
            existing_df = pd.read_csv(file_path)
            merged_df = pd.concat([existing_df, new_df]).drop_duplicates()
            merged_df.to_csv(file_path, index=False)
        else:
            # Create new file
            new_df.to_csv(file_path, index=False)

        saved_files.append(str(file_path))

    return saved_files


def manage_storage_and_upload(monthly_data, base_dir, version_manager):
    """
    Manage local storage and S3 uploads based on disk space
    Returns: bool indicating if upload was performed
    """
    is_safe, is_abundant = check_critical_disk_space()

    if not is_safe:
        print("\nCritical disk space reached. Initiating upload process...")
        # Get all local files for current version
        version_suffix = version_manager.get_current_version()
        local_files = glob.glob(
            os.path.join(base_dir, "monthly_data", f"*_{version_suffix}.csv")
        )

        # Upload to S3
        if upload_to_s3(local_files):
            print(f"Successfully uploaded {version_suffix} files to S3")
            version_manager.increment_version()

            # Clear monthly_data after successful upload
            monthly_data.clear()
            gc.collect()

            return True
        else:
            print("Failed to upload to S3. Will retry when disk space is critical again.")
            return False

    elif not is_abundant:
        print("\nWarning: Disk space is running low")

    return False


# Tracking and Safety Management Class
class ProcessingTracker:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        # Ensure directory exists
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        self.tracker_file = Path(base_dir) / "processing_status.json"
        self.batch_sizes_file = Path(base_dir) / "batch_sizes.json"
        self.load_status()

    def load_status(self):
        """Load or initialize tracking data"""
        try:
            if self.tracker_file.exists():
                with open(self.tracker_file, 'r') as f:
                    self.status = json.load(f)
            else:
                self.status = {
                    'processed_nodes': [],
                    'failed_nodes': [],
                    'current_batch': 0
                }
        except json.JSONDecodeError:
            # File exists but is corrupted/empty
            print(f"Warning: {self.tracker_file} was corrupted, reinitializing tracking data")
            self.status = {
                'processed_nodes': [],
                'failed_nodes': [],
                'current_batch': 0
            }

        if self.batch_sizes_file.exists():
            with open(self.batch_sizes_file, 'r') as f:
                self.batch_sizes = json.load(f)
        else:
            self.batch_sizes = {
                'sizes': [],
                'average': 0
            }

    def save_status(self):
        """Save current status to file"""
        with open(self.tracker_file, 'w') as f:
            json.dump(self.status, f)

    def save_batch_sizes(self):
        """Save batch size information"""
        with open(self.batch_sizes_file, 'w') as f:
            json.dump(self.batch_sizes, f)

    def update_batch_size(self, size_in_bytes):
        """Update running average of batch sizes"""
        self.batch_sizes['sizes'].append(size_in_bytes)
        self.batch_sizes['average'] = sum(self.batch_sizes['sizes']) / len(self.batch_sizes['sizes'])
        self.save_batch_sizes()

    def is_node_processed(self, node_name):
        """Check if node has been processed"""
        return node_name in self.status['processed_nodes']

    def mark_node_processed(self, node_name):
        """Mark a node as successfully processed"""
        if node_name not in self.status['processed_nodes']:
            self.status['processed_nodes'].append(node_name)
            self.save_status()

    def mark_node_failed(self, node_name):
        """Mark a node as failed"""
        if node_name not in self.status['failed_nodes']:
            self.status['failed_nodes'].append(node_name)
            self.save_status()

    def get_unprocessed_nodes(self, available_nodes):
        """Get list of nodes that haven't been processed yet"""
        return [node for node in available_nodes
                if node not in self.status['processed_nodes']]


# Directory and File Management Functions
def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def download_file(url, local_path, max_retries=3, retry_delay=5):
    """Download file with retry mechanism"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            with open(local_path, 'wb') as f:
                f.write(response.content)
            return True

        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff

    return False


def download_node_folder(node_info, save_dir, tracker):
    """
    Downloads only the required CSV files (block, cpu, nfs, mem) for a single node folder.
    To be run in a separate thread.
    """
    link, node_url = node_info
    node_name = link.text.strip('/')
    required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']

    try:
        if tracker.is_node_processed(node_name):
            print(f"Node {node_name} already processed, skipping...")
            return

        node_dir = os.path.join(save_dir, node_name)
        create_directory(node_dir)

        # Get the CSV files in the NODE directory
        node_response = requests.get(node_url)
        node_soup = BeautifulSoup(node_response.text, 'html.parser')

        download_success = True
        files_found = 0

        for csv_link in node_soup.find_all('a'):
            if csv_link.text in required_files:
                csv_url = urljoin(node_url, csv_link['href'])
                csv_path = os.path.join(node_dir, csv_link.text)
                if not download_file(csv_url, csv_path):
                    download_success = False
                    break
                files_found += 1
                time.sleep(0.5)  # Small delay between files

        if download_success and files_found == len(required_files):
            print(f"Completed downloading required files for {node_name}")
            tracker.mark_node_processed(node_name)
        else:
            print(f"Failed to download all required files for {node_name}")
            tracker.mark_node_failed(node_name)
            # Clean up partial downloads
            if os.path.exists(node_dir):
                shutil.rmtree(node_dir)

    except Exception as e:
        print(f"Error downloading node folder {node_name}: {str(e)}")
        tracker.mark_node_failed(node_name)
        # Clean up in case of error
        if os.path.exists(node_dir):
            shutil.rmtree(node_dir)


def scrape_and_download(base_url, save_dir, tracker, start_index=0):
    """
    Downloads 3 NODE folders in parallel starting from start_index.
    Returns the next start_index and whether there are more folders to process.
    """
    if not check_disk_space():
        print("Insufficient disk space. Aborting batch.")
        return None, False

    create_directory(save_dir)
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Get all unprocessed NODE directories
        node_links = [link for link in soup.find_all('a')
                      if 'NODE' in link.text and not tracker.is_node_processed(link.text.strip('/'))]

        if start_index >= len(node_links):
            return None, False

        current_batch = node_links[start_index:start_index + 3]

        # Create threads for parallel downloads
        threads = []
        batch_size = 0

        for link in current_batch:
            node_url = urljoin(base_url, link['href'])
            thread = threading.Thread(
                target=download_node_folder,
                args=((link, node_url), save_dir, tracker)
            )
            threads.append(thread)
            thread.start()

        # Wait for all downloads to complete
        for thread in threads:
            thread.join()

        # Update batch size tracking
        batch_size = sum(os.path.getsize(os.path.join(dirpath, filename))
                         for dirpath, dirnames, filenames in os.walk(save_dir)
                         for filename in filenames)
        tracker.update_batch_size(batch_size)

        next_index = start_index + 3
        has_more = next_index < len(node_links)

        return next_index, has_more

    except Exception as e:
        print(f"Error accessing {base_url}: {str(e)}")
        return None, False


def process_block_file(file_path):
    """Process block.csv file and transform to FRESCO format"""
    df = pd.read_csv(file_path)

    # Calculate total sectors and ticks
    df['total_sectors'] = df['rd_sectors'] + df['wr_sectors']
    df['total_ticks_seconds'] = (df['rd_ticks'] + df['wr_ticks']) / 1000  # Convert ms to seconds

    # Calculate bytes per second, handling potential division by zero
    df['Value'] = np.where(
        df['total_ticks_seconds'] > 0,
        (df['total_sectors'] * 512) / df['total_ticks_seconds'],
        0
    )

    # Convert to GB/s
    df['Value'] = df['Value'] / (1024 * 1024 * 1024)

    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    result = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'block',
        'Value': df['Value'],
        'Units': 'GB/s',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    return result


def process_cpu_file(file_path):
    """Process cpu.csv file and transform to FRESCO format"""
    df = pd.read_csv(file_path)

    # Calculate total CPU ticks
    df['total_ticks'] = (
            df['user'] + df['nice'] + df['system'] +
            df['idle'] + df['iowait'] + df['irq'] + df['softirq']
    )

    # Calculate user CPU percentage (user + nice), handling edge cases
    df['Value'] = np.where(
        df['total_ticks'] > 0,
        ((df['user'] + df['nice']) / df['total_ticks']) * 100,
        0
    )

    # Ensure values don't exceed 100%
    df['Value'] = df['Value'].clip(0, 100)

    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    result = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'cpuuser',
        'Value': df['Value'],
        'Units': 'CPU %',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    return result


def process_nfs_file(file_path):
    """Process nfs.csv file and transform to FRESCO format"""
    df = pd.read_csv(file_path)

    # Calculate NFS throughput in MB/s using actual NFS operation bytes
    # Sum of READ bytes received and WRITE bytes sent
    df['Value'] = (df['READ_bytes_recv'] + df['WRITE_bytes_sent']) / (1024 * 1024)  # Convert to MB

    # Calculate the time difference to get proper rate
    df['Timestamp'] = pd.to_datetime(df['timestamp'])
    df['TimeDiff'] = df['Timestamp'].diff().dt.total_seconds().fillna(600)  # Default to 600s (10 min) for first row

    # Convert to rate (MB/s)
    df['Value'] = df['Value'] / df['TimeDiff']

    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    result = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'nfs',
        'Value': df['Value'],
        'Units': 'MB/s',
        'Timestamp': df['Timestamp']
    })

    return result


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


def split_by_month(df):
    """
    Split DataFrame into monthly groups.
    Returns a dictionary with year_month as key and DataFrame as value.
    """
    monthly_data = {}
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    # Group by year and month
    for name, group in df.groupby(df['Timestamp'].dt.strftime('%Y_%m')):
        monthly_data[name] = group

    return monthly_data


def update_monthly_data(existing_data, new_data):
    """
    Update existing monthly data with new data.
    Both parameters are dictionaries with year_month keys and DataFrames as values.
    """
    for month, new_df in new_data.items():
        if month in existing_data:
            # Concatenate and remove duplicates
            existing_data[month] = pd.concat([existing_data[month], new_df]).drop_duplicates()
        else:
            existing_data[month] = new_df
    return existing_data


def upload_to_s3(file_paths, bucket_name="data-transform-stampede", max_retries=3):
    """
    Upload files to S3 bucket with retry logic.
    Returns True if all uploads successful, False otherwise.
    """
    print("\nStarting S3 upload...")
    s3_client = boto3.client('s3')

    total_files = len(file_paths)
    for i, file_path in enumerate(file_paths, 1):
        file_name = os.path.basename(file_path)

        for attempt in range(max_retries):
            try:
                s3_client.upload_file(file_path, bucket_name, file_name)
                if i % 2 == 0 or i == total_files:  # Progress update every 2 files
                    print(f"Uploaded {i}/{total_files} files to S3")
                break
            except ClientError as e:
                if attempt == max_retries - 1:
                    print(f"Failed to upload {file_name} after {max_retries} attempts")
                    print(f"Error: {str(e)}")
                    return False
                time.sleep(2 ** attempt)  # Exponential backoff

    return True


def upload_monthly_data(monthly_data, base_dir, bucket_name="data-transform-stampede", max_retries=3):
    """
    Upload final monthly data to S3
    """
    print("\nPreparing to upload final monthly data to S3...")

    # Save monthly data to temporary files
    temp_files = save_monthly_data_locally(monthly_data, base_dir)

    # Upload to S3
    upload_success = upload_to_s3(temp_files, bucket_name, max_retries)

    # Clean up temporary files
    cleanup_files([os.path.join(base_dir, "temp_monthly_data")])

    return upload_success


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


def check_memory_usage(threshold_percent=90):
    """Check if memory usage is too high"""
    memory = psutil.virtual_memory()
    return memory.percent < threshold_percent


def get_base_dir():
    """Get the base directory in a platform-agnostic way"""
    return os.path.dirname(os.path.abspath(__file__))


def main():
    """
    Main function with version-aware storage management
    """
    try:
        base_dir = get_base_dir()
        base_url = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/"

        # Set up logging and log initial state
        setup_quota_logging(base_dir)
        logging.info("Starting script execution")
        log_disk_usage()

        # Initialize trackers
        tracker = ProcessingTracker(base_dir)
        version_manager = DataVersionManager(base_dir)
        monthly_data: Dict[str, pd.DataFrame] = {}

        start_index = 0
        batch_number = tracker.status['current_batch']

        while True:
            logging.info(f"\nProcessing batch {batch_number}...")

            # Check storage and handle uploads in one operation
            if not manage_storage_and_upload(monthly_data, base_dir, version_manager):
                logging.error("Storage management failed. Saving progress and stopping.")
                if monthly_data:
                    save_monthly_data_locally(monthly_data, base_dir, version_manager)
                break

            try:
                next_index, has_more = scrape_and_download(base_url, base_dir, tracker, start_index)

                if next_index is None:
                    logging.info("No more folders to process or error occurred.")
                    break

                # Process the current batch
                node_folders = glob.glob(os.path.join(base_dir, "NODE*"))

                for folder in node_folders:
                    try:
                        logging.info(f"Processing folder: {folder}")
                        result = process_node_folder(folder)

                        if result is not None:
                            # Process one folder's results immediately
                            batch_monthly = split_by_month(result)
                            monthly_data = update_monthly_data(monthly_data, batch_monthly)

                            # Save to local storage
                            save_monthly_data_locally(batch_monthly, base_dir, version_manager)

                            # Clear memory
                            del result
                            del batch_monthly
                            gc.collect()

                        # Clean up temporary folder
                        cleanup_files([folder])

                    except Exception as folder_error:
                        logging.error(f"Error processing folder {folder}: {str(folder_error)}")
                        cleanup_files([folder])
                        continue

                # Update progress
                tracker.status['current_batch'] = batch_number
                tracker.save_status()

                if has_more:
                    start_index = next_index
                    batch_number += 1
                else:
                    # All nodes processed
                    if monthly_data:
                        # Save final batch locally
                        save_monthly_data_locally(monthly_data, base_dir, version_manager)
                    logging.info("All folders have been processed!")
                    break

            except Exception as e:
                logging.error(f"Error processing batch {batch_number}: {str(e)}")
                # Save current progress locally
                if monthly_data:
                    try:
                        save_monthly_data_locally(monthly_data, base_dir, version_manager)
                    except Exception as save_error:
                        logging.error(f"Error saving progress: {str(save_error)}")
                break

            # Small delay between batches
            time.sleep(5)

    except Exception as e:
        logging.error(f"Fatal error in main execution: {str(e)}")
        raise
    finally:
        # Ensure cleanup happens after script completion or failure
        logging.info("Performing final cleanup")
        space_freed = cleanup_temp_files(base_dir)
        logging.info(f"Final cleanup freed {space_freed:.2f}MB")
        log_disk_usage()  # Log final disk usage state
        logging.info("Script execution completed")


def test_data_processing():
    # Set up test directory
    test_dir = r""
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    # Initialize version manager
    version_manager = DataVersionManager(test_dir)
    print(f"Initial version: {version_manager.get_current_version()}")

    # Create some test data
    test_data = {
        '2024_01': pd.DataFrame({
            'Job Id': ['JOB1', 'JOB2'],
            'Host': ['node1', 'node2'],
            'Event': ['block', 'cpu'],
            'Value': [1.2, 3.4],
            'Units': ['GB/s', 'CPU %'],
            'Timestamp': pd.date_range('2024-01-01', periods=2)
        }),
        '2024_02': pd.DataFrame({
            'Job Id': ['JOB3', 'JOB4'],
            'Host': ['node3', 'node4'],
            'Event': ['nfs', 'block'],
            'Value': [2.3, 4.5],
            'Units': ['MB/s', 'GB/s'],
            'Timestamp': pd.date_range('2024-02-01', periods=2)
        })
    }

    # Test saving data locally
    print("\nSaving test data locally...")
    saved_files = save_monthly_data_locally(test_data, test_dir, version_manager)
    print(f"Saved files: {saved_files}")

    # Verify files exist
    print("\nVerifying saved files...")
    for file_path in saved_files:
        if os.path.exists(file_path):
            print(f"File exists: {file_path}")
            print(f"File size: {os.path.getsize(file_path)} bytes")

    # Test S3 upload (optional)
    print("\nTesting S3 upload...")
    upload_success = upload_to_s3(saved_files)
    print(f"Upload success: {upload_success}")

    if upload_success:
        version_manager.increment_version()
        print(f"New version after upload: {version_manager.get_current_version()}")

    return "Test completed successfully"


def test_quota_parsing():
    """Test the quota parsing functionality"""
    used_mb, quota_mb, limit_mb = get_user_disk_usage()
    if used_mb is not None:
        print(f"\nQuota parsing test results:")
        print(f"Used space: {used_mb:.2f}MB")
        print(f"Quota: {quota_mb:.2f}MB")
        print(f"Limit: {limit_mb:.2f}MB")
        print(f"Usage percentage: {(used_mb / quota_mb * 100):.1f}%")
        print(f"Available space: {(quota_mb - used_mb):.2f}MB")
        return True
    return False


# Add this to your main() function to test:
if __name__ == "__main__":
    main()
    # logging.info("Testing quota parsing...")
    # if not test_quota_parsing():
    #     logging.error("Quota parsing test failed!")
    #     sys.exit(1)
    # logging.info("Quota parsing test successful, proceeding with main execution...")
