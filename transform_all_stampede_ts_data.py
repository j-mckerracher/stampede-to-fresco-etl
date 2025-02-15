import threading
from queue import Queue
import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
import shutil
import time
import pandas as pd
import glob
import psutil
import json
from pathlib import Path


# Tracking and Safety Management Class
class ProcessingTracker:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.tracker_file = Path(base_dir) / "processing_status.json"
        self.batch_sizes_file = Path(base_dir) / "batch_sizes.json"
        self.load_status()

    def load_status(self):
        """Load or initialize tracking data"""
        if self.tracker_file.exists():
            with open(self.tracker_file, 'r') as f:
                self.status = json.load(f)
        else:
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


# Safety Check Functions
def check_disk_space(required_space_gb=10):
    """
    Check if there's enough disk space available
    Returns True if enough space, False otherwise
    """
    disk_usage = psutil.disk_usage('C:')
    available_gb = disk_usage.free / (1024 ** 3)  # Convert to GB
    return available_gb > required_space_gb


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
    Downloads all CSV files for a single node folder.
    To be run in a separate thread.
    """
    link, node_url = node_info
    node_name = link.text.strip('/')

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
        for csv_link in node_soup.find_all('a'):
            if csv_link.text.endswith('.csv'):
                csv_url = urljoin(node_url, csv_link['href'])
                csv_path = os.path.join(node_dir, csv_link.text)
                if not download_file(csv_url, csv_path):
                    download_success = False
                    break
                time.sleep(0.5)  # Small delay between files

        if download_success:
            print(f"Completed downloading files for {node_name}")
            tracker.mark_node_processed(node_name)
        else:
            print(f"Failed to download some files for {node_name}")
            tracker.mark_node_failed(node_name)

    except Exception as e:
        print(f"Error downloading node folder {node_name}: {str(e)}")
        tracker.mark_node_failed(node_name)


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


# [Keeping all existing processing functions exactly as they were]
def process_block_file(file_path):
    """Process block.csv file and transform to FRESCO format"""
    df = pd.read_csv(file_path)

    # Calculate value in GB/s (sectors are 512 bytes)
    # Calculate bytes per second from sectors and ticks
    df['Value'] = ((df['rd_sectors'] + df['wr_sectors']) * 512) / (df['rd_ticks'] + df['wr_ticks'])
    # Convert to GB/s
    df['Value'] = df['Value'] / (1024 * 1024 * 1024)

    # Create FRESCO format
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

    # Calculate CPU usage percentage
    total = df['user'] + df['nice'] + df['system'] + df['idle'] + df['iowait'] + df['irq'] + df['softirq']
    df['Value'] = ((df['user'] + df['nice']) / total) * 100

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

    # Calculate NFS throughput in MB/s
    # Using direct read/write columns
    df['Value'] = (df['direct_read'] + df['direct_write']) / 1024 / 1024  # Convert to MB/s

    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    result = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'nfs',
        'Value': df['Value'],
        'Units': 'MB/s',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    return result


def process_node_folder(folder_path):
    """Process a single node folder and return combined data"""
    results = []

    # Process block.csv
    block_file = os.path.join(folder_path, 'block.csv')
    if os.path.exists(block_file):
        results.append(process_block_file(block_file))

    # Process cpu.csv
    cpu_file = os.path.join(folder_path, 'cpu.csv')
    if os.path.exists(cpu_file):
        results.append(process_cpu_file(cpu_file))

    # Process nfs.csv
    nfs_file = os.path.join(folder_path, 'nfs.csv')
    if os.path.exists(nfs_file):
        results.append(process_nfs_file(nfs_file))

    # Combine all results
    if results:
        return pd.concat(results, ignore_index=True)
    return None


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


def save_monthly_data_locally(monthly_data, base_dir):
    """
    Save monthly data to local temporary files
    """
    temp_dir = os.path.join(base_dir, "temp_monthly_data")
    create_directory(temp_dir)

    saved_files = []
    for month, df in monthly_data.items():
        file_path = os.path.join(temp_dir, f"FRESCO_Stampede_ts_{month}.csv")
        df.to_csv(file_path, index=False)
        saved_files.append(file_path)

    return saved_files


def upload_to_s3(file_paths, bucket_name="abc123", max_retries=3):
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


def upload_monthly_data(monthly_data, base_dir, bucket_name="abc123", max_retries=3):
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


def main():
    """
    Main function to orchestrate the entire pipeline
    """
    base_dir = r"transform-data"
    base_url = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/"

    # Initialize tracker and monthly data storage
    tracker = ProcessingTracker(base_dir)
    monthly_data = {}

    start_index = 0
    batch_number = tracker.status['current_batch']

    while True:
        print(f"\nProcessing batch {batch_number}...")

        # Check disk space
        if not check_disk_space():
            print("Insufficient disk space. Saving progress and exiting.")
            if monthly_data:
                upload_monthly_data(monthly_data, base_dir)
            break

        next_index, has_more = scrape_and_download(base_url, base_dir, tracker, start_index)

        if next_index is None:
            print("No more folders to process or error occurred.")
            break

        try:
            # Process the current batch
            node_folders = glob.glob(os.path.join(base_dir, "NODE*"))
            batch_results = []

            for folder in node_folders:
                result = process_node_folder(folder)
                if result is not None:
                    batch_results.append(result)

            if batch_results:
                # Combine batch results
                batch_df = pd.concat(batch_results, ignore_index=True)

                # Split by month and update monthly data
                batch_monthly = split_by_month(batch_df)
                monthly_data = update_monthly_data(monthly_data, batch_monthly)

                # Clean up node folders after processing
                cleanup_files([folder for folder in node_folders])

            # Update progress
            tracker.status['current_batch'] = batch_number
            tracker.save_status()

            if has_more:
                start_index = next_index
                batch_number += 1
            else:
                # All nodes processed, upload final data to S3
                if monthly_data:
                    upload_success = upload_monthly_data(monthly_data, base_dir)
                    if upload_success:
                        print("All data successfully processed and uploaded!")
                    else:
                        print("Error uploading final data to S3")
                print("All folders have been processed!")
                break

        except Exception as e:
            print(f"Error processing batch {batch_number}: {str(e)}")
            # Try to save current progress before exiting
            if monthly_data:
                upload_monthly_data(monthly_data, base_dir)
            break


if __name__ == "__main__":
    main()