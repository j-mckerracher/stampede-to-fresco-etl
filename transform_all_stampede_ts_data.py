import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import platform
import shutil
import time
import data_processor
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

# Initialize the S3 client
s3_client = boto3.client(
    's3',

    region_name='us-east-1'
)


@retry(
    stop=stop_after_attempt(3),  # Try up to 3 times
    wait=wait_fixed(2),  # Wait 2 seconds between attempts
    retry=retry_if_exception_type((BotoCoreError, ClientError))
)
def upload_file_to_s3(file_path: str, bucket_name: str) -> None:
    """
    Uploads a single file to an S3 bucket with the specified S3 key.

    Args:
        file_path (str): Local path to the file.
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): The key (path) under which to store the file in S3.

    Raises:
        BotoCoreError, ClientError: If the upload fails due to a boto3 error.
    """
    # Add content type for CSV files
    extra_args = {
        'ContentType': 'text/csv'
    }
    logger.info(f"Uploading {file_path} to {bucket_name}/{file_path}")
    s3_client.upload_file(file_path, bucket_name, ExtraArgs=extra_args)


def upload_folder_to_s3(folder_path: str, bucket_name: str, s3_folder: str = "") -> None:
    """
    Uploads all files in the given folder (and its subfolders) to the specified S3 bucket.

    Args:
        folder_path (str): The local directory containing the files to upload.
        bucket_name (str): The target S3 bucket name.
        s3_folder (str, optional): The S3 folder (prefix) to upload files into. Defaults to '' (bucket root).
    """
    logger.info(f"Uploading {os.listdir(folder_path)} to {bucket_name}")
    for filename in os.listdir(folder_path):
        upload_file_to_s3(filename, bucket_name)


@retry(
    stop=stop_after_attempt(3),  # Try up to 3 times
    wait=wait_fixed(2),  # Wait 2 seconds between attempts
    retry=retry_if_exception_type(requests.exceptions.RequestException)  # Retry on network-related errors
)
def download_file(url: str, cache_dir: str) -> str:
    """
    Downloads a file from the specified URL and saves it to the cache directory.

    Args:
        url (str): URL of the file to download.
        cache_dir (str): Directory to save the downloaded file.

    Returns:
        str: Path to the downloaded file.

    Raises:
        requests.exceptions.RequestException: If a network error occurs.
        Exception: For other exceptions that may occur during file writing.
    """
    # Extract filename from URL
    filename = os.path.basename(url)

    # Create the full destination path
    destination = os.path.join(cache_dir, filename)

    # Ensure the cache directory exists
    os.makedirs(cache_dir, exist_ok=True)

    logger.info(f"Downloading {url} to {destination}")

    # Download the file
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code

    # Open the destination file in write-binary mode and write the content in chunks.
    with open(destination, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                file.write(chunk)

    return destination


def free_disk_space_available(free_space_threshold=0.8):
    """
    Check the operating system and available disk space.
    Returns True if free space is greater than the threshold percentage of total space, otherwise False.

    Parameters:
    free_space_threshold (float): Threshold as a decimal (default 0.8 for 80%)

    Returns:
    bool: True if free space > threshold, False otherwise
    """
    # Get operating system information
    operating_system = platform.system()

    # Initialize variables for disk space
    total = 0
    free = 0

    # Check disk space based on OS
    if operating_system == "Windows":
        # For Windows, check C: drive
        total, used, free = shutil.disk_usage("C:\\")

    elif operating_system == "Linux":
        # For Linux, get home directory space
        home_dir = os.path.expanduser("~")
        total, used, free = shutil.disk_usage(home_dir)

    elif operating_system == "Darwin":  # macOS
        # For macOS
        home_dir = os.path.expanduser("~")
        total, used, free = shutil.disk_usage("/")

    else:
        # Unsupported OS, return False
        return False

    # Calculate free space percentage
    free_space_percentage = free / total

    # Return True if free space threshold exceeds free space percentage
    return free_space_threshold > free_space_percentage


def process_node_data_to_monthly(node_data: pl.DataFrame) -> Dict[str, pl.DataFrame]:
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


def get_monthly_file_path(timestamp: datetime) -> Path:
    """Get the file path for a timestamp with versioning"""
    month_key = timestamp.strftime('%Y-%m')

    return Path(f"cache/node_data_{month_key}.csv")


def get_monthly_files_for_data(monthly_data: Dict[str, pl.DataFrame]) -> Dict[str, Path]:
    """Get mapping of month to file path for all months in the data"""
    month_file_map = {}

    for month in monthly_data.keys():
        try:
            month_date = datetime.strptime(month, '%Y-%m')
            file_path = get_monthly_file_path(month_date)
            month_file_map[month] = file_path
        except Exception as e:
            logger.error(f"Error getting file path for month {month}: {e}")

    return month_file_map


def handle_monthly_data(monthly_data: Dict[str, pl.DataFrame]) -> Dict[str, Path]:
    """Process monthly data - append to cached files (no immediate upload)"""
    if not monthly_data:
        logger.warning("No monthly data to handle")
        return {}

    # Get mapping of month to file path for all months
    month_file_map = get_monthly_files_for_data(monthly_data)

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

            # Add to list of processed files
            processed_files[month] = file_path

        except Exception as e:
            logger.error(f"Error processing monthly data for {month}: {e}")

    return processed_files


def process_node(node, node_data_proc, node_list):
    """Process a single node and return success status"""
    try:
        links = node_list[node]["urls"]

        # 1 - check disk space and upload to s3 if no space left
        if not free_disk_space_available():
            upload_file_to_s3("cache", "data-transform-stampede")

        # 2 - download files
        for link in links:
            download_file(link, "cache")

        # 3 - process files
        node_df = node_data_proc.process_node_data()

        # 4 - group results into monthly files
        monthly_data = process_node_data_to_monthly(node_df)
        processed_files = handle_monthly_data(monthly_data)

        return True  # Success
    except Exception as e:
        logging.error(f"Error processing {node}: {str(e)}")
        return False  # Failure


def update_node_status(node_list, node, status):
    """Update the Complete status for a node and save to the JSON file"""
    node_list[node]["Complete"] = status

    # Save the updated node list back to the file
    with open('node-list.json', 'w') as file:
        json.dump(node_list, file, indent=4)


def main(num_nodes_to_process):
    """Process a specified number of incomplete nodes"""
    start = time.time()
    processed_count = 0

    # Create data processor
    node_data_proc = data_processor.NodeDataProcessor()

    # Load the node list
    with open('node-list.json', 'r') as file:
        node_list = json.load(file)

    # Find incomplete nodes
    incomplete_nodes = [node for node, data in node_list.items()
                        if not data.get("Complete", False)]

    print(f"Found {len(incomplete_nodes)} incomplete nodes")

    # Process the specified number of nodes (or all if num_nodes_to_process is larger)
    nodes_to_process = incomplete_nodes[:num_nodes_to_process]

    for node in nodes_to_process:
        print(f"Processing {node}...")
        node_start = time.time()

        # Process the node and get success status
        success = process_node(node, node_data_proc, node_list)

        # Update the node status in the JSON file
        update_node_status(node_list, node, success)

        if success:
            processed_count += 1

        node_end = time.time()
        print(f"Completed {node} in {node_end - node_start:.2f} seconds (Success: {success})")

    end = time.time()
    print(f"Processed {processed_count} nodes successfully out of {len(nodes_to_process)} attempted")
    print(f"Total execution time: {end - start:.2f} seconds")


if __name__ == "__main__":
    main(31)
