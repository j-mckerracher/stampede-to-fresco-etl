import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import os
import time
import data_processor
import polars as pl
import upload_folder_to_s3

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


def handle_monthly_data(monthly_data: Dict[str, pl.DataFrame]) -> None:
    """Process monthly data - append to cached files (no immediate upload)"""
    if not monthly_data:
        logger.warning("No monthly data to handle")
        return {}

    # Get mapping of month to file path for all months
    month_file_map = get_monthly_files_for_data(monthly_data)

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

            else:
                # Create new file
                df.write_csv(file_path)
                logger.info(f"Created new monthly file for month {month}")

        except Exception as e:
            logger.error(f"Error processing monthly data for {month}: {e}")


def process_node(node, node_data_proc, node_list):
    """Process a single node and return success status"""
    try:
        links = node_list[node]["urls"]

        # 1 - download files
        for link in links:
            download_file(link, "cache")

        # 2 - process files
        node_df = node_data_proc.process_node_data()

        # 3 - group results into monthly files
        monthly_data = process_node_data_to_monthly(node_df)
        handle_monthly_data(monthly_data)

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


def process_node_group(start_index: int):
    """Process a specified number of incomplete nodes"""
    start = time.time()
    num_nodes_to_process = 40
    processed_count = 0

    # Create data processor
    node_data_proc = data_processor.NodeDataProcessor()

    # Load the node list
    with open('node-list.json', 'r') as file:
        node_list = json.load(file)

    # Find nodes
    nodes = [node for node, data in node_list.items()]

    # Process the specified number of nodes (or all if num_nodes_to_process is larger)
    nodes_to_process = nodes[start_index:start_index+num_nodes_to_process]

    count = 1
    for node in nodes_to_process:
        print(f"Processing {node}... number {count}/{num_nodes_to_process}")
        node_start = time.time()
        count += 1

        # Process the node and get success status
        success = process_node(node, node_data_proc, node_list)

        # Update the node status in the JSON file
        update_node_status(node_list, node, success)

        if success:
            processed_count += 1

        node_end = time.time()
        print(f"Completed {node} in {node_end - node_start:.2f} seconds (Success: {success})")

    # Upload results to S3 and clear cache directory
    upload_folder_to_s3.main(
        start_index=str(start_index),
        end_index=str(start_index + num_nodes_to_process)
    )

    end = time.time()
    print(f"Processed {processed_count} nodes successfully out of {len(nodes_to_process)} attempted")
    print(f"Total execution time: {end - start:.2f} seconds")


if __name__ == "__main__":
    for index in range(981, 6170, 41):
        process_node_group(index)
