import logging
import time
from datetime import datetime
import shutil
from pathlib import Path
from typing import List, Tuple, Dict, Optional, DefaultDict
import re
from urllib.parse import urljoin
from data_processor import NodeDataProcessor  # Assuming this exists and works as before
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import polars as pl
from collections import defaultdict  # Use defaultdict for easier aggregation

# --- Configuration ---
BASE_URL = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/"
REQUIRED_FILES = ['block.csv', 'cpu.csv', 'llite.csv', 'mem.csv']
TEMP_BASE_DIR = Path("./temp_stampede_downloads")
FINAL_OUTPUT_DIR = Path("/home/dynamo/a/jmckerra/projects/stampede-step-1/output")
NODE_DIR_PATTERN = re.compile(r'^(NODE\d+)/$')
NETWORK_RETRIES = 3
NETWORK_WAIT_SECONDS = 3
REQUEST_TIMEOUT = 60
WRITE_BATCH_SIZE = 500  # Number of NODES to process before writing
# --- End Configuration ---


# --- Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('stampede_etl_batched_daily_agg.log')  # Log file name change
    ]
)
logger = logging.getLogger(__name__)


# --- Helper Functions (fetch_html, get_node_urls, get_required_file_urls, download_file) ---
# --- These functions remain the same as in the previous script ---
@retry(
    stop=stop_after_attempt(NETWORK_RETRIES),
    wait=wait_fixed(NETWORK_WAIT_SECONDS),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def fetch_html(url: str):
    """Fetches HTML content from a URL with retries."""
    logger.debug(f"Fetching HTML from: {url}")
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        logger.debug(f"Successfully fetched HTML from: {url}")
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {url} after retries: {e}")
        raise


def get_node_urls(base_url: str) -> List[Tuple[str, str]]:
    """Scrapes the base URL to find node directories and their URLs."""
    logger.info(f"Discovering node directories at: {base_url}")
    node_list = []
    try:
        html_content = fetch_html(base_url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'lxml')
        links = soup.find_all('a', href=True)

        for link in links:
            match = NODE_DIR_PATTERN.match(link.text)
            if match:
                node_name = match.group(1)
                node_url = urljoin(base_url, link['href'])
                if not node_url.endswith('/'):
                    node_url += '/'
                node_list.append((node_name, node_url))
                logger.debug(f"Found node: {node_name} -> {node_url}")

        logger.info(f"Discovered {len(node_list)} potential node directories.")
        # Sort nodes naturally if needed (e.g., NODE1, NODE2, NODE10)
        node_list.sort(key=lambda x: int(re.search(r'\d+', x[0]).group()))

    except Exception as e:
        logger.error(f"Failed to discover nodes from {base_url}: {e}", exc_info=True)
        return []

    return node_list


def get_required_file_urls(node_url: str, required_files: List[str]) -> Optional[Dict[str, str]]:
    """Scrapes a node's URL to find download links for specific required files."""
    logger.debug(f"Getting required file URLs from: {node_url}")
    file_urls = {}
    try:
        html_content = fetch_html(node_url)
        if not html_content:
            return None

        soup = BeautifulSoup(html_content, 'lxml')
        links = soup.find_all('a', href=True)
        found_files = set()

        for link in links:
            filename = link.text.strip()
            if filename in required_files:
                file_url = urljoin(node_url, link['href'])
                file_urls[filename] = file_url
                found_files.add(filename)
                logger.debug(f"Found required file link: {filename} -> {file_url}")

        missing_files = set(required_files) - found_files
        if missing_files:
            logger.warning(f"Missing required files for {node_url}: {missing_files}")
            # logger.error(f"Cannot proceed with node {node_url}, required files missing.") # Soft warning now
            return None  # Or return partial dict if partial processing is allowed

        logger.debug(f"Found required file URLs for {node_url}")
        return file_urls

    except Exception as e:
        logger.error(f"Failed to get file URLs from {node_url}: {e}", exc_info=True)
        return None


@retry(
    stop=stop_after_attempt(NETWORK_RETRIES),
    wait=wait_fixed(NETWORK_WAIT_SECONDS),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def download_file(url: str, destination_path: Path) -> bool:
    """Downloads a file to a specific destination path with retries."""
    logger.debug(f"Downloading {url} to {destination_path}")
    try:
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        with open(destination_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        if destination_path.exists() and destination_path.stat().st_size > 0:
            logger.debug(f"Successfully downloaded {destination_path.name}")
            return True
        else:
            logger.warning(f"Download completed but destination file is missing or empty: {destination_path}")
            if destination_path.exists():
                try:
                    destination_path.unlink()
                except OSError:
                    pass
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed for {url}: {e}")
        if destination_path.exists():
            try:
                destination_path.unlink()
            except OSError:
                pass
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during download to {destination_path}: {e}")
        if destination_path.exists():
            try:
                destination_path.unlink()
            except OSError:
                pass
        return False


# --- Node Processing Function (Remains largely the same internally) ---
def process_single_node(node_name: str, node_url: str, processor: NodeDataProcessor, temp_base_dir: Path) -> Optional[
    pl.DataFrame]:
    """Downloads required files, processes data, cleans up temp files, and returns the DataFrame for the NODE."""
    node_start_time = time.time()
    logger.info(f"--- Starting processing for node: {node_name} ---")
    node_temp_dir = temp_base_dir / node_name
    node_temp_dir.mkdir(parents=True, exist_ok=True)
    processed_df = None  # Initialize return value

    try:
        # 1. Get file URLs
        required_urls = get_required_file_urls(node_url, REQUIRED_FILES)
        if not required_urls:
            # If only *some* files are missing, we might proceed if NodeDataProcessor handles it
            # For now, assume all are required as per original logic
            if len(required_urls or {}) != len(REQUIRED_FILES):
                logger.error(f"Could not get all required file URLs for {node_name}. Skipping node.")
                return None

        # 2. Download required files
        download_success_all = True
        for filename, url in required_urls.items():
            local_path = node_temp_dir / filename
            if not download_file(url, local_path):
                logger.error(f"Failed to download {filename} for {node_name}. Aborting node processing.")
                download_success_all = False
                break

        if not download_success_all:
            return None

        # 3. Process the downloaded files
        logger.info(f"Processing downloaded data for {node_name} in {node_temp_dir}")
        processor.cache_dir = node_temp_dir  # Point processor to the right temp dir
        node_df = processor.process_node_data()  # Assumes this reads from cache_dir

        if node_df is None or node_df.height == 0:
            logger.warning(f"Processing completed for {node_name}, but no data was returned/generated.")
            processed_df = None  # Explicitly None if no data
        else:
            # --- Ensure Timestamp column exists and is correct type ---
            if 'Timestamp' not in node_df.columns:
                logger.error(
                    f"CRITICAL: 'Timestamp' column missing in data from {node_name} processor. Cannot proceed with this node.")
                processed_df = None  # Mark as failed for aggregation step
            # Optional: Add more robust type checking/conversion if needed
            elif node_df.height > 0 and not isinstance(node_df['Timestamp'][0], datetime):
                logger.warning(
                    f"Timestamp column for {node_name} is not datetime type ({type(node_df['Timestamp'][0])}). Attempting Polars conversion.")
                try:
                    # Example: Assuming it might be string parseable by Polars
                    node_df = node_df.with_columns(pl.col('Timestamp').cast(pl.Datetime))  # Or strptime if format known
                except Exception as e:
                    logger.error(f"Failed to convert Timestamp column for {node_name}: {e}. Skipping node data.",
                                 exc_info=True)
                    processed_df = None
                else:
                    logger.info(f"Successfully processed data for {node_name} ({node_df.height} rows).")
                    processed_df = node_df
            else:
                logger.info(f"Successfully processed data for {node_name} ({node_df.height} rows).")
                processed_df = node_df

    except Exception as e:
        logger.error(f"An unexpected error occurred during processing cycle for node {node_name}: {e}", exc_info=True)
        processed_df = None  # Ensure None is returned on error
    finally:
        # 4. Clean up temporary directory for this node
        if node_temp_dir.exists():
            try:
                logger.debug(f"Cleaning up temporary directory: {node_temp_dir}")
                shutil.rmtree(node_temp_dir)
            except OSError as e:
                logger.error(f"Failed to clean up temporary directory {node_temp_dir}: {e}")

    node_end_time = time.time()
    status_str = "SUCCESS (data generated)" if processed_df is not None and processed_df.height > 0 else \
        "SUCCESS (no data/Timestamp error)" if processed_df is None else \
            "FAILURE"  # This needs refinement based on processed_df logic
    logger.info(
        f"--- Finished processing node: {node_name} in {node_end_time - node_start_time:.2f}s [{status_str}] ---")

    return processed_df


# --- New Function for Writing Batch ---
def write_daily_batch_data(
        daily_data_aggregator: Dict[str, List[pl.DataFrame]],
        batch_number: int,
        output_dir: Path
) -> bool:
    """
    Concatenates and writes aggregated daily data for the current batch.

    Args:
        daily_data_aggregator: Dictionary mapping date strings ('YYYY-MM-DD')
                               to lists of DataFrames for that date within the batch.
        batch_number: The sequential number of the batch being written.
        output_dir: The directory to write the Parquet files to.

    Returns:
        True if all daily files for the batch were written successfully, False otherwise.
    """
    batch_start_time = time.time()
    logger.info(f"--- Writing Batch {batch_number} (Aggregated Daily Data) ---")
    if not daily_data_aggregator:
        logger.warning(f"Batch {batch_number} has no aggregated data to write. Skipping.")
        return True  # Not an error, just nothing to do

    num_days_in_batch = len(daily_data_aggregator)
    logger.info(f"Batch {batch_number}: Found data for {num_days_in_batch} unique day(s). Saving files to {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)  # Ensure dir exists

    all_days_saved = True
    days_processed_count = 0

    # Iterate through each day aggregated in this batch
    for date_str, list_of_daily_dfs in daily_data_aggregator.items():
        days_processed_count += 1
        logger.info(f"Processing day {days_processed_count}/{num_days_in_batch}: {date_str} for batch {batch_number}")

        if not list_of_daily_dfs:
            logger.warning(
                f"No DataFrames found for date {date_str} in batch {batch_number}, skipping write for this day.")
            continue

        output_filename = f"batch_{batch_number}_{date_str}.parquet"
        output_file = output_dir / output_filename

        try:
            # Concatenate all DataFrames collected for this specific day within the batch
            logger.debug(
                f"Concatenating {len(list_of_daily_dfs)} DataFrame(s) for day {date_str}, batch {batch_number}...")
            combined_daily_df = pl.concat(list_of_daily_dfs, how='vertical')
            logger.debug(f"Concatenated DataFrame for {date_str} has {combined_daily_df.height} rows.")

            if combined_daily_df.height == 0:
                logger.warning(f"Combined DataFrame for day {date_str}, batch {batch_number} is empty. Skipping write.")
                continue

            # Write the combined data for this day and batch
            logger.debug(
                f"Saving data for batch {batch_number}, date {date_str} ({combined_daily_df.height} rows) to {output_file}")
            combined_daily_df.write_parquet(output_file, compression='zstd')
            logger.debug(f"Successfully saved {output_filename}")

        except Exception as write_e:
            logger.error(
                f"Failed to concatenate or save Parquet file {output_file} for batch {batch_number}, date {date_str}: {write_e}",
                exc_info=True)
            all_days_saved = False
            # Decide on error strategy: continue with other days or stop?
            # Let's continue to save other days but report failure at the end.

    batch_end_time = time.time()
    status_str = "SUCCESS" if all_days_saved else "FAILURE"
    logger.info(
        f"--- Finished writing batch {batch_number} in {batch_end_time - batch_start_time:.2f}s [{status_str}] ---")
    return all_days_saved


# --- Main Function (Modified for Daily Aggregation) ---
def main():
    """
    Main function: Discovers nodes, processes each, splits/aggregates data by day in memory,
    and writes daily files in batches based on node count.
    """
    overall_start_time = time.time()
    logger.info("=== Starting Stampede Data ETL Process (Batched Daily Aggregation) ===")

    # Ensure output and temp directories exist
    try:
        FINAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured final output directory exists: {FINAL_OUTPUT_DIR}")
        TEMP_BASE_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured temporary base directory exists: {TEMP_BASE_DIR}")
    except OSError as e:
        logger.critical(f"Cannot create essential directories: {e}. Exiting.")
        return

    # Instantiate the processor
    node_data_processor = NodeDataProcessor(cache_dir=str(TEMP_BASE_DIR))  # cache_dir set per node later

    # Discover nodes
    nodes_to_process = get_node_urls(BASE_URL)
    total_nodes = len(nodes_to_process)
    if total_nodes == 0:
        logger.warning("No nodes discovered. Exiting.")
        return

    logger.info(f"Found {total_nodes} nodes to process. Writing output every {WRITE_BATCH_SIZE} nodes.")

    # --- Batch Management and In-Memory Aggregation ---
    success_nodes_processed = 0  # Nodes processed without raising unhandled exceptions
    failed_nodes = 0  # Nodes that failed download or critical processing step
    nodes_processed_in_batch = 0
    current_batch_number = 1
    # Use defaultdict for convenience: auto-creates list for new dates
    daily_data_aggregator: DefaultDict[str, List[pl.DataFrame]] = defaultdict(list)
    # --- End Batch Management ---

    for i, (node_name, node_url) in enumerate(nodes_to_process):
        logger.info(f"--- Queueing node {i + 1} / {total_nodes}: {node_name} (Batch {current_batch_number}) ---")
        node_data = None
        try:
            # 1. Process node - returns single DF for the node or None
            node_data = process_single_node(node_name, node_url, node_data_processor, TEMP_BASE_DIR)
            success_nodes_processed += 1  # Count attempt as success if no exception bubbled up

            # 2. Split node data by day and add to aggregator (if valid data exists)
            if node_data is not None and node_data.height > 0:
                logger.debug(f"Node {node_name} returned {node_data.height} rows. Splitting by day...")
                try:
                    # Add temporary date string column
                    node_data_with_date = node_data.with_columns(
                        pl.col('Timestamp').dt.strftime('%Y-%m-%d').alias('date_str')
                    )
                    # Group by the date string ( Polars >= 0.19.12 allows direct iteration )
                    # For older versions, might need .agg() first, then join back - but partition_by is better
                    # Using partition_by for efficiency if Polars version supports it well for this
                    daily_partitions = node_data_with_date.partition_by('date_str', as_dict=True,
                                                                        include_key=False)  # include_key=False drops 'date_str'

                    for date_key, daily_df in daily_partitions.items():
                        if daily_df is not None and daily_df.height > 0:
                            date_key_str = str(date_key)  # Ensure string
                            logger.debug(
                                f"Adding {daily_df.height} rows for date {date_key_str} from node {node_name} to aggregator.")
                            # Append the DataFrame (already without 'date_str') to the list for that date
                            daily_data_aggregator[date_key_str].append(daily_df)
                        else:
                            logger.warning(f"Empty partition for date {date_key} from node {node_name}.")

                    logger.debug(f"Finished aggregating data for node {node_name} by day.")

                except pl.ColumnNotFoundError:
                    logger.error(f"Timestamp column not found during split phase for {node_name}, data skipped.")
                    failed_nodes += 1  # Count this as a node failure post-processing
                except Exception as split_agg_e:
                    logger.error(f"Error splitting/aggregating data for node {node_name}: {split_agg_e}", exc_info=True)
                    failed_nodes += 1  # Count as failure if splitting/aggregation fails
            else:
                logger.info(f"No valid data returned from node {node_name} to aggregate.")
                # Note: success_nodes_processed was already incremented

        except Exception as e:
            # Catch errors raised from process_single_node if any escape its internal try/except
            logger.error(f"Unhandled exception during main loop processing node {node_name}: {e}", exc_info=True)
            failed_nodes += 1
            # Need to decide if failed node increments nodes_processed_in_batch
            # Let's increment it to ensure batches trigger correctly even with failures
            # success_nodes_processed was NOT incremented here

        # Increment node counter for batch logic regardless of data presence/success
        nodes_processed_in_batch += 1

        # Check if batch is full and needs writing
        if nodes_processed_in_batch >= WRITE_BATCH_SIZE:
            logger.info(
                f"Node batch size ({WRITE_BATCH_SIZE}) reached. Writing aggregated daily data for batch {current_batch_number}...")
            if not write_daily_batch_data(daily_data_aggregator, current_batch_number, FINAL_OUTPUT_DIR):
                logger.error(f"Failed to write batch {current_batch_number}. Data for this batch might be incomplete.")
                # Consider strategy: stop? log and continue? retry later?
                # Current: Log and continue.

            # Reset for next batch
            logger.info(f"Clearing daily aggregator for next batch.")
            daily_data_aggregator.clear()  # Clear the dictionary
            nodes_processed_in_batch = 0
            current_batch_number += 1

        # Optional: Small delay between nodes
        # time.sleep(0.1)

    # After the loop, write any remaining aggregated data in the last batch
    if daily_data_aggregator:
        logger.info(
            f"Writing final remaining batch {current_batch_number} ({nodes_processed_in_batch} nodes processed in this partial batch)...")
        if not write_daily_batch_data(daily_data_aggregator, current_batch_number, FINAL_OUTPUT_DIR):
            logger.error(f"Failed to write final batch {current_batch_number}. Data may be lost.")

    # Final Summary
    overall_end_time = time.time()
    logger.info("=== Stampede Data ETL Process Finished ===")
    logger.info(f"Total nodes discovered: {total_nodes}")
    logger.info(f"Nodes processed (attempted): {success_nodes_processed}")
    logger.info(f"Nodes failed (download/critical processing/aggregation error): {failed_nodes}")
    # Note: total_nodes might not equal success + failed if some nodes were skipped early (e.g., missing URLs) without error
    logger.info(f"Total execution time: {overall_end_time - overall_start_time:.2f} seconds")
    logger.info(f"Processed data batches saved to: {FINAL_OUTPUT_DIR}")
    logger.info(f"Batch size (trigger): {WRITE_BATCH_SIZE} nodes per write cycle.")


if __name__ == "__main__":
    main()
