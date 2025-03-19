import os
import logging
import json
import polars as pl
from pathlib import Path
import shutil
import psutil
import time
import uuid
import concurrent.futures
import queue
import argparse
import pyarrow.parquet as pq
import pyarrow as pa

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Modified paths for the distributed approach
source_dir = r"/home/dynamo/a/jmckerra/projects/stampede-to-fresco-etl/cache/input"
complete_dir = r"/home/dynamo/a/jmckerra/projects/stampede-to-fresco-etl/cache/complete"
cache_base_dir = r"/home/dynamo/a/jmckerra/projects/stampede-to-fresco-etl/cache"

# Resource management configurations
MAX_MEMORY_PERCENT = 70  # Cap memory usage at 70%

# Parallel processing configuration
DEFAULT_NUM_WORKER_THREADS = 8  # Default number of threads

# Status file to track process state
STATUS_FILE = os.path.join(cache_base_dir, "processing_status.json")

# Global variable for worker threads (will be set in main)
NUM_WORKER_THREADS = DEFAULT_NUM_WORKER_THREADS


def save_status(job_id, status, details=None):
    """Save processing status to a file that can be read by the laptop script"""
    status_path = os.path.join(complete_dir, f"{job_id}.status")

    status_data = {
        "job_id": job_id,
        "status": status,
        "timestamp": time.time(),
        "details": details or {}
    }

    with open(status_path, 'w') as f:
        json.dump(status_data, f)

    logger.info(f"Status update for job {job_id}: {status}")


def list_source_files():
    """List all job files in the source directory"""
    manifest_files = []
    data_files = []

    # First look for manifest files
    for root, _, filenames in os.walk(source_dir):
        for filename in filenames:
            full_path = os.path.join(root, filename)
            if filename.endswith('.manifest.json'):
                manifest_files.append(full_path)
            elif filename.endswith('.parquet'):
                data_files.append(full_path)

    return manifest_files, data_files


def load_job_from_manifest(manifest_path):
    """Load job information from manifest file"""
    try:
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

        job_id = manifest.get('job_id')
        year_month = manifest.get('year_month')
        files = manifest.get('files', [])
        complete_month = manifest.get('complete_month', False)

        logger.info(f"Loaded job {job_id} for {year_month} with {len(files)} files, complete_month: {complete_month}")
        return job_id, year_month, files, complete_month
    except Exception as e:
        logger.error(f"Error loading manifest {manifest_path}: {str(e)}")
        return None, None, [], False


def process_parquet_row_groups(file_path, year_month, cache_dir, thread_id, start_row_group, end_row_group,
                               result_queue):
    """Process a range of row groups from a Parquet file"""
    try:
        logger.debug(f"Thread {thread_id} processing row groups {start_row_group} to {end_row_group - 1}")

        # Open the Parquet file
        parquet_file = pq.ParquetFile(file_path)

        # Read the specified row groups
        tables = []
        for row_group_idx in range(start_row_group, end_row_group):
            table = parquet_file.read_row_group(row_group_idx)
            tables.append(table)

        # Combine all tables
        if not tables:
            logger.warning(f"Thread {thread_id} found no row groups to process")
            result_queue.put((thread_id, {}))
            return

        combined_table = pa.concat_tables(tables)

        # Convert to Polars DataFrame
        df = pl.from_arrow(combined_table)

        # Process the dataframe
        df_days = move_data_into_day_dataframe(df)
        del df  # Release memory

        # Return the day dataframes through the queue
        result_queue.put((thread_id, df_days))
        logger.debug(f"Thread {thread_id} completed processing row groups")

    except Exception as e:
        logger.error(f"Error processing row groups in thread {thread_id}: {str(e)}")
        result_queue.put((thread_id, {}))


def move_data_into_day_dataframe(df):
    """
    Group data by day of the month based on Timestamp column.
    """
    # Check if the Timestamp column exists
    if "Timestamp" not in df.columns:
        logger.error("Timestamp column not found in dataframe")
        return {}

    # Convert timestamp to datetime and extract day
    try:
        df = df.with_columns(
            pl.col("Timestamp").str.strptime(pl.Datetime).dt.date().alias("Date")
        )
    except Exception as e:
        logger.error(f"Error converting timestamp: {str(e)}")
        # Try a different timestamp format if needed
        try:
            df = df.with_columns(
                pl.col("Timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").dt.date().alias("Date")
            )
        except Exception as e2:
            logger.error(f"Second attempt at converting timestamp failed: {str(e2)}")
            return {}

    # Create a dictionary to store dataframes by day
    day_dataframes = {}

    # Group by day
    for date in df["Date"].unique():
        day = date.day
        day_df = df.filter(pl.col("Date") == date)
        # Remove the temporary Date column
        day_df = day_df.drop("Date")
        day_dataframes[day] = day_df

    return day_dataframes


def process_downloaded_file_parallel(file_path, year_month, cache_dir, num_threads):
    """Process a single file using multiple threads based on Parquet row groups"""
    processed_files = set()

    try:
        # Log start of processing
        logger.info(f"Processing {file_path} with {num_threads} threads")

        # Get row group information
        parquet_file = pq.ParquetFile(file_path)
        num_row_groups = parquet_file.num_row_groups

        logger.info(f"File has {num_row_groups} row groups")

        # Determine assignment of row groups to threads
        actual_threads = min(num_threads, num_row_groups)
        if actual_threads < num_threads:
            logger.info(f"Using only {actual_threads} threads because there are only {num_row_groups} row groups")

        row_groups_per_thread = max(1, num_row_groups // actual_threads)

        # Create a thread-safe queue for results
        result_queue = queue.Queue()

        # Use ThreadPoolExecutor to manage threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=actual_threads) as executor:
            # Submit tasks to the executor
            futures = []

            for thread_id in range(actual_threads):
                # Calculate row group range for this thread
                start_row_group = thread_id * row_groups_per_thread
                end_row_group = min((thread_id + 1) * row_groups_per_thread, num_row_groups)

                if start_row_group >= num_row_groups:
                    # No more row groups to process
                    break

                future = executor.submit(
                    process_parquet_row_groups,
                    file_path,
                    year_month,
                    cache_dir,
                    thread_id,
                    start_row_group,
                    end_row_group,
                    result_queue
                )
                futures.append(future)

            # Wait for all tasks to complete
            concurrent.futures.wait(futures)

        # Process all results from the queue
        daily_dataframes = {}
        while not result_queue.empty():
            thread_id, chunk_results = result_queue.get()

            # Merge the chunk results with the overall results
            for day, day_df in chunk_results.items():
                if day in daily_dataframes:
                    # Concatenate dataframes for the same day
                    daily_dataframes[day] = pl.concat([daily_dataframes[day], day_df])
                else:
                    daily_dataframes[day] = day_df

        # Write the combined daily dataframes to files
        for day, day_df in daily_dataframes.items():
            # Write the day's data to a parquet file
            daily_file = append_to_daily_parquet(day_df, day, year_month, cache_dir)
            processed_files.add(daily_file)

        return processed_files
    except Exception as e:
        logger.error(f"Error in parallel processing of file {file_path}: {str(e)}")
        return set()


def append_to_daily_parquet(day_df, day, year_month, cache_dir):
    """
    Append data to a daily Parquet file, creating if it doesn't exist.
    Handles deduplication using a safer approach by working with Parquet.
    """
    # Format day as two digits
    day_str = f"{day:02d}"

    # Define output file path for Parquet
    parquet_file = cache_dir / f"perf_metrics_{year_month}-{day_str}.parquet"

    # Generate a unique temporary file path
    temp_parquet_file = cache_dir / f"temp_{uuid.uuid4().hex}_{year_month}-{day_str}.parquet"

    try:
        # Write day_df to temporary Parquet file
        day_df.write_parquet(temp_parquet_file)

        # If the output Parquet file already exists, we need to merge and deduplicate
        if parquet_file.exists():
            try:
                # Create a new file that will contain the merged data
                merged_parquet_file = cache_dir / f"merged_{uuid.uuid4().hex}_{year_month}-{day_str}.parquet"

                # Read both Parquet files into DataFrames
                existing_df = pl.read_parquet(parquet_file)
                new_df = pl.read_parquet(temp_parquet_file)

                # Combine and deduplicate
                merged_df = pl.concat([existing_df, new_df]).unique()

                # Write to a new file
                merged_df.write_parquet(merged_parquet_file)

                # Replace the old file with the merged file
                os.replace(merged_parquet_file, parquet_file)

                # Cleanup the temporary file
                os.remove(temp_parquet_file)

                logger.debug(f"Updated file {parquet_file} with {len(merged_df)} unique rows")
            except Exception as e:
                logger.error(f"Error merging Parquet files: {str(e)}")
                # If merging fails, just use the new data (don't lose it)
                os.replace(temp_parquet_file, parquet_file)
        else:
            # No existing file, just rename the temp file
            os.rename(temp_parquet_file, parquet_file)
            logger.debug(f"Created new file {parquet_file} with {len(day_df)} rows")

    except Exception as e:
        logger.error(f"Error in append_to_daily_parquet: {str(e)}")
        # Cleanup any temporary files
        if os.path.exists(temp_parquet_file):
            os.remove(temp_parquet_file)

    return parquet_file


def process_file(file_path, year_month, cache_dir):
    """Process a single file with parallel processing"""
    try:
        # Process file using parallel processing
        logger.debug(f"Processing {file_path} with {NUM_WORKER_THREADS} threads")
        daily_files = process_downloaded_file_parallel(file_path, year_month, cache_dir, NUM_WORKER_THREADS)

        return daily_files
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return set()


def copy_file_to_complete_dir(file_path, job_id):
    """
    Copies a processed Parquet file to the complete directory.
    """
    # Create a job-specific directory in the complete dir
    job_complete_dir = os.path.join(complete_dir, job_id)
    os.makedirs(job_complete_dir, exist_ok=True)

    # Get the file name
    file_name = os.path.basename(file_path)

    # Ensure it has a .parquet extension
    if not file_name.endswith('.parquet'):
        file_name = file_name + '.parquet'

    dest_path = os.path.join(job_complete_dir, file_name)

    try:
        # Copy the Parquet file
        shutil.copy2(file_path, dest_path)
        logger.debug(f"Copied {file_path} to {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Error copying {file_path} to {dest_path}: {str(e)}")
        return False


def process_job(job_id, year_month, files):
    """Process all files for a given job"""
    # Create cache directory for this job
    cache_dir = Path(f"{cache_base_dir}/{job_id}/{year_month}")
    cache_dir.mkdir(exist_ok=True, parents=True)

    # Set for collecting daily files
    daily_files_dict = {}  # Use a dict to track unique files

    # Check if memory usage is too high before processing
    def memory_ok():
        memory_percent = psutil.virtual_memory().percent
        return memory_percent < MAX_MEMORY_PERCENT

    # Process files sequentially to avoid corruption
    logger.info(f"Processing {len(files)} files for job {job_id} (year-month: {year_month})")

    # Update status to processing
    save_status(job_id, "processing", {"total_files": len(files), "processed_files": 0})

    # Process files in smaller batches to control memory usage
    batch_size = 5  # Small batch size for better control
    processed_count = 0

    for i in range(0, len(files), batch_size):
        # Wait if memory usage is too high
        while not memory_ok():
            logger.warning(f"Memory usage high ({psutil.virtual_memory().percent}%). Waiting...")
            time.sleep(5)

        batch = files[i:i + batch_size]
        batch_paths = [os.path.join(source_dir, f) for f in batch if os.path.exists(os.path.join(source_dir, f))]

        # Process this batch sequentially
        for file_path in batch_paths:
            try:
                # Process file with parallel threads
                new_daily_files = process_file(file_path, year_month, cache_dir)

                for daily_file in new_daily_files:
                    daily_files_dict[str(daily_file)] = daily_file

                # Delete the processed input file
                try:
                    os.remove(file_path)
                    logger.debug(f"Deleted processed input file: {file_path}")
                except Exception as e:
                    logger.error(f"Error deleting input file {file_path}: {str(e)}")

                processed_count += 1

                # Update status periodically
                if processed_count % 10 == 0 or processed_count == len(files):
                    save_status(job_id, "processing", {
                        "total_files": len(files),
                        "processed_files": processed_count,
                        "percent_complete": round((processed_count / len(files)) * 100, 2)
                    })
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")

    # Convert to list for progress tracking
    daily_files_list = list(daily_files_dict.values())

    # Copy all daily files to the complete directory
    logger.info(f"Copying {len(daily_files_list)} Parquet files to complete directory")
    copied_files = []

    for file_path in daily_files_list:
        if copy_file_to_complete_dir(str(file_path), job_id):
            copied_files.append(file_path)

    # Create a manifest of completed files
    manifest_data = {
        "job_id": job_id,
        "year_month": year_month,
        "total_files_processed": processed_count,
        "days_processed": len(daily_files_list),
        "files_copied": len(copied_files),
        "complete_month": True,  # Indicate this was a complete month job
        "timestamp": time.time()
    }

    # Write the manifest to the complete directory
    manifest_path = os.path.join(complete_dir, f"{job_id}.manifest.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest_data, f)

    # Update final status
    save_status(job_id, "completed", {
        "year_month": year_month,
        "total_files_processed": processed_count,
        "days_processed": len(daily_files_list),
        "files_copied": len(copied_files),
        "complete_month": True,  # Indicate this was a complete month job
        "ready_for_final_destination": True  # Signal ETL manager to move data
    })

    # Remove daily files after copying
    logger.info("Cleaning up daily files")
    for file_path in daily_files_list:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger.error(f"Error removing cache file {file_path}: {str(e)}")

    # Remove the job cache directory
    try:
        shutil.rmtree(os.path.join(cache_base_dir, job_id))
        logger.info(f"Cleaned up job cache directory for {job_id}")
    except Exception as e:
        logger.error(f"Error cleaning up job cache: {str(e)}")

    # Return success status and counts
    return {
        "job_id": job_id,
        "year_month": year_month,
        "files_processed": processed_count,
        "days_processed": len(daily_files_list),
        "files_copied": len(copied_files),
        "complete_month": True
    }


def main():
    """Main function to run the server processor continuously"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Server processor for ETL pipeline')
    parser.add_argument('--threads', type=int, default=DEFAULT_NUM_WORKER_THREADS,
                        help=f'Number of worker threads (default: {DEFAULT_NUM_WORKER_THREADS})')
    args = parser.parse_args()

    # Set number of worker threads from command line argument
    global NUM_WORKER_THREADS
    NUM_WORKER_THREADS = args.threads

    logger.info(f"Starting server processor with {NUM_WORKER_THREADS} worker threads")

    # Ensure directories exist
    os.makedirs(source_dir, exist_ok=True)
    os.makedirs(complete_dir, exist_ok=True)
    os.makedirs(cache_base_dir, exist_ok=True)

    # Run continuously until interrupted
    running = True
    while running:
        try:
            # Find source files
            manifest_files, data_files = list_source_files()

            if not manifest_files and not data_files:
                logger.info("No files to process. Sleeping before next check.")
                # Sleep before checking again
                time.sleep(60)  # Check every minute
                continue

            logger.info(f"Found {len(manifest_files)} manifest files and {len(data_files)} data files")

            # Process only one manifest file at a time that has complete_month=True
            active_job = False
            for manifest_path in manifest_files:
                try:
                    # Load job information from manifest
                    job_id, year_month, files, complete_month = load_job_from_manifest(manifest_path)

                    if not job_id or not year_month or not files:
                        logger.warning(f"Invalid manifest file: {manifest_path}. Skipping.")
                        continue

                    # Only process jobs with complete_month=True
                    if not complete_month:
                        logger.info(f"Job {job_id} for {year_month} is not marked as complete month. Skipping.")
                        continue

                    # Process this job
                    logger.info(f"Processing job {job_id} for {year_month} (complete month data)")
                    result = process_job(job_id, year_month, files)

                    logger.info(f"Job {job_id} completed: {result}")

                    # Move the manifest file to complete directory
                    try:
                        manifest_filename = os.path.basename(manifest_path)
                        shutil.move(manifest_path, os.path.join(complete_dir, manifest_filename))
                        logger.info(f"Moved manifest {manifest_filename} to complete directory")
                    except Exception as e:
                        logger.error(f"Error moving manifest file: {str(e)}")

                    # Set active_job to True and break to process only one job
                    active_job = True
                    break

                except Exception as e:
                    logger.error(f"Error processing manifest {manifest_path}: {str(e)}")

                    # Try to update status to failed
                    try:
                        job_id = os.path.basename(manifest_path).split('.')[0]
                        save_status(job_id, "failed", {"error": str(e)})
                    except Exception as status_err:
                        logger.error(f"Error saving failed status: {str(status_err)}")

            # If we've processed a job, continue to the next loop iteration
            if active_job:
                logger.info("Completed processing a job. Checking for more jobs.")
                continue

            # Don't process standalone data files as they should be processed by the ETL manager
            # and put into a complete month job
            if data_files:
                logger.info(
                    f"Found {len(data_files)} standalone data files but will not process them directly. Waiting for ETL manager to create a proper month job.")

            # Sleep for a while before the next iteration
            logger.info("Completed processing cycle. Sleeping before next check.")
            time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Shutting down gracefully...")
            running = False
        except Exception as e:
            logger.error(f"Unhandled exception in main function: {str(e)}")
            time.sleep(60)  # Check every minute


if __name__ == "__main__":
    main()