import os
import logging
import re
import json
from collections import defaultdict
import polars as pl
from pathlib import Path
from tqdm import tqdm
import shutil
import psutil
import time
import uuid

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("server_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Modified paths for the distributed approach
source_dir = r"U:\projects\stampede-to-fresco-etl\cache\input"
complete_dir = r"U:\projects\stampede-to-fresco-etl\cache\complete"
cache_base_dir = r"U:\projects\stampede-to-fresco-etl\cache\processing"

# Resource management configurations
MAX_MEMORY_PERCENT = 70  # Cap memory usage at 70%

# Status file to track process state
STATUS_FILE = os.path.join(cache_base_dir, "processing_status.json")


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

        logger.info(f"Loaded job {job_id} for {year_month} with {len(files)} files")
        return job_id, year_month, files
    except Exception as e:
        logger.error(f"Error loading manifest {manifest_path}: {str(e)}")
        return None, None, []


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


def append_to_daily_csv(day_df, day, year_month, cache_dir):
    """
    Append data to a daily CSV file, creating if it doesn't exist.
    Handles deduplication using a safer approach by working with CSV.
    """
    # Format day as two digits
    day_str = f"{day:02d}"

    # Define output file path for CSV (intermediate storage)
    csv_file = cache_dir / f"perf_metrics_{year_month}-{day_str}.csv"

    # Generate a unique temporary file path
    temp_csv_file = cache_dir / f"temp_{uuid.uuid4().hex}_{year_month}-{day_str}.csv"

    try:
        # Convert day_df to CSV for easier handling
        day_df.write_csv(temp_csv_file)

        # If the output CSV file already exists, we need to merge and deduplicate
        if csv_file.exists():
            try:
                # Create a new file that will contain the merged data
                merged_csv_file = cache_dir / f"merged_{uuid.uuid4().hex}_{year_month}-{day_str}.csv"

                # Read both CSVs into DataFrames
                existing_df = pl.read_csv(csv_file)
                new_df = pl.read_csv(temp_csv_file)

                # Combine and deduplicate
                merged_df = pl.concat([existing_df, new_df]).unique()

                # Write to a new file
                merged_df.write_csv(merged_csv_file)

                # Replace the old file with the merged file
                os.replace(merged_csv_file, csv_file)

                # Cleanup the temporary file
                os.remove(temp_csv_file)

                logger.debug(f"Updated file {csv_file} with {len(merged_df)} unique rows")
            except Exception as e:
                logger.error(f"Error merging CSV files: {str(e)}")
                # If merging fails, just use the new data (don't lose it)
                os.replace(temp_csv_file, csv_file)
        else:
            # No existing file, just rename the temp file
            os.rename(temp_csv_file, csv_file)
            logger.debug(f"Created new file {csv_file} with {len(day_df)} rows")

    except Exception as e:
        logger.error(f"Error in append_to_daily_csv: {str(e)}")
        # Cleanup any temporary files
        if os.path.exists(temp_csv_file):
            os.remove(temp_csv_file)

    return csv_file


def process_downloaded_file(file_path, year_month, cache_dir):
    """
    Process a single file, split by day, and write to daily CSV files.
    """
    processed_files = set()

    try:
        # Memory optimization: Use streaming for large files
        df = pl.scan_parquet(file_path).collect()

        # Optimize memory by releasing original dataframe as soon as possible
        df_days = move_data_into_day_dataframe(df)
        del df  # Explicitly release memory

        # Process each day's data
        for day, day_df in df_days.items():
            # Append to daily file
            daily_file = append_to_daily_csv(day_df, day, year_month, cache_dir)
            processed_files.add(daily_file)
            del day_df  # Release memory

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")

    return processed_files


def convert_csv_to_parquet(csv_file, parquet_file):
    """
    Converts a CSV file to a Parquet file with validation.
    """
    try:
        # Read the CSV file
        df = pl.read_csv(csv_file)

        # Write to a temporary parquet file first
        temp_parquet = str(parquet_file) + ".temp"
        df.write_parquet(temp_parquet)

        # Validate the parquet file
        _ = pl.read_parquet(temp_parquet)

        # If validation passes, rename the file
        os.replace(temp_parquet, parquet_file)
        return True
    except Exception as e:
        logger.error(f"Error converting CSV to Parquet: {str(e)}")
        # Cleanup temp file if it exists
        if os.path.exists(temp_parquet):
            os.remove(temp_parquet)
        return False


def copy_file_to_complete_dir(file_path, job_id):
    """
    Copies a processed file to the complete directory.
    """
    # Create a job-specific directory in the complete dir
    job_complete_dir = os.path.join(complete_dir, job_id)
    os.makedirs(job_complete_dir, exist_ok=True)

    # The input is a CSV file, but we want to store as parquet
    file_name = os.path.basename(file_path)
    if file_name.endswith('.csv'):
        parquet_name = file_name.replace('.csv', '.parquet')
    else:
        parquet_name = file_name + '.parquet'

    dest_path = os.path.join(job_complete_dir, parquet_name)

    try:
        # If the source is a CSV, convert it to parquet
        if file_path.endswith('.csv'):
            if convert_csv_to_parquet(file_path, dest_path):
                logger.debug(f"Converted and copied {file_path} to {dest_path}")
                return True
            else:
                logger.error(f"Failed to convert {file_path} to parquet")
                return False
        else:
            # Direct copy for non-CSV files
            shutil.copy2(file_path, dest_path)
            logger.debug(f"Copied {file_path} to {dest_path}")
            return True
    except Exception as e:
        logger.error(f"Error copying {file_path} to {dest_path}: {str(e)}")
        return False


def process_file(file_path, year_month, cache_dir):
    """Process a single file"""
    try:
        # Process file
        logger.debug(f"Processing {file_path}")
        daily_files = process_downloaded_file(file_path, year_month, cache_dir)

        return daily_files
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return set()


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
    logger.info(f"Converting and copying {len(daily_files_list)} CSV files to parquet in complete directory")
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
        "timestamp": time.time()
    }

    # Write the manifest to the complete directory
    manifest_path = os.path.join(complete_dir, f"{job_id}.manifest.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest_data, f)

    # Update final status
    save_status(job_id, "completed", manifest_data)

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
        "files_copied": len(copied_files)
    }


def main():
    """Main function to run the server processor"""
    logger.info("Starting server processor")

    # Ensure directories exist
    os.makedirs(source_dir, exist_ok=True)
    os.makedirs(complete_dir, exist_ok=True)
    os.makedirs(cache_base_dir, exist_ok=True)

    try:
        # Find source files
        manifest_files, data_files = list_source_files()

        if not manifest_files and not data_files:
            logger.info("No files to process. Exiting.")
            return

        logger.info(f"Found {len(manifest_files)} manifest files and {len(data_files)} data files")

        # Process each manifest file
        for manifest_path in manifest_files:
            try:
                # Load job information from manifest
                job_id, year_month, files = load_job_from_manifest(manifest_path)

                if not job_id or not year_month or not files:
                    logger.warning(f"Invalid manifest file: {manifest_path}. Skipping.")
                    continue

                # Process this job
                logger.info(f"Processing job {job_id} for {year_month}")
                result = process_job(job_id, year_month, files)

                logger.info(f"Job {job_id} completed: {result}")

                # Move the manifest file to complete directory
                try:
                    manifest_filename = os.path.basename(manifest_path)
                    shutil.move(manifest_path, os.path.join(complete_dir, manifest_filename))
                    logger.info(f"Moved manifest {manifest_filename} to complete directory")
                except Exception as e:
                    logger.error(f"Error moving manifest file: {str(e)}")

            except Exception as e:
                logger.error(f"Error processing manifest {manifest_path}: {str(e)}")

                # Try to update status to failed
                try:
                    job_id = os.path.basename(manifest_path).split('.')[0]
                    save_status(job_id, "failed", {"error": str(e)})
                except Exception as status_err:
                    logger.error(f"Error saving failed status: {str(status_err)}")

        # Process any standalone data files
        if data_files:
            logger.info(f"Processing {len(data_files)} standalone data files")

            # Create a default job ID for these files
            default_job_id = f"standalone_{int(time.time())}"

            # Try to extract year-month from filenames
            # Assuming standard format like "data_2023-05_something.parquet"
            year_month_pattern = re.compile(r'(\d{4}-\d{2})')
            year_months = {}

            for file_path in data_files:
                filename = os.path.basename(file_path)
                match = year_month_pattern.search(filename)

                if match:
                    year_month = match.group(1)
                else:
                    # Default to current year-month if pattern not found
                    current_time = time.localtime()
                    year_month = f"{current_time.tm_year}-{current_time.tm_mon:02d}"

                if year_month not in year_months:
                    year_months[year_month] = []

                year_months[year_month].append(os.path.relpath(file_path, source_dir))

            # Process each year-month group
            for year_month, files in year_months.items():
                job_id = f"{default_job_id}_{year_month}"
                logger.info(f"Processing standalone job {job_id} with {len(files)} files")

                try:
                    result = process_job(job_id, year_month, files)
                    logger.info(f"Standalone job {job_id} completed: {result}")
                except Exception as e:
                    logger.error(f"Error processing standalone job {job_id}: {str(e)}")
                    save_status(job_id, "failed", {"error": str(e)})

    except Exception as e:
        logger.error(f"Unhandled exception in main function: {str(e)}")

    logger.info("Server processor completed")


if __name__ == "__main__":
    main()