import os
import logging
import re
from collections import defaultdict
import polars as pl
from pathlib import Path
from tqdm import tqdm
import shutil
import concurrent.futures
import psutil
import time
import uuid
import csv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Local file paths instead of S3 buckets
source_dir = r"P:\Stampede\stampede-ts-to-fresco-ts-chunks-1"
destination_dir = r"P:\Stampede\stampede-ts-fresco-form-daily-2"

# Already processed year-month combinations to skip
processed_year_months = [
    "2013-02", "2013-03", "2013-04", "2013-05",
    "2013-06", "2013-07", "2013-08", "2013-09"
]

# Resource management configurations
MAX_WORKERS = max(1, psutil.cpu_count(logical=False) - 1)  # Use physical cores - 1
MAX_MEMORY_PERCENT = 70  # Cap memory usage at 70%


def list_source_files():
    """List all files in the source directory"""
    files = []
    for root, _, filenames in os.walk(source_dir):
        for filename in filenames:
            if filename.endswith('.parquet'):
                rel_path = os.path.relpath(os.path.join(root, filename), source_dir)
                files.append(rel_path)
    return files


def get_year_month_combos(files_list):
    """Group files by year-month"""
    files_by_year_month = defaultdict(list)

    for file in files_list:
        # Extract year-month from filename using regex
        match = re.search(r'node_data_(\d{4}-\d{2})\.parquet$', file)
        if match:
            year_month = match.group(1)
            # Skip already processed year-months
            if year_month not in processed_year_months:
                files_by_year_month[year_month].append(file)

    return files_by_year_month


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


def copy_file_to_destination(file_path, destination_bucket):
    """
    Copies a single file to the destination directory.
    """
    # The input is a CSV file, but we want to store as parquet in the destination
    file_name = os.path.basename(file_path)
    if file_name.endswith('.csv'):
        parquet_name = file_name.replace('.csv', '.parquet')
    else:
        parquet_name = file_name + '.parquet'

    dest_path = os.path.join(destination_bucket, parquet_name)

    try:
        # Ensure destination directory exists
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        # If the source is a CSV, convert it to parquet first
        if file_path.endswith('.csv'):
            if convert_csv_to_parquet(file_path, dest_path):
                logger.debug(f"Converted and copied {file_path} to {dest_path}")
                return True
            else:
                logger.error(f"Failed to convert {file_path} to parquet")
                return False
        else:
            # Direct copy for non-CSV files (shouldn't happen in this flow)
            shutil.copy2(file_path, dest_path)
            logger.debug(f"Copied {file_path} to {dest_path}")
            return True
    except Exception as e:
        logger.error(f"Error copying {file_path} to {dest_path}: {str(e)}")
        return False


def process_file(file, year_month, cache_dir):
    """Process a single file"""
    try:
        # Build full path to source file
        source_file_path = os.path.join(source_dir, file)

        # Process file
        logger.debug(f"Processing {file}")
        daily_files = process_downloaded_file(source_file_path, year_month, cache_dir)

        return daily_files
    except Exception as e:
        logger.error(f"Error processing file {file}: {str(e)}")
        return set()


def process_year_month(year_month, files):
    """Process all files for a given year-month combination"""
    # Create cache directory for this year-month
    cache_dir = Path(f"cache/{year_month}")
    cache_dir.mkdir(exist_ok=True, parents=True)

    # Set for collecting daily files
    daily_files_dict = {}  # Use a dict to track unique files

    # Check if memory usage is too high before processing
    def memory_ok():
        memory_percent = psutil.virtual_memory().percent
        return memory_percent < MAX_MEMORY_PERCENT

    # Process files sequentially to avoid corruption
    logger.info(f"Processing {len(files)} files for {year_month}")
    with tqdm(total=len(files), desc=f"Processing files for {year_month}") as file_pbar:
        # Process files in smaller batches to control memory usage
        batch_size = 5  # Small batch size for better control

        for i in range(0, len(files), batch_size):
            # Wait if memory usage is too high
            while not memory_ok():
                logger.warning(f"Memory usage high ({psutil.virtual_memory().percent}%). Waiting...")
                time.sleep(5)

            batch = files[i:i + batch_size]

            # Process this batch sequentially
            for file in batch:
                try:
                    new_daily_files = process_file(file, year_month, cache_dir)
                    for daily_file in new_daily_files:
                        daily_files_dict[str(daily_file)] = daily_file
                except Exception as e:
                    logger.error(f"Error processing file {file}: {str(e)}")

                file_pbar.update(1)

    # Convert to list for progress tracking
    daily_files_list = list(daily_files_dict.values())

    # Copy all daily files to destination
    logger.info(f"Converting and copying {len(daily_files_list)} CSV files to parquet in destination")
    copied_files = []
    with tqdm(total=len(daily_files_list), desc=f"Copying files for {year_month}") as copy_pbar:
        for file_path in daily_files_list:
            if copy_file_to_destination(str(file_path), destination_dir):
                copied_files.append(file_path)
            copy_pbar.update(1)

    # Remove daily files after copying
    logger.info("Cleaning up daily files")
    for file_path in daily_files_list:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger.error(f"Error removing cache file {file_path}: {str(e)}")

    # Return success status and counts
    return {
        "year_month": year_month,
        "files_processed": len(files),
        "days_processed": len(daily_files_list),
        "files_copied": len(copied_files)
    }


def main():
    start_time = time.time()

    # 1. Create temp directories
    for dir_path in ["cache"]:
        os.makedirs(dir_path, exist_ok=True)

    # Ensure destination directory exists
    os.makedirs(destination_dir, exist_ok=True)

    # 2. Clear any existing cache to start fresh
    try:
        logger.info("Cleaning any existing cache files to start fresh")
        for root, dirs, files in os.walk("cache"):
            for file in files:
                if file.endswith(".parquet") or file.endswith(".csv"):
                    os.remove(os.path.join(root, file))
    except Exception as e:
        logger.error(f"Error cleaning existing cache: {str(e)}")

    # 3. List all files in the source directory
    logger.info("Listing files in source directory...")
    files_list = list_source_files()
    logger.info(f"Found {len(files_list)} files in source directory")

    # 4. Group files by year-month
    logger.info("Grouping files by year-month...")
    year_month_combos = get_year_month_combos(files_list)
    logger.info(f"Found {len(year_month_combos)} year-month combinations to process")

    # Skip if no new combinations to process
    if not year_month_combos:
        logger.info("No new year-month combinations to process. Exiting.")
        return

    # 5. Process each year-month combination
    results = []
    with tqdm(total=len(year_month_combos), desc="Processing year-month combinations") as ym_pbar:
        for year_month, files in year_month_combos.items():
            logger.info(f"Starting processing for {year_month} with {len(files)} files")
            result = process_year_month(year_month, files)
            results.append(result)
            ym_pbar.update(1)

    # 6. Summarize results
    total_runtime = time.time() - start_time
    logger.info(f"Processing complete in {total_runtime:.2f} seconds. Summary:")
    for result in results:
        logger.info(f"{result['year_month']}: Processed {result['files_processed']} files, "
                    f"created {result['days_processed']} daily files, copied {result['files_copied']} files")

    # 7. Clean up temporary directories
    try:
        shutil.rmtree("cache")
        logger.info("Cleaned up temporary directories")
    except Exception as e:
        logger.error(f"Error cleaning up temporary directories: {str(e)}")


if __name__ == "__main__":
    main()