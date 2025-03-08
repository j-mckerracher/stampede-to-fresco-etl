import os
import boto3
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from botocore.exceptions import BotoCoreError, ClientError
from botocore.config import Config
import logging
import re
from collections import defaultdict
import polars as pl
from pathlib import Path
from tqdm import tqdm
import shutil

# Set up, etc.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

download_bucket = "conte-transform-to-fresco-ts"
upload_bucket = "stampede-proc-metrics-fresco-form"


def get_s3_client():
    """Create and return an S3 client with appropriate connection pool settings"""
    # Create a session with a persistent connection pool
    session = boto3.session.Session()
    return session.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1',
        config=Config(
            retries={'max_attempts': 5, 'mode': 'standard'}
        )
    )


def list_s3_files():
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=download_bucket)
    files_in_s3 = []

    # Extract files from the initial response
    if 'Contents' in response:
        files_in_s3 = [item['Key'] for item in response['Contents']]

    # Handle pagination if there are more objects
    while response.get('IsTruncated', False):
        response = s3_client.list_objects_v2(
            Bucket=download_bucket,
            ContinuationToken=response.get('NextContinuationToken')
        )
        if 'Contents' in response:
            files_in_s3.extend([item['Key'] for item in response['Contents']])

    return files_in_s3


def get_year_month_combos(files_in_s3):
    # Group files by year-month
    files_by_year_month = defaultdict(list)

    for file in files_in_s3:
        # Extract year-month from filename using regex
        match = re.search(r'node_data_(\d{4}-\d{2})\.csv$', file)
        if match:
            year_month = match.group(1)
            files_by_year_month[year_month].append(file)

    return files_by_year_month


def move_data_into_day_dataframe(df):
    """
    Group data by day of the month based on Timestamp column.

    Args:
        df (polars.DataFrame): DataFrame containing a Timestamp column

    Returns:
        dict: Dictionary where keys are days of the month (ints) and values are dataframes for that day
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


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(4),
    retry=retry_if_exception_type((BotoCoreError, ClientError, Exception))
)
def download_file(s3_client, file, temp_dir):
    """Download a single file from S3 with retry logic"""
    download_path = temp_dir / os.path.basename(file)
    s3_client.download_file(download_bucket, file, str(download_path))
    return download_path


def append_to_daily_file(day_df, day, year_month, cache_dir):
    """
    Append data to a daily file, creating if it doesn't exist.
    Handles deduplication.

    Args:
        day_df: Polars DataFrame with the day's data
        day: Day of month (integer)
        year_month: Year-month string (YYYY-MM)
        cache_dir: Directory to store the daily files

    Returns:
        Path to the daily file
    """
    # Format day as two digits
    day_str = f"{day:02d}"

    # Define output file path
    output_file = cache_dir / f"perf_metrics_{year_month}-{day_str}.csv"

    # If file exists, read, append, and deduplicate
    if output_file.exists():
        try:
            # Read existing file
            existing_df = pl.read_csv(output_file)

            # Combine with new data
            combined_df = pl.concat([existing_df, day_df])

            # Deduplicate
            unique_df = combined_df.unique()

            # Write back to file
            unique_df.write_csv(output_file)
            logger.debug(f"Updated file {output_file} with {len(unique_df)} unique rows")
        except Exception as e:
            logger.error(f"Error appending to existing file {output_file}: {str(e)}")
            # If there's an error, just write the new data
            day_df.write_csv(output_file)
    else:
        # Create new file
        day_df.write_csv(output_file)
        logger.debug(f"Created new file {output_file} with {len(day_df)} rows")

    return output_file


def process_downloaded_file(download_path, year_month, cache_dir):
    """
    Process a single downloaded file, split by day, and write to daily files.

    Args:
        download_path: Path to the downloaded CSV file
        year_month: Year-month string (YYYY-MM)
        cache_dir: Directory to store the daily files

    Returns:
        Set of file paths that were created/updated
    """
    processed_files = set()

    try:
        # Read the file with polars
        df = pl.read_csv(download_path)

        # Split into day dataframes
        df_days = move_data_into_day_dataframe(df)

        # Process each day's data
        for day, day_df in df_days.items():
            # Append to daily file
            daily_file = append_to_daily_file(day_df, day, year_month, cache_dir)
            processed_files.add(daily_file)

    except Exception as e:
        logger.error(f"Error processing file {download_path}: {str(e)}")

    return processed_files


def process_year_month(year_month, files):
    """Process all files for a given year-month combination sequentially"""
    # Create a single shared S3 client
    s3_client = get_s3_client()

    # Create cache and temp-downloads directories for this year-month
    cache_dir = Path(f"cache/{year_month}")
    cache_dir.mkdir(exist_ok=True, parents=True)

    temp_dir = Path(f"temp-downloads/{year_month}")
    temp_dir.mkdir(exist_ok=True, parents=True)

    daily_files = set()

    # Download and process files sequentially
    logger.info(f"Processing {len(files)} files for {year_month}")
    with tqdm(total=len(files), desc=f"Processing files for {year_month}") as file_pbar:
        for file in files:
            try:
                # Download file
                logger.debug(f"Downloading {file}")
                download_path = download_file(s3_client, file, temp_dir)

                # Process file immediately
                logger.debug(f"Processing {file}")
                new_daily_files = process_downloaded_file(download_path, year_month, cache_dir)
                daily_files.update(new_daily_files)

                # Delete downloaded file after processing
                os.remove(download_path)

                file_pbar.update(1)
            except Exception as e:
                logger.error(f"Error processing file {file}: {str(e)}")
                file_pbar.update(1)

    # Convert to list for progress tracking
    daily_files_list = list(daily_files)

    # Upload all daily files to S3
    logger.info(f"Uploading {len(daily_files_list)} daily files to S3")
    with tqdm(total=len(daily_files_list), desc=f"Uploading files for {year_month}") as upload_pbar:
        for file_path in daily_files_list:
            try:
                upload_file_to_s3(str(file_path), upload_bucket)
                upload_pbar.update(1)
            except Exception as e:
                logger.error(f"Error uploading {file_path}: {str(e)}")
                upload_pbar.update(1)

    # Remove daily files after upload
    logger.info("Cleaning up daily files")
    for file_path in daily_files_list:
        try:
            os.remove(file_path)
        except Exception as e:
            logger.error(f"Error removing cache file {file_path}: {str(e)}")

    # Return success status and counts
    return {
        "year_month": year_month,
        "files_processed": len(files),
        "days_processed": len(daily_files_list),
        "files_uploaded": len(daily_files_list)
    }


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(4),
    retry=retry_if_exception_type((BotoCoreError, ClientError))
)
def upload_file_to_s3(file_path: str, bucket_name: str) -> None:
    """
    Uploads a single file to an S3 bucket.

    Args:
        file_path (str): Local path to the file.
        bucket_name (str): Name of the S3 bucket.

    Raises:
        BotoCoreError, ClientError: If the upload fails due to a boto3 error.
    """
    # Add content type for CSV files
    s3_client = get_s3_client()
    extra_args = {
        'ContentType': 'text/csv'
    }

    # Use the filename as the S3 key
    s3_key = os.path.basename(file_path)

    logger.info(f"Uploading {file_path} to {bucket_name}/{s3_key}")
    s3_client.upload_file(file_path, bucket_name, s3_key, ExtraArgs=extra_args)


def main():
    # 1. Create temp directories
    for dir_path in ["cache", "temp-downloads"]:
        os.makedirs(dir_path, exist_ok=True)

    # 2. See what files are in the S3 bucket
    logger.info("Listing files in S3 bucket...")
    files_in_s3 = list_s3_files()
    logger.info(f"Found {len(files_in_s3)} files in S3 bucket")

    # 3. For each year month combination, get all files
    logger.info("Grouping files by year-month...")
    year_month_combos = get_year_month_combos(files_in_s3)
    logger.info(f"Found {len(year_month_combos)} year-month combinations")

    # 4. Process each year-month combination
    results = []
    with tqdm(total=len(year_month_combos), desc="Processing year-month combinations") as ym_pbar:
        for year_month, files in year_month_combos.items():
            logger.info(f"Starting processing for {year_month} with {len(files)} files")
            result = process_year_month(year_month, files)
            results.append(result)
            ym_pbar.update(1)

    # 5. Summarize results
    logger.info("Processing complete. Summary:")
    for result in results:
        logger.info(f"{result['year_month']}: Processed {result['files_processed']} files, "
                    f"created {result['days_processed']} daily files, uploaded {result['files_uploaded']} files")

    # 6. Clean up temporary directories
    try:
        shutil.rmtree("temp-downloads")
        shutil.rmtree("cache")
        logger.info("Cleaned up temporary directories")
    except Exception as e:
        logger.error(f"Error cleaning up temporary directories: {str(e)}")


if __name__ == "__main__":
    main()
