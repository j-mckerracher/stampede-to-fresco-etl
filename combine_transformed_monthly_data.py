import os
import boto3
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from botocore.exceptions import BotoCoreError, ClientError
import logging
import re
from collections import defaultdict
import polars as pl
from pathlib import Path

# Set up, etc.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

download_bucket = "conte-transform-to-fresco-ts"


def get_s3_client():
    """Create and return an S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )


def list_s3_files():
    s3_client = get_s3_client()
    response = s3_client.list_objects(Bucket=download_bucket)
    files_in_s3 = []

    # Extract files from the initial response
    if 'Contents' in response:
        files_in_s3 = [item['Key'] for item in response['Contents']]

    # Handle pagination if there are more objects
    while response.get('IsTruncated', False):
        response = s3_client.list_objects(Bucket=download_bucket, Marker=response['Contents'][-1]['Key'])
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


def download_and_combine_files_for_year_month(files_by_year_month) -> bool:
    # Create cache and temp-downloads directories if they don't exist
    s3_client = get_s3_client()
    cache_dir = Path("cache")
    cache_dir.mkdir(exist_ok=True)

    temp_dir = Path("temp-downloads")
    temp_dir.mkdir(exist_ok=True)

    # Process each year-month group
    for year_month, files in files_by_year_month.items():
        logger.info(f"Processing {len(files)} files for {year_month}")

        # List to store dataframes
        dfs_to_concatenate = []

        for file in files:
            try:
                # Define the download path in temp-downloads folder
                download_path = temp_dir / os.path.basename(file)

                # Download the file
                s3_client.download_file(download_bucket, file, str(download_path))

                # Read the file with polars
                df = pl.read_csv(download_path)
                dfs_to_concatenate.append(df)

                # Clean up the downloaded file
                os.remove(download_path)

                logger.info(f"Processed {file}, read {len(df)} rows")

            except Exception as e:
                logger.error(f"Error processing file {file}: {str(e)}")
                return False

        # Combine all dataframes and remove duplicates
        if dfs_to_concatenate:
            # Concatenate all dataframes
            combined_df = pl.concat(dfs_to_concatenate)

            # Remove duplicates
            unique_df = combined_df.unique()

            # Write the final deduplicated data to a CSV file
            output_file = cache_dir / f"combined_node_data_{year_month}.csv"
            unique_df.write_csv(output_file)

            logger.info(f"Created combined file {output_file} with {len(unique_df)} unique rows")

            # Clear memory
            del dfs_to_concatenate
            del combined_df
            del unique_df

    return True


@retry(
    stop=stop_after_attempt(3),  # Try up to 3 times
    wait=wait_fixed(2),  # Wait 2 seconds between attempts
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


def upload_folder_to_s3() -> bool:
    """
    Uploads all files in the given folder to the specified S3 bucket.
    """
    # Define the cache directory
    cache_dir = "cache"

    bucket_name = "stampede-proc-metrics-fresco-form"
    logger.info(f"Uploading files from {cache_dir} to {bucket_name}")

    count = 0
    for filename in os.listdir(cache_dir):
        # Use the full file path
        file_path = os.path.join(cache_dir, filename)
        upload_file_to_s3(file_path, bucket_name)
        count += 1

    # Clean up the downloaded file
    os.remove(cache_dir)
    logger.info(f"Done uploading {count} files.")
    return True


def main():
    # 1. See what files are in the S3 bucket (S3 ls)
    files_in_s3 = list_s3_files()

    # 2. For each year month combination, get all files
    year_month_combos = get_year_month_combos(files_in_s3)

    # 3. Download and combine all files into one giant file and remove duplicate rows
    download_and_combine_files_for_year_month(year_month_combos)

    # 4. Upload the year-month file to s3
    upload_folder_to_s3()


if __name__ == "__main__":
    main()
