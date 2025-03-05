import os
import boto3
import shutil
import uuid
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from botocore.exceptions import BotoCoreError, ClientError
import logging
import re
from collections import defaultdict
import polars as pl
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

download_bucket = "conte-transform-to-fresco-ts"
upload_bucket = "stampede-proc-metrics-fresco-form"

# Batch size for processing files
BATCH_SIZE = 5

# Base directory for temporary files
TEMP_BASE_DIR = "s3_processor"


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
    paginator = s3_client.get_paginator('list_objects_v2')
    files_in_s3 = []

    # Use pagination to handle large buckets efficiently
    for page in paginator.paginate(Bucket=download_bucket):
        if 'Contents' in page:
            files_in_s3.extend([item['Key'] for item in page['Contents']])

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


def create_temp_dir():
    """Create a unique temporary directory"""
    # Create base temp directory if it doesn't exist
    if not os.path.exists(TEMP_BASE_DIR):
        os.makedirs(TEMP_BASE_DIR)

    # Create a unique directory name using timestamp and UUID
    unique_dir = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"
    temp_dir = os.path.join(TEMP_BASE_DIR, unique_dir)
    os.makedirs(temp_dir)

    return temp_dir


def clean_temp_dir(temp_dir):
    """Remove a temporary directory and all its contents"""
    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up temporary directory: {temp_dir}")
    except Exception as e:
        logger.warning(f"Failed to clean up directory {temp_dir}: {str(e)}")


def process_batches_for_year_month(year_month, files, temp_dir):
    """Process files in batches to manage memory usage"""
    s3_client = get_s3_client()

    # Create a temporary directory for intermediate files
    intermediate_dir = os.path.join(temp_dir, "intermediate")
    os.makedirs(intermediate_dir, exist_ok=True)

    # Process files in batches
    batches = [files[i:i + BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]

    # Track unique rows using a set
    batch_counter = 0

    for batch_idx, batch in enumerate(batches):
        logger.info(f"Processing batch {batch_idx + 1}/{len(batches)} for {year_month}")

        # List to store dataframes for this batch
        batch_dfs = []

        for file in batch:
            try:
                # Define download path
                download_path = os.path.join(temp_dir, os.path.basename(file))

                # Download the file
                s3_client.download_file(download_bucket, file, download_path)

                # Read CSV with Polars
                df = pl.read_csv(download_path)
                batch_dfs.append(df)

                # Clean up downloaded file immediately
                os.remove(download_path)

                logger.info(f"Processed {file}, read {len(df)} rows")

            except Exception as e:
                logger.error(f"Error processing file {file}: {str(e)}")
                continue

        if batch_dfs:
            # Combine all dataframes in this batch
            batch_df = pl.concat(batch_dfs)

            # If this is the first batch, create the unique file
            if batch_idx == 0:
                unique_df = batch_df.unique()
                intermediate_path = os.path.join(intermediate_dir, f"batch_{batch_idx}_{year_month}.parquet")
                unique_df.write_parquet(intermediate_path)
            else:
                # Read the previous unique file
                prev_unique_path = os.path.join(intermediate_dir, f"batch_{batch_idx - 1}_{year_month}.parquet")
                prev_unique_df = pl.read_parquet(prev_unique_path)

                # Combine with current batch and deduplicate
                combined_df = pl.concat([prev_unique_df, batch_df])
                unique_df = combined_df.unique()

                # Write to new intermediate file
                intermediate_path = os.path.join(intermediate_dir, f"batch_{batch_idx}_{year_month}.parquet")
                unique_df.write_parquet(intermediate_path)

                # Remove previous intermediate file to save space
                os.remove(prev_unique_path)

            # Clear memory
            del batch_dfs
            del batch_df
            if batch_idx > 0:
                del prev_unique_df
                del combined_df
            del unique_df

    # Get the final intermediate file
    final_intermediate_path = os.path.join(intermediate_dir, f"batch_{len(batches) - 1}_{year_month}.parquet")

    if not os.path.exists(final_intermediate_path):
        logger.error(f"No data processed for {year_month}")
        return None

    # Read the final intermediate file
    final_df = pl.read_parquet(final_intermediate_path)

    # Create the output file directly in the final format
    output_file = os.path.join(temp_dir, f"combined_node_data_{year_month}.csv")
    final_df.write_csv(output_file)

    logger.info(f"Created combined file {output_file} with {len(final_df)} unique rows")

    # Clean up intermediate directory
    shutil.rmtree(intermediate_dir)

    return output_file


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type((BotoCoreError, ClientError))
)
def upload_file_to_s3(file_path, bucket_name):
    """Uploads a file to S3 and removes it after successful upload"""
    s3_client = get_s3_client()
    extra_args = {'ContentType': 'text/csv'}

    # Use the filename as the S3 key
    s3_key = os.path.basename(file_path)

    logger.info(f"Uploading {file_path} to {bucket_name}/{s3_key}")
    s3_client.upload_file(file_path, bucket_name, s3_key, ExtraArgs=extra_args)

    # Remove the file after successful upload
    os.remove(file_path)
    logger.info(f"Uploaded and removed local file: {file_path}")


def process_year_month(year_month, files):
    """Process all files for a given year-month"""
    # Create a custom temporary directory for this year-month
    temp_dir = create_temp_dir()
    logger.info(f"Processing {len(files)} files for {year_month} in directory {temp_dir}")

    try:
        # Process files in batches
        output_file = process_batches_for_year_month(year_month, files, temp_dir)

        if output_file:
            # Upload the final result to S3
            upload_file_to_s3(output_file, upload_bucket)

            # Clean up temporary directory
            clean_temp_dir(temp_dir)
            return True

        # Clean up even if processing failed
        clean_temp_dir(temp_dir)
        return False

    except Exception as e:
        logger.error(f"Error processing {year_month}: {str(e)}")
        # Ensure cleanup happens even if there's an exception
        clean_temp_dir(temp_dir)
        return False


def cleanup_temp_base_dir():
    """Clean up the base temporary directory if it exists"""
    if os.path.exists(TEMP_BASE_DIR):
        try:
            shutil.rmtree(TEMP_BASE_DIR)
            logger.info(f"Cleaned up base temporary directory: {TEMP_BASE_DIR}")
        except Exception as e:
            logger.warning(f"Failed to clean up base directory {TEMP_BASE_DIR}: {str(e)}")


def main():
    try:
        # 1. List files in the S3 bucket
        files_in_s3 = list_s3_files()
        logger.info(f"Found {len(files_in_s3)} files in S3 bucket")

        # 2. Group files by year-month
        year_month_combos = get_year_month_combos(files_in_s3)
        logger.info(f"Found data for {len(year_month_combos)} year-month combinations")

        # 3. Process each year-month combination separately
        for year_month, files in year_month_combos.items():
            success = process_year_month(year_month, files)
            if success:
                logger.info(f"Successfully processed {year_month}")
            else:
                logger.error(f"Failed to process {year_month}")

    finally:
        # Final cleanup of temporary directories
        cleanup_temp_base_dir()


if __name__ == "__main__":
    main()