import os
import boto3
import shutil
import uuid
import gc
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

# Base directory for temporary files
TEMP_BASE_DIR = "/tmp/s3_processor"


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


def process_single_file(s3_client, file, output_dir, file_counter):
    """Process a single file and convert to parquet for merge"""
    download_path = os.path.join(output_dir, f"download_{file_counter}.csv")
    output_path = os.path.join(output_dir, f"processed_{file_counter}.parquet")

    try:
        # Download file
        s3_client.download_file(download_bucket, file, download_path)
        logger.info(f"Downloaded {file} to {download_path}")

        # Read the CSV file with regular (non-lazy) evaluation
        # This avoids issues with the streaming engine deprecation
        df = pl.read_csv(download_path)

        # Write to parquet (more efficient storage)
        df.write_parquet(output_path)

        # Clean up CSV file immediately
        os.remove(download_path)
        logger.info(f"Processed {file} and saved to {output_path}")

        # Force garbage collection
        del df
        gc.collect()

        return output_path
    except Exception as e:
        logger.error(f"Error processing file {file}: {str(e)}")
        if os.path.exists(download_path):
            os.remove(download_path)
        return None


def batch_and_deduplicate(parquet_files, output_dir, year_month, batch_size=5):
    """Process parquet files in batches to deduplicate"""
    if not parquet_files:
        logger.error("No files to merge")
        return None

    # Final output path
    final_output = os.path.join(output_dir, f"combined_node_data_{year_month}.csv")

    # If there's just one file, convert it directly
    if len(parquet_files) == 1:
        df = pl.read_parquet(parquet_files[0])
        df = df.unique()
        df.write_csv(final_output)
        os.remove(parquet_files[0])
        return final_output

    logger.info(f"Beginning batch deduplication for {len(parquet_files)} files")

    # Process in small batches
    result_file = None
    current_batch = []

    for i, file in enumerate(parquet_files):
        current_batch.append(file)

        # When we reach batch size or the end, process the batch
        if len(current_batch) >= batch_size or i == len(parquet_files) - 1:
            logger.info(f"Processing batch of {len(current_batch)} files")

            # Load and concatenate this batch
            batch_dfs = [pl.read_parquet(f) for f in current_batch]
            batch_df = pl.concat(batch_dfs)

            # Deduplicate the batch
            batch_df = batch_df.unique()

            # If we have a result from a previous batch, merge with it
            if result_file:
                previous_df = pl.read_parquet(result_file)
                combined_df = pl.concat([previous_df, batch_df])
                combined_df = combined_df.unique()

                # Write the result
                temp_result = os.path.join(output_dir, f"result_{i}.parquet")
                combined_df.write_parquet(temp_result)

                # Clean up
                os.remove(result_file)
                result_file = temp_result

                # Clear memory
                del previous_df
                del combined_df
                gc.collect()
            else:
                # First batch, just save it
                result_file = os.path.join(output_dir, f"result_{i}.parquet")
                batch_df.write_parquet(result_file)

            # Clean up batch files and memory
            for f in current_batch:
                os.remove(f)
            del batch_dfs
            del batch_df
            gc.collect()

            # Reset for next batch
            current_batch = []

    # Convert final result to CSV
    if result_file:
        df = pl.read_parquet(result_file)
        df.write_csv(final_output)
        os.remove(result_file)

        logger.info(f"Created final combined file {final_output} with {len(df)} rows")
        return final_output

    return None


def process_year_month_streaming(year_month, files):
    """Process files for a year-month in batches to minimize memory usage"""
    temp_dir = create_temp_dir()
    logger.info(f"Processing {len(files)} files for {year_month} in directory {temp_dir}")

    try:
        s3_client = get_s3_client()

        # Process each file individually to parquet format
        parquet_files = []
        for i, file in enumerate(files):
            # Log progress
            if i % 10 == 0:
                logger.info(f"Processing file {i + 1}/{len(files)} for {year_month}")

            parquet_file = process_single_file(s3_client, file, temp_dir, i)
            if parquet_file:
                parquet_files.append(parquet_file)

            # Force garbage collection
            gc.collect()

        # Merge and deduplicate in batches
        final_output = batch_and_deduplicate(parquet_files, temp_dir, year_month, batch_size=3)

        if final_output:
            # Upload the final result to S3
            upload_file_to_s3(final_output, upload_bucket)

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
            success = process_year_month_streaming(year_month, files)
            if success:
                logger.info(f"Successfully processed {year_month}")
            else:
                logger.error(f"Failed to process {year_month}")

            # Force garbage collection between year-months
            gc.collect()

    finally:
        # Final cleanup of temporary directories
        cleanup_temp_base_dir()


if __name__ == "__main__":
    main()