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

# Smaller batch size to reduce memory usage
BATCH_SIZE = 3

# Base directory for temporary files
TEMP_BASE_DIR = "/tmp/s3_processor"

# Cap max rows in memory for streaming operations
MAX_ROWS_IN_MEMORY = 500000


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


def get_schema_from_file(s3_client, file):
    """Get schema from a file to use for lazy processing"""
    temp_file = os.path.join("/tmp", os.path.basename(file))
    try:
        s3_client.download_file(download_bucket, file, temp_file)
        # Just read the first few rows to get schema
        df = pl.scan_csv(temp_file, infer_schema_length=100)
        schema = df.collect(100).schema
        os.remove(temp_file)
        return schema
    except Exception as e:
        logger.error(f"Failed to get schema from {file}: {str(e)}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return None


def stream_process_file(s3_client, file, output_dir, file_counter, schema=None):
    """Stream process a single file using lazy evaluation"""
    download_path = os.path.join(output_dir, f"download_{file_counter}.csv")
    output_path = os.path.join(output_dir, f"processed_{file_counter}.parquet")

    try:
        # Download file
        s3_client.download_file(download_bucket, file, download_path)
        logger.info(f"Downloaded {file} to {download_path}")

        # Process using lazy evaluation
        if schema:
            df_lazy = pl.scan_csv(download_path, schema=schema)
        else:
            df_lazy = pl.scan_csv(download_path)

        # Sort by all columns to prepare for deduplication
        df_lazy = df_lazy.sort(by=df_lazy.columns)

        # Write to parquet (more efficient storage)
        df_lazy.sink_parquet(output_path)

        # Clean up CSV file immediately
        os.remove(download_path)
        logger.info(f"Processed {file} and saved to {output_path}")

        # Force garbage collection
        gc.collect()

        return output_path
    except Exception as e:
        logger.error(f"Error processing file {file}: {str(e)}")
        if os.path.exists(download_path):
            os.remove(download_path)
        return None


def external_merge_sort(parquet_files, output_dir, year_month):
    """Perform external merge sort on parquet files with deduplication"""
    if not parquet_files:
        logger.error("No files to merge")
        return None

    # Final output path
    final_output = os.path.join(output_dir, f"combined_node_data_{year_month}.csv")

    # If there's just one file, convert it directly
    if len(parquet_files) == 1:
        df = pl.scan_parquet(parquet_files[0]).collect()
        df.write_csv(final_output)
        os.remove(parquet_files[0])
        return final_output

    logger.info(f"Beginning external merge sort for {len(parquet_files)} files")

    # Process in pairs to reduce memory usage
    while len(parquet_files) > 1:
        new_parquet_files = []

        # Process files in pairs
        for i in range(0, len(parquet_files), 2):
            if i + 1 < len(parquet_files):
                # We have a pair
                file1 = parquet_files[i]
                file2 = parquet_files[i + 1]

                merge_output = os.path.join(output_dir, f"merge_{i}_{uuid.uuid4().hex[:8]}.parquet")

                # Use lazy evaluation for merging
                df1_lazy = pl.scan_parquet(file1)
                df2_lazy = pl.scan_parquet(file2)

                # Concatenate and remove duplicates
                (
                    pl.concat([df1_lazy, df2_lazy])
                    .unique()
                    .sink_parquet(merge_output)
                )

                # Clean up input files
                os.remove(file1)
                os.remove(file2)

                # Add merged file to new list
                new_parquet_files.append(merge_output)

                # Force garbage collection
                gc.collect()

            else:
                # Odd number of files, just add the last one
                new_parquet_files.append(parquet_files[i])

        # Update our file list for next iteration
        parquet_files = new_parquet_files
        logger.info(f"Merge iteration complete, now have {len(parquet_files)} files")

    # Final output conversion from parquet to csv
    if parquet_files:
        df = pl.scan_parquet(parquet_files[0]).collect()
        df.write_csv(final_output)
        os.remove(parquet_files[0])

        logger.info(f"Created final combined file {final_output} with {len(df)} rows")
        return final_output

    return None


def process_year_month_streaming(year_month, files):
    """Process files for a year-month using streaming to minimize memory usage"""
    temp_dir = create_temp_dir()
    logger.info(f"Processing {len(files)} files for {year_month} in directory {temp_dir}")

    try:
        s3_client = get_s3_client()

        # Get schema from first file to use for all files
        schema = get_schema_from_file(s3_client, files[0]) if files else None

        # Process files individually to parquet format
        parquet_files = []
        for i, file in enumerate(files):
            # Log progress
            if i % 5 == 0:
                logger.info(f"Processing file {i + 1}/{len(files)} for {year_month}")

            parquet_file = stream_process_file(s3_client, file, temp_dir, i, schema)
            if parquet_file:
                parquet_files.append(parquet_file)

            # Force garbage collection
            gc.collect()

        # Merge the parquet files with external merge sort
        final_output = external_merge_sort(parquet_files, temp_dir, year_month)

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