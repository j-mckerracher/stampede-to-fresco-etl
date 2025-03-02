import argparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('node_etl.log')
    ]
)
logger = logging.getLogger(__name__)


def get_s3_client():
    """Create and return an S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )


@retry(
    stop=stop_after_attempt(3),  # Try up to 3 times
    wait=wait_fixed(2),  # Wait 2 seconds between attempts
    retry=retry_if_exception_type((BotoCoreError, ClientError))
)
def upload_to_s3(bucket_name="conte-transform-to-fresco-ts"):
    """Upload files to S3 public bucket without requiring credentials"""
    logger.info("\nStarting S3 upload...")

    s3_client = get_s3_client()

    for i, file_path in enumerate("cache", 1):
        file_name = os.path.basename("cache")
        # Add content type for CSV files
        extra_args = {
            'ContentType': 'text/csv'
        }

        logger.info(f"Uploading {file_path} to {bucket_name}/{file_name}")
        s3_client.upload_file(
            file_path,
            bucket_name,
            file_name,
            ExtraArgs=extra_args
        )


def prefix_file_names(start: str, end: str) -> str:
    """
        Renames all CSV files in the 'cache' folder by adding a prefix in the format
        '{start}_to_{end}_' to the original filename.

        Args:
            start: The starting value for the prefix
            end: The ending value for the prefix
        """
    # Define the cache directory
    cache_dir = "cache"

    # Check if the directory exists
    if not os.path.exists(cache_dir):
        logger.error(f"Error: '{cache_dir}' directory does not exist.")
        return

    # Create the prefix
    prefix = f"{start}_to_{end}_"

    # Iterate over all files in the cache directory
    count = 1
    for filename in os.listdir(cache_dir):
        # Check if the file is a CSV
        if filename.endswith(".csv"):
            # Create the old and new paths
            old_path = os.path.join(cache_dir, filename)
            new_path = os.path.join(cache_dir, prefix + filename)

            # Rename the file
            os.rename(old_path, new_path)
            count += 1
            logger.info(f"Renamed: {filename} -> {prefix + filename}")

    logger.info(f"Done renaming. Renamed {count} files")


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process files in cache directory and upload to S3.')
    parser.add_argument('start', type=str, help='Start value for the file prefix')
    parser.add_argument('end', type=str, help='End value for the file prefix')

    # Parse arguments
    args = parser.parse_args()

    # 1. Rename all files with the provided prefix
    prefix_file_names(args.start, args.end)

    # 2. Upload all files to S3
    upload_to_s3()


if __name__ == "__main__":
    main()