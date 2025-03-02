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


# Initialize the S3 client
s3_client = boto3.client(
    's3',

    region_name='us-east-1'
)


@retry(
    stop=stop_after_attempt(3),  # Try up to 3 times
    wait=wait_fixed(2),  # Wait 2 seconds between attempts
    retry=retry_if_exception_type((BotoCoreError, ClientError))
)
def upload_file_to_s3(file_path: str, bucket_name: str) -> None:
    """
    Uploads a single file to an S3 bucket with the specified S3 key.

    Args:
        file_path (str): Local path to the file.
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): The key (path) under which to store the file in S3.

    Raises:
        BotoCoreError, ClientError: If the upload fails due to a boto3 error.
    """
    # Add content type for CSV files
    extra_args = {
        'ContentType': 'text/csv'
    }
    logger.info(f"Uploading {file_path} to {bucket_name}/{file_path}")
    s3_client.upload_file(file_path, bucket_name, ExtraArgs=extra_args)


def upload_folder_to_s3() -> None:
    """
    Uploads all files in the given folder (and its subfolders) to the specified S3 bucket.

    Args:
        folder_path (str): The local directory containing the files to upload.
        bucket_name (str): The target S3 bucket name.
    """
    # Define the cache directory
    cache_dir = "cache"

    bucket_name = "conte-transform-to-fresco-ts"
    logger.info(f"Uploading {os.listdir(cache_dir)} to {bucket_name}")

    count = 1
    for filename in os.listdir(cache_dir):
        upload_file_to_s3(filename, bucket_name)
        logger.info(f"Uploading {filename} . . .")
        count += 1

    logger.info(f"Done uploading {count} files.")


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
    upload_folder_to_s3()


if __name__ == "__main__":
    main()