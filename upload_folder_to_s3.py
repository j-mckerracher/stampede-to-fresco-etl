import glob
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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


s3_client = get_s3_client()


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

    bucket_name = "conte-transform-to-fresco-ts"
    logger.info(f"Uploading files from {cache_dir} to {bucket_name}")

    count = 1
    for filename in os.listdir(cache_dir):
        # Use the full file path
        file_path = os.path.join(cache_dir, filename)
        upload_file_to_s3(file_path, bucket_name)
        count += 1

    logger.info(f"Done uploading {count} files.")
    return True


def remove_csv_files_from_cache():
    """
    Removes all CSV files from the 'cache' directory.

    Returns:
        int: The number of files removed
    """
    # Define the cache directory
    cache_dir = "cache"

    # Check if the directory exists
    if not os.path.exists(cache_dir):
        print(f"Error: '{cache_dir}' directory does not exist.")
        return 0

    # Get a list of all CSV files in the cache directory
    csv_files = glob.glob(os.path.join(cache_dir, "*.csv"))

    # Count the number of files to be removed
    file_count = len(csv_files)

    # Remove each CSV file
    for file_path in csv_files:
        try:
            os.remove(file_path)
            print(f"Removed: {file_path}")
        except Exception as e:
            print(f"Error removing {file_path}: {e}")

    print(f"Done. Removed {file_count} CSV files from the cache directory.")
    return file_count


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

    logger.info(f"Done renaming. Renamed {count} files")


def main(start_index: str, end_index: str):
    # 1. Rename all files with the provided prefix
    prefix_file_names(start_index, end_index)

    # 2. Upload all files to S3
    if upload_folder_to_s3():
        # 3. Clear cache
        remove_csv_files_from_cache()
