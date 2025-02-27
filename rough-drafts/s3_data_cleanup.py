#!/usr/bin/env python3
"""
S3 Node Filter Script

This script:
1. Downloads all CSV files from an S3 bucket
2. Removes all rows containing a specified NODE value
3. Uploads the modified files back to the original S3 bucket
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('s3_node_filter.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration
S3_BUCKET = 'data-transform-stampede'  # Replace with your bucket name
NODE_TO_REMOVE = '104'  # Replace with the NODE value to remove
AWS_PROFILE = None  # Set to None to use environment variables or default profile, or specify a profile name
TEMP_DIR = Path(".") / "s3_node_filter"


class S3NodeFilter:
    """Filter NODE values from CSV files in S3"""

    def __init__(self, bucket_name, node_to_remove, aws_profile=None):
        """Initialize with bucket name and NODE value to remove"""
        self.bucket_name = bucket_name
        self.node_to_remove = node_to_remove
        self.temp_dir = TEMP_DIR

        # Create session with specified profile if provided
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            self.s3_client = session.client('s3')
        else:
            self.s3_client = boto3.client('s3')

        # Ensure temp directory exists
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized S3NodeFilter for bucket: {bucket_name}, filtering: {node_to_remove}")

    def list_csv_files(self):
        """List all CSV files in the bucket"""
        try:
            csv_files = []

            # S3 may return objects in pages if there are many
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.lower().endswith('.csv'):
                            csv_files.append(key)

            logger.info(f"Found {len(csv_files)} CSV files in bucket {self.bucket_name}")
            return csv_files

        except ClientError as e:
            logger.error(f"Error listing files in bucket {self.bucket_name}: {e}")
            return []

    def download_file(self, s3_key):
        """Download file from S3 to temp directory"""
        local_path = self.temp_dir / Path(s3_key).name

        try:
            logger.info(f"Downloading {s3_key} to {local_path}")
            self.s3_client.download_file(self.bucket_name, s3_key, str(local_path))
            logger.info(f"Successfully downloaded {s3_key}")
            return local_path

        except ClientError as e:
            logger.error(f"Error downloading {s3_key}: {e}")
            return None

    def process_file(self, local_path):
        """Remove rows with specified NODE value from CSV file"""
        try:
            logger.info(f"Processing file: {local_path}")

            # Read CSV file with pandas
            df = pd.read_csv(local_path)
            original_rows = len(df)

            # Check if 'Host' column exists
            if 'Host' not in df.columns:
                logger.warning(f"File {local_path} does not contain 'Host' column, skipping")
                return False

            # Filter out rows containing the NODE value
            df_filtered = df[df['Host'] != self.node_to_remove]
            removed_rows = original_rows - len(df_filtered)

            if removed_rows > 0:
                # Save filtered data back to file
                df_filtered.to_csv(local_path, index=False)
                logger.info(f"Removed {removed_rows} rows containing {self.node_to_remove} from {local_path}")
                return True
            else:
                logger.info(f"No rows with {self.node_to_remove} found in {local_path}")
                return False

        except Exception as e:
            logger.error(f"Error processing file {local_path}: {e}")
            return False

    def upload_file(self, local_path, s3_key):
        """Upload file back to S3"""
        try:
            logger.info(f"Uploading {local_path} to {s3_key}")
            self.s3_client.upload_file(
                str(local_path),
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'text/csv'}
            )
            logger.info(f"Successfully uploaded {s3_key}")
            return True

        except ClientError as e:
            logger.error(f"Error uploading {local_path} to {s3_key}: {e}")
            return False

    def process_bucket(self):
        """Process all CSV files in the bucket"""
        # Get all CSV files
        csv_files = self.list_csv_files()
        if not csv_files:
            logger.warning(f"No CSV files found in bucket {self.bucket_name}")
            return False

        modified_files = 0

        for s3_key in csv_files:
            try:
                # Download file
                local_path = self.download_file(s3_key)
                if not local_path or not local_path.exists():
                    logger.error(f"Failed to download {s3_key}, skipping")
                    continue

                # Process file
                modified = self.process_file(local_path)

                if modified:
                    # Upload modified file back to S3
                    upload_success = self.upload_file(local_path, s3_key)
                    if upload_success:
                        modified_files += 1

                # Clean up local file
                try:
                    os.remove(local_path)
                    logger.debug(f"Removed temporary file: {local_path}")
                except Exception as e:
                    logger.warning(f"Error removing temporary file {local_path}: {e}")

            except Exception as e:
                logger.error(f"Unexpected error processing {s3_key}: {e}")

        logger.info(f"Processing complete. Modified and uploaded {modified_files} files.")
        return True

    def cleanup(self):
        """Clean up temporary directory"""
        try:
            # Remove the temporary directory and all its contents
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Error cleaning up temporary directory: {e}")


def main():
    """Main entry point"""
    try:
        # Configure the script
        if len(sys.argv) > 1:
            node_to_remove = sys.argv[1]
        else:
            node_to_remove = NODE_TO_REMOVE

        if len(sys.argv) > 2:
            bucket_name = sys.argv[2]
        else:
            bucket_name = S3_BUCKET

        logger.info(f"Starting S3 Node Filter to remove '{node_to_remove}' from bucket '{bucket_name}'")

        # Initialize and run the filter
        node_filter = S3NodeFilter(bucket_name, node_to_remove, AWS_PROFILE)
        success = node_filter.process_bucket()

        # Cleanup
        node_filter.cleanup()

        if success:
            logger.info("S3 Node Filter completed successfully")
            return 0
        else:
            logger.error("S3 Node Filter encountered errors")
            return 1

    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())