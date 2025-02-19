import threading
from typing import Dict, Set
import polars as pl
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from helpers.data_version_manager import DataVersionManager
from helpers.utilities import check_critical_disk_space, cleanup_after_upload


class DataManager:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.monthly_data_lock = threading.Lock()
        self.monthly_data: Dict[str, pl.DataFrame] = {}
        self.version_manager = DataVersionManager(base_dir)

        try:
            self.s3_client = boto3.client('s3')
        except Exception as e:
            print(f"Warning: Could not initialize S3 client: {e}")
            self.s3_client = None
        self.load_checkpoint()

    def upload_to_s3(self, bucket_name="data-transform-stampede", max_retries=3):
        """Upload current version's data to S3 with retry logic"""
        if not self.s3_client:
            print("S3 client not initialized. Skipping upload.")
            return False

        with self.monthly_data_lock:
            try:
                version = self.version_manager.get_current_version()
                save_dir = Path(self.base_dir) / "monthly_data"
                save_dir.mkdir(exist_ok=True)

                # Keep track of successfully uploaded files
                uploaded_files = []

                # First, save all files
                files_to_upload = []
                for month, df in self.monthly_data.items():
                    file_path = save_dir / f"FRESCO_Stampede_ts_{month}_{version}.csv"
                    df.write_csv(file_path)
                    files_to_upload.append(file_path)

                # Then attempt upload
                for file_path in files_to_upload:
                    for attempt in range(max_retries):
                        try:
                            self.s3_client.upload_file(
                                str(file_path),
                                bucket_name,
                                file_path.name
                            )
                            uploaded_files.append(file_path)
                            print(f"Uploaded {file_path.name} to S3")
                            break
                        except ClientError as e:
                            if attempt == max_retries - 1:
                                print(f"Failed to upload {file_path.name} after {max_retries} attempts")
                                return False
                            print(f"Attempt {attempt + 1} failed, retrying...")

                # Only proceed with cleanup if all files were uploaded successfully
                if len(uploaded_files) == len(files_to_upload):
                    if cleanup_after_upload(self.base_dir, version):
                        self.version_manager.increment_version()
                        self.monthly_data.clear()
                        print(f"Successfully uploaded, cleaned up, and cleared version {version}")
                        return True
                    else:
                        print("Upload successful but cleanup failed")
                        return False
                else:
                    print("Not all files were uploaded successfully")
                    return False

            except Exception as e:
                print(f"S3 upload failed: {e}")
                return False

    def manage_storage(self):
        """Check storage and trigger S3 upload if needed"""
        is_safe, is_abundant = check_critical_disk_space()

        if not is_safe:
            print("\nCritical disk space reached. Initiating upload process...")
            if self.upload_to_s3():
                print("Successfully uploaded data to S3 and cleared local storage")
                # Verify cleanup was successful
                _, is_now_abundant = check_critical_disk_space()
                if not is_now_abundant:
                    print("Warning: Storage still low after upload")
                return True
            else:
                print("Failed to upload to S3. Local storage critical!")
                return False
        elif not is_abundant:
            print("\nWarning: Disk space is running low, consider manual intervention")

        return True

    def load_checkpoint(self):
        """Load monthly data from checkpoint"""
        checkpoint_dir = Path(self.base_dir) / "monthly_data_checkpoint"
        if checkpoint_dir.exists():
            try:
                files = list(checkpoint_dir.glob("*.parquet"))
                print(f"Found {len(files)} checkpoint files")
                for i, file in enumerate(files, 1):
                    month = file.stem
                    print(f"Loading checkpoint {i}/{len(files)}: {month}")
                    self.monthly_data[month] = pl.read_parquet(file)
                    print(f"Loaded {len(self.monthly_data[month])} rows for {month}")
                print("All checkpoints loaded")
            except Exception as e:
                print(f"Error loading checkpoint: {e}")

    def save_checkpoint(self):
        """Save monthly data checkpoint"""
        checkpoint_dir = Path(self.base_dir) / "monthly_data_checkpoint"
        checkpoint_dir.mkdir(exist_ok=True)

        try:
            with self.monthly_data_lock:
                for month, df in self.monthly_data.items():
                    file_path = checkpoint_dir / f"{month}.parquet"
                    df.write_parquet(file_path)
                print("Saved data checkpoint")
        except Exception as e:
            print(f"Error saving checkpoint: {e}")

    def cleanup_incomplete_nodes(self, nodes: Set[str]):
        """Remove data from incomplete nodes"""
        print(f"Starting cleanup of nodes: {nodes}")
        with self.monthly_data_lock:
            total_months = len(self.monthly_data)
            for i, (month, df) in enumerate(self.monthly_data.items(), 1):
                print(f"Cleaning month {month} ({i}/{total_months})")
                print(f"Before cleanup: {len(df)} rows")
                rows_with_nodes = df.filter(pl.col("Host").is_in(nodes))
                print(f"Found {len(rows_with_nodes)} rows to remove")
                self.monthly_data[month] = df.filter(~pl.col("Host").is_in(nodes))
                print(f"After cleanup: {len(self.monthly_data[month])} rows")
            print("Saving checkpoint...")
            self.save_checkpoint()
            print("Cleanup complete!")

    def update_monthly_data(self, new_data: Dict[str, pl.DataFrame]):
        """Thread-safe update of monthly data"""
        with self.monthly_data_lock:
            for month, new_df in new_data.items():
                if month in self.monthly_data:
                    # Polars efficient concat and distinct
                    self.monthly_data[month] = pl.concat([
                        self.monthly_data[month],
                        new_df
                    ]).unique()
                else:
                    self.monthly_data[month] = new_df
            self.save_checkpoint()