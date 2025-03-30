import os
import json
import time
import uuid
import shutil
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('job-metrics-producer')

# Configuration (modify these paths as needed)
SOURCE_METRICS_DIR = r"P:\Stampede\stampede-ts-fresco-form-daily-2"
SOURCE_ACCOUNTING_DIR = r"P:\Stampede\stampede-accounting"
SERVER_INPUT_DIR = r"U:\projects\stampede-step-3\input"
SERVER_COMPLETE_DIR = r"U:\projects\stampede-step-3\complete"
DESTINATION_DIR = r"P:\Stampede\stampede-converted-3"

# Active jobs tracking
MAX_ACTIVE_JOBS = 2
active_jobs = {}


def setup_directories():
    """Create necessary directories if they don't exist."""
    for directory in [SERVER_INPUT_DIR, SERVER_COMPLETE_DIR, DESTINATION_DIR]:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")


def find_source_files():
    """Find metric and accounting files that need processing."""
    # Find metric files
    metric_files = []
    for root, _, files in os.walk(SOURCE_METRICS_DIR):
        for file in files:
            if file.startswith("perf_metrics_") and file.endswith(".parquet"):
                metric_files.append(os.path.join(root, file))

    # Find accounting files
    accounting_files = []
    for root, _, files in os.walk(SOURCE_ACCOUNTING_DIR):
        for file in files:
            if file.endswith(".csv"):
                accounting_files.append(os.path.join(root, file))

    logger.info(f"Found {len(metric_files)} metric files and {len(accounting_files)} accounting files")
    return metric_files, accounting_files


def organize_by_month(metric_files):
    """Group metric files by year-month following ETL workflow."""
    files_by_month = {}

    for file_path in metric_files:
        filename = os.path.basename(file_path)
        # Extract date from filename (perf_metrics_YYYY-MM-DD.parquet)
        date_str = filename.replace("perf_metrics_", "").replace(".parquet", "")
        year_month = date_str[:7]  # YYYY-MM

        if year_month not in files_by_month:
            files_by_month[year_month] = []
        files_by_month[year_month].append(file_path)

    logger.info(f"Organized files into {len(files_by_month)} months")
    return files_by_month


def check_existing_manifests():
    """Check for existing manifest files in the input directory."""
    existing_months = set()

    for file in os.listdir(SERVER_INPUT_DIR):
        if file.endswith(".manifest.json"):
            try:
                manifest_path = os.path.join(SERVER_INPUT_DIR, file)
                with open(manifest_path, 'r') as f:
                    manifest_data = json.load(f)
                    year_month = manifest_data.get("year_month")
                    if year_month:
                        existing_months.add(year_month)
                        logger.info(f"Found existing manifest for {year_month}")
            except Exception as e:
                logger.error(f"Error reading manifest file {file}: {str(e)}")

    return existing_months


def check_active_consumer_jobs():
    """Check status files in the complete directory to determine jobs the consumer is processing."""
    active_consumer_jobs = []

    # Look for all status files in the complete directory
    for file in os.listdir(SERVER_COMPLETE_DIR):
        if file.endswith(".status"):
            status_file_path = os.path.join(SERVER_COMPLETE_DIR, file)

            try:
                with open(status_file_path, 'r') as f:
                    status_data = json.load(f)

                job_id = status_data.get("job_id")
                status = status_data.get("status")

                # Check if the job is still being processed (not completed or failed)
                if status == "processing":
                    active_consumer_jobs.append(job_id)
                    logger.info(f"Consumer is actively processing job {job_id}")
            except Exception as e:
                logger.error(f"Error reading status file {status_file_path}: {str(e)}")

    logger.info(f"Found {len(active_consumer_jobs)} active consumer jobs")
    return active_consumer_jobs


def cleanup_input_files(job_id):
    """Clean up original input files for a completed job."""
    try:
        # Get the manifest file path
        manifest_path = os.path.join(SERVER_INPUT_DIR, f"{job_id}.manifest.json")

        # Check if manifest still exists (it might have been already removed by consumer)
        if not os.path.exists(manifest_path):
            logger.info(f"Manifest for job {job_id} already removed, skipping input cleanup")
            return

        # Load the manifest to get list of files
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)

        # Get the list of files to clean up
        metric_files = manifest_data.get("metric_files", [])
        accounting_files = manifest_data.get("accounting_files", [])
        all_files = metric_files + accounting_files

        # Delete each input file
        deleted_count = 0
        for file_name in all_files:
            file_path = os.path.join(SERVER_INPUT_DIR, file_name)
            if os.path.exists(file_path):
                os.remove(file_path)
                deleted_count += 1

        # Delete the manifest file itself
        if os.path.exists(manifest_path):
            os.remove(manifest_path)
            deleted_count += 1

        logger.info(f"Cleaned up {deleted_count} input files for completed job {job_id}")

    except Exception as e:
        logger.error(f"Error cleaning up input files for job {job_id}: {str(e)}")


def distribute_jobs(files_by_month, accounting_files):
    """Create job manifests and distribute work to server."""
    jobs_created = 0

    # Check how many jobs the consumer is currently processing
    active_consumer_jobs = check_active_consumer_jobs()

    # Get months that are currently being processed
    months_in_process = set()
    for job_id in active_consumer_jobs:
        # Try to extract month from job ID
        if job_id.startswith("job_") and len(job_id.split("_")) >= 3:
            month = job_id.split("_")[1]
            months_in_process.add(month)

    # Check for existing manifests in the input directory
    existing_manifests = check_existing_manifests()

    # Combine all months that should be skipped
    months_to_skip = months_in_process.union(existing_manifests)

    logger.info(f"Months to skip (in process or existing manifests): {months_to_skip}")

    # If the consumer is already at capacity, don't create more jobs
    total_active_jobs = len(active_consumer_jobs) + len(existing_manifests)
    if total_active_jobs >= MAX_ACTIVE_JOBS:
        logger.info(
            f"Already at maximum capacity with {len(active_consumer_jobs)} active jobs and {len(existing_manifests)} pending jobs. Waiting for completion.")
        return 0

    # Calculate how many more jobs we can create
    available_slots = MAX_ACTIVE_JOBS - total_active_jobs

    # Process in order, skipping months that are already in process or have pending manifests
    for year_month, metric_files in files_by_month.items():
        # Skip if this month is already being processed or has a pending manifest
        if year_month in months_to_skip:
            logger.info(f"Month {year_month} is already being processed or has a pending manifest, skipping")
            continue

        # Find matching accounting files
        month_accounting_files = []
        for acc_file in accounting_files:
            if year_month in os.path.basename(acc_file):
                month_accounting_files.append(acc_file)

        if not month_accounting_files:
            logger.warning(f"No accounting files found for {year_month}, skipping")
            continue

        # Create job ID
        job_id = f"job_{year_month}_{uuid.uuid4().hex[:8]}"

        # Create manifest
        manifest_data = {
            "job_id": job_id,
            "year_month": year_month,
            "metric_files": [os.path.basename(f) for f in metric_files],
            "accounting_files": [os.path.basename(f) for f in month_accounting_files],
            "complete_month": True,
            "timestamp": time.time()
        }

        # Copy files to server
        for file in metric_files + month_accounting_files:
            dest_file = os.path.join(SERVER_INPUT_DIR, os.path.basename(file))
            shutil.copy2(file, dest_file)
            logger.info(f"Copied {file} to {dest_file}")

        # Write manifest file
        manifest_path = os.path.join(SERVER_INPUT_DIR, f"{job_id}.manifest.json")
        with open(manifest_path, 'w') as f:
            print("Manifest data:")
            print(manifest_data)
            json.dump(manifest_data, f)

        # Track this job
        active_jobs[job_id] = {
            "year_month": year_month,
            "status": "pending",
            "start_time": time.time()
        }

        logger.info(f"Created job {job_id} for {year_month}")
        jobs_created += 1

        # Check if we've reached the available slots limit
        if jobs_created >= available_slots:
            logger.info(f"Created {jobs_created} new jobs, reaching the limit of {MAX_ACTIVE_JOBS} total active jobs")
            break

    return jobs_created


def check_completed_jobs():
    """Check for completed jobs and process their results."""
    completed_jobs = []

    for job_id in list(active_jobs.keys()):
        status_file = os.path.join(SERVER_COMPLETE_DIR, f"{job_id}.status")

        if os.path.exists(status_file):
            with open(status_file, 'r') as f:
                status_data = json.load(f)

            if status_data.get("status") == "completed":
                year_month = active_jobs[job_id]["year_month"]
                logger.info(f"Job {job_id} for {year_month} completed")

                # Move to final destination
                process_completed_job(job_id, year_month)
                completed_jobs.append(job_id)

    # Remove completed jobs from tracking
    for job_id in completed_jobs:
        del active_jobs[job_id]

    return len(completed_jobs)


def process_completed_job(job_id, year_month):
    """
    Move completed job data to final destination.
    Verifies successful copying before deleting source files.
    Also cleans up input files for completed jobs.
    """
    source_dir = os.path.join(SERVER_COMPLETE_DIR, job_id)
    dest_dir = os.path.join(DESTINATION_DIR, year_month)
    os.makedirs(dest_dir, exist_ok=True)

    # Get all parquet files in the source directory
    source_files = []
    for file in os.listdir(source_dir):
        if file.endswith(".parquet"):
            source_files.append(os.path.join(source_dir, file))

    if not source_files:
        logger.warning(f"No parquet files found in {source_dir}")
        return False

    logger.info(f"Found {len(source_files)} files to copy for job {job_id}")

    # Copy all files to destination
    copied_count = 0
    successful_copies = []

    for source_file in source_files:
        file_name = os.path.basename(source_file)
        dest_file = os.path.join(dest_dir, file_name)

        try:
            # Copy the file
            shutil.copy2(source_file, dest_file)

            # Verify file was copied correctly by checking existence and size
            if os.path.exists(dest_file) and os.path.getsize(dest_file) == os.path.getsize(source_file):
                copied_count += 1
                successful_copies.append(source_file)
                logger.info(f"Successfully copied: {file_name} to {dest_dir}")
            else:
                logger.error(f"Verification failed for {dest_file}")
        except Exception as e:
            logger.error(f"Error copying {source_file} to {dest_file}: {str(e)}")

    # Check if all files were copied successfully
    if copied_count == len(source_files):
        logger.info(f"All {copied_count} files were successfully copied to destination")

        # Delete original files from complete directory
        deleted_count = 0
        for file_path in successful_copies:
            try:
                os.remove(file_path)
                deleted_count += 1
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {str(e)}")

        logger.info(f"Deleted {deleted_count} original files after successful copying")

        # Clean up input files for this job
        try:
            # Get the manifest file path
            manifest_path = os.path.join(SERVER_INPUT_DIR, f"{job_id}.manifest.json")

            # Check if manifest still exists (it might have been already removed by consumer)
            if os.path.exists(manifest_path):
                # Load the manifest to get list of files
                with open(manifest_path, 'r') as f:
                    manifest_data = json.load(f)

                # Get the list of files to clean up
                metric_files = manifest_data.get("metric_files", [])
                accounting_files = manifest_data.get("accounting_files", [])
                all_files = metric_files + accounting_files

                # Delete each input file
                input_deleted_count = 0
                for file_name in all_files:
                    file_path = os.path.join(SERVER_INPUT_DIR, file_name)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        input_deleted_count += 1

                # Delete the manifest file itself
                if os.path.exists(manifest_path):
                    os.remove(manifest_path)
                    input_deleted_count += 1

                logger.info(f"Cleaned up {input_deleted_count} input files for completed job {job_id}")
            else:
                logger.info(f"Manifest for job {job_id} already removed, skipping input cleanup")

        except Exception as e:
            logger.error(f"Error cleaning up input files for job {job_id}: {str(e)}")

        # Try to remove the job directory if it's empty
        try:
            # Check if directory is empty after file removal
            remaining_files = os.listdir(source_dir)
            if not remaining_files:
                shutil.rmtree(source_dir)
                logger.info(f"Removed empty job directory {source_dir}")
            else:
                logger.warning(f"Job directory {source_dir} still contains {len(remaining_files)} files, not removing")
        except Exception as e:
            logger.error(f"Error removing job directory {source_dir}: {str(e)}")

        # Remove the status file
        try:
            status_file = os.path.join(SERVER_COMPLETE_DIR, f"{job_id}.status")
            if os.path.exists(status_file):
                os.remove(status_file)
                logger.info(f"Removed status file for job {job_id}")
        except Exception as e:
            logger.error(f"Error removing status file for job {job_id}: {str(e)}")

        return True
    else:
        logger.warning(
            f"Only {copied_count} out of {len(source_files)} files were copied successfully. Not deleting originals.")
        return False


def main():
    """Main producer function."""
    setup_directories()

    while True:
        try:
            logger.info("Starting ETL producer cycle")

            # Find source files
            metric_files, accounting_files = find_source_files()

            # Organize files by month
            files_by_month = organize_by_month(metric_files)

            # Create and distribute jobs
            jobs_created = distribute_jobs(files_by_month, accounting_files)

            # Check for completed jobs
            jobs_completed = check_completed_jobs()

            logger.info(f"ETL cycle complete: {jobs_created} jobs created, {jobs_completed} jobs completed")

            # Sleep before next cycle
            time.sleep(60)

        except Exception as e:
            logger.error(f"Error in ETL producer cycle: {str(e)}")
            time.sleep(30)


if __name__ == "__main__":
    main()
