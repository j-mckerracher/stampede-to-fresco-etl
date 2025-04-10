import os
import logging
import re
import json
import shutil
import time
import uuid
from collections import defaultdict
from tqdm import tqdm
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("job_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# File paths
source_dir = r"P:\Stampede\stampede-ts-to-fresco-ts-chunks-1"
destination_dir = r"P:\Stampede\stampede-ts-fresco-form-daily-2"
server_input_dir = r"U:\projects\stampede-to-fresco-etl\cache\input"
server_complete_dir = r"U:\projects\stampede-to-fresco-etl\cache\complete"
job_tracking_dir = r"job_tracking"

# Configuration
CHUNK_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB in bytes
MAX_ACTIVE_JOBS = 2  # Maximum number of jobs to have active at once
MAX_SERVER_DIR_SIZE = 25 * 1024 * 1024 * 1024  # 25GB max in server directory
CHECK_INTERVAL = 60  # Check for completed jobs every 60 seconds

# Tracking information
active_jobs = {}  # Track active jobs and their status
processed_files = set()  # Track already processed files
month_tracking = {}  # Track month completion status
already_processed_year_months = [
    "2013-02", "2013-03", "2013-04", "2013-05",
    "2013-06", "2013-07", "2013-08", "2013-09"
]


def setup_directories():
    """Ensure all required directories exist"""
    for dir_path in [server_input_dir, server_complete_dir, job_tracking_dir]:
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Ensured directory exists: {dir_path}")


def load_processed_files():
    """Load the list of already processed files"""
    global processed_files

    processed_file_path = os.path.join(job_tracking_dir, "processed_files.json")
    if os.path.exists(processed_file_path):
        try:
            with open(processed_file_path, 'r') as f:
                processed_files = set(json.load(f))
            logger.info(f"Loaded {len(processed_files)} previously processed files")
        except Exception as e:
            logger.error(f"Error loading processed files: {str(e)}")


def save_processed_files():
    """Save the list of processed files"""
    processed_file_path = os.path.join(job_tracking_dir, "processed_files.json")
    try:
        with open(processed_file_path, 'w') as f:
            json.dump(list(processed_files), f)
        logger.debug(f"Saved {len(processed_files)} processed files")
    except Exception as e:
        logger.error(f"Error saving processed files: {str(e)}")


def load_month_tracking():
    """Load month tracking information"""
    global month_tracking

    month_tracking_path = os.path.join(job_tracking_dir, "month_tracking.json")
    if os.path.exists(month_tracking_path):
        try:
            with open(month_tracking_path, 'r') as f:
                month_tracking = json.load(f)
            logger.info(f"Loaded tracking for {len(month_tracking)} months")
        except Exception as e:
            logger.error(f"Error loading month tracking: {str(e)}")
            month_tracking = {}


def save_month_tracking():
    """Save month tracking information"""
    month_tracking_path = os.path.join(job_tracking_dir, "month_tracking.json")
    try:
        with open(month_tracking_path, 'w') as f:
            json.dump(month_tracking, f)
        logger.debug(f"Saved tracking for {len(month_tracking)} months")
    except Exception as e:
        logger.error(f"Error saving month tracking: {str(e)}")


def list_source_files():
    """List all files in the source directory that need processing"""
    files = []
    for root, _, filenames in os.walk(source_dir):
        for filename in filenames:
            if filename.endswith('.parquet'):
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, source_dir)

                # Skip if already processed
                if rel_path in processed_files:
                    continue

                # Check year-month
                match = re.search(r'node_data_(\d{4}-\d{2})\.parquet$', filename)
                if match:
                    year_month = match.group(1)
                    if year_month not in already_processed_year_months:
                        file_size = os.path.getsize(full_path)
                        files.append((rel_path, full_path, file_size, year_month))

    return files


def get_server_directory_size(directory):
    """Get the total size of files in a directory"""
    total_size = 0
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            total_size += os.path.getsize(file_path)
    return total_size


def create_job_chunks(files_to_process):
    """Group files into job chunks by year-month instead of by size"""
    global month_tracking

    # Group files by year-month
    files_by_year_month = defaultdict(list)

    for rel_path, full_path, file_size, year_month in files_to_process:
        files_by_year_month[year_month].append((rel_path, full_path, file_size))

    # Create one job per year-month
    job_chunks = []

    for year_month, files in files_by_year_month.items():
        # Skip months we're already processing
        active_jobs_for_month = [job for job_id, job in active_jobs.items()
                                 if job.get("year_month") == year_month]

        if active_jobs_for_month:
            logger.info(f"Month {year_month} already has active jobs. Skipping.")
            continue

        # Initialize month tracking entry if it doesn't exist yet
        if year_month not in month_tracking:
            month_tracking[year_month] = {
                "total_files": len(files),
                "processed_files": 0,
                "total_jobs": 1,  # Now it's always 1 job per month
                "completed_jobs": 0,
                "is_complete": False,
                "started_at": time.time(),
                "jobs": []
            }

        # Create one job per year-month
        job_id = f"month_{year_month}_{uuid.uuid4().hex[:8]}"

        # Calculate total size
        total_size = sum(file_size for _, _, file_size in files)

        job_chunk = {
            "job_id": job_id,
            "year_month": year_month,
            "files": [f[0] for f in files],  # Only keep relative paths
            "total_size": total_size,
            "complete_month": True  # Flag this as a complete month's data
        }
        job_chunks.append(job_chunk)

        # Update month tracking with job information
        month_tracking[year_month]["jobs"].append(job_id)

    # Save updated month tracking
    save_month_tracking()

    return job_chunks


def copy_files_for_job(job):
    """Copy files for a job to the server input directory"""
    job_id = job["job_id"]
    year_month = job["year_month"]
    files = job["files"]
    complete_month = job.get("complete_month", False)

    # Create a manifest file
    manifest_data = {
        "job_id": job_id,
        "year_month": year_month,
        "files": files,
        "complete_month": complete_month,  # Include the complete_month flag
        "created_at": time.time()
    }

    manifest_path = os.path.join(server_input_dir, f"{job_id}.manifest.json")

    # Copy files to server directory
    copied_files = []
    total_size = 0
    failed_files = []

    logger.info(f"Copying {len(files)} files for job {job_id} (year-month: {year_month}, complete_month: {complete_month})")

    with tqdm(total=len(files), desc=f"Copying files for job {job_id}") as pbar:
        for rel_path in files:
            source_file = os.path.join(source_dir, rel_path)
            dest_file = os.path.join(server_input_dir, os.path.basename(rel_path))

            try:
                # Check if file exists
                if not os.path.exists(source_file):
                    logger.warning(f"Source file does not exist: {source_file}")
                    failed_files.append(rel_path)
                    pbar.update(1)
                    continue

                # Copy file
                shutil.move(source_file, dest_file)
                file_size = os.path.getsize(dest_file)
                total_size += file_size
                copied_files.append(rel_path)

                logger.debug(f"Copied {rel_path} to server ({file_size} bytes)")
            except Exception as e:
                logger.error(f"Error copying file {rel_path}: {str(e)}")
                failed_files.append(rel_path)

            pbar.update(1)

    # Update the manifest with actually copied files
    manifest_data["files"] = copied_files
    manifest_data["total_size"] = total_size
    manifest_data["failed_files"] = failed_files

    # Write the manifest file
    with open(manifest_path, 'w') as f:
        json.dump(manifest_data, f)

    logger.info(f"Created manifest for job {job_id} with {len(copied_files)} files " +
                f"({len(failed_files)} failed), complete_month: {complete_month}")

    # Update job status
    active_jobs[job_id] = {
        "status": "copying_complete",
        "year_month": year_month,
        "copied_files": copied_files,
        "failed_files": failed_files,
        "manifest_path": manifest_path,
        "complete_month": complete_month,
        "start_time": time.time()
    }

    return copied_files, failed_files


def check_completed_jobs():
    """Check for jobs that have been completed by the server"""
    completed_jobs = []

    for job_id in list(active_jobs.keys()):
        # Look for status file
        status_file = os.path.join(server_complete_dir, f"{job_id}.status")
        if os.path.exists(status_file):
            try:
                with open(status_file, 'r') as f:
                    status_data = json.load(f)

                current_status = status_data.get("status", "")

                # Update job status
                active_jobs[job_id]["server_status"] = current_status
                active_jobs[job_id]["last_update"] = time.time()

                if current_status == "completed":
                    logger.info(f"Job {job_id} reported as completed by server")
                    completed_jobs.append(job_id)
                elif current_status == "error":
                    logger.error(
                        f"Job {job_id} reported error: {status_data.get('details', {}).get('error', 'Unknown error')}")
                else:
                    logger.debug(f"Job {job_id} status: {current_status}")
            except Exception as e:
                logger.error(f"Error checking status for job {job_id}: {str(e)}")

    return completed_jobs


def process_completed_month(year_month):
    """
    Process a month that has been completely processed.
    This is where we verify all data is present and move to final destination,
    then delete the source files after successful copying.
    """
    if year_month not in month_tracking:
        logger.warning(f"Month {year_month} not found in tracking data")
        return False

    logger.info(f"Month {year_month} is complete, collecting all processed files")

    # Gather all parquet files from completed jobs for this month
    month_files = []
    job_complete_dirs = []
    manifest_files = []

    for job_id in month_tracking[year_month]["jobs"]:
        job_complete_dir = os.path.join(server_complete_dir, job_id)
        if os.path.exists(job_complete_dir):
            job_complete_dirs.append(job_complete_dir)

            # Find all parquet files in this job directory
            for root, _, filenames in os.walk(job_complete_dir):
                for filename in filenames:
                    if filename.endswith('.parquet'):
                        month_files.append(os.path.join(root, filename))

            # Add manifest file to list for cleanup
            manifest_file = os.path.join(server_complete_dir, f"{job_id}.manifest.json")
            if os.path.exists(manifest_file):
                manifest_files.append(manifest_file)

    if not month_files:
        logger.warning(f"No files found for completed month {year_month}")
        return False

    logger.info(f"Found {len(month_files)} files for month {year_month}")

    # Create a special directory for this month in the destination
    month_dir = os.path.join(destination_dir, year_month)
    os.makedirs(month_dir, exist_ok=True)

    # Copy all files to the destination directory
    copied_count = 0
    successful_copies = []

    for file_path in tqdm(month_files, desc=f"Copying files for month {year_month}"):
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(month_dir, file_name)

        try:
            # Copy file
            shutil.copy2(file_path, dest_path)

            # Verify file was copied correctly by checking existence and size
            if os.path.exists(dest_path) and os.path.getsize(dest_path) == os.path.getsize(file_path):
                copied_count += 1
                successful_copies.append(file_path)
            else:
                logger.error(f"Verification failed for {dest_path}")
        except Exception as e:
            logger.error(f"Error copying {file_path} to {dest_path}: {str(e)}")

    logger.info(f"Copied {copied_count} files for month {year_month} to destination")

    # Verify all files were copied successfully
    if copied_count == len(month_files):
        logger.info(f"All {copied_count} files were successfully copied to destination")

        # Delete original files
        deleted_count = 0
        for file_path in successful_copies:
            try:
                os.remove(file_path)
                deleted_count += 1
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {str(e)}")

        logger.info(f"Deleted {deleted_count} original files after successful copying")

        # Delete job directories
        for job_dir in job_complete_dirs:
            try:
                shutil.rmtree(job_dir)
                logger.info(f"Deleted job directory: {job_dir}")
            except Exception as e:
                logger.error(f"Error deleting job directory {job_dir}: {str(e)}")

        # Delete manifest files
        for manifest_file in manifest_files:
            try:
                os.remove(manifest_file)
                logger.info(f"Deleted manifest file: {manifest_file}")
            except Exception as e:
                logger.error(f"Error deleting manifest file {manifest_file}: {str(e)}")
    else:
        logger.warning(
            f"Only {copied_count} out of {len(month_files)} files were copied successfully. Not deleting originals.")
        return False

    # Update month tracking status
    month_tracking[year_month]["is_complete"] = True
    month_tracking[year_month]["completed_at"] = time.time()
    month_tracking[year_month]["copied_to_destination"] = True
    month_tracking[year_month]["files_copied"] = copied_count
    save_month_tracking()

    # Add this month to already processed months list
    if year_month not in already_processed_year_months:
        already_processed_year_months.append(year_month)

    return True


def process_completed_job(job_id):
    """Process a job that has been completed by the server"""
    global month_tracking

    if job_id not in active_jobs:
        logger.warning(f"Job {job_id} not found in active jobs")
        return False

    job = active_jobs[job_id]
    year_month = job["year_month"]
    job_complete_dir = os.path.join(server_complete_dir, job_id)

    if not os.path.exists(job_complete_dir):
        logger.warning(f"Completed directory for job {job_id} not found")
        return False

    # Check if status file indicates this is a complete month
    status_file = os.path.join(server_complete_dir, f"{job_id}.status")
    ready_for_final_destination = False

    if os.path.exists(status_file):
        try:
            with open(status_file, 'r') as f:
                status_data = json.load(f)
                ready_for_final_destination = status_data.get("details", {}).get("ready_for_final_destination", False)
                if ready_for_final_destination:
                    logger.info(f"Job {job_id} is ready for final destination processing")
        except Exception as e:
            logger.error(f"Error reading status file for job {job_id}: {str(e)}")

    # Stage 1: Mark job as completed
    # Add processed files to the tracking set
    for file_path in job["copied_files"]:
        processed_files.add(file_path)

    # Save updated processed files list
    save_processed_files()

    # Update month tracking
    if year_month in month_tracking:
        month_tracking[year_month]["completed_jobs"] += 1
        month_tracking[year_month]["processed_files"] += len(job["copied_files"])

        # Since we now have 1 job per month, this should always be 100% when it completes
        month_tracking[year_month]["is_complete"] = True

        logger.info(f"Month {year_month} has been fully processed!")

        # Process the completed month if the server indicated it's ready
        if ready_for_final_destination:
            process_completed_month(year_month)

        # Save month tracking data
        save_month_tracking()

    # Stage 2: Job cleanup (only delete job from active_jobs since we're keeping the files
    # until month completion)
    if os.path.exists(status_file):
        try:
            os.remove(status_file)
            logger.debug(f"Deleted status file for job {job_id}")
        except Exception as e:
            logger.error(f"Error deleting status file for job {job_id}: {str(e)}")

    # Remove job from active jobs
    del active_jobs[job_id]

    return True


def monitor_completed_jobs():
    """Thread function to continuously monitor for completed jobs"""
    logger.info("Started monitoring thread for completed jobs")

    while True:
        try:
            # Check for completed jobs
            completed_jobs = check_completed_jobs()

            # Process completed jobs
            for job_id in completed_jobs:
                process_completed_job(job_id)

            # Wait before checking again
            time.sleep(CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"Error in monitoring thread: {str(e)}")
            time.sleep(CHECK_INTERVAL * 2)  # Wait longer after an error


def main():
    start_time = time.time()
    logger.info("Starting job manager...")

    # Setup required directories
    setup_directories()

    # Load list of already processed files
    load_processed_files()

    # Load month tracking information
    load_month_tracking()

    # Start monitoring thread for completed jobs
    monitor_thread = threading.Thread(target=monitor_completed_jobs, daemon=True)
    monitor_thread.start()

    try:
        while True:
            # Check current active jobs count
            current_active_jobs = len(active_jobs)

            if current_active_jobs >= MAX_ACTIVE_JOBS:
                logger.info(f"Maximum active jobs reached ({current_active_jobs}/{MAX_ACTIVE_JOBS}). Waiting...")
                time.sleep(60)
                continue

            # Check server directory size
            server_dir_size = get_server_directory_size(server_input_dir)
            if server_dir_size >= MAX_SERVER_DIR_SIZE:
                logger.info(
                    f"Server directory size limit reached ({server_dir_size}/{MAX_SERVER_DIR_SIZE} bytes). Waiting...")
                time.sleep(60)
                continue

            # List files that need processing
            logger.info("Listing files that need processing...")
            files_to_process = list_source_files()

            if not files_to_process:
                logger.info("No new files to process. Waiting...")
                time.sleep(300)  # Wait 5 minutes before checking again
                continue

            logger.info(f"Found {len(files_to_process)} files that need processing")

            # Create job chunks
            job_chunks = create_job_chunks(files_to_process)
            logger.info(f"Created {len(job_chunks)} job chunks")

            # Submit new jobs up to the maximum
            jobs_to_submit = min(MAX_ACTIVE_JOBS - current_active_jobs, len(job_chunks))

            if jobs_to_submit <= 0:
                logger.info("No capacity for new jobs. Waiting...")
                time.sleep(60)
                continue

            logger.info(f"Submitting {jobs_to_submit} new jobs")

            for i in range(jobs_to_submit):
                job = job_chunks[i]
                job_id = job["job_id"]

                logger.info(f"Submitting job {job_id} with {len(job['files'])} files")

                # Track job before copying to prevent resubmission if interrupted
                active_jobs[job_id] = {
                    "status": "copying",
                    "year_month": job["year_month"],
                    "total_files": len(job["files"]),
                    "start_time": time.time()
                }

                # Copy files to server
                copied_files, failed_files = copy_files_for_job(job)

                logger.info(f"Job {job_id} submitted with {len(copied_files)} files " +
                            f"({len(failed_files)} failed)")

            # Wait a bit before checking for more jobs
            time.sleep(30)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")

    except Exception as e:
        logger.error(f"Unexpected error in main loop: {str(e)}")

    # Wait for monitor thread to finish cleanly
    logger.info("Waiting for monitoring thread to complete...")
    monitor_thread.join(timeout=10)

    # Final save of processed files
    save_processed_files()

    # Final save of month tracking
    save_month_tracking()

    total_runtime = time.time() - start_time
    logger.info(f"Job manager shutting down after running for {total_runtime:.2f} seconds")


if __name__ == "__main__":
    main()