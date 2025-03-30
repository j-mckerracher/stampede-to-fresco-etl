#!/usr/bin/env python3
import os
import json
import time
import uuid
import shutil
import logging
from datetime import datetime
from pathlib import Path
import traceback
import glob

# Configuration (modify these paths as needed)
SOURCE_DIR = Path(r"P:\Stampede\stampede-ts-fresco-form-daily-2")
DESTINATION_DIR = Path(r"P:\Stampede\sorted-daily-metrics")
SERVER_INPUT_DIR = Path(r"U:\projects\stampede-step-3\input")
SERVER_OUTPUT_DIR = Path(r"U:\projects\stampede-step-3\output")
SERVER_COMPLETE_DIR = Path(r"U:\projects\stampede-step-3\complete")
LOGS_DIR = Path("logs")

# Maximum number of active jobs at once
MAX_ACTIVE_JOBS = 3

# Active jobs tracking
active_jobs = {}


def setup_logging():
    """Configure logging to file and console."""
    LOGS_DIR.mkdir(exist_ok=True)

    log_file = LOGS_DIR / f"metrics_processor_producer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Keep console output as well
        ]
    )

    return logging.getLogger("metrics_processor_producer")


def setup_directories():
    """Create necessary directories if they don't exist."""
    for directory in [SERVER_INPUT_DIR, SERVER_OUTPUT_DIR, SERVER_COMPLETE_DIR, LOGS_DIR]:
        os.makedirs(directory, exist_ok=True)
        logging.info(f"Ensured directory exists: {directory}")

    # Create destination directory if it doesn't exist
    DESTINATION_DIR.mkdir(exist_ok=True, parents=True)
    logging.info(f"Ensured destination directory exists: {DESTINATION_DIR}")


def find_source_folders():
    """Find folders in the source directory that need processing."""
    try:
        # List all subfolders in the source directory
        folders = [f for f in SOURCE_DIR.iterdir() if f.is_dir()]
        return folders
    except Exception as e:
        logging.error(f"Error finding source folders: {str(e)}")
        return []


def check_existing_manifests():
    """Check for existing manifest files in the input directory."""
    existing_folders = set()

    for file in os.listdir(SERVER_INPUT_DIR):
        if file.endswith(".manifest.json"):
            try:
                manifest_path = SERVER_INPUT_DIR / file
                with open(manifest_path, 'r') as f:
                    manifest_data = json.load(f)
                    year_month = manifest_data.get("year_month")
                    if year_month:
                        existing_folders.add(year_month)
                        logging.info(f"Found existing manifest for folder {year_month}")
            except Exception as e:
                logging.error(f"Error reading manifest file {file}: {str(e)}")

    return existing_folders


def check_active_consumer_jobs():
    """Check status files in the complete directory to determine jobs the consumer is processing."""
    active_consumer_jobs = []
    active_folder_names = set()

    # Look for all status files in the complete directory
    for file in os.listdir(SERVER_COMPLETE_DIR):
        if file.endswith(".status"):
            try:
                status_file_path = SERVER_COMPLETE_DIR / file

                with open(status_file_path, 'r') as f:
                    status_data = json.load(f)

                job_id = status_data.get("job_id")
                status = status_data.get("status")
                year_month = status_data.get("year_month")

                # Check if the job is still being processed (not completed or failed)
                if status == "processing" and year_month:
                    active_consumer_jobs.append(job_id)
                    active_folder_names.add(year_month)
                    logging.info(f"Consumer is actively processing folder {year_month} (job {job_id})")
            except Exception as e:
                logging.error(f"Error reading status file {file}: {str(e)}")

    return active_consumer_jobs, active_folder_names


def check_completed_folders():
    """Check for folders that have already been processed and moved to destination."""
    completed_folders = set()

    try:
        # List all subdirectories in the destination directory
        for folder in DESTINATION_DIR.iterdir():
            if folder.is_dir():
                completed_folders.add(folder.name)
    except Exception as e:
        logging.error(f"Error checking completed folders: {str(e)}")

    return completed_folders


def copy_folder_to_input(source_folder, logger):
    """Copy sorted metric files from source to input directory."""
    folder_name = source_folder.name
    input_folder = SERVER_INPUT_DIR / folder_name

    try:
        # Create the destination directory
        input_folder.mkdir(exist_ok=True)

        # Find all sorted performance metric files in the source folder
        sorted_files = list(source_folder.glob("sorted_perf_metrics_*.parquet"))

        if not sorted_files:
            # Check for unsorted files if no sorted files found
            unsorted_files = list(source_folder.glob("perf_metrics_*.parquet"))
            if not unsorted_files:
                logger.warning(f"No metric files found in {source_folder}")
                return False

            logger.info(f"Found {len(unsorted_files)} unsorted files in {source_folder}")

            # Copy unsorted files
            for file in unsorted_files:
                dest_file = input_folder / file.name
                shutil.copy2(file, dest_file)

            logger.info(f"Copied {len(unsorted_files)} unsorted files to {input_folder}")
        else:
            logger.info(f"Found {len(sorted_files)} sorted files in {source_folder}")

            # Copy sorted files
            for file in sorted_files:
                dest_file = input_folder / file.name
                shutil.copy2(file, dest_file)

            logger.info(f"Copied {len(sorted_files)} sorted files to {input_folder}")

        return True

    except Exception as e:
        logger.error(f"Error copying folder {folder_name} to input: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def create_job_manifest(year_month, sorted_files, logger):
    """Create a job manifest file for a folder."""
    try:
        # Generate a unique job ID
        job_id = f"process_{year_month}_{uuid.uuid4().hex[:8]}"

        # Get list of sorted file names
        sorted_file_names = [os.path.basename(f) for f in sorted_files]

        # Create manifest data
        manifest_data = {
            "job_id": job_id,
            "year_month": year_month,
            "sorted_metric_files": sorted_file_names,
            "metric_files": sorted_file_names,  # Include unsorted filenames for backward compatibility
            "accounting_files": [],  # This processor doesn't need accounting files
            "complete_month": True,
            "timestamp": time.time()
        }

        # Write manifest file
        manifest_path = SERVER_INPUT_DIR / f"{job_id}.manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest_data, f)

        logger.info(f"Created job manifest for folder {year_month} with job ID {job_id}")

        # Track this job
        active_jobs[job_id] = {
            "year_month": year_month,
            "status": "pending",
            "start_time": time.time()
        }

        return job_id

    except Exception as e:
        logger.error(f"Error creating job manifest for folder {year_month}: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def move_completed_folder_to_destination(year_month, logger):
    """Move a completed folder from output to destination."""
    try:
        source_dir = SERVER_OUTPUT_DIR / year_month
        destination_dir = DESTINATION_DIR / year_month

        # Create destination directory if it doesn't exist
        destination_dir.mkdir(exist_ok=True, parents=True)

        # Get all processed files in the source directory
        processed_files = list(source_dir.glob("joined_data_*.parquet"))

        if not processed_files:
            logger.warning(f"No processed files found in {source_dir}")
            return False

        logger.info(f"Moving {len(processed_files)} processed files from {source_dir} to {destination_dir}")

        # Move each file
        for file in processed_files:
            dest_file = destination_dir / file.name

            # Check if the file already exists at destination
            if dest_file.exists():
                logger.warning(f"File {dest_file} already exists, skipping")
                continue

            # Copy the file to destination
            shutil.copy2(file, dest_file)

            # Verify file was copied correctly
            if dest_file.exists() and dest_file.stat().st_size == file.stat().st_size:
                # Remove the source file
                file.unlink()
            else:
                logger.error(f"Failed to verify copy of {file.name}")
                return False

        logger.info(f"Successfully moved folder {year_month} to destination")
        return True

    except Exception as e:
        logger.error(f"Error moving folder {year_month} to destination: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def cleanup_completed_folder(year_month, logger):
    """Clean up a folder after successful processing."""
    try:
        # Clean up input directory
        input_folder = SERVER_INPUT_DIR / year_month
        if input_folder.exists():
            shutil.rmtree(input_folder)
            logger.info(f"Removed folder {year_month} from input directory")

        # Clean up output directory
        output_folder = SERVER_OUTPUT_DIR / year_month
        if output_folder.exists() and len(list(output_folder.glob("*"))) == 0:
            output_folder.rmdir()
            logger.info(f"Removed empty folder {year_month} from output directory")

        # Clean up source directory if requested (only remove sorted files)
        source_folder = SOURCE_DIR / year_month
        if source_folder.exists():
            # Find sorted files to delete (keep the original unsorted files)
            sorted_files = list(source_folder.glob("sorted_perf_metrics_*.parquet"))
            for file in sorted_files:
                file.unlink()
                logger.info(f"Removed sorted file {file.name} from source folder")

            logger.info(f"Cleaned up sorted files in source folder {year_month}")

        return True

    except Exception as e:
        logger.error(f"Error cleaning up folder {year_month}: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def check_completed_jobs(logger):
    """Check for completed jobs and process their results."""
    completed_jobs = []

    for job_id in list(active_jobs.keys()):
        status_file = SERVER_COMPLETE_DIR / f"{job_id}.status"

        if status_file.exists():
            try:
                with open(status_file, 'r') as f:
                    status_data = json.load(f)

                status = status_data.get("status")
                year_month = status_data.get("year_month")

                if status == "completed" and year_month:
                    logger.info(f"Job {job_id} for folder {year_month} completed")

                    # Move folder to destination
                    success = move_completed_folder_to_destination(year_month, logger)

                    if success:
                        # Clean up folders
                        cleanup_completed_folder(year_month, logger)
                        completed_jobs.append(job_id)

                elif status == "failed" and year_month:
                    logger.error(f"Job {job_id} for folder {year_month} failed")
                    # Keep the input folder for investigation
                    completed_jobs.append(job_id)

            except Exception as e:
                logger.error(f"Error checking status for job {job_id}: {str(e)}")
                logger.error(traceback.format_exc())

    # Remove completed jobs from tracking
    for job_id in completed_jobs:
        if job_id in active_jobs:
            del active_jobs[job_id]

            # Remove status file
            status_file = SERVER_COMPLETE_DIR / f"{job_id}.status"
            if status_file.exists():
                status_file.unlink()

    return len(completed_jobs)


def distribute_jobs(logger):
    """Create job manifests and distribute work to consumer."""
    jobs_created = 0

    # Check how many jobs the consumer is currently processing
    active_consumer_jobs, active_folder_names = check_active_consumer_jobs()

    # Check for existing manifests in the input directory
    existing_manifests = check_existing_manifests()

    # Check for folders that have already been processed and moved to destination
    completed_folders = check_completed_folders()

    # Combine all folders that should be skipped
    folders_to_skip = active_folder_names.union(existing_manifests).union(completed_folders)

    logger.info(f"Folders to skip (in process, pending, or completed): {len(folders_to_skip)}")

    # If the consumer is already at capacity, don't create more jobs
    total_active_jobs = len(active_consumer_jobs) + len(existing_manifests)
    if total_active_jobs >= MAX_ACTIVE_JOBS:
        logger.info(
            f"Already at maximum capacity with {len(active_consumer_jobs)} active jobs and {len(existing_manifests)} pending jobs")
        return 0

    # Calculate how many more jobs we can create
    available_slots = MAX_ACTIVE_JOBS - total_active_jobs

    # Find source folders
    source_folders = find_source_folders()
    logger.info(f"Found {len(source_folders)} potential source folders")

    # Process folders, skipping those that are already in process or completed
    for folder in source_folders:
        year_month = folder.name

        # Skip folders that are already being processed or completed
        if year_month in folders_to_skip:
            continue

        # Check if folder has sorted files
        sorted_files = list(folder.glob("sorted_perf_metrics_*.parquet"))

        if sorted_files:
            logger.info(f"Found {len(sorted_files)} sorted files in {year_month}")

            # Copy folder to input directory
            success = copy_folder_to_input(folder, logger)

            if success:
                # Create job manifest
                job_id = create_job_manifest(year_month, sorted_files, logger)

                if job_id:
                    jobs_created += 1
                    logger.info(f"Created job for folder {year_month}")

                    # Check if we've reached the limit
                    if jobs_created >= available_slots:
                        logger.info(f"Created {jobs_created} new jobs, reaching limit")
                        break
        else:
            # If no sorted files, check if there are unsorted files
            unsorted_files = list(folder.glob("perf_metrics_*.parquet"))

            if unsorted_files:
                logger.info(f"Found {len(unsorted_files)} unsorted files in {year_month}, needs sorting first")
                # These will be handled by the sorting process
            else:
                logger.warning(f"No metric files found in {year_month}")

    return jobs_created


def main():
    """Main producer function that continuously monitors and distributes processing jobs."""
    # Set up logging and directories
    logger = setup_logging()
    setup_directories()

    logger.info("Starting metrics processor producer")

    cycle_count = 0

    while True:
        try:
            cycle_start = time.time()
            cycle_count += 1
            logger.info(f"Starting producer cycle {cycle_count}")

            # Check for completed jobs and process results
            completed_count = check_completed_jobs(logger)

            # Create and distribute new jobs
            created_count = distribute_jobs(logger)

            # Log cycle statistics
            cycle_duration = time.time() - cycle_start
            logger.info(
                f"Producer cycle {cycle_count} completed in {cycle_duration:.1f}s: {completed_count} jobs completed, {created_count} jobs created")

            # Sleep before next cycle
            time.sleep(60)  # One minute between cycles

        except Exception as e:
            logger.error(f"Error in producer cycle: {str(e)}")
            logger.error(traceback.format_exc())
            time.sleep(30)  # Shorter sleep on error


if __name__ == "__main__":
    main()