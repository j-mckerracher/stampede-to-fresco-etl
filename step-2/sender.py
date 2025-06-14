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

# --- Configuration ---
# Source data directories
SOURCE_DIR = Path(r"C:\Users\jmckerra\Documents\Stampede\step-1-complete-sorted")
SOURCE_DIR_ACCOUNTING = Path(r"C:\Users\jmckerra\Documents\Stampede\accounting")

# Shared directories with the Consumer
SERVER_INPUT_DIR = Path(r"U:\projects\stampede-step-2\input")  # Sender writes manifests, accounting, metrics here
SERVER_COMPLETE_DIR = Path(r"U:\projects\stampede-step-2\complete")  # Needed to check for processing jobs
SERVER_OUTPUT_DIR = Path(r"U:\projects\stampede-step-2\output")  # Not strictly needed by sender, but kept for context
DESTINATION_DIR = Path(r"C:\Users\jmckerra\Documents\Stampede\step-2-complete")  # Needed to check already completed

# Local directories for the Sender
LOGS_DIR = Path("logs_sender")  # Separate log dir for sender

# Job control
MAX_ACTIVE_JOBS = 2

# --- Global State ---
# Tracks jobs submitted by this sender instance, awaiting completion signal
# (Used mainly for logging and potential future state tracking if sender restarts)
active_jobs = {}


# NOTE: In this split setup, the sender doesn't *actively* use active_jobs
#       after submission, but it's kept here for potential diagnostics
#       and if state persistence were added later. The primary check is
#       based on files present in shared directories.


# --- Functions ---

def setup_logging():
    """Configure logging for the sender script."""
    LOGS_DIR.mkdir(exist_ok=True)
    log_file = LOGS_DIR / f"metrics_processor_sender_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    # Return logger instance
    return logging.getLogger("MetricsSender")


def setup_directories(logger):
    """Create necessary local and shared directories from the sender's perspective."""
    LOGS_DIR.mkdir(exist_ok=True)
    logger.info(f"Ensured sender log directory exists: {LOGS_DIR}")

    # Directories shared with consumer (ensure they exist from sender's perspective)
    # Consumer should also ensure these on its side
    for directory in [SERVER_INPUT_DIR, SERVER_OUTPUT_DIR, SERVER_COMPLETE_DIR]:
        try:
            # Use exist_ok=True and parents=True for robustness
            directory.mkdir(exist_ok=True, parents=True)
            logger.info(f"Ensured shared directory exists: {directory}")
        except OSError as e:
            logger.error(f"Could not create or access directory {directory}: {e}. Check permissions and network paths.")
            # Consider raising an exception if critical directories are inaccessible
            raise  # Re-raise for main loop to catch


def find_source_folders(logger):
    """Find folders in the source directory that might need processing."""
    try:
        if not SOURCE_DIR.exists():
            logger.error(f"Source directory {SOURCE_DIR} not found!")
            return []
        # Filter for directories only
        folders = [f for f in SOURCE_DIR.iterdir() if f.is_dir()]
        logger.debug(f"Found {len(folders)} potential source folders in {SOURCE_DIR}")
        return folders
    except Exception as e:
        logger.error(f"Error finding source folders in {SOURCE_DIR}: {str(e)}")
        return []


def check_pending_manifests(logger):
    """Check for manifest files already submitted (in SERVER_INPUT_DIR)."""
    pending_folders = set()
    pending_job_ids = set()
    try:
        if not SERVER_INPUT_DIR.exists():
            logger.warning(f"Server input directory {SERVER_INPUT_DIR} not found during pending check.")
            return pending_folders, pending_job_ids

        manifest_count = 0
        for file in SERVER_INPUT_DIR.iterdir():
            # Look specifically for manifest files
            if file.is_file() and file.name.endswith(".manifest.json"):
                manifest_count += 1
                try:
                    with open(file, 'r') as f:
                        manifest_data = json.load(f)
                    # Extract key information
                    year_month = manifest_data.get("year_month")
                    job_id = manifest_data.get("job_id")
                    # Add to sets if found
                    if year_month:
                        pending_folders.add(year_month)
                    if job_id:
                        pending_job_ids.add(job_id)
                        logger.info(f"Found pending manifest for folder {year_month} (Job ID: {job_id})")
                except json.JSONDecodeError:
                    logger.warning(f"Could not decode JSON from manifest file: {file}. Skipping.")
                except Exception as e:
                    logger.error(f"Error reading manifest file {file}: {str(e)}")

        logger.info(f"Pending check: Found {manifest_count} manifest files for {len(pending_folders)} unique folders")
        if pending_folders:
            logger.info(f"Pending folders: {sorted(pending_folders)}")
    except Exception as e:
        logger.error(f"Error listing server input directory {SERVER_INPUT_DIR}: {str(e)}")

    return pending_folders, pending_job_ids


def check_processing_jobs_by_status(logger):
    """Check status files in the complete directory to find jobs actively being processed."""
    processing_folder_names = set()
    processing_job_ids = set()
    try:
        if not SERVER_COMPLETE_DIR.exists():
            logger.warning(f"Server complete directory {SERVER_COMPLETE_DIR} not found during processing check.")
            return processing_folder_names, processing_job_ids

        status_count = 0
        for file in SERVER_COMPLETE_DIR.iterdir():
            # Look specifically for status files
            if file.is_file() and file.name.endswith(".status"):
                status_count += 1
                try:
                    with open(file, 'r') as f:
                        status_data = json.load(f)

                    job_id = status_data.get("job_id")
                    status = status_data.get("status")
                    year_month = status_data.get("year_month")  # Consumer MUST write year_month here

                    # Check if the job status is 'processing'
                    if status == "processing" and year_month and job_id:
                        processing_folder_names.add(year_month)
                        processing_job_ids.add(job_id)
                        logger.info(f"Consumer is actively processing folder {year_month} (job {job_id})")
                except json.JSONDecodeError:
                    logger.warning(f"Could not decode JSON from status file: {file}. Skipping.")
                except (OSError, IOError) as e:
                    # Handle file access errors more gracefully (file might be being written/deleted)
                    logger.debug(
                        f"Could not read status file {file}: {str(e)} (this may be normal if file is being processed)")
                except Exception as e:
                    logger.error(f"Error reading status file {file}: {str(e)}")

        logger.info(
            f"Processing check: Found {status_count} status files, {len(processing_folder_names)} folders currently processing")
        if processing_folder_names:
            logger.info(f"Processing folders: {sorted(processing_folder_names)}")
    except Exception as e:
        logger.error(f"Error listing server complete directory {SERVER_COMPLETE_DIR}: {str(e)}")

    return processing_folder_names, processing_job_ids


def check_completed_folders_in_destination(logger):
    """Check for folders already successfully processed and in the final destination."""
    completed_folders = set()
    try:
        if not DESTINATION_DIR.exists():
            logger.warning(f"Final destination directory {DESTINATION_DIR} not found during check.")
            return completed_folders

        logger.info(f"Checking destination directory: {DESTINATION_DIR}")
        folder_count = 0
        for item in DESTINATION_DIR.iterdir():
            if item.is_dir():
                folder_count += 1
                # Simple check: does the directory exist in the final destination?
                # More robust: check if it contains expected output files (e.g., .parquet)
                parquet_files = list(item.glob("*.parquet"))
                if parquet_files:
                    completed_folders.add(item.name)
                    logger.info(
                        f"Found successfully completed folder in destination: {item.name} (contains {len(parquet_files)} parquet files)")
                else:
                    logger.info(
                        f"Found folder in destination, but no parquet files: {item.name}. Treating as incomplete.")

        logger.info(
            f"Destination check: Found {folder_count} total folders, {len(completed_folders)} with parquet files")
        if completed_folders:
            logger.info(f"Completed folders: {sorted(completed_folders)}")

    except Exception as e:
        logger.error(f"Error checking final destination directory {DESTINATION_DIR}: {str(e)}")

    return completed_folders


def copy_files_to_input(source_folder, logger):
    """Copies sorted metric files and the accounting file to SERVER_INPUT_DIR."""
    folder_name = source_folder.name  # This is the year_month
    # Metrics go into a subdirectory named after the year_month within the input dir
    input_folder_for_metrics = SERVER_INPUT_DIR / folder_name
    copied_metric_files = []
    accounting_file_details = {}  # Store name and path

    try:
        # 1. Create the year_month subdirectory in input for metrics
        input_folder_for_metrics.mkdir(exist_ok=True)  # Create if not exists

        # 2. Find and copy sorted performance metric files (*.parquet)
        # Ensure we are looking for the correct pattern with hyphen, not underscore
        sorted_files = list(source_folder.glob("sorted-*.parquet"))
        if not sorted_files:
            logger.warning(f"No 'sorted-*.parquet' files found in {source_folder}. Cannot create job.")
            # Clean up the potentially empty directory created above
            try:
                # Check if directory is empty before removing
                if not any(input_folder_for_metrics.iterdir()):
                    input_folder_for_metrics.rmdir()
            except OSError as e:
                logger.warning(f"Could not remove empty metrics input directory {input_folder_for_metrics}: {e}")
            return False, [], {}  # Indicate failure

        logger.info(f"Found {len(sorted_files)} sorted files in {source_folder}")
        for file in sorted_files:
            dest_file = input_folder_for_metrics / file.name
            logger.debug(f"Copying metric file {file.name} to {dest_file}")
            shutil.copy2(file, dest_file)  # copy2 preserves metadata
            copied_metric_files.append(dest_file)  # Store full path

        logger.info(f"Copied {len(copied_metric_files)} sorted metric files to {input_folder_for_metrics}")

        # 3. Find and copy the corresponding accounting file (.csv)
        # Assumes accounting file is named YYYY-MM.csv in SOURCE_DIR_ACCOUNTING
        accounting_file_name = f"{folder_name}.csv"
        accounting_file_path = SOURCE_DIR_ACCOUNTING / accounting_file_name

        if accounting_file_path.exists() and accounting_file_path.is_file():
            # Copy accounting file directly to the SERVER_INPUT_DIR root (not the subdir)
            dest_accounting_file = SERVER_INPUT_DIR / accounting_file_name
            logger.debug(f"Copying accounting file {accounting_file_path} to {dest_accounting_file}")
            shutil.copy2(accounting_file_path, dest_accounting_file)  # copy2 preserves metadata
            logger.info(f"Copied accounting file {accounting_file_name} to {SERVER_INPUT_DIR}")
            # Store details needed for the manifest
            accounting_file_details = {"name": accounting_file_name,
                                       "path": str(dest_accounting_file)}  # Use string for path in dict
        else:
            logger.warning(
                f"Accounting file {accounting_file_path} not found or is not a file for {folder_name}. Proceeding without it.")
            # Decide if this is critical. Current logic proceeds without it.

        return True, copied_metric_files, accounting_file_details

    except Exception as e:
        logger.error(f"Error copying files for folder {folder_name} to input: {str(e)}")
        logger.error(traceback.format_exc())
        # Attempt cleanup of potentially partially copied files
        if input_folder_for_metrics.exists():
            logger.warning(f"Attempting cleanup of partially copied metric files in {input_folder_for_metrics}")
            shutil.rmtree(input_folder_for_metrics, ignore_errors=True)
        # Use 'locals()' check cautiously, better to check existence
        acc_file_to_remove = SERVER_INPUT_DIR / f"{folder_name}.csv"
        if acc_file_to_remove.exists():
            logger.warning(f"Attempting cleanup of partially copied accounting file {acc_file_to_remove}")
            acc_file_to_remove.unlink(missing_ok=True)
        return False, [], {}


def create_job_manifest(year_month, sorted_metric_files, accounting_file_details, logger):
    """Create a job manifest file in SERVER_INPUT_DIR."""
    try:
        # Generate a unique job ID incorporating the folder name and a random element
        job_id = f"process_{year_month}_{uuid.uuid4().hex[:8]}"

        # Get list of JUST the filenames relative to the input structure consumer expects
        # Metric files are expected within SERVER_INPUT_DIR / year_month /
        sorted_metric_filenames = [f.name for f in sorted_metric_files]
        # Accounting file is expected directly within SERVER_INPUT_DIR /
        accounting_filenames = [accounting_file_details["name"]] if accounting_file_details else []

        manifest_data = {
            "job_id": job_id,
            "year_month": year_month,
            # List of metric filenames expected within the year_month subdirectory
            "metric_files": sorted_metric_filenames,
            # List of accounting filenames expected in the root of input dir
            "accounting_files": accounting_filenames,
            "complete_month": True,  # Assuming year_month folders represent complete months
            "timestamp": time.time()  # Record submission time
        }

        # Manifest file is placed in the root of the SERVER_INPUT_DIR
        manifest_path = SERVER_INPUT_DIR / f"{job_id}.manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest_data, f, indent=4)  # Use indent for readability

        logger.info(f"Created job manifest {manifest_path.name} for folder {year_month} (Job ID: {job_id})")
        if accounting_filenames:
            logger.info(f"Included accounting file '{accounting_filenames[0]}' in manifest.")
        else:
            logger.warning(f"No accounting file included in manifest for {year_month}.")

        # Track this job locally in the sender's active_jobs (optional here, but can be useful)
        active_jobs[job_id] = {
            "year_month": year_month,
            "status": "submitted",  # Mark as submitted
            "start_time": manifest_data["timestamp"],
            "manifest_path": str(manifest_path),  # Store as string
            # Store accounting filename if provided, for potential reference
            "accounting_filename": accounting_filenames[0] if accounting_filenames else None
        }
        return job_id

    except Exception as e:
        logger.error(f"Error creating job manifest for folder {year_month}: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def distribute_jobs(logger):
    """Finds new folders, creates manifests, and copies files to input, respecting limits."""
    jobs_created_this_cycle = 0

    # 1. Check current state of shared directories to determine workload and capacity
    pending_folders, pending_job_ids = check_pending_manifests(logger)
    processing_folders, processing_job_ids = check_processing_jobs_by_status(logger)
    completed_folders_dest = check_completed_folders_in_destination(logger)

    # Combine all folders that are currently being handled or are already done
    # A folder is "active" if it has a pending manifest, is being processed (status=processing),
    # or already exists in the final destination.
    folders_to_skip = pending_folders.union(processing_folders).union(completed_folders_dest)

    # Calculate current active job count based on pending manifests and processing status files
    # This represents jobs the consumer is potentially aware of or working on.
    current_active_job_count = len(pending_job_ids) + len(processing_job_ids)

    logger.info(
        f"Current state: {len(pending_folders)} pending folders (manifests), {len(processing_folders)} processing folders (status), {len(completed_folders_dest)} folders in final destination.")
    logger.info(f"Calculated active/pending job count: {current_active_job_count}. Max allowed: {MAX_ACTIVE_JOBS}.")
    logger.info(
        f"Total folders to skip: {len(folders_to_skip)} - {sorted(folders_to_skip) if folders_to_skip else 'none'}")

    # 2. Check if capacity allows for new jobs
    if current_active_job_count >= MAX_ACTIVE_JOBS:
        logger.info(
            f"Job capacity ({MAX_ACTIVE_JOBS}) reached or exceeded. Waiting for jobs to complete before submitting more.")
        return 0  # Return 0 jobs created

    available_slots = MAX_ACTIVE_JOBS - current_active_job_count
    logger.info(f"Available slots for new jobs: {available_slots}")

    # 3. Find potential source folders to process
    source_folders = find_source_folders(logger)
    if not source_folders:
        logger.info("No source folders found in {SOURCE_DIR}.")
        return 0  # Return 0 jobs created

    logger.info(f"Found {len(source_folders)} potential source folders.")

    # 4. Iterate through source folders and create jobs for eligible ones
    # Sort folders chronologically (assuming YYYY-MM format)
    sorted_folders = sorted(source_folders, key=lambda f: f.name)
    logger.info(
        f"Processing folders in order: {[f.name for f in sorted_folders[:10]]}{'...' if len(sorted_folders) > 10 else ''}")

    for folder in sorted_folders:
        year_month = folder.name  # Extract folder name (e.g., "2023-10")

        # Check if this folder should be skipped
        if year_month in folders_to_skip:
            logger.info(f"Skipping folder {year_month} (already pending, processing, or completed).")
            continue

        # Basic check: Does the folder contain the required sorted parquet files?
        sorted_parquet_files = list(folder.glob("sorted-*.parquet"))
        if not sorted_parquet_files:
            logger.info(f"Skipping folder {year_month}: No 'sorted-*.parquet' files found inside.")
            # Log if unsorted files exist, indicating a prerequisite step might be needed
            unsorted_files = list(folder.glob("perf_metrics_*.parquet"))
            if unsorted_files:
                logger.info(
                    f"Folder {year_month} contains {len(unsorted_files)} unsorted 'perf_metrics_*.parquet' files. Ensure sorting process is complete.")
            continue  # Skip this folder

        logger.info(
            f"Processing eligible source folder: {year_month} (contains {len(sorted_parquet_files)} sorted files)")

        # 5. Copy necessary files (metrics + accounting) to the shared input directory
        copy_success, copied_metric_files, accounting_details = copy_files_to_input(folder, logger)

        if not copy_success:
            logger.error(f"Failed to copy files for {year_month}. Skipping job creation for this folder.")
            continue  # Move to the next folder

        # 6. Create the job manifest file in the shared input directory
        job_id = create_job_manifest(year_month, copied_metric_files, accounting_details, logger)

        if job_id:
            jobs_created_this_cycle += 1
            logger.info(f"Successfully created and submitted job {job_id} for {year_month}.")

            # Check if we have filled all available slots in this cycle
            if jobs_created_this_cycle >= available_slots:
                logger.info(f"Reached available job slot limit ({available_slots}) for this cycle.")
                break  # Stop trying to create more jobs in this iteration
        else:
            # This case indicates copying succeeded but manifest creation failed.
            # This is problematic as files are left in the input directory without a manifest.
            logger.error(
                f"Failed to create manifest for {year_month} AFTER copying files. Manual cleanup of files in {SERVER_INPUT_DIR} might be needed for {year_month}.")
            # Consider adding automatic cleanup logic here for the copied files if manifest fails.

    logger.info(
        f"Finished processing cycle. Created {jobs_created_this_cycle} new jobs out of {available_slots} available slots.")
    return jobs_created_this_cycle


def main():
    """Main function for the sender script."""
    logger = setup_logging()
    logger.info("--- Starting Metrics Processor Sender ---")
    try:
        setup_directories(logger)  # Setup directories at the start
    except Exception as e:
        logger.critical(f"Failed to setup critical directories: {e}. Exiting.")
        return  # Exit if basic setup fails

    logger.info(f"Source Metrics Dir:   {SOURCE_DIR}")
    logger.info(f"Source Accounting Dir:{SOURCE_DIR_ACCOUNTING}")
    logger.info(f"Shared Input Dir:     {SERVER_INPUT_DIR} (Manifests, Accounting, Metric Subdirs)")
    logger.info(f"Shared Complete Dir:  {SERVER_COMPLETE_DIR} (Status files checked)")
    logger.info(f"Final Destination Dir:{DESTINATION_DIR} (Checked for existing completion)")
    logger.info(f"Max Concurrent Jobs:  {MAX_ACTIVE_JOBS}")

    cycle_count = 0
    while True:
        cycle_start_time = time.time()
        cycle_count += 1
        logger.info(f"--- Sender Cycle {cycle_count} Start ---")

        try:
            # The sender's primary role in the loop is to distribute new jobs
            created_count = distribute_jobs(logger)
            logger.info(f"Cycle {cycle_count}: Submitted {created_count} new jobs.")

        except Exception as e:
            # Catch broad exceptions in the main loop to keep the sender running
            logger.error(f"!!! Unhandled error in sender main cycle {cycle_count}: {str(e)}")
            logger.error(traceback.format_exc())
            # Consider different sleep behavior on errors if needed

        cycle_duration = time.time() - cycle_start_time
        logger.info(f"--- Sender Cycle {cycle_count} End (Duration: {cycle_duration:.2f}s) ---")

        # Wait before starting the next cycle
        sleep_time = 10  # Seconds
        logger.debug(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
