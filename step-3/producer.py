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

# Path configuration (ensure these are correct for the Sender's environment)
SOURCE_DIR = Path(r"P:\Stampede\sorted-daily-metrics")
SOURCE_DIR_ACCOUNTING = Path(r"P:\Stampede\stampede-accounting")
# *** Final destination for successfully processed data ***
DESTINATION_DIR = Path(r"P:\Stampede\stampede-converted-3")
# *** Directories shared with the Consumer ***
SERVER_INPUT_DIR = Path(r"U:\projects\stampede-step-3\input")      # Sender writes manifests, accounting, metrics here
SERVER_OUTPUT_DIR = Path(r"U:\projects\stampede-step-3\output")     # Consumer writes intermediate results here (job_id based)
SERVER_COMPLETE_DIR = Path(r"U:\projects\stampede-step-3\complete") # Consumer writes final results (job_id based) and status files here
LOGS_DIR = Path("logs") # Local logs for the sender

# Maximum number of active jobs at once (pending in input + processing by consumer)
MAX_ACTIVE_JOBS = 2

# Active jobs tracking (jobs submitted by sender, awaiting completion signal)
active_jobs = {}


def setup_logging():
    """Configure logging to file and console."""
    LOGS_DIR.mkdir(exist_ok=True)
    log_file = LOGS_DIR / f"metrics_processor_sender_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("metrics_processor_sender") # Changed logger name


def setup_directories():
    """Create necessary directories if they don't exist."""
    # Directories managed by the sender/producer
    DESTINATION_DIR.mkdir(exist_ok=True, parents=True)
    LOGS_DIR.mkdir(exist_ok=True)
    logging.info(f"Ensured sender directory exists: {DESTINATION_DIR}")
    logging.info(f"Ensured sender log directory exists: {LOGS_DIR}")

    # Directories shared with consumer (ensure they exist from sender's perspective)
    # Consumer should also ensure these on its side
    for directory in [SERVER_INPUT_DIR, SERVER_OUTPUT_DIR, SERVER_COMPLETE_DIR]:
        try:
            directory.mkdir(exist_ok=True, parents=True)
            logging.info(f"Ensured shared directory exists: {directory}")
        except OSError as e:
            # Handle potential permission issues or other OS errors gracefully
            logging.error(f"Could not create or access directory {directory}: {e}. Check permissions and network paths.")
            # Depending on severity, might want to exit here
            # raise


def find_source_folders():
    """Find folders in the source directory that need processing."""
    try:
        if not SOURCE_DIR.exists():
            logging.error(f"Source directory {SOURCE_DIR} not found!")
            return []
        folders = [f for f in SOURCE_DIR.iterdir() if f.is_dir()]
        return folders
    except Exception as e:
        logging.error(f"Error finding source folders in {SOURCE_DIR}: {str(e)}")
        return []


def check_pending_manifests():
    """Check for manifest files already submitted but not yet processed (in SERVER_INPUT_DIR)."""
    pending_folders = set()
    pending_job_ids = set()
    try:
        if not SERVER_INPUT_DIR.exists():
            logging.warning(f"Server input directory {SERVER_INPUT_DIR} not found during check.")
            return pending_folders, pending_job_ids

        for file in SERVER_INPUT_DIR.iterdir():
            if file.name.endswith(".manifest.json"):
                try:
                    with open(file, 'r') as f:
                        manifest_data = json.load(f)
                    year_month = manifest_data.get("year_month")
                    job_id = manifest_data.get("job_id")
                    if year_month:
                        pending_folders.add(year_month)
                        logging.debug(f"Found pending manifest for folder {year_month} (Job ID: {job_id})")
                    if job_id:
                        pending_job_ids.add(job_id)

                except Exception as e:
                    logging.error(f"Error reading manifest file {file}: {str(e)}")
    except Exception as e:
        logging.error(f"Error listing server input directory {SERVER_INPUT_DIR}: {str(e)}")

    return pending_folders, pending_job_ids


def check_processing_jobs_by_status():
    """Check status files in the complete directory to find jobs actively being processed by the consumer."""
    processing_folder_names = set()
    processing_job_ids = set()
    try:
        if not SERVER_COMPLETE_DIR.exists():
            logging.warning(f"Server complete directory {SERVER_COMPLETE_DIR} not found during check.")
            return processing_folder_names, processing_job_ids

        for file in SERVER_COMPLETE_DIR.iterdir():
            if file.name.endswith(".status"):
                try:
                    with open(file, 'r') as f:
                        status_data = json.load(f)

                    job_id = status_data.get("job_id")
                    status = status_data.get("status")
                    year_month = status_data.get("year_month") # Consumer MUST write year_month here

                    # Check if the job is currently being processed
                    if status == "processing" and year_month and job_id:
                        processing_folder_names.add(year_month)
                        processing_job_ids.add(job_id)
                        logging.debug(f"Consumer is actively processing folder {year_month} (job {job_id})")
                except json.JSONDecodeError:
                    logging.warning(f"Could not decode JSON from status file: {file}. Skipping.")
                except Exception as e:
                    logging.error(f"Error reading status file {file}: {str(e)}")
    except Exception as e:
        logging.error(f"Error listing server complete directory {SERVER_COMPLETE_DIR}: {str(e)}")

    return processing_folder_names, processing_job_ids


def check_completed_folders_in_destination():
    """Check for folders that have already been successfully processed and moved to the final destination."""
    completed_folders = set()
    try:
        if not DESTINATION_DIR.exists():
            logging.warning(f"Final destination directory {DESTINATION_DIR} not found during check.")
            return completed_folders

        for item in DESTINATION_DIR.iterdir():
            if item.is_dir():
                # Check if the folder contains expected output files (e.g., .parquet)
                # This helps distinguish from potentially empty folders.
                if any(item.glob("*.parquet")):
                     completed_folders.add(item.name)
                     logging.debug(f"Found successfully completed folder in destination: {item.name}")
                else:
                     logging.debug(f"Found folder in destination, but no parquet files: {item.name}. Treating as incomplete.")

    except Exception as e:
        logging.error(f"Error checking final destination directory {DESTINATION_DIR}: {str(e)}")
    return completed_folders


def copy_files_to_input(source_folder, logger):
    """Copies sorted metric files and the accounting file to SERVER_INPUT_DIR."""
    folder_name = source_folder.name # This is the year_month
    input_folder_for_metrics = SERVER_INPUT_DIR / folder_name
    copied_metric_files = []
    accounting_file_details = {} # Store name and path

    try:
        # 1. Create the year_month subdirectory in input for metrics
        input_folder_for_metrics.mkdir(exist_ok=True)

        # 2. Find and copy sorted performance metric files
        sorted_files = list(source_folder.glob("sorted_perf_metrics_*.parquet"))
        if not sorted_files:
            logger.warning(f"No 'sorted_perf_metrics_*.parquet' files found in {source_folder}. Cannot create job.")
            # Clean up potentially created directory
            try:
                if not any(input_folder_for_metrics.iterdir()):
                     input_folder_for_metrics.rmdir()
            except OSError:
                 pass # Ignore if removal fails (e.g., dir not empty unexpectedly)
            return False, [], {} # Indicate failure

        logger.info(f"Found {len(sorted_files)} sorted files in {source_folder}")
        for file in sorted_files:
            dest_file = input_folder_for_metrics / file.name
            logger.debug(f"Copying metric file {file.name} to {dest_file}")
            shutil.copy2(file, dest_file)
            copied_metric_files.append(dest_file) # Store full path for manifest

        logger.info(f"Copied {len(copied_metric_files)} sorted metric files to {input_folder_for_metrics}")

        # 3. Find and copy the corresponding accounting file
        accounting_file_name = f"{folder_name}.csv"
        accounting_file_path = SOURCE_DIR_ACCOUNTING / accounting_file_name

        if accounting_file_path.exists():
            # Copy accounting file directly to the SERVER_INPUT_DIR root
            dest_accounting_file = SERVER_INPUT_DIR / accounting_file_name
            logger.debug(f"Copying accounting file {accounting_file_path} to {dest_accounting_file}")
            shutil.copy2(accounting_file_path, dest_accounting_file)
            logger.info(f"Copied accounting file {accounting_file_name} to {SERVER_INPUT_DIR}")
            accounting_file_details = {"name": accounting_file_name, "path": dest_accounting_file}
        else:
            logger.warning(f"Accounting file {accounting_file_path} not found for {folder_name}. Proceeding without it.")
            # Decide if this is a critical error. For now, allow processing without it.

        return True, copied_metric_files, accounting_file_details

    except Exception as e:
        logger.error(f"Error copying files for folder {folder_name} to input: {str(e)}")
        logger.error(traceback.format_exc())
        # Attempt cleanup of potentially partially copied files
        if input_folder_for_metrics.exists():
             shutil.rmtree(input_folder_for_metrics, ignore_errors=True)
        if 'dest_accounting_file' in locals() and dest_accounting_file.exists():
             dest_accounting_file.unlink(missing_ok=True)
        return False, [], {}


def create_job_manifest(year_month, sorted_metric_files, accounting_file_details, logger):
    """Create a job manifest file for a folder."""
    try:
        job_id = f"process_{year_month}_{uuid.uuid4().hex[:8]}"

        # Get list of JUST the filenames relative to the input structure consumer expects
        # Consumer expects files within SERVER_INPUT_DIR / year_month /
        sorted_metric_filenames = [f.name for f in sorted_metric_files]
        # Consumer expects accounting file within SERVER_INPUT_DIR /
        accounting_filenames = [accounting_file_details["name"]] if accounting_file_details else []

        manifest_data = {
            "job_id": job_id,
            "year_month": year_month,
            # "metric_files": sorted_metric_filenames, # Keep if consumer uses this? Consumer code uses both
            "metric_files": sorted_metric_filenames, # Consumer uses this primarily now
            "accounting_files": accounting_filenames, # Consumer expects list of filenames in SERVER_INPUT_DIR root
            "complete_month": True,  # Assuming year_month folders represent complete months
            "timestamp": time.time()
        }

        manifest_path = SERVER_INPUT_DIR / f"{job_id}.manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest_data, f, indent=4) # Add indent for readability

        logger.info(f"Created job manifest {manifest_path.name} for folder {year_month} (Job ID: {job_id})")
        if accounting_filenames:
            logger.info(f"Included accounting file '{accounting_filenames[0]}' in manifest.")
        else:
            logger.warning(f"No accounting file included in manifest for {year_month}.")

        # Track this job locally in the sender
        active_jobs[job_id] = {
            "year_month": year_month,
            "status": "submitted", # Changed from pending to submitted
            "start_time": time.time(),
            "manifest_path": manifest_path,
             # Store accounting filename for potential later cleanup needs
            "accounting_filename": accounting_filenames[0] if accounting_filenames else None
        }
        return job_id

    except Exception as e:
        logger.error(f"Error creating job manifest for folder {year_month}: {str(e)}")
        logger.error(traceback.format_exc())
        return None


# <<< MODIFIED FUNCTION >>>
def move_completed_job_to_destination(job_id, year_month, logger):
    """Move successfully processed data from SERVER_COMPLETE_DIR/job_id to DESTINATION_DIR/year_month."""
    source_job_dir = SERVER_COMPLETE_DIR / job_id
    final_destination_dir = DESTINATION_DIR / year_month

    try:
        if not source_job_dir.exists() or not source_job_dir.is_dir():
            logger.warning(f"Source directory for completed job {job_id} not found at {source_job_dir}. Cannot move data.")
            # Check if data is already in final destination (e.g., retry scenario)
            if final_destination_dir.exists() and any(final_destination_dir.glob("*.parquet")):
                 logger.info(f"Data for {year_month} already exists in final destination {final_destination_dir}. Assuming prior success.")
                 return True # Treat as success if destination already populated
            return False # Indicate failure if source doesn't exist and destination is empty

        # Ensure final destination year_month directory exists
        final_destination_dir.mkdir(exist_ok=True, parents=True)
        logger.info(f"Ensured final destination directory exists: {final_destination_dir}")

        # Find processed parquet files in the source job directory
        processed_files = list(source_job_dir.glob("*.parquet"))

        if not processed_files:
            logger.warning(f"No processed parquet files found in {source_job_dir} for job {job_id}. Moving status file only.")
            # Still consider this a "success" in terms of workflow if status was 'completed_no_data'
            # Let cleanup handle the empty source dir

        files_moved_count = 0
        for file_path in processed_files:
            dest_file = final_destination_dir / file_path.name
            try:
                logger.debug(f"Moving {file_path.name} from {source_job_dir} to {final_destination_dir}")
                # Use move for efficiency, but copy2 might be safer across different filesystems/mounts
                # shutil.move(str(file_path), str(dest_file)) # Ensure paths are strings if needed
                shutil.copy2(file_path, dest_file) # Use copy then delete source for safety
                file_path.unlink() # Delete source after successful copy
                files_moved_count += 1
            except Exception as e:
                 logger.error(f"Error moving file {file_path.name} for job {job_id}: {e}")
                 # Decide how to handle partial failures - rollback? Log and continue?
                 # For now, log and continue, but this might leave inconsistent state.
                 return False # Treat partial failure as overall failure for this move operation

        logger.info(f"Successfully moved {files_moved_count} processed files for job {job_id} to {final_destination_dir}")

        # Optionally move the status file to the destination for archival
        status_file_src = SERVER_COMPLETE_DIR / f"{job_id}.status"
        status_file_dest = final_destination_dir / f"{job_id}.status.archive" # Rename to avoid confusion
        if status_file_src.exists():
            try:
                shutil.move(str(status_file_src), str(status_file_dest))
                logger.info(f"Archived status file to {status_file_dest}")
            except Exception as e:
                logger.warning(f"Could not archive status file {status_file_src}: {e}")
                # Don't fail the whole process for this, but log it. Cleanup will remove original later.

        return True

    except Exception as e:
        logger.error(f"Error moving completed job {job_id} data to destination: {str(e)}")
        logger.error(traceback.format_exc())
        return False


# <<< MODIFIED FUNCTION >>>
def cleanup_job_artifacts(job_id, year_month, logger, cleanup_source=True):
    """Clean up intermediate files and directories for a job."""
    logger.info(f"Starting cleanup for job {job_id} (year_month: {year_month})")
    try:
        # 1. Clean up input directory metric files (SERVER_INPUT_DIR / year_month)
        input_metrics_dir = SERVER_INPUT_DIR / year_month
        if input_metrics_dir.exists() and input_metrics_dir.is_dir():
            logger.debug(f"Removing input metrics directory: {input_metrics_dir}")
            shutil.rmtree(input_metrics_dir, ignore_errors=True)
        else:
            logger.debug(f"Input metrics directory not found, skipping cleanup: {input_metrics_dir}")

        # 2. Clean up input directory accounting file (SERVER_INPUT_DIR / year_month.csv)
        # Retrieve filename from active_jobs if possible, otherwise infer
        accounting_filename = active_jobs.get(job_id, {}).get("accounting_filename")
        if not accounting_filename:
            accounting_filename = f"{year_month}.csv" # Fallback to inference
            logger.debug(f"Inferred accounting filename for cleanup: {accounting_filename}")

        if accounting_filename:
             input_accounting_file = SERVER_INPUT_DIR / accounting_filename
             if input_accounting_file.exists():
                 logger.debug(f"Removing input accounting file: {input_accounting_file}")
                 input_accounting_file.unlink(missing_ok=True)
             else:
                 logger.debug(f"Input accounting file not found, skipping cleanup: {input_accounting_file}")
        else:
            logger.debug(f"No accounting file associated with job {job_id}, skipping its cleanup.")

        # 3. Clean up output directory (SERVER_OUTPUT_DIR / job_id) - Consumer's intermediate batches
        output_job_dir = SERVER_OUTPUT_DIR / job_id
        if output_job_dir.exists() and output_job_dir.is_dir():
            logger.debug(f"Removing output job directory: {output_job_dir}")
            shutil.rmtree(output_job_dir, ignore_errors=True)
        else:
            logger.debug(f"Output job directory not found, skipping cleanup: {output_job_dir}")

        # 4. Clean up complete directory (SERVER_COMPLETE_DIR / job_id) - Consumer's final staging
        complete_job_dir = SERVER_COMPLETE_DIR / job_id
        if complete_job_dir.exists() and complete_job_dir.is_dir():
            # This should ideally be empty if move_completed_job_to_destination worked
            # But clean it up regardless to handle partial moves or leftover files
            logger.debug(f"Removing complete job directory: {complete_job_dir}")
            shutil.rmtree(complete_job_dir, ignore_errors=True)
        else:
             logger.debug(f"Complete job directory not found, skipping cleanup: {complete_job_dir}")

        # 5. Clean up the original sorted metric files from SOURCE_DIR (Optional)
        if cleanup_source:
            source_dir_year_month = SOURCE_DIR / year_month
            if source_dir_year_month.exists():
                files_to_delete = list(source_dir_year_month.glob("sorted_perf_metrics_*.parquet"))
                if files_to_delete:
                    logger.info(f"Cleaning up {len(files_to_delete)} sorted source files from {source_dir_year_month}")
                    for f in files_to_delete:
                        f.unlink(missing_ok=True)
            else:
                 logger.debug(f"Original source directory not found, skipping source cleanup: {source_dir_year_month}")

        logger.info(f"Cleanup finished for job {job_id}")
        return True

    except Exception as e:
        logger.error(f"Error during cleanup for job {job_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return False


# <<< MODIFIED FUNCTION >>>
def check_completed_jobs(logger):
    """Check SERVER_COMPLETE_DIR for job status files and handle completion/failure."""
    jobs_finalized = 0
    job_ids_to_remove_from_active = []

    try:
        if not SERVER_COMPLETE_DIR.exists():
            logger.warning(f"Server complete directory {SERVER_COMPLETE_DIR} not found. Cannot check job status.")
            return 0

        status_files = list(SERVER_COMPLETE_DIR.glob("*.status"))
        logger.debug(f"Found {len(status_files)} potential status files in {SERVER_COMPLETE_DIR}")

        for status_file in status_files:
            job_id_from_filename = status_file.stem # Job ID is the filename part before .status
            status_data = None # Reset for each file

            try:
                with open(status_file, 'r') as f:
                    status_data = json.load(f)

                # Validate essential data from status file
                status = status_data.get("status")
                job_id = status_data.get("job_id")
                year_month = status_data.get("year_month") # Must be present

                # Basic validation
                if not all([status, job_id, year_month]):
                     logger.warning(f"Status file {status_file.name} is missing required fields (status, job_id, year_month). Skipping.")
                     # Consider moving/deleting invalid status files after logging
                     # status_file.rename(status_file.with_suffix('.status.invalid'))
                     continue

                # Double check filename job_id matches content job_id
                if job_id_from_filename != job_id:
                    logger.warning(f"Mismatch between status filename job ID ('{job_id_from_filename}') and content job ID ('{job_id}') in {status_file.name}. Using content ID.")
                    # Decide on strategy: trust content? skip? For now, trust content.

                # --- Handle COMPLETED jobs ---
                if status in ["completed", "completed_no_data"]:
                    logger.info(f"Detected completed job: {job_id} for {year_month} (status: {status})")

                    # 1. Move data to final destination
                    move_success = move_completed_job_to_destination(job_id, year_month, logger)

                    if move_success:
                        logger.info(f"Successfully moved data for completed job {job_id} to final destination.")
                        # 2. Clean up all intermediate artifacts
                        # Set cleanup_source=True to remove original sorted files
                        cleanup_success = cleanup_job_artifacts(job_id, year_month, logger, cleanup_source=True)
                        if not cleanup_success:
                            logger.error(f"Cleanup failed for completed job {job_id}, but data was moved. Manual cleanup might be needed.")
                        # Mark job for removal from tracking, even if cleanup had issues
                        job_ids_to_remove_from_active.append(job_id)
                        jobs_finalized += 1
                    else:
                        logger.error(f"Failed to move data for completed job {job_id}. Artifacts will NOT be cleaned up automatically. Manual intervention needed.")
                        # Do NOT remove status file or job from active tracking yet if move failed. Retry might be desired.

                # --- Handle FAILED jobs ---
                elif status == "failed":
                    logger.error(f"Detected failed job: {job_id} for {year_month}. Error details should be in consumer logs.")
                    error_details = status_data.get("error", "No details provided.")
                    logger.error(f"Job {job_id} failure reason: {error_details}")

                    # 1. Clean up intermediate OUTPUT and COMPLETE directories ONLY.
                    # Keep INPUT files (metrics subdir + accounting file) for debugging.
                    logger.info(f"Cleaning up output/complete artifacts for failed job {job_id}, keeping input.")
                    # Pass cleanup_source=False to preserve original source files
                    cleanup_partial_success = cleanup_job_artifacts(job_id, year_month, logger, cleanup_source=False)
                    if not cleanup_partial_success:
                         logger.warning(f"Partial cleanup for failed job {job_id} encountered issues.")

                    # 2. Archive the status file to input dir for reference? Or just delete? Delete for now.
                    try:
                        status_file.unlink()
                        logger.info(f"Removed status file for failed job {job_id}.")
                    except OSError as e:
                        logger.error(f"Failed to remove status file {status_file.name} for failed job: {e}")


                    # 3. Mark job for removal from tracking
                    job_ids_to_remove_from_active.append(job_id)
                    jobs_finalized += 1 # Count failed as finalized in this cycle

                # --- Handle UNKNOWN/OTHER statuses ---
                # Includes "processing" which we check separately in distribute_jobs
                # Or any unexpected status values
                elif status != "processing":
                    logger.warning(f"Detected job {job_id} with unexpected status '{status}' in {status_file.name}. Ignoring.")
                    # Optionally delete or rename these unexpected status files

            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in status file: {status_file}. Moving to invalid.")
                try:
                     status_file.rename(status_file.with_suffix('.status.invalid'))
                except OSError as e:
                     logger.error(f"Could not rename invalid status file {status_file.name}: {e}")
            except Exception as e:
                logger.error(f"Error processing status file {status_file}: {str(e)}")
                logger.error(traceback.format_exc())
                # Decide if we should attempt to remove the job ID from active tracking

    except Exception as e:
         logger.error(f"General error checking completed jobs: {str(e)}")
         logger.error(traceback.format_exc())


    # Remove finalized jobs from local tracking
    removed_count = 0
    for job_id in job_ids_to_remove_from_active:
        if job_id in active_jobs:
            del active_jobs[job_id]
            removed_count += 1
            logger.debug(f"Removed job {job_id} from active tracking.")
        else:
             logger.warning(f"Attempted to remove job {job_id} from tracking, but it was not found.")

    if removed_count > 0:
         logger.info(f"Removed {removed_count} finalized jobs from active tracking.")

    return jobs_finalized # Return count of jobs handled (completed or failed)


def distribute_jobs(logger):
    """Finds new folders, creates manifests, and copies files to input."""
    jobs_created_this_cycle = 0

    # 1. Check current state
    pending_folders, pending_job_ids = check_pending_manifests()
    processing_folders, processing_job_ids = check_processing_jobs_by_status()
    completed_folders_dest = check_completed_folders_in_destination()

    # Combine all folders that should be skipped
    # A folder is "active" if it's pending submission, being processed, or already in the final destination.
    folders_to_skip = pending_folders.union(processing_folders).union(completed_folders_dest)
    current_active_job_count = len(pending_job_ids) + len(processing_job_ids)

    logger.info(f"Current state: {len(pending_folders)} pending folders, {len(processing_folders)} processing folders, {len(completed_folders_dest)} folders in final destination.")
    logger.info(f"Total active/pending job count: {current_active_job_count}. Max allowed: {MAX_ACTIVE_JOBS}.")
    logger.debug(f"Folders to skip: {folders_to_skip}")

    # 2. Check capacity
    if current_active_job_count >= MAX_ACTIVE_JOBS:
        logger.info(f"Job capacity ({MAX_ACTIVE_JOBS}) reached. Waiting for jobs to complete.")
        return 0 # No capacity to create new jobs

    available_slots = MAX_ACTIVE_JOBS - current_active_job_count
    logger.info(f"Available slots for new jobs: {available_slots}")

    # 3. Find potential source folders
    source_folders = find_source_folders()
    if not source_folders:
        logger.info("No source folders found to process.")
        return 0

    logger.info(f"Found {len(source_folders)} potential source folders in {SOURCE_DIR}")

    # 4. Iterate and create jobs for eligible folders
    for folder in sorted(source_folders, key=lambda f: f.name): # Process chronologically
        year_month = folder.name

        if year_month in folders_to_skip:
            logger.debug(f"Skipping folder {year_month} (already pending, processing, or completed).")
            continue

        # Check if folder has the required sorted files
        if not any(folder.glob("sorted_perf_metrics_*.parquet")):
             logger.debug(f"Skipping folder {year_month}: No 'sorted_perf_metrics_*.parquet' files found.")
             # Log if unsorted files exist?
             if any(folder.glob("perf_metrics_*.parquet")):
                  logger.info(f"Folder {year_month} contains unsorted 'perf_metrics_*.parquet' files. Needs sorting first (external process).")
             continue

        logger.info(f"Processing eligible folder: {year_month}")

        # 5. Copy files to input directory
        copy_success, copied_metric_files, accounting_details = copy_files_to_input(folder, logger)

        if not copy_success:
            logger.error(f"Failed to copy files for {year_month}. Skipping job creation.")
            continue # Skip this folder if copy failed

        # 6. Create job manifest
        job_id = create_job_manifest(year_month, copied_metric_files, accounting_details, logger)

        if job_id:
            jobs_created_this_cycle += 1
            logger.info(f"Successfully created job {job_id} for {year_month}.")

            # Check if we've reached the limit for this cycle
            if jobs_created_this_cycle >= available_slots:
                logger.info(f"Reached available job slot limit ({available_slots}) for this cycle.")
                break # Stop creating jobs for this cycle
        else:
            logger.error(f"Failed to create manifest for {year_month} after copying files. Manual cleanup of input files might be needed.")
            # Attempt to clean up copied files if manifest creation failed? Difficult state.

    return jobs_created_this_cycle


def main():
    """Main sender function."""
    logger = setup_logging()
    try:
        setup_directories()
    except Exception as e:
        logger.critical(f"Failed to setup directories: {e}. Exiting.")
        return # Exit if basic setup fails

    logger.info(f"Starting Sender")
    logger.info(f"Source Metrics:       {SOURCE_DIR}")
    logger.info(f"Source Accounting:    {SOURCE_DIR_ACCOUNTING}")
    logger.info(f"Final Destination:    {DESTINATION_DIR}")
    logger.info(f"Shared Input Dir:     {SERVER_INPUT_DIR}")
    logger.info(f"Shared Output Dir:    {SERVER_OUTPUT_DIR}")
    logger.info(f"Shared Complete Dir:  {SERVER_COMPLETE_DIR}")
    logger.info(f"Max Concurrent Jobs:  {MAX_ACTIVE_JOBS}")

    cycle_count = 0
    while True:
        cycle_start_time = time.time()
        cycle_count += 1
        logger.info(f"--- Sender Cycle {cycle_count} Start ---")

        try:
            # 1. Check for completed or failed jobs and handle them (move data, cleanup)
            finalized_count = check_completed_jobs(logger)
            logger.info(f"Cycle {cycle_count}: Handled {finalized_count} completed/failed jobs.")

            # 2. Distribute new jobs if capacity allows
            created_count = distribute_jobs(logger)
            logger.info(f"Cycle {cycle_count}: Created {created_count} new jobs.")

        except Exception as e:
            logger.error(f"!!! Unhandled error in main sender cycle {cycle_count}: {str(e)}")
            logger.error(traceback.format_exc())
            # Consider more robust error handling or shorter sleep on errors

        cycle_duration = time.time() - cycle_start_time
        logger.info(f"--- Sender Cycle {cycle_count} End (Duration: {cycle_duration:.2f}s) ---")

        # Wait before next cycle
        sleep_time = 60 # Seconds
        logger.debug(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()