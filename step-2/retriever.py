#!/usr/bin/env python3
import os
import json
import time
import shutil
import logging
from datetime import datetime
from pathlib import Path
import traceback

# --- Configuration ---
# Final destination for successfully processed data
DESTINATION_DIR = Path(r"C:\Users\jmckerra\Documents\Stampede\step-3-complete")

# Directories shared with the Consumer/Sender for coordination
SERVER_INPUT_DIR = Path(r"U:\projects\stampede-step-3\input")  # Needed for cleanup
SERVER_OUTPUT_DIR = Path(r"U:\projects\stampede-step-3\output")  # Needed for cleanup
SERVER_COMPLETE_DIR = Path(r"U:\projects\stampede-step-3\complete")  # Primary dir to check for status

# Local directories for the Retriever/Finalizer
LOGS_DIR = Path("logs_retriever")  # Separate log dir


# --- Functions ---

def setup_logging():
    """Configure logging for the retriever script."""
    LOGS_DIR.mkdir(exist_ok=True)
    log_file = LOGS_DIR / f"metrics_processor_retriever_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    # Return logger instance
    return logging.getLogger("MetricsRetriever")


def setup_directories(logger):
    """Create necessary local and shared directories from the retriever's perspective."""
    DESTINATION_DIR.mkdir(exist_ok=True, parents=True)  # Ensure final destination exists
    LOGS_DIR.mkdir(exist_ok=True)
    logger.info(f"Ensured retriever log directory exists: {LOGS_DIR}")
    logger.info(f"Ensured final destination directory exists: {DESTINATION_DIR}")

    # Ensure shared directories are accessible (consumer interaction points)
    for directory in [SERVER_INPUT_DIR, SERVER_OUTPUT_DIR, SERVER_COMPLETE_DIR]:
        try:
            # Only need to ensure they exist for reading/cleanup, not necessarily create fully
            # Using mkdir with exist_ok is safe
            directory.mkdir(exist_ok=True, parents=True)
            logger.info(f"Ensured shared directory exists/accessible: {directory}")
        except OSError as e:
            logger.error(f"Could not access directory {directory}: {e}. Check permissions and network paths.")
            # Depending on severity, might want to exit or handle gracefully
            raise  # Re-raise for main loop


def move_completed_job_to_destination(job_id, year_month, logger):
    """Move successfully processed data from SERVER_COMPLETE_DIR/job_id to DESTINATION_DIR/year_month."""
    # Source directory created by the consumer containing final output files
    source_job_dir = SERVER_COMPLETE_DIR / job_id
    # Final resting place for the data, organized by year_month
    final_destination_dir = DESTINATION_DIR / year_month

    try:
        # Check if the source directory exists and is actually a directory
        if not source_job_dir.is_dir():  # More specific check than exists()
            logger.warning(
                f"Source directory for completed job {job_id} not found or not a directory at {source_job_dir}.")
            # Check if data might *already* be in the final destination (e.g., from a previous run/retry)
            if final_destination_dir.is_dir() and any(final_destination_dir.glob("*.parquet")):
                logger.info(
                    f"Data for {year_month} already exists in final destination {final_destination_dir}. Assuming "
                    f"prior success or manual move.")
                # If data is already there, we might still want to clean up the source job dir if it exists (handled
                # later in cleanup)
                return True  # Treat as success for workflow progression
            else:
                logger.error(
                    f"Source directory {source_job_dir} missing and no data found in destination {final_destination_dir}. Cannot move data.")
                return False  # Indicate failure if source doesn't exist and destination is empty

        # Ensure the final destination year_month directory exists
        final_destination_dir.mkdir(exist_ok=True, parents=True)
        logger.info(f"Ensured final destination directory exists: {final_destination_dir}")

        # Find the processed parquet files within the source job directory
        # Assumes consumer places final .parquet files directly in SERVER_COMPLETE_DIR/job_id/
        processed_files = list(source_job_dir.glob("*.parquet"))
        logger.debug(f"Found {len(processed_files)} processed files in {source_job_dir} for job {job_id}")

        if not processed_files:
            # This might happen if the job completed but produced no output (e.g., no data for the month)
            # The status "completed_no_data" should ideally indicate this.
            logger.warning(
                f"No processed parquet files found in {source_job_dir} for job {job_id}. Proceeding with cleanup, assuming intended.")
            # Continue the process (moving status, cleanup) as the job *is* complete per the status file.

        files_moved_count = 0
        all_moves_succeeded = True
        for file_path in processed_files:
            dest_file = final_destination_dir / file_path.name
            try:
                logger.debug(f"Moving {file_path.name} from {source_job_dir} to {final_destination_dir}")
                # Using move is efficient, but copy2+unlink is safer across filesystems/mounts
                # shutil.move(str(file_path), str(dest_file)) # Ensure paths are strings if needed by OS/shutil version
                shutil.copy2(file_path, dest_file)  # Copy first
                file_path.unlink()  # Delete source only after successful copy
                files_moved_count += 1
            except Exception as e:
                logger.error(f"Error moving file {file_path.name} for job {job_id} to {dest_file}: {e}")
                # If a move fails, log it but potentially continue? Or stop?
                # Stopping might leave partial state. Continuing might leave inconsistent state.
                # Let's log and mark the overall move as failed for this job.
                all_moves_succeeded = False
                # Do not attempt to move more files for this job if one failed? Or try all? Try all for now.

        if not all_moves_succeeded:
            logger.error(f"One or more file moves failed for job {job_id}. Manual cleanup/retry might be needed.")
            return False  # Indicate overall move failure

        logger.info(
            f"Successfully moved {files_moved_count} processed files for job {job_id} to {final_destination_dir}")

        # Optionally move the status file itself to the destination for archival purposes
        status_file_src = SERVER_COMPLETE_DIR / f"{job_id}.status"
        status_file_dest = final_destination_dir / f"{job_id}.status.archive"  # Rename to avoid confusion
        if status_file_src.exists():
            try:
                # Use move here, as we want it gone from the source location
                shutil.move(str(status_file_src), str(status_file_dest))
                logger.info(f"Archived status file to {status_file_dest}")
            except Exception as e:
                logger.warning(f"Could not archive status file {status_file_src}: {e}")
                # Don't fail the whole process for this, but log it. Cleanup might remove original later if move failed.
        else:
            logger.debug(
                f"Status file {status_file_src} not found for archival (might have been moved/deleted already).")

        return True  # Indicate overall success

    except Exception as e:
        logger.error(f"General error moving completed job {job_id} data to destination: {str(e)}")
        logger.error(traceback.format_exc())
        return False  # Indicate failure


def cleanup_job_artifacts(job_id, year_month, logger, cleanup_input=True):
    """Clean up intermediate files and directories for a finished job (completed or failed)."""
    logger.info(f"Starting cleanup for job {job_id} (YearMonth: {year_month})")
    success = True  # Track overall cleanup success

    try:
        # 1. Clean up input directory metric files (SERVER_INPUT_DIR / year_month)
        # This is only done if cleanup_input is True (typically for success, not failure)
        input_metrics_dir = SERVER_INPUT_DIR / year_month
        if cleanup_input:
            if input_metrics_dir.is_dir():  # Check if it is a directory
                logger.debug(f"Removing input metrics directory: {input_metrics_dir}")
                try:
                    shutil.rmtree(input_metrics_dir)
                except Exception as e:
                    logger.error(f"Failed to remove input metrics dir {input_metrics_dir}: {e}")
                    success = False
            else:
                logger.debug(f"Input metrics directory not found or not a dir, skipping cleanup: {input_metrics_dir}")
        else:
            logger.info(f"Skipping cleanup of input metrics directory as requested: {input_metrics_dir}")

        # 2. Clean up input directory accounting file (SERVER_INPUT_DIR / YYYY-MM.csv)
        # This is only done if cleanup_input is True
        # Infer accounting filename based on year_month
        accounting_filename = f"{year_month}.csv"
        input_accounting_file = SERVER_INPUT_DIR / accounting_filename
        if cleanup_input:
            if input_accounting_file.is_file():  # Check if it is a file
                logger.debug(f"Removing input accounting file: {input_accounting_file}")
                try:
                    input_accounting_file.unlink()
                except Exception as e:
                    logger.error(f"Failed to remove input accounting file {input_accounting_file}: {e}")
                    success = False
            else:
                logger.debug(
                    f"Input accounting file not found or not a file, skipping cleanup: {input_accounting_file}")
        else:
            logger.info(f"Skipping cleanup of input accounting file as requested: {input_accounting_file}")

        # 3. Clean up the input manifest file (SERVER_INPUT_DIR / job_id.manifest.json)
        # This is only done if cleanup_input is True
        manifest_file = SERVER_INPUT_DIR / f"{job_id}.manifest.json"
        if cleanup_input:
            if manifest_file.is_file():
                logger.debug(f"Removing input manifest file: {manifest_file}")
                try:
                    manifest_file.unlink()
                except Exception as e:
                    logger.error(f"Failed to remove input manifest file {manifest_file}: {e}")
                    success = False
            else:
                logger.debug(f"Input manifest file not found or not a file, skipping cleanup: {manifest_file}")
        else:
            logger.info(f"Skipping cleanup of input manifest file as requested: {manifest_file}")

        # 4. Clean up output directory (SERVER_OUTPUT_DIR / job_id) - Consumer's intermediate work
        # Always clean this up for completed or failed jobs.
        output_job_dir = SERVER_OUTPUT_DIR / job_id
        if output_job_dir.is_dir():
            logger.debug(f"Removing intermediate output job directory: {output_job_dir}")
            try:
                shutil.rmtree(output_job_dir)
            except Exception as e:
                logger.error(f"Failed to remove output job dir {output_job_dir}: {e}")
                success = False  # Log error but continue cleanup
        else:
            logger.debug(f"Output job directory not found or not a dir, skipping cleanup: {output_job_dir}")

        # 5. Clean up complete directory (SERVER_COMPLETE_DIR / job_id) - Consumer's final staging area
        # Always clean this up. Should be empty if move_completed_job_to_destination worked, but clean regardless.
        complete_job_dir = SERVER_COMPLETE_DIR / job_id
        if complete_job_dir.is_dir():
            logger.debug(f"Removing final staging directory: {complete_job_dir}")
            try:
                # This directory should contain only the parquet files if move succeeded (which were moved/deleted)
                # Or potentially leftovers if move failed partially.
                shutil.rmtree(complete_job_dir)
            except Exception as e:
                logger.error(f"Failed to remove complete job dir {complete_job_dir}: {e}")
                success = False  # Log error but continue cleanup
        else:
            logger.debug(f"Complete job directory not found or not a dir, skipping cleanup: {complete_job_dir}")

        # 6. Clean up the status file (SERVER_COMPLETE_DIR / job_id.status) itself
        # This should happen AFTER successfully processing the status (move, archive, handle failure)
        # It might have been moved already by move_completed_job_to_destination.
        status_file_src = SERVER_COMPLETE_DIR / f"{job_id}.status"
        if status_file_src.is_file():
            logger.debug(f"Removing status file: {status_file_src}")
            try:
                status_file_src.unlink()
            except Exception as e:
                logger.error(f"Failed to remove status file {status_file_src}: {e}")
                # This is less critical if already handled, but log it.
                # Don't necessarily mark overall cleanup as failed just for this.
        else:
            logger.debug(f"Status file {status_file_src} not found, likely already moved or deleted.")

        if success:
            logger.info(f"Cleanup finished successfully for job {job_id}")
        else:
            logger.warning(f"Cleanup finished for job {job_id} with one or more errors.")

        return success

    except Exception as e:
        logger.error(f"General error during cleanup for job {job_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return False  # Indicate overall cleanup failure


def check_completed_jobs(logger):
    """Check SERVER_COMPLETE_DIR for job status files and handle completion/failure."""
    jobs_finalized_this_cycle = 0

    try:
        if not SERVER_COMPLETE_DIR.is_dir(): # Use is_dir() for clarity
            logger.warning(f"Server complete directory {SERVER_COMPLETE_DIR} not found or is not a directory. Cannot check job status.")
            return 0  # Return 0 jobs handled

        # --- CORRECTED: Use glob to find status files directly ---
        status_files = list(SERVER_COMPLETE_DIR.glob("*.status"))
        # ---------------------------------------------------------

        logger.debug(f"Found {len(status_files)} potential status files in {SERVER_COMPLETE_DIR}")
        if status_files: # Log the found files only if any exist
             logger.debug(f"Status files: {[file.name for file in status_files]}")
        else:
            logger.debug("No status files found to process.")
            return 0

        for status_file in status_files:
            # Extract job_id from the status filename (e.g., "process_2014-06_34778d60" from "process_2014-06_34778d60.status")
            job_id_from_filename = status_file.stem
            status_data = None
            job_handled = False # Flag to track if we actioned this status file (redundant now, cleanup handles removal)

            try:
                logger.info(f"Processing status file: {status_file.name}")
                with open(status_file, 'r') as f:
                    status_data = json.load(f)

                # --- Validate essential data from status file ---
                status = status_data.get("status")
                job_id = status_data.get("job_id")
                year_month = status_data.get("year_month")

                if not all([status, job_id, year_month]):
                    logger.warning(f"Status file {status_file.name} is missing required fields (status, job_id, year_month). Moving to invalid.")
                    invalid_file_path = status_file.with_suffix('.status.invalid')
                    try:
                        # Use replace for atomic rename if possible, otherwise move
                        status_file.replace(invalid_file_path)
                    except OSError: # Fallback for cross-drive or permission issues
                        try:
                             shutil.move(str(status_file), str(invalid_file_path))
                        except Exception as e_rename:
                             logger.error(f"Could not rename invalid status file {status_file.name}: {e_rename}")
                    continue # Skip processing this file further

                # Sanity check: filename job ID should match content job ID
                if job_id_from_filename != job_id:
                    logger.warning(f"Mismatch between status filename job ID ('{job_id_from_filename}') and content job ID ('{job_id}') in {status_file.name}. Trusting content ID '{job_id}'.")
                    # Proceed using the job_id from the file content

                # --- Handle COMPLETED jobs ---
                if status in ["completed", "completed_no_data"]:
                    logger.info(f"Detected completed job: {job_id} for {year_month} (Status: {status})")

                    # 1. Move data (if any) to final destination
                    # This function correctly expects data in SERVER_COMPLETE_DIR / job_id
                    move_success = move_completed_job_to_destination(job_id, year_month, logger)

                    if move_success:
                        logger.info(f"Successfully finalized data (moved/archived) for completed job {job_id}.")
                        # 2. Clean up ALL artifacts (input, output, complete dirs, manifest, status file)
                        # Set cleanup_input=True because the job was successful
                        cleanup_success = cleanup_job_artifacts(job_id, year_month, logger, cleanup_input=True)
                        if not cleanup_success:
                            logger.error(f"Cleanup failed for completed job {job_id}, but data was moved/archived. Manual cleanup of remaining artifacts might be needed.")
                        # If cleanup succeeded, it removed the status file. If it failed, the status file might remain, but we count it as finalized.
                        jobs_finalized_this_cycle += 1
                    else:
                        # If move failed, DO NOT clean up artifacts. Requires intervention.
                        logger.error(f"Failed to move/archive data for completed job {job_id}. Artifacts will NOT be cleaned up. Status file {status_file.name} will remain. Manual intervention required.")
                        # Do not increment finalized count, leave status file.

                # --- Handle FAILED jobs ---
                elif status == "failed":
                    logger.error(f"Detected failed job: {job_id} for {year_month}.")
                    error_details = status_data.get("error", "No specific error details provided.")
                    error_traceback = status_data.get("traceback", "") # Get traceback if included
                    logger.error(f"Job {job_id} failure reason from status: {error_details}")
                    if error_traceback:
                         logger.error(f"Traceback from consumer:\n{error_traceback}")

                    # 1. Clean up intermediate OUTPUT and COMPLETE directories ONLY.
                    # Keep INPUT files (metrics subdir + accounting file + manifest) for debugging/retry.
                    logger.info(f"Cleaning up output/complete artifacts for failed job {job_id}. Input files will be kept.")
                    # Set cleanup_input=False to preserve input files
                    # Cleanup also removes the status file itself.
                    cleanup_partial_success = cleanup_job_artifacts(job_id, year_month, logger, cleanup_input=False)
                    if not cleanup_partial_success:
                        logger.warning(f"Partial cleanup (output/complete/status) for failed job {job_id} encountered issues. Status file might remain.")
                    else:
                        logger.info(f"Partial cleanup for failed job {job_id} completed.")

                    jobs_finalized_this_cycle += 1  # Count failed job as finalized for this cycle

                # --- Handle UNKNOWN/OTHER statuses ---
                elif status == "processing":
                    logger.debug(f"Ignoring job {job_id} with status 'processing' in {status_file.name}. Consumer is still working.")
                else:
                    logger.warning(f"Detected job {job_id} with unexpected status '{status}' in {status_file.name}. Ignoring, leaving status file.")
                    # Consider renaming to *.status.unknown if desired

            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in status file: {status_file.name}. Moving to invalid.")
                invalid_file_path = status_file.with_suffix('.status.invalid')
                try:
                    status_file.replace(invalid_file_path)
                except OSError:
                    try:
                         shutil.move(str(status_file), str(invalid_file_path))
                    except Exception as e_rename:
                         logger.error(f"Could not rename invalid JSON status file {status_file.name}: {e_rename}")
            except Exception as e:
                logger.error(f"Error processing status file {status_file.name}: {str(e)}")
                logger.error(traceback.format_exc())
                # Leave the file for manual inspection

    except Exception as e:
        logger.error(f"General error checking completed jobs in {SERVER_COMPLETE_DIR}: {str(e)}")
        logger.error(traceback.format_exc())

    return jobs_finalized_this_cycle


def main():
    """Main function for the retriever script."""
    logger = setup_logging()
    logger.info("--- Starting Metrics Processor Retriever/Finalizer ---")
    try:
        setup_directories(logger)  # Setup directories at the start
    except Exception as e:
        logger.critical(f"Failed to setup critical directories: {e}. Exiting.")
        return  # Exit if basic setup fails

    logger.info(f"Checking for status files in: {SERVER_COMPLETE_DIR}")
    logger.info(f"Moving successful job data to: {DESTINATION_DIR}")
    logger.info(f"Cleaning up artifacts from: {SERVER_INPUT_DIR}, {SERVER_OUTPUT_DIR}, {SERVER_COMPLETE_DIR}")

    cycle_count = 0
    while True:
        cycle_start_time = time.time()
        cycle_count += 1
        logger.info(f"--- Retriever Cycle {cycle_count} Start ---")

        try:
            # The retriever's main job is to check for and process finished jobs
            finalized_count = check_completed_jobs(logger)
            logger.info(f"Cycle {cycle_count}: Finalized {finalized_count} jobs (completed or failed).")

        except Exception as e:
            # Catch broad exceptions in the main loop to keep the retriever running
            logger.error(f"!!! Unhandled error in retriever main cycle {cycle_count}: {str(e)}")
            logger.error(traceback.format_exc())
            # Consider different sleep behavior on errors if needed

        cycle_duration = time.time() - cycle_start_time
        logger.info(f"--- Retriever Cycle {cycle_count} End (Duration: {cycle_duration:.2f}s) ---")

        # Wait before starting the next cycle
        sleep_time = 60  # Seconds (adjust as needed)
        logger.debug(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
