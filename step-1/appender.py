import os
import time
import shutil
import uuid
import re
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor
import threading


# --- Logging Setup ---
# Using print statements for now as per original script, but logging is recommended for production
def log_info(msg): print(f"INFO: {msg}")


def log_warning(msg): print(f"WARNING: {msg}")


def log_error(msg): print(f"ERROR: {msg}")


# --- Configuration ---
# !! CRITICAL !! Reduce workers significantly, especially with network/slow storage
MAX_WORKERS = 8  # Start low (e.g., 2-4) and increase cautiously ONLY if P: drive is stable
MAX_RETRIES = 5
# Increase delay slightly, maybe add exponential backoff later if needed
RETRY_DELAY_BASE = 2  # Base delay in seconds

# Configure your folders (use raw strings for Windows paths)
WATCH_FOLDER = r"C:\Users\jmckerra\Documents\Stampede\step-1-staging"
PROCESSING_FOLDER = r"C:\Users\jmckerra\Documents\Stampede\processing"
OUTPUT_FOLDER = r"C:\Users\jmckerra\Documents\Stampede\step-1-complete"
ERROR_FOLDER = ""


# --- End Configuration ---

class ParquetHandler(FileSystemEventHandler):
    def __init__(self, watch_folder, processing_folder, output_folder, error_folder, executor, max_retries=MAX_RETRIES,
                 retry_delay_base=RETRY_DELAY_BASE):
        self.watch_folder = watch_folder
        self.processing_folder = processing_folder
        self.output_folder = output_folder
        self.error_folder = error_folder  # Store error folder path
        self.executor = executor
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base
        self._target_locks = {}  # Dictionary to hold locks for target files
        self._dict_lock = threading.Lock()  # Lock for accessing the _target_locks dictionary safely

        # Create folders if they don't exist
        for folder in [watch_folder, processing_folder, output_folder, error_folder]:
            try:
                os.makedirs(folder, exist_ok=True)
            except OSError as e:
                log_error(f"CRITICAL: Could not create folder {folder}: {e}. Check permissions and path validity.")
                # Depending on which folder fails, you might want to exit
                if folder == watch_folder or folder == output_folder:
                    raise  # Cannot proceed without watch/output folders
        log_info(
            f"Initialized ParquetHandler:\n  Watch: {watch_folder}\n  Processing: {processing_folder}\n  Output: {output_folder}\n  Error: {error_folder}\n  Workers: {MAX_WORKERS}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.parquet'):
            log_info(f"Detected new file: {event.src_path}")
            # Submit the processing task to the thread pool
            # Pass a copy of the path in case the event object changes
            self.executor.submit(self.process_file, str(event.src_path))

    def extract_date_from_filename(self, filename):
        # Extract date part from NODE1_('2013-02-27',).parquet style filenames
        match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
        if match:
            return f"{match.group(1)}.parquet"
        else:
            log_warning(f"Could not extract date from '{filename}'. Using a generic name.")
            # Use a generic name to avoid potential issues with original complex names
            base = os.path.splitext(filename)[0]
            # Sanitize base name slightly if needed (replace invalid chars)
            sanitized_base = re.sub(r'[^\w\-]+', '_', base)
            return f"unknown_date_{sanitized_base[:30]}.parquet"

    def get_target_lock(self, target_filename):
        """Gets or creates a lock for a specific target filename."""
        with self._dict_lock:
            if target_filename not in self._target_locks:
                self._target_locks[target_filename] = threading.Lock()
            return self._target_locks[target_filename]

    def move_to_error_folder(self, file_path, reason):
        """Moves a file to the error folder with a unique name."""
        if not os.path.exists(file_path):
            log_warning(f"Tried to move {file_path} to error folder, but it doesn't exist anymore.")
            return

        try:
            base_name = os.path.basename(file_path)
            # Add unique ID to avoid collisions in error folder
            error_filename = f"{base_name}.{uuid.uuid4().hex[:8]}.error"
            error_path = os.path.join(self.error_folder, error_filename)
            shutil.move(file_path, error_path)
            log_warning(f"Moved '{base_name}' to error folder: {error_path}. Reason: {reason}")
        except Exception as e:
            log_error(
                f"CRITICAL: Failed to move '{os.path.basename(file_path)}' from '{os.path.dirname(file_path)}' to error folder! Manual cleanup needed. Error: {e}")

    def process_file(self, file_path):
        thread_id = threading.get_ident()
        original_filename = os.path.basename(file_path)
        log_info(f"[Thread-{thread_id}] Attempting to process {original_filename} (Path: {file_path})")

        # Check if file still exists before proceeding
        if not os.path.exists(file_path):
            log_warning(
                f"[Thread-{thread_id}] File {original_filename} no longer exists at {file_path} before processing started.")
            return

        time.sleep(0.2)  # Short initial delay

        standardized_filename = self.extract_date_from_filename(original_filename)
        unique_id = uuid.uuid4().hex[:8]  # Use hex for filesystem friendliness
        processing_name = f"{os.path.splitext(standardized_filename)[0]}_{unique_id}.processing.tmp"
        processing_path = os.path.join(self.processing_folder, processing_name)

        moved_to_processing = False
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                # --- Stage 1: Move to Processing (Atomicity and Readiness Check) ---
                log_info(
                    f"[Thread-{thread_id}] Attempt {attempt + 1}/{self.max_retries}: Moving '{original_filename}' to '{processing_path}'")
                # Use rename for potential atomicity on the same filesystem
                os.rename(file_path, processing_path)
                moved_to_processing = True
                log_info(f"[Thread-{thread_id}] Moved '{original_filename}' to {processing_path}")

                # --- Stage 2: Append to Target ---
                target_lock = self.get_target_lock(standardized_filename)
                with target_lock:
                    log_info(f"[Thread-{thread_id}] Acquired lock for {standardized_filename}")
                    self.append_to_target_pandas(processing_path, standardized_filename, unique_id)
                    # If append succeeds, lock is released automatically

                # --- Stage 3: Cleanup processing file ---
                try:
                    os.remove(processing_path)
                    log_info(f"[Thread-{thread_id}] Successfully processed and removed {processing_path}")
                except OSError as e:
                    log_warning(
                        f"[Thread-{thread_id}] Could not remove temporary processing file {processing_path} after success: {e}")
                return  # Success! Exit function.

            except (PermissionError, OSError) as e:
                last_exception = e
                log_warning(
                    f"[Thread-{thread_id}] Attempt {attempt + 1}/{self.max_retries}: File access error for '{original_filename}'. Error: {e}")

                if isinstance(e, OSError) and e.winerror == 1117:
                    log_error(
                        f"[Thread-{thread_id}] !! Detected WinError 1117 (I/O device error) for '{original_filename}'. This indicates a storage problem (Network drive? Failing disk?). Check drive P: !!")
                    # Decide if we should retry less for this specific error? Maybe break early?
                    # For now, we continue retrying, but this error needs external investigation.

                if moved_to_processing:
                    # Error occurred *after* moving to processing (likely during append)
                    log_warning(
                        f"[Thread-{thread_id}] Error occurred after moving file to {processing_path}. Will retry append if attempts remain.")
                    # Don't break here, allow retry loop to handle the append failure
                else:
                    # Error occurred during the initial os.rename
                    log_warning(f"[Thread-{thread_id}] Failed to move '{original_filename}' to processing folder.")

                if attempt < self.max_retries - 1:
                    delay = self.retry_delay_base * (2 ** attempt)  # Exponential backoff
                    log_info(f"[Thread-{thread_id}] Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                # Continue to next attempt in the loop

            except FileNotFoundError:
                log_warning(
                    f"[Thread-{thread_id}] File not found during processing: {file_path if not moved_to_processing else processing_path}. Might have been moved/deleted externally.")
                return  # File is gone, nothing more to do.

            except SchemaMismatchError as e:
                last_exception = e
                log_error(f"[Thread-{thread_id}] Schema Mismatch Error processing '{original_filename}': {e}")
                # No point retrying schema mismatch - move to error immediately
                self.move_to_error_folder(processing_path, f"Schema Mismatch: {e}")
                return  # Stop processing this file

            except Exception as e:
                last_exception = e
                log_error(
                    f"[Thread-{thread_id}] Unexpected Error processing '{original_filename}' (Attempt {attempt + 1}/{self.max_retries}): {e}",
                    exc_info=True)  # Log traceback for unexpected errors
                # If move succeeded, file is stuck in processing. If not, original file remains.
                # Break the loop for unexpected errors to avoid potentially harmful retries
                break

        # If loop finishes without returning, it means all retries failed
        log_error(
            f"[Thread-{thread_id}] Failed to process '{original_filename}' after {self.max_retries} attempts. Last error: {last_exception}")
        if moved_to_processing:
            # Move the stranded processing file to the error folder
            self.move_to_error_folder(processing_path,
                                      f"Failed after {self.max_retries} attempts. LastError: {last_exception}")
        else:
            # Original file likely still in watch folder, try moving it to error
            self.move_to_error_folder(file_path,
                                      f"Failed initial move/processing after {self.max_retries} attempts. LastError: {last_exception}")

    def append_to_target_pandas(self, processing_path, standardized_filename, unique_id):
        """Appends data using pandas with safe write-to-temp-then-replace."""
        output_path = os.path.join(self.output_folder, standardized_filename)
        # Temporary file placed in the *output* folder to ensure atomic replace works
        temp_output_path = f"{output_path}.{unique_id}.tmp"
        thread_id = threading.get_ident()

        try:
            # Read the new data (from processing folder)
            log_info(f"[Thread-{thread_id}] Reading source data from {processing_path}")
            df_new = pd.read_parquet(processing_path)

            if df_new.empty:
                log_warning(f"[Thread-{thread_id}] Skipping empty file: {processing_path}")
                # We successfully "processed" the empty file, it will be deleted by the caller.
                return  # Nothing to append

            df_to_write = None

            if os.path.exists(output_path):
                log_info(f"[Thread-{thread_id}] Target file exists: {output_path}. Reading existing data.")
                try:
                    # First, let's check schema compatibility
                    df_existing = pd.read_parquet(output_path)

                    # Compare column sets
                    new_columns = set(df_new.columns)
                    existing_columns = set(df_existing.columns)

                    if new_columns != existing_columns:
                        schema_diff_msg = f"Column mismatch: New columns {new_columns} vs Existing columns {existing_columns}"
                        log_error(
                            f"[Thread-{thread_id}] Schema mismatch detected for {standardized_filename}. {schema_diff_msg}")
                        raise SchemaMismatchError(
                            f"Columns of {os.path.basename(processing_path)} do not match target {standardized_filename}")

                    # Combine dataframes
                    log_info(f"[Thread-{thread_id}] Concatenating dataframes for {standardized_filename}")
                    df_to_write = pd.concat([df_existing, df_new], ignore_index=True)
                    log_info(
                        f"[Thread-{thread_id}] Appending {len(df_new)} rows to {standardized_filename}. New total: {len(df_to_write)}")

                except Exception as e:
                    # Handle cases where existing file is corrupted or unreadable
                    log_error(
                        f"[Thread-{thread_id}] Error reading existing target file {output_path}: {e}. Moving corrupted target and writing new data.")
                    # Move the potentially corrupted file aside BEFORE trying to write the new one
                    self.move_to_error_folder(output_path,
                                              f"Corrupted or unreadable during append operation by {os.path.basename(processing_path)}. Error: {e}")
                    # Proceed as if the target file didn't exist, writing only the new data
                    df_to_write = df_new
                    log_info(
                        f"[Thread-{thread_id}] Writing new data only to {standardized_filename} after moving corrupted original.")

            else:
                # Target file doesn't exist, write the new dataframe directly
                log_info(f"[Thread-{thread_id}] Target file {output_path} does not exist. Creating new file.")
                df_to_write = df_new

            # --- Safe Write Operation ---
            log_info(
                f"[Thread-{thread_id}] Writing {len(df_to_write)} rows to temporary file: {temp_output_path}")
            df_to_write.to_parquet(temp_output_path, index=False)

            # --- Atomic Replace ---
            log_info(f"[Thread-{thread_id}] Replacing {output_path} with {temp_output_path}")
            os.replace(temp_output_path,
                       output_path)  # Atomic on most local/networked filesystems if target dir is same
            log_info(f"[Thread-{thread_id}] Successfully updated {output_path}")

        except pd.errors.EmptyDataError as e:
            log_error(
                f"[Thread-{thread_id}] Empty Data Error: Processing: {processing_path}, Target: {output_path}. Error: {e}")
            # This is likely an issue with the *source* file (processing_path)
            # Let process_file handle moving processing_path to error folder by re-raising
            raise
        except SchemaMismatchError:
            # Propagate this specific error type to be caught by process_file
            raise
        except OSError as e:
            # Catch OS errors specifically, including WinError 1117 during write/replace
            log_error(
                f"[Thread-{thread_id}] OS Error during write/replace for {output_path} (Temp: {temp_output_path}): {e}")
            if hasattr(e, 'winerror') and e.winerror == 1117:
                log_error(
                    f"[Thread-{thread_id}] !! WinError 1117 (I/O device error) occurred during write/replace to {output_path}. Storage issue likely !!")
            # Clean up temporary file if it exists and write failed
            if os.path.exists(temp_output_path):
                try:
                    os.remove(temp_output_path)
                    log_info(f"[Thread-{thread_id}] Removed failed temporary file: {temp_output_path}")
                except OSError as rm_err:
                    log_warning(
                        f"[Thread-{thread_id}] Could not remove failed temporary file {temp_output_path}: {rm_err}")
            # Re-raise the error to be caught by process_file retry logic
            raise
        except Exception as e:
            log_error(f"[Thread-{thread_id}] Failed to append/write {output_path} (Temp: {temp_output_path}): {e}",
                      exc_info=True)
            # Clean up temporary file if it exists
            if os.path.exists(temp_output_path):
                try:
                    os.remove(temp_output_path)
                    log_info(f"[Thread-{thread_id}] Removed failed temporary file: {temp_output_path}")
                except OSError as rm_err:
                    log_warning(
                        f"[Thread-{thread_id}] Could not remove failed temporary file {temp_output_path}: {rm_err}")
            # Re-raise the error to be caught by process_file retry logic
            raise


def process_existing_files(watch_folder, handler):
    """Scans the watch folder for existing Parquet files and processes them."""
    log_info(f"Scanning for existing files in {watch_folder}...")
    count = 0
    try:
        for filename in os.listdir(watch_folder):
            if filename.lower().endswith('.parquet'):
                file_path = os.path.join(watch_folder, filename)
                # Double check it's a file and not the processing dir if nested (though it shouldn't be)
                if os.path.isfile(file_path):
                    count += 1
                    log_info(f"Found existing file: {file_path}")
                    # Submit to the thread pool just like new files
                    handler.executor.submit(handler.process_file, file_path)
        log_info(f"Submitted {count} existing files for processing.")
    except FileNotFoundError:
        log_error(f"Watch folder {watch_folder} not found during initial scan.")
    except OSError as e:
        log_error(f"Error listing files in watch folder {watch_folder}: {e}")


# Custom Exception for Schema Issues
class SchemaMismatchError(ValueError):
    pass


def start_monitoring(watch_folder, processing_folder, output_folder, error_folder):
    # Using ThreadPoolExecutor for I/O bound tasks
    # Ensure MAX_WORKERS is appropriate for the storage system's capabilities
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='ParquetWorker') as executor:
        event_handler = ParquetHandler(watch_folder, processing_folder, output_folder, error_folder, executor)

        # --- Crucial Pre-computation Note ---
        log_info("--- IMPORTANT PRE-COMPUTATION NOTE ---")
        log_warning(f"The stability of this script heavily depends on the reliability of the storage at 'P:'.")
        log_warning(f"WinError 1117 indicates an underlying I/O device error. Please investigate:")
        log_warning(f"  1. Network connection/stability if P: is a network drive.")
        log_warning(f"  2. Health of the drive if P: is local or external.")
        log_warning(f"  3. Check for other processes heavily accessing P: simultaneously.")
        log_warning(f"  4. Ensure adequate permissions on all folders.")
        log_warning(f"Using {MAX_WORKERS} worker threads. Reduce this number if I/O errors persist.")
        log_info("------------------------------------")
        time.sleep(5)  # Give user time to read warnings

        # Process existing files *before* starting the observer
        # This prevents the observer potentially picking them up *while* this function runs
        process_existing_files(watch_folder, event_handler)

        observer = Observer()
        observer.schedule(event_handler, watch_folder, recursive=False)
        observer.start()
        log_info(f"Observer started. Monitoring {watch_folder}...")

        print(f"\nMonitoring active. Press Ctrl+C to stop.")
        try:
            while observer.is_alive():  # Check if observer thread is running
                observer.join(timeout=1)  # Wait for 1 sec, allows Ctrl+C capture
        except KeyboardInterrupt:
            log_info("\nCtrl+C received. Stopping observer...")
            observer.stop()
            log_info("Observer stopped. Waiting for running tasks to complete (may take time)...")
            # Executor shutdown (including waiting for tasks) is handled by the 'with' statement exiting
        except Exception as e:
            log_error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            observer.stop()

    # Observer.join() should be called after stopping and outside the 'with executor' block
    log_info("Executor shut down.")
    log_info("Monitoring stopped.")


if __name__ == "__main__":
    # Configure your folders - defined globally above
    start_monitoring(WATCH_FOLDER, PROCESSING_FOLDER, OUTPUT_FOLDER, ERROR_FOLDER)
