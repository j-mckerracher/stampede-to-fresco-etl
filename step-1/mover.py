import os
import shutil
import time
import logging
import threading
from queue import Queue, Empty
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- Configuration ---
# Use raw strings (r"...") or double backslashes ("\\") for Windows paths
SOURCE_DIR = r"U:\projects\stampede-step-1\output"
DEST_DIR = r"P:\Stampede\step-1-staging"
POLL_INTERVAL_SECONDS = 1  # How often watchdog checks (lower for faster detection)
NUM_WORKER_THREADS = 10  # Adjust based on network/disk speed, start with 1 or 2
# --- End Configuration ---

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Use a thread-safe queue to pass file paths
file_queue = Queue()


class MoveEventHandler(FileSystemEventHandler):
    """Handles file system events from watchdog."""

    def __init__(self, queue):
        self.queue = queue

    def on_created(self, event):
        """Called when a file or directory is created."""
        if not event.is_directory:
            # Basic check to avoid queuing temporary/incomplete files (optional)
            # You might need more robust checks depending on how files are written
            try:
                # A tiny delay might help ensure the file is fully written,
                # but can introduce slight latency. Adjust or remove as needed.
                # time.sleep(0.1)
                if os.path.exists(event.src_path) and os.path.isfile(event.src_path):
                    # Check file size stability (optional, adds overhead)
                    # size1 = os.path.getsize(event.src_path)
                    # time.sleep(0.2)
                    # size2 = os.path.getsize(event.src_path)
                    # if size1 == size2:
                    logging.info(f"Detected new file: {event.src_path}")
                    self.queue.put(event.src_path)
                    # else:
                    #    logging.warning(f"File size changed, possibly still writing: {event.src_path}")
                else:
                    logging.warning(f"File detected but vanished before queueing: {event.src_path}")

            except Exception as e:
                logging.error(f"Error accessing detected file {event.src_path}: {e}")

    # You might also want to handle on_moved if files could be moved *into* the source dir
    # def on_moved(self, event):
    #     if not event.is_directory:
    #         logging.info(f"Detected moved-in file: {event.dest_path}")
    #         self.queue.put(event.dest_path)


def move_worker(q, dest_dir):
    """Worker thread function to move files from the queue."""
    while True:
        try:
            # Get a file path from the queue. The `True` blocks until an item is available.
            # Use a timeout if you prefer non-blocking with checks
            # src_path = q.get(block=True, timeout=1)
            src_path = q.get()

            # Sentinel value (None) to signal thread termination
            if src_path is None:
                logging.info("Sentinel received, worker thread shutting down.")
                break  # Exit the loop

            if not os.path.exists(src_path) or not os.path.isfile(src_path):
                logging.warning(f"File vanished before move attempt: {src_path}")
                q.task_done()  # Mark task as done even if file vanished
                continue

            filename = os.path.basename(src_path)
            dest_path = os.path.join(dest_dir, filename)

            logging.info(f"Moving: {src_path} -> {dest_path}")
            try:
                shutil.move(src_path, dest_path)
                logging.info(f"Successfully moved: {dest_path}")
            except FileNotFoundError:
                # Race condition: File might have been deleted/moved by another process
                # between the check and the move attempt.
                logging.warning(f"File not found during move operation (race condition?): {src_path}")
            except PermissionError:
                logging.error(f"Permission error moving {src_path}. Check file/folder permissions.")
                # Decide if you want to requeue or drop the file on permission error
                # q.put(src_path) # Example: Requeue (beware infinite loops)
            except Exception as e:
                logging.error(f"Failed to move {src_path} to {dest_path}: {e}")
                # Consider adding logic here to retry, move to an error folder, etc.
            finally:
                # Important: Mark the task as done for queue.join()
                q.task_done()

        except Empty:
            # This won't happen with block=True unless a timeout is used
            continue  # Or sleep briefly time.sleep(0.1)
        except Exception as e:
            # Catch unexpected errors in the worker loop itself
            logging.critical(f"Critical error in worker thread: {e}", exc_info=True)
            # Consider breaking the loop or implementing recovery logic
            # break # Example: Stop worker on critical error


def main():
    """Main function to set up and run the observer and workers."""
    logging.info("--- Starting File Mover Script ---")
    logging.info(f"Source Directory: {SOURCE_DIR}")
    logging.info(f"Destination Directory: {DEST_DIR}")
    logging.info(f"Number of worker threads: {NUM_WORKER_THREADS}")

    # Ensure source directory exists
    if not os.path.isdir(SOURCE_DIR):
        logging.error(f"Source directory not found: {SOURCE_DIR}")
        return

    # Ensure destination directory exists, create if not
    try:
        os.makedirs(DEST_DIR, exist_ok=True)
        logging.info(f"Ensured destination directory exists: {DEST_DIR}")
    except Exception as e:
        logging.error(f"Could not create or access destination directory {DEST_DIR}: {e}")
        return

    # --- Start worker threads ---
    threads = []
    for i in range(NUM_WORKER_THREADS):
        thread = threading.Thread(target=move_worker, args=(file_queue, DEST_DIR), daemon=True)
        thread.name = f"MoveWorker-{i + 1}"
        thread.start()
        threads.append(thread)
    logging.info(f"Started {len(threads)} worker threads.")

    # --- Handle files already present at startup ---
    logging.info(f"Scanning for existing files in {SOURCE_DIR}...")
    initial_file_count = 0
    try:
        for filename in os.listdir(SOURCE_DIR):
            file_path = os.path.join(SOURCE_DIR, filename)
            if os.path.isfile(file_path):
                logging.info(f"Queueing existing file: {file_path}")
                file_queue.put(file_path)
                initial_file_count += 1
    except Exception as e:
        logging.error(f"Error scanning initial files in {SOURCE_DIR}: {e}")
        # Decide if you want to continue or exit based on the error
    logging.info(f"Finished scanning for existing files. Queued {initial_file_count} files.")

    # --- Setup and start watchdog observer ---
    event_handler = MoveEventHandler(file_queue)
    observer = Observer()
    try:
        # Schedule monitoring: recursive=False means only watch SOURCE_DIR itself, not subfolders
        observer.schedule(event_handler, SOURCE_DIR, recursive=False)
        observer.start()
        logging.info("Watchdog observer started. Monitoring for new files...")
    except Exception as e:
        logging.error(f"Failed to start watchdog observer on {SOURCE_DIR}: {e}")
        logging.error(
            "This might be due to permissions or the path being inaccessible (e.g., disconnected network drive).")
        # Attempt cleanup before exiting
        observer.stop()  # Stop observer if it partially started
        # Signal workers to exit
        for _ in range(NUM_WORKER_THREADS):
            file_queue.put(None)
        return  # Exit if observer fails

    # --- Keep the main thread alive, handle shutdown ---
    try:
        while True:
            # Keep the main thread running. Observer and workers are in background threads.
            # You can add checks here if needed (e.g., check observer.is_alive())
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Ctrl+C received. Initiating shutdown...")
    finally:
        logging.info("Stopping watchdog observer...")
        observer.stop()
        # Wait for the observer thread to finish
        observer.join()
        logging.info("Watchdog observer stopped.")

        logging.info("Waiting for file queue to empty...")
        # Wait until all queued items have been processed
        file_queue.join()
        logging.info("File queue processing complete.")

        # Signal worker threads to exit by putting None onto the queue
        logging.info("Sending shutdown signal to worker threads...")
        for _ in range(NUM_WORKER_THREADS):
            file_queue.put(None)

        # Wait for all worker threads to terminate
        logging.info("Waiting for worker threads to finish...")
        for t in threads:
            t.join()  # Wait for each worker thread to exit its loop

        logging.info("--- File Mover Script Finished ---")


if __name__ == "__main__":
    main()
