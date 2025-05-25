import polars as pl
from pathlib import Path
import logging
import concurrent.futures
import os
import time
from typing import Union

# --- Configuration ---
ROOT_DIR = Path("/home/dynamo/a/jmckerra/projects/stampede-step-4/input")
# Adjust based on your system's cores, leave some for the OS
# os.cpu_count() can give the total number of logical cores
# Start with slightly less than os.cpu_count() and adjust based on performance
NUM_WORKERS = 4

# Configure logging for better tracking (include process ID for parallel runs)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s')


# --- Helper Function for Transformations (Remains Mostly the Same) ---
def transform_dataframe(df: pl.DataFrame) -> tuple[pl.DataFrame, bool]:
    """
    Applies the required transformations to the DataFrame columns.
    Returns the transformed DataFrame and a boolean indicating if relevant columns were found.
    """
    transformations = []
    relevant_columns_found = False

    # Columns to potentially modify
    jid_col = "jid"
    append_s_cols = ["host", "host_list", "username"]

    # 1. Transform 'jid' column
    if jid_col in df.columns:
        relevant_columns_found = True
        # Ensure jid is treated as string if it's not already
        if df[jid_col].dtype != pl.Utf8:
            # Apply cast within the expression for efficiency
            transformations.append(
                pl.col(jid_col).cast(pl.Utf8)
                .str.replace("ID", "", literal=True)
                .str.replace("job", "JOB", literal=True)
                .str.concat("_S")
                .alias(jid_col)
            )
        else:
            transformations.append(
                pl.col(jid_col)
                .str.replace("ID", "", literal=True)
                .str.replace("job", "JOB", literal=True)
                .str.concat("_S")
                .alias(jid_col)
            )
        # logging.debug(f"Added '{jid_col}' transformation.") # Reduce logging noise in parallel
    # else:
    # logging.warning(f"Column '{jid_col}' not found. Skipping transformation.") # Reduce logging noise

    # 2. Transform other columns
    for col_name in append_s_cols:
        if col_name in df.columns:
            relevant_columns_found = True
            # Ensure column is treated as string if it's not already
            if df[col_name].dtype != pl.Utf8:
                transformations.append(
                    pl.col(col_name).cast(pl.Utf8)
                    .str.concat("_S")
                    .alias(col_name)
                )
            else:
                transformations.append(
                    pl.col(col_name)
                    .str.concat("_S")
                    .alias(col_name)
                )
            # logging.debug(f"Added '{col_name}' transformation.") # Reduce logging noise
        # else:
        # logging.warning(f"Column '{col_name}' not found. Skipping transformation.") # Reduce logging noise

    # Apply all transformations if any were added
    if transformations:
        # logging.debug(f"Applying transformations: {transformations}") # Reduce logging noise
        try:
            df_transformed = df.with_columns(transformations)
            return df_transformed, relevant_columns_found
        except Exception as e:
            # Log error specific to transformation step if needed
            logging.error(f"Error applying transformations: {e}")
            # Return original df and relevant_columns_found status on transform error
            return df, relevant_columns_found
    else:
        # logging.info("No applicable columns found for transformation.") # Reduce logging noise
        pass

    # Return the original dataframe if no transformations were applicable/defined
    return df, relevant_columns_found


# --- Function to Process a Single File (for parallel execution) ---
def process_single_file(file_path: Path) -> tuple[str, Union[str, None]]:
    """
    Reads, transforms, and overwrites a single parquet file.
    Returns a tuple: (status, error_message | None).
    Status can be 'processed', 'skipped', 'error'.
    """
    thread_start_time = time.monotonic()
    logging.debug(f"Starting processing: {file_path.name}")
    try:
        # Read the Parquet file using Polars' Rust implementation (usually default)
        # Consider low_memory=True ONLY if hitting memory limits, it can be slower.
        df = pl.read_parquet(file_path)  # Removed use_pyarrow=True, let Polars decide unless specific need

        # Apply transformations
        # No need to clone, we'll decide whether to write based on relevant_columns_found
        df_modified, relevant_columns_found = transform_dataframe(df)

        # Only overwrite if relevant columns were found (meaning transformations were attempted)
        if relevant_columns_found:
            # Overwrite the original file
            # Ensure the output directory exists (should already)
            # file_path.parent.mkdir(parents=True, exist_ok=True) # Usually not needed for overwrite
            df_modified.write_parquet(file_path, compression='zstd',
                                      compression_level=3)  # Default compression is good, zstd often balances well
            duration = time.monotonic() - thread_start_time
            logging.debug(f"Successfully processed and overwrote: {file_path.name} in {duration:.2f}s")
            return 'processed', None
        else:
            duration = time.monotonic() - thread_start_time
            logging.debug(f"Skipped write (no relevant columns): {file_path.name} in {duration:.2f}s")
            return 'skipped', None

    except pl.exceptions.ArrowError as e:
        # Catch Arrow errors which often happen during read/write
        logging.error(f"Arrow/IO error processing {file_path.name}: {e}")
        return 'error', str(e)
    except pl.exceptions.ComputeError as e:
        logging.error(f"Polars computation error processing {file_path.name}: {e}")
        return 'error', str(e)
    except pl.exceptions.SchemaError as e:
        logging.error(f"Polars schema error processing {file_path.name}: {e}")
        return 'error', str(e)
    except FileNotFoundError:
        logging.error(f"File not found during processing: {file_path.name}")
        return 'error', "File not found"
    except PermissionError:
        logging.error(f"Permission denied for file: {file_path.name}")
        return 'error', "Permission denied"
    except Exception as e:
        # Catch other potential errors
        logging.error(f"An unexpected error occurred processing {file_path.name}: {e}",
                      exc_info=False)  # exc_info=False reduces log noise
        return 'error', str(e)


# --- Main Processing Logic ---
def main():
    """
    Finds parquet files and processes them in parallel.
    """
    start_time = time.monotonic()

    if not ROOT_DIR.is_dir():
        logging.error(f"Root directory not found or is not a directory: {ROOT_DIR}")
        return

    logging.info(f"Starting processing in directory: {ROOT_DIR} using {NUM_WORKERS} workers.")

    # Use rglob to recursively find all .parquet files - this itself is usually fast
    try:
        parquet_files = list(ROOT_DIR.rglob("*.parquet"))
    except Exception as e:
        logging.error(f"Error finding files in {ROOT_DIR}: {e}")
        return

    if not parquet_files:
        logging.warning(f"No .parquet files found in {ROOT_DIR} or its subdirectories.")
        return

    total_files = len(parquet_files)
    logging.info(f"Found {total_files} .parquet files to process.")

    processed_count = 0
    skipped_count = 0
    error_count = 0
    errors_list = []  # Keep track of specific errors if needed

    # Use ProcessPoolExecutor for parallel processing
    # The 'with' statement ensures the pool is properly shut down
    with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all tasks and get future objects
        # Using map is simpler if you don't need fine-grained control immediately
        results = executor.map(process_single_file, parquet_files)

        # Process results as they complete
        for i, result in enumerate(results, 1):
            status, error_message = result
            if status == 'processed':
                processed_count += 1
            elif status == 'skipped':
                skipped_count += 1
            elif status == 'error':
                error_count += 1
                # Optionally store file path with error:
                # errors_list.append((parquet_files[i-1], error_message))

            # Log progress periodically
            if i % 100 == 0 or i == total_files:  # Log every 100 files or at the end
                logging.info(f"Progress: {i}/{total_files} files checked...")

    end_time = time.monotonic()
    total_duration = end_time - start_time

    logging.info("--- Processing Summary ---")
    logging.info(f"Total files found: {total_files}")
    logging.info(f"Files successfully processed and overwritten: {processed_count}")
    logging.info(f"Files skipped (no relevant columns found): {skipped_count}")
    logging.info(f"Files with errors: {error_count}")
    logging.info(f"Total execution time: {total_duration:.2f} seconds")
    # if errors_list:
    #     logging.warning("--- Files with Errors ---")
    #     for file_path, msg in errors_list[:10]: # Log first 10 errors
    #         logging.warning(f"{file_path.name}: {msg}")
    #     if len(errors_list) > 10:
    #          logging.warning(f"...and {len(errors_list)-10} more errors.")
    logging.info("Processing finished.")


# --- Script Execution ---
if __name__ == "__main__":
    # Make sure the root directory path is correct
    if not ROOT_DIR.exists():
        print(f"ERROR: The specified root directory does not exist: {ROOT_DIR}")
        print("Please update the ROOT_DIR variable in the script.")
    else:
        main()
