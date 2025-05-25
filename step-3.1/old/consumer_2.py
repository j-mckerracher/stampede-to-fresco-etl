import json
import os
import shutil
import time
import logging
import gc
import traceback
import multiprocessing
from datetime import timedelta, datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set, Any
from contextlib import contextmanager

import polars as pl
import psutil
from tqdm import tqdm

# --- Configuration ---
BASE_SHARED_PATH = Path("/home/dynamo/a/jmckerra/projects/stampede-step-3")
SERVER_INPUT_DIR = BASE_SHARED_PATH / "input"
SERVER_OUTPUT_DIR = BASE_SHARED_PATH / "output"  # Intermediate results
SERVER_COMPLETE_DIR = BASE_SHARED_PATH / "complete"  # Final results and status
LOG_DIR = BASE_SHARED_PATH / "logs"

MAX_MEMORY_PERCENT: int = 80  # Trigger intermediate writes above this %
PROCESSING_CHUNK_DURATION: timedelta = timedelta(minutes=5)


@contextmanager
def timing(description: str):
    start = time.time()
    yield
    elapsed = time.time() - start
    logger.info(f"{description}: {elapsed:.2f} seconds")


# Enable string cache for better performance with categorical data
pl.enable_string_cache()

# Configure logging
log_dir = "/home/dynamo/a/jmckerra/projects/stampede-step-3/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"job_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Keep console output as well
    ]
)
logger = logging.getLogger('consumer')


def setup_directories():
    """Create necessary directories if they don't exist."""
    for directory in [SERVER_INPUT_DIR, SERVER_OUTPUT_DIR, SERVER_COMPLETE_DIR]:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")


def list_source_files():
    """List manifest and data files in the input directory."""
    manifest_files = []
    data_files = []

    for file in os.listdir(SERVER_INPUT_DIR):
        file_path = os.path.join(SERVER_INPUT_DIR, file)
        if file.endswith(".manifest.json"):
            manifest_files.append(file_path)
        elif file.endswith(".parquet") or file.endswith(".csv"):
            data_files.append(file_path)

    return manifest_files, data_files


def load_job_from_manifest(manifest_path):
    """Load job information from manifest file."""
    with open(manifest_path, 'r') as f:
        manifest_data = json.load(f)

    job_id = manifest_data["job_id"]
    year_month = manifest_data["year_month"]

    # Define possible directories to look in
    base_dir = SERVER_INPUT_DIR
    year_month_dir = os.path.join(SERVER_INPUT_DIR, year_month)

    # Check if metric files exist, looking in both base and year_month directories
    metric_files = manifest_data["metric_files"]
    existing_metric_files = []

    for f in metric_files:
        base_path = os.path.join(base_dir, f)
        year_month_path = os.path.join(year_month_dir, os.path.basename(f))

        if os.path.exists(base_path):
            existing_metric_files.append(base_path)
        elif os.path.exists(year_month_path):
            existing_metric_files.append(year_month_path)
            logger.info(f"Found metric file in year-month directory: {year_month_path}")

    if len(existing_metric_files) != len(metric_files):
        logger.warning(
            f"Some metric files don't exist. Expected {len(metric_files)}, found {len(existing_metric_files)}")

    # Check for sorted metric files - CHANGED: Use metric_files as sorted files
    # The producer now puts sorted files directly in metric_files
    sorted_metric_files = manifest_data.get("sorted_metric_files", [])

    # If no specific sorted_metric_files field, treat metric_files as sorted
    if not sorted_metric_files and all(f.startswith("sorted_") for f in metric_files):
        logger.info(f"Using metric_files as sorted files since they appear to be sorted")
        sorted_metric_files = metric_files

    existing_sorted_files = []

    for f in sorted_metric_files:
        base_path = os.path.join(base_dir, f)
        year_month_path = os.path.join(year_month_dir, os.path.basename(f))

        if os.path.exists(base_path):
            existing_sorted_files.append(base_path)
        elif os.path.exists(year_month_path):
            existing_sorted_files.append(year_month_path)
            logger.info(f"Found sorted metric file in year-month directory: {year_month_path}")

    if len(existing_sorted_files) != len(sorted_metric_files):
        logger.warning(
            f"Some sorted metric files don't exist. Expected {len(sorted_metric_files)}, found {len(existing_sorted_files)}")

    # Check if accounting files exist (these appear to be in the base directory)
    accounting_files = [os.path.join(base_dir, f) for f in manifest_data["accounting_files"]]
    existing_accounting_files = [f for f in accounting_files if os.path.exists(f)]

    if len(existing_accounting_files) != len(accounting_files):
        logger.warning(
            f"Some accounting files don't exist. Expected {len(accounting_files)}, found {len(existing_accounting_files)}")

    # Use existing files only
    metric_files = existing_metric_files
    sorted_metric_files = existing_sorted_files
    accounting_files = existing_accounting_files

    complete_month = manifest_data["complete_month"]

    return job_id, year_month, metric_files, sorted_metric_files, accounting_files, complete_month


def save_status(job_id, status, metadata=None):
    """Save job status to a file."""
    if metadata is None:
        metadata = {}

    status_data = {
        "job_id": job_id,
        "status": status,
        "timestamp": time.time(),
        **metadata
    }

    # Create job directory in complete dir
    job_dir = os.path.join(SERVER_COMPLETE_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    # Write status file
    status_file = os.path.join(SERVER_COMPLETE_DIR, f"{job_id}.status")
    with open(status_file, 'w') as f:
        json.dump(status_data, f)

    logger.info(f"Updated job {job_id} status to {status}")


def parse_datetime_column(df, column, formats=None):
    """Parse a datetime column with multiple format attempts"""
    if formats is None:
        formats = [
            "%m/%d/%Y %H:%M:%S",  # MM/DD/YYYY HH:MM:SS format (from CSV job data)
            "%Y-%m-%d %H:%M:%S",  # YYYY-MM-DD HH:MM:SS format (from parquet time series)
        ]

    if column not in df.columns:
        return df

    # If already datetime, return as-is
    if df.schema[column] == pl.Datetime:
        return df

    # Try each format
    for fmt in formats:
        try:
            # Try conversion with current format
            result = df.with_columns(
                pl.col(column).str.to_datetime(fmt, strict=False).alias("_datetime_temp")
            )

            # Check if we have successfully parsed values
            non_null_count = result.select(pl.col("_datetime_temp").is_not_null().sum()).item()
            total_count = len(result)

            if non_null_count > 0:
                success_pct = (non_null_count / total_count) * 100

                # Use this format if it parsed a good portion of the data
                df = df.with_columns(
                    pl.col(column).str.to_datetime(fmt, strict=False).alias(column)
                )
                return df
        except Exception as e:
            logger.debug(f"Format '{fmt}' failed for {column}: {e}")

    # If we get here, all formats failed
    logger.warning(f"Failed to parse {column} as datetime with any format")
    return df


def load_accounting_data(accounting_files):
    """Load accounting data from CSV files using LazyFrames with defined schema."""
    if not accounting_files:
        logger.warning("No accounting files provided")
        return pl.LazyFrame()

    # Define schema for better performance and reliability
    schema = {
        "jobID": pl.Utf8,
        "user": pl.Utf8,
        "account": pl.Utf8,
        "jobname": pl.Utf8,
        "queue": pl.Utf8,
        "nnodes": pl.Int64,
        "ncpus": pl.Int64,
        "walltime": pl.Int64,
        "start": pl.Utf8,
        "end": pl.Utf8,
        "submit": pl.Utf8,
        "exit_status": pl.Utf8  # Using Utf8 since some values are text
    }

    lazy_frames = []
    for file_path in accounting_files:
        try:
            # Use scan_csv to create LazyFrame
            lf = pl.scan_csv(
                file_path,
                schema_overrides=schema,
                low_memory=True,
                ignore_errors=True
            )

            # Parse date columns with correct format
            lf = lf.with_columns([
                pl.col("start").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S"),
                pl.col("end").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S"),
                pl.col("submit").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S")
            ])

            lazy_frames.append(lf)
            logger.info(f"Successfully scanned accounting file: {file_path}")
        except Exception as e:
            logger.error(f"Error scanning accounting file {file_path}: {str(e)}")

    if lazy_frames:
        result = pl.concat(lazy_frames)
        logger.info(f"Created lazy accounting dataframe from {len(lazy_frames)} files")
        return result
    else:
        return pl.LazyFrame()


def load_sorted_metric_data(sorted_files, time_filter=None):
    if not sorted_files:
        logger.warning("No sorted metric files provided")
        return pl.LazyFrame()

    # Only read essential columns that we need
    columns_needed = ["Job Id", "Timestamp", "Host", "Event", "Value"]

    lazy_frames = []
    for file_path in sorted_files:
        try:
            # Use scan_parquet to create LazyFrame, then select columns
            lf = pl.scan_parquet(file_path).select(columns_needed)

            # Filter to only keep relevant events directly
            # (This filter can stay as it is)
            lf = lf.filter(
                pl.col("Event").is_in(["cpuuser", "memused", "memused_minus_diskcache", "nfs", "block"])
            )

            # Apply time filter if provided
            if time_filter and time_filter[0] and time_filter[1]:
                start_time, end_time = time_filter
                lf = lf.filter(
                    (pl.col("Timestamp") >= start_time) &
                    (pl.col("Timestamp") <= end_time)
                )

            lazy_frames.append(lf)
            logger.info(f"Successfully scanned sorted metric file: {file_path}")
        except Exception as e:
            logger.error(f"Error scanning sorted metric file {file_path}: {str(e)}")

    if lazy_frames:
        result = pl.concat(lazy_frames)
        logger.info(f"Created lazy metrics dataframe from {len(lazy_frames)} files")
        return result
    else:
        logger.warning("No valid lazy frames created, returning empty LazyFrame")
        return pl.LazyFrame()


PROCESSING_CHUNK_DURATION: timedelta = timedelta(minutes=5)


def efficient_job_processing(
        sorted_metrics_df: pl.DataFrame,  # Input is now collected DataFrame
        accounting_df: pl.DataFrame,  # Input is now collected DataFrame
        output_base_dir: Path,
        year_month: str,
        job_id_prefix: str  # The job_id from the manifest for naming output files
) -> List[Path]:
    """
    Process job metrics and accounting data efficiently using pre-sorted metrics.
    Writes results to intermediate batch files in output_base_dir.

    Returns:
        List of paths to the directories containing intermediate result files.
    """
    processing_start_time = time.time()
    logger.info(
        f"Starting efficient processing for {year_month}. Metrics shape: {sorted_metrics_df.shape}, Accounting shape: {accounting_df.shape}")

    if "Job Id" not in sorted_metrics_df.columns:
        logger.error("'Job Id' column missing from metrics data. Cannot process.")
        return []
    if "jobID" not in accounting_df.columns:
        logger.error("'jobID' column missing from accounting data. Cannot process.")
        return []
    if sorted_metrics_df.is_empty() or accounting_df.is_empty():
        logger.warning("Input dataframes are empty. No processing needed.")
        return []

    # Determine batch size for accounting data processing
    # Aim for roughly equal work per core, but with a minimum size
    num_cores = multiprocessing.cpu_count()
    target_batches = num_cores * 2  # Process slightly more batches than cores for better balancing
    batch_size = max(100, (len(accounting_df) + target_batches - 1) // target_batches)
    logger.info(f"Accounting batch size: {batch_size} (for {len(accounting_df)} records)")

    all_batch_dirs: Set[Path] = set()
    batch_num = 0
    total_rows_written = 0

    # Iterate through accounting data in batches
    for i in range(0, len(accounting_df), batch_size):
        batch_start_time = time.time()
        batch_num += 1

        # Extract job IDs and time range - wrap in timing block
        with timing(f"Batch {batch_num}: Job ID extraction and preparation"):
            batch_acc_df = accounting_df.slice(i, batch_size)
            logger.info(f"--- Processing Accounting Batch {batch_num} ({len(batch_acc_df)} jobs) ---")
            batch_job_ids_acc: Set[str] = set(batch_acc_df["jobID"].unique().to_list())
            batch_job_ids_metrics: Set[str] = set()
            for acc_job_id in batch_job_ids_acc:
                batch_job_ids_metrics.add(acc_job_id)  # Original form
                if "jobID" in acc_job_id:
                    numeric_part = acc_job_id.replace('jobID', '')
                    batch_job_ids_metrics.add(f"JOB{numeric_part}")
                    batch_job_ids_metrics.add(f"JOBID{numeric_part}")
                else:  # Assume it might just be numeric string
                    batch_job_ids_metrics.add(f"JOB{acc_job_id}")
                    batch_job_ids_metrics.add(f"JOBID{acc_job_id}")

            batch_min_start = batch_acc_df["start"].min()
            batch_max_end = batch_acc_df["end"].max()

        if batch_min_start is None or batch_max_end is None:
            logger.warning(f"Batch {batch_num} has null start/end times, skipping metrics filtering.")
            continue

        # Pre-filter metrics for this batch's jobs and time window
        with timing(f"Batch {batch_num}: Initial metrics filtering"):
            batch_metrics_df = sorted_metrics_df.filter(
                pl.col("Job Id").is_in(list(batch_job_ids_metrics)) &
                (pl.col("Timestamp") >= batch_min_start) &
                (pl.col("Timestamp") <= batch_max_end)
            )

        if batch_metrics_df.is_empty():
            logger.info(f"Batch {batch_num}: No relevant metric data found for this batch's jobs/timeframe.")
            continue
        logger.info(f"Batch {batch_num}: Pre-filtered metrics shape: {batch_metrics_df.shape}")

        # Process each job within the accounting batch
        batch_results: List[Dict[str, Any]] = []
        jobs_with_data_count = 0

        # Process jobs loop
        with timing(f"Batch {batch_num}: Job-level processing"):
            for job_row in tqdm(batch_acc_df.iter_rows(named=True), total=len(batch_acc_df),
                                desc=f"Batch {batch_num} Jobs",
                                leave=False):
                job_id_acc = job_row["jobID"]
                start_time = job_row["start"]
                end_time = job_row["end"]

                if not start_time or not end_time or start_time >= end_time:
                    logger.debug(f"Skipping job {job_id_acc} due to invalid time range: {start_time} - {end_time}")
                    continue

                # Create job ID variations for filtering metrics
                job_id_metrics_variations: List[str] = [job_id_acc]
                if "jobID" in job_id_acc:
                    numeric_part = job_id_acc.replace('jobID', '')
                    job_id_metrics_variations.extend([f"JOB{numeric_part}", f"JOBID{numeric_part}"])
                else:
                    job_id_metrics_variations.extend([f"JOB{job_id_acc}", f"JOBID{job_id_acc}"])

                # Filter pre-filtered batch metrics for this specific job
                with timing(f"Job {job_id_acc}: Job-specific filtering"):
                    job_metrics = batch_metrics_df.filter(
                        pl.col("Job Id").is_in(job_id_metrics_variations) &
                        (pl.col("Timestamp") >= start_time) &
                        (pl.col("Timestamp") <= end_time)
                    )

                if job_metrics.is_empty():
                    continue  # No metrics for this job in the time range

                jobs_with_data_count += 1

                # Get unique hosts for this job
                hosts = job_metrics["Host"].unique().to_list()
                host_list_str = ",".join(filter(None, hosts))  # Handle potential null hosts

                # --- Time Chunking and Aggregation ---
                current_chunk_start = start_time
                while current_chunk_start < end_time:
                    current_chunk_end = min(current_chunk_start + PROCESSING_CHUNK_DURATION, end_time)

                    # Filter metrics for the current time chunk
                    chunk_metrics = job_metrics.filter(
                        (pl.col("Timestamp") >= current_chunk_start) &
                        (pl.col("Timestamp") < current_chunk_end)  # End is exclusive
                    )

                    if chunk_metrics.is_empty():
                        current_chunk_start = current_chunk_end  # Move to next chunk
                        continue

                    # Aggregate metrics within the chunk, grouped by Host and Event
                    start_agg = time.time()
                    aggregated_chunk = chunk_metrics.group_by(["Host", "Event"]).agg(
                        pl.col("Value").mean().alias("avg_value")
                    )
                    logger.info(f"Chunk {current_chunk_start}: Aggregation: {time.time() - start_agg:.2f} seconds")

                    # Pivot the aggregated data with safer column handling
                    start_pivot = time.time()
                    try:
                        # First pivot
                        pivoted_chunk = aggregated_chunk.pivot(
                            index="Host",
                            columns="Event",
                            values="avg_value"
                        )

                        # Create a safer rename operation that only renames columns that exist
                        rename_dict = {}
                        for event_type in ["cpuuser", "memused", "memused_minus_diskcache", "nfs", "block"]:
                            if event_type in pivoted_chunk.columns:
                                rename_dict[event_type] = f"value_{event_type}"

                        # Only perform rename if we have columns to rename
                        if rename_dict:
                            pivoted_chunk = pivoted_chunk.rename(rename_dict)

                        logger.info(
                            f"Chunk {current_chunk_start}: Pivot operation: {time.time() - start_pivot:.2f} seconds")

                        # Iterate through hosts that had data in this chunk
                        start_row = time.time()
                        for host_row in pivoted_chunk.iter_rows(named=True):
                            host = host_row["Host"]
                            # Base row structure with accounting data
                            base_row = {
                                "time": current_chunk_start,  # Start of the 5-min interval
                                "submit_time": job_row["submit"],
                                "start_time": start_time,
                                "end_time": end_time,
                                "timelimit": job_row["walltime"],
                                "nhosts": job_row["nnodes"],
                                "ncores": job_row["ncpus"],
                                "account": job_row["account"],
                                "queue": job_row["queue"],
                                "host": host,
                                "jid": job_id_acc,  # Use the accounting job ID
                                "jobname": job_row["jobname"],
                                "exitcode": job_row["exit_status"],
                                "host_list": host_list_str,
                                "username": job_row["user"],
                                # Add pivoted metric values (safely get each value)
                                "value_cpuuser": host_row.get("value_cpuuser"),
                                "value_memused": host_row.get("value_memused"),
                                "value_memused_minus_diskcache": host_row.get("value_memused_minus_diskcache"),
                                "value_nfs": host_row.get("value_nfs"),
                                "value_block": host_row.get("value_block"),
                                "unit": "Mixed"  # Placeholder unit
                            }
                            batch_results.append(base_row)

                        logger.info(
                            f"Chunk {current_chunk_start}: Result row creation: {time.time() - start_row:.2f} seconds")

                    except Exception as e:
                        logger.error(f"Error processing chunk {current_chunk_start} for job {job_id_acc}: {e}")
                        logger.debug(f"Pivot columns: {aggregated_chunk['Event'].unique().to_list()}")
                        # Continue with next chunk even if this one fails

                    current_chunk_start = current_chunk_end  # Move to the next chunk

                # Memory check and intermediate write (unchanged)
                if i > 0 and i % 1000 == 0:
                    memory_percent = psutil.virtual_memory().percent
                    if memory_percent > MAX_MEMORY_PERCENT and len(batch_results) > 5000:
                        logger.warning(
                            f"High memory ({memory_percent}%) during Batch {batch_num}. Writing intermediate results.")
                        # Rest of memory check code...

        # Write batch results
        if batch_results:
            with timing(f"Batch {batch_num}: Writing results"):
                try:
                    final_batch_df = pl.DataFrame(batch_results)
                    batch_dir = output_base_dir / f"batch_{batch_num}"
                    batch_dir.mkdir(exist_ok=True)
                    batch_file = batch_dir / f"joined_data_{year_month}_batch{batch_num}.parquet"
                    final_batch_df.write_parquet(batch_file, compression="zstd", compression_level=3, use_pyarrow=True)
                    logger.info(
                        f"Wrote final batch {batch_num} with {len(final_batch_df)} rows ({jobs_with_data_count} jobs had data) to {batch_file}")
                    all_batch_dirs.add(batch_dir)
                    total_rows_written += len(final_batch_df)
                except Exception as write_err:
                    logger.error(f"Failed to write final batch file {batch_num}: {write_err}", exc_info=True)

        batch_duration = time.time() - batch_start_time
        logger.info(f"--- Accounting Batch {batch_num} finished in {batch_duration:.2f}s ---")
        gc.collect()  # Collect garbage between major batches

    processing_duration = time.time() - processing_start_time
    logger.info(
        f"Finished efficient processing for {year_month} in {processing_duration:.2f}s. Total rows written: {total_rows_written}.")
    return list(all_batch_dirs)


def process_job(
        job_id: str,
        year_month: str,
        metric_files: List[Path],  # Assumed sorted
        accounting_files: List[Path]
) -> bool:
    """
    Process a single job: load data, run efficient processing, copy results.
    """
    with timing(f"Total job processing for {job_id}"):
        global_start_time = time.time()
        logger.info(f"=== Starting job processing for {job_id} ({year_month}) ===")
        save_status(job_id, "processing")  # Update status early

        intermediate_output_dir = SERVER_OUTPUT_DIR / job_id
        final_output_dir = SERVER_COMPLETE_DIR / job_id

        try:
            # --- Load Data ---
            with timing("Loading accounting data"):
                logger.info("Loading accounting data...")
                accounting_lazy = load_accounting_data(accounting_files)
                accounting_df = accounting_lazy.collect()  # Collect accounting data (usually smaller)
                logger.info(f"Accounting data loaded: {accounting_df.shape}.")

            with timing("Loading metric data"):
                logger.info("Loading sorted metric data...")
                metrics_lazy = load_sorted_metric_data(metric_files)
                # Collect metrics data - Necessary for the current row-by-row processing logic
                metrics_df = metrics_lazy.collect()
                logger.info(f"Metric data loaded: {metrics_df.shape}.")

            if accounting_df.is_empty():
                logger.warning(f"Job {job_id}: No accounting data found or loaded. Marking as completed_no_data.")
                save_status(job_id, year_month, "completed_no_data", {"reason": "No accounting data"})
                return True  # Treat as success from workflow perspective

            if metrics_df.is_empty():
                logger.warning(f"Job {job_id}: No metric data found or loaded. Marking as completed_no_data.")
                save_status(job_id, "completed_no_data", {"reason": "No metric data"})
                return True  # Treat as success

            # Ensure output directory for intermediate files exists
            intermediate_output_dir.mkdir(exist_ok=True, parents=True)

            # --- Process Data ---
            with timing("Processing metrics and accounting data"):
                logger.info("Starting efficient job processing...")
                batch_dirs = efficient_job_processing(
                    metrics_df,
                    accounting_df,
                    intermediate_output_dir,
                    year_month,
                    job_id
                )
                logger.info(f"Processing finished.")

            # Cleanup collected dataframes to free memory before copying
            del metrics_df
            del accounting_df
            gc.collect()

            # --- Copy Results to Final Location ---
            if batch_dirs:
                with timing("Copying results to final location"):
                    copy_start = time.time()
                    logger.info(f"Copying results from {len(batch_dirs)} intermediate batch directories to final location.")
                    final_output_dir.mkdir(exist_ok=True, parents=True)  # Create final dir

                    copied_files_count = 0
                    total_files_to_copy = 0
                    files_to_copy: List[Tuple[Path, Path]] = []

                    # First, collect all files to copy
                    with timing("Finding files to copy"):
                        for batch_dir in batch_dirs:
                            try:
                                if batch_dir.is_dir():
                                    for file_path in batch_dir.glob("*.parquet"):
                                        files_to_copy.append((file_path, final_output_dir / file_path.name))
                                        total_files_to_copy += 1
                            except OSError as list_err:
                                logger.error(f"Error listing files in intermediate directory {batch_dir}: {list_err}")

                    # Copy files with progress bar
                    logger.info(f"Found {total_files_to_copy} parquet files to copy.")
                    copy_errors = 0
                    with timing("Performing file copy operations"):
                        with tqdm(total=total_files_to_copy, desc=f"Copying {job_id} results", unit="file") as pbar:
                            for source_file, dest_file in files_to_copy:
                                try:
                                    shutil.copy2(source_file, dest_file)  # copy2 preserves metadata
                                    copied_files_count += 1
                                except Exception as copy_err:
                                    logger.error(f"Failed to copy {source_file.name} to {dest_file}: {copy_err}", exc_info=True)
                                    copy_errors += 1
                                pbar.update(1)

                    copy_duration = time.time() - copy_start
                    logger.info(f"Finished copying {copied_files_count} files ({copy_errors} errors) in {copy_duration:.2f}s")

                    if copy_errors > 0:
                        raise IOError(f"{copy_errors} errors occurred during file copying to final destination.")
                    if copied_files_count == 0 and total_files_to_copy > 0:
                        raise IOError("No files were successfully copied despite intermediate files existing.")

                # --- Final Status Update ---
                total_job_time = time.time() - global_start_time
                save_status(job_id, year_month, "completed", {
                    "total_batches_processed": len(batch_dirs),
                    "files_copied_to_complete": copied_files_count,
                    "processing_time_seconds": round(total_job_time, 2),
                })
                logger.info(f"=== Job {job_id} completed successfully in {total_job_time:.2f}s ===")
                return True
            else:
                # Processing ran but produced no output batches
                logger.warning(f"Job {job_id}: Processing finished, but no result batches were generated.")
                total_job_time = time.time() - global_start_time
                save_status(job_id, year_month, "completed_no_data", {
                    "reason": "Processing generated no output files",
                    "processing_time_seconds": round(total_job_time, 2),
                })
                logger.info(f"=== Job {job_id} completed (no data) in {total_job_time:.2f}s ===")
                return True

        except Exception as e:
            total_job_time = time.time() - global_start_time
            logger.error(f"!!! Error processing job {job_id}: {e}", exc_info=True)
            save_status(job_id, "failed", {
                "error": str(e),
                "processing_time_seconds": round(total_job_time, 2),
            })
            return False
        finally:
            # --- Cleanup Intermediate Files ---
            try:
                with timing("Cleaning up intermediate files"):
                    if intermediate_output_dir.exists():
                        logger.info(f"Cleaning up intermediate directory: {intermediate_output_dir}")
                        shutil.rmtree(intermediate_output_dir)
            except Exception as cleanup_err:
                logger.error(f"Error cleaning up intermediate directory {intermediate_output_dir}: {cleanup_err}",
                            exc_info=True)


def process_manifest(manifest_path):
    """Process a manifest file."""
    try:
        # Load job info from manifest
        job_id, year_month, metric_files, sorted_metric_files, accounting_files, complete_month = load_job_from_manifest(
            manifest_path)

        # Update job status to processing
        save_status(job_id, "processing")

        # Process the job
        success = process_job(job_id, year_month, metric_files, accounting_files)

        if success:
            logger.info(f"Manifest {manifest_path} processed successfully")
        else:
            logger.error(f"Failed to process manifest {manifest_path}")

        # Remove manifest even if processing failed
        try:
            os.remove(manifest_path)
            logger.info(f"Removed manifest {manifest_path}")
        except Exception as e:
            logger.error(f"Error removing manifest {manifest_path}: {str(e)}")

    except Exception as e:
        logger.error(f"Error processing manifest {manifest_path}: {str(e)}")
        logger.error(traceback.format_exc())


def main():
    """Main function that continuously processes job manifests."""
    setup_directories()

    # Configure optimal thread count
    global NUM_THREADS
    NUM_THREADS = max(10, min(16, multiprocessing.cpu_count()))

    logger.info(f"Starting job processor with {NUM_THREADS} threads and {MAX_MEMORY_PERCENT}% memory limit")

    cycle_count = 0

    while True:
        try:
            cycle_start = time.time()
            cycle_count += 1
            logger.info(f"Starting processor cycle {cycle_count}")

            # Report system status
            memory_info = psutil.virtual_memory()
            logger.info(
                f"Memory: {memory_info.percent}% used ({memory_info.used / 1024 ** 3:.1f}GB / {memory_info.total / 1024 ** 3:.1f}GB)")

            # List manifest files
            manifest_files, _ = list_source_files()

            if manifest_files:
                logger.info(f"Found {len(manifest_files)} manifests to process")

                # Process each manifest sequentially
                for manifest_path in manifest_files:
                    try:
                        # Get the manifest name for logging
                        manifest_name = os.path.basename(manifest_path)
                        logger.info(f"Processing manifest {manifest_name}")

                        # Process this manifest
                        process_manifest(manifest_path)

                        # Check memory after each job
                        memory_percent = psutil.virtual_memory().percent
                        if memory_percent > 90:
                            logger.warning(f"High memory usage ({memory_percent}%), taking a break")
                            time.sleep(60)  # Give system time to recover
                    except Exception as e:
                        logger.error(f"Error processing manifest {manifest_path}: {str(e)}")
                        logger.error(traceback.format_exc())

                        # Try to remove the manifest even if processing failed
                        try:
                            os.remove(manifest_path)
                            logger.info(f"Removed problematic manifest {manifest_path}")
                        except Exception as remove_error:
                            logger.error(f"Could not remove manifest {manifest_path}: {str(remove_error)}")
            else:
                logger.info("No manifests found, waiting...")

            # Log cycle duration
            cycle_duration = time.time() - cycle_start
            logger.info(f"Processor cycle {cycle_count} completed in {cycle_duration:.1f}s")

            # Sleep before next cycle
            time.sleep(10)

        except Exception as e:
            logger.error(f"Error in processor cycle: {str(e)}")
            logger.error(traceback.format_exc())
            time.sleep(30)  # Longer sleep on main loop error for system recovery


if __name__ == "__main__":
    main()
