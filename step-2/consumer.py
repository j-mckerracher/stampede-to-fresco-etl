import json
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
import math  # For ceiling division

import polars as pl
import numpy as np  # For np.nan
import psutil
from tqdm import tqdm

# --- Configuration ---
BASE_SHARED_PATH = Path("/home/dynamo/a/jmckerra/projects/stampede-step-2")
SERVER_INPUT_DIR = BASE_SHARED_PATH / "input"
SERVER_OUTPUT_DIR = BASE_SHARED_PATH / "output"  # Intermediate results
SERVER_COMPLETE_DIR = BASE_SHARED_PATH / "complete"  # Final results and status
LOG_DIR = BASE_SHARED_PATH / "logs"

MAX_MEMORY_PERCENT: int = 80  # Trigger intermediate writes above this %
PROCESSING_CHUNK_DURATION: timedelta = timedelta(minutes=1)
# Define the exact output columns required
OUTPUT_COLUMNS = [
    "time", "submit_time", "start_time", "end_time", "timelimit", "nhosts",
    "ncores", "account", "queue", "host", "jid", "jobname", "exitcode",
    "host_list", "username", "value_cpuuser", "value_gpu", "value_memused",
    "value_memused_minus_diskcache", "value_nfs", "value_block"
]

# Polars configuration
try:
    pl.enable_string_cache(True)
except TypeError:
    pl.enable_string_cache()  # For older versions

# --- Logging Setup ---
LOG_DIR.mkdir(exist_ok=True, parents=True)
log_file = LOG_DIR / f"consumer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('consumer')


@contextmanager
def timing(description: str):
    start_mem = psutil.virtual_memory().used
    start = time.time()
    yield
    elapsed = time.time() - start
    end_mem = psutil.virtual_memory().used
    mem_diff = end_mem - start_mem
    logger.info(f"{description}: {elapsed:.2f} seconds. Memory change: {mem_diff / (1024 ** 2):+.1f} MB")


# --- Directory Setup ---
def setup_directories() -> None:
    """Create necessary base directories if they don't exist."""
    try:
        for directory in [SERVER_INPUT_DIR, SERVER_OUTPUT_DIR, SERVER_COMPLETE_DIR, LOG_DIR]:
            directory.mkdir(exist_ok=True, parents=True)
            logger.info(f"Ensured directory exists: {directory}")
    except OSError as e:
        logger.critical(f"Failed to create or access required directory: {e}. Check permissions.", exc_info=True)
        raise


# --- File Operations ---
def list_manifest_files() -> List[Path]:
    """List manifest files in the input directory."""
    if not SERVER_INPUT_DIR.exists():
        logger.warning(f"Input directory {SERVER_INPUT_DIR} not found.")
        return []
    try:
        return sorted(list(SERVER_INPUT_DIR.glob("*.manifest.json")))
    except OSError as e:
        logger.error(f"Error listing manifest files in {SERVER_INPUT_DIR}: {e}", exc_info=True)
        return []


def load_job_from_manifest(manifest_path: Path) -> Tuple[str, str, List[Path], List[Path], bool]:
    """Load job information from manifest file."""
    # (Keep existing implementation - it seems correct)
    logger.info(f"Loading manifest: {manifest_path.name}")
    with open(manifest_path, 'r') as f:
        manifest_data = json.load(f)

    job_id: str = manifest_data["job_id"]
    year_month: str = manifest_data["year_month"]
    complete_month: bool = manifest_data["complete_month"]
    metric_filenames: List[str] = manifest_data["metric_files"]
    accounting_filenames: List[str] = manifest_data["accounting_files"]

    year_month_dir = SERVER_INPUT_DIR / year_month
    metric_files: List[Path] = []
    for fname in metric_filenames:
        expected_path = year_month_dir / fname
        if expected_path.exists():
            metric_files.append(expected_path)
        else:
            logger.warning(f"Metric file specified in manifest not found: {expected_path}")

    if len(metric_files) != len(metric_filenames):
        logger.warning(f"Job {job_id}: Found {len(metric_files)} out of {len(metric_filenames)} expected metric files.")

    accounting_files_paths: List[Path] = []
    for fname in accounting_filenames:
        expected_path = SERVER_INPUT_DIR / fname
        if expected_path.exists():
            accounting_files_paths.append(expected_path)
        else:
            logger.warning(f"Accounting file specified in manifest not found: {expected_path}")

    if len(accounting_files_paths) != len(accounting_filenames):
        logger.warning(
            f"Job {job_id}: Found {len(accounting_files_paths)} out of {len(accounting_filenames)} expected accounting files.")

    logger.info(
        f"Job {job_id} ({year_month}): Found {len(metric_files)} metric files, {len(accounting_files_paths)} accounting files.")
    return job_id, year_month, metric_files, accounting_files_paths, complete_month


def save_status(job_id: str, year_month: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
    """Save job status to a file in the SERVER_COMPLETE_DIR."""
    # (Keep existing implementation - it seems correct)
    if metadata is None:
        metadata = {}
    status_data = {
        "job_id": job_id,
        "year_month": year_month,
        "status": status,
        "timestamp": datetime.now().isoformat(),
        **metadata
    }
    status_file = SERVER_COMPLETE_DIR / f"{job_id}.status"
    try:
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=4)
        logger.info(f"Updated job {job_id} status to '{status}' in {status_file}")
    except OSError as e:
        logger.error(f"Failed to write status file {status_file}: {e}", exc_info=True)
    except TypeError as e:
        logger.error(f"Failed to serialize status data for {job_id} to JSON: {e}", exc_info=True)


# --- Data Loading & Preprocessing ---

# REVISED: Added Job ID Normalization Function
def normalize_job_id(job_id_series: pl.Expr) -> pl.Expr:
    """
    Normalizes job IDs from various formats (e.g., "jobID123", "JOB123", "123")
    to a consistent string format (e.g., "123").
    Handles nulls gracefully.
    """
    # Extract numeric part assuming prefixes like 'jobID', 'JOBID', 'JOB' or just numbers
    # Use regex to capture the numeric part robustly
    return (
        job_id_series
        .str.extract(r"(\d+)$", 1)  # Extract the trailing digits
        .fill_null(job_id_series.cast(pl.Utf8))  # If no digits found, use original (casted to string)
        .cast(pl.Utf8)  # Ensure final type is Utf8
    )


# REVISED: Apply normalization during loading
def load_accounting_data(accounting_files: List[Path]) -> pl.LazyFrame:
    """Load accounting data, parse dates, and normalize jobID."""
    if not accounting_files:
        logger.warning("No accounting files provided for loading.")
        return pl.LazyFrame()

    schema = {
        "jobID": pl.Utf8, "user": pl.Utf8, "account": pl.Utf8, "jobname": pl.Utf8,
        "queue": pl.Utf8, "nnodes": pl.Int64, "ncpus": pl.Int64, "walltime": pl.Int64,
        "start": pl.Utf8, "end": pl.Utf8, "submit": pl.Utf8,
        "exit_status": pl.Utf8
    }
    date_format = "%m/%d/%Y %H:%M:%S"

    lazy_frames: List[pl.LazyFrame] = []
    for file_path in accounting_files:
        try:
            lf = pl.scan_csv(
                file_path,
                schema_overrides=schema,
                low_memory=True,
                ignore_errors=True,
                null_values=["NA", "NULL", ""],
            )
            lf = lf.with_columns([
                pl.col("start").str.strptime(pl.Datetime, format=date_format, strict=False, exact=True).alias("start"),
                pl.col("end").str.strptime(pl.Datetime, format=date_format, strict=False, exact=True).alias("end"),
                pl.col("submit").str.strptime(pl.Datetime, format=date_format, strict=False, exact=True).alias(
                    "submit"),
                # Normalize jobID and create 'jid' column
                normalize_job_id(pl.col("jobID")).alias("jid")
            ])
            lf = lf.drop_nulls(subset=["start", "end", "submit", "jid"])  # Ensure essential cols are non-null

            lazy_frames.append(lf)
            logger.debug(f"Successfully scanned accounting file: {file_path.name}")
        except Exception as e:
            logger.error(f"Error scanning accounting file {file_path}: {e}", exc_info=True)

    if lazy_frames:
        result = pl.concat(lazy_frames, how="vertical_relaxed")  # Use relaxed for schema flexibility if needed
        logger.info(f"Created lazy accounting dataframe from {len(lazy_frames)} files.")
        return result
    else:
        logger.warning("No valid accounting lazy frames created.")
        return pl.LazyFrame()


# REVISED: Apply normalization during loading
def load_sorted_metric_data(sorted_metric_files: List[Path]) -> pl.LazyFrame:
    """Load pre-sorted metric data, normalize 'Job Id', filter events."""
    if not sorted_metric_files:
        logger.warning("No sorted metric files provided for loading.")
        return pl.LazyFrame()

    columns_needed = ["Job Id", "Timestamp", "Host", "Event", "Value"]
    relevant_events = ["cpuuser", "memused", "memused_minus_diskcache", "nfs", "block"]
    timestamp_format = "%Y-%m-%d %H:%M:%S"

    lazy_frames: List[pl.LazyFrame] = []
    for file_path in sorted_metric_files:
        try:
            lf = pl.scan_parquet(file_path)
            # logger.debug(f"Scanned metric file: {file_path.name} with columns: {lf.columns}")

            # Handle case differences in column names
            column_mapping = {}
            current_columns_lower = {col.lower(): col for col in lf.columns}
            required_lower = {col.lower(): col for col in columns_needed}
            final_columns = {}  # Map desired name -> actual name

            for req_lower, req_orig in required_lower.items():
                if req_lower in current_columns_lower:
                    final_columns[req_orig] = current_columns_lower[req_lower]
                else:
                    logger.error(
                        f"Missing required column '{req_orig}' (or case variation) in {file_path.name}. Skipping file.")
                    raise ValueError(f"Missing column {req_orig}")  # Raise to skip file

            # Rename columns if necessary (actual_name -> desired_name)
            rename_map = {v: k for k, v in final_columns.items() if k != v}
            if rename_map:
                lf = lf.rename(rename_map)

            lf = lf.select(list(final_columns.keys()))  # Select only the needed columns by their desired names

            # Ensure Timestamp is datetime
            if lf.schema["Timestamp"] != pl.Datetime:
                lf = lf.with_columns(
                    pl.col("Timestamp").str.strptime(pl.Datetime, format=timestamp_format, strict=False).alias(
                        "Timestamp")
                )

            # Normalize 'Job Id' and create 'jid' column
            lf = lf.with_columns(
                normalize_job_id(pl.col("Job Id")).alias("jid")
            )

            # Filter relevant events and drop nulls in essential columns
            lf = lf.filter(pl.col("Event").is_in(relevant_events))
            lf = lf.drop_nulls(subset=["Timestamp", "Host", "Event", "Value", "jid"])

            lazy_frames.append(lf)
        except Exception as e:
            logger.error(f"Error scanning/preprocessing sorted metric file {file_path}: {e}", exc_info=True)

    if lazy_frames:
        result = pl.concat(lazy_frames, how="vertical_relaxed")
        logger.info(f"Created lazy metrics dataframe from {len(lazy_frames)} files.")
        return result
    else:
        logger.warning("No valid metric lazy frames created.")
        return pl.LazyFrame()


# --- Core Processing Logic ---
# REVISED: Uses 'jid', calculates midpoint time, adds 'value_gpu', uses defined OUTPUT_COLUMNS
def efficient_job_processing(
        metrics_df: pl.DataFrame,  # Input is now collected DataFrame with 'jid'
        accounting_df: pl.DataFrame,  # Input is now collected DataFrame with 'jid'
        output_base_dir: Path,
        year_month: str,
        job_id_prefix: str  # Used for naming output files
) -> List[Path]:
    """
    Process job metrics and accounting data efficiently using normalized 'jid'.
    Writes results adhering to the specified output schema.

    Returns:
        List of paths to the directories containing intermediate result files.
    """
    processing_start_time = time.time()
    logger.info(
        f"Starting efficient processing for {year_month}. Metrics shape: {metrics_df.shape}, Accounting shape: {accounting_df.shape}")

    # Check for 'jid' column
    if "jid" not in metrics_df.columns:
        logger.error("'jid' column missing from metrics data after preprocessing. Cannot process.")
        return []
    if "jid" not in accounting_df.columns:
        logger.error("'jid' column missing from accounting data after preprocessing. Cannot process.")
        return []

    if metrics_df.is_empty() or accounting_df.is_empty():
        logger.warning("Input dataframes are empty. No processing needed.")
        return []

    # Optional: Pre-sort metrics for potentially faster filtering within jobs
    # metrics_df = metrics_df.sort("jid", "Timestamp")

    # Determine batch size for accounting data processing
    num_cores = multiprocessing.cpu_count()
    target_batches = num_cores * 2
    # Ensure batch_size is at least 1, use ceiling division
    batch_size = max(1, math.ceil(len(accounting_df) / target_batches))
    logger.info(f"Accounting batch size: {batch_size} (for {len(accounting_df)} records)")

    all_batch_dirs: Set[Path] = set()
    batch_num = 0
    total_rows_written = 0

    # Iterate through accounting data in batches
    for i in range(0, len(accounting_df), batch_size):
        batch_start_time = time.time()
        batch_num += 1
        batch_acc_df = accounting_df.slice(i, batch_size)
        logger.info(f"--- Processing Accounting Batch {batch_num} ({len(batch_acc_df)} jobs) ---")

        # Extract job IDs and time range for the batch
        with timing(f"Batch {batch_num}: Job ID extraction and time range"):
            batch_jids: Set[str] = set(batch_acc_df["jid"].unique().to_list())
            batch_min_start = batch_acc_df["start"].min()
            batch_max_end = batch_acc_df["end"].max()

        if batch_min_start is None or batch_max_end is None:
            logger.warning(f"Batch {batch_num} has null start/end times, skipping metrics filtering.")
            continue

        # Pre-filter metrics for this batch's jobs and time window
        # This is an optimization to reduce the search space for each job
        with timing(f"Batch {batch_num}: Initial metrics filtering"):
            # Filter using the normalized 'jid'
            batch_metrics_df = metrics_df.filter(
                pl.col("jid").is_in(list(batch_jids)) &
                (pl.col("Timestamp") >= batch_min_start) &
                (pl.col("Timestamp") < batch_max_end)  # Use exclusive end for consistency
            )

        if batch_metrics_df.is_empty():
            logger.info(f"Batch {batch_num}: No relevant metric data found for this batch's jobs/timeframe.")
            continue
        logger.info(f"Batch {batch_num}: Pre-filtered metrics shape: {batch_metrics_df.shape}")

        # Process each job within the accounting batch
        batch_results: List[Dict[str, Any]] = []
        jobs_with_data_count = 0

        with timing(f"Batch {batch_num}: Job-level processing"):
            # Iterate through accounting job records in the current batch
            for job_row in tqdm(batch_acc_df.iter_rows(named=True), total=len(batch_acc_df),
                                desc=f"Batch {batch_num} Jobs", leave=False):
                job_jid = job_row["jid"]
                start_time = job_row["start"]
                end_time = job_row["end"]

                # Basic validation
                if not start_time or not end_time or start_time >= end_time:
                    logger.debug(f"Skipping job {job_jid} due to invalid time range: {start_time} - {end_time}")
                    continue

                # Filter the pre-filtered batch metrics for THIS specific job's jid and exact time range
                # No timing block here as it should be faster with pre-filtering
                job_metrics = batch_metrics_df.filter(
                    (pl.col("jid") == job_jid) &
                    (pl.col("Timestamp") >= start_time) &
                    (pl.col("Timestamp") < end_time)  # Job end time is exclusive for intervals
                )

                if job_metrics.is_empty():
                    # logger.debug(f"No metrics found for job {job_jid} between {start_time} and {end_time}")
                    continue  # No metrics for this job in its execution window

                jobs_with_data_count += 1

                # Get unique hosts FOR THIS JOB from its metrics
                hosts = job_metrics["Host"].unique().to_list()
                host_list_str = ",".join(sorted(filter(None, hosts)))  # Sorted list

                # --- Time Chunking and Aggregation ---
                current_interval_start = start_time
                while current_interval_start < end_time:
                    current_interval_end = min(current_interval_start + PROCESSING_CHUNK_DURATION, end_time)

                    # Calculate interval midpoint (as required by objective)
                    # Ensure midpoint calculation works correctly even for microsecond precision
                    time_diff_micros = int((current_interval_end - current_interval_start).total_seconds() * 1_000_000)
                    interval_midpoint = current_interval_start + timedelta(microseconds=time_diff_micros // 2)

                    # Filter metrics for the current time interval [start, end)
                    interval_metrics = job_metrics.filter(
                        (pl.col("Timestamp") >= current_interval_start) &
                        (pl.col("Timestamp") < current_interval_end)  # Interval end is exclusive
                    )

                    if interval_metrics.is_empty():
                        current_interval_start = current_interval_end  # Move to next interval
                        continue

                    # Aggregate metrics within the interval, grouped by Host and Event
                    # This implicitly filters for hosts present in this interval
                    aggregated_interval = interval_metrics.group_by(["Host", "Event"]).agg(
                        pl.col("Value").mean().alias("avg_value")
                    )

                    if aggregated_interval.is_empty():  # Should not happen if interval_metrics wasn't empty, but safe check
                        current_interval_start = current_interval_end
                        continue

                    # Pivot the aggregated data to get Events as columns per Host
                    try:
                        pivoted_interval = aggregated_interval.pivot(
                            index="Host",
                            columns="Event",
                            values="avg_value"
                        )

                        # Rename pivoted columns to match output spec ('value_*')
                        # Only rename columns that actually exist after pivoting
                        rename_dict = {}
                        event_cols_present = pivoted_interval.columns[1:]  # Skip 'Host' column
                        for event_type in ["cpuuser", "memused", "memused_minus_diskcache", "nfs", "block"]:
                            if event_type in event_cols_present:
                                rename_dict[event_type] = f"value_{event_type}"
                        if rename_dict:
                            pivoted_interval = pivoted_interval.rename(rename_dict)

                        # Iterate through hosts that had data in this interval (rows in pivoted_interval)
                        for host_row in pivoted_interval.iter_rows(named=True):
                            host = host_row["Host"]
                            if not host: continue  # Skip if host is null/empty

                            # Construct the output row dictionary according to the objective
                            output_row = {
                                # Interval/Host specific fields
                                "time": interval_midpoint,  # Use midpoint time
                                "host": host,

                                # Job constant fields from accounting_df
                                "submit_time": job_row["submit"],
                                "start_time": start_time,
                                "end_time": end_time,
                                "timelimit": job_row["walltime"],
                                "nhosts": job_row["nnodes"],
                                "ncores": job_row["ncpus"],
                                "account": job_row["account"],
                                "queue": job_row["queue"],
                                "jid": job_jid,  # Use the normalized jid
                                "jobname": job_row["jobname"],
                                "exitcode": job_row["exit_status"],
                                "username": job_row["user"],

                                # Other fields
                                "host_list": host_list_str,  # Comma-separated list of all hosts for the job

                                # Aggregated metric values (use .get for safety, default to NaN)
                                "value_cpuuser": host_row.get("value_cpuuser", np.nan),
                                "value_memused": host_row.get("value_memused", np.nan),
                                "value_memused_minus_diskcache": host_row.get("value_memused_minus_diskcache", np.nan),
                                "value_nfs": host_row.get("value_nfs", np.nan),
                                "value_block": host_row.get("value_block", np.nan),

                                # value_gpu is specifically requested, set to NaN as we don't have input data
                                "value_gpu": np.nan,
                            }
                            batch_results.append(output_row)

                    except Exception as pivot_err:
                        # Log error during pivoting/row creation for this interval
                        logger.error(
                            f"Error processing interval {current_interval_start} for job {job_jid}: {pivot_err}",
                            exc_info=True)
                        # Attempt to log available events if possible
                        try:
                            events_in_interval = aggregated_interval['Event'].unique().to_list()
                            logger.debug(f"Events present during failed pivot: {events_in_interval}")
                        except:
                            logger.debug("Could not retrieve events during pivot error.")
                        # Continue to the next interval

                    # Move to the next interval
                    current_interval_start = current_interval_end

                # --- Memory Check (Optional but good practice) ---
                # Check memory periodically within the batch loop if processing many small jobs
                # This example keeps the check less frequent (e.g., end of job iteration)
                # Consider adding a check here if jobs are extremely long running

        # --- End of Job Loop for the Batch ---

        # Write batch results if any were generated
        if batch_results:
            with timing(f"Batch {batch_num}: Writing results"):
                try:
                    # Create DataFrame from the list of dictionaries
                    final_batch_df = pl.DataFrame(batch_results)

                    # Ensure columns are in the specified order and add missing ones as null
                    # This also handles the case where some value_* columns might be missing entirely if
                    # those events never occurred in the batch.
                    cols_to_select = []
                    for col_name in OUTPUT_COLUMNS:
                        if col_name in final_batch_df.columns:
                            cols_to_select.append(pl.col(col_name))
                        else:
                            # If a column is missing entirely (e.g., value_nfs never appeared)
                            # add it as a literal null column with the correct type (Float64 for values)
                            col_type = pl.Float64 if col_name.startswith(
                                "value_") else pl.Utf8  # Adjust types as needed
                            logger.warning(
                                f"Column '{col_name}' not found in batch {batch_num} results, adding as null ({col_type}).")
                            # Handle specific types if needed (e.g., Int64 for counts, Datetime for times)
                            if col_name in ["submit_time", "start_time", "end_time", "time"]:
                                col_type = pl.Datetime
                            elif col_name in ["nhosts", "ncores", "timelimit"]:  # Assuming these should be Int
                                col_type = pl.Int64
                            elif col_name in ["exitcode"]:  # Assuming exitcode might be int or string
                                col_type = pl.Utf8  # Keep as string for flexibility

                            cols_to_select.append(pl.lit(None, dtype=col_type).alias(col_name))

                    final_batch_df = final_batch_df.select(cols_to_select)

                    # Define output directory and file path
                    batch_dir = output_base_dir / f"batch_{batch_num}"
                    batch_dir.mkdir(exist_ok=True, parents=True)
                    batch_file = batch_dir / f"joined_data_{year_month}_batch{batch_num}.parquet"

                    # Write to Parquet
                    final_batch_df.write_parquet(
                        batch_file,
                        compression="zstd",
                        compression_level=3,
                        use_pyarrow=True,  # Often needed for datetime/nan handling
                        statistics=True  # Write statistics for faster reads later
                    )
                    logger.info(
                        f"Wrote final batch {batch_num} with {len(final_batch_df)} rows ({jobs_with_data_count} jobs had data) to {batch_file}")
                    all_batch_dirs.add(batch_dir)
                    total_rows_written += len(final_batch_df)

                    # Memory management check (after writing results)
                    memory_percent = psutil.virtual_memory().percent
                    if memory_percent > MAX_MEMORY_PERCENT:
                        logger.warning(
                            f"High memory usage ({memory_percent}%) after writing batch {batch_num}. Triggering GC.")
                        del final_batch_df  # Explicitly delete
                        gc.collect()
                        logger.info(f"Memory usage after GC: {psutil.virtual_memory().percent:.1f}%")

                except Exception as write_err:
                    logger.error(f"Failed to write final batch file {batch_num}: {write_err}", exc_info=True)

        batch_duration = time.time() - batch_start_time
        logger.info(f"--- Accounting Batch {batch_num} finished in {batch_duration:.2f}s ---")
        # Explicit garbage collection between batches can sometimes help
        del batch_acc_df
        del batch_metrics_df
        if batch_results: del final_batch_df
        gc.collect()

    processing_duration = time.time() - processing_start_time
    logger.info(
        f"Finished efficient processing for {year_month} in {processing_duration:.2f}s. Total rows written: {total_rows_written}.")
    return list(all_batch_dirs)


# --- Job Orchestration ---
# REVISED: Ensure LazyFrames are collected *after* preprocessing (adding jid)
def process_job(
        job_id: str,
        year_month: str,
        metric_files: List[Path],
        accounting_files: List[Path]
) -> bool:
    """
    Process a single job: load & preprocess data, run processing, copy results.
    """
    with timing(f"Total job processing for {job_id}"):
        global_start_time = time.time()
        logger.info(f"=== Starting job processing for {job_id} ({year_month}) ===")
        save_status(job_id, year_month, "processing")

        intermediate_output_dir = SERVER_OUTPUT_DIR / job_id
        final_output_dir = SERVER_COMPLETE_DIR / job_id

        try:
            # --- Load and Preprocess Data ---
            with timing("Loading and preprocessing accounting data"):
                logger.info("Loading accounting data...")
                # Load lazy, preprocess (incl. normalize jid), then collect
                accounting_lazy = load_accounting_data(accounting_files)
                accounting_df = accounting_lazy.collect(engine='streaming')  # Use streaming if data might be large
                logger.info(f"Accounting data loaded and preprocessed: {accounting_df.shape}.")

            with timing("Loading and preprocessing metric data"):
                logger.info("Loading sorted metric data...")
                # Load lazy, preprocess (incl. normalize jid), then collect
                metrics_lazy = load_sorted_metric_data(metric_files)
                metrics_df = metrics_lazy.collect(engine='streaming')  # Use streaming
                logger.info(f"Metric data loaded and preprocessed: {metrics_df.shape}.")

            # Check for empty dataframes after loading and preprocessing
            if accounting_df.is_empty():
                logger.warning(
                    f"Job {job_id}: No valid accounting data after preprocessing. Marking as completed_no_data.")
                save_status(job_id, year_month, "completed_no_data", {"reason": "No valid accounting data"})
                return True  # Treat as success

            if metrics_df.is_empty():
                logger.warning(
                    f"Job {job_id}: No relevant metric data after preprocessing. Marking as completed_no_data.")
                # Still process accounting data if needed for other purposes? No, objective requires both.
                save_status(job_id, year_month, "completed_no_data", {"reason": "No relevant metric data"})
                return True  # Treat as success

            # Ensure intermediate output directory exists
            intermediate_output_dir.mkdir(exist_ok=True, parents=True)

            # --- Process Data ---
            with timing("Core data processing (aggregation and joining)"):
                logger.info("Starting efficient job processing...")
                batch_dirs = efficient_job_processing(
                    metrics_df,
                    accounting_df,
                    intermediate_output_dir,
                    year_month,
                    job_id  # Pass the original job_id for file naming if needed
                )
                logger.info(f"Core processing finished. Found {len(batch_dirs)} output batch directories.")

            # --- Cleanup DataFrames ---
            # Explicitly delete large DataFrames and collect garbage before copying
            with timing("DataFrame cleanup"):
                del metrics_df
                del accounting_df
                gc.collect()
                logger.info(f"Memory usage after DF cleanup: {psutil.virtual_memory().percent:.1f}%")

            # --- Copy Results to Final Location ---
            if batch_dirs:
                with timing("Copying results to final location"):
                    copy_start = time.time()
                    logger.info(
                        f"Copying results from {len(batch_dirs)} intermediate batch directories to final location: {final_output_dir}")
                    final_output_dir.mkdir(exist_ok=True, parents=True)  # Create final dir

                    copied_files_count = 0
                    total_files_to_copy = 0
                    files_to_copy: List[Tuple[Path, Path]] = []

                    # Collect files to copy first
                    for batch_dir in batch_dirs:
                        try:
                            if batch_dir.is_dir():
                                for file_path in batch_dir.glob("*.parquet"):
                                    files_to_copy.append((file_path, final_output_dir / file_path.name))
                                    total_files_to_copy += 1
                        except OSError as list_err:
                            logger.error(f"Error listing files in intermediate directory {batch_dir}: {list_err}")

                    logger.info(f"Found {total_files_to_copy} parquet files to copy.")
                    copy_errors = 0
                    # Copy files with progress bar
                    with tqdm(total=total_files_to_copy, desc=f"Copying {job_id} results", unit="file",
                              leave=False) as pbar:
                        for source_file, dest_file in files_to_copy:
                            try:
                                # Use move for speed if source and dest are on the same filesystem,
                                # otherwise fallback to copy. Check first.
                                try:
                                    source_dev = source_file.stat().st_dev
                                    dest_dev = final_output_dir.stat().st_dev
                                    if source_dev == dest_dev:
                                        shutil.move(str(source_file), str(dest_file))
                                    else:
                                        shutil.copy2(source_file, dest_file)  # copy2 preserves metadata
                                except OSError:  # Fallback if stat fails or other OS issues
                                    shutil.copy2(source_file, dest_file)
                                copied_files_count += 1
                            except Exception as copy_err:
                                logger.error(f"Failed to copy/move {source_file.name} to {dest_file}: {copy_err}",
                                             exc_info=True)
                                copy_errors += 1
                            pbar.update(1)

                    copy_duration = time.time() - copy_start
                    logger.info(
                        f"Finished copying/moving {copied_files_count} files ({copy_errors} errors) in {copy_duration:.2f}s")

                    if copy_errors > 0:
                        raise IOError(f"{copy_errors} errors occurred during file transfer to final destination.")
                    if copied_files_count == 0 and total_files_to_copy > 0:
                        # This case should be less likely with the move/copy logic, but check anyway
                        logger.warning("No files were transferred despite intermediate files existing.")
                        # Don't raise an error, but log it. The status below will reflect 0 files.

                # --- Final Status Update ---
                total_job_time = time.time() - global_start_time
                save_status(job_id, year_month, "completed", {
                    "total_batches_processed": len(batch_dirs),
                    "files_transferred_to_complete": copied_files_count,
                    "processing_time_seconds": round(total_job_time, 2),
                })
                logger.info(f"=== Job {job_id} completed successfully in {total_job_time:.2f}s ===")
                return True
            else:
                # Processing ran but produced no output batches (e.g., jobs filtered out, no metrics found)
                logger.warning(f"Job {job_id}: Processing finished, but no result batches were generated.")
                total_job_time = time.time() - global_start_time
                save_status(job_id, year_month, "completed_no_data", {
                    "reason": "Processing generated no output files (no matching data or all jobs filtered)",
                    "processing_time_seconds": round(total_job_time, 2),
                })
                logger.info(f"=== Job {job_id} completed (no data generated) in {total_job_time:.2f}s ===")
                return True

        except Exception as e:
            total_job_time = time.time() - global_start_time
            logger.error(f"!!! Error processing job {job_id}: {e}", exc_info=True)
            # Ensure status reflects failure
            save_status(job_id, year_month, "failed", {
                "error": str(e),
                "traceback": traceback.format_exc(),  # Include traceback in status
                "processing_time_seconds": round(total_job_time, 2),
            })
            return False
        finally:
            # --- Cleanup Intermediate Files ---
            # Always attempt cleanup, even if the job failed
            try:
                with timing("Cleaning up intermediate files"):
                    if intermediate_output_dir.exists():
                        logger.info(f"Cleaning up intermediate directory: {intermediate_output_dir}")
                        shutil.rmtree(intermediate_output_dir)
            except Exception as cleanup_err:
                logger.error(f"Error cleaning up intermediate directory {intermediate_output_dir}: {cleanup_err}",
                             exc_info=True)


def process_manifest(manifest_path: Path) -> None:
    """Load, process, and handle cleanup for a single manifest file."""
    # (Keep existing implementation - seems correct)
    job_id = manifest_path.stem.replace(".manifest", "")  # More robust stem extraction
    logger.info(f"--- Processing Manifest: {manifest_path.name} (Job ID: {job_id}) ---")
    processed_successfully = False
    year_month_for_status = "unknown"

    try:
        job_id_from_file, year_month, metric_files, accounting_files, _ = load_job_from_manifest(manifest_path)
        year_month_for_status = year_month

        if job_id != job_id_from_file:
            logger.warning(
                f"Manifest filename job ID '{job_id}' differs from content job ID '{job_id_from_file}'. Using content ID.")
            job_id = job_id_from_file  # Trust the content

        processed_successfully = process_job(job_id, year_month, metric_files, accounting_files)

        if processed_successfully:
            logger.info(f"Manifest {manifest_path.name} processed successfully (status set by process_job).")
        else:
            logger.error(
                f"Failed to process job {job_id} from manifest {manifest_path.name} (status set by process_job).")

    except FileNotFoundError:
        logger.error(f"Manifest file {manifest_path} not found (possibly removed concurrently). Skipping.")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in manifest file {manifest_path}: {e}")
        # Try to save failure status using filename job_id if possible
        save_status(job_id, year_month_for_status, "failed", {"error": f"Invalid JSON in manifest: {e}"})
    except Exception as e:
        logger.error(f"Critical error processing manifest {manifest_path}: {e}", exc_info=True)
        save_status(job_id, year_month_for_status, "failed",
                    {"error": f"Manifest processing error: {e}", "traceback": traceback.format_exc()})
        processed_successfully = False

    finally:
        # --- Remove Manifest File ---
        try:
            if manifest_path.exists():
                manifest_path.unlink()
                logger.info(f"Removed manifest file: {manifest_path.name}")
        except OSError as e:
            logger.error(f"Error removing manifest file {manifest_path}: {e}", exc_info=True)


# --- Main Loop ---
def main() -> None:
    """Main function: continuously finds and processes job manifests."""
    try:
        setup_directories()
    except Exception:
        logger.critical("Directory setup failed. Exiting.")
        return

    logger.info(f"Starting Polars Consumer. Watching: {SERVER_INPUT_DIR}")
    logger.info(f"Intermediate output: {SERVER_OUTPUT_DIR}")
    logger.info(f"Final results/status: {SERVER_COMPLETE_DIR}")
    logger.info(f"Memory threshold for intermediate writes: {MAX_MEMORY_PERCENT}%")
    logger.info(f"Processing chunk duration: {PROCESSING_CHUNK_DURATION}")
    logger.info(f"Polars thread pool size: {pl.thread_pool_size()}")

    cycle_count = 0
    while True:
        cycle_start_time = time.time()
        cycle_count += 1
        logger.info(f"***** Consumer Cycle {cycle_count} Start *****")

        try:
            memory_info = psutil.virtual_memory()
            logger.info(f"Memory Usage: {memory_info.percent:.1f}% ({memory_info.used / 1024 ** 3:.1f} GB used)")
            if memory_info.percent > 95:
                logger.warning("System memory usage is critically high (>95%). Pausing before processing.")
                time.sleep(120)
                continue  # Re-check memory after pause

            manifests = list_manifest_files()

            if not manifests:
                logger.info("No manifests found to process.")
                sleep_time = 30  # Sleep longer if idle
            else:
                logger.info(f"Found {len(manifests)} manifests. Processing sequentially.")
                for manifest_path in manifests:
                    process_manifest(manifest_path)
                    gc.collect()  # Encourage cleanup between jobs
                    time.sleep(0.5)  # Small pause
                sleep_time = 5  # Check more frequently if work was done

        except Exception as e:
            logger.critical(f"Unhandled error in main consumer cycle {cycle_count}: {e}", exc_info=True)
            sleep_time = 60  # Wait longer after a major loop error
        finally:
            # Ensure some sleep happens
            cycle_duration = time.time() - cycle_start_time
            logger.info(f"***** Consumer Cycle {cycle_count} End (Duration: {cycle_duration:.2f}s) *****")
            actual_sleep = max(1, sleep_time - cycle_duration)  # Ensure at least 1s sleep
            logger.debug(f"Sleeping for {actual_sleep:.1f} seconds...")
            time.sleep(actual_sleep)


if __name__ == "__main__":
    main()
