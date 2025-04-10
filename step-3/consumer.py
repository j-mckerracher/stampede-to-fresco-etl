#!/usr/bin/env python3
import os
import json
import shutil
import time
import logging
from pathlib import Path
import polars as pl
from datetime import timedelta, datetime
import psutil
import gc
import traceback
import multiprocessing
from tqdm import tqdm

# Enable string cache for better performance with categorical data
pl.enable_string_cache()

# Directories configuration
SERVER_INPUT_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/input"
SERVER_OUTPUT_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/output"
SERVER_COMPLETE_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/complete"
MAX_MEMORY_PERCENT = 80

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

    # Check if sorted metric files exist, looking in both base and year_month directories
    sorted_metric_files = manifest_data.get("sorted_metric_files", [])
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
            # Use scan_parquet to create LazyFrame
            lf = pl.scan_parquet(file_path)

            # Log the schema and columns
            # logger.info(f"Scanned parquet file: {file_path}")
            schema = lf.schema
            # logger.info(f"Schema: {schema}")
            # logger.info(f"Columns: {lf.columns}")

            # Check if all required columns exist
            missing_columns = [col for col in columns_needed if col not in lf.columns]
            if missing_columns:
                logger.error(f"Missing required columns in {file_path}: {missing_columns}")
                # Try to adapt to potential column naming differences
                column_mapping = {}
                # Check for case differences
                for needed_col in missing_columns:
                    for actual_col in lf.columns:
                        if needed_col.lower() == actual_col.lower():
                            column_mapping[actual_col] = needed_col
                            logger.info(f"Found column with different case: {actual_col} -> {needed_col}")

                # Rename columns if mapping was found
                if column_mapping:
                    lf = lf.rename(column_mapping)
                    logger.info(f"Renamed columns: {column_mapping}")

            # Select only the columns we need
            lf = lf.select(columns_needed)

            # Parse Timestamp to datetime with the correct format (if needed)
            if "Timestamp" in lf.columns:
                # Check if timestamp is already parsed
                schema = lf.schema
                if schema["Timestamp"] != pl.Datetime:
                    lf = lf.with_columns([
                        pl.col("Timestamp").str.strptime(
                            pl.Datetime,
                            format="%Y-%m-%d %H:%M:%S",
                            strict=False
                        )
                    ])

            # Apply time filter if provided
            if time_filter and time_filter[0] and time_filter[1]:
                start_time, end_time = time_filter
                lf = lf.filter(
                    (pl.col("Timestamp") >= start_time) &
                    (pl.col("Timestamp") <= end_time)
                )

            # Filter to only keep relevant events
            lf = lf.filter(
                pl.col("Event").is_in(["cpuuser", "memused", "memused_minus_diskcache", "nfs", "block"])
            )

            lazy_frames.append(lf)
            logger.info(f"Successfully scanned sorted metric file: {file_path}")
        except Exception as e:
            logger.error(f"Error scanning sorted metric file {file_path}: {str(e)}")
            logger.error(traceback.format_exc())

    if lazy_frames:
        result = pl.concat(lazy_frames)
        # Log information about the concatenated frame
        logger.info(f"Created lazy metrics dataframe from {len(lazy_frames)} files")
        logger.info(f"Columns after concat: {result.columns}")
        return result
    else:
        logger.warning("No valid lazy frames created, returning empty LazyFrame")
        return pl.LazyFrame()


def efficient_job_processing(sorted_metrics_df, accounting_df, output_dir, year_month):
    """
    Process job metrics and accounting data efficiently using the pre-sorted metrics data.
    """
    # Validate inputs
    logger.info(f"Starting efficient_job_processing with metrics shape: {sorted_metrics_df.shape}")
    logger.info(f"Accounting shape: {accounting_df.shape}")

    # Check if metrics dataframe has expected columns
    if "Job Id" not in sorted_metrics_df.columns:
        logger.error(f"Metrics dataframe missing 'Job Id' column. Available columns: {sorted_metrics_df.columns}")
        return []

    # Create batch processing for accounting data
    batch_size = max(100, len(accounting_df) // multiprocessing.cpu_count())
    batches = []

    for i in range(0, len(accounting_df), batch_size):
        batches.append(accounting_df.slice(i, min(batch_size, len(accounting_df) - i)))

    # Define common constants
    chunk_duration = timedelta(minutes=5)
    all_batch_dirs = []
    batch_num = 1

    logger.info(f"Processing accounting data in {len(batches)} batches")

    # Process each batch of accounting data
    for batch_idx, batch_df in enumerate(batches):
        joined_results = []
        jobs_with_data_count = 0

        logger.info(f"Processing batch {batch_idx + 1}/{len(batches)}")
        batch_start_time = time.time()

        # Extract unique job IDs in this batch for faster lookup
        batch_job_ids = set()
        job_id_variations = set()

        for job_id in batch_df["jobID"].unique().to_list():
            batch_job_ids.add(job_id)
            # Add variations for matching
            job_id_alt1 = f"JOB{job_id.replace('jobID', '')}" if "jobID" in job_id else f"JOB{job_id}"
            job_id_alt2 = f"JOBID{job_id.replace('jobID', '')}" if "jobID" in job_id else f"JOBID{job_id}"
            job_id_variations.add(job_id_alt1)
            job_id_variations.add(job_id_alt2)

        # Combine all job ID variations
        all_job_ids = batch_job_ids.union(job_id_variations)

        # Pre-filter the sorted metrics for this batch's job IDs
        # Since the data is sorted by Job Id, this operation is very efficient
        batch_metrics = sorted_metrics_df.filter(pl.col("Job Id").is_in(list(all_job_ids)))

        # Process each job in the batch 
        for job_idx, job_row in enumerate(batch_df.iter_rows(named=True)):
            job_id = job_row["jobID"]
            start_time = job_row["start"]
            end_time = job_row["end"]

            # Log progress periodically
            if job_idx % 200 == 0 and job_idx > 0:
                elapsed = time.time() - batch_start_time
                # logger.info(f"Processed {job_idx}/{len(batch_df)} jobs in {elapsed:.1f}s")

                # Check memory and write interim results if needed
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > MAX_MEMORY_PERCENT * 0.8 and len(joined_results) > 10000:
                    logger.warning(f"Memory usage high ({memory_percent}%), writing partial batch")

                    # Write current results to disk
                    interim_df = pl.DataFrame(joined_results)
                    batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
                    os.makedirs(batch_dir, exist_ok=True)
                    interim_file = os.path.join(batch_dir, f"joined_data_{year_month}_part{batch_num}.parquet")
                    interim_df.write_parquet(interim_file, compression="zstd", compression_level=3)

                    all_batch_dirs.append(batch_dir)
                    batch_num += 1

                    # Clear results and force GC
                    joined_results = []
                    gc.collect()

            # Get job ID variations
            job_id_alt1 = f"JOB{job_id.replace('jobID', '')}" if "jobID" in job_id else f"JOB{job_id}"
            job_id_alt2 = f"JOBID{job_id.replace('jobID', '')}" if "jobID" in job_id else f"JOBID{job_id}"

            # Filter metrics for this job - efficient since we're starting from pre-filtered batch_metrics
            # and the data is already sorted by Job Id and Timestamp
            job_metrics = batch_metrics.filter(
                ((pl.col("Job Id") == job_id) |
                 (pl.col("Job Id") == job_id_alt1) |
                 (pl.col("Job Id") == job_id_alt2)) &
                (pl.col("Timestamp") >= start_time) &
                (pl.col("Timestamp") <= end_time)
            )

            if len(job_metrics) == 0:
                continue

            # Increment our counter for jobs with data
            jobs_with_data_count += 1

            # Get all hosts for this job
            hosts = job_metrics.select(pl.col("Host").unique()).to_series().to_list()
            host_list_str = ",".join(hosts)

            # Calculate time chunks
            chunks = []
            current_time = start_time
            while current_time < end_time:
                next_time = min(current_time + chunk_duration, end_time)
                chunks.append((current_time, next_time))
                current_time = next_time

            # Process each host
            for host in hosts:
                # Filter metrics for this host 
                host_metrics = job_metrics.filter(pl.col("Host") == host)

                # Process each time chunk - this is efficient since the data is sorted by Timestamp
                for chunk_start, chunk_end in chunks:
                    # Extract metrics for this chunk
                    chunk_metrics = host_metrics.filter(
                        (pl.col("Timestamp") >= chunk_start) &
                        (pl.col("Timestamp") < chunk_end)
                    )

                    if len(chunk_metrics) == 0:
                        continue

                    # Use efficient groupby for metric calculation
                    metrics_by_event = chunk_metrics.group_by("Event").agg(
                        pl.col("Value").mean().alias("avg_value")
                    )

                    # Convert to dictionary for easy lookup
                    metric_values = {
                        "value_cpuuser": None,
                        "value_gpu": None,
                        "value_memused": None,
                        "value_memused_minus_diskcache": None,
                        "value_nfs": None,
                        "value_block": None
                    }

                    # Fill in values we found
                    for row in metrics_by_event.iter_rows(named=True):
                        metric_values[f"value_{row['Event']}"] = row["avg_value"]

                    # Create rows for each unit type
                    for unit in ["CPU %", "GB", "GB/s", "MB/s"]:
                        row = {
                            "time": chunk_start,
                            "submit_time": job_row["submit"],
                            "start_time": start_time,
                            "end_time": end_time,
                            "timelimit": job_row["walltime"],
                            "nhosts": job_row["nnodes"],
                            "ncores": job_row["ncpus"],
                            "account": job_row["account"],
                            "queue": job_row["queue"],
                            "host": host,
                            "jid": job_id,
                            "unit": unit,
                            "jobname": job_row["jobname"],
                            "exitcode": job_row["exit_status"],
                            "host_list": host_list_str,
                            "username": job_row["user"]
                        }

                        # Add all metric values
                        row.update(metric_values)
                        joined_results.append(row)

        # Write the final batch results
        if joined_results:
            batch_df = pl.DataFrame(joined_results)
            batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
            os.makedirs(batch_dir, exist_ok=True)
            batch_file = os.path.join(batch_dir, f"joined_data_{year_month}_batch{batch_num}.parquet")
            batch_df.write_parquet(batch_file, compression="zstd", compression_level=3)

            logger.info(f"Wrote batch with {len(batch_df)} rows to {batch_file}")
            all_batch_dirs.append(batch_dir)
            batch_num += 1

        batch_end_time = time.time()
        logger.info(
            f"Batch {batch_idx + 1} processing completed in {batch_end_time - batch_start_time:.1f}s with {jobs_with_data_count} jobs with data")

        # Force garbage collection between batches
        gc.collect()

    # Remove duplicates from batch_dirs list
    unique_batch_dirs = list(set(all_batch_dirs))
    logger.info(f"Completed join with {len(unique_batch_dirs)} output batch directories")

    return unique_batch_dirs


def process_job(job_id, year_month, metric_files, sorted_metric_files, accounting_files):
    """
    Process a job with optimized data loading and processing using pre-sorted metrics.

    Args:
        job_id: Unique identifier for the job
        year_month: Year and month for the job data
        metric_files: List of unsorted metric files
        sorted_metric_files: List of pre-sorted metric files
        accounting_files: List of accounting files

    Returns:
        Boolean indicating success/failure
    """
    try:
        start_time = time.time()
        logger.info(f"Processing job {job_id} for {year_month}")

        # Load accounting data first
        logger.info(f"Loading accounting data from {len(accounting_files)} files")
        accounting_lazy = load_accounting_data(accounting_files)

        # Collect the accounting data
        accounting_df = accounting_lazy.collect()
        if len(accounting_df) == 0:
            logger.error(f"No accounting data found for job {job_id}")
            save_status(job_id, "failed", {"error": "No accounting data found"})
            return False

        logger.info(f"Accounting data loaded with {len(accounting_df)} rows in {time.time() - start_time:.1f}s")

        # Setup output directory
        output_dir = Path(SERVER_OUTPUT_DIR) / job_id
        os.makedirs(output_dir, exist_ok=True)

        # Decide whether to use sorted or unsorted metric files
        if sorted_metric_files:
            logger.info(f"Using {len(sorted_metric_files)} pre-sorted metric files")

            # Load metric data using LazyFrames with sorted files
            metrics_lazy = load_sorted_metric_data(sorted_metric_files)
            metrics_df = metrics_lazy.collect()

            # Validate metrics dataframe
            logger.info(f"Collected metrics dataframe with {len(metrics_df)} rows")
            logger.info(f"Metrics columns: {metrics_df.columns}")

            if len(metrics_df) == 0:
                logger.error(f"No metric data found for job {job_id}")
                save_status(job_id, "failed", {"error": "No metric data found"})
                return False

            # Check if required columns exist
            required_columns = ["Job Id", "Timestamp", "Host", "Event", "Value"]
            missing_columns = [col for col in required_columns if col not in metrics_df.columns]
            if missing_columns:
                logger.error(f"Missing required columns in metrics data: {missing_columns}")
                save_status(job_id, "failed", {"error": f"Missing required columns: {missing_columns}"})
                return False

            # Process job data efficiently with pre-sorted metrics
            batch_dirs = efficient_job_processing(metrics_df, accounting_df, output_dir, year_month)
        else:
            logger.warning(f"No pre-sorted metric files found. Using {len(metric_files)} unsorted files")

            # Fall back to loading unsorted metrics by weekly time windows if no sorted files available
            # Group accounting jobs by time windows (weeks)
            accounting_df = accounting_df.with_columns([
                pl.col("start").dt.week().alias("week_num")
            ])

            # Get unique weeks
            weeks = accounting_df["week_num"].unique().sort().to_list()
            logger.info(f"Split accounting data into {len(weeks)} weekly time windows")

            # Process each time window separately
            all_batch_dirs = []

            for week_idx, week in enumerate(weeks):
                week_start_time = time.time()
                logger.info(f"Processing time window {week_idx + 1}/{len(weeks)} (week {week})")

                # Filter accounting jobs for this week
                week_accounting = accounting_df.filter(pl.col("week_num") == week)

                # Get overall time range for this week
                week_start = week_accounting["start"].min()
                week_end = week_accounting["end"].max()

                logger.info(f"Time window {week_idx + 1} spans {week_start} to {week_end}")

                # Load metric data for this time window
                from concurrent.futures import ThreadPoolExecutor

                # Function to load a single parquet file
                def load_parquet_file(file_path):
                    try:
                        # Read parquet file
                        df = pl.read_parquet(file_path, columns=["Job Id", "Timestamp", "Host", "Event", "Value"])

                        # Ensure Timestamp is datetime
                        if df.schema["Timestamp"] != pl.Datetime:
                            df = parse_datetime_column(df, "Timestamp")

                        # Apply time filter and sort
                        df = df.filter(
                            (pl.col("Timestamp") >= week_start) &
                            (pl.col("Timestamp") <= week_end) &
                            pl.col("Event").is_in(["cpuuser", "memused", "memused_minus_diskcache", "nfs", "block"])
                        )

                        # Sort by Job Id and Timestamp
                        df = df.sort(["Job Id", "Timestamp"])

                        return df
                    except Exception as e:
                        logger.error(f"Error loading file {file_path}: {str(e)}")
                        return None

                # Load files in parallel
                dfs = []
                with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                    futures = [executor.submit(load_parquet_file, file) for file in metric_files]

                    for future in futures:
                        df = future.result()
                        if df is not None and len(df) > 0:
                            dfs.append(df)

                if not dfs:
                    logger.warning(f"No metric data found for week {week}")
                    continue

                # Combine all dataframes for this week
                week_metrics = pl.concat(dfs)

                # Create a batch directory 
                batch_dir = os.path.join(output_dir, f"batch_{len(all_batch_dirs) + 1}")
                os.makedirs(batch_dir, exist_ok=True)

                # Process this week's data
                batch_dirs = efficient_job_processing(week_metrics, week_accounting, batch_dir, year_month)
                all_batch_dirs.extend(batch_dirs)

                # Log completion for this week
                week_time = time.time() - week_start_time
                logger.info(f"Completed time window {week_idx + 1}/{len(weeks)} in {week_time:.1f}s")

                # Force garbage collection
                gc.collect()

            # Use the collected batch directories
            batch_dirs = list(set(all_batch_dirs))

        # Copy results to complete directory
        if batch_dirs:
            copy_start = time.time()
            logger.info(f"Copying results from {len(batch_dirs)} batch directories")

            complete_dir = Path(SERVER_COMPLETE_DIR) / job_id
            os.makedirs(complete_dir, exist_ok=True)

            copied_files = []
            total_files = sum(len([f for f in os.listdir(d) if f.endswith(".parquet")]) for d in batch_dirs)

            # Copy files with progress tracking
            with tqdm(total=total_files, desc="Copying files") as pbar:
                for batch_dir in batch_dirs:
                    parquet_files = [f for f in os.listdir(batch_dir) if f.endswith(".parquet")]
                    for file_name in parquet_files:
                        source_file = os.path.join(batch_dir, file_name)
                        dest_file = os.path.join(complete_dir, file_name)

                        shutil.copy2(source_file, dest_file)
                        copied_files.append(dest_file)
                        pbar.update(1)

            copy_time = time.time() - copy_start
            logger.info(f"Copied {len(copied_files)} files in {copy_time:.1f}s")

            # Update job status
            total_time = time.time() - start_time
            save_status(job_id, "completed", {
                "year_month": year_month,
                "total_batches": len(batch_dirs),
                "files_copied": len(copied_files),
                "processing_time_seconds": total_time,
                "ready_for_final_destination": True
            })

            logger.info(f"Job {job_id} completed successfully in {total_time:.1f}s")
            return True
        else:
            logger.warning(f"No data processed for job {job_id}")
            save_status(job_id, "completed_no_data", {
                "year_month": year_month,
                "processing_time_seconds": time.time() - start_time,
            })
            return True

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
        logger.error(traceback.format_exc())
        save_status(job_id, "failed", {"error": str(e)})
        return False


def process_manifest(manifest_path):
    """Process a manifest file."""
    try:
        # Load job info from manifest
        job_id, year_month, metric_files, sorted_metric_files, accounting_files, complete_month = load_job_from_manifest(
            manifest_path)

        # Update job status to processing
        save_status(job_id, "processing")

        # Process the job
        success = process_job(job_id, year_month, metric_files, sorted_metric_files, accounting_files)

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