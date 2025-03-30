import os
import json
import shutil
import time
import logging
from pathlib import Path
import polars as pl
from datetime import timedelta, datetime
import psutil
from datetime import timedelta
import gc
import traceback
import multiprocessing

pl.enable_string_cache()


def print_current_time():
    # Get the current date and time
    now = datetime.now()

    # Format the date and time as mm-dd hh:mm:ss
    formatted_time = now.strftime("%m-%d-%H:%M:%S")

    # Print the formatted string
    print(formatted_time)


log_dir = "/home/dynamo/a/jmckerra/projects/stampede-step-3/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"consumer.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Keep console output as well
    ]
)
logger = logging.getLogger('job-metrics-consumer')

SERVER_INPUT_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/input"
SERVER_OUTPUT_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/output"
SERVER_COMPLETE_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/complete"
MAX_MEMORY_PERCENT = 80


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

    # Create a sample for debugging
    sample_values = df.select(pl.col(column)).head(5).to_series()
    logger.debug(f"Sample {column} values: {sample_values}")

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
                # logger.info(
                #     f"Format '{fmt}' successfully parsed {non_null_count}/{total_count} rows ({success_pct:.2f}%) of {column}")

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


def load_job_from_manifest(manifest_path):
    """Load job information from manifest file."""
    with open(manifest_path, 'r') as f:
        manifest_data = json.load(f)

    job_id = manifest_data["job_id"]
    year_month = manifest_data["year_month"]
    metric_files = [os.path.join(SERVER_INPUT_DIR, f) for f in manifest_data["metric_files"]]
    accounting_files = [os.path.join(SERVER_INPUT_DIR, f) for f in manifest_data["accounting_files"]]
    complete_month = manifest_data["complete_month"]

    return job_id, year_month, metric_files, accounting_files, complete_month


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


def estimate_output_rows(job_metrics, start_time, end_time, hosts_count):
    """Estimate the number of output rows that will be generated."""
    duration_seconds = (end_time - start_time).total_seconds()
    chunks = max(1, duration_seconds / 300)  # 300 seconds = 5 minutes
    units = 4  # CPU %, GB, GB/s, MB/s
    return int(chunks * hosts_count * units)


def load_accounting_data(accounting_files):
    """Load accounting data from CSV files using LazyFrames."""
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


def load_metric_data(metric_files, time_filter=None):
    """
    Load metric data from Parquet files using LazyFrames with time filtering.

    Args:
        metric_files: List of metric file paths to load
        time_filter: Optional tuple of (start_time, end_time) to filter data
    """
    if not metric_files:
        logger.warning("No metric files provided")
        return pl.LazyFrame()

    # Only read essential columns that we need
    columns_needed = ["Job Id", "Timestamp", "Host", "Event", "Value"]

    lazy_frames = []
    for file_path in metric_files:
        try:
            # Use scan_parquet to create LazyFrame
            lf = pl.scan_parquet(file_path)

            # Select only the columns we need
            lf = lf.select(columns_needed)

            # Parse Timestamp to datetime with the correct format
            if "Timestamp" in lf.columns:
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
            logger.info(f"Successfully scanned metric file: {file_path}")
        except Exception as e:
            logger.error(f"Error scanning metric file {file_path}: {str(e)}")

    if lazy_frames:
        result = pl.concat(lazy_frames)
        logger.info(f"Created lazy metrics dataframe from {len(lazy_frames)} files")
        return result
    else:
        return pl.LazyFrame()


def join_data(metric_df, accounting_df, output_dir, year_month):
    """
    Join metric data with accounting data using 5-minute time chunks.
    Optimized with batch processing and indexing.
    """
    # Create index on Job Id in metric_df to speed up filtering
    # logger.info("Creating indexes for faster joins")
    # Extract unique job IDs from accounting data
    accounting_job_ids = accounting_df["jobID"].unique().to_list()

    # Create batch processing for accounting data
    batch_size = max(100, len(accounting_df) // NUM_THREADS)
    batches = []

    for i in range(0, len(accounting_df), batch_size):
        batches.append(accounting_df.slice(i, min(batch_size, len(accounting_df) - i)))

    # logger.info(f"Split accounting data into {len(batches)} batches for processing")

    # Define common constants
    chunk_duration = timedelta(minutes=5)
    all_batch_dirs = []
    batch_num = 1

    for batch_idx, batch_df in enumerate(batches):
        joined_results = []
        jobs_with_data_count = 0

        logger.info(f"Processing batch {batch_idx + 1}/{len(batches)}")
        batch_start_time = time.time()

        # Process each job in the batch
        for job_idx, job_row in enumerate(batch_df.iter_rows(named=True)):
            job_id = job_row["jobID"]
            start_time = job_row["start"]
            end_time = job_row["end"]

            # # Log progress for large batches
            # if job_idx % 100 == 0 or job_idx == len(batch_df) - 1:
            #     logger.info(f"Processing job {job_idx + 1}/{len(batch_df)}")

            # Try different job ID formats - polars doesn't support regex so we create variations
            job_id_alt1 = f"JOB{job_id.replace('jobID', '')}" if "jobID" in job_id else f"JOB{job_id}"
            job_id_alt2 = f"JOBID{job_id.replace('jobID', '')}" if "jobID" in job_id else f"JOBID{job_id}"

            # Filter metrics for this job and time window
            job_metrics = metric_df.filter(
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

            # Calculate chunks more efficiently
            chunks = []
            current_time = start_time
            while current_time < end_time:
                next_time = min(current_time + chunk_duration, end_time)
                chunks.append((current_time, next_time))
                current_time = next_time

            # Process each host - this is most efficient outside the chunk loop
            for host in hosts:
                # Filter metrics for this host (once per host, not per chunk)
                host_metrics = job_metrics.filter(pl.col("Host") == host)

                # Now process each time chunk
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

                    # Create rows for each unit type (a constant set of 4)
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

            # Check if memory is getting too high - save batch if needed
            if job_idx % 100 == 0:
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > MAX_MEMORY_PERCENT * 0.8 and len(joined_results) > 10000:
                    logger.warning(f"Memory usage high ({memory_percent}%), writing partial batch")

                    # Write current results to disk
                    interim_df = pl.DataFrame(joined_results)
                    batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
                    os.makedirs(batch_dir, exist_ok=True)
                    interim_file = os.path.join(batch_dir, f"joined_data_{year_month}_part{batch_num}.parquet")
                    interim_df.write_parquet(interim_file)

                    # logger.info(f"Wrote interim {len(interim_df)} rows to {interim_file}")
                    all_batch_dirs.append(batch_dir)
                    batch_num += 1

                    # Clear results and force GC
                    joined_results = []
                    gc.collect()

        # Write the final batch results
        if joined_results:
            batch_df = pl.DataFrame(joined_results)
            batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
            os.makedirs(batch_dir, exist_ok=True)
            batch_file = os.path.join(batch_dir, f"joined_data_{year_month}_batch{batch_num}.parquet")
            batch_df.write_parquet(batch_file)

            # logger.info(f"Wrote final {len(batch_df)} rows to {batch_file}")
            all_batch_dirs.append(batch_dir)
            batch_num += 1

        batch_end_time = time.time()
        logger.info(f"Batch {batch_idx + 1} processing completed in {batch_end_time - batch_start_time:.1f}s")

        # Force garbage collection between batches
        gc.collect()

    # Remove duplicates from batch_dirs list
    unique_batch_dirs = list(set(all_batch_dirs))
    # logger.info(f"Completed join with {len(unique_batch_dirs)} output batch directories")

    return unique_batch_dirs


def vectorized_join(metrics_lazy, accounting_lazy):
    """
    Perform vectorized join between metrics and accounting data using LazyFrames.

    Args:
        metrics_lazy: LazyFrame containing metric data
        accounting_lazy: LazyFrame containing accounting data

    Returns:
        LazyFrame with joined data
    """
    # Create canonical job IDs for joining
    metrics_lazy = metrics_lazy.with_columns([
        pl.col("Job Id").alias("job_id_canonical")
    ])

    # Time bucket for metrics (5-minute intervals)
    metrics_lazy = metrics_lazy.with_columns([
        pl.col("Timestamp").dt.truncate("5m").alias("time_bucket")
    ])

    # Pre-aggregate metrics by job, host, time bucket, and event
    metrics_agg = metrics_lazy.group_by(
        ["job_id_canonical", "Host", "time_bucket", "Event"]
    ).agg([
        pl.col("Value").mean().alias("avg_value")
    ])

    # Create separate aggregations for each event type
    cpuuser_metrics = metrics_agg.filter(pl.col("Event") == "cpuuser").select(
        ["job_id_canonical", "Host", "time_bucket", pl.col("avg_value").alias("value_cpuuser")]
    )

    memused_metrics = metrics_agg.filter(pl.col("Event") == "memused").select(
        ["job_id_canonical", "Host", "time_bucket", pl.col("avg_value").alias("value_memused")]
    )

    memused_minus_diskcache_metrics = metrics_agg.filter(pl.col("Event") == "memused_minus_diskcache").select(
        ["job_id_canonical", "Host", "time_bucket", pl.col("avg_value").alias("value_memused_minus_diskcache")]
    )

    nfs_metrics = metrics_agg.filter(pl.col("Event") == "nfs").select(
        ["job_id_canonical", "Host", "time_bucket", pl.col("avg_value").alias("value_nfs")]
    )

    block_metrics = metrics_agg.filter(pl.col("Event") == "block").select(
        ["job_id_canonical", "Host", "time_bucket", pl.col("avg_value").alias("value_block")]
    )

    # Get distinct combinations of job, host, and time bucket as base
    base_metrics = metrics_agg.select(["job_id_canonical", "Host", "time_bucket"]).unique()

    # Join all event metrics to the base - using left joins
    combined_metrics = base_metrics

    # Use a sequence of left joins to combine all metrics
    if "value_cpuuser" not in combined_metrics.columns:
        combined_metrics = combined_metrics.join(
            cpuuser_metrics,
            on=["job_id_canonical", "Host", "time_bucket"],
            how="left"
        )

    if "value_memused" not in combined_metrics.columns:
        combined_metrics = combined_metrics.join(
            memused_metrics,
            on=["job_id_canonical", "Host", "time_bucket"],
            how="left"
        )

    if "value_memused_minus_diskcache" not in combined_metrics.columns:
        combined_metrics = combined_metrics.join(
            memused_minus_diskcache_metrics,
            on=["job_id_canonical", "Host", "time_bucket"],
            how="left"
        )

    if "value_nfs" not in combined_metrics.columns:
        combined_metrics = combined_metrics.join(
            nfs_metrics,
            on=["job_id_canonical", "Host", "time_bucket"],
            how="left"
        )

    if "value_block" not in combined_metrics.columns:
        combined_metrics = combined_metrics.join(
            block_metrics,
            on=["job_id_canonical", "Host", "time_bucket"],
            how="left"
        )

    # Add GPU column which is always null
    combined_metrics = combined_metrics.with_columns([
        pl.lit(None).cast(pl.Float64).alias("value_gpu")
    ])

    # Prepare accounting data with multiple job ID formats
    accounting_lazy = accounting_lazy.with_columns([
        pl.col("jobID").alias("original_job_id")
    ])

    # Join with accounting data using the job_id_canonical field
    # Try to match it against the jobID field in accounting
    joined_data = combined_metrics.join(
        accounting_lazy,
        left_on="job_id_canonical",
        right_on="jobID",
        how="inner"
    )

    # Filter joined data by time boundaries
    joined_data = joined_data.filter(
        (pl.col("time_bucket") >= pl.col("start")) &
        (pl.col("time_bucket") <= pl.col("end"))
    )

    # Duplicate rows for each unit type - one LazyFrame per unit type
    cpu_pct = joined_data.with_columns([
        pl.lit("CPU %").alias("unit"),
        pl.col("time_bucket").alias("time"),
        pl.col("submit").alias("submit_time"),
        pl.col("start").alias("start_time"),
        pl.col("end").alias("end_time"),
        pl.col("walltime").alias("timelimit"),
        pl.col("nnodes").alias("nhosts"),
        pl.col("ncpus").alias("ncores"),
        pl.col("jobID").alias("jid"),
        pl.col("Host").alias("host")
    ])

    gb = joined_data.with_columns([
        pl.lit("GB").alias("unit"),
        pl.col("time_bucket").alias("time"),
        pl.col("submit").alias("submit_time"),
        pl.col("start").alias("start_time"),
        pl.col("end").alias("end_time"),
        pl.col("walltime").alias("timelimit"),
        pl.col("nnodes").alias("nhosts"),
        pl.col("ncpus").alias("ncores"),
        pl.col("jobID").alias("jid"),
        pl.col("Host").alias("host")
    ])

    gb_s = joined_data.with_columns([
        pl.lit("GB/s").alias("unit"),
        pl.col("time_bucket").alias("time"),
        pl.col("submit").alias("submit_time"),
        pl.col("start").alias("start_time"),
        pl.col("end").alias("end_time"),
        pl.col("walltime").alias("timelimit"),
        pl.col("nnodes").alias("nhosts"),
        pl.col("ncpus").alias("ncores"),
        pl.col("jobID").alias("jid"),
        pl.col("Host").alias("host")
    ])

    mb_s = joined_data.with_columns([
        pl.lit("MB/s").alias("unit"),
        pl.col("time_bucket").alias("time"),
        pl.col("submit").alias("submit_time"),
        pl.col("start").alias("start_time"),
        pl.col("end").alias("end_time"),
        pl.col("walltime").alias("timelimit"),
        pl.col("nnodes").alias("nhosts"),
        pl.col("ncpus").alias("ncores"),
        pl.col("jobID").alias("jid"),
        pl.col("Host").alias("host")
    ])

    # Combine all unit types
    result = pl.concat([cpu_pct, gb, gb_s, mb_s])

    # Add missing columns that the original data had
    result = result.with_columns([
        pl.lit("").alias("host_list"),
        pl.col("user").alias("username"),
        pl.col("jobname").alias("jobname"),
        pl.col("account").alias("account"),
        pl.col("queue").alias("queue"),
        pl.col("exit_status").alias("exitcode")
    ])

    return result


def process_job(job_id, year_month, metric_files, accounting_files):
    """Process a job with LazyFrames for loading but DataFrame for processing."""
    try:
        start_time = time.time()
        logger.info(f"Processing job {job_id} for {year_month}")

        # Load accounting data using LazyFrames
        logger.info(f"Loading accounting data from {len(accounting_files)} files")
        accounting_lazy = load_accounting_data(accounting_files)

        # Check if accounting data is empty
        accounting_df = accounting_lazy.collect()
        if len(accounting_df) == 0:
            logger.error(f"No accounting data found for job {job_id}")
            save_status(job_id, "failed", {"error": "No accounting data found"})
            return False

        logger.info(f"Accounting data loaded with {len(accounting_df)} rows in {time.time() - start_time:.1f}s")

        # Setup output directory
        output_dir = Path(SERVER_OUTPUT_DIR) / job_id
        os.makedirs(output_dir, exist_ok=True)

        # Group accounting jobs by time windows (weeks)
        accounting_df = accounting_df.with_columns([
            pl.col("start").dt.week().alias("week_num")
        ])

        # Get unique weeks
        weeks = accounting_df["week_num"].unique().sort().to_list()
        logger.info(f"Split accounting data into {len(weeks)} weekly time windows")

        all_batch_dirs = []
        batch_num = 1

        # Process each time window separately
        for week_idx, week in enumerate(weeks):
            week_start_time = time.time()
            logger.info(f"Processing time window {week_idx + 1}/{len(weeks)} (week {week})")

            # Filter accounting jobs for this week
            week_accounting = accounting_df.filter(pl.col("week_num") == week)
            total_jobs_in_week = len(week_accounting)

            logger.info(f"Found {total_jobs_in_week} jobs in time window {week_idx + 1}")

            # Get overall time range for this week
            week_start = week_accounting["start"].min()
            week_end = week_accounting["end"].max()

            logger.info(f"Time window {week_idx + 1} spans {week_start} to {week_end} with {total_jobs_in_week} jobs")

            # Load only metric data for this time window using LazyFrames
            metric_load_start = time.time()
            logger.info(f"Loading metrics for time window {week_idx + 1} from {len(metric_files)} files")

            week_metrics_lazy = load_metric_data(metric_files, time_filter=(week_start, week_end))

            # Collect the metrics data - this materializes it
            week_metrics = week_metrics_lazy.collect()

            if len(week_metrics) == 0:
                logger.warning(f"No metrics found for time window {week_idx + 1}")
                continue

            logger.info(
                f"Loaded {len(week_metrics)} metric rows for time window {week_idx + 1} in {time.time() - metric_load_start:.1f}s")

            # Create a batch directory
            batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
            os.makedirs(batch_dir, exist_ok=True)

            # Process the data with vectorized operations
            join_start = time.time()
            logger.info(f"Processing data for time window {week_idx + 1}")

            # Prepare a results container
            results = []

            # Add time buckets (5-minute intervals)
            week_metrics = week_metrics.with_columns([
                pl.col("Timestamp").dt.truncate("5m").alias("time_bucket")
            ])

            # Pre-aggregate metrics by job, host, time bucket, and event
            metrics_agg = week_metrics.group_by(
                ["Job Id", "Host", "time_bucket", "Event"]
            ).agg([
                pl.col("Value").mean().alias("avg_value")
            ])

            # Process each job in this time window
            processed_jobs = 0
            for job_row in week_accounting.iter_rows(named=True):
                job_id_value = job_row["jobID"]
                job_id_alt1 = f"JOB{job_id_value.replace('jobID', '')}" if 'jobID' in job_id_value else f"JOB{job_id_value}"
                job_id_alt2 = f"JOBID{job_id_value.replace('jobID', '')}" if 'jobID' in job_id_value else f"JOBID{job_id_value}"

                # Filter metrics for this job
                job_metrics = metrics_agg.filter(
                    ((pl.col("Job Id") == job_id_value) |
                     (pl.col("Job Id") == job_id_alt1) |
                     (pl.col("Job Id") == job_id_alt2))
                )

                if len(job_metrics) == 0:
                    continue

                # Track progress
                processed_jobs += 1
                if processed_jobs % 100 == 0:
                    logger.info(f"Processed {processed_jobs}/{total_jobs_in_week} jobs for time window {week_idx + 1}")

                # Get hosts for this job
                hosts = job_metrics["Host"].unique().to_list()
                host_list_str = ",".join(hosts)

                # Process each host and event
                for host in hosts:
                    # Filter metrics for this host
                    host_metrics = job_metrics.filter(pl.col("Host") == host)

                    # Get all time buckets for this host within the job's time range
                    time_buckets = host_metrics.filter(
                        (pl.col("time_bucket") >= job_row["start"]) &
                        (pl.col("time_bucket") <= job_row["end"])
                    )["time_bucket"].unique().to_list()

                    # Process each time bucket
                    for time_bucket in time_buckets:
                        # Get metrics for this time bucket
                        bucket_metrics = host_metrics.filter(pl.col("time_bucket") == time_bucket)

                        # Convert to a dict for easier lookup
                        metrics_dict = {}
                        for m_row in bucket_metrics.iter_rows(named=True):
                            metrics_dict[m_row["Event"]] = m_row["avg_value"]

                        # Create rows for each unit type
                        for unit in ["CPU %", "GB", "GB/s", "MB/s"]:
                            row = {
                                "time": time_bucket,
                                "submit_time": job_row["submit"],
                                "start_time": job_row["start"],
                                "end_time": job_row["end"],
                                "timelimit": job_row["walltime"],
                                "nhosts": job_row["nnodes"],
                                "ncores": job_row["ncpus"],
                                "account": job_row["account"],
                                "queue": job_row["queue"],
                                "host": host,
                                "jid": job_id_value,
                                "unit": unit,
                                "jobname": job_row["jobname"],
                                "exitcode": job_row["exit_status"],
                                "host_list": host_list_str,
                                "username": job_row["user"],
                                "value_cpuuser": metrics_dict.get("cpuuser", None),
                                "value_gpu": None,  # Not present in data
                                "value_memused": metrics_dict.get("memused", None),
                                "value_memused_minus_diskcache": metrics_dict.get("memused_minus_diskcache", None),
                                "value_nfs": metrics_dict.get("nfs", None),
                                "value_block": metrics_dict.get("block", None)
                            }

                            results.append(row)

                # Save partial results if memory usage is high or we've processed many jobs
                if processed_jobs % 1000 == 0 or len(results) > 100000:
                    memory_percent = psutil.virtual_memory().percent
                    if memory_percent > MAX_MEMORY_PERCENT * 0.8 or len(results) > 100000:
                        if results:
                            logger.info(
                                f"Saving partial results ({len(results)} rows) after processing {processed_jobs} jobs")
                            partial_df = pl.DataFrame(results)
                            partial_file = os.path.join(batch_dir,
                                                        f"joined_data_{year_month}_week{week}_part{processed_jobs}.parquet")
                            partial_df.write_parquet(partial_file, compression="zstd", compression_level=3)
                            results = []  # Clear results
                            gc.collect()  # Force garbage collection

            # Save any remaining results
            if results:
                logger.info(f"Saving final results ({len(results)} rows) for time window {week_idx + 1}")
                result_df = pl.DataFrame(results)
                result_file = os.path.join(batch_dir, f"joined_data_{year_month}_week{week}_final.parquet")
                result_df.write_parquet(result_file, compression="zstd", compression_level=3)

            logger.info(
                f"Processed {processed_jobs} jobs for time window {week_idx + 1} in {time.time() - join_start:.1f}s")

            # Only add the batch directory if we processed some jobs
            if processed_jobs > 0:
                all_batch_dirs.append(batch_dir)
                batch_num += 1

            # Force garbage collection
            gc.collect()

        # Copy results to complete directory
        if all_batch_dirs:
            copy_start = time.time()
            logger.info(f"Copying results from {len(all_batch_dirs)} batch directories")

            complete_dir = Path(SERVER_COMPLETE_DIR) / job_id
            os.makedirs(complete_dir, exist_ok=True)

            copied_files = []
            total_files = sum(len([f for f in os.listdir(d) if f.endswith(".parquet")]) for d in all_batch_dirs)
            files_copied = 0
            last_copy_progress = 0

            for batch_dir in all_batch_dirs:
                parquet_files = [f for f in os.listdir(batch_dir) if f.endswith(".parquet")]
                for i, file_name in enumerate(parquet_files):
                    source_file = os.path.join(batch_dir, file_name)
                    dest_file = os.path.join(complete_dir, file_name)

                    shutil.copy2(source_file, dest_file)
                    copied_files.append(dest_file)

                    # Update copy progress
                    files_copied += 1
                    copy_progress = int(100 * files_copied / total_files)

                    if copy_progress >= last_copy_progress + 10 or files_copied == total_files:
                        logger.info(f"Copy progress: {copy_progress}% ({files_copied}/{total_files} files)")
                        last_copy_progress = copy_progress

            copy_time = time.time() - copy_start
            logger.info(f"Copied {len(copied_files)} files in {copy_time:.1f}s")

            # Update job status
            total_time = time.time() - start_time
            save_status(job_id, "completed", {
                "year_month": year_month,
                "total_weeks_processed": len(weeks),
                "total_batches": len(all_batch_dirs),
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
                "total_weeks_processed": len(weeks),
                "processing_time_seconds": time.time() - start_time,
            })
            return True

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        save_status(job_id, "failed", {"error": str(e)})
        return False


def process_manifest(manifest_path):
    """Process a manifest file."""
    job_id, year_month, metric_files, accounting_files, complete_month = load_job_from_manifest(manifest_path)

    # Update job status to processing
    save_status(job_id, "processing")

    # Process the job
    success = process_job(job_id, year_month, metric_files, accounting_files)

    if success:
        logger.info(f"Manifest {manifest_path} processed successfully")
    else:
        logger.error(f"Failed to process manifest {manifest_path}")


def main():
    """Main ETL consumer function with optimized processing and monitoring."""
    setup_directories()

    # Configure optimal thread count
    import multiprocessing
    global NUM_THREADS
    NUM_THREADS = max(10, min(16, multiprocessing.cpu_count()))

    # Enable string cache globally to handle categorical data
    pl.enable_string_cache()

    logger.info(f"Starting ETL consumer with {NUM_THREADS} threads and {MAX_MEMORY_PERCENT}% memory limit")

    cycle_count = 0

    while True:
        try:
            cycle_start = time.time()
            cycle_count += 1
            logger.info(f"Starting ETL consumer cycle {cycle_count}")

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
                        import traceback
                        logger.error(traceback.format_exc())
            else:
                logger.info("No manifests found, waiting...")

            # Log cycle duration
            cycle_duration = time.time() - cycle_start
            logger.info(f"ETL consumer cycle {cycle_count} completed in {cycle_duration:.1f}s")

            # Sleep before next cycle
            time.sleep(10)

        except Exception as e:
            logger.error(f"Error in ETL consumer cycle: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(30)  # Longer sleep on main loop error for system recovery


if __name__ == "__main__":
    main()
