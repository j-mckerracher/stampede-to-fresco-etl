import os
import json
import shutil
import time
import logging
from pathlib import Path
import polars as pl
from datetime import timedelta, datetime
import psutil


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
NUM_THREADS = 10
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
    """Load accounting data from CSV files with optimization."""
    dfs = []

    # Process files in parallel
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(NUM_THREADS, len(accounting_files))) as executor:
        futures = []

        for file_path in accounting_files:
            futures.append(executor.submit(pl.read_csv, file_path))

        for future in futures:
            try:
                df = future.result()
                # Parse date columns with correct format
                df = df.with_columns([
                    pl.col("start").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S"),
                    pl.col("end").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S"),
                    pl.col("submit").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S")
                ])
                dfs.append(df)
                logger.info(f"Successfully loaded accounting file with {len(df)} rows")
            except Exception as e:
                logger.error(f"Error loading accounting file: {str(e)}")

    if dfs:
        result = pl.concat(dfs)
        logger.info(f"Loaded {len(result)} total accounting rows")
        return result
    else:
        return pl.DataFrame()


def load_metric_data(metric_files):
    """Load metric data from Parquet files with batch processing and memory optimization."""
    dfs = []
    total_files = len(metric_files)

    # Process in smaller batches to manage memory
    batch_size = 50  # Process 50 files at a time

    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        current_batch = metric_files[batch_start:batch_end]

        logger.info(f"Loading metric files batch {batch_start // batch_size + 1}/{(total_files - 1) // batch_size + 1}")

        batch_dfs = []
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(NUM_THREADS, len(current_batch))) as executor:
            futures = []

            # Only read essential columns to save memory
            columns = ["Job Id", "Timestamp", "Host", "Event", "Value"]

            for file_path in current_batch:
                futures.append(executor.submit(pl.read_parquet, file_path, columns=columns))

            for future in futures:
                try:
                    df = future.result()
                    # Ensure Timestamp column is datetime using parse_datetime_column
                    if "Timestamp" in df.columns:
                        df = parse_datetime_column(df, "Timestamp")
                    batch_dfs.append(df)
                except Exception as e:
                    logger.error(f"Error loading metric file: {str(e)}")

        if batch_dfs:
            # Concatenate the batch and add to our list
            batch_df = pl.concat(batch_dfs)
            dfs.append(batch_df)
            logger.info(f"Added batch with {len(batch_df)} rows")

            # Check memory usage and wait if needed
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > MAX_MEMORY_PERCENT:
                logger.warning(f"Memory usage high ({memory_percent}%), waiting for GC")
                import gc
                gc.collect()
                time.sleep(5)  # Wait a bit for memory to be freed

    if dfs:
        logger.info(f"Concatenating {len(dfs)} batches")
        result = pl.concat(dfs)
        logger.info(f"Loaded {len(result)} total metric rows")
        return result
    else:
        return pl.DataFrame()


def join_data(metric_df, accounting_df, output_dir, year_month):
    """
    Join metric data with accounting data using 5-minute time chunks.
    Optimized with batch processing and indexing.
    """
    from datetime import timedelta
    import gc

    # Create index on Job Id in metric_df to speed up filtering
    logger.info("Creating indexes for faster joins")
    # Extract unique job IDs from accounting data
    accounting_job_ids = accounting_df["jobID"].unique().to_list()

    # Create batch processing for accounting data
    batch_size = max(100, len(accounting_df) // NUM_THREADS)
    batches = []

    for i in range(0, len(accounting_df), batch_size):
        batches.append(accounting_df.slice(i, min(batch_size, len(accounting_df) - i)))

    logger.info(f"Split accounting data into {len(batches)} batches for processing")

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

            # Log progress for large batches
            if job_idx % 100 == 0 or job_idx == len(batch_df) - 1:
                logger.info(f"Processing job {job_idx + 1}/{len(batch_df)}")
                memory_info = psutil.virtual_memory()
                logger.info(f"Memory: {memory_info.percent}% ({memory_info.used / 1024 ** 3:.1f}GB)")

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

                    logger.info(f"Wrote interim {len(interim_df)} rows to {interim_file}")
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

            logger.info(f"Wrote final {len(batch_df)} rows to {batch_file}")
            all_batch_dirs.append(batch_dir)
            batch_num += 1

        batch_end_time = time.time()
        logger.info(f"Batch {batch_idx + 1} processing completed in {batch_end_time - batch_start_time:.1f}s")

        # Force garbage collection between batches
        gc.collect()

    # Remove duplicates from batch_dirs list
    unique_batch_dirs = list(set(all_batch_dirs))
    logger.info(f"Completed join with {len(unique_batch_dirs)} output batch directories")

    return unique_batch_dirs


def process_job(job_id, year_month, metric_files, accounting_files):
    """Process a job with optimized data loading and joining."""
    try:
        start_time = time.time()
        logger.info(f"Processing job {job_id} for {year_month}")

        # Load accounting data first (usually smaller)
        logger.info(f"Loading accounting data from {len(accounting_files)} files")
        accounting_df = load_accounting_data(accounting_files)

        if len(accounting_df) == 0:
            logger.error(f"No accounting data found for job {job_id}")
            save_status(job_id, "failed", {"error": "No accounting data found"})
            return False

        logger.info(f"Accounting data loaded with {len(accounting_df)} rows in {time.time() - start_time:.1f}s")

        # Load metric data (usually larger)
        metric_load_start = time.time()
        logger.info(f"Loading metric data from {len(metric_files)} files")
        metric_df = load_metric_data(metric_files)

        if len(metric_df) == 0:
            logger.error(f"No metric data found for job {job_id}")
            save_status(job_id, "failed", {"error": "No metric data found"})
            return False

        logger.info(f"Metric data loaded with {len(metric_df)} rows in {time.time() - metric_load_start:.1f}s")

        # Setup output directory
        output_dir = Path(SERVER_OUTPUT_DIR) / job_id
        os.makedirs(output_dir, exist_ok=True)

        # Join the data
        join_start = time.time()
        logger.info("Joining metric and accounting data")
        batch_dirs = join_data(metric_df, accounting_df, str(output_dir), year_month)

        logger.info(f"Data joining completed in {time.time() - join_start:.1f}s")

        # Free memory
        del metric_df
        del accounting_df
        import gc
        gc.collect()

        # Copy results to complete directory
        copy_start = time.time()
        complete_dir = Path(SERVER_COMPLETE_DIR) / job_id
        os.makedirs(complete_dir, exist_ok=True)

        copied_files = []
        for batch_dir in batch_dirs:
            for file_name in os.listdir(batch_dir):
                if file_name.endswith(".parquet"):
                    source_file = os.path.join(batch_dir, file_name)
                    dest_file = os.path.join(complete_dir, file_name)

                    # Copy the file
                    shutil.copy2(source_file, dest_file)
                    copied_files.append(dest_file)

        logger.info(f"Copied {len(copied_files)} files in {time.time() - copy_start:.1f}s")

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

    # Remove manifest even if processing failed
    try:
        os.remove(manifest_path)
        logger.info(f"Removed manifest {manifest_path}")
    except Exception as e:
        logger.error(f"Error removing manifest {manifest_path}: {str(e)}")


def main():
    """Main ETL consumer function with optimized processing and monitoring."""
    setup_directories()

    logger.info(f"Starting ETL consumer with {NUM_THREADS} threads and {MAX_MEMORY_PERCENT}% memory limit")

    while True:
        try:
            logger.info("Starting ETL consumer cycle")

            # Report system status
            memory_info = psutil.virtual_memory()
            logger.info(
                f"Memory: {memory_info.percent}% used ({memory_info.used / 1024 ** 3:.1f}GB / {memory_info.total / 1024 ** 3:.1f}GB)")

            # List manifest files
            manifest_files, _ = list_source_files()

            if manifest_files:
                logger.info(f"Found {len(manifest_files)} manifests to process")

                # Process each manifest sequentially for stability
                # This is safer than parallel processing for large data jobs
                for manifest_path in manifest_files:
                    try:
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

                        # Try to remove the manifest even if processing failed
                        try:
                            os.remove(manifest_path)
                            logger.info(f"Removed problematic manifest {manifest_path}")
                        except:
                            logger.error(f"Could not remove manifest {manifest_path}")
            else:
                logger.info("No manifests found, waiting...")

            # Sleep before next cycle
            time.sleep(10)

        except Exception as e:
            logger.error(f"Error in ETL consumer cycle: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            time.sleep(30)  # Longer sleep on main loop error for system recovery


if __name__ == "__main__":
    main()
