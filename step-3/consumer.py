import os
import json
import shutil
import time
import uuid
import logging
from pathlib import Path
import polars as pl
from datetime import datetime, timedelta
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('job-metrics-consumer')

SERVER_INPUT_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/input"
SERVER_OUTPUT_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/output"
SERVER_COMPLETE_DIR = "/home/dynamo/a/jmckerra/projects/stampede-step-3/complete"
NUM_THREADS = 20
MAX_MEMORY_PERCENT = 80  # Maximum memory usage before throttling


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
    """Load accounting data from CSV files."""
    dfs = []

    for file_path in accounting_files:
        try:
            df = pl.read_csv(file_path)

            # Parse date columns with correct format
            df = df.with_columns([
                pl.col("start").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S"),
                pl.col("end").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S"),
                pl.col("submit").str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M:%S")
            ])

            dfs.append(df)
            logger.info(f"Successfully loaded accounting file {file_path} with {len(df)} rows")
        except Exception as e:
            logger.error(f"Error loading accounting file {file_path}: {str(e)}")

    if dfs:
        result = pl.concat(dfs)
        logger.info(f"Loaded {len(result)} total accounting rows")
        return result
    else:
        return pl.DataFrame()


def load_metric_data(metric_files):
    """Load metric data from Parquet files."""
    dfs = []
    total_files = len(metric_files)

    logger.info(f"Loading {total_files} metric files")

    for i, file_path in enumerate(metric_files):
        try:
            if i % 20 == 0:  # Log progress for large numbers of files
                logger.info(f"Loading metric file {i + 1}/{total_files}")

            df = pl.read_parquet(file_path)

            # Ensure Timestamp column is datetime
            if "Timestamp" in df.columns:
                df = df.with_columns([
                    pl.col("Timestamp").str.to_datetime()
                ])

            dfs.append(df)

            # Check memory usage and wait if needed
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > MAX_MEMORY_PERCENT:
                logger.warning(f"Memory usage high ({memory_percent}%), waiting for GC")
                # Force some garbage collection by clearing already concatenated dataframes
                if len(dfs) > 1:
                    combined = pl.concat(dfs)
                    dfs = [combined]
                time.sleep(5)  # Wait a bit for memory to be freed

        except Exception as e:
            logger.error(f"Error loading metric file {file_path}: {str(e)}")

    if dfs:
        logger.info(f"Concatenating {len(dfs)} dataframes")
        result = pl.concat(dfs)
        logger.info(f"Loaded {len(result)} total metric rows")
        return result
    else:
        return pl.DataFrame()


def join_data(metric_df, accounting_df, output_dir, year_month):
    """
    Join metric data with accounting data using 5-minute time chunks.
    Periodically writes data to disk to manage memory.
    """
    joined_results = []
    jobs_with_data_count = 0
    batch_num = 1

    # Process each job in accounting data
    total_jobs = len(accounting_df)
    logger.info(f"Processing {total_jobs} jobs for {year_month}")

    for job_idx, job_row in enumerate(accounting_df.iter_rows(named=True)):
        job_id = job_row["jobID"]

        # Log progress for large jobs
        if job_idx % 100 == 0 or job_idx == total_jobs - 1:
            logger.info(f"Processing job {job_idx + 1}/{total_jobs}: {job_id}")
            # Also log memory usage
            memory_info = psutil.virtual_memory()
            logger.info(
                f"Memory usage: {memory_info.percent}% ({memory_info.used / 1024 ** 3:.1f}GB / {memory_info.total / 1024 ** 3:.1f}GB)")

        start_time = job_row["start"]
        end_time = job_row["end"]

        # Filter metrics for this job and time window (try different job ID formats)
        job_metrics = metric_df.filter(
            ((pl.col("Job Id") == job_id) |
             (pl.col("Job Id") == f"JOB{job_id.replace('jobID', '')}") |
             (pl.col("Job Id") == f"JOBID{job_id.replace('jobID', '')}")) &
            (pl.col("Timestamp") >= start_time) &
            (pl.col("Timestamp") <= end_time)
        )

        if len(job_metrics) == 0:
            if job_idx % 1000 == 0:  # Reduce log volume by only logging every 1000th empty job
                logger.info(f"No metrics found for job {job_id}")
            continue

        # This job has data - increment our counter
        jobs_with_data_count += 1

        # Get all hosts for this job
        hosts = job_metrics.select(pl.col("Host").unique()).to_series().to_list()
        host_list_str = ",".join(hosts)

        # Log job details
        total_job_minutes = (end_time - start_time).total_seconds() / 60
        logger.info(
            f"Job {job_id} has {len(hosts)} hosts, duration: {total_job_minutes:.1f} minutes, {len(job_metrics)} metrics")

        # Create 5-minute chunks covering the job's duration
        chunk_duration = timedelta(minutes=5)
        current_chunk_start = start_time

        while current_chunk_start < end_time:
            current_chunk_end = current_chunk_start + chunk_duration
            if current_chunk_end > end_time:
                current_chunk_end = end_time

            # Filter metrics for this time chunk
            chunk_metrics = job_metrics.filter(
                (pl.col("Timestamp") >= current_chunk_start) &
                (pl.col("Timestamp") < current_chunk_end)
            )

            # Process each host in this time chunk
            for host in hosts:
                # Initialize metrics dictionary for this host and chunk
                host_metrics = {
                    "value_cpuuser": None,
                    "value_gpu": None,
                    "value_memused": None,
                    "value_memused_minus_diskcache": None,
                    "value_nfs": None,
                    "value_block": None
                }

                # Filter metrics for this host
                host_chunk_data = chunk_metrics.filter(pl.col("Host") == host)

                # Process each metric type
                for event in ["cpuuser", "memused", "memused_minus_diskcache", "nfs", "block"]:
                    event_data = host_chunk_data.filter(pl.col("Event") == event)

                    if len(event_data) > 0:
                        # If multiple values exist in this 5-min chunk, average them
                        avg_value = event_data["Value"].mean()
                        host_metrics[f"value_{event}"] = avg_value

                # Create rows for each unit type
                units_list = ["CPU %", "GB", "GB/s", "MB/s"]
                for unit in units_list:
                    row = {
                        "time": current_chunk_start,
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
                    for metric_key, value in host_metrics.items():
                        row[metric_key] = value

                    joined_results.append(row)

            # Move to next chunk
            current_chunk_start = current_chunk_end

        # Check if we've processed 10,000 jobs with data - if so, write to disk
        if jobs_with_data_count % 10000 == 0:
            if len(joined_results) > 0:
                logger.info(f"Reached {jobs_with_data_count} jobs with data. Writing batch {batch_num} to disk...")

                # Convert to DataFrame
                batch_df = pl.DataFrame(joined_results)

                # Create batch directory
                batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
                os.makedirs(batch_dir, exist_ok=True)

                # Write to parquet
                batch_file = os.path.join(batch_dir, f"joined_data_{year_month}_batch{batch_num}.parquet")
                batch_df.write_parquet(batch_file)
                logger.info(f"Wrote {len(batch_df)} rows to {batch_file}")

                # Clear memory
                joined_results = []

                # Force garbage collection to reclaim memory
                import gc
                gc.collect()

                # Log memory after clearing
                memory_info = psutil.virtual_memory()
                logger.info(f"Memory after clearing: {memory_info.percent}% ({memory_info.used / 1024 ** 3:.1f}GB)")

                batch_num += 1

    # Write any remaining results
    if len(joined_results) > 0:
        logger.info(f"Writing final batch {batch_num} with {len(joined_results)} rows")
        batch_df = pl.DataFrame(joined_results)

        # Create batch directory
        batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
        os.makedirs(batch_dir, exist_ok=True)

        # Write to parquet
        batch_file = os.path.join(batch_dir, f"joined_data_{year_month}_batch{batch_num}.parquet")
        batch_df.write_parquet(batch_file)
        logger.info(f"Wrote {len(batch_df)} rows to {batch_file}")

    logger.info(f"Processed {jobs_with_data_count} jobs with data across {batch_num} batches")

    # Return list of batch directories
    return [os.path.join(output_dir, f"batch_{i}") for i in range(1, batch_num + 1)]


def process_job(job_id, year_month, metric_files, accounting_files):
    """Process a job to join metric and accounting data."""
    try:
        logger.info(f"Processing job {job_id} for {year_month}")

        # Load accounting data
        logger.info(f"Loading accounting data from {len(accounting_files)} files")
        accounting_df = load_accounting_data(accounting_files)

        if len(accounting_df) == 0:
            logger.error(f"No accounting data found for job {job_id}")
            save_status(job_id, "failed", {"error": "No accounting data found"})
            return False

        # Load metric data
        logger.info(f"Loading metric data from {len(metric_files)} files")
        metric_df = load_metric_data(metric_files)

        if len(metric_df) == 0:
            logger.error(f"No metric data found for job {job_id}")
            save_status(job_id, "failed", {"error": "No metric data found"})
            return False

        # Setup output directory
        output_dir = Path(SERVER_OUTPUT_DIR) / job_id
        os.makedirs(output_dir, exist_ok=True)

        # Join the data with periodic writing to manage memory
        logger.info("Joining metric and accounting data with 5-minute chunking")
        batch_dirs = join_data(metric_df, accounting_df, str(output_dir), year_month)

        # Copy results to complete directory
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
                    logger.info(f"Copied {source_file} to {dest_file}")

        # Update job status
        save_status(job_id, "completed", {
            "year_month": year_month,
            "total_batches": len(batch_dirs),
            "files_copied": len(copied_files),
            "ready_for_final_destination": True
        })

        logger.info(f"Job {job_id} completed successfully with {len(batch_dirs)} batches")
        return True

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
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
    """Main consumer function."""
    setup_directories()

    while True:
        try:
            logger.info("Starting ETL consumer cycle")

            # List manifest files
            manifest_files, _ = list_source_files()

            if manifest_files:
                logger.info(f"Found {len(manifest_files)} manifests to process")

                for manifest_path in manifest_files:
                    # Process this manifest
                    process_manifest(manifest_path)

                    # Check memory after each job
                    memory_percent = psutil.virtual_memory().percent
                    if memory_percent > 90:
                        logger.warning(f"High memory usage ({memory_percent}%), taking a break")
                        time.sleep(60)  # Give system time to recover
            else:
                logger.info("No manifests found, waiting...")

            # Sleep before next cycle
            time.sleep(10)

        except Exception as e:
            logger.error(f"Error in ETL consumer cycle: {str(e)}")
            time.sleep(10)


if __name__ == "__main__":
    main()