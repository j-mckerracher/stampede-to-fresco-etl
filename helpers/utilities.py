import gc
import os
import shutil
import time
from pathlib import Path
from urllib.parse import urljoin
import polars as pl
import psutil
import requests
from bs4 import BeautifulSoup


def check_critical_disk_space(quota_mb=24512, warning_threshold_pct=30, critical_threshold_pct=15):
    """
    Check disk space status based on quota and thresholds
    Returns:
        - (True, True) if space is fine
        - (True, False) if warning level reached
        - (False, False) if critical level reached
    """
    current_dir = os.path.abspath(os.curdir)
    disk_usage = psutil.disk_usage(current_dir)
    available_mb = disk_usage.free / (1024 * 1024)

    # Calculate thresholds based on quota percentage
    warning_mb = quota_mb * (warning_threshold_pct / 100)
    critical_mb = quota_mb * (critical_threshold_pct / 100)

    return (
        available_mb > critical_mb,  # is_safe
        available_mb > warning_mb  # is_abundant
    )


def cleanup_after_upload(base_dir: str, version: str):
    """
    Clean up local files after successful S3 upload
    Returns True if cleanup was successful
    """
    try:
        # Clean up monthly data files
        save_dir = Path(base_dir) / "monthly_data"
        if save_dir.exists():
            pattern = f"FRESCO_Stampede_ts_*_{version}.csv"
            for file in save_dir.glob(pattern):
                file.unlink()

            # Remove directory if empty
            if not any(save_dir.iterdir()):
                save_dir.rmdir()

        return True
    except Exception as e:
        print(f"Error during post-upload cleanup: {e}")
        return False


def get_base_dir():
    """Get the base directory in a platform-agnostic way"""
    return os.path.dirname(os.path.abspath(__file__))


def check_disk_space(required_space_gb=10):
    """
    Check if there's enough disk space available in current directory
    Returns True if enough space, False otherwise
    """
    current_dir = os.path.abspath(os.curdir)
    disk_usage = psutil.disk_usage(current_dir)
    available_gb = disk_usage.free / (1024 ** 3)
    return available_gb > required_space_gb


# Directory and File Management Functions
def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def download_file(url, local_path, max_retries=3, retry_delay=5):
    """Download file with retry mechanism"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            with open(local_path, 'wb') as f:
                f.write(response.content)
            return True

        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff

    return False


def process_node_folder(folder_path, process_pool):
    """Process a single node folder using multiprocessing"""
    try:
        # Prepare arguments for multiprocessing
        file_types = ['block', 'cpu', 'nfs', 'mem']
        args = [(folder_path, file_type) for file_type in file_types]

        # Process files in parallel
        results = []
        for file_results in process_pool.imap_unordered(process_file_multiprocess, args):
            if file_results:
                results.extend(file_results)

            # Clean up as we go
            gc.collect()

        if results:
            # Concatenate results using Polars
            final_result = pl.concat(results)
            return final_result

        return None
    except Exception as e:
        print(f"Error processing folder {folder_path}: {e}")
        return None


def process_file_multiprocess(args):
    """Process a single file type in a multiprocessing worker"""
    folder_path, file_type = args
    file_path = os.path.join(folder_path, f'{file_type}.csv')
    if not os.path.exists(file_path):
        return None

    if file_type == 'mem':
        memused_df, memused_nocache_df = process_memory_metrics(file_path)
        return [memused_df, memused_nocache_df]
    else:
        return [globals()[f'process_{file_type}_file'](file_path)]


def process_memory_metrics(file_path):
    """Process mem.csv file using Polars"""
    df = pl.read_csv(file_path)

    # Convert KB to bytes
    memory_cols = ["MemTotal", "MemFree", "MemUsed", "FilePages"]
    for col in memory_cols:
        if col in df.columns:
            df = df.with_columns([
                (pl.col(col) * 1024).alias(col)
            ])

    # Calculate memory metrics
    df = df.with_columns([
        (pl.col("MemUsed") / (1024 * 1024 * 1024)).alias("memused"),
        ((pl.col("MemUsed") - pl.col("FilePages")) / (1024 * 1024 * 1024))
        .clip(0, None)
        .alias("memused_minus_diskcache")
    ])

    # Format Job Id
    df = df.with_columns([
        pl.col("jobID").str.replace("job", "JOB", literal=True)
        .str.replace("ID", "")
        .alias("Job Id")
    ])

    # Create separate dataframes for each metric
    memused_df = df.select([
        "Job Id",
        "node",
        pl.lit("memused").alias("Event"),
        pl.col("memused").alias("Value"),
        pl.lit("GB").alias("Units"),
        pl.col("timestamp").str.strptime(pl.Datetime).alias("Timestamp")
    ])

    memused_nocache_df = df.select([
        "Job Id",
        "node",
        pl.lit("memused_minus_diskcache").alias("Event"),
        pl.col("memused_minus_diskcache").alias("Value"),
        pl.lit("GB").alias("Units"),
        pl.col("timestamp").str.strptime(pl.Datetime).alias("Timestamp")
    ])

    return memused_df, memused_nocache_df


def cleanup_files(dirs_to_delete):
    """
    Delete specified directories and their contents.
    """
    print("\nCleaning up local files...")
    for dir_path in dirs_to_delete:
        if os.path.exists(dir_path):
            try:
                if os.path.isfile(dir_path):
                    os.remove(dir_path)
                else:
                    shutil.rmtree(dir_path)
                print(f"Deleted: {dir_path}")
            except Exception as e:
                print(f"Error deleting {dir_path}: {str(e)}")


def download_node_folder(node_info, save_dir, tracker):
    """
    Downloads only the required CSV files (block, cpu, nfs, mem) for a single node folder.
    To be run in a separate thread.
    """
    link, node_url = node_info
    node_name = link.text.strip('/')
    required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']

    try:
        if tracker.is_node_processed(node_name):
            print(f"Node {node_name} already processed, skipping...")
            return

        node_dir = os.path.join(save_dir, node_name)
        create_directory(node_dir)

        # Get the CSV files in the NODE directory
        node_response = requests.get(node_url)
        node_soup = BeautifulSoup(node_response.text, 'html.parser')

        download_success = True
        files_found = 0

        for csv_link in node_soup.find_all('a'):
            if csv_link.text in required_files:
                csv_url = urljoin(node_url, csv_link['href'])
                csv_path = os.path.join(node_dir, csv_link.text)
                if not download_file(csv_url, csv_path):
                    download_success = False
                    break
                files_found += 1
                time.sleep(0.5)  # Small delay between files

        if download_success and files_found == len(required_files):
            print(f"Completed downloading required files for {node_name}")
            tracker.mark_node_processed(node_name)
        else:
            print(f"Failed to download all required files for {node_name}")
            tracker.mark_node_failed(node_name)
            # Clean up partial downloads
            if os.path.exists(node_dir):
                shutil.rmtree(node_dir)

    except Exception as e:
        print(f"Error downloading node folder {node_name}: {str(e)}")
        tracker.mark_node_failed(node_name)
        # Clean up in case of error
        if os.path.exists(node_dir):
            shutil.rmtree(node_dir)


def process_block_file(file_path):
    """Process block.csv file using Polars"""
    df = pl.read_csv(file_path)

    # Calculate totals
    df = df.with_columns([
        (pl.col("rd_sectors") + pl.col("wr_sectors")).alias("total_sectors"),
        ((pl.col("rd_ticks") + pl.col("wr_ticks")) / 1000).alias("total_ticks_seconds")
    ])

    # Calculate bytes per second
    df = df.with_columns([
        pl.when(pl.col("total_ticks_seconds") > 0)
        .then((pl.col("total_sectors") * 512) / pl.col("total_ticks_seconds"))
        .otherwise(0)
        .alias("Value")
    ])

    # Convert to GB/s
    df = df.with_columns([
        (pl.col("Value") / (1024 * 1024 * 1024)).alias("Value")
    ])

    # Format Job Id
    df = df.with_columns([
        pl.col("jobID").str.replace("job", "JOB", literal=True)
        .str.replace("ID", "")
        .alias("Job Id")
    ])

    return df.select([
        "Job Id",
        "node",
        pl.lit("block").alias("Event"),
        "Value",
        pl.lit("GB/s").alias("Units"),
        pl.col("timestamp").str.strptime(pl.Datetime).alias("Timestamp")
    ])


def process_cpu_file(file_path):
    """Process cpu.csv file using Polars"""
    df = pl.read_csv(file_path)

    # Calculate total CPU ticks
    df = df.with_columns([
        (pl.col("user") + pl.col("nice") + pl.col("system") +
         pl.col("idle") + pl.col("iowait") + pl.col("irq") +
         pl.col("softirq")).alias("total_ticks")
    ])

    # Calculate CPU percentage
    df = df.with_columns([
        pl.when(pl.col("total_ticks") > 0)
        .then(((pl.col("user") + pl.col("nice")) / pl.col("total_ticks")) * 100)
        .otherwise(0)
        .alias("Value")
    ])

    # Clip values to 0-100 range
    df = df.with_columns([
        pl.col("Value").clip(0, 100)
    ])

    # Format Job Id
    df = df.with_columns([
        pl.col("jobID").str.replace("job", "JOB", literal=True)
        .str.replace("ID", "")
        .alias("Job Id")
    ])

    return df.select([
        "Job Id",
        "node",
        pl.lit("cpuuser").alias("Event"),
        "Value",
        pl.lit("CPU %").alias("Units"),
        pl.col("timestamp").str.strptime(pl.Datetime).alias("Timestamp")
    ])


def process_memory_metrics(file_path):
    """Process mem.csv file using Polars"""
    df = pl.read_csv(file_path)

    # Convert KB to bytes
    memory_cols = ["MemTotal", "MemFree", "MemUsed", "FilePages"]
    for col in memory_cols:
        if col in df.columns:
            df = df.with_columns([
                (pl.col(col) * 1024).alias(col)
            ])

    # Calculate memory metrics
    df = df.with_columns([
        (pl.col("MemUsed") / (1024 * 1024 * 1024)).alias("memused"),
        ((pl.col("MemUsed") - pl.col("FilePages")) / (1024 * 1024 * 1024))
        .clip(0, None)
        .alias("memused_minus_diskcache")
    ])

    # Format Job Id
    df = df.with_columns([
        pl.col("jobID").str.replace("job", "JOB", literal=True)
        .str.replace("ID", "")
        .alias("Job Id")
    ])

    # Create separate dataframes for each metric
    memused_df = df.select([
        "Job Id",
        "node",
        pl.lit("memused").alias("Event"),
        pl.col("memused").alias("Value"),
        pl.lit("GB").alias("Units"),
        pl.col("timestamp").str.strptime(pl.Datetime).alias("Timestamp")
    ])

    memused_nocache_df = df.select([
        "Job Id",
        "node",
        pl.lit("memused_minus_diskcache").alias("Event"),
        pl.col("memused_minus_diskcache").alias("Value"),
        pl.lit("GB").alias("Units"),
        pl.col("timestamp").str.strptime(pl.Datetime).alias("Timestamp")
    ])

    return memused_df, memused_nocache_df


def process_nfs_file(file_path):
    """Process nfs.csv file using Polars"""
    df = pl.read_csv(file_path)

    # Convert to MB
    df = df.with_columns([
        ((pl.col("READ_bytes_recv") + pl.col("WRITE_bytes_sent")) / (1024 * 1024))
        .alias("Value")
    ])

    # Calculate time difference
    df = df.with_columns([
        pl.col("timestamp").str.strptime(pl.Datetime).alias("Timestamp"),
        pl.col("timestamp").str.strptime(pl.Datetime).diff().fill_null(pl.duration(seconds=600))
        .dt.seconds().alias("TimeDiff")
    ])

    # Convert to rate
    df = df.with_columns([
        (pl.col("Value") / pl.col("TimeDiff")).alias("Value")
    ])

    # Format Job Id
    df = df.with_columns([
        pl.col("jobID").str.replace("job", "JOB", literal=True)
        .str.replace("ID", "")
        .alias("Job Id")
    ])

    return df.select([
        "Job Id",
        "node",
        pl.lit("nfs").alias("Event"),
        "Value",
        pl.lit("MB/s").alias("Units"),
        "Timestamp"
    ])