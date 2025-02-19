import gc
import logging
import os
import shutil
import time
import subprocess
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
import polars as pl
import requests
from bs4 import BeautifulSoup


def setup_quota_logging(base_dir: str):
    """Configure logging for disk quota monitoring"""
    log_dir = Path(base_dir) / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"disk_usage_{datetime.now().strftime('%Y%m')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Also print to console
        ]
    )


def get_user_disk_usage():
    """Get current user's disk usage using quota command"""
    try:
        result = subprocess.run(['quota', '-s'], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')

            # Find the line with actual values (it's the line after /dev/mapper)
            for i, line in enumerate(lines):
                if '/dev/mapper/dyndatavg-home_a' in line:
                    # The values are in the next line
                    if i + 1 < len(lines):
                        values_line = lines[i + 1].strip()
                        fields = [f for f in values_line.split() if f]

                        if len(fields) >= 3:
                            # Parse values, removing 'M' suffix
                            used_mb = float(fields[0].rstrip('M'))
                            quota_mb = float(fields[1].rstrip('M'))
                            limit_mb = float(fields[2].rstrip('M'))

                            logging.debug(
                                f"Parsed quota values: used={used_mb}MB, quota={quota_mb}MB, limit={limit_mb}MB")
                            return used_mb, quota_mb, limit_mb

            logging.error(f"Could not find quota values in output:\n{result.stdout}")
            return None, None, None
    except Exception as e:
        logging.error(f"Error getting quota information: {e}")
        return None, None, None


def check_critical_disk_space(warning_threshold_pct=70, critical_threshold_pct=80):
    """
    Check disk space status based on user quota
    Returns:
        - (True, True) if space is fine
        - (True, False) if warning level reached
        - (False, False) if critical level reached
    """
    used_mb, quota_mb, _ = get_user_disk_usage()

    if used_mb is None or quota_mb is None:
        logging.error("Could not determine quota usage - assuming critical")
        return False, False

    percent_used = (used_mb / quota_mb) * 100
    logging.info(f"Quota Usage - Used: {used_mb:.2f}MB ({percent_used:.1f}%) of {quota_mb:.2f}MB quota")

    return (
        percent_used < critical_threshold_pct,  # is_safe
        percent_used < warning_threshold_pct  # is_abundant
    )


def log_disk_usage():
    """Log current disk usage statistics based on quota"""
    used_mb, quota_mb, limit_mb = get_user_disk_usage()

    if used_mb is not None and quota_mb is not None:
        percent_used = (used_mb / quota_mb) * 100
        available_mb = quota_mb - used_mb

        logging.info(
            f"Quota Usage - Used: {used_mb:.2f}MB, "
            f"Available: {available_mb:.2f}MB, "
            f"Quota: {quota_mb:.2f}MB, "
            f"Percent Used: {percent_used:.1f}%"
        )

        if percent_used > 85:
            logging.warning(f"High quota usage: {percent_used:.1f}% of quota used!")

        return percent_used
    else:
        logging.error("Could not determine quota usage")
        return None


def check_disk_space(required_space_mb=1024):
    """Check if there's enough disk space available based on quota"""
    used_mb, quota_mb, _ = get_user_disk_usage()

    if used_mb is None or quota_mb is None:
        logging.error("Could not check available space - assuming insufficient space")
        return False

    available_mb = quota_mb - used_mb
    logging.info(f"Space check - Available: {available_mb:.2f}MB, Required: {required_space_mb}MB")
    return available_mb > required_space_mb


def find_temp_files(base_dir: str, older_than_hours: int = 24) -> list:
    """Find temporary files older than specified hours"""
    temp_patterns = [
        "*.tmp",
        "temp_*",
        "*.temp",
        "~*",
        "*.bak",
        "*.swp"
    ]

    current_time = datetime.now().timestamp()
    old_files = []

    base_path = Path(base_dir)
    for pattern in temp_patterns:
        for file_path in base_path.rglob(pattern):
            if file_path.is_file():
                file_age = current_time - file_path.stat().st_mtime
                if file_age > (older_than_hours * 3600):
                    old_files.append(file_path)

    return old_files


def cleanup_temp_files(base_dir: str) -> int:
    """Clean up temporary files and return space freed in MB"""
    space_freed = 0
    for file_path in find_temp_files(base_dir):
        try:
            size = file_path.stat().st_size
            file_path.unlink()
            space_freed += size
            logging.info(f"Cleaned up temporary file: {file_path}")
        except Exception as e:
            logging.error(f"Failed to remove temporary file {file_path}: {e}")

    return space_freed / (1024 * 1024)  # Convert to MB


def cleanup_files(dirs_to_delete):
    """Delete specified directories and their contents"""
    space_freed = 0
    logging.info("\nCleaning up local files...")

    for dir_path in dirs_to_delete:
        path = Path(dir_path)
        if path.exists():
            try:
                if path.is_file():
                    size = path.stat().st_size
                    path.unlink()
                    space_freed += size
                else:
                    size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                    shutil.rmtree(path)
                    space_freed += size

                logging.info(f"Deleted: {path} (freed {size / (1024 * 1024):.2f}MB)")
            except Exception as e:
                logging.error(f"Error deleting {path}: {e}")

    return space_freed / (1024 * 1024)  # Convert to MB


def cleanup_after_upload(base_dir: str, version: str):
    """Clean up local files after successful S3 upload"""
    try:
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
        logging.error(f"Error during post-upload cleanup: {e}")
        return False


def get_base_dir():
    """Get the base directory in a platform-agnostic way"""
    return os.path.dirname(os.path.abspath(__file__))


def create_directory(path):
    """Create directory if it doesn't exist"""
    Path(path).mkdir(exist_ok=True)


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
            logging.error(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff

    return False


def download_node_folder(node_info, save_dir, tracker):
    """Downloads required CSV files for a single node folder"""
    link, node_url = node_info
    node_name = link.text.strip('/')
    required_files = ['block.csv', 'cpu.csv', 'nfs.csv', 'mem.csv']

    try:
        if tracker.is_node_processed(node_name):
            logging.info(f"Node {node_name} already processed, skipping...")
            return

        node_dir = Path(save_dir) / node_name
        create_directory(node_dir)

        node_response = requests.get(node_url)
        node_soup = BeautifulSoup(node_response.text, 'html.parser')

        download_success = True
        files_found = 0

        for csv_link in node_soup.find_all('a'):
            if csv_link.text in required_files:
                csv_url = urljoin(node_url, csv_link['href'])
                csv_path = node_dir / csv_link.text
                if not download_file(csv_url, str(csv_path)):
                    download_success = False
                    break
                files_found += 1
                time.sleep(0.5)

        if download_success and files_found == len(required_files):
            logging.info(f"Completed downloading required files for {node_name}")
            tracker.mark_node_processed(node_name)
        else:
            logging.error(f"Failed to download all required files for {node_name}")
            tracker.mark_node_failed(node_name)
            if node_dir.exists():
                shutil.rmtree(node_dir)

    except Exception as e:
        logging.error(f"Error downloading node folder {node_name}: {str(e)}")
        tracker.mark_node_failed(node_name)
        if Path(node_dir).exists():
            shutil.rmtree(node_dir)


# File processing functions remain the same as they are different for each file type
def process_node_folder(folder_path, process_pool):
    """Process a single node folder using multiprocessing"""
    try:
        file_types = ['block', 'cpu', 'nfs', 'mem']
        args = [(folder_path, file_type) for file_type in file_types]

        results = []
        for file_results in process_pool.imap_unordered(process_file_multiprocess, args):
            if file_results:
                results.extend(file_results)
            gc.collect()

        if results:
            final_result = pl.concat(results)
            return final_result

        return None
    except Exception as e:
        logging.error(f"Error processing folder {folder_path}: {e}")
        return None


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
