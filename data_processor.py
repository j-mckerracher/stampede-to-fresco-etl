import os
import logging
import multiprocessing
from typing import List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import polars as pl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('node_etl.log')
    ]
)
logger = logging.getLogger(__name__)


class NodeDataProcessor:
    """Process node data files using Polars to transform Stampede data into Anvil format

    The Anvil format standardizes resource usage metrics across multiple HPC systems
    with a common schema: Job Id, Host, Timestamp, Event, Value, Units.
    This processor handles the transformation of block I/O, CPU usage, NFS traffic,
    and memory metrics into this unified format.
    """

    def __init__(self):
        self.cache_dir = Path('cache')
        self.cache_dir.mkdir(exist_ok=True, parents=True)

    def process_block_file(self, file_path: Path) -> pl.DataFrame:
        """Transform block device metrics into Anvil format

        Calculates block device throughput (GB/s) by:
        1. Summing read and write sectors (512 bytes each)
        2. Dividing by the time spent on I/O operations (in milliseconds)
        3. Converting to GB/s for Anvil's standardized reporting
        """
        logger.info(f"Processing block file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Calculate throughput
            # Convert sector and tick columns to numeric for calculations
            df = df.with_columns([
                pl.col('rd_sectors').cast(pl.Float64).alias('rd_sectors_num'),
                pl.col('wr_sectors').cast(pl.Float64).alias('wr_sectors_num'),
                pl.col('rd_ticks').cast(pl.Float64).alias('rd_ticks_num'),
                pl.col('wr_ticks').cast(pl.Float64).alias('wr_ticks_num')
            ])

            # Total sectors represent data volume (each sector = 512 bytes)
            # Total ticks represent time spent on I/O (in milliseconds, converted to seconds)
            df = df.with_columns([
                (pl.col('rd_sectors_num') + pl.col('wr_sectors_num')).alias('total_sectors'),
                ((pl.col('rd_ticks_num') + pl.col('wr_ticks_num')) / 1000).alias('total_ticks')
            ])

            # Convert to GB/s: (sectors * 512 bytes) / (time in seconds) / (bytes in GB)
            # Handle division by zero with conditional logic to prevent errors
            df = df.with_columns([
                pl.when(pl.col('total_ticks') > 0)
                .then((pl.col('total_sectors') * 512) / pl.col('total_ticks') / (1024 ** 3))
                .otherwise(0)
                .alias('Value')
            ])

            # Transform to Anvil schema with standardized column names and formats
            # Event type 'block' represents block device I/O throughput
            return df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('block').alias('Event'),
                pl.col('Value'),
                pl.lit('GB/s').alias('Units')
            ])
        except Exception as e:
            logger.error(f"Error processing block file {file_path}: {e}")
            return None

    def process_cpu_file(self, file_path: Path) -> pl.DataFrame:
        """Transform CPU usage metrics into Anvil format

        Calculates CPU user mode percentage by:
        1. Computing total CPU ticks across all states
        2. Determining the percentage of user+nice ticks relative to total
        3. Ensuring values are in valid percentage range (0-100%)

        This matches Anvil's 'cpuuser' metric which represents user-space CPU utilization
        """
        logger.info(f"Processing CPU file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Calculate total CPU ticks across all CPU states
            # This forms the denominator for percentage calculation
            df = df.with_columns([
                (pl.col('user') + pl.col('nice') + pl.col('system') +
                 pl.col('idle') + pl.col('iowait') + pl.col('irq') +
                 pl.col('softirq')).alias('total_ticks')
            ])

            # Calculate percentage of CPU time spent in user mode (user + nice)
            # Clip values to 0-100% range to handle any potential data anomalies
            df = df.with_columns([
                pl.when(pl.col('total_ticks') > 0)
                .then(((pl.col('user') + pl.col('nice')) / pl.col('total_ticks')) * 100)
                .otherwise(0)
                .clip(0, 100)
                .alias('Value')
            ])

            # Transform to Anvil schema with standardized column names
            # Event type 'cpuuser' represents CPU utilization in user mode
            return df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('cpuuser').alias('Event'),
                pl.col('Value'),
                pl.lit('CPU %').alias('Units')
            ])
        except Exception as e:
            logger.error(f"Error processing CPU file {file_path}: {e}")
            return None

    def process_nfs_file(self, file_path: Path) -> pl.DataFrame:
        """Transform NFS traffic metrics into Anvil format

        Calculates NFS throughput (MB/s) by:
        1. Summing bytes received during READ operations and bytes sent during WRITE operations
        2. Converting to MB by dividing by (1024*1024)
        3. Computing the rate by dividing cumulative bytes by time elapsed between measurements

        This provides a measure of network file system activity matching Anvil's 'nfs' metric
        """
        logger.info(f"Processing NFS file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Calculate total NFS traffic by combining READ bytes received and WRITE bytes sent
            # Convert to MB for consistent reporting in Anvil
            df = df.with_columns([
                ((pl.col('READ_bytes_recv') + pl.col('WRITE_bytes_sent')) / (1024 * 1024)).alias('Value'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp')
            ])

            # Calculate time differences between consecutive rows to convert cumulative counters to rates
            # Default to 600 seconds (10 minutes) for first measurement or missing intervals
            df = df.with_columns([
                pl.col('Timestamp').diff().cast(pl.Duration).dt.total_seconds().fill_null(600).alias('TimeDiff')
            ])

            # Calculate rate by dividing accumulated data by time interval
            # This converts cumulative byte counters to throughput in MB/s
            df = df.with_columns([
                (pl.col('Value') / pl.col('TimeDiff')).alias('Value')
            ])

            # Transform to Anvil schema with standardized column names
            # Event type 'nfs' represents network file system throughput
            return df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('nfs').alias('Event'),
                pl.col('Value'),
                pl.lit('MB/s').alias('Units')
            ])
        except Exception as e:
            logger.error(f"Error processing NFS file {file_path}: {e}")
            return None

    def process_memory_metrics(self, file_path: Path) -> List[pl.DataFrame]:
        """Transform memory usage metrics into Anvil format

        Calculates two memory metrics:
        1. Total memory usage (memused) - All physical memory used by the OS
        2. Memory usage excluding disk cache (memused_minus_diskcache) - Memory used by applications

        Both metrics are converted to GB to align with Anvil's standardized reporting
        """
        logger.info(f"Processing memory file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Memory values in source data are in KB, convert to bytes for precision
            # during calculations by multiplying by 1024
            memory_cols = ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']
            df = df.with_columns([
                pl.col(col).mul(1024) for col in memory_cols if col in df.columns
            ])

            # Calculate memory metrics in GB for Anvil standardization:
            # 1. Total memory used by the system
            # 2. Memory used excluding disk cache (FilePages represents cache)
            # The .clip(0, None) prevents negative values that could occur due to timing differences
            df = df.with_columns([
                (pl.col('MemUsed') / (1024 ** 3)).alias('memused'),
                ((pl.col('MemUsed') - pl.col('FilePages')) / (1024 ** 3))
                .clip(0, None)
                .alias('memused_minus_diskcache')
            ])

            # Create Anvil-formatted DataFrame for total memory usage
            memused_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('memused').alias('Event'),
                pl.col('memused').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            # Create Anvil-formatted DataFrame for memory usage excluding disk cache
            memused_nocache_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('memused_minus_diskcache').alias('Event'),
                pl.col('memused_minus_diskcache').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            # Return both metrics as a list of DataFrames to be concatenated later
            return [memused_df, memused_nocache_df]
        except Exception as e:
            logger.error(f"Error processing memory file {file_path}: {e}")
            return None

    def process_node_data(self) -> pl.DataFrame:
        """Process all metrics for a node and combine into a unified Anvil-format DataFrame

        This function orchestrates the processing of different metric types (block, CPU, NFS, memory)
        and combines them into a single DataFrame conforming to the Anvil schema:
        - Job Id: Unique job identifier (standardized format)
        - Host: Origin node where data was gathered
        - Timestamp: When the data point was recorded
        - Event: Resource usage metric type (block, cpuuser, nfs, memused, memused_minus_diskcache)
        - Value: Numeric metric value
        - Units: Measurement unit (GB/s, CPU %, MB/s, GB)
        """
        logger.info(f"Processing all files for node in directory: {self.cache_dir}")

        dfs = []
        try:
            # Process each metric type if the corresponding file exists
            # For each file: read, transform, append to list, then remove to save space

            # Process block device I/O metrics
            block_path = self.cache_dir / 'block.csv'
            if block_path.exists() and block_path.stat().st_size > 0:
                block_df = self.process_block_file(block_path)
                if isinstance(block_df, pl.DataFrame) and len(block_df) > 0:
                    dfs.append(block_df)
                # Delete file after processing
                os.remove(block_path)

            # Process CPU utilization metrics
            cpu_path = self.cache_dir / 'cpu.csv'
            if cpu_path.exists() and cpu_path.stat().st_size > 0:
                cpu_df = self.process_cpu_file(cpu_path)
                if isinstance(cpu_df, pl.DataFrame) and len(cpu_df) > 0:
                    dfs.append(cpu_df)
                # Delete file after processing
                os.remove(cpu_path)

            # Process NFS throughput metrics
            nfs_path = self.cache_dir / 'nfs.csv'
            if nfs_path.exists() and nfs_path.stat().st_size > 0:
                nfs_df = self.process_nfs_file(nfs_path)
                if isinstance(nfs_df, pl.DataFrame) and len(nfs_df) > 0:
                    dfs.append(nfs_df)
                # Delete file after processing
                os.remove(nfs_path)

            # Process memory usage metrics (produces two DataFrames)
            mem_path = self.cache_dir / 'mem.csv'
            if mem_path.exists() and mem_path.stat().st_size > 0:
                memory_dfs = self.process_memory_metrics(mem_path)
                if memory_dfs is not None and len(memory_dfs) > 0:
                    dfs.extend(memory_dfs)
                # Delete file after processing
                os.remove(mem_path)

            # Combine all DataFrames into a single Anvil-format DataFrame
            if dfs:
                # ThreadPoolExecutor is set up for potential future parallelization
                # Currently, it simply wraps the DataFrames
                with ThreadPoolExecutor(max_workers=max(1, multiprocessing.cpu_count() - 1)) as executor:
                    # No actual parallel work here, but setting up the structure
                    # for potential future parallelization of data transformations
                    futures = [executor.submit(lambda df=df: df) for df in dfs]
                    processed_dfs = [future.result() for future in futures]

                # Concatenate all metrics into a unified DataFrame following Anvil schema
                result = pl.concat(processed_dfs)
                logger.info(f"Successfully processed node data with {len(result)} rows")
                return result
            else:
                logger.warning(f"No valid data processed for node in {self.cache_dir}")
                return pl.DataFrame()

        except Exception as e:
            logger.error(f"Error processing node data in {self.cache_dir}: {e}")
            return pl.DataFrame()
