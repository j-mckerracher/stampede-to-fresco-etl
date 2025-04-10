import logging
from typing import Union

import polars as pl
from pathlib import Path
import logging
import os

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

# --- Constants ---
SECTOR_SIZE_BYTES = 512
BYTES_TO_GB = 1 / (1024 ** 3)
BYTES_TO_MB = 1 / (1024 ** 2)
MIN_TIME_DELTA_SECONDS = 0.1  # Minimum interval for rate calculation


class NodeDataProcessor:
    """Process node data files using Polars to transform Stampede data into Anvil format

    The Anvil format standardizes resource usage metrics across multiple HPC systems
    with a common schema: Job Id, Host, Timestamp, Event, Value, Units.
    This processor handles the transformation of block I/O, CPU usage, Lustre traffic,
    and memory metrics into this unified format, assuming host-level aggregation
    is desired for block, CPU, and Lustre metrics based on typical HPC monitoring needs.
    Memory is processed per-node as specified in context.
    """

    def __init__(self, cache_dir: str = 'cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        logger.info(f"NodeDataProcessor initialized. Cache directory: {self.cache_dir}")

    # --- Helper Function for Robust Reading ---
    def _read_csv_robust(self, file_path: Path, dtypes: dict = None) -> Union[pl.DataFrame, None]:
        """Reads a CSV using Polars with basic error handling."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            logger.warning(f"File does not exist or is empty: {file_path}")
            return None
        try:
            # Use infer_schema_length=0 to rely on dtypes or let polars infer fully
            # null_values handles potential non-standard nulls if needed
            # ignore_errors=True can skip bad rows but might hide issues
            df = pl.read_csv(file_path, dtypes=dtypes, infer_schema_length=1000, ignore_errors=False)
            if df.height == 0:
                logger.warning(f"Read file {file_path}, but it resulted in an empty DataFrame.")
                return None
            return df
        except Exception as e:
            logger.error(f"Failed to read or parse CSV file {file_path}: {e}")
            return None

    # --- Processing Functions ---

    def process_block_file(self, file_path: Path) -> Union[pl.DataFrame, None]:
        """Transform block device metrics into Anvil format (Host Aggregated GB/s).

        Calculates block device throughput rate (GB/s) over wall-clock time
        by:
        1. Summing cumulative read/write sectors across all devices per host/timestamp.
        2. Calculating the change (delta) in total sectors between consecutive timestamps.
        3. Calculating the change (delta) in wall-clock time.
        4. Calculating Rate = (Sector Delta * 512 Bytes) / Time Delta (seconds).
        5. Converting Bytes/s to GB/s.
        """
        logger.info(f"Processing block file: {file_path}")
        df = self._read_csv_robust(file_path, dtypes={'rd_sectors': pl.Float64, 'wr_sectors': pl.Float64})
        if df is None: return None

        try:
            # 1. Prepare data: Cast, Parse Timestamp, Calculate total sectors per row
            df = df.with_columns([
                pl.col('rd_sectors').fill_null(0).cast(pl.Float64),
                pl.col('wr_sectors').fill_null(0).cast(pl.Float64),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False).alias('Timestamp'),
                (pl.col('rd_sectors') + pl.col('wr_sectors')).alias('total_sectors')
            ]).drop_nulls(subset=['Timestamp', 'jobID', 'node'])  # Need these for grouping/diff

            # 2. Aggregate sectors per host/timestamp
            group_keys_ts = ['jobID', 'node', 'Timestamp']
            aggregated_df = df.group_by(group_keys_ts, maintain_order=True).agg(
                pl.sum('total_sectors').alias('sum_total_sectors')
            )

            # 3. Calculate deltas within each host group
            group_keys_host = ['jobID', 'node']
            # Ensure sorting before applying window functions like diff
            aggregated_df = aggregated_df.sort(group_keys_ts)

            diff_df = aggregated_df.with_columns([
                pl.col('sum_total_sectors').diff().over(group_keys_host).alias('sector_delta'),
                pl.col('Timestamp').diff().over(group_keys_host).dt.total_seconds().alias('time_delta_seconds')
            ])

            # 4. Calculate Rate (GB/s)
            # Filter out invalid deltas (first entry per group, negative sector delta, too small time delta)
            rate_df = diff_df.filter(
                (pl.col('time_delta_seconds') >= MIN_TIME_DELTA_SECONDS) &
                (pl.col('sector_delta') >= 0)  # Cumulative counters shouldn't decrease
            )

            # Perform rate calculation only on valid rows
            rate_df = rate_df.with_columns([
                (((pl.col('sector_delta') * SECTOR_SIZE_BYTES) / pl.col('time_delta_seconds')) * BYTES_TO_GB)
                .clip(lower_bound=0)  # Ensure non-negative rate
                .alias('Value')
            ])

            # 5. Transform to Anvil schema
            return rate_df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('block').alias('Event'),
                pl.col('Value'),
                pl.lit('GB/s').alias('Units')
            ])

        except Exception as e:
            logger.error(f"Error processing block file {file_path}: {e}", exc_info=True)
            return None

    def process_cpu_file(self, file_path: Path) -> Union[pl.DataFrame, None]:
        """Transform CPU usage metrics into Anvil format (Host Aggregated User%).

        Calculates CPU user mode percentage relative to *active* time by:
        1. Summing cumulative CPU time components across all cores per host/timestamp.
        2. Calculating total aggregated active time (all states except idle).
        3. Calculating total aggregated user+nice time.
        4. Determining the percentage of user+nice time relative to active time.
        5. Ensuring values are in valid percentage range (0-100%).
        """
        logger.info(f"Processing CPU file: {file_path}")
        cpu_time_cols = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
        dtypes = {col: pl.Float64 for col in cpu_time_cols}
        df = self._read_csv_robust(file_path, dtypes=dtypes)
        if df is None: return None

        try:
            # 1. Prepare data: Cast, Parse Timestamp
            df = df.with_columns([
                pl.col(col).fill_null(0).cast(pl.Float64) for col in cpu_time_cols
            ]).with_columns(
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False).alias('Timestamp')
            ).drop_nulls(subset=['Timestamp', 'jobID', 'node'])

            # 2. Aggregate CPU times per host/timestamp
            group_keys_ts = ['jobID', 'node', 'Timestamp']
            aggregated_df = df.group_by(group_keys_ts, maintain_order=True).agg([
                pl.sum(col).alias(col) for col in cpu_time_cols
            ])

            # 3. Calculate percentage on aggregated data
            aggregated_df = aggregated_df.with_columns([
                (pl.col('user') + pl.col('nice') + pl.col('system') +
                 pl.col('iowait') + pl.col('irq') + pl.col('softirq')).alias('total_active'),
                (pl.col('user') + pl.col('nice')).alias('total_user_nice')
            ])

            aggregated_df = aggregated_df.with_columns([
                pl.when(pl.col('total_active') > 0)
                .then((pl.col('total_user_nice') / pl.col('total_active')) * 100)
                .otherwise(0.0)  # Set to float 0.0
                .clip(lower_bound=0.0, upper_bound=100.0)  # Use floats for bounds
                .alias('Value')
            ])

            # 4. Transform to Anvil schema
            return aggregated_df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('cpuuser').alias('Event'),
                pl.col('Value'),
                pl.lit('CPU %').alias('Units')
            ])

        except Exception as e:
            logger.error(f"Error processing CPU file {file_path}: {e}", exc_info=True)
            return None

    def process_llite_file(self, file_path: Path) -> Union[pl.DataFrame, None]:
        """Transform Lustre ('llite') traffic metrics into Anvil format (Host Aggregated MB/s).

        Calculates Lustre filesystem throughput rate (MB/s) over wall-clock time by:
        1. Summing cumulative read/write bytes across all mounts per host/timestamp.
        2. Calculating the change (delta) in total bytes between consecutive timestamps.
        3. Calculating the change (delta) in wall-clock time.
        4. Calculating Rate = (Byte Delta) / Time Delta (seconds).
        5. Converting Bytes/s to MB/s. Maps event type to 'nfs' for Anvil standard.
        """
        logger.info(f"Processing Lustre (llite) file: {file_path}")
        dtypes = {'read_bytes': pl.Float64, 'write_bytes': pl.Float64}
        df = self._read_csv_robust(file_path, dtypes=dtypes)
        if df is None: return None

        try:
            # 1. Prepare data: Cast, Parse Timestamp, Calculate total bytes per row
            df = df.with_columns([
                pl.col('read_bytes').fill_null(0).cast(pl.Float64),
                pl.col('write_bytes').fill_null(0).cast(pl.Float64),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False).alias('Timestamp'),
                (pl.col('read_bytes') + pl.col('write_bytes')).alias('total_bytes')
            ]).drop_nulls(subset=['Timestamp', 'jobID', 'node'])

            # 2. Aggregate bytes per host/timestamp
            group_keys_ts = ['jobID', 'node', 'Timestamp']
            aggregated_df = df.group_by(group_keys_ts, maintain_order=True).agg(
                pl.sum('total_bytes').alias('sum_total_bytes')
            )

            # 3. Calculate deltas within each host group
            group_keys_host = ['jobID', 'node']
            # Ensure sorting before applying window functions like diff
            aggregated_df = aggregated_df.sort(group_keys_ts)

            diff_df = aggregated_df.with_columns([
                pl.col('sum_total_bytes').diff().over(group_keys_host).alias('byte_delta'),
                pl.col('Timestamp').diff().over(group_keys_host).dt.total_seconds().alias('time_delta_seconds')
            ])

            # 4. Calculate Rate (MB/s)
            # Filter out invalid deltas
            rate_df = diff_df.filter(
                (pl.col('time_delta_seconds') >= MIN_TIME_DELTA_SECONDS) &
                (pl.col('byte_delta') >= 0)  # Cumulative counters shouldn't decrease
            )

            # Perform rate calculation only on valid rows
            rate_df = rate_df.with_columns([
                ((pl.col('byte_delta') / pl.col('time_delta_seconds')) * BYTES_TO_MB)
                .clip(lower_bound=0)  # Ensure non-negative rate
                .alias('Value')
            ])

            # 5. Transform to Anvil schema (mapping event to 'nfs')
            return rate_df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('nfs').alias('Event'),  # Standard Anvil event name
                pl.col('Value'),
                pl.lit('MB/s').alias('Units')
            ])

        except Exception as e:
            logger.error(f"Error processing llite file {file_path}: {e}", exc_info=True)
            return None

    def process_memory_metrics(self, file_path: Path) -> Union[pl.DataFrame, None]:
        """Transform memory usage metrics (per-node) into Anvil format (GB).

        Processes memory data assuming input units are Bytes and data is per-node
        (as per documentation, ignoring 'device' column if present). Calculates:
        1. Total memory usage ('memused') = MemUsed
        2. Memory usage excluding disk cache ('memused_minus_diskcache') = MemUsed - FilePages

        Both metrics are converted from Bytes to GB.
        """
        logger.info(f"Processing memory file: {file_path}")
        # Define expected columns and their types (assuming input is Bytes)
        memory_cols = ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']
        dtypes = {col: pl.Float64 for col in memory_cols}
        df = self._read_csv_robust(file_path, dtypes=dtypes)
        if df is None: return None

        try:
            # 1. Prepare data: Cast to float, Parse Timestamp
            # Do NOT multiply by 1024 - context confirms input is Bytes.
            df = df.with_columns([
                pl.col(col).fill_null(0).cast(pl.Float64) for col in memory_cols if col in df.columns
            ]).with_columns(
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False).alias('Timestamp')
            ).drop_nulls(
                subset=['Timestamp', 'jobID', 'node', 'MemUsed', 'FilePages'])  # Ensure necessary columns are non-null

            # 2. Calculate memory metrics in GB (no aggregation needed per context)
            # Clip(lower_bound=0) ensures non-negative values.
            df = df.with_columns([
                (pl.col('MemUsed') * BYTES_TO_GB).clip(lower_bound=0).alias('memused'),
                ((pl.col('MemUsed') - pl.col('FilePages')) * BYTES_TO_GB)
                .clip(lower_bound=0)
                .alias('memused_minus_diskcache')
            ])

            # 3. Create Anvil-formatted DataFrame for 'memused'
            memused_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('memused').alias('Event'),
                pl.col('memused').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            # 4. Create Anvil-formatted DataFrame for 'memused_minus_diskcache'
            memused_nocache_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('memused_minus_diskcache').alias('Event'),
                pl.col('memused_minus_diskcache').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            # Return both metrics as a list of DataFrames
            return [memused_df, memused_nocache_df]

        except Exception as e:
            logger.error(f"Error processing memory file {file_path}: {e}", exc_info=True)
            return None

    # --- Orchestration Method ---

    def process_node_data(self) -> Union[pl.DataFrame, None]:
        """Process all metrics for a node and combine into a unified Anvil-format DataFrame

        Orchestrates the processing of different metric types (block, CPU, llite, memory)
        by calling the respective processing functions and concatenating the results.
        Removes processed source files from the cache directory.
        """
        logger.info(f"Processing all metric files in directory: {self.cache_dir}")

        all_processed_dfs = []
        files_to_process = {
            'block': (self.cache_dir / 'block.csv', self.process_block_file),
            'cpu': (self.cache_dir / 'cpu.csv', self.process_cpu_file),
            'llite': (self.cache_dir / 'llite.csv', self.process_llite_file),  # Changed from nfs
            'memory': (self.cache_dir / 'mem.csv', self.process_memory_metrics),
        }

        for metric_name, (file_path, process_func) in files_to_process.items():
            logger.info(f"Checking for {metric_name} file: {file_path}")
            if file_path.exists() and file_path.stat().st_size > 0:
                try:
                    result = process_func(file_path)

                    # Handle results: None, single DataFrame, or list of DataFrames (for memory)
                    if isinstance(result, pl.DataFrame) and result.height > 0:
                        all_processed_dfs.append(result)
                        logger.info(f"Successfully processed {metric_name}. Added {result.height} rows.")
                    elif isinstance(result, list):  # Handle memory's list output
                        valid_dfs_in_list = [df for df in result if isinstance(df, pl.DataFrame) and df.height > 0]
                        if valid_dfs_in_list:
                            all_processed_dfs.extend(valid_dfs_in_list)
                            total_rows = sum(df.height for df in valid_dfs_in_list)
                            logger.info(
                                f"Successfully processed {metric_name}. Added {total_rows} rows from {len(valid_dfs_in_list)} DataFrame(s).")
                        else:
                            logger.warning(
                                f"Processed {metric_name} file, but no valid data rows were generated in the list.")
                    elif result is None or (isinstance(result, pl.DataFrame) and result.height == 0):
                        logger.warning(f"Processed {metric_name} file, but no valid data rows were generated.")
                    else:
                        logger.warning(
                            f"Processing function for {metric_name} returned unexpected type: {type(result)}")

                    # Delete file after attempting processing (success or failure)
                    try:
                        os.remove(file_path)
                        logger.info(f"Removed processed source file: {file_path}")
                    except OSError as e:
                        logger.error(f"Failed to remove source file {file_path}: {e}")

                except Exception as e:
                    logger.error(f"Critical error calling processing function for {metric_name} file {file_path}: {e}",
                                 exc_info=True)
                    # Optionally try to remove file even after critical error
                    if file_path.exists():
                        try:
                            os.remove(file_path)
                            logger.info(f"Removed source file {file_path} after critical processing error.")
                        except OSError as re:
                            logger.error(f"Failed to remove source file {file_path} after critical error: {re}")

            else:
                logger.info(f"Skipping {metric_name}: File not found or is empty at {file_path}")

        # Combine all DataFrames into a single Anvil-format DataFrame
        if all_processed_dfs:
            try:
                logger.info(f"Concatenating results from {len(all_processed_dfs)} processed DataFrame(s)...")
                # Use low_memory=True if concatenating many large frames, may increase memory usage temporarily
                final_result = pl.concat(all_processed_dfs, how='vertical')
                logger.info(f"Successfully combined node data into final DataFrame with {final_result.height} rows.")
                if final_result.height == 0:
                    logger.warning("Concatenation resulted in an empty DataFrame.")
                    return None
                return final_result
            except Exception as e:
                logger.error(f"Failed to concatenate processed DataFrames: {e}", exc_info=True)
                return None  # Return None on concatenation error
        else:
            logger.warning(f"No valid data processed from any metric file in {self.cache_dir}")
            return None  # Return None explicitly if nothing was processed
