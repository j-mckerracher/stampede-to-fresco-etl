import logging
from typing import Union, List, Dict, Optional
import polars as pl
from pathlib import Path
import os
import gc  # Added for explicit garbage collection if needed

# Set up logging (assuming it's configured as in the orchestrator,
# or you can duplicate the basicConfig here if this file is run standalone for testing)
# For this snippet, we'll assume logger is obtained if this is part of a larger module
logger = logging.getLogger(__name__)
if not logger.hasHandlers():  # Add a basic handler if no handlers are configured
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

# --- Constants ---
SECTOR_SIZE_BYTES = 512
BYTES_TO_GB = 1 / (1024 ** 3)
BYTES_TO_MB = 1 / (1024 ** 2)
MIN_TIME_DELTA_SECONDS = 0.1  # Minimum interval for rate calculation


# Helper functions that might be used by NodeDataProcessor methods
# Ensure these are defined or imported if they were part of the original context.
# For this example, defining simplified versions or assuming their existence.
def safe_division(numerator, denominator, default: float = 0.0) -> pl.Expr:
    """Safely perform division in Polars, returning default if denominator is zero or null."""
    return pl.when(denominator != 0).then(numerator / denominator).otherwise(default)


def validate_metric(value_expr: pl.Expr, min_val: float = 0.0, max_val: float = float('inf')) -> pl.Expr:
    """Ensure metric values are within a valid range using Polars expressions."""
    return value_expr.clip(lower_bound=min_val, upper_bound=max_val).fill_null(min_val)


class NodeDataProcessor:
    """Process node data files using Polars to transform Stampede data into FRESCO intermediate format.

    Outputs data with high fidelity using original timestamps. The FRESCO format
    standardizes resource usage metrics with a common schema:
    Job Id, Host, Timestamp, Event, Value, Units.
    """

    def __init__(self, cache_dir: str = 'cache_placeholder'):  # Placeholder if not set by orchestrator
        self.cache_dir = Path(cache_dir)
        # Cache_dir is expected to be set by the orchestrator to point to the specific node's temp download dir
        # self.cache_dir.mkdir(exist_ok=True, parents=True) # Orchestrator handles temp dir creation
        logger.info(f"NodeDataProcessor initialized. Expecting cache_dir to be set externally per node.")

    def _read_csv_robust(self, file_path: Path, dtypes: dict = None, schema_cols: Optional[List[str]] = None) -> Union[
        pl.DataFrame, None]:
        """Reads a CSV using Polars with basic error handling and column selection."""
        if not file_path.exists() or file_path.stat().st_size == 0:
            logger.warning(f"File does not exist or is empty: {file_path}")
            return None
        try:
            # Try to read only necessary columns if schema_cols is provided
            # Polars read_csv dtypes parameter helps with initial type inference.
            df = pl.read_csv(file_path, dtypes=dtypes, infer_schema_length=1000, ignore_errors=False,
                             null_values=["", "NA", "NULL"])

            if df.height == 0:
                logger.warning(f"Read file {file_path}, but it resulted in an empty DataFrame.")
                return None

            # If schema_cols are specified, ensure they exist and select them
            if schema_cols:
                existing_cols = [col for col in schema_cols if col in df.columns]
                if len(existing_cols) < len(schema_cols):
                    missing = set(schema_cols) - set(existing_cols)
                    logger.warning(
                        f"File {file_path} is missing expected columns: {missing}. Proceeding with available columns.")
                if not existing_cols:
                    logger.error(f"File {file_path} has none of the expected columns: {schema_cols}.")
                    return None
                df = df.select(existing_cols)

            return df
        except Exception as e:
            logger.error(f"Failed to read or parse CSV file {file_path}: {e}")
            return None

    def process_block_file(self, file_path: Path) -> Union[pl.DataFrame, None]:
        logger.info(f"Processing block file: {file_path}")
        # Define expected columns for reading to optimize memory
        block_read_cols = ['jobID', 'node', 'timestamp', 'rd_sectors', 'wr_sectors',
                           'device']  # device is used for context if needed
        # Dtypes for robust parsing
        block_dtypes = {'rd_sectors': pl.Float64, 'wr_sectors': pl.Float64, 'jobID': pl.Utf8, 'node': pl.Utf8,
                        'timestamp': pl.Utf8, 'device': pl.Utf8}

        df = self._read_csv_robust(file_path, dtypes=block_dtypes, schema_cols=block_read_cols)
        if df is None: return None

        try:
            df = df.with_columns([
                pl.col('rd_sectors').fill_null(0),  # Already float from dtypes
                pl.col('wr_sectors').fill_null(0),  # Already float from dtypes
                pl.col('timestamp').str.strptime(pl.Datetime(time_unit="us"), "%m/%d/%Y %H:%M:%S", strict=False).alias(
                    'Timestamp'),
                (pl.col('rd_sectors') + pl.col('wr_sectors')).alias('total_sectors_device')  # per device
            ]).drop_nulls(
                subset=['Timestamp', 'jobID', 'node'])  # device is not strictly needed for node aggregation if not used

            group_keys_node_ts = ['jobID', 'node', 'Timestamp']
            node_aggregated_sectors = df.group_by(group_keys_node_ts, maintain_order=True).agg(
                pl.sum('total_sectors_device').alias('sum_total_sectors_node')  # Node total sectors at this timestamp
            )

            node_aggregated_sectors = node_aggregated_sectors.sort(
                group_keys_node_ts)  # Ensure order for diff over node

            group_keys_node_only = ['jobID', 'node']
            rate_df = node_aggregated_sectors.with_columns([
                pl.col('sum_total_sectors_node').diff().over(group_keys_node_only).alias('sector_delta_node'),
                pl.col('Timestamp').diff().over(group_keys_node_only).dt.total_seconds().alias('time_delta_seconds')
            ]).filter(
                (pl.col('time_delta_seconds') >= MIN_TIME_DELTA_SECONDS) &
                (pl.col('sector_delta_node') >= 0)
            ).with_columns(
                validate_metric(
                    safe_division(
                        (pl.col('sector_delta_node') * SECTOR_SIZE_BYTES),
                        pl.col('time_delta_seconds')
                    ) * BYTES_TO_GB
                ).alias('Value')
            )

            return rate_df.select([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=True)  # Standardize prefix
                .str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('block').alias('Event'),
                pl.col('Value'),
                pl.lit('GB/s').alias('Units')
            ]).drop_nulls('Value')  # Drop if rate calculation failed

        except Exception as e:
            logger.error(f"Error processing block file {file_path}: {e}", exc_info=True)
            return None

    def process_cpu_file(self, file_path: Path) -> Union[pl.DataFrame, None]:
        logger.info(f"Processing CPU file for 'cpuuser' with high fidelity: {file_path}")
        cpu_jiffy_columns = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
        # Define expected columns for reading
        cpu_read_cols = ['jobID', 'node', 'timestamp', 'device'] + cpu_jiffy_columns
        # Dtypes for robust parsing
        cpu_dtypes = {col: pl.Float64 for col in cpu_jiffy_columns}
        cpu_dtypes.update({'jobID': pl.Utf8, 'node': pl.Utf8, 'timestamp': pl.Utf8, 'device': pl.Utf8})

        df = self._read_csv_robust(file_path, dtypes=cpu_dtypes, schema_cols=cpu_read_cols)
        if df is None: return None

        try:
            # 1. Prepare data: Parse Timestamp, ensure numeric types
            df = df.with_columns([
                pl.col(col).fill_null(0) for col in cpu_jiffy_columns  # Already float from dtypes
            ]).with_columns(
                pl.col('timestamp').str.strptime(pl.Datetime(time_unit="us"), "%m/%d/%Y %H:%M:%S", strict=False).alias(
                    'Timestamp_original')
            ).drop_nulls(subset=['Timestamp_original', 'jobID', 'node', 'device'])

            # 2. Sum Core Jiffies to Node Level (per Original Timestamp)
            logger.debug(f"Aggregating raw per-core jiffies to node level for {file_path.name}...")
            node_level_jiffies = df.group_by(
                ['jobID', 'node', 'Timestamp_original'], maintain_order=True
            ).agg([
                pl.sum(col).alias(col) for col in cpu_jiffy_columns
                # Results in columns 'user', 'nice', etc. with summed values
            ])

            if node_level_jiffies.height == 0:
                logger.warning(f"Node-level jiffy aggregation resulted in empty DataFrame for {file_path.name}.")
                return None

            # 3. Calculate Deltas for Node-Level Jiffies
            logger.debug(f"Calculating deltas for node-level jiffies for {file_path.name}...")
            node_level_jiffies = node_level_jiffies.sort(['jobID', 'node', 'Timestamp_original'])

            delta_col_exprs = []
            for col in cpu_jiffy_columns:
                delta_col_exprs.append(
                    pl.col(col).diff().over(['jobID', 'node']).alias(f"{col}_node_delta")
                )
            node_level_jiffies = node_level_jiffies.with_columns(delta_col_exprs)

            # 4. Calculate Total Jiffies Delta
            delta_sum_expr = pl.sum_horizontal([pl.col(f"{col}_node_delta") for col in cpu_jiffy_columns])
            node_level_jiffies = node_level_jiffies.with_columns(
                delta_sum_expr.alias('total_jiffies_node_delta')
            )

            # 5. Calculate `cpuuser` Percentage
            node_level_jiffies = node_level_jiffies.with_columns(
                validate_metric(  # Handles clip 0-100 and null fill
                    safe_division(
                        pl.col('user_node_delta'),  # 'user_node_delta' comes from the delta calculation
                        pl.col('total_jiffies_node_delta')
                    ) * 100,
                    min_val=0.0, max_val=100.0
                ).alias('Value_cpuuser')
            )

            # 6. Filter Invalid Deltas (where total_jiffies_node_delta is null or not positive)
            final_df = node_level_jiffies.filter(
                pl.col('total_jiffies_node_delta').is_not_null() & (pl.col('total_jiffies_node_delta') > 0)
            )

            if final_df.height == 0:
                logger.warning(f"No valid CPU data after delta calculation and filtering for {file_path.name}")
                return None

            # 7. Transform to FRESCO schema
            return final_df.select([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=True)
                .str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp_original').alias('Timestamp'),  # Use the original timestamp
                pl.lit('cpuuser').alias('Event'),
                pl.col('Value_cpuuser').alias('Value'),
                pl.lit('CPU %').alias('Units')
            ])

        except Exception as e:
            logger.error(f"Error processing CPU file {file_path}: {e}", exc_info=True)
            return None

    def process_llite_file(self, file_path: Path) -> Union[pl.DataFrame, None]:
        logger.info(f"Processing Lustre (llite) file: {file_path}")
        # Define expected columns for reading
        llite_read_cols = ['jobID', 'node', 'timestamp', 'read_bytes',
                           'write_bytes']  # Assuming no 'device' for llite node total
        # Dtypes for robust parsing
        llite_dtypes = {'read_bytes': pl.Float64, 'write_bytes': pl.Float64, 'jobID': pl.Utf8, 'node': pl.Utf8,
                        'timestamp': pl.Utf8}

        df = self._read_csv_robust(file_path, dtypes=llite_dtypes, schema_cols=llite_read_cols)
        if df is None: return None

        try:
            df = df.with_columns([
                pl.col('read_bytes').fill_null(0),  # Already float
                pl.col('write_bytes').fill_null(0),  # Already float
                pl.col('timestamp').str.strptime(pl.Datetime(time_unit="us"), "%m/%d/%Y %H:%M:%S", strict=False).alias(
                    'Timestamp'),
                (pl.col('read_bytes') + pl.col('write_bytes')).alias('total_bytes_node')
                # Assuming these are already node totals or should be summed if multiple entries per timestamp
            ]).drop_nulls(subset=['Timestamp', 'jobID', 'node'])

            # If llite can have multiple rows per (jobID, node, Timestamp) e.g. different mounts, sum them first.
            # Based on typical llite data, it's often one aggregated line per node snapshot.
            # If it *could* have multiple, uncomment and test this:
            # group_keys_node_ts = ['jobID', 'node', 'Timestamp']
            # node_aggregated_bytes = df.group_by(group_keys_node_ts, maintain_order=True).agg(
            #     pl.sum('total_bytes_node').alias('sum_total_bytes_node')
            # )
            # df_to_diff = node_aggregated_bytes.sort(group_keys_node_ts) # Diff this
            # col_to_diff = 'sum_total_bytes_node'

            # Assuming df is already effectively node-aggregated per timestamp after initial sum, or llite provides one row
            df_to_diff = df.sort(['jobID', 'node', 'Timestamp'])
            col_to_diff = 'total_bytes_node'

            group_keys_node_only = ['jobID', 'node']
            rate_df = df_to_diff.with_columns([
                pl.col(col_to_diff).diff().over(group_keys_node_only).alias('byte_delta_node'),
                pl.col('Timestamp').diff().over(group_keys_node_only).dt.total_seconds().alias('time_delta_seconds')
            ]).filter(
                (pl.col('time_delta_seconds') >= MIN_TIME_DELTA_SECONDS) &
                (pl.col('byte_delta_node') >= 0)
            ).with_columns(
                validate_metric(
                    safe_division(
                        pl.col('byte_delta_node'),
                        pl.col('time_delta_seconds')
                    ) * BYTES_TO_MB  # Convert to MB/s
                ).alias('Value')
            )

            return rate_df.select([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=True)
                .str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('nfs').alias('Event'),
                pl.col('Value'),
                pl.lit('MB/s').alias('Units')
            ]).drop_nulls('Value')

        except Exception as e:
            logger.error(f"Error processing llite file {file_path}: {e}", exc_info=True)
            return None

    def process_memory_metrics(self, file_path: Path) -> Union[List[pl.DataFrame], None]:
        logger.info(f"Processing memory file: {file_path}")
        memory_cols_read = ['jobID', 'node', 'timestamp', 'MemTotal', 'MemFree', 'MemUsed', 'FilePages']
        mem_dtypes = {col: pl.Float64 for col in ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']}
        mem_dtypes.update({'jobID': pl.Utf8, 'node': pl.Utf8, 'timestamp': pl.Utf8})

        df = self._read_csv_robust(file_path, dtypes=mem_dtypes, schema_cols=memory_cols_read)
        if df is None: return None

        try:
            df = df.with_columns([
                pl.col(col).fill_null(0) for col in ['MemTotal', 'MemFree', 'MemUsed', 'FilePages'] if col in df.columns
                # Already float
            ]).with_columns(
                pl.col('timestamp').str.strptime(pl.Datetime(time_unit="us"), "%m/%d/%Y %H:%M:%S", strict=False).alias(
                    'Timestamp')
            ).drop_nulls(subset=['Timestamp', 'jobID', 'node', 'MemUsed', 'FilePages'])  # MemUsed is directly available

            # Use MemUsed directly if available and valid, otherwise MemTotal - MemFree
            # This sample logic assumes MemUsed is the primary source
            if 'MemUsed' not in df.columns:
                if 'MemTotal' in df.columns and 'MemFree' in df.columns:
                    logger.info("MemUsed not found, calculating from MemTotal - MemFree for memory processing.")
                    df = df.with_columns((pl.col('MemTotal') - pl.col('MemFree')).alias('MemUsed_calc'))
                    mem_used_col = 'MemUsed_calc'
                else:
                    logger.error("Cannot determine used memory: MemUsed, MemTotal, or MemFree missing.")
                    return None
            else:
                mem_used_col = 'MemUsed'

            df = df.with_columns([
                (pl.col(mem_used_col) * BYTES_TO_GB).clip(lower_bound=0).alias('Value_memused'),
                ((pl.col(mem_used_col) - pl.col('FilePages')) * BYTES_TO_GB).clip(lower_bound=0).alias(
                    'Value_memused_minus_diskcache')
            ])

            memused_df = df.select([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=True)
                .str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('memused').alias('Event'),
                pl.col('Value_memused').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            memused_nocache_df = df.select([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=True)
                .str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('Timestamp'),
                pl.lit('memused_minus_diskcache').alias('Event'),
                pl.col('Value_memused_minus_diskcache').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            # Filter out rows where Value might be null if calculation failed or inputs were bad
            memused_df = memused_df.drop_nulls('Value')
            memused_nocache_df = memused_nocache_df.drop_nulls('Value')

            return [memused_df, memused_nocache_df]

        except Exception as e:
            logger.error(f"Error processing memory file {file_path}: {e}", exc_info=True)
            return None

    def process_node_data(self) -> Union[pl.DataFrame, None]:
        logger.info(f"Processing all metric files in node directory: {self.cache_dir}")
        all_processed_dfs = []
        # Ensure REQUIRED_FILES matches the keys here and filenames on disk
        files_to_process_map: Dict[str, Dict[str, Union[str, callable]]] = {
            'block': {'filename': 'block.csv', 'func': self.process_block_file},
            'cpu': {'filename': 'cpu.csv', 'func': self.process_cpu_file},
            'llite': {'filename': 'llite.csv', 'func': self.process_llite_file},
            'memory': {'filename': 'mem.csv', 'func': self.process_memory_metrics},
        }

        for metric_name, details in files_to_process_map.items():
            file_path = self.cache_dir / str(details['filename'])  # Cast to string for Path object
            process_func = details['func']

            logger.info(f"Checking for {metric_name} file: {file_path}")
            if file_path.exists() and file_path.stat().st_size > 0:
                try:
                    result = process_func(file_path)
                    if isinstance(result, pl.DataFrame) and result.height > 0:
                        all_processed_dfs.append(result)
                        logger.info(f"Successfully processed {metric_name}. Added {result.height} rows.")
                    elif isinstance(result, list):
                        valid_dfs = [df for df in result if isinstance(df, pl.DataFrame) and df.height > 0]
                        if valid_dfs:
                            all_processed_dfs.extend(valid_dfs)
                            total_rows = sum(df.height for df in valid_dfs)
                            logger.info(
                                f"Successfully processed {metric_name} (multi-event). Added {total_rows} rows from {len(valid_dfs)} DataFrame(s).")
                        else:
                            logger.warning(
                                f"Processed {metric_name} file, but no valid data rows generated from list output.")
                    elif result is None or (isinstance(result, pl.DataFrame) and result.height == 0):
                        logger.warning(f"Processed {metric_name} file, but no valid data rows were generated.")

                    # Orchestrator script (stampede_etl_batched_daily_agg.py) handles deleting temp node files
                    # So, NodeDataProcessor should not delete them here.
                    # if file_path.exists():
                    #     try:
                    #         os.remove(file_path)
                    #         logger.info(f"Removed processed source file: {file_path}")
                    #     except OSError as e:
                    #         logger.error(f"Failed to remove source file {file_path}: {e}")

                except Exception as e:
                    logger.error(f"Critical error calling processing function for {metric_name} file {file_path}: {e}",
                                 exc_info=True)
            else:
                logger.info(f"Skipping {metric_name}: File not found or is empty at {file_path}")

        if all_processed_dfs:
            try:
                logger.info(
                    f"Concatenating results from {len(all_processed_dfs)} processed DataFrame(s) for node {self.cache_dir.name}...")
                final_result = pl.concat(all_processed_dfs,
                                         how='vertical_relaxed')  # Use vertical_relaxed for differing schemas before
                # final selection
                if final_result.height == 0:
                    logger.warning(f"Concatenation for node {self.cache_dir.name} resulted in an empty DataFrame.")
                    return None

                # Ensure standard FRESCO columns are present and correctly named before returning
                # This is a good place for a final schema check if desired, though individual functions aim for it.
                # Example: required_fresco_cols = ['Job Id', 'Host', 'Timestamp', 'Event', 'Value', 'Units']
                # final_result = final_result.select(required_fresco_cols) # If strict schema adherence is needed here

                logger.info(
                    f"Successfully combined data for node {self.cache_dir.name} into final DataFrame with {final_result.height} rows.")
                return final_result
            except Exception as e:
                logger.error(f"Failed to concatenate processed DataFrames for node {self.cache_dir.name}: {e}",
                             exc_info=True)
                return None
        else:
            logger.warning(f"No valid data processed from any metric file in node directory {self.cache_dir}")
            return None