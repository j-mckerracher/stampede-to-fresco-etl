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
    """Process node data files using Polars"""
    def __init__(self):
        self.cache_dir = Path('cache')
        self.cache_dir.mkdir(exist_ok=True, parents=True)

    def process_block_file(self, file_path: Path) -> pl.DataFrame:
        logger.info(f"Processing block file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Calculate throughput
            df = df.with_columns([
                pl.col('rd_sectors').cast(pl.Float64).alias('rd_sectors_num'),
                pl.col('wr_sectors').cast(pl.Float64).alias('wr_sectors_num'),
                pl.col('rd_ticks').cast(pl.Float64).alias('rd_ticks_num'),
                pl.col('wr_ticks').cast(pl.Float64).alias('wr_ticks_num')
            ])

            df = df.with_columns([
                (pl.col('rd_sectors_num') + pl.col('wr_sectors_num')).alias('total_sectors'),
                ((pl.col('rd_ticks_num') + pl.col('wr_ticks_num')) / 1000).alias('total_ticks')
            ])

            # Convert to GB/s
            df = df.with_columns([
                pl.when(pl.col('total_ticks') > 0)
                .then((pl.col('total_sectors') * 512) / pl.col('total_ticks') / (1024 ** 3))
                .otherwise(0)
                .alias('Value')
            ])

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
        logger.info(f"Processing CPU file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            df = df.with_columns([
                (pl.col('user') + pl.col('nice') + pl.col('system') +
                 pl.col('idle') + pl.col('iowait') + pl.col('irq') +
                 pl.col('softirq')).alias('total_ticks')
            ])

            df = df.with_columns([
                pl.when(pl.col('total_ticks') > 0)
                .then(((pl.col('user') + pl.col('nice')) / pl.col('total_ticks')) * 100)
                .otherwise(0)
                .clip(0, 100)
                .alias('Value')
            ])

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
        logger.info(f"Processing NFS file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            df = df.with_columns([
                ((pl.col('READ_bytes_recv') + pl.col('WRITE_bytes_sent')) / (1024 * 1024)).alias('Value'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp')
            ])

            # Calculate time differences
            df = df.with_columns([
                pl.col('Timestamp').diff().cast(pl.Duration).dt.total_seconds().fill_null(600).alias('TimeDiff')
            ])

            df = df.with_columns([
                (pl.col('Value') / pl.col('TimeDiff')).alias('Value')
            ])

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
        logger.info(f"Processing memory file: {file_path}")
        try:
            df = pl.read_csv(file_path)

            # Convert KB to bytes
            memory_cols = ['MemTotal', 'MemFree', 'MemUsed', 'FilePages']
            df = df.with_columns([
                pl.col(col).mul(1024) for col in memory_cols if col in df.columns
            ])

            # Calculate metrics in GB
            df = df.with_columns([
                (pl.col('MemUsed') / (1024 ** 3)).alias('memused'),
                ((pl.col('MemUsed') - pl.col('FilePages')) / (1024 ** 3))
                .clip(0, None)
                .alias('memused_minus_diskcache')
            ])

            memused_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('memused').alias('Event'),
                pl.col('memused').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            memused_nocache_df = df.select([
                pl.col('jobID').str.replace_all('job', 'JOB', literal=True).alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.col('timestamp').str.strptime(pl.Datetime, "%m/%d/%Y %H:%M:%S").alias('Timestamp'),
                pl.lit('memused_minus_diskcache').alias('Event'),
                pl.col('memused_minus_diskcache').alias('Value'),
                pl.lit('GB').alias('Units')
            ])

            return [memused_df, memused_nocache_df]
        except Exception as e:
            logger.error(f"Error processing memory file {file_path}: {e}")
            return None

    def process_node_data(self) -> pl.DataFrame:
        """Process all files for a node and return combined dataframe"""
        logger.info(f"Processing all files for node in directory: {self.cache_dir}")

        dfs = []
        try:
            # Process block file
            block_path = self.cache_dir / 'block.csv'
            if block_path.exists() and block_path.stat().st_size > 0:
                block_df = self.process_block_file(block_path)
                if isinstance(block_df, pl.DataFrame) and len(block_df) > 0:
                    dfs.append(block_df)
                # Delete file after processing
                os.remove(block_path)

            # Process CPU file
            cpu_path = self.cache_dir / 'cpu.csv'
            if cpu_path.exists() and cpu_path.stat().st_size > 0:
                cpu_df = self.process_cpu_file(cpu_path)
                if isinstance(cpu_df, pl.DataFrame) and len(cpu_df) > 0:
                    dfs.append(cpu_df)
                # Delete file after processing
                os.remove(cpu_path)

            # Process NFS file
            nfs_path = self.cache_dir / 'nfs.csv'
            if nfs_path.exists() and nfs_path.stat().st_size > 0:
                nfs_df = self.process_nfs_file(nfs_path)
                if isinstance(nfs_df, pl.DataFrame) and len(nfs_df) > 0:
                    dfs.append(nfs_df)
                # Delete file after processing
                os.remove(nfs_path)

            # Process memory file
            mem_path = self.cache_dir / 'mem.csv'
            if mem_path.exists() and mem_path.stat().st_size > 0:
                memory_dfs = self.process_memory_metrics(mem_path)
                if memory_dfs is not None and len(memory_dfs) > 0:
                    dfs.extend(memory_dfs)
                # Delete file after processing
                os.remove(mem_path)

            # Combine all dataframes
            if dfs:
                # Use thread pool to process dataframes in parallel
                with ThreadPoolExecutor(max_workers=max(1, multiprocessing.cpu_count() - 1)) as executor:
                    # No actual parallel work here, but setting up the structure
                    # for potential future parallelization of data transformations
                    futures = [executor.submit(lambda df=df: df) for df in dfs]
                    processed_dfs = [future.result() for future in futures]

                result = pl.concat(processed_dfs)
                logger.info(f"Successfully processed node data with {len(result)} rows")
                return result
            else:
                logger.warning(f"No valid data processed for node in {self.cache_dir}")
                return pl.DataFrame()

        except Exception as e:
            logger.error(f"Error processing node data in {self.cache_dir}: {e}")
            return pl.DataFrame()