import polars as pl
import pandas as pd


def convert_timestamp_format(ts_str):
    """Convert timestamp string to standard format."""
    try:
        # Parse timestamp using Polars
        dt = pl.from_pandas(pd.to_datetime(ts_str))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ts_str


def get_host_list_from_metrics(metrics_df, job_id):
    """Extract unique host list for a specific job."""
    hosts = metrics_df.filter(pl.col("Job Id") == job_id).select(
        pl.col("Host").unique()
    ).to_series().to_list()

    return hosts


def create_complete_data_structure(row_data):
    """Create a complete data structure for a row in the final output."""
    # Ensure all required fields are present
    template = {
        "time": None,
        "submit_time": None,
        "start_time": None,
        "end_time": None,
        "timelimit": None,
        "nhosts": None,
        "ncores": None,
        "account": None,
        "queue": None,
        "host": None,
        "jid": None,
        "unit": None,
        "jobname": None,
        "exitcode": None,
        "host_list": None,
        "username": None,
        "value_cpuuser": None,
        "value_gpu": None,
        "value_memused": None,
        "value_memused_minus_diskcache": None,
        "value_nfs": None,
        "value_block": None
    }

    # Update with provided data
    template.update(row_data)
    return template