import os
import pandas as pd
import glob


def process_block_file(file_path):
    """Process block.csv file and transform to FRESCO format"""
    df = pd.read_csv(file_path)

    # Calculate value in GB/s (sectors are 512 bytes)
    # Calculate bytes per second from sectors and ticks
    df['Value'] = ((df['rd_sectors'] + df['wr_sectors']) * 512) / (df['rd_ticks'] + df['wr_ticks'])
    # Convert to GB/s
    df['Value'] = df['Value'] / (1024 * 1024 * 1024)

    # Create FRESCO format
    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    result = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'block',
        'Value': df['Value'],
        'Units': 'GB/s',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    return result


def process_cpu_file(file_path):
    """Process cpu.csv file and transform to FRESCO format"""
    df = pd.read_csv(file_path)

    # Calculate CPU usage percentage
    total = df['user'] + df['nice'] + df['system'] + df['idle'] + df['iowait'] + df['irq'] + df['softirq']
    df['Value'] = ((df['user'] + df['nice']) / total) * 100

    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    result = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'cpuuser',
        'Value': df['Value'],
        'Units': 'CPU %',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    return result


def process_nfs_file(file_path):
    """Process nfs.csv file and transform to FRESCO format"""
    df = pd.read_csv(file_path)

    # Calculate NFS throughput in MB/s
    # Using direct read/write columns
    df['Value'] = (df['direct_read'] + df['direct_write']) / 1024 / 1024  # Convert to MB/s

    # Ensure Job Id uses uppercase "JOB" and remove "ID" if present
    df['jobID'] = df['jobID'].str.replace('job', 'JOB', case=False).str.replace('ID', '')

    result = pd.DataFrame({
        'Job Id': df['jobID'],
        'Host': df['node'],
        'Event': 'nfs',
        'Value': df['Value'],
        'Units': 'MB/s',
        'Timestamp': pd.to_datetime(df['timestamp'])
    })

    return result


def process_node_folder(folder_path):
    """Process a single node folder and return combined data"""
    results = []

    # Process block.csv
    block_file = os.path.join(folder_path, 'block.csv')
    if os.path.exists(block_file):
        results.append(process_block_file(block_file))

    # Process cpu.csv
    cpu_file = os.path.join(folder_path, 'cpu.csv')
    if os.path.exists(cpu_file):
        results.append(process_cpu_file(cpu_file))

    # Process nfs.csv
    nfs_file = os.path.join(folder_path, 'nfs.csv')
    if os.path.exists(nfs_file):
        results.append(process_nfs_file(nfs_file))

    # Combine all results
    if results:
        return pd.concat(results, ignore_index=True)
    return None


def main():
    # Base directory containing NODE folders
    base_dir = r"---"

    # Get all NODE folders
    node_folders = glob.glob(os.path.join(base_dir, "NODE*"))

    # Process each node folder
    all_results = []
    for folder in node_folders:
        result = process_node_folder(folder)
        if result is not None:
            all_results.append(result)

    # Combine all results and sort by timestamp
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df = final_df.sort_values(['Timestamp', 'Host', 'Event'])

        # Save to CSV
        output_file = os.path.join(base_dir, "FRESCO_transformed_data.csv")
        final_df.to_csv(output_file, index=False)
        print(f"Transformed data saved to: {output_file}")


if __name__ == "__main__":
    main()