import os
import pandas as pd


def explore_csv_files(folder_path, num_rows=5):
    """
    Explores CSV files in the given folder, printing column names and sample rows.

    Args:
        folder_path (str): Path to the folder containing CSV files
        num_rows (int): Number of sample rows to display for each file
    """
    # Get all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    print(f"Found {len(csv_files)} CSV files in {folder_path}")
    print("-" * 80)

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        try:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin1')

            print(f"\nFile: {file}")
            print("Columns:")
            print(df.columns.tolist())
            print(f"\nFirst {num_rows} rows:")
            print(df.head(num_rows))
            print("\nShape:", df.shape)
            print("-" * 80)

        except Exception as e:
            print(f"Error reading {file}: {str(e)}")
            print("-" * 80)


folder_path = r"--"
explore_csv_files(folder_path)