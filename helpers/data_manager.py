import threading
from typing import Dict, Set
import polars as pl
from pathlib import Path


class DataManager:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.monthly_data_lock = threading.Lock()
        self.monthly_data: Dict[str, pl.DataFrame] = {}
        self.load_checkpoint()

    def load_checkpoint(self):
        """Load monthly data from checkpoint"""
        checkpoint_dir = Path(self.base_dir) / "monthly_data_checkpoint"
        if checkpoint_dir.exists():
            try:
                files = list(checkpoint_dir.glob("*.parquet"))
                print(f"Found {len(files)} checkpoint files")
                for i, file in enumerate(files, 1):
                    month = file.stem
                    print(f"Loading checkpoint {i}/{len(files)}: {month}")
                    self.monthly_data[month] = pl.read_parquet(file)
                    print(f"Loaded {len(self.monthly_data[month])} rows for {month}")
                print("All checkpoints loaded")
            except Exception as e:
                print(f"Error loading checkpoint: {e}")

    def save_checkpoint(self):
        """Save monthly data checkpoint"""
        checkpoint_dir = Path(self.base_dir) / "monthly_data_checkpoint"
        checkpoint_dir.mkdir(exist_ok=True)

        try:
            with self.monthly_data_lock:
                for month, df in self.monthly_data.items():
                    file_path = checkpoint_dir / f"{month}.parquet"
                    df.write_parquet(file_path)
                print("Saved data checkpoint")
        except Exception as e:
            print(f"Error saving checkpoint: {e}")

    def cleanup_incomplete_nodes(self, nodes: Set[str]):
        """Remove data from incomplete nodes"""
        print(f"Starting cleanup of nodes: {nodes}")
        with self.monthly_data_lock:
            total_months = len(self.monthly_data)
            for i, (month, df) in enumerate(self.monthly_data.items(), 1):
                print(f"Cleaning month {month} ({i}/{total_months})")
                print(f"Before cleanup: {len(df)} rows")
                rows_with_nodes = df.filter(pl.col("Host").is_in(nodes))
                print(f"Found {len(rows_with_nodes)} rows to remove")
                self.monthly_data[month] = df.filter(~pl.col("Host").is_in(nodes))
                print(f"After cleanup: {len(self.monthly_data[month])} rows")
            print("Saving checkpoint...")
            self.save_checkpoint()
            print("Cleanup complete!")

    def update_monthly_data(self, new_data: Dict[str, pl.DataFrame]):
        """Thread-safe update of monthly data"""
        with self.monthly_data_lock:
            for month, new_df in new_data.items():
                if month in self.monthly_data:
                    # Polars efficient concat and distinct
                    self.monthly_data[month] = pl.concat([
                        self.monthly_data[month],
                        new_df
                    ]).unique()
                else:
                    self.monthly_data[month] = new_df
            self.save_checkpoint()