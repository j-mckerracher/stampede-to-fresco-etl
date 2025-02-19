from datetime import datetime
from pathlib import Path
import json


class DataVersionManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.version_file = Path(base_dir) / "version_info.json"
        self.load_version_info()

    def load_version_info(self):
        """Load or initialize version tracking data"""
        if self.version_file.exists():
            try:
                with open(self.version_file, 'r') as f:
                    self.version_info = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                # Initialize with default values if file is empty or invalid
                self.version_info = {
                    "current_version": 1,
                    "last_updated": datetime.now().isoformat()
                }
                self.save_version_info()
        else:
            self.version_info = {
                'current_version': 1,
                'uploaded_versions': []
            }
            self.save_version_info()

    def save_version_info(self):
        """Save version tracking data"""
        with open(self.version_file, 'w') as f:
            json.dump(self.version_info, f)

    def get_current_version(self):
        """Get current version number as string (v1, v2, etc)"""
        return f"v{self.version_info['current_version']}"

    def increment_version(self):
        """Increment version number after successful upload"""
        self.version_info['uploaded_versions'].append(self.version_info['current_version'])
        self.version_info['current_version'] += 1
        self.save_version_info()