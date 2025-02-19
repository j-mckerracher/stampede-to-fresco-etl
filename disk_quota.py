from helpers.utilities import get_user_disk_usage


class DiskQuotaManager:
    def __init__(self, quota_mb=24512):
        self.quota_mb = quota_mb
        self.warning_threshold = 0.7 * quota_mb
        self.critical_threshold = 0.85 * quota_mb  # More conservative buffer

    def check_space(self):
        used_mb, _, _ = get_user_disk_usage()
        if used_mb > self.critical_threshold:
            return "CRITICAL"
        elif used_mb > self.warning_threshold:
            return "WARNING"
        return "OK"