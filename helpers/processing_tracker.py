import json
from helpers.node_status import NodeStatus
from pathlib import Path
from typing import Dict, Set


class ProcessingTracker:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.tracker_file = Path(base_dir) / "processing_status.json"
        self.batch_sizes_file = Path(base_dir) / "batch_sizes.json"
        self.node_status: Dict[str, NodeStatus] = {}
        self.current_batch = 0
        self.load_status()

    def is_node_processed(self, node_name):
        """Check if node has been processed"""
        return self.node_status.get(node_name) == NodeStatus.COMPLETED

    def mark_node_processed(self, node_name):
        """Mark node as processed"""
        self.update_node_status(node_name, NodeStatus.COMPLETED)

    def mark_node_failed(self, node_name):
        """Mark node as failed"""
        self.update_node_status(node_name, NodeStatus.FAILED)

    def load_status(self):
        """Load or initialize tracking data with migration from old format"""
        try:
            if self.tracker_file.exists():
                with open(self.tracker_file, 'r') as f:
                    data = json.load(f)

                    # Handle old format migration
                    if 'processed_nodes' in data:
                        print("Migrating from old status format...")
                        self.current_batch = data.get('current_batch', 0)

                        # Initialize node statuses
                        self.node_status = {}

                        # Set completed nodes
                        for node in data['processed_nodes']:
                            self.node_status[node] = NodeStatus.COMPLETED

                        # Set failed nodes
                        for node in data.get('failed_nodes', []):
                            self.node_status[node] = NodeStatus.FAILED

                        # The last batch was incomplete - mark those nodes for cleanup
                        last_batch = data['processed_nodes'][-3:]
                        print(f"Last incomplete batch: {last_batch}")
                        for node in last_batch:
                            self.node_status[node] = NodeStatus.PROCESSING

                        # Save in new format
                        self.save_status()
                        print("Migration complete")
                    else:
                        # New format
                        self.node_status = {
                            node: NodeStatus(status) if isinstance(status, str) else status
                            for node, status in data.get('node_status', {}).items()
                        }
                        self.current_batch = data.get('current_batch', 0)
            else:
                self.node_status = {}
                self.current_batch = 0
        except Exception as e:
            print(f"Error loading tracker: {e}")
            self.node_status = {}
            self.current_batch = 0

    def save_status(self):
        """Save current status to file"""
        try:
            data = {
                'node_status': {
                    node: status.value
                    for node, status in self.node_status.items()
                },
                'current_batch': self.current_batch
            }
            with open(self.tracker_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"Error saving tracker: {e}")

    def update_node_status(self, node: str, status: NodeStatus):
        """Update status for a single node"""
        self.node_status[node] = status
        self.save_status()

    def get_incomplete_nodes(self) -> Set[str]:
        """Get set of nodes that need cleanup on restart"""
        return {
            node for node, status in self.node_status.items()
            if status in (NodeStatus.DOWNLOADING, NodeStatus.DOWNLOADED, NodeStatus.PROCESSING)
        }