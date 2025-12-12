# config.py
# Central configuration for the Distributed Key-Value Store

CONTROLLER_PORT = 5000
WORKER_BASE_PORT = 5001
NUM_WORKERS = 4
REPLICA_COUNT = 4          # 1 Primary + 3 Replicas (total 4 copies)
HEARTBEAT_INTERVAL_MS = 3000       # How often workers send heartbeats
HEARTBEAT_TIMEOUT_MS = 8000        # Timeout to mark worker as dead
ENTRY_POINT_SCRIPT = "kv_store.py" # Main script for restarting workers (if needed)

# Persistence
STORAGE_DIR = "."                  # Current directory for storage files