# utils.py
# Shared utility functions for partitioning and hashing

import hashlib
from config import REPLICA_COUNT, NUM_WORKERS

def string_hash(key: str) -> int:
    """Deterministic hash function matching the JavaScript version"""
    hash_val = 0
    for char in key:
        hash_val = ((hash_val << 5) - hash_val) + ord(char)
        hash_val = hash_val & 0xFFFFFFFF  # Keep it 32-bit
    return abs(hash_val)

def get_partition_nodes(key: str, total_workers: int = NUM_WORKERS) -> list[int]:
    """
    Returns ordered list of worker IDs responsible for a key.
    Primary first, followed by next (REPLICA_COUNT - 1) nodes in ring.
    """
    if total_workers == 0:
        return []
    
    key_hash = string_hash(key)
    primary_id = key_hash % total_workers
    
    nodes = []
    for i in range(REPLICA_COUNT):
        node_id = (primary_id + i) % total_workers
        nodes.append(node_id)
    
    return nodes

# Optional: For future auto-restart (not implemented in Python version yet)
# def make_restart_command(script_path: str, worker_id: int) -> str:
#     import platform
#     system = platform.system()
#     if system == "Windows":
#         return f'start "Worker {worker_id}" cmd /k python {script_path} worker {worker_id}'
#     # Add macOS/Linux later if needed