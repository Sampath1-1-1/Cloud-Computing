# kv_store.py
# Main entry point for the Distributed Key-Value Store
# Usage:
#   python kv_store.py controller
#   python kv_store.py worker <id>   (e.g., worker 0)

import sys
import os
from controller import start_controller
from worker import start_worker
from logger import logger

def print_banner():
    print("---------------------------------------------------")
    print("   Distributed Key-Value Store (Python Version)   ")
    print("---------------------------------------------------")

if __name__ == "__main__":
    print_banner()

    if len(sys.argv) < 2:
        logger.error("Usage: python kv_store.py [controller | worker <id>]")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "controller":
        logger.info("Starting in CONTROLLER mode")
        start_controller()
    elif mode == "worker":
        if len(sys.argv) < 3:
            logger.error("Worker mode requires an ID: python kv_store.py worker <id>")
            sys.exit(1)
        try:
            worker_id = int(sys.argv[2])
            if worker_id < 0 or worker_id >= 4:
                logger.warn(f"Worker ID {worker_id} is outside typical range (0-3), but continuing...")
            logger.info(f"Starting in WORKER mode with ID {worker_id}")
            start_worker(worker_id)
        except ValueError:
            logger.error("Worker ID must be an integer")
            sys.exit(1)
    else:
        logger.error("Invalid mode. Use 'controller' or 'worker <id>'")
        sys.exit(1)