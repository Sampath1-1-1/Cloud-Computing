# controller.py
# Controller node - manages workers, partitioning, and failure detection

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import threading
import time
import requests
from config import (
    CONTROLLER_PORT, NUM_WORKERS, HEARTBEAT_TIMEOUT_MS,
    WORKER_BASE_PORT, REPLICA_COUNT
)
from utils import get_partition_nodes
from logger import logger

app = Flask(__name__)

CORS(app)

# In-memory worker registry: {worker_id: {address, last_heartbeat, alive}}
workers = {}

# Lock for thread-safe access
workers_lock = threading.Lock()

@app.route('/')
def serve_dashboard():
    return send_from_directory('.', 'index.html')

@app.route('/status', methods=['GET'])
def status():
    with workers_lock:
        return jsonify({
            "config": {
                "total_workers": NUM_WORKERS,
                "replica_count": REPLICA_COUNT
            },
            "workers": workers
        })


@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    worker_id = data.get('worker_id')
    address = data.get('address')

    if worker_id is None or address is None:
        return jsonify({"error": "Missing worker_id or address"}), 400

    with workers_lock:
        workers[worker_id] = {
            "address": address,
            "last_heartbeat": time.time(),
            "alive": True
        }

    logger.info(f"✅ Registered Worker {worker_id} at {address}")
    return jsonify({"status": "registered"})


@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.get_json()
    worker_id = data.get('worker_id')

    if worker_id is not None and worker_id in workers:
        with workers_lock:
            workers[worker_id]["last_heartbeat"] = time.time()
            if not workers[worker_id]["alive"]:
                workers[worker_id]["alive"] = True
                logger.info(f"Worker {worker_id} is back ONLINE.")

    return jsonify({"status": "ack"})


@app.route('/partition/<key>', methods=['GET'])
def partition(key):
    node_ids = get_partition_nodes(key)

    primary_address = None

    with workers_lock:
        for nid in node_ids:
            if nid in workers and workers[nid]["alive"]:
                primary_address = workers[nid]["address"]
                break

    if not primary_address:
        return jsonify({"error": "No available workers"}), 503

    return jsonify({
        "primary_address": primary_address,
        "replica_node_ids": node_ids
    })

def trigger_recovery(dead_node_id):
    logger.info(f"🚑 Initiating RECOVERY for dead Worker {dead_node_id}")

    alive_workers = []
    target_address = None

    with workers_lock:
        for wid, info in workers.items():
            if info["alive"]:
                alive_workers.append(wid)
                if target_address is None:
                    target_address = info["address"]

    if not alive_workers or not target_address:
        logger.error("No alive workers to initiate recovery.")
        return

    logger.info(f"Target for reseeding: {target_address}")

    for wid in alive_workers:
        addr = workers[wid]["address"]
        try:
            requests.post(
                f"{addr}/internal/recover",
                json={
                    "dead_node_id": dead_node_id,
                    "target_node_address": target_address
                },
                timeout=5
            )
        except Exception as e:
            logger.warn(f"Recovery request to Worker {wid} failed: {str(e)}")

# -------------------------------
# ✅ Health Monitor Thread
# -------------------------------
def health_monitor():
    while True:
        time.sleep(5)
        now = time.time()
        dead_nodes = []

        with workers_lock:
            for wid, info in workers.items():
                if info["alive"] and (now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT_MS / 1000):
                    logger.warn(f"❌ Worker {wid} missed heartbeats. Marking DEAD.")
                    info["alive"] = False
                    dead_nodes.append(wid)

        for dead_id in dead_nodes:
            threading.Thread(target=trigger_recovery, args=(dead_id,), daemon=True).start()


def start_controller():
    monitor_thread = threading.Thread(target=health_monitor, daemon=True)
    monitor_thread.start()

    logger.info(f"Starting CONTROLLER on port {CONTROLLER_PORT}")
    app.run(host='0.0.0.0', port=CONTROLLER_PORT, debug=False, use_reloader=False)

#main entry point
if __name__ == "__main__":
    start_controller()
