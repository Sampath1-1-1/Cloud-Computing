# worker.py
# Worker node - handles data storage, replication, and recovery

from flask import Flask, request, jsonify
from flask_cors import CORS
import threading
import time
import json
import os
import requests

from config import (
    WORKER_BASE_PORT, CONTROLLER_PORT, HEARTBEAT_INTERVAL_MS,
    NUM_WORKERS, REPLICA_COUNT
)
from utils import get_partition_nodes
from logger import logger

CONTROLLER_URL = f"http://localhost:{CONTROLLER_PORT}"

def start_worker(worker_id: int):
    app = Flask(__name__)
    CORS(app)

    PORT = WORKER_BASE_PORT + worker_id
    STORAGE_FILE = f"storage_worker_{worker_id}.json"

    def load_data():
        if os.path.exists(STORAGE_FILE):
            try:
                with open(STORAGE_FILE, 'r') as f:
                    data = json.load(f)
                    logger.info(f"📂 Loaded {len(data)} keys from disk.")
                    return data
            except Exception as e:
                logger.error(f"Failed to load storage: {str(e)}")
        return {}

    def save_data(data):
        try:
            with open(STORAGE_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save storage: {str(e)}")

    data_store = load_data()

# registration
    def register():
        try:
            requests.post(
                f"{CONTROLLER_URL}/register",
                json={
                    "worker_id": worker_id,
                    "address": f"http://localhost:{PORT}"
                },
                timeout=3
            )
            logger.info(f"✅ Registered with Controller as Worker {worker_id}")
        except Exception:
            logger.error("❌ Could not connect to Controller. Will retry later.")

    register()

    # Heartbeat Loop
    def heartbeat_loop():
        while True:
            try:
                requests.post(
                    f"{CONTROLLER_URL}/heartbeat",
                    json={"worker_id": worker_id},
                    timeout=3
                )
            except:
                pass
            time.sleep(HEARTBEAT_INTERVAL_MS / 1000)

    threading.Thread(target=heartbeat_loop, daemon=True).start()

# routes
    @app.route('/debug/dump', methods=['GET'])
    def debug_dump():
        return jsonify(data_store)

    @app.route('/kv/<key>', methods=['GET'])
    def get_key(key):
        value = data_store.get(key)
        if value is None:
            return jsonify({"found": False}), 404
        return jsonify({"found": True, "value": value, "node_id": worker_id})

    @app.route('/kv/<key>', methods=['PUT'])
    def put_key(key):
        body = request.get_json()
        if not body or "value" not in body:
            return jsonify({"error": "Missing 'value'"}), 400

        value = body["value"]

        data_store[key] = value
        save_data(data_store)

        logger.info(f"💾 Stored key='{key}' locally on Worker {worker_id}")
        target_ids = get_partition_nodes(key)
        replica_ids = [rid for rid in target_ids if rid != worker_id]

        success_count = 1  # Local write

        def replicate_to(rep_id):
            nonlocal success_count
            rep_port = WORKER_BASE_PORT + rep_id
            try:
                requests.put(
                    f"http://localhost:{rep_port}/internal/replicate/{key}",
                    json={"value": value},
                    timeout=3
                )
                success_count += 1
            except Exception as e:
                logger.warn(f"Failed to replicate to Worker {rep_id}: {str(e)}")

        threads = []
        for rep_id in replica_ids:
            t = threading.Thread(target=replicate_to, args=(rep_id,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=3)

        if success_count >= 2:
            return jsonify({"status": "success", "replicas_written": success_count})
        else:
            return jsonify({
                "status": "partial",
                "message": "Quorum not reached",
                "replicas_written": success_count
            }), 500

    @app.route('/internal/replicate/<key>', methods=['PUT'])
    def internal_replicate(key):
        body = request.get_json()
        if not body or "value" not in body:
            return jsonify({"error": "Missing 'value'"}), 400

        value = body["value"]
        data_store[key] = value
        save_data(data_store)

        logger.info(f"🔗 Replicated key='{key}' from peer")
        return jsonify({"status": "ack"})

    @app.route('/internal/recover', methods=['POST'])
    def internal_recover():
        data = request.get_json()
        dead_node_id = data.get("dead_node_id")
        target_address = data.get("target_node_address")

        logger.info(f"🛠️ Recovery: Reseeding keys for dead node {dead_node_id}...")

        reseeded = 0
        for key, value in list(data_store.items()):
            nodes = get_partition_nodes(key)
            if dead_node_id in nodes:
                try:
                    requests.put(
                        f"{target_address}/internal/replicate/{key}",
                        json={"value": value},
                        timeout=5
                    )
                    reseeded += 1
                except Exception as e:
                    logger.warn(f"Failed to reseed key {key}: {str(e)}")

        return jsonify({"status": "recovery_complete", "keys_reseeded": reseeded})


    logger.info(f"🚀 Starting WORKER {worker_id} on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    if len(os.sys.argv) > 1:
        start_worker(int(os.sys.argv[1]))
