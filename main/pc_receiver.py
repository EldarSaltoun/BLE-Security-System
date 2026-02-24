import base64
import json
import queue
import threading
import socket
import time
import requests
from flask import Flask, request, jsonify, Response
from zeroconf import ServiceInfo, Zeroconf  # NEW: For wireless discovery

app = Flask(__name__)

# Thread-safe queue to pass data from 'ingest' to the 'stream'
data_queue = queue.Queue(maxsize=1000)

# --- NEW: Track Active Scanners ---
# Dictionary to hold: scanner_id -> {"ip": "192.168.X.X", "last_seen": timestamp}
active_scanners = {}

def start_mdns(ip_address, port):
    """
    Broadcasts this PC's location to the ESP32s wirelessly.
    The ESP32 will look for 'grid-server.local'.
    """
    desc = {'path': '/api/ble/ingest'}
    info = ServiceInfo(
        "_ble-ingest._tcp.local.",
        "Grid-Receiver._ble-ingest._tcp.local.",
        addresses=[socket.inet_aton(ip_address)],
        port=port,
        properties=desc,
        server="grid-server.local.",
    )
    zc = Zeroconf()
    zc.register_service(info)
    print(f"[mDNS] Broadcaster active: grid-server.local at {ip_address}")
    return zc

def process_batch_async(scanner_id, events):
    """Worker to move events to the stream without blocking the ESP32."""
    for ev in events:
        ev['scanner'] = scanner_id
        try:
            data_queue.put_nowait(ev)
        except queue.Full:
            pass 

@app.route('/api/ble/ingest', methods=['POST'])
def ingest():
    data = request.get_json()
    if not data:
        return "Invalid JSON", 400

    scanner_id = str(data.get('scanner', 'unknown'))
    client_ip = request.remote_addr # Capture the ESP32's IP!
    
    # Update our tracker
    active_scanners[scanner_id] = {
        "ip": client_ip,
        "last_seen": time.time()
    }

    events = data.get('events', [])

    # Fast-Ack: Process in background and return immediately
    threading.Thread(target=process_batch_async, args=(scanner_id, events)).start()

    return jsonify({"status": "ack"}), 200

# --- NEW: Control API Endpoints ---
@app.route('/api/control/scanners', methods=['GET'])
def get_scanners():
    """Returns a list of currently active scanners and their IPs"""
    # Filter out scanners that haven't been seen in 30 seconds
    current_time = time.time()
    alive = {k: v for k, v in active_scanners.items() if current_time - v['last_seen'] < 30}
    return jsonify(alive)

@app.route('/api/control/send', methods=['POST'])
def send_command():
    """Proxies a command to a specific ESP32 or all of them"""
    data = request.get_json()
    target = str(data.get('target')) # "all" or specific scanner_id
    state = data.get('state')   # 1 or 0 (optional)
    mode = data.get('mode')     # 0, 37, 38, 39 (optional)
    
    params = []
    if state is not None: params.append(f"state={state}")
    if mode is not None: params.append(f"mode={mode}")
    query_string = "&".join(params)

    targets = list(active_scanners.keys()) if target == "all" else [target]
    
    results = {}
    for t_id in targets:
        if t_id in active_scanners:
            ip = active_scanners[t_id]['ip']
            url = f"http://{ip}/cmd?{query_string}"
            try:
                # Fire and forget with a short timeout
                requests.get(url, timeout=2)
                results[t_id] = "Success"
            except Exception as e:
                results[t_id] = f"Failed: {str(e)}"
        else:
            results[t_id] = "Not found"
            
    return jsonify(results)

def event_stream():
    """Generator that feeds the SSE stream for ble_popup.py"""
    while True:
        ev = data_queue.get()
        yield f"data: {json.dumps(ev)}\n\n"

@app.route('/api/ble/stream')
def stream():
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    # 1. Automatically detect current local IP
    hostname = socket.gethostname()
    my_ip = socket.gethostbyname(hostname)
    print(f"[INFO] Starting Receiver on {my_ip}:8000")

    # 2. Start the wireless broadcaster
    # This is what the ESP32 listens for
    zc_instance = start_mdns(my_ip, 8000)
    
    try:
        # 3. Run Flask
        app.run(host='0.0.0.0', port=8000, debug=False, threaded=True)
    finally:
        # Clean up mDNS on exit
        print("[INFO] Shutting down mDNS...")
        zc_instance.unregister_all_services()
        zc_instance.close()