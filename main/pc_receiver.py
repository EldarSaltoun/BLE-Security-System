import base64
import json
import queue
import threading
import socket
from flask import Flask, request, jsonify, Response
from zeroconf import ServiceInfo, Zeroconf  # NEW: For wireless discovery

app = Flask(__name__)

# Thread-safe queue to pass data from 'ingest' to the 'stream'
data_queue = queue.Queue(maxsize=1000)

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

    scanner_id = data.get('scanner', 'unknown')
    events = data.get('events', [])

    # Fast-Ack: Process in background and return immediately
    threading.Thread(target=process_batch_async, args=(scanner_id, events)).start()

    return jsonify({"status": "ack"}), 200

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