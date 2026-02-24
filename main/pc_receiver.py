import base64
import json
import queue
import threading
import socket
import time
import requests
import csv  
import logging
import os  # Included to check file existence for headers
from flask import Flask, request, jsonify, Response
from zeroconf import ServiceInfo, Zeroconf  

# Silence Flask Logs for a cleaner terminal
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)
data_queue = queue.Queue(maxsize=1000)
active_scanners = {}

# Calibration settings
CALIBRATION_TARGET = "eldarcalib" 
SAMPLES_PER_CHANNEL = 10
calib_state = {
    "active": False,
    "target_mac": None,
    "coords": {"x": 0.0, "y": 0.0, "z": 0.0},
    "buckets": {}
}

def start_mdns(ip_address, port):
    """Broadcasts PC location for ESP32 discovery."""
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

def print_calibration_progress():
    """Prints a live tracker of the calibration progress in the terminal."""
    print("\n--- Calibration Progress ---")
    # Added header for terminal visibility
    print(f"{'Scanner':<10} | {'Ch 37':<7} | {'Ch 38':<7} | {'Ch 39':<7}")
    print("-" * 40)
    for s_id, channels in calib_state["buckets"].items():
        c37 = len(channels.get(37, []))
        c38 = len(channels.get(38, []))
        c39 = len(channels.get(39, []))
        print(f"ID {s_id:<7} | {c37:2}/{SAMPLES_PER_CHANNEL} | {c38:2}/{SAMPLES_PER_CHANNEL} | {c39:2}/{SAMPLES_PER_CHANNEL}")
    print("----------------------------")

def decode_name_from_payload(payload_b64):
    """Extracts the BLE device name from a base64-encoded raw payload."""
    try:
        data = base64.b64decode(payload_b64)
        i = 0
        while i < len(data):
            length = data[i]
            if length == 0: break
            ad_type = data[i+1]
            if ad_type in [0x08, 0x09]: # Shortened or Complete Local Name
                return data[i+2:i+1+length].decode('utf-8', errors='ignore')
            i += length + 1
    except: pass
    return ""

def process_batch_async(scanner_id, events):
    """Processes batches of BLE events and handles calibration locking."""
    for ev in events:
        ev['scanner'] = scanner_id
        
        if calib_state["active"]:
            # Support both condensed keys (a, r, c, p) and verbose keys
            mac = str(ev.get('a', ev.get('mac', ''))).upper().strip()
            rssi = ev.get('r', ev.get('rssi'))
            channel = ev.get('c', ev.get('channel'))
            payload = ev.get('p', ev.get('payload', ''))
            
            # Attempt to find name in dedicated field or raw payload
            name = str(ev.get('n', ev.get('name', ''))).lower()
            if not name or name == "unknown":
                name = decode_name_from_payload(payload).lower()

            # Trigger lock if target name is found
            if CALIBRATION_TARGET in name and not calib_state["target_mac"]:
                calib_state["target_mac"] = mac
                print(f"[*] SUCCESS: Locked via Name Match: {mac} ('{name}')")

            # Collect data if MAC is locked
            if calib_state["target_mac"] and mac == calib_state["target_mac"]:
                if scanner_id not in calib_state["buckets"]:
                    calib_state["buckets"][scanner_id] = {37: [], 38: [], 39: []}
                
                bucket = calib_state["buckets"][scanner_id].get(channel, [])
                if len(bucket) < SAMPLES_PER_CHANNEL:
                    bucket.append(rssi)
                    calib_state["buckets"][scanner_id][channel] = bucket
                    print_calibration_progress()
                    check_calib_completion()

        try:
            data_queue.put_nowait(ev)
        except queue.Full: pass 

@app.route('/api/ble/ingest', methods=['POST'])
def ingest():
    data = request.get_json()
    if not data: return "Invalid JSON", 400
    
    scanner_id = str(data.get('scanner', 'unknown'))
    active_scanners[scanner_id] = {"ip": request.remote_addr, "last_seen": time.time()}
    events = data.get('events', [])

    threading.Thread(target=process_batch_async, args=(scanner_id, events)).start()
    return jsonify({"status": "ack"}), 200

@app.route('/api/control/scanners', methods=['GET'])
def get_scanners():
    alive = {k: v for k, v in active_scanners.items() if time.time() - v['last_seen'] < 30}
    return jsonify(alive)

@app.route('/api/control/send', methods=['POST'])
def send_command():
    data = request.get_json()
    target, state, mode = str(data.get('target')), data.get('state'), data.get('mode')
    params = []
    if state is not None: params.append(f"state={state}")
    if mode is not None: params.append(f"mode={mode}")
    query = "&".join(params)
    targets = list(active_scanners.keys()) if target == "all" else [target]
    results = {}
    for t_id in targets:
        if t_id in active_scanners:
            try:
                requests.get(f"http://{active_scanners[t_id]['ip']}/cmd?{query}", timeout=2)
                results[t_id] = "Success"
            except Exception as e: results[t_id] = str(e)
    return jsonify(results)

def check_calib_completion():
    scanners_to_check = [s for s, d in active_scanners.items() if time.time() - d['last_seen'] < 30]
    if not scanners_to_check: return 
    for s_id in scanners_to_check:
        if s_id not in calib_state["buckets"]: return
        for ch in [37, 38, 39]:
            if len(calib_state["buckets"][s_id].get(ch, [])) < SAMPLES_PER_CHANNEL: return
    
    if calib_state["active"]: save_calibration_results()

def save_calibration_results():
    log_file = 'calibration_log.csv'
    # Check if headers need to be written
    write_header = not os.path.exists(log_file)
    
    with open(log_file, 'a', newline='') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(['X_Coord', 'Y_Coord', 'Z_Coord', 'Scanner_ID', 'Avg_RSSI_Ch37', 'Avg_RSSI_Ch38', 'Avg_RSSI_Ch39'])
            
        for s_id, channels in calib_state["buckets"].items():
            avgs = [sum(channels[ch])/len(channels[ch]) if channels[ch] else 0 for ch in [37, 38, 39]]
            writer.writerow([calib_state["coords"]["x"], calib_state["coords"]["y"], calib_state["coords"]["z"], s_id] + avgs)
            
    print("\n[!] Point Saved. System IDLE.")
    calib_state["active"] = False
    requests.post("http://localhost:8000/api/control/send", json={"target": "all", "state": 0})

@app.route('/api/calibrate/start', methods=['POST'])
def start_calib():
    data = request.json
    calib_state.update({
        "active": True, 
        "target_mac": data.get("manual_mac"), 
        "coords": data.get("coords"), 
        "buckets": {}
    })
    print(f"\n[*] Calibration started for: {calib_state['coords']}")
    if calib_state["target_mac"]:
        print(f"[*] Manual lock engaged for MAC: {calib_state['target_mac']}")
    return jsonify({"status": "Started"}), 200

@app.route('/api/ble/stream')
def stream():
    def event_stream():
        while True: yield f"data: {json.dumps(data_queue.get())}\n\n"
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    my_ip = socket.gethostbyname(socket.gethostname())
    zc_instance = start_mdns(my_ip, 8000)
    try: app.run(host='0.0.0.0', port=8000, threaded=True)
    finally: zc_instance.unregister_all_services(); zc_instance.close()