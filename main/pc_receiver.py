import base64
import json
import queue
import threading
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# Thread-safe queue to pass data from 'ingest' to the 'stream'
# Increased size to 1000 to handle high-density bursts
data_queue = queue.Queue(maxsize=1000)

def process_batch_async(scanner_id, events):
    """Worker to move events to the stream without blocking the ESP32."""
    for ev in events:
        ev['scanner'] = scanner_id
        try:
            # Non-blocking put to the stream queue
            data_queue.put_nowait(ev)
        except queue.Full:
            pass # Drop if the GUI cannot keep up

@app.route('/api/ble/ingest', methods=['POST'])
def ingest():
    data = request.get_json()
    if not data:
        return "Invalid JSON", 400

    scanner_id = data.get('scanner', 'unknown')
    events = data.get('events', [])

    # CRITICAL: Launch background thread and return 200 OK immediately.
    # This prevents the ESP32 from waiting and dropping packets.
    threading.Thread(target=process_batch_async, args=(scanner_id, events)).start()

    return jsonify({"status": "ack"}), 200

def event_stream():
    """Generator that feeds the SSE stream for ble_popup.py"""
    while True:
        ev = data_queue.get()
        yield f"data: {json.dumps(ev)}\n\n"

@app.route('/api/ble/stream')
def stream():
    """The endpoint ble_popup.py connects to"""
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    # threaded=True is required to handle ingest and stream simultaneously
    app.run(host='0.0.0.0', port=8000, debug=False, threaded=True)