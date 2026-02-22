import base64
import json
import queue
from flask import Flask, request, jsonify, Response
from ble_adv_parser import AdvParser

app = Flask(__name__)

# A thread-safe queue to pass data from 'ingest' to the 'stream'
data_queue = queue.Queue(maxsize=100)

@app.route('/api/ble/ingest', methods=['POST'])
def ingest():
    data = request.get_json()
    if not data:
        return "Invalid JSON", 400

    scanner_id = data.get('scanner', 'unknown')
    events = data.get('events', [])

    for ev in events:
        # 1. Decode payload
        try:
            # We just pass the raw event down the stream to the popup
            # The popup's reader_thread (the one we updated earlier) 
            # will handle the AdvParser.parse() part locally.
            
            # Add scanner ID to the individual event
            ev['scanner'] = scanner_id
            
            # Put it in the queue for the stream (non-blocking)
            try:
                data_queue.put_nowait(ev)
            except queue.Full:
                pass # Drop if popup is too slow
                
        except Exception as e:
            print(f"Error queuing event: {e}")

    return jsonify({"status": "ok", "count": len(events)}), 200

def event_stream():
    """Generator that feeds the SSE stream for ble_popup.py"""
    while True:
        # Get data from the queue
        ev = data_queue.get()
        # Format as SSE (data: <json>\n\n)
        yield f"data: {json.dumps(ev)}\n\n"

@app.route('/api/ble/stream')
def stream():
    """The endpoint ble_popup.py connects to"""
    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    # Use threaded=True so ingest and stream can run at the same time
    app.run(host='0.0.0.0', port=8000, debug=False, threaded=True)