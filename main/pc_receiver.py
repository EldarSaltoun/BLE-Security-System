# pc_receiver.py
# Updated: Auto-generates Packet Hash from Raw Data
# Run: uvicorn pc_receiver:app --host 0.0.0.0 --port 8000

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import json
import queue
import time
import zlib  # <--- Added for hashing

app = FastAPI()
event_queue = queue.Queue(maxsize=10000)

def _to_int(v, default=0):
    try:
        return int(v)
    except:
        return default

def _normalize_event(ev: dict, scanner_default="UNKNOWN") -> dict:
    out = {}
    
    # Case-insensitive key lookup
    raw_keys = {k.lower(): v for k, v in ev.items()}
    
    # 1. Identity
    if "device" in raw_keys:
        dev = raw_keys["device"]
        out["mac"] = dev.get("mac", "UNKNOWN").upper()
    else:
        out["mac"] = str(raw_keys.get("mac", raw_keys.get("addr", "UNKNOWN"))).upper()

    # 2. Signal & Location
    out["rssi"] = _to_int(raw_keys.get("rssi", -100))
    out["channel"] = _to_int(raw_keys.get("channel", 37))
    out["scanner"] = str(raw_keys.get("scanner", scanner_default))
    out["timestamp_esp_us"] = _to_int(raw_keys.get("timestamp_esp_us", raw_keys.get("timestamp", 0)))

    # 3. Payload Content
    # Prefer "mfg_data", fallback to "mfg_data_hex"
    mfg = raw_keys.get("mfg_data", "")
    if not mfg:
        mfg = raw_keys.get("mfg_data_hex", "")
    out["mfg_data"] = str(mfg)
    
    # --- FIX: Auto-Generate Packet Hash ---
    # If the ESP32 didn't send a hash, we create one from the unique payload.
    existing_hash = str(raw_keys.get("packet_hash", ""))
    if existing_hash:
        out["packet_hash"] = existing_hash
    elif out["mfg_data"]:
        # Create a short 8-char unique ID based on the payload content
        # This ensures Unit 1 and Unit 2 get the SAME hash for the SAME packet.
        crc = zlib.crc32(out["mfg_data"].encode())
        out["packet_hash"] = f"{crc:08X}"
    else:
        out["packet_hash"] = ""
    # --------------------------------------

    out["name"] = str(raw_keys.get("name", ""))
    out["txpwr"] = _to_int(raw_keys.get("txpwr", 0))
    out["mfg_id"] = _to_int(raw_keys.get("mfg_id", 0))
    out["adv_len"] = _to_int(raw_keys.get("adv_len", 0))
    out["has_services"] = _to_int(raw_keys.get("has_services", 0))
    out["n_services_16"] = _to_int(raw_keys.get("n_services_16", 0))
    out["n_services_128"] = _to_int(raw_keys.get("n_services_128", 0))

    return out

@app.post("/api/ble/ingest")
async def ingest(req: Request):
    try:
        payload = await req.json()
        
        if "events" in payload and isinstance(payload["events"], list):
            scanner_id = payload.get("scanner_id", "UNKNOWN")
            for ev in payload["events"]:
                _push(_normalize_event(ev, scanner_id))
        else:
            scanner_id = payload.get("scanner", "UNKNOWN")
            _push(_normalize_event(payload, scanner_id))

        return JSONResponse({"ok": True})
    except Exception as e:
        print(f"[ERR] Ingest failed: {e}")
        return JSONResponse({"ok": False}, status_code=500)

def _push(ev: dict):
    try:
        if not isinstance(ev, dict) or not ev: return
        ev["pc_rx_time"] = time.time()
        event_queue.put_nowait(ev)
    except queue.Full:
        pass 

@app.get("/api/ble/stream")
def stream():
    def gen():
        while True:
            ev = event_queue.get()
            yield f"data: {json.dumps(ev)}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    # Listening on 0.0.0.0 allows the ESP32s to connect
    uvicorn.run(app, host="0.0.0.0", port=8000)