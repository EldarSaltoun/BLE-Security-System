# pc_receiver.py
#
# Minimal HTTP bridge for ESP32 BLE scanner → ble_popup.py
#
# Run:
#   pip install fastapi uvicorn
#   uvicorn pc_receiver:app --host 0.0.0.0 --port 8000

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import json
import queue
import time


def _to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _normalize_event(ev: dict, scanner_fallback: str = "UNKNOWN") -> dict:
    """Normalize/validate fields so clients can assume sane defaults.

    - Ensures new service-indicator keys exist (with defaults).
    - Ensures numeric fields are integers.
    - Accepts either `timestamp_esp_us` or legacy `timestamp`.
    """
    if not isinstance(ev, dict):
        return {}

    out = dict(ev)

    # Required-ish identity/context
    out["mac"] = str(out.get("mac", "")).upper()
    out["scanner"] = str(out.get("scanner", scanner_fallback) or scanner_fallback)

    # Timestamps
    if "timestamp_esp_us" not in out and "timestamp" in out:
        out["timestamp_esp_us"] = out.get("timestamp")
    out["timestamp_esp_us"] = _to_int(out.get("timestamp_esp_us", 0), 0)

    # RF/payload basics
    out["rssi"] = _to_int(out.get("rssi", 0), 0)
    out["txpwr"] = _to_int(out.get("txpwr", 0), 0)
    out["adv_len"] = _to_int(out.get("adv_len", 0), 0)
    out["mfg_id"] = _to_int(out.get("mfg_id", 0), 0)

    # NEW: service indicators (counts only)
    out["n_services_16"] = _to_int(out.get("n_services_16", 0), 0)
    out["n_services_128"] = _to_int(out.get("n_services_128", 0), 0)

    # NEW: Raw Manufacturer Data (Hex String)
    # Defaults to empty string if missing
    out["mfg_data"] = str(out.get("mfg_data", ""))

    # has_services may be absent or boolean; normalize to 0/1
    hs = out.get("has_services", None)
    if hs is None:
        hs = 1 if (out["n_services_16"] + out["n_services_128"]) > 0 else 0
    out["has_services"] = 1 if bool(hs) else 0

    return out


app = FastAPI()
event_queue = queue.Queue(maxsize=10000)


@app.post("/api/ble/ingest")
async def ingest(req: Request):
    payload = await req.json()

    # Batch mode: { "scanner_id": "...", "events": [...] }
    if "events" in payload and isinstance(payload["events"], list):
        scanner_id = payload.get("scanner_id", "UNKNOWN")
        for ev in payload["events"]:
            if isinstance(ev, dict):
                ev.setdefault("scanner", scanner_id)
            _push(_normalize_event(ev, scanner_id))
    else:
        _push(
            _normalize_event(
                payload,
                payload.get("scanner", "UNKNOWN") if isinstance(payload, dict) else "UNKNOWN",
            )
        )

    return JSONResponse({"ok": True})


def _push(ev: dict):
    try:
        if not isinstance(ev, dict) or not ev:
            return
        ev["pc_rx_time"] = time.time()
        event_queue.put_nowait(ev)
    except queue.Full:
        pass  # drop if overloaded


@app.get("/api/ble/stream")
def stream():
    def gen():
        while True:
            ev = event_queue.get()
            yield f"data: {json.dumps(ev)}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")