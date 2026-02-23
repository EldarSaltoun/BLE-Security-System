import argparse
import csv
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
import base64
import zlib
from ble_adv_parser import AdvParser

import requests
import tkinter as tk
from tkinter import ttk

# ---------------- Manufacturer DB ----------------
MFG_IDS = {}

def load_mfg_ids(filename="mfg_ids.csv"):
    global MFG_IDS
    try:
        with open(filename, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2: continue
                try:
                    cid = int(row[0], 16)
                    name = row[1].strip()
                    MFG_IDS[cid] = name
                except ValueError: continue
        print(f"[INFO] Loaded {len(MFG_IDS)} manufacturer IDs")
    except FileNotFoundError:
        print(f"[WARN] Manufacturer ID file '{filename}' not found")

def resolve_mfg_name(mfg) -> str:
    try:
        mfg_val = int(mfg)
        if mfg_val == 0: return "(none)"
        return MFG_IDS.get(mfg_val, f"Unknown(0x{mfg_val:04X})")
    except Exception: return str(mfg)

# ---------------- Data Model ----------------
class DeviceModel:
    def __init__(self, presence_window_s=5, min_rssi=None, csv_log="ble_log.csv"):
        self.presence_window_s = presence_window_s
        self.min_rssi = min_rssi
        self.devices = {}
        self.events = [] 
        self.csv_log = csv_log

        if not os.path.exists(self.csv_log):
            with open(self.csv_log, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp_local", "mac", "name", "rssi", "channel", "txpwr",
                    "mfg", "adv_len", "adv_int_ms", "has_services", 
                    "n_services_16", "n_services_128", "mfg_data", 
                    "packet_hash", "scanner", "timestamp_epoch_us"
                ])

    def prune_stale(self):
        cutoff = time.monotonic() - self.presence_window_s
        stale = [mac for mac, d in self.devices.items() if d["last_seen_mono"] < cutoff]
        for mac in stale:
            del self.devices[mac]

    def ingest(self, mac, rssi, channel, name, txpwr, mfg_id, adv_len,
               has_services, n_services_16, n_services_128, mfg_data,
               packet_hash, ts_epoch_us, ts_mono_us, scanner):
        
        mfg_resolved = resolve_mfg_name(mfg_id)
        now_mono = time.monotonic()
        now_dt = datetime.now()
        now_str = now_dt.strftime("%H:%M:%S")
        now_iso = now_dt.isoformat(timespec="milliseconds")

        entry = self.devices.get(mac)
        prev_mono = int(entry.get("last_mono_us", 0)) if entry else 0
        
        adv_int_ms = None
        if prev_mono > 0 and ts_mono_us > 0:
            dt_ms = (ts_mono_us - prev_mono) / 1000.0
            if 10 < dt_ms <= 10240: adv_int_ms = round(dt_ms, 1)

        self.devices[mac] = {
            "name": name.strip(), "rssi": int(rssi), "channel": int(channel), 
            "txpwr": int(txpwr), "mfg": mfg_resolved, "adv_len": int(adv_len),
            "has_services": int(has_services), "n_services_16": int(n_services_16), 
            "n_services_128": int(n_services_128), "mfg_data": mfg_data, 
            "packet_hash": packet_hash, "last_seen_mono": now_mono,
            "last_seen_str": now_str, "last_ts": int(ts_epoch_us),
            "last_mono_us": int(ts_mono_us), "adv_int": adv_int_ms, "scanner": scanner
        }

        self.events.append({"mac": mac, "rssi": int(rssi), "channel": int(channel), "scanner": scanner, "ts": int(ts_epoch_us)})

        with open(self.csv_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now_iso, mac, name.strip(), rssi, channel, txpwr, mfg_resolved, 
                adv_len, adv_int_ms or "", has_services, n_services_16, 
                n_services_128, mfg_data, packet_hash, scanner, int(ts_epoch_us)
            ])

    def export_json(self, output_path: Path, session_meta: dict):
        payload = {"meta": session_meta, "events": self.events}
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

# ---------------- App ----------------
class BLEPopupApp:
    def __init__(self, stream_url, json_out):
        self.stream_url = stream_url
        self.json_out = json_out
        self.start_iso = datetime.now().isoformat(timespec="seconds")
        self.model = DeviceModel()

        self.root = tk.Tk()
        self.root.title("BLE Live Presence — ESP32 Grid Scanner")

        top = ttk.Frame(self.root, padding=8)
        top.pack(fill="x")
        self.lbl_present = ttk.Label(top, text="Present devices: 0", font=("Segoe UI", 12, "bold"))
        self.lbl_present.pack(side="left")

        cols = ("mac", "name", "rssi", "ch", "txpwr", "mfg", "adv_len", "adv_int", 
                "has_services", "n_services_16", "n_services_128", "mfg_data", "hash", "scanner", "last_seen")
        self.tree = ttk.Treeview(self.root, columns=cols, show="headings", height=18)
        
        widths = (160, 140, 50, 40, 50, 180, 60, 70, 80, 80, 80, 100, 80, 100, 100)
        for c, w in zip(cols, widths):
            self.tree.heading(c, text=c.upper())
            self.tree.column(c, width=w, anchor="w")
        self.tree.pack(fill="both", expand=True, padx=8, pady=8)

        self.stop_flag = threading.Event()
        threading.Thread(target=self.reader_thread, daemon=True).start()
        self.root.after(300, self.refresh_ui)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def reader_thread(self):
        try:
            with requests.get(self.stream_url, stream=True, timeout=15) as r:
                for line in r.iter_lines(decode_unicode=True):
                    if not line or not line.startswith("data: "): continue
                    ev = json.loads(line[6:])
                    try:
                        raw_payload = base64.b64decode(ev["p"])
                        p_hash = f"{zlib.crc32(raw_payload) & 0xFFFFFFFF:08X}"
                        parsed = AdvParser.parse(raw_payload)

                        self.model.ingest(
                            mac=ev["a"].upper(), rssi=ev["r"], channel=ev.get("c", 0),
                            name=parsed["name"], txpwr=parsed["tx_pwr"] or 0,
                            mfg_id=parsed["mfg_id"] or 0, adv_len=len(raw_payload),
                            has_services=1 if (parsed["services_16"] or parsed["services_128"]) else 0,
                            n_services_16=len(parsed["services_16"]), n_services_128=len(parsed["services_128"]),
                            mfg_data=parsed["mfg_data_hex"], packet_hash=p_hash,
                            ts_epoch_us=ev.get("timestamp_epoch_us", ev["ts"]), 
                            ts_mono_us=ev.get("timestamp_mono_us", ev["ts"]), 
                            scanner=ev.get("scanner", "UNK")
                        )
                    except Exception: continue
        except Exception as e: print(f"Stream error: {e}")

    def refresh_ui(self):
        self.model.prune_stale()
        self.lbl_present.config(text=f"Present devices: {len(self.model.devices)}")
        current_iids = set(self.tree.get_children())
        for mac, d in list(self.model.devices.items()):
            vals = (mac, d["name"], d["rssi"], d["channel"] or "N/A", d["txpwr"], d["mfg"], d["adv_len"], 
                    f"{d['adv_int']} ms" if d["adv_int"] else "-", d["has_services"], 
                    d["n_services_16"], d["n_services_128"], d["mfg_data"], d["packet_hash"], 
                    d["scanner"], d["last_seen_str"])
            if mac in current_iids: self.tree.item(mac, values=vals)
            else: self.tree.insert("", "end", iid=mac, values=vals)
        for iid in current_iids - set(self.model.devices.keys()): self.tree.delete(iid)
        self.root.after(300, self.refresh_ui)

    def on_close(self):
        print(f"[INFO] Closing application and saving session...")
        self.stop_flag.set()
        if self.json_out:
            meta = {"start": self.start_iso, "devices": len(self.model.devices)}
            try: 
                self.model.export_json(Path(self.json_out), meta)
                print(f"[SUCCESS] Session summary saved to {self.json_out}")
            except Exception as e: 
                print(f"[ERROR] Failed to save JSON summary: {e}")
        self.root.destroy()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    # Default is None so we can generate the unique timestamped name
    parser.add_argument("--json-out", default=None, help="Output JSON filename")
    args = parser.parse_args()
    
    # --- UNIQUE FILENAME LOGIC ---
    if args.json_out is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.json_out = f"session_{timestamp}.json"
    
    load_mfg_ids()
    print(f"[INFO] Initializing app. Data will be saved to: {args.json_out}")
    BLEPopupApp(args.url, json_out=args.json_out).root.mainloop()