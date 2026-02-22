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
                if len(row) < 2:
                    continue
                try:
                    cid = int(row[0], 16)
                    name = row[1].strip()
                    MFG_IDS[cid] = name
                except ValueError:
                    continue
        print(f"[INFO] Loaded {len(MFG_IDS)} manufacturer IDs from {filename}")
    except FileNotFoundError:
        print(f"[WARN] Manufacturer ID file '{filename}' not found, using empty DB")

def resolve_mfg_name(mfg) -> str:
    try:
        if isinstance(mfg, str):
            s = mfg.strip().lower()
            if s.startswith("0x"): s = s[2:]
            mfg_val = int(s, 16) if s else 0
        else:
            mfg_val = int(mfg)

        if mfg_val == 0: return "(none)"
        return MFG_IDS.get(mfg_val, f"Unknown(0x{mfg_val:04X})")
    except Exception:
        return str(mfg)

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
                    "mfg", "adv_len", "adv_int_ms",
                    "has_services", "n_services_16", "n_services_128",
                    "mfg_data", "packet_hash", "scanner", "timestamp_epoch_us"
                ])

    def ingest(self, mac, rssi, channel, name, txpwr, mfg_id, adv_len,
           has_services, n_services_16, n_services_128, mfg_data,
           packet_hash, ts_epoch_us, ts_mono_us, scanner):
        
        rssi = int(rssi)
        channel = int(channel) if channel is not None else 0
        txpwr = int(txpwr)
        adv_len = int(adv_len)
        mfg_id_int = int(mfg_id) if mfg_id is not None else 0
        mfg_resolved = resolve_mfg_name(mfg_id_int)
        
        has_services = int(has_services) if has_services is not None else 0
        n_services_16 = int(n_services_16) if n_services_16 is not None else 0
        n_services_128 = int(n_services_128) if n_services_128 is not None else 0

        now_mono = time.monotonic()
        now_dt = datetime.now()
        now_str = now_dt.strftime("%H:%M:%S")
        now_iso = now_dt.isoformat(timespec="milliseconds")

        entry = self.devices.get(mac)
        ts_mono_us = int(ts_mono_us) if ts_mono_us is not None else 0
        prev_mono = int(entry.get("last_mono_us", 0)) if entry else 0

        adv_int_ms = None
        if prev_mono > 0 and ts_mono_us > 0:
            dt_ms = (ts_mono_us - prev_mono) / 1000.0
            if 10 < dt_ms <= 10240:
                adv_int_ms = round(dt_ms, 1)

        self.devices[mac] = {
            "name": name.strip(),
            "rssi": rssi,
            "channel": channel,
            "txpwr": txpwr,
            "mfg": mfg_resolved,
            "adv_len": adv_len,
            "has_services": has_services,
            "n_services_16": n_services_16,
            "n_services_128": n_services_128,
            "mfg_data": mfg_data,
            "packet_hash": packet_hash,
            "last_seen_mono": now_mono,
            "last_seen_str": now_str,
            "last_ts": int(ts_epoch_us),
            "last_mono_us": ts_mono_us,
            "adv_int": adv_int_ms,
            "scanner": scanner
        }

        with open(self.csv_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now_iso, mac, name.strip(), rssi, channel, txpwr,
                mfg_resolved, adv_len,
                adv_int_ms if adv_int_ms else "",
                has_services, n_services_16, n_services_128,
                mfg_data, packet_hash, scanner, int(ts_epoch_us)
            ])

    def prune_stale(self):
        cutoff = time.monotonic() - self.presence_window_s
        stale = [mac for mac, d in self.devices.items() if d["last_seen_mono"] < cutoff]
        for mac in stale: del self.devices[mac]

# ---------------- App ----------------

class BLEPopupApp:
    def __init__(self, stream_url, presence_window_s=5, min_rssi=None, json_out=None):
        self.stream_url = stream_url
        self.json_out = json_out
        self.start_iso = datetime.now().isoformat(timespec="seconds")
        self.model = DeviceModel(presence_window_s, min_rssi)

        self.root = tk.Tk()
        self.root.title("BLE Live Presence — ESP32 Grid Scanner")

        top = ttk.Frame(self.root, padding=8)
        top.pack(fill="x")

        self.lbl_present = ttk.Label(top, text="Present devices: 0", font=("Segoe UI", 12, "bold"))
        self.lbl_window = ttk.Label(top, text=f"Window: {presence_window_s}s", font=("Segoe UI", 10))

        self.lbl_present.pack(side="left", padx=(0, 16))
        self.lbl_window.pack(side="left")

        cols = ("mac", "name", "rssi", "ch", "txpwr", "mfg", "adv_len", "adv_int", 
                "has_services", "n_services_16", "n_services_128", "mfg_data", "hash", "scanner", "last_seen")

        self.tree = ttk.Treeview(self.root, columns=cols, show="headings", height=18)

        # RESTORED: Your exact visual configuration (widths & headings)
        widths = (160, 140, 50, 40, 50, 180, 60, 70, 80, 80, 80, 100, 80, 100, 100)
        for c, w in zip(cols, widths):
            if c == "ch": heading_text = "CH"
            elif c == "hash": heading_text = "HASH"
            elif c == "mfg_data": heading_text = "RAW DATA"
            else: heading_text = c.upper()
            self.tree.heading(c, text=heading_text)
            self.tree.column(c, width=w, anchor="w")

        self.tree.pack(fill="both", expand=True, padx=8, pady=8)

        self.stop_flag = threading.Event()
        threading.Thread(target=self.reader_thread, daemon=True).start()
        self.root.after(300, self.refresh_ui)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def reader_thread(self):
        try:
            with requests.get(self.stream_url, stream=True, timeout=None) as r:
                r.raise_for_status()
                for line in r.iter_lines(decode_unicode=True):
                    if self.stop_flag.is_set(): break
                    if not line or not line.startswith("data: "): continue
                    
                    ev = json.loads(line[6:])
                    try:
                        raw_payload = base64.b64decode(ev["p"])
                        p_hash = f"{zlib.crc32(raw_payload) & 0xFFFFFFFF:08X}"
                        parsed = AdvParser.parse(raw_payload)

                        # FIX: Mapping fields and using ev["ts"] for the interval calculation
                        self.model.ingest(
                            mac=ev["a"].upper(), rssi=ev["r"], channel=ev.get("c", 0),
                            name=parsed["name"], txpwr=parsed["tx_pwr"] if parsed["tx_pwr"] is not None else 0,
                            mfg_id=parsed["mfg_id"] if parsed["mfg_id"] is not None else 0,
                            adv_len=len(raw_payload),
                            has_services=1 if (parsed["services_16"] or parsed["services_128"]) else 0,
                            n_services_16=len(parsed["services_16"]), n_services_128=len(parsed["services_128"]),
                            mfg_data=parsed["mfg_data_hex"], packet_hash=p_hash,
                            ts_epoch_us=ev["ts"], ts_mono_us=ev["ts"], 
                            scanner=ev.get("scanner", "UNK")
                        )
                    except Exception: continue
        except Exception as e: print(f"[ERR] HTTP stream failed: {e}")

    def refresh_ui(self):
        self.model.prune_stale()
        self.lbl_present.config(text=f"Present devices: {len(self.model.devices)}")
        current_iids = set(self.tree.get_children())
        live_macs = set(self.model.devices.keys())

        for mac, d in list(self.model.devices.items()):
            vals = (mac, d["name"], str(d["rssi"]), str(d["channel"]), str(d["txpwr"]), d["mfg"],
                    str(d["adv_len"]), f"{d['adv_int']} ms" if d["adv_int"] else "-",
                    str(d["has_services"]), str(d["n_services_16"]), str(d["n_services_128"]),
                    d["mfg_data"], d["packet_hash"], d["scanner"], d["last_seen_str"])
            if mac in current_iids: self.tree.item(mac, values=vals)
            else: self.tree.insert("", "end", iid=mac, values=vals)

        for iid in current_iids - live_macs: self.tree.delete(iid)
        self.root.after(300, self.refresh_ui)

    def on_close(self):
        self.stop_flag.set()
        self.root.destroy()

    def run(self): self.root.mainloop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    load_mfg_ids("mfg_ids.csv")
    BLEPopupApp(args.url).run()