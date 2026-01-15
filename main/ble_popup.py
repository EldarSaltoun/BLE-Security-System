import argparse
import csv
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

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
    """Resolve manufacturer identifier to a friendly name.

    Accepts either:
      - int (e.g. 76)
      - hex string (e.g. "004C" or "0x004C")
    """
    try:
        if isinstance(mfg, str):
            s = mfg.strip().lower()
            if s.startswith("0x"):
                s = s[2:]
            mfg_val = int(s, 16) if s else 0
        else:
            mfg_val = int(mfg)

        if mfg_val == 0:
            return "(none)"
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
                    "timestamp_local", "mac", "name", "rssi", "txpwr",
                    "mfg", "adv_len", "adv_int_ms",
                    "has_services", "n_services_16", "n_services_128",
                    "mfg_data", "scanner", "timestamp_esp_us"  # <--- ADDED mfg_data
                ])

    def ingest(self, mac, rssi, name, txpwr, mfg_id, adv_len,
               has_services, n_services_16, n_services_128, mfg_data, # <--- ADDED ARG
               ts_us, scanner):
        rssi = int(rssi)
        txpwr = int(txpwr)
        adv_len = int(adv_len)
        mfg_id_int = int(mfg_id) if mfg_id is not None else 0
        mfg_resolved = resolve_mfg_name(mfg_id_int)
        has_services = int(has_services) if has_services is not None else 0
        n_services_16 = int(n_services_16) if n_services_16 is not None else 0
        n_services_128 = int(n_services_128) if n_services_128 is not None else 0
        mfg_data_str = str(mfg_data) if mfg_data else "" # <--- Normalize

        now_mono = time.monotonic()
        now_dt = datetime.now()
        now_str = now_dt.strftime("%H:%M:%S")
        now_iso = now_dt.isoformat(timespec="milliseconds")

        entry = self.devices.get(mac)
        adv_int_ms = None
        if entry and entry.get("last_ts"):
            adv_int_ms = round((ts_us - entry["last_ts"]) / 1000.0, 1)

        self.devices[mac] = {
            "name": name.strip(),
            "rssi": rssi,
            "txpwr": txpwr,
            "mfg": mfg_resolved,
            "adv_len": adv_len,
            "has_services": has_services,
            "n_services_16": n_services_16,
            "n_services_128": n_services_128,
            "mfg_data": mfg_data_str, # <--- Store raw data
            "last_seen_mono": now_mono,
            "last_seen_str": now_str,
            "last_ts": ts_us,
            "adv_int": adv_int_ms,
            "scanner": scanner
        }

        self.events.append({
            "mac": mac,
            "name": name.strip(),
            "rssi": rssi,
            "txpwr": txpwr,
            "mfg": {
                "raw_hex": f"0x{mfg_id_int:04X}",
                "resolved": mfg_resolved
            },
            "adv_len": adv_len,
            "has_services": has_services,
            "n_services_16": n_services_16,
            "n_services_128": n_services_128,
            "mfg_data": mfg_data_str, # <--- Log raw data
            "scanner": scanner,
            "timestamp_local": now_iso,
            "timestamp_esp_us": int(ts_us),
            "adv_int_ms": adv_int_ms
        })

        if self.min_rssi is not None and rssi < self.min_rssi:
            return

        with open(self.csv_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now_iso, mac, name.strip(), rssi, txpwr,
                mfg_resolved, adv_len,
                adv_int_ms if adv_int_ms else "",
                has_services, n_services_16, n_services_128,
                mfg_data_str, scanner, int(ts_us) # <--- CSV Write
            ])

    def prune_stale(self):
        cutoff = time.monotonic() - self.presence_window_s
        stale = [
            mac for mac, d in self.devices.items()
            if d["last_seen_mono"] < cutoff
        ]
        for mac in stale:
            del self.devices[mac]

    @property
    def present_count(self):
        return len(self.devices)

    def export_json(self, output_path: Path, session_meta: dict):
        first_seen = {}
        last_seen = {}
        for ev in self.events:
            mac = ev["mac"]
            ts = ev["timestamp_local"]
            first_seen.setdefault(mac, ts)
            last_seen[mac] = ts

        payload = {
            "meta": session_meta,
            "counts": {
                "total_events": len(self.events),
                "unique_devices": len(first_seen),
            },
            "devices_seen": [
                {
                    "mac": mac,
                    "first_seen_local": first_seen[mac],
                    "last_seen_local": last_seen[mac]
                }
                for mac in sorted(first_seen.keys())
            ],
            "events": self.events
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------------- App ----------------

class BLEPopupApp:
    def __init__(self, stream_url, presence_window_s=5, min_rssi=None, json_out=None):
        self.stream_url = stream_url
        self.json_out = json_out
        self.start_iso = datetime.now().isoformat(timespec="seconds")

        self.model = DeviceModel(presence_window_s, min_rssi)

        self.root = tk.Tk()
        self.root.title("BLE Live Presence — ESP32 Scanner (HTTP)")

        top = ttk.Frame(self.root, padding=8)
        top.pack(fill="x")

        self.lbl_present = ttk.Label(
            top, text="Present devices: 0",
            font=("Segoe UI", 12, "bold")
        )
        self.lbl_window = ttk.Label(
            top, text=f"Window: {presence_window_s}s",
            font=("Segoe UI", 10)
        )

        self.lbl_present.pack(side="left", padx=(0, 16))
        self.lbl_window.pack(side="left")

        # UPDATED COLUMNS: Added mfg_data
        cols = (
            "mac", "name", "rssi", "txpwr",
            "mfg", "adv_len", "adv_int",
            "has_services", "n_services_16", "n_services_128",
            "mfg_data", "scanner", "last_seen"
        )

        self.tree = ttk.Treeview(
            self.root, columns=cols,
            show="headings", height=18
        )

        # UPDATED COLUMN WIDTHS: Added width for mfg_data (index 10)
        for c, w in zip(
            cols,
            (170, 150, 60, 60, 200, 80, 80, 90, 110, 120, 150, 100, 120) 
        ):
            heading_text = "RAW DATA" if c == "mfg_data" else c.upper()
            self.tree.heading(c, text=heading_text)
            self.tree.column(c, width=w, anchor="w")

        self.tree.pack(fill="both", expand=True, padx=8, pady=8)

        self.stop_flag = threading.Event()
        self.th = threading.Thread(
            target=self.reader_thread, daemon=True
        )
        self.th.start()

        self.root.after(300, self.refresh_ui)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def reader_thread(self):
        try:
            with requests.get(
                self.stream_url,
                stream=True,
                timeout=None,
                proxies={"http": None, "https": None}
            ) as r:
                r.raise_for_status()
                for line in r.iter_lines(decode_unicode=True):
                    if self.stop_flag.is_set():
                        break
                    if not line or not line.startswith("data: "):
                        continue

                    ev = json.loads(line[6:])

                    self.model.ingest(
                        ev["mac"].upper(),
                        ev["rssi"],
                        ev.get("name", ""),
                        ev.get("txpwr", 0),
                        int(ev.get("mfg_id", 0)),
                        ev.get("adv_len", 0),
                        int(ev.get("has_services", 0)),
                        int(ev.get("n_services_16", 0)),
                        int(ev.get("n_services_128", 0)),
                        ev.get("mfg_data", ""),  # <--- PASS RAW DATA
                        int(ev["timestamp_esp_us"]),
                        ev.get("scanner", "UNK")
                    )
        except Exception as e:
            print(f"[ERR] HTTP stream failed: {e}", file=sys.stderr)

    def refresh_ui(self):
        self.model.prune_stale()
        self.lbl_present.config(
            text=f"Present devices: {self.model.present_count}"
        )

        current_iids = set(self.tree.get_children())
        live_macs = set(self.model.devices.keys())

        for mac, d in self.model.devices.items():
            adv_int_str = (
                f"{d['adv_int']} ms"
                if d["adv_int"] else "-"
            )
            vals = (
                mac, d["name"], str(d["rssi"]),
                str(d["txpwr"]), d["mfg"],
                str(d["adv_len"]), adv_int_str,
                str(d.get("has_services", 0)),
                str(d.get("n_services_16", 0)),
                str(d.get("n_services_128", 0)),
                d.get("mfg_data", ""), # <--- DISPLAY RAW DATA
                d["scanner"], d["last_seen_str"]
            )
            if mac in current_iids:
                self.tree.item(mac, values=vals)
            else:
                self.tree.insert("", "end", iid=mac, values=vals)

        for iid in current_iids - live_macs:
            self.tree.delete(iid)

        self.root.after(300, self.refresh_ui)

    def on_close(self):
        self.stop_flag.set()

        out_path = self.json_out
        if out_path is None:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = Path(f"ble_session_{ts}.json")

        meta = {
            "created_at_local": datetime.now().isoformat(timespec="seconds"),
            "session_started_at_local": self.start_iso,
            "input": {"http_stream": self.stream_url},
            "params": {
                "presence_window_s": self.model.presence_window_s,
                "min_rssi": self.model.min_rssi
            },
            "csv_stream_log": str(self.model.csv_log),
            "notes": (
                "ESP timestamps are authoritative; "
                "HTTP transport delay ignored."
            )
        }

        try:
            self.model.export_json(Path(out_path), meta)
        finally:
            self.root.destroy()

    def run(self):
        self.root.mainloop()


# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser(
        description="Live BLE presence viewer (HTTP)"
    )
    ap.add_argument(
        "--url", required=True,
        help="HTTP SSE stream URL"
    )
    ap.add_argument("--window", type=float, default=5.0)
    ap.add_argument("--min-rssi", type=int, default=None)
    ap.add_argument("--mfg-db", type=str, default="mfg_ids.csv")
    ap.add_argument("--json-out", type=str, default=None)
    args = ap.parse_args()

    load_mfg_ids(args.mfg_db)

    json_out = Path(args.json_out) if args.json_out else None
    app = BLEPopupApp(
        args.url,
        args.window,
        args.min_rssi,
        json_out=json_out
    )
    app.run()


if __name__ == "__main__":
    main()