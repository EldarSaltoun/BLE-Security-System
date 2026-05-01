import argparse
import csv
import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
import base64
from ble_adv_parser import AdvParser

import requests
import tkinter as tk
from tkinter import ttk

# ---------------- Manufacturer & Service DB ----------------
MFG_IDS = {}

GATT_SERVICES = {
    "1800": "Generic Access",
    "1801": "Generic Attribute",
    "1802": "Immediate Alert",
    "1803": "Link Loss",
    "1804": "Tx Power",
    "180A": "Device Info",
    "180D": "Heart Rate",
    "180F": "Battery",
    "1812": "HID (Mouse/KB)",
    "1819": "Location/Nav",
    "fe9f": "Google Fast Pair",
    "fd6f": "Exposure Notification",
    "fee7": "Tencent WeChat",
    "feaa": "Eddystone",
    "feaf": "Disney"
}

def load_mfg_ids(filename="mfg_ids.csv"):
    global MFG_IDS
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, filename)
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2: continue
                try:
                    cid = int(row[0], 16)
                    name = row[1].strip()
                    MFG_IDS[cid] = name
                except ValueError: continue
    except Exception:
        pass

def resolve_mfg_name(mfg) -> str:
    try:
        if mfg is None:
            return "(none)"
        mfg_val = int(mfg)
        if mfg_val == 0: return "(none)"
        return MFG_IDS.get(mfg_val, f"Unknown(0x{mfg_val:04X})")
    except Exception:
        return str(mfg)

def resolve_service_names(uuid_list):
    if not uuid_list: return "None"
    resolved = [GATT_SERVICES.get(str(u).lower(), f"UUID-{u}") for u in uuid_list]
    return ", ".join(resolved)

def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default

def safe_base64_decode(payload_str):
    if not payload_str:
        return b""
    try:
        return base64.b64decode(payload_str, validate=False)
    except Exception:
        return b""

# ---------------- Integrated Control Panel ----------------
class ScannerControlWindow(tk.Toplevel):
    def __init__(self, parent, base_url):
        super().__init__(parent)
        self.title("BLE Scanner Control & Calibration")
        self.geometry("650x550")
        self.base_url = base_url
        self.api_base = f"{base_url}/api/control"
        self.calib_base = f"{base_url}/api/calibrate"

        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        ctrl_frame = ttk.Frame(notebook)
        notebook.add(ctrl_frame, text="Scanner Management")
        self._setup_control_tab(ctrl_frame)

        calib_frame = ttk.Frame(notebook)
        notebook.add(calib_frame, text="3D Calibration")
        self._setup_calib_tab(calib_frame)

    def _setup_control_tab(self, frame):
        global_frame = ttk.LabelFrame(frame, text="Global Controls", padding=10)
        global_frame.pack(fill="x", padx=10, pady=5)
        ttk.Button(global_frame, text="All IDLE", command=lambda: self.send_cmd("all", state=0)).pack(side="left", padx=5)
        ttk.Button(global_frame, text="All ACTIVE", command=lambda: self.send_cmd("all", state=1)).pack(side="left", padx=5)
        ttk.Button(global_frame, text="Rotate Mode", command=lambda: self.send_cmd("all", mode=0)).pack(side="right", padx=5)
        self.list_frame = ttk.LabelFrame(frame, text="Active Scanners", padding=10)
        self.list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        ttk.Button(frame, text="Refresh Scanner List", command=self.refresh_scanners).pack(pady=5)
        self.refresh_scanners()

    def _setup_calib_tab(self, frame):
        ttk.Label(frame, text="3D Calibration Point", font=('Segoe UI', 12, 'bold')).pack(pady=10)
        self.coords = {"x": tk.DoubleVar(value=0.0), "y": tk.DoubleVar(value=0.0), "z": tk.DoubleVar(value=1.0)}
        entry_frame = ttk.Frame(frame)
        entry_frame.pack(pady=10)
        for i, (axis, var) in enumerate(self.coords.items()):
            ttk.Label(entry_frame, text=f"{axis.upper()} (m):").grid(row=i, column=0, padx=5, pady=5)
            ttk.Entry(entry_frame, textvariable=var, width=10).grid(row=i, column=1, padx=5, pady=5)
        self.btn_calib = tk.Button(frame, text="Start Sampling Point", bg="green", fg="white", command=self.start_calib, font=('Segoe UI', 10, 'bold'))
        self.btn_calib.pack(pady=20)
        self.lbl_cal_status = ttk.Label(frame, text="Status: Ready")
        self.lbl_cal_status.pack()

    def refresh_scanners(self):
        for w in self.list_frame.winfo_children(): w.destroy()
        try:
            r = requests.get(f"{self.api_base}/scanners", timeout=2)
            scanners = r.json()
            if not scanners:
                ttk.Label(self.list_frame, text="No active scanners detected.").pack()
                return
            for s_id, info in scanners.items():
                f = ttk.Frame(self.list_frame, padding=5)
                f.pack(fill="x")
                ttk.Label(f, text=f"Scanner {s_id} ({info['ip']})", width=25).pack(side="left")
                ttk.Button(f, text="Idle", width=8, command=lambda id=s_id: self.send_cmd(id, state=0)).pack(side="left", padx=2)
                ttk.Button(f, text="Scan", width=8, command=lambda id=s_id: self.send_cmd(id, state=1)).pack(side="left", padx=2)
                ttk.Button(f, text="Auto", width=8, command=lambda id=s_id: self.send_cmd(id, mode=0)).pack(side="right")
        except Exception:
            pass

    def send_cmd(self, target, state=None, mode=None):
        payload = {"target": target}
        if state is not None: payload["state"] = state
        if mode is not None: payload["mode"] = mode
        try: requests.post(f"{self.api_base}/send", json=payload, timeout=2)
        except Exception as e: print(f"Cmd Failed: {e}")

    def start_calib(self):
        payload = {"coords": {k: v.get() for k, v in self.coords.items()}}
        try:
            r = requests.post(f"{self.calib_base}/start", json=payload, timeout=2)
            color = "blue" if r.status_code == 200 else "red"
            self.lbl_cal_status.config(text=f"Status: {r.json().get('status', 'Error')}", foreground=color)
        except Exception:
            self.lbl_cal_status.config(text="Status: Connection Error", foreground="red")

# ---------------- Data Model ----------------
class DeviceModel:
    def __init__(self, presence_window_s=5, min_rssi=None, csv_log="ble_log.csv"):
        self.presence_window_s = presence_window_s
        self.min_rssi = min_rssi

        # Raw signal rows keyed by MAC. This preserves the original Signal View behavior.
        self.devices = {}

        # Physical-device rows keyed by uid from pc_receiver DeviceTracker.
        self.physical_devices = {}

        self.events = []
        self.csv_log = csv_log
        self.lock = threading.Lock()

        if not os.path.exists(self.csv_log):
            with open(self.csv_log, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp_local", "uid", "status", "physical_label",
                    "mac", "name", "rssi", "channel", "txpwr",
                    "mfg", "adv_len", "has_services",
                    "n_services_16", "n_services_128", "mfg_data",
                    "scanner", "timestamp_epoch_us", "payload_sig", "dna"
                ])

    def prune_stale(self):
        cutoff = time.monotonic() - self.presence_window_s
        with self.lock:
            stale = [mac for mac, d in self.devices.items() if d["last_seen_mono"] < cutoff]
            for mac in stale:
                del self.devices[mac]

            stale_uid = [uid for uid, d in self.physical_devices.items() if d["last_seen_mono"] < cutoff]
            for uid in stale_uid:
                del self.physical_devices[uid]

    def snapshot_devices(self):
        with self.lock:
            return dict(self.devices)

    def snapshot_physical_devices(self):
        with self.lock:
            return dict(self.physical_devices)

    def ingest(self, mac, rssi, channel, name, txpwr, mfg_id, adv_len,
               has_services, n_services_128, mfg_data,
               ts_epoch_us, scanner, payload_str, services_list, payload_sig="",
               uid="ALIAS_UNKNOWN", status="UNCLASSIFIED", dna="", physical_label=""):

        if self.min_rssi is not None and safe_int(rssi, -999) < self.min_rssi:
            return

        mac = str(mac or "UNK").upper().strip()
        name = str(name or "Unknown").strip() or "Unknown"
        scanner = str(scanner or "UNK")
        payload_str = str(payload_str or "")
        payload_sig = str(payload_sig or "")
        uid = str(uid or "ALIAS_UNKNOWN")
        status = str(status or "UNCLASSIFIED")
        dna = str(dna or "")
        physical_label = str(physical_label or uid)

        mfg_resolved = resolve_mfg_name(mfg_id)
        now_mono = time.monotonic()
        now_dt = datetime.now()

        rssi_i = safe_int(rssi, 0)
        channel_i = safe_int(channel, 0)
        txpwr_i = safe_int(txpwr, 0)
        adv_len_i = safe_int(adv_len, 0)
        has_services_i = safe_int(has_services, 0)
        n_services_128_i = safe_int(n_services_128, 0)
        ts_epoch_i = safe_int(ts_epoch_us, 0)

        base_row = {
            "uid": uid,
            "status": status,
            "dna": dna,
            "physical_label": physical_label,
            "name": name,
            "rssi": rssi_i,
            "channel": channel_i,
            "txpwr": txpwr_i,
            "mfg": mfg_resolved,
            "adv_len": adv_len_i,
            "has_services": has_services_i,
            "services_16": services_list or [],
            "n_services_128": n_services_128_i,
            "mfg_data": mfg_data or "",
            "payload_sig": payload_sig,
            "last_seen_mono": now_mono,
            "last_seen_str": now_dt.strftime("%H:%M:%S"),
            "last_ts": ts_epoch_i,
            "scanner": scanner,
        }

        with self.lock:
            # Raw Signal View row.
            self.devices[mac] = dict(base_row)

            # Physical Device View row.
            pd = self.physical_devices.get(uid)
            if pd is None:
                pd = {
                    "uid": uid,
                    "status": status,
                    "physical_label": physical_label,
                    "name": name,
                    "mfg": mfg_resolved,
                    "services_16": set(services_list or []),
                    "last_seen_mono": now_mono,
                    "last_seen_str": now_dt.strftime("%H:%M:%S"),
                    "best_rssi": rssi_i,
                    "last_rssi": rssi_i,
                    "scanner": scanner,
                    "macs": set(),
                    "payload_sigs": set(),
                    "dna": dna,
                }
                self.physical_devices[uid] = pd

            pd["status"] = status
            pd["physical_label"] = physical_label or pd.get("physical_label", uid)
            pd["last_seen_mono"] = now_mono
            pd["last_seen_str"] = now_dt.strftime("%H:%M:%S")
            pd["last_rssi"] = rssi_i
            pd["scanner"] = scanner
            pd["dna"] = dna or pd.get("dna", "")

            if rssi_i > pd.get("best_rssi", -999):
                pd["best_rssi"] = rssi_i
                pd["name"] = name
                pd["mfg"] = mfg_resolved

            pd.setdefault("macs", set()).add(mac)
            if payload_sig:
                pd.setdefault("payload_sigs", set()).add(payload_sig)
            if services_list:
                pd.setdefault("services_16", set()).update(services_list)

            # Keep the exported session JSON schema compatible with the MATLAB workflow.
            self.events.append({
                "mac": mac,
                "name": name, "rssi": rssi_i, "channel": channel_i,
                "scanner": scanner, "ts": ts_epoch_i, "payload": payload_str
            })

        with open(self.csv_log, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                now_dt.isoformat(), uid, status, physical_label,
                mac, name, rssi_i, channel_i, txpwr_i, mfg_resolved,
                adv_len_i, has_services_i, len(services_list or []),
                n_services_128_i, mfg_data or "", scanner, ts_epoch_i,
                payload_sig, dna
            ])

    def export_json(self, output_path: Path, session_meta: dict):
        with self.lock:
            payload = {"meta": session_meta, "events": list(self.events)}
        with open(output_path, "w", encoding="utf-8") as f: json.dump(payload, f, indent=2)

# ---------------- App ----------------
class BLEPopupApp:
    def __init__(self, stream_url, json_out):
        self.base_url = stream_url.replace("/api/ble/stream", "")
        self.stream_url = stream_url
        self.json_out = json_out
        self.start_iso = datetime.now().isoformat(timespec="seconds")
        self.model = DeviceModel()
        self.view_mode = "SIGNALS"

        self.root = tk.Tk()
        self.root.title("BLE Grid Scanner Dashboard")

        # Top Bar
        top = ttk.Frame(self.root, padding=8)
        top.pack(fill="x")
        self.lbl_present = ttk.Label(top, text="Active: 0", font=("Segoe UI", 12, "bold"))
        self.lbl_present.pack(side="left")

        self.btn_toggle_view = ttk.Button(top, text="Switch to Device View", command=self.toggle_view)
        self.btn_toggle_view.pack(side="left", padx=20)

        self.btn_control = ttk.Button(top, text="Open Control Panel", command=self.open_control_panel)
        self.btn_control.pack(side="right", padx=5)

        self.btn_reconnect = ttk.Button(top, text="Reconnect Stream", command=self.start_reader)
        self.btn_reconnect.pack(side="right", padx=5)

        self.lbl_status = ttk.Label(top, text="Status: Starting...", font=("Segoe UI", 9))
        self.lbl_status.pack(side="right", padx=10)

        # Treeview Configuration
        self.tree_frame = ttk.Frame(self.root)
        self.tree_frame.pack(fill="both", expand=True, padx=8, pady=8)
        self.tree = None
        self.setup_columns()

        self.stop_flag = threading.Event()
        self.reader_thread_handle = None
        self.start_reader()
        self.root.after(300, self.refresh_ui)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def setup_columns(self):
        if self.tree: self.tree.destroy()

        if self.view_mode == "SIGNALS":
            # Simplified columns for raw signal monitoring
            cols = ("mac", "name", "rssi", "ch", "mfg", "mfg_data", "scanner", "last_seen")
            widths = (160, 140, 50, 40, 180, 200, 100, 100)
        else: # DEVICES View
            cols = ("mac", "mfg", "name", "services", "last_seen")
            widths = (160, 200, 140, 450, 100)

        self.tree = ttk.Treeview(self.tree_frame, columns=cols, show="headings", height=18)

        for c, w in zip(cols, widths):
            self.tree.heading(c, text=c.upper())
            self.tree.column(c, width=w, anchor="w")
        self.tree.pack(fill="both", expand=True)

    def toggle_view(self):
        self.view_mode = "DEVICES" if self.view_mode == "SIGNALS" else "SIGNALS"
        self.btn_toggle_view.config(text="Switch to Signal View" if self.view_mode == "DEVICES" else "Switch to Device View")
        self.setup_columns()

    def open_control_panel(self):
        ScannerControlWindow(self.root, self.base_url)

    def start_reader(self):
        self.lbl_status.config(text="Status: Connecting...", foreground="orange")
        self.stop_flag.set()
        time.sleep(0.2)
        self.stop_flag.clear()
        self.reader_thread_handle = threading.Thread(target=self.reader_thread, daemon=True)
        self.reader_thread_handle.start()

    def reader_thread(self):
        try:
            with requests.get(self.stream_url, stream=True, timeout=5) as r:
                r.raise_for_status()
                self.lbl_status.config(text="Status: Connected", foreground="green")
                for line in r.iter_lines(decode_unicode=True):
                    if self.stop_flag.is_set(): break
                    if not line or not line.startswith("data: "): continue
                    try:
                        ev = json.loads(line[6:])
                        self._handle_stream_event(ev)
                    except Exception:
                        continue
        except Exception:
            self.lbl_status.config(text="Status: Disconnected", foreground="red")

    def _handle_stream_event(self, ev):
        p_data = ev.get("payload", ev.get("p", "")) or ""

        # Prefer parsed fields from the fixed receiver. Fall back to local parsing for compatibility.
        raw_payload = safe_base64_decode(p_data)
        parsed = {}
        if raw_payload:
            try:
                parsed = AdvParser.parse(raw_payload)
            except Exception:
                parsed = {}

        name = ev.get("name")
        if not name or name == "Unknown":
            name = parsed.get("name", "Unknown")

        mfg_id = ev.get("mfg_id", parsed.get("mfg_id", 0) if parsed else 0)
        mfg_data = ev.get("mfg_data_hex", parsed.get("mfg_data_hex", "") if parsed else "")
        txpwr = ev.get("txpwr", ev.get("tx_pwr", parsed.get("tx_pwr", 0) if parsed else 0))
        adv_len = ev.get("adv_len", len(raw_payload))
        services_16 = ev.get("services_16", parsed.get("services_16", []) if parsed else [])
        services_128 = ev.get("services_128", parsed.get("services_128", []) if parsed else [])
        payload_sig = ev.get("payload_sig", parsed.get("payload_sig", "") if parsed else "")

        self.model.ingest(
            mac=ev.get("mac", "UNK").upper(),
            rssi=ev.get("rssi", 0),
            channel=ev.get("channel", 0),
            name=name,
            txpwr=txpwr or 0,
            mfg_id=mfg_id or 0,
            adv_len=adv_len,
            has_services=1 if services_16 else 0,
            n_services_128=len(services_128) if isinstance(services_128, list) else 0,
            mfg_data=mfg_data,
            ts_epoch_us=ev.get("ts", 0),
            scanner=ev.get("scanner", "UNK"),
            payload_str=p_data,
            services_list=services_16,
            payload_sig=payload_sig,
            uid=ev.get("uid", "ALIAS_UNKNOWN"),
            status=ev.get("status", "UNCLASSIFIED"),
            dna=ev.get("dna", payload_sig),
            physical_label=ev.get("physical_label", ev.get("uid", "ALIAS_UNKNOWN"))
        )

    def refresh_ui(self):
        self.model.prune_stale()

        raw_snapshot = self.model.snapshot_devices()
        physical_snapshot = self.model.snapshot_physical_devices()

        if self.view_mode == "SIGNALS":
            self.lbl_present.config(text=f"Active: {len(raw_snapshot)}")
        else:
            self.lbl_present.config(text=f"Active: {len(physical_snapshot)}")

        current_rows = set(self.tree.get_children())
        active_keys = set()

        if self.view_mode == "SIGNALS":
            sorted_items = sorted(raw_snapshot.items(), key=lambda x: x[0])
            for mac, d in sorted_items:
                vals = (mac, d["name"], d["rssi"], d["channel"],
                        d["mfg"], d["mfg_data"], d["scanner"], d["last_seen_str"])
                if mac in current_rows: self.tree.item(mac, values=vals)
                else: self.tree.insert("", "end", iid=mac, values=vals)
                active_keys.add(mac)
        else:
            for uid, d in sorted(physical_snapshot.items(), key=lambda x: x[0]):
                services_text = resolve_service_names(sorted(list(d.get("services_16", set()))))
                num_macs = len(d.get("macs", set()))
                num_payloads = len(d.get("payload_sigs", set()))

                # Keep the same Device View columns/widths.
                # The first column is still named "MAC" in the GUI, but now contains the physical UID.
                vals = (
                    uid,
                    d.get("mfg", ""),
                    d.get("physical_label", d.get("name", "")),
                    f"{services_text} | MACs:{num_macs} Payloads:{num_payloads} Status:{d.get('status', '')}",
                    d.get("last_seen_str", "")
                )
                if uid in current_rows: self.tree.item(uid, values=vals)
                else: self.tree.insert("", "end", iid=uid, values=vals)
                active_keys.add(uid)

        for iid in current_rows - active_keys: self.tree.delete(iid)
        self.root.after(300, self.refresh_ui)

    def on_close(self):
        self.stop_flag.set()
        if self.json_out:
            meta = {"start": self.start_iso, "devices": len(self.model.snapshot_physical_devices()), "end": datetime.now().isoformat()}
            try: self.model.export_json(Path(self.json_out), meta)
            except Exception: pass
        self.root.destroy()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()
    if args.json_out is None:
        args.json_out = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    load_mfg_ids()
    BLEPopupApp(args.url, json_out=args.json_out).root.mainloop()
