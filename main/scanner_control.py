import tkinter as tk
from tkinter import ttk
import requests

API_BASE = "http://localhost:8000/api/control"
CALIB_API_BASE = "http://localhost:8000/api/calibrate"

REQUEST_TIMEOUT_S = 2

class CalibrationTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)

        tk.Label(self, text="3x3 Grid Calibration", font=('Arial', 12, 'bold')).pack(pady=10)

        info = (
            "Place EldarCalib in ONE grid block, enter block 1-9, and start sampling. "
            "The receiver collects 100 valid RSSI samples per active scanner and ignores pseudo-channel labels."
        )
        tk.Label(self, text=info, wraplength=520, justify="center").pack(padx=12, pady=5)

        self.grid_block_val = tk.IntVar(value=1)

        frame_inputs = tk.Frame(self)
        frame_inputs.pack(pady=10)

        tk.Label(frame_inputs, text="Grid Block (1-9):").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        tk.Spinbox(frame_inputs, from_=1, to=9, textvariable=self.grid_block_val, width=8).grid(row=0, column=1, padx=5, pady=5, sticky="w")

        self.btn = tk.Button(
            self,
            text="Start Grid Block Sampling",
            command=self.start_calib,
            bg="green",
            fg="white",
            font=('Arial', 10, 'bold')
        )
        self.btn.pack(pady=20)

        self.status = tk.Label(self, text="Status: Ready", font=('Arial', 10))
        self.status.pack()

        tk.Label(
            self,
            text="Output files: calibration_raw.csv and calibration_summary.csv",
            fg="gray"
        ).pack(pady=8)

    def start_calib(self):
        try:
            grid_block = int(self.grid_block_val.get())
        except Exception:
            self.status.config(text="Status: Invalid grid block", fg="red")
            return

        if grid_block < 1 or grid_block > 9:
            self.status.config(text="Status: Grid block must be 1-9", fg="red")
            return
        payload = {"grid_block": grid_block}

        try:
            r = requests.post(f"{CALIB_API_BASE}/start", json=payload, timeout=REQUEST_TIMEOUT_S)
            if r.status_code == 200:
                body = {}
                try:
                    body = r.json()
                except Exception:
                    body = {}
                samples = body.get("samples_per_scanner", 100)
                session_id = body.get("session_id", "")
                suffix = f" | {session_id}" if session_id else ""
                self.status.config(text=f"Status: Sampling block {grid_block} ({samples}/scanner){suffix}", fg="blue")
            else:
                detail = ""
                try:
                    body = r.json()
                    detail = f" - {body.get('status', body.get('message', ''))}"
                except Exception:
                    detail = ""
                self.status.config(text=f"Status: Server Error ({r.status_code}){detail}", fg="red")
        except requests.exceptions.RequestException:
            self.status.config(text="Status: Error Connecting to Server", fg="red")

class ControlTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)

        # Global Controls
        global_frame = tk.LabelFrame(self, text="Global Controls (All Scanners)", padx=10, pady=10)
        global_frame.pack(fill="x", padx=10, pady=5)

        tk.Button(global_frame, text="All IDLE (Sleep)", bg="#ff9999", command=lambda: self.send_cmd("all", state=0)).pack(side="left", padx=5)
        tk.Button(global_frame, text="All ACTIVE (Scan)", bg="#99ff99", command=lambda: self.send_cmd("all", state=1)).pack(side="left", padx=5)

        tk.Button(global_frame, text="All Auto Label Mode", command=lambda: self.send_cmd("all", mode=0)).pack(side="right", padx=5)

        # Scanner List Frame
        self.list_frame = tk.LabelFrame(self, text="Active Scanners", padx=10, pady=10)
        self.list_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.refresh_btn = tk.Button(self, text="Refresh List", command=self.refresh_scanners)
        self.refresh_btn.pack(pady=5)

        self.refresh_scanners()

    def refresh_scanners(self):
        for widget in self.list_frame.winfo_children():
            widget.destroy()

        try:
            r = requests.get(f"{API_BASE}/scanners", timeout=REQUEST_TIMEOUT_S)
            r.raise_for_status()
            scanners = r.json()

            if not isinstance(scanners, dict) or not scanners:
                tk.Label(self.list_frame, text="No active scanners found.").pack()
                return

            for s_id, info in sorted(scanners.items(), key=lambda item: str(item[0])):
                if not isinstance(info, dict):
                    info = {}
                ip = info.get("ip", "unknown")

                # BUG FIX: Moved borderwidth and relief inside tk.Frame()
                frame = tk.Frame(self.list_frame, pady=5, borderwidth=1, relief="solid")
                frame.pack(fill="x", pady=2)

                tk.Label(frame, text=f"Scanner {s_id} ({ip})", width=20, anchor="w").pack(side="left", padx=5)

                # Individual Controls
                tk.Button(frame, text="Idle", command=lambda id=s_id: self.send_cmd(id, state=0)).pack(side="left", padx=2)
                tk.Button(frame, text="Active", command=lambda id=s_id: self.send_cmd(id, state=1)).pack(side="left", padx=2)

                tk.Label(frame, text=" | Label:").pack(side="left", padx=5)
                tk.Button(frame, text="Auto", command=lambda id=s_id: self.send_cmd(id, mode=0)).pack(side="left", padx=2)
                tk.Button(frame, text="37", command=lambda id=s_id: self.send_cmd(id, mode=37)).pack(side="left", padx=2)
                tk.Button(frame, text="38", command=lambda id=s_id: self.send_cmd(id, mode=38)).pack(side="left", padx=2)
                tk.Button(frame, text="39", command=lambda id=s_id: self.send_cmd(id, mode=39)).pack(side="left", padx=2)

        except Exception as e:
            tk.Label(self.list_frame, text=f"Error connecting to server: {e}").pack()

    def send_cmd(self, target, state=None, mode=None):
        payload = {"target": target}
        if state is not None: payload["state"] = state
        if mode is not None: payload["mode"] = mode

        try:
            r = requests.post(f"{API_BASE}/send", json=payload, timeout=REQUEST_TIMEOUT_S)
            if r.status_code != 200:
                print(f"Command failed: HTTP {r.status_code} {r.text}")
                return False

            # New pc_receiver/cmd_server path returns JSON. Older versions may still
            # return plain text, so JSON parsing is optional.
            try:
                result = r.json()
                errors = {k: v for k, v in result.items() if str(v).lower() not in ("success", "ok")}
                if errors:
                    print(f"Command returned errors: {errors}")
                    return False
            except Exception:
                pass

            return True
        except Exception as e:
            print(f"Command failed: {e}")
            return False

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("BLE Grid Controller")
        self.geometry("600x500")

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=10)

        self.control_tab = ControlTab(self.notebook)
        self.notebook.add(self.control_tab, text="Scanner Control")

        self.calib_tab = CalibrationTab(self.notebook)
        self.notebook.add(self.calib_tab, text="Grid Calibration")

if __name__ == "__main__":
    app = App()
    app.mainloop()
