import tkinter as tk
from tkinter import ttk
import requests

API_BASE = "http://localhost:8000/api/control"
CALIB_API_BASE = "http://localhost:8000/api/calibrate"

class CalibrationTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        
        tk.Label(self, text="3D Calibration Point", font=('Arial', 12, 'bold')).pack(pady=10)
        
        # Coordinates
        self.x_val = tk.DoubleVar(value=0.0)
        self.y_val = tk.DoubleVar(value=0.0)
        self.z_val = tk.DoubleVar(value=1.0) # Default pocket height

        frame_coords = tk.Frame(self)
        frame_coords.pack(pady=10)

        tk.Label(frame_coords, text="X Position (m):").grid(row=0, column=0, padx=5, pady=5)
        tk.Entry(frame_coords, textvariable=self.x_val, width=10).grid(row=0, column=1, padx=5, pady=5)
        
        tk.Label(frame_coords, text="Y Position (m):").grid(row=1, column=0, padx=5, pady=5)
        tk.Entry(frame_coords, textvariable=self.y_val, width=10).grid(row=1, column=1, padx=5, pady=5)
        
        tk.Label(frame_coords, text="Z Position (m):").grid(row=2, column=0, padx=5, pady=5)
        tk.Entry(frame_coords, textvariable=self.z_val, width=10).grid(row=2, column=1, padx=5, pady=5)

        self.btn = tk.Button(self, text="Start Sampling Point", command=self.start_calib, bg="green", fg="white", font=('Arial', 10, 'bold'))
        self.btn.pack(pady=20)
        
        self.status = tk.Label(self, text="Status: Ready", font=('Arial', 10))
        self.status.pack()

    def start_calib(self):
        payload = {
            "coords": {"x": self.x_val.get(), "y": self.y_val.get(), "z": self.z_val.get()}
        }
        try:
            r = requests.post(f"{CALIB_API_BASE}/start", json=payload, timeout=2)
            if r.status_code == 200:
                self.status.config(text="Status: Sampling... Stay still!", fg="blue")
            else:
                self.status.config(text=f"Status: Server Error ({r.status_code})", fg="red")
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
        
        tk.Button(global_frame, text="All Auto Mode (Rotate)", command=lambda: self.send_cmd("all", mode=0)).pack(side="right", padx=5)

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
            r = requests.get(f"{API_BASE}/scanners", timeout=2)
            scanners = r.json()
            
            if not scanners:
                tk.Label(self.list_frame, text="No active scanners found.").pack()
                return
                
            for s_id, info in scanners.items():
                # BUG FIX: Moved borderwidth and relief inside tk.Frame()
                frame = tk.Frame(self.list_frame, pady=5, borderwidth=1, relief="solid")
                frame.pack(fill="x", pady=2)
                
                tk.Label(frame, text=f"Scanner {s_id} ({info['ip']})", width=20, anchor="w").pack(side="left", padx=5)
                
                # Individual Controls
                tk.Button(frame, text="Idle", command=lambda id=s_id: self.send_cmd(id, state=0)).pack(side="left", padx=2)
                tk.Button(frame, text="Active", command=lambda id=s_id: self.send_cmd(id, state=1)).pack(side="left", padx=2)
                
                tk.Label(frame, text=" | Ch:").pack(side="left", padx=5)
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
            requests.post(f"{API_BASE}/send", json=payload, timeout=2)
        except Exception as e:
            print(f"Command failed: {e}")

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
        self.notebook.add(self.calib_tab, text="3D Calibration")

if __name__ == "__main__":
    app = App()
    app.mainloop()