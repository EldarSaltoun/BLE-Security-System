import tkinter as tk
from tkinter import ttk, messagebox
import requests

API_BASE = "http://127.0.0.1:8000/api/control"

class ScannerControlPanel:
    def __init__(self, root):
        self.root = root
        self.root.title("BLE Scanner Control Panel")
        self.root.geometry("550x450")
        
        # --- Global Controls (All Boards) ---
        global_frame = ttk.LabelFrame(root, text="Global State Control (All Boards)", padding=10)
        global_frame.pack(fill="x", padx=10, pady=10)
        
        ttk.Button(global_frame, text="Set ALL ACTIVE", command=lambda: self.send_cmd("all", state=1)).pack(side="left", padx=5)
        ttk.Button(global_frame, text="Set ALL IDLE", command=lambda: self.send_cmd("all", state=0)).pack(side="left", padx=5)

        # --- Per-Board Controls ---
        board_frame = ttk.LabelFrame(root, text="Active Boards Configuration", padding=10)
        board_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.list_frame = ttk.Frame(board_frame)
        self.list_frame.pack(fill="both", expand=True)

        ttk.Button(board_frame, text="Refresh Active Boards", command=self.refresh_boards).pack(pady=10)
        
        self.refresh_boards()

    def refresh_boards(self):
        # Clear existing rows
        for widget in self.list_frame.winfo_children():
            widget.destroy()
            
        try:
            resp = requests.get(f"{API_BASE}/scanners", timeout=2)
            scanners = resp.json()
            
            if not scanners:
                ttk.Label(self.list_frame, text="No active boards detected.").pack()
                return

            # Header row
            header = ttk.Frame(self.list_frame)
            header.pack(fill="x", pady=5)
            ttk.Label(header, text="Board ID (IP Address)", width=30, font=("TkDefaultFont", 10, "bold")).pack(side="left")
            ttk.Label(header, text="Set Mode", font=("TkDefaultFont", 10, "bold")).pack(side="left", padx=5)

            # Create a row for each active scanner
            for s_id, data in scanners.items():
                row = ttk.Frame(self.list_frame)
                row.pack(fill="x", pady=2)
                
                ttk.Label(row, text=f"Scanner {s_id} ({data['ip']})", width=30).pack(side="left")
                
                # Mode Selection Dropdown
                mode_var = tk.StringVar(value="Select Mode")
                modes = {"Auto (1s)": 0, "Fixed 37": 37, "Fixed 38": 38, "Fixed 39": 39}
                dropdown = ttk.OptionMenu(row, mode_var, "Select Mode", *modes.keys(), 
                                          command=lambda selection, id=s_id, m=modes: self.send_cmd(id, mode=m[selection]))
                dropdown.pack(side="left", padx=5)
                
        except Exception as e:
            ttk.Label(self.list_frame, text=f"Error connecting to Receiver: Make sure pc_receiver.py is running!").pack()
            print(f"[ERROR] {e}")

    def send_cmd(self, target, state=None, mode=None):
        payload = {"target": target}
        if state is not None: payload["state"] = state
        if mode is not None: payload["mode"] = mode
        
        try:
            resp = requests.post(f"{API_BASE}/send", json=payload, timeout=3)
            results = resp.json()
            print(f"[INFO] Command results: {results}")
            
            if target == "all":
                messagebox.showinfo("Result", "Global command sent. Check terminal for specific board results.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to send command: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = ScannerControlPanel(root)
    root.mainloop()