import time
import hashlib
import json
import os
import threading # Added for thread safety with pc_receiver.py

class IdentificationEngine:
    def __init__(self, authorized_file="main/authorized_devices.json"):
        self.authorized_file = authorized_file
        self.lock = threading.Lock() # Prevents race conditions during identity resolution
        self.physical_tracks = {}
        self.mac_to_uid = {}
        self.authorized_uids = self._load_authorized_list()

        self.agg_window = 10.0
        self.alphabet_size = 7
        self.rssi_min = -100
        self.rssi_max = -30

    def _load_authorized_list(self):
        if os.path.exists(self.authorized_file):
            try:
                with open(self.authorized_file, 'r') as f:
                    return set(json.load(f))
            except Exception:
                return set()
        return set()

    def generate_payload_dna(self, parsed):
        # Uses keys explicitly defined in ble_adv_parser.py
        mfg_id = str(parsed.get('mfg_id', '0000'))
        services = "|".join(sorted(parsed.get('services_16', [])))
        mfg_prefix = parsed.get('mfg_data_hex', '')[:4]
        
        dna_raw = f"{mfg_id}-{services}-{mfg_prefix}"
        return hashlib.md5(dna_raw.encode()).hexdigest()

    def process_event(self, mac, parsed_payload, rssi, scanner_id):
        now = time.time()
        dna = self.generate_payload_dna(parsed_payload)
        
        with self.lock: # Critical for pc_receiver.py's multi-threaded batch processing
            # 1. Resolve Identity
            uid = self.mac_to_uid.get(mac)
            if not uid:
                uid = self._find_match(dna)
                
            if not uid:
                uid = f"PHYS_{int(now * 1000)}"
                self.physical_tracks[uid] = {
                    "dna": dna,
                    "last_seen": now,
                    "rssi_buffers": {}
                }
            
            # 2. Update Track State
            self.mac_to_uid[mac] = uid
            track = self.physical_tracks[uid]
            track["last_seen"] = now
            
            if scanner_id not in track["rssi_buffers"]:
                track["rssi_buffers"][scanner_id] = []
            
            track["rssi_buffers"][scanner_id].append(rssi)
            if len(track["rssi_buffers"][scanner_id]) > 50:
                track["rssi_buffers"][scanner_id].pop(0)

            status = "AUTHORIZED" if uid in self.authorized_uids else "INTRUDER"
            self._cleanup_stale_data(now)

            return {
                "uid": uid,
                "status": status,
                "dna": dna
            }

    def _find_match(self, dna):
        now = time.time()
        for uid, track in self.physical_tracks.items():
            if track["dna"] == dna and (now - track["last_seen"]) < 900:
                return uid
        return None

    def _cleanup_stale_data(self, now):
        stale_uids = [u for u, t in self.physical_tracks.items() if (now - t["last_seen"]) > 1200]
        for u in stale_uids:
            del self.physical_tracks[u]
        self.mac_to_uid = {m: uid for m, uid in self.mac_to_uid.items() if uid in self.physical_tracks}

    def reload_authorized(self):
        with self.lock:
            self.authorized_uids = self._load_authorized_list()