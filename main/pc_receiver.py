import base64
import csv
import hashlib
import json
import logging
import math
import os
import queue
import socket
import threading
import time
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, Response, jsonify, request
from zeroconf import ServiceInfo, Zeroconf

# Import the AdvParser for calibration, UI display, and payload fingerprinting
from ble_adv_parser import AdvParser

# Silence Flask logs for a cleaner terminal
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

app = Flask(__name__)

# ---------------- Runtime queues/state ----------------

# Queue for processed peak-RSSI events sent to the UI stream
data_queue = queue.Queue(maxsize=5000)

# Active scanner registry: scanner_id -> {ip, last_seen}
active_scanners: Dict[str, Dict[str, Any]] = {}

# Buffering for windowed processing
event_buffer = []
buffer_lock = threading.Lock()

# Parsed-payload cache to avoid decoding the same advertisement repeatedly
parse_cache: Dict[str, Dict[str, Any]] = {}
parse_cache_lock = threading.Lock()
PARSE_CACHE_MAX = 5000

# Robust absolute pathing
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Bluetooth SIG Company Identifier lookup loaded from mfg_ids.csv.
# The CSV format is:
#   HEX_ID,Company Name
MFG_NAMES: Dict[int, str] = {}

# ---------------- Filtering settings ----------------

# Wait for events to be at least this old before processing, to absorb Wi-Fi/network jitter.
SAFETY_MARGIN_US = 200_000      # 200 ms
WINDOW_SIZE_US = 100_000        # 100 ms

# ---------------- Real-time device tracking settings ----------------

TRACKER_ENABLED = True

# Rolling-memory window for each physical device fingerprint.
TRACKER_MEMORY_SEC = 20.0

# Track confirmation rules. Candidate tracks still stream to UI, but "CONFIRMED"
# means enough evidence exists to count them as real in-room physical devices.
CONFIRM_MIN_PACKETS = 40
CONFIRM_MIN_SCANNERS = 3
CONFIRM_MIN_DURATION_SEC = 2.5

# Confirmation quality rules.
# A device can be detected for a long time even if it is outside the room.
# These RSSI gates keep weak/outside devices as CANDIDATE instead of CONFIRMED.
CONFIRM_MIN_STRONGEST_RSSI_DBM = -88.0
CONFIRM_MIN_TOP2_AVG_RSSI_DBM = -92.0

# If a track has a known class such as Apple/Samsung/Microsoft, Unknown packets
# are allowed to attach, but they should not be enough by themselves to confirm
# the physical device as a reliable known in-room track.
CONFIRM_MIN_KNOWN_PACKETS = 40
CONFIRM_MIN_KNOWN_RATIO = 0.20

# ---------------- Optional weak/outside labeling ----------------
# We keep the public status as CANDIDATE/CONFIRMED for compatibility, but expose
# confirm_block_reason in /api/devices so weak/outside tracks are easy to filter.
WEAK_TRACK_LABEL = "WEAK_OR_OUTSIDE"

# ---------------- Passive idle-phone / burst tracking ----------------
# Phones with screen off often do not advertise continuously. They can appear as
# short bursts with silence in between, sometimes with rotating MAC/payloads.
# Keep identity memory much longer than the live display timeout so later bursts
# can reconnect to the same physical track.
LIVE_ACTIVE_TIMEOUT_SEC = 15.0
IDENTITY_MEMORY_SEC = 600.0
BURST_GAP_SEC = 3.0
MIN_BURSTS_FOR_INTERMITTENT_PHONE = 2
MIN_PACKETS_PER_PHONE_BURST = 3
PHONE_LIKE_SCORE_THRESHOLD = 4.0

# A persistent weak 1-2 scanner source is usually an outside fixed device, not a
# phone. PD_002 in your test is exactly this pattern: outside Samsung TV.
OUTSIDE_STABLE_MIN_AGE_SEC = 45.0
OUTSIDE_STABLE_MAX_SCANNERS = 2
OUTSIDE_STABLE_MAX_STRONGEST_RSSI_DBM = -82.0
OUTSIDE_STABLE_MIN_PACKETS = 80
OUTSIDE_STABLE_MIN_KNOWN_RATIO = 0.50

# Stable fixed-device role.
# This catches continuous, dense, fixed-payload devices such as TVs/laptops/tags
# so they are not mislabeled as PHONE_LIKE only because they have multiple bursts.
STABLE_DEVICE_MIN_AGE_SEC = 45.0
STABLE_DEVICE_MIN_PACKETS = 120
STABLE_DEVICE_MIN_AVG_PACKETS_PER_BURST = 30.0
STABLE_DEVICE_MAX_MACS = 2
STABLE_DEVICE_MAX_PAYLOADS = 2

# Phone-like tuning.
# A phone-like natural BLE source usually has rotating aliases OR repeated short
# bursts. A continuous one-MAC/one-payload source should be STABLE_DEVICE.
PHONE_BURSTY_MAX_AVG_BURST_DURATION_SEC = 8.0
PHONE_BURSTY_MAX_AVG_PACKETS_PER_BURST = 25.0
PHONE_ROTATION_MIN_MACS_OR_PAYLOADS = 2

# ---------------- Mobile service-data tracking ----------------
# Test result:
#   Samsung phone near scanner 2 appeared mainly as Unknown service-data
#   FCF1 / FEF3, not as Samsung MFG_0075.
#
# These UUIDs are not treated as "Samsung". They are treated as mobile/service
# identity evidence so they can form their own physical track and avoid polluting
# Samsung/Apple/Microsoft tracks.
MOBILE_SERVICE_UUIDS = {"FCF1", "FEF3"}
MOBILE_SERVICE_LABEL = "MOBILE_SERVICE_DATA"

MOBILE_SERVICE_SCORE_THRESHOLD = 5.0
MOBILE_SERVICE_HIGH_SCORE_THRESHOLD = 7.0
MOBILE_SERVICE_MIN_PACKETS = 40
MOBILE_SERVICE_MIN_SCANNERS = 3
MOBILE_SERVICE_STRONG_RSSI_DBM = -70.0
MOBILE_SERVICE_TOP2_AVG_DBM = -74.0
MOBILE_SERVICE_MIN_UNKNOWN_RATIO = 0.35

# Strict guard for Unknown mobile service-data trying to attach to a known
# Samsung/Apple/Microsoft/Laptop track. This is the main anti-pollution fix.
MOBILE_TO_KNOWN_REQUIRE_SAME_STRONGEST = True
MOBILE_TO_KNOWN_REQUIRE_TOP2_OVERLAP = True
MOBILE_TO_KNOWN_MAX_REL_RMSE_DB = 2.8
MOBILE_TO_KNOWN_MAX_ABS_RMSE_DB = 5.5
MOBILE_TO_KNOWN_MAX_PER_SCANNER_DIFF_DB = 6.0

# Mobile-service aliases may merge with other mobile-service aliases if their
# spatial fingerprints are very close. This is intended to join PD_002/PD_050
# style FCF1/FEF3 splits into one physical mobile-service source.
MOBILE_TO_MOBILE_MAX_REL_RMSE_DB = 4.2
MOBILE_TO_MOBILE_MAX_ABS_RMSE_DB = 8.0
MOBILE_TO_MOBILE_MAX_PER_SCANNER_DIFF_DB = 9.0
MOBILE_TO_MOBILE_REQUIRE_SAME_STRONGEST = True
MOBILE_TO_MOBILE_REQUIRE_TOP2_OVERLAP = True

# Stale tracks are removed after not being seen for this long from the live map,
# but physical identities are retained until IDENTITY_MEMORY_SEC.
TRACK_STALE_SEC = 12.0

# Main assignment threshold. Lower = stricter/more devices. Higher = looser/fewer devices.
# V2 is intentionally stricter because a room can contain many Apple/Samsung devices.
MATCH_THRESHOLD = 9.0

# New aliases are allowed to mature briefly before they are attached to an existing
# physical device. This prevents one vendor-wide Apple/Samsung bucket.
ALIAS_MIN_PACKETS_FOR_MATCH = 6
ALIAS_MIN_SCANNERS_FOR_MATCH = 2
ALIAS_MATURITY_TIMEOUT_SEC = 1.5

# If a track is known Apple/Samsung/Microsoft/Calibration and the incoming alias is a different
# known class, do not merge unless it is an exact MAC+payload already assigned.
STRICT_METADATA_CONFLICTS = True

# For same-vendor devices, metadata alone must not merge tracks.
# Require spatial evidence across at least this many common scanners.
MIN_COMMON_SCANNERS_STRONG_MATCH = 2
MIN_ALIAS_PACKETS_ONE_SCANNER = 25

# V4 universal anti-overmerge gates.
# Different MAC+payload aliases are only joined when there is strong spatial and time evidence.
# V5 balance:
# Different aliases may belong to the same physical device, but only with strong
# spatial evidence. Timestamp overlap is a penalty, not an automatic rejection,
# because one physical device may advertise several payload families.
MAX_RSSI_RMSE_DIFFERENT_ALIAS_DB = 6.5
MAX_ABS_RSSI_RMSE_DIFFERENT_ALIAS_DB = 11.0
REQUIRE_TOP2_SCANNER_OVERLAP = True
HIGH_ALIAS_OVERLAP_RATIO = 0.75

# Prevent one vendor-wide bucket.
# Once a track already contains several aliases, a new different alias must match
# the existing cluster more tightly.
LARGE_TRACK_ALIAS_COUNT = 4
MAX_RSSI_RMSE_LARGE_TRACK_DB = 5.2
MAX_ABS_RSSI_RMSE_LARGE_TRACK_DB = 9.5

# Unknown advertisements may belong to a known device, but they are also the main
# path by which unrelated packets got absorbed into Samsung/Apple tracks.
MAX_RSSI_RMSE_UNKNOWN_TO_KNOWN_DB = 4.5
MAX_ABS_RSSI_RMSE_UNKNOWN_TO_KNOWN_DB = 8.0

# Per-scanner RSSI anti-merge gates.
# These catch the case where average/RMSE looks close enough, but one scanner
# clearly sees the two devices very differently.
MAX_PER_SCANNER_RSSI_DIFF_DB = 12.0

# Layered physical-track merge policy.
# Candidate <-> candidate can use the normal gates above.
# Candidate <-> confirmed is stricter.
# Confirmed <-> confirmed is very strict because two already confirmed devices
# should not merge unless the evidence is extremely strong.
CANDIDATE_CONFIRMED_MERGE_REL_RMSE_DB = 5.0
CANDIDATE_CONFIRMED_MERGE_ABS_RMSE_DB = 9.0
CANDIDATE_CONFIRMED_MERGE_MAX_PER_SCANNER_DIFF_DB = 10.0

CONFIRMED_MERGE_REL_RMSE_DB = 3.5
CONFIRMED_MERGE_ABS_RMSE_DB = 6.0
CONFIRMED_MERGE_MAX_PER_SCANNER_DIFF_DB = 7.0
CONFIRMED_MERGE_REQUIRE_SAME_STRONGEST = True
CONFIRMED_MERGE_REQUIRE_TOP2_EXACT = True

# Advertising interval compatibility.
# If both sides have enough samples and their mean advertising intervals differ
# by more than 100 ms, they should not merge. This separates devices advertising
# around 100 ms from devices advertising around 250 ms.
MIN_INTERVAL_SAMPLES_FOR_COMPARE = 5
MAX_ADV_INTERVAL_MEAN_DIFF_MS = 100.0
MAX_ADV_INTERVAL_RATIO = 1.8

# Confirmed-confirmed overlap protection.
# If two confirmed tracks have different MAC sets and their active windows heavily
# overlap, treat them as separate unless they also pass the very strict RSSI gates.
CONFIRMED_HIGH_OVERLAP_REJECT_RATIO = 0.85

# Track drift / bucket-growth protection.
# A track must become stricter as it becomes larger/confirmed, not looser.
# This prevents "chain merging" where A is close to B, B is close to C,
# but A and C are actually different physical devices.
TRACK_RADIUS_GUARD_MIN_ALIAS_FEATURES = 3

LARGE_TRACK_MAX_REL_RADIUS_DB = 5.2
LARGE_TRACK_MAX_ABS_RADIUS_DB = 9.5
LARGE_TRACK_MAX_PER_SCANNER_DIFF_DB = 10.0

CONFIRMED_TRACK_MAX_REL_RADIUS_DB = 4.0
CONFIRMED_TRACK_MAX_ABS_RADIUS_DB = 7.0
CONFIRMED_TRACK_MAX_PER_SCANNER_DIFF_DB = 8.0

# Unknown payloads may attach to a known-class track only with very strong evidence,
# and they are not allowed to expand that track's identity/core fingerprint.
UNKNOWN_TO_KNOWN_MAX_REL_RADIUS_DB = 3.5
UNKNOWN_TO_KNOWN_MAX_ABS_RADIUS_DB = 7.0
UNKNOWN_TO_KNOWN_MAX_PER_SCANNER_DIFF_DB = 7.0
UNKNOWN_ALIAS_CAN_EXPAND_KNOWN_TRACK_CORE = False

# Once both tracks are confirmed, do not merge them just because RSSI is plausible.
# Same MAC is still allowed earlier as a direct merge reason. Different-MAC
# confirmed-confirmed merges must also pass the track-radius guard.
DISABLE_CONFIRMED_CONFIRMED_DIFFERENT_MAC_MERGE = False

# ---------------- Calibration settings ----------------

CALIBRATION_TARGET = "eldarcalib"
SAMPLES_PER_CHANNEL = 10

calib_state = {
    "active": False,
    "target_mac": None,             # Optional manual MAC or first matching MAC
    "target_payload_sig": None,     # Allows calibration to survive MAC rotation
    "coords": {"x": 0.0, "y": 0.0, "z": 0.0},
    "buckets": {},                  # scanner_id -> {37: [], 38: [], 39: []}
    "last_progress_print": 0.0,
}

calib_lock = threading.Lock()

# ---------------- Diagnostics ----------------

stats_lock = threading.Lock()
stats = {
    "ingest_events": 0,
    "processed_events": 0,
    "streamed_events": 0,
    "dropped_queue_full": 0,
    "parse_errors": 0,
    "bad_events": 0,
    "tracker_tracks": 0,
    "tracker_confirmed": 0,
    "tracker_rejected_class_conflicts": 0,
    "tracker_rejected_track_expansion": 0,
    "tracker_blocked_confirmation_weak_rssi": 0,
    "tracker_blocked_confirmation_unknown_heavy": 0,
    "tracker_phone_like_tracks": 0,
    "tracker_outside_stable_tracks": 0,
    "tracker_stable_device_tracks": 0,
    "tracker_mobile_service_tracks": 0,
}


# ---------------- mDNS ----------------

def start_mdns(ip_address: str, port: int) -> Zeroconf:
    desc = {"path": "/api/ble/ingest"}
    info = ServiceInfo(
        "_ble-ingest._tcp.local.",
        "Grid-Receiver._ble-ingest._tcp.local.",
        addresses=[socket.inet_aton(ip_address)],
        port=port,
        properties=desc,
        server="grid-server.local.",
    )
    zc = Zeroconf()
    zc.register_service(info)
    print(f"[mDNS] Broadcaster active: grid-server.local at {ip_address}:{port}")
    return zc


# ---------------- Utility ----------------

def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def load_mfg_ids(filename: str = "mfg_ids.csv") -> None:
    """
    Load Bluetooth Company Identifier names from mfg_ids.csv.

    This does not affect identity directly. It only improves labels and gives
    classify_metadata() more information than hardcoded Apple/Samsung/Microsoft.
    """
    global MFG_NAMES

    path = os.path.join(BASE_DIR, filename)
    loaded = {}

    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue

                raw_id = str(row[0]).strip()
                name = str(row[1]).strip()

                if not raw_id or not name:
                    continue

                try:
                    cid = int(raw_id, 16)
                except ValueError:
                    try:
                        cid = int(raw_id, 10)
                    except ValueError:
                        continue

                # Ignore placeholder names from old partial lists.
                if name.lower().startswith("company_"):
                    continue

                loaded[cid] = name

        MFG_NAMES = loaded
        print(f"[MFG] Loaded {len(MFG_NAMES)} Bluetooth company identifiers from {path}")
    except Exception as e:
        MFG_NAMES = {}
        print(f"[MFG] Could not load {path}: {e}")


def mfg_name_from_id(mfg_id: Any) -> str:
    if not isinstance(mfg_id, int):
        return ""

    if mfg_id == 0xFFFF:
        return ""

    return MFG_NAMES.get(mfg_id, "")


def normalize_label_text(text_value: str, max_len: int = 32) -> str:
    text_value = str(text_value or "").strip()
    if not text_value:
        return ""
    cleaned = []
    for ch in text_value:
        if ch.isalnum():
            cleaned.append(ch)
        elif ch in (" ", "-", "_", "/", "."):
            cleaned.append("_")
    out = "".join(cleaned).strip("_")
    while "__" in out:
        out = out.replace("__", "_")
    return out[:max_len] if out else ""


def is_known_metadata_class(cls: str) -> bool:
    return bool(cls) and cls != "Unknown"


def payload_signature(payload_b64: str) -> str:
    """
    Short stable signature for the raw payload string.
    This is not a physical-device ID. It is only a packet/payload clue.
    """
    if not payload_b64:
        return "EMPTY"
    return hashlib.sha1(payload_b64.encode("utf-8", errors="ignore")).hexdigest()[:12].upper()


def parse_payload(payload_b64: str) -> Dict[str, Any]:
    """
    Decode and parse a base64 BLE advertisement payload.
    Uses a bounded cache because many packets repeat the same payload.
    """
    if not payload_b64:
        return {
            "name": "Unknown",
            "mfg_id": None,
            "mfg_data_hex": "",
            "mfg_name": "",
            "tx_pwr": None,
            "services_16": [],
            "services_128": [],
            "services_32": [],
            "service_data": [],
            "ad_structure": "",
            "adv_len": 0,
            "payload_sig": "EMPTY",
        }

    with parse_cache_lock:
        cached = parse_cache.get(payload_b64)
        if cached is not None:
            return cached

    result = {
        "name": "Unknown",
        "mfg_id": None,
        "mfg_data_hex": "",
        "tx_pwr": None,
        "services_16": [],
        "services_128": [],
        "services_32": [],
        "service_data": [],
        "ad_structure": "",
        "adv_len": 0,
        "payload_sig": payload_signature(payload_b64),
    }

    try:
        raw_payload = base64.b64decode(payload_b64, validate=False)
        parsed = AdvParser.parse(raw_payload)

        result.update({
            "name": parsed.get("name", "Unknown") or "Unknown",
            "mfg_id": parsed.get("mfg_id"),
            "mfg_data_hex": parsed.get("mfg_data_hex", "") or "",
            "mfg_name": mfg_name_from_id(parsed.get("mfg_id")),
            "tx_pwr": parsed.get("tx_pwr"),
            "services_16": parsed.get("services_16", []) or [],
            "services_128": parsed.get("services_128", []) or [],
            "services_32": parsed.get("services_32", []) or [],
            "service_data": parsed.get("service_data", []) or [],
            "ad_structure": parsed.get("ad_structure", "") or "",
            "adv_len": len(raw_payload),
            "payload_sig": parsed.get("payload_sig", payload_signature(payload_b64)),
        })
    except Exception:
        with stats_lock:
            stats["parse_errors"] += 1

    with parse_cache_lock:
        if len(parse_cache) >= PARSE_CACHE_MAX:
            # Simple bounded cache policy: clear all.
            # Good enough here because payload parsing is cheap and repeated payloads refill quickly.
            parse_cache.clear()
        parse_cache[payload_b64] = result

    return result


def classify_metadata(parsed: Dict[str, Any]) -> str:
    """
    Coarse metadata class.

    This is NOT the physical identity. It is only used for:
      1. avoiding impossible merges, and
      2. making labels less "Unknown".

    Manufacturer name is loaded from mfg_ids.csv when available.
    """
    name = str(parsed.get("name", "") or "").lower()
    mfg = parsed.get("mfg_id")
    mfg_name = str(parsed.get("mfg_name", "") or mfg_name_from_id(mfg)).lower()

    # Only the explicit EldarCalib advertisement is calibration.
    if "eldarcalib" in name:
        return "Calibration"

    # Tondo-like devices are not calibration.
    if "tondo" in name:
        return "Tondo/Beacon"

    services_16 = {str(x).upper() for x in (parsed.get("services_16") or [])}
    services_128 = {str(x).upper() for x in (parsed.get("services_128") or [])}

    # Important:
    # This Windows/Microsoft-style service family often arrives without MFG data.
    # If we leave it as Unknown, RSSI similarity can wrongly pull it into Samsung/Apple.
    if "180A" in services_16 and "61CE1C20-E8BC-4287-91FD-7CC25F0DF500" in services_128:
        return "Microsoft/Laptop"

    # Strong common classes.
    combined = f"{name} {mfg_name}"

    if "apple" in combined or "iphone" in combined or "ipad" in combined:
        return "Apple"

    if "samsung" in combined:
        # We keep the older label for compatibility with your UI, but this means
        # "Samsung-like", not necessarily one TV.
        return "Samsung"

    if "microsoft" in combined or "windows" in combined or "laptop" in combined:
        return "Microsoft/Laptop"

    if "google" in combined:
        return "Google"

    if "sony" in combined:
        return "Sony"

    if "lg electronics" in combined or combined.strip() == "lg":
        return "LG"

    # If we know the official company name, expose it as a class/label.
    if isinstance(mfg, int):
        official = mfg_name_from_id(mfg)
        cleaned = normalize_label_text(official)
        if cleaned:
            return f"MFG:{cleaned}"

    return "Unknown"


def normalize_uuid16(value: Any) -> str:
    """
    Normalize a 16-bit BLE UUID/service identifier into four uppercase hex chars.
    Accepts forms like "FCF1", "0xFCF1", 0xFCF1, or dict-like parser outputs.
    """
    if isinstance(value, dict):
        for key in ("uuid", "uuid16", "service_uuid", "service"):
            if key in value:
                return normalize_uuid16(value.get(key))
        return ""

    if isinstance(value, int):
        if 0 <= value <= 0xFFFF:
            return f"{value:04X}"
        return ""

    s = str(value or "").strip().upper()
    if not s:
        return ""

    if s.startswith("0X"):
        s = s[2:]

    # Some parser outputs may include punctuation. Keep hex characters only.
    s = "".join(ch for ch in s if ch in "0123456789ABCDEF")
    if len(s) >= 4:
        return s[-4:]
    return ""


def service_data_uuids(parsed: Dict[str, Any]) -> set:
    """
    Extract service-data UUIDs from parsed BLE data.

    The parser may return service_data as strings, integers, tuples, or dicts,
    depending on AdvParser version. This function is intentionally defensive.
    """
    out = set()
    for item in (parsed.get("service_data") or []):
        if isinstance(item, dict):
            uid = normalize_uuid16(item)
            if uid:
                out.add(uid)
            continue

        if isinstance(item, (list, tuple)) and item:
            uid = normalize_uuid16(item[0])
            if uid:
                out.add(uid)
            continue

        uid = normalize_uuid16(item)
        if uid:
            out.add(uid)

    return out


def is_mobile_service_parsed(parsed: Dict[str, Any]) -> bool:
    return bool(service_data_uuids(parsed) & MOBILE_SERVICE_UUIDS)


def mobile_service_uuids_from_sigs(payload_sigs: set) -> set:
    """
    Extract SD[...] UUIDs from our compact payload signatures.
    Example:
      MFG_NONE;...;SD[FCF1];...
    """
    out = set()
    for sig in payload_sigs or []:
        s = str(sig)
        start = s.find("SD[")
        if start < 0:
            continue
        end = s.find("]", start)
        if end < 0:
            continue
        inside = s[start + 3:end]
        for part in inside.replace("|", ",").split(","):
            uid = normalize_uuid16(part)
            if uid:
                out.add(uid)
    return out & MOBILE_SERVICE_UUIDS


def relative_vector(rssi_by_scanner: Dict[str, float]) -> Dict[str, float]:
    if not rssi_by_scanner:
        return {}
    mx = max(rssi_by_scanner.values())
    return {k: v - mx for k, v in rssi_by_scanner.items()}


def rmse_common(a: Dict[str, float], b: Dict[str, float]) -> Optional[float]:
    common = sorted(set(a.keys()) & set(b.keys()))
    if not common:
        return None
    err2 = [(a[k] - b[k]) ** 2 for k in common]
    return math.sqrt(sum(err2) / len(err2))


def strongest_scanner(rssi_by_scanner: Dict[str, float]) -> Optional[str]:
    if not rssi_by_scanner:
        return None
    return max(rssi_by_scanner.items(), key=lambda kv: kv[1])[0]


def top_scanners(rssi_by_scanner: Dict[str, float], n: int = 2) -> set:
    if not rssi_by_scanner:
        return set()
    return {
        k for k, _ in sorted(
            rssi_by_scanner.items(),
            key=lambda kv: kv[1],
            reverse=True
        )[:n]
    }


def time_overlap_ratio(a_first: float, a_last: float, b_first: float, b_last: float) -> float:
    a_dur = max(0.001, a_last - a_first)
    b_dur = max(0.001, b_last - b_first)
    overlap = max(0.0, min(a_last, b_last) - max(a_first, b_first))
    return overlap / max(0.001, min(a_dur, b_dur))


def rssi_distance_pair(a_raw: Dict[str, float], b_raw: Dict[str, float]) -> Tuple[Optional[float], Optional[float], int]:
    ar = relative_vector(a_raw)
    br = relative_vector(b_raw)
    common = len(set(ar.keys()) & set(br.keys()))
    return rmse_common(ar, br), rmse_common(a_raw, b_raw), common


def max_abs_rssi_diff_common(a_raw: Dict[str, float], b_raw: Dict[str, float]) -> Optional[float]:
    common = sorted(set(a_raw.keys()) & set(b_raw.keys()))
    if not common:
        return None
    return max(abs(a_raw[k] - b_raw[k]) for k in common)


def top_scanner_set(rssi_by_scanner: Dict[str, float], n: int = 2) -> set:
    return top_scanners(rssi_by_scanner, n)


def adv_interval_compatible(mean_a: Optional[float], count_a: int,
                            mean_b: Optional[float], count_b: int) -> bool:
    """
    True when interval evidence is unavailable/immature or compatible.
    False only when both sides have enough samples and clearly differ.
    """
    if mean_a is None or mean_b is None:
        return True

    if count_a < MIN_INTERVAL_SAMPLES_FOR_COMPARE or count_b < MIN_INTERVAL_SAMPLES_FOR_COMPARE:
        return True

    diff = abs(mean_a - mean_b)
    if diff > MAX_ADV_INTERVAL_MEAN_DIFF_MS:
        return False

    low = max(1.0, min(mean_a, mean_b))
    high = max(mean_a, mean_b)
    if high / low > MAX_ADV_INTERVAL_RATIO:
        return False

    return True


def jaccard_distance(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    union = len(a | b)
    if union == 0:
        return 0.0
    return 1.0 - (len(a & b) / union)


# ---------------- Real-time DeviceTracker ----------------

class DeviceTrack:
    def __init__(self, uid: str, first_ts_us: int, now_mono: float):
        self.uid = uid
        self.created_ts_us = int(first_ts_us)
        self.first_seen_mono = now_mono
        self.last_seen_mono = now_mono
        self.packet_count = 0

        self.aliases = set()
        self.macs = set()
        self.payload_sigs = set()
        self.mfg_ids = set()
        self.names = set()
        self.metadata_classes = defaultdict(int)
        self.mobile_service_uuids = set()
        self.mobile_service_packet_count = 0

        # alias_key -> compact spatial fingerprint of that alias while it was mature.
        # Used to prevent one large same-vendor bucket from swallowing multiple devices.
        self.alias_features: Dict[str, Dict[str, Any]] = {}

        # Rolling observations for 20 seconds:
        # each item: (mono_time, scanner, channel, rssi)
        self.obs = deque()

        # Last-known spatial fingerprint is retained beyond the rolling RSSI
        # window so INACTIVE/INTERMITTENT phone-memory tracks remain useful in
        # /api/devices after live observations are pruned.
        self.last_known_rssi_vals: Dict[str, List[int]] = defaultdict(list)
        self.last_known_scanner_rssi: Dict[str, float] = {}
        self.last_known_scanner_update_mono = now_mono

        # For interval estimation.
        # IMPORTANT:
        # Scanner ts is local to each ESP32 and resets when that scanner reboots.
        # Therefore, intervals are calculated only within the same (alias, scanner) stream.
        self.last_alias_scanner_ts_us: Dict[Tuple[str, str], int] = {}
        self.adv_intervals_ms = deque(maxlen=50)

        self.confirmed = False

        # Burst/presence tracking for passive idle-phone detection.
        self.burst_count = 0
        self.current_burst_packets = 0
        self.current_burst_start_mono = now_mono
        self.current_burst_end_mono = now_mono
        self.burst_packet_counts = deque(maxlen=40)
        self.burst_durations_sec = deque(maxlen=40)
        self.burst_scanner_sets = deque(maxlen=40)
        self.burst_rssi_maps = deque(maxlen=40)
        self.current_burst_scanners = set()
        self.current_burst_rssi_vals: Dict[str, List[int]] = defaultdict(list)

    def can_accept_known_classes(self, incoming_known: set) -> bool:
        own_known = self.known_classes()
        if STRICT_METADATA_CONFLICTS and incoming_known and own_known:
            return not incoming_known.isdisjoint(own_known)
        return True

    def update(self, ev: Dict[str, Any], parsed: Dict[str, Any], alias_key: str) -> bool:
        now_mono = time.monotonic()
        ts_us = safe_int(ev.get("ts"), 0)
        scanner = str(ev.get("scanner", "UNK"))
        channel = safe_int(ev.get("channel"), 0)
        rssi = safe_int(ev.get("rssi"), 0)
        mac = str(ev.get("mac", "UNK")).upper()
        payload_sig = parsed.get("payload_sig", payload_signature(ev.get("payload", "")))
        name = str(parsed.get("name", "Unknown") or "Unknown").strip()
        mfg = parsed.get("mfg_id")
        meta_class = classify_metadata(parsed)
        mobile_uuids = service_data_uuids(parsed) & MOBILE_SERVICE_UUIDS
        incoming_known = {meta_class} if is_known_metadata_class(meta_class) else set()
        if not self.can_accept_known_classes(incoming_known):
            return False

        self._update_burst(now_mono, scanner, rssi)

        self.last_seen_mono = now_mono
        self.packet_count += 1
        self.aliases.add(alias_key)
        self.macs.add(mac)
        self.payload_sigs.add(payload_sig)

        if isinstance(mfg, int):
            self.mfg_ids.add(mfg)

        if name and name != "Unknown":
            self.names.add(name[:80])

        self.metadata_classes[meta_class] += 1
        if mobile_uuids:
            self.mobile_service_uuids.update(mobile_uuids)
            self.mobile_service_packet_count += 1

        self.obs.append((now_mono, scanner, channel, rssi))
        self._update_last_known_rssi(scanner, rssi, now_mono)
        self._prune_obs(now_mono)

        ts_stream_key = (alias_key, scanner)
        prev_ts = self.last_alias_scanner_ts_us.get(ts_stream_key)
        if prev_ts is not None and ts_us > prev_ts:
            dt_ms = (ts_us - prev_ts) / 1000.0
            if 10.0 <= dt_ms <= 10_240.0:
                self.adv_intervals_ms.append(dt_ms)
        self.last_alias_scanner_ts_us[ts_stream_key] = ts_us

        self.confirmed = self.is_confirmed()
        return True

    def _prune_obs(self, now_mono: Optional[float] = None) -> None:
        if now_mono is None:
            now_mono = time.monotonic()
        cutoff = now_mono - TRACKER_MEMORY_SEC
        while self.obs and self.obs[0][0] < cutoff:
            self.obs.popleft()

    def scanner_rssi(self) -> Dict[str, float]:
        """
        EWMA-like average RSSI per scanner over rolling memory.
        Channel is not used strongly because current firmware channel is a software label.
        """
        self._prune_obs()
        vals: Dict[str, List[int]] = defaultdict(list)
        for _, scanner, _, rssi in self.obs:
            vals[scanner].append(rssi)

        out = {}
        for scanner, samples in vals.items():
            if not samples:
                continue
            # Use top-half mean to reduce deep fades and preserve peak-RSSI behavior.
            samples_sorted = sorted(samples, reverse=True)
            keep_n = max(1, len(samples_sorted) // 2)
            out[scanner] = sum(samples_sorted[:keep_n]) / keep_n
        return out

    def _update_last_known_rssi(self, scanner: str, rssi: int, now_mono: float) -> None:
        vals = self.last_known_rssi_vals[scanner]
        vals.append(rssi)
        # Keep bounded memory per scanner.
        if len(vals) > 80:
            del vals[:-80]

        samples_sorted = sorted(vals, reverse=True)
        keep_n = max(1, len(samples_sorted) // 2)
        self.last_known_scanner_rssi[scanner] = sum(samples_sorted[:keep_n]) / keep_n
        self.last_known_scanner_update_mono = now_mono

    def last_known_scanner_visibility(self) -> set:
        return set(self.last_known_scanner_rssi.keys())

    def last_known_strongest_rssi_value(self) -> Optional[float]:
        if not self.last_known_scanner_rssi:
            return None
        return max(self.last_known_scanner_rssi.values())

    def last_known_top2_avg_rssi_value(self) -> Optional[float]:
        if not self.last_known_scanner_rssi:
            return None
        vals = sorted(self.last_known_scanner_rssi.values(), reverse=True)[:2]
        if not vals:
            return None
        return sum(vals) / len(vals)

    def channel_visibility(self) -> set:
        self._prune_obs()
        return {(scanner, channel) for _, scanner, channel, _ in self.obs}

    def scanner_visibility(self) -> set:
        self._prune_obs()
        return {scanner for _, scanner, _, _ in self.obs}

    def known_classes(self) -> set:
        return {cls for cls in self.metadata_classes.keys() if is_known_metadata_class(cls)}

    def dominant_class(self) -> str:
        if not self.metadata_classes:
            return "Unknown"
        return max(self.metadata_classes.items(), key=lambda kv: kv[1])[0]

    def mean_adv_interval_ms(self) -> Optional[float]:
        if not self.adv_intervals_ms:
            return None
        return sum(self.adv_intervals_ms) / len(self.adv_intervals_ms)

    def adv_interval_sample_count(self) -> int:
        return len(self.adv_intervals_ms)

    def age_sec(self) -> float:
        return max(0.0, self.last_seen_mono - self.first_seen_mono)

    def known_packet_count(self) -> int:
        return sum(
            count for cls, count in self.metadata_classes.items()
            if is_known_metadata_class(cls)
        )

    def unknown_packet_count(self) -> int:
        return int(self.metadata_classes.get("Unknown", 0))

    def known_packet_ratio(self) -> float:
        if self.packet_count <= 0:
            return 0.0
        return self.known_packet_count() / max(1, self.packet_count)

    def strongest_rssi_value(self) -> Optional[float]:
        rssi_map = self.scanner_rssi()
        if not rssi_map:
            return None
        return max(rssi_map.values())

    def top2_avg_rssi_value(self) -> Optional[float]:
        rssi_map = self.scanner_rssi()
        if not rssi_map:
            return None
        vals = sorted(rssi_map.values(), reverse=True)[:2]
        if not vals:
            return None
        return sum(vals) / len(vals)

    def confirm_quality(self) -> Tuple[bool, str]:
        """
        Return (confirmed_allowed, reason).

        The basic evidence gates prevent early confirmation.
        The RSSI quality gates prevent weak/outside devices from becoming
        CONFIRMED merely because they were observed long enough.
        The known-ratio gate prevents mostly-Unknown evidence from confirming
        a known-class track.
        """
        if self.packet_count < CONFIRM_MIN_PACKETS:
            return False, "not_enough_packets"

        if len(self.scanner_visibility()) < CONFIRM_MIN_SCANNERS:
            return False, "not_enough_scanners"

        if self.age_sec() < CONFIRM_MIN_DURATION_SEC:
            return False, "not_old_enough"

        strongest = self.strongest_rssi_value()
        top2_avg = self.top2_avg_rssi_value()

        if strongest is None or top2_avg is None:
            return False, "no_rssi"

        rssi_ok = (
            strongest >= CONFIRM_MIN_STRONGEST_RSSI_DBM or
            top2_avg >= CONFIRM_MIN_TOP2_AVG_RSSI_DBM
        )

        if not rssi_ok:
            return False, "weak_rssi"

        if self.known_classes():
            known_packets = self.known_packet_count()
            known_ratio = self.known_packet_ratio()

            if known_packets < CONFIRM_MIN_KNOWN_PACKETS:
                return False, "not_enough_known_packets"

            if known_ratio < CONFIRM_MIN_KNOWN_RATIO:
                return False, "unknown_heavy"

        return True, "confirmed"

    def is_confirmed(self) -> bool:
        ok, _ = self.confirm_quality()
        return ok

    def remember_alias_feature(self, alias_key: str, alias: Optional["AliasTrack"]) -> None:
        if alias is None:
            return
        rssi_map = alias.scanner_rssi()
        if not rssi_map:
            return

        alias_known = set(alias.known_classes())
        own_known = self.known_classes()

        # Unknown aliases may be attached to a known track, but they should not
        # become part of the trusted identity core. Otherwise, over time the core
        # grows into a broad "bucket" and future merges become too easy.
        if own_known and not alias_known and not UNKNOWN_ALIAS_CAN_EXPAND_KNOWN_TRACK_CORE:
            return

        self.alias_features[alias_key] = {
            "rssi": dict(rssi_map),
            "known": alias_known,
            "class": alias.dominant_class(),
            "first": alias.first_seen_mono,
            "last": alias.last_seen_mono,
            "packets": alias.packet_count,
        }

    def min_distance_to_alias_features(self, rssi_map: Dict[str, float]) -> Tuple[Optional[float], Optional[float], int]:
        best_rel = None
        best_abs = None
        best_common = 0

        for feat in self.alias_features.values():
            rel, absr, common = rssi_distance_pair(feat.get("rssi", {}), rssi_map)
            if rel is None or absr is None:
                continue
            if best_rel is None or (rel + 0.25 * absr) < (best_rel + 0.25 * best_abs):
                best_rel = rel
                best_abs = absr
                best_common = common

        return best_rel, best_abs, best_common

    def max_distance_to_alias_features(self, rssi_map: Dict[str, float]) -> Tuple[Optional[float], Optional[float], Optional[float], int]:
        """
        Distance from a candidate alias fingerprint to the farthest existing
        trusted/core alias fingerprint in this track.

        This is the anti-drift guard: a new alias may not match only one edge
        member if it makes the whole track too wide.
        """
        worst_rel = None
        worst_abs = None
        worst_diff = None
        comparable = 0

        for feat in self.alias_features.values():
            feat_rssi = feat.get("rssi", {})
            rel, absr, common = rssi_distance_pair(feat_rssi, rssi_map)
            max_diff = max_abs_rssi_diff_common(feat_rssi, rssi_map)

            if rel is None or absr is None or max_diff is None:
                continue

            if common < MIN_COMMON_SCANNERS_STRONG_MATCH:
                continue

            comparable += 1
            worst_rel = rel if worst_rel is None else max(worst_rel, rel)
            worst_abs = absr if worst_abs is None else max(worst_abs, absr)
            worst_diff = max_diff if worst_diff is None else max(worst_diff, max_diff)

        return worst_rel, worst_abs, worst_diff, comparable

    def would_expand_identity_too_much(self, rssi_map: Dict[str, float], incoming_known: set) -> bool:
        """
        Return True when accepting a new alias would make this track's trusted
        spatial identity too broad.

        The larger/more confirmed the track is, the tighter this guard becomes.
        """
        if not rssi_map:
            return True

        own_known = self.known_classes()
        is_unknown_to_known = bool(own_known) and not bool(incoming_known)

        # Unknown payloads joining known tracks are the riskiest source of drift.
        if is_unknown_to_known:
            rel_limit = UNKNOWN_TO_KNOWN_MAX_REL_RADIUS_DB
            abs_limit = UNKNOWN_TO_KNOWN_MAX_ABS_RADIUS_DB
            diff_limit = UNKNOWN_TO_KNOWN_MAX_PER_SCANNER_DIFF_DB
            guard_required = True
        elif self.is_confirmed():
            rel_limit = CONFIRMED_TRACK_MAX_REL_RADIUS_DB
            abs_limit = CONFIRMED_TRACK_MAX_ABS_RADIUS_DB
            diff_limit = CONFIRMED_TRACK_MAX_PER_SCANNER_DIFF_DB
            guard_required = True
        elif len(self.alias_features) >= TRACK_RADIUS_GUARD_MIN_ALIAS_FEATURES:
            rel_limit = LARGE_TRACK_MAX_REL_RADIUS_DB
            abs_limit = LARGE_TRACK_MAX_ABS_RADIUS_DB
            diff_limit = LARGE_TRACK_MAX_PER_SCANNER_DIFF_DB
            guard_required = True
        else:
            guard_required = False
            rel_limit = LARGE_TRACK_MAX_REL_RADIUS_DB
            abs_limit = LARGE_TRACK_MAX_ABS_RADIUS_DB
            diff_limit = LARGE_TRACK_MAX_PER_SCANNER_DIFF_DB

        if not guard_required:
            return False

        worst_rel, worst_abs, worst_diff, comparable = self.max_distance_to_alias_features(rssi_map)

        # If a large/confirmed track has no comparable core member, do not allow
        # the new alias to broaden it.
        if comparable == 0 or worst_rel is None or worst_abs is None or worst_diff is None:
            return True

        return worst_rel > rel_limit or worst_abs > abs_limit or worst_diff > diff_limit

    def can_absorb_track_without_drift(self, other: "DeviceTrack") -> bool:
        """
        Anti-drift guard for track-to-track merges.
        Every trusted/core alias in the other track must fit inside this track's
        current trusted/core radius.
        """
        if not other.alias_features:
            return True

        incoming_known = other.known_classes()
        for feat in other.alias_features.values():
            rssi_map = feat.get("rssi", {})
            if self.would_expand_identity_too_much(rssi_map, incoming_known):
                return False

        return True

    def _finalize_current_burst(self) -> None:
        if self.current_burst_packets <= 0:
            return

        dur = max(0.0, self.current_burst_end_mono - self.current_burst_start_mono)
        rssi_map = {}
        for scanner, vals in self.current_burst_rssi_vals.items():
            if vals:
                vals_sorted = sorted(vals, reverse=True)
                keep_n = max(1, len(vals_sorted) // 2)
                rssi_map[scanner] = sum(vals_sorted[:keep_n]) / keep_n

        self.burst_packet_counts.append(self.current_burst_packets)
        self.burst_durations_sec.append(dur)
        self.burst_scanner_sets.append(set(self.current_burst_scanners))
        self.burst_rssi_maps.append(rssi_map)

    def _update_burst(self, now_mono: float, scanner: str, rssi: int) -> None:
        # First packet in this track starts the first burst.
        if self.burst_count == 0:
            self.burst_count = 1
            self.current_burst_start_mono = now_mono
            self.current_burst_end_mono = now_mono
            self.current_burst_packets = 0
            self.current_burst_scanners = set()
            self.current_burst_rssi_vals = defaultdict(list)

        gap = now_mono - self.current_burst_end_mono

        if self.current_burst_packets > 0 and gap > BURST_GAP_SEC:
            self._finalize_current_burst()
            self.burst_count += 1
            self.current_burst_start_mono = now_mono
            self.current_burst_end_mono = now_mono
            self.current_burst_packets = 0
            self.current_burst_scanners = set()
            self.current_burst_rssi_vals = defaultdict(list)

        self.current_burst_end_mono = now_mono
        self.current_burst_packets += 1
        self.current_burst_scanners.add(scanner)
        self.current_burst_rssi_vals[scanner].append(rssi)

    def all_burst_packet_counts(self) -> List[int]:
        out = list(self.burst_packet_counts)
        if self.current_burst_packets > 0:
            out.append(self.current_burst_packets)
        return out

    def all_burst_durations_sec(self) -> List[float]:
        out = list(self.burst_durations_sec)
        if self.current_burst_packets > 0:
            out.append(max(0.0, self.current_burst_end_mono - self.current_burst_start_mono))
        return out

    def all_burst_scanner_sets(self) -> List[set]:
        out = [set(x) for x in self.burst_scanner_sets]
        if self.current_burst_packets > 0:
            out.append(set(self.current_burst_scanners))
        return out

    def last_burst_age_sec(self) -> float:
        return max(0.0, time.monotonic() - self.current_burst_end_mono)

    def avg_burst_duration_sec(self) -> float:
        vals = self.all_burst_durations_sec()
        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    def avg_packets_per_burst(self) -> float:
        vals = self.all_burst_packet_counts()
        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    def recent_burst_scanners(self) -> set:
        if self.current_burst_packets > 0:
            return set(self.current_burst_scanners)
        if self.burst_scanner_sets:
            return set(self.burst_scanner_sets[-1])
        return set()

    def mobile_service_packet_ratio(self) -> float:
        if self.packet_count <= 0:
            return 0.0
        return self.mobile_service_packet_count / max(1, self.packet_count)

    def has_mobile_service_data(self) -> bool:
        if self.mobile_service_uuids:
            return True
        return bool(mobile_service_uuids_from_sigs(self.payload_sigs))

    def mobile_service_score(self) -> float:
        if not self.has_mobile_service_data():
            return 0.0

        # Be conservative with beacons/outside weak sources.
        cls = self.dominant_class().lower()
        if "tondo" in cls or "beacon" in cls:
            return 0.0
        if self.is_outside_stable_source():
            return 0.0

        rssi_map = self.scanner_rssi()
        if not rssi_map:
            rssi_map = dict(self.last_known_scanner_rssi)

        scanners = len(rssi_map)
        strongest = max(rssi_map.values()) if rssi_map else None
        top2 = None
        if rssi_map:
            vals = sorted(rssi_map.values(), reverse=True)[:2]
            top2 = sum(vals) / len(vals)

        score = 0.0

        # The key identity cue.
        if self.mobile_service_uuids or mobile_service_uuids_from_sigs(self.payload_sigs):
            score += 2.0

        if self.mobile_service_packet_count >= MOBILE_SERVICE_MIN_PACKETS:
            score += 1.5

        if scanners >= MOBILE_SERVICE_MIN_SCANNERS:
            score += 1.5

        if strongest is not None and strongest >= MOBILE_SERVICE_STRONG_RSSI_DBM:
            score += 1.5

        if top2 is not None and top2 >= MOBILE_SERVICE_TOP2_AVG_DBM:
            score += 1.0

        if len(self.macs) >= 2:
            score += 1.0

        if len(self.payload_sigs) >= 2:
            score += 0.5

        # Unknown-heavy service-data is exactly the pattern seen with the Samsung
        # phone test. Do not require Unknown only, but reward it.
        if self.mobile_service_packet_ratio() >= MOBILE_SERVICE_MIN_UNKNOWN_RATIO:
            score += 1.0

        # If this is mostly a known stable manufacturer device with a tiny amount
        # of service-data pollution, do not promote it to mobile-service.
        if self.known_packet_ratio() >= 0.85 and self.mobile_service_packet_ratio() < 0.25:
            score -= 3.0

        return max(0.0, score)

    def mobile_service_presence(self) -> str:
        score = self.mobile_service_score()
        if score >= MOBILE_SERVICE_HIGH_SCORE_THRESHOLD:
            return "HIGH"
        if score >= MOBILE_SERVICE_SCORE_THRESHOLD:
            return "MEDIUM"
        if self.has_mobile_service_data():
            return "LOW"
        return "NONE"

    def is_mobile_service_data_like(self) -> bool:
        return self.mobile_service_score() >= MOBILE_SERVICE_SCORE_THRESHOLD

    def mobile_service_reason(self) -> str:
        if not self.has_mobile_service_data():
            return "no_mobile_service_uuid"
        parts = []
        if self.mobile_service_uuids:
            parts.append("uuid_" + "_".join(sorted(self.mobile_service_uuids)))
        if self.mobile_service_packet_count >= MOBILE_SERVICE_MIN_PACKETS:
            parts.append("enough_mobile_packets")
        if len(self.scanner_visibility()) >= MOBILE_SERVICE_MIN_SCANNERS:
            parts.append("multi_scanner")
        strongest = self.strongest_rssi_value()
        if strongest is not None and strongest >= MOBILE_SERVICE_STRONG_RSSI_DBM:
            parts.append("strong_rssi")
        if self.mobile_service_packet_ratio() >= MOBILE_SERVICE_MIN_UNKNOWN_RATIO:
            parts.append("service_data_dominant")
        return ",".join(parts) if parts else "weak_mobile_service_evidence"

    def outside_likelihood_score(self) -> float:
        """
        Outside-stable is deliberately gated by limited scanner coverage.
        A 4-scanner/localizable device may be weak or stable, but it should not
        be called OUTSIDE_STABLE only because it is old and packet-dense.
        """
        live_scanners = len(self.scanner_visibility())
        last_scanners = len(self.last_known_scanner_visibility())
        scanners_for_role = live_scanners if live_scanners > 0 else last_scanners

        live_strongest = self.strongest_rssi_value()
        strongest = live_strongest if live_strongest is not None else self.last_known_strongest_rssi_value()

        if scanners_for_role > OUTSIDE_STABLE_MAX_SCANNERS:
            return 0.0

        score = 0.0
        if self.age_sec() >= OUTSIDE_STABLE_MIN_AGE_SEC:
            score += 2.0
        if self.packet_count >= OUTSIDE_STABLE_MIN_PACKETS:
            score += 1.0
        if scanners_for_role > 0:
            score += 2.0
        if strongest is not None and strongest <= OUTSIDE_STABLE_MAX_STRONGEST_RSSI_DBM:
            score += 2.0
        if self.known_packet_ratio() >= OUTSIDE_STABLE_MIN_KNOWN_RATIO:
            score += 1.0
        if self.avg_packets_per_burst() >= 20:
            score += 1.0
        return score

    def is_outside_stable_source(self) -> bool:
        live_scanners = len(self.scanner_visibility())
        last_scanners = len(self.last_known_scanner_visibility())
        scanners_for_role = live_scanners if live_scanners > 0 else last_scanners
        if scanners_for_role > OUTSIDE_STABLE_MAX_SCANNERS:
            return False
        return self.outside_likelihood_score() >= 6.0

    def is_stable_device(self) -> bool:
        cls = self.dominant_class().lower()
        if "tondo" in cls or "beacon" in cls:
            return False
        if self.is_outside_stable_source():
            return False
        if self.age_sec() < STABLE_DEVICE_MIN_AGE_SEC:
            return False
        if self.packet_count < STABLE_DEVICE_MIN_PACKETS:
            return False
        if self.avg_packets_per_burst() < STABLE_DEVICE_MIN_AVG_PACKETS_PER_BURST:
            return False
        if len(self.macs) > STABLE_DEVICE_MAX_MACS:
            return False
        if len(self.payload_sigs) > STABLE_DEVICE_MAX_PAYLOADS:
            return False
        return True

    def phone_likelihood_score(self) -> float:
        # Outside fixed sources, stable fixed devices, and strong mobile-service
        # tracks should not be lumped into generic PHONE_LIKE.
        if self.is_outside_stable_source() or self.is_stable_device() or self.is_mobile_service_data_like():
            return 0.0

        cls_set = self.known_classes()
        cls_text = " ".join(sorted(cls_set)).lower()
        score = 0.0

        if any(x in cls_text for x in ("apple", "samsung", "google", "microsoft")):
            score += 2.0

        if "tondo" in cls_text or "beacon" in cls_text:
            score -= 4.0

        rotating_aliases = (
            len(self.macs) >= PHONE_ROTATION_MIN_MACS_OR_PAYLOADS or
            len(self.payload_sigs) >= PHONE_ROTATION_MIN_MACS_OR_PAYLOADS
        )

        if len(self.macs) >= 2:
            score += 1.0
        if len(self.payload_sigs) >= 2:
            score += 1.0

        burst_counts = self.all_burst_packet_counts()
        useful_bursts = sum(1 for c in burst_counts if c >= MIN_PACKETS_PER_PHONE_BURST)
        avg_burst_dur = self.avg_burst_duration_sec()
        avg_packets = self.avg_packets_per_burst()
        bursty = (
            self.burst_count >= MIN_BURSTS_FOR_INTERMITTENT_PHONE and
            useful_bursts >= MIN_BURSTS_FOR_INTERMITTENT_PHONE and
            avg_burst_dur <= PHONE_BURSTY_MAX_AVG_BURST_DURATION_SEC and
            avg_packets <= PHONE_BURSTY_MAX_AVG_PACKETS_PER_BURST
        )

        if self.burst_count >= MIN_BURSTS_FOR_INTERMITTENT_PHONE:
            score += 1.0
        if bursty:
            score += 1.5

        if not rotating_aliases and not bursty:
            # Avoid classifying one fixed MAC/payload continuous source as phone-like.
            score -= 3.0

        if self.age_sec() >= 45.0 and len(self.scanner_visibility()) <= 2 and avg_packets >= 25:
            score -= 2.0

        return max(0.0, score)

    def is_phone_like(self) -> bool:
        return self.phone_likelihood_score() >= PHONE_LIKE_SCORE_THRESHOLD

    def presence_state(self) -> str:
        last_age = time.monotonic() - self.last_seen_mono
        if last_age <= LIVE_ACTIVE_TIMEOUT_SEC:
            return "ACTIVE"
        if self.is_phone_like() and last_age <= IDENTITY_MEMORY_SEC:
            return "INTERMITTENT"
        if last_age <= IDENTITY_MEMORY_SEC:
            return "INACTIVE"
        return "STALE"

    def device_role(self) -> str:
        cls = self.dominant_class().lower()
        if "tondo" in cls or "beacon" in cls:
            return "BEACON_LIKE"
        if self.is_outside_stable_source():
            return "OUTSIDE_STABLE"
        if self.is_mobile_service_data_like():
            return MOBILE_SERVICE_LABEL
        if self.is_stable_device():
            return "STABLE_DEVICE"
        if self.is_phone_like():
            return "PHONE_LIKE"
        return "UNKNOWN"

    def is_localizable(self) -> bool:
        return len(self.scanner_visibility()) >= CONFIRM_MIN_SCANNERS

    def label(self) -> str:
        if self.is_mobile_service_data_like():
            uuids = sorted(self.mobile_service_uuids or mobile_service_uuids_from_sigs(self.payload_sigs))
            if uuids:
                return f"{MOBILE_SERVICE_LABEL}({','.join(uuids)})"
            return MOBILE_SERVICE_LABEL

        cls = self.dominant_class()
        if cls != "Unknown":
            return cls

        known = sorted(list(self.known_classes()))
        if known:
            return known[0]

        if self.names:
            return sorted(self.names)[0]
        return self.uid

    def dna(self) -> str:
        cls = self.dominant_class()
        sigs = sorted(list(self.payload_sigs))[:3]
        return f"{cls}|{','.join(sigs)}"


class AliasTrack:
    """
    Temporary MAC/payload alias track.

    A packet does not go straight into a physical device anymore.
    First it builds a short fingerprint here; only then it is matched to a
    physical device. This avoids vendor-wide over-merging.
    """
    def __init__(self, alias_key: str, ev: Dict[str, Any], parsed: Dict[str, Any]):
        self.alias_key = alias_key
        self.first_seen_mono = time.monotonic()
        self.last_seen_mono = self.first_seen_mono
        self.packet_count = 0
        self.obs = deque()
        self.macs = set()
        self.payload_sigs = set()
        self.mfg_ids = set()
        self.names = set()
        self.metadata_classes = defaultdict(int)
        self.mobile_service_uuids = set()
        self.mobile_service_packet_count = 0
        self.last_ts_by_scanner: Dict[str, int] = {}
        self.adv_intervals_ms = deque(maxlen=50)
        self.update(ev, parsed)

    def update(self, ev: Dict[str, Any], parsed: Dict[str, Any]) -> None:
        now_mono = time.monotonic()
        scanner = str(ev.get("scanner", "UNK"))
        channel = safe_int(ev.get("channel"), 0)
        rssi = safe_int(ev.get("rssi"), 0)
        ts_us = safe_int(ev.get("ts"), 0)
        mac = str(ev.get("mac", "UNK")).upper()
        payload_sig = parsed.get("payload_sig", payload_signature(ev.get("payload", "")))
        name = str(parsed.get("name", "Unknown") or "Unknown").strip()
        mfg = parsed.get("mfg_id")
        meta_class = classify_metadata(parsed)
        mobile_uuids = service_data_uuids(parsed) & MOBILE_SERVICE_UUIDS

        self.last_seen_mono = now_mono

        prev_ts = self.last_ts_by_scanner.get(scanner)
        if prev_ts is not None and ts_us > prev_ts:
            dt_ms = (ts_us - prev_ts) / 1000.0
            if 10.0 <= dt_ms <= 10_240.0:
                self.adv_intervals_ms.append(dt_ms)

        self.last_ts_by_scanner[scanner] = ts_us
        self.packet_count += 1
        self.obs.append((now_mono, scanner, channel, rssi))

        self.macs.add(mac)
        self.payload_sigs.add(payload_sig)
        if isinstance(mfg, int):
            self.mfg_ids.add(mfg)
        if name and name != "Unknown":
            self.names.add(name[:80])
        self.metadata_classes[meta_class] += 1
        if mobile_uuids:
            self.mobile_service_uuids.update(mobile_uuids)
            self.mobile_service_packet_count += 1
        self._prune_obs(now_mono)

    def _prune_obs(self, now_mono: Optional[float] = None) -> None:
        if now_mono is None:
            now_mono = time.monotonic()
        cutoff = now_mono - TRACKER_MEMORY_SEC
        while self.obs and self.obs[0][0] < cutoff:
            self.obs.popleft()

    def scanner_rssi(self) -> Dict[str, float]:
        self._prune_obs()
        vals: Dict[str, List[int]] = defaultdict(list)
        for _, scanner, _, rssi in self.obs:
            vals[scanner].append(rssi)

        out = {}
        for scanner, samples in vals.items():
            samples_sorted = sorted(samples, reverse=True)
            keep_n = max(1, len(samples_sorted) // 2)
            out[scanner] = sum(samples_sorted[:keep_n]) / keep_n
        return out

    def scanner_visibility(self) -> set:
        self._prune_obs()
        return {scanner for _, scanner, _, _ in self.obs}

    def known_classes(self) -> set:
        return {cls for cls in self.metadata_classes.keys() if is_known_metadata_class(cls)}

    def dominant_class(self) -> str:
        if not self.metadata_classes:
            return "Unknown"
        return max(self.metadata_classes.items(), key=lambda kv: kv[1])[0]

    def mean_adv_interval_ms(self) -> Optional[float]:
        if not self.adv_intervals_ms:
            return None
        return sum(self.adv_intervals_ms) / len(self.adv_intervals_ms)

    def adv_interval_sample_count(self) -> int:
        return len(self.adv_intervals_ms)

    def has_mobile_service_data(self) -> bool:
        return bool(self.mobile_service_uuids) or bool(mobile_service_uuids_from_sigs(self.payload_sigs))

    def mobile_service_packet_ratio(self) -> float:
        if self.packet_count <= 0:
            return 0.0
        return self.mobile_service_packet_count / max(1, self.packet_count)

    def ready_for_physical_match(self) -> bool:
        age = time.monotonic() - self.first_seen_mono

        # Strong case: enough packets from at least two scanners.
        if self.packet_count >= ALIAS_MIN_PACKETS_FOR_MATCH and len(self.scanner_visibility()) >= ALIAS_MIN_SCANNERS_FOR_MATCH:
            return True

        # Weak case: one-scanner aliases need much more evidence before becoming
        # a physical track. This keeps distant/noisy devices as aliases/candidates.
        if age >= ALIAS_MATURITY_TIMEOUT_SEC and self.packet_count >= MIN_ALIAS_PACKETS_ONE_SCANNER:
            return True

        return False


class DeviceTracker:
    """
    Fast real-time identity layer.

    It does not change the raw JSON/session schema. It only attaches uid/status/dna
    fields to streamed events so the UI can group signals into physical-device tracks.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.tracks: Dict[str, DeviceTrack] = {}
        self.alias_to_uid: Dict[str, str] = {}
        self.alias_tracks: Dict[str, AliasTrack] = {}
        self.next_id = 1
        self.last_merge_mono = 0.0
        self.rejected_class_conflicts = 0
        self.rejected_track_expansion = 0
        self.blocked_confirmation_weak_rssi = 0
        self.blocked_confirmation_unknown_heavy = 0

    def make_alias_key(self, ev: Dict[str, Any], parsed: Dict[str, Any]) -> str:
        mac = str(ev.get("mac", "UNK")).upper()
        sig = parsed.get("payload_sig", payload_signature(ev.get("payload", "")))
        mfg = parsed.get("mfg_id")
        adv_len = parsed.get("adv_len", 0)
        ad_structure = parsed.get("ad_structure", "")
        return f"{mac}|{sig}|MFG:{mfg}|LEN:{adv_len}|AD:{ad_structure}"

    def _alias_is_mobile_service(self, parsed: Dict[str, Any], alias: Optional[AliasTrack]) -> bool:
        if alias is not None and alias.has_mobile_service_data():
            return True
        return is_mobile_service_parsed(parsed)

    def _track_is_known_non_mobile(self, track: DeviceTrack) -> bool:
        return bool(track.known_classes()) and not track.is_mobile_service_data_like()

    def _mobile_service_merge_allowed(
        self,
        mobile_rssi: Dict[str, float],
        target_rssi: Dict[str, float],
        strict_known_target: bool,
    ) -> bool:
        if not mobile_rssi or not target_rssi:
            return False

        common = len(set(mobile_rssi.keys()) & set(target_rssi.keys()))
        if common < MIN_COMMON_SCANNERS_STRONG_MATCH:
            return False

        rel, absr, _ = rssi_distance_pair(mobile_rssi, target_rssi)
        max_diff = max_abs_rssi_diff_common(mobile_rssi, target_rssi)
        if rel is None or absr is None or max_diff is None:
            return False

        if strict_known_target:
            rel_limit = MOBILE_TO_KNOWN_MAX_REL_RMSE_DB
            abs_limit = MOBILE_TO_KNOWN_MAX_ABS_RMSE_DB
            diff_limit = MOBILE_TO_KNOWN_MAX_PER_SCANNER_DIFF_DB
            require_same_strongest = MOBILE_TO_KNOWN_REQUIRE_SAME_STRONGEST
            require_top2_overlap = MOBILE_TO_KNOWN_REQUIRE_TOP2_OVERLAP
        else:
            rel_limit = MOBILE_TO_MOBILE_MAX_REL_RMSE_DB
            abs_limit = MOBILE_TO_MOBILE_MAX_ABS_RMSE_DB
            diff_limit = MOBILE_TO_MOBILE_MAX_PER_SCANNER_DIFF_DB
            require_same_strongest = MOBILE_TO_MOBILE_REQUIRE_SAME_STRONGEST
            require_top2_overlap = MOBILE_TO_MOBILE_REQUIRE_TOP2_OVERLAP

        if rel > rel_limit or absr > abs_limit or max_diff > diff_limit:
            return False

        if require_same_strongest and strongest_scanner(mobile_rssi) != strongest_scanner(target_rssi):
            return False

        if require_top2_overlap and not (top_scanners(mobile_rssi, 2) & top_scanners(target_rssi, 2)):
            return False

        return True

    def process_event(self, ev: Dict[str, Any], parsed: Dict[str, Any]) -> Dict[str, Any]:
        alias_key = self.make_alias_key(ev, parsed)

        with self.lock:
            self._prune_stale_locked()

            alias = self.alias_tracks.get(alias_key)
            if alias is None:
                alias = AliasTrack(alias_key, ev, parsed)
                self.alias_tracks[alias_key] = alias
            else:
                alias.update(ev, parsed)

            uid = self.alias_to_uid.get(alias_key)
            if uid and uid in self.tracks:
                track = self.tracks[uid]
                if track.update(ev, parsed, alias_key):
                    track.remember_alias_feature(alias_key, alias)
                    self._periodic_merge_locked()
                    return self._identity_result(track)

                # The alias was previously mapped to this track, but after better
                # metadata/classification it conflicts. Detach it and let it form
                # or join the correct physical track below.
                self.rejected_class_conflicts += 1
                self.alias_to_uid.pop(alias_key, None)

            # Do not immediately throw every new Apple/Samsung alias into a vendor bucket.
            # Let it collect a small spatial fingerprint first.
            if not alias.ready_for_physical_match():
                uid = f"ALIAS_{alias_key[:10]}"
                return {
                    "uid": uid,
                    "status": "CANDIDATE",
                    "dna": f"{alias.dominant_class()}|alias warming",
                    "physical_label": f"{alias.dominant_class()} candidate",
                    "confirm_reason": "alias_warming",
                    "presence_state": "ACTIVE",
                    "device_role": MOBILE_SERVICE_LABEL if alias.has_mobile_service_data() else "",
                    "phone_likelihood": 0.0,
                    "outside_likelihood": 0.0,
                    "mobile_service_score": 1.0 if alias.has_mobile_service_data() else 0.0,
                    "mobile_service_presence": "LOW" if alias.has_mobile_service_data() else "NONE",
                    "mobile_service_uuids": sorted(list(alias.mobile_service_uuids)),
                    "mobile_service_reason": "alias_warming_mobile_service" if alias.has_mobile_service_data() else "",
                    "localizable": False,
                }

            best_uid = None
            best_score = float("inf")

            for candidate_uid, track in self.tracks.items():
                score = self._score_alias_to_track(ev, parsed, alias_key, track, alias)
                if score < best_score:
                    best_score = score
                    best_uid = candidate_uid

            if best_uid is not None and best_score <= MATCH_THRESHOLD:
                track = self.tracks[best_uid]
            else:
                uid = f"PD_{self.next_id:03d}"
                self.next_id += 1
                track = DeviceTrack(uid, safe_int(ev.get("ts"), 0), time.monotonic())
                self.tracks[uid] = track

            if not track.update(ev, parsed, alias_key):
                self.rejected_class_conflicts += 1
                uid = f"PD_{self.next_id:03d}"
                self.next_id += 1
                track = DeviceTrack(uid, safe_int(ev.get("ts"), 0), time.monotonic())
                self.tracks[uid] = track
                # A fresh track cannot conflict with itself. If this fails, keep it as a candidate shell.
                track.update(ev, parsed, alias_key)

            track.remember_alias_feature(alias_key, alias)
            self.alias_to_uid[alias_key] = track.uid

            self._periodic_merge_locked()
            return self._identity_result(track)

    def _identity_result(self, track: DeviceTrack) -> Dict[str, str]:
        confirmed, reason = track.confirm_quality()

        if not confirmed:
            if reason == "weak_rssi":
                self.blocked_confirmation_weak_rssi += 1
            elif reason in ("unknown_heavy", "not_enough_known_packets"):
                self.blocked_confirmation_unknown_heavy += 1

        return {
            "uid": track.uid,
            "status": "CONFIRMED" if confirmed else "CANDIDATE",
            "dna": track.dna(),
            "physical_label": track.label(),
            "confirm_reason": reason,
            "presence_state": track.presence_state(),
            "device_role": track.device_role(),
            "phone_likelihood": round(track.phone_likelihood_score(), 2),
            "outside_likelihood": round(track.outside_likelihood_score(), 2),
            "mobile_service_score": round(track.mobile_service_score(), 2),
            "mobile_service_presence": track.mobile_service_presence(),
            "mobile_service_uuids": sorted(list(track.mobile_service_uuids or mobile_service_uuids_from_sigs(track.payload_sigs))),
            "mobile_service_reason": track.mobile_service_reason(),
            "localizable": track.is_localizable(),
        }

    def _score_alias_to_track(self, ev: Dict[str, Any], parsed: Dict[str, Any],
                              alias_key: str, track: DeviceTrack,
                              alias: Optional[AliasTrack] = None) -> float:
        now_mono = time.monotonic()
        gap = now_mono - track.last_seen_mono
        if gap > TRACK_STALE_SEC:
            # Only phone-like tracks get long reconnect memory. Fixed outside sources
            # should not attract unrelated future aliases after silence.
            if gap > IDENTITY_MEMORY_SEC or not track.is_phone_like() or track.is_outside_stable_source():
                return float("inf")

        incoming_class = classify_metadata(parsed)
        track_known = track.known_classes()
        alias_known = alias.known_classes() if alias is not None else ({incoming_class} if is_known_metadata_class(incoming_class) else set())

        if STRICT_METADATA_CONFLICTS and alias_known and track_known:
            if alias_known.isdisjoint(track_known):
                return float("inf")

        alias_mobile = self._alias_is_mobile_service(parsed, alias)
        track_mobile = track.is_mobile_service_data_like() or track.has_mobile_service_data()

        # Mobile service-data is allowed to form its own physical track. Do not
        # let it freely attach to known Samsung/Apple/Microsoft tracks; that was
        # the main source of polluted PDs during the control test.
        mobile_to_known_guard = alias_mobile and track_known and not track_mobile

        # Advertising interval anti-merge gate.
        # Example: one phone advertises around 100 ms and another around 250 ms.
        # If both estimates are reliable, do not merge them.
        if alias is not None and not adv_interval_compatible(
            track.mean_adv_interval_ms(),
            track.adv_interval_sample_count(),
            alias.mean_adv_interval_ms(),
            alias.adv_interval_sample_count(),
        ):
            return float("inf")

        mac = str(ev.get("mac", "UNK")).upper()
        payload_sig = parsed.get("payload_sig", payload_signature(ev.get("payload", "")))
        scanner = str(ev.get("scanner", "UNK"))
        rssi = safe_int(ev.get("rssi"), 0)

        score = 0.0

        # Direct continuity.
        has_same_alias = alias_key in track.aliases
        has_same_mac = mac in track.macs
        has_same_payload = payload_sig in track.payload_sigs
        has_direct_continuity = has_same_alias or has_same_mac

        # Same exact alias or MAC is strong evidence. Same payload/manufacturer is weak only.
        if has_same_alias:
            score -= 8.0
        if has_same_mac:
            score -= 5.0
        if has_same_payload:
            score -= 0.6

        mfg = parsed.get("mfg_id")
        if isinstance(mfg, int) and mfg in track.mfg_ids:
            score -= 0.2

        name = str(parsed.get("name", "Unknown") or "Unknown").strip()
        if name != "Unknown" and name in track.names:
            score -= 1.0

        trssi = track.scanner_rssi()
        if trssi:
            if alias is not None and alias.scanner_rssi():
                incoming_rssi = alias.scanner_rssi()
                vis_in = alias.scanner_visibility()
            else:
                incoming_rssi = {scanner: rssi}
                vis_in = {scanner}

            rel_track = relative_vector(trssi)
            rel_in = relative_vector(incoming_rssi)

            common = len(set(rel_track.keys()) & set(rel_in.keys()))
            rel_rmse = rmse_common(rel_track, rel_in)
            abs_rmse = rmse_common(trssi, incoming_rssi)

            if mobile_to_known_guard:
                if not self._mobile_service_merge_allowed(
                    incoming_rssi,
                    trssi,
                    strict_known_target=True,
                ):
                    self.rejected_track_expansion += 1
                    return float("inf")

            if alias_mobile and track_mobile and not mobile_to_known_guard:
                if not self._mobile_service_merge_allowed(
                    incoming_rssi,
                    trssi,
                    strict_known_target=False,
                ):
                    return float("inf")

            if not has_direct_continuity:
                # V7 gate:
                # Allow rotating/private aliases to merge, but only if their spatial
                # fingerprint is close in BOTH relative shape and absolute level.
                if rel_rmse is None or abs_rmse is None or common < MIN_COMMON_SCANNERS_STRONG_MATCH:
                    return float("inf")
                if rel_rmse > MAX_RSSI_RMSE_DIFFERENT_ALIAS_DB:
                    return float("inf")
                if abs_rmse > MAX_ABS_RSSI_RMSE_DIFFERENT_ALIAS_DB:
                    return float("inf")

                max_diff = max_abs_rssi_diff_common(trssi, incoming_rssi)
                if max_diff is None or max_diff > MAX_PER_SCANNER_RSSI_DIFF_DB:
                    return float("inf")

                if REQUIRE_TOP2_SCANNER_OVERLAP and not (top_scanners(trssi, 2) & top_scanners(incoming_rssi, 2)):
                    return float("inf")

                # Unknown -> known is the path that produced the bad Samsung bucket.
                # Only allow it when it is very close spatially.
                if alias_known == set() and track_known:
                    if rel_rmse > MAX_RSSI_RMSE_UNKNOWN_TO_KNOWN_DB or abs_rmse > MAX_ABS_RSSI_RMSE_UNKNOWN_TO_KNOWN_DB:
                        return float("inf")

                # If the track is already large, compare to the nearest existing alias
                # feature, not just the averaged track centroid.
                if alias is not None and len(track.alias_features) >= LARGE_TRACK_ALIAS_COUNT:
                    min_rel, min_abs, min_common = track.min_distance_to_alias_features(incoming_rssi)
                    if min_rel is None or min_abs is None or min_common < MIN_COMMON_SCANNERS_STRONG_MATCH:
                        self.rejected_track_expansion += 1
                        return float("inf")
                    if min_rel > MAX_RSSI_RMSE_LARGE_TRACK_DB or min_abs > MAX_ABS_RSSI_RMSE_LARGE_TRACK_DB:
                        self.rejected_track_expansion += 1
                        return float("inf")

                # Anti-drift guard: do not let a confirmed/large track become a
                # broader bucket over time.
                if alias is not None and track.would_expand_identity_too_much(incoming_rssi, alias_known):
                    self.rejected_track_expansion += 1
                    return float("inf")

                if alias is not None:
                    ov = time_overlap_ratio(
                        track.first_seen_mono, track.last_seen_mono,
                        alias.first_seen_mono, alias.last_seen_mono
                    )
                    if ov > HIGH_ALIAS_OVERLAP_RATIO:
                        # Penalty only, not rejection: same device can advertise several families.
                        score += 2.5

            if rel_rmse is not None and common >= MIN_COMMON_SCANNERS_STRONG_MATCH:
                score += 0.85 * rel_rmse
            elif rel_rmse is not None:
                score += 10.0 + 0.35 * rel_rmse
            else:
                score += 12.0

            if abs_rmse is not None and common >= MIN_COMMON_SCANNERS_STRONG_MATCH:
                score += 0.20 * abs_rmse

            vis_track = track.scanner_visibility()
            score += 5.0 * jaccard_distance(vis_track, vis_in)

        # Recency penalty.
        score += min(3.0, max(0.0, gap) / 3.0)

        # Advertising interval is already used above as a hard anti-merge gate via
        # adv_interval_compatible().
        #
        # Do not use scanner-local ts here as a score term. Each ESP32 scanner has
        # its own timestamp base, so interval scoring is only valid inside the
        # per-alias/per-scanner collectors in AliasTrack and DeviceTrack.update().
        return score


    def _periodic_merge_locked(self) -> None:
        now = time.monotonic()
        if now - self.last_merge_mono < 1.0:
            return
        self.last_merge_mono = now

        uids = list(self.tracks.keys())
        for i in range(len(uids)):
            a_uid = uids[i]
            if a_uid not in self.tracks:
                continue
            for j in range(i + 1, len(uids)):
                b_uid = uids[j]
                if b_uid not in self.tracks:
                    continue

                a = self.tracks[a_uid]
                b = self.tracks[b_uid]

                if not self._should_merge_tracks(a, b):
                    continue

                self._merge_tracks_locked(a_uid, b_uid)

    def _should_merge_tracks(self, a: DeviceTrack, b: DeviceTrack) -> bool:
        a_known = a.known_classes()
        b_known = b.known_classes()

        if STRICT_METADATA_CONFLICTS and a_known and b_known:
            if a_known.isdisjoint(b_known):
                return False

        a_mobile = a.is_mobile_service_data_like() or a.has_mobile_service_data()
        b_mobile = b.is_mobile_service_data_like() or b.has_mobile_service_data()

        # Same MAC means two payload families from the same physical BLE address.
        if a.macs & b.macs:
            return True

        # Advertising interval anti-merge gate.
        # Only applies when both tracks have enough interval samples.
        if not adv_interval_compatible(
            a.mean_adv_interval_ms(),
            a.adv_interval_sample_count(),
            b.mean_adv_interval_ms(),
            b.adv_interval_sample_count(),
        ):
            return False

        ar_raw = a.scanner_rssi()
        br_raw = b.scanner_rssi()
        ar = relative_vector(ar_raw)
        br = relative_vector(br_raw)
        common = set(ar.keys()) & set(br.keys())

        if len(common) < MIN_COMMON_SCANNERS_STRONG_MATCH:
            return False

        rel = rmse_common(ar, br)
        absr = rmse_common(ar_raw, br_raw)
        max_diff = max_abs_rssi_diff_common(ar_raw, br_raw)

        if rel is None or absr is None or max_diff is None:
            return False

        # Dedicated mobile-service merge policy.
        if a_mobile != b_mobile:
            known_side = a if a_known else b
            mobile_side = a if a_mobile else b
            if known_side.known_classes() and not known_side.is_mobile_service_data_like():
                if not self._mobile_service_merge_allowed(
                    mobile_side.scanner_rssi(),
                    known_side.scanner_rssi(),
                    strict_known_target=True,
                ):
                    self.rejected_track_expansion += 1
                    return False
        elif a_mobile and b_mobile:
            if not self._mobile_service_merge_allowed(
                a.scanner_rssi(),
                b.scanner_rssi(),
                strict_known_target=False,
            ):
                return False

        a_confirmed = a.is_confirmed()
        b_confirmed = b.is_confirmed()

        # Layered merge policy:
        # candidate-candidate: normal thresholds
        # candidate-confirmed: medium strict thresholds
        # confirmed-confirmed: very strict thresholds
        rel_limit = MAX_RSSI_RMSE_DIFFERENT_ALIAS_DB
        abs_limit = MAX_ABS_RSSI_RMSE_DIFFERENT_ALIAS_DB
        max_diff_limit = MAX_PER_SCANNER_RSSI_DIFF_DB
        require_same_strongest = False
        require_top2_exact = False

        if a_confirmed and b_confirmed:
            if DISABLE_CONFIRMED_CONFIRMED_DIFFERENT_MAC_MERGE:
                self.rejected_track_expansion += 1
                return False

            rel_limit = CONFIRMED_MERGE_REL_RMSE_DB
            abs_limit = CONFIRMED_MERGE_ABS_RMSE_DB
            max_diff_limit = CONFIRMED_MERGE_MAX_PER_SCANNER_DIFF_DB
            require_same_strongest = CONFIRMED_MERGE_REQUIRE_SAME_STRONGEST
            require_top2_exact = CONFIRMED_MERGE_REQUIRE_TOP2_EXACT

            # If both confirmed tracks have different MAC sets and are active over
            # the same time window, be extra conservative.
            overlap = time_overlap_ratio(a.first_seen_mono, a.last_seen_mono, b.first_seen_mono, b.last_seen_mono)
            if overlap >= CONFIRMED_HIGH_OVERLAP_REJECT_RATIO:
                rel_limit = min(rel_limit, 3.0)
                abs_limit = min(abs_limit, 5.0)
                max_diff_limit = min(max_diff_limit, 6.0)

        elif a_confirmed or b_confirmed:
            rel_limit = CANDIDATE_CONFIRMED_MERGE_REL_RMSE_DB
            abs_limit = CANDIDATE_CONFIRMED_MERGE_ABS_RMSE_DB
            max_diff_limit = CANDIDATE_CONFIRMED_MERGE_MAX_PER_SCANNER_DIFF_DB

        if rel > rel_limit or absr > abs_limit or max_diff > max_diff_limit:
            return False

        if require_same_strongest and strongest_scanner(ar_raw) != strongest_scanner(br_raw):
            return False

        top2_a = top_scanners(ar_raw, 2)
        top2_b = top_scanners(br_raw, 2)

        if require_top2_exact:
            if top2_a != top2_b:
                return False
        elif REQUIRE_TOP2_SCANNER_OVERLAP and not (top2_a & top2_b):
            return False

        # Unknown-only tracks can merge into known tracks only if extremely close.
        if (a_known and not b_known) or (b_known and not a_known):
            if rel > MAX_RSSI_RMSE_UNKNOWN_TO_KNOWN_DB or absr > MAX_ABS_RSSI_RMSE_UNKNOWN_TO_KNOWN_DB:
                return False

        # If either side is already a large bucket, require nearest-member agreement too.
        if len(a.alias_features) >= LARGE_TRACK_ALIAS_COUNT:
            min_rel, min_abs, min_common = a.min_distance_to_alias_features(br_raw)
            if min_rel is None or min_abs is None or min_common < MIN_COMMON_SCANNERS_STRONG_MATCH:
                return False
            if min_rel > min(MAX_RSSI_RMSE_LARGE_TRACK_DB, rel_limit) or min_abs > min(MAX_ABS_RSSI_RMSE_LARGE_TRACK_DB, abs_limit):
                return False

        if len(b.alias_features) >= LARGE_TRACK_ALIAS_COUNT:
            min_rel, min_abs, min_common = b.min_distance_to_alias_features(ar_raw)
            if min_rel is None or min_abs is None or min_common < MIN_COMMON_SCANNERS_STRONG_MATCH:
                self.rejected_track_expansion += 1
                return False
            if min_rel > min(MAX_RSSI_RMSE_LARGE_TRACK_DB, rel_limit) or min_abs > min(MAX_ABS_RSSI_RMSE_LARGE_TRACK_DB, abs_limit):
                self.rejected_track_expansion += 1
                return False

        # Final anti-drift guard: neither track may broaden the other's trusted
        # spatial identity.
        if not a.can_absorb_track_without_drift(b):
            self.rejected_track_expansion += 1
            return False

        if not b.can_absorb_track_without_drift(a):
            self.rejected_track_expansion += 1
            return False

        return True


    def _merge_tracks_locked(self, keep_uid: str, drop_uid: str) -> None:
        if keep_uid not in self.tracks or drop_uid not in self.tracks:
            return

        keep = self.tracks[keep_uid]
        drop = self.tracks[drop_uid]

        keep_known = keep.known_classes()
        drop_known = drop.known_classes()
        if STRICT_METADATA_CONFLICTS and keep_known and drop_known and keep_known.isdisjoint(drop_known):
            self.rejected_class_conflicts += 1
            return

        keep.packet_count += drop.packet_count
        keep.aliases |= drop.aliases
        keep.macs |= drop.macs
        keep.payload_sigs |= drop.payload_sigs
        keep.mfg_ids |= drop.mfg_ids
        keep.names |= drop.names
        keep.alias_features.update(drop.alias_features)

        for k, v in drop.metadata_classes.items():
            keep.metadata_classes[k] += v

        keep.mobile_service_uuids |= drop.mobile_service_uuids
        keep.mobile_service_packet_count += drop.mobile_service_packet_count

        for obs in drop.obs:
            keep.obs.append(obs)
        keep._prune_obs()

        for k, v in drop.last_alias_scanner_ts_us.items():
            keep.last_alias_scanner_ts_us[k] = max(keep.last_alias_scanner_ts_us.get(k, 0), v)

        for v in drop.adv_intervals_ms:
            keep.adv_intervals_ms.append(v)

        for scanner, vals in drop.last_known_rssi_vals.items():
            for rssi_val in vals:
                keep._update_last_known_rssi(scanner, rssi_val, max(keep.last_seen_mono, drop.last_seen_mono))

        keep.first_seen_mono = min(keep.first_seen_mono, drop.first_seen_mono)
        keep.last_seen_mono = max(keep.last_seen_mono, drop.last_seen_mono)
        for v in drop.burst_packet_counts:
            keep.burst_packet_counts.append(v)
        for v in drop.burst_durations_sec:
            keep.burst_durations_sec.append(v)
        for v in drop.burst_scanner_sets:
            keep.burst_scanner_sets.append(set(v))
        for v in drop.burst_rssi_maps:
            keep.burst_rssi_maps.append(dict(v))
        keep.burst_count += drop.burst_count
        keep.confirmed = keep.is_confirmed()

        for alias in drop.aliases:
            self.alias_to_uid[alias] = keep_uid

        del self.tracks[drop_uid]

    def _prune_stale_locked(self) -> None:
        now = time.monotonic()

        # Keep physical-device identities much longer than the live display timeout.
        # This is required for screen-off phones that disappear and later return as
        # another burst. They can be marked INACTIVE/INTERMITTENT in the API instead
        # of being deleted immediately.
        stale = [uid for uid, t in self.tracks.items() if now - t.last_seen_mono > IDENTITY_MEMORY_SEC]

        for uid in stale:
            tr = self.tracks.pop(uid, None)
            if tr:
                for alias in tr.aliases:
                    if self.alias_to_uid.get(alias) == uid:
                        del self.alias_to_uid[alias]

        stale_aliases = [
            alias_key for alias_key, alias in self.alias_tracks.items()
            if now - alias.last_seen_mono > TRACK_STALE_SEC
        ]
        for alias_key in stale_aliases:
            self.alias_tracks.pop(alias_key, None)
            self.alias_to_uid.pop(alias_key, None)

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            self._prune_stale_locked()
            tracks = []
            for uid, t in sorted(self.tracks.items()):
                confirmed, confirm_reason = t.confirm_quality()
                strongest_rssi_val = t.strongest_rssi_value()
                top2_avg_rssi_val = t.top2_avg_rssi_value()

                tracks.append({
                    "uid": uid,
                    "status": "CONFIRMED" if confirmed else "CANDIDATE",
                    "label": t.label(),
                    "dna": t.dna(),
                    "confirm_reason": confirm_reason,
                    "confirm_block_reason": "" if confirmed else confirm_reason,
                    "strongest_rssi": round(strongest_rssi_val, 2) if strongest_rssi_val is not None else None,
                    "top2_avg_rssi": round(top2_avg_rssi_val, 2) if top2_avg_rssi_val is not None else None,
                    "known_packet_count": t.known_packet_count(),
                    "unknown_packet_count": t.unknown_packet_count(),
                    "known_packet_ratio": round(t.known_packet_ratio(), 3),
                    "weak_track_label": "" if confirmed else (WEAK_TRACK_LABEL if confirm_reason == "weak_rssi" else ""),
                    "presence_state": t.presence_state(),
                    "device_role": t.device_role(),
                    "phone_likelihood": round(t.phone_likelihood_score(), 2),
                    "outside_likelihood": round(t.outside_likelihood_score(), 2),
                    "mobile_service_score": round(t.mobile_service_score(), 2),
                    "mobile_service_presence": t.mobile_service_presence(),
                    "mobile_service_uuids": sorted(list(t.mobile_service_uuids or mobile_service_uuids_from_sigs(t.payload_sigs))),
                    "mobile_service_packet_count": t.mobile_service_packet_count,
                    "mobile_service_packet_ratio": round(t.mobile_service_packet_ratio(), 3),
                    "mobile_service_reason": t.mobile_service_reason(),
                    "localizable": t.is_localizable(),
                    "burst_count": t.burst_count,
                    "last_burst_age_sec": round(t.last_burst_age_sec(), 2),
                    "avg_burst_duration_sec": round(t.avg_burst_duration_sec(), 2),
                    "avg_packets_per_burst": round(t.avg_packets_per_burst(), 2),
                    "recent_burst_scanners": sorted(list(t.recent_burst_scanners())),
                    "last_known_scanner_rssi": dict(t.last_known_scanner_rssi),
                    "last_known_num_scanners": len(t.last_known_scanner_visibility()),
                    "last_known_strongest_scanner": strongest_scanner(t.last_known_scanner_rssi),
                    "last_known_top2_scanners": sorted(list(top_scanners(t.last_known_scanner_rssi, 2))),
                    "last_known_strongest_rssi": round(t.last_known_strongest_rssi_value(), 2) if t.last_known_strongest_rssi_value() is not None else None,
                    "last_known_top2_avg_rssi": round(t.last_known_top2_avg_rssi_value(), 2) if t.last_known_top2_avg_rssi_value() is not None else None,
                    "packets": t.packet_count,
                    "num_macs": len(t.macs),
                    "num_payloads": len(t.payload_sigs),
                    "num_aliases": len(t.aliases),
                    "num_scanners": len(t.scanner_visibility()),
                    "age_sec": round(t.age_sec(), 2),
                    "last_seen_age_sec": round(time.monotonic() - t.last_seen_mono, 2),
                    "scanner_rssi": t.scanner_rssi(),
                    "macs": sorted(list(t.macs))[:10],
                    "payload_sigs": sorted(list(t.payload_sigs))[:10],
                    "known_classes": sorted(list(t.known_classes())),
                    "class_counts": dict(sorted(t.metadata_classes.items())),
                    "alias_feature_count": len(t.alias_features),
                    "identity_core_aliases": len(t.alias_features),
                    "adv_interval_mean_ms": round(t.mean_adv_interval_ms(), 2) if t.mean_adv_interval_ms() is not None else None,
                    "adv_interval_samples": t.adv_interval_sample_count(),
                    "adv_interval_source": "scanner_local_same_alias_same_scanner",
                    "strongest_scanner": strongest_scanner(t.scanner_rssi()),
                    "top2_scanners": sorted(list(top_scanners(t.scanner_rssi(), 2))),
                })

            return {
                "enabled": TRACKER_ENABLED,
                "tracks": tracks,
                "num_tracks": len(tracks),
                "num_alias_tracks": len(self.alias_tracks),
                "num_confirmed": sum(1 for t in self.tracks.values() if t.is_confirmed()),
                "rejected_class_conflicts": self.rejected_class_conflicts,
                "rejected_track_expansion": self.rejected_track_expansion,
                "blocked_confirmation_weak_rssi": self.blocked_confirmation_weak_rssi,
                "blocked_confirmation_unknown_heavy": self.blocked_confirmation_unknown_heavy,
                "phone_like_tracks": sum(1 for t in self.tracks.values() if t.is_phone_like()),
                "outside_stable_tracks": sum(1 for t in self.tracks.values() if t.is_outside_stable_source()),
                "stable_device_tracks": sum(1 for t in self.tracks.values() if t.is_stable_device()),
                "mobile_service_tracks": sum(1 for t in self.tracks.values() if t.is_mobile_service_data_like()),
                "presence_thresholds": {
                    "live_active_timeout_sec": LIVE_ACTIVE_TIMEOUT_SEC,
                    "identity_memory_sec": IDENTITY_MEMORY_SEC,
                    "burst_gap_sec": BURST_GAP_SEC,
                    "min_bursts_for_intermittent_phone": MIN_BURSTS_FOR_INTERMITTENT_PHONE,
                    "phone_like_score_threshold": PHONE_LIKE_SCORE_THRESHOLD,
                    "outside_stable_min_age_sec": OUTSIDE_STABLE_MIN_AGE_SEC,
                    "outside_stable_max_scanners": OUTSIDE_STABLE_MAX_SCANNERS,
                    "outside_stable_max_strongest_rssi_dbm": OUTSIDE_STABLE_MAX_STRONGEST_RSSI_DBM,
                    "stable_device_min_age_sec": STABLE_DEVICE_MIN_AGE_SEC,
                    "stable_device_min_packets": STABLE_DEVICE_MIN_PACKETS,
                    "stable_device_min_avg_packets_per_burst": STABLE_DEVICE_MIN_AVG_PACKETS_PER_BURST,
                    "phone_bursty_max_avg_burst_duration_sec": PHONE_BURSTY_MAX_AVG_BURST_DURATION_SEC,
                    "phone_bursty_max_avg_packets_per_burst": PHONE_BURSTY_MAX_AVG_PACKETS_PER_BURST,
                    "mobile_service_uuids": sorted(list(MOBILE_SERVICE_UUIDS)),
                    "mobile_service_score_threshold": MOBILE_SERVICE_SCORE_THRESHOLD,
                    "mobile_service_high_score_threshold": MOBILE_SERVICE_HIGH_SCORE_THRESHOLD,
                    "mobile_service_min_packets": MOBILE_SERVICE_MIN_PACKETS,
                    "mobile_service_min_scanners": MOBILE_SERVICE_MIN_SCANNERS,
                    "mobile_to_known_max_rel_rmse_db": MOBILE_TO_KNOWN_MAX_REL_RMSE_DB,
                    "mobile_to_known_max_abs_rmse_db": MOBILE_TO_KNOWN_MAX_ABS_RMSE_DB,
                    "mobile_to_mobile_max_rel_rmse_db": MOBILE_TO_MOBILE_MAX_REL_RMSE_DB,
                    "mobile_to_mobile_max_abs_rmse_db": MOBILE_TO_MOBILE_MAX_ABS_RMSE_DB,
                },
                "confirmation_thresholds": {
                    "min_packets": CONFIRM_MIN_PACKETS,
                    "min_scanners": CONFIRM_MIN_SCANNERS,
                    "min_duration_sec": CONFIRM_MIN_DURATION_SEC,
                    "min_strongest_rssi_dbm": CONFIRM_MIN_STRONGEST_RSSI_DBM,
                    "min_top2_avg_rssi_dbm": CONFIRM_MIN_TOP2_AVG_RSSI_DBM,
                    "min_known_packets": CONFIRM_MIN_KNOWN_PACKETS,
                    "min_known_ratio": CONFIRM_MIN_KNOWN_RATIO,
                },
            }


device_tracker = DeviceTracker()


# ---------------- Calibration ----------------

def print_calibration_progress(force: bool = False) -> None:
    now = time.time()
    if not force and now - calib_state.get("last_progress_print", 0.0) < 1.0:
        return

    calib_state["last_progress_print"] = now

    print("\n--- Calibration Progress ---")
    print(f"{'Scanner ID':<12} | {'Ch37':<7} | {'Ch38':<7} | {'Ch39':<7}")
    print("-" * 42)

    for s_id in sorted(calib_state["buckets"].keys(), key=str):
        channels = calib_state["buckets"][s_id]
        c37 = len(channels.get(37, []))
        c38 = len(channels.get(38, []))
        c39 = len(channels.get(39, []))
        print(
            f"Scanner {str(s_id):<3} | "
            f"{c37:2}/{SAMPLES_PER_CHANNEL} | "
            f"{c38:2}/{SAMPLES_PER_CHANNEL} | "
            f"{c39:2}/{SAMPLES_PER_CHANNEL}"
        )

    print("-" * 42)


def is_calibration_packet(mac: str, payload: str, parsed: Dict[str, Any]) -> bool:
    """
    Calibration matching:
    1. If manual_mac was provided, only that MAC matches.
    2. Otherwise, accept packets whose decoded name contains CALIBRATION_TARGET.
    3. After first target payload is found, also accept same payload signature.
       This protects us if a device rotates MAC while keeping its calibration advertisement format.
    """
    name = str(parsed.get("name", "Unknown") or "Unknown").lower()
    sig = parsed.get("payload_sig", payload_signature(payload))

    manual_mac = calib_state.get("target_mac")
    target_sig = calib_state.get("target_payload_sig")

    if manual_mac:
        return mac.upper() == str(manual_mac).upper().strip()

    if CALIBRATION_TARGET.lower() in name:
        if not calib_state.get("target_payload_sig"):
            calib_state["target_payload_sig"] = sig
        return True

    if target_sig and sig == target_sig:
        return True

    return False


def check_calib_completion_locked() -> None:
    """
    Must be called with calib_lock held.
    """
    scanners_to_check = [
        s_id for s_id, data in active_scanners.items()
        if time.time() - data.get("last_seen", 0) < 30
    ]

    if not scanners_to_check:
        return

    for s_id in scanners_to_check:
        if s_id not in calib_state["buckets"]:
            return
        for ch in [37, 38, 39]:
            if len(calib_state["buckets"][s_id].get(ch, [])) < SAMPLES_PER_CHANNEL:
                return

    if calib_state["active"]:
        print("\n[!] Calibration complete.")
        save_calibration_results_locked()


def save_calibration_results_locked() -> None:
    """
    Must be called with calib_lock held.
    """
    filename = os.path.join(BASE_DIR, "calibration_log.csv")
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "X_Pos", "Y_Pos", "Z_Pos", "Scanner_ID",
                "Avg_RSSI_Ch37", "Avg_RSSI_Ch38", "Avg_RSSI_Ch39"
            ])

        for s_id in sorted(calib_state["buckets"].keys(), key=str):
            channels = calib_state["buckets"][s_id]
            avgs = [
                sum(channels[ch]) / len(channels[ch]) if channels.get(ch) else 0
                for ch in [37, 38, 39]
            ]
            writer.writerow([
                calib_state["coords"]["x"],
                calib_state["coords"]["y"],
                calib_state["coords"]["z"],
                s_id,
                *avgs,
            ])

    calib_state["active"] = False
    print(f"[CALIB] Saved calibration point to {filename}")

    # Stop scanners after calibration. This is a request to our own HTTP endpoint.
    try:
        requests.post(
            "http://localhost:8000/api/control/send",
            json={"target": "all", "state": 0},
            timeout=2,
        )
    except Exception as e:
        print(f"[CALIB] Calibration saved, but failed to stop scanners automatically: {e}")


def update_calibration_if_needed(ev: Dict[str, Any], parsed: Dict[str, Any]) -> None:
    with calib_lock:
        if not calib_state["active"]:
            return

        mac = ev["mac"]
        payload = ev["payload"]
        scanner_id = ev["scanner"]
        channel = ev["channel"]
        rssi = ev["rssi"]

        if channel not in (37, 38, 39):
            return

        if not is_calibration_packet(mac, payload, parsed):
            return

        if scanner_id not in calib_state["buckets"]:
            calib_state["buckets"][scanner_id] = {37: [], 38: [], 39: []}

        bucket = calib_state["buckets"][scanner_id].setdefault(channel, [])

        if len(bucket) < SAMPLES_PER_CHANNEL:
            bucket.append(rssi)
            print_calibration_progress(force=False)
            check_calib_completion_locked()


# ---------------- Processing pipeline ----------------

def process_final_event(ev: Dict[str, Any]) -> None:
    """
    Handles parsing, calibration, DeviceTracker assignment, and queueing for filtered peak events.
    """
    parsed = parse_payload(ev["payload"])

    update_calibration_if_needed(ev, parsed)

    if TRACKER_ENABLED:
        ident = device_tracker.process_event(ev, parsed)
    else:
        sig = parsed.get("payload_sig", payload_signature(ev["payload"]))
        ident = {
            "uid": f"ALIAS_{sig}",
            "status": "UNCLASSIFIED",
            "dna": sig,
            "physical_label": "Alias only",
        }

    out = {
        "mac": ev["mac"],
        "rssi": ev["rssi"],
        "channel": ev["channel"],
        "scanner": ev["scanner"],
        "payload": ev["payload"],
        "ts": ev["ts"],
        "rx_ts_us": ev.get("rx_ts_us"),

        # Parsed fields for UI and future localization.
        "name": parsed.get("name", "Unknown"),
        "mfg_id": parsed.get("mfg_id"),
        "mfg_name": parsed.get("mfg_name", ""),
        "mfg_data_hex": parsed.get("mfg_data_hex", ""),
        "txpwr": parsed.get("tx_pwr"),
        "adv_len": parsed.get("adv_len", 0),
        "payload_sig": parsed.get("payload_sig", payload_signature(ev["payload"])),
        "services_16": parsed.get("services_16", []),
        "services_128": parsed.get("services_128", []),
        "services_32": parsed.get("services_32", []),
        "service_data": parsed.get("service_data", []),
        "ad_structure": parsed.get("ad_structure", ""),

        # Physical-device identity fields.
        "uid": ident["uid"],
        "status": ident["status"],
        "dna": ident["dna"],
        "physical_label": ident.get("physical_label", ident["uid"]),
        "confirm_reason": ident.get("confirm_reason", ""),
        "presence_state": ident.get("presence_state", ""),
        "device_role": ident.get("device_role", ""),
        "phone_likelihood": ident.get("phone_likelihood", 0.0),
        "outside_likelihood": ident.get("outside_likelihood", 0.0),
        "mobile_service_score": ident.get("mobile_service_score", 0.0),
        "mobile_service_presence": ident.get("mobile_service_presence", "NONE"),
        "mobile_service_uuids": ident.get("mobile_service_uuids", []),
        "mobile_service_reason": ident.get("mobile_service_reason", ""),
        "localizable": ident.get("localizable", False),
    }

    try:
        data_queue.put_nowait(out)
        with stats_lock:
            stats["streamed_events"] += 1
    except queue.Full:
        with stats_lock:
            stats["dropped_queue_full"] += 1


def window_processor() -> None:
    """
    Background thread that processes buffered raw events in 100 ms windows.

    Important:
    The peak filter preserves scanner+channel observations:
        (mac, payload) -> (scanner, channel) -> strongest RSSI event

    This does not change the JSON format, but it prevents us from losing channel-labeled
    observations before the DeviceTracker sees them.
    """
    global event_buffer

    print("[WINDOW] Window processor started.")

    while True:
        time.sleep(0.05)

        with buffer_lock:
            if not event_buffer:
                continue

            # Use receiver-local monotonic time for buffering/windowing.
            # Scanner "ts" is local to each ESP32 and changes on reset, so it must not
            # be used to compare events across scanners.
            event_buffer.sort(key=lambda x: x.get("rx_ts_us", x["ts"]))
            newest_ts = event_buffer[-1].get("rx_ts_us", event_buffer[-1]["ts"])
            threshold_ts = newest_ts - SAFETY_MARGIN_US

            to_process = [e for e in event_buffer if e.get("rx_ts_us", e["ts"]) <= threshold_ts]
            event_buffer = [e for e in event_buffer if e.get("rx_ts_us", e["ts"]) > threshold_ts]

        if not to_process:
            continue

        buckets: Dict[int, list] = defaultdict(list)
        for ev in to_process:
            bucket_idx = ev.get("rx_ts_us", ev["ts"]) // WINDOW_SIZE_US
            buckets[bucket_idx].append(ev)

        for idx in sorted(buckets.keys()):
            batch = buckets[idx]

            # key: (mac, payload) -> {(scanner_id, channel): max_rssi_event}
            uniques: Dict[Tuple[str, str], Dict[Tuple[str, int], Dict[str, Any]]] = {}

            for ev in batch:
                pk_key = (ev["mac"], ev["payload"])
                obs_key = (ev["scanner"], ev["channel"])

                if pk_key not in uniques:
                    uniques[pk_key] = {}

                current = uniques[pk_key].get(obs_key)
                if current is None or ev["rssi"] > current["rssi"]:
                    uniques[pk_key][obs_key] = ev

            for scanner_channel_peaks in uniques.values():
                for peak_ev in scanner_channel_peaks.values():
                    process_final_event(peak_ev)
                    with stats_lock:
                        stats["processed_events"] += 1


# ---------------- Flask routes ----------------

@app.route("/api/ble/ingest", methods=["POST"])
def ingest():
    data = request.get_json()
    if not data:
        return "Invalid JSON", 400

    scanner_id = str(data.get("scanner", "unknown")).strip()
    if not scanner_id:
        scanner_id = "unknown"

    active_scanners[scanner_id] = {
        "ip": request.remote_addr,
        "last_seen": time.time(),
    }

    events = data.get("events", [])
    if not isinstance(events, list):
        return "Invalid events list", 400

    accepted = 0
    bad = 0
    now_us = int(time.time() * 1_000_000)
    rx_batch_us = time.monotonic_ns() // 1000

    with buffer_lock:
        for ev in events:
            try:
                mac_raw = str(ev.get("a", ev.get("mac", ""))).upper().strip()
                if not mac_raw:
                    bad += 1
                    continue

                ts = safe_int(ev.get("ts", 0), 0)
                if ts == 0:
                    ts = now_us

                rssi = safe_int(ev.get("r", ev.get("rssi", 0)), 0)
                channel = safe_int(ev.get("c", ev.get("channel", 0)), 0)
                payload = ev.get("p", ev.get("payload", "")) or ""

                # Keep the JSON data as-is, but normalize internal field names.
                event_buffer.append({
                    "mac": mac_raw,
                    "rssi": rssi,
                    "channel": channel,
                    "payload": payload,
                    "ts": ts,                  # scanner-local timestamp, kept for logs/session compatibility
                    "rx_ts_us": rx_batch_us + accepted,  # receiver-local monotonic timestamp for windowing/debug
                    "scanner": scanner_id,
                })
                accepted += 1
            except Exception:
                bad += 1

    with stats_lock:
        stats["ingest_events"] += accepted
        stats["bad_events"] += bad

    return jsonify({"status": "ack", "accepted": accepted, "bad": bad}), 200


@app.route("/api/control/scanners", methods=["GET"])
def get_scanners():
    alive = {
        k: v for k, v in active_scanners.items()
        if time.time() - v.get("last_seen", 0) < 30
    }
    return jsonify(alive)


@app.route("/api/control/send", methods=["POST"])
def send_command():
    data = request.get_json() or {}

    target = str(data.get("target", "")).strip()
    state = data.get("state")
    mode = data.get("mode")

    params = []
    if state is not None:
        params.append(f"state={state}")
    if mode is not None:
        params.append(f"mode={mode}")

    query = "&".join(params)
    targets = list(active_scanners.keys()) if target == "all" else [target]
    results = {}

    print(f"[CONTROL] Sending command '{query}' to {targets}")

    for t_id in targets:
        if t_id in active_scanners:
            ip = active_scanners[t_id]["ip"]
            try:
                r = requests.get(f"http://{ip}/cmd?{query}", timeout=2)
                if r.status_code == 200:
                    results[t_id] = "Success"
                else:
                    results[t_id] = f"HTTP {r.status_code}"
            except Exception as e:
                print(f"[CONTROL ERROR] Failed to reach scanner {t_id} at {ip}: {e}")
                results[t_id] = "Error"
        else:
            results[t_id] = "Unknown scanner"

    return jsonify(results)


@app.route("/api/calibrate/start", methods=["POST"])
def start_calib():
    data = request.get_json() or {}

    coords = data.get("coords") or {"x": 0.0, "y": 0.0, "z": 0.0}
    manual_mac = data.get("manual_mac")

    with calib_lock:
        calib_state.update({
            "active": True,
            "target_mac": str(manual_mac).upper().strip() if manual_mac else None,
            "target_payload_sig": None,
            "coords": {
                "x": float(coords.get("x", 0.0)),
                "y": float(coords.get("y", 0.0)),
                "z": float(coords.get("z", 0.0)),
            },
            "buckets": {},
            "last_progress_print": 0.0,
        })

    target_desc = calib_state["target_mac"] or f'name contains "{CALIBRATION_TARGET}"'
    print(f"\n[*] Calibration started for coords={calib_state['coords']} target={target_desc}")

    return jsonify({"status": "Started"}), 200


@app.route("/api/ble/stream")
def stream():
    def event_stream():
        yield ": open\n\n"
        while True:
            try:
                ev = data_queue.get(timeout=3)
                yield f"data: {json.dumps(ev)}\n\n"
            except queue.Empty:
                yield ": heartbeat\n\n"
            except GeneratorExit:
                break
            except Exception:
                break

    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/api/stats", methods=["GET"])
def get_stats():
    with stats_lock:
        snapshot = dict(stats)

    tracker_snapshot = device_tracker.snapshot()

    snapshot["queue_size"] = data_queue.qsize()
    snapshot["buffer_size"] = len(event_buffer)
    snapshot["active_scanners"] = len([
        s for s, d in active_scanners.items()
        if time.time() - d.get("last_seen", 0) < 30
    ])
    snapshot["tracker"] = tracker_snapshot

    with stats_lock:
        stats["tracker_tracks"] = tracker_snapshot["num_tracks"]
        stats["tracker_confirmed"] = tracker_snapshot["num_confirmed"]
        stats["tracker_rejected_class_conflicts"] = tracker_snapshot.get("rejected_class_conflicts", 0)
        stats["tracker_rejected_track_expansion"] = tracker_snapshot.get("rejected_track_expansion", 0)
        stats["tracker_blocked_confirmation_weak_rssi"] = tracker_snapshot.get("blocked_confirmation_weak_rssi", 0)
        stats["tracker_blocked_confirmation_unknown_heavy"] = tracker_snapshot.get("blocked_confirmation_unknown_heavy", 0)
        stats["tracker_phone_like_tracks"] = tracker_snapshot.get("phone_like_tracks", 0)
        stats["tracker_outside_stable_tracks"] = tracker_snapshot.get("outside_stable_tracks", 0)
        stats["tracker_stable_device_tracks"] = tracker_snapshot.get("stable_device_tracks", 0)
        stats["tracker_mobile_service_tracks"] = tracker_snapshot.get("mobile_service_tracks", 0)

    return jsonify(snapshot)


@app.route("/api/devices", methods=["GET"])
def get_devices():
    return jsonify(device_tracker.snapshot())


# ---------------- Background diagnostics ----------------

def stats_reporter() -> None:
    last = None
    while True:
        time.sleep(1.0)
        with stats_lock:
            cur = dict(stats)

        if last is None:
            last = cur
            continue

        din = cur["ingest_events"] - last["ingest_events"]
        dproc = cur["processed_events"] - last["processed_events"]
        dout = cur["streamed_events"] - last["streamed_events"]
        drops = cur["dropped_queue_full"] - last["dropped_queue_full"]
        parse_errs = cur["parse_errors"] - last["parse_errors"]

        last = cur

        alive = [
            s for s, d in active_scanners.items()
            if time.time() - d.get("last_seen", 0) < 30
        ]

        tracker_snapshot = device_tracker.snapshot()
        n_tracks = tracker_snapshot["num_tracks"]
        n_confirmed = tracker_snapshot["num_confirmed"]

        if din or dproc or dout or drops or parse_errs:
            print(
                f"[STATS] in={din}/s processed={dproc}/s streamed={dout}/s "
                f"drops={drops}/s parse_err={parse_errs}/s "
                f"tracks={n_tracks} confirmed={n_confirmed} "
                f"phone_like={tracker_snapshot.get('phone_like_tracks', 0)} "
                f"stable={tracker_snapshot.get('stable_device_tracks', 0)} "
                f"mobile={tracker_snapshot.get('mobile_service_tracks', 0)} "
                f"outside={tracker_snapshot.get('outside_stable_tracks', 0)} "
                f"weak_block={tracker_snapshot.get('blocked_confirmation_weak_rssi', 0)} "
                f"unknown_block={tracker_snapshot.get('blocked_confirmation_unknown_heavy', 0)} "
                f"buffer={len(event_buffer)} queue={data_queue.qsize()} "
                f"scanners={alive}"
            )


def get_local_ip() -> str:
    """
    More reliable than socket.gethostbyname(socket.gethostname()) on some Windows setups.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.2)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return socket.gethostbyname(socket.gethostname())


if __name__ == "__main__":
    load_mfg_ids()
    my_ip = get_local_ip()
    zc_instance = start_mdns(my_ip, 8000)

    threading.Thread(target=window_processor, daemon=True).start()
    threading.Thread(target=stats_reporter, daemon=True).start()

    try:
        app.run(host="0.0.0.0", port=8000, threaded=True)
    finally:
        zc_instance.unregister_all_services()
        zc_instance.close()
