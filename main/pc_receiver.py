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
from datetime import datetime
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, Response, jsonify, request, render_template
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

# Generic manufacturer conflict policy. This is intentionally manufacturer-agnostic:
# once a physical cluster contains a concrete Bluetooth manufacturer/company ID,
# another concrete manufacturer ID cannot merge into it. Unknown/no-MFG packets may
# still attach to manufacturer clusters, but only through the existing strong
# RSSI/payload/time evidence gates.
STRICT_MANUFACTURER_ID_CONFLICTS = True

# Role labeling guards. Weak low-confidence known tracks should not become
# PHONE_LIKE just because their class label is a phone brand during tests.
PHONE_LIKE_BLOCK_WEAK_RSSI_DBM = -80.0
PHONE_LIKE_BLOCK_LOW_MARGIN_DB = 4.0
KNOWN_DOMINANT_BACKGROUND_OVERRIDE_RATIO = 0.85

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

# Generic identity-continuity hard merge.
# This is deliberately not brand-specific. It is used for cases where two tracks
# are almost certainly the same BLE identity family after MAC/payload rotation.
IDENTITY_CONTINUITY_REQUIRE_SHARED_LOW_LEVEL_EVIDENCE = True
IDENTITY_CONTINUITY_MAX_REL_RMSE_DB = 3.2
IDENTITY_CONTINUITY_MAX_ABS_RMSE_DB = 6.5
IDENTITY_CONTINUITY_MAX_PER_SCANNER_DIFF_DB = 7.0
IDENTITY_CONTINUITY_REQUIRE_SAME_STRONGEST = True
IDENTITY_CONTINUITY_REQUIRE_TOP2_OVERLAP = True
IDENTITY_CONTINUITY_MAX_LAST_SEEN_GAP_SEC = IDENTITY_MEMORY_SEC
IDENTITY_CONTINUITY_MIN_SHARED_CRC = 1

# Cross-personality association is weaker than a hard merge. It links a clean
# mobile-service identity to a known/manufacturer-data identity when they likely
# belong to the same physical object, but keeps raw PDs visible for debugging.
CROSS_PERSONALITY_ASSOCIATIONS_ENABLED = True
CROSS_PERSONALITY_MAX_REL_RMSE_DB = 4.2
CROSS_PERSONALITY_MAX_ABS_RMSE_DB = 20.0
CROSS_PERSONALITY_MAX_PER_SCANNER_DIFF_DB = 22.0
CROSS_PERSONALITY_REQUIRE_SAME_STRONGEST = True
CROSS_PERSONALITY_REQUIRE_TOP2_OVERLAP = True
CROSS_PERSONALITY_MIN_CONFIDENCE = 0.62

# Association tuning v2:
# Associations are useful for cross-personality evidence, but the UI should not
# show a mobile-service track as "probably associated" with many unrelated known
# tracks. Keep only a clear best association per mobile-service track. If the
# best and second-best candidates are too close, mark the result as ambiguous.
CROSS_PERSONALITY_ACTIVE_MIN_TIME_OVERLAP = 0.50
CROSS_PERSONALITY_MEMORY_MAX_LAST_SEEN_GAP_SEC = 30.0
CROSS_PERSONALITY_BEST_ONLY = True
CROSS_PERSONALITY_BEST_MIN_CONFIDENCE = 0.75
CROSS_PERSONALITY_BEST_MARGIN = 0.08

# In high absolute-offset cases, allow an association only when the relative
# scanner shape is very strong. This preserves same-phone MFG<->service-data
# behavior while rejecting weaker same-area matches.
CROSS_PERSONALITY_HIGH_ABS_DIFF_DB = 12.0
CROSS_PERSONALITY_HIGH_ABS_REQUIRE_REL_RMSE_DB = 2.2

# API display tuning. Weak inactive candidates remain in memory for merging, but
# are moved out of the main tracks list to reduce dashboard clutter.
HIDE_WEAK_INACTIVE_CANDIDATES_IN_MAIN_API = True
WEAK_INACTIVE_MAX_PACKETS_FOR_HIDE = 10

# ---------------- Background/mobile false-positive suppression ----------------
# Test conclusion:
# scanner-4-dominant FCF1/FEF3 families can persist for many minutes while not
# being real in-room phones. Treat those as background mobile-service unless
# they are strong, clearly localizable, or clearly associated with a known phone.
BACKGROUND_MOBILE_ROLE = "BACKGROUND_MOBILE_SERVICE"
BACKGROUND_MOBILE_MIN_AGE_SEC = 45.0
BACKGROUND_MOBILE_MIN_PACKETS = 120
BACKGROUND_MOBILE_MAX_STRONGEST_RSSI_DBM = -75.0
BACKGROUND_MOBILE_MAX_TOP2_AVG_RSSI_DBM = -78.0
BACKGROUND_MOBILE_MIN_MOBILE_RATIO = 0.60
BACKGROUND_MOBILE_MIN_UNKNOWN_RATIO = 0.50
BACKGROUND_MOBILE_MAX_LOCATION_MARGIN_DB = 6.0
BACKGROUND_MOBILE_UNKNOWN_HEAVY_KNOWN_RATIO_MAX = 0.20

# Location confidence is separated from device identity. A weak strongest scanner
# with only a small margin over the runner-up should not be presented as a strong
# location statement.
LOCATION_HIGH_MARGIN_DB = 8.0
LOCATION_MEDIUM_MARGIN_DB = 4.0
LOCATION_STRONG_RSSI_DBM = -75.0
LOCATION_WEAK_RSSI_DBM = -85.0

# Generic weak-flat background suppression.
# If almost all scanners see roughly -90 dBm and no scanner clearly dominates,
# the source is probably outside/background RF, not an in-room phone.
# This is manufacturer-agnostic and applies to all classes.
WEAK_FLAT_BACKGROUND_ROLE = "WEAK_FLAT_BACKGROUND"
WEAK_FLAT_MIN_SCANNERS = 3
WEAK_FLAT_SCANNER_RSSI_DBM = -90.0
WEAK_FLAT_MIN_WEAK_SCANNERS = 3
WEAK_FLAT_MAX_STRONGEST_RSSI_DBM = -88.0
WEAK_FLAT_MAX_TOP2_AVG_RSSI_DBM = -89.0
WEAK_FLAT_MAX_MARGIN_DB = 4.0
WEAK_FLAT_BLOCK_CONFIRMATION = True

# Mixed-track pollution detection. If one PD contains mobile-service and known
# payload families with very different per-payload RSSI vectors, treat it as a
# split/polluted track rather than one physical phone.
POLLUTION_MIN_PAYLOAD_PACKETS = 20
POLLUTION_MIN_COMMON_SCANNERS = 2
POLLUTION_REL_RMSE_DB = 7.0
POLLUTION_ABS_RMSE_DB = 10.0
POLLUTION_MAX_SCANNER_DIFF_DB = 12.0

# Protect against associating mobile-service aliases with fixed/stable devices
# such as TVs, laptops, beacons, or other stationary infrastructure.
CROSS_PERSONALITY_BLOCK_STABLE_KNOWN_TARGETS = True
CROSS_PERSONALITY_STABLE_STRONG_RSSI_DBM = -55.0
CROSS_PERSONALITY_STABLE_MIN_AGE_SEC = 45.0
CROSS_PERSONALITY_STABLE_MIN_PACKETS = 120

# ---------------- Calibration settings ----------------

CALIBRATION_TARGET = "eldarcalib"

# The ESP32/NimBLE scan callback used by the current scanner firmware does not
# expose the real BLE advertising RF channel. The field named channel/c is only
# a software label, so calibration must be based on per-scanner RSSI samples.
CHANNEL_LABEL_IS_REAL_BLE_CHANNEL = False
SAMPLES_PER_SCANNER = 100

CALIBRATION_RAW_CSV = "calibration_raw.csv"
CALIBRATION_SUMMARY_CSV = "calibration_summary.csv"

calib_state = {
    "active": False,
    "grid_block": None,             # 3x3 grid block ID entered by the user
    "session_id": "",
    "buckets": {},                  # scanner_id -> [raw measurement dicts]
    "last_progress_print": 0.0,
}

calib_lock = threading.Lock()

# ---------------- Grid localization settings ----------------

GRID_LOCALIZATION_ENABLED = True
LOCALIZATION_FINGERPRINTS_JSON = "calibration_fingerprints.json"

# The localization engine intentionally ignores Tx Power. It relies on
# calibrated multi-scanner RSSI shape, absolute level as a weak secondary clue,
# and RSSI STD as both reliability and fingerprint behavior.
LOCALIZATION_REQUIRE_MOBILE_ROLE = True
LOCALIZATION_MIN_SCANNERS = 3
LOCALIZATION_MIN_STD_SAMPLES = 5

# Score weights.
# V2 tuning:
#   - Relative RSSI is still important because live device Tx power is unknown.
#   - Absolute level is stronger than before because calibration showed nearby
#     blocks can share the same dominant scanner and relative shape.
#   - STD is used as a real fingerprint feature, not only as noise.
#   - Shape score adds dominant/top-2/weakest/margin checks to stop one block
#     from absorbing neighboring blocks.
LOCALIZATION_RELATIVE_WEIGHT = 0.40
LOCALIZATION_ABSOLUTE_WEIGHT = 0.35
LOCALIZATION_STD_WEIGHT = 0.15
LOCALIZATION_SHAPE_WEIGHT = 0.10

LOCALIZATION_SOFTMAX_TEMPERATURE = 0.85
LOCALIZATION_HIGH_PROBABILITY = 0.65
LOCALIZATION_MEDIUM_PROBABILITY = 0.45
LOCALIZATION_HIGH_MARGIN = 0.22
LOCALIZATION_MEDIUM_MARGIN = 0.12

# Do not force a block when the model is not confident enough. Those devices
# remain visible in "mobile candidates without block" instead of being placed
# in a wrong block.
LOCALIZATION_ASSIGN_MIN_PROBABILITY = 0.45
LOCALIZATION_ASSIGN_MIN_MARGIN = 0.12

# Simple hysteresis to avoid block jumping on noisy BLE windows.
LOCALIZATION_SWITCH_PROBABILITY = 0.70
LOCALIZATION_KEEP_PREVIOUS_MARGIN = 0.08

# Motion evidence is required before generic PHONE_LIKE/UNKNOWN tracks are
# localized. This prevents stationary Apple/Microsoft/Samsung/background devices
# from appearing as mobile just because their metadata looks phone-like.
LOCALIZATION_REQUIRE_MOTION_FOR_PHONE_LIKE = True
LOCALIZATION_MOTION_MIN_BURST_MAPS = 2
LOCALIZATION_MOTION_REL_RMSE_DB = 5.5
LOCALIZATION_MOTION_MIN_COMMON_SCANNERS = 2
LOCALIZATION_PHONE_LIKE_MIN_LOCAL_RSSI_DBM = -76.0
LOCALIZATION_PHONE_LIKE_MIN_TOP2_DBM = -80.0

# ---------------- Final display classification layer ----------------
# device_role is an internal heuristic role. display_class is the stricter UI/API
# decision used to decide whether a track should be shown as a real nearby mobile,
# hidden as background, or treated as stationary/debug.
DISPLAY_CLASS_NEARBY_MOBILE = "NEARBY_MOBILE"
DISPLAY_CLASS_MOBILE_CANDIDATE = "MOBILE_CANDIDATE"
DISPLAY_CLASS_MOBILE_WEAK = "MOBILE_WEAK"
DISPLAY_CLASS_MOBILE_BACKGROUND = "MOBILE_BACKGROUND"
DISPLAY_CLASS_MOBILE_MIXED = "MOBILE_MIXED"
DISPLAY_CLASS_STATIONARY_IN_ROOM = "STATIONARY_IN_ROOM"
DISPLAY_CLASS_STATIONARY_OUTSIDE = "STATIONARY_OUTSIDE"
DISPLAY_CLASS_BEACON = "BEACON"
DISPLAY_CLASS_UNKNOWN_CANDIDATE = "UNKNOWN_CANDIDATE"
DISPLAY_CLASS_POLLUTION_SUSPECT = "POLLUTION_SUSPECT"
DISPLAY_CLASS_HIDDEN_BACKGROUND = "HIDDEN_BACKGROUND"
DISPLAY_CLASS_CALIBRATION = "CALIBRATION_DEVICE"

DEVICE_TYPE_MOBILE = "MOBILE"
DEVICE_TYPE_STATIONARY = "STATIONARY"
DEVICE_TYPE_BACKGROUND = "BACKGROUND"
DEVICE_TYPE_BEACON = "BEACON"
DEVICE_TYPE_UNKNOWN = "UNKNOWN"
DEVICE_TYPE_CALIBRATION = "CALIBRATION"

DISPLAY_NEARBY_MOBILE_STRONG_RSSI_DBM = -74.0
DISPLAY_NEARBY_MOBILE_TOP2_AVG_DBM = -78.0
DISPLAY_NEARBY_MOBILE_MIN_MARGIN_DB = 4.0
DISPLAY_NEARBY_MOBILE_CLEAR_MARGIN_DB = 8.0
DISPLAY_NEARBY_MOBILE_MIN_SCANNERS = 3
DISPLAY_NEARBY_MOBILE_MIN_PACKETS = 40
DISPLAY_MOBILE_WEAK_RSSI_DBM = -82.0
DISPLAY_STATIONARY_IN_ROOM_MIN_RSSI_DBM = -82.0

# Classes that should not become PHONE_LIKE / MOBILE_CANDIDATE from RSSI
# variation alone. This is generic class handling, not brand-specific matching:
# these labels describe stationary/fixed infrastructure or laptops in our system.
KNOWN_STATIONARY_PHONE_LIKE_BLOCK_KEYWORDS = (
    "microsoft/laptop",
    "microsoft",
    "windows",
    "laptop",
    "tondo",
    "beacon",
)

# Mobile-service family merge protection. FCF1, FEF3, FD5A-like service data,
# and tiny OTHER_ADV payloads must not be chained into one PD only because an
# averaged RSSI vector is plausible. Merge mobile-service families only when the
# same UUID/family has a tight spatial fingerprint, or when exact MAC+payload
# continuity proves it.
MOBILE_FAMILY_MIN_PACKETS_FOR_MERGE = 6
MOBILE_FAMILY_MAX_REL_RMSE_DB = 2.8
MOBILE_FAMILY_MAX_ABS_RMSE_DB = 6.5
MOBILE_FAMILY_MAX_PER_SCANNER_DIFF_DB = 8.0
MOBILE_FAMILY_REQUIRE_SAME_STRONGEST = True
MOBILE_FAMILY_REQUIRE_TOP2_OVERLAP = True

# Polluted tracks are diagnostic/debug tracks. Once a track is polluted, it must
# stop absorbing other aliases except exact same MAC + same payload family.
POLLUTED_TRACK_REQUIRE_SAME_MAC_AND_PAYLOAD = True

# Continuous phones may not create burst gaps, so motion evidence also uses fixed
# time-sliced RSSI maps. These are only a mobility cue; they do not replace the
# raw RSSI fingerprint or the calibrated block model.
MOTION_SLICE_SEC = 2.0
MOTION_SLICE_MIN_PACKETS = 4
MOTION_SLICE_MIN_SCANNERS = 2
MOTION_SLICE_HISTORY = 8

LOCALIZATION_MOBILE_ROLES = {
    MOBILE_SERVICE_LABEL,
    "PHONE_LIKE",
}

LOCALIZATION_STATIONARY_OR_BACKGROUND_ROLES = {
    "STABLE_DEVICE",
    "OUTSIDE_STABLE",
    "BEACON_LIKE",
    WEAK_FLAT_BACKGROUND_ROLE,
    BACKGROUND_MOBILE_ROLE,
    "WEAK_KNOWN_BACKGROUND",
}

# Grid display policy for localization testing.
# Only these final display classes are allowed to appear inside the 3x3 grid.
# Stationary/background/debug tracks remain available in skipped/debug payloads.
LOCALIZATION_GRID_DISPLAY_CLASSES = {
    DISPLAY_CLASS_NEARBY_MOBILE,
    DISPLAY_CLASS_CALIBRATION,
}

# During calibration/location tuning we want to see where the model *would* place
# an eligible mobile even when the probability distribution is still ambiguous.
# This does not change location_block; it only adds grid_display_block with a
# clear LOW/AMBIGUOUS/test-estimate marker.
LOCALIZATION_GRID_SHOW_BEST_CANDIDATE_FOR_TEST = True

# ---------------- Real-time localization/display tuning ----------------
# Identity/classification needs long memory, but grid localization must not.
# The visual grid now uses only fresh RSSI samples so EldarCalib/mobile movement
# is reflected in seconds instead of being dragged by 20-60 seconds of old data.
LOCALIZATION_REALTIME_WINDOW_SEC = 4.0
LOCALIZATION_MAX_SAMPLE_AGE_SEC = 6.0
LOCALIZATION_ALLOW_LAST_KNOWN_FALLBACK_FOR_GRID = False
LOCALIZATION_DISABLE_HYSTERESIS_FOR_TEST = True
LOCALIZATION_STALE_AFTER_SEC = 6.0
LOCALIZATION_MIN_TOTAL_FRESH_SAMPLES = 2

# For measurement/debug, do not erase a valid grid point the instant the fresh
# 3-scanner window becomes thin. Hold the last valid visual estimate briefly,
# clearly marked as HELD_RECENT, so the operator can record it. This is not the
# old 30-60 s sticky behavior; it is a short UI stabilization hold only.
LOCALIZATION_DISPLAY_HOLD_SEC = 6.0

# The BLE event stream is for live UI/debug. If the GUI cannot consume every
# event fast enough, stale events should be discarded instead of replayed later.
STREAM_REALTIME_DROP_OLD_EVENTS = True
STREAM_MAX_BACKLOG_EVENTS = 250
STREAM_DROP_TO_BACKLOG_EVENTS = 80

localization_lock = threading.Lock()
localization_state: Dict[str, Any] = {
    "loaded": False,
    "enabled": GRID_LOCALIZATION_ENABLED,
    "path": "",
    "message": "not_loaded",
    "fingerprints": {},
}


# ---------------- Diagnostics ----------------

stats_lock = threading.Lock()
stats = {
    "ingest_events": 0,
    "processed_events": 0,
    "streamed_events": 0,
    "dropped_queue_full": 0,
    "dropped_queue_realtime_backlog": 0,
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
    "tracker_background_mobile_tracks": 0,
    "tracker_pollution_suspect_tracks": 0,
    "tracker_weak_flat_background_tracks": 0,
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


def is_stationary_known_class_name(cls: str) -> bool:
    """Return True for known classes that should not become mobile by RSSI motion alone."""
    text = str(cls or "").lower()
    return any(key in text for key in KNOWN_STATIONARY_PHONE_LIKE_BLOCK_KEYWORDS)


def has_stationary_known_class(classes: set) -> bool:
    return any(is_stationary_known_class_name(cls) for cls in (classes or set()))


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


def parse_payload_sig_parts(payload_sigs: set) -> Dict[str, Any]:
    """
    Extract low-level identity clues from compact payload signatures.

    This is intentionally generic. It does not special-case Samsung/Apple/etc.
    It gives the merge layer stable evidence such as:
      - manufacturer IDs
      - service-data UUIDs
      - AD structure families
      - payload CRCs
      - payload lengths
    """
    out = {
        "mfg_ids": set(),
        "service_uuids": set(),
        "crc_set": set(),
        "ad_structures": set(),
        "lengths": set(),
        "has_mfg": False,
        "has_mobile_service": False,
    }

    for raw_sig in payload_sigs or set():
        sig = str(raw_sig or "")

        for part in sig.split(";"):
            part = part.strip()

            if part.startswith("MFG_"):
                mfg_text = part[4:]
                if mfg_text and mfg_text != "NONE":
                    try:
                        out["mfg_ids"].add(int(mfg_text, 16))
                        out["has_mfg"] = True
                    except ValueError:
                        pass

            elif part.startswith("SD["):
                end = part.find("]")
                if end >= 0:
                    inside = part[3:end]
                    for uid_part in inside.replace("|", ",").split(","):
                        uid = normalize_uuid16(uid_part)
                        if uid:
                            out["service_uuids"].add(uid)

            elif part.startswith("AD["):
                end = part.find("]")
                if end >= 0:
                    ad = part[3:end].strip()
                    if ad:
                        out["ad_structures"].add(ad)

            elif part.startswith("LEN_"):
                try:
                    out["lengths"].add(int(part[4:]))
                except ValueError:
                    pass

            elif part.startswith("CRC_"):
                crc = part[4:].strip().upper()
                if crc:
                    out["crc_set"].add(crc)

    out["has_mobile_service"] = bool(out["service_uuids"] & MOBILE_SERVICE_UUIDS)
    return out


def concrete_mfg_ids_from_summary(summary: Dict[str, Any]) -> set:
    return {x for x in summary.get("mfg_ids", set()) if isinstance(x, int)}


def concrete_mfg_ids_from_parsed(parsed: Dict[str, Any]) -> set:
    mfg = parsed.get("mfg_id")
    return {mfg} if isinstance(mfg, int) else set()


def has_manufacturer_conflict(existing_mfg_ids: set, incoming_mfg_ids: set) -> bool:
    """
    Generic hard rule: concrete manufacturer IDs must not mix.

    Unknown/no-MFG advertisements are represented by an empty set and are allowed
    to be considered by the normal evidence gates. A conflict exists only when
    both sides contain concrete manufacturer IDs and the sets are disjoint.
    """
    if not STRICT_MANUFACTURER_ID_CONFLICTS:
        return False
    existing = {x for x in existing_mfg_ids if isinstance(x, int)}
    incoming = {x for x in incoming_mfg_ids if isinstance(x, int)}
    return bool(existing and incoming and existing.isdisjoint(incoming))


def identity_summary_from_track(track: "DeviceTrack") -> Dict[str, Any]:
    sig_parts = parse_payload_sig_parts(track.payload_sigs)
    dominant_class = track.dominant_class()
    known_classes = track.known_classes()
    known_packet_ratio = track.known_packet_ratio()
    mobile_ratio = track.mobile_service_packet_ratio()

    dominant_mfg_id = None
    all_mfg_ids = set(track.mfg_ids) | sig_parts["mfg_ids"]
    if all_mfg_ids:
        dominant_mfg_id = sorted(all_mfg_ids)[0]

    service_uuids = set(track.mobile_service_uuids) | sig_parts["service_uuids"]

    if known_classes and known_packet_ratio >= 0.60 and mobile_ratio >= 0.05:
        mix_state = "MIXED_KNOWN_DOMINANT"
    elif known_classes and known_packet_ratio >= 0.60:
        mix_state = "KNOWN_DOMINANT"
    elif mobile_ratio >= 0.60 and known_packet_ratio >= 0.05:
        mix_state = "MIXED_MOBILE_DOMINANT"
    elif mobile_ratio >= 0.60:
        mix_state = "MOBILE_DOMINANT"
    elif known_classes and mobile_ratio >= 0.05:
        mix_state = "MIXED_KNOWN_DOMINANT"
    elif known_classes:
        mix_state = "KNOWN"
    elif service_uuids & MOBILE_SERVICE_UUIDS:
        mix_state = "MOBILE_SERVICE"
    else:
        mix_state = "UNKNOWN"

    return {
        "dominant_known_class": dominant_class if is_known_metadata_class(dominant_class) else "",
        "known_classes": set(known_classes),
        "dominant_mfg_id": dominant_mfg_id,
        "mfg_ids": all_mfg_ids,
        "service_uuid_set": service_uuids,
        "payload_crc_set": set(sig_parts["crc_set"]),
        "ad_structure_set": set(sig_parts["ad_structures"]),
        "payload_lengths": set(sig_parts["lengths"]),
        "known_packet_ratio": known_packet_ratio,
        "mobile_service_packet_ratio": mobile_ratio,
        "is_known_dominant": bool(known_classes) and known_packet_ratio >= 0.60,
        "is_mobile_service_dominant": mobile_ratio >= 0.60,
        "is_mixed": bool(known_classes) and mobile_ratio >= 0.05,
        "mix_state": mix_state,
    }


def identity_summary_for_api(summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "dominant_known_class": summary.get("dominant_known_class", ""),
        "dominant_mfg_id": (
            f"{summary.get('dominant_mfg_id'):04X}"
            if isinstance(summary.get("dominant_mfg_id"), int) else None
        ),
        "mfg_ids": [f"{x:04X}" for x in sorted(summary.get("mfg_ids", set())) if isinstance(x, int)],
        "service_uuid_set": sorted(list(summary.get("service_uuid_set", set()))),
        "payload_crc_set": sorted(list(summary.get("payload_crc_set", set())))[:10],
        "ad_structure_set": sorted(list(summary.get("ad_structure_set", set())))[:5],
        "payload_lengths": sorted(list(summary.get("payload_lengths", set()))),
        "known_packet_ratio": round(float(summary.get("known_packet_ratio", 0.0)), 3),
        "mobile_service_packet_ratio": round(float(summary.get("mobile_service_packet_ratio", 0.0)), 3),
        "is_known_dominant": bool(summary.get("is_known_dominant", False)),
        "is_mobile_service_dominant": bool(summary.get("is_mobile_service_dominant", False)),
        "is_mixed": bool(summary.get("is_mixed", False)),
        "mix_state": summary.get("mix_state", "UNKNOWN"),
    }


def effective_scanner_rssi(track: "DeviceTrack") -> Dict[str, float]:
    """
    Use live RSSI when present, otherwise use last-known RSSI while the track is
    still inside identity memory. This lets inactive tracks participate in safe
    identity-continuity merges without reviving very old stale tracks.
    """
    live = track.scanner_rssi()
    if live:
        return live
    if track.presence_state() in ("INACTIVE", "INTERMITTENT"):
        return dict(track.last_known_scanner_rssi)
    return {}


def rssi_fingerprint_metrics(a_raw: Dict[str, float], b_raw: Dict[str, float]) -> Dict[str, Any]:
    rel, absr, common_n = rssi_distance_pair(a_raw, b_raw)
    return {
        "rel_rmse": rel,
        "abs_rmse": absr,
        "common_scanners": common_n,
        "max_diff": max_abs_rssi_diff_common(a_raw, b_raw),
        "same_strongest": bool(a_raw and b_raw and strongest_scanner(a_raw) == strongest_scanner(b_raw)),
        "top2_overlap": bool(top_scanners(a_raw, 2) & top_scanners(b_raw, 2)),
        "top2_exact": top_scanners(a_raw, 2) == top_scanners(b_raw, 2) if a_raw and b_raw else False,
    }


def rounded_metric(value: Optional[float]) -> Optional[float]:
    return round(value, 3) if isinstance(value, (int, float)) else None


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

        # Per-payload RSSI fingerprints are used only for diagnostics and
        # split/pollution detection. They do not replace the raw PD/debug view.
        self.payload_rssi_vals: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))

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

        # Fixed-duration motion slices for continuous advertisers. Burst maps
        # only finalize after silence; these slices keep producing RF snapshots
        # while a phone keeps advertising continuously.
        self.motion_slice_start_mono = now_mono
        self.motion_slice_end_mono = now_mono
        self.motion_slice_packets = 0
        self.motion_slice_scanners = set()
        self.motion_slice_rssi_vals: Dict[str, List[int]] = defaultdict(list)
        self.motion_rssi_maps = deque(maxlen=MOTION_SLICE_HISTORY)
        self.motion_slice_packet_counts = deque(maxlen=MOTION_SLICE_HISTORY)

        # Debug/traceability for the identity layer.
        # Raw PDs remain visible, but each hard merge records why it happened.
        self.merge_history = deque(maxlen=20)
        self.association_cache = []

        # Grid localization smoothing state. This is used only for mobile/mobile-
        # candidate devices after role classification.
        self.grid_location_history = deque(maxlen=8)
        self.grid_location_block = None
        self.grid_location_probability = 0.0
        self.grid_location_confidence = "NONE"
        self.grid_location_last_update_mono = 0.0

        # Short visual hold for /api/localization.blocks. This is separate from
        # strict location_block. It prevents a measurement target from blinking
        # off the grid between BLE bursts while still expiring quickly.
        self.grid_display_hold_block = None
        self.grid_display_hold_probability = 0.0
        self.grid_display_hold_confidence = "NONE"
        self.grid_display_hold_reason = ""
        self.grid_display_hold_is_test_estimate = False
        self.grid_display_hold_candidate_rank = None
        self.grid_display_hold_candidates = []
        self.grid_display_hold_updated_mono = 0.0

    def identity_summary(self) -> Dict[str, Any]:
        return identity_summary_from_track(self)

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
        self._update_motion_window(now_mono, scanner, rssi)

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
        self._update_payload_rssi(payload_sig, scanner, rssi)
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

    def _update_payload_rssi(self, payload_sig: str, scanner: str, rssi: int) -> None:
        vals = self.payload_rssi_vals[payload_sig][scanner]
        vals.append(rssi)
        if len(vals) > 120:
            del vals[:-120]

    def payload_rssi_map(self, payload_sig: str) -> Dict[str, float]:
        out = {}
        for scanner, vals in self.payload_rssi_vals.get(payload_sig, {}).items():
            if not vals:
                continue
            vals_sorted = sorted(vals, reverse=True)
            keep_n = max(1, len(vals_sorted) // 2)
            out[scanner] = sum(vals_sorted[:keep_n]) / keep_n
        return out

    def payload_packet_count(self, payload_sig: str) -> int:
        return sum(len(vals) for vals in self.payload_rssi_vals.get(payload_sig, {}).values())

    def payload_family_rssi_maps(self) -> Dict[str, Dict[str, Any]]:
        out = {}
        for sig in self.payload_sigs:
            rssi_map = self.payload_rssi_map(sig)
            if not rssi_map:
                continue
            out[sig] = {
                "rssi": rssi_map,
                "packets": self.payload_packet_count(sig),
                "parts": parse_payload_sig_parts({sig}),
            }
        return out

    def polluted_family_debug(self, max_families: int = 10) -> Dict[str, Any]:
        """
        Compact debug view for split/polluted tracks.

        It exposes each payload family's own RSSI vector so the GUI/log can show
        whether a phone-like service-data family was swallowed into a known or
        beacon-like physical-device bucket.
        """
        family_maps = self.payload_family_rssi_maps()
        families = []

        for sig, info in sorted(
            family_maps.items(),
            key=lambda kv: kv[1].get("packets", 0),
            reverse=True,
        )[:max_families]:
            parts = info.get("parts", {}) or {}
            rssi_map = {str(k): round(float(v), 3) for k, v in (info.get("rssi") or {}).items()}
            service_uuids = sorted(list(parts.get("service_uuids", set())))
            mobile_uuids = sorted(list(set(service_uuids) & MOBILE_SERVICE_UUIDS))
            mfg_ids = sorted([f"{x:04X}" for x in parts.get("mfg_ids", set()) if isinstance(x, int)])
            crc_set = sorted(list(parts.get("crc_set", set())))
            ad_structures = sorted(list(parts.get("ad_structures", set())))
            payload_lengths = sorted(list(parts.get("lengths", set())))

            if mobile_uuids:
                family_type = "MOBILE_SERVICE"
            elif mfg_ids:
                family_type = "MANUFACTURER_DATA"
            elif service_uuids:
                family_type = "SERVICE_DATA"
            else:
                family_type = "OTHER_ADV"

            families.append({
                "payload_sig": sig,
                "family_type": family_type,
                "packets": int(info.get("packets", 0)),
                "mfg_ids": mfg_ids,
                "service_uuids": service_uuids,
                "mobile_service_uuids": mobile_uuids,
                "payload_crc": crc_set[0] if crc_set else "",
                "ad_structures": ad_structures[:4],
                "payload_lengths": payload_lengths,
                "rssi": rssi_map,
                "strongest_scanner": strongest_scanner(rssi_map),
                "top2_scanners": sorted(list(top_scanners(rssi_map, 2))),
            })

        divergent_pairs = []
        significant = [f for f in families if int(f.get("packets", 0)) >= POLLUTION_MIN_PAYLOAD_PACKETS]
        for i in range(len(significant)):
            for j in range(i + 1, len(significant)):
                a = significant[i]
                b = significant[j]
                metrics = rssi_fingerprint_metrics(a.get("rssi", {}), b.get("rssi", {}))
                rel = metrics.get("rel_rmse")
                absr = metrics.get("abs_rmse")
                max_diff = metrics.get("max_diff")
                if metrics.get("common_scanners", 0) < POLLUTION_MIN_COMMON_SCANNERS:
                    continue
                if rel is None or absr is None or max_diff is None:
                    continue
                if rel >= POLLUTION_REL_RMSE_DB or absr >= POLLUTION_ABS_RMSE_DB or max_diff >= POLLUTION_MAX_SCANNER_DIFF_DB:
                    divergent_pairs.append({
                        "a_crc": a.get("payload_crc", ""),
                        "a_type": a.get("family_type", ""),
                        "b_crc": b.get("payload_crc", ""),
                        "b_type": b.get("family_type", ""),
                        "rel_rmse": rounded_metric(rel),
                        "abs_rmse": rounded_metric(absr),
                        "max_diff": rounded_metric(max_diff),
                        "common_scanners": metrics.get("common_scanners", 0),
                    })

        return {
            "families": families,
            "divergent_pairs": divergent_pairs[:10],
        }

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

        if WEAK_FLAT_BLOCK_CONFIRMATION and self.is_weak_flat_background():
            return False, "weak_flat_background"

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

    def _finalize_current_motion_window(self) -> None:
        if self.motion_slice_packets < MOTION_SLICE_MIN_PACKETS:
            return

        rssi_map = {}
        for scanner, vals in self.motion_slice_rssi_vals.items():
            if vals:
                vals_sorted = sorted(vals, reverse=True)
                keep_n = max(1, len(vals_sorted) // 2)
                rssi_map[str(scanner)] = sum(vals_sorted[:keep_n]) / keep_n

        if len(rssi_map) >= MOTION_SLICE_MIN_SCANNERS:
            self.motion_rssi_maps.append(rssi_map)
            self.motion_slice_packet_counts.append(self.motion_slice_packets)

    def _current_motion_rssi_map(self) -> Dict[str, float]:
        if self.motion_slice_packets < MOTION_SLICE_MIN_PACKETS:
            return {}

        rssi_map = {}
        for scanner, vals in self.motion_slice_rssi_vals.items():
            if vals:
                vals_sorted = sorted(vals, reverse=True)
                keep_n = max(1, len(vals_sorted) // 2)
                rssi_map[str(scanner)] = sum(vals_sorted[:keep_n]) / keep_n
        return rssi_map if len(rssi_map) >= MOTION_SLICE_MIN_SCANNERS else {}

    def _update_motion_window(self, now_mono: float, scanner: str, rssi: int) -> None:
        if self.motion_slice_packets == 0:
            self.motion_slice_start_mono = now_mono
            self.motion_slice_end_mono = now_mono
            self.motion_slice_scanners = set()
            self.motion_slice_rssi_vals = defaultdict(list)

        if self.motion_slice_packets > 0 and (now_mono - self.motion_slice_start_mono) >= MOTION_SLICE_SEC:
            self._finalize_current_motion_window()
            self.motion_slice_start_mono = now_mono
            self.motion_slice_end_mono = now_mono
            self.motion_slice_packets = 0
            self.motion_slice_scanners = set()
            self.motion_slice_rssi_vals = defaultdict(list)

        self.motion_slice_end_mono = now_mono
        self.motion_slice_packets += 1
        self.motion_slice_scanners.add(str(scanner))
        self.motion_slice_rssi_vals[str(scanner)].append(int(rssi))

    def motion_slice_count(self) -> int:
        count = len(self.motion_rssi_maps)
        if self._current_motion_rssi_map():
            count += 1
        return count

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

    def strongest_margin_db(self) -> Optional[float]:
        rssi_map = self.scanner_rssi()
        if not rssi_map:
            rssi_map = dict(self.last_known_scanner_rssi)
        if len(rssi_map) < 2:
            return None
        vals = sorted(rssi_map.values(), reverse=True)
        return vals[0] - vals[1]

    def location_confidence(self) -> Tuple[str, str]:
        rssi_map = self.scanner_rssi()
        source = "live"
        if not rssi_map:
            rssi_map = dict(self.last_known_scanner_rssi)
            source = "last_known"
        if not rssi_map:
            return "NONE", "no_rssi"

        strongest = max(rssi_map.values())
        margin = self.strongest_margin_db()
        scanners = len(rssi_map)

        if scanners < 2:
            return "LOW", f"{source}:single_scanner"

        if strongest <= LOCATION_WEAK_RSSI_DBM and (margin is None or margin < LOCATION_HIGH_MARGIN_DB):
            return "LOW", f"{source}:weak_rssi_or_small_margin"

        if margin is None:
            return "LOW", f"{source}:no_margin"

        if strongest >= LOCATION_STRONG_RSSI_DBM and margin >= LOCATION_HIGH_MARGIN_DB:
            return "HIGH", f"{source}:strong_rssi_and_clear_margin"

        if margin >= LOCATION_MEDIUM_MARGIN_DB:
            return "MEDIUM", f"{source}:moderate_margin"

        return "LOW", f"{source}:ambiguous_margin"

    def weak_flat_background_score(self) -> float:
        """Generic weak+flat outside/background detector.

        A real in-room phone may be weak on some scanners, especially in the
        center of the room. We suppress only tracks that are weak across most
        scanners and have no clear local RSSI peak.
        """
        rssi_map = self.scanner_rssi()
        if not rssi_map:
            rssi_map = dict(self.last_known_scanner_rssi)
        if len(rssi_map) < WEAK_FLAT_MIN_SCANNERS:
            return 0.0

        vals = sorted(rssi_map.values(), reverse=True)
        strongest = vals[0]
        top2_avg = sum(vals[:2]) / min(2, len(vals))
        margin = vals[0] - vals[1] if len(vals) >= 2 else None
        weak_scanners = sum(1 for v in vals if v <= WEAK_FLAT_SCANNER_RSSI_DBM)

        score = 0.0
        if strongest <= WEAK_FLAT_MAX_STRONGEST_RSSI_DBM:
            score += 2.0
        if top2_avg <= WEAK_FLAT_MAX_TOP2_AVG_RSSI_DBM:
            score += 1.5
        if margin is not None and margin <= WEAK_FLAT_MAX_MARGIN_DB:
            score += 2.0
        if weak_scanners >= WEAK_FLAT_MIN_WEAK_SCANNERS:
            score += 2.0
        if len(rssi_map) >= 4 and weak_scanners >= 3:
            score += 0.5

        # Preserve real local peaks. Example: a phone near one scanner can have
        # one strong value and several weak values; that is not weak-flat noise.
        if margin is not None and margin >= LOCATION_HIGH_MARGIN_DB:
            score -= 3.0
        if strongest >= LOCATION_STRONG_RSSI_DBM:
            score -= 3.0

        return max(0.0, score)

    def is_weak_flat_background(self) -> bool:
        return self.weak_flat_background_score() >= 5.0

    def weak_flat_background_reason(self) -> str:
        rssi_map = self.scanner_rssi()
        source = "live"
        if not rssi_map:
            rssi_map = dict(self.last_known_scanner_rssi)
            source = "last_known"
        if not rssi_map:
            return "no_rssi"
        vals = sorted(rssi_map.values(), reverse=True)
        strongest = vals[0]
        top2_avg = sum(vals[:2]) / min(2, len(vals))
        margin = vals[0] - vals[1] if len(vals) >= 2 else None
        weak_scanners = sum(1 for v in vals if v <= WEAK_FLAT_SCANNER_RSSI_DBM)
        parts = [
            source,
            f"scanners={len(rssi_map)}",
            f"weak_scanners={weak_scanners}",
            f"strongest={strongest:.2f}",
            f"top2_avg={top2_avg:.2f}",
        ]
        if margin is not None:
            parts.append(f"margin={margin:.2f}")
        return ",".join(parts)

    def background_mobile_service_score(self) -> float:
        if not self.has_mobile_service_data():
            return 0.0

        summary = self.identity_summary()
        mobile_ratio = self.mobile_service_packet_ratio()
        unknown_ratio = self.unknown_packet_count() / max(1, self.packet_count)
        known_ratio = self.known_packet_ratio()
        strongest = self.strongest_rssi_value()
        top2 = self.top2_avg_rssi_value()
        if strongest is None:
            strongest = self.last_known_strongest_rssi_value()
        if top2 is None:
            top2 = self.last_known_top2_avg_rssi_value()

        margin = self.strongest_margin_db()
        score = 0.0

        if mobile_ratio >= BACKGROUND_MOBILE_MIN_MOBILE_RATIO:
            score += 2.0
        if unknown_ratio >= BACKGROUND_MOBILE_MIN_UNKNOWN_RATIO:
            score += 1.5
        if self.packet_count >= BACKGROUND_MOBILE_MIN_PACKETS:
            score += 1.0
        if self.age_sec() >= BACKGROUND_MOBILE_MIN_AGE_SEC:
            score += 1.0
        if strongest is not None and strongest <= BACKGROUND_MOBILE_MAX_STRONGEST_RSSI_DBM:
            score += 1.5
        if top2 is not None and top2 <= BACKGROUND_MOBILE_MAX_TOP2_AVG_RSSI_DBM:
            score += 1.0
        if margin is not None and margin <= BACKGROUND_MOBILE_MAX_LOCATION_MARGIN_DB:
            score += 1.0
        if self.is_weak_flat_background():
            score += 1.5
        if summary.get("mix_state") == "MIXED_MOBILE_DOMINANT" and known_ratio <= BACKGROUND_MOBILE_UNKNOWN_HEAVY_KNOWN_RATIO_MAX:
            score += 1.0
        if self.pollution_suspect()[0]:
            score += 1.0

        # Strong close-by mobile-service identities, like the verified phone at
        # scanner 1 around -64 dBm, must not be demoted to background.
        if strongest is not None and strongest >= MOBILE_SERVICE_STRONG_RSSI_DBM:
            score -= 3.0
        if top2 is not None and top2 >= MOBILE_SERVICE_TOP2_AVG_DBM:
            score -= 2.0

        return max(0.0, score)

    def is_background_mobile_service(self) -> bool:
        return self.background_mobile_service_score() >= 5.0

    def background_mobile_reason(self) -> str:
        if not self.has_mobile_service_data():
            return "no_mobile_service_uuid"
        parts = []
        if self.mobile_service_packet_ratio() >= BACKGROUND_MOBILE_MIN_MOBILE_RATIO:
            parts.append("mobile_dominant")
        if (self.unknown_packet_count() / max(1, self.packet_count)) >= BACKGROUND_MOBILE_MIN_UNKNOWN_RATIO:
            parts.append("unknown_heavy")
        if self.age_sec() >= BACKGROUND_MOBILE_MIN_AGE_SEC:
            parts.append("persistent")
        strongest = self.strongest_rssi_value()
        if strongest is None:
            strongest = self.last_known_strongest_rssi_value()
        if strongest is not None and strongest <= BACKGROUND_MOBILE_MAX_STRONGEST_RSSI_DBM:
            parts.append("weak_strongest_rssi")
        margin = self.strongest_margin_db()
        if margin is not None and margin <= BACKGROUND_MOBILE_MAX_LOCATION_MARGIN_DB:
            parts.append("low_location_margin")
        if self.is_weak_flat_background():
            parts.append("weak_flat_background")
        polluted, reason = self.pollution_suspect()
        if polluted:
            parts.append("pollution_suspect:" + reason)
        return ",".join(parts) if parts else "weak_background_evidence"

    def pollution_suspect(self) -> Tuple[bool, str]:
        """
        Generic split/pollution detector.

        It is not manufacturer-specific. It flags a physical cluster when its
        payload families have clearly different RSSI vectors, which usually means
        the PD absorbed more than one RF source/personality. It checks both:
          1) mobile-service vs known/manufacturer payload families, and
          2) same-manufacturer/multiple-payload families that diverge spatially.
        """
        family_maps = self.payload_family_rssi_maps()
        if len(family_maps) < 2:
            return False, ""

        significant_items = []
        mobile_items = []
        known_items = []

        for sig, info in family_maps.items():
            if info.get("packets", 0) < POLLUTION_MIN_PAYLOAD_PACKETS:
                continue
            parts = info.get("parts", {})
            is_mobile = bool(parts.get("service_uuids", set()) & MOBILE_SERVICE_UUIDS)
            is_mfg_or_known = bool(parts.get("mfg_ids", set())) or not is_mobile
            significant_items.append((sig, info))
            if is_mobile:
                mobile_items.append((sig, info))
            if is_mfg_or_known:
                known_items.append((sig, info))

        def diverges(sig_a: str, info_a: Dict[str, Any], sig_b: str, info_b: Dict[str, Any]) -> Optional[str]:
            metrics = rssi_fingerprint_metrics(info_a["rssi"], info_b["rssi"])
            if metrics["common_scanners"] < POLLUTION_MIN_COMMON_SCANNERS:
                return None
            rel = metrics.get("rel_rmse")
            absr = metrics.get("abs_rmse")
            max_diff = metrics.get("max_diff")
            if rel is None or absr is None or max_diff is None:
                return None
            if rel >= POLLUTION_REL_RMSE_DB or absr >= POLLUTION_ABS_RMSE_DB or max_diff >= POLLUTION_MAX_SCANNER_DIFF_DB:
                return (
                    f"payload_vectors_diverge:{sig_a[-12:]}_vs_{sig_b[-12:]},"
                    f"rel={rel:.2f},abs={absr:.2f},max={max_diff:.2f}"
                )
            return None

        # First, the known/mobile-service pollution pattern.
        for msig, minfo in mobile_items:
            for ksig, kinfo in known_items:
                if msig == ksig:
                    continue
                reason = diverges(msig, minfo, ksig, kinfo)
                if reason:
                    return True, reason

        # Second, a generic same-track split pattern: any two sufficiently common
        # payload families with incompatible RF fingerprints. This catches cases
        # where all payloads share the same manufacturer ID but clearly do not
        # behave like one spatial source.
        for i in range(len(significant_items)):
            sig_a, info_a = significant_items[i]
            for sig_b, info_b in significant_items[i + 1:]:
                reason = diverges(sig_a, info_a, sig_b, info_b)
                if reason:
                    return True, reason

        return False, ""

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

        if self.is_weak_flat_background():
            return 0.0

        # Be conservative with beacons/outside weak sources.
        cls = self.dominant_class().lower()
        if "tondo" in cls or "beacon" in cls:
            return 0.0
        if self.is_outside_stable_source():
            return 0.0

        # Keep raw mobile-service evidence visible, but do not score persistent
        # weak/background FCF1/FEF3 families as strong phone-like mobile sources.
        # background_mobile_service_score() does not call mobile_service_score(),
        # so this is safe from recursion.
        if self.background_mobile_service_score() >= 5.0:
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
        if self.is_weak_flat_background():
            return 0.0

        # Weak low-confidence tracks are background/ambiguous, not phone-like.
        strongest = self.strongest_rssi_value()
        margin = self.strongest_margin_db()
        if strongest is not None and strongest <= PHONE_LIKE_BLOCK_WEAK_RSSI_DBM:
            if margin is None or margin < PHONE_LIKE_BLOCK_LOW_MARGIN_DB:
                return 0.0

        cls_set = self.known_classes()
        cls_text = " ".join(sorted(cls_set)).lower()

        # V3: known stationary/fixed classes must never become PHONE_LIKE only
        # because their RSSI vector changed indoors. This specifically prevents
        # Microsoft/Laptop tracks from showing as MOBILE_CANDIDATE. Mobile-service
        # evidence is handled above by the mobile-service classifier, not here.
        if has_stationary_known_class(cls_set) and not self.has_mobile_service_data():
            return 0.0

        score = 0.0

        if any(x in cls_text for x in ("apple", "samsung", "google")):
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
        if self.is_weak_flat_background():
            return WEAK_FLAT_BACKGROUND_ROLE

        # Do not let a small amount of service-data contamination override a
        # strongly known/manufacturer-dominant track. Keep background/pollution
        # information as diagnostic fields instead.
        known_dominant = self.known_packet_ratio() >= KNOWN_DOMINANT_BACKGROUND_OVERRIDE_RATIO
        if self.is_background_mobile_service() and not known_dominant:
            return BACKGROUND_MOBILE_ROLE

        if self.is_mobile_service_data_like():
            return MOBILE_SERVICE_LABEL
        if self.is_stable_device():
            return "STABLE_DEVICE"
        if self.is_phone_like():
            return "PHONE_LIKE"
        if self.is_background_mobile_service():
            return "WEAK_KNOWN_BACKGROUND" if self.known_classes() else BACKGROUND_MOBILE_ROLE
        return "UNKNOWN"

    def movement_state(self) -> Tuple[str, str]:
        """
        Human-facing movement state.

        RF vectors often change indoors even when the device itself is fixed
        because of multipath, body shadowing, and scanner timing. Therefore,
        raw RF motion evidence is not allowed to make stationary/background/
        beacon/polluted tracks appear as MOVING. The raw evidence is still
        exposed separately as rf_motion_evidence / rf_motion_reason.
        """
        presence = self.presence_state()
        if presence == "STALE":
            return "STALE", "identity_memory_expired"
        if presence in ("INACTIVE", "INTERMITTENT"):
            return presence, f"last_seen_age={time.monotonic() - self.last_seen_mono:.2f}s"

        rf_moving, rf_reason = _track_motion_evidence(self)
        cls = self.dominant_class().lower()

        if "tondo" in cls or "beacon" in cls:
            return "NOT_APPLICABLE", f"beacon_or_tondo_role;rf={rf_reason}"

        polluted, pollution_reason = self.pollution_suspect()
        if polluted:
            return "NOT_APPLICABLE", f"pollution_suspect:{pollution_reason};rf={rf_reason}"

        if self.is_weak_flat_background():
            return "BACKGROUND", f"weak_flat_background;rf={rf_reason}"

        if self.is_background_mobile_service():
            return "BACKGROUND", f"background_mobile_service;rf={rf_reason}"

        if self.is_outside_stable_source():
            return "STATIONARY", f"outside_stable;rf={rf_reason}"

        if self.is_stable_device():
            return "STATIONARY", f"stable_device;rf={rf_reason}"

        role = self.device_role()
        if role in (MOBILE_SERVICE_LABEL, "PHONE_LIKE"):
            if rf_moving:
                return "MOVING", rf_reason
            return "STILL_OR_UNKNOWN", rf_reason

        # Keep this distinct from MOVING so the GUI does not show fixed or
        # unknown tracks as physically moving just because RSSI changed.
        if rf_moving:
            return "RF_VARIATION", rf_reason

        return "STILL_OR_UNKNOWN", rf_reason

    def display_classification(self) -> Dict[str, Any]:
        """
        Final UI/API classification layer.

        device_role is intentionally broad. This layer answers the practical GUI
        question: should this track be shown as a real nearby mobile, treated as
        stationary, or hidden/debugged as background/weak/mixed?
        """
        role = self.device_role()
        label = self.label()
        dominant_class = self.dominant_class()
        presence = self.presence_state()
        confirmed, confirm_reason = self.confirm_quality()
        loc_conf, loc_reason = self.location_confidence()
        rf_moving, rf_reason = _track_motion_evidence(self)
        moving_state, moving_reason = self.movement_state()
        polluted, pollution_reason = self.pollution_suspect()

        rssi_map = self.scanner_rssi() or dict(self.last_known_scanner_rssi)
        strongest = max(rssi_map.values()) if rssi_map else None
        vals = sorted(rssi_map.values(), reverse=True) if rssi_map else []
        top2_avg = sum(vals[:2]) / min(2, len(vals)) if vals else None
        margin = self.strongest_margin_db()
        scanners = len(rssi_map)

        strong_local = (
            (strongest is not None and strongest >= DISPLAY_NEARBY_MOBILE_STRONG_RSSI_DBM) or
            (top2_avg is not None and top2_avg >= DISPLAY_NEARBY_MOBILE_TOP2_AVG_DBM)
        )
        clear_margin = margin is not None and margin >= DISPLAY_NEARBY_MOBILE_MIN_MARGIN_DB
        very_clear_margin = margin is not None and margin >= DISPLAY_NEARBY_MOBILE_CLEAR_MARGIN_DB
        enough_evidence = self.packet_count >= DISPLAY_NEARBY_MOBILE_MIN_PACKETS
        enough_scanners = scanners >= DISPLAY_NEARBY_MOBILE_MIN_SCANNERS
        active = presence == "ACTIVE"
        known_text = " ".join(sorted(self.known_classes())).lower()

        def result(display_class: str, device_type: str, confidence: str, reason: str,
                   grid_ok: bool = False) -> Dict[str, Any]:
            return {
                "display_class": display_class,
                "device_type": device_type,
                "movement_state": moving_state,
                "rf_motion_evidence": bool(rf_moving),
                "rf_motion_reason": rf_reason,
                "classification_confidence": confidence,
                "classification_reason": reason,
                "grid_display_eligible": bool(grid_ok),
            }

        if dominant_class == "Calibration" or "eldarcalib" in str(label).lower():
            return result(
                DISPLAY_CLASS_CALIBRATION,
                DEVICE_TYPE_CALIBRATION,
                "HIGH" if active else "MEDIUM",
                "explicit_calibration_advertisement",
                grid_ok=True,
            )

        if polluted:
            return result(
                DISPLAY_CLASS_POLLUTION_SUSPECT,
                DEVICE_TYPE_UNKNOWN,
                "LOW",
                f"pollution_suspect:{pollution_reason}",
                grid_ok=False,
            )

        if role == "BEACON_LIKE":
            return result(DISPLAY_CLASS_BEACON, DEVICE_TYPE_BEACON, "HIGH", "beacon_or_tondo_role", False)

        if role == "OUTSIDE_STABLE":
            return result(
                DISPLAY_CLASS_STATIONARY_OUTSIDE,
                DEVICE_TYPE_BACKGROUND,
                "HIGH" if confirmed else "MEDIUM",
                f"outside_stable:{confirm_reason}",
                False,
            )

        if role == WEAK_FLAT_BACKGROUND_ROLE:
            return result(
                DISPLAY_CLASS_HIDDEN_BACKGROUND,
                DEVICE_TYPE_BACKGROUND,
                "HIGH",
                f"weak_flat_background:{self.weak_flat_background_reason()}",
                False,
            )

        if role == BACKGROUND_MOBILE_ROLE or self.is_background_mobile_service():
            return result(
                DISPLAY_CLASS_MOBILE_BACKGROUND,
                DEVICE_TYPE_BACKGROUND,
                "HIGH" if role == BACKGROUND_MOBILE_ROLE else "MEDIUM",
                f"background_mobile_service:{self.background_mobile_reason()}",
                False,
            )

        if role in ("STABLE_DEVICE", "WEAK_KNOWN_BACKGROUND"):
            if strongest is not None and strongest >= DISPLAY_STATIONARY_IN_ROOM_MIN_RSSI_DBM and scanners >= 2:
                return result(
                    DISPLAY_CLASS_STATIONARY_IN_ROOM,
                    DEVICE_TYPE_STATIONARY,
                    "HIGH" if confirmed else "MEDIUM",
                    f"stable_known_or_fixed:{role};location={loc_conf}:{loc_reason}",
                    False,
                )
            return result(
                DISPLAY_CLASS_STATIONARY_OUTSIDE,
                DEVICE_TYPE_BACKGROUND,
                "MEDIUM",
                f"weak_stationary_or_known_background:{role};location={loc_conf}:{loc_reason}",
                False,
            )

        if role == MOBILE_SERVICE_LABEL:
            if not active:
                return result(
                    DISPLAY_CLASS_MOBILE_CANDIDATE,
                    DEVICE_TYPE_MOBILE,
                    "MEDIUM",
                    f"mobile_service_not_active:{presence}",
                    False,
                )

            if enough_evidence and enough_scanners and strong_local and (clear_margin or loc_conf in ("MEDIUM", "HIGH")):
                conf = "HIGH" if self.mobile_service_score() >= MOBILE_SERVICE_HIGH_SCORE_THRESHOLD and (very_clear_margin or loc_conf == "HIGH") else "MEDIUM"
                return result(
                    DISPLAY_CLASS_NEARBY_MOBILE,
                    DEVICE_TYPE_MOBILE,
                    conf,
                    f"nearby_mobile_service:score={self.mobile_service_score():.2f};location={loc_conf}:{loc_reason}",
                    True,
                )

            if self.mobile_service_score() >= MOBILE_SERVICE_SCORE_THRESHOLD and moving_state == "MOVING" and enough_scanners:
                return result(
                    DISPLAY_CLASS_NEARBY_MOBILE,
                    DEVICE_TYPE_MOBILE,
                    "MEDIUM",
                    f"mobile_service_motion:{moving_reason}",
                    True,
                )

            if strongest is not None and strongest <= DISPLAY_MOBILE_WEAK_RSSI_DBM:
                return result(
                    DISPLAY_CLASS_MOBILE_WEAK,
                    DEVICE_TYPE_MOBILE,
                    "LOW",
                    f"mobile_service_weak:strongest={strongest:.2f};margin={margin}",
                    False,
                )

            return result(
                DISPLAY_CLASS_MOBILE_CANDIDATE,
                DEVICE_TYPE_MOBILE,
                "LOW",
                f"mobile_service_candidate:score={self.mobile_service_score():.2f};location={loc_conf}:{loc_reason}",
                False,
            )

        if role == "PHONE_LIKE":
            if moving_state == "MOVING" and active and enough_scanners and (
                (strongest is not None and strongest >= LOCALIZATION_PHONE_LIKE_MIN_LOCAL_RSSI_DBM) or
                (top2_avg is not None and top2_avg >= LOCALIZATION_PHONE_LIKE_MIN_TOP2_DBM)
            ):
                return result(
                    DISPLAY_CLASS_NEARBY_MOBILE,
                    DEVICE_TYPE_MOBILE,
                    "MEDIUM",
                    f"phone_like_with_motion:{moving_reason}",
                    True,
                )

            return result(
                DISPLAY_CLASS_MOBILE_CANDIDATE,
                DEVICE_TYPE_MOBILE,
                "LOW",
                f"phone_like_without_enough_motion_or_local_rssi:{moving_reason}",
                False,
            )

        if role == "UNKNOWN":
            if moving_state == "MOVING" and enough_scanners and self.phone_likelihood_score() >= max(2.5, PHONE_LIKE_SCORE_THRESHOLD - 1.0):
                return result(
                    DISPLAY_CLASS_NEARBY_MOBILE,
                    DEVICE_TYPE_MOBILE,
                    "LOW",
                    f"unknown_mobile_motion:{moving_reason}",
                    True,
                )

            if any(x in known_text for x in ("microsoft/laptop", "tondo", "beacon")):
                return result(DISPLAY_CLASS_STATIONARY_IN_ROOM, DEVICE_TYPE_STATIONARY, "LOW", f"known_stationary_text:{known_text}", False)

            return result(
                DISPLAY_CLASS_UNKNOWN_CANDIDATE,
                DEVICE_TYPE_UNKNOWN,
                "LOW",
                f"unknown_candidate:{confirm_reason};location={loc_conf}:{loc_reason}",
                False,
            )

        return result(
            DISPLAY_CLASS_UNKNOWN_CANDIDATE,
            DEVICE_TYPE_UNKNOWN,
            "LOW",
            f"unhandled_role:{role};{confirm_reason}",
            False,
        )

    def is_localizable(self) -> bool:
        return len(self.scanner_visibility()) >= CONFIRM_MIN_SCANNERS

    def label(self) -> str:
        summary = self.identity_summary()
        uuids = sorted(self.mobile_service_uuids or mobile_service_uuids_from_sigs(self.payload_sigs))

        if summary.get("mix_state") == "MIXED_MOBILE_DOMINANT":
            known = sorted(list(summary.get("known_classes", set())))
            known_part = ",".join(known) if known else "KNOWN"
            uuid_part = ",".join(uuids) if uuids else "SERVICE"
            return f"MIXED_MOBILE_DOMINANT({uuid_part}+{known_part})"

        if summary.get("mix_state") == "MIXED_KNOWN_DOMINANT":
            known = sorted(list(summary.get("known_classes", set())))
            known_part = ",".join(known) if known else self.dominant_class()
            uuid_part = ",".join(uuids) if uuids else "SERVICE"
            return f"MIXED_KNOWN_DOMINANT({known_part}+{uuid_part})"

        if self.is_mobile_service_data_like():
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


# ---------------- Grid localization model ----------------

def load_localization_fingerprints(filename: str = LOCALIZATION_FINGERPRINTS_JSON) -> None:
    """
    Load calibration_fingerprints.json generated by build_calibration_fingerprints.py.
    Safe to call at startup and again after recalibration.
    """
    path = os.path.join(BASE_DIR, filename)

    with localization_lock:
        localization_state["path"] = path

        if not GRID_LOCALIZATION_ENABLED:
            localization_state.update({
                "loaded": False,
                "enabled": False,
                "message": "disabled_by_config",
                "fingerprints": {},
            })
            print("[LOCALIZATION] Grid localization disabled by config.")
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            blocks = payload.get("blocks", {})
            if not isinstance(blocks, dict) or not blocks:
                raise ValueError("fingerprint file has no blocks")

            localization_state.update({
                "loaded": True,
                "enabled": True,
                "message": f"loaded {len(blocks)} blocks",
                "fingerprints": payload,
            })
            print(f"[LOCALIZATION] Loaded grid fingerprints from {path} ({len(blocks)} blocks).")

        except Exception as e:
            localization_state.update({
                "loaded": False,
                "enabled": True,
                "message": f"load_failed: {e}",
                "fingerprints": {},
            })
            print(f"[LOCALIZATION] Could not load {path}: {e}")


def localization_status_for_api() -> Dict[str, Any]:
    with localization_lock:
        fp = localization_state.get("fingerprints", {}) or {}
        return {
            "enabled": bool(localization_state.get("enabled", False)),
            "loaded": bool(localization_state.get("loaded", False)),
            "path": localization_state.get("path", ""),
            "message": localization_state.get("message", ""),
            "model": fp.get("model", ""),
            "schema_version": fp.get("schema_version"),
            "grid_layout": fp.get("grid_layout", [[1, 2, 3], [6, 5, 4], [7, 8, 9]]),
            "blocks_loaded": sorted(list((fp.get("blocks", {}) or {}).keys()), key=lambda x: int(x) if str(x).isdigit() else str(x)),
            "weights": {
                "relative": LOCALIZATION_RELATIVE_WEIGHT,
                "absolute": LOCALIZATION_ABSOLUTE_WEIGHT,
                "std": LOCALIZATION_STD_WEIGHT,
                "shape": LOCALIZATION_SHAPE_WEIGHT,
            },
            "assignment_thresholds": {
                "min_probability": LOCALIZATION_ASSIGN_MIN_PROBABILITY,
                "min_margin": LOCALIZATION_ASSIGN_MIN_MARGIN,
                "medium_probability": LOCALIZATION_MEDIUM_PROBABILITY,
                "medium_margin": LOCALIZATION_MEDIUM_MARGIN,
            },
            "real_time": {
                "window_sec": LOCALIZATION_REALTIME_WINDOW_SEC,
                "max_sample_age_sec": LOCALIZATION_MAX_SAMPLE_AGE_SEC,
                "stale_after_sec": LOCALIZATION_STALE_AFTER_SEC,
                "last_known_fallback_for_grid": LOCALIZATION_ALLOW_LAST_KNOWN_FALLBACK_FOR_GRID,
                "hysteresis_disabled_for_test": LOCALIZATION_DISABLE_HYSTERESIS_FOR_TEST,
                "min_total_fresh_samples": LOCALIZATION_MIN_TOTAL_FRESH_SAMPLES,
                "display_hold_sec": LOCALIZATION_DISPLAY_HOLD_SEC,
            },
            "min_scanners": LOCALIZATION_MIN_SCANNERS,
            "tx_power_used": False,
        }


def _safe_float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _top_half_mean(samples: List[int]) -> Optional[float]:
    if not samples:
        return None
    ordered = sorted([safe_int(v, 0) for v in samples], reverse=True)
    keep_n = max(1, len(ordered) // 2)
    return sum(ordered[:keep_n]) / keep_n


def _sample_std(samples: List[int]) -> Optional[float]:
    vals = [safe_int(v, 0) for v in samples]
    if not vals:
        return None
    if len(vals) == 1:
        return 0.0
    mean = sum(vals) / len(vals)
    return math.sqrt(sum((v - mean) ** 2 for v in vals) / (len(vals) - 1))


def scanner_stats_for_localization(track: "DeviceTrack") -> Dict[str, Any]:
    """
    Return fresh per-scanner RSSI statistics for real-time grid localization.

    Important design split:
      - Device identity/classification may use long rolling memory.
      - Grid placement must use only recent observations.

    This prevents the GUI from showing a block based on RSSI samples that are
    tens of seconds old after EldarCalib/mobile moved to a different block.
    """
    now_mono = time.monotonic()
    track._prune_obs(now_mono)

    values: Dict[str, List[int]] = defaultdict(list)
    sample_ages: List[float] = []
    per_scanner_age: Dict[str, Dict[str, float]] = {}
    source = "realtime_live"

    # Use only samples that are both inside the live window and below the hard
    # max-age limit. The hard limit protects against future tuning that might
    # make the window larger than the acceptable latency.
    max_age = min(float(LOCALIZATION_REALTIME_WINDOW_SEC), float(LOCALIZATION_MAX_SAMPLE_AGE_SEC))
    for mono_ts, scanner, _, rssi in track.obs:
        age = now_mono - float(mono_ts)
        if age < 0.0 or age > max_age:
            continue
        scanner_id = str(scanner)
        values[scanner_id].append(safe_int(rssi, 0))
        sample_ages.append(age)
        info = per_scanner_age.setdefault(scanner_id, {"oldest": 0.0, "newest": None})
        info["oldest"] = max(float(info.get("oldest") or 0.0), age)
        if info.get("newest") is None:
            info["newest"] = age
        else:
            info["newest"] = min(float(info["newest"]), age)

    # Optional compatibility fallback, disabled for the real-time grid. Keeping
    # this as a switch makes it easy to compare old/sticky behavior if needed.
    if not values and LOCALIZATION_ALLOW_LAST_KNOWN_FALLBACK_FOR_GRID and track.last_known_rssi_vals:
        source = "last_known"
        for scanner, vals in track.last_known_rssi_vals.items():
            if vals:
                values[str(scanner)].extend([safe_int(v, 0) for v in vals])
        sample_ages = []

    matching_mean: Dict[str, float] = {}
    std: Dict[str, float] = {}
    count: Dict[str, int] = {}

    for scanner, samples in values.items():
        if not samples:
            continue
        mean_val = _top_half_mean(samples)
        std_val = _sample_std(samples)
        if mean_val is None or std_val is None:
            continue
        matching_mean[str(scanner)] = round(mean_val, 3)
        std[str(scanner)] = round(std_val, 3)
        count[str(scanner)] = len(samples)

    total_samples = sum(count.values())
    oldest_age = max(sample_ages) if sample_ages else None
    newest_age = min(sample_ages) if sample_ages else None
    is_stale = (
        not matching_mean or
        source != "realtime_live" or
        total_samples < LOCALIZATION_MIN_TOTAL_FRESH_SAMPLES or
        (newest_age is not None and newest_age > LOCALIZATION_STALE_AFTER_SEC) or
        (oldest_age is not None and oldest_age > LOCALIZATION_MAX_SAMPLE_AGE_SEC)
    )

    return {
        "source": source,
        "matching_mean": matching_mean,
        "std": std,
        "count": count,
        "fresh_sample_count": total_samples,
        "fresh_scanners": sorted(list(matching_mean.keys())),
        "fresh_scanner_count": len(matching_mean),
        "newest_sample_age_sec": round(newest_age, 3) if newest_age is not None else None,
        "oldest_sample_age_sec": round(oldest_age, 3) if oldest_age is not None else None,
        "max_allowed_sample_age_sec": LOCALIZATION_MAX_SAMPLE_AGE_SEC,
        "realtime_window_sec": LOCALIZATION_REALTIME_WINDOW_SEC,
        "is_realtime": bool(source == "realtime_live" and not is_stale),
        "is_stale": bool(is_stale),
        "per_scanner_age_sec": {
            str(scanner): {
                "newest": round(float(info.get("newest") or 0.0), 3),
                "oldest": round(float(info.get("oldest") or 0.0), 3),
            }
            for scanner, info in per_scanner_age.items()
        },
    }

def _burst_rssi_maps_for_motion(track: "DeviceTrack") -> List[Dict[str, float]]:
    """
    Return recent per-burst RSSI maps for movement evidence.

    This is intentionally lightweight. It does not try to localize from bursts;
    it only checks whether the RF fingerprint changes over time enough to justify
    treating a generic PHONE_LIKE/UNKNOWN track as mobile.
    """
    maps: List[Dict[str, float]] = []

    # Fixed time slices are added first so continuously advertising phones can
    # still prove movement before a burst gap occurs.
    for raw_map in list(getattr(track, "motion_rssi_maps", []))[-MOTION_SLICE_HISTORY:]:
        if isinstance(raw_map, dict):
            clean = {str(k): float(v) for k, v in raw_map.items() if isinstance(v, (int, float))}
            if len(clean) >= LOCALIZATION_MOTION_MIN_COMMON_SCANNERS:
                maps.append(clean)

    current_motion = track._current_motion_rssi_map() if hasattr(track, "_current_motion_rssi_map") else {}
    if current_motion:
        maps.append(current_motion)

    # Burst maps remain useful for screen-off/intermittent behavior.
    for raw_map in list(track.burst_rssi_maps)[-6:]:
        if isinstance(raw_map, dict):
            clean = {str(k): float(v) for k, v in raw_map.items() if isinstance(v, (int, float))}
            if len(clean) >= LOCALIZATION_MOTION_MIN_COMMON_SCANNERS:
                maps.append(clean)

    if getattr(track, "current_burst_packets", 0) > 0:
        current = {}
        for scanner, vals in getattr(track, "current_burst_rssi_vals", {}).items():
            if vals:
                val = _top_half_mean([safe_int(v, 0) for v in vals])
                if val is not None:
                    current[str(scanner)] = float(val)
        if len(current) >= LOCALIZATION_MOTION_MIN_COMMON_SCANNERS:
            maps.append(current)

    # Remove immediate duplicates that can happen when the current burst and
    # current time-slice cover the same packets.
    deduped = []
    seen = set()
    for m in maps:
        key = tuple(sorted((k, round(float(v), 1)) for k, v in m.items()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(m)

    return deduped[-MOTION_SLICE_HISTORY:]


def _track_motion_evidence(track: "DeviceTrack") -> Tuple[bool, str]:
    """
    Generic motion evidence for localization gating.

    It is deliberately stricter than device_role(). A stationary Apple laptop,
    TV, or background phone-like source may be classified as PHONE_LIKE by BLE
    metadata/burst behavior, but it should not enter the grid unless the RSSI
    vector actually changes over time.
    """
    maps = _burst_rssi_maps_for_motion(track)
    if len(maps) < LOCALIZATION_MOTION_MIN_BURST_MAPS:
        return False, f"not_enough_motion_windows:{len(maps)}"

    strongest_seq = [strongest_scanner(m) for m in maps if strongest_scanner(m) is not None]
    if len(set(strongest_seq)) >= 2:
        return True, "strongest_scanner_changed"

    top2_seq = [tuple(sorted(list(top_scanners(m, 2)))) for m in maps if len(m) >= 2]
    if len(set(top2_seq)) >= 2:
        return True, "top2_pattern_changed"

    # Compare first/last and also all pairs. A stationary source can fluctuate,
    # so the threshold is intentionally moderate/high.
    best_rel = 0.0
    best_common = 0
    for i in range(len(maps)):
        for j in range(i + 1, len(maps)):
            rel, _, common = rssi_distance_pair(maps[i], maps[j])
            if rel is not None and common >= LOCALIZATION_MOTION_MIN_COMMON_SCANNERS:
                if rel > best_rel:
                    best_rel = rel
                    best_common = common

    if best_rel >= LOCALIZATION_MOTION_REL_RMSE_DB:
        return True, f"rssi_vector_changed:rel_rmse={best_rel:.2f},common={best_common}"

    return False, f"stationary_rssi_shape:best_rel_rmse={best_rel:.2f},common={best_common}"


def _track_is_strong_local_mobile_service(track: "DeviceTrack") -> Tuple[bool, str]:
    if not track.has_mobile_service_data():
        return False, "no_mobile_service"

    if track.is_background_mobile_service() or track.is_weak_flat_background() or track.is_outside_stable_source():
        return False, f"background_mobile_gate:{track.device_role()}"

    rssi_map = track.scanner_rssi() or dict(track.last_known_scanner_rssi)
    if len(rssi_map) < LOCALIZATION_MIN_SCANNERS:
        return False, f"not_enough_scanners:{len(rssi_map)}"

    strongest = max(rssi_map.values()) if rssi_map else None
    vals = sorted(rssi_map.values(), reverse=True)
    top2_avg = sum(vals[:2]) / min(2, len(vals)) if vals else None

    if track.mobile_service_score() >= MOBILE_SERVICE_HIGH_SCORE_THRESHOLD:
        return True, "high_mobile_service_score"

    if (
        track.mobile_service_score() >= MOBILE_SERVICE_SCORE_THRESHOLD and
        strongest is not None and strongest >= MOBILE_SERVICE_STRONG_RSSI_DBM
    ):
        return True, "mobile_service_strong_local_peak"

    if (
        track.mobile_service_score() >= MOBILE_SERVICE_SCORE_THRESHOLD and
        top2_avg is not None and top2_avg >= MOBILE_SERVICE_TOP2_AVG_DBM
    ):
        return True, "mobile_service_strong_top2"

    # A moving mobile-service track is still useful even if its absolute RSSI is
    # not very strong, but persistent weak mobile-service tracks remain filtered.
    moving, motion_reason = _track_motion_evidence(track)
    if track.mobile_service_score() >= MOBILE_SERVICE_SCORE_THRESHOLD and moving:
        return True, f"mobile_service_motion:{motion_reason}"

    return False, (
        f"weak_mobile_service:score={track.mobile_service_score():.2f},"
        f"strongest={strongest},top2={top2_avg}"
    )


def _localization_candidate_role(track: "DeviceTrack") -> Tuple[bool, str]:
    if not LOCALIZATION_REQUIRE_MOBILE_ROLE:
        return True, "role_gate_disabled"

    role = track.device_role()
    label = track.label()
    dominant_class = track.dominant_class()
    known_classes = {str(x) for x in track.known_classes()}

    if dominant_class == "Calibration" or "eldarcalib" in str(label).lower():
        return True, "calibration_device"

    display_info = track.display_classification()
    display_class = display_info.get("display_class", "")
    if display_class not in (DISPLAY_CLASS_NEARBY_MOBILE, DISPLAY_CLASS_CALIBRATION):
        return False, f"display_class_gate:{display_class}:{display_info.get('classification_reason', '')}"

    if role in LOCALIZATION_STATIONARY_OR_BACKGROUND_ROLES:
        return False, f"stationary_or_background_role:{role}"

    # Mobile-service identities are the strongest phone-like evidence, but only
    # if they are not weak/background and they are local/moving.
    if role == MOBILE_SERVICE_LABEL or track.has_mobile_service_data():
        ok, reason = _track_is_strong_local_mobile_service(track)
        if ok:
            return True, reason
        return False, reason

    # Do not localize known laptop/beacon/fixed classes through the generic
    # PHONE_LIKE path.
    known_text = " ".join(sorted(known_classes)).lower()
    if any(x in known_text for x in ("microsoft/laptop", "tondo", "beacon")):
        return False, f"known_stationary_class:{known_text}"

    if role == "PHONE_LIKE":
        if not LOCALIZATION_REQUIRE_MOTION_FOR_PHONE_LIKE:
            return True, "phone_like_motion_gate_disabled"

        moving, motion_reason = _track_motion_evidence(track)
        if not moving:
            return False, f"phone_like_without_motion:{motion_reason}"

        strongest = track.strongest_rssi_value()
        top2 = track.top2_avg_rssi_value()
        if strongest is None:
            strongest = track.last_known_strongest_rssi_value()
        if top2 is None:
            top2 = track.last_known_top2_avg_rssi_value()

        if (
            strongest is not None and strongest >= LOCALIZATION_PHONE_LIKE_MIN_LOCAL_RSSI_DBM
        ) or (
            top2 is not None and top2 >= LOCALIZATION_PHONE_LIKE_MIN_TOP2_DBM
        ):
            return True, f"phone_like_with_motion:{motion_reason}"

        return False, (
            f"phone_like_motion_but_weak:strongest={strongest},top2={top2},"
            f"motion={motion_reason}"
        )

    # Conservative fallback for unknown tracks: require actual movement evidence,
    # not only a weak phone_likelihood score.
    if role == "UNKNOWN":
        moving, motion_reason = _track_motion_evidence(track)
        if (
            moving and
            track.phone_likelihood_score() >= max(2.5, PHONE_LIKE_SCORE_THRESHOLD - 1.0) and
            len(track.scanner_visibility()) >= LOCALIZATION_MIN_SCANNERS
        ):
            return True, f"unknown_mobile_motion:{motion_reason}"

    return False, f"not_mobile_role:{role}"

def _ordered_scanners_by_rssi(rssi_map: Dict[str, float]) -> List[str]:
    return [
        str(k) for k, _ in sorted(
            rssi_map.items(),
            key=lambda kv: kv[1],
            reverse=True
        )
    ]


def _rssi_dominance_margin(rssi_map: Dict[str, float]) -> Optional[float]:
    if len(rssi_map) < 2:
        return None
    vals = sorted(rssi_map.values(), reverse=True)
    return float(vals[0] - vals[1])


def _weakest_scanner(rssi_map: Dict[str, float]) -> Optional[str]:
    if not rssi_map:
        return None
    return str(min(rssi_map.items(), key=lambda kv: kv[1])[0])


def _block_score(
    live_mean: Dict[str, float],
    live_std: Dict[str, float],
    live_count: Dict[str, int],
    block_info: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    block_mean = block_info.get("matching_mean") or block_info.get("top_half_mean") or block_info.get("mean") or {}
    block_rel = block_info.get("relative_matching_mean") or relative_vector(block_mean)
    block_std = block_info.get("std") or {}

    if not isinstance(block_mean, dict) or not block_mean:
        return None

    # Normalize keys because CSV/JSON may contain scanner IDs as ints or strings.
    block_mean = {str(k): float(v) for k, v in block_mean.items() if _safe_float_or_none(v) is not None}
    block_rel = {str(k): float(v) for k, v in block_rel.items() if _safe_float_or_none(v) is not None}
    block_std = {str(k): float(v) for k, v in block_std.items() if _safe_float_or_none(v) is not None}
    live_mean = {str(k): float(v) for k, v in live_mean.items() if _safe_float_or_none(v) is not None}
    live_std = {str(k): float(v) for k, v in live_std.items() if _safe_float_or_none(v) is not None}
    live_count = {str(k): int(v) for k, v in live_count.items()}

    live_rel = relative_vector(live_mean)
    common = sorted(set(live_mean.keys()) & set(block_mean.keys()))

    if len(common) < LOCALIZATION_MIN_SCANNERS:
        return None

    rel_num = 0.0
    rel_den = 0.0
    abs_num = 0.0
    abs_den = 0.0
    std_num = 0.0
    std_den = 0.0

    for scanner in common:
        cal_std = _safe_float_or_none(block_std.get(scanner))
        if cal_std is None:
            cal_std = 5.0

        # Clamp so a very low std does not dominate completely and a very high
        # std does not erase the scanner entirely.
        sigma_rel = min(8.0, max(2.0, cal_std))
        reliability = 1.0 / (sigma_rel * sigma_rel)

        rel_diff = float(live_rel.get(scanner, 0.0)) - float(block_rel.get(scanner, 0.0))
        rel_num += reliability * (rel_diff / sigma_rel) ** 2
        rel_den += reliability

        # Absolute RSSI is stronger in V2 because neighboring blocks can share
        # the same dominant scanner. Use a soft sigma to avoid overfitting Tx
        # power, but still let absolute level separate blocks 1/2/3 and 7/8/9.
        abs_diff = float(live_mean.get(scanner, 0.0)) - float(block_mean.get(scanner, 0.0))
        abs_sigma = max(6.0, sigma_rel + 3.0)
        abs_num += (abs_diff / abs_sigma) ** 2
        abs_den += 1.0

        # STD is used as an ambiguity resolver, only when the live window has
        # enough samples from that scanner.
        live_std_val = _safe_float_or_none(live_std.get(scanner))
        if live_std_val is not None and live_count.get(scanner, 0) >= LOCALIZATION_MIN_STD_SAMPLES:
            std_diff = live_std_val - cal_std
            std_num += (std_diff / 4.0) ** 2
            std_den += 1.0

    relative_score = rel_num / rel_den if rel_den > 0 else 999.0
    absolute_score = abs_num / abs_den if abs_den > 0 else 999.0
    std_score = std_num / std_den if std_den > 0 else 0.0

    live_order = _ordered_scanners_by_rssi(live_mean)
    block_order = _ordered_scanners_by_rssi(block_mean)
    live_top2 = live_order[:2]
    block_top2 = [str(x) for x in (block_info.get("top2_scanners") or block_order[:2])][:2]
    live_dominant = live_order[0] if live_order else None
    block_dominant = str(block_info.get("dominant_scanner") or (block_order[0] if block_order else ""))
    live_weakest = _weakest_scanner(live_mean)
    block_weakest = _weakest_scanner(block_mean)

    shape_score = 0.0
    shape_parts = []

    if live_dominant and block_dominant and live_dominant != block_dominant:
        shape_score += 1.20
        shape_parts.append("dominant_mismatch")
    else:
        shape_parts.append("dominant_match")

    live_top2_set = set(live_top2)
    block_top2_set = set(block_top2)
    if live_top2_set == block_top2_set:
        top2_match = "exact"
        shape_parts.append("top2_exact")
    elif live_top2_set & block_top2_set:
        top2_match = "overlap"
        shape_score += 0.45
        shape_parts.append("top2_overlap")
    else:
        top2_match = "different"
        shape_score += 1.35
        shape_parts.append("top2_different")

    if live_weakest and block_weakest and live_weakest != block_weakest:
        shape_score += 0.45
        shape_parts.append("weakest_mismatch")
    else:
        shape_parts.append("weakest_match")

    live_margin = _rssi_dominance_margin(live_mean)
    block_margin = _rssi_dominance_margin(block_mean)
    margin_score = 0.0
    if live_margin is not None and block_margin is not None:
        margin_score = min(1.5, (abs(live_margin - block_margin) / 8.0) ** 2)
        shape_score += margin_score
        shape_parts.append(f"margin_diff={abs(live_margin - block_margin):.2f}")

    total_score = (
        LOCALIZATION_RELATIVE_WEIGHT * relative_score +
        LOCALIZATION_ABSOLUTE_WEIGHT * absolute_score +
        LOCALIZATION_STD_WEIGHT * std_score +
        LOCALIZATION_SHAPE_WEIGHT * shape_score
    )

    return {
        "block": str(block_info.get("block_id", "")),
        "score": round(total_score, 4),
        "relative_score": round(relative_score, 4),
        "absolute_score": round(absolute_score, 4),
        "std_score": round(std_score, 4),
        "shape_score": round(shape_score, 4),
        "shape_reason": ",".join(shape_parts),
        "top2_match": top2_match,
        "common_scanners": len(common),
        "block_top2": block_top2,
        "live_top2": live_top2,
        "live_dominant": live_dominant,
        "block_dominant": block_dominant,
        "live_weakest": live_weakest,
        "block_weakest": block_weakest,
        "live_margin_db": rounded_metric(live_margin),
        "block_margin_db": rounded_metric(block_margin),
        "margin_score": round(margin_score, 4),
        "block_ambiguity": block_info.get("ambiguity", ""),
    }

def _scores_to_probabilities(scored_blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not scored_blocks:
        return []

    min_score = min(float(item["score"]) for item in scored_blocks)
    weights = []
    for item in scored_blocks:
        w = math.exp(-1.0 * (float(item["score"]) - min_score) / max(0.001, LOCALIZATION_SOFTMAX_TEMPERATURE))
        weights.append(w)

    total = sum(weights) or 1.0
    out = []
    for item, weight in zip(scored_blocks, weights):
        row = dict(item)
        row["probability"] = round(weight / total, 4)
        out.append(row)

    out.sort(key=lambda x: (x["probability"], -x["score"]), reverse=True)
    return out


def _probability_confidence(candidates: List[Dict[str, Any]]) -> Tuple[str, str]:
    if not candidates:
        return "NONE", "no_candidates"

    p1 = float(candidates[0].get("probability", 0.0))
    p2 = float(candidates[1].get("probability", 0.0)) if len(candidates) > 1 else 0.0
    margin = p1 - p2

    if p1 >= LOCALIZATION_HIGH_PROBABILITY and margin >= LOCALIZATION_HIGH_MARGIN:
        return "HIGH", f"p={p1:.2f},margin={margin:.2f}"

    if p1 >= LOCALIZATION_MEDIUM_PROBABILITY and margin >= LOCALIZATION_MEDIUM_MARGIN:
        return "MEDIUM", f"p={p1:.2f},margin={margin:.2f}"

    return "LOW", f"p={p1:.2f},margin={margin:.2f}"


def region_hint_for_track(track: "DeviceTrack") -> Dict[str, Any]:
    """
    Conservative real-time region fallback for the GUI.

    Exact grid block localization may be ambiguous, but the strongest/top-2
    scanner pattern is still useful. This function now uses the same fresh RSSI
    window as grid localization so the region hint does not lag behind movement.
    """
    stats_for_loc = scanner_stats_for_localization(track)
    rssi_map = stats_for_loc.get("matching_mean", {}) or {}

    if not rssi_map or stats_for_loc.get("is_stale", True):
        return {
            "region_hint": "UNKNOWN_REGION",
            "region_confidence": "NONE",
            "region_scanners": [],
            "region_reason": (
                f"no_fresh_realtime_rssi:"
                f"samples={stats_for_loc.get('fresh_sample_count', 0)},"
                f"scanners={stats_for_loc.get('fresh_scanner_count', 0)},"
                f"newest_age={stats_for_loc.get('newest_sample_age_sec')},"
                f"oldest_age={stats_for_loc.get('oldest_sample_age_sec')}"
            ),
        }

    ordered = _ordered_scanners_by_rssi(rssi_map)
    strongest = ordered[0] if ordered else None
    top2 = ordered[:2]
    vals = sorted(rssi_map.values(), reverse=True)
    strongest_rssi = vals[0] if vals else None
    top2_avg = sum(vals[:2]) / min(2, len(vals)) if vals else None
    margin = vals[0] - vals[1] if len(vals) >= 2 else None
    age_part = (
        f"newest_age={stats_for_loc.get('newest_sample_age_sec')},"
        f"oldest_age={stats_for_loc.get('oldest_sample_age_sec')},"
        f"samples={stats_for_loc.get('fresh_sample_count', 0)}"
    )

    if len(top2) == 1:
        confidence = "MEDIUM" if strongest_rssi is not None and strongest_rssi >= LOCATION_WEAK_RSSI_DBM else "LOW"
        return {
            "region_hint": f"scanner_{top2[0]}_area",
            "region_confidence": confidence,
            "region_scanners": top2,
            "region_reason": (
                f"realtime_single_scanner,strongest={strongest_rssi:.2f},{age_part}"
                if strongest_rssi is not None else f"realtime_single_scanner,{age_part}"
            ),
        }

    if margin is not None and margin >= LOCATION_HIGH_MARGIN_DB:
        confidence = "HIGH" if strongest_rssi is not None and strongest_rssi >= LOCATION_STRONG_RSSI_DBM else "MEDIUM"
        return {
            "region_hint": f"scanner_{strongest}_area",
            "region_confidence": confidence,
            "region_scanners": [strongest],
            "region_reason": f"realtime_clear_strongest,margin={margin:.2f},strongest={strongest_rssi:.2f},{age_part}",
        }

    if top2_avg is not None and top2_avg >= -82.0:
        confidence = "MEDIUM"
    elif top2_avg is not None and top2_avg >= -90.0:
        confidence = "LOW"
    else:
        confidence = "LOW"

    return {
        "region_hint": f"between_scanner_{top2[0]}_{top2[1]}",
        "region_confidence": confidence,
        "region_scanners": top2,
        "region_reason": (
            f"realtime_top2_region,strongest={strongest},margin={margin:.2f},"
            f"top2_avg={top2_avg:.2f},{age_part}"
            if margin is not None and top2_avg is not None else f"realtime_top2_region,{age_part}"
        ),
    }

def _grid_display_hold_payload(track: "DeviceTrack", base: Dict[str, Any], reason: str) -> Optional[Dict[str, Any]]:
    """
    Return a short-lived held grid-display estimate for measurement stability.

    This is intentionally limited to LOCALIZATION_DISPLAY_HOLD_SEC. It prevents
    blinking when the fresh RSSI window briefly lacks 3 scanners, while avoiding
    the old 30-60 second delayed-position problem.
    """
    block = getattr(track, "grid_display_hold_block", None)
    updated = float(getattr(track, "grid_display_hold_updated_mono", 0.0) or 0.0)
    if not block or updated <= 0.0:
        return None

    age = max(0.0, time.monotonic() - updated)
    if age > LOCALIZATION_DISPLAY_HOLD_SEC:
        return None

    held = dict(base)
    held.update({
        "localization_enabled": True,
        "localization_skip_reason": f"held_recent_display:{reason};hold_age={age:.2f}s",
        "location_block": None,
        "location_probability": round(float(getattr(track, "grid_display_hold_probability", 0.0) or 0.0), 4),
        "location_candidates": list(getattr(track, "grid_display_hold_candidates", []) or [])[:5],
        "block_location_confidence": "HELD_RECENT",
        "block_location_reason": f"held_recent_display:{reason};hold_age={age:.2f}s",
        "grid_display_block": str(block),
        "grid_display_probability": round(float(getattr(track, "grid_display_hold_probability", 0.0) or 0.0), 4),
        "grid_display_confidence": "HELD_RECENT",
        "grid_display_reason": (
            f"held_recent_display:{reason};hold_age={age:.2f}s;"
            f"last_reason={getattr(track, 'grid_display_hold_reason', '')}"
        ),
        "grid_display_is_test_estimate": bool(getattr(track, "grid_display_hold_is_test_estimate", True)),
        "grid_display_candidate_rank": getattr(track, "grid_display_hold_candidate_rank", None),
        "position_age_sec": round(age, 3),
        "is_realtime": False,
        "is_stale": False,
    })
    return held


def _remember_grid_display_estimate(
    track: "DeviceTrack",
    block: Any,
    probability: float,
    confidence: str,
    reason: str,
    candidates: List[Dict[str, Any]],
    is_test_estimate: bool,
    candidate_rank: Optional[int],
) -> None:
    if block is None:
        return
    track.grid_display_hold_block = str(block)
    track.grid_display_hold_probability = float(probability or 0.0)
    track.grid_display_hold_confidence = str(confidence or "NONE")
    track.grid_display_hold_reason = str(reason or "")
    track.grid_display_hold_is_test_estimate = bool(is_test_estimate)
    track.grid_display_hold_candidate_rank = candidate_rank
    track.grid_display_hold_candidates = list(candidates or [])[:5]
    track.grid_display_hold_updated_mono = time.monotonic()


def localize_track_to_grid(track: "DeviceTrack") -> Dict[str, Any]:
    """
    Mobile-only probabilistic 3x3 grid localization.

    Output fields are intentionally separate from the older strongest-scanner
    location_confidence field.
    """
    base = {
        "localization_enabled": False,
        "localization_skip_reason": "",
        "location_block": None,
        "location_probability": 0.0,
        "location_candidates": [],
        "block_location_confidence": "NONE",
        "block_location_reason": "",
        "block_location_source": "",
        "block_live_rssi_mean": {},
        "block_live_rssi_std": {},
        "block_live_sample_count": {},
        "fresh_sample_count": 0,
        "fresh_scanners": [],
        "fresh_scanner_count": 0,
        "newest_sample_age_sec": None,
        "oldest_sample_age_sec": None,
        "position_age_sec": None,
        "is_realtime": False,
        "is_stale": True,
        "realtime_window_sec": LOCALIZATION_REALTIME_WINDOW_SEC,
        "max_allowed_sample_age_sec": LOCALIZATION_MAX_SAMPLE_AGE_SEC,
        "per_scanner_age_sec": {},
        # Grid-display fields are separate from the strict assignment fields.
        # location_block stays conservative; grid_display_block may show the
        # best test candidate for eligible mobiles/EldarCalib during tuning.
        "grid_display_block": None,
        "grid_display_probability": 0.0,
        "grid_display_confidence": "NONE",
        "grid_display_reason": "",
        "grid_display_is_test_estimate": False,
        "grid_display_candidate_rank": None,
    }

    with localization_lock:
        loaded = bool(localization_state.get("loaded", False))
        fp = localization_state.get("fingerprints", {}) or {}

    if not GRID_LOCALIZATION_ENABLED:
        base["localization_skip_reason"] = "disabled"
        return base

    if not loaded:
        base["localization_skip_reason"] = "fingerprints_not_loaded"
        base["block_location_reason"] = localization_state.get("message", "")
        return base

    eligible, role_reason = _localization_candidate_role(track)
    if not eligible:
        base["localization_skip_reason"] = role_reason
        return base

    stats_for_loc = scanner_stats_for_localization(track)
    live_mean = stats_for_loc.get("matching_mean", {})
    live_std = stats_for_loc.get("std", {})
    live_count = stats_for_loc.get("count", {})

    base["block_live_rssi_mean"] = live_mean
    base["block_live_rssi_std"] = live_std
    base["block_live_sample_count"] = live_count
    base["block_location_source"] = stats_for_loc.get("source", "")
    base["fresh_sample_count"] = stats_for_loc.get("fresh_sample_count", 0)
    base["fresh_scanners"] = stats_for_loc.get("fresh_scanners", [])
    base["fresh_scanner_count"] = stats_for_loc.get("fresh_scanner_count", 0)
    base["newest_sample_age_sec"] = stats_for_loc.get("newest_sample_age_sec")
    base["oldest_sample_age_sec"] = stats_for_loc.get("oldest_sample_age_sec")
    base["position_age_sec"] = stats_for_loc.get("newest_sample_age_sec")
    base["is_realtime"] = stats_for_loc.get("is_realtime", False)
    base["is_stale"] = stats_for_loc.get("is_stale", True)
    base["realtime_window_sec"] = stats_for_loc.get("realtime_window_sec", LOCALIZATION_REALTIME_WINDOW_SEC)
    base["max_allowed_sample_age_sec"] = stats_for_loc.get("max_allowed_sample_age_sec", LOCALIZATION_MAX_SAMPLE_AGE_SEC)
    base["per_scanner_age_sec"] = stats_for_loc.get("per_scanner_age_sec", {})

    if stats_for_loc.get("is_stale", True):
        reason = (
            f"stale_or_insufficient_realtime_samples:"
            f"samples={stats_for_loc.get('fresh_sample_count', 0)},"
            f"scanners={stats_for_loc.get('fresh_scanner_count', 0)},"
            f"newest_age={stats_for_loc.get('newest_sample_age_sec')},"
            f"oldest_age={stats_for_loc.get('oldest_sample_age_sec')}"
        )
        held = _grid_display_hold_payload(track, base, reason)
        if held is not None:
            return held
        base["localization_skip_reason"] = reason
        return base

    if len(live_mean) < LOCALIZATION_MIN_SCANNERS:
        reason = f"not_enough_scanners:{len(live_mean)}"
        held = _grid_display_hold_payload(track, base, reason)
        if held is not None:
            return held
        base["localization_skip_reason"] = reason
        return base

    blocks = fp.get("blocks", {}) or {}
    scored = []
    for block_id, block_info in blocks.items():
        if not isinstance(block_info, dict):
            continue
        block_info = dict(block_info)
        block_info.setdefault("block_id", str(block_id))
        row = _block_score(live_mean, live_std, live_count, block_info)
        if row is not None:
            scored.append(row)

    candidates = _scores_to_probabilities(scored)
    if not candidates:
        reason = "no_matching_blocks"
        held = _grid_display_hold_payload(track, base, reason)
        if held is not None:
            return held
        base["localization_skip_reason"] = reason
        return base

    # Hysteresis: keep previous block unless a different block clearly wins.
    raw_best = candidates[0]
    chosen = raw_best
    smooth_reason = "raw_best"

    previous_block = getattr(track, "grid_location_block", None)
    if (not LOCALIZATION_DISABLE_HYSTERESIS_FOR_TEST) and previous_block and str(previous_block) != str(raw_best["block"]):
        previous_candidate = next((c for c in candidates if str(c.get("block")) == str(previous_block)), None)
        if previous_candidate is not None:
            raw_p = float(raw_best.get("probability", 0.0))
            prev_p = float(previous_candidate.get("probability", 0.0))
            if raw_p < LOCALIZATION_SWITCH_PROBABILITY and prev_p >= raw_p - LOCALIZATION_KEEP_PREVIOUS_MARGIN:
                chosen = previous_candidate
                smooth_reason = f"smoothed_keep_previous:{previous_block}"

    confidence, conf_reason = _probability_confidence(candidates)

    chosen_probability = float(chosen.get("probability", 0.0))
    second_probability = float(candidates[1].get("probability", 0.0)) if len(candidates) > 1 else 0.0
    chosen_margin = chosen_probability - second_probability

    # V2: do not force a block when the probability distribution is flat or weak.
    # This is the main fix for "block 1 absorbs 2/3/4" and similar wrong placements.
    assign_block = (
        confidence in ("MEDIUM", "HIGH") and
        chosen_probability >= LOCALIZATION_ASSIGN_MIN_PROBABILITY and
        chosen_margin >= LOCALIZATION_ASSIGN_MIN_MARGIN
    )

    track.grid_location_history.append({
        "raw_block": raw_best.get("block"),
        "chosen_block": chosen.get("block") if assign_block else None,
        "raw_probability": raw_best.get("probability"),
        "chosen_probability": chosen_probability,
        "assigned": assign_block,
        "time_mono": round(time.monotonic(), 3),
    })

    if assign_block:
        track.grid_location_block = str(chosen.get("block"))
        track.grid_location_probability = chosen_probability
        track.grid_location_confidence = confidence
        track.grid_location_last_update_mono = time.monotonic()
        _remember_grid_display_estimate(
            track,
            chosen.get("block"),
            chosen_probability,
            confidence,
            f"assigned:{role_reason};{conf_reason};{smooth_reason}",
            candidates,
            False,
            1,
        )

        base.update({
            "localization_enabled": True,
            "localization_skip_reason": "",
            "location_block": str(chosen.get("block")),
            "location_probability": round(chosen_probability, 4),
            "location_candidates": candidates[:5],
            "block_location_confidence": confidence,
            "block_location_reason": f"{role_reason};{conf_reason};{smooth_reason};assigned",
            "grid_display_block": str(chosen.get("block")),
            "grid_display_probability": round(chosen_probability, 4),
            "grid_display_confidence": confidence,
            "grid_display_reason": f"assigned:{role_reason};{conf_reason};{smooth_reason}",
            "grid_display_is_test_estimate": False,
            "grid_display_candidate_rank": 1,
        })
        return base

    track.grid_location_block = None
    track.grid_location_probability = chosen_probability
    track.grid_location_confidence = "AMBIGUOUS"
    track.grid_location_last_update_mono = time.monotonic()

    # For tuning runs, keep strict location_block=None, but expose where the
    # model would have placed this eligible mobile/calibration signal.
    grid_display_block = None
    grid_display_probability = 0.0
    grid_display_confidence = "NONE"
    grid_display_reason = ""
    grid_display_is_test_estimate = False
    grid_display_rank = None

    if LOCALIZATION_GRID_SHOW_BEST_CANDIDATE_FOR_TEST and candidates:
        grid_display_block = str(chosen.get("block"))
        grid_display_probability = round(chosen_probability, 4)
        grid_display_confidence = "AMBIGUOUS" if confidence == "LOW" else confidence
        grid_display_reason = (
            f"test_best_candidate:{role_reason};{conf_reason};"
            f"{smooth_reason};not_strictly_assigned"
        )
        grid_display_is_test_estimate = True
        try:
            grid_display_rank = 1 + next(
                idx for idx, cand in enumerate(candidates)
                if str(cand.get("block")) == str(chosen.get("block"))
            )
        except Exception:
            grid_display_rank = 1

    if grid_display_block is not None:
        _remember_grid_display_estimate(
            track,
            grid_display_block,
            grid_display_probability,
            grid_display_confidence,
            grid_display_reason,
            candidates,
            grid_display_is_test_estimate,
            grid_display_rank,
        )

    base.update({
        "localization_enabled": True,
        "localization_skip_reason": (
            f"ambiguous_or_low_probability:p={chosen_probability:.2f},"
            f"margin={chosen_margin:.2f},confidence={confidence}"
        ),
        "location_block": None,
        "location_probability": round(chosen_probability, 4),
        "location_candidates": candidates[:5],
        "block_location_confidence": "AMBIGUOUS",
        "block_location_reason": f"{role_reason};{conf_reason};{smooth_reason};not_assigned",
        "grid_display_block": grid_display_block,
        "grid_display_probability": grid_display_probability,
        "grid_display_confidence": grid_display_confidence,
        "grid_display_reason": grid_display_reason,
        "grid_display_is_test_estimate": grid_display_is_test_estimate,
        "grid_display_candidate_rank": grid_display_rank,
    })
    return base


def make_json_safe(value: Any) -> Any:
    """
    Recursively convert Python-only container types into JSON-safe values.
    This protects /api/localization from accidental set/tuple objects in
    diagnostic fields or localization candidates.
    """
    if isinstance(value, dict):
        return {str(k): make_json_safe(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted([make_json_safe(v) for v in value], key=lambda x: str(x))
    if isinstance(value, (list, tuple, deque)):
        return [make_json_safe(v) for v in value]
    return value


def build_localization_api_payload(tracker_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    status = localization_status_for_api()
    layout = status.get("grid_layout") or [[1, 2, 3], [6, 5, 4], [7, 8, 9]]
    block_ids = [str(block) for row in layout for block in row]

    blocks: Dict[str, List[Dict[str, Any]]] = {block_id: [] for block_id in block_ids}
    mobile_unlocalized = []
    skipped = []

    for row in tracker_snapshot.get("tracks", []):
        item = {
            "uid": row.get("uid"),
            "label": row.get("label"),
            "status": row.get("status"),
            "role": row.get("device_role"),
            "display_class": row.get("display_class"),
            "device_type": row.get("device_type"),
            "movement_state": row.get("movement_state"),
            "rf_motion_evidence": row.get("rf_motion_evidence", False),
            "rf_motion_reason": row.get("rf_motion_reason", ""),
            "classification_confidence": row.get("classification_confidence"),
            "classification_reason": row.get("classification_reason"),
            "grid_display_eligible": row.get("grid_display_eligible", False),
            "region_hint": row.get("region_hint"),
            "region_confidence": row.get("region_confidence"),
            "region_scanners": row.get("region_scanners", []),
            "region_reason": row.get("region_reason"),
            "presence_state": row.get("presence_state"),
            "probability": row.get("location_probability"),
            "confidence": row.get("block_location_confidence"),
            "reason": row.get("block_location_reason"),
            "grid_display_block": row.get("grid_display_block"),
            "grid_display_probability": row.get("grid_display_probability"),
            "grid_display_confidence": row.get("grid_display_confidence"),
            "grid_display_reason": row.get("grid_display_reason"),
            "grid_display_is_test_estimate": row.get("grid_display_is_test_estimate", False),
            "grid_display_candidate_rank": row.get("grid_display_candidate_rank"),
            "strongest_scanner": row.get("strongest_scanner"),
            "top2_scanners": row.get("top2_scanners", []),
            "scanner_rssi": row.get("scanner_rssi", {}),
            "live_rssi_mean": row.get("block_live_rssi_mean", {}),
            "live_rssi_std": row.get("block_live_rssi_std", {}),
            "fresh_sample_count": row.get("fresh_sample_count", 0),
            "fresh_scanners": row.get("fresh_scanners", []),
            "fresh_scanner_count": row.get("fresh_scanner_count", 0),
            "newest_sample_age_sec": row.get("newest_sample_age_sec"),
            "oldest_sample_age_sec": row.get("oldest_sample_age_sec"),
            "position_age_sec": row.get("position_age_sec"),
            "is_realtime": row.get("is_realtime", False),
            "is_stale": row.get("is_stale", True),
            "realtime_window_sec": row.get("realtime_window_sec"),
            "max_allowed_sample_age_sec": row.get("max_allowed_sample_age_sec"),
            "per_scanner_age_sec": row.get("per_scanner_age_sec", {}),
            "candidates": row.get("location_candidates", []),
            "mobile_service_uuids": row.get("mobile_service_uuids", []),
            "last_seen_age_sec": row.get("last_seen_age_sec"),
        }

        allowed_grid_class = row.get("display_class") in LOCALIZATION_GRID_DISPLAY_CLASSES
        grid_block = row.get("grid_display_block") or row.get("location_block")

        if row.get("localization_enabled") and allowed_grid_class and row.get("grid_display_eligible", False) and grid_block:
            # The visual grid is intentionally filtered: only real nearby mobiles
            # and explicit EldarCalib/calibration advertisements may enter it.
            # Stationary/background/debug tracks are never placed in blocks.
            block_id = str(grid_block)
            item["grid_display_only_allowed_classes"] = sorted(list(LOCALIZATION_GRID_DISPLAY_CLASSES))
            blocks.setdefault(block_id, []).append(item)
        elif row.get("localization_enabled") and allowed_grid_class:
            # Eligible mobile/calibration track, but there is not enough RSSI
            # information to place even a test candidate on the grid.
            item["skip_reason"] = row.get("localization_skip_reason", "")
            mobile_unlocalized.append(item)
        elif row.get("localization_enabled"):
            # Defensive: localization was enabled, but the final display class is
            # not allowed on the visual grid. Keep it out of the grid.
            item["skip_reason"] = f"grid_display_class_filtered:{row.get('display_class')}"
            skipped.append(item)
        else:
            # Not eligible for localization. This includes stationary/background
            # tracks and PHONE_LIKE tracks without movement evidence.
            skipped.append({
                "uid": row.get("uid"),
                "label": row.get("label"),
                "role": row.get("device_role"),
                "display_class": row.get("display_class"),
                "device_type": row.get("device_type"),
                "movement_state": row.get("movement_state"),
                "rf_motion_evidence": row.get("rf_motion_evidence", False),
                "rf_motion_reason": row.get("rf_motion_reason", ""),
                "status": row.get("status"),
                "skip_reason": row.get("localization_skip_reason", ""),
                "region_hint": row.get("region_hint"),
                "region_confidence": row.get("region_confidence"),
                "region_scanners": row.get("region_scanners", []),
                "region_reason": row.get("region_reason"),
                "strongest_scanner": row.get("strongest_scanner"),
                "last_seen_age_sec": row.get("last_seen_age_sec"),
                "fresh_sample_count": row.get("fresh_sample_count", 0),
                "fresh_scanners": row.get("fresh_scanners", []),
                "position_age_sec": row.get("position_age_sec"),
                "is_realtime": row.get("is_realtime", False),
                "is_stale": row.get("is_stale", True),
            })

    for block_devices in blocks.values():
        block_devices.sort(key=lambda item: float(item.get("probability") or 0.0), reverse=True)

    return {
        "localization": status,
        "layout": layout,
        "blocks": blocks,
        "mobile_unlocalized": mobile_unlocalized,
        "skipped_count": len(skipped),
        "skipped_preview": skipped[:30],
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }



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
        self._pending_merge_reason = ""
        self.last_merge_events = deque(maxlen=50)

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

    def _payload_mobile_uuid_set(self, payload_sig: str) -> set:
        parts = parse_payload_sig_parts({payload_sig})
        return set(parts.get("service_uuids", set())) & MOBILE_SERVICE_UUIDS

    def _payload_family_key(self, payload_sig: str) -> Tuple[str, Tuple[str, ...], Tuple[str, ...], Tuple[int, ...]]:
        """Compact exact-ish payload family key for safe polluted-track expansion."""
        parts = parse_payload_sig_parts({payload_sig})
        service_uuids = tuple(sorted(str(x) for x in parts.get("service_uuids", set())))
        mobile_uuids = tuple(sorted(str(x) for x in (parts.get("service_uuids", set()) & MOBILE_SERVICE_UUIDS)))
        mfg_ids = tuple(sorted(f"{x:04X}" for x in parts.get("mfg_ids", set()) if isinstance(x, int)))
        lengths = tuple(sorted(int(x) for x in parts.get("lengths", set()) if isinstance(x, int)))

        if mobile_uuids:
            return ("MOBILE_SERVICE", mobile_uuids, tuple(), lengths)
        if service_uuids:
            return ("SERVICE_DATA", service_uuids, tuple(), lengths)
        if mfg_ids:
            return ("MANUFACTURER_DATA", tuple(), mfg_ids, lengths)
        return ("OTHER_ADV", tuple(), tuple(), lengths)

    def _shared_payload_family(self, a: DeviceTrack, b: DeviceTrack) -> bool:
        a_keys = {self._payload_family_key(sig) for sig in a.payload_sigs}
        b_keys = {self._payload_family_key(sig) for sig in b.payload_sigs}
        return bool(a_keys & b_keys)

    def _same_mac_and_shared_payload_family(self, a: DeviceTrack, b: DeviceTrack) -> bool:
        return bool((a.macs & b.macs) and self._shared_payload_family(a, b))

    def _mobile_payload_family_items(self, track: DeviceTrack, min_packets: int = MOBILE_FAMILY_MIN_PACKETS_FOR_MERGE) -> List[Dict[str, Any]]:
        """
        Return mobile-service payload families with their own RSSI vectors.

        This is stricter than whole-track RSSI. Whole-track averages allowed PD_006
        to swallow FCF1, FEF3, FD5A, and weak OTHER_ADV families. Per-family gates
        keep those families separate unless they really match.
        """
        items = []
        for sig, info in track.payload_family_rssi_maps().items():
            parts = info.get("parts", {}) or {}
            service_uuids = set(parts.get("service_uuids", set()))
            mobile_uuids = service_uuids & MOBILE_SERVICE_UUIDS
            if not mobile_uuids:
                continue
            packets = int(info.get("packets", 0) or 0)
            if packets < min_packets:
                continue
            rssi = {str(k): float(v) for k, v in (info.get("rssi") or {}).items()}
            if len(rssi) < MIN_COMMON_SCANNERS_STRONG_MATCH:
                continue
            items.append({
                "payload_sig": sig,
                "mobile_uuids": mobile_uuids,
                "family_key": self._payload_family_key(sig),
                "packets": packets,
                "rssi": rssi,
            })

        # Fallback for very young mobile tracks before per-payload history is rich.
        if not items and track.has_mobile_service_data():
            rssi = effective_scanner_rssi(track)
            if len(rssi) >= MIN_COMMON_SCANNERS_STRONG_MATCH:
                items.append({
                    "payload_sig": "TRACK_LEVEL_MOBILE_SERVICE",
                    "mobile_uuids": set(track.mobile_service_uuids or mobile_service_uuids_from_sigs(track.payload_sigs)),
                    "family_key": ("MOBILE_SERVICE", tuple(sorted(track.mobile_service_uuids or mobile_service_uuids_from_sigs(track.payload_sigs))), tuple(), tuple()),
                    "packets": int(track.mobile_service_packet_count or track.packet_count),
                    "rssi": {str(k): float(v) for k, v in rssi.items()},
                })
        return items

    def _mobile_family_rssi_compatible(self, a_rssi: Dict[str, float], b_rssi: Dict[str, float]) -> bool:
        metrics = rssi_fingerprint_metrics(a_rssi, b_rssi)
        if metrics.get("common_scanners", 0) < MIN_COMMON_SCANNERS_STRONG_MATCH:
            return False
        rel = metrics.get("rel_rmse")
        absr = metrics.get("abs_rmse")
        max_diff = metrics.get("max_diff")
        if rel is None or absr is None or max_diff is None:
            return False
        if rel > MOBILE_FAMILY_MAX_REL_RMSE_DB:
            return False
        if absr > MOBILE_FAMILY_MAX_ABS_RMSE_DB:
            return False
        if max_diff > MOBILE_FAMILY_MAX_PER_SCANNER_DIFF_DB:
            return False
        if MOBILE_FAMILY_REQUIRE_SAME_STRONGEST and not metrics.get("same_strongest", False):
            return False
        if MOBILE_FAMILY_REQUIRE_TOP2_OVERLAP and not metrics.get("top2_overlap", False):
            return False
        return True

    def _mobile_service_track_family_merge_allowed(self, a: DeviceTrack, b: DeviceTrack) -> bool:
        """
        Prevent mobile-service chain merges.

        A shared service UUID or shared CRC is not enough if one family is strong
        around scanner 2 and another is strong around scanner 4. Exact same MAC +
        same payload family is still allowed because it is real address-level
        continuity.
        """
        if not (a.has_mobile_service_data() and b.has_mobile_service_data()):
            return True

        if self._same_mac_and_shared_payload_family(a, b):
            return True

        a_items = self._mobile_payload_family_items(a)
        b_items = self._mobile_payload_family_items(b)
        if not a_items or not b_items:
            return False

        # Require at least one common mobile-service UUID family. This blocks
        # FCF1-only from merging with FEF3-only background families.
        a_uuid_union = set().union(*(item["mobile_uuids"] for item in a_items)) if a_items else set()
        b_uuid_union = set().union(*(item["mobile_uuids"] for item in b_items)) if b_items else set()
        if a_uuid_union and b_uuid_union and a_uuid_union.isdisjoint(b_uuid_union):
            return False

        compatible_pairs = 0
        checked_pairs = 0
        for ai in a_items:
            for bi in b_items:
                if ai["mobile_uuids"].isdisjoint(bi["mobile_uuids"]):
                    continue
                checked_pairs += 1
                if self._mobile_family_rssi_compatible(ai["rssi"], bi["rssi"]):
                    compatible_pairs += 1

        if checked_pairs == 0:
            return False

        return compatible_pairs > 0

    def _mobile_service_alias_family_allowed(
        self,
        track: DeviceTrack,
        alias: Optional[AliasTrack],
        parsed: Dict[str, Any],
        incoming_rssi: Dict[str, float],
        has_direct_low_level_proof: bool,
    ) -> bool:
        """Gate a mobile-service alias before it is absorbed into a mobile-service track."""
        if not track.has_mobile_service_data():
            return True

        alias_uuids = set()
        if alias is not None:
            alias_uuids |= set(alias.mobile_service_uuids)
            alias_uuids |= mobile_service_uuids_from_sigs(alias.payload_sigs)
        alias_uuids |= (service_data_uuids(parsed) & MOBILE_SERVICE_UUIDS)

        if not alias_uuids:
            return True

        track_uuids = set(track.mobile_service_uuids) | mobile_service_uuids_from_sigs(track.payload_sigs)
        if track_uuids and alias_uuids.isdisjoint(track_uuids) and not has_direct_low_level_proof:
            return False

        if not incoming_rssi or len(incoming_rssi) < MIN_COMMON_SCANNERS_STRONG_MATCH:
            return False

        candidates = [item for item in self._mobile_payload_family_items(track) if not alias_uuids.isdisjoint(item["mobile_uuids"])]
        if not candidates:
            return has_direct_low_level_proof

        return any(self._mobile_family_rssi_compatible(incoming_rssi, item["rssi"]) for item in candidates)

    def _identity_continuity_merge_reason(self, a: DeviceTrack, b: DeviceTrack) -> Optional[str]:
        """
        Strict generic hard merge for same identity-family continuations.

        This handles cases like:
          - known manufacturer-data track reappears under a new MAC/payload
          - mixed known/mobile track shares manufacturer evidence with known track

        It intentionally requires low-level evidence plus a close RF fingerprint.
        It is not Samsung-specific.
        """
        a_summary = a.identity_summary()
        b_summary = b.identity_summary()

        if has_manufacturer_conflict(a_summary["mfg_ids"], b_summary["mfg_ids"]):
            return None

        # Do not bridge incompatible known classes.
        a_known = a_summary["known_classes"]
        b_known = b_summary["known_classes"]
        if STRICT_METADATA_CONFLICTS and a_known and b_known and a_known.isdisjoint(b_known):
            return None

        # Low-level compatibility evidence.
        shared_mfg = a_summary["mfg_ids"] & b_summary["mfg_ids"]
        shared_crc = a_summary["payload_crc_set"] & b_summary["payload_crc_set"]
        shared_service = a_summary["service_uuid_set"] & b_summary["service_uuid_set"]
        shared_ad_structure = a_summary["ad_structure_set"] & b_summary["ad_structure_set"]
        shared_payload_lengths = a_summary["payload_lengths"] & b_summary["payload_lengths"]

        same_known_family = bool(a_known and b_known and not a_known.isdisjoint(b_known))
        same_mfg_family = bool(shared_mfg)
        same_service_family = bool(shared_service)
        strong_payload_overlap = len(shared_crc) >= IDENTITY_CONTINUITY_MIN_SHARED_CRC
        direct_track_proof = bool((a.macs & b.macs) or strong_payload_overlap)
        same_mac_and_payload_family = self._same_mac_and_shared_payload_family(a, b)
        a_has_mobile_service = a.has_mobile_service_data()
        b_has_mobile_service = b.has_mobile_service_data()

        # V4 safety: mobile-service identity continuity must be per-family, not
        # whole-track average based. This blocks FCF1/FEF3/FD5A chain merges.
        if a_has_mobile_service and b_has_mobile_service:
            if not self._mobile_service_track_family_merge_allowed(a, b):
                return None

        # Polluted mobile/debug tracks are frozen unless exact same MAC+payload
        # family proves continuity. Shared CRC alone is no longer enough here.
        if (a.pollution_suspect()[0] or b.pollution_suspect()[0]) and not same_mac_and_payload_family:
            return None

        # V3 safety:
        # Do not use identity-continuity as a bridge between known/fixed payload
        # families and mobile-service families unless there is direct proof.
        # RSSI + same manufacturer is not enough; it caused polluted PD buckets.
        if (a.pollution_suspect()[0] or b.pollution_suspect()[0]) and not direct_track_proof:
            return None

        if (a_has_mobile_service != b_has_mobile_service) and (a_known or b_known) and not direct_track_proof:
            return None

        if (a_has_mobile_service and b_has_mobile_service and (a_known or b_known)):
            if (a_summary.get("is_mixed") or b_summary.get("is_mixed")) and not direct_track_proof:
                return None

        # V2 safety:
        # A mobile-dominant track and a known-dominant track must not hard-merge
        # just because both contain some known/manufacturer evidence. This was
        # the path that allowed Apple+FCF1 mixed tracks to pollute Apple tracks.
        # Keep those as cross-personality associations unless there is direct
        # proof: same MAC or shared exact payload CRC.
        a_mobile_dom = bool(a_summary.get("is_mobile_service_dominant", False))
        b_mobile_dom = bool(b_summary.get("is_mobile_service_dominant", False))
        a_known_dom = bool(a_summary.get("is_known_dominant", False))
        b_known_dom = bool(b_summary.get("is_known_dominant", False))
        cross_mobile_known_dom = (
            (a_mobile_dom and b_known_dom) or
            (b_mobile_dom and a_known_dom)
        )
        if cross_mobile_known_dom and not direct_track_proof:
            return None

        # Require actual low-level identity evidence. RSSI alone is not enough.
        if IDENTITY_CONTINUITY_REQUIRE_SHARED_LOW_LEVEL_EVIDENCE:
            if not (strong_payload_overlap or same_mfg_family or (same_known_family and same_service_family)):
                return None

        # Same vendor/class but no CRC may still be compatible only if the payload
        # family also looks similar. This avoids broad same-vendor buckets.
        if same_mfg_family and not strong_payload_overlap:
            if not shared_ad_structure and not shared_payload_lengths:
                return None

        # Do not revive stale memory beyond configured identity memory.
        last_seen_gap = abs(a.last_seen_mono - b.last_seen_mono)
        if last_seen_gap > IDENTITY_CONTINUITY_MAX_LAST_SEEN_GAP_SEC:
            return None

        ar_raw = effective_scanner_rssi(a)
        br_raw = effective_scanner_rssi(b)
        metrics = rssi_fingerprint_metrics(ar_raw, br_raw)

        if metrics["common_scanners"] < MIN_COMMON_SCANNERS_STRONG_MATCH:
            return None
        if metrics["rel_rmse"] is None or metrics["abs_rmse"] is None or metrics["max_diff"] is None:
            return None
        if metrics["rel_rmse"] > IDENTITY_CONTINUITY_MAX_REL_RMSE_DB:
            return None
        if metrics["abs_rmse"] > IDENTITY_CONTINUITY_MAX_ABS_RMSE_DB:
            return None
        if metrics["max_diff"] > IDENTITY_CONTINUITY_MAX_PER_SCANNER_DIFF_DB:
            return None
        if IDENTITY_CONTINUITY_REQUIRE_SAME_STRONGEST and not metrics["same_strongest"]:
            return None
        if IDENTITY_CONTINUITY_REQUIRE_TOP2_OVERLAP and not metrics["top2_overlap"]:
            return None

        # Avoid merging a stable/fixed known device with a rotating nearby source
        # unless there is very strong direct evidence.
        if (a.is_stable_device() or b.is_stable_device()) and not (strong_payload_overlap and metrics["abs_rmse"] <= 3.0):
            return None

        reason_parts = ["identity_continuity"]
        if same_known_family:
            reason_parts.append("same_known_class")
        if same_mfg_family:
            reason_parts.append("shared_mfg")
        if strong_payload_overlap:
            reason_parts.append("shared_crc")
        if same_service_family:
            reason_parts.append("shared_service")
        reason_parts.append(f"rel_rmse={metrics['rel_rmse']:.2f}")
        reason_parts.append(f"abs_rmse={metrics['abs_rmse']:.2f}")
        reason_parts.append(f"max_diff={metrics['max_diff']:.2f}")
        reason_parts.append(f"common_scanners={metrics['common_scanners']}")
        return ",".join(reason_parts)

    def _stable_known_target_for_association(self, track: DeviceTrack) -> bool:
        if not track.known_classes():
            return False

        strongest = track.strongest_rssi_value()
        if strongest is None:
            strongest = track.last_known_strongest_rssi_value()

        if track.is_outside_stable_source():
            return True

        cls_text = " ".join(sorted(track.known_classes())).lower()
        if any(x in cls_text for x in ("microsoft", "laptop", "tondo", "beacon")):
            return True

        # A stable known track is not automatically infrastructure. A phone MFG
        # identity can become stable during a long test. Block only very strong
        # fixed-looking targets such as the TV around -40 dBm.
        if track.is_stable_device():
            if strongest is not None and strongest >= CROSS_PERSONALITY_STABLE_STRONG_RSSI_DBM:
                return True
            return False

        if (
            strongest is not None and strongest >= CROSS_PERSONALITY_STABLE_STRONG_RSSI_DBM and
            track.age_sec() >= CROSS_PERSONALITY_STABLE_MIN_AGE_SEC and
            track.packet_count >= CROSS_PERSONALITY_STABLE_MIN_PACKETS
        ):
            return True

        return False

    def _cross_personality_association(self, a: DeviceTrack, b: DeviceTrack) -> Optional[Dict[str, Any]]:
        """
        Cautious physical association, not a hard merge.

        This connects a clean mobile-service identity to a known/manufacturer-data
        identity when their behavior is compatible. It keeps the raw PDs visible.
        """
        if not CROSS_PERSONALITY_ASSOCIATIONS_ENABLED:
            return None

        a_summary = a.identity_summary()
        b_summary = b.identity_summary()

        a_known = bool(a_summary["known_classes"])
        b_known = bool(b_summary["known_classes"])
        a_mobile_dom = a_summary["is_mobile_service_dominant"]
        b_mobile_dom = b_summary["is_mobile_service_dominant"]

        # We only want cross-personality association between a mobile-service
        # dominant side and a known/manufacturer-data side.
        if a_mobile_dom == b_mobile_dom:
            return None
        if a_known == b_known:
            # If both already share known evidence, hard merge logic should decide.
            return None

        mobile_track = a if a_mobile_dom else b
        known_track = b if a_mobile_dom else a

        if mobile_track.is_background_mobile_service():
            return None

        if known_track.is_background_mobile_service():
            return None

        if CROSS_PERSONALITY_BLOCK_STABLE_KNOWN_TARGETS and self._stable_known_target_for_association(known_track):
            return None

        ar_raw = effective_scanner_rssi(mobile_track)
        br_raw = effective_scanner_rssi(known_track)
        metrics = rssi_fingerprint_metrics(ar_raw, br_raw)

        if metrics["common_scanners"] < MIN_COMMON_SCANNERS_STRONG_MATCH:
            return None
        if metrics["rel_rmse"] is None or metrics["abs_rmse"] is None or metrics["max_diff"] is None:
            return None
        if metrics["rel_rmse"] > CROSS_PERSONALITY_MAX_REL_RMSE_DB:
            return None
        if metrics["abs_rmse"] > CROSS_PERSONALITY_MAX_ABS_RMSE_DB:
            return None
        if metrics["max_diff"] > CROSS_PERSONALITY_MAX_PER_SCANNER_DIFF_DB:
            return None
        if CROSS_PERSONALITY_REQUIRE_SAME_STRONGEST and not metrics["same_strongest"]:
            return None
        if CROSS_PERSONALITY_REQUIRE_TOP2_OVERLAP and not metrics["top2_overlap"]:
            return None

        # Time evidence:
        # Active cross-personality association should overlap in time. For old
        # memory tracks, allow a short handoff gap only; otherwise stale weak
        # mobile-service fragments can falsely associate with a later known track.
        overlap = time_overlap_ratio(a.first_seen_mono, a.last_seen_mono, b.first_seen_mono, b.last_seen_mono)
        last_seen_gap = abs(a.last_seen_mono - b.last_seen_mono)
        a_active = a.presence_state() == "ACTIVE"
        b_active = b.presence_state() == "ACTIVE"

        if a_active and b_active and overlap < CROSS_PERSONALITY_ACTIVE_MIN_TIME_OVERLAP:
            return None

        if not (a_active and b_active):
            if overlap < 0.20 and last_seen_gap > CROSS_PERSONALITY_MEMORY_MAX_LAST_SEEN_GAP_SEC:
                return None

        if last_seen_gap > IDENTITY_MEMORY_SEC:
            return None

        # Large absolute offsets are normal between different BLE personalities
        # from the same phone, but only if their relative scanner shape is strong.
        if (
            metrics["max_diff"] is not None and
            metrics["max_diff"] > CROSS_PERSONALITY_HIGH_ABS_DIFF_DB and
            metrics["rel_rmse"] > CROSS_PERSONALITY_HIGH_ABS_REQUIRE_REL_RMSE_DB
        ):
            return None

        confidence = 0.45
        confidence += max(0.0, 0.20 * (1.0 - min(metrics["rel_rmse"], CROSS_PERSONALITY_MAX_REL_RMSE_DB) / CROSS_PERSONALITY_MAX_REL_RMSE_DB))
        confidence += max(0.0, 0.15 * (1.0 - min(metrics["abs_rmse"], CROSS_PERSONALITY_MAX_ABS_RMSE_DB) / CROSS_PERSONALITY_MAX_ABS_RMSE_DB))
        confidence += 0.10 if metrics["same_strongest"] else 0.0
        confidence += 0.05 if metrics["top2_overlap"] else 0.0
        confidence += 0.05 if overlap > 0.20 else 0.0
        confidence = min(0.99, confidence)

        if confidence < CROSS_PERSONALITY_MIN_CONFIDENCE:
            return None

        return {
            "other_uid": known_track.uid if mobile_track.uid == a.uid else mobile_track.uid,
            "mobile_uid": mobile_track.uid,
            "known_uid": known_track.uid,
            "confidence": round(confidence, 3),
            "reason": "cross_personality_association",
            "rel_rmse": rounded_metric(metrics["rel_rmse"]),
            "abs_rmse": rounded_metric(metrics["abs_rmse"]),
            "max_diff": rounded_metric(metrics["max_diff"]),
            "common_scanners": metrics["common_scanners"],
            "same_strongest": metrics["same_strongest"],
            "top2_overlap": metrics["top2_overlap"],
            "time_overlap_ratio": round(overlap, 3),
        }

    def _association_map_locked(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Build cross-personality associations.

        V2 policy:
          - Evaluate all candidate associations.
          - For each mobile-service-dominant track, keep only the clear best
            known-side association.
          - If the best and runner-up are too close, mark the best as ambiguous
            instead of presenting several unrelated devices as likely matches.
        """
        raw_by_mobile: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        final_associations: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        uids = list(self.tracks.keys())

        for i in range(len(uids)):
            for j in range(i + 1, len(uids)):
                a = self.tracks.get(uids[i])
                b = self.tracks.get(uids[j])
                if a is None or b is None:
                    continue
                assoc = self._cross_personality_association(a, b)
                if not assoc:
                    continue
                raw_by_mobile[assoc["mobile_uid"]].append(dict(assoc))

        for mobile_uid, candidates in raw_by_mobile.items():
            candidates.sort(key=lambda item: item.get("confidence", 0.0), reverse=True)
            if not candidates:
                continue

            best = dict(candidates[0])
            second = candidates[1] if len(candidates) > 1 else None
            best_conf = float(best.get("confidence", 0.0))
            second_conf = float(second.get("confidence", 0.0)) if second else 0.0
            margin = best_conf - second_conf

            ambiguous = False
            if CROSS_PERSONALITY_BEST_ONLY:
                if best_conf < CROSS_PERSONALITY_BEST_MIN_CONFIDENCE:
                    continue
                if second is not None and margin < CROSS_PERSONALITY_BEST_MARGIN:
                    ambiguous = True

            best["ambiguous"] = ambiguous
            best["runner_up_uid"] = second.get("known_uid") if second else None
            best["runner_up_confidence"] = round(second_conf, 3) if second else None
            best["confidence_margin"] = round(margin, 3) if second else None
            best["candidate_count"] = len(candidates)

            known_uid = best.get("known_uid")
            if not known_uid or mobile_uid not in self.tracks or known_uid not in self.tracks:
                continue

            assoc_for_mobile = dict(best)
            assoc_for_mobile["other_uid"] = known_uid
            assoc_for_known = dict(best)
            assoc_for_known["other_uid"] = mobile_uid

            final_associations[mobile_uid].append(assoc_for_mobile)
            final_associations[known_uid].append(assoc_for_known)

        for uid in final_associations:
            final_associations[uid].sort(key=lambda item: item.get("confidence", 0.0), reverse=True)
            final_associations[uid] = final_associations[uid][:3]

        return final_associations

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
                    "display_class": DISPLAY_CLASS_MOBILE_CANDIDATE if alias.has_mobile_service_data() else DISPLAY_CLASS_UNKNOWN_CANDIDATE,
                    "device_type": DEVICE_TYPE_MOBILE if alias.has_mobile_service_data() else DEVICE_TYPE_UNKNOWN,
                    "movement_state": "STILL_OR_UNKNOWN",
                    "rf_motion_evidence": False,
                    "rf_motion_reason": "alias_warming",
                    "classification_confidence": "LOW",
                    "classification_reason": "alias_warming",
                    "grid_display_eligible": False,
                    "region_hint": "UNKNOWN_REGION",
                    "region_confidence": "NONE",
                    "region_scanners": [],
                    "region_reason": "alias_warming",
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

        display_info = track.display_classification()
        region_info = region_hint_for_track(track)

        return {
            "uid": track.uid,
            "status": "CONFIRMED" if confirmed else "CANDIDATE",
            "dna": track.dna(),
            "physical_label": track.label(),
            "confirm_reason": reason,
            "presence_state": track.presence_state(),
            "device_role": track.device_role(),
            "display_class": display_info.get("display_class"),
            "device_type": display_info.get("device_type"),
            "movement_state": display_info.get("movement_state"),
            "rf_motion_evidence": display_info.get("rf_motion_evidence", False),
            "rf_motion_reason": display_info.get("rf_motion_reason", ""),
            "classification_confidence": display_info.get("classification_confidence"),
            "classification_reason": display_info.get("classification_reason"),
            "grid_display_eligible": display_info.get("grid_display_eligible", False),
            "region_hint": region_info.get("region_hint"),
            "region_confidence": region_info.get("region_confidence"),
            "region_scanners": region_info.get("region_scanners", []),
            "region_reason": region_info.get("region_reason"),
            "phone_likelihood": round(track.phone_likelihood_score(), 2),
            "outside_likelihood": round(track.outside_likelihood_score(), 2),
            "mobile_service_score": round(track.mobile_service_score(), 2),
            "mobile_service_presence": track.mobile_service_presence(),
            "mobile_service_uuids": sorted(list(track.mobile_service_uuids or mobile_service_uuids_from_sigs(track.payload_sigs))),
            "mobile_service_reason": track.mobile_service_reason(),
            "background_mobile_service_score": round(track.background_mobile_service_score(), 2),
            "is_background_mobile_service": track.is_background_mobile_service(),
            "background_mobile_reason": track.background_mobile_reason(),
            "weak_flat_background_score": round(track.weak_flat_background_score(), 2),
            "is_weak_flat_background": track.is_weak_flat_background(),
            "weak_flat_background_reason": track.weak_flat_background_reason(),
            "strongest_margin_db": rounded_metric(track.strongest_margin_db()),
            "location_confidence": track.location_confidence()[0],
            "location_reason": track.location_confidence()[1],
            "pollution_suspect": track.pollution_suspect()[0],
            "pollution_reason": track.pollution_suspect()[1],
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

        incoming_mfg_ids = set(alias.mfg_ids) if alias is not None else concrete_mfg_ids_from_parsed(parsed)
        if has_manufacturer_conflict(track.mfg_ids, incoming_mfg_ids):
            self.rejected_track_expansion += 1
            return float("inf")

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
        has_direct_low_level_proof = has_same_alias or has_same_mac or has_same_payload
        incoming_family_key = self._payload_family_key(payload_sig)
        has_same_payload_family = incoming_family_key in {self._payload_family_key(sig) for sig in track.payload_sigs}
        has_strict_polluted_continuity = has_same_alias or (has_same_mac and has_same_payload_family)

        # Do not let already-polluted tracks continue absorbing unrelated aliases.
        # V4: a polluted/debug PD is frozen unless exact same MAC + same payload
        # family proves this is the same source/personality. Same CRC alone or
        # RSSI similarity alone is not enough.
        if track.pollution_suspect()[0] and not has_strict_polluted_continuity:
            self.rejected_track_expansion += 1
            return float("inf")

        if alias_mobile and track_known and not has_direct_low_level_proof:
            self.rejected_track_expansion += 1
            return float("inf")

        if track.has_mobile_service_data() and alias_known and not has_direct_low_level_proof:
            self.rejected_track_expansion += 1
            return float("inf")

        if alias_mobile and (track.is_stable_device() or track.is_outside_stable_source() or track.is_weak_flat_background()) and not has_direct_low_level_proof:
            self.rejected_track_expansion += 1
            return float("inf")

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

            if mobile_to_known_guard and not has_direct_low_level_proof:
                # Clean/mobile-service aliases should not be absorbed into known
                # manufacturer/class tracks using RSSI alone. If low-level evidence
                # exists, the track-level identity-continuity merge will handle it;
                # otherwise the snapshot layer can expose a cautious association.
                self.rejected_track_expansion += 1
                return float("inf")

            if alias_mobile and track_mobile and not mobile_to_known_guard:
                # First check same UUID/family RSSI, then the older whole-track
                # RSSI gate. This prevents FCF1/FEF3/FD5A chain-merges.
                if not self._mobile_service_alias_family_allowed(
                    track, alias, parsed, incoming_rssi, has_strict_polluted_continuity
                ):
                    self.rejected_track_expansion += 1
                    return float("inf")
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

                reason = self._pending_merge_reason or "generic_merge"
                self._merge_tracks_locked(a_uid, b_uid, reason=reason)

    def _should_merge_tracks(self, a: DeviceTrack, b: DeviceTrack) -> bool:
        self._pending_merge_reason = ""
        a_known = a.known_classes()
        b_known = b.known_classes()

        if has_manufacturer_conflict(a.mfg_ids, b.mfg_ids):
            self.rejected_track_expansion += 1
            return False

        if STRICT_METADATA_CONFLICTS and a_known and b_known:
            if a_known.isdisjoint(b_known):
                return False

        a_mobile = a.is_mobile_service_data_like() or a.has_mobile_service_data()
        b_mobile = b.is_mobile_service_data_like() or b.has_mobile_service_data()

        a_summary_pre = a.identity_summary()
        b_summary_pre = b.identity_summary()
        shared_crc_pre = a_summary_pre.get("payload_crc_set", set()) & b_summary_pre.get("payload_crc_set", set())
        direct_track_proof = bool((a.macs & b.macs) or shared_crc_pre)
        same_mac_and_payload_family = self._same_mac_and_shared_payload_family(a, b)

        if (a.pollution_suspect()[0] or b.pollution_suspect()[0]) and not same_mac_and_payload_family:
            self.rejected_track_expansion += 1
            return False

        # Mobile-service tracks are only allowed to merge when their matching
        # mobile-service families are spatially compatible. This blocks the
        # PD_006-style FCF1/FEF3/FD5A chain bucket before it becomes polluted.
        if a_mobile and b_mobile and not self._mobile_service_track_family_merge_allowed(a, b):
            self.rejected_track_expansion += 1
            return False

        if (a_mobile != b_mobile) and (a_known or b_known) and not direct_track_proof:
            self.rejected_track_expansion += 1
            return False

        if (a_mobile and b_mobile and (a_known or b_known)):
            if (a_summary_pre.get("is_mixed") or b_summary_pre.get("is_mixed")) and not direct_track_proof:
                self.rejected_track_expansion += 1
                return False

        # Safe generic identity-continuity merge. This runs before the older
        # mobile/known policy because mixed labels can hide shared low-level
        # identity evidence.
        continuity_reason = self._identity_continuity_merge_reason(a, b)
        if continuity_reason:
            self._pending_merge_reason = continuity_reason
            return True

        # Same MAC means two payload families from the same physical BLE address.
        if a.macs & b.macs:
            self._pending_merge_reason = "same_mac"
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

        ar_raw = effective_scanner_rssi(a)
        br_raw = effective_scanner_rssi(b)
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
            if known_side.known_classes() and not known_side.is_mobile_service_data_like():
                # Do not hard-merge clean mobile-service and known/manufacturer
                # personalities by RSSI alone. The identity-continuity path above
                # already accepted safe cases with shared MFG/CRC/service evidence.
                # Remaining cases are exposed as associations in /api/devices.
                self.rejected_track_expansion += 1
                return False
        elif a_mobile and b_mobile:
            if not self._mobile_service_track_family_merge_allowed(a, b):
                self.rejected_track_expansion += 1
                return False
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

        self._pending_merge_reason = (
            f"rssi_merge,rel_rmse={rel:.2f},abs_rmse={absr:.2f},"
            f"max_diff={max_diff:.2f},common_scanners={len(common)}"
        )
        return True


    def _merge_tracks_locked(self, keep_uid: str, drop_uid: str, reason: str = "merge") -> None:
        if keep_uid not in self.tracks or drop_uid not in self.tracks:
            return

        keep = self.tracks[keep_uid]
        drop = self.tracks[drop_uid]

        merge_event = {
            "kept_uid": keep_uid,
            "dropped_uid": drop_uid,
            "reason": reason,
            "time_mono": round(time.monotonic(), 3),
            "keep_label_before": keep.label(),
            "drop_label_before": drop.label(),
        }

        keep_known = keep.known_classes()
        drop_known = drop.known_classes()
        if STRICT_METADATA_CONFLICTS and keep_known and drop_known and keep_known.isdisjoint(drop_known):
            self.rejected_class_conflicts += 1
            return

        keep_summary = keep.identity_summary()
        drop_summary = drop.identity_summary()
        shared_crc = keep_summary.get("payload_crc_set", set()) & drop_summary.get("payload_crc_set", set())
        direct_track_proof = bool((keep.macs & drop.macs) or shared_crc)
        same_mac_and_payload_family = self._same_mac_and_shared_payload_family(keep, drop)
        keep_mobile = keep.has_mobile_service_data()
        drop_mobile = drop.has_mobile_service_data()

        if (keep.pollution_suspect()[0] or drop.pollution_suspect()[0]) and not same_mac_and_payload_family:
            self.rejected_track_expansion += 1
            return

        if keep_mobile and drop_mobile and not self._mobile_service_track_family_merge_allowed(keep, drop):
            self.rejected_track_expansion += 1
            return

        if (keep_mobile != drop_mobile) and (keep_known or drop_known) and not direct_track_proof:
            self.rejected_track_expansion += 1
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

        for sig, by_scanner in drop.payload_rssi_vals.items():
            for scanner, vals in by_scanner.items():
                for rssi_val in vals:
                    keep._update_payload_rssi(sig, scanner, rssi_val)

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
        for v in getattr(drop, "motion_rssi_maps", []):
            keep.motion_rssi_maps.append(dict(v))
        for v in getattr(drop, "motion_slice_packet_counts", []):
            keep.motion_slice_packet_counts.append(v)
        keep.burst_count += drop.burst_count
        keep.confirmed = keep.is_confirmed()

        keep.merge_history.append(merge_event)
        self.last_merge_events.append(merge_event)

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
            association_map = self._association_map_locked()
            tracks = []
            weak_memory_tracks = []
            for uid, t in sorted(self.tracks.items()):
                confirmed, confirm_reason = t.confirm_quality()
                strongest_rssi_val = t.strongest_rssi_value()
                top2_avg_rssi_val = t.top2_avg_rssi_value()

                identity_summary = t.identity_summary()
                display_info = t.display_classification()
                region_info = region_hint_for_track(t)
                polluted, pollution_reason = t.pollution_suspect()
                weak_inactive_candidate = (
                    HIDE_WEAK_INACTIVE_CANDIDATES_IN_MAIN_API and
                    not confirmed and
                    t.presence_state() == "INACTIVE" and
                    t.packet_count <= WEAK_INACTIVE_MAX_PACKETS_FOR_HIDE and
                    not t.is_localizable()
                )

                row = {
                    "uid": uid,
                    "status": "CONFIRMED" if confirmed else "CANDIDATE",
                    "label": t.label(),
                    "identity_summary": identity_summary_for_api(identity_summary),
                    "associated_tracks": association_map.get(uid, []),
                    "merge_history": list(t.merge_history),
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
                    "display_class": display_info.get("display_class"),
                    "device_type": display_info.get("device_type"),
                    "movement_state": display_info.get("movement_state"),
                    "rf_motion_evidence": display_info.get("rf_motion_evidence", False),
                    "rf_motion_reason": display_info.get("rf_motion_reason", ""),
                    "classification_confidence": display_info.get("classification_confidence"),
                    "classification_reason": display_info.get("classification_reason"),
                    "grid_display_eligible": display_info.get("grid_display_eligible", False),
                    "region_hint": region_info.get("region_hint"),
                    "region_confidence": region_info.get("region_confidence"),
                    "region_scanners": region_info.get("region_scanners", []),
                    "region_reason": region_info.get("region_reason"),
                    "phone_likelihood": round(t.phone_likelihood_score(), 2),
                    "outside_likelihood": round(t.outside_likelihood_score(), 2),
                    "mobile_service_score": round(t.mobile_service_score(), 2),
                    "mobile_service_presence": t.mobile_service_presence(),
                    "mobile_service_uuids": sorted(list(t.mobile_service_uuids or mobile_service_uuids_from_sigs(t.payload_sigs))),
                    "mobile_service_packet_count": t.mobile_service_packet_count,
                    "mobile_service_packet_ratio": round(t.mobile_service_packet_ratio(), 3),
                    "mobile_service_reason": t.mobile_service_reason(),
                    "background_mobile_service_score": round(t.background_mobile_service_score(), 2),
                    "is_background_mobile_service": t.is_background_mobile_service(),
                    "background_mobile_reason": t.background_mobile_reason(),
                    "weak_flat_background_score": round(t.weak_flat_background_score(), 2),
                    "is_weak_flat_background": t.is_weak_flat_background(),
                    "weak_flat_background_reason": t.weak_flat_background_reason(),
                    "strongest_margin_db": rounded_metric(t.strongest_margin_db()),
                    "location_confidence": t.location_confidence()[0],
                    "location_reason": t.location_confidence()[1],
                    "pollution_suspect": polluted,
                    "pollution_reason": pollution_reason,
                    "polluted_family_debug": t.polluted_family_debug() if polluted else {"families": [], "divergent_pairs": []},
                    "localizable": t.is_localizable(),
                    "burst_count": t.burst_count,
                    "last_burst_age_sec": round(t.last_burst_age_sec(), 2),
                    "avg_burst_duration_sec": round(t.avg_burst_duration_sec(), 2),
                    "avg_packets_per_burst": round(t.avg_packets_per_burst(), 2),
                    "recent_burst_scanners": sorted(list(t.recent_burst_scanners())),
                    "motion_slice_count": t.motion_slice_count(),
                    "motion_slice_packets_current": t.motion_slice_packets,
                    "motion_slice_history_packets": list(t.motion_slice_packet_counts),
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
                }

                # Mobile-only grid/block localization. This uses the calibrated
                # probabilistic RSSI fingerprint model and adds location_block /
                # location_probability fields to the API row.
                row.update(localize_track_to_grid(t))

                if weak_inactive_candidate:
                    weak_memory_tracks.append(row)
                else:
                    tracks.append(row)

            return {
                "enabled": TRACKER_ENABLED,
                "localization": localization_status_for_api(),
                "tracks": tracks,
                "weak_memory_tracks": weak_memory_tracks,
                "num_tracks": len(tracks),
                "num_weak_memory_tracks": len(weak_memory_tracks),
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
                "background_mobile_tracks": sum(1 for t in self.tracks.values() if t.is_background_mobile_service()),
                "pollution_suspect_tracks": sum(1 for t in self.tracks.values() if t.pollution_suspect()[0]),
                "weak_flat_background_tracks": sum(1 for t in self.tracks.values() if t.is_weak_flat_background()),
                "display_class_counts": dict(sorted({
                    cls: sum(1 for t in self.tracks.values() if t.display_classification().get("display_class") == cls)
                    for cls in {
                        DISPLAY_CLASS_NEARBY_MOBILE,
                        DISPLAY_CLASS_MOBILE_CANDIDATE,
                        DISPLAY_CLASS_MOBILE_WEAK,
                        DISPLAY_CLASS_MOBILE_BACKGROUND,
                        DISPLAY_CLASS_MOBILE_MIXED,
                        DISPLAY_CLASS_STATIONARY_IN_ROOM,
                        DISPLAY_CLASS_STATIONARY_OUTSIDE,
                        DISPLAY_CLASS_BEACON,
                        DISPLAY_CLASS_UNKNOWN_CANDIDATE,
                        DISPLAY_CLASS_POLLUTION_SUSPECT,
                        DISPLAY_CLASS_HIDDEN_BACKGROUND,
                        DISPLAY_CLASS_CALIBRATION,
                    }
                }.items())),
                "last_merge_events": list(self.last_merge_events),
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
                    "identity_continuity_max_rel_rmse_db": IDENTITY_CONTINUITY_MAX_REL_RMSE_DB,
                    "identity_continuity_max_abs_rmse_db": IDENTITY_CONTINUITY_MAX_ABS_RMSE_DB,
                    "identity_continuity_max_per_scanner_diff_db": IDENTITY_CONTINUITY_MAX_PER_SCANNER_DIFF_DB,
                    "cross_personality_associations_enabled": CROSS_PERSONALITY_ASSOCIATIONS_ENABLED,
                    "cross_personality_min_confidence": CROSS_PERSONALITY_MIN_CONFIDENCE,
                    "cross_personality_active_min_time_overlap": CROSS_PERSONALITY_ACTIVE_MIN_TIME_OVERLAP,
                    "cross_personality_memory_max_last_seen_gap_sec": CROSS_PERSONALITY_MEMORY_MAX_LAST_SEEN_GAP_SEC,
                    "cross_personality_best_only": CROSS_PERSONALITY_BEST_ONLY,
                    "cross_personality_best_min_confidence": CROSS_PERSONALITY_BEST_MIN_CONFIDENCE,
                    "cross_personality_best_margin": CROSS_PERSONALITY_BEST_MARGIN,
                    "background_mobile_role": BACKGROUND_MOBILE_ROLE,
                    "background_mobile_min_age_sec": BACKGROUND_MOBILE_MIN_AGE_SEC,
                    "background_mobile_min_packets": BACKGROUND_MOBILE_MIN_PACKETS,
                    "background_mobile_max_strongest_rssi_dbm": BACKGROUND_MOBILE_MAX_STRONGEST_RSSI_DBM,
                    "background_mobile_max_top2_avg_rssi_dbm": BACKGROUND_MOBILE_MAX_TOP2_AVG_RSSI_DBM,
                    "background_mobile_max_location_margin_db": BACKGROUND_MOBILE_MAX_LOCATION_MARGIN_DB,
                    "location_high_margin_db": LOCATION_HIGH_MARGIN_DB,
                    "location_medium_margin_db": LOCATION_MEDIUM_MARGIN_DB,
                    "weak_flat_background_role": WEAK_FLAT_BACKGROUND_ROLE,
                    "weak_flat_min_scanners": WEAK_FLAT_MIN_SCANNERS,
                    "weak_flat_scanner_rssi_dbm": WEAK_FLAT_SCANNER_RSSI_DBM,
                    "weak_flat_min_weak_scanners": WEAK_FLAT_MIN_WEAK_SCANNERS,
                    "weak_flat_max_strongest_rssi_dbm": WEAK_FLAT_MAX_STRONGEST_RSSI_DBM,
                    "weak_flat_max_top2_avg_rssi_dbm": WEAK_FLAT_MAX_TOP2_AVG_RSSI_DBM,
                    "weak_flat_max_margin_db": WEAK_FLAT_MAX_MARGIN_DB,
                    "weak_flat_block_confirmation": WEAK_FLAT_BLOCK_CONFIRMATION,
                    "pollution_rel_rmse_db": POLLUTION_REL_RMSE_DB,
                    "pollution_abs_rmse_db": POLLUTION_ABS_RMSE_DB,
                    "pollution_max_scanner_diff_db": POLLUTION_MAX_SCANNER_DIFF_DB,
                    "hide_weak_inactive_candidates_in_main_api": HIDE_WEAK_INACTIVE_CANDIDATES_IN_MAIN_API,
                    "weak_inactive_max_packets_for_hide": WEAK_INACTIVE_MAX_PACKETS_FOR_HIDE,
                    "strict_manufacturer_id_conflicts": STRICT_MANUFACTURER_ID_CONFLICTS,
                    "phone_like_block_weak_rssi_dbm": PHONE_LIKE_BLOCK_WEAK_RSSI_DBM,
                    "phone_like_block_low_margin_db": PHONE_LIKE_BLOCK_LOW_MARGIN_DB,
                    "known_dominant_background_override_ratio": KNOWN_DOMINANT_BACKGROUND_OVERRIDE_RATIO,
                    "display_nearby_mobile_strong_rssi_dbm": DISPLAY_NEARBY_MOBILE_STRONG_RSSI_DBM,
                    "display_nearby_mobile_top2_avg_dbm": DISPLAY_NEARBY_MOBILE_TOP2_AVG_DBM,
                    "display_nearby_mobile_min_margin_db": DISPLAY_NEARBY_MOBILE_MIN_MARGIN_DB,
                    "display_nearby_mobile_min_scanners": DISPLAY_NEARBY_MOBILE_MIN_SCANNERS,
                    "display_nearby_mobile_min_packets": DISPLAY_NEARBY_MOBILE_MIN_PACKETS,
                    "motion_slice_sec": MOTION_SLICE_SEC,
                    "motion_slice_min_packets": MOTION_SLICE_MIN_PACKETS,
                    "motion_slice_min_scanners": MOTION_SLICE_MIN_SCANNERS,
                    "motion_slice_history": MOTION_SLICE_HISTORY,
                    "localization_grid_display_classes": sorted(list(LOCALIZATION_GRID_DISPLAY_CLASSES)),
                    "localization_grid_show_best_candidate_for_test": LOCALIZATION_GRID_SHOW_BEST_CANDIDATE_FOR_TEST,
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

def _safe_grid_block(value: Any) -> str:
    """Return a valid 3x3 grid block number as a string, or empty string."""
    try:
        block = int(str(value or "").strip())
    except Exception:
        return ""

    if 1 <= block <= 9:
        return str(block)

    return ""


def _new_calibration_session_id(grid_block: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_block = _safe_grid_block(grid_block) or "UNK"
    return f"calib_{stamp}_block_{safe_block}"


def _rssi_summary(samples: List[int]) -> Dict[str, Any]:
    vals = [safe_int(v, 0) for v in samples]
    n = len(vals)
    if n == 0:
        return {
            "n_samples": 0,
            "mean_rssi": "",
            "median_rssi": "",
            "std_rssi": "",
            "min_rssi": "",
            "max_rssi": "",
            "p90_rssi": "",
            "top_half_mean_rssi": "",
        }

    vals_sorted = sorted(vals)
    mean = sum(vals) / n

    mid = n // 2
    if n % 2:
        median = float(vals_sorted[mid])
    else:
        median = (vals_sorted[mid - 1] + vals_sorted[mid]) / 2.0

    if n > 1:
        var = sum((v - mean) ** 2 for v in vals) / (n - 1)
        std = math.sqrt(var)
    else:
        std = 0.0

    # RSSI is negative, so the 90th percentile means a strong-RSSI percentile.
    # Example: -45 dBm is stronger than -80 dBm.
    p90_index = min(n - 1, max(0, math.ceil(0.90 * n) - 1))
    p90 = vals_sorted[p90_index]

    strongest_first = sorted(vals, reverse=True)
    keep_n = max(1, n // 2)
    top_half_mean = sum(strongest_first[:keep_n]) / keep_n

    return {
        "n_samples": n,
        "mean_rssi": round(mean, 3),
        "median_rssi": round(median, 3),
        "std_rssi": round(std, 3),
        "min_rssi": min(vals),
        "max_rssi": max(vals),
        "p90_rssi": p90,
        "top_half_mean_rssi": round(top_half_mean, 3),
    }


def _append_csv_row(filename: str, header: List[str], rows: List[List[Any]]) -> str:
    path = os.path.join(BASE_DIR, filename)
    file_exists = os.path.isfile(path) and os.path.getsize(path) > 0

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(rows)

    return path


def print_calibration_progress(force: bool = False) -> None:
    now = time.time()
    if not force and now - calib_state.get("last_progress_print", 0.0) < 1.0:
        return

    calib_state["last_progress_print"] = now

    grid_block = calib_state.get("grid_block", "")
    print("\n--- Grid Calibration Progress ---")
    print(f"Grid block: {grid_block} | target samples per scanner: {SAMPLES_PER_SCANNER}")
    print(f"{'Scanner ID':<12} | {'Samples':<12} | {'Complete':<8}")
    print("-" * 42)

    seen_scanners = sorted(calib_state["buckets"].keys(), key=str)
    if not seen_scanners:
        print("Waiting for EldarCalib packets...")

    for s_id in seen_scanners:
        rows = calib_state["buckets"].get(s_id, [])
        count = len(rows)
        complete = "yes" if count >= SAMPLES_PER_SCANNER else "no"
        print(f"Scanner {str(s_id):<3} | {count:3}/{SAMPLES_PER_SCANNER:<6} | {complete:<8}")

    print("Channel labels are ignored for calibration because they are not real BLE RF channel data.")
    print("-" * 42)


def is_calibration_packet(mac: str, payload: str, parsed: Dict[str, Any]) -> bool:
    """
    Calibration matching is intentionally simple:
    accept only packets whose decoded BLE name contains CALIBRATION_TARGET
    (currently "EldarCalib"). No manual MAC preselection is required.
    """
    name = str(parsed.get("name", "Unknown") or "Unknown").lower()
    return CALIBRATION_TARGET.lower() in name


def check_calib_completion_locked() -> None:
    """
    Must be called with calib_lock held.

    Calibration completion is based only on per-scanner raw RSSI samples. The
    old channel labels are intentionally ignored.
    """
    scanners_to_check = [
        s_id for s_id, data in active_scanners.items()
        if time.time() - data.get("last_seen", 0) < 30
    ]

    if not scanners_to_check:
        return

    for s_id in scanners_to_check:
        if len(calib_state["buckets"].get(s_id, [])) < SAMPLES_PER_SCANNER:
            return

    if calib_state["active"]:
        print("\n[!] Grid calibration complete.")
        save_calibration_results_locked()


def save_calibration_results_locked() -> None:
    """
    Must be called with calib_lock held.

    Writes two files:
      1. calibration_raw.csv      -> every raw RSSI measurement
      2. calibration_summary.csv  -> per-scanner summary for the localization model
    """
    session_id = str(calib_state.get("session_id") or _new_calibration_session_id(calib_state.get("grid_block")))
    grid_block = str(calib_state.get("grid_block") or "")

    raw_header = [
        "session_id",
        "grid_block",
        "scanner_id",
        "sample_index",
        "timestamp_local",
        "timestamp_us",
        "rx_ts_us",
        "scanner_tm_us",
        "rssi",
        "mac",
        "payload_sig",
        "name",
        "mfg_id",
        "mfg_name",
        "adv_len",
        "channel_label",
        "channel_label_is_real_ble_channel",
    ]
    raw_rows = []

    summary_header = [
        "session_id",
        "grid_block",
        "scanner_id",
        "n_samples",
        "mean_rssi",
        "median_rssi",
        "std_rssi",
        "min_rssi",
        "max_rssi",
        "p90_rssi",
        "top_half_mean_rssi",
        "complete",
        "channel_label_is_real_ble_channel",
    ]
    summary_rows = []

    for s_id in sorted(calib_state["buckets"].keys(), key=str):
        rows = list(calib_state["buckets"].get(s_id, []))
        rssi_values = [safe_int(row.get("rssi", 0), 0) for row in rows]
        stats_row = _rssi_summary(rssi_values)
        complete = 1 if stats_row["n_samples"] >= SAMPLES_PER_SCANNER else 0

        for row in rows:
            raw_rows.append([
                session_id,
                grid_block,
                s_id,
                row.get("sample_index", ""),
                row.get("timestamp_local", ""),
                row.get("timestamp_us", ""),
                row.get("rx_ts_us", ""),
                row.get("scanner_tm_us", ""),
                row.get("rssi", ""),
                row.get("mac", ""),
                row.get("payload_sig", ""),
                row.get("name", ""),
                row.get("mfg_id", ""),
                row.get("mfg_name", ""),
                row.get("adv_len", ""),
                row.get("channel_label", ""),
                CHANNEL_LABEL_IS_REAL_BLE_CHANNEL,
            ])

        summary_rows.append([
            session_id,
            grid_block,
            s_id,
            stats_row["n_samples"],
            stats_row["mean_rssi"],
            stats_row["median_rssi"],
            stats_row["std_rssi"],
            stats_row["min_rssi"],
            stats_row["max_rssi"],
            stats_row["p90_rssi"],
            stats_row["top_half_mean_rssi"],
            complete,
            CHANNEL_LABEL_IS_REAL_BLE_CHANNEL,
        ])

    raw_path = _append_csv_row(CALIBRATION_RAW_CSV, raw_header, raw_rows)
    summary_path = _append_csv_row(CALIBRATION_SUMMARY_CSV, summary_header, summary_rows)

    calib_state["active"] = False
    print(f"[CALIB] Saved raw calibration samples to {raw_path}")
    print(f"[CALIB] Saved calibration summary to {summary_path}")
    print("[CALIB] Use top_half_mean_rssi or median_rssi per scanner for grid-block localization.")

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
        rssi = ev["rssi"]

        if not is_calibration_packet(mac, payload, parsed):
            return

        rows = calib_state["buckets"].setdefault(scanner_id, [])

        if len(rows) >= SAMPLES_PER_SCANNER:
            return

        sample_index = len(rows) + 1
        rows.append({
            "sample_index": sample_index,
            "timestamp_local": datetime.now().isoformat(timespec="milliseconds"),
            "timestamp_us": ev.get("ts", ""),
            "rx_ts_us": ev.get("rx_ts_us", ""),
            "scanner_tm_us": ev.get("scanner_tm_us", ""),
            "rssi": safe_int(rssi, 0),
            "mac": mac,
            "payload_sig": parsed.get("payload_sig", payload_signature(payload)),
            "name": parsed.get("name", "Unknown"),
            "mfg_id": parsed.get("mfg_id", ""),
            "mfg_name": parsed.get("mfg_name", ""),
            "adv_len": parsed.get("adv_len", 0),
            "channel_label": ev.get("channel", ""),
        })

        print_calibration_progress(force=False)
        check_calib_completion_locked()




# ---------------- Processing pipeline ----------------

def enqueue_stream_event_realtime(out: Dict[str, Any]) -> None:
    """
    Queue one event for the live SSE debug stream without allowing old backlog
    to create 30-60 second visual delay.

    The stream is a live display channel, not a guaranteed log. The session JSON
    remains the durable record; this queue should prefer current events.
    """
    dropped_backlog = 0

    if STREAM_REALTIME_DROP_OLD_EVENTS:
        try:
            while data_queue.qsize() > STREAM_MAX_BACKLOG_EVENTS:
                try:
                    data_queue.get_nowait()
                    dropped_backlog += 1
                except queue.Empty:
                    break

            # When heavily backed up, drain more aggressively to the target size.
            if dropped_backlog > 0:
                while data_queue.qsize() > STREAM_DROP_TO_BACKLOG_EVENTS:
                    try:
                        data_queue.get_nowait()
                        dropped_backlog += 1
                    except queue.Empty:
                        break
        except Exception:
            pass

    try:
        data_queue.put_nowait(out)
        with stats_lock:
            stats["streamed_events"] += 1
            if dropped_backlog:
                stats["dropped_queue_realtime_backlog"] += dropped_backlog
    except queue.Full:
        # Last resort: drop one old item and insert the current event. This keeps
        # the stream live even if the GUI temporarily stopped consuming.
        try:
            data_queue.get_nowait()
            data_queue.put_nowait(out)
            with stats_lock:
                stats["streamed_events"] += 1
                stats["dropped_queue_realtime_backlog"] += dropped_backlog + 1
        except Exception:
            with stats_lock:
                stats["dropped_queue_full"] += 1
                if dropped_backlog:
                    stats["dropped_queue_realtime_backlog"] += dropped_backlog


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
        "display_class": ident.get("display_class", ""),
        "device_type": ident.get("device_type", ""),
        "movement_state": ident.get("movement_state", ""),
        "rf_motion_evidence": ident.get("rf_motion_evidence", False),
        "rf_motion_reason": ident.get("rf_motion_reason", ""),
        "classification_confidence": ident.get("classification_confidence", ""),
        "classification_reason": ident.get("classification_reason", ""),
        "grid_display_eligible": ident.get("grid_display_eligible", False),
        "region_hint": ident.get("region_hint", ""),
        "region_confidence": ident.get("region_confidence", ""),
        "region_scanners": ident.get("region_scanners", []),
        "region_reason": ident.get("region_reason", ""),
        "phone_likelihood": ident.get("phone_likelihood", 0.0),
        "outside_likelihood": ident.get("outside_likelihood", 0.0),
        "mobile_service_score": ident.get("mobile_service_score", 0.0),
        "mobile_service_presence": ident.get("mobile_service_presence", "NONE"),
        "mobile_service_uuids": ident.get("mobile_service_uuids", []),
        "mobile_service_reason": ident.get("mobile_service_reason", ""),
        "background_mobile_service_score": ident.get("background_mobile_service_score", 0.0),
        "is_background_mobile_service": ident.get("is_background_mobile_service", False),
        "background_mobile_reason": ident.get("background_mobile_reason", ""),
        "weak_flat_background_score": ident.get("weak_flat_background_score", 0.0),
        "is_weak_flat_background": ident.get("is_weak_flat_background", False),
        "weak_flat_background_reason": ident.get("weak_flat_background_reason", ""),
        "strongest_margin_db": ident.get("strongest_margin_db"),
        "location_confidence": ident.get("location_confidence", ""),
        "location_reason": ident.get("location_reason", ""),
        "pollution_suspect": ident.get("pollution_suspect", False),
        "pollution_reason": ident.get("pollution_reason", ""),
        "localizable": ident.get("localizable", False),
    }

    enqueue_stream_event_realtime(out)


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

    # One calibration run measures exactly one selected 3x3 grid block.
    grid_block = _safe_grid_block(data.get("grid_block"))
    if not grid_block:
        return jsonify({
            "status": "error",
            "message": "grid_block must be an integer from 1 to 9",
        }), 400

    session_id = _new_calibration_session_id(grid_block)

    with calib_lock:
        calib_state.update({
            "active": True,
            "grid_block": grid_block,
            "session_id": session_id,
            "buckets": {},
            "last_progress_print": 0.0,
        })

    target_desc = f'name contains "{CALIBRATION_TARGET}"'
    print(
        f"\n[*] Grid calibration started: block={grid_block} "
        f"session_id={session_id} target={target_desc} "
        f"samples_per_scanner={SAMPLES_PER_SCANNER}"
    )

    return jsonify({
        "status": "Started",
        "grid_block": grid_block,
        "session_id": session_id,
        "target": CALIBRATION_TARGET,
        "samples_per_scanner": SAMPLES_PER_SCANNER,
        "channel_labels_ignored": True,
    }), 200


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
        stats["tracker_background_mobile_tracks"] = tracker_snapshot.get("background_mobile_tracks", 0)
        stats["tracker_pollution_suspect_tracks"] = tracker_snapshot.get("pollution_suspect_tracks", 0)
        stats["tracker_weak_flat_background_tracks"] = tracker_snapshot.get("weak_flat_background_tracks", 0)

    return jsonify(snapshot)


@app.route("/api/localization", methods=["GET"])
def get_localization():
    tracker_snapshot = device_tracker.snapshot()
    return jsonify(make_json_safe(build_localization_api_payload(tracker_snapshot)))


@app.route("/localization", methods=["GET"])
def localization_dashboard():
    return render_template("localization_dashboard.html")


@app.route("/api/localization/reload", methods=["POST"])
def reload_localization():
    load_localization_fingerprints()
    return jsonify(localization_status_for_api())


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
        realtime_drops = cur.get("dropped_queue_realtime_backlog", 0) - last.get("dropped_queue_realtime_backlog", 0)
        parse_errs = cur["parse_errors"] - last["parse_errors"]

        last = cur

        alive = [
            s for s, d in active_scanners.items()
            if time.time() - d.get("last_seen", 0) < 30
        ]

        tracker_snapshot = device_tracker.snapshot()
        n_tracks = tracker_snapshot["num_tracks"]
        n_confirmed = tracker_snapshot["num_confirmed"]

        if din or dproc or dout or drops or realtime_drops or parse_errs:
            print(
                f"[STATS] in={din}/s processed={dproc}/s streamed={dout}/s "
                f"drops={drops}/s realtime_backlog_drops={realtime_drops}/s parse_err={parse_errs}/s "
                f"tracks={n_tracks} confirmed={n_confirmed} "
                f"phone_like={tracker_snapshot.get('phone_like_tracks', 0)} "
                f"stable={tracker_snapshot.get('stable_device_tracks', 0)} "
                f"mobile={tracker_snapshot.get('mobile_service_tracks', 0)} "
                f"bg_mobile={tracker_snapshot.get('background_mobile_tracks', 0)} "
                f"weak_flat={tracker_snapshot.get('weak_flat_background_tracks', 0)} "
                f"polluted={tracker_snapshot.get('pollution_suspect_tracks', 0)} "
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
    load_localization_fingerprints()
    my_ip = get_local_ip()
    zc_instance = start_mdns(my_ip, 8000)

    threading.Thread(target=window_processor, daemon=True).start()
    threading.Thread(target=stats_reporter, daemon=True).start()

    try:
        app.run(host="0.0.0.0", port=8000, threaded=True)
    finally:
        zc_instance.unregister_all_services()
        zc_instance.close()
