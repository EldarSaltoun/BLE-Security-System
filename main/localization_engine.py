"""
localization_engine.py

Generic RSSI-shape localization for BLE physical-device (PD) tracks.

Design goals:
- Keep pc_receiver.py focused on packet ingest, grouping, and identity logic.
- Avoid device/manufacturer/payload TX-power bias tables.
- Use fixed scanner coordinates + optional scanner-only calibration bias.
- Localize from the relative RSSI shape across scanners, not from absolute RSSI-to-distance.

Coordinate units: centimeters.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple


# -----------------------------------------------------------------------------
# Fixed scanner geometry, provided by the user.
# Coordinate unit: centimeters.
# x values are negative because the room axis was drawn from right to left.
# -----------------------------------------------------------------------------

SCANNER_POSITIONS_CM: Dict[str, Dict[str, float]] = {
    "1": {"x": 0.0, "y": 56.0, "z": 142.0},
    "2": {"x": 0.0, "y": 532.5, "z": 220.0},
    "3": {"x": -375.0, "y": 557.0, "z": 205.0},
    "4": {"x": -398.0, "y": 63.0, "z": 197.0},
}

# Scanner-only calibration bias. Positive means this scanner reads too strong and
# should be reduced. Keep all zeros until you run a calibration session.
SCANNER_BIAS_DB: Dict[str, float] = {
    "1": 0.0,
    "2": 0.0,
    "3": 0.0,
    "4": 0.0,
}

# Weight sharpness. Lower = position sticks closer to strongest scanner.
# Higher = weaker scanners pull the centroid more.
RSSI_WEIGHT_K_DB = 10.0

# Ignore scanners more than this far below the strongest scanner. This avoids tiny
# numerical weights from very weak scanners pulling the centroid unnecessarily.
RELATIVE_RSSI_FLOOR_DB = -35.0

# Basic gates. These mirror the receiver's logic but live here so localization can
# stay independent and easy to tune.
MIN_LOCALIZATION_SCANNERS = 3
MIN_STRONGEST_RSSI_DBM = -88.0
MIN_TOP2_AVG_RSSI_DBM = -92.0

DISALLOWED_ROLES = {
    "WEAK_FLAT_BACKGROUND",
    "BACKGROUND_MOBILE_SERVICE",
    "OUTSIDE_STABLE",
    "BEACON_LIKE",
}


# -----------------------------------------------------------------------------
# Small utilities
# -----------------------------------------------------------------------------

def _safe_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _round_or_none(value: Optional[float], digits: int = 3) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _sorted_scanners_by_rssi(rssi: Mapping[str, float]) -> list[Tuple[str, float]]:
    return sorted(rssi.items(), key=lambda kv: kv[1], reverse=True)


def _top2_avg(rssi: Mapping[str, float]) -> Optional[float]:
    if not rssi:
        return None
    vals = [v for _, v in _sorted_scanners_by_rssi(rssi)[:2]]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _strongest_margin(rssi: Mapping[str, float]) -> Optional[float]:
    vals = [v for _, v in _sorted_scanners_by_rssi(rssi)]
    if len(vals) < 2:
        return None
    return vals[0] - vals[1]


def _normalize_scanner_id(scanner_id: Any) -> str:
    return str(scanner_id).strip()


def _extract_scanner_rssi(track: Mapping[str, Any]) -> Dict[str, float]:
    raw = track.get("scanner_rssi") or {}
    if not isinstance(raw, Mapping):
        return {}

    out: Dict[str, float] = {}
    for scanner_id, value in raw.items():
        sid = _normalize_scanner_id(scanner_id)
        if sid not in SCANNER_POSITIONS_CM:
            continue
        val = _safe_float(value)
        if val is not None:
            out[sid] = val
    return out


# -----------------------------------------------------------------------------
# Localization gates
# -----------------------------------------------------------------------------

def localizability_reason(track: Mapping[str, Any]) -> Tuple[bool, str]:
    """Return (is_localizable, reason)."""
    if str(track.get("status", "")).upper() != "CONFIRMED":
        return False, "not_confirmed"

    if str(track.get("presence_state", "")).upper() != "ACTIVE":
        return False, "not_active"

    role = str(track.get("device_role", "")).upper()
    if role in DISALLOWED_ROLES:
        return False, f"disallowed_role:{role}"

    if bool(track.get("is_weak_flat_background", False)):
        return False, "weak_flat_background"

    if bool(track.get("is_background_mobile_service", False)):
        return False, "background_mobile_service"

    rssi = _extract_scanner_rssi(track)
    if len(rssi) < MIN_LOCALIZATION_SCANNERS:
        return False, "not_enough_scanners"

    strongest = max(rssi.values()) if rssi else None
    top2 = _top2_avg(rssi)

    if strongest is None or top2 is None:
        return False, "no_valid_rssi"

    if strongest < MIN_STRONGEST_RSSI_DBM and top2 < MIN_TOP2_AVG_RSSI_DBM:
        return False, "weak_rssi"

    return True, "localizable"


# -----------------------------------------------------------------------------
# RSSI-shape localization
# -----------------------------------------------------------------------------

def corrected_rssi(scanner_rssi: Mapping[str, float]) -> Dict[str, float]:
    """Apply scanner-only bias correction."""
    out: Dict[str, float] = {}
    for sid, rssi in scanner_rssi.items():
        bias = SCANNER_BIAS_DB.get(sid, 0.0)
        out[sid] = float(rssi) - float(bias)
    return out


def relative_rssi(corrected: Mapping[str, float]) -> Dict[str, float]:
    """Normalize this PD by its strongest scanner, removing global TX-power offset."""
    if not corrected:
        return {}
    strongest = max(corrected.values())
    return {sid: float(value) - strongest for sid, value in corrected.items()}


def rssi_weights(relative: Mapping[str, float], k_db: float = RSSI_WEIGHT_K_DB) -> Dict[str, float]:
    """Convert relative RSSI values into centroid weights."""
    if not relative:
        return {}
    k = max(1.0, float(k_db))
    weights: Dict[str, float] = {}
    for sid, rel_db in relative.items():
        clipped = max(float(rel_db), RELATIVE_RSSI_FLOOR_DB)
        weights[sid] = 10.0 ** (clipped / k)
    return weights


def weighted_centroid_3d(weights: Mapping[str, float]) -> Optional[Dict[str, float]]:
    total = sum(max(0.0, float(w)) for w in weights.values())
    if total <= 0.0:
        return None

    x = y = z = 0.0
    used = 0
    for sid, weight in weights.items():
        pos = SCANNER_POSITIONS_CM.get(sid)
        if not pos:
            continue
        w = max(0.0, float(weight))
        x += w * pos["x"]
        y += w * pos["y"]
        z += w * pos["z"]
        used += 1

    if used == 0:
        return None

    return {
        "x_cm": x / total,
        "y_cm": y / total,
        "z_cm": z / total,
    }


def confidence_for_track(track: Mapping[str, Any], scanner_rssi: Mapping[str, float]) -> Tuple[str, str]:
    strongest = max(scanner_rssi.values()) if scanner_rssi else None
    margin = _strongest_margin(scanner_rssi)
    top2 = _top2_avg(scanner_rssi)
    scanners = len(scanner_rssi)
    pollution = bool(track.get("pollution_suspect", False))
    role = str(track.get("device_role", "")).upper()

    if strongest is None or margin is None or top2 is None:
        return "LOW", "missing_rssi_or_margin"

    if pollution:
        if strongest >= -75.0 and margin >= 8.0 and scanners >= 4:
            return "MEDIUM", "pollution_suspect_but_strong_clear_margin"
        return "LOW", "pollution_suspect"

    if role.startswith("MIXED"):
        if strongest >= -75.0 and margin >= 8.0 and scanners >= 4:
            return "MEDIUM", "mixed_track_but_strong_clear_margin"
        return "LOW", "mixed_track"

    if strongest >= -75.0 and margin >= 8.0 and scanners >= 4:
        return "HIGH", "strong_rssi_clear_margin"

    if strongest >= -85.0 and margin >= 4.0 and scanners >= 3:
        return "MEDIUM", "moderate_rssi_or_margin"

    return "LOW", "weak_or_ambiguous_rssi_shape"


def localize_track(track: Mapping[str, Any]) -> Dict[str, Any]:
    ok, reason = localizability_reason(track)
    if not ok:
        return {
            "enabled": False,
            "reason": reason,
            "method": "relative_rssi_weighted_centroid_3d",
        }

    raw = _extract_scanner_rssi(track)
    corr = corrected_rssi(raw)
    rel = relative_rssi(corr)
    weights = rssi_weights(rel)
    pos = weighted_centroid_3d(weights)

    if pos is None:
        return {
            "enabled": False,
            "reason": "centroid_failed",
            "method": "relative_rssi_weighted_centroid_3d",
        }

    ordered = _sorted_scanners_by_rssi(corr)
    strongest_sid = ordered[0][0] if ordered else None
    top2_sids = [sid for sid, _ in ordered[:2]]
    confidence, confidence_reason = confidence_for_track(track, corr)

    return {
        "enabled": True,
        "method": "relative_rssi_weighted_centroid_3d",
        "units": "cm",
        "x_cm": _round_or_none(pos["x_cm"], 2),
        "y_cm": _round_or_none(pos["y_cm"], 2),
        "z_cm": _round_or_none(pos["z_cm"], 2),
        "vertical_confidence": "LOW",
        "vertical_note": "z is a weighted estimate from scanner heights; RSSI alone cannot strongly resolve height.",
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "strongest_scanner": strongest_sid,
        "top2_scanners": top2_sids,
        "strongest_margin_db": _round_or_none(_strongest_margin(corr), 3),
        "top2_avg_rssi_dbm": _round_or_none(_top2_avg(corr), 3),
        "scanner_positions_cm": {sid: SCANNER_POSITIONS_CM[sid] for sid in sorted(SCANNER_POSITIONS_CM)},
        "scanner_bias_db": {sid: SCANNER_BIAS_DB.get(sid, 0.0) for sid in sorted(SCANNER_POSITIONS_CM)},
        "corrected_rssi": {sid: _round_or_none(val, 3) for sid, val in sorted(corr.items())},
        "relative_rssi": {sid: _round_or_none(val, 3) for sid, val in sorted(rel.items())},
        "weights": {sid: _round_or_none(val, 6) for sid, val in sorted(weights.items())},
    }


def add_localization_to_tracks(tracks: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out = []
    for track in tracks:
        row = dict(track)
        row["localization"] = localize_track(row)
        out.append(row)
    return out


def add_localization_to_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Attach localization to /api/devices or tracker snapshots."""
    if not isinstance(snapshot, dict):
        return snapshot

    updated = dict(snapshot)
    updated["tracks"] = add_localization_to_tracks(updated.get("tracks", []))
    updated["weak_memory_tracks"] = add_localization_to_tracks(updated.get("weak_memory_tracks", []))
    updated["localization_config"] = {
        "enabled": True,
        "method": "relative_rssi_weighted_centroid_3d",
        "units": "cm",
        "scanner_positions_cm": SCANNER_POSITIONS_CM,
        "scanner_bias_db": SCANNER_BIAS_DB,
        "rssi_weight_k_db": RSSI_WEIGHT_K_DB,
        "relative_rssi_floor_db": RELATIVE_RSSI_FLOOR_DB,
        "device_or_mfg_bias_used": False,
    }
    return updated
