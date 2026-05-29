#!/usr/bin/env python3
"""
build_calibration_fingerprints.py

Builds a 3x3 BLE RSSI fingerprint database from calibration_raw.csv and
calibration_summary.csv.

Input files:
    calibration_raw.csv
    calibration_summary.csv

Output file:
    calibration_fingerprints.json

The output is used by pc_receiver.py for mobile-only probabilistic block
localization.

Model notes:
    - Tx Power is intentionally ignored.
    - The current ESP32 firmware channel field is a software label, not a real
      BLE RF channel, so channel labels are ignored.
    - matching_mean_rssi is based on top_half_mean_rssi because pc_receiver.py
      uses top-half RSSI means for live device fingerprints.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


GRID_LAYOUT = [[1, 2, 3], [6, 5, 4], [7, 8, 9]]

# Physical nearest scanner anchors from the room drawing / measurement notes.
PHYSICAL_NEAREST_SCANNER = {
    "1": "3",
    "3": "2",
    "7": "4",
    "9": "1",
}

# Approximate physical grid scale from user notes.
TILE_SIZE_CM = 42.0
BLOCK_TILES_W = 3
BLOCK_TILES_H = 4
BLOCK_SIZE_CM = {
    "x_cm": TILE_SIZE_CM * BLOCK_TILES_W,
    "y_cm": TILE_SIZE_CM * BLOCK_TILES_H,
}


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(value))
    except Exception:
        return default


def percentile(values: List[float], q: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    if len(vals) == 1:
        return round(vals[0], 3)
    pos = (len(vals) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return round(vals[int(pos)], 3)
    frac = pos - lo
    out = vals[lo] * (1.0 - frac) + vals[hi] * frac
    return round(out, 3)


def relative_vector(values: Dict[str, float]) -> Dict[str, float]:
    clean = {str(k): float(v) for k, v in values.items() if v is not None}
    if not clean:
        return {}
    strongest = max(clean.values())
    return {k: round(v - strongest, 3) for k, v in clean.items()}


def top_scanners(values: Dict[str, float], n: int = 2) -> List[str]:
    return [
        str(k)
        for k, _ in sorted(values.items(), key=lambda item: item[1], reverse=True)[:n]
    ]


def summarize_samples(samples: List[int]) -> Dict[str, Any]:
    if not samples:
        return {
            "n_samples": 0,
            "mean_rssi": None,
            "median_rssi": None,
            "std_rssi": None,
            "min_rssi": None,
            "max_rssi": None,
            "p10_rssi": None,
            "p25_rssi": None,
            "p50_rssi": None,
            "p75_rssi": None,
            "p90_rssi": None,
            "top_half_mean_rssi": None,
        }

    vals = [int(v) for v in samples]
    vals_sorted = sorted(vals)
    strongest_first = sorted(vals, reverse=True)
    keep_n = max(1, len(vals) // 2)

    if len(vals) > 1:
        std = statistics.stdev(vals)
    else:
        std = 0.0

    return {
        "n_samples": len(vals),
        "mean_rssi": round(statistics.mean(vals), 3),
        "median_rssi": round(statistics.median(vals), 3),
        "std_rssi": round(std, 3),
        "min_rssi": min(vals),
        "max_rssi": max(vals),
        "p10_rssi": percentile(vals_sorted, 0.10),
        "p25_rssi": percentile(vals_sorted, 0.25),
        "p50_rssi": percentile(vals_sorted, 0.50),
        "p75_rssi": percentile(vals_sorted, 0.75),
        "p90_rssi": percentile(vals_sorted, 0.90),
        "top_half_mean_rssi": round(statistics.mean(strongest_first[:keep_n]), 3),
    }


def read_summary(path: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    if not path.exists():
        return out

    with path.open("r", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            block = str(row.get("grid_block", "")).strip()
            scanner = str(row.get("scanner_id", "")).strip()
            if not block or not scanner:
                continue

            out[block][scanner] = {
                "session_id": row.get("session_id", ""),
                "n_samples": safe_int(row.get("n_samples"), 0),
                "mean_rssi": safe_float(row.get("mean_rssi")),
                "median_rssi": safe_float(row.get("median_rssi")),
                "std_rssi": safe_float(row.get("std_rssi")),
                "min_rssi": safe_float(row.get("min_rssi")),
                "max_rssi": safe_float(row.get("max_rssi")),
                "p90_rssi": safe_float(row.get("p90_rssi")),
                "top_half_mean_rssi": safe_float(row.get("top_half_mean_rssi")),
                "complete": safe_int(row.get("complete"), 0),
            }

    return out


def read_raw(path: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    out: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    if not path.exists():
        return out

    with path.open("r", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            block = str(row.get("grid_block", "")).strip()
            scanner = str(row.get("scanner_id", "")).strip()
            rssi = safe_int(row.get("rssi"))
            if not block or not scanner or rssi is None:
                continue

            out[block][scanner].append({
                "sample_index": safe_int(row.get("sample_index"), 0),
                "rssi": int(rssi),
                "timestamp_local": row.get("timestamp_local", ""),
                "payload_sig": row.get("payload_sig", ""),
            })

    return out


def build_block_from_summary_and_raw(
    block: str,
    summary_by_scanner: Dict[str, Dict[str, Any]],
    raw_by_scanner: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    scanners = sorted(set(summary_by_scanner.keys()) | set(raw_by_scanner.keys()), key=lambda x: int(x) if x.isdigit() else x)

    raw_stats = {}
    for scanner in scanners:
        raw_samples = [safe_int(item.get("rssi")) for item in raw_by_scanner.get(scanner, [])]
        raw_samples = [v for v in raw_samples if v is not None]
        raw_stats[scanner] = summarize_samples(raw_samples)

    mean = {}
    median = {}
    std = {}
    min_rssi = {}
    max_rssi = {}
    p10 = {}
    p25 = {}
    p50 = {}
    p75 = {}
    p90 = {}
    top_half = {}
    matching_mean = {}
    n_samples = {}

    for scanner in scanners:
        src = summary_by_scanner.get(scanner, {})
        raw_src = raw_stats.get(scanner, {})

        mean_val = src.get("mean_rssi")
        if mean_val is None:
            mean_val = raw_src.get("mean_rssi")

        std_val = src.get("std_rssi")
        if std_val is None:
            std_val = raw_src.get("std_rssi")

        top_half_val = src.get("top_half_mean_rssi")
        if top_half_val is None:
            top_half_val = raw_src.get("top_half_mean_rssi")

        median_val = src.get("median_rssi")
        if median_val is None:
            median_val = raw_src.get("median_rssi")

        mean[scanner] = round(float(mean_val), 3) if mean_val is not None else None
        median[scanner] = round(float(median_val), 3) if median_val is not None else None
        std[scanner] = round(float(std_val), 3) if std_val is not None else None
        min_rssi[scanner] = raw_src.get("min_rssi", src.get("min_rssi"))
        max_rssi[scanner] = raw_src.get("max_rssi", src.get("max_rssi"))
        p10[scanner] = raw_src.get("p10_rssi")
        p25[scanner] = raw_src.get("p25_rssi")
        p50[scanner] = raw_src.get("p50_rssi", median[scanner])
        p75[scanner] = raw_src.get("p75_rssi")
        p90[scanner] = raw_src.get("p90_rssi", src.get("p90_rssi"))
        top_half[scanner] = round(float(top_half_val), 3) if top_half_val is not None else mean[scanner]
        matching_mean[scanner] = top_half[scanner]
        n_samples[scanner] = int(src.get("n_samples") or raw_src.get("n_samples") or 0)

    # Dominance/top-2 pattern from sample_index-aligned raw measurements.
    by_index: Dict[int, Dict[str, int]] = defaultdict(dict)
    for scanner, rows in raw_by_scanner.items():
        for item in rows:
            idx = safe_int(item.get("sample_index"), 0)
            rssi = safe_int(item.get("rssi"))
            if idx is None or idx <= 0 or rssi is None:
                continue
            by_index[idx][scanner] = int(rssi)

    strongest_counter: Counter[str] = Counter()
    top2_counter: Counter[str] = Counter()
    usable_windows = 0

    for _, scanner_values in sorted(by_index.items()):
        if len(scanner_values) < 2:
            continue
        usable_windows += 1
        ordered = sorted(scanner_values.items(), key=lambda item: item[1], reverse=True)
        strongest_counter[str(ordered[0][0])] += 1
        top2 = ",".join(str(k) for k, _ in ordered[:2])
        top2_counter[top2] += 1

    if usable_windows > 0:
        strongest_frequency = {
            scanner: round(count / usable_windows, 4)
            for scanner, count in sorted(strongest_counter.items(), key=lambda item: item[0])
        }
        top2_frequency = {
            pair: round(count / usable_windows, 4)
            for pair, count in top2_counter.most_common()
        }
    else:
        strongest_frequency = {}
        top2_frequency = {}

    clean_matching_mean = {k: v for k, v in matching_mean.items() if isinstance(v, (int, float))}
    clean_mean = {k: v for k, v in mean.items() if isinstance(v, (int, float))}
    clean_std = {k: v for k, v in std.items() if isinstance(v, (int, float))}

    dominant_by_matching_mean = top_scanners(clean_matching_mean, 1)[0] if clean_matching_mean else None
    top2_by_matching_mean = top_scanners(clean_matching_mean, 2)

    # Reliability: lower std means more stable. Clamp to avoid extreme weights.
    reliability_weight = {}
    for scanner, std_val in clean_std.items():
        std_safe = min(8.0, max(2.0, float(std_val)))
        reliability_weight[scanner] = round(1.0 / (std_safe * std_safe), 5)

    if len(top2_by_matching_mean) >= 2:
        gap = abs(clean_matching_mean[top2_by_matching_mean[0]] - clean_matching_mean[top2_by_matching_mean[1]])
    else:
        gap = None

    ambiguity = "HIGH"
    if gap is not None:
        if gap >= 8.0:
            ambiguity = "LOW"
        elif gap >= 3.0:
            ambiguity = "MEDIUM"
        else:
            ambiguity = "HIGH"

    return {
        "block_id": block,
        "session_ids": sorted({v.get("session_id", "") for v in summary_by_scanner.values() if v.get("session_id")}),
        "physical_nearest_scanner": PHYSICAL_NEAREST_SCANNER.get(block, ""),
        "mean": clean_mean,
        "median": {k: v for k, v in median.items() if isinstance(v, (int, float))},
        "std": clean_std,
        "min": {k: v for k, v in min_rssi.items() if isinstance(v, (int, float))},
        "max": {k: v for k, v in max_rssi.items() if isinstance(v, (int, float))},
        "p10": {k: v for k, v in p10.items() if isinstance(v, (int, float))},
        "p25": {k: v for k, v in p25.items() if isinstance(v, (int, float))},
        "p50": {k: v for k, v in p50.items() if isinstance(v, (int, float))},
        "p75": {k: v for k, v in p75.items() if isinstance(v, (int, float))},
        "p90": {k: v for k, v in p90.items() if isinstance(v, (int, float))},
        "top_half_mean": {k: v for k, v in top_half.items() if isinstance(v, (int, float))},
        "matching_mean": clean_matching_mean,
        "relative_mean": relative_vector(clean_mean),
        "relative_matching_mean": relative_vector(clean_matching_mean),
        "n_samples": {k: int(v) for k, v in n_samples.items()},
        "dominant_scanner": dominant_by_matching_mean,
        "top2_scanners": top2_by_matching_mean,
        "dominance_gap_db": round(gap, 3) if gap is not None else None,
        "ambiguity": ambiguity,
        "reliability_weight": reliability_weight,
        "strongest_frequency": strongest_frequency,
        "top2_frequency": top2_frequency,
        "top2_dominant_pattern": next(iter(top2_frequency.keys()), ""),
        "usable_top2_windows": usable_windows,
    }


def build_fingerprints(raw_csv: Path, summary_csv: Path) -> Dict[str, Any]:
    summary = read_summary(summary_csv)
    raw = read_raw(raw_csv)

    all_blocks = sorted(set(summary.keys()) | set(raw.keys()), key=lambda x: int(x) if x.isdigit() else x)
    blocks = {}

    for block in all_blocks:
        blocks[block] = build_block_from_summary_and_raw(
            block,
            summary.get(block, {}),
            raw.get(block, {}),
        )

    return {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "model": "mobile_only_weighted_probabilistic_rssi_fingerprint",
        "notes": [
            "Tx Power is intentionally ignored.",
            "matching_mean uses top_half_mean_rssi to match pc_receiver.py live top-half scanner_rssi().",
            "STD is used both as scanner reliability and as a fingerprint feature.",
            "Channel labels are ignored because current scanner firmware reports software labels only.",
            "Block 1 is physically nearest scanner 3, but measured RF dominance can be scanner 2; do not manually correct measured fingerprints.",
        ],
        "grid_layout": GRID_LAYOUT,
        "tile_size_cm": TILE_SIZE_CM,
        "block_size_cm": BLOCK_SIZE_CM,
        "scanners": ["1", "2", "3", "4"],
        "physical_nearest_scanner_anchors": PHYSICAL_NEAREST_SCANNER,
        "blocks": blocks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BLE grid calibration fingerprints.")
    parser.add_argument("--raw", default="calibration_raw.csv", help="Path to calibration_raw.csv")
    parser.add_argument("--summary", default="calibration_summary.csv", help="Path to calibration_summary.csv")
    parser.add_argument("--out", default="calibration_fingerprints.json", help="Output JSON path")
    args = parser.parse_args()

    payload = build_fingerprints(Path(args.raw), Path(args.summary))
    out_path = Path(args.out)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Wrote {out_path.resolve()}")
    print(f"Blocks: {', '.join(payload['blocks'].keys())}")
    for block, info in payload["blocks"].items():
        print(
            f"Block {block}: dominant={info.get('dominant_scanner')} "
            f"top2={info.get('top2_scanners')} gap={info.get('dominance_gap_db')} "
            f"ambiguity={info.get('ambiguity')}"
        )


if __name__ == "__main__":
    main()
