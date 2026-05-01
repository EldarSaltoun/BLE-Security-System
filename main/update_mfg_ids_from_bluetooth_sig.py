"""
update_mfg_ids_from_bluetooth_sig.py

Downloads the official Bluetooth SIG Company Identifiers YAML and converts it
to the simple CSV format used by this project:

    HEX_ID,Company Name

Run:
    python update_mfg_ids_from_bluetooth_sig.py

It writes:
    mfg_ids.csv

Source:
    Bluetooth SIG Assigned Numbers public YAML repository
"""

import csv
import re
from pathlib import Path

import requests


SIG_COMPANY_IDS_URL = (
    "https://bitbucket.org/bluetooth-SIG/public/raw/main/"
    "assigned_numbers/company_identifiers/company_identifiers.yaml"
)

OUT_CSV = Path("mfg_ids.csv")


def parse_company_identifier_yaml(text: str):
    """
    Lightweight parser for the Bluetooth SIG company_identifiers.yaml format.

    The file structure is simple enough that we do not require PyYAML.
    Expected entries look like:
        - value: 0x004C
          name: Apple, Inc.
    """
    entries = []
    current_value = None
    current_name = None

    value_re = re.compile(r"^\s*-\s*value:\s*(0x[0-9A-Fa-f]+|\d+)\s*$")
    name_re = re.compile(r"^\s*name:\s*(.+?)\s*$")

    def flush():
        nonlocal current_value, current_name
        if current_value is not None and current_name:
            entries.append((current_value, current_name.strip().strip('"').strip("'")))
        current_value = None
        current_name = None

    for line in text.splitlines():
        vm = value_re.match(line)
        if vm:
            flush()
            raw_value = vm.group(1)
            current_value = int(raw_value, 16) if raw_value.lower().startswith("0x") else int(raw_value)
            continue

        nm = name_re.match(line)
        if nm and current_value is not None:
            current_name = nm.group(1)

    flush()

    # Remove duplicates while preserving the latest name if any duplicate exists.
    dedup = {}
    for value, name in entries:
        dedup[value] = name

    return sorted(dedup.items())


def main():
    print(f"Downloading Bluetooth SIG company identifiers from:\n{SIG_COMPANY_IDS_URL}")
    r = requests.get(SIG_COMPANY_IDS_URL, timeout=20)
    r.raise_for_status()

    entries = parse_company_identifier_yaml(r.text)
    if not entries:
        raise RuntimeError("No company identifiers parsed. Source format may have changed.")

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for value, name in entries:
            writer.writerow([f"{value:04X}", name])

    print(f"Wrote {len(entries)} company identifiers to {OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
