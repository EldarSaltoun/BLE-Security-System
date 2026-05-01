"""
ble_adv_parser.py

Robust BLE Advertising Data parser.

Input:
    raw BLE advertising payload bytes (legacy AD structure format):
        [Length][AD Type][AD Data] ...

Output:
    dictionary with decoded fields used by the receiver, dashboard,
    calibration flow, and future device-tracking engine.

Notes:
    - UUIDs in BLE AD payloads are little-endian on air. This parser returns
      normalized human-readable uppercase UUID strings.
    - The parser is intentionally defensive: malformed/truncated fields are
      recorded in "parse_errors" instead of crashing the receiver.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import base64
import binascii
import hashlib
import zlib


class AdvParser:
    # BLE AD Type constants
    FLAGS = 0x01

    UUID16_INCOMPLETE = 0x02
    UUID16_COMPLETE = 0x03
    UUID32_INCOMPLETE = 0x04
    UUID32_COMPLETE = 0x05
    UUID128_INCOMPLETE = 0x06
    UUID128_COMPLETE = 0x07

    SHORT_NAME = 0x08
    COMPLETE_NAME = 0x09
    TX_POWER = 0x0A
    APPEARANCE = 0x19

    SERVICE_DATA_16 = 0x16
    SERVICE_DATA_32 = 0x20
    SERVICE_DATA_128 = 0x21

    MANUFACTURER_SPECIFIC = 0xFF

    @staticmethod
    def parse(payload_bytes: bytes) -> Dict[str, Any]:
        """
        Parse raw BLE advertising bytes into a stable dictionary.

        This function keeps backwards-compatible keys used by your current code:
            name, mfg_id, mfg_data_hex, tx_pwr, services_16, services_128

        It also adds stronger fingerprinting fields:
            flags, appearance, services_32, service_data, ad_types,
            ad_structure, payload_len, payload_crc32, payload_sha1_8,
            payload_sig, malformed, parse_errors
        """
        if payload_bytes is None:
            payload_bytes = b""

        if not isinstance(payload_bytes, (bytes, bytearray)):
            raise TypeError("payload_bytes must be bytes or bytearray")

        payload_bytes = bytes(payload_bytes)

        results: Dict[str, Any] = {
            # Backwards-compatible fields
            "name": "Unknown",
            "mfg_id": None,
            "mfg_data_hex": "",
            "tx_pwr": None,
            "services_16": [],
            "services_128": [],

            # Additional useful fields for fingerprinting
            "flags": None,
            "appearance": None,
            "services_32": [],
            "service_data": [],  # list of dicts: {"uuid": "...", "data_hex": "...", "ad_type": int}
            "ad_types": [],
            "ad_structure": "",  # compact signature of AD types and lengths
            "payload_len": len(payload_bytes),
            "payload_crc32": f"{zlib.crc32(payload_bytes) & 0xFFFFFFFF:08X}",
            "payload_sha1_8": hashlib.sha1(payload_bytes).hexdigest()[:8].upper(),
            "payload_sig": "",
            "malformed": False,
            "parse_errors": [],
            "raw_hex": payload_bytes.hex().upper(),
        }

        structures: List[str] = []
        i = 0
        n = len(payload_bytes)

        while i < n:
            try:
                length = payload_bytes[i]

                # Length 0 means end of AD structures.
                if length == 0:
                    break

                # Length includes AD type byte + AD data bytes.
                field_end_exclusive = i + 1 + length
                if field_end_exclusive > n:
                    results["malformed"] = True
                    results["parse_errors"].append(
                        f"Truncated AD field at offset {i}: length={length}, payload_len={n}"
                    )
                    break

                if length < 1:
                    results["malformed"] = True
                    results["parse_errors"].append(
                        f"Invalid AD length at offset {i}: length={length}"
                    )
                    break

                ad_type = payload_bytes[i + 1]
                value = payload_bytes[i + 2: field_end_exclusive]
                value_len = len(value)

                results["ad_types"].append(ad_type)
                structures.append(f"{ad_type:02X}:{value_len}")

                if ad_type == AdvParser.FLAGS:
                    if value_len >= 1:
                        results["flags"] = value[0]

                elif ad_type in (AdvParser.SHORT_NAME, AdvParser.COMPLETE_NAME):
                    decoded_name = value.decode("utf-8", errors="ignore").strip("\x00").strip()
                    if decoded_name:
                        # Prefer complete name over shortened name.
                        if ad_type == AdvParser.COMPLETE_NAME or results["name"] == "Unknown":
                            results["name"] = decoded_name

                elif ad_type == AdvParser.TX_POWER:
                    if value_len >= 1:
                        results["tx_pwr"] = int.from_bytes(value[:1], byteorder="little", signed=True)

                elif ad_type == AdvParser.MANUFACTURER_SPECIFIC:
                    if value_len >= 2:
                        results["mfg_id"] = int.from_bytes(value[0:2], byteorder="little", signed=False)
                        results["mfg_data_hex"] = value[2:].hex().upper()
                    else:
                        results["mfg_data_hex"] = value.hex().upper()

                elif ad_type in (AdvParser.UUID16_INCOMPLETE, AdvParser.UUID16_COMPLETE):
                    results["services_16"].extend(AdvParser._parse_uuid16_list(value))

                elif ad_type in (AdvParser.UUID32_INCOMPLETE, AdvParser.UUID32_COMPLETE):
                    results["services_32"].extend(AdvParser._parse_uuid32_list(value))

                elif ad_type in (AdvParser.UUID128_INCOMPLETE, AdvParser.UUID128_COMPLETE):
                    results["services_128"].extend(AdvParser._parse_uuid128_list(value))

                elif ad_type == AdvParser.SERVICE_DATA_16:
                    if value_len >= 2:
                        uuid = AdvParser._uuid16_to_str(value[0:2])
                        results["service_data"].append({
                            "ad_type": ad_type,
                            "uuid": uuid,
                            "data_hex": value[2:].hex().upper(),
                        })

                elif ad_type == AdvParser.SERVICE_DATA_32:
                    if value_len >= 4:
                        uuid = AdvParser._uuid32_to_str(value[0:4])
                        results["service_data"].append({
                            "ad_type": ad_type,
                            "uuid": uuid,
                            "data_hex": value[4:].hex().upper(),
                        })

                elif ad_type == AdvParser.SERVICE_DATA_128:
                    if value_len >= 16:
                        uuid = AdvParser._uuid128_to_str(value[0:16])
                        results["service_data"].append({
                            "ad_type": ad_type,
                            "uuid": uuid,
                            "data_hex": value[16:].hex().upper(),
                        })

                elif ad_type == AdvParser.APPEARANCE:
                    if value_len >= 2:
                        results["appearance"] = int.from_bytes(value[0:2], byteorder="little", signed=False)

                # Continue to next AD structure
                i = field_end_exclusive

            except Exception as exc:
                results["malformed"] = True
                results["parse_errors"].append(f"Parser exception at offset {i}: {exc}")
                break

        results["ad_structure"] = "|".join(structures)
        results["payload_sig"] = AdvParser.make_payload_signature(results)

        return results

    @staticmethod
    def parse_base64(payload_b64: str) -> Dict[str, Any]:
        """
        Convenience parser for Base64 encoded payload strings.
        """
        if payload_b64 is None:
            payload_b64 = ""

        try:
            raw = base64.b64decode(payload_b64, validate=False)
        except (binascii.Error, ValueError):
            raw = b""

        return AdvParser.parse(raw)

    @staticmethod
    def make_payload_signature(parsed: Dict[str, Any]) -> str:
        """
        Build a compact signature that is stable enough for grouping/metadata,
        without pretending every payload hash is a separate physical device.

        This is NOT the final physical-device ID.
        It is only an alias/fingerprint clue.
        """
        mfg = parsed.get("mfg_id")
        mfg_s = f"MFG_{mfg:04X}" if isinstance(mfg, int) else "MFG_NONE"

        name = str(parsed.get("name", "Unknown") or "Unknown").strip()
        name_s = f"NAME_{name.lower()}" if name and name != "Unknown" else "NAME_NONE"

        services16 = ",".join(parsed.get("services_16", []) or [])
        services32 = ",".join(parsed.get("services_32", []) or [])
        services128 = ",".join(parsed.get("services_128", []) or [])

        service_data = parsed.get("service_data", []) or []
        service_data_uuids = ",".join(str(x.get("uuid", "")) for x in service_data if isinstance(x, dict))

        ad_structure = parsed.get("ad_structure", "")
        plen = parsed.get("payload_len", 0)
        crc = parsed.get("payload_crc32", "")

        return (
            f"{mfg_s};{name_s};"
            f"S16[{services16}];S32[{services32}];S128[{services128}];"
            f"SD[{service_data_uuids}];"
            f"AD[{ad_structure}];LEN_{plen};CRC_{crc}"
        )

    @staticmethod
    def _parse_uuid16_list(value: bytes) -> List[str]:
        out: List[str] = []
        for j in range(0, len(value) - 1, 2):
            out.append(AdvParser._uuid16_to_str(value[j:j + 2]))
        return out

    @staticmethod
    def _parse_uuid32_list(value: bytes) -> List[str]:
        out: List[str] = []
        for j in range(0, len(value) - 3, 4):
            out.append(AdvParser._uuid32_to_str(value[j:j + 4]))
        return out

    @staticmethod
    def _parse_uuid128_list(value: bytes) -> List[str]:
        out: List[str] = []
        for j in range(0, len(value) - 15, 16):
            out.append(AdvParser._uuid128_to_str(value[j:j + 16]))
        return out

    @staticmethod
    def _uuid16_to_str(raw_le: bytes) -> str:
        return f"{int.from_bytes(raw_le, byteorder='little', signed=False):04X}"

    @staticmethod
    def _uuid32_to_str(raw_le: bytes) -> str:
        return f"{int.from_bytes(raw_le, byteorder='little', signed=False):08X}"

    @staticmethod
    def _uuid128_to_str(raw_le: bytes) -> str:
        """
        Convert 16-byte little-endian BLE UUID to standard UUID text.
        BLE transmits 128-bit UUIDs little-endian in AD structures.
        """
        if len(raw_le) != 16:
            return raw_le.hex().upper()

        b = raw_le[::-1].hex().upper()
        return f"{b[0:8]}-{b[8:12]}-{b[12:16]}-{b[16:20]}-{b[20:32]}"
