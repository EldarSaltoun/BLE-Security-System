class AdvParser:
    @staticmethod
    def parse(payload_bytes):
        """
        Parses raw BLE advertisement bytes into a readable dictionary.
        """
        results = {
            "name": "Unknown",
            "mfg_id": None,
            "mfg_data_hex": "",
            "tx_pwr": None,
            "services_16": [],
            "services_128": []
        }
        
        i = 0
        while i < len(payload_bytes):
            try:
                length = payload_bytes[i]
                if length == 0: break
                
                ad_type = payload_bytes[i+1]
                value = payload_bytes[i+2 : i+1+length]
                
                # 0x08: Shortened Local Name, 0x09: Complete Local Name
                if ad_type in [0x08, 0x09]:
                    results["name"] = value.decode('utf-8', errors='ignore')
                
                # 0x0A: Tx Power Level
                elif ad_type == 0x0A:
                    results["tx_pwr"] = int.from_bytes(value, byteorder='little', signed=True)
                
                # 0xFF: Manufacturer Specific Data
                elif ad_type == 0xFF:
                    if len(value) >= 2:
                        results["mfg_id"] = int.from_bytes(value[0:2], byteorder='little')
                        results["mfg_data_hex"] = value[2:].hex().upper()
                
                # 0x02/0x03: 16-bit Service UUIDs
                elif ad_type in [0x02, 0x03]:
                    for j in range(0, len(value), 2):
                        results["services_16"].append(value[j:j+2].hex().upper())
                
                i += length + 1
            except Exception:
                break
                
        return results