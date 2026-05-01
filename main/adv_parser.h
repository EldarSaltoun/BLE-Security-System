#ifndef ADV_PARSER_H
#define ADV_PARSER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Parse BLE advertising data into a compact human-readable string.
 *
 * This function is intended for debugging/logging only.
 * The PC-side ble_adv_parser.py remains the main parser used by the tracking
 * and dashboard logic.
 */
void adv_parse(const uint8_t *adv, uint8_t adv_len, char *out, size_t out_size);

/*
 * Extract the best available device name.
 *
 * Complete Local Name (AD type 0x09) is preferred over Shortened Local Name
 * (AD type 0x08).
 *
 * Returns number of bytes copied into out_name, excluding the null terminator.
 */
size_t adv_find_name(const uint8_t *adv, uint8_t adv_len, char *out_name, size_t out_size);

/*
 * Extract Tx Power and Manufacturer Specific Data metadata.
 *
 * txpwr:
 *   Set to AD type 0x0A value when present. Defaults to 0 if not present.
 *
 * mfg_id:
 *   Set to the little-endian Bluetooth company identifier from AD type 0xFF.
 *   Defaults to 0xFFFF if not present/unknown.
 *
 * mfg_data_hex:
 *   Optional output buffer. If provided, receives the manufacturer data bytes
 *   after the 2-byte company identifier as uppercase hex.
 *
 * Returns number of metadata fields found.
 */
int adv_extract_metrics(const uint8_t *adv,
                        uint8_t adv_len,
                        int8_t *txpwr,
                        uint16_t *mfg_id,
                        char *mfg_data_hex,
                        size_t max_hex_len);

/*
 * Extract service UUID presence/count indicators.
 *
 * Counts 16-bit and 128-bit UUID entries from UUID-list AD structures and
 * service-data AD structures.
 *
 * 32-bit service UUIDs are not counted separately by this API, but they do set
 * has_service_uuid=true if present.
 */
void adv_extract_services(const uint8_t *adv,
                          uint8_t adv_len,
                          bool *has_service_uuid,
                          uint8_t *n_services_16,
                          uint8_t *n_services_128);

#ifdef __cplusplus
}
#endif

#endif /* ADV_PARSER_H */
