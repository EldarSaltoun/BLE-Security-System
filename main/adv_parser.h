#ifndef ADV_PARSER_H
#define ADV_PARSER_H

#include <stdint.h>
#include <stddef.h>  // for size_t
#include <stdbool.h>

// Parse BLE advertisement data into a human-readable string (kept for your console logs)
void adv_parse(const uint8_t *adv, uint8_t adv_len, char *out, size_t out_size);

// Extract the best-available device name (Complete or Shortened). Returns length copied.
size_t adv_find_name(const uint8_t *adv, uint8_t adv_len, char *out_name, size_t out_size);

// Extract TxPower (if present) and Manufacturer ID (if present). Returns number of fields found.
int adv_extract_metrics(const uint8_t *adv, uint8_t adv_len, int8_t *txpwr, uint16_t *mfg_id, char *mfg_data_hex, size_t max_hex_len);

// NEW: extract service UUID indicators for fingerprinting
void adv_extract_services(const uint8_t *adv,
                          uint8_t adv_len,
                          bool *has_service_uuid,
                          uint8_t *n_services_16,
                          uint8_t *n_services_128);

#endif // ADV_PARSER_H