#include "adv_parser.h"
#include "esp_log.h"
#include <stdio.h>
#include <string.h>

static const char *TAG = "ADV_PARSER";

static int adv_iter_next(const uint8_t *adv, uint8_t adv_len, uint8_t *pos,
                         uint8_t *type, const uint8_t **val, uint8_t *val_len)
{
    if (*pos >= adv_len) return 0;
    uint8_t field_len = adv[*pos];
    if (field_len == 0) return 0;
    if (*pos + field_len >= adv_len) return 0; // malformed / truncated

    uint8_t t = adv[*pos + 1];
    const uint8_t *v = &adv[*pos + 2];
    uint8_t vl = field_len - 1;

    *type = t;
    *val = v;
    *val_len = vl;
    *pos += field_len + 1;
    return 1;
}

void adv_parse(const uint8_t *adv_data, uint8_t adv_len, char *out_str, size_t out_len)
{
    out_str[0] = '\0';
    char buf[96];

    uint8_t pos = 0, type, vlen;
    const uint8_t *val;

    while (adv_iter_next(adv_data, adv_len, &pos, &type, &val, &vlen)) {
        buf[0] = '\0';
        switch (type) {
            case 0x09: // Complete Local Name
            case 0x08: // Shortened Local Name
                snprintf(buf, sizeof(buf), "Name=%.*s ", (int)vlen, (const char*)val);
                break;
            case 0x0A: // Tx Power Level
                if (vlen >= 1) snprintf(buf, sizeof(buf), "TxPwr=%d ", (int8_t)val[0]);
                break;
            case 0x01: // Flags
                if (vlen >= 1) snprintf(buf, sizeof(buf), "Flags=0x%02X ", val[0]);
                break;
            case 0xFF: // Manufacturer Specific Data
                if (vlen >= 2) {
                    uint16_t cid = (uint16_t)val[0] | ((uint16_t)val[1] << 8);
                    snprintf(buf, sizeof(buf), "MFG=0x%04X(%u) ", cid, vlen - 2);
                } else {
                    snprintf(buf, sizeof(buf), "MFG(%u) ", vlen);
                }
                break;
            case 0x02: case 0x03:
            case 0x06: case 0x07:
                snprintf(buf, sizeof(buf), "UUIDs(%u) ", vlen);
                break;
            default:
                snprintf(buf, sizeof(buf), "Type0x%02X(%u) ", type, vlen);
                break;
        }
        strncat(out_str, buf, out_len - strlen(out_str) - 1);
    }
    ESP_LOGD(TAG, "Parsed ADV: %s", out_str);
}

size_t adv_find_name(const uint8_t *adv, uint8_t adv_len, char *out_name, size_t out_size)
{
    if (!out_name || out_size == 0) return 0;
    out_name[0] = '\0';

    uint8_t pos = 0, type, vlen;
    const uint8_t *val;

    const uint8_t *best = NULL;
    uint8_t best_len = 0;
    uint8_t best_type = 0;

    while (adv_iter_next(adv, adv_len, &pos, &type, &val, &vlen)) {
        if (type == 0x09) {
            best = val; best_len = vlen; best_type = type; break;
        }
        if (type == 0x08 && best_type != 0x09) {
            best = val; best_len = vlen; best_type = type;
        }
    }

    if (best && best_len > 0) {
        size_t cpy = (best_len < out_size - 1) ? best_len : (out_size - 1);
        memcpy(out_name, best, cpy);
        out_name[cpy] = '\0';
        return cpy;
    }
    return 0;
}

// MODIFIED: Added mfg_data_hex and max_hex_len arguments
int adv_extract_metrics(const uint8_t *adv, uint8_t adv_len, int8_t *txpwr, uint16_t *mfg_id, char *mfg_data_hex, size_t max_hex_len)
{
    if (!adv || adv_len == 0) return 0;
    uint8_t pos = 0, type, vlen;
    const uint8_t *val;
    int found = 0;

    *txpwr = 0;
    *mfg_id = 0xFFFF; // Default unknown

    // Initialize hex string if buffer is provided
    if (mfg_data_hex && max_hex_len > 0) {
        mfg_data_hex[0] = '\0';
    }

    while (adv_iter_next(adv, adv_len, &pos, &type, &val, &vlen)) {
        if (type == 0x0A && vlen >= 1) {
            *txpwr = (int8_t)val[0];
            found++;
        }
        if (type == 0xFF && vlen >= 2) {
            *mfg_id = (uint16_t)val[0] | ((uint16_t)val[1] << 8);
            found++;

            // Extract Raw Payload (Remaining bytes) to Hex String
            if (mfg_data_hex && max_hex_len > 0 && vlen > 2) {
                char *ptr = mfg_data_hex;
                size_t remaining = max_hex_len;
                
                // Start loop at 2 to skip the 2-byte ID we just read
                for (int i = 2; i < vlen; i++) {
                    int written = snprintf(ptr, remaining, "%02X", val[i]);
                    if (written < 0 || (size_t)written >= remaining) break; // prevent overflow
                    ptr += written;
                    remaining -= written;
                }
            }
        }
    }
    return found;
}

/* ========================= NEW FUNCTION ========================= */

void adv_extract_services(const uint8_t *adv,
                          uint8_t adv_len,
                          bool *has_service_uuid,
                          uint8_t *n_services_16,
                          uint8_t *n_services_128)
{
    uint8_t pos = 0, type, vlen;
    const uint8_t *val;

    *has_service_uuid = false;
    *n_services_16 = 0;
    *n_services_128 = 0;

    while (adv_iter_next(adv, adv_len, &pos, &type, &val, &vlen)) {
        switch (type) {
            /* 16-bit Service UUIDs */
            case 0x02: // Incomplete 16-bit UUIDs
            case 0x03: // Complete 16-bit UUIDs
                *has_service_uuid = true;
                *n_services_16 += vlen / 2;
                break;
            
            case 0x16: // Service Data - 16-bit UUID (Added as per requirement)
                *has_service_uuid = true;
                // Counts as 1 service (UUID is the first 2 bytes)
                if (vlen >= 2) *n_services_16 += 1; 
                break;

            /* 128-bit Service UUIDs */
            case 0x06: // Incomplete 128-bit UUIDs
            case 0x07: // Complete 128-bit UUIDs
                *has_service_uuid = true;
                *n_services_128 += vlen / 16;
                break;

            case 0x21: // Service Data - 128-bit UUID (Added as per requirement)
                *has_service_uuid = true;
                // Counts as 1 service (UUID is the first 16 bytes)
                if (vlen >= 16) *n_services_128 += 1;
                break;

            default:
                break;
        }
    }
}