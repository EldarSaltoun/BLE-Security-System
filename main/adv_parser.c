#include "adv_parser.h"

#include "esp_log.h"

#include <stdio.h>
#include <string.h>

static const char *TAG = "ADV_PARSER";

/* BLE AD types used by this parser */
#define AD_TYPE_FLAGS                 0x01
#define AD_TYPE_UUID16_INCOMPLETE     0x02
#define AD_TYPE_UUID16_COMPLETE       0x03
#define AD_TYPE_UUID32_INCOMPLETE     0x04
#define AD_TYPE_UUID32_COMPLETE       0x05
#define AD_TYPE_UUID128_INCOMPLETE    0x06
#define AD_TYPE_UUID128_COMPLETE      0x07
#define AD_TYPE_SHORT_NAME            0x08
#define AD_TYPE_COMPLETE_NAME         0x09
#define AD_TYPE_TX_POWER              0x0A
#define AD_TYPE_APPEARANCE            0x19
#define AD_TYPE_SERVICE_DATA_16       0x16
#define AD_TYPE_SERVICE_DATA_32       0x20
#define AD_TYPE_SERVICE_DATA_128      0x21
#define AD_TYPE_MANUFACTURER          0xFF

/*
 * Iterate over BLE advertising data structures:
 *   [Length][AD Type][AD Data...]
 *
 * Length includes the AD Type byte, but not the Length byte itself.
 *
 * Returns:
 *   1 = valid field produced
 *   0 = no more fields or malformed/truncated field
 */
static int adv_iter_next(const uint8_t *adv,
                         uint8_t adv_len,
                         uint8_t *pos,
                         uint8_t *type,
                         const uint8_t **val,
                         uint8_t *val_len)
{
    if (!adv || !pos || !type || !val || !val_len) {
        return 0;
    }

    if (*pos >= adv_len) {
        return 0;
    }

    uint8_t field_len = adv[*pos];

    /* Length 0 means end of AD structures. */
    if (field_len == 0) {
        return 0;
    }

    /*
     * field_end_exclusive points one byte after this AD structure.
     * Need: pos + 1 + field_len <= adv_len
     */
    size_t field_end_exclusive = (size_t)(*pos) + 1U + (size_t)field_len;
    if (field_end_exclusive > adv_len || field_len < 1) {
        return 0;  /* malformed / truncated */
    }

    *type = adv[*pos + 1];
    *val = &adv[*pos + 2];
    *val_len = field_len - 1;
    *pos = (uint8_t)field_end_exclusive;

    return 1;
}

static void append_text(char *out, size_t out_len, const char *text)
{
    if (!out || out_len == 0 || !text) {
        return;
    }

    size_t used = strlen(out);
    if (used >= out_len - 1) {
        return;
    }

    strncat(out, text, out_len - used - 1);
}

static void bytes_to_hex(const uint8_t *bytes, size_t len, char *out, size_t out_len)
{
    if (!out || out_len == 0) {
        return;
    }

    out[0] = '\0';

    if (!bytes || len == 0) {
        return;
    }

    size_t pos = 0;
    for (size_t i = 0; i < len && pos + 3 <= out_len; i++) {
        int written = snprintf(out + pos, out_len - pos, "%02X", bytes[i]);
        if (written != 2) {
            break;
        }
        pos += 2;
    }
}

void adv_parse(const uint8_t *adv_data, uint8_t adv_len, char *out_str, size_t out_len)
{
    if (!out_str || out_len == 0) {
        return;
    }

    out_str[0] = '\0';

    if (!adv_data || adv_len == 0) {
        return;
    }

    char buf[128];

    uint8_t pos = 0;
    uint8_t type = 0;
    uint8_t vlen = 0;
    const uint8_t *val = NULL;

    while (adv_iter_next(adv_data, adv_len, &pos, &type, &val, &vlen)) {
        buf[0] = '\0';

        switch (type) {
            case AD_TYPE_COMPLETE_NAME:
            case AD_TYPE_SHORT_NAME:
                snprintf(buf, sizeof(buf), "Name=%.*s ", (int)vlen, (const char *)val);
                break;

            case AD_TYPE_TX_POWER:
                if (vlen >= 1) {
                    snprintf(buf, sizeof(buf), "TxPwr=%d ", (int)((int8_t)val[0]));
                }
                break;

            case AD_TYPE_FLAGS:
                if (vlen >= 1) {
                    snprintf(buf, sizeof(buf), "Flags=0x%02X ", val[0]);
                }
                break;

            case AD_TYPE_MANUFACTURER:
                if (vlen >= 2) {
                    uint16_t cid = (uint16_t)val[0] | ((uint16_t)val[1] << 8);
                    snprintf(buf, sizeof(buf), "MFG=0x%04X(%u) ", cid, (unsigned)(vlen - 2));
                } else {
                    snprintf(buf, sizeof(buf), "MFG(%u) ", (unsigned)vlen);
                }
                break;

            case AD_TYPE_UUID16_INCOMPLETE:
            case AD_TYPE_UUID16_COMPLETE:
                snprintf(buf, sizeof(buf), "UUID16(%u) ", (unsigned)(vlen / 2));
                break;

            case AD_TYPE_UUID32_INCOMPLETE:
            case AD_TYPE_UUID32_COMPLETE:
                snprintf(buf, sizeof(buf), "UUID32(%u) ", (unsigned)(vlen / 4));
                break;

            case AD_TYPE_UUID128_INCOMPLETE:
            case AD_TYPE_UUID128_COMPLETE:
                snprintf(buf, sizeof(buf), "UUID128(%u) ", (unsigned)(vlen / 16));
                break;

            case AD_TYPE_SERVICE_DATA_16:
                snprintf(buf, sizeof(buf), "SvcData16(%u) ", (unsigned)vlen);
                break;

            case AD_TYPE_SERVICE_DATA_32:
                snprintf(buf, sizeof(buf), "SvcData32(%u) ", (unsigned)vlen);
                break;

            case AD_TYPE_SERVICE_DATA_128:
                snprintf(buf, sizeof(buf), "SvcData128(%u) ", (unsigned)vlen);
                break;

            case AD_TYPE_APPEARANCE:
                if (vlen >= 2) {
                    uint16_t app = (uint16_t)val[0] | ((uint16_t)val[1] << 8);
                    snprintf(buf, sizeof(buf), "Appearance=0x%04X ", app);
                }
                break;

            default:
                snprintf(buf, sizeof(buf), "Type0x%02X(%u) ", type, (unsigned)vlen);
                break;
        }

        append_text(out_str, out_len, buf);
    }

    ESP_LOGD(TAG, "Parsed ADV: %s", out_str);
}

size_t adv_find_name(const uint8_t *adv, uint8_t adv_len, char *out_name, size_t out_size)
{
    if (!out_name || out_size == 0) {
        return 0;
    }

    out_name[0] = '\0';

    if (!adv || adv_len == 0) {
        return 0;
    }

    uint8_t pos = 0;
    uint8_t type = 0;
    uint8_t vlen = 0;
    const uint8_t *val = NULL;

    const uint8_t *best = NULL;
    uint8_t best_len = 0;
    uint8_t best_type = 0;

    while (adv_iter_next(adv, adv_len, &pos, &type, &val, &vlen)) {
        if (type == AD_TYPE_COMPLETE_NAME) {
            best = val;
            best_len = vlen;
            best_type = type;
            break;
        }

        if (type == AD_TYPE_SHORT_NAME && best_type != AD_TYPE_COMPLETE_NAME) {
            best = val;
            best_len = vlen;
            best_type = type;
        }
    }

    if (best && best_len > 0) {
        size_t cpy = (best_len < (out_size - 1)) ? best_len : (out_size - 1);
        memcpy(out_name, best, cpy);
        out_name[cpy] = '\0';
        return cpy;
    }

    return 0;
}

int adv_extract_metrics(const uint8_t *adv,
                        uint8_t adv_len,
                        int8_t *txpwr,
                        uint16_t *mfg_id,
                        char *mfg_data_hex,
                        size_t max_hex_len)
{
    if (txpwr) {
        *txpwr = 0;
    }

    if (mfg_id) {
        *mfg_id = 0xFFFF;  /* unknown */
    }

    if (mfg_data_hex && max_hex_len > 0) {
        mfg_data_hex[0] = '\0';
    }

    if (!adv || adv_len == 0) {
        return 0;
    }

    uint8_t pos = 0;
    uint8_t type = 0;
    uint8_t vlen = 0;
    const uint8_t *val = NULL;
    int found = 0;

    while (adv_iter_next(adv, adv_len, &pos, &type, &val, &vlen)) {
        if (type == AD_TYPE_TX_POWER && vlen >= 1) {
            if (txpwr) {
                *txpwr = (int8_t)val[0];
            }
            found++;
        } else if (type == AD_TYPE_MANUFACTURER && vlen >= 2) {
            if (mfg_id) {
                *mfg_id = (uint16_t)val[0] | ((uint16_t)val[1] << 8);
            }

            if (mfg_data_hex && max_hex_len > 0) {
                bytes_to_hex(&val[2], (size_t)(vlen - 2), mfg_data_hex, max_hex_len);
            }

            found++;
        }
    }

    return found;
}

void adv_extract_services(const uint8_t *adv,
                          uint8_t adv_len,
                          bool *has_service_uuid,
                          uint8_t *n_services_16,
                          uint8_t *n_services_128)
{
    if (has_service_uuid) {
        *has_service_uuid = false;
    }

    if (n_services_16) {
        *n_services_16 = 0;
    }

    if (n_services_128) {
        *n_services_128 = 0;
    }

    if (!adv || adv_len == 0) {
        return;
    }

    uint8_t pos = 0;
    uint8_t type = 0;
    uint8_t vlen = 0;
    const uint8_t *val = NULL;

    while (adv_iter_next(adv, adv_len, &pos, &type, &val, &vlen)) {
        (void)val;

        switch (type) {
            case AD_TYPE_UUID16_INCOMPLETE:
            case AD_TYPE_UUID16_COMPLETE:
                if (has_service_uuid) *has_service_uuid = true;
                if (n_services_16) *n_services_16 += (uint8_t)(vlen / 2);
                break;

            case AD_TYPE_SERVICE_DATA_16:
                if (vlen >= 2) {
                    if (has_service_uuid) *has_service_uuid = true;
                    if (n_services_16) *n_services_16 += 1;
                }
                break;

            case AD_TYPE_UUID128_INCOMPLETE:
            case AD_TYPE_UUID128_COMPLETE:
                if (has_service_uuid) *has_service_uuid = true;
                if (n_services_128) *n_services_128 += (uint8_t)(vlen / 16);
                break;

            case AD_TYPE_SERVICE_DATA_128:
                if (vlen >= 16) {
                    if (has_service_uuid) *has_service_uuid = true;
                    if (n_services_128) *n_services_128 += 1;
                }
                break;

            /*
             * We do not expose a separate 32-bit counter in the current API.
             * Still mark "has services" when 32-bit UUID/service-data is present.
             */
            case AD_TYPE_UUID32_INCOMPLETE:
            case AD_TYPE_UUID32_COMPLETE:
                if (vlen >= 4) {
                    if (has_service_uuid) *has_service_uuid = true;
                }
                break;

            case AD_TYPE_SERVICE_DATA_32:
                if (vlen >= 4) {
                    if (has_service_uuid) *has_service_uuid = true;
                }
                break;

            default:
                break;
        }
    }
}
