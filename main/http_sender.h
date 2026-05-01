#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Maximum legacy BLE advertising payload length.
 *
 * Your current scanner uses legacy advertising reports, where the advertising
 * data field is up to 31 bytes. If you later move to extended advertising,
 * increase this value and update http_sender.c Base64 buffer sizing too.
 */
#define BLE_ADV_PAYLOAD_MAX_LEN 31

/*
 * Minimal event passed from ble_scan.c to http_sender.c.
 *
 * This struct intentionally contains only raw/transport-safe fields.
 * Parsing names, manufacturer data, service UUIDs, and payload signatures is
 * done on the PC side by ble_adv_parser.py.
 *
 * Notes:
 * - addr is NimBLE's little-endian BLE address byte array.
 *   http_sender.c formats it as human-readable big-endian/no-colon MAC.
 *
 * - channel is currently a software label from ble_scan.c, not verified RF
 *   channel metadata. Keep it for compatibility/calibration, but treat it as
 *   weak metadata unless true channel reporting is implemented later.
 *
 * - timestamp_epoch_us is wall-clock epoch time in microseconds, from NTP when
 *   available.
 *
 * - timestamp_mono_us is ESP monotonic time in microseconds, useful for
 *   advertisement interval / real-time tracking logic.
 */
typedef struct {
    uint8_t  addr[6];
    uint8_t  addr_type;
    uint8_t  adv_type;
    int8_t   rssi;
    uint8_t  channel;

    uint8_t  payload_len;
    uint8_t  payload[BLE_ADV_PAYLOAD_MAX_LEN];

    int64_t  timestamp_epoch_us;
    int64_t  timestamp_mono_us;
} ble_minimal_event_t;

/*
 * Start the HTTP sender queue/task.
 * Safe to call more than once; later calls are ignored.
 */
void http_sender_init(void);

/*
 * Enqueue one BLE event for batched HTTP sending.
 * Returns 0 on success, -1 if the queue is unavailable/full.
 */
int http_sender_enqueue(const ble_minimal_event_t *ev);

/*
 * Update the PC receiver target IP discovered by mDNS.
 * The final target URL becomes:
 *   http://<ip>:8000/api/ble/ingest
 */
void http_sender_update_ip(const char *ip);

#ifdef __cplusplus
}
#endif
