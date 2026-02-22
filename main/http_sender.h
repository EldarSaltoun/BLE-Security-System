#pragma once
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint8_t  addr[6];           // Binary MAC
    uint8_t  addr_type;
    uint8_t  adv_type;
    int8_t   rssi;
    uint8_t  payload_len;
    uint8_t  payload[31];       // Raw bytes
    int64_t  timestamp_epoch_us;
    int64_t  timestamp_mono_us;
} ble_minimal_event_t;

void http_sender_init(void);
int http_sender_enqueue(const ble_minimal_event_t *ev);

#ifdef __cplusplus
}
#endif