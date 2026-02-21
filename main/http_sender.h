#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

typedef struct {
    char     mac[18];            // "AA:BB:CC:DD:EE:FF"
    int8_t   rssi;
    char     name[64];
    int8_t   txpwr;
    uint16_t mfg_id;
    uint8_t  adv_len;

    // NEW — service fingerprinting
    bool     has_service_uuid;
    uint8_t  n_services_16;
    uint8_t  n_services_128;

    // NEW — raw manufacturer data for deep inspection
    char     mfg_data_hex[64];

    int64_t  timestamp_epoch_us;   // NTP epoch microseconds (global timeline)
    int64_t  timestamp_mono_us;    // monotonic microseconds since boot (stable Δt)
    char     scanner[32];
} ble_http_event_t;


// Start sender task + queue
void http_sender_init(void);

// Non-blocking enqueue. Returns 1 if queued, 0 if dropped.
int http_sender_enqueue(const ble_http_event_t *ev);

#ifdef __cplusplus
}
#endif