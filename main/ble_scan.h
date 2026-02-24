#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void ble_scan_start(void);

// --- NEW COMMAND SERVER SETTERS ---
void ble_set_scan_mode(uint8_t mode);
void ble_set_system_state(uint8_t state);

#ifdef __cplusplus
}
#endif