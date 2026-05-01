#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Start BLE scanning runtime.
 *
 * Safe to call once from app_main() after:
 *   - NVS init
 *   - Wi-Fi init
 *   - http_sender_init()
 *   - optional NTP init
 *
 * Current implementation uses passive NimBLE GAP scanning with duplicate
 * filtering disabled.
 */
void ble_scan_start(void);

/*
 * Set scanner reporting mode.
 *
 * Valid values:
 *   0  = rotate reported channel label 37 -> 38 -> 39
 *   37 = report channel label 37
 *   38 = report channel label 38
 *   39 = report channel label 39
 *
 * Important:
 *   In the current firmware, this controls a SOFTWARE LABEL only.
 *   It does not force the ESP32/NimBLE controller to listen on a specific
 *   BLE advertising RF channel, because the standard NimBLE scan callback
 *   used here does not expose the true received channel.
 */
void ble_set_scan_mode(uint8_t mode);

/*
 * Set whether scan results are forwarded to the HTTP sender.
 *
 * Valid values:
 *   0 = idle/drop scan events
 *   1 = active/enqueue scan events
 *
 * The BLE scanner itself may continue running in the background; this state
 * controls whether discovered packets are forwarded.
 */
void ble_set_system_state(uint8_t state);

#ifdef __cplusplus
}
#endif
