#include "esp_log.h"
#include "nvs_flash.h"
#include "esp_bt.h"
#include "esp_timer.h"

#include "nimble/nimble_port.h"
#include "nimble/nimble_port_freertos.h"
#include "host/ble_hs.h"
#include "host/ble_gap.h"

#include "adv_parser.h"
#include "http_sender.h"

// --- NEW INCLUDES ---
#include "ntp_time.h"       // For get_time_us()
#include "scanner_config.h" // For SCANNER_ID
// --------------------

#include <stdbool.h>
#include <string.h>
#include <stdio.h>

static const char *TAG = "BLE_SCAN";

#define BLE_ADDR_STR_LEN 18

// --- PERFORMANCE TUNING CONSTANTS ---
// NimBLE uses 0.625ms units.
// 160 * 0.625 = 100ms (Total Cycle)
// 120 * 0.625 = 75ms  (Listening Window)
// This leaves 25ms for Wi-Fi to send BATCH_SIZE 50 without fighting.
#define SCAN_INTERVAL 160
#define SCAN_WINDOW   120
// ------------------------------------

static void addr_to_str(const uint8_t *addr, char *out)
{
    snprintf(out, BLE_ADDR_STR_LEN,
             "%02X:%02X:%02X:%02X:%02X:%02X",
             addr[5], addr[4], addr[3],
             addr[2], addr[1], addr[0]);
}

static int gap_event(struct ble_gap_event *ev, void *arg)
{
    if (ev->type == BLE_GAP_EVENT_DISC) {
        const struct ble_gap_disc_desc *d = &ev->disc;

        ble_minimal_event_t hev = {0};
        
        // 1. Copy raw metadata
        memcpy(hev.addr, d->addr.val, 6);
        hev.addr_type = d->addr.type;
        hev.adv_type  = d->event_type;
        hev.rssi      = (int8_t)d->rssi;
        hev.channel   = 0;
        // 2. Copy raw payload
        hev.payload_len = (d->length_data > 31) ? 31 : d->length_data;
        memcpy(hev.payload, d->data, hev.payload_len);

        // 3. Timestamps
        hev.timestamp_epoch_us = get_time_us();
        hev.timestamp_mono_us  = esp_timer_get_time();

        // 4. Enqueue (non-blocking)
        (void)http_sender_enqueue(&hev);
        return 0;
    }

    if (ev->type == BLE_GAP_EVENT_DISC_COMPLETE) {
        struct ble_gap_disc_params p = {
            .passive = 1,
            .itvl = SCAN_INTERVAL, // UPDATED: 100ms cycle
            .window = SCAN_WINDOW, // UPDATED: 75ms scan, 25ms Wi-Fi gap
            .filter_policy = 0,
            .limited = 0,
            .filter_duplicates = 0
        };
        ble_gap_disc(BLE_OWN_ADDR_PUBLIC, BLE_HS_FOREVER, &p, gap_event, NULL);
        return 0;
    }
    return 0;
}

static void nimble_host_task(void *param)
{
    (void)param;
    nimble_port_run();
    nimble_port_freertos_deinit();
}

void ble_scan_start(void)
{
    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_ERROR_CHECK(esp_bt_controller_mem_release(ESP_BT_MODE_CLASSIC_BT));

    nimble_port_init();
    ble_hs_cfg.reset_cb = NULL;
    ble_hs_cfg.sync_cb  = NULL;

    nimble_port_freertos_init(nimble_host_task);

    while (!ble_hs_synced()) {
        vTaskDelay(pdMS_TO_TICKS(10));
    }

    struct ble_gap_disc_params p = {
        .passive = 1,
        .itvl = SCAN_INTERVAL, // UPDATED: Match new coexistence timing
        .window = SCAN_WINDOW, // UPDATED: Match new coexistence timing
        .filter_policy = 0,
        .limited = 0,
        .filter_duplicates = 0
    };

    int rc = ble_gap_disc(BLE_OWN_ADDR_PUBLIC, BLE_HS_FOREVER, &p, gap_event, NULL);
    if (rc) {
        ESP_LOGE(TAG, "ble_gap_disc start failed rc=%d", rc);
    } else {
        ESP_LOGI(TAG, "BLE scan started (75ms Window / 100ms Interval)");
        ESP_LOGI(TAG, "25ms gap reserved for Wi-Fi Batching (Size: 50)");
    }
}