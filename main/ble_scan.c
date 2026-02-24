#include "esp_log.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "esp_bt.h"
#include "esp_timer.h"
#include "nimble/nimble_port.h"
#include "nimble/nimble_port_freertos.h"
#include "host/ble_hs.h"
#include "host/ble_gap.h"
#include "adv_parser.h"
#include "http_sender.h"
#include "ntp_time.h"
#include "scanner_config.h"
#include "ble_scan.h" // Added this to access our new setter signatures
#include <stdbool.h>
#include <string.h>
#include <stdio.h>

static const char *TAG = "BLE_SCAN";

// --- DYNAMIC GLOBALS ---
volatile uint8_t g_active_channel = 37; 
volatile uint8_t g_scan_mode = 0; 
volatile uint8_t g_system_state = 1; // 1 = Active, 0 = Idle

// --- SETTERS FOR CMD_SERVER ---
void ble_set_scan_mode(uint8_t mode) {
    g_scan_mode = mode;
    ESP_LOGI(TAG, "Scan mode dynamically updated to: %d", mode);
}

void ble_set_system_state(uint8_t state) {
    g_system_state = state;
    ESP_LOGI(TAG, "System state dynamically updated to: %s", state ? "ACTIVE" : "IDLE");
}

// NimBLE units are 0.625ms. 1600 * 0.625 = 1000ms (1 second).
#define DWELL_INTERVAL 1600 
#define DWELL_WINDOW   1520 // 950ms active, 50ms gap for processing

/**
 * Mirror the hardware's natural hopping.
 */
void ble_channel_sequencer_task(void *pv) {
    ESP_LOGI(TAG, "Software sequencer started.");
    while(1) {
        vTaskDelay(pdMS_TO_TICKS(1000));
        
        if (g_scan_mode != 0) {
            // Fixed mode: force the active channel to the requested mode
            g_active_channel = g_scan_mode;
        } else {
            // Auto mode: Follow natural hardware rotation: 37 -> 38 -> 39
            if (g_active_channel == 37)      g_active_channel = 38;
            else if (g_active_channel == 38) g_active_channel = 39;
            else                             g_active_channel = 37;
        }
        ESP_LOGD(TAG, "Hardware considered on Channel %d", g_active_channel);
    }
}

static int gap_event(struct ble_gap_event *ev, void *arg) {
    if (ev->type == BLE_GAP_EVENT_DISC) {
        // --- NEW: Drop packet if system is IDLE ---
        if (g_system_state == 0) return 0;

        const struct ble_gap_disc_desc *d = &ev->disc;
        ble_minimal_event_t hev = {0};
        
        memcpy(hev.addr, d->addr.val, 6);
        hev.addr_type = d->addr.type;
        hev.adv_type  = d->event_type;
        hev.rssi      = (int8_t)d->rssi;
        hev.channel   = g_active_channel; // Correctly tag for localization

        hev.payload_len = (d->length_data > 31) ? 31 : d->length_data;
        memcpy(hev.payload, d->data, hev.payload_len);

        hev.timestamp_epoch_us = get_time_us();
        hev.timestamp_mono_us  = esp_timer_get_time();

        (void)http_sender_enqueue(&hev);
    }
    return 0;
}

static void nimble_host_task(void *param) {
    nimble_port_run();
    nimble_port_freertos_deinit();
}

void ble_scan_start(void) {
    nvs_handle_t h;
    if (nvs_open("storage", NVS_READONLY, &h) == ESP_OK) {
        uint8_t initial_mode;
        // Load initial mode from NVS if available
        if (nvs_get_u8(h, "scan_mode", &initial_mode) == ESP_OK) {
            g_scan_mode = initial_mode;
        }
        nvs_close(h);
    }

    ESP_ERROR_CHECK(esp_bt_controller_mem_release(ESP_BT_MODE_CLASSIC_BT));
    nimble_port_init();
    nimble_port_freertos_init(nimble_host_task);

    while (!ble_hs_synced()) vTaskDelay(pdMS_TO_TICKS(10));

    // STARTING POINT: Reset to 37 because hardware always starts there.
    g_active_channel = 37;

    struct ble_gap_disc_params p = {
        .passive = 1,
        .itvl = DWELL_INTERVAL, // 1000ms
        .window = DWELL_WINDOW, // 950ms
        .filter_policy = 0,
        .limited = 0,
        .filter_duplicates = 0
    };

    int rc = ble_gap_disc(BLE_OWN_ADDR_PUBLIC, BLE_HS_FOREVER, &p, gap_event, NULL);
    if (rc == 0) {
        ESP_LOGI(TAG, "BLE Scan Started (1s Dwell per channel)");
        xTaskCreate(ble_channel_sequencer_task, "ble_sync", 2048, NULL, 5, NULL);
    } else {
        ESP_LOGE(TAG, "Scan failed rc=%d", rc);
    }
}