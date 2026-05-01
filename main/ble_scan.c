#include "esp_log.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "esp_bt.h"
#include "esp_timer.h"

#include "nimble/nimble_port.h"
#include "nimble/nimble_port_freertos.h"
#include "host/ble_hs.h"
#include "host/ble_gap.h"

#include "http_sender.h"
#include "ntp_time.h"
#include "scanner_config.h"
#include "ble_scan.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

static const char *TAG = "BLE_SCAN";

/*
 * Important:
 * NimBLE GAP scan results on ESP-IDF do not expose the real advertising RF channel
 * of each received packet.
 *
 * g_reported_channel is therefore only a SOFTWARE LABEL used by the current control
 * panel / calibration flow. It must not be treated as verified RF channel metadata
 * by the tracking algorithm unless the firmware is later changed to use a lower-level
 * sniffer API that exposes the true channel.
 *
 * mode = 0  -> rotate reported label 37 -> 38 -> 39
 * mode = 37 -> report label 37
 * mode = 38 -> report label 38
 * mode = 39 -> report label 39
 */
static volatile uint8_t g_reported_channel = 37;
static volatile uint8_t g_scan_mode = 0;
static volatile uint8_t g_system_state = 1;  // 1 = active/stream events, 0 = idle/drop events

static TaskHandle_t s_channel_task_handle = NULL;
static bool s_scan_started = false;

/* NimBLE scan units are 0.625 ms. */
#define BLE_SCAN_INTERVAL_UNITS 1600  /* 1000 ms */
#define BLE_SCAN_WINDOW_UNITS   1520  /* 950 ms */

/* Keep the software label cadence aligned with the previous behavior. */
#define CHANNEL_LABEL_PERIOD_MS 1000

static bool is_valid_scan_mode(uint8_t mode)
{
    return (mode == 0 || mode == 37 || mode == 38 || mode == 39);
}

static uint8_t next_channel_label(uint8_t ch)
{
    if (ch == 37) return 38;
    if (ch == 38) return 39;
    return 37;
}

/* --- Setters used by cmd_server.c --- */

void ble_set_scan_mode(uint8_t mode)
{
    if (!is_valid_scan_mode(mode)) {
        ESP_LOGW(TAG, "Ignoring invalid scan mode %u. Valid values: 0, 37, 38, 39", (unsigned)mode);
        return;
    }

    g_scan_mode = mode;

    if (mode != 0) {
        g_reported_channel = mode;
        ESP_LOGW(TAG,
                 "Reported channel label set to %u. This is NOT verified RF channel data.",
                 (unsigned)mode);
    } else {
        ESP_LOGI(TAG, "Reported channel label rotation enabled: 37 -> 38 -> 39");
    }
}

void ble_set_system_state(uint8_t state)
{
    g_system_state = state ? 1 : 0;
    ESP_LOGI(TAG, "System state updated to: %s", g_system_state ? "ACTIVE" : "IDLE");
}

/*
 * Software label sequencer.
 * This does not force the radio to a BLE advertising channel. It only labels events.
 */
static void ble_channel_label_task(void *pv)
{
    (void)pv;

    ESP_LOGI(TAG, "Software channel-label sequencer started.");

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(CHANNEL_LABEL_PERIOD_MS));

        uint8_t mode = g_scan_mode;
        if (mode == 0) {
            g_reported_channel = next_channel_label(g_reported_channel);
        } else if (is_valid_scan_mode(mode)) {
            g_reported_channel = mode;
        } else {
            g_scan_mode = 0;
            g_reported_channel = next_channel_label(g_reported_channel);
        }

        ESP_LOGD(TAG, "Current reported channel label: %u", (unsigned)g_reported_channel);
    }
}

static void start_channel_label_task_once(void)
{
    if (s_channel_task_handle != NULL) {
        return;
    }

    BaseType_t ok = xTaskCreate(
        ble_channel_label_task,
        "ble_ch_label",
        2048,
        NULL,
        5,
        &s_channel_task_handle
    );

    if (ok != pdPASS) {
        ESP_LOGE(TAG, "Failed to start channel-label task");
        s_channel_task_handle = NULL;
    }
}

static void load_initial_scan_mode_from_nvs(void)
{
    nvs_handle_t h;
    if (nvs_open("storage", NVS_READONLY, &h) != ESP_OK) {
        return;
    }

    uint8_t initial_mode = 0;
    if (nvs_get_u8(h, "scan_mode", &initial_mode) == ESP_OK) {
        if (is_valid_scan_mode(initial_mode)) {
            g_scan_mode = initial_mode;
            if (initial_mode != 0) {
                g_reported_channel = initial_mode;
            }
            ESP_LOGI(TAG, "Loaded scan mode from NVS: %u", (unsigned)initial_mode);
        } else {
            ESP_LOGW(TAG, "Ignoring invalid NVS scan_mode: %u", (unsigned)initial_mode);
        }
    }

    nvs_close(h);
}

static void fill_event_from_disc_desc(ble_minimal_event_t *out,
                                      const struct ble_gap_disc_desc *d)
{
    memset(out, 0, sizeof(*out));

    memcpy(out->addr, d->addr.val, sizeof(out->addr));
    out->addr_type = d->addr.type;
    out->adv_type = d->event_type;
    out->rssi = (int8_t)d->rssi;

    /*
     * Software label only. See file-level comment.
     * The downstream system may keep this field for compatibility, but should use it
     * as weak metadata unless true RF channel reporting is implemented.
     */
    out->channel = g_reported_channel;

    out->payload_len = (d->length_data > sizeof(out->payload))
                       ? sizeof(out->payload)
                       : d->length_data;

    if (out->payload_len > 0) {
        memcpy(out->payload, d->data, out->payload_len);
    }

    out->timestamp_epoch_us = get_time_us();
    out->timestamp_mono_us = esp_timer_get_time();
}

static int gap_event(struct ble_gap_event *ev, void *arg)
{
    (void)arg;

    switch (ev->type) {
        case BLE_GAP_EVENT_DISC: {
            if (g_system_state == 0) {
                return 0;
            }

            const struct ble_gap_disc_desc *d = &ev->disc;
            ble_minimal_event_t hev;

            fill_event_from_disc_desc(&hev, d);

            if (http_sender_enqueue(&hev) != 0) {
                ESP_LOGD(TAG, "HTTP sender queue full; event dropped");
            }

            return 0;
        }

        case BLE_GAP_EVENT_DISC_COMPLETE: {
            /*
             * BLE_HS_FOREVER scans should not normally complete, but restart if the
             * controller reports completion for any reason.
             */
            ESP_LOGW(TAG, "BLE discovery complete event received; restarting scan");

            struct ble_gap_disc_params p = {
                .passive = 1,
                .itvl = BLE_SCAN_INTERVAL_UNITS,
                .window = BLE_SCAN_WINDOW_UNITS,
                .filter_policy = 0,
                .limited = 0,
                .filter_duplicates = 0,
            };

            int rc = ble_gap_disc(BLE_OWN_ADDR_PUBLIC, BLE_HS_FOREVER, &p, gap_event, NULL);
            if (rc != 0 && rc != BLE_HS_EALREADY) {
                ESP_LOGE(TAG, "BLE scan restart failed rc=%d", rc);
            }
            return 0;
        }

        default:
            return 0;
    }
}

static void nimble_host_task(void *param)
{
    (void)param;
    nimble_port_run();
    nimble_port_freertos_deinit();
}

void ble_scan_start(void)
{
    if (s_scan_started) {
        ESP_LOGW(TAG, "ble_scan_start called more than once; ignoring");
        return;
    }
    s_scan_started = true;

    load_initial_scan_mode_from_nvs();

    /*
     * Ensure the HTTP sender queue/task exists before scan results arrive.
     * If it was already initialized elsewhere, http_sender_init() is safe to call again.
     */
    http_sender_init();

    esp_err_t rel = esp_bt_controller_mem_release(ESP_BT_MODE_CLASSIC_BT);
    if (rel != ESP_OK && rel != ESP_ERR_INVALID_STATE) {
        ESP_LOGW(TAG, "Classic BT memory release returned: %s", esp_err_to_name(rel));
    }

    ESP_ERROR_CHECK(nimble_port_init());
    nimble_port_freertos_init(nimble_host_task);

    while (!ble_hs_synced()) {
        vTaskDelay(pdMS_TO_TICKS(10));
    }

    start_channel_label_task_once();

    struct ble_gap_disc_params p = {
        .passive = 1,  /* passive scan: no SCAN_REQ; preserves non-invasive collection */
        .itvl = BLE_SCAN_INTERVAL_UNITS,
        .window = BLE_SCAN_WINDOW_UNITS,
        .filter_policy = 0,
        .limited = 0,
        .filter_duplicates = 0,
    };

    int rc = ble_gap_disc(BLE_OWN_ADDR_PUBLIC, BLE_HS_FOREVER, &p, gap_event, NULL);
    if (rc == 0 || rc == BLE_HS_EALREADY) {
        ESP_LOGI(TAG, "BLE scan started: passive, duplicate filter OFF");
        ESP_LOGW(TAG, "Event channel field is a software label, not verified RF channel.");
    } else {
        ESP_LOGE(TAG, "BLE scan start failed rc=%d", rc);
    }
}
