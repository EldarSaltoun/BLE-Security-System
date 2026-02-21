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

// Helper to format MAC address
static void addr_to_str(const uint8_t *addr, char *out)
{
    snprintf(out, BLE_ADDR_STR_LEN,
             "%02X:%02X:%02X:%02X:%02X:%02X",
             addr[5], addr[4], addr[3],
             addr[2], addr[1], addr[0]);
}

static int gap_event(struct ble_gap_event *ev, void *arg)
{
    (void)arg;

    if (ev->type == BLE_GAP_EVENT_DISC) {
        const struct ble_gap_disc_desc *d = &ev->disc;

        char mac[BLE_ADDR_STR_LEN];
        addr_to_str(d->addr.val, mac);

        char name[64];
        adv_find_name(d->data, d->length_data, name, sizeof(name));

        int8_t txpwr;
        uint16_t mfg_id;
        
        // Buffer for raw hex data (metrics)
        char mfg_hex[64] = ""; 

        // Extract metrics
        adv_extract_metrics(d->data, d->length_data, &txpwr, &mfg_id, mfg_hex, sizeof(mfg_hex));

        bool has_service_uuid;
        uint8_t n_services_16;
        uint8_t n_services_128;
        adv_extract_services(d->data, d->length_data,
                             &has_service_uuid,
                             &n_services_16,
                             &n_services_128);

        // --- CHANGE 1: Use Real-World NTP Time ---
        // Old: int64_t ts_us = esp_timer_get_time();
        int64_t t_epoch_us = get_time_us();        // NTP epoch time
        int64_t t_mono_us  = esp_timer_get_time(); // monotonic since boot
        // -----------------------------------------

        // --- CHANGE 2: Generate Scanner Name from Config ---
        char scanner_name[16];
        snprintf(scanner_name, sizeof(scanner_name), "ESP32-S3-%02d", SCANNER_ID);
        // ---------------------------------------------------

        ESP_LOGI(TAG,
         "BLECSV: mac=%s,rssi=%d,name=%s,txpwr=%d,mfg=0x%04X,"
         "adv_len=%u,has_svc=%d,svc16=%u,svc128=%u,"
         "t_epoch_us=%lld,t_mono_us=%lld,scanner=%s",
         mac, d->rssi, (*name) ? name : "",
         txpwr, mfg_id,
         (unsigned)d->length_data,
         has_service_uuid,
         n_services_16,
         n_services_128,
         (long long)t_epoch_us,
         (long long)t_mono_us,
         scanner_name); // Log the dynamic name

        /* ---- enqueue for HTTP sender (popup/PC ingest) ---- */
        ble_http_event_t hev = {0};
        strncpy(hev.mac, mac, sizeof(hev.mac) - 1);
        hev.rssi = (int8_t)d->rssi;
        strncpy(hev.name, name, sizeof(hev.name) - 1);
        hev.txpwr = txpwr;
        hev.mfg_id = mfg_id;
        hev.adv_len = (uint8_t)d->length_data;

        hev.has_service_uuid = has_service_uuid;
        hev.n_services_16 = n_services_16;
        hev.n_services_128 = n_services_128;

        // Copy hex string to HTTP event
        strncpy(hev.mfg_data_hex, mfg_hex, sizeof(hev.mfg_data_hex) - 1);

        hev.timestamp_epoch_us = t_epoch_us;
        hev.timestamp_mono_us  = t_mono_us;
        
        // Use the dynamic scanner name in the HTTP packet
        strncpy(hev.scanner, scanner_name, sizeof(hev.scanner) - 1);

        (void)http_sender_enqueue(&hev);

        return 0;
    }

    if (ev->type == BLE_GAP_EVENT_DISC_COMPLETE) {
        struct ble_gap_disc_params p = {
            .passive = 1,
            .itvl = 0x0010,
            .window = 0x0010,
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
        .itvl = 0x0010,
        .window = 0x0010,
        .filter_policy = 0,
        .limited = 0,
        .filter_duplicates = 0
    };

    int rc = ble_gap_disc(BLE_OWN_ADDR_PUBLIC, BLE_HS_FOREVER, &p, gap_event, NULL);
    if (rc) {
        ESP_LOGE(TAG, "ble_gap_disc start failed rc=%d", rc);
    } else {
        ESP_LOGI(TAG, "BLE scan started (passive, dup-filter OFF)");
    }
}