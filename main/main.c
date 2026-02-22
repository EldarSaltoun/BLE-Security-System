#include "ble_scan.h"
#include "http_sender.h"
#include "wifi_config.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"

#include "esp_log.h"
#include "nvs_flash.h"

#include "esp_netif.h"
#include "esp_event.h"
#include "esp_wifi.h"

// --- NEW INCLUDE ---
#include "ntp_time.h"
// -------------------

static const char *TAG = "MAIN";
static EventGroupHandle_t s_wifi_ev = NULL;

#define WIFI_CONNECTED_BIT BIT0

static void wifi_event_handler(void *arg, esp_event_base_t event_base,
                               int32_t event_id, void *event_data) {
    (void)arg;
    (void)event_data;

    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW(TAG, "Wi-Fi disconnected, retrying...");
        esp_wifi_connect();
        xEventGroupClearBits(s_wifi_ev, WIFI_CONNECTED_BIT);
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        xEventGroupSetBits(s_wifi_ev, WIFI_CONNECTED_BIT);
        ESP_LOGI(TAG, "Wi-Fi got IP");
    }
}

static void wifi_init_sta(void) {
    s_wifi_ev = xEventGroupCreate();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

    wifi_config_t wifi_config = {0};
    strncpy((char *)wifi_config.sta.ssid, WIFI_SSID, sizeof(wifi_config.sta.ssid) - 1);
    strncpy((char *)wifi_config.sta.password, WIFI_PASS, sizeof(wifi_config.sta.password) - 1);

    // FIX 1: Relax threshold for mobile hotspots (which often toggle between WPA2/WPA3)
    wifi_config.sta.threshold.authmode = WIFI_AUTH_OPEN;
    wifi_config.sta.pmf_cfg.capable = true;
    wifi_config.sta.pmf_cfg.required = false;

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    
    ESP_LOGI(TAG, "Starting Wi-Fi and disabling Power Save for high-throughput...");
    ESP_ERROR_CHECK(esp_wifi_start());

    // FIX 2: FORCE RADIO ALWAYS-ON
    // This stops the 'DELBA' drops and keeps latency low for your large batches.
    esp_wifi_set_ps(WIFI_PS_NONE);

    ESP_LOGI(TAG, "Connecting to Wi-Fi SSID=%s ...", WIFI_SSID);
    xEventGroupWaitBits(s_wifi_ev, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
    ESP_LOGI(TAG, "Wi-Fi connected");
}

void app_main(void) {
    // NVS needed for Wi-Fi + NimBLE
    esp_err_t nvs = nvs_flash_init();
    if (nvs == ESP_ERR_NVS_NO_FREE_PAGES || nvs == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    } else {
        ESP_ERROR_CHECK(nvs);
    }

    // 1. Start Wi-Fi and wait for connection
    wifi_init_sta();

    // 2. --- NEW: SYNC TIME VIA NTP ---
    time_sync_init(); 

    // 3. Start HTTP sender task/queue
    http_sender_init();

    // 4. Start BLE scanner
    ble_scan_start();

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}