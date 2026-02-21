#include "http_sender.h"
#include "wifi_config.h"

#include <string.h>
#include <stdio.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "esp_log.h"
#include "esp_http_client.h"

static const char *TAG = "HTTP_SENDER";

#define HTTP_QUEUE_LEN  256
#define HTTP_TASK_STACK 4096
#define HTTP_TASK_PRIO  5

static QueueHandle_t s_q = NULL;

static esp_err_t http_event_handler(esp_http_client_event_t *evt) {
    // We don't need body parsing; keep minimal
    (void)evt;
    return ESP_OK;
}

static void http_sender_task(void *arg) {
    (void)arg;

    esp_http_client_config_t cfg = {
        .url = PC_INGEST_URL,
        .method = HTTP_METHOD_POST,
        .timeout_ms = 2000,
        .event_handler = http_event_handler,
        .keep_alive_enable = true,
    };

    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    if (!client) {
        ESP_LOGE(TAG, "esp_http_client_init failed");
        vTaskDelete(NULL);
        return;
    }

    esp_http_client_set_header(client, "Content-Type", "application/json");

    ble_http_event_t ev;
    char body[512];

    while (1) {
        if (xQueueReceive(s_q, &ev, portMAX_DELAY) != pdTRUE) continue;

        // JSON (must match PC receiver schema)
        // Note: name may contain quotes; keep it safe by truncating at first quote/backslash.
        // (Minimal safety. If you expect arbitrary UTF-8/quotes, we can add full escaping.)
        char safe_name[64];
        strncpy(safe_name, ev.name, sizeof(safe_name) - 1);
        safe_name[sizeof(safe_name) - 1] = '\0';
        for (int i = 0; safe_name[i]; i++) {
            if (safe_name[i] == '"' || safe_name[i] == '\\') {
                safe_name[i] = '\0';
                break;
            }
        }

        int n = snprintf(
            body, sizeof(body),
            "{"
              "\"mac\":\"%s\","
              "\"rssi\":%d,"
              "\"name\":\"%s\","
              "\"txpwr\":%d,"
              "\"mfg_id\":%u,"
              "\"adv_len\":%u,"
              "\"has_service_uuid\":%d,"
              "\"n_services_16\":%u,"
              "\"n_services_128\":%u,"
              "\"mfg_data\":\"%s\","  // <--- NEW FIELD ADDED HERE
              "\"timestamp_epoch_us\":%lld,"
              "\"timestamp_mono_us\":%lld,"
              "\"scanner\":\"%s\""
            "}",
            ev.mac,
            (int)ev.rssi,
            safe_name,
            (int)ev.txpwr,
            (unsigned)ev.mfg_id,
            (unsigned)ev.adv_len,
            ev.has_service_uuid ? 1 : 0,
            (unsigned)ev.n_services_16,
            (unsigned)ev.n_services_128,
            ev.mfg_data_hex,         // <--- NEW ARGUMENT ADDED HERE
            (long long)ev.timestamp_epoch_us,
            (long long)ev.timestamp_mono_us,     
            ev.scanner
        );

        if (n <= 0 || n >= (int)sizeof(body)) {
            ESP_LOGW(TAG, "JSON build failed/overflow; dropped");
            continue;
        }

        esp_http_client_set_post_field(client, body, n);

        esp_err_t err = esp_http_client_perform(client);
        if (err != ESP_OK) {
            ESP_LOGW(TAG, "POST failed: %s", esp_err_to_name(err));
            // Drop and continue (don't block BLE scanning)
            continue;
        }

        int code = esp_http_client_get_status_code(client);
        if (code < 200 || code >= 300) {
            ESP_LOGW(TAG, "POST HTTP %d", code);
        }
    }
}

void http_sender_init(void) {
    if (s_q) return;

    s_q = xQueueCreate(HTTP_QUEUE_LEN, sizeof(ble_http_event_t));
    if (!s_q) {
        ESP_LOGE(TAG, "Queue create failed");
        return;
    }

    xTaskCreate(http_sender_task, "http_sender", HTTP_TASK_STACK, NULL, HTTP_TASK_PRIO, NULL);
    ESP_LOGI(TAG, "HTTP sender ready -> %s", PC_INGEST_URL);
}

int http_sender_enqueue(const ble_http_event_t *ev) {
    if (!s_q || !ev) return 0;
    return (xQueueSend(s_q, ev, 0) == pdTRUE) ? 1 : 0;
}