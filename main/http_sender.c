#include "http_sender.h"
#include "scanner_config.h" // Prioritizing integer SCANNER_ID
#include "esp_system.h"
#include <string.h>
#include <stdio.h>
#include "mbedtls/base64.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "esp_timer.h"
#include "esp_log.h"
#include "esp_http_client.h"

#define BATCH_SIZE      50      // Reduced to 50 for more frequent, smaller bursts
#define FLUSH_MS        100     
#define HTTP_QUEUE_LEN  512     // Large queue to prevent drops during Wi-Fi lag
#define HTTP_TASK_STACK 10240
#define HTTP_TASK_PRIO  5
#define JSON_BUF_SIZE   (BATCH_SIZE * 512)

static const char *TAG = "HTTP_SENDER";
static QueueHandle_t s_q = NULL;
static uint32_t s_enq_ok = 0;
static uint32_t s_enq_drop = 0;

static esp_err_t http_event_handler(esp_http_client_event_t *evt) {
    return ESP_OK;
}

static void http_sender_task(void *arg) {
    esp_http_client_config_t cfg = {
        .url = "http://192.168.1.19:8000/api/ble/ingest",
        .method = HTTP_METHOD_POST,
        .timeout_ms = 3000,     // 3-second timeout to handle Wi-Fi jitter
        .event_handler = http_event_handler,
        .keep_alive_enable = true,
    };

    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    esp_http_client_set_header(client, "Content-Type", "application/json");

    char *json_buffer = malloc(JSON_BUF_SIZE);
    ble_minimal_event_t *batch = malloc(sizeof(ble_minimal_event_t) * BATCH_SIZE);
    
    int batch_count = 0;
    int64_t last_flush = esp_timer_get_time();
    int64_t last_stats_log = 0;

    while (1) {
        ble_minimal_event_t ev;
        if (xQueueReceive(s_q, &ev, pdMS_TO_TICKS(10)) == pdTRUE) {
            batch[batch_count++] = ev;
        }

        int64_t now = esp_timer_get_time();
        if (now - last_stats_log > 1000000) {
            last_stats_log = now;
            ESP_LOGI(TAG, "enq_ok=%u drop=%u q_free=%u", s_enq_ok, s_enq_drop, (unsigned)uxQueueSpacesAvailable(s_q));
        }

        if (batch_count > 0 && (batch_count >= BATCH_SIZE || (now - last_flush) > (FLUSH_MS * 1000))) {
            // Using %d for the integer SCANNER_ID from scanner_config.h
            int pos = snprintf(json_buffer, JSON_BUF_SIZE, "{\"scanner\":%d,\"events\":[", (int)SCANNER_ID);
            
            for (int i = 0; i < batch_count; i++) {
                char b64_payload[64];
                size_t b64_len;
                mbedtls_base64_encode((unsigned char *)b64_payload, sizeof(b64_payload), &b64_len, 
                                      batch[i].payload, batch[i].payload_len);
                b64_payload[b64_len] = '\0';

                int space_left = JSON_BUF_SIZE - pos - 5;
                int written = snprintf(json_buffer + pos, space_left,
                    "%s{\"a\":\"%02X%02X%02X%02X%02X%02X\",\"at\":%d,\"et\":%d,\"r\":%d,\"ts\":%lld,\"p\":\"%s\"}",
                    (i == 0) ? "" : ",",
                    batch[i].addr[5], batch[i].addr[4], batch[i].addr[3], 
                    batch[i].addr[2], batch[i].addr[1], batch[i].addr[0],
                    batch[i].addr_type, batch[i].adv_type, (int)batch[i].rssi,
                    (long long)batch[i].timestamp_epoch_us, b64_payload);
                if (written > 0 && written < space_left) pos += written;
            }
            strcpy(json_buffer + pos, "]}");

            esp_http_client_set_post_field(client, json_buffer, strlen(json_buffer));
            esp_err_t err = esp_http_client_perform(client);
            if (err != ESP_OK) ESP_LOGW(TAG, "POST failed: %s", esp_err_to_name(err));
            
            batch_count = 0;
            last_flush = now;
        }
    }
}

void http_sender_init(void) {
    if (s_q) return;
    s_q = xQueueCreate(HTTP_QUEUE_LEN, sizeof(ble_minimal_event_t));
    xTaskCreate(http_sender_task, "http_sender", HTTP_TASK_STACK, NULL, HTTP_TASK_PRIO, NULL);
}

int http_sender_enqueue(const ble_minimal_event_t *ev) {
    if (!s_q) return -1;
    if (xQueueSend(s_q, ev, 0) == pdTRUE) {
        s_enq_ok++;
        return 0;
    } else {
        s_enq_drop++;
        return -1;
    }
}