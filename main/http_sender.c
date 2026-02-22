#include "http_sender.h"
#include "wifi_config.h"
#include "esp_system.h"
#include <string.h>
#include <stdio.h>
#include "mbedtls/base64.h"

#define BATCH_SIZE      100      // Number of events per HTTP POST
#define FLUSH_MS        100     // Max time to wait before sending a partial batch
#define JSON_BUF_SIZE   (BATCH_SIZE * 512) // Buffer for the full JSON array

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "esp_timer.h"

#include "esp_log.h"
#include "esp_http_client.h"

static const char *TAG = "HTTP_SENDER";

#define HTTP_QUEUE_LEN  256
#define HTTP_TASK_STACK 10240
#define HTTP_TASK_PRIO  5


static QueueHandle_t s_q = NULL;
// --- NEW: enqueue statistics ---
static uint32_t s_enq_ok   = 0;
static uint32_t s_enq_drop = 0;
// --------------------------------

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
        .timeout_ms = 500,
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

    // ALLOCATE BUFFERS ON THE HEAP
    char *json_buffer = malloc(JSON_BUF_SIZE);
    ble_minimal_event_t *batch = malloc(sizeof(ble_minimal_event_t) * BATCH_SIZE);

    if (!json_buffer || !batch) {
        ESP_LOGE(TAG, "Failed to allocate JSON/Batch buffers");
        if (json_buffer) free(json_buffer);
        if (batch) free(batch);
        esp_http_client_cleanup(client);
        vTaskDelete(NULL);
        return;
    }

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
            ESP_LOGI(TAG, "enq_ok=%u drop=%u q_free=%u", 
                     s_enq_ok, s_enq_drop, uxQueueSpacesAvailable(s_q));
        }

        if (batch_count > 0 && (batch_count >= BATCH_SIZE || (now - last_flush) > (FLUSH_MS * 1000))) {
            
            // Note: Using %d for integer SCANNER_ID
            int pos = snprintf(json_buffer, JSON_BUF_SIZE, "{\"scanner\":%d,\"events\":[", (int)SCANNER_ID);

            for (int i = 0; i < batch_count; i++) {
                char b64_payload[64];
                size_t b64_len;
                
                mbedtls_base64_encode((unsigned char *)b64_payload, sizeof(b64_payload), &b64_len, 
                                      batch[i].payload, batch[i].payload_len);
                b64_payload[b64_len] = '\0';

                int space_left = JSON_BUF_SIZE - pos - 5; 
                if (space_left < 150) {
                    ESP_LOGW(TAG, "Buffer near capacity, truncating batch at %d", i);
                    break; 
                }

                int written = snprintf(json_buffer + pos, space_left,
                    "%s{\"a\":\"%02X%02X%02X%02X%02X%02X\",\"at\":%d,\"et\":%d,\"r\":%d,\"ts\":%lld,\"p\":\"%s\"}",
                    (i == 0) ? "" : ",",
                    batch[i].addr[5], batch[i].addr[4], batch[i].addr[3], 
                    batch[i].addr[2], batch[i].addr[1], batch[i].addr[0],
                    batch[i].addr_type, batch[i].adv_type, (int)batch[i].rssi,
                    (long long)batch[i].timestamp_epoch_us, b64_payload);

                if (written > 0 && written < space_left) {
                    pos += written;
                }
            }

            if (pos < JSON_BUF_SIZE - 3) {
                strcpy(json_buffer + pos, "]}");
            }

            esp_http_client_set_post_field(client, json_buffer, strlen(json_buffer));
            esp_err_t err = esp_http_client_perform(client);
            
            if (err != ESP_OK) {
                ESP_LOGW(TAG, "POST failed: %s", esp_err_to_name(err));
            }
            
            batch_count = 0;
            last_flush = now;
        }
    }
    // Infinite loop, but for completeness:
    free(json_buffer);
    free(batch);
    esp_http_client_cleanup(client);
}

void http_sender_init(void) {
    if (s_q) return;

    ESP_LOGI(TAG, "Free heap before queue: %u", (unsigned)esp_get_free_heap_size());
    s_q = xQueueCreate(HTTP_QUEUE_LEN, sizeof(ble_minimal_event_t));

    if (!s_q) {
        ESP_LOGE(TAG, "Queue create failed");
        return;
    }

    xTaskCreate(http_sender_task, "http_sender", HTTP_TASK_STACK, NULL, HTTP_TASK_PRIO, NULL);
    ESP_LOGI(TAG, "HTTP sender ready (Batched Mode) -> %s", PC_INGEST_URL);
}

int http_sender_enqueue(const ble_minimal_event_t *ev) {
    if (!s_q || !ev) return 0;

    if (xQueueSend(s_q, ev, 0) == pdTRUE) {
        s_enq_ok++;
        return 1;
    } else {
        s_enq_drop++;
        return 0;
    }
}