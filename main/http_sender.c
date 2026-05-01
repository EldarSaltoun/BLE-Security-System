#include "http_sender.h"
#include "wifi_config.h"
#include "scanner_config.h"

#include "esp_system.h"
#include "esp_timer.h"
#include "esp_log.h"
#include "esp_http_client.h"
#include "esp_err.h"

#include "driver/gpio.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

#include "nvs_flash.h"
#include "nvs.h"

#include "mbedtls/base64.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* --- Hardware Diagnostics Config --- */
#define STATUS_LED_GPIO 48

/* --- Working parameters --- */
#define BATCH_SIZE       50
#define FLUSH_MS         100
#define HTTP_QUEUE_LEN   512
#define HTTP_TASK_STACK  12288
#define HTTP_TASK_PRIO   5

/*
 * Worst case per event is small:
 *   MAC + metadata + timestamps + Base64 payload.
 * 31 raw payload bytes become 44 Base64 bytes.
 * 512 bytes/event is enough; keep a margin for scanner wrapper.
 */
#define JSON_BUF_SIZE    (BATCH_SIZE * 512)

/* 31 bytes legacy BLE payload -> 44 Base64 chars + NUL. Keep more for safety. */
#define B64_PAYLOAD_SIZE 96

static const char *TAG = "HTTP_SENDER";

static QueueHandle_t s_q = NULL;
static TaskHandle_t s_task_handle = NULL;

static volatile uint32_t s_enq_ok = 0;
static volatile uint32_t s_enq_drop = 0;
static volatile uint32_t s_post_ok = 0;
static volatile uint32_t s_post_fail = 0;

static char dynamic_url[160] = "";
static bool s_url_ready = false;
static bool s_url_needs_update = false;
static SemaphoreHandle_t url_lock = NULL;

static esp_err_t http_event_handler(esp_http_client_event_t *evt)
{
    (void)evt;
    return ESP_OK;
}

static void set_led(int level)
{
    gpio_set_level(STATUS_LED_GPIO, level ? 1 : 0);
}

static void init_status_led(void)
{
    gpio_reset_pin(STATUS_LED_GPIO);
    gpio_set_direction(STATUS_LED_GPIO, GPIO_MODE_OUTPUT);
    set_led(0);
}

static void blink_waiting_pattern(void)
{
    for (int i = 0; i < 5; i++) {
        set_led(1);
        vTaskDelay(pdMS_TO_TICKS(100));
        set_led(0);
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

static void copy_current_url(char *out, size_t out_len, bool *needs_update)
{
    if (!out || out_len == 0) return;

    out[0] = '\0';
    if (needs_update) *needs_update = false;

    if (!url_lock) return;

    xSemaphoreTake(url_lock, portMAX_DELAY);
    snprintf(out, out_len, "%s", dynamic_url);
    if (needs_update) {
        *needs_update = s_url_needs_update;
        s_url_needs_update = false;
    }
    xSemaphoreGive(url_lock);
}

/**
 * Allows the mDNS resolver to push a new PC receiver IP dynamically.
 */
void http_sender_update_ip(const char *ip)
{
    if (!ip || strlen(ip) == 0) {
        ESP_LOGW(TAG, "Ignoring empty receiver IP update");
        return;
    }

    if (!url_lock) {
        ESP_LOGW(TAG, "URL lock not ready yet; IP update ignored");
        return;
    }

    xSemaphoreTake(url_lock, portMAX_DELAY);
    snprintf(dynamic_url, sizeof(dynamic_url), "http://%s:8000/api/ble/ingest", ip);
    s_url_ready = true;
    s_url_needs_update = true;
    xSemaphoreGive(url_lock);

    ESP_LOGI(TAG, "Target URL updated via mDNS: %s", dynamic_url);
}

/**
 * Loads receiver IP from NVS as fallback when mDNS has not found the PC yet.
 */
static esp_err_t load_receiver_settings(void)
{
    if (!url_lock) return ESP_ERR_INVALID_STATE;

    nvs_handle_t h;
    esp_err_t err = nvs_open("storage", NVS_READONLY, &h);
    if (err != ESP_OK) return err;

    char pc_ip[32] = {0};
    size_t size = sizeof(pc_ip);

    err = nvs_get_str(h, "pc_ip", pc_ip, &size);
    nvs_close(h);

    if (err == ESP_OK && strlen(pc_ip) > 0) {
        xSemaphoreTake(url_lock, portMAX_DELAY);
        if (!s_url_ready) {
            snprintf(dynamic_url, sizeof(dynamic_url), "http://%s:8000/api/ble/ingest", pc_ip);
            s_url_ready = true;
            s_url_needs_update = true;
            ESP_LOGI(TAG, "Cached NVS receiver loaded: %s", dynamic_url);
        }
        xSemaphoreGive(url_lock);
        return ESP_OK;
    }

    return ESP_FAIL;
}

static bool is_url_ready(void)
{
    if (!url_lock) return false;

    xSemaphoreTake(url_lock, portMAX_DELAY);
    bool ready = s_url_ready;
    xSemaphoreGive(url_lock);

    return ready;
}

static int append_event_json(char *json_buffer,
                             int pos,
                             int json_buf_size,
                             const ble_minimal_event_t *ev,
                             bool first_event)
{
    char b64_payload[B64_PAYLOAD_SIZE] = {0};
    size_t b64_len = 0;

    size_t olen = 0;
    int b64_rc = mbedtls_base64_encode(
        (unsigned char *)b64_payload,
        sizeof(b64_payload) - 1,
        &olen,
        ev->payload,
        ev->payload_len
    );

    if (b64_rc == 0) {
        b64_len = olen;
        b64_payload[b64_len] = '\0';
    } else {
        /*
         * This should not happen for 31-byte payloads with B64_PAYLOAD_SIZE=96.
         * Send an empty payload rather than corrupt JSON.
         */
        ESP_LOGW(TAG, "Base64 encode failed rc=%d payload_len=%u", b64_rc, (unsigned)ev->payload_len);
        b64_payload[0] = '\0';
    }

    int space_left = json_buf_size - pos;
    if (space_left <= 1) {
        return -1;
    }

    /*
     * Keep compact names expected by pc_receiver.py:
     *   a  = MAC address, no colons
     *   at = BLE address type
     *   et = GAP event/advertisement type
     *   r  = RSSI
     *   c  = channel/software-channel-label
     *   ts = epoch timestamp in microseconds
     *   tm = monotonic timestamp in microseconds, useful for future timing logic
     *   p  = Base64 raw advertising payload
     */
    int written = snprintf(
        json_buffer + pos,
        space_left,
        "%s{\"a\":\"%02X%02X%02X%02X%02X%02X\","
        "\"at\":%u,\"et\":%u,\"r\":%d,\"c\":%u,"
        "\"ts\":%lld,\"tm\":%lld,\"p\":\"%s\"}",
        first_event ? "" : ",",
        ev->addr[5], ev->addr[4], ev->addr[3],
        ev->addr[2], ev->addr[1], ev->addr[0],
        (unsigned)ev->addr_type,
        (unsigned)ev->adv_type,
        (int)ev->rssi,
        (unsigned)ev->channel,
        (long long)ev->timestamp_epoch_us,
        (long long)ev->timestamp_mono_us,
        b64_payload
    );

    if (written <= 0 || written >= space_left) {
        ESP_LOGW(TAG, "JSON event append overflow: written=%d space_left=%d", written, space_left);
        return -1;
    }

    return pos + written;
}

static int build_batch_json(char *json_buffer,
                            int json_buf_size,
                            const ble_minimal_event_t *batch,
                            int batch_count)
{
    if (!json_buffer || json_buf_size <= 0 || !batch || batch_count <= 0) {
        return -1;
    }

    int pos = snprintf(json_buffer, json_buf_size,
                       "{\"scanner\":%d,\"events\":[", (int)SCANNER_ID);

    if (pos <= 0 || pos >= json_buf_size) {
        return -1;
    }

    int emitted = 0;
    for (int i = 0; i < batch_count; i++) {
        int new_pos = append_event_json(
            json_buffer,
            pos,
            json_buf_size,
            &batch[i],
            emitted == 0
        );

        if (new_pos < 0) {
            /*
             * Stop before corrupting JSON. The next flush will continue with future events;
             * we prefer dropping overflowed events over sending invalid JSON.
             */
            ESP_LOGW(TAG, "Stopped JSON batch early at event %d/%d", i, batch_count);
            break;
        }

        pos = new_pos;
        emitted++;
    }

    int space_left = json_buf_size - pos;
    int written = snprintf(json_buffer + pos, space_left, "]}");
    if (written <= 0 || written >= space_left || emitted == 0) {
        return -1;
    }

    pos += written;
    return pos;
}

static void wait_for_receiver_url(void)
{
    while (!is_url_ready()) {
        load_receiver_settings();

        if (is_url_ready()) {
            break;
        }

        ESP_LOGW(TAG, "Waiting for receiver IP configuration through mDNS or NVS...");
        blink_waiting_pattern();
        vTaskDelay(pdMS_TO_TICKS(2000));
    }
}

static void http_sender_task(void *arg)
{
    (void)arg;

    init_status_led();
    wait_for_receiver_url();
    set_led(1);

    char current_url[160] = {0};
    bool needs_update = false;
    copy_current_url(current_url, sizeof(current_url), &needs_update);

    esp_http_client_config_t cfg = {
        .url = current_url[0] ? current_url : "http://127.0.0.1:8000/api/ble/ingest",
        .method = HTTP_METHOD_POST,
        .timeout_ms = 3000,
        .event_handler = http_event_handler,
        .keep_alive_enable = true,
    };

    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    if (!client) {
        ESP_LOGE(TAG, "Failed to initialize HTTP client");
        vTaskDelete(NULL);
        return;
    }

    esp_http_client_set_header(client, "Content-Type", "application/json");

    char *json_buffer = (char *)malloc(JSON_BUF_SIZE);
    ble_minimal_event_t *batch = (ble_minimal_event_t *)malloc(sizeof(ble_minimal_event_t) * BATCH_SIZE);

    if (!json_buffer || !batch) {
        ESP_LOGE(TAG, "Failed to allocate HTTP buffers");
        free(json_buffer);
        free(batch);
        esp_http_client_cleanup(client);
        vTaskDelete(NULL);
        return;
    }

    int batch_count = 0;
    int64_t last_flush = esp_timer_get_time();
    int64_t last_stats_log = 0;

    ESP_LOGI(TAG, "HTTP sender task started. Target: %s", current_url);

    while (1) {
        copy_current_url(current_url, sizeof(current_url), &needs_update);
        if (needs_update && current_url[0]) {
            esp_http_client_set_url(client, current_url);
            ESP_LOGI(TAG, "HTTP client URL changed to: %s", current_url);
        }

        ble_minimal_event_t ev;
        if (batch_count < BATCH_SIZE &&
            xQueueReceive(s_q, &ev, pdMS_TO_TICKS(10)) == pdTRUE) {
            batch[batch_count++] = ev;
        }

        int64_t now = esp_timer_get_time();

        if (now - last_stats_log > 1000000) {
            last_stats_log = now;
            ESP_LOGI(TAG,
                     "enq_ok=%u drop=%u post_ok=%u post_fail=%u q_free=%u batch=%d",
                     (unsigned)s_enq_ok,
                     (unsigned)s_enq_drop,
                     (unsigned)s_post_ok,
                     (unsigned)s_post_fail,
                     (unsigned)uxQueueSpacesAvailable(s_q),
                     batch_count);
        }

        bool flush_by_size = (batch_count >= BATCH_SIZE);
        bool flush_by_time = (batch_count > 0 && ((now - last_flush) > (FLUSH_MS * 1000)));

        if (flush_by_size || flush_by_time) {
            int json_len = build_batch_json(json_buffer, JSON_BUF_SIZE, batch, batch_count);

            if (json_len > 0 && current_url[0]) {
                esp_http_client_set_post_field(client, json_buffer, json_len);

                esp_err_t err = esp_http_client_perform(client);
                int status = esp_http_client_get_status_code(client);

                if (err == ESP_OK && status >= 200 && status < 300) {
                    s_post_ok++;
                    set_led(1);
                } else {
                    s_post_fail++;
                    ESP_LOGW(TAG,
                             "POST failed: err=%s status=%d url=%s",
                             esp_err_to_name(err),
                             status,
                             current_url);
                    /*
                     * Brief LED dip on failed send, without blocking too long.
                     */
                    set_led(0);
                    vTaskDelay(pdMS_TO_TICKS(30));
                    set_led(1);
                }
            } else {
                s_post_fail++;
                ESP_LOGW(TAG, "Skipping POST: json_len=%d current_url='%s'", json_len, current_url);
            }

            batch_count = 0;
            last_flush = now;
        }
    }
}

void http_sender_init(void)
{
    if (s_q) {
        return;
    }

    url_lock = xSemaphoreCreateMutex();
    if (!url_lock) {
        ESP_LOGE(TAG, "Failed to create URL mutex");
        return;
    }

    s_q = xQueueCreate(HTTP_QUEUE_LEN, sizeof(ble_minimal_event_t));
    if (!s_q) {
        ESP_LOGE(TAG, "Failed to create HTTP sender queue");
        vSemaphoreDelete(url_lock);
        url_lock = NULL;
        return;
    }

    BaseType_t ok = xTaskCreate(
        http_sender_task,
        "http_sender",
        HTTP_TASK_STACK,
        NULL,
        HTTP_TASK_PRIO,
        &s_task_handle
    );

    if (ok != pdPASS) {
        ESP_LOGE(TAG, "Failed to create HTTP sender task");
        vQueueDelete(s_q);
        s_q = NULL;
        vSemaphoreDelete(url_lock);
        url_lock = NULL;
        s_task_handle = NULL;
    }
}

int http_sender_enqueue(const ble_minimal_event_t *ev)
{
    if (!s_q || !ev) {
        return -1;
    }

    if (xQueueSend(s_q, ev, 0) == pdTRUE) {
        s_enq_ok++;
        return 0;
    }

    s_enq_drop++;
    return -1;
}
