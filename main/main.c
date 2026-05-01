#include "ble_scan.h"
#include "http_sender.h"
#include "wifi_config.h"
#include "cmd_server.h"
#include "ntp_time.h"
#include "scanner_config.h"

#include "mdns.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"

#include "esp_err.h"
#include "esp_event.h"
#include "esp_http_server.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_system.h"
#include "esp_wifi.h"

#include "nvs_flash.h"
#include "nvs.h"

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "MAIN";

static EventGroupHandle_t s_wifi_ev = NULL;

#define WIFI_CONNECTED_BIT BIT0
#define WIFI_FAIL_BIT      BIT1

#define WIFI_CONNECT_TIMEOUT_MS 30000
#define WIFI_RETRY_DELAY_MS     2000

#define SETUP_AP_SSID           "Scanner_Setup"
#define SETUP_HTTP_PORT         80

#define MDNS_QUERY_TIMEOUT_MS   2000
#define MDNS_RETRY_MS           5000
#define MDNS_RECHECK_MS         30000

static const char *SETUP_HTML =
    "<html><body>"
    "<h1>Scanner Setup</h1>"
    "<form method='POST' action='/save'>"
    "SSID: <input name='ssid'><br>"
    "Pass: <input name='pass' type='password'><br>"
    "PC IP: <input name='ip'><br>"
    "Mode: <select name='mode'>"
    "<option value='0'>Auto (1s)</option>"
    "<option value='37'>37</option>"
    "<option value='38'>38</option>"
    "<option value='39'>39</option>"
    "</select><br>"
    "<input type='submit' value='Save'>"
    "</form>"
    "</body></html>";

static void wifi_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg;

    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        ESP_LOGI(TAG, "Wi-Fi STA started; connecting...");
        esp_wifi_connect();
        return;
    }

    if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        const ip_event_got_ip_t *event = (const ip_event_got_ip_t *)data;
        ESP_LOGI(TAG, "Wi-Fi connected. IP=" IPSTR, IP2STR(&event->ip_info.ip));
        if (s_wifi_ev) {
            xEventGroupSetBits(s_wifi_ev, WIFI_CONNECTED_BIT);
            xEventGroupClearBits(s_wifi_ev, WIFI_FAIL_BIT);
        }
        return;
    }

    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW(TAG, "Wi-Fi disconnected; reconnecting in %d ms...", WIFI_RETRY_DELAY_MS);
        if (s_wifi_ev) {
            xEventGroupClearBits(s_wifi_ev, WIFI_CONNECTED_BIT);
        }
        vTaskDelay(pdMS_TO_TICKS(WIFI_RETRY_DELAY_MS));
        esp_wifi_connect();
        return;
    }
}

static bool is_valid_scan_mode(uint8_t mode)
{
    return mode == 0 || mode == 37 || mode == 38 || mode == 39;
}

static int hex_nibble(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    return -1;
}

/*
 * Small URL decoder for setup form values.
 * Handles %XX and '+' -> space.
 */
static void url_decode(const char *src, char *dst, size_t dst_len)
{
    if (!src || !dst || dst_len == 0) return;

    size_t di = 0;
    for (size_t si = 0; src[si] != '\0' && di + 1 < dst_len; si++) {
        if (src[si] == '+') {
            dst[di++] = ' ';
        } else if (src[si] == '%' && src[si + 1] && src[si + 2]) {
            int hi = hex_nibble(src[si + 1]);
            int lo = hex_nibble(src[si + 2]);
            if (hi >= 0 && lo >= 0) {
                dst[di++] = (char)((hi << 4) | lo);
                si += 2;
            } else {
                dst[di++] = src[si];
            }
        } else {
            dst[di++] = src[si];
        }
    }

    dst[di] = '\0';
}

static bool form_get_value(const char *form, const char *key, char *out, size_t out_len)
{
    if (!form || !key || !out || out_len == 0) return false;

    out[0] = '\0';

    size_t key_len = strlen(key);
    const char *p = form;

    while (*p) {
        if (strncmp(p, key, key_len) == 0 && p[key_len] == '=') {
            const char *value_start = p + key_len + 1;
            const char *value_end = strchr(value_start, '&');
            size_t enc_len = value_end ? (size_t)(value_end - value_start) : strlen(value_start);

            char encoded[128] = {0};
            if (enc_len >= sizeof(encoded)) enc_len = sizeof(encoded) - 1;
            memcpy(encoded, value_start, enc_len);
            encoded[enc_len] = '\0';

            url_decode(encoded, out, out_len);
            return true;
        }

        p = strchr(p, '&');
        if (!p) break;
        p++;
    }

    return false;
}

/*
 * mDNS resolver task.
 *
 * The PC receiver advertises grid-server.local. We keep checking periodically
 * so a PC IP change can be picked up without reflashing/reconfiguring scanners.
 */
static void resolve_grid_server_task(void *pv)
{
    (void)pv;

    char last_ip[16] = {0};

    ESP_LOGI(TAG, "mDNS resolver started; looking for grid-server.local");

    while (1) {
        esp_ip4_addr_t addr;
        memset(&addr, 0, sizeof(addr));

        esp_err_t err = mdns_query_a("grid-server", MDNS_QUERY_TIMEOUT_MS, &addr);
        if (err == ESP_OK && addr.addr != 0) {
            char ip_str[16] = {0};
            esp_ip4addr_ntoa(&addr, ip_str, sizeof(ip_str));

            if (strcmp(ip_str, last_ip) != 0) {
                ESP_LOGI(TAG, "mDNS found receiver: %s", ip_str);
                http_sender_update_ip(ip_str);
                snprintf(last_ip, sizeof(last_ip), "%s", ip_str);
            }

            vTaskDelay(pdMS_TO_TICKS(MDNS_RECHECK_MS));
        } else {
            ESP_LOGW(TAG, "mDNS query for grid-server.local failed; retrying...");
            vTaskDelay(pdMS_TO_TICKS(MDNS_RETRY_MS));
        }
    }
}

static esp_err_t setup_get_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, SETUP_HTML, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t save_setup_to_nvs(const char *ssid, const char *pass, const char *ip, uint8_t mode)
{
    nvs_handle_t h;
    esp_err_t err = nvs_open("storage", NVS_READWRITE, &h);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "NVS open failed: %s", esp_err_to_name(err));
        return err;
    }

    err = nvs_set_str(h, "wifi_ssid", ssid);
    if (err == ESP_OK) err = nvs_set_str(h, "wifi_pass", pass);
    if (err == ESP_OK) err = nvs_set_str(h, "pc_ip", ip);
    if (err == ESP_OK) err = nvs_set_u8(h, "scan_mode", mode);
    if (err == ESP_OK) err = nvs_set_u8(h, "scan_state", 1);
    if (err == ESP_OK) err = nvs_commit(h);

    nvs_close(h);
    return err;
}

static esp_err_t setup_post_handler(httpd_req_t *req)
{
    char buf[512] = {0};

    int total = 0;
    int remaining = req->content_len;
    if (remaining >= (int)sizeof(buf)) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Form too large");
        return ESP_OK;
    }

    while (remaining > 0) {
        int ret = httpd_req_recv(req, buf + total, remaining);
        if (ret <= 0) {
            httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "Failed to read form");
            return ESP_OK;
        }
        total += ret;
        remaining -= ret;
    }
    buf[total] = '\0';

    char ssid[33] = {0};
    char pass[65] = {0};
    char ip[32] = {0};
    char mode_str[8] = {0};

    bool ok_ssid = form_get_value(buf, "ssid", ssid, sizeof(ssid));
    bool ok_pass = form_get_value(buf, "pass", pass, sizeof(pass));
    bool ok_ip   = form_get_value(buf, "ip", ip, sizeof(ip));
    bool ok_mode = form_get_value(buf, "mode", mode_str, sizeof(mode_str));

    if (!ok_ssid || !ok_pass || !ok_ip || strlen(ssid) == 0 || strlen(ip) == 0) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Missing ssid/pass/ip");
        return ESP_OK;
    }

    uint8_t mode = 0;
    if (ok_mode) {
        int mode_i = atoi(mode_str);
        if (!is_valid_scan_mode((uint8_t)mode_i)) {
            httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Invalid mode");
            return ESP_OK;
        }
        mode = (uint8_t)mode_i;
    }

    esp_err_t err = save_setup_to_nvs(ssid, pass, ip, mode);
    if (err != ESP_OK) {
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "Failed to save settings");
        return ESP_OK;
    }

    httpd_resp_send(req, "Saved. Rebooting...", HTTPD_RESP_USE_STRLEN);
    ESP_LOGI(TAG, "Setup saved. SSID='%s', PC_IP='%s', mode=%u. Rebooting...", ssid, ip, (unsigned)mode);

    vTaskDelay(pdMS_TO_TICKS(1500));
    esp_restart();
    return ESP_OK;
}

static esp_err_t wifi_init_sta(const char *ssid, const char *pass)
{
    if (!ssid || strlen(ssid) == 0) {
        return ESP_ERR_INVALID_ARG;
    }

    s_wifi_ev = xEventGroupCreate();
    if (!s_wifi_ev) {
        return ESP_ERR_NO_MEM;
    }

    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

    wifi_config_t wifi_config = {0};
    snprintf((char *)wifi_config.sta.ssid, sizeof(wifi_config.sta.ssid), "%s", ssid);
    snprintf((char *)wifi_config.sta.password, sizeof(wifi_config.sta.password), "%s", pass ? pass : "");

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    ESP_ERROR_CHECK(esp_wifi_start());

    EventBits_t bits = xEventGroupWaitBits(
        s_wifi_ev,
        WIFI_CONNECTED_BIT,
        pdFALSE,
        pdTRUE,
        pdMS_TO_TICKS(WIFI_CONNECT_TIMEOUT_MS)
    );

    if ((bits & WIFI_CONNECTED_BIT) == 0) {
        ESP_LOGE(TAG, "Wi-Fi connection timeout");
        return ESP_ERR_TIMEOUT;
    }

    return ESP_OK;
}

static void start_setup_ap(void)
{
    ESP_LOGW(TAG, "No Wi-Fi credentials found. Starting setup AP: %s", SETUP_AP_SSID);

    esp_netif_create_default_wifi_ap();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    wifi_config_t ap_cfg = {0};
    snprintf((char *)ap_cfg.ap.ssid, sizeof(ap_cfg.ap.ssid), "%s", SETUP_AP_SSID);
    ap_cfg.ap.ssid_len = strlen(SETUP_AP_SSID);
    ap_cfg.ap.channel = 1;
    ap_cfg.ap.authmode = WIFI_AUTH_OPEN;
    ap_cfg.ap.max_connection = 1;

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_cfg));
    ESP_ERROR_CHECK(esp_wifi_start());

    httpd_handle_t server = NULL;
    httpd_config_t conf = HTTPD_DEFAULT_CONFIG();
    conf.server_port = SETUP_HTTP_PORT;

    esp_err_t err = httpd_start(&server, &conf);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to start setup HTTP server: %s", esp_err_to_name(err));
        return;
    }

    httpd_uri_t u1 = {
        .uri = "/",
        .method = HTTP_GET,
        .handler = setup_get_handler,
        .user_ctx = NULL
    };
    httpd_register_uri_handler(server, &u1);

    httpd_uri_t u2 = {
        .uri = "/save",
        .method = HTTP_POST,
        .handler = setup_post_handler,
        .user_ctx = NULL
    };
    httpd_register_uri_handler(server, &u2);

    ESP_LOGI(TAG, "Setup page available at http://192.168.4.1/");
}

static bool load_wifi_credentials(char *ssid, size_t ssid_len, char *pass, size_t pass_len)
{
    nvs_handle_t h;
    esp_err_t err = nvs_open("storage", NVS_READONLY, &h);
    if (err != ESP_OK) {
        return false;
    }

    esp_err_t e1 = nvs_get_str(h, "wifi_ssid", ssid, &ssid_len);
    esp_err_t e2 = nvs_get_str(h, "wifi_pass", pass, &pass_len);

    nvs_close(h);

    return e1 == ESP_OK && e2 == ESP_OK && strlen(ssid) > 0;
}

static void init_nvs(void)
{
    esp_err_t nvs = nvs_flash_init();
    if (nvs == ESP_ERR_NVS_NO_FREE_PAGES || nvs == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    } else {
        ESP_ERROR_CHECK(nvs);
    }
}

void app_main(void)
{
    ESP_LOGI(TAG, "Starting BLE scanner node. Scanner ID=%d", (int)SCANNER_ID);

    init_nvs();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    char ssid[33] = {0};
    char pass[65] = {0};

    if (load_wifi_credentials(ssid, sizeof(ssid), pass, sizeof(pass))) {
        ESP_LOGI(TAG, "Wi-Fi credentials found. Connecting to SSID='%s'", ssid);

        esp_err_t wifi_err = wifi_init_sta(ssid, pass);
        if (wifi_err != ESP_OK) {
            ESP_LOGE(TAG, "Wi-Fi STA failed: %s. Starting setup AP.", esp_err_to_name(wifi_err));
            start_setup_ap();
        } else {
            /*
             * Start sender before mDNS resolution so http_sender_update_ip()
             * always has its mutex/queue ready.
             */
            http_sender_init();

            ESP_ERROR_CHECK(mdns_init());

            char hostname[32] = {0};
            snprintf(hostname, sizeof(hostname), "esp32-scanner-%d", (int)SCANNER_ID);
            ESP_ERROR_CHECK(mdns_hostname_set(hostname));
            ESP_ERROR_CHECK(mdns_instance_name_set(hostname));

            xTaskCreate(resolve_grid_server_task, "mdns_res", 4096, NULL, 5, NULL);

            ESP_ERROR_CHECK(start_cmd_server());

            /*
             * SNTP may take a few seconds. If it fails, ntp_time.c falls back
             * to system/default time, and http_sender.c also sends monotonic tm.
             */
            time_sync_init();

            ble_scan_start();

            ESP_LOGI(TAG, "Scanner runtime started.");
        }
    } else {
        start_setup_ap();
    }

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}
