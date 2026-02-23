#include "ble_scan.h"
#include "http_sender.h"
#include "wifi_config.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "esp_log.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "esp_netif.h"
#include "esp_event.h"
#include "esp_wifi.h"
#include "esp_http_server.h"
#include "ntp_time.h"

static const char *TAG = "MAIN";
static EventGroupHandle_t s_wifi_ev = NULL;
#define WIFI_CONNECTED_BIT BIT0

// --- FIX: DEFINE HANDLER FIRST ---
static void wifi_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data) {
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) esp_wifi_connect();
    else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) xEventGroupSetBits(s_wifi_ev, WIFI_CONNECTED_BIT);
    else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) esp_wifi_connect();
}

static const char* SETUP_HTML = "<html><body><h1>Scanner Setup</h1><form method='POST' action='/save'>SSID: <input name='ssid'><br>Pass: <input name='pass' type='password'><br>PC IP: <input name='ip'><br>Mode: <select name='mode'><option value='0'>Auto (1s)</option><option value='37'>37</option><option value='38'>38</option><option value='39'>39</option></select><br><input type='submit' value='Save'></form></body></html>";

static esp_err_t setup_get_handler(httpd_req_t *req) {
    httpd_resp_send(req, SETUP_HTML, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t setup_post_handler(httpd_req_t *req) {
    char buf[256];
    int ret = httpd_req_recv(req, buf, sizeof(buf));
    if (ret <= 0) return ESP_FAIL;
    buf[ret] = '\0';
    nvs_handle_t h;
    nvs_open("storage", NVS_READWRITE, &h);
    char ssid[32], pass[64], ip[16], mode[4];
    if (sscanf(buf, "ssid=%[^&]&pass=%[^&]&ip=%[^&]&mode=%s", ssid, pass, ip, mode) >= 3) {
        nvs_set_str(h, "wifi_ssid", ssid); nvs_set_str(h, "wifi_pass", pass);
        nvs_set_str(h, "pc_ip", ip); nvs_set_u8(h, "scan_mode", (uint8_t)atoi(mode));
        nvs_commit(h);
        httpd_resp_send(req, "Saved. Rebooting...", HTTPD_RESP_USE_STRLEN);
        vTaskDelay(pdMS_TO_TICKS(1500)); esp_restart();
    }
    nvs_close(h);
    return ESP_OK;
}

static void wifi_init_sta(const char* ssid, const char* pass) {
    s_wifi_ev = xEventGroupCreate();
    esp_netif_create_default_wifi_sta();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));
    wifi_config_t wifi_config = {0};
    strncpy((char *)wifi_config.sta.ssid, ssid, 31);
    strncpy((char *)wifi_config.sta.password, pass, 63);
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
    esp_wifi_set_ps(WIFI_PS_NONE);
    xEventGroupWaitBits(s_wifi_ev, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
}

void app_main(void) {
    ESP_LOGI(TAG, "Starting Scanner...");
    esp_err_t nvs = nvs_flash_init();
    if (nvs == ESP_ERR_NVS_NO_FREE_PAGES || nvs == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        nvs_flash_init();
    }
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    ESP_ERROR_CHECK(esp_netif_init());

    nvs_handle_t h;
    char s[32], p[64]; size_t s1=32, s2=64;
    if (nvs_open("storage", NVS_READONLY, &h) == ESP_OK && nvs_get_str(h, "wifi_ssid", s, &s1) == ESP_OK) {
        nvs_get_str(h, "wifi_pass", p, &s2); nvs_close(h);
        wifi_init_sta(s, p);
        time_sync_init(); http_sender_init(); ble_scan_start();
    } else {
        esp_netif_create_default_wifi_ap();
        wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
        esp_wifi_init(&cfg);
        wifi_config_t ap_cfg = { .ap = { .ssid = "Scanner_Setup", .authmode = WIFI_AUTH_OPEN, .max_connection = 1 }};
        esp_wifi_set_mode(WIFI_MODE_AP); esp_wifi_set_config(WIFI_IF_AP, &ap_cfg); esp_wifi_start();
        httpd_handle_t server = NULL; httpd_config_t conf = HTTPD_DEFAULT_CONFIG();
        if (httpd_start(&server, &conf) == ESP_OK) {
            httpd_uri_t u1 = {.uri="/",.method=HTTP_GET,.handler=setup_get_handler}; httpd_register_uri_handler(server, &u1);
            httpd_uri_t u2 = {.uri="/save",.method=HTTP_POST,.handler=setup_post_handler}; httpd_register_uri_handler(server, &u2);
        }
    }
    while(1) vTaskDelay(1000);
}