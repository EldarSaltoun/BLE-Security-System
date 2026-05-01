#include "cmd_server.h"

#include "ble_scan.h"

#include "esp_err.h"
#include "esp_http_server.h"
#include "esp_log.h"
#include "nvs.h"
#include "nvs_flash.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *TAG = "CMD_SERVER";

#define CMD_SERVER_PORT 80
#define CMD_QUERY_BUF_LEN 128
#define CMD_PARAM_BUF_LEN 24

static httpd_handle_t s_cmd_server = NULL;

static bool is_valid_state(int state)
{
    return state == 0 || state == 1;
}

static bool is_valid_mode(int mode)
{
    return mode == 0 || mode == 37 || mode == 38 || mode == 39;
}

static esp_err_t save_u8_to_nvs(const char *key, uint8_t value)
{
    nvs_handle_t h;
    esp_err_t err = nvs_open("storage", NVS_READWRITE, &h);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "NVS open failed while saving %s: %s", key, esp_err_to_name(err));
        return err;
    }

    err = nvs_set_u8(h, key, value);
    if (err == ESP_OK) {
        err = nvs_commit(h);
    }

    nvs_close(h);

    if (err != ESP_OK) {
        ESP_LOGW(TAG, "NVS save failed for %s=%u: %s", key, (unsigned)value, esp_err_to_name(err));
    }

    return err;
}

static void send_json_response(httpd_req_t *req,
                               int http_status,
                               const char *status,
                               const char *message,
                               int state_applied,
                               int mode_applied)
{
    char resp[256];

    httpd_resp_set_type(req, "application/json");

    if (http_status == 400) {
        httpd_resp_set_status(req, "400 Bad Request");
    } else if (http_status == 500) {
        httpd_resp_set_status(req, "500 Internal Server Error");
    } else {
        httpd_resp_set_status(req, "200 OK");
    }

    snprintf(resp, sizeof(resp),
             "{\"status\":\"%s\",\"message\":\"%s\",\"state\":%d,\"mode\":%d}",
             status ? status : "unknown",
             message ? message : "",
             state_applied,
             mode_applied);

    httpd_resp_send(req, resp, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t cmd_get_handler(httpd_req_t *req)
{
    char query[CMD_QUERY_BUF_LEN] = {0};
    char param[CMD_PARAM_BUF_LEN] = {0};

    int applied_state = -1;
    int applied_mode = -1;
    bool had_any_param = false;

    esp_err_t qerr = httpd_req_get_url_query_str(req, query, sizeof(query));
    if (qerr != ESP_OK) {
        send_json_response(req, 400, "error", "missing query string", applied_state, applied_mode);
        return ESP_OK;
    }

    if (httpd_query_key_value(query, "state", param, sizeof(param)) == ESP_OK) {
        had_any_param = true;

        char *endptr = NULL;
        long state_l = strtol(param, &endptr, 10);

        if (endptr == param || *endptr != '\0' || !is_valid_state((int)state_l)) {
            ESP_LOGW(TAG, "Invalid state command: '%s'", param);
            send_json_response(req, 400, "error", "invalid state; use 0 or 1", applied_state, applied_mode);
            return ESP_OK;
        }

        applied_state = (int)state_l;
        ble_set_system_state((uint8_t)applied_state);

        /*
         * Persisting state is useful after reset: a scanner can remain IDLE if
         * intentionally disabled. If you prefer always-active boot, remove this line.
         */
        save_u8_to_nvs("scan_state", (uint8_t)applied_state);

        ESP_LOGI(TAG, "Applied command: state=%d", applied_state);
    }

    if (httpd_query_key_value(query, "mode", param, sizeof(param)) == ESP_OK) {
        had_any_param = true;

        char *endptr = NULL;
        long mode_l = strtol(param, &endptr, 10);

        if (endptr == param || *endptr != '\0' || !is_valid_mode((int)mode_l)) {
            ESP_LOGW(TAG, "Invalid mode command: '%s'", param);
            send_json_response(req, 400, "error", "invalid mode; use 0, 37, 38, or 39", applied_state, applied_mode);
            return ESP_OK;
        }

        applied_mode = (int)mode_l;
        ble_set_scan_mode((uint8_t)applied_mode);

        /*
         * ble_scan.c loads scan_mode from NVS at boot, so keep this key name.
         */
        save_u8_to_nvs("scan_mode", (uint8_t)applied_mode);

        ESP_LOGI(TAG, "Applied command: mode=%d", applied_mode);
    }

    if (!had_any_param) {
        send_json_response(req, 400, "error", "no supported parameters; use state and/or mode", applied_state, applied_mode);
        return ESP_OK;
    }

    send_json_response(req, 200, "ok", "command applied", applied_state, applied_mode);
    return ESP_OK;
}

esp_err_t start_cmd_server(void)
{
    if (s_cmd_server != NULL) {
        ESP_LOGW(TAG, "Command server already running");
        return ESP_OK;
    }

    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.server_port = CMD_SERVER_PORT;
    config.lru_purge_enable = true;

    ESP_LOGI(TAG, "Starting command server on port %d", config.server_port);

    esp_err_t err = httpd_start(&s_cmd_server, &config);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to start command server: %s", esp_err_to_name(err));
        s_cmd_server = NULL;
        return err;
    }

    httpd_uri_t cmd_uri = {
        .uri      = "/cmd",
        .method   = HTTP_GET,
        .handler  = cmd_get_handler,
        .user_ctx = NULL
    };

    err = httpd_register_uri_handler(s_cmd_server, &cmd_uri);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Failed to register /cmd handler: %s", esp_err_to_name(err));
        httpd_stop(s_cmd_server);
        s_cmd_server = NULL;
        return err;
    }

    ESP_LOGI(TAG, "Command server ready: GET /cmd?state=0|1&mode=0|37|38|39");
    return ESP_OK;
}

esp_err_t stop_cmd_server(void)
{
    if (s_cmd_server == NULL) {
        return ESP_OK;
    }

    esp_err_t err = httpd_stop(s_cmd_server);
    if (err == ESP_OK) {
        s_cmd_server = NULL;
        ESP_LOGI(TAG, "Command server stopped");
    } else {
        ESP_LOGW(TAG, "Failed to stop command server: %s", esp_err_to_name(err));
    }

    return err;
}
