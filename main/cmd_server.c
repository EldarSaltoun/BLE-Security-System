#include "cmd_server.h"
#include "ble_scan.h" // We will update this header in the next step to include our new setters
#include "esp_http_server.h"
#include "esp_log.h"
#include <string.h>
#include <stdlib.h>

static const char *TAG = "CMD_SERVER";

static esp_err_t cmd_get_handler(httpd_req_t *req) {
    char buf[100];
    
    // Extract the query string from the URL (e.g., "?state=1&mode=37")
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK) {
        char param[16];
        
        // Check for and apply the "state" parameter (1 = Active, 0 = Idle)
        if (httpd_query_key_value(buf, "state", param, sizeof(param)) == ESP_OK) {
            int state = atoi(param);
            ble_set_system_state((uint8_t)state);
            ESP_LOGI(TAG, "Command received: State = %d", state);
        }

        // Check for and apply the "mode" parameter (0=Auto, 37, 38, or 39)
        if (httpd_query_key_value(buf, "mode", param, sizeof(param)) == ESP_OK) {
            int mode = atoi(param);
            ble_set_scan_mode((uint8_t)mode);
            ESP_LOGI(TAG, "Command received: Mode = %d", mode);
        }
    }

    // Send a simple acknowledgment back to the sender
    const char *resp = "OK";
    httpd_resp_send(req, resp, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

void start_cmd_server(void) {
    httpd_handle_t server = NULL;
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    
    // Listen on port 80. This is safe because your setup portal only runs in AP mode,
    // while this command server will only run in STA (Station) mode.
    config.server_port = 80; 

    ESP_LOGI(TAG, "Starting command server on port: '%d'", config.server_port);
    if (httpd_start(&server, &config) == ESP_OK) {
        httpd_uri_t cmd_uri = {
            .uri       = "/cmd",
            .method    = HTTP_GET,
            .handler   = cmd_get_handler,
            .user_ctx  = NULL
        };
        httpd_register_uri_handler(server, &cmd_uri);
    } else {
        ESP_LOGE(TAG, "Failed to start command server!");
    }
}