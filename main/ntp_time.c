#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "esp_system.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_sntp.h"
#include "ntp_time.h"

static const char *TAG = "NTP_TIME";

void time_sync_init(void)
{
    ESP_LOGI(TAG, "Initializing SNTP");
    
    // New API: Use esp_sntp_... prefix
    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_init();

    // Wait for time to be set
    time_t now = 0;
    struct tm timeinfo = { 0 };
    int retry = 0;
    const int retry_count = 15;
    
    while (esp_sntp_get_sync_status() == SNTP_SYNC_STATUS_RESET && ++retry < retry_count) {
        ESP_LOGI(TAG, "Waiting for system time to be set... (%d/%d)", retry, retry_count);
        vTaskDelay(2000 / portTICK_PERIOD_MS);
    }
    
    time(&now);
    localtime_r(&now, &timeinfo);
    
    if (retry == retry_count) {
        ESP_LOGW(TAG, "Could not sync time! Using default boot time.");
    } else {
        ESP_LOGI(TAG, "Time synced: %s", asctime(&timeinfo));
    }
}

int64_t get_time_us(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000000LL + (int64_t)tv.tv_usec;
}