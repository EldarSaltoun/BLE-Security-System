#include "ntp_time.h"

#include "esp_log.h"
#include "esp_sntp.h"
#include "esp_timer.h"

#include <stdbool.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>

static const char *TAG = "NTP_TIME";

#define NTP_SERVER_1 "pool.ntp.org"
#define NTP_SERVER_2 "time.google.com"
#define NTP_SERVER_3 "time.cloudflare.com"

#define NTP_SYNC_WAIT_MS      10000
#define NTP_SYNC_POLL_MS      250
#define MIN_VALID_EPOCH_SEC   1700000000LL  /* ~2023-11-14 */

static volatile bool s_time_synced = false;
static int64_t s_boot_mono_us = 0;
static int64_t s_epoch_at_boot_us = 0;

static bool timeval_is_valid_epoch(const struct timeval *tv)
{
    return tv && ((int64_t)tv->tv_sec >= MIN_VALID_EPOCH_SEC);
}

static void time_sync_notification_cb(struct timeval *tv)
{
    if (timeval_is_valid_epoch(tv)) {
        s_time_synced = true;
        s_boot_mono_us = esp_timer_get_time();
        s_epoch_at_boot_us = ((int64_t)tv->tv_sec * 1000000LL) + (int64_t)tv->tv_usec;

        ESP_LOGI(TAG,
                 "SNTP synchronized: epoch=%lld.%06ld",
                 (long long)tv->tv_sec,
                 (long)tv->tv_usec);
    } else {
        ESP_LOGW(TAG, "SNTP callback received invalid epoch");
    }
}

void time_sync_init(void)
{
    /*
     * Epoch time is UTC. Do not set TZ here; timezone is only for human-readable
     * local formatting and must not affect JSON timestamps.
     */
    s_boot_mono_us = esp_timer_get_time();

    struct timeval now = {0};
    gettimeofday(&now, NULL);
    if (timeval_is_valid_epoch(&now)) {
        s_time_synced = true;
        s_epoch_at_boot_us = ((int64_t)now.tv_sec * 1000000LL) + (int64_t)now.tv_usec;
        s_boot_mono_us = esp_timer_get_time();
        ESP_LOGI(TAG, "System time already valid before SNTP init");
        return;
    }

    ESP_LOGI(TAG, "Initializing SNTP time sync");

    esp_sntp_stop();
    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, NTP_SERVER_1);
    esp_sntp_setservername(1, NTP_SERVER_2);
    esp_sntp_setservername(2, NTP_SERVER_3);
    esp_sntp_set_time_sync_notification_cb(time_sync_notification_cb);
    esp_sntp_init();

    int waited_ms = 0;
    while (waited_ms < NTP_SYNC_WAIT_MS) {
        gettimeofday(&now, NULL);
        if (timeval_is_valid_epoch(&now)) {
            s_time_synced = true;
            s_boot_mono_us = esp_timer_get_time();
            s_epoch_at_boot_us = ((int64_t)now.tv_sec * 1000000LL) + (int64_t)now.tv_usec;

            ESP_LOGI(TAG,
                     "SNTP synchronized after %d ms: epoch=%lld.%06ld",
                     waited_ms,
                     (long long)now.tv_sec,
                     (long)now.tv_usec);
            return;
        }

        vTaskDelay(pdMS_TO_TICKS(NTP_SYNC_POLL_MS));
        waited_ms += NTP_SYNC_POLL_MS;
    }

    /*
     * If NTP fails, we do NOT fake epoch time. get_time_us() will return a
     * monotonic-based fallback. http_sender.c also sends timestamp_mono_us as
     * "tm", so real-time tracking still has reliable relative timing.
     */
    s_time_synced = false;
    ESP_LOGW(TAG, "SNTP did not synchronize within %d ms; using monotonic fallback", NTP_SYNC_WAIT_MS);
}

int64_t get_time_us(void)
{
    struct timeval now = {0};
    gettimeofday(&now, NULL);

    if (timeval_is_valid_epoch(&now)) {
        if (!s_time_synced) {
            s_time_synced = true;
            s_boot_mono_us = esp_timer_get_time();
            s_epoch_at_boot_us = ((int64_t)now.tv_sec * 1000000LL) + (int64_t)now.tv_usec;
            ESP_LOGI(TAG, "System time became valid during runtime");
        }

        return ((int64_t)now.tv_sec * 1000000LL) + (int64_t)now.tv_usec;
    }

    /*
     * Fallback before SNTP sync.
     * This is not a real UTC epoch. It is monotonic and useful for ordering.
     * PC receiver can still use this if needed; future tracking should prefer
     * the "tm" monotonic field for interval calculations.
     */
    return esp_timer_get_time();
}

int64_t get_mono_time_us(void)
{
    return esp_timer_get_time();
}

bool time_is_synced(void)
{
    return s_time_synced;
}

int64_t get_epoch_boot_estimate_us(void)
{
    return s_epoch_at_boot_us;
}
