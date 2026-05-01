#pragma once

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Initialize SNTP and wait briefly for UTC epoch synchronization.
 *
 * This function should be called after Wi-Fi is connected.
 * If sync fails, timestamps still remain monotonic through esp_timer.
 */
void time_sync_init(void);

/*
 * Returns current UTC epoch time in microseconds when SNTP/system time is valid.
 *
 * Before SNTP sync, returns a monotonic microsecond fallback from esp_timer.
 * For interval calculations, prefer get_mono_time_us() or the "tm" field sent
 * by http_sender.c.
 */
int64_t get_time_us(void);

/*
 * Always returns ESP monotonic time in microseconds since boot.
 * This is the safest timestamp for advertisement interval and real-time tracking.
 */
int64_t get_mono_time_us(void);

/*
 * True after a valid UTC epoch has been observed.
 */
bool time_is_synced(void);

/*
 * Returns the UTC epoch timestamp captured around boot/sync time, if available.
 * Returns 0 before synchronization.
 */
int64_t get_epoch_boot_estimate_us(void);

#ifdef __cplusplus
}
#endif
