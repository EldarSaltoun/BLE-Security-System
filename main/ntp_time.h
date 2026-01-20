#ifndef NTP_TIME_H
#define NTP_TIME_H

#include <stdint.h>
#include <time.h>

/**
 * @brief Initialize SNTP and wait for time synchronization.
 * This should be called AFTER Wi-Fi is connected.
 */
void time_sync_init(void);

/**
 * @brief Get the current time in microseconds since Unix Epoch.
 * Useful for high-precision jitter calculation.
 * * @return int64_t Timestamp in microseconds
 */
int64_t get_time_us(void);

#endif // NTP_TIME_H