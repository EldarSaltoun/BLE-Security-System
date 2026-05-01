#pragma once

#include "esp_err.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Starts the local HTTP command server on port 80.
 *
 * Supported endpoint:
 *   GET /cmd?state=0|1&mode=0|37|38|39
 *
 * state:
 *   0 = idle/drop scan events
 *   1 = active/enqueue scan events
 *
 * mode:
 *   0  = rotate reported channel label 37 -> 38 -> 39
 *   37 = report channel label 37
 *   38 = report channel label 38
 *   39 = report channel label 39
 *
 * Note:
 *   The current firmware channel value is a software label, not verified RF
 *   channel metadata.
 */
esp_err_t start_cmd_server(void);

/*
 * Stops the command server if running.
 */
esp_err_t stop_cmd_server(void);

#ifdef __cplusplus
}
#endif
