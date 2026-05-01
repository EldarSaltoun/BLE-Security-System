#pragma once

/*
 * wifi_config.h
 *
 * Wi-Fi fallback/default configuration.
 *
 * Current runtime behavior:
 *   - main.c first tries to load Wi-Fi credentials from NVS.
 *   - If NVS credentials are missing, the scanner starts the setup AP.
 *
 * These macros are kept as convenient defaults / references.
 * They do not override NVS unless main.c is later changed to use them
 * as fallback credentials.
 */

// ---- Identify this scanner in your logs/UI ----
// SCANNER_ID is now defined in scanner_config.h only.
// Do not define it here to avoid duplicate/conflicting scanner IDs.
//#define SCANNER_ID      "ESP32-S3-01"

// ---- Wi-Fi credentials ----
// home wifi
#define WIFI_SSID       "limor22"
#define WIFI_PASS       "26051960"


//riechman wifi
/*#define WIFI_SSID       "Runi-WIFI"
#define WIFI_PASS       "Runi_WiFi_2023"
*/


/* Yuli network
#define WIFI_SSID       "msmsms5"
#define WIFI_PASS       "moirouterrabotaetotli4no"
*/

/* Eldar hotspot
#define WIFI_SSID       "Eldar hotspot"
#define WIFI_PASS       "Eldar1!2@3#"
*/

/*
 * Optional receiver defaults.
 *
 * The receiver IP is normally discovered by mDNS as grid-server.local.
 * If mDNS is unavailable, the scanner can use pc_ip stored in NVS.
 * This macro is only a reference/default for future fallback use.
 */
//#define DEFAULT_PC_IP    "192.168.1.100"

// Optional: drop RSSI weaker than this (set to -127 to disable)
// Currently filtering is done on the PC side. Keep this for future firmware-side filtering.
#define MIN_RSSI_FILTER (-100)
