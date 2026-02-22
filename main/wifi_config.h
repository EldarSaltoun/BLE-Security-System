#pragma once

// ---- Identify this scanner in your logs/UI ----
#define SCANNER_ID      "ESP32-S3-01"

// ---- Wi-Fi credentials ----
// home wifi
/*#define WIFI_SSID       "limor22"
#define WIFI_PASS       "26051960"
*/

//riechman wifi
#define WIFI_SSID       "Runi-WIFI"
#define WIFI_PASS       "Runi_WiFi_2023"

// ---- PC receiver endpoint (pc_receiver.py) ----
// Example: "http://192.168.1.50:8000/api/ble/ingest"
#define PC_INGEST_URL "http://192.168.1.19:8000/api/ble/ingest"


// Optional: drop RSSI weaker than this (set to -127 to disable)
#define MIN_RSSI_FILTER (-127)
