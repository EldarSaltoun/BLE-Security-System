idf_component_register(
    SRCS 
        "main.c" 
        "ble_scan.c" 
        "http_sender.c" 
        "adv_parser.c" 
        "ntp_time.c"
    INCLUDE_DIRS "."
    REQUIRES 
        nvs_flash 
        esp_netif 
        esp_wifi 
        esp_event
        lwip                # Networking
        bt                  # Bluetooth (for esp_bt.h)
        esp_http_client     # HTTP (for esp_http_client.h)
)