idf_component_register(
    SRCS 
        "main.c" 
        "ble_scan.c" 
        "http_sender.c" 
        "adv_parser.c" 
        "ntp_time.c"
        "cmd_server.c"      # NEW: Added cmd_server.c here
    INCLUDE_DIRS "."
    REQUIRES 
        nvs_flash 
        esp_netif 
        esp_wifi 
        esp_event
        esp_http_server
        lwip                # Networking
        bt                  # Bluetooth (Includes NimBLE references)
        esp_http_client     # HTTP
        mbedtls             # Required for Base64 encoding
        mdns                # <--- ADDED FOR WIRELESS DISCOVERY
        esp_driver_gpio
)