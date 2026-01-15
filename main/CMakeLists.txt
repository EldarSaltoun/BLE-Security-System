idf_component_register(
    SRCS
        "main.c"
        "ble_scan.c"
        "adv_parser.c"
        "http_sender.c"
    INCLUDE_DIRS "."
    REQUIRES bt nvs_flash esp_wifi esp_http_client
)
