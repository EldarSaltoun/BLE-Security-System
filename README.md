BLE-Based Security System for Detecting and Classifying Unknown Devices
Overview

This project implements a BLE-based security system designed to detect, monitor, and classify unknown Bluetooth Low Energy (BLE) devices in a controlled environment.

The system is built around an ESP32 (ESP-IDF) firmware that performs continuous BLE scanning and extracts advertisement-level features, combined with PC-side Python tools for receiving, logging, and visualizing the detected devices.
The project is developed as part of an academic institution project and is structured to support future extensions such as device fingerprinting, behavioral analysis, and localization.

System Architecture

The system is divided into two main parts:

1. ESP32 Firmware (ESP-IDF)

Performs continuous BLE scanning

Parses BLE advertisement packets

Extracts relevant metadata (RSSI, advertisement structure, services, manufacturer data, etc.)

Sends processed scan data to a host PC over HTTP

2. PC-Side Tools (Python)

Receives BLE scan data from the ESP32

Displays and logs detected devices

Enables offline analysis of captured BLE sessions

Serves as a foundation for future classification and fingerprinting logic

ESP32 (BLE Scanner)
        │
        │  HTTP
        ▼
PC Receiver (Python)
        │
        ├── Live device display
        ├── Session logging
        └── Offline analysis / visualization

Repository Structure
BLE-Security-System/
├── main/                     # ESP-IDF application + PC-side scripts
│   ├── ble_scan.c/h          # BLE scanning logic
│   ├── adv_parser.c/h        # Advertisement parsing utilities
│   ├── http_sender.c/h       # HTTP communication from ESP32
│   ├── main.c                # ESP-IDF application entry point
│   ├── ble_popup.py          # PC-side visualization / popup tool
│   ├── pc_receiver.py        # PC-side HTTP receiver
│   ├── run_ble_system.bat    # Helper script to start PC-side services
│   ├── wifi_config.h         # Wi-Fi configuration (local)
│   ├── CMakeLists.txt
│   └── idf_component.yml
│
├── CMakeLists.txt            # ESP-IDF project configuration
├── sdkconfig                 # ESP-IDF build configuration
├── README.md
├── .gitignore
└── BLE_JSON_File_Manual.pdf  # Documentation of BLE JSON format


⚠️ Runtime logs, captured BLE sessions, and build artifacts are intentionally excluded from version control.

Features (Current State)

✅ BLE scanning using ESP32 (ESP-IDF)

✅ Advertisement packet parsing

✅ HTTP-based data transmission from ESP32 to PC

✅ Python-based receiver and visualization

✅ Clean separation between firmware, PC tools, and data

✅ Git repository structured for reproducibility and extension

Planned Extensions

The current implementation provides the infrastructure for more advanced security features, including:

🔜 Physical device identification and fingerprinting

🔜 Classification of unknown vs known devices

🔜 Behavioral analysis based on advertisement patterns

🔜 Multi-receiver localization and triangulation

🔜 Offline clustering and statistical analysis

Build & Run (High-Level)
ESP32 Firmware

Built using ESP-IDF

Standard workflow:

idf.py build
idf.py flash
idf.py monitor

PC-Side Tools

Python 3.x required

Run the receiver and visualization scripts from the main/ directory

A helper batch file (run_ble_system.bat) is provided for convenience

Notes on Data Handling

BLE capture files (JSON / CSV) are generated at runtime

These files are not committed to the repository

The repository contains only source code and documentation

Example or synthetic datasets may be added later in a dedicated directory

Project Context

This project is developed as part of an academic security-oriented BLE research effort, focusing on real-world BLE environments and adversarial device detection.
The emphasis is on system architecture, data integrity, and extensibility, rather than a single fixed experiment.

Authors

Eldar Saltoun

Tomer Mizrachi

Status

🟢 Active development
