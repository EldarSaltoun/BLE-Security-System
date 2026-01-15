# BLE-Based Security System for Detecting and Classifying Unknown Devices

## Overview

This project implements a **Bluetooth Low Energy (BLE)–based security system** designed to detect, monitor, and classify unknown BLE devices in a controlled environment.

The system is based on an **ESP32 (ESP-IDF)** firmware that performs continuous BLE scanning and extracts advertisement-level features, combined with **PC-side Python tools** for receiving, logging, and visualizing detected devices.

This project is developed as part of an **academic institution project** and is structured to support future extensions such as device fingerprinting, behavioral analysis, and localization.

---

## System Architecture

The system consists of two main components:

### ESP32 Firmware (ESP-IDF)
- Continuous BLE scanning
- BLE advertisement packet parsing
- Extraction of metadata (RSSI, advertisement structure, services, manufacturer data)
- HTTP-based transmission of scan data to a host PC

### PC-Side Tools (Python)
- HTTP receiver for BLE scan data
- Live device visualization
- Session logging

```
ESP32 (BLE Scanner)
        |
        |  HTTP
        v
PC Receiver (Python)
        |
        +-- Live device display
        +-- Session logging
```

---

## Repository Structure

```
BLE-Security-System/
├── main/
│   ├── ble_scan.c / ble_scan.h
│   ├── adv_parser.c / adv_parser.h
│   ├── http_sender.c / http_sender.h
│   ├── main.c
│   ├── ble_popup.py
│   ├── pc_receiver.py
│   ├── run_ble_system.bat
│   ├── wifi_config.h
│   ├── CMakeLists.txt
│   └── idf_component.yml
├── CMakeLists.txt
├── sdkconfig
├── README.md
├── .gitignore
└── BLE_JSON_File_Manual.pdf
```

> Runtime logs, BLE capture files, and build artifacts are intentionally excluded from version control.

---

## Current Features

- BLE scanning using ESP32 (ESP-IDF)
- BLE advertisement packet parsing
- HTTP-based data transmission from ESP32 to PC
- Python-based receiver and visualization
- Clean separation between firmware, PC tools, and runtime data

---

## Planned Extensions

- Physical device identification and fingerprinting
- Classification of known vs unknown devices
- Behavioral analysis of BLE advertisement patterns
- Multi-receiver localization and triangulation
- Offline clustering and statistical analysis

---

## Build and Run (High-Level)

### ESP32 Firmware

Built using **ESP-IDF**:

```
idf.py build
idf.py flash
idf.py monitor
```

### PC-Side Tools

- Python 3.x required
- Receiver and visualization scripts are located in the `main/` directory
- A helper batch file (`run_ble_system.bat`) is provided

---

## Data Handling Notes

- BLE capture files (JSON / CSV) are generated at runtime
- Runtime data is not committed to the repository
- The repository contains only source code and documentation

---

## Project Context

This project is part of an **academic security-oriented BLE research effort**, focusing on real-world BLE environments and adversarial device detection.

The emphasis is on **system architecture, data integrity, and extensibility**.

---

## Authors

- Eldar Saltoun
- Tomer Mizrachi

---

## Status

Active development
