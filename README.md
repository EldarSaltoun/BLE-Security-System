# BLE Scanner Project

## Overview
The BLE Scanner project is designed to scan for Bluetooth Low Energy (BLE) devices in the vicinity. It utilizes the ESP-IDF framework to implement the scanning functionality and parse advertisement data from discovered devices.

## Project Structure
```
ble-scanner
├── CMakeLists.txt
├── partitions.csv
├── sdkconfig.defaults
├── README.md
└── main
    ├── CMakeLists.txt
    ├── main.c
    ├── ble_scan.c
    ├── ble_scan.h
    ├── adv_parser.c
    └── adv_parser.h
```

## Files Description

- **CMakeLists.txt**: The main configuration file for CMake, defining the project and build settings.
- **partitions.csv**: Defines the memory partition layout for the application, including sections for bootloader, application, and data storage.
- **sdkconfig.defaults**: Contains default configuration settings for the ESP-IDF SDK, controlling various features and parameters.
- **README.md**: Documentation for the project, providing an overview and instructions.
- **main/CMakeLists.txt**: CMake configuration specific to the main directory for compiling source files.
- **main/main.c**: Entry point of the application, initializing the BLE scanner and starting the scanning process.
- **main/ble_scan.c**: Implementation of BLE scanning functionality, including initialization and scan management.
- **main/ble_scan.h**: Header file declaring functions and types for BLE scanning.
- **main/adv_parser.c**: Implementation of advertisement data parsing from scanned devices.
- **main/adv_parser.h**: Header file declaring functions and types for advertisement data parsing.

## Building the Project
To build the BLE Scanner project, follow these steps:

1. Ensure you have the ESP-IDF environment set up on your machine.
2. Navigate to the project directory:
   ```
   cd ble-scanner
   ```
3. Run the following command to build the project:
   ```
   idf.py build
   ```

## Running the Application
After building the project, you can flash it to your ESP device and run the application using:
```
idf.py -p <PORT> flash monitor
```
Replace `<PORT>` with the appropriate serial port for your device.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.