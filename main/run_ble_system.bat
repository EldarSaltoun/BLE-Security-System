@echo off
title BLE Security System — Wireless Grid Launcher
color 0b

REM === CONFIGURATION ===
set PYTHON=python
set PROJECT_DIR=%~dp0
REM The GUI talks to the receiver via the local loopback (same PC)
set STREAM_URL=http://127.0.0.1:8000/api/ble/stream

echo ==================================================
echo   BLE SECURITY SYSTEM : WIRELESS AUTO-DISCOVERY  
echo ==================================================
echo.

REM === STEP 1: Start Asynchronous Receiver & mDNS Broadcaster ===
echo [1/3] Starting Receiver + Wireless Discovery Beacon...
[cite_start]echo (The ESP32s will now find this laptop automatically via mDNS) [cite: 5]
start "BLE_SERVER_RECEIVER" cmd /k ^
  "cd /d "%PROJECT_DIR%" && %PYTHON% pc_receiver.py"

REM === STEP 2: Wait for Server & Discovery Bind ===
echo.
[cite_start]echo Waiting 6 seconds for services to initialize... [cite: 6]
timeout /t 6 >nul

REM === STEP 3: Start Real-Time Popup GUI ===
echo [2/3] Starting Live Presence Popup...
start "BLE_GUI_POPUP" cmd /k ^
  "cd /d "%PROJECT_DIR%" && %PYTHON% ble_popup.py --url %STREAM_URL%"

REM === STEP 4: Start Wireless Scanner Control Panel ===
echo [3/3] Starting Wireless Control Panel...
start "BLE_CONTROL_PANEL" cmd /k ^
  "cd /d "%PROJECT_DIR%" && %PYTHON% scanner_control.py"

echo.
echo --------------------------------------------------
[cite_start]echo BLE System initialized successfully. [cite: 7]
[cite_start]echo - Discovery Mode: mDNS (grid-server.local) [cite: 7]
echo - Remote Control: ENABLED
echo.
[cite_start]echo You may now power on the ESP32-S3 boards. [cite: 8]
[cite_start]echo Watch the LEDs: Rapid Blink = Searching | [cite: 8]
[cite_start]echo Solid = Linked [cite: 9]
echo --------------------------------------------------
pause