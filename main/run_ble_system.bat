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
echo [1/2] Starting Receiver + Wireless Discovery Beacon...
echo (The ESP32s will now find this laptop automatically via mDNS)
start "BLE_SERVER_RECEIVER" cmd /k ^
  "cd /d "%PROJECT_DIR%" && %PYTHON% pc_receiver.py"

REM === STEP 2: Wait for Server & Discovery Bind ===
echo.
echo Waiting 5 seconds for services to initialize...
timeout /t 5 >nul

REM === STEP 3: Start Real-Time Popup GUI ===
echo [2/2] Starting Live Presence Popup...
start "BLE_GUI_POPUP" cmd /k ^
  "cd /d "%PROJECT_DIR%" && %PYTHON% ble_popup.py --url %STREAM_URL%"

echo.
echo --------------------------------------------------
echo BLE System initialized successfully.
echo - Discovery Mode: mDNS (grid-server.local)
echo - Fast-Ack Mode: ENABLED
echo.
echo You may now power on the ESP32-S3 boards.
echo Watch the LEDs: Rapid Blink = Searching | Solid = Linked
echo --------------------------------------------------
pause