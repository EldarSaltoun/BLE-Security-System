@echo off
title BLE Grid Scanner — Peak RSSI Localization
color 0b

REM === CONFIGURATION ===
set PYTHON=python
set PROJECT_DIR=%~dp0
REM The GUI talks to the receiver via the local loopback (same PC)
set STREAM_URL=http://127.0.0.1:8000/api/ble/stream

echo ==================================================
echo   BLE GRID SCANNER : MULTI-POINT LOCALIZATION  
echo ==================================================
echo.

REM === STEP 1: Start Windowed Receiver & mDNS Broadcaster ===
echo [1/2] Starting Receiver + Peak RSSI Window Processor...
start "BLE_SERVER_RECEIVER" cmd /k ^
  "cd /d "%PROJECT_DIR%" && %PYTHON% pc_receiver.py"

REM === STEP 2: Wait for Server & Discovery Bind ===
echo.
echo Waiting 6 seconds for services to initialize...
timeout /t 6 >nul

REM === STEP 3: Start Real-Time Dashboard ===
echo [2/2] Starting Live Dashboard...
start "BLE_GUI_POPUP" cmd /k ^
  "cd /d "%PROJECT_DIR%" && %PYTHON% ble_popup_working.py --url %STREAM_URL%"

echo.
echo --------------------------------------------------
echo BLE System initialized successfully.
echo - 100ms Peak RSSI filtering is ACTIVE.
echo - Control Panel is inside the Dashboard window.
echo --------------------------------------------------
pause