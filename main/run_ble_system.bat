@echo off
title BLE Security System — Distributed Launcher
color 0b

REM === CONFIGURATION ===
set PYTHON=python
set PROJECT_DIR=%~dp0
set SERVER_URL=http://127.0.0.1:8000
set STREAM_URL=%SERVER_URL%/api/ble/stream

echo ==================================================
echo   BLE SECURITY SYSTEM : MINIMAL-ESP / MAX-PYTHON  
echo ==================================================
echo.

REM === STEP 1: Start High-Throughput Receiver ===
echo [1/2] Starting Batched HTTP Receiver...
start "BLE Collector (Receiver)" cmd /k ^
  cd /d "%PROJECT_DIR%" ^&^& ^
  echo Launching Flask server for batched ingest... ^&^& ^
  %PYTHON% pc_receiver.py

REM === STEP 2: Wait for Server Bind ===
echo.
echo Waiting for server to initialize...
timeout /t 3 >nul

REM === STEP 3: Start Real-Time Popup GUI ===
echo [2/2] Starting Live Presence Popup...
start "BLE Viewer (Popup)" cmd /k ^
  cd /d "%PROJECT_DIR%" ^&^& ^
  echo Initializing GUI with raw payload parsing... ^&^& ^
  %PYTHON% ble_popup.py --url %STREAM_URL% --mfg-db mfg_ids.csv

echo.
echo --------------------------------------------------
echo BLE System initialized successfully.
echo - High-Performance Ingest: ACTIVE
echo - Batch Processing: ENABLED
echo.
echo You may now power on the ESP32-S3 boards.
echo --------------------------------------------------
pause