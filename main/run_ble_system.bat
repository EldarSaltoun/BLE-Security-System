@echo off
title BLE System Launcher

REM === CONFIG ===
set PYTHON=python
set PROJECT_DIR=%~dp0
set SERVER_URL=http://10.0.0.30:8000
set STREAM_URL=%SERVER_URL%/api/ble/stream

REM === Start HTTP Receiver ===
echo Starting HTTP receiver...
start "BLE HTTP Receiver" cmd /k ^
  cd /d "%PROJECT_DIR%" ^&^& ^
  %PYTHON% -m uvicorn pc_receiver:app --host 0.0.0.0 --port 8000

REM === Give server time to start ===
timeout /t 2 >nul

REM === Start Popup ===
echo Starting BLE popup...
start "BLE Popup" cmd /k ^
  cd /d "%PROJECT_DIR%" ^&^& ^
  %PYTHON% ble_popup.py --url %STREAM_URL% --mfg-db mfg_ids.csv

echo.
echo BLE system started successfully.
echo You may now power the ESP32.
