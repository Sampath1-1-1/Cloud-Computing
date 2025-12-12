@echo off
echo ---------------------------------------------------
echo   Launching Distributed KV Store (Python Version)
echo ---------------------------------------------------

echo.
echo Starting Controller...
start "Controller" cmd /k python kv_store.py controller

timeout /t 3 /nobreak >nul

echo Starting Workers...
start "Worker 0" cmd /k python kv_store.py worker 0
start "Worker 1" cmd /k python kv_store.py worker 1
start "Worker 2" cmd /k python kv_store.py worker 2
start "Worker 3" cmd /k python kv_store.py worker 3

echo.
echo All nodes launched! Open index.html for dashboard.
pause