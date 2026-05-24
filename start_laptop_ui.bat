@echo off
cd /d D:\projects\options-trading-platform

echo Checking if port 8000 is already in use...

REM Get PID using port 8000
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8000 ^| findstr LISTENING') do (
    set PID=%%a
    goto :port_in_use
)

echo Port 8000 is free. Starting server...
goto :start_server

:port_in_use
echo.
echo ❌ Port 8000 is already in use!
echo 👉 Process ID (PID): %PID%
echo.
echo To stop it, run:
echo    taskkill /PID %PID% /F
echo.
echo To inspect:
echo    netstat -ano ^| findstr :8000
echo.
pause
exit /b

:start_server
echo Starting server...
start "Trading Platform Server" C:\Users\Jalal\anaconda3\envs\trading-platform\python.exe ^
    -m uvicorn backend.api.chart_server:app ^
    --host 127.0.0.1 ^
    --port 8000 ^
    --log-level info

echo Waiting for server to be ready...
:wait_loop
timeout /t 1 /nobreak >nul
netstat -ano | findstr "127.0.0.1:8000" | findstr LISTENING >nul 2>&1
if errorlevel 1 goto :wait_loop

echo Server is ready. Opening browser...
start http://127.0.0.1:8000/

pause