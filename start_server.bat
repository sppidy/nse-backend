@echo off
setlocal
if not defined BACKEND_HOST set "BACKEND_HOST=127.0.0.1"
if not defined API_PORT set "API_PORT=8443"
echo ==========================================
echo   AI Trading Agent - API Server
echo   Backend host: %BACKEND_HOST%
echo ==========================================
echo.
echo Server:    https://%BACKEND_HOST%:%API_PORT%
echo WebSocket: wss://%BACKEND_HOST%:%API_PORT%/ws/logs
echo API docs:  https://%BACKEND_HOST%:%API_PORT%/docs
echo.
echo Only accessible from the configured private network
echo.

if not defined AGENT_DIR set "AGENT_DIR=%~dp0..\nse-agent"
set API_PORT=%API_PORT%
python api_server.py
pause
