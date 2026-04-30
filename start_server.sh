#!/bin/bash
set -e

BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8443}"

echo "=========================================="
echo "  AI Trading Agent - API Server"
echo "  Backend host: ${BACKEND_HOST}"
echo "=========================================="
echo ""
echo "Server:    https://${BACKEND_HOST}:${API_PORT}"
echo "WebSocket: wss://${BACKEND_HOST}:${API_PORT}/ws/logs"
echo "API docs:  https://${BACKEND_HOST}:${API_PORT}/docs"
echo ""
echo "Only accessible from the configured private network"
echo ""

export AGENT_DIR="${AGENT_DIR:-$(dirname "$0")/../../ai-trading-agent}"
export API_PORT
python api_server.py
