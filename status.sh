#!/bin/bash
# Check if the Ross Pullback Scanner is running.

WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="${WORK_DIR}/scanner.pid"
LOGFILE="${WORK_DIR}/scanner.log"

if [ ! -f "$PIDFILE" ]; then
    echo "Scanner is NOT running (no PID file)"
    exit 1
fi

PID=$(cat "$PIDFILE")

if kill -0 "$PID" 2>/dev/null; then
    SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo 'YOUR_SERVER_IP')
    echo "Scanner is RUNNING (PID $PID)"
    echo ""
    echo "  Dashboard:  http://${SERVER_IP}:${SCANNER_PORT:-5050}"
    echo "  Logs:       tail -f ${LOGFILE}"
    echo ""
    exit 0
else
    echo "Scanner is NOT running (stale PID file)"
    rm -f "$PIDFILE"
    exit 1
fi
