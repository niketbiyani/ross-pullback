#!/bin/bash
# Stop the Ross Pullback Scanner.

WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="${WORK_DIR}/scanner.pid"

if [ ! -f "$PIDFILE" ]; then
    echo "Scanner is not running (no PID file found)"
    exit 0
fi

PID=$(cat "$PIDFILE")

if kill -0 "$PID" 2>/dev/null; then
    echo "Stopping scanner (PID $PID)..."
    kill "$PID"
    sleep 2
    if kill -0 "$PID" 2>/dev/null; then
        kill -9 "$PID"
        sleep 1
    fi
    rm -f "$PIDFILE"
    echo "Scanner stopped."
else
    echo "Scanner was not running (stale PID file). Cleaning up."
    rm -f "$PIDFILE"
fi
