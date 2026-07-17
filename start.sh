#!/bin/bash
# Start the Ross Pullback Scanner in the background.
#
# Usage:
#   bash start.sh       # Start the scanner
#   bash stop.sh        # Stop the scanner
#   bash status.sh      # Check if running
#
# Logs: tail -f scanner.log
#
# NOTE: First startup takes ~5 min for bootstrap (20-day history per symbol).
#       Dashboard will not respond until bootstrap is complete — this is normal.

WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="${WORK_DIR}/scanner.pid"
LOGFILE="${WORK_DIR}/scanner.log"
PYTHON="${WORK_DIR}/venv/bin/python"

# Check if already running
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Scanner is already running (PID $OLD_PID)"
        echo "Use: bash stop.sh to stop it first"
        exit 1
    else
        rm -f "$PIDFILE"
    fi
fi

# Ensure venv exists
if [ ! -f "$PYTHON" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "${WORK_DIR}/venv"
fi

# Sync dependencies if requirements.txt changed
REQ_FILE="${WORK_DIR}/requirements.txt"
REQ_HASH_FILE="${WORK_DIR}/venv/.requirements.hash"
CURRENT_HASH=$(md5sum "$REQ_FILE" 2>/dev/null | awk '{print $1}')
CACHED_HASH=$(cat "$REQ_HASH_FILE" 2>/dev/null)
if [ "$CURRENT_HASH" != "$CACHED_HASH" ]; then
    echo "Installing/updating dependencies..."
    "${WORK_DIR}/venv/bin/pip" install -r "$REQ_FILE" --quiet
    echo "$CURRENT_HASH" > "$REQ_HASH_FILE"
fi

cd "$WORK_DIR"

echo "Starting Ross Pullback Scanner..."

# Run in background, log to file
nohup "$PYTHON" -u main.py "$@" >> "$LOGFILE" 2>&1 &
PID=$!
echo $PID > "$PIDFILE"

# Brief check that process launched cleanly
sleep 3
if ! kill -0 "$PID" 2>/dev/null; then
    echo "ERROR: Scanner failed to start."
    echo ""
    echo "--- Last 20 lines of log ---"
    tail -20 "$LOGFILE"
    echo "----------------------------"
    rm -f "$PIDFILE"
    exit 1
fi

SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo 'YOUR_SERVER_IP')

echo ""
echo "============================================"
echo "  Scanner Started!"
echo "============================================"
echo ""
echo "  PID:        $PID"
echo "  Dashboard:  http://${SERVER_IP}:${SCANNER_PORT:-5050}"
echo "  Logs:       tail -f $LOGFILE"
echo ""
echo "  NOTE: Bootstrap takes ~5 min on first run."
echo "        Dashboard will be ready after that."
echo ""
echo "  Stop:       bash ${WORK_DIR}/stop.sh"
echo "  Status:     bash ${WORK_DIR}/status.sh"
echo ""
echo "You can close the terminal — the scanner keeps running."
echo ""
