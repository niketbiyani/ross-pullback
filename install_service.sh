#!/bin/bash
# Install the Ross Pullback Scanner as a systemd service.
# Run once: sudo bash install_service.sh
#
# After installation:
#   sudo systemctl start ross-scanner
#   sudo systemctl stop ross-scanner
#   sudo systemctl restart ross-scanner
#   sudo systemctl status ross-scanner
#   journalctl -u ross-scanner -f
#
# The service will:
#   - Auto-start on boot
#   - Auto-restart on crash (after 10s)
#   - Restart daily at 8:50 AM IST (fresh bootstrap before market open)

set -e

WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="${WORK_DIR}/venv"
PYTHON="${VENV_DIR}/bin/python"
RUN_USER=$(stat -c '%U' "$WORK_DIR" 2>/dev/null || echo 'root')

# Ensure venv exists
if [ ! -d "${VENV_DIR}" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "${VENV_DIR}"
fi

echo "Installing dependencies..."
"${VENV_DIR}/bin/pip" install -r "${WORK_DIR}/requirements.txt" --quiet

# ── Main Service ─────────────────────────────────────────────────────────

cat > /etc/systemd/system/ross-scanner.service << EOF
[Unit]
Description=Ross Cameron Micro-Pullback Scanner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${WORK_DIR}
ExecStart=${PYTHON} main.py
Restart=on-failure
RestartSec=10
StandardOutput=append:${WORK_DIR}/scanner.log
StandardError=append:${WORK_DIR}/scanner.log
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# ── Daily Pre-Market Restart ──────────────────────────────────────────────
# Restarts at 8:50 AM IST so bootstrap completes before 9:15 AM market open.

cat > /etc/systemd/system/ross-scanner-restart.service << 'EOF'
[Unit]
Description=Restart Ross Scanner for fresh bootstrap before market open

[Service]
Type=oneshot
ExecStart=/bin/systemctl restart ross-scanner
EOF

cat > /etc/systemd/system/ross-scanner-restart.timer << 'EOF'
[Unit]
Description=Daily pre-market restart of Ross Scanner (8:50 AM IST)

[Timer]
OnCalendar=*-*-* 08:50:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

# ── FNO Scanner Service ──────────────────────────────────────────────────

cat > /etc/systemd/system/fno-scanner.service << EOF
[Unit]
Description=FNO Micro-Pullback Scanner (F&O universe, 15/30/60-min MACD)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${WORK_DIR}
ExecStart=${PYTHON} fno_main.py
Restart=on-failure
RestartSec=10
StandardOutput=append:${WORK_DIR}/fno_scanner.log
StandardError=append:${WORK_DIR}/fno_scanner.log
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/fno-scanner-restart.service << 'EOF'
[Unit]
Description=Restart FNO Scanner for fresh bootstrap before market open

[Service]
Type=oneshot
ExecStart=/bin/systemctl restart fno-scanner
EOF

cat > /etc/systemd/system/fno-scanner-restart.timer << 'EOF'
[Unit]
Description=Daily pre-market restart of FNO Scanner (8:50 AM IST)

[Timer]
OnCalendar=*-*-* 08:50:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

# ── Reload and Enable ────────────────────────────────────────────────────

systemctl daemon-reload
systemctl enable ross-scanner
systemctl enable ross-scanner-restart.timer
systemctl enable fno-scanner
systemctl enable fno-scanner-restart.timer

echo ""
echo "============================================"
echo "  Installation Complete!"
echo "============================================"
echo ""
echo "Ross Scanner (intraday):"
echo "  sudo systemctl start ross-scanner      - Start"
echo "  sudo systemctl stop ross-scanner       - Stop"
echo "  sudo systemctl restart ross-scanner    - Restart"
echo "  sudo systemctl status ross-scanner     - Check status"
echo "  journalctl -u ross-scanner -f          - Live logs"
echo "  Dashboard: http://localhost:5050"
echo ""
echo "FNO Scanner (F&O universe, 15/30/60-min):"
echo "  sudo systemctl start fno-scanner       - Start"
echo "  sudo systemctl stop fno-scanner        - Stop"
echo "  sudo systemctl restart fno-scanner     - Restart"
echo "  sudo systemctl status fno-scanner      - Check status"
echo "  journalctl -u fno-scanner -f           - Live logs"
echo "  Dashboard: http://localhost:5051"
echo ""
