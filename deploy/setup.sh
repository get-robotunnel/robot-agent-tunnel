#!/usr/bin/env bash
# Install/refresh the tunnel-svc binary + systemd unit on the VPS.
# Expects ./tunnel-svc (the linux binary) and ./tunnel.service in CWD.
# Used by the Deploy Tunnel GitHub workflow; safe to run by hand as root.
# Does NOT touch /opt/roboat/config/.env.
set -euo pipefail

APP_DIR=/opt/roboat
BIN_DIR="$APP_DIR/bin"

id -u roboat >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin roboat || true
install -d -o roboat -g roboat "$APP_DIR" "$BIN_DIR" "$APP_DIR/config"

install -m 0755 ./tunnel-svc "$BIN_DIR/tunnel-svc"
install -m 0644 ./tunnel.service /etc/systemd/system/roboat.service

systemctl daemon-reload
systemctl enable roboat
systemctl restart roboat
echo "roboat restarted; status:"
systemctl --no-pager --full status roboat | head -n 8 || true
