#!/usr/bin/env bash
# One-time VPS preparation for the tunnel service: config dir, .env template,
# Caddy tunnel.robotunnel.io vhost. Run as root. Re-running is safe; it will not
# overwrite an existing .env.
#
# NOTE: this does NOT modify the existing api.robotunnel.io vhost. The
# zero-downtime cutover strangler (Part B in Caddyfile.tunnel) is applied
# manually during the cutover step so it can be staged and rolled back.
set -euo pipefail

APP_DIR=/opt/roboat
CONF="$APP_DIR/config/.env"

id -u roboat >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin roboat
install -d -o roboat -g roboat "$APP_DIR" "$APP_DIR/bin" "$APP_DIR/config"

if [ ! -f "$CONF" ]; then
  cat >"$CONF" <<'EOF'
PORT=8091
# Postgres connection string for the tunnel's OWN database (separate from ops + registry).
DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/postgres?sslmode=require
TUNNEL_BASE_URL=https://tunnel.robotunnel.io
# coturn — connection layer owns these now (moved out of the ops .env).
TURN_HOST=turn.robotunnel.io
TURN_SECRET=CHANGE_ME
TURN_ADVERTISE_TLS=true
# ed25519 seed for platform->agent auth (moved out of the ops .env).
ROBOAT_AGENT_AUTH_SEED_HEX=CHANGE_ME
# ops internal API (localhost) + shared secret for the ops<->tunnel boundary.
OPS_INTERNAL_URL=http://127.0.0.1:8080
INTERNAL_API_SECRET=CHANGE_ME
HEARTBEAT_OFFLINE_SECS=60
EOF
  chmod 600 "$CONF"
  chown roboat:roboat "$CONF"
  echo "Wrote template $CONF — edit DATABASE_URL, TURN_SECRET, ROBOAT_AGENT_AUTH_SEED_HEX, INTERNAL_API_SECRET before starting."
else
  echo "$CONF already exists; leaving it untouched."
fi

# Append the canonical tunnel vhost (Part A) if not already present.
CADDYFILE=${CADDYFILE:-/etc/caddy/Caddyfile}
if [ -f "$CADDYFILE" ] && ! grep -q "tunnel.robotunnel.io" "$CADDYFILE"; then
  printf '\ntunnel.robotunnel.io {\n\treverse_proxy 127.0.0.1:8091\n}\n' >>"$CADDYFILE"
  systemctl reload caddy || true
  echo "Added tunnel.robotunnel.io vhost to $CADDYFILE and reloaded Caddy."
fi

echo "Bootstrap done. Next: run setup.sh to install the binary, then 'systemctl start roboat'."
echo "Cutover (api.robotunnel.io strangler) is a separate, staged manual step — see deploy/README.md."
