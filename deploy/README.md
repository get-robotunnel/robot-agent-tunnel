# Deploying the roboat tunnel service

`tunnel-svc` runs on the shared VPS on port **8091**, behind Caddy at
`tunnel.robotunnel.io`, as a separate systemd service from Robot Operations
(`:8080`) and the Registry (`:8090`).

## Prerequisites (operator-provided)

1. **DNS:** `tunnel.robotunnel.io` A record → VPS IP.
2. **Dedicated database:** a Postgres/Supabase project for the tunnel, separate
   from ops and registry. Put its connection string in `DATABASE_URL`.
3. **GitHub `production` environment secrets** (shared with ops + registry):
   `PROD_SSH_HOST`, `PROD_SSH_PORT`, `PROD_SSH_USER`, `PROD_SSH_PRIVATE_KEY`,
   `PROD_SSH_KNOWN_HOSTS`.
4. **Connection secrets** moved from the ops `.env` into the tunnel `.env`:
   `TURN_SECRET`, `ROBOAT_AGENT_AUTH_SEED_HEX` (see the rotation note below — rotate
   while moving them), plus a fresh `INTERNAL_API_SECRET`
   (`openssl rand -hex 32`) shared with ops.

## First deploy

```bash
# On the VPS, as root — one-time prep (config template + tunnel vhost):
sudo ./bootstrap.sh
sudo vi /opt/roboat/config/.env    # fill DATABASE_URL, TURN_SECRET, ROBOAT_AGENT_AUTH_SEED_HEX, INTERNAL_API_SECRET

# Then deploy the binary (from GitHub → Actions → "Deploy Tunnel"), or by hand:
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tunnel-svc ./cmd/tunnel-svc   (run in go/)
#   scp tunnel-svc tunnel.service setup.sh root@VPS:/tmp && ssh root@VPS 'cd /tmp && ./setup.sh'
```

Migrations apply automatically on boot. Health check:
`curl https://tunnel.robotunnel.io/health`.

## Zero-downtime cutover (the api.robotunnel.io strangler)

Already-deployed agents talk to `api.robotunnel.io`. To move them onto the
tunnel service **without a client change or downtime**, route the connection
path-prefixes on the existing `api.robotunnel.io` Caddy vhost to `:8091` (see
Part B in `Caddyfile.tunnel`). Stage it:

- **Phase 1** — once signaling + TURN are live on tunnel-svc *and* ops exposes
  the internal endpoints (`/internal/authz/client`, `/internal/agent/bootstrap`)
  and robots are provisioned into `robot_conn`: route `/api/signal/*` and
  `/api/turn-credentials*` to `:8091`. Verify with `val/route-acceptance.sh` /
  `val/route-matrix.sh` against a live agent.
- **Phase 2** — once the CP/DP relay is extracted: add the remaining prefixes
  (`/api/agent/connect*`, `/api/agent/relay*`, `/v1/agent/*`, `/api/relay/ws*`,
  `/api/heartbeat*`, `/api/agent-auth-public-key*`, `/api/agent/authorized-keys*`).

**Rollback:** remove the `@tunnel` matcher/route from the `api.robotunnel.io`
block and `systemctl reload caddy` — traffic returns to ops `:8080`, which still
contains the connection handlers until the extraction is finalized.

---

## Self-hosting the full open-source stack

For teams running entirely on their own infrastructure (no hosted
`tunnel.robotunnel.io` or `reg.robotunnel.io`), `self-host-full.sh` installs
everything on a single Ubuntu VPS.

### What's installed

| Component | Binary | Default port |
|-----------|--------|-------------|
| `tunnel-svc` | Go | 8091 (Caddy → tunnel.YOUR_DOMAIN) |
| `robot-agent-registry` | Go | 8090 (Caddy → reg.YOUR_DOMAIN) |
| `roboatd` | Rust | 11411 (direct TCP, agent → agent) |
| `coturn` | C | 3478 UDP/TCP, 5349 TLS |
| `Caddy` | Go | 80, 443 (TLS termination) |

### Quick start

```bash
# 1. Set your domain and email
export DOMAIN=example.com
export ADMIN_EMAIL=you@example.com

# 2. Run the setup script (as root on a fresh Ubuntu 22.04+ VPS)
sudo -E bash deploy/self-host-full.sh

# 3. Build and place the binaries (see script output for exact commands)
#    then:
sudo systemctl start roboat roboar roboatd

# 4. Verify
curl https://tunnel.example.com/health
curl https://reg.example.com/health
```

### TURN server (coturn)

`deploy/coturn.conf` is an annotated reference configuration. Key points:

- Uses **HMAC-SHA1 time-limited credentials**: `tunnel-svc` generates them
  with `TURN_SECRET`; coturn verifies them. Agents never see the secret.
- TLS on port 5349 requires certificates — point coturn at the Caddy-managed
  certs (or use your own).
- Relay port range `49152–65535` must be open in your firewall (UDP + TCP).
- `no-stun` prevents unauthenticated STUN allocation.
- `denied-peer-ip` blocks relay to RFC 1918 addresses (SSRF protection).

### Agent configuration

On the robot side, point the daemon at your self-hosted stack:

```bash
# /etc/roboat/daemon.env (or add to the EnvironmentFile)
ROBOAT_REGISTRY_URL=https://reg.example.com
ROBOAT_LISTEN_PORT=11411
ROBOAT_INSECURE=false
```

In the agent IPC `listen` call, pass the registry token:
```json
{"op":"listen","agent_id":"agt_B","registry_token":"<64-hex-Ed25519-seed>"}
```

Dial other agents by `agent_id` — resolution is automatic:
```json
{"op":"dial","target_agent_id":"agt_A","stream_class":"control"}
```

## Security / rotation

The tunnel now owns the connection secrets. When you move `TURN_SECRET` and
`ROBOAT_AGENT_AUTH_SEED_HEX` out of the ops `.env`, **rotate them** (they were
exposed in plaintext historically). Rotating `ROBOAT_AGENT_AUTH_SEED_HEX` changes
`/api/agent-auth-public-key`; agents re-fetch it, so do it in a maintenance
window. Keep all secrets in `/opt/roboat/config/.env` (chmod 600),
never in git.
