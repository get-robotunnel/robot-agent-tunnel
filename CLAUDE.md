# CLAUDE.md — roboat (open-source connection layer)

This repo is the **shared, open-source tunnel**: the connection layer extracted
from the Operations platform and agent so that both Robot Operations and the
Robot Agent Registry can build on it. Apache-2.0. Org: `get-robotunnel`.

## What lives here vs. elsewhere

- **Here:** the wire protocol (`spec/`), the Rust agent/client connection crates
  (`rust/`), and the Go platform/relay (`go/`, binary `tunnel-svc`). Nothing
  business-specific — no debug skills, no ROS2 logic, no LLM, no billing.
- **`../robotunnel`** (Robot Operations, commercial, Go): imports the tunnel `go/`
  module and talks to `tunnel-svc` over a localhost internal API. Owns debug,
  monitoring, Discord, LLM, payments, and the `robots` *metadata*.
- **`../agent`** (Robot Operations agent, Rust): depends on this repo's `rust/`
  crates; keeps skills, dispatch, and the interaction bridge.
- **`../robot-agent-registry`** (open source): only stores tunnel *endpoint
  metadata*; does no relaying.

## Authoritative contract

`spec/tunnel-protocol.md` is the source of truth. `rust/` and `go/` are reference
implementations of it. If you change framing/handshake/signaling/route behavior,
update the spec in the same change.

## Boundaries (important)

- The tunnel authenticates robots with `robot_api_key` (`X-Robot-API-Key`). It
  does NOT know about `platform_token`/users — for client relay it validates a
  short-lived **relay session token** minted by ops via the internal API.
- Robot *connection identity* (id, agent_id, api_key hash, network info, route
  health, liveness) is owned by the tunnel DB. Robot *business metadata* (owner,
  name, role, tier) stays in the ops DB. They join on `robot_id`.
- This is a **lift-and-shift** of working code. Do not rewrite connection logic
  here; the reliability redesign is a separate, deferred effort.

## Deploy

`tunnel-svc` on `:8091`, systemd (`roboat` user), Caddy `tunnel.robotunnel.io`
plus a path-strangler on `api.robotunnel.io` so already-deployed agents cut over
with no client change. Rollback = flip the Caddy routes back to ops `:8080`.
Reference scripts in `deploy/`.

**Live:** deployed at `tunnel.robotunnel.io` on the shared VPS `92.5.43.70`
(Oracle Linux 9.7, **aarch64**; `ssh -i .ssh/vps.key opc@92.5.43.70`), unit
`roboat`, dir `/opt/roboat`, source `/opt/src/roboat`, native Postgres (db
`roboat`). Only Caddyfile **Part A** (`tunnel.robotunnel.io→8091`) is applied —
the `api.robotunnel.io` strangler is NOT enabled; the old ops `:8080` keeps
serving `api.` unchanged (coexist). TURN/coturn is not yet deployed (`TURN_HOST`
unset) — relay-only NAT traversal for now; add coturn when symmetric-NAT peers
need it. `deploy/.env` carries a committed `INTERNAL_API_SECRET` — the live box
uses a fresh one; rotate/untrack the committed value.

## Security

Connection secrets (`ROBOAT_AGENT_AUTH_SEED_HEX`, `TURN_SECRET`, the tunnel
`DATABASE_URL`, the internal-API shared secret) live in
`/opt/roboat/config/.env` (chmod 600), never in git. If you see live
secrets in any doc, flag for rotation — do not echo them.
