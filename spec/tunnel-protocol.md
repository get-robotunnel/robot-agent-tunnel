# RoboTunnel Tunnel Protocol — v0.3

Status: **Stable (reference implementation)** · License: Apache-2.0

This document is the authoritative wire-protocol contract for the **RoboTunnel
tunnel** — the open, shared connection layer that links a robot-side **agent** to
a **client** (CLI, browser, or another service) over the public internet, with
automatic path selection. It is the public-service contract that both
[Robot Operations](https://ops.robotunnel.io) and the
[Robot Agent Registry](https://reg.robotunnel.io) build on.

The reference implementations live in this repo:
- `rust/` — agent-side + client-side (crates `rt-connect-core`, `rt-connect-webrtc`)
- `go/`   — platform/relay-side (`cmd/tunnel-svc`, served at `tunnel.robotunnel.io`)

> **Scope note.** This spec describes the protocol *as implemented today*. The
> connection-layer **reliability redesign** (DP state machine, backpressure,
> platform-relay repositioning, P2P hardening — see the project roadmap) is a
> separate future effort and is intentionally **not** specified here yet.

---

## 1. Roles

| Role | Description | Auth |
|------|-------------|------|
| **agent** | Robot-side runtime. Hosts a local TCP tunnel server and an outbound control-plane connection to the platform. | `robot_api_key` (per-robot, `X-Robot-API-Key`) |
| **client** | CLI / browser / service initiating a debug or data session. | `platform_token` (Bearer) — issued by the Operations layer; the tunnel only validates a short-lived **relay session token** minted from it. |
| **platform / relay** | The hosted tunnel service (`tunnel-svc`). Brokers signaling, issues TURN credentials, hosts the control plane (CP) and data-plane (DP) relay. | trusts agent `robot_api_key`; validates client relay session tokens. |
| **initiator** | Daemon dialing a remote endpoint. | Ed25519 nonce-challenge (§3.1). |
| **responder** | Daemon accepting a connection. | Verifies initiator's Ed25519 signature. |

Connection identity (`robot_api_key`) is **independent** of business/user auth
(`platform_token`). A self-hosted tunnel needs only the former.

## 2. Endpoints

All HTTP(S)/WS(S) endpoints are served by `tunnel-svc` (default `:8091`,
public `https://tunnel.robotunnel.io`).

| Method | Path | Role | Purpose |
|--------|------|------|---------|
| GET | `/api/signal/:robot_id?role=agent\|client` | both | WebSocket signaling relay (SDP/ICE). |
| GET | `/api/turn-credentials?robot_id=<id>` | both | Short-lived TURN credential (HMAC-SHA1, 1h TTL). |
| GET | `/api/agent/connect` (alias `/v1/agent/connect`) | agent | Control-plane (CP) persistent connection. |
| GET | `/api/agent/relay` (alias `/v1/agent/relay`) | agent | Data-plane (DP) relay socket. |
| GET | `/api/relay/ws?robot_id=<id>&port=<p>&session_key=<tok>` | client | User relay stream (multiplexed over agent DP). |
| GET | `/api/agent-auth-public-key` | public | Ed25519 public key used to verify platform→agent auth. |
| GET | `/api/agent/authorized-keys` | agent | Authorized client public keys for the local tunnel allowlist. |
| POST | `/api/heartbeat` | agent | Liveness + self-reported network posture. |

## 3. Control-plane (CP) handshake & framing

The local agent tunnel server (default TCP `:11411`) and the platform CP channel
share one framed protocol.

### 3.1 Authentication handshake (Ed25519 nonce-challenge)

```
server → client:  [32-byte random nonce]
client → server:  [32-byte Ed25519 public key]
client → server:  [64-byte Ed25519 signature over the nonce]
server → client:  [0x01]   ; 1-byte ACK on success, else the connection closes
```

The server accepts the client if its public key is on the authorized list, or in
dev mode (`insecure_allow_any_client`) any valid signature is accepted.

### 3.2 Frame format

After the handshake, every message is a length-prefixed frame:

```
[frame_type: u8] [length: u32 big-endian] [payload: length bytes]
```

`length` MUST NOT exceed `64 MiB`.

### 3.3 Frame types

| Byte | Name | Direction | Payload |
|------|------|-----------|---------|
| `0x01` | `TunnelPacket` | agent ↔ client | Legacy ROS2 topic packet (see §3.4). |
| `0x02` | `CommandRequest` | platform → agent | JSON `{id, skill, action, params}`. |
| `0x03` | `CommandResponse` | agent → platform | JSON `{id, status: ok\|error\|timeout, data?, error?}`. |
| `0x10` | `Ping` | both | keepalive. |
| `0x11` | `Pong` | both | keepalive reply. |
| `0x20` | `WebRtcBootstrap` | platform → agent | JSON `{bootstrap_id, cli_public_ip?, cli_lan_cidr?, route_type?}`. |
| `0x21` | `WebRtcTeardown` | platform → agent | JSON `{bootstrap_id?}`. |
| `0x30` | `RelayOpen` | initiator → responder | Open a direct relay stream. Payload: `[stream_id:u32 BE][class:u8]`. |
| `0x31` | `RelayOpenAck` | responder → initiator | Relay open acknowledgement (echo of RelayOpen payload). |
| `0x32` | `RelayData` | both | Relay data chunk. |
| `0x33` | `RelayClose` | both | Relay stream close. |
| `0x40` | `StreamOpen` | initiator → responder | Open a multiplexed stream (v0.3). Payload: `[stream_id:u32 BE][class:u8]`. |
| `0x41` | `StreamData` | both | Data on a multiplexed stream. Payload: `[stream_id:u32 BE][data…]`. |
| `0x42` | `StreamClose` | both | Close a multiplexed stream. Payload: `[stream_id:u32 BE]`. |
| `0x43` | `FlowControl` | both | Credit-based flow control (reserved). Payload: `[stream_id:u32 BE][credits:u32 BE]`. |

### 3.4 Legacy TunnelPacket body (`0x01`)

Big-endian, used for the ROS2 topic proxy:

```
[topic_len: u16][topic][type_len: u16][msg_type][timestamp_ns: u64][payload_len: u32][payload]
```

## 4. Signaling messages

WebSocket JSON envelope on `/api/signal/:robot_id` (relayed verbatim between the
two peers; media/data never traverse the server):

```json
{ "type": "offer|answer|ice-candidate|ready|bye|session-preempted",
  "payload": <SDP | ICE candidate | null>,
  "robot_id": "<id>",
  "bootstrap_id": "<per-session correlation id>" }
```

- `ready` — peer is connected and ready to negotiate.
- `bye` — graceful teardown.
- `session-preempted` — the relay dropped a stale peer (3s debounce) in favor of
  a newer one for the same `robot_id`.

## 5. Route selection (path strategy)

The agent attempts routes in priority order; lower latency / cost first:

1. **LAN TCP** — direct on the local network.
2. **Public TCP** — direct to a reachable public address.
3. **STUN P2P** — ICE direct, no relay bandwidth.
4. **TURN relay** — coturn (`turn.robotunnel.io`), used only if STUN fails.
5. **Platform relay** — last-resort relay through `tunnel-svc`.

TURN credentials are HMAC-SHA1 with a 1-hour TTL, fetched from
`/api/turn-credentials`.

> The repositioning of platform-relay to an idle pre-established standby (rather
> than a last entry in the route table) is part of the deferred reliability
> redesign and is **not** reflected above yet.

## 6. Heartbeat & liveness

Agents POST `/api/heartbeat` periodically with self-reported posture (NAT type,
public/local IP, last-known-good route). The relay derives **online** from
heartbeat recency (default TTL window), falling back to live CP connection state.

## 7. Configuration (client/agent)

| Env | Default | Meaning |
|-----|---------|---------|
| `RT_API_URL` | `https://api.robotunnel.io` → migrating to `https://tunnel.robotunnel.io` | Tunnel base URL. Signaling/TURN URLs are derived from it. |
| `RT_API_KEY` | — | `robot_api_key`. |
| `RT_LISTEN_PORT` | `11411` | Local tunnel TCP port. |
| `RT_WEBRTC_ENABLED` | `true` | Enable STUN/TURN path. |
| `RT_AUTHORIZED_KEYS` | — | Comma-separated hex Ed25519 client keys. |

## 8. Multiplexed connections (v0.3, Phase C)

When both daemons support v0.3, a single TCP connection carries N logical streams.
The mode is detected by the first frame: `RelayOpen` (0x30) = per-stream mode;
`StreamOpen` (0x40) = multiplexed mode.

### Stream ID assignment

- **Initiator** uses odd IDs: 1, 3, 5 …
- **Responder** uses even IDs: 2, 4, 6 … (for responder-initiated streams, reserved).

### QoS scheduling

Outbound frames are enqueued by stream class:

| Class | Byte | Queue |
|-------|------|-------|
| `control` | `0x01` | high-priority |
| `meta`    | `0x02` | high-priority |
| `bulk`    | `0x03` | low-priority |

The write task drains the high-priority queue completely before serving bulk. A
`biased tokio::select!` ensures control frames win when both queues become ready
simultaneously.

### Connection pooling

Each daemon maintains one mux connection per remote `host:port` (outbound pool).
New streams over the same endpoint reuse the existing TCP connection without
re-authentication.

---

## 9. Versioning

This is protocol **v0.3**. Frame-type bytes are stable and append-only; new
frame types take unused byte values. Breaking changes bump the minor version and
are negotiated out-of-band (via the agent build channel) until an in-band
version handshake is added.

Changes from v0.2:
- Added `initiator` and `responder` roles for daemon-to-daemon connections.
- Added `StreamOpen` (0x40), `StreamData` (0x41), `StreamClose` (0x42),
  `FlowControl` (0x43) frames for multiplexed mode.
- Mode detection via first frame: 0x30 → direct relay; 0x40 → multiplexed.
