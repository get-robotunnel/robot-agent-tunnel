# RoboTunnel Daemon IPC Protocol — v0.2

Status: **Stable** · License: Apache-2.0

This document defines the local IPC protocol between a **daemon** (`robotunneld`)
and **agent processes** (in any language) running on the same machine.

The daemon (`robotunneld`) is the single component that owns all tunnel complexity:
NAT traversal, Ed25519 auth, WebRTC, connection management, and (in Phase B)
registry address resolution. Agent processes communicate with it via a local Unix
socket using this protocol.

---

## 1. Transport

| Property | Value |
|----------|-------|
| Default socket path (Unix) | `/var/run/robotunnel/rt.sock` |
| Windows | `\\.\pipe\robotunnel` (future) |
| Override | `RT_DAEMON_SOCKET` environment variable |
| Frame format | `[length: u32 big-endian][JSON payload: bytes]` |
| Max frame size | 4 MiB |
| Authentication | Local only (UNIX socket permissions); future: local token file |
| Encoding | UTF-8 JSON for all messages; binary data is base64-encoded within JSON |

Messages are full-duplex. Each side sends independently; there is no
request/response correlation except for `ping`/`pong` and operations that have
explicit response ops (`dial` → `connected`/`error`).

---

## 2. Message format

Every message is a JSON object with a mandatory string field `"op"` that identifies
the message type. Additional fields depend on the op.

### 2.1 Agent → daemon

| op | Required fields | Optional fields | Meaning |
|----|----------------|-----------------|---------|
| `listen` | `agent_id` | `registry_token`, `tunnel_endpoint` | Register as a responder on the daemon's TCP port. |
| `unlisten` | — | — | Deregister responder (Phase A: no-op). |
| `dial` | `target_agent_id`, `stream_class` | `request_id` | Connect to a remote agent. Phase A: `target_agent_id` is `host:port`. |
| `send` | `stream_id`, `data` | — | Send base64-encoded bytes on a stream. |
| `close` | `stream_id` | — | Close a stream. |
| `ping` | — | — | Keepalive. |

#### `listen`
```json
{"op":"listen","agent_id":"agt_B","registry_token":"<64-hex-Ed25519-seed>","tunnel_endpoint":"1.2.3.4:11411"}
```
Causes the daemon to start listening for inbound TCP tunnel connections (idempotent).
The daemon sends `incoming` notifications to this IPC client for each new inbound
connection.

- `registry_token` — hex-encoded Ed25519 seed (32 bytes = 64 hex chars) used for
  Agent-Signature heartbeats. When present, the daemon spawns a background heartbeat
  loop that keeps `tunnel_endpoint` current in the registry.
- `tunnel_endpoint` — explicit `host:port` to publish in the registry. If omitted,
  the daemon detects its local IP automatically.

#### `dial`
```json
{"op":"dial","target_agent_id":"127.0.0.1:11411","stream_class":"control","request_id":"r1"}
```
`stream_class` values: `"control"` (default), `"meta"`, `"bulk"`.

In Phase A/direct mode, `target_agent_id` is parsed as `host:port`. Starting in
Phase B, an `agt_xxx` target is resolved by `rt-resolver` via the registry
discovery API before dialing.

#### `send`
```json
{"op":"send","stream_id":7,"data":"aGVsbG8="}
```
`data` is standard base64 (RFC 4648).

### 2.2 Daemon → agent

| op | Fields | Meaning |
|----|--------|---------|
| `listening` | `agent_id` | Daemon is ready to receive inbound connections. |
| `connected` | `stream_id`, `target_agent_id`, `request_id?` | Dial succeeded. |
| `incoming` | `stream_id`, `from_agent_id`, `class` | New inbound stream arrived. |
| `recv` | `stream_id`, `data` | Bytes received from remote peer. |
| `closed` | `stream_id`, `reason?` | Stream was closed by remote or by error. |
| `pong` | — | Keepalive reply. |
| `error` | `code`, `message`, `request_id?` | Operation failed. |

#### Error codes

| code | Meaning |
|------|---------|
| `listen_failed` | Daemon could not bind TCP port. |
| `dial_failed` | Could not connect to target. |
| `send_failed` | Stream closed or not found. |
| `invalid_data` | base64 decode failed. |

---

## 3. Stream lifecycle

```
Initiator side:
  agent ──dial──► daemon ──TCP connect+auth+RelayOpen──► responder daemon
  daemon ──connected──► agent
  agent ──send──► daemon ──RelayData──► remote ──RelayData──► recv──► agent

Responder side:
  agent ──listen──► daemon (starts TCP listener)
  daemon ──incoming──► agent   (for each new inbound connection)
  recv ──► agent   (data from initiator)
  agent ──send──► daemon ──RelayData──► initiator
```

A stream is closed when:
- Either side sends `close` (IPC) or `RelayClose` (wire).
- The underlying TCP connection drops.
- The daemon sends a `closed` notification to the agent.

---

## 4. Wire handshake for daemon↔daemon connections

This describes what `robotunneld` does internally; implementers of the daemon
do not need to implement this (use the thin client library instead).

```
TCP connect
  └─► Ed25519 nonce-challenge (rt-core §3.1)
      └─► RelayOpen  [stream_id:u32 BE][class:u8]
          └─► RelayOpenAck [echo of RelayOpen payload]
              └─► RelayData / RelayData / ... (bidirectional)
                  └─► RelayClose (either side)
```

`stream_id` in the wire frame is assigned by the **initiator** and echoed in the
ack. Responder-side IPC `stream_id` is derived from this wire value.

---

## 5. Configuration (daemon)

| Env var | Default | Meaning |
|---------|---------|---------|
| `RT_DAEMON_SOCKET` | `/var/run/robotunnel/rt.sock` | IPC socket path. |
| `RT_DAEMON_LISTEN_PORT` | `11411` | TCP port for inbound tunnel connections. |
| `RT_DAEMON_INSECURE` | `true` | Accept any valid Ed25519 key (dev). Set `false` in production with an allowlist. |
| `RT_REGISTRY_URL` | — | Phase B: registry base URL (e.g. `https://reg.robotunnel.io`). |
| `RT_DAEMON_USE_MUX` | `true` | Phase C: use multiplexed connections (StreamOpen/Data/Close frames). |
| `RT_HEARTBEAT_INTERVAL_SECS` | `30` | Registry heartbeat interval in seconds. |

---

## 6. Versioning

This is IPC protocol **v0.2** (Phases A + B + C implemented).

Changes from v0.1:
- `listen` gains optional `registry_token` and `tunnel_endpoint` fields (Phase B).
- `dial` `target_agent_id` can now be an `agt_xxx` registry ID (Phase B).
- `stream_class` QoS aligns with tunnel protocol v0.3 (`StreamOpen`/`StreamData`/`StreamClose`) — scheduling is handled transparently by the daemon (Phase C).
