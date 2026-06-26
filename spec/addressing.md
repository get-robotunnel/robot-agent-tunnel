# roboat Addressing — agent_id → tunnel endpoint resolution

Status: **Implemented (Phase B)** · License: Apache-2.0

This document defines how a tunnel **initiator** resolves a registry `agent_id`
(`agt_xxx`) to a directly-diallable daemon endpoint (`host:port`), bridging the
registry identity layer and the tunnel routing layer.

---

## 1. Problem

The daemon-to-daemon tunnel uses a raw `host:port` to connect. The Robot Agent
Registry uses `agent_id` (`agt_xxx`) as the stable identity. An initiator that
knows only `agt_B` cannot connect without a lookup step.

---

## 2. Resolution flow

```
initiator daemon                 registry API                 responder daemon
     │                                │                              │
     │  1. dial("agt_B")              │                              │
     │                                │                              │
     │  2. GET /v1/discover/agents/agt_B                             │
     │  ──────────────────────────►   │                              │
     │                                │                              │
     │  3. { connection: { tunnel_endpoint: "1.2.3.4:11411" } }     │
     │  ◄──────────────────────────   │                              │
     │                                │                              │
     │  4. TCP connect + Ed25519 auth + StreamOpen (or RelayOpen)   │
     │  ────────────────────────────────────────────────────────►   │
```

The registry's `agents.connection.tunnel_endpoint` field is the `host:port`
advertised by the responder daemon. It is set/refreshed by the responder via
periodic **heartbeats** (Phase B).

---

## 3. Registry API

### Discovery (public, no auth)

```
GET /v1/discover/agents/{agent_id}
```

Response (200 OK):
```json
{
  "id": "agt_xxx",
  "connection": {
    "tunnel_endpoint": "1.2.3.4:11411"
  }
}
```

Returns 404 if the agent is not registered.

### Heartbeat (Agent-Signature auth)

```
POST /v1/agents/{agent_id}/heartbeat
X-Agent-Nonce: <16-byte-random-hex>
X-Agent-Timestamp: <unix-seconds>
X-Agent-Signature: <Ed25519-hex>
Content-Type: application/json

{"tunnel_endpoint":"1.2.3.4:11411","status":"online"}
```

**Signature** = `Ed25519.sign(agent_id + nonce + sha256hex(body))` using the
agent's private signing key. The registry verifies against the agent's registered
public key (derived from the same seed used for tunnel auth).

---

## 4. roboat-resolver crate

`rust/crates/roboat-resolver` implements two types:

### Resolver

```rust
pub struct Resolver { ... }

impl Resolver {
    pub fn new(registry_url: impl Into<String>) -> Self;
    /// Resolve agent_id → tunnel_endpoint (host:port).
    /// TTL cache: 60s positive, 5s negative.
    pub async fn resolve(&self, agent_id: &str) -> Result<String, ResolveError>;
    /// Remove a cached entry (call on dial failure to force re-fetch).
    pub async fn invalidate(&self, agent_id: &str);
}
```

### Registrar

```rust
pub struct Registrar { ... }

impl Registrar {
    /// Build from hex-encoded 32-byte Ed25519 seed.
    pub fn from_seed_hex(registry_url, agent_id, seed_hex: &str) -> Result<Self, RegisterError>;
    /// Send one heartbeat.
    pub async fn heartbeat(&self, tunnel_endpoint: &str) -> Result<(), RegisterError>;
    /// Spawn background task: sends heartbeat immediately then every `interval`.
    pub fn start_heartbeat_loop(self, tunnel_endpoint: String, interval: Duration) -> JoinHandle<()>;
}
```

---

## 5. Daemon integration

The daemon manager (`DaemonManager`) wires these together:

- **Dial**: if `target` starts with `agt_`, the `Resolver` maps it to `host:port`
  before dialing. Falls back to direct `host:port` if resolver is not configured.
- **Listen**: when `listen` IPC message contains `registry_token`, the manager
  calls `start_registry_heartbeat(agent_id, token, explicit_endpoint?)` which
  builds a `Registrar` and calls `start_heartbeat_loop`.
- **IP detection**: if no `tunnel_endpoint` is provided, `detect_local_ip()` (UDP
  connect trick to 8.8.8.8) guesses the outbound IP.

---

## 6. Fallback (Phase A compatibility)

If `dial` receives a raw `host:port` (no `agt_` prefix), the daemon dials it
directly without any registry interaction. Phase A clients are unaffected.
