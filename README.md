# RoboTunnel Tunnel

**Agent-to-agent tunneling for robots.** Connect any two robot agents directly —
across NAT, firewalls, and networks — with automatic path selection and zero
broker dependency once connected.

Apache-2.0 · [Robot Agent Registry](https://reg.robotunnel.io) · [Docs](spec/)

---

## What this is

A daemon (`robotunneld`) runs alongside your agent process. Agents talk to it
over a local Unix socket using a simple JSON IPC protocol. The daemon handles
everything else: Ed25519 authentication, NAT traversal, connection multiplexing,
and (optionally) registry-based discovery so agents find each other by ID rather
than IP address.

```
robot A                          robot B
────────                         ────────
your code                        your code
   │  IPC (Unix socket)             │  IPC
   ▼                                ▼
robotunneld ── Ed25519 TCP ──► robotunneld
   │                                │
rt-resolver ◄── GET /discover ──► registry
```

Path selection: **LAN TCP → public TCP → STUN P2P → TURN relay**.

---

## Getting started

### 1. Run the daemon

```bash
# From source
cargo build --release -p robotunneld
./target/release/robotunneld

# Or with registry discovery enabled
RT_REGISTRY_URL=https://reg.robotunnel.io ./target/release/robotunneld
```

Default socket: `/var/run/robotunnel/rt.sock`  
Default TCP port: `11411`

### 2. Connect from your agent

**Python**
```python
import asyncio
from rt_connect import Daemon

async def main():
    async with Daemon() as rt:
        # Responder side
        await rt.listen(
            agent_id="agt_B",
            registry_token="<64-hex-ed25519-seed>",  # optional: publish to registry
        )
        async for stream in rt.incoming():
            data = await stream.recv()
            await stream.send(b"pong: " + data)

asyncio.run(main())
```

**Go**
```go
rt, _ := rtconnect.NewDaemon("")
rt.Listen("agt_A", "")

for stream := range rt.Incoming() {
    data, _ := stream.Recv()
    stream.Send(append([]byte("pong: "), data...))
}
```

**Initiator (either language)**
```python
stream = await rt.dial("agt_B")          # or "192.168.1.10:11411" directly
await stream.send(b"hello")
reply = await stream.recv()
```

See [`examples/agent-to-agent/`](examples/agent-to-agent/) for a full Python ↔ Go demo.

---

## Layout

```
spec/
  tunnel-protocol.md   Wire protocol v0.3 (authoritative contract)
  daemon-ipc.md        Local IPC protocol v0.2
  addressing.md        agent_id → tunnel_endpoint resolution
rust/
  crates/rt-core       Protocol framing + Ed25519 auth
  crates/rt-daemon     Daemon manager + IPC server + mux
  crates/rt-resolver   Registry discovery + heartbeat
  crates/rt-webrtc     WebRTC path (STUN/TURN)
  bin/robotunneld      Daemon binary
clients/
  python/rt_connect    Async Python client (pip install rt-connect)
  go/rtconnect         Go client module
examples/
  agent-to-agent/      Python responder + Go initiator demo
deploy/
  robotunneld.service  systemd unit
  coturn.conf          TURN server reference config
  self-host-full.sh    One-click self-hosted stack (daemon + registry + TURN)
  README.md            Deploy guide
```

---

## Configuration

| Env var | Default | Meaning |
|---------|---------|---------|
| `RT_DAEMON_SOCKET` | `/var/run/robotunnel/rt.sock` | IPC socket path |
| `RT_DAEMON_LISTEN_PORT` | `11411` | TCP port for inbound connections |
| `RT_DAEMON_INSECURE` | `true` | Accept any valid Ed25519 key (dev mode) |
| `RT_REGISTRY_URL` | — | Registry URL for agent discovery / heartbeat |
| `RT_DAEMON_USE_MUX` | `true` | Multiplex streams over one TCP connection |
| `RT_HEARTBEAT_INTERVAL_SECS` | `30` | Registry heartbeat interval |

---

## Protocol

The wire protocol is defined in [`spec/tunnel-protocol.md`](spec/tunnel-protocol.md).
The frame format is `[type:u8][len:u32][payload]`.

| Range | Purpose |
|-------|---------|
| `0x01–0x33` | Legacy + relay frames (v0.1/v0.2, backward compatible) |
| `0x40–0x43` | Multiplexed stream frames: StreamOpen / StreamData / StreamClose / FlowControl (v0.3) |

Authentication is Ed25519 nonce-challenge: server sends 32-byte nonce → client
replies `[pubkey(32) ‖ sig(64)]` → server sends `0x01` accept.

---

## Self-hosting

Run the full open-source stack on your own VPS:

```bash
export DOMAIN=example.com ADMIN_EMAIL=you@example.com
sudo -E bash deploy/self-host-full.sh
```

Installs: `tunnel-svc` (signaling/relay), `robot-agent-registry`, `robotunneld`,
`coturn` (TURN), and `Caddy` (TLS). See [`deploy/README.md`](deploy/README.md).

---

## Related repos

- **[robot-agent-registry](https://github.com/get-robotunnel/robot-agent-registry)** — open-source agent registration & discovery (Apache-2.0)
- **[Robot Operations](https://ops.robotunnel.io)** — commercial ops platform built on this tunnel

---

## License

Apache-2.0. See [LICENSE](LICENSE).
