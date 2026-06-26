# roboat thin clients

These libraries let agents in any language use the tunnel by connecting to the
local `roboatd` daemon socket. They contain **no transport code** — all tunnel
complexity lives in the daemon.

## Architecture

```
Agent (Python/Go/any) ── Unix socket ──► roboatd ── TCP/WebRTC ──► remote daemon ──► remote agent
```

Each library does exactly one thing: encode/decode the
[daemon IPC protocol](../spec/daemon-ipc.md) over the local Unix socket.

## Available clients

| Language | Directory | Package |
|----------|-----------|---------|
| Python ≥ 3.10 | `python/` | `roboat` (zero dependencies) |
| Go ≥ 1.22 | `go/` | `roboat` (stdlib only) |

## Quick start

### Python

```python
from roboat import Daemon

# Responder
async with Daemon() as rt:
    await rt.listen(agent_id="agt_responder")
    async for stream in rt.incoming():
        data = await stream.recv()
        await stream.send(b"pong: " + data)

# Initiator (Phase A: host:port; Phase B: agent_id)
async with Daemon() as rt:
    stream = await rt.dial("127.0.0.1:11412")
    await stream.send(b"ping")
    reply = await stream.recv()
```

### Go

```go
rt, _ := roboat.NewDaemon("")
defer rt.Close()

// Initiator
stream, _ := rt.Dial("127.0.0.1:11412", "control")
stream.Send([]byte("ping"))
reply, _ := stream.Recv()

// Responder
rt.Listen("agt_responder", "")
ch, _ := rt.Incoming(ctx)
for s := range ch {
    data, _ := s.Recv()
    s.Send(append([]byte("pong: "), data...))
}
```

## Protocol reference

See [`spec/daemon-ipc.md`](../spec/daemon-ipc.md) for the full IPC protocol spec.

## Design principles

- **Zero dependencies**: Python client uses only stdlib; Go client uses only stdlib.
- **Thin**: each client is a few hundred lines — no transport, no crypto.
- **Async-first**: Python client is native async; sync wrapper provided for simple scripts.
- **Base64 for binary**: all binary data is base64-encoded in JSON to keep parsing trivial.
