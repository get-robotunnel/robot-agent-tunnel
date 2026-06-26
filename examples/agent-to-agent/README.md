# Example: agent-to-agent via daemon

Demonstrates two agents — one Python, one Go — communicating through two
`roboatd` daemon instances without writing a line of Rust.

## Architecture

```
Python responder agent
  └─► roboatd (socket: /tmp/roboat-responder.sock, TCP port: 11412)
          │
          │ (direct TCP tunnel connection)
          │
Go initiator agent
  └─► roboatd (socket: /tmp/roboat-initiator.sock, TCP port: 11411)
```

## Prerequisites

- Rust toolchain installed (`cargo build` from `rust/`)
- Python ≥ 3.10
- Go ≥ 1.22

## Steps

### 1. Build the daemon

```bash
cd ../../rust
cargo build -p roboatd
```

The binary is at `../../rust/target/debug/roboatd`.

### 2. Start the responder daemon

```bash
ROBOAT_SOCKET=/tmp/roboat-responder.sock \
ROBOAT_LISTEN_PORT=11412 \
../../rust/target/debug/roboatd
```

### 3. Start the initiator daemon

In another terminal:

```bash
ROBOAT_SOCKET=/tmp/roboat-initiator.sock \
ROBOAT_LISTEN_PORT=11411 \
../../rust/target/debug/roboatd
```

### 4. Run the Python responder

In another terminal:

```bash
ROBOAT_SOCKET=/tmp/roboat-responder.sock \
python3 python_responder.py
```

Expected output:
```
responder: listening for incoming connections...
```

### 5. Run the Go initiator

In another terminal:

```bash
cd go_initiator
ROBOAT_SOCKET=/tmp/roboat-initiator.sock \
go run . 127.0.0.1:11412
```

Expected output:
```
initiator: dialing 127.0.0.1:11412 ...
initiator: sent: "hello from go initiator!"
initiator: got reply: "hello from python responder!"
```

And the Python terminal should show:
```
responder: incoming stream 1 from <key-prefix>
responder: received: 'hello from go initiator!'
responder: done
```

## What this proves

Neither the Python nor the Go agent imports any Rust code. All tunnel complexity
(TCP connection, Ed25519 auth, RelayOpen handshake, data framing) is handled by
the two `roboatd` daemon instances. The agents talk only to their local Unix
socket.
