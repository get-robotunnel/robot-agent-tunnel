---
name: tunnel-verify
description: Build and verify the roboat tunnel service (Go tunnel-svc + Rust connection crates) locally. Use after changing anything under roboat/ to confirm both implementations still build, vet, and test before pushing or deploying.
---

# Verify the tunnel service

Run from the repo root (`roboat/`).

## Go (tunnel-svc: signaling, TURN, CP/DP relay, internal API)

```bash
cd go
go build ./...
go vet ./...
go test ./...
```

`connstate` has unit tests; the rest are wired in `cmd/tunnel-svc`. A clean
build + vet + green `connstate` tests is the bar for a structural change.

## Rust (connection crates roboat-core, roboat-webrtc)

```bash
cd rust
cargo build --locked      # pulls the patched webrtc-ice fork from vendor/
cargo test --locked
```

## Run tunnel-svc locally (smoke)

`INTERNAL_API_SECRET` is required; `DATABASE_URL` is optional (omit it to defer
robot auth to ops — Phase 1). `OPS_INTERNAL_URL` points at a running ops platform.

```bash
cd go
INTERNAL_API_SECRET=devsecret OPS_INTERNAL_URL=http://127.0.0.1:8080 \
  go run ./cmd/tunnel-svc &
curl -s localhost:8091/health        # -> {"status":"ok","service":"tunnel-svc"}
```

## What to check after a connection-code change

- The wire protocol still matches `spec/tunnel-protocol.md` (update the spec in
  the same change if framing/handshake/signaling/route behavior changed).
- The `relay` package only depends on the `RobotResolver` seam — never on ops
  internals. The `webrtc` package only depends on `connauth.Authenticator`.
