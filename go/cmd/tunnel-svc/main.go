// Command tunnel-svc is the RoboTunnel tunnel service: the open-source,
// standalone connection layer (signaling, TURN credentials, and — as the
// extraction completes — the CP/DP relay) shared by Robot Operations and the
// Robot Agent Registry.
//
// This binary serves the connection endpoints under tunnel.robotunnel.io.
// During the zero-downtime cutover, Caddy routes the connection path-prefixes
// on api.robotunnel.io here while ops business endpoints stay on the platform.
package main

import (
	"context"
	"encoding/hex"
	"log"
	"net/http"
	"strings"

	"github.com/get-robotunnel/robotunnel-tunnel/go/internal/config"
	"github.com/get-robotunnel/robotunnel-tunnel/go/internal/opsauth"
	"github.com/get-robotunnel/robotunnel-tunnel/go/internal/store"
	"github.com/get-robotunnel/robotunnel-tunnel/go/relay"
	"github.com/get-robotunnel/robotunnel-tunnel/go/webrtc"
	"github.com/gin-gonic/gin"
)

// authenticator satisfies connauth.Authenticator. Robot (agent) auth is
// local-first: if the robot is provisioned in the tunnel identity store, verify
// against it; otherwise fall back to the ops authority (transition behaviour).
// Client (CLI/browser) auth always goes to ops, which owns platform_token.
type authenticator struct {
	store *store.Store // nil in Phase 1 (no dedicated tunnel DB yet)
	ops   *opsauth.Client
}

func (a *authenticator) VerifyRobotAPIKey(robotID, apiKey string) (bool, error) {
	if a.store != nil {
		hash, found, err := a.store.LookupAPIKeyHash(robotID)
		if err != nil {
			return false, err
		}
		if found {
			return hash != "" && hash == store.HashAPIKey(apiKey), nil
		}
	}
	return a.ops.VerifyRobotAPIKey(robotID, apiKey)
}

func (a *authenticator) AuthorizeClient(c *gin.Context, robotID string) bool {
	return a.ops.AuthorizeClient(c, robotID)
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[tunnel-svc] config: %v", err)
	}

	ctx := context.Background()
	var st *store.Store
	if cfg.DatabaseURL != "" {
		st, err = store.New(ctx, cfg.DatabaseURL)
		if err != nil {
			log.Fatalf("[tunnel-svc] store: %v", err)
		}
		defer st.Close()
		if err := st.Migrate(ctx); err != nil {
			log.Fatalf("[tunnel-svc] migrate: %v", err)
		}
		log.Printf("[tunnel-svc] tunnel identity store active (local-first robot auth)")
	} else {
		log.Printf("[tunnel-svc] no DATABASE_URL — deferring robot auth to ops (Phase 1)")
	}

	ops := opsauth.New(cfg.OpsInternalURL, cfg.InternalSecret)
	authn := &authenticator{store: st, ops: ops}

	// CP/DP relay: the agent control plane, agent data-plane relay, and user
	// relay all live here now. The signaling bootstrap trigger is wired to the
	// local relay (the agent's control channel is held by this process).
	resolver := &robotResolver{store: st, ops: ops}
	var seed []byte
	if h := strings.TrimSpace(cfg.AgentAuthSeedHex); h != "" {
		if b, derr := hex.DecodeString(h); derr == nil {
			seed = b
		} else {
			log.Printf("[tunnel-svc] WARNING: invalid ROBOAT_AGENT_AUTH_SEED_HEX: %v", derr)
		}
	}
	relaySvc := relay.NewServer(resolver, seed)

	r := gin.New()
	r.Use(gin.Recovery())

	health := func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "tunnel-svc"}) }
	r.GET("/health", health)
	r.GET("/api/health", health)

	// WebRTC connection surface.
	r.GET("/api/turn-credentials",
		webrtc.TURNCredentialHandler(authn, cfg.TurnHost, cfg.TurnSecret, cfg.TurnAdvertiseTLS))
	r.GET("/api/signal/:robot_id",
		webrtc.SignalingHandler(authn, relaySvc.TriggerBootstrap, relaySvc.SetActiveSession))

	// CP/DP relay endpoints.
	relaySvc.Routes(r)

	// Internal API consumed by ops (gated by INTERNAL_API_SECRET): send commands
	// to a robot over its control channel, and query connection liveness.
	registerInternalAPI(r, cfg.InternalSecret, relaySvc.Hub())

	// MCP reverse-proxy: /mcp/:agent_id routes incoming MCP tool calls to the
	// agent's registered mcp_endpoint (looked up from REGISTRY_URL).
	// Phase 2 will forward through the tunnel relay for NAT-traversed agents.
	mcpProxy := newMCPProxyHandler(cfg.RegistryURL)
	r.Any("/mcp/:agent_id", mcpProxy.handleMCPProxy)
	r.Any("/mcp/:agent_id/*path", mcpProxy.handleMCPProxy)

	addr := ":" + cfg.Port
	log.Printf("[tunnel-svc] listening on %s (base=%s)", addr, cfg.BaseURL)
	if err := r.Run(addr); err != nil {
		log.Fatalf("[tunnel-svc] run: %v", err)
	}
}
