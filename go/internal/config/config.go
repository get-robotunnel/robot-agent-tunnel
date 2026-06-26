// Package config loads tunnel-svc runtime configuration from the environment.
package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	Port             string // HTTP listen port (default 8091)
	DatabaseURL      string // tunnel Postgres connection string (optional in Phase 1)
	BaseURL          string // public base URL, e.g. https://tunnel.robotunnel.io
	TurnHost         string // coturn host, e.g. turn.robotunnel.io
	TurnSecret       string // coturn shared secret (HMAC-SHA1 credentials)
	TurnAdvertiseTLS bool   // also advertise turns:host:5349
	AgentAuthSeedHex string // ed25519 seed for platform->agent auth (hex)
	OpsInternalURL   string // ops internal API base, e.g. http://127.0.0.1:8080
	InternalSecret   string // shared secret for the ops<->tunnel internal API
	OfflineAfterSecs int    // heartbeat staleness window; default 60
	RegistryURL      string // robot-agent-registry base URL (optional; enables /mcp/:agent_id proxy)
}

// Load reads configuration from environment variables and applies defaults.
//
// DATABASE_URL is OPTIONAL: in Phase 1 the tunnel defers robot/client auth to
// the ops internal API, so it can run with no dedicated database. When set, the
// tunnel uses its own robot_conn identity store (local-first, ops-fallback).
// INTERNAL_API_SECRET is required so the ops internal API calls are authenticated.
func Load() (*Config, error) {
	c := &Config{
		Port:             getenv("PORT", "8091"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		BaseURL:          getenv("TUNNEL_BASE_URL", "https://tunnel.robotunnel.io"),
		TurnHost:         os.Getenv("TURN_HOST"),
		TurnSecret:       os.Getenv("TURN_SECRET"),
		TurnAdvertiseTLS: getenvBool("TURN_ADVERTISE_TLS", true),
		AgentAuthSeedHex: os.Getenv("ROBOAT_AGENT_AUTH_SEED_HEX"),
		OpsInternalURL:   getenv("OPS_INTERNAL_URL", "http://127.0.0.1:8080"),
		InternalSecret:   os.Getenv("INTERNAL_API_SECRET"),
		OfflineAfterSecs: getenvInt("HEARTBEAT_OFFLINE_SECS", 60),
		RegistryURL:      os.Getenv("REGISTRY_URL"),
	}
	if c.InternalSecret == "" {
		return nil, errors.New("INTERNAL_API_SECRET is required")
	}
	return c, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
