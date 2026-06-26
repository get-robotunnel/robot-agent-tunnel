package main

// MCP reverse-proxy: GET /mcp/:agent_id (and any sub-path) is routed to the
// agent's registered mcp_endpoint by looking it up in the robot-agent-registry
// and HTTP-proxying there (spec agent-registration-mcp-spec §2.3).
//
// This provides the public-facing URL:
//   https://tunnel.robotunnel.io/mcp/<agent_id>
//
// In production the mcp_endpoint is typically the agent's local address, and a
// full tunnel-relay implementation (Phase 2) would forward the HTTP traffic
// through the existing WebSocket relay connection. For now we do a direct HTTP
// proxy, which works when the registry stores a publicly accessible endpoint.

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// registryAgent is the minimal subset of model.Agent we need.
type registryAgent struct {
	Connection struct {
		MCPEndpoint string `json:"mcp_endpoint"`
	} `json:"connection"`
}

type mcpProxyHandler struct {
	registryURL string
	httpClient  *http.Client
}

func newMCPProxyHandler(registryURL string) *mcpProxyHandler {
	return &mcpProxyHandler{
		registryURL: strings.TrimRight(registryURL, "/"),
		httpClient:  &http.Client{Timeout: 5 * time.Second},
	}
}

// lookupMCPEndpoint fetches the agent's mcp_endpoint from the registry.
func (h *mcpProxyHandler) lookupMCPEndpoint(agentID string) (string, error) {
	u := fmt.Sprintf("%s/v1/discover/agents/%s", h.registryURL, agentID)
	resp, err := h.httpClient.Get(u)
	if err != nil {
		return "", fmt.Errorf("registry lookup: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("agent %s not found", agentID)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registry returned %d", resp.StatusCode)
	}
	var agent registryAgent
	if err := json.NewDecoder(resp.Body).Decode(&agent); err != nil {
		return "", fmt.Errorf("decode registry response: %w", err)
	}
	if agent.Connection.MCPEndpoint == "" {
		return "", fmt.Errorf("agent %s has no mcp_endpoint registered", agentID)
	}
	return agent.Connection.MCPEndpoint, nil
}

// handleMCPProxy proxies HTTP requests for /mcp/:agent_id/** to the agent's
// registered mcp_endpoint.
func (h *mcpProxyHandler) handleMCPProxy(c *gin.Context) {
	if h.registryURL == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "REGISTRY_URL not configured; MCP proxy unavailable",
		})
		return
	}

	agentID := c.Param("agent_id")
	mcpEndpoint, err := h.lookupMCPEndpoint(agentID)
	if err != nil {
		log.Printf("[mcp-proxy] lookup failed agent=%s: %v", agentID, err)
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	target, err := url.Parse(mcpEndpoint)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "invalid mcp_endpoint: " + err.Error()})
		return
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			// Strip /mcp/:agent_id prefix; keep the rest of the path.
			suffix := strings.TrimPrefix(c.Param("path"), "/")
			req.URL.Path = target.Path
			if suffix != "" {
				req.URL.Path = strings.TrimRight(target.Path, "/") + "/" + suffix
			}
			req.Host = target.Host
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, proxyErr error) {
			log.Printf("[mcp-proxy] proxy error agent=%s: %v", agentID, proxyErr)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"error":"upstream MCP server unreachable"}`)
		},
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}
