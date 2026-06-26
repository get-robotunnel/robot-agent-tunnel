//! RoboTunnel registry resolver.
//!
//! Two responsibilities:
//! 1. **Resolver** — maps a registry `agent_id` (`agt_xxx`) to a `tunnel_endpoint`
//!    (`host:port`) by querying `GET /v1/discover/agents/{agent_id}`.  Results are
//!    TTL-cached (60 s positive, 5 s negative).
//!
//! 2. **Registrar** — sends periodic heartbeats to `POST /v1/agents/{agent_id}/heartbeat`
//!    using Agent-Signature auth (Ed25519), updating the `tunnel_endpoint` field so
//!    other agents can discover this daemon.

use std::{
    collections::HashMap,
    net::UdpSocket,
    sync::Arc,
    time::{Duration, Instant},
};

use ed25519_dalek::{Signer, SigningKey};
use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;

// ── Resolver ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Agent not found: {0}")]
    NotFound(String),
    #[error("Agent has no tunnel_endpoint: {0}")]
    NoEndpoint(String),
    #[error("Cached as unavailable (try again shortly)")]
    NegativeCached,
}

struct CacheEntry {
    value: Option<String>, // Some(endpoint) = positive; None = negative
    expires: Instant,
}

#[derive(Deserialize)]
struct DiscoverConnection {
    #[serde(default)]
    tunnel_endpoint: String,
}

#[derive(Deserialize)]
struct DiscoverAgent {
    connection: DiscoverConnection,
}

/// Resolves registry `agent_id` → `tunnel_endpoint` with TTL cache.
pub struct Resolver {
    registry_url: String,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    client: reqwest::Client,
}

impl Resolver {
    pub fn new(registry_url: impl Into<String>) -> Self {
        Self {
            registry_url: registry_url.into().trim_end_matches('/').to_string(),
            cache: Arc::new(Mutex::new(HashMap::new())),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
        }
    }

    /// Resolve `agent_id` → `tunnel_endpoint`.  Caches the result.
    pub async fn resolve(&self, agent_id: &str) -> Result<String, ResolveError> {
        {
            let cache = self.cache.lock().await;
            if let Some(entry) = cache.get(agent_id) {
                if entry.expires > Instant::now() {
                    return match &entry.value {
                        Some(ep) => Ok(ep.clone()),
                        None => Err(ResolveError::NegativeCached),
                    };
                }
            }
        }

        let url = format!("{}/v1/discover/agents/{}", self.registry_url, agent_id);
        tracing::debug!("resolver: GET {}", url);
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();

        let mut cache = self.cache.lock().await;

        if status == 404 {
            cache.insert(agent_id.to_string(), CacheEntry {
                value: None,
                expires: Instant::now() + Duration::from_secs(5),
            });
            return Err(ResolveError::NotFound(agent_id.to_string()));
        }
        if !status.is_success() {
            return Err(ResolveError::Http(resp.error_for_status().unwrap_err()));
        }

        let agent: DiscoverAgent = resp.json().await?;
        let ep = agent.connection.tunnel_endpoint;

        if ep.is_empty() {
            cache.insert(agent_id.to_string(), CacheEntry {
                value: None,
                expires: Instant::now() + Duration::from_secs(5),
            });
            return Err(ResolveError::NoEndpoint(agent_id.to_string()));
        }

        tracing::debug!("resolver: {} → {}", agent_id, ep);
        cache.insert(agent_id.to_string(), CacheEntry {
            value: Some(ep.clone()),
            expires: Instant::now() + Duration::from_secs(60),
        });
        Ok(ep)
    }

    /// Remove a cached entry.  Call after a dial failure so the next attempt
    /// re-fetches from registry.
    pub async fn invalidate(&self, agent_id: &str) {
        self.cache.lock().await.remove(agent_id);
    }
}

// ── Registrar ─────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum RegisterError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Invalid key seed (must be 32-byte hex): {0}")]
    InvalidKey(String),
    #[error("Registry rejected heartbeat: HTTP {0}")]
    Rejected(u16),
}

/// Sends agent heartbeats to the registry using Agent-Signature auth.
///
/// The `registry_token` in the IPC `listen` message is the hex-encoded Ed25519
/// seed (32 bytes = 64 hex chars) of the agent's private key.  The daemon uses
/// this to sign heartbeat requests so the registry can update `tunnel_endpoint`.
pub struct Registrar {
    registry_url: String,
    agent_id: String,
    signing_key: SigningKey,
    client: reqwest::Client,
}

impl Registrar {
    /// Build a Registrar from a hex-encoded 32-byte Ed25519 seed.
    pub fn from_seed_hex(
        registry_url: impl Into<String>,
        agent_id: impl Into<String>,
        seed_hex: &str,
    ) -> Result<Self, RegisterError> {
        let bytes = hex::decode(seed_hex)
            .map_err(|e| RegisterError::InvalidKey(e.to_string()))?;
        let seed: [u8; 32] = bytes
            .try_into()
            .map_err(|_| RegisterError::InvalidKey("need exactly 32 bytes".into()))?;
        Ok(Self {
            registry_url: registry_url.into().trim_end_matches('/').to_string(),
            agent_id: agent_id.into(),
            signing_key: SigningKey::from_bytes(&seed),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        })
    }

    /// Send one heartbeat, updating `tunnel_endpoint` in the registry.
    pub async fn heartbeat(&self, tunnel_endpoint: &str) -> Result<(), RegisterError> {
        let body = serde_json::json!({
            "tunnel_endpoint": tunnel_endpoint,
            "status": "online"
        });
        let body_bytes = serde_json::to_vec(&body).expect("json");

        let mut nonce_bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = hex::encode(nonce_bytes);

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string();

        // Agent-Signature: sign(agent_id + nonce + sha256hex(body))
        let body_hash = hex::encode(Sha256::digest(&body_bytes));
        let msg = format!("{}{}{}", self.agent_id, nonce, body_hash);
        let sig = self.signing_key.sign(msg.as_bytes());
        let sig_hex = hex::encode(sig.to_bytes());

        let url = format!(
            "{}/v1/agents/{}/heartbeat",
            self.registry_url, self.agent_id
        );
        let resp = self
            .client
            .post(&url)
            .header("X-Agent-Nonce", &nonce)
            .header("X-Agent-Timestamp", &ts)
            .header("X-Agent-Signature", &sig_hex)
            .header("Content-Type", "application/json")
            .body(body_bytes)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(RegisterError::Rejected(resp.status().as_u16()));
        }
        tracing::debug!("registrar: heartbeat ok for {}", self.agent_id);
        Ok(())
    }

    /// Spawn a background task that heartbeats every `interval`.
    /// Sends one heartbeat immediately on start.
    pub fn start_heartbeat_loop(
        self,
        tunnel_endpoint: String,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Send immediately on start
            if let Err(e) = self.heartbeat(&tunnel_endpoint).await {
                tracing::warn!("registrar: initial heartbeat failed: {}", e);
            }
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // discard the immediate first tick
            loop {
                ticker.tick().await;
                if let Err(e) = self.heartbeat(&tunnel_endpoint).await {
                    tracing::warn!("registrar: heartbeat failed: {}", e);
                }
            }
        })
    }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/// Best-effort local IP detection via UDP connect trick.
pub fn detect_local_ip() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    Some(socket.local_addr().ok()?.ip().to_string())
}
