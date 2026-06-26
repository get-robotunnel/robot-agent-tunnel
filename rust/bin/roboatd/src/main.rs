use roboat_daemon::{
    manager::{DaemonConfig, DaemonManager},
    server::IpcServer,
};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let socket_path = std::env::var("ROBOAT_SOCKET")
        .unwrap_or_else(|_| "/var/run/roboat/roboatd.sock".to_string());

    let listen_port: u16 = std::env::var("ROBOAT_LISTEN_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(11411);

    let insecure = std::env::var("ROBOAT_INSECURE")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(true);

    // Phase B: optional registry URL for agent discovery / heartbeat
    let registry_url = std::env::var("ROBOAT_REGISTRY_URL").ok();

    // Phase C: multiplexed connections (default on)
    let use_mux = std::env::var("ROBOAT_USE_MUX")
        .map(|v| !matches!(v.to_lowercase().as_str(), "0" | "false" | "no"))
        .unwrap_or(true);

    let heartbeat_interval_secs: u64 = std::env::var("ROBOAT_HEARTBEAT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    let config = DaemonConfig {
        listen_port,
        socket_path: socket_path.clone(),
        insecure_allow_any_client: insecure,
        auth_seed: [0u8; 32],
        registry_url,
        use_mux,
        heartbeat_interval_secs,
    };

    let manager = Arc::new(DaemonManager::new(config));
    let server = IpcServer::new(socket_path.clone().into(), manager);

    tracing::info!(
        "roboatd socket={} tunnel_port={} use_mux={}",
        socket_path,
        listen_port,
        use_mux,
    );

    if let Err(e) = server.run().await {
        tracing::error!("daemon fatal error: {}", e);
        std::process::exit(1);
    }
}
