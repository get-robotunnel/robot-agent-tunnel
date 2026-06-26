//! Daemon connection manager.
//!
//! Supports two connection modes:
//!
//! * **Phase A/B — direct**: one TCP connection per logical stream, using
//!   RelayOpen / RelayData / RelayClose.  `use_mux = false` or target is raw
//!   `host:port`.
//!
//! * **Phase C — multiplexed** (`use_mux = true`): a shared TCP connection per
//!   remote; logical streams use StreamOpen / StreamData / StreamClose with QoS
//!   scheduling (control ≻ bulk).
//!
//! Phase B: when `dial` receives an `agt_xxx` target, the `Resolver` maps it to
//! `host:port` via the registry discovery API.  When `listen` is called with a
//! `registry_token`, a background heartbeat loop keeps `tunnel_endpoint` current
//! in the registry.

use crate::{
    ipc::StreamClass,
    mux::{IncomingMuxStream, MuxConn},
};
use roboat_core::{
    auth::{ClientAuthenticator, ServerAuthenticator},
    protocol::{read_frame, write_frame, FrameType, ProtocolError},
};
use roboat_resolver::{detect_local_ip, Registrar, Resolver};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::{broadcast, mpsc, Mutex},
};
use tracing;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Auth error: {0}")]
    Auth(#[from] roboat_core::auth::AuthError),
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("Stream {0} not found")]
    StreamNotFound(u32),
    #[error("Send failed: channel closed")]
    SendFailed,
    #[error("Resolve error: {0}")]
    Resolve(String),
    #[error("No registry URL configured; cannot resolve agent_id")]
    NoResolver,
}

// ── Config ────────────────────────────────────────────────────────────────────

pub struct DaemonConfig {
    pub listen_port: u16,
    pub socket_path: String,
    pub insecure_allow_any_client: bool,
    pub auth_seed: [u8; 32],
    /// Phase B: registry base URL (e.g. `https://reg.robotunnel.io/v1`).
    pub registry_url: Option<String>,
    /// Phase C: use multiplexed connections (default: true).
    pub use_mux: bool,
    /// Heartbeat interval for registry registration (seconds).
    pub heartbeat_interval_secs: u64,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            listen_port: 11411,
            socket_path: "/var/run/roboat/roboatd.sock".to_string(),
            insecure_allow_any_client: true,
            auth_seed: [0u8; 32],
            registry_url: None,
            use_mux: true,
            heartbeat_interval_secs: 30,
        }
    }
}

// ── Stream entry ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IncomingStreamInfo {
    pub stream_id: u32,
    pub from_agent_id: String,
    pub class: StreamClass,
}

struct StreamEntry {
    /// Routes IPC `send` to the wire: for direct streams via bridge task channel;
    /// for mux streams via a bridge task that calls MuxConn::send.
    data_tx: mpsc::Sender<Vec<u8>>,
    recv_rx: Arc<Mutex<Option<mpsc::Receiver<Vec<u8>>>>>,
    #[allow(dead_code)]
    class: StreamClass,
    /// Kept alive so the MuxConn write-task is not torn down while streams remain.
    #[allow(dead_code)]
    mux_conn: Option<Arc<MuxConn>>,
}

// ── DaemonManager ─────────────────────────────────────────────────────────────

pub struct DaemonManager {
    pub config: DaemonConfig,
    streams: Arc<Mutex<HashMap<u32, StreamEntry>>>,
    next_stream_id: Arc<AtomicU32>,
    incoming_tx: broadcast::Sender<IncomingStreamInfo>,
    listener_started: Arc<Mutex<bool>>,
    resolver: Option<Arc<Resolver>>,
    /// Phase C: outbound mux connection pool, keyed by remote "host:port".
    mux_pool: Arc<Mutex<HashMap<String, Arc<MuxConn>>>>,
}

impl DaemonManager {
    pub fn new(config: DaemonConfig) -> Self {
        let resolver = config
            .registry_url
            .as_ref()
            .map(|url| Arc::new(Resolver::new(url)));
        let (incoming_tx, _) = broadcast::channel(64);
        Self {
            config,
            streams: Arc::new(Mutex::new(HashMap::new())),
            next_stream_id: Arc::new(AtomicU32::new(1)),
            incoming_tx,
            listener_started: Arc::new(Mutex::new(false)),
            resolver,
            mux_pool: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe_incoming(&self) -> broadcast::Receiver<IncomingStreamInfo> {
        self.incoming_tx.subscribe()
    }

    pub async fn take_recv_rx(&self, stream_id: u32) -> Option<mpsc::Receiver<Vec<u8>>> {
        let streams = self.streams.lock().await;
        if let Some(entry) = streams.get(&stream_id) {
            entry.recv_rx.lock().await.take()
        } else {
            None
        }
    }

    // ── Listener ──────────────────────────────────────────────────────────────

    pub async fn start_listener(self: Arc<Self>) -> Result<(), DaemonError> {
        let mut started = self.listener_started.lock().await;
        if *started {
            return Ok(());
        }
        *started = true;
        drop(started);

        let port = self.config.listen_port;
        let listener = TcpListener::bind(("0.0.0.0", port)).await?;
        tracing::info!("daemon: TCP listener on 0.0.0.0:{}", port);

        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        tracing::debug!("daemon: inbound connection from {}", addr);
                        let mgr = manager.clone();
                        tokio::spawn(async move {
                            if let Err(e) = mgr.handle_inbound(stream).await {
                                tracing::warn!("daemon: inbound error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("daemon: accept error: {}", e);
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    /// Phase B: start a background heartbeat loop to keep `tunnel_endpoint`
    /// current in the registry.
    pub async fn start_registry_heartbeat(
        &self,
        agent_id: String,
        key_seed_hex: String,
        explicit_endpoint: Option<String>,
    ) {
        let registry_url = match &self.config.registry_url {
            Some(url) => url.clone(),
            None => {
                tracing::warn!("daemon: no ROBOAT_REGISTRY_URL; skipping heartbeat");
                return;
            }
        };
        let tunnel_endpoint = explicit_endpoint.unwrap_or_else(|| {
            let ip = detect_local_ip().unwrap_or_else(|| "127.0.0.1".to_string());
            format!("{}:{}", ip, self.config.listen_port)
        });
        let registrar = match Registrar::from_seed_hex(&registry_url, &agent_id, &key_seed_hex) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("daemon: registry auth init: {}", e);
                return;
            }
        };
        tracing::info!(
            "daemon: registry heartbeat for {} @ {}",
            agent_id,
            tunnel_endpoint
        );
        registrar.start_heartbeat_loop(
            tunnel_endpoint,
            Duration::from_secs(self.config.heartbeat_interval_secs),
        );
    }

    // ── Inbound connection handling ───────────────────────────────────────────

    async fn handle_inbound(self: Arc<Self>, mut stream: TcpStream) -> Result<(), DaemonError> {
        // Auth on the unsplit TcpStream (requires unified AsyncRead+AsyncWrite).
        let server_auth = ServerAuthenticator::new(vec![]);
        let pub_key_hex = server_auth.authenticate(&mut stream).await?;
        let from_agent_id = pub_key_hex[..16].to_string();

        // Split only after auth succeeds.
        let (mut reader, writer) = stream.into_split();
        let (frame_type, payload) = read_frame(&mut reader).await?;

        match frame_type {
            FrameType::RelayOpen => {
                self.handle_inbound_relay(reader, writer, payload, from_agent_id)
                    .await
            }
            FrameType::StreamOpen => {
                self.handle_inbound_mux(reader, writer, payload, from_agent_id)
                    .await
            }
            _ => Err(DaemonError::Protocol(ProtocolError::InvalidPacket(format!(
                "unexpected first frame: 0x{:02x}",
                frame_type as u8
            )))),
        }
    }

    async fn handle_inbound_relay(
        self: Arc<Self>,
        reader: OwnedReadHalf,
        mut writer: OwnedWriteHalf,
        relay_open_payload: Vec<u8>,
        from_agent_id: String,
    ) -> Result<(), DaemonError> {
        if relay_open_payload.len() < 5 {
            return Err(DaemonError::Protocol(ProtocolError::InvalidPacket(
                "RelayOpen payload too short".into(),
            )));
        }
        let stream_id = u32::from_be_bytes([
            relay_open_payload[0],
            relay_open_payload[1],
            relay_open_payload[2],
            relay_open_payload[3],
        ]);
        let class = StreamClass::from_byte(relay_open_payload[4]);

        write_frame(&mut writer, FrameType::RelayOpenAck, &relay_open_payload).await?;

        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(64);
        let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(64);

        self.streams.lock().await.insert(
            stream_id,
            StreamEntry {
                data_tx,
                recv_rx: Arc::new(Mutex::new(Some(recv_rx))),
                class: class.clone(),
                mux_conn: None,
            },
        );

        let _ = self.incoming_tx.send(IncomingStreamInfo {
            stream_id,
            from_agent_id,
            class,
        });

        let streams = self.streams.clone();
        tokio::spawn(async move {
            run_stream_bridge(stream_id, reader, writer, data_rx, recv_tx, streams).await;
        });
        Ok(())
    }

    async fn handle_inbound_mux(
        self: Arc<Self>,
        reader: OwnedReadHalf,
        writer: OwnedWriteHalf,
        first_open_payload: Vec<u8>,
        from_agent_id: String,
    ) -> Result<(), DaemonError> {
        let (mux_incoming_tx, mut mux_incoming_rx) = mpsc::channel::<IncomingMuxStream>(64);
        let conn = MuxConn::new_responder(
            reader,
            writer,
            from_agent_id,
            mux_incoming_tx,
            first_open_payload,
        );

        let mgr = self.clone();
        tokio::spawn(async move {
            while let Some(mux_stream) = mux_incoming_rx.recv().await {
                mgr.register_mux_incoming(mux_stream, conn.clone()).await;
            }
        });
        Ok(())
    }

    /// Register an inbound mux stream and wire a data_tx→mux bridge.
    async fn register_mux_incoming(&self, mux_stream: IncomingMuxStream, conn: Arc<MuxConn>) {
        let stream_id = mux_stream.stream_id;
        let class = mux_stream.class.clone();
        let from_agent_id = mux_stream.from_agent_id.clone();
        let recv_rx = mux_stream.recv_rx;

        let (data_tx, mut data_rx) = mpsc::channel::<Vec<u8>>(64);
        let conn2 = conn.clone();
        let class2 = class.clone();
        tokio::spawn(async move {
            while let Some(data) = data_rx.recv().await {
                if conn2.send(stream_id, data, &class2).is_err() {
                    break;
                }
            }
            conn2.close_stream(stream_id, &class2).await;
        });

        self.streams.lock().await.insert(
            stream_id,
            StreamEntry {
                data_tx,
                recv_rx: Arc::new(Mutex::new(Some(recv_rx))),
                class: class.clone(),
                mux_conn: Some(conn), // keeps write-task alive
            },
        );

        let _ = self.incoming_tx.send(IncomingStreamInfo {
            stream_id,
            from_agent_id,
            class,
        });
    }

    // ── Dial (outbound) ───────────────────────────────────────────────────────

    pub async fn dial(
        self: Arc<Self>,
        target: String,
        class: StreamClass,
    ) -> Result<(u32, mpsc::Receiver<Vec<u8>>), DaemonError> {
        // Phase B: resolve agt_xxx → host:port
        let resolved = if target.starts_with("agt_") {
            match &self.resolver {
                Some(r) => r
                    .resolve(&target)
                    .await
                    .map_err(|e| DaemonError::Resolve(e.to_string()))?,
                None => return Err(DaemonError::NoResolver),
            }
        } else {
            target
        };

        if self.config.use_mux {
            self.dial_mux(resolved, class).await
        } else {
            self.dial_direct(resolved, class).await
        }
    }

    async fn dial_mux(
        self: Arc<Self>,
        addr: String,
        class: StreamClass,
    ) -> Result<(u32, mpsc::Receiver<Vec<u8>>), DaemonError> {
        let mux_conn = {
            let mut pool = self.mux_pool.lock().await;
            if let Some(conn) = pool.get(&addr) {
                conn.clone()
            } else {
                let conn = self.clone().create_mux_conn(addr.clone()).await?;
                pool.insert(addr.clone(), conn.clone());
                conn
            }
        };

        let (stream_id, recv_rx) = mux_conn
            .open_stream(class.clone())
            .await
            .map_err(|_| DaemonError::SendFailed)?;

        let (data_tx, mut data_rx) = mpsc::channel::<Vec<u8>>(64);
        let mux_conn2 = mux_conn.clone();
        let class2 = class.clone();
        tokio::spawn(async move {
            while let Some(data) = data_rx.recv().await {
                if mux_conn2.send(stream_id, data, &class2).is_err() {
                    break;
                }
            }
            mux_conn2.close_stream(stream_id, &class2).await;
        });

        self.streams.lock().await.insert(
            stream_id,
            StreamEntry {
                data_tx,
                recv_rx: Arc::new(Mutex::new(None)), // recv_rx returned directly to caller
                class,
                mux_conn: Some(mux_conn),
            },
        );

        Ok((stream_id, recv_rx))
    }

    async fn create_mux_conn(self: Arc<Self>, addr: String) -> Result<Arc<MuxConn>, DaemonError> {
        let mut tcp = TcpStream::connect(&addr).await?;
        let client_auth = ClientAuthenticator::from_seed(&self.config.auth_seed);
        client_auth.authenticate(&mut tcp).await?;

        let (reader, writer) = tcp.into_split();
        let (incoming_tx, mut incoming_rx) = mpsc::channel::<IncomingMuxStream>(64);
        let conn = MuxConn::new_initiator(reader, writer, addr, incoming_tx);

        let mgr = self.clone();
        let conn2 = conn.clone();
        tokio::spawn(async move {
            while let Some(mux_stream) = incoming_rx.recv().await {
                mgr.register_mux_incoming(mux_stream, conn2.clone()).await;
            }
        });
        Ok(conn)
    }

    async fn dial_direct(
        self: Arc<Self>,
        addr: String,
        class: StreamClass,
    ) -> Result<(u32, mpsc::Receiver<Vec<u8>>), DaemonError> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let mut tcp = TcpStream::connect(&addr).await?;

        let client_auth = ClientAuthenticator::from_seed(&self.config.auth_seed);
        client_auth.authenticate(&mut tcp).await?;

        let (mut reader, mut writer) = tcp.into_split();

        let mut payload = Vec::with_capacity(5);
        payload.extend_from_slice(&stream_id.to_be_bytes());
        payload.push(class.as_byte());
        write_frame(&mut writer, FrameType::RelayOpen, &payload).await?;

        let (frame_type, _) = read_frame(&mut reader).await?;
        if frame_type != FrameType::RelayOpenAck {
            return Err(DaemonError::Protocol(ProtocolError::InvalidPacket(
                "expected RelayOpenAck".into(),
            )));
        }

        let (data_tx, data_rx) = mpsc::channel::<Vec<u8>>(64);
        let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(64);

        self.streams.lock().await.insert(
            stream_id,
            StreamEntry {
                data_tx,
                recv_rx: Arc::new(Mutex::new(None)),
                class,
                mux_conn: None,
            },
        );

        let streams = self.streams.clone();
        tokio::spawn(async move {
            run_stream_bridge(stream_id, reader, writer, data_rx, recv_tx, streams).await;
        });
        Ok((stream_id, recv_rx))
    }

    // ── Data path ─────────────────────────────────────────────────────────────

    pub async fn send(&self, stream_id: u32, data: Vec<u8>) -> Result<(), DaemonError> {
        let streams = self.streams.lock().await;
        let entry = streams
            .get(&stream_id)
            .ok_or(DaemonError::StreamNotFound(stream_id))?;
        entry
            .data_tx
            .send(data)
            .await
            .map_err(|_| DaemonError::SendFailed)
    }

    pub async fn close_stream(&self, stream_id: u32) {
        self.streams.lock().await.remove(&stream_id);
    }
}

// ── Phase A/B stream bridge ───────────────────────────────────────────────────

async fn run_stream_bridge(
    stream_id: u32,
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    mut data_rx: mpsc::Receiver<Vec<u8>>,
    recv_tx: mpsc::Sender<Vec<u8>>,
    streams: Arc<Mutex<HashMap<u32, StreamEntry>>>,
) {
    loop {
        tokio::select! {
            maybe_data = data_rx.recv() => {
                match maybe_data {
                    Some(data) => {
                        if write_frame(&mut writer, FrameType::RelayData, &data).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        let _ = write_frame(&mut writer, FrameType::RelayClose, &[]).await;
                        break;
                    }
                }
            }
            frame_result = read_frame(&mut reader) => {
                match frame_result {
                    Ok((FrameType::RelayData, data)) => {
                        if recv_tx.send(data).await.is_err() {
                            break;
                        }
                    }
                    Ok((FrameType::RelayClose, _)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        }
    }
    streams.lock().await.remove(&stream_id);
    tracing::debug!("daemon: stream {} closed", stream_id);
}
