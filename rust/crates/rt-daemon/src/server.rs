//! Unix socket IPC server — accepts local agent connections and bridges them to DaemonManager.

use crate::{
    ipc::{
        read_agent_msg, write_daemon_msg, AgentMsg, DaemonMsg, IpcError, StreamClass,
    },
    manager::DaemonManager,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use std::{path::PathBuf, sync::Arc};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::Mutex,
};
use tracing;

pub struct IpcServer {
    socket_path: PathBuf,
    manager: Arc<DaemonManager>,
}

impl IpcServer {
    pub fn new(socket_path: PathBuf, manager: Arc<DaemonManager>) -> Self {
        Self {
            socket_path,
            manager,
        }
    }

    /// Run the IPC server. Blocks until a fatal error.
    pub async fn run(&self) -> Result<(), std::io::Error> {
        // Remove stale socket file
        let _ = std::fs::remove_file(&self.socket_path);

        // Ensure parent directory exists
        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let listener = UnixListener::bind(&self.socket_path)?;
        tracing::info!("daemon: IPC socket at {:?}", self.socket_path);

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let mgr = self.manager.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, mgr).await {
                            match e {
                                IpcError::Closed => {}
                                e => tracing::warn!("daemon: IPC client error: {}", e),
                            }
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("daemon: IPC accept error: {}", e);
                    return Err(e);
                }
            }
        }
    }
}

async fn handle_client(stream: UnixStream, manager: Arc<DaemonManager>) -> Result<(), IpcError> {
    let (reader, writer) = stream.into_split();
    let mut reader = reader;
    let writer = Arc::new(Mutex::new(writer));

    loop {
        let msg = read_agent_msg(&mut reader).await?;

        match msg {
            AgentMsg::Ping => {
                let mut w = writer.lock().await;
                write_daemon_msg(&mut *w, &DaemonMsg::Pong).await?;
            }

            AgentMsg::Listen {
                agent_id,
                registry_token,
                tunnel_endpoint,
            } => {
                let mgr = manager.clone();
                if let Err(e) = mgr.start_listener().await {
                    let mut w = writer.lock().await;
                    write_daemon_msg(
                        &mut *w,
                        &DaemonMsg::Error {
                            code: "listen_failed".to_string(),
                            message: e.to_string(),
                            request_id: None,
                        },
                    )
                    .await?;
                    continue;
                }

                // Phase B: start registry heartbeat if credentials were provided
                if let Some(token) = registry_token {
                    manager
                        .start_registry_heartbeat(agent_id.clone(), token, tunnel_endpoint)
                        .await;
                }

                // Forward incoming stream notifications to this IPC client
                let mut incoming_rx = manager.subscribe_incoming();
                let writer_clone = writer.clone();
                let manager_clone = manager.clone();
                tokio::spawn(async move {
                    loop {
                        match incoming_rx.recv().await {
                            Ok(info) => {
                                // Claim recv_rx for this stream
                                let recv_rx = manager_clone.take_recv_rx(info.stream_id).await;
                                if let Some(recv_rx) = recv_rx {
                                    // Spawn task to forward incoming data as `recv` messages
                                    let w2 = writer_clone.clone();
                                    let sid = info.stream_id;
                                    forward_recv_to_ipc(sid, recv_rx, w2);
                                }
                                // Send `incoming` notification
                                let mut w = writer_clone.lock().await;
                                let _ = write_daemon_msg(
                                    &mut *w,
                                    &DaemonMsg::Incoming {
                                        stream_id: info.stream_id,
                                        from_agent_id: info.from_agent_id,
                                        class: info.class,
                                    },
                                )
                                .await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!("daemon: IPC client lagged {} incoming notifications", n);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });

                let mut w = writer.lock().await;
                write_daemon_msg(&mut *w, &DaemonMsg::Listening { agent_id }).await?;
            }

            AgentMsg::Unlisten => {
                // No-op in Phase A
            }

            AgentMsg::Dial {
                target_agent_id,
                stream_class,
                request_id,
            } => {
                let mgr = manager.clone();
                let target = target_agent_id.clone();
                let class: StreamClass = stream_class;
                match mgr.dial(target, class).await {
                    Ok((stream_id, recv_rx)) => {
                        // Forward incoming data to IPC client
                        let w2 = writer.clone();
                        forward_recv_to_ipc(stream_id, recv_rx, w2);

                        let mut w = writer.lock().await;
                        write_daemon_msg(
                            &mut *w,
                            &DaemonMsg::Connected {
                                stream_id,
                                target_agent_id,
                                request_id,
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        let mut w = writer.lock().await;
                        write_daemon_msg(
                            &mut *w,
                            &DaemonMsg::Error {
                                code: "dial_failed".to_string(),
                                message: e.to_string(),
                                request_id,
                            },
                        )
                        .await?;
                    }
                }
            }

            AgentMsg::Send { stream_id, data } => {
                let bytes = match B64.decode(&data) {
                    Ok(b) => b,
                    Err(e) => {
                        let mut w = writer.lock().await;
                        write_daemon_msg(
                            &mut *w,
                            &DaemonMsg::Error {
                                code: "invalid_data".to_string(),
                                message: format!("base64 decode failed: {}", e),
                                request_id: None,
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                if let Err(e) = manager.send(stream_id, bytes).await {
                    let mut w = writer.lock().await;
                    write_daemon_msg(
                        &mut *w,
                        &DaemonMsg::Error {
                            code: "send_failed".to_string(),
                            message: e.to_string(),
                            request_id: None,
                        },
                    )
                    .await?;
                }
            }

            AgentMsg::Close { stream_id } => {
                manager.close_stream(stream_id).await;
            }
        }
    }
}

/// Spawn a task that reads from `recv_rx` and writes `DaemonMsg::Recv` to the IPC writer.
fn forward_recv_to_ipc(
    stream_id: u32,
    mut recv_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    writer: Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
) {
    tokio::spawn(async move {
        loop {
            match recv_rx.recv().await {
                Some(data) => {
                    let encoded = B64.encode(&data);
                    let mut w = writer.lock().await;
                    if write_daemon_msg(&mut *w, &DaemonMsg::Recv { stream_id, data: encoded })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                None => {
                    let mut w = writer.lock().await;
                    let _ = write_daemon_msg(
                        &mut *w,
                        &DaemonMsg::Closed {
                            stream_id,
                            reason: Some("remote closed".to_string()),
                        },
                    )
                    .await;
                    break;
                }
            }
        }
    });
}
