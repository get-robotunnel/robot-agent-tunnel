//! Phase C multiplexed daemon-to-daemon connection.
//!
//! A `MuxConn` wraps one TCP connection and carries N logical streams using the
//! v0.3 StreamOpen / StreamData / StreamClose frames (0x40–0x42).
//!
//! QoS scheduling: `control` and `meta` streams are routed to a high-priority
//! write queue; `bulk` streams go to a low-priority queue.  The write task
//! drains the high-priority queue first, so a saturated bulk stream cannot
//! delay a control message.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use rt_core::protocol::{read_frame, write_frame, FrameType, ProtocolError};
use thiserror::Error;
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::{mpsc, Mutex},
};
use tracing;

use crate::ipc::StreamClass;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum MuxError {
    #[error("Mux connection closed")]
    Disconnected,
    #[error("Stream not found: {0}")]
    StreamNotFound(u32),
}

// ── Public incoming notification ──────────────────────────────────────────────

/// Notification delivered to `DaemonManager` when a remote peer opens a new
/// stream on an existing mux connection.
pub struct IncomingMuxStream {
    pub stream_id: u32,
    pub class: StreamClass,
    pub from_agent_id: String,
    /// The receive half for inbound data on this stream.
    /// `DaemonManager` claims it and stores it in its `streams` table.
    pub recv_rx: mpsc::Receiver<Vec<u8>>,
}

// ── Internal write item ───────────────────────────────────────────────────────

struct WriteItem {
    frame_type: FrameType,
    payload: Vec<u8>,
}

// ── Per-stream state stored in MuxConn ───────────────────────────────────────

struct MuxStreamEntry {
    recv_tx: mpsc::Sender<Vec<u8>>,
}

// ── MuxConn ───────────────────────────────────────────────────────────────────

/// A multiplexed TCP connection between two daemons.
///
/// - The **initiator** (dialing side) assigns odd stream IDs: 1, 3, 5 …
/// - The **responder** (accepting side) assigns even IDs if it ever opens its
///   own streams (reserved; Phase C only initiator opens streams).
///
/// New streams are created with `open_stream`.  Outbound data is enqueued with
/// `send`.  Incoming data is routed by the internal read task.  Incoming
/// *stream* notifications (remote peer opened a stream) are sent to
/// `incoming_tx` so `DaemonManager` can forward them to the IPC server.
pub struct MuxConn {
    /// Monotonic counter; initiator starts at 1 and steps by 2.
    next_stream_id: AtomicU32,
    /// Active streams: stream_id → recv channel for IPC-bound data.
    streams: Arc<Mutex<HashMap<u32, MuxStreamEntry>>>,
    /// High-priority write queue (control + meta).
    ctrl_tx: mpsc::UnboundedSender<WriteItem>,
    /// Low-priority write queue (bulk).
    bulk_tx: mpsc::UnboundedSender<WriteItem>,
}

impl MuxConn {
    /// Create a mux connection for the **initiator** side.
    /// `incoming_tx` receives notifications when the remote peer opens streams.
    pub fn new_initiator(
        reader: OwnedReadHalf,
        writer: OwnedWriteHalf,
        from_agent_id: String,
        incoming_tx: mpsc::Sender<IncomingMuxStream>,
    ) -> Arc<Self> {
        Self::new_inner(reader, writer, from_agent_id, incoming_tx, 1)
    }

    /// Create a mux connection for the **responder** side.
    /// Must be provided the already-consumed first frame payload (the StreamOpen
    /// that triggered mode detection) so the read task can process it immediately.
    pub fn new_responder(
        reader: OwnedReadHalf,
        writer: OwnedWriteHalf,
        from_agent_id: String,
        incoming_tx: mpsc::Sender<IncomingMuxStream>,
        first_stream_open_payload: Vec<u8>,
    ) -> Arc<Self> {
        let conn = Self::new_inner(reader, writer, from_agent_id.clone(), incoming_tx.clone(), 2);
        // Synthesize the first incoming stream from the already-read payload
        let streams = conn.streams.clone();
        tokio::spawn(async move {
            process_stream_open(first_stream_open_payload, &streams, &incoming_tx, &from_agent_id).await;
        });
        conn
    }

    fn new_inner(
        reader: OwnedReadHalf,
        writer: OwnedWriteHalf,
        from_agent_id: String,
        incoming_tx: mpsc::Sender<IncomingMuxStream>,
        start_id: u32,
    ) -> Arc<Self> {
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<WriteItem>();
        let (bulk_tx, bulk_rx) = mpsc::unbounded_channel::<WriteItem>();
        let streams = Arc::new(Mutex::new(HashMap::<u32, MuxStreamEntry>::new()));

        let conn = Arc::new(Self {
            next_stream_id: AtomicU32::new(start_id),
            streams: streams.clone(),
            ctrl_tx,
            bulk_tx,
        });

        tokio::spawn(mux_write_task(writer, ctrl_rx, bulk_rx));
        tokio::spawn(mux_read_task(reader, streams, incoming_tx, from_agent_id));

        conn
    }

    /// Open a new logical stream.  Sends a StreamOpen frame and returns
    /// `(stream_id, recv_rx)` immediately (optimistic; TCP ensures delivery).
    pub async fn open_stream(
        &self,
        class: StreamClass,
    ) -> Result<(u32, mpsc::Receiver<Vec<u8>>), MuxError> {
        let stream_id = self.next_stream_id.fetch_add(2, Ordering::Relaxed);

        // StreamOpen payload: [stream_id: u32 BE][class: u8]
        let mut payload = Vec::with_capacity(5);
        payload.extend_from_slice(&stream_id.to_be_bytes());
        payload.push(class.as_byte());

        self.enqueue(
            &class,
            WriteItem {
                frame_type: FrameType::StreamOpen,
                payload,
            },
        )?;

        let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(64);
        self.streams
            .lock()
            .await
            .insert(stream_id, MuxStreamEntry { recv_tx });

        Ok((stream_id, recv_rx))
    }

    /// Enqueue data for an active stream.
    pub fn send(&self, stream_id: u32, data: Vec<u8>, class: &StreamClass) -> Result<(), MuxError> {
        // StreamData payload: [stream_id: u32 BE][data ...]
        let mut payload = Vec::with_capacity(4 + data.len());
        payload.extend_from_slice(&stream_id.to_be_bytes());
        payload.extend_from_slice(&data);
        self.enqueue(
            class,
            WriteItem {
                frame_type: FrameType::StreamData,
                payload,
            },
        )
    }

    /// Close a stream and remove it from the active table.
    pub async fn close_stream(&self, stream_id: u32, class: &StreamClass) {
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&stream_id.to_be_bytes());
        let _ = self.enqueue(
            class,
            WriteItem {
                frame_type: FrameType::StreamClose,
                payload,
            },
        );
        self.streams.lock().await.remove(&stream_id);
    }

    fn enqueue(&self, class: &StreamClass, item: WriteItem) -> Result<(), MuxError> {
        let tx = match class {
            StreamClass::Control | StreamClass::Meta => &self.ctrl_tx,
            StreamClass::Bulk => &self.bulk_tx,
        };
        tx.send(item).map_err(|_| MuxError::Disconnected)
    }
}

// ── Write task (QoS scheduler) ────────────────────────────────────────────────

async fn mux_write_task(
    mut writer: OwnedWriteHalf,
    mut ctrl_rx: mpsc::UnboundedReceiver<WriteItem>,
    mut bulk_rx: mpsc::UnboundedReceiver<WriteItem>,
) {
    loop {
        // Drain high-priority queue first (non-blocking)
        loop {
            match ctrl_rx.try_recv() {
                Ok(item) => {
                    if write_frame(&mut writer, item.frame_type, &item.payload)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) => break,
            }
        }

        // Wait for either queue; `biased` ensures control wins when both ready
        tokio::select! {
            biased;
            item = ctrl_rx.recv() => {
                match item {
                    Some(i) => {
                        if write_frame(&mut writer, i.frame_type, &i.payload).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
            item = bulk_rx.recv() => {
                match item {
                    Some(i) => {
                        if write_frame(&mut writer, i.frame_type, &i.payload).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                }
            }
        }
    }
}

// ── Read task ─────────────────────────────────────────────────────────────────

async fn mux_read_task(
    mut reader: OwnedReadHalf,
    streams: Arc<Mutex<HashMap<u32, MuxStreamEntry>>>,
    incoming_tx: mpsc::Sender<IncomingMuxStream>,
    from_agent_id: String,
) {
    loop {
        let frame = read_frame(&mut reader).await;
        match frame {
            Ok((FrameType::StreamOpen, payload)) => {
                process_stream_open(payload, &streams, &incoming_tx, &from_agent_id).await;
            }
            Ok((FrameType::StreamData, payload)) => {
                if payload.len() < 4 {
                    continue;
                }
                let stream_id =
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let data = payload[4..].to_vec();
                let guard = streams.lock().await;
                if let Some(entry) = guard.get(&stream_id) {
                    let _ = entry.recv_tx.send(data).await;
                }
            }
            Ok((FrameType::StreamClose, payload)) => {
                if payload.len() < 4 {
                    continue;
                }
                let stream_id =
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                streams.lock().await.remove(&stream_id);
                tracing::debug!("mux: remote closed stream {}", stream_id);
            }
            Ok(_) => {} // ignore unknown frames
            Err(ProtocolError::ConnectionClosed) | Err(_) => {
                tracing::debug!("mux: read task exiting");
                break;
            }
        }
    }
}

async fn process_stream_open(
    payload: Vec<u8>,
    streams: &Arc<Mutex<HashMap<u32, MuxStreamEntry>>>,
    incoming_tx: &mpsc::Sender<IncomingMuxStream>,
    from_agent_id: &str,
) {
    if payload.len() < 5 {
        return;
    }
    let stream_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let class = StreamClass::from_byte(payload[4]);

    let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(64);
    streams
        .lock()
        .await
        .insert(stream_id, MuxStreamEntry { recv_tx });

    tracing::debug!("mux: incoming stream {} class={:?}", stream_id, class);

    let _ = incoming_tx
        .send(IncomingMuxStream {
            stream_id,
            class,
            from_agent_id: from_agent_id.to_string(),
            recv_rx,
        })
        .await;
}
