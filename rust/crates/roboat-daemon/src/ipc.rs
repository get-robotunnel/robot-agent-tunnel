//! IPC protocol types and framing for the daemon ↔ agent local socket.
//!
//! Frame format: [length: u32 big-endian][JSON payload: bytes]
//! Maximum message size: 4 MiB.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_MSG_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Message too large: {0} bytes")]
    TooLarge(usize),
    #[error("Connection closed")]
    Closed,
}

impl From<std::io::ErrorKind> for IpcError {
    fn from(kind: std::io::ErrorKind) -> Self {
        IpcError::Io(std::io::Error::from(kind))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StreamClass {
    #[default]
    Control,
    Meta,
    Bulk,
}

impl StreamClass {
    pub fn as_byte(&self) -> u8 {
        match self {
            StreamClass::Control => 0x01,
            StreamClass::Meta => 0x02,
            StreamClass::Bulk => 0x03,
        }
    }

    pub fn from_byte(b: u8) -> Self {
        match b {
            0x02 => StreamClass::Meta,
            0x03 => StreamClass::Bulk,
            _ => StreamClass::Control,
        }
    }
}

/// Messages sent from an agent to the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum AgentMsg {
    /// Register this agent as a responder on the daemon's TCP listener.
    Listen {
        agent_id: String,
        /// Hex-encoded Ed25519 seed (64 chars) for registry Agent-Signature auth.
        #[serde(skip_serializing_if = "Option::is_none")]
        registry_token: Option<String>,
        /// Explicit public host:port to advertise in registry heartbeats.
        /// If omitted, daemon attempts local IP detection.
        #[serde(skip_serializing_if = "Option::is_none")]
        tunnel_endpoint: Option<String>,
    },
    /// Deregister responder (no-op in Phase A).
    Unlisten,
    /// Initiate a connection to a remote agent (host:port in Phase A).
    Dial {
        target_agent_id: String,
        #[serde(default)]
        stream_class: StreamClass,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    /// Send data on an established stream.
    Send {
        stream_id: u32,
        /// Base64-encoded binary payload.
        data: String,
    },
    /// Close an established stream.
    Close { stream_id: u32 },
    /// Keepalive ping.
    Ping,
}

/// Messages sent from the daemon to an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DaemonMsg {
    /// Confirmation that the daemon is now listening for inbound connections.
    Listening { agent_id: String },
    /// Confirmation that an outbound dial succeeded.
    Connected {
        stream_id: u32,
        target_agent_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    /// Notification of an inbound connection from a remote peer.
    Incoming {
        stream_id: u32,
        from_agent_id: String,
        class: StreamClass,
    },
    /// Data received from a remote peer on an established stream.
    Recv {
        stream_id: u32,
        /// Base64-encoded binary payload.
        data: String,
    },
    /// Notification that a stream was closed.
    Closed {
        stream_id: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Keepalive pong.
    Pong,
    /// Error response.
    Error {
        code: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
}

/// Write a daemon message to an async writer as a length-prefixed JSON frame.
pub async fn write_daemon_msg<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &DaemonMsg,
) -> Result<(), IpcError> {
    let json = serde_json::to_vec(msg)?;
    if json.len() > MAX_MSG_SIZE {
        return Err(IpcError::TooLarge(json.len()));
    }
    writer.write_u32(json.len() as u32).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

/// Read an agent message from an async reader as a length-prefixed JSON frame.
pub async fn read_agent_msg<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Result<AgentMsg, IpcError> {
    let len = match reader.read_u32().await {
        Ok(n) => n as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(IpcError::Closed),
        Err(e) => return Err(IpcError::Io(e)),
    };
    if len > MAX_MSG_SIZE {
        return Err(IpcError::TooLarge(len));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            IpcError::Closed
        } else {
            IpcError::Io(e)
        }
    })?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Read a daemon message from an async reader (used in tests / clients).
pub async fn read_daemon_msg<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Result<DaemonMsg, IpcError> {
    let len = match reader.read_u32().await {
        Ok(n) => n as usize,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(IpcError::Closed),
        Err(e) => return Err(IpcError::Io(e)),
    };
    if len > MAX_MSG_SIZE {
        return Err(IpcError::TooLarge(len));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            IpcError::Closed
        } else {
            IpcError::Io(e)
        }
    })?;
    Ok(serde_json::from_slice(&buf)?)
}

/// Write an agent message to an async writer (used in tests / clients).
pub async fn write_agent_msg<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &AgentMsg,
) -> Result<(), IpcError> {
    let json = serde_json::to_vec(msg)?;
    if json.len() > MAX_MSG_SIZE {
        return Err(IpcError::TooLarge(json.len()));
    }
    writer.write_u32(json.len() as u32).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_agent_msg_roundtrip() {
        let msg = AgentMsg::Dial {
            target_agent_id: "127.0.0.1:11411".to_string(),
            stream_class: StreamClass::Control,
            request_id: Some("r1".to_string()),
        };
        let (mut writer, mut reader) = duplex(4096);
        write_agent_msg(&mut writer, &msg).await.unwrap();
        let decoded = read_agent_msg(&mut reader).await.unwrap();
        match decoded {
            AgentMsg::Dial {
                target_agent_id,
                request_id,
                ..
            } => {
                assert_eq!(target_agent_id, "127.0.0.1:11411");
                assert_eq!(request_id, Some("r1".to_string()));
            }
            _ => panic!("unexpected message"),
        }
    }

    #[tokio::test]
    async fn test_daemon_msg_roundtrip() {
        let msg = DaemonMsg::Connected {
            stream_id: 42,
            target_agent_id: "127.0.0.1:11411".to_string(),
            request_id: Some("r1".to_string()),
        };
        let (mut writer, mut reader) = duplex(4096);
        write_daemon_msg(&mut writer, &msg).await.unwrap();
        let decoded = read_daemon_msg(&mut reader).await.unwrap();
        match decoded {
            DaemonMsg::Connected { stream_id, .. } => assert_eq!(stream_id, 42),
            _ => panic!("unexpected message"),
        }
    }

    #[test]
    fn test_stream_class_default() {
        let c: StreamClass = Default::default();
        assert_eq!(c, StreamClass::Control);
    }
}
