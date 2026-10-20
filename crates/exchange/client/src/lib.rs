//! Client library for connecting to the trading server.
//!
//! Provides a typed API over the binary wire protocol. The public
//! `Client` type speaks TCP via blocking I/O against the server's
//! TCP listener.
//!
//! `melin-ec-client` — "ec" for exchange core — leaves `melin-client`
//! to the sequencer's own client crate, which speaks the node's wire
//! protocol without knowing the exchange's requests.

use std::io;

pub use melin_client::RefusalKind;
use melin_wire_protocol::error::ProtocolError;

/// Error returned by client operations.
#[derive(Debug)]
pub enum ClientError {
    /// I/O error (connection lost, etc.).
    Io(io::Error),
    /// Protocol encoding/decoding error.
    Protocol(ProtocolError),
    /// The node speaks client protocol `node`, this client `client`:
    /// found in the node's challenge, before anything was signed.
    /// Upgrade clients with the nodes.
    ProtocolVersion { node: u16, client: u16 },
    /// Server closed the connection before sending BatchEnd.
    Disconnected,
    /// Server rejected the Ed25519 challenge-response authentication
    /// (unknown key, invalid signature, or a key listed under the
    /// `replication` role, which may not connect as a client).
    AuthFailed,
    /// The node shed the request under load: it was not applied, and may
    /// be resent after a brief backoff. The connection is still usable.
    ServerBusy,
    /// The node refused the request, which was not applied: `kind` says
    /// why (a key whose role may not send it, or a request the node
    /// cannot decode), `reason` is the node's text, for people. The
    /// connection is still usable.
    Refused { kind: RefusalKind, reason: String },
    /// The reply is backed by the primary's own disk alone, weaker than
    /// the node's ack policy: a primary that lost its last replica sends
    /// it once its degraded-ack grace period has passed. The responses
    /// are the reply's, in order; the request is unconfirmed until the
    /// cluster is whole again, and the connection is still usable.
    Degraded(Vec<melin_ec_protocol::message::ResponseKind>),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::ProtocolVersion { node, client } => write!(
                f,
                "the node speaks client protocol {node}, this client {client}: \
                 upgrade clients with the nodes"
            ),
            Self::Disconnected => write!(f, "disconnected from server"),
            Self::AuthFailed => write!(f, "authentication failed"),
            Self::ServerBusy => write!(
                f,
                "server busy: the request was not applied, resend it after a backoff"
            ),
            Self::Refused { kind, reason } => {
                let kind = match kind {
                    RefusalKind::PermissionDenied => "not permitted",
                    RefusalKind::Malformed => "malformed",
                };
                write!(f, "request refused ({kind}): {reason}")
            }
            Self::Degraded(_) => write!(
                f,
                "reply backed by the primary's disk alone, not the ack policy: \
                 unconfirmed until the cluster is whole again"
            ),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

/// Snapshot of server stats returned by [`Client::query_stats`].
#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub active_connections: u64,
    pub events_processed: u64,
    pub journal_sequence: u64,
}

mod tcp;
pub use tcp::Client;
