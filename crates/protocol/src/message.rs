//! Wire message types for the trading protocol.
//!
//! Only trading operations (submit/cancel) are exposed to clients.
//! Administrative operations (add instrument, deposit) are server-side
//! only — they'll be configured at startup or via a separate admin API.

use trading_engine::journal::trace::{TraceTimestamp, trace_ts};
use trading_engine::types::{ExecutionReport, Order, OrderId, Symbol};

/// Connection identifier assigned by the server.
///
/// Uses `u64` — monotonically increasing, never reused within a server
/// lifetime. Fits in a register and supports more connections than any
/// single server will ever handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Client → server request.
///
/// Limited to trading operations. Administrative actions (instrument
/// registration, deposits) are not client-facing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// Submit an order for matching.
    SubmitOrder { symbol: Symbol, order: Order },
    /// Cancel a resting or pending stop order.
    CancelOrder { symbol: Symbol, order_id: OrderId },
}

/// Server → client response.
///
/// Carries trace timestamps (`()` when `latency-trace` is disabled)
/// to measure the tokio mpsc hop and server-side end-to-end latency.
#[derive(Debug, Clone, Copy)]
pub struct Response {
    pub kind: ResponseKind,
    /// Timestamp when the response stage enqueued this to the writer channel.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub sent_ts: TraceTimestamp,
    /// Timestamp when the reader task received this request from the wire.
    /// Flows through the entire pipeline to measure server-side end-to-end latency.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub recv_ts: TraceTimestamp,
}

impl Response {
    /// Create a new response with the current trace timestamp.
    pub fn new(kind: ResponseKind, recv_ts: TraceTimestamp) -> Self {
        Self {
            kind,
            sent_ts: trace_ts(),
            recv_ts,
        }
    }
}

/// The response payload type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseKind {
    /// An execution report from the matching engine.
    Report(ExecutionReport),
    /// The engine encountered an internal error processing the request.
    EngineError,
    /// Signals the end of a response batch for a single request.
    /// A single request (e.g., SubmitOrder) can produce multiple Reports
    /// (fills, placements, triggers). BatchEnd tells the client that all
    /// reports for this request have been sent.
    BatchEnd,
}

/// Commands routed through the engine's command channel.
///
/// Connect/disconnect events flow through the same channel as orders.
/// This means the engine thread owns the connection table — no mutex
/// needed, consistent with the LMAX single-writer principle.
pub enum EngineCommand {
    /// A client request to be processed by the engine.
    Request {
        connection_id: ConnectionId,
        request: Request,
        /// Timestamp when the reader task sent this command.
        /// `()` (zero-sized) when `latency-trace` is disabled.
        sent_ts: TraceTimestamp,
    },
    /// A new client connection. The engine stores the sender to push
    /// responses back to the writer task for this connection.
    Connected {
        connection_id: ConnectionId,
        sender: tokio::sync::mpsc::Sender<Response>,
    },
    /// A client disconnected. The engine removes its sender.
    Disconnected { connection_id: ConnectionId },
}
