//! Wire message types for the trading protocol.
//!
//! Only trading operations (submit/cancel) are exposed to clients.
//! Administrative operations (add instrument, deposit) are server-side
//! only — they'll be configured at startup or via a separate admin API.

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

/// Server → client response payload.
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
