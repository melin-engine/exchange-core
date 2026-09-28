//! Wire message types for the trading protocol.
//!
//! Includes trading operations (submit/cancel), fund management
//! (deposit/withdraw) and administrative commands (add instrument, set risk
//! limits). Each request belongs to one [`RequestCategory`], which decides
//! the roles that may send it (see [`crate::role`]); the node checks it on
//! the reader thread.

use melin_ec_types::types::{
    AccountBalance, AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule,
    InstrumentSpec, Order, OrderId, Price, Quantity, RiskLimits, Side, Symbol,
};

use crate::role::RequestCategory;

pub use melin_wire_protocol::control::ConnectionId;

/// Client → server request.
///
/// The section comments below name the role that may send each request;
/// [`Request::category`] is what the node enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    // --- Trading operations (trader) ---
    /// Submit an order for matching.
    SubmitOrder { symbol: Symbol, order: Order },
    /// Cancel a resting or pending stop order.
    CancelOrder {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
    },
    /// Cancel all resting orders and pending stops for an account
    /// across all instruments (kill switch).
    CancelAll { account: AccountId },
    /// Atomically amend a resting limit order's price and/or quantity.
    /// If the amendment fails (e.g. insufficient balance), the original
    /// order remains intact. `new_quantity` is the desired new remaining.
    CancelReplace {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },

    // --- Administrative operations (operator; Deposit and Withdraw: custodian) ---
    /// Register a new instrument with its base/quote currency pair.
    AddInstrument { spec: InstrumentSpec },
    /// Credit funds to an account. Used for initial seeding and
    /// operational adjustments.
    Deposit {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Debit available funds from an account. Rejects if the account
    /// has resting orders (must CancelAll first) or insufficient balance.
    /// Removes the balance entry when it reaches zero.
    Withdraw {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Set or update fat-finger risk limits for an instrument.
    /// `None` fields clear the corresponding limit.
    SetRiskLimits { symbol: Symbol, limits: RiskLimits },
    /// Configure circuit breakers for an instrument: price bands
    /// and/or trading halt. Replaces the current configuration.
    SetCircuitBreaker {
        symbol: Symbol,
        config: CircuitBreakerConfig,
    },
    /// Set maker/taker fee schedule for an instrument.
    SetFeeSchedule {
        symbol: Symbol,
        schedule: FeeSchedule,
    },

    /// Cancel all resting orders and pending stops with `TimeInForce::Day`
    /// across all instruments. Triggered by an operator at end-of-session.
    EndOfDay,

    /// Disable an instrument: reject new orders and cancel all resting
    /// orders and pending stops. Re-enable is possible.
    DisableInstrument { symbol: Symbol },
    /// Re-enable a previously disabled instrument for trading.
    EnableInstrument { symbol: Symbol },
    /// Permanently remove a disabled instrument. Only succeeds if the
    /// instrument is disabled and has no resting orders.
    RemoveInstrument { symbol: Symbol },

    // --- Query operations (operator) ---
    /// Request a snapshot of server stats (connections, throughput, book
    /// depth, balances). Tag-only, no payload. Flows through the pipeline
    /// like any other request so the matching stage can read Exchange state
    /// without concurrency issues.
    QueryStats,

    // --- Control messages (any client role) ---
    /// Keepalive heartbeat. Resets the server's idle timeout for this
    /// connection. Tag-only, no payload. The auth handshake is not
    /// here: it is the sequencer's, run before any request.
    Heartbeat,

    /// Subscribe to the event firehose for specific symbols.
    /// Sent after auth+ServerReady. `count == 0` means all symbols.
    /// Fixed-size array avoids heap allocation on the codec hot path.
    Subscribe { symbols: [Symbol; 8], count: u8 },

    // --- Trader queries (trader) ---
    /// Query balances for an account. Flows through the pipeline like QueryStats
    /// so the matching stage can read Exchange state without concurrency issues.
    QueryPosition { account: AccountId },

    /// Query the engine's current request_seq HWM for *this connection's*
    /// authenticated key. Tag-only: the engine reads the calling key's
    /// hash from its connection registration, so a client cannot ask
    /// about other keys' state.
    ///
    /// Reconnecting clients should call this immediately after
    /// `ServerReady` and seed their next outbound seq to `hwm + 1` so
    /// subsequent requests bypass the engine's idempotency dedup —
    /// without it a fresh client process would re-use seqs the engine
    /// has already accepted across an earlier connection lifetime and
    /// every request would be rejected as `DuplicateRequest`.
    QueryRequestSeq,
}

impl Request {
    /// The duty this request belongs to, which decides the roles that may
    /// send it ([`RequestCategory::admits`]).
    ///
    /// Exhaustive on purpose, with no wildcard arm: a request added to
    /// [`Request`] does not compile until it is given a category, so it
    /// can never fall into one by default. The same holds per variant, so
    /// each request has exactly one category.
    #[inline]
    pub fn category(&self) -> RequestCategory {
        match self {
            Request::SubmitOrder { .. }
            | Request::CancelOrder { .. }
            | Request::CancelAll { .. }
            | Request::CancelReplace { .. }
            | Request::QueryPosition { .. }
            | Request::QueryRequestSeq => RequestCategory::Trading,
            Request::Deposit { .. } | Request::Withdraw { .. } => RequestCategory::FundManagement,
            Request::AddInstrument { .. }
            | Request::SetRiskLimits { .. }
            | Request::SetCircuitBreaker { .. }
            | Request::SetFeeSchedule { .. }
            | Request::EndOfDay
            | Request::DisableInstrument { .. }
            | Request::EnableInstrument { .. }
            | Request::RemoveInstrument { .. }
            | Request::QueryStats => RequestCategory::Administration,
            Request::Heartbeat | Request::Subscribe { .. } => RequestCategory::Connection,
        }
    }
}

/// Server → client application response payload.
///
/// The transport's own frames — heartbeats, `BatchEnd`, `ServerBusy`,
/// `EngineError`, and the auth handshake — are not here: the sequencer
/// encodes them and its client tells them apart (`melin_client::classify`)
/// before a frame reaches this codec.
///
/// PositionSnapshot (389 bytes) dominates the enum size, but ResponseKind must
/// be `Copy` for zero-allocation codec paths. Boxing would add heap indirection.
/// Position queries are infrequent (trader/operator initiated), so the per-value
/// overhead is acceptable on the response path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum ResponseKind {
    /// An execution report from the matching engine.
    Report(ExecutionReport),

    // --- Stats response ---
    /// Server stats snapshot. Sent in response to `QueryStats`.
    StatsHeader {
        /// Number of currently authenticated connections.
        active_connections: u64,
        /// Total events processed by the matching engine since startup.
        events_processed: u64,
        /// Current journal sequence number (last durable event).
        journal_sequence: u64,
    },

    // --- Market-data snapshot ---
    /// Start of a book snapshot for one symbol.
    BookSnapshotBegin {
        symbol: Symbol,
        last_applied_seq: u64,
    },
    /// One price level in a book snapshot.
    BookSnapshotLevel {
        symbol: Symbol,
        side: Side,
        price: Price,
        qty: u64,
        order_count: u32,
    },
    /// End of a book snapshot for one symbol.
    BookSnapshotEnd { symbol: Symbol, level_count: u32 },
    /// All requested snapshots have been sent.
    SnapshotComplete { last_applied_seq: u64 },

    /// Account balance snapshot in response to `QueryPosition`.
    PositionSnapshot {
        account: AccountId,
        /// Per-currency snapshot. Fixed-size array avoids heap allocation;
        /// max 16 currencies per account. Slots past `count` are zeroed.
        balances: [AccountBalance; 16],
        /// Number of valid entries in `balances`. Remaining slots are zeroed.
        count: u8,
    },

    /// Per-key request_seq HWM snapshot in response to
    /// `QueryRequestSeq`. The engine has accepted requests up to and
    /// including this seq for the calling key; the client should set
    /// its next outbound seq to `hwm + 1` to bypass dedup. `0` for a
    /// key with no prior accepted activity.
    RequestSeqHwm { hwm: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;

    /// Every request kind with its category, as the request table in
    /// `docs/wire-protocol.md` states it: an oracle written from the spec,
    /// not from [`Request::category`]. A kind missing here fails the test
    /// below, so a new request is placed in this table as well as in the
    /// match.
    const DOCUMENTED: [(u8, RequestCategory); 19] = [
        (0x10, RequestCategory::Trading),        // SubmitOrder
        (0x11, RequestCategory::Trading),        // CancelOrder
        (0x12, RequestCategory::Connection),     // Heartbeat
        (0x13, RequestCategory::Trading),        // CancelAll
        (0x14, RequestCategory::Trading),        // CancelReplace
        (0x15, RequestCategory::Administration), // AddInstrument
        (0x16, RequestCategory::FundManagement), // Deposit
        (0x17, RequestCategory::FundManagement), // Withdraw
        (0x18, RequestCategory::Administration), // SetRiskLimits
        (0x19, RequestCategory::Administration), // SetCircuitBreaker
        (0x1A, RequestCategory::Administration), // SetFeeSchedule
        (0x1B, RequestCategory::Administration), // EndOfDay
        (0x1C, RequestCategory::Administration), // DisableInstrument
        (0x1D, RequestCategory::Administration), // EnableInstrument
        (0x1E, RequestCategory::Administration), // RemoveInstrument
        (0x1F, RequestCategory::Connection),     // Subscribe
        (0x20, RequestCategory::Administration), // QueryStats
        (0x21, RequestCategory::Trading),        // QueryPosition
        (0x22, RequestCategory::Trading),        // QueryRequestSeq
    ];

    #[test]
    fn every_request_has_its_documented_category() {
        let mut seen = [false; DOCUMENTED.len()];
        for request in codec::tests::make_requests() {
            let mut body = [0u8; codec::MAX_REQUEST_BODY];
            codec::encode_request_body(&request, 0, &mut body).unwrap();
            // A body opens with its request kind.
            let kind = body[0];
            let index = DOCUMENTED
                .iter()
                .position(|&(documented, _)| documented == kind)
                .unwrap_or_else(|| panic!("kind {kind:#04x} is not in the documented table"));
            assert_eq!(request.category(), DOCUMENTED[index].1, "{request:?}");
            seen[index] = true;
        }
        let unsampled: Vec<u8> = DOCUMENTED
            .iter()
            .zip(seen)
            .filter(|&(_, seen)| !seen)
            .map(|(&(kind, _), _)| kind)
            .collect();
        assert!(
            unsampled.is_empty(),
            "no sample request of kind {unsampled:#04x?}"
        );
    }
}
