//! Trading-shaped wire protocol: `Request` / `Response` enums and the
//! binary codec. Framing, transport listeners, and the protocol error
//! type live in `melin-wire-protocol`.

pub mod codec;
pub mod message;

/// Re-export engine types that clients need to construct requests and
/// interpret responses, so they don't need a direct dependency on the
/// engine crate.
pub mod types {
    pub use melin_types::types::{
        AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, InstrumentSpec,
        InstrumentStatus, Order, OrderId, OrderType, Price, Quantity, RejectReason, RiskLimits,
        SelfTradeProtection, Side, Symbol, TimeInForce,
    };
}
