//! Trading-shaped wire protocol: `Request` / `Response` enums, the binary
//! codec, and the access model deciding which role may send which
//! request. Framing, transport listeners, and the protocol error type
//! live in `melin-wire-protocol`.

pub mod codec;
pub mod message;
pub mod role;

/// Re-export engine types that clients need to construct requests and
/// interpret responses, so they don't need a direct dependency on the
/// engine crate.
pub mod types {
    pub use melin_ec_types::types::{
        AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, InstrumentSpec,
        InstrumentStatus, Order, OrderId, OrderType, Price, Quantity, RejectReason, RiskLimits,
        SelfTradeProtection, Side, Symbol, TimeInForce,
    };
}
