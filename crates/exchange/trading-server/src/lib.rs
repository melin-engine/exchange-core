//! Trading-specific server wiring.
//!
//! Holds the trading adapter for the generic
//! [`melin-server-runtime`](melin_server_runtime) pipeline:
//!
//! - [`exchange_app::ServerApp`] — the `Application`-impl newtype
//!   wrapping `melin_engine::exchange::Exchange` (orphan-rule
//!   workaround: the trait lives in `melin-app`, the engine in
//!   `melin-engine`, so the impl can only attach here).
//! - [`app_factory::ExchangeAppFactory`] — `AppFactory` impl that
//!   builds empty / seed-ready exchanges and yields the bulk-seed
//!   events the runtime journals on first start.
//! - [`request::ExchangeRequestDecoder`] — wire-`Request` →
//!   `TradingEvent` decoder.
//! - [`response_encoder::ExchangeResponseEncoder`] —
//!   `ExecutionReport` / `QueryResponse` → wire encoder.
//! - [`event_publisher`] — market-data firehose (trading-only;
//!   gated on `feature = "trading"`).

pub mod app_factory;
pub mod exchange_app;
pub mod request;
pub mod response_encoder;

#[cfg(all(feature = "trading", not(feature = "skip-order-exec")))]
pub mod event_publisher;
