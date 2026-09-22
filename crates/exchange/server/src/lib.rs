//! Trading-specific server wiring.
//!
//! Holds the trading adapter for the generic
//! `melin-server-runtime` pipeline:
//!
//! - [`exchange_app::ServerApp`] — the `Application`-impl newtype
//!   wrapping `melin_ec::exchange::Exchange` (orphan-rule
//!   workaround: the trait lives in `melin-app`, the engine in
//!   `melin-ec`, so the impl can only attach here).
//! - [`startup::StartupConfig`] — the node's startup configuration:
//!   the genesis seed and account limits the runtime journals, and the
//!   sizing it hands to `prefault`.
//! - [`request_decoder::RequestDecoder`] — wire-`Request` →
//!   `TradingRequest` decoder.
//! - [`response_encoder::ResponseEncoder`] —
//!   `ExecutionReport` / `QueryResponse` → wire encoder.
//! - [`event_publisher`] — market-data firehose.
//! - [`named_cores`] — the benches' `thread=core` flag parser, in the
//!   server's `--cores` shape.

pub mod exchange_app;
pub mod request_decoder;
pub mod response_encoder;
pub mod startup;

pub mod event_publisher;
pub mod named_cores;

// Crate-root re-exports for the three trading adapters most often
// referenced from outside this crate — the `melin-ec-server` binary, the
// `melin-server-runtime` doc comments, and bench code all reach them by
// short path. Keeps doc-links like `melin_ec_server::StartupConfig`
// resolving without requiring callers to know the internal module layout.
pub use exchange_app::ServerApp;
pub use request_decoder::RequestDecoder;
pub use response_encoder::ResponseEncoder;
pub use startup::StartupConfig;
