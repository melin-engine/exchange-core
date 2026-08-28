//! Transport-agnostic benchmark harness for servers built on the Melin
//! sequencer.
//!
//! Everything here is independent of *what* is being benchmarked: the
//! low-overhead clock, the warmup/measured/cooldown phase model, the
//! open-loop pacer, the latency time-series recorder, the deterministic
//! per-client key derivation, and the two scrapers for the server's
//! `/health` and `/stats-dump` endpoints.
//!
//! The application-specific half — what a request looks like on the wire,
//! how a response is classified, what an "outcome" counts as — lives in
//! the benchmark binary that drives this harness (today `melin-bench` in
//! the exchange repository).
//!
//! ## Module map
//!
//! * [`clock`] — `rdtscp`/`cntvct_el0` reads and the anchored [`clock::TscClock`]
//!   that turns raw ticks into UNIX nanoseconds without a vDSO call.
//! * [`phases`] — the three wall-clock phases every bench loop runs
//!   through, plus the shared CLI defaults.
//! * [`pacing`] — open-loop scheduling ([`pacing::PaceClock`]) and its
//!   telemetry ([`pacing::PaceStats`]), the coordinated-omission fix.
//! * [`series`] — interval-percentile time series for stability plots.
//! * [`keys`] — deterministic per-connection ed25519 key derivation.
//! * [`health`] — background poller for the server's Prometheus endpoint.
//! * [`stats`] — client for the server's `/stats-dump` per-stage histograms.

pub mod clock;
pub mod health;
pub mod keys;
pub mod pacing;
pub mod phases;
pub mod series;
pub mod stats;
