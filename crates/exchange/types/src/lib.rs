//! Shared trading domain primitives (`Side`, `TimeInForce`, `AccountId`, …)
//! and little-endian encoding helpers, extracted from `melin-ec-trading` so
//! that crates which only need the wire-level data model (the protocol
//! codec, market-data publishers, gateway crates) don't transitively pull
//! the trading engine glue.
//!
//! `melin-ec-trading` re-exports both modules for backwards compatibility,
//! so existing `melin_ec_trading::types::*` / `melin_ec_trading::le::*` paths
//! keep resolving while consumers migrate.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod le;
pub mod types;
