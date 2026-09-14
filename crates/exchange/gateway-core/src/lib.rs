//! Shared FIX protocol and session infrastructure for Melin gateways.
//!
//! Contains the FIX 4.4 parser/serializer and tag constants. Both
//! `melin-ec-oe-gateway` (order entry) and `melin-ec-md-gateway`
//! (market data) depend on this crate. Authentication towards the
//! sequencer is the sequencer client's (`melin-client`), keys included.

pub mod fix;
