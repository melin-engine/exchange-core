//! Wire protocol for the trading engine.
//!
//! Defines message types, binary codec, and transport abstraction.
//! Shared by the server and client crates.

pub mod codec;
pub mod error;
pub mod message;
pub mod tcp;
pub mod transport;
