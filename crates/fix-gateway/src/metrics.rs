//! FIX gateway metrics surface.
//!
//! Atomic counters incremented on the io_uring hot path; the
//! `/metrics` endpoint thread reads them with relaxed ordering. Mirrors
//! the hand-rolled, allocation-free pattern in
//! `crates/server/src/replication/mod.rs::ReplicationMetrics` so the
//! gateway can expose itself in the same Prometheus text format
//! without pulling in an external metrics crate.

use std::sync::atomic::AtomicU64;

/// Per-gateway counters. A single instance lives for the lifetime of
/// the process and is shared between the event loop (incrementing on
/// the hot path) and the metrics endpoint thread (reading on demand).
#[derive(Default)]
pub struct GatewayMetrics {
    /// Cumulative count of accepted FIX client connections.
    pub sessions_accepted_total: AtomicU64,
    /// Currently active sessions (gauge).
    pub sessions_active: AtomicU64,
    /// Complete FIX frames handed to the session for dispatch
    /// (includes those that subsequently fail to parse — see
    /// `parse_errors_total` for the failed subset).
    pub messages_received_total: AtomicU64,
    /// FIX messages queued for transmission to clients.
    pub messages_sent_total: AtomicU64,
    /// Inbound messages that failed to parse (trust-boundary rejects).
    pub parse_errors_total: AtomicU64,
    /// ResendRequest messages we sent in response to detected gaps.
    pub resend_requests_sent_total: AtomicU64,
    /// ResendRequest messages we received from peers.
    pub resend_requests_received_total: AtomicU64,
    /// Outbound store evictions (oldest message dropped because the
    /// store hit `MAX_OUTBOUND_STORE_MSGS`).
    pub store_evictions_total: AtomicU64,
    /// Inbound messages dropped because the per-session rate limit
    /// was exceeded in the current window.
    pub rate_limit_hits_total: AtomicU64,
}

impl GatewayMetrics {
    /// Allocate a fresh metrics instance and leak it as `'static`.
    /// The gateway process holds one of these for its lifetime, so a
    /// one-time leak is the simplest way to share it across threads
    /// without ref counting on the hot path.
    pub fn leak_default() -> &'static Self {
        Box::leak(Box::new(Self::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn leak_default_returns_zeroed_static() {
        let m = GatewayMetrics::leak_default();
        assert_eq!(m.sessions_active.load(Ordering::Relaxed), 0);
        assert_eq!(m.parse_errors_total.load(Ordering::Relaxed), 0);
    }
}
