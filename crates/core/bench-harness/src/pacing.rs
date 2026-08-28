//! Open-loop pacing: issue requests on a fixed schedule rather than as
//! fast as the server acknowledges them, and record latency against the
//! *scheduled* send time. This is the standard coordinated-omission fix —
//! without it, a server stall hides itself by also slowing the client's
//! send rate.

use std::sync::atomic::{AtomicU64, Ordering};

/// Slack tolerance for late sends. Any send issued more than this far past
/// its scheduled time counts toward `late_sends`. Set wider than the
/// natural event-loop fill granularity (one `submit_and_wait` cycle, on
/// the order of one RTT for kernel transports) so submit-cycle jitter
/// does not inflate the count. A non-zero value here means the bench is
/// structurally behind its schedule — back-pressure from the server or
/// the inflight cap — not that individual sends are a few microseconds
/// late.
pub const PACE_LATE_SLACK_NS: u64 = 1_000_000;

/// Per-connection open-loop scheduler. Each connection advances on its own
/// schedule (rate is split across connections at construction time), which
/// avoids cross-thread atomic contention on a shared cursor without
/// changing the aggregate target rate.
///
/// All arithmetic is in TSC ticks: the uring/dpdk hot paths already keep
/// per-frame timing in ticks, so reusing the same unit lets the scheduled
/// timestamp flow directly into the in-flight queue (no per-send
/// conversion).
#[derive(Clone, Copy)]
pub struct PaceClock {
    /// Ticks between consecutive scheduled sends on this connection.
    period_ticks: u64,
    /// TSC tick of the next scheduled send.
    next_due_ticks: u64,
}

impl PaceClock {
    /// Build a pacer for one connection given the *aggregate* target rate
    /// (requests/sec across all connections), the connection count it is
    /// shared with, the TSC calibration factor, the bench-start TSC, and
    /// the connection's index within the run. `conn_index` is used to
    /// stagger the first send by a fraction of one period — this avoids a
    /// thundering herd at `start_tsc` while preserving the aggregate rate.
    pub fn new(
        target_rate: u64,
        clients: u64,
        ticks_per_ns: f64,
        start_tsc: u64,
        conn_index: u64,
    ) -> Self {
        debug_assert!(target_rate > 0, "PaceClock::new requires target_rate > 0");
        debug_assert!(clients > 0, "PaceClock::new requires clients > 0");
        let rate_per_conn = target_rate as f64 / clients as f64;
        let period_ns = 1_000_000_000.0 / rate_per_conn;
        // u64 ticks: a period of ~10 ns at 3 GHz is ~30 ticks; rounding to
        // the nearest tick is well below clock skew across the run.
        let period_ticks = (period_ns * ticks_per_ns).round().max(1.0) as u64;
        // Stagger first send by conn_index * (period / clients). For
        // single-thread runs this leaves a uniform offset; for multi-thread
        // runs threads stay slightly out of phase, which is closer to real
        // client behavior.
        let stagger = period_ticks
            .saturating_mul(conn_index)
            .checked_div(clients)
            .unwrap_or(0);
        Self {
            period_ticks,
            next_due_ticks: start_tsc.saturating_add(stagger),
        }
    }

    /// If the next scheduled send is due at `now_ticks`, return its
    /// scheduled TSC and advance the cursor; otherwise return `None`. The
    /// returned tick is the *scheduled* time, not `now_ticks` — pushing
    /// the scheduled time into the latency record is the standard fix for
    /// coordinated omission.
    #[inline]
    pub fn pop_due(&mut self, now_ticks: u64) -> Option<u64> {
        if now_ticks >= self.next_due_ticks {
            let scheduled = self.next_due_ticks;
            self.next_due_ticks = self.next_due_ticks.saturating_add(self.period_ticks);
            Some(scheduled)
        } else {
            None
        }
    }

    /// Unconditionally return the next scheduled tick and advance the
    /// cursor. Intended for synchronous loops (in-process modes) where the
    /// caller spin-waits until the returned tick before doing work; for
    /// event-loop callers see `pop_due`.
    #[inline]
    pub fn advance(&mut self) -> u64 {
        let scheduled = self.next_due_ticks;
        self.next_due_ticks = self.next_due_ticks.saturating_add(self.period_ticks);
        scheduled
    }

    /// Reverse the most recent `pop_due` or `advance` so that the popped
    /// scheduled slot is re-issued next call. Used by transports that
    /// pop optimistically but may need to roll back when the wire send
    /// fails — without it, a transient send error would drop a scheduled
    /// slot and skew the achieved rate downward. Only userspace-TCP
    /// transports currently roll back (smoltcp can return Ok(0) on
    /// transient back-pressure); the kernel-TCP uring path never reaches
    /// a state where a popped frame isn't queued for send.
    #[inline]
    pub fn unpop(&mut self) {
        self.next_due_ticks = self.next_due_ticks.saturating_sub(self.period_ticks);
    }

    /// Ticks between consecutive scheduled sends. Exposed for tests and
    /// for reporting the resolved per-connection period.
    pub fn period_ticks(&self) -> u64 {
        self.period_ticks
    }

    /// TSC tick of the next scheduled send. Exposed for tests and for
    /// reporting how far ahead of the schedule a connection is.
    pub fn next_due_ticks(&self) -> u64 {
        self.next_due_ticks
    }
}

/// Aggregate pacing telemetry shared across bench threads. Updated lock-free.
#[derive(Default)]
pub struct PaceStats {
    /// Sends whose actual submission time exceeded `scheduled + slack`.
    /// A non-zero value indicates back-pressure from the server or
    /// inflight cap.
    pub late_sends: AtomicU64,
    /// Maximum observed `actual_send_tsc - scheduled_tsc` in ticks. Read
    /// once at end-of-run and converted to µs for reporting.
    pub max_send_delay_ticks: AtomicU64,
    /// Total scheduled sends (issued or skipped). Useful for the progress
    /// reporter when target-rate is set.
    pub scheduled: AtomicU64,
}

impl PaceStats {
    /// Record a paced send. `now_ticks` is the actual submission time;
    /// `scheduled_ticks` is what `PaceClock::pop_due` returned. If the
    /// delay exceeds `PACE_LATE_SLACK_NS`, `late_sends` is incremented.
    #[inline]
    pub fn record_send(&self, now_ticks: u64, scheduled_ticks: u64, ticks_per_ns: f64) {
        let delay_ticks = now_ticks.saturating_sub(scheduled_ticks);
        // Lazy max via CAS loop. Contention is essentially nil — only one
        // writer per bench thread, and at multi-M ops/s the value moves
        // monotonically toward the run max.
        let mut prev = self.max_send_delay_ticks.load(Ordering::Relaxed);
        while delay_ticks > prev {
            match self.max_send_delay_ticks.compare_exchange_weak(
                prev,
                delay_ticks,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
        let slack_ticks = (PACE_LATE_SLACK_NS as f64 * ticks_per_ns) as u64;
        if delay_ticks > slack_ticks {
            self.late_sends.fetch_add(1, Ordering::Relaxed);
        }
        self.scheduled.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod pace_clock_tests {
    use super::*;

    // 1 tick = 1 ns for predictable arithmetic in these tests.
    const TICKS_PER_NS: f64 = 1.0;

    #[test]
    fn period_matches_aggregate_rate_split_across_clients() {
        // 1 M requests/sec / 4 clients = 250 k/sec per client = 4 µs period.
        let p = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 0, 0);
        assert_eq!(p.period_ticks(), 4_000);
    }

    #[test]
    fn advance_returns_scheduled_and_steps_by_period() {
        let mut p = PaceClock::new(1_000_000, 1, TICKS_PER_NS, 5_000, 0);
        assert_eq!(p.advance(), 5_000);
        assert_eq!(p.advance(), 6_000);
        assert_eq!(p.advance(), 7_000);
    }

    #[test]
    fn unpop_reverses_one_step() {
        let mut p = PaceClock::new(1_000_000, 1, TICKS_PER_NS, 5_000, 0);
        assert_eq!(p.advance(), 5_000);
        assert_eq!(p.advance(), 6_000);
        p.unpop();
        // After unpop, the next advance re-issues 6_000.
        assert_eq!(p.advance(), 6_000);
        assert_eq!(p.advance(), 7_000);
    }

    #[test]
    fn pop_due_is_monotonic_and_paced() {
        let mut p = PaceClock::new(1_000_000, 1, TICKS_PER_NS, 0, 0);
        // 1 µs period at 1 M/s; first 3 sends due at 0, 1000, 2000.
        assert_eq!(p.pop_due(0), Some(0));
        assert_eq!(p.pop_due(999), None);
        assert_eq!(p.pop_due(1_000), Some(1_000));
        assert_eq!(p.pop_due(2_500), Some(2_000));
        // After popping at 2_500, next due is 3_000.
        assert_eq!(p.next_due_ticks(), 3_000);
    }

    #[test]
    fn stagger_offsets_conns_within_one_period() {
        let p0 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 0);
        let p1 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 1);
        let p2 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 2);
        let p3 = PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, 3);
        // period = 4 µs / 4 conns = 1 µs offsets.
        assert_eq!(p0.next_due_ticks(), 10_000);
        assert_eq!(p1.next_due_ticks(), 11_000);
        assert_eq!(p2.next_due_ticks(), 12_000);
        assert_eq!(p3.next_due_ticks(), 13_000);
    }

    /// Regression pin for the multi-thread stagger bug: when bench
    /// threads each constructed pacers using their *thread-local* conn
    /// index instead of the global one, every thread's conn-0 fired at
    /// the same offset, collapsing the herd. Modelling that here: four
    /// conns distributed round-robin across two threads use global
    /// indices 0..3; using local indices 0..1 on each thread would
    /// produce two pacers at the 10_000 anchor and two at the 12_000
    /// stagger — never covering the full period.
    #[test]
    fn stagger_uses_global_index_across_threads() {
        // 1 M aggregate, 4 clients → 4 µs period, 1 µs stagger.
        // Round-robin distribution across 2 threads: thread 0 owns
        // global conns {0, 2}, thread 1 owns {1, 3}.
        let global_indices = [0u64, 2, 1, 3];
        let dues: Vec<u64> = global_indices
            .iter()
            .map(|&i| PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, i).next_due_ticks())
            .collect();
        let mut sorted = dues.clone();
        sorted.sort();
        // First sends cover the whole period at 1 µs spacing.
        assert_eq!(sorted, vec![10_000, 11_000, 12_000, 13_000]);

        // Bug sibling: using the thread-local index (0, 1, 0, 1)
        // collapses two pairs onto the same tick.
        let local_indices = [0u64, 1, 0, 1];
        let buggy: Vec<u64> = local_indices
            .iter()
            .map(|&i| PaceClock::new(1_000_000, 4, TICKS_PER_NS, 10_000, i).next_due_ticks())
            .collect();
        // Two pacers at 10_000 and two at 11_000 — herd flattened only
        // within each thread, not across them.
        let mut buggy_sorted = buggy.clone();
        buggy_sorted.sort();
        assert_eq!(buggy_sorted, vec![10_000, 10_000, 11_000, 11_000]);
    }

    #[test]
    fn period_clamps_to_at_least_one_tick() {
        // Absurdly high rate would round period_ns to 0; clamp prevents
        // an infinite loop in `pop_due` (which would otherwise see every
        // `now` as due forever).
        let p = PaceClock::new(u64::MAX / 2, 1, TICKS_PER_NS, 0, 0);
        assert!(p.period_ticks() >= 1);
    }

    #[test]
    fn record_send_increments_late_when_past_slack() {
        let stats = PaceStats::default();
        // delay just over slack → late.
        stats.record_send(PACE_LATE_SLACK_NS + 1, 0, TICKS_PER_NS);
        // delay just under slack → not late.
        stats.record_send(PACE_LATE_SLACK_NS - 1, 0, TICKS_PER_NS);
        // delay = 0 → not late.
        stats.record_send(0, 0, TICKS_PER_NS);
        assert_eq!(stats.late_sends.load(Ordering::Relaxed), 1);
        assert_eq!(stats.scheduled.load(Ordering::Relaxed), 3);
        // Max should track the largest delay observed.
        assert_eq!(
            stats.max_send_delay_ticks.load(Ordering::Relaxed),
            PACE_LATE_SLACK_NS + 1
        );
    }
}
