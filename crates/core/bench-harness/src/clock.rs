//! TSC (Time Stamp Counter) utilities for low-overhead per-order timing.

use std::time::{Duration, Instant};

/// Read the TSC with a serializing instruction (`rdtscp`). Returns raw tick
/// count. ~4ns overhead vs ~15-25ns for `Instant::now()` via vDSO.
/// `rdtscp` waits for all prior instructions to complete before reading,
/// preventing the CPU from reordering the timestamp relative to the work
/// being measured.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn rdtscp() -> u64 {
    unsafe {
        let mut _aux: u32 = 0;
        core::arch::x86_64::__rdtscp(&mut _aux)
    }
}

/// Read the ARM virtual counter (`cntvct_el0`). ~2-5ns overhead,
/// equivalent to x86's `rdtscp`. An `isb` (instruction synchronization
/// barrier) serializes the pipeline to prevent reordering the read
/// relative to the work being measured.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn rdtscp() -> u64 {
    let cnt: u64;
    unsafe {
        core::arch::asm!(
            "isb",
            "mrs {}, cntvct_el0",
            out(reg) cnt,
            options(nostack, nomem),
        );
    }
    cnt
}

/// Calibrate TSC/counter ticks per nanosecond by measuring a short sleep
/// against `Instant::now()`. Returns the conversion factor (ticks / ns).
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn calibrate_tsc() -> f64 {
    calibrate_tsc_clock().ticks_per_ns
}

/// Anchored TSC clock: ticks-per-ns plus a `(tsc, unix_ns)` pair captured at
/// calibration time. Lets the hot path turn any later `rdtscp()` reading
/// into a UNIX-nanos timestamp without a `clock_gettime()` vDSO call —
/// previously `~25 ns` per event and visible in flamegraphs as ~6 % of
/// the bench's `pipeline-pub` thread.
///
/// Two sources of error to be aware of when reading derived timestamps:
///
/// - **Anchor-capture offset** (~30–50 ns, constant): the calibration
///   loop reads `unix_ns` first and the TSC second, so derived values
///   undershoot truth by the time it takes one `clock_gettime` call to
///   complete (plus a few cycles of bookkeeping). Choosing
///   undershoot is deliberate — a "did we pass deadline X?" check
///   downstream falsing earlier is safer than falsing later.
/// - **Linear drift** from the calibration's `ticks_per_ns` measurement
///   error. On a 10 ms sleep against `Instant::now()`, that's typically
///   bounded by sleep jitter (single-digit µs) plus the host's TSC
///   stability vs the kernel's `CLOCK_MONOTONIC`. Empirically ~100 ppm
///   on this fleet, so a 60 s bench drifts up to ~6 ms — well below
///   anything the workloads we publish exercise, but a bench that drives
///   a time-triggered engine feature (scheduled expiry, rate limiting)
///   would need a fresher anchor.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[derive(Clone, Copy)]
pub struct TscClock {
    pub ticks_per_ns: f64,
    /// Inverse of `ticks_per_ns`, precomputed so the hot path uses
    /// multiplication instead of division (a few cycles per event).
    pub ns_per_tick: f64,
    /// TSC reading at calibration time. Pairs with `anchor_unix_ns`.
    pub anchor_tsc: u64,
    /// UNIX nanos at calibration time. Pairs with `anchor_tsc`.
    pub anchor_unix_ns: u64,
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
impl TscClock {
    /// Convert a TSC reading taken later in this process to UNIX
    /// nanoseconds. Saturates at the anchor if `ts < anchor_tsc`
    /// (shouldn't happen on any monotonic counter, but defensive
    /// against unexpected CPU migrations on cores with un-synchronised
    /// TSCs). See the struct docs for the small constant offset and the
    /// linear drift bound.
    #[inline(always)]
    pub fn unix_ns(&self, ts: u64) -> u64 {
        let delta_ticks = ts.saturating_sub(self.anchor_tsc);
        self.anchor_unix_ns + (delta_ticks as f64 * self.ns_per_tick) as u64
    }
}

/// Calibrate TSC and capture an anchor pair (`tsc`, `unix_ns`) so the hot
/// path can derive wall-clock timestamps from `rdtscp()` alone.
///
/// `anchor_unix_ns` is sampled *before* `anchor_tsc` so the natural
/// inter-call delay (one vDSO `clock_gettime`, ~25–50 ns) pushes the
/// recorded UNIX-nanos slightly into the past relative to the TSC
/// anchor. The result: `TscClock::unix_ns(ts)` always returns a value
/// no later than what `clock_gettime` would have returned at the same
/// `ts`. See `TscClock` docs for the full error model.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn calibrate_tsc_clock() -> TscClock {
    // Warm up the counter path.
    for _ in 0..100 {
        let _ = rdtscp();
    }

    let duration = Duration::from_millis(10);
    // Order matters: capture `anchor_unix_ns` *before* `anchor_tsc` so
    // any inter-call slippage rounds derived timestamps earlier rather
    // than later (see fn docs).
    let anchor_unix_ns = melin_app::unix_epoch_nanos();
    let anchor_tsc = rdtscp();
    let t0_wall = Instant::now();
    std::thread::sleep(duration);
    let t1_tsc = rdtscp();
    let elapsed_ns = t0_wall.elapsed().as_nanos() as f64;
    let elapsed_tsc = (t1_tsc - anchor_tsc) as f64;
    let ticks_per_ns = elapsed_tsc / elapsed_ns;
    TscClock {
        ticks_per_ns,
        ns_per_tick: 1.0 / ticks_per_ns,
        anchor_tsc,
        anchor_unix_ns,
    }
}

/// Convert counter tick delta to nanoseconds using a pre-calibrated factor.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[inline(always)]
pub fn tsc_to_ns(ticks: u64, ticks_per_ns: f64) -> u64 {
    (ticks as f64 / ticks_per_ns) as u64
}

#[cfg(test)]
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
mod tsc_clock_tests {
    use super::*;

    /// A freshly calibrated `TscClock`, queried immediately, must agree
    /// with `melin_app::unix_epoch_nanos()` to within a millisecond.
    /// That window catches both flipped-sign anchor regressions
    /// (derived value diverges by anchor_unix_ns) and a units mix-up in
    /// `ns_per_tick` (the elapsed delta is small immediately after
    /// calibration, so any factor error would still surface as a few-µs
    /// drift before the kernel clock advances by the same amount).
    #[test]
    fn freshly_calibrated_clock_matches_wall_clock_within_1ms() {
        let clock = calibrate_tsc_clock();
        let derived = clock.unix_ns(rdtscp());
        let now_unix = melin_app::unix_epoch_nanos();
        let diff = derived.abs_diff(now_unix);
        assert!(
            diff < 1_000_000,
            "derived {derived} vs wall {now_unix}, |Δ| = {diff} ns"
        );
    }

    /// `unix_ns` must not underflow when the supplied TSC reading is
    /// older than the anchor (which can happen if a thread migrated to
    /// a core with an out-of-sync TSC, or simply if a TSC reading
    /// captured pre-calibration is fed in by mistake).
    #[test]
    fn unix_ns_saturates_on_pre_anchor_tsc() {
        let clock = calibrate_tsc_clock();
        let value = clock.unix_ns(clock.anchor_tsc.saturating_sub(1_000));
        assert_eq!(value, clock.anchor_unix_ns);
    }
}
