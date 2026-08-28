//! The three wall-clock phases every bench loop runs through, and the
//! CLI defaults shared by the binaries that drive this harness.
//!
//! Run length is wall-clock-driven rather than count-driven: each phase
//! is a duration, and completions are classified by *receive time*
//! against shared deadlines. That lets every bench thread agree on which
//! phase a sample belongs to without further coordination.

use std::time::{Duration, Instant};

/// Default measured-phase duration.
pub const DEFAULT_DURATION: Duration = Duration::from_secs(60);

/// Default warmup duration — primes caches, branch predictors, allocator
/// arenas, and the disruptor ring before measurement starts.
pub const DEFAULT_WARMUP: Duration = Duration::from_secs(5);

/// Default cooldown duration — drains the final fsync-tail batches whose
/// per-event cost isn't amortised across a full window. The samples
/// recorded during cooldown are discarded.
pub const DEFAULT_COOLDOWN: Duration = Duration::from_secs(5);

/// Default number of requests in flight simultaneously per client. Controls
/// the level of pipelining — enough to keep the server pipeline saturated
/// (journal + application stages overlap), small enough that per-request
/// latency reflects actual processing time rather than unbounded queueing.
pub const DEFAULT_WINDOW: usize = 64;

/// Default number of concurrent client connections.
pub const DEFAULT_CLIENTS: usize = 16;

/// Default number of bench client threads. Each thread manages a subset of
/// connections via io_uring. Pinned to cores 7-10 (2 physical + 2 HT siblings
/// on 8C/16T). With 4 bench + 6 server (3 pipeline + 2 reader + 1 repl-sender)
/// = 10 pinned threads total, leaving core 0 for OS/IRQ.
pub const DEFAULT_BENCH_THREADS: usize = 4;

/// Clap value parser: accept any humantime-recognised duration (`30s`,
/// `2m`, `500ms`, …). Surfaces parse errors as clap-friendly strings.
pub fn parse_duration(s: &str) -> Result<humantime::Duration, String> {
    s.parse::<humantime::Duration>()
        .map_err(|e| format!("invalid duration `{s}`: {e}"))
}

/// `BenchPhases` carries the three wall-clock durations that drive every
/// bench loop: warmup (priming), measured (recorded into the histogram),
/// and cooldown (final drain whose samples are discarded).
#[derive(Clone, Copy)]
pub struct BenchPhases {
    pub warmup: Duration,
    pub measured: Duration,
    pub cooldown: Duration,
}

impl BenchPhases {
    /// Deadlines relative to a shared `start` instant.
    pub fn deadlines(self, start: Instant) -> PhaseDeadlines {
        let warmup_end = start + self.warmup;
        let measured_end = warmup_end + self.measured;
        let cooldown_end = measured_end + self.cooldown;
        PhaseDeadlines {
            warmup_end,
            measured_end,
            cooldown_end,
        }
    }
}

/// Wall-clock cutoffs for the three phases.
#[derive(Clone, Copy)]
pub struct PhaseDeadlines {
    pub warmup_end: Instant,
    pub measured_end: Instant,
    pub cooldown_end: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadlines_are_cumulative_from_start() {
        let phases = BenchPhases {
            warmup: Duration::from_secs(1),
            measured: Duration::from_secs(10),
            cooldown: Duration::from_secs(2),
        };
        let start = Instant::now();
        let d = phases.deadlines(start);
        assert_eq!(d.warmup_end, start + Duration::from_secs(1));
        assert_eq!(d.measured_end, start + Duration::from_secs(11));
        assert_eq!(d.cooldown_end, start + Duration::from_secs(13));
    }

    /// Zero-length phases must not reorder the deadlines — a run with
    /// `--warmup-duration=0` still has `warmup_end <= measured_end`, so
    /// the "which phase is this sample in?" comparison chain stays sound.
    #[test]
    fn zero_length_phases_keep_deadlines_ordered() {
        let phases = BenchPhases {
            warmup: Duration::ZERO,
            measured: Duration::from_secs(5),
            cooldown: Duration::ZERO,
        };
        let start = Instant::now();
        let d = phases.deadlines(start);
        assert_eq!(d.warmup_end, start);
        assert_eq!(d.measured_end, d.cooldown_end);
        assert!(d.warmup_end <= d.measured_end);
    }

    #[test]
    fn parse_duration_accepts_humantime_and_reports_errors() {
        assert_eq!(
            parse_duration("500ms").unwrap(),
            humantime::Duration::from(Duration::from_millis(500))
        );
        assert!(parse_duration("banana").unwrap_err().contains("banana"));
    }
}
