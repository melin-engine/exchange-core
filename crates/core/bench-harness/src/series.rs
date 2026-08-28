//! Interval-percentile time series.
//!
//! The cumulative histogram answers "what was p99 over the whole run?".
//! It cannot show *when* a stall happened, because one bad millisecond is
//! averaged into millions of good ones. This module records a second,
//! short-lived histogram that is snapshotted and reset every
//! [`SAMPLE_INTERVAL`] completions, so the resulting series shows
//! temporal variation — the shape you need to tell a steady p99 from one
//! that spikes once a second.

use hdrhistogram::Histogram;
use std::time::Instant;

/// Number of completed requests between latency time-series samples.
/// Each sample captures interval p99/p99.9 (reset after each sample),
/// giving temporal variation rather than cumulative smoothing.
pub const SAMPLE_INTERVAL: usize = 1_000;

/// One latency time-series sample: interval percentiles at a point in time.
/// Captured every [`SAMPLE_INTERVAL`] completed requests using an interval
/// histogram (snapshot + reset), so each sample reflects recent behavior
/// rather than cumulative averages.
pub struct LatencySample {
    /// Seconds elapsed since measurement start.
    pub elapsed_secs: f64,
    /// Interval p99 latency in microseconds.
    pub p99_us: f64,
    /// Interval p99.9 latency in microseconds.
    pub p999_us: f64,
    /// Interval p99.99 latency in microseconds.
    pub p9999_us: f64,
}

/// Time-series of latency samples for chart display and stability plots.
pub type TimeSeries = Vec<LatencySample>;

/// Record a latency sample if [`SAMPLE_INTERVAL`] requests have accumulated
/// in the interval histogram. Resets the interval histogram after sampling.
pub fn maybe_sample(
    interval_hist: &mut Histogram<u64>,
    interval_count: &mut usize,
    series: &mut TimeSeries,
    start: Instant,
) {
    if *interval_count >= SAMPLE_INTERVAL {
        if !interval_hist.is_empty() {
            series.push(LatencySample {
                elapsed_secs: start.elapsed().as_secs_f64(),
                p99_us: interval_hist.value_at_quantile(0.99) as f64 / 1000.0,
                p999_us: interval_hist.value_at_quantile(0.999) as f64 / 1000.0,
                p9999_us: interval_hist.value_at_quantile(0.9999) as f64 / 1000.0,
            });
        }
        interval_hist.reset();
        *interval_count = 0;
    }
}

/// Render a series as the `time_series` array of the bench's JSON output.
///
/// The schema lives here rather than in the reporting code so the plot
/// tools that consume it have a single definition to track. Hand-rolled
/// rather than serde-derived to keep this crate dependency-light and the
/// numeric formatting (fixed decimals, not float round-trips) stable
/// across runs.
pub fn to_json(series: &[LatencySample]) -> String {
    if series.is_empty() {
        return String::from("[]");
    }
    let entries: Vec<String> = series
        .iter()
        .map(|s| {
            format!(
                "{{\"elapsed_secs\":{:.3},\"p99_us\":{:.2},\"p999_us\":{:.2},\"p9999_us\":{:.2}}}",
                s.elapsed_secs, s.p99_us, s.p999_us, s.p9999_us,
            )
        })
        .collect();
    format!("[{}]", entries.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hist() -> Histogram<u64> {
        Histogram::new(3).expect("create histogram")
    }

    #[test]
    fn does_not_sample_before_the_interval_is_reached() {
        let mut h = hist();
        h.record(1_000).unwrap();
        let mut count = SAMPLE_INTERVAL - 1;
        let mut series = TimeSeries::new();
        maybe_sample(&mut h, &mut count, &mut series, Instant::now());
        assert!(series.is_empty());
        // Counter and histogram must be left untouched so the pending
        // samples still land in the next interval.
        assert_eq!(count, SAMPLE_INTERVAL - 1);
        assert!(!h.is_empty());
    }

    #[test]
    fn samples_and_resets_once_the_interval_is_reached() {
        let mut h = hist();
        // 1000 ns → 1 µs at every percentile.
        for _ in 0..SAMPLE_INTERVAL {
            h.record(1_000).unwrap();
        }
        let mut count = SAMPLE_INTERVAL;
        let mut series = TimeSeries::new();
        maybe_sample(&mut h, &mut count, &mut series, Instant::now());
        assert_eq!(series.len(), 1);
        assert!((series[0].p99_us - 1.0).abs() < 0.01);
        assert_eq!(count, 0, "counter must reset");
        assert!(h.is_empty(), "interval histogram must reset");
    }

    /// An interval that hit the count but recorded nothing (every sample
    /// discarded as out-of-phase) must reset the counter without pushing
    /// a bogus all-zeros point into the series.
    #[test]
    fn empty_interval_resets_without_emitting_a_sample() {
        let mut h = hist();
        let mut count = SAMPLE_INTERVAL;
        let mut series = TimeSeries::new();
        maybe_sample(&mut h, &mut count, &mut series, Instant::now());
        assert!(series.is_empty());
        assert_eq!(count, 0);
    }

    #[test]
    fn empty_series_renders_as_empty_json_array() {
        assert_eq!(to_json(&[]), "[]");
    }

    #[test]
    fn json_uses_fixed_precision_per_field() {
        let series = vec![LatencySample {
            elapsed_secs: 1.23456,
            p99_us: 10.987,
            p999_us: 20.0,
            p9999_us: 30.5,
        }];
        assert_eq!(
            to_json(&series),
            "[{\"elapsed_secs\":1.235,\"p99_us\":10.99,\"p999_us\":20.00,\"p9999_us\":30.50}]"
        );
    }
}
