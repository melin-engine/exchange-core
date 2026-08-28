//! End-of-run reporting: console summary and the JSON results file.
//!
//! Everything here is application-neutral except the outcome section,
//! which the [`Outcomes`] implementation renders for itself. The JSON
//! schema is the harness's contract with the plotting tools, so changing a
//! key here changes what they can read.

use std::path::Path;
use std::time::Duration;

use hdrhistogram::Histogram;

use crate::health::HealthReport;
use crate::phases::BenchPhases;
use crate::series::{self as series, LatencySample};
use crate::stats;
use crate::workload::Outcomes;

/// The noun a benchmark counts, used in console labels. An exchange
/// counts orders; a generic server counts requests.
///
/// Spelled out per form rather than derived from one string: deriving
/// "Per-Order" from "order" means title-casing, which goes wrong the
/// moment a unit is two words or an acronym.
pub struct Unit {
    /// Singular, lowercase. Renders as `µs/order`.
    pub singular: &'static str,
    /// Plural, lowercase. Renders as `orders/sec`.
    pub plural: &'static str,
    /// Title-case heading fragment. Renders as `Per-Order Latency`.
    pub heading: &'static str,
}

impl Unit {
    /// The default for a server with no more specific noun.
    pub const REQUEST: Unit = Unit {
        singular: "request",
        plural: "requests",
        heading: "Per-Request",
    };
}

/// End-of-run pacing report. `None` when no target rate was set (the
/// closed-loop case); rendered into the JSON output and the console
/// summary lines otherwise.
pub struct PacingReport {
    pub target_rate: u64,
    pub scheduled: u64,
    pub late_sends: u64,
    pub max_send_delay_us: f64,
}

/// Everything one finished run reports.
pub struct RunReport<'a, O: Outcomes> {
    /// Mode name for the console header and the JSON `label` field.
    pub label: &'a str,
    /// Noun for console labels.
    pub unit: &'a Unit,
    /// Completions recorded during the measured phase — the throughput
    /// numerator and the histogram's sample count.
    pub measured_count: usize,
    pub phases: BenchPhases,
    pub histogram: &'a Histogram<u64>,
    /// Measured-phase wall time, the throughput denominator.
    pub wall: Duration,
    /// Mode-specific lines printed under the header (core pinning,
    /// transport, feature flags).
    pub extra_lines: &'a [String],
    /// Where to write the JSON results, if anywhere.
    pub json_path: Option<&'a Path>,
    pub series: &'a [LatencySample],
    pub health: &'a HealthReport,
    pub server_stages: &'a stats::Body,
    pub pacing: Option<&'a PacingReport>,
    pub outcomes: &'a O,
}

/// Print a latency histogram in µs. Adaptive nines: only prints p99.9, p99.99,
/// etc. when `sample_count` is large enough (10× per extra nine).
pub fn print_latency_histogram(hist: &Histogram<u64>, sample_count: usize) {
    println!("    min:     {:>8.2} µs", hist.min() as f64 / 1_000.0);
    println!(
        "    p50:     {:>8.2} µs",
        hist.value_at_quantile(0.50) as f64 / 1_000.0
    );
    println!(
        "    p90:     {:>8.2} µs",
        hist.value_at_quantile(0.90) as f64 / 1_000.0
    );
    let mut nines = 2;
    let mut threshold = 1_000usize;
    while threshold <= sample_count {
        let quantile = 1.0 - 10.0f64.powi(-(nines as i32));
        let label = if nines <= 2 {
            "p99".to_string()
        } else {
            format!("p99.{}", "9".repeat(nines - 2))
        };
        let value = hist.value_at_quantile(quantile) as f64 / 1_000.0;
        let padded = format!("{label}:");
        println!("    {padded:<9}{value:>8.2} µs");
        nines += 1;
        threshold *= 10;
    }
    println!("    max:     {:>8.2} µs", hist.max() as f64 / 1_000.0);
}

/// Render the latency percentiles as the JSON `latency` object. Uses the
/// same adaptive-nines rule as the console histogram, so the two agree on
/// which percentiles a run had the samples to support.
fn percentiles_json(histogram: &Histogram<u64>, measured_count: usize) -> String {
    let mut percentiles = String::from("{");
    percentiles.push_str(&format!(
        "\"min_us\":{:.2},\"p50_us\":{:.2},\"p90_us\":{:.2}",
        histogram.min() as f64 / 1000.0,
        histogram.value_at_quantile(0.50) as f64 / 1000.0,
        histogram.value_at_quantile(0.90) as f64 / 1000.0,
    ));
    let mut n = 2;
    let mut t = 1_000usize;
    while t <= measured_count {
        let q = 1.0 - 10.0f64.powi(-(n as i32));
        let label = if n <= 2 {
            "p99_us".to_string()
        } else {
            format!("p99{}_us", ".9".repeat(n - 2))
        };
        percentiles.push_str(&format!(
            ",\"{}\":{:.2}",
            label,
            histogram.value_at_quantile(q) as f64 / 1000.0
        ));
        n += 1;
        t *= 10;
    }
    percentiles.push_str(&format!(
        ",\"max_us\":{:.2}}}",
        histogram.max() as f64 / 1000.0
    ));
    percentiles
}

/// Serialize health samples: the fixed fields plus any extra metrics the
/// scraper picked up.
fn health_json(health: &HealthReport) -> String {
    if health.samples.is_empty() {
        return String::from("[]");
    }
    let entries: Vec<String> = health.samples
        .iter()
        .map(|s| {
            let mut json = format!(
                "{{\"elapsed_secs\":{:.3},\"active_connections\":{},\"events_processed\":{},\"journal_sequence\":{},\"replication_lag\":{},\"input_queue_depth\":{},\"input_queue_capacity\":{},\"pipeline_healthy\":{},\"trading_active\":{}",
                s.elapsed_secs,
                s.active_connections,
                s.events_processed,
                s.journal_sequence,
                s.replication_lag,
                s.input_queue_depth,
                s.input_queue_capacity,
                s.pipeline_healthy,
                s.trading_active,
            );
            // Append extra metrics (per-replica replication stats, etc.).
            // Sorted for deterministic output. Prometheus label syntax
            // like `metric{slot="0"}` is sanitized to `metric_slot_0`
            // for valid JSON keys.
            let mut keys: Vec<&String> = s.extra.keys().collect();
            keys.sort();
            for key in keys {
                let val = s.extra[key];
                // Sanitize Prometheus label syntax for JSON keys:
                // melin_replica_lag{slot="0"} → melin_replica_lag_slot_0
                let safe_key: String = key
                    .chars()
                    .filter_map(|c| match c {
                        '{' | '=' => Some('_'),
                        '}' | '"' => None,
                        other => Some(other),
                    })
                    .collect();
                // Emit integers without decimal point for cleaner JSON.
                if val == val.trunc() && val.abs() < u64::MAX as f64 {
                    json.push_str(&format!(",\"{safe_key}\":{}", val as i64));
                } else {
                    json.push_str(&format!(",\"{safe_key}\":{val:.3}"));
                }
            }
            json.push('}');
            json
        })
        .collect();
    format!("[{}]", entries.join(","))
}

/// Print benchmark results: header, throughput, latency histogram,
/// outcomes, health, and server-side stages. Optionally writes results to
/// a JSON file for post-processing.
pub fn print_results<O: Outcomes>(report: RunReport<'_, O>) {
    let RunReport {
        label,
        unit,
        measured_count,
        phases,
        histogram,
        wall,
        extra_lines,
        json_path,
        series,
        health,
        server_stages,
        pacing,
        outcomes,
    } = report;

    let throughput = (measured_count as f64) / wall.as_secs_f64();
    let wall_ms = wall.as_micros() as f64 / 1000.0;

    println!(
        "=== {label} Benchmark ({measured_count} measured, warmup={} measured={} cooldown={}) ===",
        humantime::format_duration(phases.warmup),
        humantime::format_duration(phases.measured),
        humantime::format_duration(phases.cooldown),
    );
    for line in extra_lines {
        println!("{line}");
    }
    println!();
    println!("  Throughput");
    println!("    wall time:  {wall_ms:.2} ms");
    println!(
        "    throughput: {throughput:.0} {}/sec ({:.2} µs/{})",
        unit.plural,
        1_000_000.0 / throughput,
        unit.singular,
    );
    println!();
    println!("  {} Latency", unit.heading);
    print_latency_histogram(histogram, measured_count);

    outcomes.render_console();

    // Print health summary if we collected anything — drops alone (no
    // successful sample) still warrant a line so the gap is visible.
    if !health.samples.is_empty() || health.dropped > 0 {
        let duration = health.samples.last().map_or(0.0, |s| s.elapsed_secs)
            - health.samples.first().map_or(0.0, |s| s.elapsed_secs);
        let peak_depth = health
            .samples
            .iter()
            .map(|s| s.input_queue_depth)
            .max()
            .unwrap_or(0);
        let capacity = health
            .samples
            .iter()
            .map(|s| s.input_queue_capacity)
            .max()
            .unwrap_or(0);
        let final_events = health.samples.last().map_or(0, |s| s.events_processed);
        println!();
        let total = health.samples.len() as u64 + health.dropped;
        let drop_pct = if total > 0 {
            health.dropped as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        println!(
            "  Health ({} samples over {duration:.1}s, {dropped} dropped, {drop_pct:.1}%)",
            health.samples.len(),
            dropped = health.dropped,
        );
        if capacity > 0 {
            let pct = peak_depth as f64 / capacity as f64 * 100.0;
            println!("    peak queue depth: {peak_depth} / {capacity} ({pct:.1}%)");
        } else {
            println!("    peak queue depth: {peak_depth}");
        }
        println!("    events processed: {final_events}");
    }

    // Server-side per-stage decomposition (tick-to-trade). Fetched
    // from the server's /stats-dump endpoint at end of run; only
    // populated for network modes against a server built with
    // --features latency-trace.
    stats::render_console(server_stages);

    // Write JSON results if requested.
    if let Some(path) = json_path {
        use std::io::Write;

        let percentiles = percentiles_json(histogram, measured_count);
        // Schema owned by the harness so the plotting tools have one
        // definition to track.
        let ts_json = series::to_json(series);
        let health_json = health_json(health);
        let stages_json = stats::render_json(server_stages);
        let outcomes_json = outcomes.render_json();

        // Pacing fragment: emitted only when a target rate was set, so the
        // schema for closed-loop runs is unchanged.
        let pacing_json = match pacing {
            Some(p) => format!(
                ",\"pacing\":{{\"target_rate\":{},\"scheduled\":{},\"achieved_rate\":{:.0},\"late_sends\":{},\"max_send_delay_us\":{:.2}}}",
                p.target_rate, p.scheduled, throughput, p.late_sends, p.max_send_delay_us,
            ),
            None => String::new(),
        };

        // `measured_orders` is kept as the key name for compatibility with
        // existing result files and the plotting tools, even though the
        // harness itself counts requests.
        let json = format!(
            "{{\"label\":\"{label}\",\"measured_orders\":{measured_count},\"warmup_ms\":{:.2},\"measured_ms\":{:.2},\"cooldown_ms\":{:.2},\"wall_ms\":{:.2},\"throughput_ops\":{:.0},\"latency\":{percentiles},\"time_series\":{ts_json},\"health\":{health_json},\"server_stages\":{stages_json}{pacing_json},\"outcomes\":{outcomes_json}}}",
            phases.warmup.as_secs_f64() * 1000.0,
            phases.measured.as_secs_f64() * 1000.0,
            phases.cooldown.as_secs_f64() * 1000.0,
            wall.as_secs_f64() * 1000.0,
            throughput,
        );

        let mut file = std::fs::File::create(path).expect("create json file");
        file.write_all(json.as_bytes()).expect("write json");
        file.write_all(b"\n").expect("write newline");
        eprintln!("Results written to {}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hist(values: &[u64]) -> Histogram<u64> {
        let mut h = Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("bounds");
        for v in values {
            h.record(*v).expect("record");
        }
        h
    }

    /// Below 1000 samples only p99 is reported; each extra nine needs a
    /// 10× larger sample count to be meaningful.
    #[test]
    fn percentiles_json_adds_nines_with_sample_count() {
        let h = hist(&[1_000; 10]);
        let small = percentiles_json(&h, 999);
        assert!(small.contains("\"p90_us\""));
        assert!(!small.contains("\"p99_us\""), "{small}");

        let medium = percentiles_json(&h, 1_000);
        assert!(medium.contains("\"p99_us\""));
        assert!(!medium.contains("\"p99.9_us\""), "{medium}");

        let large = percentiles_json(&h, 10_000);
        assert!(large.contains("\"p99.9_us\""), "{large}");
    }

    #[test]
    fn percentiles_json_reports_microseconds() {
        // 2_000 ns = 2.00 µs at every percentile.
        let h = hist(&[2_000; 10]);
        let json = percentiles_json(&h, 10);
        assert!(json.contains("\"min_us\":2.00"), "{json}");
        assert!(json.contains("\"max_us\":2.00"), "{json}");
    }

    #[test]
    fn health_json_is_empty_array_without_samples() {
        assert_eq!(health_json(&HealthReport::default()), "[]");
    }

    /// Prometheus label syntax is not valid in a JSON key, so
    /// `metric{slot="0"}` has to survive as `metric_slot_0`.
    #[test]
    fn health_json_sanitizes_prometheus_label_syntax() {
        let mut sample = crate::health::HealthSample {
            elapsed_secs: 1.0,
            active_connections: 0,
            events_processed: 0,
            journal_sequence: 0,
            replication_lag: 0,
            input_queue_depth: 0,
            input_queue_capacity: 0,
            pipeline_healthy: true,
            trading_active: true,
            extra: Default::default(),
        };
        sample
            .extra
            .insert("melin_replica_lag{slot=\"0\"}".to_string(), 7.0);
        let report = HealthReport {
            samples: vec![sample],
            dropped: 0,
        };
        let json = health_json(&report);
        assert!(json.contains("\"melin_replica_lag_slot_0\":7"), "{json}");
        assert!(!json.contains('{') || !json.contains("slot=\""), "{json}");
    }
}
