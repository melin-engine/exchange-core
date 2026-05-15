//! Calibration test: drive [`OrderFlowGenerator`] through the same
//! pipeline that extracts ITCH reference stats and compare the
//! resulting marginals against the committed reference fixture.
//!
//! Default fixture is embedded from `tests/fixtures/reference-stats.json`
//! and compared against `DEFAULT_SYMBOL`. To compare against a
//! different reference, set `MELIN_CALIBRATION_FIXTURE` to a JSON
//! path and `MELIN_CALIBRATION_SYMBOL` to a ticker present in that
//! file — both override the defaults.
//!
//! Two tests:
//!
//! 1. **Diagnostic** — `calibration_report` always passes. Prints a
//!    quantile-by-quantile comparison between the generator and the
//!    reference.
//!
//! 2. **Regression** — `calibration_basics_within_tolerance` asserts
//!    the small set of marginals the generator's *current* design
//!    intends to match: side balance ≈ 50/50, cancel/replace-to-add
//!    ratio in a sane band, and zero book-tracker errors.

use std::env;

use melin_bench::calibration::fixture::{ReferenceFixture, SymbolFixture};
use melin_bench::calibration::generator_adapter::GeneratorAdapter;
use melin_bench::calibration::stats::StatsAggregator;
use melin_bench::generator::GeneratorConfig;

/// 200k events is enough to stabilize quantiles to ~1% for the body
/// of the distribution and ~5% for the deep tail; finishes in well
/// under a second so the test stays in the fast `cargo test` lane.
const GENERATED_EVENTS: usize = 200_000;

/// Embedded reference fixture: aggregated quantile/scalar summaries
/// per ticker, no per-event records.
const DEFAULT_FIXTURE: &str = include_str!("fixtures/reference-stats.json");
const DEFAULT_SYMBOL: &str = "AAPL";

fn load_reference() -> SymbolFixture {
    let raw: String;
    let raw_ref: &str = match env::var("MELIN_CALIBRATION_FIXTURE") {
        Ok(path) => {
            raw = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read calibration fixture {path}: {e}"));
            &raw
        }
        Err(_) => DEFAULT_FIXTURE,
    };
    let symbol =
        env::var("MELIN_CALIBRATION_SYMBOL").unwrap_or_else(|_| DEFAULT_SYMBOL.to_string());
    let fixture: ReferenceFixture = serde_json::from_str(raw_ref).expect("parse reference fixture");
    fixture
        .symbols
        .get(&symbol)
        .cloned()
        .unwrap_or_else(|| panic!("symbol {symbol} missing from fixture"))
}

/// Run the generator with the bench's default config and return the
/// same `SymbolFixture` shape the ITCH extractor produces. Pinned
/// seed so the test is fully deterministic — any change in the rand
/// stream will surface as a calibration-test diff rather than a
/// flake.
fn run_generator() -> SymbolFixture {
    let cfg = GeneratorConfig {
        seed: 0xCA11_BDA7E,
        ..GeneratorConfig::default()
    };
    let mut adapter = GeneratorAdapter::new(cfg);
    let tickers: Vec<_> = adapter.tickers().to_vec();
    let mut agg = StatsAggregator::new(tickers.iter().copied());
    for d in adapter.directory() {
        agg.apply(&d);
    }
    let mut emitted = 0usize;
    while emitted < GENERATED_EVENTS {
        if let Some(event) = adapter.next_event() {
            agg.apply(&event);
            emitted += 1;
        }
    }
    // GeneratorAdapter emits a single symbol; pull its stats.
    let (_, stats) = agg
        .stats()
        .iter()
        .next()
        .expect("aggregator has one symbol");
    SymbolFixture::from_stats(stats)
}

fn buy_fraction(s: &SymbolFixture) -> f64 {
    let total = s.side_balance.add_buy + s.side_balance.add_sell;
    if total == 0 {
        return 0.0;
    }
    s.side_balance.add_buy as f64 / total as f64
}

/// Removes-per-add: (deletes + replaces) / (adds + add_attr). Approximates
/// the venue's "cancel ratio" — typically slightly above 1.0 because
/// Replace removes an old ref without an explicit Add ahead of itself.
fn removes_per_add(s: &SymbolFixture) -> f64 {
    let adds = s.event_counts.add + s.event_counts.add_attr;
    let removes = s.event_counts.delete + s.event_counts.replace;
    if adds == 0 {
        return 0.0;
    }
    removes as f64 / adds as f64
}

#[test]
fn calibration_report() {
    let reference = load_reference();
    let g = run_generator();

    println!("=== Calibration report: generator vs reference ===");
    println!(
        "Reference adds: {}",
        reference.event_counts.add + reference.event_counts.add_attr
    );
    println!(
        "Generator adds: {}",
        g.event_counts.add + g.event_counts.add_attr
    );
    println!();
    println!("Side balance (buy fraction)");
    println!("  reference: {:.4}", buy_fraction(&reference));
    println!("  generator: {:.4}", buy_fraction(&g));
    println!();
    println!("Removes per add (cancel ratio proxy)");
    println!("  reference: {:.4}", removes_per_add(&reference));
    println!("  generator: {:.4}", removes_per_add(&g));
    println!();
    println!("Add size quantiles");
    print_quantile_table(&reference.add_size.quantiles, &g.add_size.quantiles);
    println!();
    println!("Buy distance below mid (passive bids)");
    print_quantile_table(
        &reference.buy_distance_from_mid.negative.quantiles,
        &g.buy_distance_from_mid.negative.quantiles,
    );
    println!();
    println!("Sell distance above mid (passive asks)");
    print_quantile_table(
        &reference.sell_distance_from_mid.positive.quantiles,
        &g.sell_distance_from_mid.positive.quantiles,
    );
    println!();
    println!("Generator book-tracker diagnostics:");
    println!("  unknown_order_errors:   {}", g.unknown_order_errors);
    println!("  share_underflow_errors: {}", g.share_underflow_errors);
    println!("  new_ref_collision:      {}", g.new_ref_collision_errors);
}

fn print_quantile_table(
    reference: &[melin_bench::calibration::fixture::QuantilePoint],
    generator: &[melin_bench::calibration::fixture::QuantilePoint],
) {
    println!("  q       reference     generator     ratio (g/ref)");
    for (r, g) in reference.iter().zip(generator.iter()) {
        let ratio = if r.value == 0 {
            f64::NAN
        } else {
            g.value as f64 / r.value as f64
        };
        println!(
            "  {:>6.4}  {:>12}  {:>12}  {:>6.2}x",
            r.q, r.value, g.value, ratio
        );
    }
}

#[test]
fn calibration_basics_within_tolerance() {
    let reference = load_reference();
    let g = run_generator();

    // Side balance: generator is 50/50 by design (Bernoulli over a
    // single uniform sample); the reference is typically near 50%
    // with venue-specific drift. Allow 5 absolute percentage points
    // so a small drift in either side doesn't false-positive.
    let ref_buy = buy_fraction(&reference);
    let gen_buy = buy_fraction(&g);
    assert!(
        (gen_buy - ref_buy).abs() < 0.05,
        "buy fraction off: reference {ref_buy:.4}, generator {gen_buy:.4}"
    );

    // Cancel ratio (removes per add). The generator targets ≈0.90
    // cancel + replace in its config; ring-buffer eviction layered on
    // top pushes the empirical default-config ratio higher than the
    // typical real-venue ratio of ~1.07. The assertion only checks
    // the generator hasn't gone wildly off (e.g., emitting cancels
    // for orders it never added).
    let ref_ratio = removes_per_add(&reference);
    let gen_ratio = removes_per_add(&g);
    assert!(
        (0.5..=2.0).contains(&gen_ratio),
        "removes-per-add wildly off: reference {ref_ratio:.4}, generator {gen_ratio:.4}"
    );

    // Replaces should be a non-trivial fraction of removes since the
    // generator config targets cancel_replace_ratio=0.30.
    assert!(
        g.event_counts.replace > 0,
        "generator should emit some replaces"
    );

    // The headline state-machine assertion: the adapter never emits
    // refs the book tracker can't resolve. Any drift here would
    // invalidate every downstream comparison.
    assert_eq!(g.unknown_order_errors, 0);
    assert_eq!(g.share_underflow_errors, 0);
    assert_eq!(g.new_ref_collision_errors, 0);
}
