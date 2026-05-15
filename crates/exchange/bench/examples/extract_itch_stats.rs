//! Extract reference calibration statistics from an ITCH 5.0 dump and
//! write the aggregated summary to a JSON fixture.
//!
//! Usage:
//!     cargo run --release --example extract_itch_stats -- \
//!         <itch-path> <out-fixture.json> <date> <ticker1> [ticker2 ...]
//!
//! `date` is recorded in fixture metadata (informational). Tickers are
//! padded to 8 ASCII bytes with trailing spaces to match ITCH's wire
//! format. Accepts raw or gzipped (`.gz`) ITCH files.
//!
//! The output fixture contains only quantile-reduced summary scalars —
//! no raw messages, no full histograms. See
//! `crates/exchange/bench/src/calibration/fixture.rs` for the licensing
//! bar.

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

use melin_bench::calibration::fixture::{
    FixtureMetadata, ReferenceFixture, SymbolFixture, ticker_key,
};
use melin_bench::calibration::itch::{self, ItchParser};
use melin_bench::calibration::stats::StatsAggregator;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 4 {
        eprintln!(
            "usage: extract_itch_stats <itch-path> <out-fixture.json> <date> <ticker1> [ticker2 ...]"
        );
        eprintln!(
            "       attribution can be set via MELIN_FIXTURE_ATTRIBUTION (defaults to empty)"
        );
        std::process::exit(2);
    }
    let itch_path = Path::new(&args[0]);
    let out_path = Path::new(&args[1]);
    let date = &args[2];
    let attribution = env::var("MELIN_FIXTURE_ATTRIBUTION").unwrap_or_default();
    let tickers: Vec<[u8; 8]> = args[3..].iter().map(|t| pad_ticker(t)).collect();

    eprintln!(
        "Extracting reference stats for {} ticker(s) from {}",
        tickers.len(),
        itch_path.display()
    );
    for t in &tickers {
        eprintln!("  - {}", ticker_key(t));
    }

    let mut agg = StatsAggregator::new(tickers.iter().copied());
    let start = Instant::now();

    let total_events = if itch_path.extension().and_then(|s| s.to_str()) == Some("gz") {
        let mut parser = itch::open_gz_path(itch_path).expect("open gz");
        drive(&mut parser, &mut agg)
    } else {
        // 8 MiB buffer — matches itch_smoke; raw files are big and
        // sequential reads benefit from a large buffer.
        let file = File::open(itch_path).expect("open file");
        let reader = BufReader::with_capacity(8 << 20, file);
        let mut parser = ItchParser::new(reader);
        drive(&mut parser, &mut agg)
    };
    let elapsed = start.elapsed();
    eprintln!(
        "Parsed {total_events} events in {:.2}s ({:.2} M/s)",
        elapsed.as_secs_f64(),
        total_events as f64 / 1e6 / elapsed.as_secs_f64()
    );

    let mut symbols = BTreeMap::new();
    for (stock, stats) in agg.stats() {
        let key = ticker_key(stock);
        let fixture = SymbolFixture::from_stats(stats);
        // Sanity log so an operator running this can spot a missing
        // symbol immediately (e.g., the ticker didn't trade that day).
        eprintln!(
            "  {key}: adds={}, deletes={}, executes={}, hidden={}",
            fixture.event_counts.add + fixture.event_counts.add_attr,
            fixture.event_counts.delete,
            fixture.event_counts.exec + fixture.event_counts.exec_with_price,
            fixture.event_counts.hidden_trade,
        );
        symbols.insert(key, fixture);
    }

    let fixture = ReferenceFixture {
        metadata: FixtureMetadata::new("ITCH 5.0", date, &attribution),
        symbols,
    };
    let json = serde_json::to_string_pretty(&fixture).expect("serialize fixture");
    std::fs::write(out_path, json).expect("write fixture");
    eprintln!("Wrote fixture to {}", out_path.display());
}

fn drive<R: std::io::BufRead>(parser: &mut ItchParser<R>, agg: &mut StatsAggregator) -> u64 {
    let mut total: u64 = 0;
    loop {
        match parser.next_event() {
            Ok(None) => return total,
            Ok(Some(event)) => {
                agg.apply(&event);
                total += 1;
            }
            Err(e) => {
                eprintln!("parser error after {total} events: {e}");
                std::process::exit(1);
            }
        }
    }
}

fn pad_ticker(s: &str) -> [u8; 8] {
    let mut out = [b' '; 8];
    let bytes = s.as_bytes();
    let n = bytes.len().min(8);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}
