//! Minimal benchmark for the journal writer.
//!
//! Drives `flush_batch_sync` directly — encode a batch into the writer's
//! buffer, then one `pwrite + fdatasync` — without any pipeline or
//! matching engine overhead. Useful to isolate the raw disk cost of a
//! batch from the sequencing work in front of it.
//!
//! Usage:
//!     cargo run --release -p melin-bench --bin journal_writer_bench -- [OPTIONS]
//!
//! Options:
//!     --events <N>               Events to write (default: 1_000_000)
//!     --batch-size <N>           Events per fsync batch (default: 1_024)

use clap::Parser;
use std::num::NonZero;
use std::path::Path;
use std::time::Instant;

use melin_journal::BufferedWriter;
use melin_journal::JournalEvent;
use melin_journal::JournalWrite;
use melin_trading::trading_event::TradingEvent;

#[derive(Parser)]
struct Args {
    /// Total events to write.
    #[arg(long, default_value_t = 1_000_000)]
    events: usize,

    /// Events per fsync batch.
    #[arg(long, default_value_t = 1_024)]
    batch_size: usize,
}

fn main() {
    let args = Args::parse();

    println!("=== Journal Writer Benchmark ===");
    println!("Events: {}", args.events);
    println!("Batch size: {}", args.batch_size);
    println!();

    let journal_path = std::path::PathBuf::from("/tmp/journal_writer_bench.journal");
    // A missing file is the expected state on a fresh run.
    let _ = std::fs::remove_file(&journal_path);

    let writer = BufferedWriter::create(&journal_path).expect("create journal");
    run_sync_mode(writer, args.events, args.batch_size, &journal_path);
}

/// Build a `SubmitOrder` event for slot `i`. Alternates Buy/Sell so the
/// generated stream is not trivially compressible.
fn make_event(i: usize) -> JournalEvent<TradingEvent> {
    let nz = |v: u64| NonZero::new(v).expect("non-zero");
    let order_id = melin_types::types::OrderId((i as u64) + 1);
    let side = if i.is_multiple_of(2) {
        melin_types::types::Side::Buy
    } else {
        melin_types::types::Side::Sell
    };
    JournalEvent::App(TradingEvent::SubmitOrder {
        symbol: melin_types::types::Symbol(1),
        order: melin_types::types::Order {
            id: order_id,
            account: melin_types::types::AccountId(1),
            side,
            order_type: melin_types::types::OrderType::Limit {
                price: melin_types::types::Price(nz(100)),
                post_only: false,
            },
            time_in_force: melin_types::types::TimeInForce::GTC,
            quantity: melin_types::types::Quantity(nz(1)),
            stp: melin_types::types::SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
    })
}

fn report(num_events: usize, elapsed_us: u128, journal_path: &Path) {
    let throughput = (num_events as f64 * 1_000_000.0) / elapsed_us as f64;
    println!("  Events: {}", num_events);
    println!("  Time: {} us", elapsed_us);
    println!("  Throughput: {:.2} events/sec", throughput);
    println!(
        "  Latency: {:.2} us/event",
        elapsed_us as f64 / num_events as f64
    );
    println!();
    if let Ok(metadata) = std::fs::metadata(journal_path) {
        let size_bytes = metadata.len();
        println!(
            "  Journal file size: {} bytes ({:.2} MB)",
            size_bytes,
            size_bytes as f64 / 1_048_576.0
        );
    }
}

/// `flush_batch_sync` path — encodes a batch into the writer's internal
/// buffer, then issues a single sync. Same path the journal's disk
/// thread runs per batch in production.
fn run_sync_mode<W: JournalWrite<TradingEvent>>(
    mut writer: W,
    num_events: usize,
    batch_size: usize,
    journal_path: &Path,
) {
    println!("Measurement phase...");
    let start = Instant::now();

    let num_batches = num_events.div_ceil(batch_size);
    let mut events_written = 0;
    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, num_events);
        for i in batch_start..batch_end {
            let event = make_event(i);
            writer
                .batch_append_with_ts(&event, 0, 0, 0)
                .expect("batch_append_with_ts");
            events_written += 1;
            if events_written % 10_000 == 0 {
                println!("  Written {} events", events_written);
            }
        }
        writer.flush_batch_sync().expect("sync");
    }

    report(num_events, start.elapsed().as_micros(), journal_path);
}

// Use jemalloc for better performance.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
