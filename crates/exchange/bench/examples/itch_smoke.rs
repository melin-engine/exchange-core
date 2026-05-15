//! Smoke-test the ITCH parser against a real ITCH 5.0 dump. Walks the
//! entire file, tallies decoded events by type, and
//! prints throughput. Validates that:
//!   - frame lengths match the spec for every recognized message,
//!   - no `UnexpectedLength` errors fire,
//!   - the file ends cleanly at a frame boundary,
//!   - the event mix looks plausible (e.g., add-to-delete is roughly
//!     balanced; orders-of-magnitude check, nothing more).
//!
//! Usage:
//!     cargo run --release --example itch_smoke -- <path-to-itch-file>
//!
//! Accepts both raw and `.gz` files based on extension.

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

use melin_bench::calibration::book::{BookTracker, TrackerError};
use melin_bench::calibration::itch::{self, ItchEvent, ItchParser};

fn main() {
    let path = env::args().nth(1).expect("usage: itch_smoke <path>");
    let path = Path::new(&path);

    let mut counts = Counts::default();
    let start = Instant::now();

    if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        let mut parser = itch::open_gz_path(path).expect("open gz");
        walk(&mut parser, &mut counts);
    } else {
        // 8 MiB buffer — raw files are big and sequential reads benefit
        // from larger buffers than the gz path needs.
        let file = File::open(path).expect("open file");
        let reader = BufReader::with_capacity(8 << 20, file);
        let mut parser = ItchParser::new(reader);
        walk(&mut parser, &mut counts);
    }

    let elapsed = start.elapsed();
    let total = counts.total();
    println!("Parsed in {:.2}s", elapsed.as_secs_f64());
    println!("Total decoded events: {total}");
    println!(
        "Throughput: {:.2} M evt/s",
        total as f64 / 1e6 / elapsed.as_secs_f64()
    );
    println!();
    println!("By type:");
    println!("  StockDirectory (R):           {:>12}", counts.stock_dir);
    println!("  AddOrder (A):                 {:>12}", counts.add);
    println!("  AddOrderAttributed (F):       {:>12}", counts.add_attr);
    println!("  OrderExecuted (E):            {:>12}", counts.exec);
    println!("  OrderExecutedWithPrice (C):   {:>12}", counts.exec_price);
    println!("  OrderCancel (X):              {:>12}", counts.cancel);
    println!("  OrderDelete (D):              {:>12}", counts.delete);
    println!("  OrderReplace (U):             {:>12}", counts.replace);
    println!("  HiddenTrade (P):              {:>12}", counts.hidden);
    println!();
    let adds = counts.add + counts.add_attr;
    let removes = counts.delete + counts.replace;
    let execs = counts.exec + counts.exec_price;
    println!("Sanity ratios:");
    println!("  adds:           {adds}");
    println!(
        "  deletes+replace:{removes}  (ratio to adds: {:.3})",
        removes as f64 / adds as f64
    );
    println!(
        "  executes:       {execs}  (ratio to adds: {:.4})",
        execs as f64 / adds as f64
    );
    println!(
        "  partial cancels:{}  (ratio to adds: {:.4})",
        counts.cancel,
        counts.cancel as f64 / adds as f64
    );
    println!();
    println!("Book-tracker diagnostics:");
    println!("  peak live orders:   {}", counts.peak_live_orders);
    println!(
        "  unknown-order errs: {}  (ratio to events: {:.4e})",
        counts.unknown_order,
        counts.unknown_order as f64 / total as f64
    );
    println!("  share-underflow:    {}", counts.share_underflow);
    println!("  new-ref collisions: {}", counts.new_ref_collision);
}

#[derive(Default)]
struct Counts {
    stock_dir: u64,
    add: u64,
    add_attr: u64,
    exec: u64,
    exec_price: u64,
    cancel: u64,
    delete: u64,
    replace: u64,
    hidden: u64,
    // Book-tracker diagnostics. These shouldn't be high on well-formed
    // ITCH; if they are, something is wrong with our state machine.
    unknown_order: u64,
    share_underflow: u64,
    new_ref_collision: u64,
    peak_live_orders: usize,
}

impl Counts {
    fn total(&self) -> u64 {
        self.stock_dir
            + self.add
            + self.add_attr
            + self.exec
            + self.exec_price
            + self.cancel
            + self.delete
            + self.replace
            + self.hidden
    }
}

fn walk<R: std::io::BufRead>(parser: &mut ItchParser<R>, counts: &mut Counts) {
    let mut book = BookTracker::new();
    // Sample peak live count every N events to bound the cost of len()
    // queries; on a HashMap this is O(1) anyway but cuts function-call
    // overhead.
    const PEAK_SAMPLE_EVERY: u64 = 100_000;
    let mut event_idx: u64 = 0;
    loop {
        match parser.next_event() {
            Ok(None) => {
                println!("Final book live-order count: {}", book.live_order_count());
                counts.peak_live_orders = counts.peak_live_orders.max(book.live_order_count());
                return;
            }
            Ok(Some(event)) => {
                let res = apply(&mut book, &event);
                event_idx += 1;
                if event_idx.is_multiple_of(PEAK_SAMPLE_EVERY) {
                    counts.peak_live_orders = counts.peak_live_orders.max(book.live_order_count());
                }
                match event {
                    ItchEvent::StockDirectory { .. } => counts.stock_dir += 1,
                    ItchEvent::AddOrder { .. } => counts.add += 1,
                    ItchEvent::AddOrderAttributed { .. } => counts.add_attr += 1,
                    ItchEvent::OrderExecuted { .. } => counts.exec += 1,
                    ItchEvent::OrderExecutedWithPrice { .. } => counts.exec_price += 1,
                    ItchEvent::OrderCancel { .. } => counts.cancel += 1,
                    ItchEvent::OrderDelete { .. } => counts.delete += 1,
                    ItchEvent::OrderReplace { .. } => counts.replace += 1,
                    ItchEvent::HiddenTrade { .. } => counts.hidden += 1,
                }
                match res {
                    Ok(()) => {}
                    Err(TrackerError::UnknownOrder { .. }) => counts.unknown_order += 1,
                    Err(TrackerError::ShareUnderflow { .. }) => counts.share_underflow += 1,
                    Err(TrackerError::NewRefAlreadyExists { .. }) => counts.new_ref_collision += 1,
                }
            }
            Err(e) => {
                eprintln!("parser error after {} events: {e}", counts.total());
                std::process::exit(1);
            }
        }
    }
}

/// Apply one parsed event to the book tracker. Hidden trades and
/// StockDirectory don't mutate book state — the former is a print of
/// a non-displayed order, the latter is just metadata.
fn apply(book: &mut BookTracker, event: &ItchEvent) -> Result<(), TrackerError> {
    match *event {
        ItchEvent::StockDirectory { .. } | ItchEvent::HiddenTrade { .. } => Ok(()),
        ItchEvent::AddOrder {
            stock_locate,
            order_ref,
            side,
            shares,
            price,
            ..
        }
        | ItchEvent::AddOrderAttributed {
            stock_locate,
            order_ref,
            side,
            shares,
            price,
            ..
        } => book
            .add(order_ref, stock_locate, side, price, shares)
            .map(|_| ()),
        ItchEvent::OrderExecuted {
            order_ref, shares, ..
        }
        | ItchEvent::OrderExecutedWithPrice {
            order_ref, shares, ..
        } => book.execute(order_ref, shares).map(|_| ()),
        ItchEvent::OrderCancel {
            order_ref,
            cancelled_shares,
            ..
        } => book.cancel_partial(order_ref, cancelled_shares).map(|_| ()),
        ItchEvent::OrderDelete { order_ref, .. } => book.delete(order_ref).map(|_| ()),
        ItchEvent::OrderReplace {
            old_order_ref,
            new_order_ref,
            shares,
            price,
            ..
        } => book
            .replace(old_order_ref, new_order_ref, price, shares)
            .map(|_| ()),
    }
}
