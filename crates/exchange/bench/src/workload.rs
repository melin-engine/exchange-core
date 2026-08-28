//! The exchange side of the harness seam.
//!
//! [`melin_bench_harness::uring`] drives connections without knowing what
//! flows over them, and [`melin_bench_harness::report`] prints a run
//! without knowing what it counted. This module supplies the exchange
//! half of both: order frames out of
//! [`crate::generator::OrderFlowGenerator`], responses decoded with the
//! exchange codec, and the execution-report tallies in [`OutcomeReport`]
//! — including how they render to console and JSON.

use melin_bench_harness::workload::{Outcomes, Workload};
use melin_protocol::codec;
use melin_protocol::message::ResponseKind;
use melin_types::types::{ExecutionReport, RejectReason};

use crate::generator::OrderFlowGenerator;

/// One connection's worth of exchange order flow.
pub(crate) struct ExchangeWorkload {
    flow: OrderFlowGenerator,
    outcomes: OutcomeReport,
}

impl ExchangeWorkload {
    pub(crate) fn new(flow: OrderFlowGenerator) -> Self {
        Self {
            flow,
            outcomes: OutcomeReport::default(),
        }
    }
}

impl Workload for ExchangeWorkload {
    type Response = ResponseKind;
    type Outcomes = OutcomeReport;

    #[inline]
    fn next_frame(&mut self, out: &mut Vec<u8>) {
        self.flow.next_wire_frame(out);
    }

    #[inline]
    fn decode(&self, frame: &[u8]) -> ResponseKind {
        codec::decode_response(frame).expect("decode response")
    }

    /// The server closes every request with a `BatchEnd`, whatever
    /// execution reports preceded it — so it, not the reports, is the
    /// one-per-request completion marker the harness needs.
    #[inline]
    fn completes_request(&self, response: &ResponseKind) -> bool {
        matches!(response, ResponseKind::BatchEnd)
    }

    #[inline]
    fn record(&mut self, response: &ResponseKind) {
        self.outcomes.record(response);
    }

    fn outcomes(&self) -> &OutcomeReport {
        &self.outcomes
    }
}

impl Outcomes for OutcomeReport {
    fn merge(&mut self, other: &Self) {
        OutcomeReport::merge(self, other);
    }

    fn render_console(&self) {
        print_outcome_summary(self);
    }

    /// Per-reason counts are emitted only for reasons that actually
    /// fired, so a clean run's JSON stays small and a consumer reading
    /// `reject_reasons` sees only real signal.
    fn render_json(&self) -> String {
        let mut reasons = String::from("{");
        let mut first = true;
        for (i, (_, name)) in REJECT_REASONS.iter().enumerate() {
            let count = self.reject_reasons[i];
            if count == 0 {
                continue;
            }
            if !first {
                reasons.push(',');
            }
            first = false;
            reasons.push_str(&format!("\"{name}\":{count}"));
        }
        reasons.push('}');
        format!(
            "{{\"batch_ends\":{},\"placed\":{},\"fills\":{},\"cancelled\":{},\"triggered\":{},\"replaced\":{},\"rejected\":{},\"engine_errors\":{},\"server_busy\":{},\"reject_reasons\":{reasons}}}",
            self.batch_ends,
            self.placed,
            self.fills,
            self.cancelled,
            self.triggered,
            self.replaced,
            self.rejected,
            self.engine_errors,
            self.server_busy,
        )
    }
}

/// Stable ordering of [`RejectReason`] variants used as the index space
/// for [`OutcomeReport::reject_reasons`]. Adding a new reject variant is
/// a compile error inside [`reject_reason_index`] until the entry is
/// appended here too — keep the two in sync.
pub(crate) const REJECT_REASONS: &[(RejectReason, &str)] = &[
    (RejectReason::NoLiquidity, "NoLiquidity"),
    (RejectReason::FOKCannotFill, "FOKCannotFill"),
    (RejectReason::InsufficientBalance, "InsufficientBalance"),
    (RejectReason::UnknownAccount, "UnknownAccount"),
    (RejectReason::UnknownSymbol, "UnknownSymbol"),
    (RejectReason::SelfTradePrevented, "SelfTradePrevented"),
    (RejectReason::DuplicateOrderId, "DuplicateOrderId"),
    (RejectReason::ExceedsMaxOrderQty, "ExceedsMaxOrderQty"),
    (RejectReason::ExceedsMaxNotional, "ExceedsMaxNotional"),
    (RejectReason::TradingHalted, "TradingHalted"),
    (RejectReason::OutsidePriceBand, "OutsidePriceBand"),
    (RejectReason::UnknownOrder, "UnknownOrder"),
    (RejectReason::PriceWouldCross, "PriceWouldCross"),
    (RejectReason::PostOnlyWouldCross, "PostOnlyWouldCross"),
    (RejectReason::HasRestingOrders, "HasRestingOrders"),
    (RejectReason::DuplicateRequest, "DuplicateRequest"),
    (RejectReason::ReplicaDisconnected, "ReplicaDisconnected"),
    (RejectReason::InvalidExpiry, "InvalidExpiry"),
    (RejectReason::InstrumentDisabled, "InstrumentDisabled"),
    (RejectReason::ExceedsMaxOpenOrders, "ExceedsMaxOpenOrders"),
    (RejectReason::ExceedsOrderRate, "ExceedsOrderRate"),
    (RejectReason::Superseded, "Superseded"),
];

fn reject_reason_index(reason: RejectReason) -> usize {
    // `RejectReason` is not `#[repr(u8)]`, so the discriminant isn't a
    // stable index. An exhaustive match makes adding a new variant a
    // compile error until both this function and `REJECT_REASONS` above
    // are updated.
    let idx = match reason {
        RejectReason::NoLiquidity => 0,
        RejectReason::FOKCannotFill => 1,
        RejectReason::InsufficientBalance => 2,
        RejectReason::UnknownAccount => 3,
        RejectReason::UnknownSymbol => 4,
        RejectReason::SelfTradePrevented => 5,
        RejectReason::DuplicateOrderId => 6,
        RejectReason::ExceedsMaxOrderQty => 7,
        RejectReason::ExceedsMaxNotional => 8,
        RejectReason::TradingHalted => 9,
        RejectReason::OutsidePriceBand => 10,
        RejectReason::UnknownOrder => 11,
        RejectReason::PriceWouldCross => 12,
        RejectReason::PostOnlyWouldCross => 13,
        RejectReason::HasRestingOrders => 14,
        RejectReason::DuplicateRequest => 15,
        RejectReason::ReplicaDisconnected => 16,
        RejectReason::InvalidExpiry => 17,
        RejectReason::InstrumentDisabled => 18,
        RejectReason::ExceedsMaxOpenOrders => 19,
        RejectReason::ExceedsOrderRate => 20,
        RejectReason::Superseded => 21,
    };
    // Catch silent label/index swaps: an exhaustive match would still
    // type-check if two arms had their integers swapped, but the
    // `REJECT_REASONS` table would then mislabel counts at print time.
    // The existing `reject_reasons_indices_are_unique_and_match_table_length`
    // test calls this for every variant, so a swap explodes there.
    debug_assert_eq!(
        REJECT_REASONS[idx].0, reason,
        "REJECT_REASONS table and reject_reason_index match arms diverged at idx {idx}",
    );
    idx
}

/// Counts of execution-report variants observed by the bench client over
/// the lifetime of a run. Folded across connections and bench threads to
/// surface the rejection ratio in the run summary — without this, a
/// misconfigured run where every order is rejected looks identical to a
/// clean run in the latency histogram.
///
/// Plain `u64` fields (not atomics) because each connection is owned by
/// a single bench thread; merging happens after thread join.
#[derive(Default, Clone)]
pub(crate) struct OutcomeReport {
    /// `BatchEnd` frames received — one per acknowledged request, so
    /// this is the denominator for the rejection ratio.
    pub batch_ends: u64,
    pub placed: u64,
    pub fills: u64,
    pub cancelled: u64,
    pub triggered: u64,
    pub replaced: u64,
    pub instrument_status: u64,
    pub rejected: u64,
    pub engine_errors: u64,
    pub server_busy: u64,
    /// Per-reason rejection counts. Index space defined by
    /// [`REJECT_REASONS`] / [`reject_reason_index`].
    pub reject_reasons: [u64; REJECT_REASONS.len()],
}

impl OutcomeReport {
    /// Increment the counter that matches `response`. Untracked variants
    /// (handshake / market-data / stats frames) are ignored.
    #[inline]
    pub fn record(&mut self, response: &ResponseKind) {
        match response {
            ResponseKind::BatchEnd => self.batch_ends += 1,
            ResponseKind::Report(report) => self.record_execution_report(report),
            ResponseKind::EngineError => self.engine_errors += 1,
            ResponseKind::ServerBusy => self.server_busy += 1,
            // Non-trading frames (Challenge, ServerReady, Heartbeat,
            // AuthFailed, stats/market-data snapshots) — not part of the
            // request/ack accounting.
            _ => {}
        }
    }

    /// Increment the counter for a single execution-report variant.
    /// Used by in-process bench modes (engine, pipeline) which observe
    /// the matching stage's reports directly without going through the
    /// wire `ResponseKind::Report` wrapper.
    #[inline]
    pub fn record_execution_report(&mut self, report: &ExecutionReport) {
        match report {
            ExecutionReport::Placed { .. } => self.placed += 1,
            ExecutionReport::Fill { .. } => self.fills += 1,
            ExecutionReport::Cancelled { .. } => self.cancelled += 1,
            ExecutionReport::Triggered { .. } => self.triggered += 1,
            ExecutionReport::Replaced { .. } => self.replaced += 1,
            ExecutionReport::InstrumentStatusChanged { .. } => self.instrument_status += 1,
            ExecutionReport::Rejected { reason, .. } => {
                self.rejected += 1;
                self.reject_reasons[reject_reason_index(*reason)] += 1;
            }
        }
    }

    pub fn merge(&mut self, other: &OutcomeReport) {
        self.batch_ends += other.batch_ends;
        self.placed += other.placed;
        self.fills += other.fills;
        self.cancelled += other.cancelled;
        self.triggered += other.triggered;
        self.replaced += other.replaced;
        self.instrument_status += other.instrument_status;
        self.rejected += other.rejected;
        self.engine_errors += other.engine_errors;
        self.server_busy += other.server_busy;
        for (a, b) in self
            .reject_reasons
            .iter_mut()
            .zip(other.reject_reasons.iter())
        {
            *a += *b;
        }
    }

    /// Fraction of acknowledged requests that were rejected. Returns
    /// `0.0` when no batches were observed, so callers comparing against
    /// a threshold treat a zero-response run as "no rejections seen"
    /// rather than 100% — a stalled run is a separate failure mode and
    /// is already surfaced by the throughput line.
    pub fn rejection_ratio(&self) -> f64 {
        if self.batch_ends == 0 {
            0.0
        } else {
            self.rejected as f64 / self.batch_ends as f64
        }
    }
}

/// Fail the run with a non-zero exit if more than `max_pct` percent of
/// acknowledged requests were rejected. The CLI default is 50% — the
/// generator naturally produces a few percent of rejections, so the
/// gate targets catastrophic misconfig ("most orders rejected") rather
/// than noise. Lower `max_pct` for production-flow runs where rejections
/// should be near-zero; set it to 100.0 to disable.
pub(crate) fn enforce_rejection_threshold(outcomes: &OutcomeReport, max_pct: f64) {
    let pct = outcomes.rejection_ratio() * 100.0;
    if pct > max_pct {
        eprintln!(
            "error: rejection ratio {pct:.2}% exceeds --max-reject-pct {max_pct:.2}% \
             ({} rejected of {} acknowledged requests). Likely a misconfiguration — \
             check account funding, instrument symbols, and risk limits.",
            outcomes.rejected, outcomes.batch_ends,
        );
        std::process::exit(2);
    }
}

/// Print the outcome summary: acknowledged request count, rejection
/// ratio, and the top reject reasons. Surfaces misconfigured runs (e.g.
/// every order rejected with `InsufficientBalance`) that the latency
/// histogram would otherwise hide.
fn print_outcome_summary(outcomes: &OutcomeReport) {
    println!();
    println!("  Outcomes ({} acknowledged requests)", outcomes.batch_ends);
    if outcomes.batch_ends == 0 {
        println!("    (no responses observed — bench may have stalled before any ack)");
        return;
    }
    let total = outcomes.batch_ends as f64;
    let pct = |n: u64| n as f64 / total * 100.0;
    println!(
        "    rejected:  {:>10} ({:.2}%)",
        outcomes.rejected,
        pct(outcomes.rejected)
    );
    println!("    placed:    {:>10}", outcomes.placed);
    println!("    fills:     {:>10}", outcomes.fills);
    println!("    cancelled: {:>10}", outcomes.cancelled);
    if outcomes.triggered > 0 {
        println!("    triggered: {:>10}", outcomes.triggered);
    }
    if outcomes.replaced > 0 {
        println!("    replaced:  {:>10}", outcomes.replaced);
    }
    if outcomes.engine_errors > 0 {
        println!(
            "    engine errors: {} ({:.2}%)",
            outcomes.engine_errors,
            pct(outcomes.engine_errors)
        );
    }
    if outcomes.server_busy > 0 {
        println!(
            "    server-busy:   {} ({:.2}%)",
            outcomes.server_busy,
            pct(outcomes.server_busy)
        );
    }
    if outcomes.rejected > 0 {
        let mut reasons: Vec<(&str, u64)> = REJECT_REASONS
            .iter()
            .enumerate()
            .filter_map(|(i, (_, name))| {
                let count = outcomes.reject_reasons[i];
                if count > 0 {
                    Some((*name, count))
                } else {
                    None
                }
            })
            .collect();
        // Descending by count so the dominant reason is on top.
        reasons.sort_by_key(|r| std::cmp::Reverse(r.1));
        println!("    reject reasons:");
        for (name, count) in reasons.iter().take(5) {
            println!("      {name}: {count} ({:.2}%)", pct(*count));
        }
    }
}

#[cfg(test)]
mod outcome_report_tests {
    use std::num::NonZeroU64;

    use melin_types::types::{AccountId, OrderId, Price, Quantity, Side, Symbol};

    use super::*;

    fn dummy_order() -> (OrderId, Symbol, AccountId) {
        (OrderId(1), Symbol(0), AccountId(7))
    }

    fn one_qty() -> Quantity {
        Quantity(NonZeroU64::new(1).unwrap())
    }

    fn one_price() -> Price {
        Price(NonZeroU64::new(100).unwrap())
    }

    #[test]
    fn records_each_variant_into_the_right_bucket() {
        let (oid, sym, acc) = dummy_order();
        let mut r = OutcomeReport::default();
        r.record(&ResponseKind::BatchEnd);
        r.record(&ResponseKind::Report(ExecutionReport::Placed {
            order_id: oid,
            symbol: sym,
            account: acc,
            side: Side::Buy,
            price: one_price(),
            quantity: one_qty(),
        }));
        r.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::InsufficientBalance,
        }));
        r.record(&ResponseKind::EngineError);
        r.record(&ResponseKind::ServerBusy);
        // Heartbeat is intentionally untracked.
        r.record(&ResponseKind::Heartbeat);

        assert_eq!(r.batch_ends, 1);
        assert_eq!(r.placed, 1);
        assert_eq!(r.rejected, 1);
        assert_eq!(r.engine_errors, 1);
        assert_eq!(r.server_busy, 1);
        assert_eq!(
            r.reject_reasons[reject_reason_index(RejectReason::InsufficientBalance)],
            1
        );
    }

    #[test]
    fn merge_sums_all_fields_including_reason_buckets() {
        let (oid, sym, acc) = dummy_order();
        let mut a = OutcomeReport::default();
        a.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::NoLiquidity,
        }));
        a.record(&ResponseKind::BatchEnd);

        let mut b = OutcomeReport::default();
        b.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::NoLiquidity,
        }));
        b.record(&ResponseKind::BatchEnd);
        b.record(&ResponseKind::BatchEnd);

        a.merge(&b);
        assert_eq!(a.batch_ends, 3);
        assert_eq!(a.rejected, 2);
        assert_eq!(
            a.reject_reasons[reject_reason_index(RejectReason::NoLiquidity)],
            2
        );
    }

    #[test]
    fn rejection_ratio_is_zero_when_no_batches_observed() {
        let r = OutcomeReport::default();
        // A stalled run with zero responses returns 0.0 rather than NaN
        // or 1.0 — distinct failure mode, surfaced by throughput, not by
        // the threshold check.
        assert_eq!(r.rejection_ratio(), 0.0);
    }

    #[test]
    fn rejection_ratio_divides_rejected_by_batch_ends() {
        let r = OutcomeReport {
            batch_ends: 1000,
            rejected: 25,
            ..OutcomeReport::default()
        };
        assert!((r.rejection_ratio() - 0.025).abs() < 1e-9);
    }

    #[test]
    fn record_execution_report_and_record_agree_on_report_variants() {
        // Engine and pipeline modes call `record_execution_report`
        // directly; the network bench reaches it via `record` ->
        // `ResponseKind::Report(_)`. Both paths must produce identical
        // counter state for the same input.
        let (oid, sym, acc) = dummy_order();
        let rep = ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::ExceedsMaxOrderQty,
        };

        let mut via_direct = OutcomeReport::default();
        via_direct.record_execution_report(&rep);

        let mut via_wire = OutcomeReport::default();
        via_wire.record(&ResponseKind::Report(rep));

        assert_eq!(via_direct.rejected, via_wire.rejected);
        assert_eq!(via_direct.reject_reasons, via_wire.reject_reasons);
    }

    #[test]
    fn reject_reasons_indices_are_unique_and_match_table_length() {
        // Sanity check: REJECT_REASONS and reject_reason_index must stay
        // in lockstep. Indices must cover [0, len) without collisions.
        let mut seen = vec![false; REJECT_REASONS.len()];
        for (reason, _) in REJECT_REASONS {
            let idx = reject_reason_index(*reason);
            assert!(idx < REJECT_REASONS.len(), "index {idx} out of range");
            assert!(!seen[idx], "duplicate index {idx} for {reason:?}");
            seen[idx] = true;
        }
        assert!(seen.iter().all(|b| *b), "missing variant in REJECT_REASONS");
    }
}
