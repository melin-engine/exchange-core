//! The exchange side of the harness seam.
//!
//! [`melin_bench_harness::uring`] drives connections without knowing what
//! flows over them. This module supplies the exchange half: order frames
//! out of [`crate::generator::OrderFlowGenerator`], responses decoded with
//! the exchange codec, and the execution-report tallies in
//! [`crate::OutcomeReport`].

use melin_bench_harness::workload::{Outcomes, Workload};
use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

use crate::OutcomeReport;
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
}
