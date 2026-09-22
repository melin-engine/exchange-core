//! Trading-side [`ResponseEncoder`] implementation.
//!
//! Mirror of [`crate::request_decoder::RequestDecoder`] on the
//! outbound path: maps trading-shaped output payloads
//! (`ExecutionReport`, `QueryResponse`) to response bodies, which the
//! runtime frames. Transport-shaped variants (`BatchEnd`, `EngineError`)
//! are handled by the runtime directly and never reach this encoder.

use melin_app::encoder::{Encoded, ResponseEncoder as ResponseEncoderTrait};
use melin_ec_protocol::codec;
use melin_ec_protocol::message::ResponseKind;
use melin_ec_types::types::{ExecutionReport, QueryResponse};

// The runtime hands the encoder a buffer of its own bound and drops a
// response that needs more, logging at `error!`; the widest response body
// must fit, and this is where a change on either side is caught.
const _: () = assert!(
    codec::MAX_RESPONSE_BODY <= melin_server_runtime::MAX_RESPONSE_BODY,
    "the widest response body must fit the node runtime's encode buffer"
);

/// Encoder for the trading wire protocol.
///
/// Zero-sized. The runtime owns an `Arc<dyn ResponseEncoder<...>>`;
/// constructing one is `Arc::new(ResponseEncoder)`.
#[derive(Debug, Clone, Copy)]
pub struct ResponseEncoder;

/// Encode `kind`'s body into `buf` and name it for the runtime.
#[inline]
fn encode_body(kind: &ResponseKind, buf: &mut [u8]) -> Result<Encoded, &'static str> {
    codec::encode_response_body(kind, buf)
        .map(|(tag, len)| Encoded { tag, len })
        .map_err(|_| "encode error")
}

impl ResponseEncoderTrait for ResponseEncoder {
    type Report = ExecutionReport;
    type Query = QueryResponse;

    fn encode_report(
        &self,
        report: &ExecutionReport,
        buf: &mut [u8],
    ) -> Result<Encoded, &'static str> {
        encode_body(&ResponseKind::Report(*report), buf)
    }

    fn encode_query(&self, query: &QueryResponse, buf: &mut [u8]) -> Result<Encoded, &'static str> {
        let kind = match *query {
            QueryResponse::Stats {
                active_connections,
                events_processed,
                journal_sequence,
            } => ResponseKind::StatsHeader {
                active_connections,
                events_processed,
                journal_sequence,
            },
            QueryResponse::Position {
                account,
                balances,
                count,
            } => ResponseKind::PositionSnapshot {
                account,
                balances,
                count,
            },
            QueryResponse::RequestSeqHwm { hwm } => ResponseKind::RequestSeqHwm { hwm },
        };
        encode_body(&kind, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    use melin_ec_types::types::*;

    const SCRATCH: usize = melin_server_runtime::MAX_RESPONSE_BODY;

    /// Put the tag back in front of the body, as the runtime's framing
    /// does, and hand the result to `decode_response`. Keeps the
    /// round-trip asserts below symmetric.
    fn round_trip(encoded: Encoded, body: &[u8]) -> ResponseKind {
        let mut payload = vec![encoded.tag];
        payload.extend_from_slice(&body[..encoded.len]);
        codec::decode_response(&payload).expect("decode")
    }

    fn sample_placed() -> ExecutionReport {
        ExecutionReport::Placed {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Buy,
            price: Price(NonZeroU64::new(100).unwrap()),
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
        }
    }

    #[test]
    fn encodes_report() {
        let mut buf = [0u8; SCRATCH];
        let encoded = ResponseEncoder
            .encode_report(&sample_placed(), &mut buf)
            .unwrap();
        assert!(matches!(
            round_trip(encoded, &buf),
            ResponseKind::Report(ExecutionReport::Placed { order_id, .. })
                if order_id == OrderId(1)
        ));
    }

    /// The tag is the runtime's to write, so it must be an application
    /// tag: the runtime refuses one in the protocol's reserved range.
    #[test]
    fn every_tag_is_an_application_tag() {
        let mut buf = [0u8; SCRATCH];
        let report = ResponseEncoder
            .encode_report(&sample_placed(), &mut buf)
            .unwrap();
        let query = ResponseEncoder
            .encode_query(&QueryResponse::RequestSeqHwm { hwm: 1 }, &mut buf)
            .unwrap();
        for encoded in [report, query] {
            assert!(
                encoded.tag >= melin_wire_protocol::control_codec::FIRST_APP_TAG,
                "{encoded:?}"
            );
        }
    }

    #[test]
    fn encodes_query_stats() {
        let q = QueryResponse::Stats {
            active_connections: 7,
            events_processed: 12345,
            journal_sequence: 999,
        };
        let mut buf = [0u8; SCRATCH];
        let encoded = ResponseEncoder.encode_query(&q, &mut buf).unwrap();
        assert!(matches!(
            round_trip(encoded, &buf),
            ResponseKind::StatsHeader {
                active_connections: 7,
                events_processed: 12345,
                journal_sequence: 999,
            }
        ));
    }

    /// The widest response there is, in the buffer the runtime gives.
    #[test]
    fn encodes_query_position() {
        let balances = [AccountBalance {
            currency: CurrencyId(1),
            free: 100,
            reserved: 0,
        }; 16];
        let q = QueryResponse::Position {
            account: AccountId(42),
            balances,
            count: 16,
        };
        let mut buf = [0u8; SCRATCH];
        let encoded = ResponseEncoder.encode_query(&q, &mut buf).unwrap();
        assert_eq!(encoded.len, codec::MAX_RESPONSE_BODY);
        assert!(matches!(
            round_trip(encoded, &buf),
            ResponseKind::PositionSnapshot { account, count: 16, .. }
                if account == AccountId(42)
        ));
    }

    #[test]
    fn encodes_query_request_seq_hwm() {
        let q = QueryResponse::RequestSeqHwm { hwm: 4242 };
        let mut buf = [0u8; SCRATCH];
        let encoded = ResponseEncoder.encode_query(&q, &mut buf).unwrap();
        assert!(matches!(
            round_trip(encoded, &buf),
            ResponseKind::RequestSeqHwm { hwm: 4242 }
        ));
    }

    // Note: the encoder's `Err` arm exists for codec-level failures
    // (e.g. an `InvalidField` propagated up); the codec does NOT
    // check buffer length and will panic with index-out-of-bounds
    // on an undersized scratch. The runtime always passes a scratch of
    // `melin_server_runtime::MAX_RESPONSE_BODY` bytes, which the
    // assertion at the top of this file proves fits any single response
    // body, so this is a caller-guarantee contract — not something the
    // encoder defends against.
}
