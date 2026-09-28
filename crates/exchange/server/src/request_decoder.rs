//! Trading-side [`RequestDecoder`] implementation.
//!
//! Owns the bytes -> `melin_ec_protocol::Request` -> `TradingEvent`
//! pipeline. Hides the wire enum behind the [`RequestDecoder`] trait
//! so the server runtime never needs to pattern-match on
//! application-shaped variants.

use melin_app::auth::ClientRole;
use melin_app::decoder::{Decoded, RequestDecoder as RequestDecoderTrait};
use melin_ec_protocol::codec;
use melin_ec_protocol::message::Request;
use melin_ec_protocol::role::{ExchangeRole, RequestCategory};
use melin_ec_trading::trading_event::{TradingEvent, TradingRequest};
use melin_wire_protocol::error::ProtocolError;

// A request the readers cannot hand over whole never reaches the decoder:
// the connection is dropped instead. The widest request body must fit,
// and this is where a change on either side is caught.
const _: () = assert!(
    codec::MAX_REQUEST_BODY <= melin_server_runtime::MAX_REQUEST_BODY,
    "the widest request body must fit one client frame"
);

/// Decoder for the trading wire protocol.
///
/// Zero-sized. The runtime holds it with its role type erased, as an
/// `Arc<dyn ErasedDecoder<...>>`; the server hands it over by value.
#[derive(Debug, Clone, Copy)]
pub struct RequestDecoder;

impl RequestDecoderTrait for RequestDecoder {
    type Event = TradingRequest;
    type Role = ExchangeRole;

    /// The runtime has stripped its tag; the body opens with the request's
    /// kind, then its sequence, which travels on into the event: the
    /// engine's idempotency check reads it from there, in `apply`.
    fn decode(&self, body: &[u8], role: ClientRole<ExchangeRole>) -> Decoded<TradingRequest> {
        let (request_seq, request) = match codec::decode_request_body(body) {
            Ok(pair) => pair,
            Err(e) => return Decoded::DecodeError(protocol_error_reason(&e)),
        };

        // The protocol's access rule decides; this only enforces it.
        let category = request.category();
        if !category.admits(role) {
            return Decoded::PermissionDenied(denial_reason(category));
        }

        // Heartbeats and subscription control are the node's business:
        // never published to the pipeline.
        if category == RequestCategory::Connection {
            return Decoded::Filter;
        }

        Decoded::Permitted(TradingRequest {
            request_seq,
            event: to_trading_event(&request),
        })
    }
}

/// Collapse a typed `ProtocolError` into the static reason carried by
/// `Decoded::DecodeError`. The reader's debug log surfaces this
/// reason; a misbehaving client gets diagnosed without exposing the
/// full error chain.
#[inline]
fn protocol_error_reason(e: &ProtocolError) -> &'static str {
    match e {
        ProtocolError::Truncated => "truncated frame",
        ProtocolError::UnknownTag(_) => "unknown request kind",
        ProtocolError::InvalidField(_) => "invalid field",
        ProtocolError::MessageTooLarge(_) => "message too large",
        ProtocolError::Io(_) => "io error",
    }
}

/// The reason carried by `Decoded::PermissionDenied` for a request of
/// `category` that its connection's role may not send. The reader's debug
/// log surfaces it.
#[inline]
fn denial_reason(category: RequestCategory) -> &'static str {
    match category {
        RequestCategory::Trading => "non-trader attempted trading",
        RequestCategory::FundManagement => "non-custodian attempted fund management",
        RequestCategory::Administration => "non-operator attempted operator command",
        // Every client role may send these today; named for completeness.
        RequestCategory::Connection => "role may not send connection messages",
    }
}

/// Per-variant `Request -> TradingEvent` mapping. Caller must have
/// filtered transport-level frames first; this panics on heartbeats /
/// post-auth handshakes / subscribe frames.
#[inline]
fn to_trading_event(request: &Request) -> TradingEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => TradingEvent::SubmitOrder { symbol, order },
        Request::CancelOrder {
            symbol,
            account,
            order_id,
        } => TradingEvent::CancelOrder {
            symbol,
            account,
            order_id,
        },
        Request::CancelAll { account } => TradingEvent::CancelAll { account },
        Request::AddInstrument { spec } => TradingEvent::AddInstrument { spec },
        Request::Deposit {
            account,
            currency,
            amount,
        } => TradingEvent::Deposit {
            account,
            currency,
            amount,
        },
        Request::Withdraw {
            account,
            currency,
            amount,
        } => TradingEvent::Withdraw {
            account,
            currency,
            amount,
        },
        Request::SetRiskLimits { symbol, limits } => TradingEvent::SetRiskLimits { symbol, limits },
        Request::SetCircuitBreaker { symbol, config } => {
            TradingEvent::SetCircuitBreaker { symbol, config }
        }
        Request::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => TradingEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        },
        Request::SetFeeSchedule { symbol, schedule } => {
            TradingEvent::SetFeeSchedule { symbol, schedule }
        }
        Request::QueryStats => TradingEvent::QueryStats,
        Request::QueryPosition { account } => TradingEvent::QueryPosition { account },
        Request::QueryRequestSeq => TradingEvent::QueryRequestSeq,
        Request::EndOfDay => TradingEvent::EndOfDay,
        Request::DisableInstrument { symbol } => TradingEvent::DisableInstrument { symbol },
        Request::EnableInstrument { symbol } => TradingEvent::EnableInstrument { symbol },
        Request::RemoveInstrument { symbol } => TradingEvent::RemoveInstrument { symbol },
        Request::Heartbeat | Request::Subscribe { .. } => {
            unreachable!("filtered before to_trading_event")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    use melin_app::AppEvent;
    use melin_ec_types::types::*;

    /// Encode a Request into what the runtime hands the decoder: the
    /// body (kind + seq + payload), its framing already stripped.
    fn encode(request: &Request, seq: u64) -> Vec<u8> {
        let mut buf = vec![0u8; codec::MAX_REQUEST_BODY];
        let len = codec::encode_request_body(request, seq, &mut buf).unwrap();
        buf.truncate(len);
        buf
    }

    const OPERATOR: ClientRole<ExchangeRole> = ClientRole::Operator;
    const TRADER: ClientRole<ExchangeRole> = ClientRole::App(ExchangeRole::Trader);
    const CUSTODIAN: ClientRole<ExchangeRole> = ClientRole::App(ExchangeRole::Custodian);
    const READONLY: ClientRole<ExchangeRole> = ClientRole::App(ExchangeRole::ReadOnly);

    /// Decode as the runtime would call it.
    fn decode(body: &[u8], role: ClientRole<ExchangeRole>) -> Decoded<TradingRequest> {
        RequestDecoder.decode(body, role)
    }

    fn order() -> Order {
        Order {
            id: OrderId(1),
            account: AccountId(1),
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            time_in_force: TimeInForce::GTC,
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    #[test]
    fn heartbeat_is_filtered() {
        let bytes = encode(&Request::Heartbeat, 0);
        assert!(matches!(decode(&bytes, TRADER), Decoded::Filter));
    }

    #[test]
    fn subscribe_is_filtered() {
        let bytes = encode(
            &Request::Subscribe {
                symbols: [Symbol(0); 8],
                count: 0,
            },
            0,
        );
        assert!(matches!(decode(&bytes, TRADER), Decoded::Filter));
    }

    #[test]
    fn submit_order_as_trader_is_permitted() {
        let bytes = encode(
            &Request::SubmitOrder {
                symbol: Symbol(1),
                order: order(),
            },
            42,
        );
        match decode(&bytes, TRADER) {
            Decoded::Permitted(TradingRequest { request_seq, event }) => {
                assert_eq!(request_seq, 42, "the frame's sequence rides in the event");
                assert!(matches!(event, TradingEvent::SubmitOrder { .. }));
                // Trading-side query taxonomy: order submission is not a query.
                assert!(!event.is_query());
            }
            other => panic!("expected Permitted, got {:?}", debug_variant(&other)),
        }
    }

    #[test]
    fn submit_order_as_readonly_is_denied() {
        let bytes = encode(
            &Request::SubmitOrder {
                symbol: Symbol(1),
                order: order(),
            },
            0,
        );
        assert!(matches!(
            decode(&bytes, READONLY),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn add_instrument_as_operator_is_permitted() {
        let bytes = encode(
            &Request::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(1),
                    quote: CurrencyId(2),
                },
            },
            7,
        );
        assert!(matches!(decode(&bytes, OPERATOR), Decoded::Permitted(_)));
    }

    #[test]
    fn add_instrument_as_trader_is_denied() {
        let bytes = encode(
            &Request::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(1),
                    quote: CurrencyId(2),
                },
            },
            0,
        );
        assert!(matches!(
            decode(&bytes, TRADER),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn deposit_as_custodian_is_permitted() {
        let bytes = encode(
            &Request::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100,
            },
            3,
        );
        assert!(matches!(decode(&bytes, CUSTODIAN), Decoded::Permitted(_)));
    }

    #[test]
    fn deposit_as_trader_is_denied() {
        let bytes = encode(
            &Request::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100,
            },
            0,
        );
        assert!(matches!(
            decode(&bytes, TRADER),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn query_stats_is_permitted_and_flagged() {
        // QueryStats is an operator-only request: see `Request::category`.
        let bytes = encode(&Request::QueryStats, 1);
        match decode(&bytes, OPERATOR) {
            Decoded::Permitted(request) => {
                assert!(matches!(request.event, TradingEvent::QueryStats));
                assert!(request.is_query());
            }
            other => panic!("expected Permitted, got {:?}", debug_variant(&other)),
        }
    }

    #[test]
    fn malformed_request_yields_decode_error() {
        // A known kind, cut short inside the seq behind it.
        let body = encode(&Request::Heartbeat, 0);
        assert!(matches!(
            decode(&body[..8], TRADER),
            Decoded::DecodeError("truncated frame")
        ));
        // Nothing at all: the runtime hands on an empty body as it is.
        assert!(matches!(
            decode(&[], TRADER),
            Decoded::DecodeError("truncated frame")
        ));
        // A kind this codec does not know, over a well-formed seq.
        let mut unknown = [0u8; 9];
        unknown[0] = 0xFF;
        assert!(matches!(
            decode(&unknown, TRADER),
            Decoded::DecodeError("unknown request kind")
        ));
    }

    /// The full path an access decision takes on a node: a token in the
    /// keys file, the role the runtime carries for it, turned back into an
    /// [`ExchangeRole`] by the runtime's erased decoder, and gated here.
    /// Pins that existing key files keep their meaning and that the
    /// decoder enforces the protocol's rule, one request per category (the
    /// rule itself, per request, is pinned in `melin-ec-protocol`).
    #[test]
    fn each_keys_file_role_may_send_exactly_its_own_category() {
        use base64::Engine;
        use melin_app::auth::AuthorizedKeys;
        use melin_app::decoder::ErasedDecoder;

        let operator_command = encode(&Request::EndOfDay, 1);
        let fund_management = encode(
            &Request::Withdraw {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 5,
            },
            1,
        );
        let trading = encode(
            &Request::CancelAll {
                account: AccountId(1),
            },
            1,
        );
        let heartbeat = encode(&Request::Heartbeat, 1);

        let categories = [
            (&operator_command, RequestCategory::Administration),
            (&fund_management, RequestCategory::FundManagement),
            (&trading, RequestCategory::Trading),
        ];

        let listed = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        // Each token, and whether it may send each of `categories`.
        for (token, permitted) in [
            ("operator", [true, false, false]),
            ("custodian", [false, true, false]),
            ("trader", [false, false, true]),
            ("readonly", [false, false, false]),
        ] {
            let keys = AuthorizedKeys::parse::<ExchangeRole>(&format!("{token} {listed} k\n"))
                .unwrap_or_else(|e| panic!("'{token}' must load: {e}"));
            assert!(RequestDecoder.matches_keys(&keys));
            let role = keys
                .lookup(&[0u8; 32])
                .unwrap()
                .client()
                .unwrap_or_else(|| panic!("'{token}' must be a client role"));

            for ((body, category), permitted) in categories.into_iter().zip(permitted) {
                match RequestDecoder.decode_erased(body, role) {
                    Decoded::Permitted(_) => assert!(permitted, "{token} {category:?}"),
                    Decoded::PermissionDenied(reason) => {
                        assert!(!permitted, "{token} {category:?}");
                        assert_eq!(reason, denial_reason(category));
                    }
                    other => panic!("{token} {category:?}: {}", debug_variant(&other)),
                }
            }
            // Connection-level messages pass whatever the role.
            assert!(matches!(
                RequestDecoder.decode_erased(&heartbeat, role),
                Decoded::Filter
            ));
        }

        // A replication key never gets as far as the decoder.
        let keys =
            AuthorizedKeys::parse::<ExchangeRole>(&format!("replication {listed} k\n")).unwrap();
        assert_eq!(keys.lookup(&[0u8; 32]).unwrap().client(), None);
    }

    fn debug_variant<E: AppEvent>(d: &Decoded<E>) -> &'static str {
        match d {
            Decoded::Filter => "Filter",
            Decoded::Permitted(_) => "Permitted",
            Decoded::PermissionDenied(_) => "PermissionDenied",
            Decoded::DecodeError(_) => "DecodeError",
        }
    }

    // ------------------------------------------------------------------
    // Per-variant `Request -> TradingEvent` mapping checks.
    //
    // The trait tests above only assert `matches!(event,
    // TradingEvent::Foo { .. })`; these go one level deeper and
    // confirm the field-by-field mapping for every variant we
    // currently translate. They call `to_trading_event` directly
    // because asserting "the SubmitOrder symbol came through as
    // Symbol(1)" doesn't need a wire round-trip.
    // ------------------------------------------------------------------

    fn full_order(id: u64, account: u32, side: Side) -> Order {
        Order {
            id: OrderId(id),
            account: AccountId(account),
            side,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(100).unwrap()),
                post_only: false,
            },
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            time_in_force: TimeInForce::GTC,
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        }
    }

    #[test]
    fn maps_submit_order() {
        let req = Request::SubmitOrder {
            symbol: Symbol(1),
            order: full_order(1, 1, Side::Buy),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SubmitOrder { symbol, .. } if symbol == Symbol(1)
        ));
    }

    #[test]
    fn maps_cancel_order() {
        let req = Request::CancelOrder {
            symbol: Symbol(2),
            account: AccountId(5),
            order_id: OrderId(42),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::CancelOrder { symbol, account, order_id }
                if symbol == Symbol(2) && account == AccountId(5) && order_id == OrderId(42)
        ));
    }

    #[test]
    fn maps_cancel_all() {
        let req = Request::CancelAll {
            account: AccountId(7),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::CancelAll { account } if account == AccountId(7)
        ));
    }

    #[test]
    fn maps_deposit() {
        let req = Request::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 1000,
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::Deposit { account, currency, amount }
                if account == AccountId(1) && currency == CurrencyId(2) && amount == 1000
        ));
    }

    #[test]
    fn maps_add_instrument() {
        let spec = InstrumentSpec {
            symbol: Symbol(10),
            base: CurrencyId(1),
            quote: CurrencyId(2),
        };
        let req = Request::AddInstrument { spec };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::AddInstrument { spec: s } if s.symbol == Symbol(10)
        ));
    }

    #[test]
    fn maps_cancel_replace() {
        let req = Request::CancelReplace {
            symbol: Symbol(1),
            account: AccountId(1),
            order_id: OrderId(5),
            new_price: Price(NonZeroU64::new(200).unwrap()),
            new_quantity: Quantity(NonZeroU64::new(50).unwrap()),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::CancelReplace { order_id, .. } if order_id == OrderId(5)
        ));
    }

    #[test]
    fn maps_set_risk_limits() {
        let req = Request::SetRiskLimits {
            symbol: Symbol(1),
            limits: RiskLimits::default(),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SetRiskLimits { symbol, .. } if symbol == Symbol(1)
        ));
    }

    #[test]
    fn maps_set_circuit_breaker() {
        let req = Request::SetCircuitBreaker {
            symbol: Symbol(1),
            config: CircuitBreakerConfig::default(),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SetCircuitBreaker { symbol, .. } if symbol == Symbol(1)
        ));
    }

    #[test]
    fn maps_set_fee_schedule() {
        let req = Request::SetFeeSchedule {
            symbol: Symbol(3),
            schedule: FeeSchedule::default(),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SetFeeSchedule { symbol, .. } if symbol == Symbol(3)
        ));
    }

    #[test]
    fn maps_query_stats() {
        assert!(matches!(
            to_trading_event(&Request::QueryStats),
            TradingEvent::QueryStats
        ));
    }

    #[test]
    #[should_panic(expected = "filtered before to_trading_event")]
    fn heartbeat_panics_if_not_filtered() {
        to_trading_event(&Request::Heartbeat);
    }
}
