//! `Application` impl for the trading engine.
//!
//! `melin-ec` owns the matching domain (`Exchange`) and knows nothing
//! about the LMAX transport pipeline. The transport's `Application`
//! contract lives in `melin-app`, and `melin-ec-server` is what wires the
//! two together — so the trait impl lives here, on a thin newtype around
//! `Exchange` that satisfies the orphan rule.
//!
//! The newtype is transparent: `Deref`/`DerefMut` forward every non-trait
//! call to the inner `Exchange`, so callers that need direct engine
//! methods (`set_max_orders_per_second`, `add_instrument`, etc.) keep
//! their existing call sites unchanged.

use std::io::{self, Read, Write};
use std::ops::{Deref, DerefMut};

use melin_app::{Application, ApplyCtx, RejectReason as TransportRejectReason};
use melin_ec::exchange::Exchange;
use melin_ec::snapshot as engine_snapshot;
use melin_ec_trading::trading_event::{TradingEvent, TradingRequest};
use melin_ec_types::types::{
    AccountId, ExecutionReport, OrderId, QueryResponse, RejectReason as EngineRejectReason, Symbol,
};

// Hot-path size budget. Disruptor slots are copied by value on every
// publish/consume — growing these silently would tax cache footprint
// across the whole pipeline. A prior review caught `ExecutionReport`
// ballooning from 64 B → 392 B via an inlined `Position` variant; these
// assertions would have failed at compile time and tripped CI.
// Numbers match the layout on x86_64 Linux; bump deliberately if a
// genuine field addition requires it.
//
// Forced to 128 by `#[repr(align(64))]` on `InputSlot` itself (natural
// layout is 104 B without `latency-trace`, 120 B with it). The alignment
// attribute rounds either configuration up to two cache lines, so the
// production footprint stays constant whether trace timestamps are
// included or not — the assertion no longer needs a cfg-gate.
const _: () =
    assert!(size_of::<melin_transport_core::pipeline::InputSlot<TradingRequest>>() == 128);
// Bumped from 416 → 424 (one extra u64) when `OutputSlot.wire_seq` was
// added so the response stage's durability gate can compare against
// replica metrics in wire-seq space rather than the unsound local-vs-wire
// mix that previously let the gate open on un-replicated events on a
// recovered primary. Correctness > footprint here.
#[cfg(not(feature = "latency-trace"))]
const _: () = assert!(
    size_of::<melin_transport_core::pipeline::OutputSlot<ExecutionReport, QueryResponse>>() == 424
);
// The request sequence rides inside the event since the sequencer stopped
// carrying it in the slot: eight bytes moved, none added.
const _: () = assert!(size_of::<melin_journal::JournalEvent<TradingRequest>>() == 72);
const _: () = assert!(size_of::<ExecutionReport>() == 64);

/// Transparent newtype around [`Exchange`] that carries the
/// `Application` trait impl. Exists solely so the impl can live in
/// `melin-ec-server` (the wiring crate) without violating the orphan rule —
/// neither `Application` (in `melin-app`) nor `Exchange` (in
/// `melin-ec`) is local to `melin-ec-server`, but `ServerApp` is.
///
/// The inner field is `pub` because benches and tests construct an
/// `Exchange` directly and wrap it; making the wrap explicit at every
/// construction site is cheaper than introducing a parallel set of
/// constructors here.
pub struct ServerApp(pub Exchange);

/// The state every node starts from: an empty engine with production
/// capacity reserved and its pages touched. The runtime builds one of
/// these on every node and then seeds a genesis into it, replays a
/// journal into it, or applies a primary's stream to it — so reserving
/// here, before the first event, is what keeps growth and page faults off
/// the matching thread whichever way the node comes up. A snapshot
/// restore builds the same shape by another route (see `restore`).
///
/// Holds no state, and nothing local to the node: every node's history
/// starts from the same empty engine, as `Application` requires.
///
/// A heavy constructor: it allocates and touches over a hundred
/// megabytes. It is for the runtime, not for test fixtures — a test
/// wraps `Exchange::new()` instead.
impl Default for ServerApp {
    fn default() -> Self {
        let mut exchange = Exchange::with_capacity();
        exchange.prefault();
        ServerApp(exchange)
    }
}

impl Deref for ServerApp {
    type Target = Exchange;

    #[inline]
    fn deref(&self) -> &Exchange {
        &self.0
    }
}

impl DerefMut for ServerApp {
    #[inline]
    fn deref_mut(&mut self) -> &mut Exchange {
        &mut self.0
    }
}

impl Application for ServerApp {
    type Event = TradingRequest;
    type Report = ExecutionReport;
    type QueryResponse = QueryResponse;
    /// Account and instrument counts to reserve for — see
    /// [`crate::startup::ExchangeSizing`].
    type Sizing = crate::startup::ExchangeSizing;

    /// Schema version for the snapshot payload. Tracks the underlying
    /// `snapshot` module's `PAYLOAD_VERSION` — any change there forces a
    /// bump here too, surfaced through the transport-owned frame.
    const APP_VERSION: u16 = engine_snapshot::PAYLOAD_VERSION;

    /// The idempotency check, then a thin dispatcher over `TradingEvent`.
    /// Marked `#[inline]` so the matching stage's monomorphised hot loop
    /// can see through to each concrete `Exchange` method: the inner
    /// methods (`execute`, `cancel`, …) own the real work and keep their
    /// own inlining attrs.
    #[inline]
    fn apply(
        &mut self,
        request: Self::Event,
        ctx: &ApplyCtx,
        out: &mut Vec<Self::Report>,
    ) -> Option<Self::QueryResponse> {
        let TradingRequest { request_seq, event } = request;

        // A repeated request is refused before it touches any engine
        // state, the event-timestamp stash below included. The sequencer
        // hands every event to `apply` — live, on replay, on a replica and
        // in the shadow copy — and this one check is what keeps them all
        // refusing the same ones. What a duplicate does not skip is the
        // clock: the sequencer's dispatch ticks the scheduler to the
        // event's timestamp before calling `apply`, on every path alike,
        // so due work (an expiry, say) fires on a refused event as on an
        // accepted one. Queries are exempt: they change nothing, are never
        // journaled, and a client resynchronising its counter sends one
        // first. Internal events carry key 0, which the engine exempts.
        if !event.is_query() && !self.0.check_request_seq(ctx.key_hash, request_seq) {
            out.push(rejected(&event, EngineRejectReason::DuplicateRequest));
            return None;
        }

        // Stash the journaled event timestamp so per-event methods
        // (`execute` and friends) can read a deterministic clock for the
        // SEC-04 rate limiter without taking a `now_ns` parameter. Set
        // unconditionally so the value reflects exactly the event being
        // applied — no risk of reading a stale stamp from an earlier event.
        self.0.set_current_event_ts_ns(ctx.now_ns);
        match event {
            TradingEvent::AddInstrument { spec } => {
                self.0.add_instrument(spec);
                None
            }
            TradingEvent::Deposit {
                account,
                currency,
                amount,
            } => {
                self.0.deposit(account, currency, amount);
                None
            }
            TradingEvent::SubmitOrder { symbol, order } => {
                self.0.execute(symbol, order, out);
                None
            }
            TradingEvent::CancelOrder {
                symbol,
                account,
                order_id,
            } => {
                self.0.cancel(symbol, account, order_id, out);
                None
            }
            TradingEvent::SetRiskLimits { symbol, limits } => {
                self.0.set_risk_limits(symbol, limits);
                None
            }
            TradingEvent::CancelAll { account } => {
                self.0.cancel_all(account, out);
                None
            }
            TradingEvent::SetCircuitBreaker { symbol, config } => {
                self.0.set_circuit_breaker(symbol, config);
                None
            }
            TradingEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                self.0
                    .cancel_replace(symbol, account, order_id, new_price, new_quantity, out);
                None
            }
            TradingEvent::SetFeeSchedule { symbol, schedule } => {
                self.0.set_fee_schedule(symbol, schedule, out);
                None
            }
            TradingEvent::ProvisionAccount { account, amount } => {
                self.0.provision_account(account, amount);
                None
            }
            TradingEvent::Withdraw {
                account,
                currency,
                amount,
            } => {
                // The engine reports a refused withdrawal as a `Result`;
                // the client hears of it as a rejection like any other.
                if let Err(reason) = self.0.withdraw(account, currency, amount) {
                    out.push(rejected(&event, reason));
                }
                None
            }
            TradingEvent::EndOfDay => {
                self.0.end_of_day(out);
                None
            }
            TradingEvent::DisableInstrument { symbol } => {
                self.0.disable_instrument(symbol, out);
                None
            }
            TradingEvent::EnableInstrument { symbol } => {
                self.0.enable_instrument(symbol, out);
                None
            }
            TradingEvent::RemoveInstrument { symbol } => {
                self.0.remove_instrument(symbol, out);
                None
            }
            TradingEvent::QueryStats => {
                // Read-only query: the transport owns the counters, so
                // the app synthesises the report directly from the
                // `ApplyCtx` it was handed. No `Exchange` state touched.
                Some(QueryResponse::Stats {
                    active_connections: ctx.active_connections,
                    events_processed: ctx.events_processed,
                    journal_sequence: ctx.journal_sequence.get(),
                })
            }
            TradingEvent::QueryPosition { account } => {
                let (balances, count) = self.0.accounts().balances_for(account);
                Some(QueryResponse::Position {
                    account,
                    balances,
                    count,
                })
            }
            TradingEvent::QueryRequestSeq => {
                // Self-introspection: read the idempotency high-water
                // mark for the calling connection's key (transport-
                // supplied via `ApplyCtx`). The event itself carries no
                // identity, so a client cannot ask about other keys.
                Some(QueryResponse::RequestSeqHwm {
                    hwm: self.0.request_seq_hwm(ctx.key_hash),
                })
            }
            TradingEvent::SetAccountLimits {
                max_open_orders_per_account,
                max_orders_per_second,
                max_orders_burst,
            } => {
                // Journaled on every promotion, so usually a re-apply of
                // the values already in force — which both setters leave
                // unchanged (the limiter keeps its buckets unless the
                // rate or burst actually changes).
                self.0
                    .set_max_open_orders_per_account(max_open_orders_per_account);
                self.0
                    .set_max_orders_per_second(max_orders_per_second, max_orders_burst);
                None
            }
        }
    }

    #[inline]
    fn tick(&mut self, now_ns: u64, out: &mut Vec<Self::Report>) {
        self.0.drain_due_scheduled_tasks(now_ns, out);
    }

    /// Reserve what only the node knows the size of: the balance map and,
    /// past its built-in capacity, the per-account maps, from
    /// `--accounts` and `--instruments`. Everything else is reserved and
    /// pre-faulted by `Default` and `restore`. The runtime calls this on
    /// a genesis instance before it replays a journal into it, and again
    /// before the instance serves, so a restored engine — whose balance
    /// map is sized to its snapshot — gets its room here.
    fn prefault(&mut self, sizing: &Self::Sizing) {
        self.0
            .reserve_for_accounts(sizing.accounts as usize, sizing.instruments as usize);
    }

    /// `Exchange` exposes an in-memory `clone_via_snapshot` that skips
    /// the byte serialisation — faster than the default
    /// serialise-then-deserialise path. Keep the optimisation for the
    /// shadow-snapshot stage.
    fn clone_via_snapshot(&self) -> std::io::Result<Self> {
        Ok(ServerApp(Exchange::clone_via_snapshot(&self.0)))
    }

    /// A rejection the sequencer decided on its own, before `apply` saw
    /// the event: today only a write refused while the node is halted.
    fn build_reject(request: &Self::Event, reason: TransportRejectReason) -> Self::Report {
        let engine_reason = match reason {
            TransportRejectReason::ReplicaDisconnected => EngineRejectReason::ReplicaDisconnected,
        };
        rejected(&request.event, engine_reason)
    }

    /// Writes the engine payload bytes verbatim. The transport stores
    /// `APP_VERSION` in its frame and rejects mismatching files before
    /// `restore` is ever called, so duplicating the version in the
    /// payload would be unreachable. If multi-version migration ever
    /// lands, drop the transport-side `APP_VERSION` check and reintroduce
    /// an in-payload version prefix here.
    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let bytes = engine_snapshot::encode_exchange_payload(&self.0);
        w.write_all(&bytes)
    }

    /// The decoder rebuilds the engine production-sized and pre-faulted,
    /// the same shape `Default` produces, so a restored engine is as ready
    /// to serve as a fresh one.
    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut bytes = Vec::new();
        r.read_to_end(&mut bytes)?;
        engine_snapshot::decode_exchange_payload(&bytes)
            .map(ServerApp)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// The rejection report for `event`, naming what the event named: its
/// order, symbol and account where it carries them, zero where it does
/// not. One shape for every rejection decided outside the matching
/// methods — a duplicate, a halted node, a refused withdrawal — so a
/// client reads them all the same way.
fn rejected(event: &TradingEvent, reason: EngineRejectReason) -> ExecutionReport {
    ExecutionReport::Rejected {
        order_id: extract_order_id(event),
        symbol: extract_symbol(event),
        account: extract_account_id(event),
        reason,
    }
}

/// Order ID attached to reject reports, or `OrderId(0)` if the variant
/// does not carry one.
fn extract_order_id(event: &TradingEvent) -> OrderId {
    match event {
        TradingEvent::SubmitOrder { order, .. } => order.id,
        TradingEvent::CancelOrder { order_id, .. }
        | TradingEvent::CancelReplace { order_id, .. } => *order_id,
        _ => OrderId(0),
    }
}

fn extract_account_id(event: &TradingEvent) -> AccountId {
    match event {
        TradingEvent::SubmitOrder { order, .. } => order.account,
        TradingEvent::CancelOrder { account, .. }
        | TradingEvent::CancelAll { account }
        | TradingEvent::CancelReplace { account, .. }
        | TradingEvent::Deposit { account, .. }
        | TradingEvent::Withdraw { account, .. }
        | TradingEvent::ProvisionAccount { account, .. }
        | TradingEvent::QueryPosition { account } => *account,
        _ => AccountId(0),
    }
}

fn extract_symbol(event: &TradingEvent) -> Symbol {
    match event {
        TradingEvent::SubmitOrder { symbol, .. }
        | TradingEvent::CancelOrder { symbol, .. }
        | TradingEvent::CancelReplace { symbol, .. }
        | TradingEvent::SetRiskLimits { symbol, .. }
        | TradingEvent::SetCircuitBreaker { symbol, .. }
        | TradingEvent::SetFeeSchedule { symbol, .. }
        | TradingEvent::DisableInstrument { symbol }
        | TradingEvent::EnableInstrument { symbol }
        | TradingEvent::RemoveInstrument { symbol } => *symbol,
        _ => Symbol(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;
    use std::num::NonZeroU64;

    use melin_ec_types::types::{
        CurrencyId, InstrumentSpec, Order, OrderType, Price, Quantity, SelfTradeProtection, Side,
        TimeInForce,
    };

    fn price(p: u64) -> Price {
        Price(NonZeroU64::new(p).unwrap())
    }
    fn qty(q: u64) -> Quantity {
        Quantity(NonZeroU64::new(q).unwrap())
    }

    /// A freshly-constructed `ServerApp` with one registered instrument
    /// and a deposited account. Enough to exercise the full `apply` path.
    fn seeded_app() -> ServerApp {
        let mut ex = Exchange::new();
        ex.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(1),
            quote: CurrencyId(2),
        });
        ex.deposit(AccountId(1), CurrencyId(2), 1_000_000);
        ServerApp(ex)
    }

    /// The context the sequencer would hand `apply` for an event
    /// submitted under `key_hash`; the advisory counters are zero.
    fn ctx(key_hash: u64) -> ApplyCtx {
        ApplyCtx {
            now_ns: 0,
            journal_sequence: melin_app::WireSeq::new(0),
            active_connections: 0,
            events_processed: 0,
            key_hash,
        }
    }

    /// Apply `event` as the node itself would journal it: key 0, no
    /// sequence.
    fn apply_internal(
        app: &mut ServerApp,
        event: TradingEvent,
        out: &mut Vec<ExecutionReport>,
    ) -> Option<QueryResponse> {
        <ServerApp as Application>::apply(app, TradingRequest::internal(event), &ctx(0), out)
    }

    /// Apply `event` as a client would submit it: under its key, with the
    /// sequence it stamped on the request.
    fn apply_from(
        app: &mut ServerApp,
        key_hash: u64,
        request_seq: u64,
        event: TradingEvent,
        out: &mut Vec<ExecutionReport>,
    ) -> Option<QueryResponse> {
        let request = TradingRequest { request_seq, event };
        <ServerApp as Application>::apply(app, request, &ctx(key_hash), out)
    }

    fn deposit(account: u32, amount: u64) -> TradingEvent {
        TradingEvent::Deposit {
            account: AccountId(account),
            currency: CurrencyId(2),
            amount,
        }
    }

    #[test]
    fn apply_submit_order_produces_placed_report() {
        let mut app = seeded_app();
        let mut reports = Vec::new();
        let ev = TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(1),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                quantity: qty(10),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        };
        apply_internal(&mut app, ev, &mut reports);
        assert!(
            !reports.is_empty(),
            "apply should emit at least one report for a resting order"
        );
    }

    #[test]
    fn tick_advances_scheduler_clock() {
        // No scheduled tasks yet — just assert the method is callable via
        // the trait without panicking. Real scheduler exercise is covered
        // by exchange.rs unit tests.
        let mut app = ServerApp(Exchange::new());
        let mut reports = Vec::new();
        <ServerApp as Application>::tick(&mut app, 1_000_000_000, &mut reports);
        assert!(reports.is_empty());
    }

    #[test]
    fn apply_query_request_seq_returns_per_key_hwm() {
        let mut app = seeded_app();
        let mut reports = Vec::new();

        // Advance two distinct keys to different marks with accepted
        // writes — the same key and sequence pairs the live pipeline
        // would hand `apply`.
        let key_a: u64 = 0xAAAA_AAAA_AAAA_AAAA;
        let key_b: u64 = 0xBBBB_BBBB_BBBB_BBBB;
        for seq in 1..=7 {
            apply_from(&mut app, key_a, seq, deposit(1, 1), &mut reports);
        }
        for seq in 1..=3 {
            apply_from(&mut app, key_b, seq, deposit(1, 1), &mut reports);
        }
        assert!(reports.is_empty(), "every write was accepted: {reports:?}");

        // Each key sees only its own mark — the engine reads
        // `ctx.key_hash`, not anything from the (payloadless) event
        // itself — and the query's own sequence plays no part.
        let mut query = |app: &mut ServerApp, key_hash| {
            apply_from(
                app,
                key_hash,
                0,
                TradingEvent::QueryRequestSeq,
                &mut reports,
            )
        };
        assert_eq!(
            query(&mut app, key_a),
            Some(QueryResponse::RequestSeqHwm { hwm: 7 })
        );
        assert_eq!(
            query(&mut app, key_b),
            Some(QueryResponse::RequestSeqHwm { hwm: 3 })
        );
        // A key with no prior activity reads back as zero.
        assert_eq!(
            query(&mut app, 0xDEAD_BEEF),
            Some(QueryResponse::RequestSeqHwm { hwm: 0 })
        );

        // Query is read-only: the marks are unchanged after the queries above.
        assert_eq!(app.0.request_seq_hwm(key_a), 7);
        assert_eq!(app.0.request_seq_hwm(key_b), 3);
    }

    /// The idempotency check is `apply`'s first act. A sequence at or
    /// below the key's mark is refused as a duplicate, naming what the
    /// request named, and leaves the engine as it was — the balance and
    /// the mark included. The mark only ever moves forward, by however
    /// much the client skipped.
    #[test]
    fn apply_refuses_a_repeated_sequence_before_it_touches_state() {
        let mut app = seeded_app();
        let mut reports = Vec::new();
        let key = 42;
        let balance = |app: &ServerApp| app.0.accounts().balance(AccountId(1), CurrencyId(2));

        apply_from(&mut app, key, 1, deposit(1, 100), &mut reports);
        assert!(reports.is_empty());
        assert_eq!(balance(&app).available, 1_000_100);

        for stale in [1, 0] {
            apply_from(&mut app, key, stale, deposit(1, 100), &mut reports);
            assert_eq!(
                reports.pop(),
                Some(ExecutionReport::Rejected {
                    order_id: OrderId(0),
                    symbol: Symbol(0),
                    account: AccountId(1),
                    reason: EngineRejectReason::DuplicateRequest,
                }),
                "sequence {stale} against a mark of 1"
            );
            assert!(reports.is_empty());
            assert_eq!(
                balance(&app).available,
                1_000_100,
                "a duplicate credits nothing"
            );
            assert_eq!(app.0.request_seq_hwm(key), 1, "a duplicate moves no mark");
        }

        // Skipping ahead is fine, and the mark follows.
        apply_from(&mut app, key, 10, deposit(1, 100), &mut reports);
        assert!(reports.is_empty());
        assert_eq!(balance(&app).available, 1_000_200);
        assert_eq!(app.0.request_seq_hwm(key), 10);
        apply_from(&mut app, key, 5, deposit(1, 100), &mut reports);
        assert_eq!(reports.len(), 1, "5 is behind the mark of 10");
    }

    /// A refused order is reported against its own id and symbol, as any
    /// other rejection of that order would be.
    #[test]
    fn apply_refuses_a_repeated_order_by_its_id() {
        let mut app = seeded_app();
        let mut reports = Vec::new();
        let order = TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(42),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                quantity: qty(10),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        };
        apply_from(&mut app, 7, 1, order, &mut reports);
        assert!(matches!(
            reports.as_slice(),
            [ExecutionReport::Placed { .. }]
        ));
        reports.clear();

        apply_from(&mut app, 7, 1, order, &mut reports);
        assert_eq!(
            reports,
            vec![ExecutionReport::Rejected {
                order_id: OrderId(42),
                symbol: Symbol(1),
                account: AccountId(1),
                reason: EngineRejectReason::DuplicateRequest,
            }]
        );
    }

    /// Two things the check never refuses: a query, whatever sequence it
    /// carries, since a client resynchronising its counter sends one
    /// first; and an event the node journaled itself, which carries key 0
    /// and sequence 0 every time.
    #[test]
    fn apply_exempts_queries_and_internal_events_from_the_check() {
        let mut app = seeded_app();
        let mut reports = Vec::new();
        let key = 42;
        apply_from(&mut app, key, 5, deposit(1, 1), &mut reports);

        // A stale sequence on a query is still answered, and moves no mark.
        let position = apply_from(
            &mut app,
            key,
            0,
            TradingEvent::QueryPosition {
                account: AccountId(1),
            },
            &mut reports,
        );
        assert!(matches!(position, Some(QueryResponse::Position { .. })));
        assert!(reports.is_empty());
        assert_eq!(app.0.request_seq_hwm(key), 5);

        // The same internal event twice over is applied twice over.
        apply_internal(&mut app, deposit(1, 1), &mut reports);
        apply_internal(&mut app, deposit(1, 1), &mut reports);
        assert!(reports.is_empty());
        assert_eq!(
            app.0
                .accounts()
                .balance(AccountId(1), CurrencyId(2))
                .available,
            1_000_003
        );
        assert_eq!(app.0.request_seq_hwm(0), 0, "key 0 keeps no mark");
    }

    #[test]
    fn build_reject_maps_transport_reasons() {
        let r = <ServerApp as Application>::build_reject(
            &TradingRequest {
                request_seq: 3,
                event: TradingEvent::CancelAll {
                    account: AccountId(9),
                },
            },
            TransportRejectReason::ReplicaDisconnected,
        );
        match r {
            ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            } => {
                assert_eq!(order_id, OrderId(0));
                assert_eq!(symbol, Symbol(0));
                assert_eq!(account, AccountId(9));
                assert_eq!(reason, EngineRejectReason::ReplicaDisconnected);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn apply_withdraw_emits_rejection_on_failure() {
        let mut app = seeded_app();

        // 1. Insufficient balance: account has 1_000_000 in CurrencyId(2),
        //    so a 2_000_000 withdrawal must reject.
        let mut reports = Vec::new();
        apply_internal(
            &mut app,
            TradingEvent::Withdraw {
                account: AccountId(1),
                currency: CurrencyId(2),
                amount: 2_000_000,
            },
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        match reports[0] {
            ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            } => {
                assert_eq!(order_id, OrderId(0));
                assert_eq!(symbol, Symbol(0));
                assert_eq!(account, AccountId(1));
                assert_eq!(reason, EngineRejectReason::InsufficientBalance);
            }
            ref other => panic!("expected Rejected, got {other:?}"),
        }

        // 2. Unknown account: withdraw from an account that was never
        //    provisioned/deposited.
        let mut reports = Vec::new();
        apply_internal(
            &mut app,
            TradingEvent::Withdraw {
                account: AccountId(999),
                currency: CurrencyId(2),
                amount: 1,
            },
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        match reports[0] {
            ExecutionReport::Rejected {
                reason, account, ..
            } => {
                assert_eq!(account, AccountId(999));
                assert_eq!(reason, EngineRejectReason::UnknownAccount);
            }
            ref other => panic!("expected Rejected, got {other:?}"),
        }

        // 3. Has resting orders: place an order, then attempt to withdraw.
        let mut placed = Vec::new();
        apply_internal(
            &mut app,
            TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(1),
                    account: AccountId(1),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: price(100),
                        post_only: false,
                    },
                    quantity: qty(10),
                    time_in_force: TimeInForce::GTC,
                    stp: SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            },
            &mut placed,
        );

        let mut reports = Vec::new();
        apply_internal(
            &mut app,
            TradingEvent::Withdraw {
                account: AccountId(1),
                currency: CurrencyId(2),
                amount: 1,
            },
            &mut reports,
        );
        assert_eq!(reports.len(), 1);
        match reports[0] {
            ExecutionReport::Rejected {
                reason, account, ..
            } => {
                assert_eq!(account, AccountId(1));
                assert_eq!(reason, EngineRejectReason::HasRestingOrders);
            }
            ref other => panic!("expected Rejected, got {other:?}"),
        }

        // 4. Successful withdraw on a clean account emits nothing.
        let mut reports = Vec::new();
        let mut clean = ServerApp(Exchange::new());
        clean.0.deposit(AccountId(7), CurrencyId(2), 500);
        apply_internal(
            &mut clean,
            TradingEvent::Withdraw {
                account: AccountId(7),
                currency: CurrencyId(2),
                amount: 200,
            },
            &mut reports,
        );
        assert!(
            reports.is_empty(),
            "successful withdraw must not emit reports"
        );
    }

    #[test]
    fn snapshot_restore_round_trip_preserves_state() {
        let mut before = seeded_app();
        let mut reports = Vec::new();
        // Submit a resting order so there's non-trivial book state to
        // round-trip through the snapshot.
        before.0.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                quantity: qty(10),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        let reports_before = reports.clone();

        let mut buf = Vec::new();
        <ServerApp as Application>::snapshot(&before, &mut buf).expect("snapshot");

        let mut cursor = Cursor::new(buf);
        let mut after = <ServerApp as Application>::restore(&mut cursor).expect("restore");

        // Placing an additional order against both and comparing the
        // emitted reports is a cheap proxy for structural equality —
        // the restored book must match price-time priority.
        let mut reports_after = reports_before.clone();
        reports_after.clear();
        after.0.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(99),
                    post_only: false,
                },
                quantity: qty(5),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports_after,
        );
        assert!(
            !reports_after.is_empty(),
            "restored exchange must accept orders"
        );
    }
}
