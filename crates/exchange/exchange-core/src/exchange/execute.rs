//! The order-submission hot path. Pulled into its own submodule because
//! it dominates `exchange.rs` by line count and is the most heavily
//! exercised path in the engine — keeping it isolated makes targeted
//! review and perf work easier.

use super::Exchange;
use super::instrument::{inst_mut, inst_ref};
use super::token_bucket::TokenBucket;
use crate::types::{ExecutionReport, Order, OrderType, RejectReason, Side, Symbol, TimeInForce};

/// Basis-point denominator (1 bp = 0.01%). The split identity in
/// `fee_from_bps` is only valid when every division and modulus in the
/// function uses this same value — a named const ties them together.
/// u64: the base type of the fast-path arithmetic; widened at use sites.
const BPS_DENOM: u64 = 10_000;

/// `value * bps / 10_000` with the exact truncating semantics of the
/// naive i128 expression, but without a 128-bit division on the hot
/// path: LLVM does not strength-reduce `i128 / 10_000` and emits a
/// `__divti3` library call (~2.5% of the matching thread in profiles).
///
/// Splitting `value = q·10_000 + r` gives
/// `value·bps/10_000 == q·bps + (r·bps)/10_000` exactly — the first
/// term is an integer and `trunc(k + x) == k + trunc(x)` for integer
/// `k` — and the two remaining divisions are 64-bit by-constant, which
/// compile to multiply-shift. The `q·bps` multiply still widens to
/// i128 (`q` can reach `u64::MAX/10_000` and `|bps|` up to `i16::MAX`);
/// 128-bit *multiplication* is cheap, only division is a library call.
/// `r·bps` fits i64 comfortably (`r < 10_000`).
#[inline]
fn fee_from_bps(value: u128, bps: i16) -> i64 {
    match u64::try_from(value) {
        Ok(v) => {
            let q = (v / BPS_DENOM) as i128;
            let r = (v % BPS_DENOM) as i64;
            let head = q * bps as i128;
            let tail = (r * bps as i64) / BPS_DENOM as i64;
            (head + tail as i128) as i64
        }
        Err(_) => {
            // Unreachable through order flow: buy reservations bound
            // notional to u64 (`AccountManager::required_reserve`'s
            // `u64::try_from(cost)`), market/stop buys are clamped to
            // the quote budget in `OrderBook::execute_market`, and
            // `fill` clamps its own cost the same way. Loud in checked
            // builds; in release the fallback matches the old
            // expression bit-for-bit (both wrap the >i128::MAX product
            // identically under release semantics).
            debug_assert!(
                false,
                "fee_from_bps: cost {value} exceeds u64::MAX — a notional bound upstream is broken"
            );
            fee_from_bps_slow(value, bps)
        }
    }
}

/// Naive i128-division fallback for `fee_from_bps`, outlined so the
/// `__divti3` call sequence stays out of the fill path's instruction
/// stream and the fast path compiles to a branch-free fall-through
/// (`#[cold]`/`#[inline(never)]`, same convention as the account
/// module's `log_underflow`/`log_overflow`).
#[cold]
#[inline(never)]
fn fee_from_bps_slow(value: u128, bps: i16) -> i64 {
    ((value as i128) * (bps as i128) / BPS_DENOM as i128) as i64
}

impl Exchange {
    /// Submit an order to the matching engine for the given instrument.
    ///
    /// Validates the instrument exists, reserves funds, then executes.
    /// On fill, balances are updated. On reject/cancel, reserves are released.
    ///
    /// Under `feature = "skip-order-exec"` the body is short-circuited
    /// to a single `Rejected{NoLiquidity}` push, used by the server's
    /// transport-only benchmark build to isolate transport throughput
    /// from matching cost. Same wire shape — bench clients still see
    /// one response per `SubmitOrder` — but no order book / account
    /// state touched.
    #[inline]
    pub fn execute(&mut self, symbol: Symbol, order: Order, reports: &mut Vec<ExecutionReport>) {
        #[cfg(feature = "skip-order-exec")]
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::NoLiquidity,
            });
            return;
        }
        #[cfg_attr(feature = "skip-order-exec", allow(unreachable_code))]
        let Some(inst) = inst_ref(&self.instruments, symbol) else {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::UnknownSymbol,
            });
            return;
        };
        // Disabled instruments reject before HWM advance — the order is
        // never "processed", same as UnknownSymbol.
        if inst.disabled {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InstrumentDisabled,
            });
            return;
        }
        // Copy spec before taking mutable borrow on instruments below.
        // InstrumentSpec is Copy (3 × u32 = 12 bytes).
        let spec = inst.spec;

        // Dedup: reject if `(account, order_id)` already names a live
        // order. Cancel/replace look up by the same key, so two live
        // orders sharing it would make those operations ambiguous.
        // Replay-safety is provided one layer up by `check_request_seq`
        // (transport-level idempotency on `(key_hash, request_seq)`),
        // not here — duplicate journaled SubmitOrder events never reach
        // this point. Reuse of an `OrderId` after the original closes
        // is permitted by design.
        if self.live_order_ids.contains(&(order.account, order.id)) {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::DuplicateOrderId,
            });
            return;
        }

        // Existence already established by the `let Some(inst) = inst_ref(...)
        // else { ... return; }` guard at the top of `execute`. The matcher
        // is single-threaded and no instrument deregistration runs between
        // events, so the slot is still populated here.
        let inst = inst_ref(&self.instruments, symbol).expect("instrument verified to exist above");

        // Circuit breaker checks: trading halt rejects all orders; price
        // bands reject limit/stop-limit orders outside [lower, upper].
        // No HashMap lookup — circuit breaker is in the same struct.
        let cb = &inst.circuit_breaker;
        if cb.halted {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::TradingHalted,
            });
            return;
        }
        // Price band check applies only to orders with a known price.
        // Market and Stop orders have no submission-time price and
        // bypass bands by design (SEC-12). A large market order can
        // fill far outside the intended bands. Mitigation: use the
        // trading halt flag, or implement automatic volatility halts
        // (Phase 3 of the circuit breaker plan).
        let limit_price = match order.order_type {
            OrderType::Limit { price, .. } => Some(price),
            OrderType::StopLimit { limit_price, .. } => Some(limit_price),
            OrderType::Market | OrderType::Stop { .. } => None,
        };
        if let Some(price) = limit_price {
            if let Some(lower) = cb.price_band_lower
                && price < lower
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::OutsidePriceBand,
                });
                return;
            }
            if let Some(upper) = cb.price_band_upper
                && price > upper
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::OutsidePriceBand,
                });
                return;
            }
        }

        // Fat finger checks: reject orders exceeding per-instrument limits.
        let limits = &inst.risk_limits;
        if let Some(max_qty) = limits.max_order_qty
            && order.quantity.get() > max_qty.get()
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::ExceedsMaxOrderQty,
            });
            return;
        }
        if let Some(max_notional) = limits.max_order_notional {
            // Notional check applies only to orders with a known price.
            // Market and Stop orders have no submission-time price.
            // StopLimit uses limit_price (worst-case resting price).
            let limit_price = match order.order_type {
                OrderType::Limit { price, .. } => Some(price),
                OrderType::StopLimit { limit_price, .. } => Some(limit_price),
                OrderType::Market | OrderType::Stop { .. } => None,
            };
            if let Some(price) = limit_price {
                let notional = price.get() as u128 * order.quantity.get() as u128;
                if notional > max_notional as u128 {
                    reports.push(ExecutionReport::Rejected {
                        order_id: order.id,
                        symbol,
                        account: order.account,
                        reason: RejectReason::ExceedsMaxNotional,
                    });
                    return;
                }
            }
        }

        // GTD validation: GTD orders must carry an expiry strictly in the
        // future of the event clock; zero ("no expiry set") is covered by
        // the same comparison. An expiry at or before the clock has no
        // valid lifetime — the head-of-event expiry drain already ran for
        // this timestamp, so an accepted order would rest (or, for a stop
        // whose trigger is already satisfied, even fire and trade) despite
        // being past its deadline, until some later event reaps it. `<=`
        // matches the scheduler's due condition (`fire_ns <= now`).
        if order.time_in_force == TimeInForce::GTD && order.expiry_ns <= self.current_event_ts_ns {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InvalidExpiry,
            });
            return;
        }
        if order.time_in_force != TimeInForce::GTD && order.expiry_ns != 0 {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::InvalidExpiry,
            });
            return;
        }

        // Per-account open-order cap (SEC-03). Runs after every other
        // reject reason (UnknownSymbol, InstrumentDisabled, DuplicateOrderId,
        // TradingHalted, OutsidePriceBand, ExceedsMaxOrderQty,
        // ExceedsMaxNotional, InvalidExpiry) so an order that would have
        // been rejected for a venue-side or order-shape reason still
        // reports that reason — the cap is account-state, akin to
        // InsufficientBalance, and belongs adjacent to reservation.
        // Order: cap before reservation so a capped account doesn't churn
        // the slab. `order_counts` tracks (resting + pending stops +
        // in-flight) per account; `>=` rejects when accepting this order
        // would push the count past the limit. `0` = unlimited (opt-out).
        if self.max_open_orders_per_account > 0
            && self.order_counts.get(&order.account).copied().unwrap_or(0)
                >= self.max_open_orders_per_account
        {
            reports.push(ExecutionReport::Rejected {
                order_id: order.id,
                symbol,
                account: order.account,
                reason: RejectReason::ExceedsMaxOpenOrders,
            });
            return;
        }

        // Per-account order-submission rate limit (SEC-04). Token bucket
        // refilled at `max_orders_per_second`, capped at `max_orders_burst`,
        // metered against the journaled event timestamp
        // (`current_event_ts_ns`) so primary and replicas see identical
        // accept/reject decisions. Sits next to the open-orders cap above
        // because both are per-account policy gates that take effect
        // *before* any reservation work — a throttled order should not
        // perturb the slab or `order_counts`. Disabled when either knob
        // is `0`.
        if self.max_orders_per_second > 0 && self.max_orders_burst > 0 {
            let now_ns = self.current_event_ts_ns;
            let rate = self.max_orders_per_second;
            let burst = self.max_orders_burst;
            let bucket = self
                .order_buckets
                .entry(order.account)
                .or_insert_with(|| TokenBucket::new(burst, now_ns));
            if !bucket.refill_and_consume(now_ns, rate, burst) {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason: RejectReason::ExceedsOrderRate,
                });
                return;
            }
        }

        // Reserve pure notional (no fee cushion). Fees are settled from
        // the fill's received asset, not from this reservation, so a
        // schedule change after placement can never make the reservation
        // insufficient — by construction.
        let (reserved, slot) = match self.accounts.try_reserve(&order, &spec) {
            Ok(result) => result,
            Err(reason) => {
                reports.push(ExecutionReport::Rejected {
                    order_id: order.id,
                    symbol,
                    account: order.account,
                    reason,
                });
                return;
            }
        };

        // For buy-side market/stop-market orders, pass a cost budget so
        // the matching engine stops before exceeding the reservation. The
        // budget is exactly the reservation amount — no fee carve-out
        // needed since fees come out of the buyer's base credit, not the
        // quote reservation.
        let quote_budget = match (order.side, order.order_type) {
            (Side::Buy, OrderType::Market) | (Side::Buy, OrderType::Stop { .. }) => Some(reserved),
            _ => None,
        };

        *self.order_counts.entry(order.account).or_default() += 1;
        // Tentatively claim the (account, order_id) slot for the live
        // dedup check. If the order closes within this `execute` call
        // (IOC/FOK fill, FOK kill, etc.) the entry is freed in the
        // `freed` loop below; if it rests, the entry stays put.
        self.live_order_ids.insert((order.account, order.id));

        let taker_account = order.account;
        let taker_id = order.id;
        let report_start = reports.len();

        // Take scratch buffers out of `self` BEFORE the `inst_mut` borrow
        // below. `inst` mutably borrows `self.instruments` for the rest
        // of the function, so we can't touch `self.scratch_*` once it's
        // live. `mem::take` swaps with an empty Vec (no allocation —
        // `Vec::new()` is const) and the populated buffer is restored
        // at the end. Net effect: the inner loop has the same shape as
        // before but no per-event Vec allocation.
        //
        // The leading `clear()` calls are belt-and-braces: the put-back
        // at function end leaves the field empty, so under normal
        // control flow the take yields an already-empty Vec. The clear
        // only does work if a previous `execute` panicked between take
        // and put-back, leaving stale entries in the scratch.
        let mut consumed = std::mem::take(&mut self.scratch_consumed);
        consumed.clear();
        let mut freed = std::mem::take(&mut self.scratch_freed);
        freed.clear();

        // Single mutable lookup: book, fees all from the same struct.
        // Existence was established by the `inst_ref` guard at the top of
        // `execute`; same single-threaded invariant as the earlier
        // re-lookup applies.
        let inst =
            inst_mut(&mut self.instruments, symbol).expect("instrument verified to exist above");
        let taker_rested = inst.book.execute(order, quote_budget, slot, reports);

        // Capture the fee schedule for use inside the loop (we need
        // `maker_side` to attribute maker_fee/taker_fee to base vs quote
        // legs, so fees must be computed alongside the maker/taker slot
        // lookup rather than in a separate pre-pass).
        let fee_schedule = inst.fee_schedule;

        // Process reports to update balances. Mirrors the old process_reports
        // logic but resolves slots from the book instead of a separate HashMap.
        //
        // consumed_slots: fully-filled or STP-cancelled makers, with their
        // reservation slots. Typically 0-5 entries per aggressive order.
        consumed.extend(inst.book.drain_consumed_slots());

        for report in &mut reports[report_start..] {
            match report {
                ExecutionReport::Fill {
                    maker_order_id,
                    taker_order_id,
                    symbol: _,
                    maker_account,
                    taker_account: fill_taker_account,
                    price,
                    quantity,
                    maker_fee,
                    taker_fee,
                } => {
                    // Dereference for clarity; the `&mut` references are
                    // used only to write maker_fee/taker_fee below.
                    let maker_order_id = *maker_order_id;
                    let taker_order_id = *taker_order_id;
                    let maker_account = *maker_account;
                    let fill_taker_account = *fill_taker_account;
                    let price = *price;
                    let quantity = *quantity;
                    // Resolve maker slot: consumed list (fully filled) or
                    // order_index (partially filled, still on book).
                    let maker_info = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == maker_account && *id == maker_order_id)
                        .map(|(_, _, side, slot)| (*side, *slot))
                        .or_else(|| {
                            inst.book
                                .peek_order_location(maker_account, maker_order_id)
                                .map(|(side, _, slot)| (side, slot))
                        });

                    let Some((maker_side, maker_slot)) = maker_info else {
                        continue;
                    };

                    // Resolve taker slot. The fill's taker may be the original
                    // order (use `slot`) or a triggered stop (consumed_slots
                    // if fully filled/cancelled, or order_index if it rested).
                    let taker_slot = if fill_taker_account == taker_account
                        && taker_order_id == taker_id
                    {
                        slot
                    } else {
                        // Triggered stop's slot — check consumed first,
                        // then order_index (stop-limit that partially
                        // filled and rested).
                        match consumed
                            .iter()
                            .find(|(a, id, _, _)| *a == fill_taker_account && *id == taker_order_id)
                            .map(|(_, _, _, s)| *s)
                            .or_else(|| {
                                inst.book
                                    .peek_order_location(fill_taker_account, taker_order_id)
                                    .map(|(_, _, s)| s)
                            }) {
                            Some(s) => s,
                            None => continue,
                        }
                    };

                    // Compute fees from the schedule. The wire-format
                    // report carries fees in **quote currency** (cost-based)
                    // for both legs — that's the economic value of the
                    // fee, stable across A's received-asset settlement.
                    // Internally, fill() takes the buyer fee in base
                    // units and the seller fee in quote units (each
                    // deducted from that side's received asset).
                    let cost = price.get() as u128 * quantity.get() as u128;
                    let (buyer_slot, seller_slot, buyer_fee_bps, seller_fee_bps) = match maker_side
                    {
                        Side::Buy => (
                            maker_slot,
                            taker_slot,
                            fee_schedule.maker_fee_bps,
                            fee_schedule.taker_fee_bps,
                        ),
                        Side::Sell => (
                            taker_slot,
                            maker_slot,
                            fee_schedule.taker_fee_bps,
                            fee_schedule.maker_fee_bps,
                        ),
                    };
                    let buyer_quote_fee_report = fee_from_bps(cost, buyer_fee_bps);
                    let seller_quote_fee = fee_from_bps(cost, seller_fee_bps);
                    let buyer_base_fee = fee_from_bps(quantity.get() as u128, buyer_fee_bps);
                    // Update the report fields (quote-denominated).
                    match maker_side {
                        Side::Buy => {
                            *maker_fee = buyer_quote_fee_report;
                            *taker_fee = seller_quote_fee;
                        }
                        Side::Sell => {
                            *maker_fee = seller_quote_fee;
                            *taker_fee = buyer_quote_fee_report;
                        }
                    }
                    self.accounts.fill(
                        buyer_slot,
                        seller_slot,
                        price,
                        quantity,
                        buyer_base_fee,
                        seller_quote_fee,
                        &spec,
                    );

                    // Free fully consumed reservation slots (remaining == 0).
                    if self.accounts.reservation_remaining(maker_slot) == 0 {
                        self.accounts.free_slot(maker_slot);
                        freed.push((maker_account, maker_order_id));
                    }
                    if self.accounts.reservation_remaining(taker_slot) == 0 {
                        self.accounts.free_slot(taker_slot);
                        freed.push((fill_taker_account, taker_order_id));
                    }
                }
                ExecutionReport::Cancelled {
                    order_id, account, ..
                } => {
                    let order_id = *order_id;
                    let account = *account;
                    let key = (account, order_id);
                    if freed.contains(&key) {
                        continue;
                    }
                    // Cancelled: taker or STP-cancelled maker.
                    if account == taker_account && order_id == taker_id {
                        self.accounts.release(slot);
                    } else if let Some((_, _, _, maker_slot)) = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == account && *id == order_id)
                    {
                        self.accounts.release(*maker_slot);
                    }
                    freed.push(key);
                }
                ExecutionReport::Rejected {
                    order_id, account, ..
                } => {
                    let order_id = *order_id;
                    let account = *account;
                    let key = (account, order_id);
                    if freed.contains(&key) {
                        continue;
                    }
                    if account == taker_account && order_id == taker_id {
                        self.accounts.release(slot);
                    } else if let Some((_, _, _, triggered_slot)) = consumed
                        .iter()
                        .find(|(a, id, _, _)| *a == account && *id == order_id)
                    {
                        self.accounts.release(*triggered_slot);
                    }
                    freed.push(key);
                }
                _ => {}
            }
        }

        // Release leftover reservations for orders no longer on the book
        // (price improvement, market buy budget surplus, etc.).
        // Determined from report analysis — no HashMap lookup needed.
        if !taker_rested && !freed.contains(&(taker_account, taker_id)) {
            self.accounts.release(slot);
            freed.push((taker_account, taker_id));
        }
        for &(account, order_id, _, maker_slot) in &consumed {
            if !freed.contains(&(account, order_id)) {
                self.accounts.release(maker_slot);
                freed.push((account, order_id));
            }
        }

        // Decrement order_counts and free the live_order_ids entry
        // for every order that closed this turn (consumed maker slots
        // plus the taker if it didn't rest). Both maps are kept in
        // lockstep — they have to agree on "which orders are live."
        for &(account, order_id) in &freed {
            self.live_order_ids.remove(&(account, order_id));
            self.release_open_order(account);
        }

        // Schedule GTD expiry if the order rested (limit) or is now pending
        // (stop). Stop orders that triggered and fully filled in this same
        // execute call won't appear in the book any more — find_gtd_expiry
        // will return None and we won't schedule. Triggered stops that
        // re-rest as limits keep the same OrderId/expiry_ns, so the single
        // task scheduled here covers both lifecycle stages.
        if order.time_in_force == TimeInForce::GTD
            && order.expiry_ns > 0
            && inst_ref(&self.instruments, symbol)
                .and_then(|inst| inst.book.find_gtd_expiry(taker_account, taker_id))
                .is_some()
        {
            self.schedule_gtd_expiry(symbol, taker_account, taker_id, order.expiry_ns);
        }

        // Clear before restoring so the next call starts from an empty
        // Vec; capacity is retained. (`consumed` is iterated by reference
        // in the loop above and may still hold entries; `freed` is also
        // by-reference in its loop. Neither is drained as a side effect.)
        consumed.clear();
        freed.clear();
        self.scratch_consumed = consumed;
        self.scratch_freed = freed;
    }
}

#[cfg(test)]
mod tests {
    use super::{fee_from_bps, fee_from_bps_slow};
    use proptest::prelude::*;

    /// The naive expression `fee_from_bps` must match exactly. Written
    /// independently of production code (including its own `10_000`
    /// literal) so it can serve as a differential oracle.
    fn naive(value: u128, bps: i16) -> i64 {
        ((value as i128) * (bps as i128) / 10_000) as i64
    }

    proptest! {
        /// Full-domain differential check: the split-division fast path
        /// must agree with the naive oracle for every representable
        /// (value, bps) pair, not just the hand-picked grid below.
        #[test]
        fn fee_from_bps_matches_naive_for_any_input(v in any::<u64>(), b in any::<i16>()) {
            prop_assert_eq!(fee_from_bps(v as u128, b), naive(v as u128, b));
        }
    }

    #[test]
    fn fee_from_bps_matches_naive_division_across_edge_grid() {
        // Cross product of boundary-heavy values and fee/rebate rates,
        // including the split points around multiples of 10_000 where
        // the q/r decomposition changes, and u64::MAX where the i128
        // wrapping cast engages.
        let values: &[u128] = &[
            0,
            1,
            9_999,
            10_000,
            10_001,
            19_999,
            20_000,
            123_456_789,
            u64::MAX as u128 - 1,
            u64::MAX as u128,
        ];
        let rates: &[i16] = &[
            i16::MIN,
            -10_000,
            -9_999,
            -20,
            -1,
            0,
            1,
            20,
            9_999,
            10_000,
            i16::MAX,
        ];
        for &v in values {
            for &b in rates {
                assert_eq!(
                    fee_from_bps(v, b),
                    naive(v, b),
                    "fee mismatch for value={v} bps={b}"
                );
            }
        }
    }

    #[test]
    fn fee_from_bps_slow_path_above_u64_matches_naive() {
        // Defensive corner: cost = price × quantity can exceed u64 only
        // through paths the notional bounds block. The outlined fallback
        // must still agree with the naive expression. Tested directly —
        // the `fee_from_bps` wrapper debug_asserts before reaching it.
        let values: &[u128] = &[
            u64::MAX as u128 + 1,
            (u64::MAX as u128) * 2,
            u64::MAX as u128 * u64::MAX as u128,
        ];
        for &v in values {
            for &b in &[-10_000i16, -1, 1, 20, 10_000] {
                assert_eq!(
                    fee_from_bps_slow(v, b),
                    naive(v, b),
                    "fee mismatch for value={v} bps={b}"
                );
            }
        }
    }

    // debug_assert-based: only fires in debug builds, so the test is
    // meaningless (and would fail) under --release.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "notional bound upstream is broken")]
    fn fee_from_bps_panics_in_debug_above_u64() {
        // In debug builds a >u64 cost must fail loudly at the fee
        // helper rather than silently computing from a wrapped value.
        let _ = fee_from_bps(u64::MAX as u128 + 1, 1);
    }

    #[test]
    fn fee_from_bps_truncates_toward_zero_for_rebates() {
        // trunc semantics: -0.5 bp of 5_000 is 0, not -1 — sign must not
        // leak into the rounding direction.
        assert_eq!(fee_from_bps(5_000, -1), 0);
        assert_eq!(fee_from_bps(5_000, 1), 0);
        assert_eq!(fee_from_bps(19_999, -3), -5);
        assert_eq!(fee_from_bps(19_999, 3), 5);
    }
}
