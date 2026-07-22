# Differential testing against Bullet (bullet.xyz)

Contributor notes. Goal: use Bullet's publicly documented exchange semantics as an
independent oracle for Melin's matching behavior (and vice versa). Divergences fall
into three buckets, all useful: a Melin bug, a Bullet bug (reportable), or a real
semantic difference worth documenting for operators.

## Bullet public surface

Bullet's matching engine is closed-source; the comparison anchors on its protocol
contract and documentation:

- [`bullet-exchange-interface`](https://github.com/bulletxyz/bullet-exchange-interface) — canonical protocol types (orders, events, cancel reasons)
- [`bullet-rust-sdk`](https://github.com/bulletxyz/bullet-rust-sdk) — REST/WS client, used to drive scenarios against their testnet
- [Trading API docs](https://tradingapi.bullet.xyz/docs/), esp. [order-fields](https://tradingapi.bullet.xyz/docs/order-fields.html) and [decimal-encoding](https://tradingapi.bullet.xyz/docs/decimal-encoding.html)

Bullet is a perpetuals DEX (margin, funding, liquidations — no Melin overlap there).
The overlapping surface is the central limit order book itself.

## Semantic mapping

| Concept | Melin | Bullet |
|---|---|---|
| Order types | Market, Limit(+post_only), Stop, StopLimit | Limit, PostOnly, FillOrKill, ImmediateOrCancel, PostOnlySlide, PostOnlyFront (no native Market — aggressive IOC instead) |
| TIF | GTC, IOC, FOK, Day, GTD | encoded in order type (FOK/IOC); no Day/GTD equivalent |
| Post-only on cross | reject `PostOnlyWouldCross` | PostOnly: reject; PostOnlySlide: reprice to best non-crossing (no Melin equivalent) |
| Amend | atomic cancel-replace; keeps priority on same-price qty-decrease | `AmendOrders` = cancel + place (always loses priority) |
| Amend to crossing price | reject `PriceWouldCross` | executes (it's a fresh place) |
| STP | 4 modes, default `CancelNewest` | none exposed |
| Stops/triggers | trigger on last trade only | Mark / Oracle / LastTrade conditions; TP/SL pairs; TWAP |
| Numerics | integer ticks/lots (`NonZeroU64`) | `rust_decimal` (96-bit mantissa), 12-dp fixed scale internally, explicit Up/Down rounding |
| Fees | flat per-instrument maker/taker bps | volume tiers (Tier0–9) |
| Market states | active / halted / disabled | Active, Halted, Cleaning, Cleaned, PostOnly, CancelOnly |
| Book capacity | per-account open-order cap | book-level eviction (`BootOrder`, `OrderbookOverflow` cancel reason) |

## Scenario matrix

Melin expectations are pinned by existing tests where cited; run Bullet legs against
their testnet via `bullet-rust-sdk` and record observed behavior in the last column.

Legend: ✅ behaviors should agree · ⚠️ documented divergence expected · 🔎 open — needs Bullet observation.

### Matching core

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| CORE-01 | Two makers same price, taker crosses | older maker fills first (FIFO; proptests) | same (price-time claimed) | ✅🔎 |
| CORE-02 | Taker walks multiple price levels | fills best→worst, partials at last level | same | ✅🔎 |
| CORE-03 | Partial fill leaves remainder resting (GTC limit) | `Fill` + remainder on book | same | ✅🔎 |

### Post-only

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| PO-01 | Post-only priced at opposite best (would cross) | reject `PostOnlyWouldCross` | "rejected if it would immediately match" | ✅🔎 |
| PO-02 | Post-only priced inside spread | rests | rests | ✅🔎 |
| PO-03 | Post-only equal to same-side best | rests | 🔎 (PostOnlyFront would front-run queue — different feature) | 🔎 |
| PO-04 | PostOnlySlide on cross | n/a (no equivalent; Melin rejects) | slides to best non-crossing price | ⚠️ |

### FOK

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| FOK-01 | Exact liquidity at limit | full fill | full fill | ✅🔎 |
| FOK-02 | Insufficient liquidity at limit | reject `FOKCannotFill`, zero fills | "cancelled", zero fills | ✅🔎 |
| FOK-03 | Sufficient liquidity only beyond limit price | reject | reject/cancel | ✅🔎 |
| FOK-04 | Liquidity sufficient only via own resting order (STP active) | reject (`stp_tests.rs`: `stp_cancel_newest_fok_mixed_book_no_partial_fill`) | n/a — no STP; would self-fill | ⚠️ |
| FOK-05 | Non-self liquidity sufficient but partly queued *behind* own order, STP `CancelNewest`/`CancelBoth` | reject, zero fills (`stp_tests.rs`: `stp_cancel_newest_fok_liquidity_behind_self_order_no_partial_fill`) | n/a | **found Melin bug — fixed** (see Findings) |
| FOK-06 | FOK market buy, base liquidity sufficient but quote balance can't afford it | reject, zero fills (`tests.rs`: `fok_market_buy_insufficient_quote_balance_rejected`) | n/a — margin model | **found Melin bug — fixed** (see Findings) |

### IOC

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| IOC-01 | Partial liquidity at limit | fill available, `Cancelled` remainder | "fills as much as possible immediately, cancels any remaining size" | ✅🔎 |
| IOC-02 | No liquidity at limit | zero fills, remainder `Cancelled` (never rests) | full cancel | ✅🔎 |

### Market orders

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| MKT-01 | Market on empty book | reject `NoLiquidity` | no market type; aggressive IOC cancels quietly | ⚠️ |
| MKT-02 | Market buy exceeding quote balance | fill clamped by quote budget | margin model — not comparable | ⚠️ |

### Amend / cancel-replace

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| AMD-01 | Same price, qty decrease | keeps queue priority (`cancel_replace.rs`) | loses priority (cancel+place) | ⚠️🔎 verify observationally |
| AMD-02 | Price change | loses priority | loses priority | ✅🔎 |
| AMD-03 | Amend to a crossing price | reject `PriceWouldCross`, original untouched | executes as taker | ⚠️ |
| AMD-04 | Amend nonexistent / filled order | reject `UnknownOrder`, atomic no-op | 🔎 batch semantics — does one bad leg poison the batch? | 🔎 |
| AMD-05 | Amend a partially-filled order | qty applies to remainder; all-or-nothing validation | 🔎 | 🔎 |

### Duplicate / ID semantics

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| DUP-01 | Reuse client order ID while original still live | reject `DuplicateOrderId` | `client_order_id` optional; `PlaceOrders` has a `replace` flag | ⚠️🔎 |
| DUP-02 | Reuse ID after original closed | accepted | 🔎 | 🔎 |

### Stops / triggers

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| TRG-01 | Trigger boundary inclusivity: last trade exactly at trigger price | triggers (buy: last ≥ trigger; sell: ≤) | `TriggerDirection::GreaterThanOrEqual/LessThanOrEqual` — also inclusive | ✅🔎 |
| TRG-02 | Stop-limit triggers, limit would cross | re-enters matching pipeline, may fill immediately | 🔎 (`TryExecuteTriggerOrder` / `FailureExecuteTriggerOrder` events suggest triggered orders can fail placement) | 🔎 |
| TRG-03 | Trade that triggers a stop whose fill triggers another stop | iterative trigger loop, no recursion (`matching-engine.md`) | 🔎 | 🔎 |

### Numerics / fees

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| NUM-01 | Price with more precision than tick | not representable (integer ticks; gateway rejects) | accepted then rounded to 12 dp: Up=AwayFromZero, Down=ToZero | ⚠️ |
| NUM-02 | Fee rounding on odd notional | truncate toward zero (proptest-verified vs i128 oracle) | 🔎 rounding direction per fee leg | 🔎 |

## Findings log

| ID | Date | Outcome |
|---|---|---|
| FOK-05 | 2026-07-22 | **Melin bug confirmed and fixed.** FOK pre-check (`BookSide::available_quantity`) excluded own resting quantity but still counted non-self liquidity queued behind a self-order; under `CancelNewest`/`CancelBoth` matching terminates at the self-order, so a FOK could partially fill then be cancelled. Fix: STP-aware reachability in `available_quantity`. Regression tests: `stp_cancel_newest_fok_liquidity_behind_self_order_no_partial_fill`, `stp_cancel_both_fok_liquidity_behind_self_order_no_partial_fill`, `available_quantity_honors_stp_reachability`. |
| FOK-06 | 2026-07-22 | **Melin bug confirmed and fixed** (found reviewing FOK-05 — same class, different termination condition). A market buy's quote budget (the account's entire available quote balance) clamps matching, but the FOK pre-check only counted base quantity — a FOK market buy the account couldn't afford would partially fill then cancel. Fix: `BookSide::fillable_quantity` (renamed from `available_quantity`) replays the budget clamp with matching's integer arithmetic. Regression tests: `fok_market_buy_insufficient_quote_balance_rejected`, `fok_market_buy_multi_level_budget_shortfall_rejected`, `fok_market_buy_exact_quote_balance_fills`, `fillable_quantity_honors_quote_budget`. |

## Running the Bullet legs

Not yet set up. Plan: small harness crate (out-of-workspace, `tools/bullet-diff/`)
using `bullet-rust-sdk` against Bullet's testnet — needs a funded testnet account
and two sub-accounts (maker/taker) per scenario. Scenarios marked 🔎 get their
"observed" column filled from there. Rate limits and market-data race conditions
(other testnet traffic) mean scenarios should run on an illiquid market or use
prices far from the touch where possible.
