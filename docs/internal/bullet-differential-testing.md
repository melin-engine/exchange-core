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
| Cancel reporting | single `Cancelled` report, no cause field | `CancelOrderV1` carries a 15-variant `CancelReason` (user/amend/replace/admin/halt/margin-call/trigger-failed/overflow/…) |
| Fill reporting | `Fill` (per-fill qty, fees) | `TradeV1` adds `cumulative_filled_size`, `remaining_size`, `is_full_fill` per fill |
| Batching | one order per request | all place/amend/cancel take a `Vec`; `PlaceOrders(replace=true)` wipes the account's resting orders on the market first; `CancelAndPlaceOrders` is the atomic MM primitive |

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
| PO-04 | PostOnlySlide on cross | n/a (no equivalent; Melin rejects) | slides to best non-crossing price — but `PostOnlySlide`/`PostOnlyFront` are marked `// TODO: Delete this` in `bullet-exchange-interface` and no official bot/SDK helper uses them; being removed | ⚠️ (deprecated on Bullet side) |

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
| AMD-04 | Amend nonexistent / filled order | reject `UnknownOrder`, atomic no-op | source-derived: `AmendOrders` batch is all-or-nothing — one bad leg fails the whole batch (bullet-bots pre-filter would-cross rungs for exactly this reason); `PlaceOrders` is per-order (results keyed by `client_order_id`, partial acceptance tolerated) | ⚠️ verify observationally |
| AMD-05 | Amend a partially-filled order | qty applies to remainder; all-or-nothing validation | 🔎 | 🔎 |

### Duplicate / ID semantics

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| DUP-01 | Reuse client order ID while original still live | reject `DuplicateOrderId` | `client_order_id` optional (u64); `replace` on `PlaceOrders` is NOT per-ID — `replace=true` cancels all the account's resting orders on the market (`CancelReason::Replaced`) before placing; live-duplicate behavior still unobserved | ⚠️🔎 |
| DUP-02 | Reuse ID after original closed | accepted | 🔎 | 🔎 |

### Stops / triggers

| ID | Scenario | Expected Melin | Expected Bullet | Status |
|---|---|---|---|---|
| TRG-01 | Trigger boundary inclusivity: last trade exactly at trigger price | triggers (buy: last ≥ trigger; sell: ≤) | `TriggerDirection::GreaterThanOrEqual/LessThanOrEqual` — also inclusive | ✅🔎 |
| TRG-02 | Stop-limit triggers, limit would cross | re-enters matching pipeline, may fill immediately | source-derived: triggers are queued (`PendingTriggerOrders` → `TryExecuteTriggerOrder` in a later tx), not inline; failure cancels with `CancelReason::TriggerExecutionFailed` + a `RejectTriggerOrder`/`FailureExecuteTriggerOrder` event carrying a string reason; may also be `ReactivateTriggerOrder`-ed | ⚠️ (async trigger execution vs Melin's same-event cascade) |
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

## Org-repo source sweep (2026-07-24)

Full read of the public `bulletxyz` GitHub org (`bullet-exchange-interface`,
`bullet-rust-sdk`, `bullet-ws-interface`, `bullet-bots`, `bullet-app-changelog`
releases; `sovereign-sdk` is generic rollup infra, `dimension-adapters` has no
Bullet adapter). Source-derived — mark observationally before promoting to the
findings log.

### Melin improvements suggested by the comparison (ranked)

1. **Cancel reason on `ExecutionReport::Cancelled`.** Melin emits the same
   report for ≥7 distinct causes (user cancel, cancel-all, IOC remainder,
   STP ×3 modes, Day/EOD, GTD expiry, instrument disable) with no way for the
   client — or the audit trail — to distinguish them. Bullet versioned its
   cancel event (`CancelOrderV1`) specifically to add a 15-variant reason
   enum. Regulatory-audit and drop-copy value; one small enum field.
2. **Fill progress fields.** Bullet superseded `Trade` with `TradeV1` to add
   `cumulative_filled_size`/`remaining_size`/`is_full_fill` — real-world proof
   clients need per-fill order progress without local bookkeeping. Trade-off:
   grows the `ExecutionReport` enum (kept small deliberately for the scratch
   vec); measure before adding.
3. **Explicit ack for accepted stops.** A stop that parks emits no report —
   the client's only signal is an empty `BatchEnd` batch. Bullet emits
   `CreateTriggerOrder`. A `StopAccepted` (or reusing `Placed`) report would
   make the lifecycle observable and the audit trail explicit.
4. **Batch operations / atomic cancel-and-place.** Bullet's MM primitive is
   `CancelAndPlaceOrders` (atomic quote refresh) plus batch place/amend/cancel;
   its official bots lean on them heavily. Melin is one-order-per-request.
   For an MM-focused venue this is a real product gap (and a wire-efficiency
   win at 10M orders/sec).
5. **Market-order price protection.** Bullet has no naked market order at all —
   clients must submit a slippage-bounded IOC (webapp: mark price + allowance
   collar, changelog v2026.29.x "preventing unexpected fills at outlier book
   levels"). Melin market orders walk the book arbitrarily deep (quote-budget
   clamp aside). An operator-configurable protection band (max levels/ticks
   from touch) is standard on CLOB venues and cheap on the hot path.
6. **Market states `PostOnly`/`CancelOnly`** (already backlogged) — Bullet uses
   `PostOnly` for pre-launch price discovery ("Pre-launch (post-only) markets",
   changelog v2026.29.6) and documents legal state transitions. Strengthens
   the operator story for listings and halts-with-orderly-unwind.
7. **Book capacity policy.** Bullet bounds the book and evicts
   (`OrderbookOverflow` cancel + `BootOrder` event). Melin's slab grows by
   Vec reallocation (latency spike, unbounded by book) — the real bound is
   per-account cap × accounts. Consider a per-book hard cap with reject-new
   or evict-worst semantics, and a `warn!` approaching it.
8. **Event schema versioning discipline.** Bullet never mutates a journaled
   event: deprecated variants stay ("discriminators have to stay constant"),
   additions arrive as `*V1` variants. Worth adopting as an explicit rule for
   Melin's journal/protocol before the first production schema change forces
   an ad-hoc decision.

### Bullet quirks / candidate bugs spotted from Melin's perspective

Reportable only if observed live; all from source:

- `Event::event_key()` typo: `SetMarketTradingStatusFailed` maps to the string
  `"Exchange/SetMarketsTradingStatusFailed"` (stray `s`) — key-based filters miss it.
- WS error-code mismatches: `symbol_not_found()` emits `-1122 InvalidSymbol`
  (never `-1005 SymbolNotFound`); `server_busy()` maps to `-1001 Disconnected`.
- `event_time` units differ per WS message type (order acks µs; status/pong/error
  ms) — documented inconsistently in their own comments.
- Order-status strings use both `"CANCELED"` and `"CANCELLED"` spellings.
- `delegateOf` returns live `400` + `"is not a delegate"` where the spec says
  `404` (their bots key off the message string as a workaround).
- `SurrogateDecimal` `unsafe transmute` assumes `rust_decimal`'s private field
  layout — a dependency bump silently corrupts every price.
- Unchecked `Add`/`Sub`/`Mul`/`Div` impls on `PositiveDecimal` are exported with
  only a "for tests, do not use in production" comment as the guard.
- SDK WS event channel drops messages on overflow with only a `warn!` — no gap
  marker; a slow consumer desyncs its book invisibly.
- No server-side STP: their bots pre-skip would-cross quotes client-side; the
  race window remains (Melin's 4-mode STP is a genuine differentiator).
- Amend always loses queue priority (cancel+place); Melin's same-price
  qty-decrease keeps priority — differentiator worth documenting for operators.

Melin defect found and fixed during the sweep: the `RejectReason::DuplicateOrderId`
doc comment described high-water-mark ID semantics; the implementation is a
live-set check (reuse after close permitted). Comment corrected in
`crates/exchange/types/src/types.rs`.

## Running the Bullet legs

Plan: small harness crate (out-of-workspace, `tools/bullet-diff/`) using
`bullet-rust-sdk` against Bullet's **mainnet** (`tradingapi.bullet.xyz`).
Bullet's testnet webapp is access-gated (Vercel SSO, manual team approval) and
is not an option, so the Bullet legs run on mainnet restricted to the
**no-fill safe subset** — scenarios that never execute a trade:

- Runnable: PO-01, PO-02, PO-03, AMD-01, AMD-02, AMD-04, DUP-01, DUP-02, NUM-01.
- Out of scope on mainnet (require fills, or execute as taker on Bullet):
  CORE-01..03, FOK-01..03, IOC-01/02, MKT-01/02, AMD-03, AMD-05, TRG-01..03,
  NUM-02. These stay 🔎 unless testnet access materializes.

Hard safety rules for the harness (real-money venue):

1. Post-only on every order — never a plain limit, market, IOC, or FOK leg.
2. Minimum order size; resting probes priced far from the touch on an
   illiquid market. Exceptions by design: PO-01 must target the touch but is
   rejected without resting; PO-02 rests inside the spread — cancel
   immediately after acknowledgement, tiny size bounds the residual risk.
3. Cancel-all before and after every scenario.
4. Single account only — no trade between own accounts is possible, so
   wash-trading exposure is zero by construction.

Account setup: one wallet signed in at `app.bullet.xyz`, small USDC deposit
(margin for resting probes only), one locally generated ed25519 delegate key
registered via the webapp (delegates can place/cancel orders but not deposit
or withdraw). Credentials live outside the repo in
`~/.config/bullet-diff/mainnet.env` (delegate key hex + main address).
Scenarios get their "observed" column filled from there. Rate limits and
market-data races (other participants) mean observations should be retried
and confirmed via the private WS event stream, not inferred from a single
REST snapshot.
