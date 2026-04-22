# Branch review — `refactor/transport-app-split`

Scope: 12 commits, 87 files, +8928 / −7674 lines. Melin decomposed from a
monolithic trading exchange into a durable transport (`melin-app` +
`melin-journal` + `melin-transport-core`) with pluggable applications
(`melin-engine` for trading, `melin-noop` for transport-only bench).
Review performed against `main` at branch tip.

---

## 🔴 Blockers

### 1. DPDK build is broken

`crates/server/src/server.rs:1519` still destructures the old 2-tuple
from `init_engine`; the new signature returns a 3-tuple. `cargo check
-p melin-server --features dpdk,trading --no-default-features` fails
with four cascading errors:

```
error[E0308]: mismatched types
    --> crates/server/src/server.rs:1519:9
     |
1519 |     let (engine, needs_seeding) = init_engine(&config)?;
     |         ^^^^^^^^^^^^^^^^^^^^^^^   --------------------- this expression has type `(Exchange, JournalWriter<TradingEvent>, bool)`
     |         expected a tuple with 3 elements, found one with 2 elements
```

Violates the CLAUDE.md pre-commit rule mandating `--features dpdk
--no-default-features` checks on the server. Trivial fix — align with
the kernel-TCP path at `server.rs:527`:

```rust
let (mut exchange, writer, needs_seeding) = init_engine(&config)?;
exchange.prefault();
```

---

## 🟠 Substantiated perf regressions

### 2. `ExecutionReport` grew 64 B → 392 B

Verified with a size-check binary. The new `Position { balances:
[(CurrencyId, u64, u64); 16], ... }` variant
(`crates/trading/src/types.rs:329-333`) dominates the tagged-union
size. Every one of the ~39 `reports.push(ExecutionReport::{Placed,
Fill, Cancelled, ...})` sites in `engine/src/exchange.rs` and
`engine/src/orderbook.rs` now writes 392 B per push instead of 64 B.
The matching stage's scratch `Vec<A::Report>` grows from ~16 KB to
~100 KB resident.

`OutputSlot` itself is unchanged (408 B on both branches — the
oversize variant was already in `OutputPayload` on main). The
regression is specifically in per-event report fan-out inside
`Exchange::execute`.

**Fix options**
- Box the balances array: `balances: Box<[(CurrencyId, u64, u64); 16]>`.
- Route position/stats snapshots out-of-band (separate ring slot or a
  dedicated response variant), matching main's
  `OutputPayload::PositionSnapshot` pattern.

### 3. `events_processed.store` moved from per-batch to per-event

`crates/transport-core/src/pipeline.rs:1213` now fires a `Relaxed`
store on every event; main did it per-batch + once per `QueryStats`
(`git show main:crates/engine/src/journal/pipeline.rs:1227,1286,1411`).
~16× more cacheline traffic on a 100 ns/order budget.

**Fix** Keep `local_events` thread-local and flush to the shared
atomic once per batch; `ApplyCtx` can read from the local counter
directly.

### 4. `ApplyCtx` triggers 3 atomic loads per App event

`pipeline.rs:1366-1368` loads `journal_cursor`, `active_connections`,
`events_processed` on every `apply`. On main these were paid only for
query paths.

**Fix** Either build `ctx` once per batch (the counters are advisory)
or pull counters lazily in the app's snapshot-query synthesis path.

---

## 🟡 Correctness / consistency

### 5. Shadow stage skips HWM updates under trading

`crates/server/src/shadow.rs:159-198` `dispatch_event` routes through
`app.apply(...)` without calling `app.check_request_seq`. Under
`Exchange`, `key_hwm` is part of the snapshot payload
(`crates/engine/src/journal/snapshot.rs:246,393-394`), so a shadow
snapshot has an empty `key_hwm` while the primary's is populated.
Restoring from that shadow snapshot would let previously-rejected
duplicate `request_seq` through.

Pre-existing behavior on main (shadow was trading-only), but the
generified code's docstring claims "Same event handling as the
matching stage's `process_event`" — materially wrong now.

**Fix** (one line) Thread `slot.key_hash` / `slot.request_seq` into
`dispatch_event` and call `check_request_seq` before `apply`,
mirroring `JournaledApp::replay_entry` at
`transport-core/src/journaled_app.rs:285`.

### 6. `replay_entry` discards the `check_request_seq` bool

`transport-core/src/journaled_app.rs:285` — correct for HWM rebuild
(HWM only advances if new), but if a journaled duplicate exists
(journal + matching race on the primary, so duplicates *can* land in
the journal), replay will re-apply it and diverge from live state.

Pre-existing on main (`crates/engine/src/journal/engine.rs:466`). Fix
slot: gate `tick` + `apply` on `check_request_seq` returning true.

### 7. Snapshot save crash-safety gaps

`crates/transport-core/src/snapshot.rs:113-129` — `.tmp` rename writes
`sync_all` but doesn't fsync the parent directory; on some filesystems
a post-rename crash can lose the rename. `MAX_SNAPSHOT_SIZE` is
checked on `load` but not enforced on `save`, so a runaway
`A::snapshot` could write a file subsequent `load` rejects.

---

## 🟡 Test coverage — concrete gaps

`melin-transport-core` has **zero tests** (no `#[test]` in the crate).
It's the single most critical new crate in the refactor. Missing:

- `JournaledApp<A>::{create, recover, recover_from_snapshot, rotate}`
  round-trips. Nothing beyond the trading-specific
  `engine/src/journal/*` suite exercises this.
- `snapshot::{save, load}` negative paths: BadMagic,
  UnsupportedTransportVersion, UnsupportedAppVersion, CRC tamper,
  truncation.
- `build_replica_pipeline<NoopApp>` assembly + smoke run.
- Noop-flavored primary ↔ replica integration (current
  `failover.rs` is gated to trading).
- Compile-time size asserts
  (`size_of::<InputSlot<TradingEvent>>() == 104`,
  `size_of::<OutputSlot<ExecutionReport>>() == 408`,
  `size_of::<JournalEvent<TradingEvent>>() == 64`). Without these, the
  `ExecutionReport` bloat in (2) would not have tripped CI.
- A test that a journaled duplicate re-applied under
  `JournaledApp::recover` does not mutate state twice (would
  catch (6)).

### Existing coverage that works

- `crates/noop/src/lib.rs:152-234` — `NoopApp` apply, dedup,
  snapshot round-trip.
- `crates/noop/tests/pipeline.rs` — end-to-end `Pipeline<NoopApp>`
  assembly with no engine in the dep graph.
- `crates/server/src/shadow.rs` tests (trading-gated) —
  dispatch-equivalence, shutdown promptness, snapshot-at-interval.
- `crates/server/tests/failover.rs` — 21 multi-process failover
  tests, trading-only.
- `crates/engine/src/application_impl.rs:250-361` — trait impl
  coverage (apply / tick / check_request_seq / build_reject /
  snapshot round-trip).

---

## 🟢 What's solid

- **Trait surface** (`melin-app`) is minimal and well-documented;
  every `ApplyCtx` field has a real caller; `clone_via_snapshot` +
  `prefault` default impls are pragmatic.
- **Types extraction** — `crates/trading/` is a clean 1:1 move-out
  from `melin-engine`. `TradingEvent` variants match the prior
  `JournalEvent::*` trading arms 1:1 (verified against
  `git show main:crates/engine/src/journal/event.rs`).
  `crates/trading/src/le.rs` is a strict superset of
  `crates/journal/src/le.rs` — no drift.
- **Journal framing** — `FORMAT_VERSION` bumped to 12 with rationale
  (`crates/journal/src/codec.rs:56-59`). Tag byte layout (Genesis /
  Checkpoint / Tick / App) is clean.
- **Chain-hash continuity** — verified across rotation boundaries
  (`JournaledApp::rotate` → `create_continuing` with current chain
  hash) and snapshot recovery (`seed_chain_hash` at
  `reader.rs:358-362`).
- **Hot-path shape** — no `dyn Application` anywhere on the
  per-event path (`grep "dyn Application"` returns nothing). The
  only `Box<dyn QueueCursor>` / `Arc<dyn QueueCursor>` occurrences
  are cold (health endpoint, monitoring setup, once-per-batch).
- **Feature-gating hygiene** — only 5 `cfg(feature = "trading"|
  "noop")` sites in `melin-server`, each at a genuine type /
  dependency boundary (`App` alias, `empty_app`,
  `spawn_event_publisher`, `event_publisher` module,
  `compile_error!` mutual-exclusion guard). Zero mentions in any
  trading-side crate (`engine`, `trading`, `market-data`, `protocol`,
  `bench`, `app`, `journal`, `transport-core`).
- **Noop engine-freedom** — `cargo tree -p melin-server
  --no-default-features --features noop | grep -cE
  'melin-engine|melin-market-data'` returns **0**.
- **Replication generification** — TCP + DPDK sender/receiver paths
  cleanly parameterized over `A`; rotation, chain-hash, snapshot
  transfer all `A`-agnostic.
- **Dedup-reject payloads** — on main, dedup rejects used placeholder
  zero `order_id` / `symbol` / `account`
  (`crates/engine/src/journal/pipeline.rs:1297-1306`). Branch now
  extracts real routing info via `A::build_reject`
  (`crates/engine/src/application_impl.rs:179-215`). Semantic
  improvement.

---

## 🟢 Low-priority polish

- **Stale comment** in `crates/transport-core/src/lib.rs:10-14`
  claiming snapshot framing "still lives with the concrete
  application." The framing moved into
  `transport-core/src/snapshot.rs` this branch.
- **CLAUDE.md / README.md unchanged** — no mention of `melin-app`,
  `melin-transport-core`, `melin-journal`, `melin-trading`,
  `melin-noop`. CLAUDE.md preamble explicitly directs "this file
  must be kept up to date."
- **`#[inline]` gaps** — `Exchange::execute`, `Exchange::cancel`,
  `Exchange::check_request_seq`, `TradingEvent::is_query` carry no
  inline hint. Trait thunks are `#[inline]`. Fat-LTO papers over
  this in release, but dev / test / bench profiles pay a call per
  event.
- **Snapshot-only recovery** — `server.rs:2111-2126`,
  `tcp_receiver.rs:728`, `dpdk.rs:785` all inline the
  "snapshot-exists-but-journal-missing" recovery path. Worth a
  `JournaledApp::recover_from_snapshot_only` helper.
- **No compile-time assertion that `size_of::<JournalEvent<
  TradingEvent>>() <= 64`** — add `const _: () = assert!(...);` in
  `engine/src/journal/mod.rs` or `trading/src/trading_event.rs`.

---

## Prioritized fix list

| # | Priority | Item |
|---|---|---|
| 1 | **blocker** | Fix DPDK build at `server.rs:1519` |
| 2 | perf       | Shrink `ExecutionReport::Position` (box or out-of-band) |
| 3 | perf       | Hoist `events_processed.store` + `ApplyCtx` loads to per-batch |
| 4 | correctness| Shadow's `dispatch_event` should call `check_request_seq` before `apply` |
| 5 | tests      | `transport-core` test module — `JournaledApp` round-trips, `snapshot` negative paths, size asserts |
| 6 | tests      | Noop primary ↔ replica smoke test (`run_sender` + `run_receiver` + snapshot transfer) |
| 7 | correctness| Gate replay `tick` + `apply` on `check_request_seq` returning true (closes pre-existing duplicate-replay hole) |
| 8 | safety     | Fsync parent dir after snapshot rename; enforce `MAX_SNAPSHOT_SIZE` on save |
| 9 | docs       | Update CLAUDE.md, README.md, `transport-core/src/lib.rs:10-14` for new crate layout |
| 10 | perf polish | `#[inline]` on `Exchange::execute`, `Exchange::cancel`, `Exchange::check_request_seq`, `TradingEvent::is_query` |

---

## Methodology

Three parallel sub-reviews covered:
1. Trait design + journal/snapshot framing + recovery paths (`melin-app`,
   `melin-journal`, `melin-transport-core/journaled_app.rs`, `snapshot.rs`,
   `melin-trading`, `melin-engine/application_impl.rs`).
2. Hot-path correctness + perf (`transport-core/pipeline.rs`, trait
   monomorphization, ring slots, replica path).
3. Server integration + test coverage (feature gates, shadow,
   replication, `melin-noop`, `failover.rs`, `lan-bench-suite.sh`).

Substantive claims were verified locally: the DPDK build break was
reproduced with `cargo check`; `ExecutionReport` size was measured with
a standalone binary; `events_processed` and `ApplyCtx` load-cadence
claims were cross-referenced against `git show main:...`;
`key_hwm`-in-snapshot was confirmed by grepping
`engine/src/journal/snapshot.rs`.
