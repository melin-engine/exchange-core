# Code Smell Cleanup — Hot Path

Survey scope: `crates/engine/`, `crates/journal/`, replication code.
Branch: `chore/code-smell-cleanup`.

Tackle items one by one. Mark `[x]` when done and reference the commit.

## High

- [x] **`engine/src/journal/snapshot.rs:553–723` — split `decode_exchange_state()`**
  Done: 12 per-section helpers + `decode_opt_nz_u64` + `read_section_len`;
  `decode_exchange_state` is now a ~75-line orchestrator. 364 engine tests pass, clippy clean.

- [x] **`engine/src/journal/snapshot.rs:1309–1380` — split `restore_state()`**
  Done: extracted `build_indexed_instruments` (map-building + sparse-Vec
  assembly, ~45 lines) and `inject_reservation_slots_into_instruments`.
  `restore_state` shrunk from 82 to ~30 lines and reads as a linear
  orchestrator. 364 engine tests pass, clippy clean.

- [x] **`engine/src/orderbook.rs:1686, 1704, 1770` — three `.expect("front existed")` in `match_against()` hot loop**
  Done (option a): each site now carries a comment naming the
  `front_node_idx(price)` guard and stating why the panic is preferable
  (silently dropping a fill would corrupt balances/leak a reservation).

- [x] **`engine/src/exchange.rs:1059, 1259` — `.expect("instrument verified to exist above")` lacks pointer to the check**
  Done: both call sites now reference the `inst_ref` guard at the top of
  `execute` (~line 1017) and note the single-threaded invariant that
  keeps the slot occupied.

## Medium

- [~] **`engine/src/application_impl.rs:240` — `Vec::new()` in `restore()` snapshot deserialization** — **won't-do.**
  The `App::restore<R: Read>` trait has no size hint, so any pre-allocation
  is a guess. `read_to_end` already grows exponentially, and for the
  `Cursor<Vec<u8>>` path used by `clone_via_snapshot` the std impl
  specializes to a single `extend_from_slice` (zero reallocs). Real-recovery
  cost is sub-millisecond even for a 10 MB snapshot. A real fix would plumb
  size through the trait — out of scope for a Medium item.

- [x] **`engine/src/orderbook.rs:979–982, 1083–1086` — `Vec::new()` for hot-path scratch buffers in `new()`**
  Done: `new()` and `from_parts` now use `Vec::with_capacity(64)` for all
  four scratch buffers, matching `with_capacity()`. Comments explain the
  cleared-and-reused-per-order rationale.

- [x] **`engine/src/account.rs:397` — `.unwrap_or_default()` on balance lookup**
  Done: added comment explaining the missing-key → zero-Balance contract
  and why it is replay-safe (accounts come into existence via `deposit`).

- [x] **`engine/src/journal/snapshot.rs:1330` — `Vec::resize_with(max_sym + 1, || None)` sparse symbol table**
  Done as part of the `restore_state` split: rationale now lives in the
  `build_indexed_instruments` doc comment (sparse Vec vs HashMap, cache
  locality, branch-light indexing).

- [x] **`engine/src/journal/snapshot.rs:327` — split `encode_exchange_state()` (~120 lines)**
  Done: 12 per-section encoders + `encode_opt_nz_u64` mirror the decode
  helpers. Orchestrator destructures `ExchangeSnapshot` exhaustively so
  the compiler errors if a new field is ever added without an encoder
  call. Same exhaustive-destructure trick applied to `restore_state`,
  which also documents that `order_sides` is derived state (not consumed
  on restore — see follow-up below).

- [ ] **`engine/src/journal/snapshot.rs` — `order_sides` is redundant snapshot field**
  Discovered while applying exhaustive destructuring: `restore_state`
  never reads `order_sides`, because the value is regenerated from the
  restored books via `Exchange::snapshot_order_sides` (which queries
  each book's `active_order_slots` / `active_stop_slots`). Worth a
  follow-up to either drop the field from the wire format (saves bytes
  on every snapshot) or wire it into a restore-time consistency check.

## Low

- [ ] **`crates/journal/src/trace.rs:435` — `writer.join().unwrap()`**
  Dev/bench only, but replace with proper error: `.map_err(|_| "thread panicked during trace flush")`.

- [ ] **`engine/src/scheduler.rs:148, 150, 154` — `.unwrap()` on `pop_due()` in unit tests**
  Replace with `assert_eq!(heap.pop_due(150), Some(expected))` for clarity.

- [ ] **`engine/src/application_impl.rs:308, 311` — `NonZeroU64::new(p).unwrap()` in test helpers**
  Test-only; consider const helpers / `const fn` to signal compile-time guarantee.

- [ ] **`journal/src/writer.rs:244, 1327` — `let _ = ...` discards**
  Add a one-line comment above each explaining why the result is safe to drop.

- [ ] **`engine/src/exchange.rs:898` — `let _ = reports` in a test**
  Replace with comment if intentional, or assert on it.

---

Note: no `panic!`/`todo!`/`unimplemented!` on the hot path, no locks on matching, allocations mostly pre-sized. This list is the long tail.
