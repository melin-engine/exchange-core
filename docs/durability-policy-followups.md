# Durability policy & ack-on-receive — open follow-ups

Action-oriented list of work remaining on the durability policy
framework (roadmap item #4, on `feat/durability-policy`) and the
ack-on-receive plumbing (roadmap item #5, on `feat/ack-on-receive`).
Items are things to implement, improve, or fix — completed work is
not tracked here.

---

## Bugs to fix on `feat/ack-on-receive` before merge

### Coalescing is per-iteration, not per-time-window

On a `busy_spin` loop, iterations are sub-microsecond. Each cursor
advance triggers an ack — potentially millions of acks/sec instead
of the ~20k/sec roadmap #5 projected. The "coalescing falls out
naturally while SEND is in flight" claim holds only when SEND is
genuinely pending; on a fast loop with no in-flight SEND, every
delta fires.

Fix path (decide after bench):

- Bench the actual ack rate under realistic load.
- If excessive, add a 50–100 µs minimum-interval throttle on the
  flush block (matches the design call in roadmap #5).
- Otherwise document the per-iteration behaviour as acceptable and
  correct the overstated comment at `tcp_receiver.rs:262`.

### Stale roadmap framing after the ack-on-receive landing

- `docs/roadmap.md:16` (item #5) is still phrased as future work. The receiver-side dual-track flush has landed on `feat/ack-on-receive`; the CLI-flag swap (3-variant `DurabilityMode`, next section) is what remains.

---

## Refactor: extract `try_flush_dual_track` helper — done

Landed on `feat/ack-on-receive`: shared helper in
`crates/server/src/replication/mod.rs` (`try_flush_dual_track`) now
backs all three receivers (`tcp_receiver`, `dpdk`, `rumcast_receiver`).
Carries the load-bearing namespace-translation comment and the
`debug_assert!` for cursor monotonicity in one place.

---

## Tests to add on `feat/ack-on-receive`

- **Backpressure-drain → flush duplicate-ack sequence**: simulate a queue-full event, drive a follow-up batch, assert the next ack does not regress `acked_sequence` on the primary. Hard to drive deterministically from integration tests (`PendingAckQueue` only fills when the journal stage is slower than the wire); easier as a unit test against `try_flush_dual_track` paired with a hand-driven `PendingAckQueue` sequence.

### Done

- ~~Regression for the namespace bug~~ — `in_memory_cursor_runs_ahead_of_persisted_under_sustained_traffic` in `crates/server/tests/failover.rs` exposes `melin_replica_in_memory_sequence` via `/metrics` and asserts in_memory never drops below acked across a 200-order burst with a concurrent sampler. The strict correctness guarantee is the `debug_assert!` inside `try_flush_dual_track`; the integration test pins the metric plumbing and provides a wire-level inversion check.
- ~~Unit test for the dual-track coalescing rule~~ — five focused tests in `crates/server/src/replication/mod.rs::tests` (`dual_track_*`) cover idle, persisted-only, in-memory-only, coalesce-on-stale-tracker, and async-mode paths.

---

## Next interface step: 3-variant `DurabilityMode` enum

After ack-on-receive validates on the bench, swap the operator-
facing surface from the DSL (`--durability-policy <STRING>`) to a
single `--durability-mode <local|hybrid|durably-replicated>` flag.

Target enum:

```rust
pub enum DurabilityMode {
    /// `persisted>=1`. Single-node durability — the primary's
    /// fsync is the only confirmation needed. Standalone / dev.
    Local,

    /// `persisted>=1 && in_memory>=2`. One durable copy on disk
    /// plus an in-memory ack from another node. Single-failure-
    /// safe; ~80 µs RAM-only window for the secondary copy. The
    /// new default — typical exchange deployments on PLP-backed
    /// NVMe. Saves ~50–80 µs per fill vs `DurablyReplicated`.
    Hybrid,

    /// `persisted>=2`. Two durable copies before client ack. Zero
    /// RAM-only window; gate stalls if a replica is unreachable.
    /// Compliance-driven venues.
    DurablyReplicated,
}
```

### What gets dropped

- `--durability-policy <STRING>` flag.
- The DSL parser (~150 LOC + ~30 unit tests).
- The `best_effort` modifier syntax.
- The floor pattern (`persisted>=3 best_effort && persisted>=2`) —
  largely redundant with the matching-stage halt at
  `replicas_connected==0` for new orders.

### What stays

- `Policy` / `Clause` / `Level` types as internal construction
  helpers (each mode builds its clause list in code).
- Wire protocol unchanged: `Ack { acked_sequence, in_memory_sequence }`
  carries forward identically.
- All cursor plumbing, observability (`policy_degraded` gauge,
  periodic warn, `DegradationLogger`), and tests.

### Why retire `async_ack` here too

`async_ack` is hardcoded `false` at every call site in production
since `--async-replica-ack` was removed. The receiver code still
threads the boolean through the streaming loop and the
`pop_all_async` branch in the flush block remains live. When the
enum swap lands, drop the parameter from receiver signatures and
remove the `pop_all_async` branch — the dual-track flush already
delivers the latency the legacy flag was trying to enable.

Net code reduction across both pieces: ~250 LOC plus the tests.
Operator-facing surface shrinks to one flag with three values.

---

## Commercial polish (buyer-driven)

These are real features but only worth building when a specific
buyer asks:

- **Degraded-duration counter on `/healthz`** — turn `melin_durability_policy_degraded` from a 0/1 gauge into a paired counter (`melin_durability_policy_degraded_seconds_total`) so SLO dashboards can compute time-in-degraded over arbitrary windows.
- **Multi-region awareness** — operators with replicas across availability zones want "≥1 ack from each zone" (Cassandra `EACH_QUORUM`). Needs node-tagging at handshake plus a richer policy clause shape. Would justify a 4th `DurabilityMode` variant.
- **Per-request policy override** — let the client specify a stronger consistency level per high-stakes order (Cassandra `w=` / MongoDB pattern). The wire protocol already carries a per-request envelope that could be extended. Composes cleanly with the enum: operator's `--durability-mode` becomes a default, per-request overrides scoped to the same named-mode set.
