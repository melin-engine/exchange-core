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

---

## Re-frame `docs/replication.md` around the three-tier menu

The replication design doc still describes the legacy
`--quorum-durability` surface (default 2 replicas, "quorum mode",
journal-min fallback). Rewrite around the `DurabilityMode` enum:
`Local` / `Hybrid` / `DurablyReplicated`, with the strict
fail-closed contract, the halt-on-replica-loss behaviour, and the
halt-state rejection bypass surfaced explicitly so operators
understand what clients see during an outage. Audience is exchange
operators and customers, so describe behaviour and guarantees,
not struct names.

---

## Runtime durability mode switch via admin command

The `DurabilityMode` enum swap landed strict fail-closed semantics:
under `Hybrid`, a primary halts when no replica is connected, and a
freshly-promoted replica running standalone can't satisfy `in_memory>=2`
either. The failover integration tests work around this by passing
`--durability-mode local` to replicas (with a `TODO(durability-admin)`
marker at each override site).

The production answer is a signed admin command — same channel as
`PROMOTE` / `ROTATE` — that swaps the active mode at runtime. Drives
the failover playbook: promoted node resumes trading at reduced
durability in seconds without a restart, then operators restore the
target mode once peers reattach. Every mode change is audit-logged
through the existing admin auth path. Drop the `TODO` overrides in
`failover.rs` and convert at least one promote-path test to exercise
the runtime swap end-to-end when this lands.

---

## Commercial polish (buyer-driven)

These are real features but only worth building when a specific
buyer asks:

- **Degraded-duration counter on `/healthz`** — turn `melin_durability_policy_degraded` from a 0/1 gauge into a paired counter (`melin_durability_policy_degraded_seconds_total`) so SLO dashboards can compute time-in-degraded over arbitrary windows.
- **Multi-region awareness** — operators with replicas across availability zones want "≥1 ack from each zone" (Cassandra `EACH_QUORUM`). Needs node-tagging at handshake plus a richer policy clause shape. Would justify a 4th `DurabilityMode` variant.
- **Per-request policy override** — let the client specify a stronger consistency level per high-stakes order (Cassandra `w=` / MongoDB pattern). The wire protocol already carries a per-request envelope that could be extended. Composes cleanly with the enum: operator's `--durability-mode` becomes a default, per-request overrides scoped to the same named-mode set.
