# Bench harness extraction

Splitting `melin-bench` into a transport-agnostic harness (destined for the
sequencer repository) and an exchange-specific driver (staying here).

## Why

`melin-bench` is the only way to measure a sequencer change — journal,
transport, pipeline — and it can only be built from this repository, on top
of the entire exchange. A Melin contributor benchmarking a journal change
has to build the matching engine, the protocol codec, and the order-flow
generator to do it.

The obvious fix — move `melin-bench` to the Melin repository — inverts the
dependency. Melin would depend on `melin-exchange-core`, which depends on
Melin. Cargo tolerates that through crates.io, but the release loop
deadlocks: benching Melin `0.15` needs an exchange built against `0.15`,
which cannot exist until `0.15` is published. Benchmarking each release
against the *previous* exchange is exactly the contamination that A/B
results have to avoid.

So the split runs the other way: extract the half that has no exchange
dependency, and leave the exchange-shaped half where it is.

## Where the seam falls

Roughly a third of `melin-bench` is hard-coupled to the exchange and never
moves:

| Component | Coupling |
| --- | --- |
| `generator.rs` | `Request::SubmitOrder`/`CancelOrder`/`CancelReplace`, `Order`, `Price`, `TimeInForce` |
| `calibration/` | ITCH 5.0 parser and limit-order-book replica |
| `--mode=engine` | `Exchange::with_capacity`, `add_instrument`, `deposit` |
| `journal_writer_bench` | `TradingEvent` as the journal payload |
| `OutcomeReport` | exhaustive match over every `RejectReason` variant |

The rest is generic: the clock, the phase model, the pacer, the latency
time series, key derivation, and the two endpoint scrapers — plus, behind
a workload seam that does not exist yet, the io_uring event loop.

## Staging

`melin-bench-harness` lives at `crates/core/bench-harness` — under
`crates/core/`, mirroring the sequencer repository's layout, **not** under
`crates/exchange/`. That is deliberate: the directory is a marker that the
crate is a guest here. It must never grow a dependency on `melin-types`,
`melin-protocol`, `melin-exchange-core`, or `melin-server`; anything that
needs one belongs in `melin-bench`.

Extracting it here first (rather than opening with a cross-repo move) keeps
every step verifiable: the real benchmark compiles and runs against the
harness at each commit, so the eventual relocation is a directory move plus
a dependency-line edit rather than a rewrite done blind.

The crate is listed in `scripts/publish.sh` only because it is still
released from this workspace. After the move, the sequencer repository
publishes it and this workspace consumes it from crates.io like the other
Melin crates.

### Moving it to the sequencer repository

1. Move `crates/core/bench-harness` to `crates/core/bench-harness` there
   (`members = ["crates/core/*"]` picks it up with no manifest edit).
2. In its `Cargo.toml`: set an explicit `version` (that workspace has no
   `version` in `[workspace.package]`) and change `melin-app` to
   `{ path = "../app" }`.
3. Here: drop the workspace member entry, remove it from
   `scripts/publish.sh`, and move `melin-bench-harness` from the exchange
   group to the sequencer group in `[workspace.dependencies]`.

## Status

**Done** — the pieces with no exchange coupling and no design work:

* `clock` — `rdtscp`/`cntvct_el0`, `TscClock`, calibration
* `phases` — `BenchPhases`/`PhaseDeadlines`, shared CLI defaults
* `pacing` — `PaceClock`, `PaceStats`
* `series` — `LatencySample`, `maybe_sample`, and the `time_series` JSON
  schema (previously hand-rolled at the reporting call site)
* `keys` — deterministic per-connection key derivation
* `health` — `/metrics` poller (both gauges it reads are emitted by the
  sequencer's `transport-core/src/health.rs`)
* `stats` — `/stats-dump` client

**Next** — the workload seam. `run_uring_loop` decodes every response
frame with the exchange codec to decide whether it completed a request:

```rust
let response = codec::decode_response(frame).expect("decode response");
if matches!(response, ResponseKind::BatchEnd) { /* record latency */ }
conn.outcomes.record(&response);
```

A `Workload` trait — produce a request frame, classify a response frame,
accumulate and render outcomes — lets the event loop, the connect/auth
helpers, and the reporting move too, with the exchange as one
implementation and the sequencer's `counter` example as another. That is
what makes the harness usable from the sequencer repository at all; until
then it is a library with no benchmark of its own.

**After that** — `melin-plot`, which is already exchange-free but reads the
JSON schema the harness now owns.
