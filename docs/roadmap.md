# Roadmap

Planned features sorted by value/complexity ratio for commercial readiness (exchange operators and investors).

**Scope.** This roadmap covers the exchange domain only — matching, accounts, risk, fees, gateways, and the benchmark harness. Infrastructure shared with the Melin sequencer (journaling, replication and failover, transport, pipeline) is planned in the sequencer repository's roadmap.

## Active

| Feature | Complexity | Why |
|---------|:---:|-----|
| Align `OutputSlot` to a cache line | Low | Sibling to the `InputSlot` cache-line alignment landed in `perf/input-slot-cache-line-padding`. `OutputSlot` is currently 424 B (≈6.6 cache lines), so adjacent slots in the matching→response ring straddle line boundaries and false-share across the producer (matching stage) / consumer (response stage) pair. Add `#[repr(align(64))]` on the struct and update the size assertion in `crates/exchange/server/src/exchange_app.rs` (likely 424 → 448). Layout-only change; the compile-time assertion is the test. Microbench against `perf/disruptor-slot-false-sharing-bench` first to confirm the win is comparable to the InputSlot ~16% on EPYC cross-CCD, then publish end-to-end p99 numbers. Independent of all other roadmap work. |
| Investigate bench open-loop pacer × dpdk-dual-repl stall | Medium | The bench client's open-loop pacer (`TARGET_RATE > 0` in `crates/exchange/bench/src/dpdk.rs`) collapses to near-zero throughput specifically on the dpdk-dual-repl transport. Two reproductions on EPYC 9255: one run sustained ~14K ops/s aggregate (Outcomes 985K over 70 s wall, almost entirely during warmup), a subsequent run on a fresh reboot was 100× worse — Outcomes 9 218 over 70 s = ~130 ops/s aggregate. Closed-loop on the same setup did 2.88M ops/s ten minutes earlier, so the pipeline is healthy; the failure is in the bench client's open-loop path interacting with dpdk-dual-repl's higher per-response latency. `scheduled` and `done` counters both stay at 0; `health-poller: health scrape failed` appears in the second run, suggesting the bench host also can't reach the server's kernel-TCP health endpoint during the stall (possibly DPDK NIC capture × kernel TCP coexistence). dpdk standalone × TARGET_RATE=1M works fine on the same boxes, isolating this to the dpdk-dual-repl combo. Diagnostic shape: add periodic `info!` traces in `run_dpdk_poll_bench` for per-conn `inflight.len`, `pop_due_some/none` counts, and send/recv byte counts; rerun with `RUST_LOG=info`; identify where the throughput drops out (pacer never firing? send returning Ok(0)? recv starvation?). Doesn't affect closed-loop benches, which we use throughout. |

## FIX Gateway Hardening

Follow-ups to take the FIX 4.4 gateway from minimum-viable to production-ready for a real exchange operator. The foundation (sessions, gap recovery, order entry, exec reports) is on `main`; these items make it deployable.

| # | Feature | Commercial value | Complexity | Value/effort | Why |
|---|---------|:---:|:---:|:---:|-----|
| 1 | Third-party FIX client soak test | High | Low | ★★★★★ | Current end-to-end tests use our own serializer on both sides — a closed loop that can't catch interop bugs. Run a sustained session against QuickFIX/J (or similar) to validate against an independent implementation. |
| 2 | IPv6 support | Medium | Low | ★★★☆☆ | `server_addr` and `listen_addr` are IPv4-only today (validation rejects IPv6). Many modern data centers require IPv6 dual-stack. |
| 3 | Market data (35=V/W/X) | Medium | High | ★★☆☆☆ | MarketDataRequest, snapshot/full refresh, incremental refresh. Requires a feed builder that consumes the engine's output event channel and maintains per-subscription book state. Larger surface than order entry. |

## Deferred

Features targeting regulated venues, gateway responsibilities, or with limited near-term value. Will revisit when the core product is mature or a specific buyer requires them.

| Feature | Why deferred |
|---------|-------------|
| Per-account trading permissions | Gateway concern — each firm's gateway instance restricts which accounts that connection can trade. Multi-tenant access control. |
| Replica analytics (6 items) | External service — throughput counters, latency histograms, volume/book depth analytics, audit trail queries, fee/PnL. Consumes the journal stream, not engine code. |
| Output event log | Regulatory audit trail. Depends on output event channel. |
| Subscription management | Gateway concern — the engine broadcasts, the gateway filters per-subscriber. |
| Iceberg orders | Niche — only matters for venues with institutional flow. |
| Auction mechanisms | Regulated venues only. Massive complexity (state machine, indicative pricing, uncrossing). |
| Position/exposure limits | Important for derivatives, less so for spot. Defer until a derivatives buyer needs it. |
| Tiered fee schedules | Volume-based tiers and per-account overrides. Can be implemented outside Melin — a fee service looks up the account's tier and sets the rate via the existing per-instrument fee API. |
