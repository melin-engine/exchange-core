# CLAUDE.md

> **This file must be kept up to date** as the project evolves — update structure, dependencies, and conventions whenever they change.

## Project

Sub-millisecond, production-grade trading engine targeting **10M orders/sec**, built on the **LMAX architecture** (single-threaded business logic, event sourcing, mechanical sympathy). Rust (edition 2024). Early stage.

The engine must include all features required for production deployment.

## Build & Run

```sh
cargo build          # compile
cargo run            # run
cargo test           # run tests
cargo clippy         # lint
cargo fmt            # format
```

## Conventions

- Follow Rust best practices (idiomatic patterns, clippy clean, formatted with `cargo fmt`).
- Write unit tests for all non-trivial code. Skip only when genuinely unreasonable (e.g., trivial glue code).
- **Correctness is critical** — the matching engine is financial infrastructure. Correctness always comes first.
- **Reasonably optimized from the start** — don't prematurely optimize, but make performance-conscious choices by default: minimize allocations, avoid locks on the hot path, favor cache-friendly data structures. Profile before micro-optimizing.
- **No `.unwrap()` in production code** — use proper error handling. `.unwrap()` is fine in tests.
- **No `#[ignore]` on tests** — if a test fails, fix the bug. Never suppress a failing test with `#[ignore]`.
- **No silently ignored results** — do not use `let _ =` to discard `Result` values unless there is a clear reason (e.g., best-effort diagnostic writes). Handle errors explicitly.
- **Comment data structure and type choices** — always add a comment justifying why a specific collection, data structure, or numeric type was chosen (e.g., why `BTreeMap` over `HashMap`, why `u64` over `u128`).
- **Log levels** — `error!`: server malfunctions only (bugs, journal I/O failures) — must never fire due to bad client input or client network issues. `info!`: server lifecycle events (start, stop, recovery). `debug!`: client-caused events (connections, disconnects, malformed messages, write failures).

### Git
- **No co-authored commits** — do not add `Co-Authored-By` trailers.
- **Conventional Commits** — all commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/) spec (e.g., `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`).
- **Never commit without explicit request** — do NOT commit unless the user explicitly asks (e.g. "commit", "commit and push"). Completing a task does NOT imply permission to commit. Always wait for the user to request the commit.
- **Never push without explicit confirmation** — always ask for review before pushing. Do not push unless the user confirms.
- **Never commit CLAUDE.md** — this file is managed manually by the user. Do not `git add` or commit it, even if it has been modified.
- **Commit intermediary steps** — for large multi-step tasks, commit each logical step separately rather than batching everything into one giant commit. This keeps history clean and bisectable. Always ask for review after each commit before moving to the next.
- **Always check `Cargo.lock`** — when dependencies change, `Cargo.lock` must be staged and committed alongside `Cargo.toml` changes. The pre-commit hook enforces this.

## Key Design Constraints

- **~100ns per order budget** — at 10M orders/sec, every allocation, cache miss, and branch misprediction counts
- **Single-threaded business logic** (LMAX core) — no locks on the hot path; I/O and journaling happen on separate threads via ring buffers
- **Deterministic replay** — given the same input events, output must be identical; this is the foundation of event sourcing and crash recovery
- **Strict price-time priority** — no order may jump the queue; correctness here is non-negotiable
- **Durable journaling** — every event is persisted before acknowledgement; snapshots prevent full replay from genesis on recovery
- **Full audit trail** — every order, fill, and cancellation must be recorded (regulatory requirement)
- **Hot-path scope** — risk checks, self-trade prevention, and order throttling all run on the critical path and must be zero/low-cost
- **Tail latency matters** — measure p99/p99.9, not averages
- **Extensive testing** — property-based and fuzz testing for edge cases (partial fills at price boundaries, cancel-replace races, empty book scenarios)

## Roadmap

### Order Types
- [x] Market
- [x] Limit
- [x] Stop (stop-loss)
- [x] Stop-Limit

### Time-in-Force
- [x] GTC (Good-Til-Cancelled)
- [x] IOC (Immediate-Or-Cancel)
- [x] FOK (Fill-Or-Kill)
- [ ] GTD (Good-Til-Date)
- [ ] Day

### Matching Engine
- [ ] Cancel-replace / order amendment (atomic modify without losing queue priority for unchanged price)
- [ ] Circuit breakers (price bands, trading halts)
- [ ] Auction mechanisms (opening/closing/volatility auctions)

### Conditional / Advanced Orders
- [ ] Iceberg (hidden quantity)
- [ ] Trailing Stop
- [ ] OCO (One-Cancels-Other)
- [ ] Bracket (entry + take-profit + stop-loss)

### Execution Qualifiers
- [ ] Post-Only (maker-only)
- [ ] Reduce-Only

### Fees
- [ ] Maker/taker fee model (configurable per instrument or tier)
- [ ] Fee deduction on fill (deduct from proceeds, include in ExecutionReport)
- [ ] Fee schedules (volume-based tiers, account-level overrides)

### Testing
- [ ] `proptest` invariant tests on order book (fill quantities, book consistency, volume conservation)
- [ ] `cargo-fuzz` crash discovery (arbitrary order sequences, overflow/saturation edge cases)
- [x] Verify `price × quantity` intermediate calculations don't overflow `u64` (use `u128` for computed values)

### Event Sourcing
- [x] Write-ahead journal (input commands, CRC32C checksums, crash recovery)
- [x] Snapshot save/load (version-boundary recovery, CRC32C integrity)
- [x] `JournaledExchange` wrapper (persist-before-ack, deterministic replay)
- [x] Pipelined journal I/O via LMAX disruptor ring buffer pipeline
- [x] io_uring async fsync with group commit (overlapped fsync + encoding)
- [x] Client deduplication (per-account OrderId high-water mark — rejects duplicate/stale submissions on crash-recovery retry)
- [ ] Journal rotation
- [ ] Journal compaction (automatic snapshot trigger)
- [ ] Output event log (deferred — can be produced by a replica via deterministic replay)

### Risk Checks
- [x] Account balances (per-account, per-currency; reserve on order, update on fill, release on cancel)
- [x] Self-trade prevention (per-order modes: Allow, CancelNewest, CancelOldest, CancelBoth)
- [x] Fat finger checks (max order size, max notional value — per-instrument `RiskLimits`, journaled via `SetRiskLimits`)
- [x] Kill switch (cancel all orders for an account — `CancelAll` request + journal event)
- [ ] Price band checks (reject orders too far from reference price)
- [ ] Position/exposure limits (deferred)
- [ ] Order throttling (deferred — conflicts with benchmark; add when untrusted clients exist)

### Networking
- [x] Binary wire protocol (custom codec, length-prefixed framing)
- [x] Transport abstraction (TCP now, QUIC/kernel bypass later)
- [x] TCP transport with `TCP_NODELAY`
- [x] Server (pipeline orchestration, accept loop)
- [x] Client library
- [x] Unix domain socket transport (benchmarking comparison point only — production requires TCP for remote clients)
- [x] Dedicated I/O threads (OS threads with blocking I/O, zero-tokio architecture)
- [x] Epoll reader pool (edge-triggered, non-blocking multiplexed reads)
- [x] Lock-free CAS-based multi-producer disruptor (no mutex on input path)
- [x] io_uring transport (separate read/write rings, replacing epoll + blocking writes) — `uring_reader.rs`, `uring_response.rs`, bench client
- [ ] Investigate unified io_uring I/O thread (Option B: single ring for both recv+send per connection set — merges reader + response roles, saves one `io_uring_enter` per cycle but breaks pipeline stage separation)
- [x] Multishot RECV (`RecvMulti` + provided buffer groups) — eliminates SQE resubmission; `uring_reader.rs`
- [x] Heartbeats and connection timeouts (bidirectional keepalive, idle timeout detection, `--heartbeat-interval-secs`, `--connection-timeout-secs`)
- [ ] Backpressure handling (defined policy when disruptor is full)
- [ ] TLS (rustls or native-tls for encrypted client connections)
- [ ] DDoS protection (connection rate limiting, per-IP limits, SYN cookies, max connections cap)
- [ ] QUIC transport (investigate `quinn`)
- [ ] Kernel bypass (DPDK/ef_vi) for single-digit µs latency — now the primary throughput bottleneck is TCP stack overhead (2x gap between no-persist and engine-only on LAN)

### Gateway
- [x] Gateway crate — proxy between clients and engine (binary protocol, TCP)
- [x] Accept client connections, forward orders to engine, relay execution reports back
- [ ] Scalable I/O model — current 2-threads-per-client won't scale past ~500 connections; switch to epoll/io_uring multiplexing (must support 1000+ concurrent clients)
- [ ] Output event channel from matching stage (SPSC/broadcast — prerequisite for market data)
- [ ] Book replica maintained from execution report stream
- [ ] L2 order book snapshots (top N price levels, aggregated quantity)
- [ ] Public trade feed (price, quantity, timestamp per fill)
- [ ] BBO (best bid/offer) push updates
- [ ] Subscription management (subscribe/unsubscribe per instrument)
- [ ] Reference data management (instrument lifecycle)
- [ ] Rate limiting and connection management (per-client throttling)

### Authentication & Authorization
- [ ] Client authentication
- [ ] Per-account trading permissions (who can trade what)
- [ ] Admin API (instrument management, circuit breaker controls, kill switch)

### Horizontal Scaling
- [ ] Instrument sharding (partition instruments across engine instances, each single-threaded — linear throughput scaling)
- [ ] Cross-shard routing (gateway routes orders to the correct shard by symbol)
- [ ] Cross-shard risk checks (portfolio-level margin/exposure requires message passing between shards, adds latency and complexity)

### Redundancy & High Availability
- [ ] Journal replication (stream WAL to replica; sync for zero data loss, async for lower latency)
- [ ] State machine replication (replica replays journal for identical exchange state via deterministic replay)
- [ ] Failover detection and promotion (leader election, split-brain prevention)
- [ ] Client failover (detect primary failure, reconnect to new primary, resume with sequence numbers)
- [ ] Network partition handling (fencing, quorum-based decisions)

### Operations & Reliability
- [x] Graceful shutdown (SIGINT/SIGTERM handler, ordered drain: readers → journal → matching → response)
- [x] Configuration management (CLI args for bind address, journal path, core affinity, reader threads)
- [x] Health checks / readiness probes (`ServerReady` wire handshake on connect)

### Logging & Observability
- [x] Structured logging (`tracing` crate, error-level for malfunctions)
- [x] Per-stage pipeline latency tracing (`latency-trace` feature gate)
- [ ] Output event log (ExecutionReports for audit trail)

### Metrics & Observability

Most analytics can run on a **replica** replaying the journal, keeping the primary's hot path free of instrumentation jitter. Only networking and pipeline health metrics require the primary.

#### Primary node (lightweight, operational health)
- [ ] Metrics transport (decide where/how to expose: separate stats file, output event channel, Prometheus endpoint, or admin socket — must not touch the hot path)
- [x] Pipeline stage utilization (`pipeline-stats` feature gate — busy/idle ratio per stage)
- [ ] Connection counts (active clients, connects/disconnects per second)
- [ ] Disruptor queue depth / backpressure monitoring (input ring fill level)
- [ ] Health/liveness endpoint (beyond current `ServerReady` handshake)

#### Replica or offline (journal-derived, zero primary impact)
- [ ] Order/fill/cancel throughput counters (events per second by type)
- [ ] Latency histograms (journal `timestamp_ns` → matching → response, per-event)
- [ ] Volume analytics (traded volume per instrument, per account)
- [ ] Book depth analytics (resting order counts, spread tracking)
- [ ] Audit trail queries (full event history for regulatory compliance)
- [ ] Fee/PnL accounting (when fees and position tracking exist)

### Tail Latency Optimization
- [x] Release profile tuning (`lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `target-cpu=native`)
- [x] ~~Switch to jemalloc~~ (done — `tikv-jemallocator` in server and bench)
- [x] ~~Hoist per-order Vec allocations to reusable struct fields in `check_triggers()`~~ (done — `trigger_price_buf` and `triggered_buf` on `OrderBook`, pre-allocated to 64)
- [x] ~~Hoist per-order Vec allocation to reusable field in `process_reports()`~~ (done — `consumed_buf` on `Exchange`, pre-allocated to 256)
- [x] ~~Increase pre-allocated `reports` Vec capacity in matching stage~~ (done — capacity 256)
- [x] ~~Pre-size hot-path HashMaps with `with_capacity()`~~ (done — `order_index` 1M, `order_sides` 2M, `stop_index` 100K)
- [x] ~~io_uring transport~~ (done — see Networking section)
- [x] ~~IRQ affinity~~ (done — `bench-isolate.sh` pins all interrupts to core 0, stops irqbalance; p99.9 822µs→497µs)
- [x] ~~`bench-isolate.sh` IRQ affinity pinning~~ (done — saves/restores per-IRQ masks on exit)
- [x] ~~CPU core pinning for readers and bench threads~~ (done — readers cores 4-5, bench cores 6+)
- [x] ~~`isolcpus=nohz,domain,1-5` + `nohz_full=1-5` + `rcu_nocbs=1-5` boot params~~ (done — fully isolates pipeline+reader cores from scheduler; `bench-isolate.sh` reports active boot tuning; fsync throughput +6.5%, max latency -29%)

### Trading Game

The engine is also the foundation for a multiplayer trading game. The goal is to reach a **playable live demo as soon as possible** — focus on the minimum viable path (market data, basic positions, a web UI, and bot liquidity) and defer everything else (auth, game sessions, leaderboards, settlement) until the core loop works end-to-end.

Features needed beyond the core exchange:

#### Market Data Dissemination
- [ ] L2 order book snapshots (top N price levels with aggregated quantity)
- [ ] Public trade feed (price, quantity, timestamp per fill)
- [ ] BBO (best bid/offer) push updates
- [ ] Server-push transport for market data (subscription-based, per-connection)
- [ ] Subscription management (subscribe/unsubscribe to instruments)

#### Position & PnL Tracking
- [ ] Per-account, per-instrument position tracking (net quantity, average entry price)
- [ ] Mark-to-market unrealized PnL (using last trade or mid price)
- [ ] Realized PnL on position closes
- [ ] Portfolio value (sum of balances + mark-to-market positions)

#### Player Management
- [ ] Account creation / registration
- [ ] Authentication (token-based or simple credentials)
- [ ] Starting capital distribution on join

#### Game Lifecycle
- [ ] Game session management (create, start, end, reset)
- [ ] Time-limited rounds (configurable duration)
- [ ] Leaderboard / ranking (by portfolio value, PnL, or Sharpe)
- [ ] End-of-game settlement (flatten all positions, compute final scores)

#### Liquidity
- [ ] Bot market makers (quote around a reference price with configurable spread/depth)
- [ ] External price feed ingestion (drive reference prices for bots)
- [ ] Pre-seeded order books (initial liquidity on game start)

#### Game Client
- [ ] Web UI (order entry, book visualization, portfolio dashboard, leaderboard)
- [ ] Or enhanced TUI with live market data, positions, and PnL

## Dead Ends / Investigated & Rejected

### SMI count tracking via MSR 0x34 (AMD Ryzen)

**Date**: 2026-03-14 | **CPU**: AMD Ryzen 7 5800X3D

Attempted to read MSR 0x34 (IA32_SMI_COUNT) to track SMI interrupts during benchmarks and explain the 20-112µs max latency spikes in engine-only mode. `modprobe msr` succeeded but `rdmsr -p 0 0x34` returns "cannot read MSR" — AMD doesn't expose this Intel-specific MSR.

**Conclusion**: Can't measure SMIs on this CPU. The max latency spikes (~1 in 20M orders) are likely SMIs/NMIs/kernel interrupts but not worth chasing — p99.99 is rock-solid at 0.11µs.

### io_uring registered buffers for socket I/O (kernel 6.8)

**Date**: 2026-03-13 | **Kernel**: 6.8.0-101-generic | **io_uring crate**: 0.7

Pre-registering buffers via `IORING_REGISTER_BUFFERS` to skip per-SQE `get_user_pages()` page table walks. Two approaches tested:

1. **`ReadFixed`/`WriteFixed` opcodes** — works but routes through VFS layer (`vfs_read` → `sock_read_iter`) instead of the direct socket path (`sock_recvmsg`). Benchmarked ~5% *slower* than plain `Recv`/`Send`.

2. **`IORING_RECVSEND_FIXED_BUF` flag (value=4) on `Recv`/`Send` SQEs** — stays on the direct socket path. Requires SQE patching (ioprio at offset 2, buf_index at offset 40) since the io_uring 0.7 crate doesn't expose these fields for Recv/Send. Returns `EINVAL` on kernel 6.8 for `IORING_OP_RECV`. `SEND` support landed in kernel 6.0; reliable `RECV` support came later.

**Conclusion**: Not viable on kernel 6.8. The per-SQE `get_user_pages()` cost (~100-200ns) is already optimized by the kernel's GUP fast path for recently-used pages. **Revisit on kernel ≥6.10** where `IORING_RECVSEND_FIXED_BUF` may work for both RECV and SEND.

### Group commit delay with TCP transport

**Date**: 2026-03-13

Tested `--group-commit-us` values 8, 16, 25, 64, 100, 128 µs on TCP loopback (16 clients, window 16). All values hurt throughput and p50 vs the zero-delay baseline (201K/s, p50 1114µs). Even 8µs dropped throughput to 194K/s. Reason: the delay holds the journal cursor longer, making the response stage block on the cursor spin-wait and accumulate larger TCP send buffers. The io_uring overlapped fsync already provides natural batching — while fsync A is in flight, events accumulate for batch B — so explicit delay adds no benefit.

Group commit *does* help UDS (270K/s at 100µs, +34% over baseline) because UDS transport is near-free (response stage 0.18% busy vs 25% on TCP).

**Conclusion**: Keep `group_commit_delay = 0` for TCP. Only use group commit with UDS or after making TCP response sends cheaper.

### Response stage per-slot journal cursor gating with mid-batch flush

**Date**: 2026-03-13

Moved journal cursor check from batch-level (wait for max input_seq) to per-slot, with mid-batch `flush_sends()` before spinning on journal cursor. Goal: send already-durable responses while waiting for fresher events.

Results: p99 improved 15% (1740→1476µs), journal utilization jumped from 0.38% to 19.86% (pipeline doing more useful work), but p50 regressed 13% (1115→1263µs) and response stage went from 23% to 31% busy. The synchronous `flush_sends()` (io_uring submit_and_wait) in the inner loop added per-iteration overhead.

Without the mid-batch flush (per-slot check only), results were identical to baseline — responses still sit in send_bufs until SPSC empties, so earlier encoding of durable slots doesn't help.

**Conclusion**: Mid-batch flush is the right direction (proved by 20x journal utilization increase) but synchronous sends are too expensive. Revisit with non-blocking send submission or when TCP response overhead is reduced.

## Performance Profile

Performance figures are in the [README](README.md#performance). Keep them up to date when making performance-related changes.

LAN benchmark (two Cherry AMD Ryzen 9950X servers, dedicated NVMe journal disk):
- **With fsync/FUA**: 5.2M orders/sec, p99.9 = 939 µs, max = 1.47 ms
- **Without persistence**: 11.2M orders/sec, p99.9 = 747 µs, max = 915 µs
- **Single-order latency**: 70 µs p50 (1 client, no pipelining, full durability)
- **Engine-only**: 17.3M orders/sec, p99 = 0.06 µs

### Current bottleneck: TCP network stack

The 2x gap between fsync (5.2M) and no-persist (11.2M) shows that journal I/O is no longer the dominant bottleneck on PLP NVMe hardware. The TCP stack (syscalls, kernel buffers, io_uring send/recv overhead) is now the primary throughput limiter. The engine itself runs at 17.3M/s — the pipeline and network consume the remaining headroom.

**How to apply:** Further throughput gains require reducing TCP overhead: kernel bypass (DPDK/ef_vi), QUIC, or batched io_uring multishot send. Journal optimization is no longer the priority.

Core layout: 0=OS/IRQ, 1-3=pipeline (journal/matching/response), 4-5=readers, 6+=bench.

**Benchmarking constraint**: do NOT optimize by batching multiple client requests into a single write — real clients send one order at a time. Batch submission is unrealistic and inflates throughput numbers artificially.

## Next Steps

Priority: productionize the engine, then build the trading game.

### Completed phases
- ~~**Phase 1: LAN benchmark**~~ — done: two Cherry AMD Ryzen 9950X servers, 5.2M orders/sec with full durability, 70µs single-order latency
- ~~**Phase 1.5: Realistic benchmark**~~ — done: power-law price/size distributions, Zipf accounts, aggressive fills, Market/IOC/FOK, STP diversity, recency-biased cancels, JSON output, TUI charts

### Benchmark improvements
- **Multi-machine benchmark support** — `--account-id` and `--order-id-offset` flags so multiple bench processes on separate hosts can target the same engine without OrderId collisions
- **Saturation curve** — multi-run script sweeping `--clients` and `--window`, collect JSON results, plot latency vs throughput
- **Real-world data replay** — parse and replay actual exchange data for maximum credibility. Candidate sources: NASDAQ ITCH 5.0 (public protocol spec, sample data available), Databento (normalized L3 data, free samples), Lobster (academic NASDAQ message-level data). Legal review needed before publishing results with third-party data

### Phase 2: Product credibility
4. ~~**Graceful shutdown**~~ — done: SIGINT/SIGTERM handler, ordered drain (readers → pipeline), ~47ms shutdown
5. ~~**Client deduplication**~~ — done: per-account OrderId high-water mark, snapshot version 3
6. ~~**Risk checks**~~ — done: fat finger checks (max order size / notional), kill switch. Order throttling deferred
7. **Circuit breakers** — price band checks, trading halts
8. ~~**Output event log**~~ — deferred: can be produced by a replica replaying the journal (deterministic replay reproduces identical execution reports)
9. **Metrics & observability** — primary: connection counts, queue depth. Bulk metrics deferred to replica
10. **Journal rotation + compaction** — prevent unbounded disk usage
11. **Backpressure policy** — defined behavior when disruptor is full
12. **Gateway scalability** — epoll/io_uring multiplexing (current 2-threads-per-client caps at ~500 connections)
13. **TLS** — encrypted client connections for non-loopback deployments

### Phase 3: Trading game demo
14. **Output event channel** — broadcast execution reports from matching stage for book replicas
15. **L2 snapshots + trade feed** — aggregated book depth and recent trades
16. **Bot market makers** — liquidity around a reference price
17. **Position & PnL tracking** — per-account positions, mark-to-market, portfolio value
18. **Game client** — web UI or enhanced TUI for order entry, book view, and leaderboard

### Benchmark visualization (immediate)
- [ ] **Latency histogram** — HDR histogram rendered in the TUI (single run)
- [ ] **Tail latency stability** — p99/p99.9/p99.99 over time in the TUI (single run)
- [ ] **Saturation curve** — latency vs throughput at different load levels (multi-run script, sweep `--clients` and `--window`, collect results, plot)

Deferred:
- Tail latency micro-optimizations (p99.9 < 1ms) — revisit after LAN demo
- GTD / Day time-in-force
- Auth, game sessions, leaderboards, settlement
- Replication / HA

## Structure

### `crates/disruptor/` — generic lock-free ring buffers (no trading-domain knowledge)
- `src/padding.rs` — cache-line alignment (`CachePadded<T>`)
- `src/ring.rs` — multi-consumer disruptor (single-producer or CAS-based multi-producer, N gated consumers)
- `src/spsc.rs` — single-producer, single-consumer queue

### `crates/engine/` — matching engine and event sourcing
- `src/types.rs` — core types (OrderId, AccountId, CurrencyId, Price, Quantity, Order, ExecutionReport, InstrumentSpec, etc.)
- `src/account.rs` — account balance management (deposit, withdraw, reserve, fill, release)
- `src/orderbook.rs` — order book with price-time priority matching and stop trigger logic
- `src/exchange.rs` — multi-instrument dispatcher with integrated balance validation
- `src/journal/` — durable write-ahead log for event sourcing and crash recovery
  - `event.rs` — `JournalEvent` enum (input commands only)
  - `codec.rs` — binary encode/decode with CRC32C checksums
  - `writer.rs` — `JournalWriter` (append + fsync to disk, batch append API)
  - `reader.rs` — `JournalReader` (sequential read + validate)
  - `engine.rs` — `JournaledExchange` wrapper (journal-before-execute + replay recovery)
  - `pipeline.rs` — disruptor pipeline stages (`JournalStage`, `MatchingStage`, slot types)
  - `snapshot.rs` — snapshot save/load for Exchange state (version-boundary recovery)
  - `error.rs` — `JournalError` enum

### `crates/server/` — server and pipeline orchestration
- `src/server.rs` — builds disruptor pipeline, spawns 3 OS threads, accept loop
- `src/response.rs` — response stage thread (output SPSC → direct socket writes via `BlockingFrameWriter`)
- `src/reader.rs` — epoll-based multiplexed reader pool (edge-triggered, non-blocking I/O → lock-free `MultiProducer`)
- `src/affinity.rs` — CPU core pinning for pipeline and reader threads

### `crates/protocol/` — wire protocol (zero async, no tokio)
- `src/message.rs` — `Request`, `ResponseKind`, `ConnectionId`
- `src/codec.rs` — binary encode/decode for wire messages
- `src/transport.rs` — `BlockingTransportListener` trait (TCP, UDS, future io_uring)
- `src/blocking.rs` — `BlockingFrameReader`/`BlockingFrameWriter` for length-prefixed framing
- `src/tcp.rs` — `BlockingTcpListener` (std `TcpListener`, `TCP_NODELAY`)
- `src/uds.rs` — `BlockingUdsListener` (std `UnixListener`)

### `crates/gateway/` — client-facing proxy (planned)
### `crates/client/` — typed blocking client library (std `TcpStream`)
### `crates/bench/` — pipelined end-to-end benchmark with latency histograms (TCP default, `--uds` flag)
### `crates/tui/` — terminal UI for interactive testing

### `scripts/`
- `bench-isolate.sh` — CPU governor tuning, NMI watchdog disable, IRQ affinity pinning, dmesg capture for latency benchmarking (requires root)
- `grub-bench.conf` — kernel boot parameters for `isolcpus` + `nohz_full` + `rcu_nocbs` core isolation


