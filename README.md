# Trading Engine

A sub-millisecond, production-grade trading engine targeting **10M orders/sec**, built on the [LMAX architecture](https://martinfowler.com/articles/lmax.html) in Rust.

## Architecture

```
Clients ──TCP/UDS──> Accept Loop
                         │
                    Epoll Reader Pool (edge-triggered, non-blocking I/O)
                         │
                    lock-free MultiProducer ──> Input Disruptor (ring buffer)
                                                         │
                                          ┌──────────────┼──────────────────┐
                                          │                                 │
                                     Journal Thread                Matching Thread
                                     batch write + fsync           execute on Exchange
                                     (io_uring async fsync)        publish to output SPSC
                                          │                                 │
                                     advances cursor ────────┐              │
                                                             ▼              │
                                                      Response Thread  ◄───┘
                                                      gates on journal cursor
                                                      writes directly to sockets
                                                             │
                                                      ──TCP/UDS──> Clients
```

- **Zero tokio** — dedicated OS threads with blocking I/O; no async runtime anywhere in the codebase
- **Single-threaded matching engine** — no locks on the hot path; one thread executes all matching logic
- **LMAX disruptor pipeline** — 3 OS threads (journal, matching, response) on lock-free ring buffers; lock-free CAS-based multi-producer from reader pool; journal and matching run in parallel on the same events
- **Persist-before-ack** — responses are held until the journal confirms fsync, but matching proceeds in parallel with I/O
- **Batch fsync amortization** — under load, one fsync covers many events; optional io_uring async fsync overlaps I/O wait with encoding; `posix_fallocate` pre-allocates 64 MiB chunks so fsync only flushes data pages, not extent metadata
- **Event sourcing** — deterministic replay for crash recovery and audit; snapshots for fast restart
- **Mechanical sympathy** — cache-line-padded sequences, fixed-point pricing (no floats), zero allocations on the hot path

## Features

### Matching Engine
- Order types: Market, Limit, Stop, Stop-Limit
- Time-in-force: GTC, IOC, FOK
- Strict price-time priority (BTreeMap + VecDeque order book)
- Execution reports: Fill, Placed, Triggered, Cancelled, Rejected
- Multi-instrument exchange with shared account balances

### Event Sourcing
- Write-ahead journal with CRC32C checksums
- Batch journal I/O via disruptor ring buffer pipeline
- Pre-allocated storage (`posix_fallocate`) for reduced fsync latency
- Snapshot save/load for fast recovery
- Deterministic replay from journal

### Networking
- Custom binary wire protocol (length-prefixed framing)
- TCP transport with `TCP_NODELAY` and Unix domain socket transport
- Epoll reader pool (edge-triggered, non-blocking) with dedicated I/O threads (zero tokio)
- Transport abstraction (TCP/UDS now, io_uring/kernel bypass later)
- Typed client library
- Terminal UI for interactive testing

### Risk & Accounting
- Per-account, per-currency balance management
- Reserve on order, update on fill, release on cancel

## Build

```sh
cargo build          # compile
cargo run            # run server
cargo test           # run tests (126 tests across workspace)
cargo clippy         # lint
cargo fmt            # format
```

## Project Structure

```
crates/
├── disruptor/     Lock-free ring buffers (generic, no trading-domain knowledge)
├── engine/        Matching engine, order books, event sourcing, journal pipeline
├── protocol/      Binary wire protocol, transport abstractions, blocking I/O
├── server/        Server, pipeline orchestration, dedicated I/O threads
├── bench/         Pipelined end-to-end benchmark (TCP default, --uds flag)
├── client/        Typed client library
└── tui/           Terminal UI for interactive testing
```

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.
