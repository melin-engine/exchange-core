# Trading Engine

A sub-millisecond, production-grade trading engine targeting **10M orders/sec**, built on the [LMAX architecture](https://martinfowler.com/articles/lmax.html) in Rust.

## Architecture

- **Single-threaded matching engine** — no locks on the hot path; I/O and journaling on separate threads via ring buffers
- **Event sourcing** — deterministic replay for crash recovery and audit
- **Mechanical sympathy** — cache-friendly data structures, zero allocations on the hot path, fixed-point pricing (no floats)

## Status

Early development. Core matching engine is functional:

- [x] Fixed-point price and quantity types (`NonZeroU64`-backed, niche-optimized)
- [x] Order types: Market, Limit, Stop, Stop-Limit
- [x] Time-in-force: GTC, IOC, FOK
- [x] Execution reports (Fill, Placed, Triggered, Cancelled, Rejected)
- [x] Order book (price-time priority, BTreeMap + VecDeque)
- [x] Matching engine with stop trigger logic
- [x] Multi-instrument exchange dispatcher
- [x] Account balance management (per-account, per-currency reserves)
- [ ] Event journal / recovery
- [ ] Risk checks (self-trade prevention, order throttling, position limits)
- [ ] Fuzz & property-based testing (`cargo-fuzz`, `proptest`)
- [ ] Gateway / network layer

## Build

```sh
cargo build
cargo test
cargo clippy
```

## License

Copyright (c) 2026 Pierre Larger. All Rights Reserved.
