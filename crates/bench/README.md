# Benchmark Suite

Latency and throughput benchmark for the trading engine with three modes, from bare matching engine to full network round-trip.

All modes use self-trade pairs (buy + sell at the same price, same account) for unlimited cycles with zero net balance change.

## Modes

### `--mode=engine`

Calls `Exchange::execute()` directly in a tight loop. No disruptor, no journal, no I/O. Measures pure matching engine throughput and per-order latency.

```sh
cargo run --release -p trading-bench -- --mode=engine 1000000
```

### `--mode=pipeline`

Builds the full disruptor pipeline (journal + matching stages on separate OS threads) but bypasses network transport. The bench thread publishes `InputSlot`s directly to the `MultiProducer` and drains `OutputSlot`s from the output SPSC queue. Isolates pipeline latency from TCP/UDS overhead.

```sh
cargo run --release -p trading-bench -- --mode=pipeline 1000000
cargo run --release -p trading-bench --features no-persist -- --mode=pipeline 1000000   # skip journal I/O
cargo run --release -p trading-bench --features no-fsync  -- --mode=pipeline 1000000    # journal writes, no fsync
```

### `--mode=roundtrip` (default)

Full end-to-end benchmark. Boots the server in-process, connects via TCP (default) or Unix domain socket, and measures client-perceived round-trip latency through the entire pipeline: transport, queuing, journaling, matching, and response dispatch.

```sh
cargo run --release -p trading-bench -- 1000000                          # TCP, default settings
cargo run --release -p trading-bench -- --uds 1000000                    # Unix domain socket
cargo run --release -p trading-bench -- --clients=32 --window=8 1000000  # 32 concurrent clients
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--mode=MODE` | `roundtrip` | Benchmark mode: `engine`, `pipeline`, or `roundtrip` |
| `--uds` | off | Use Unix domain socket instead of TCP (roundtrip only) |
| `--clients=N` | `1` | Number of concurrent client connections (roundtrip only) |
| `--window=N` | `64` | In-flight orders per client (roundtrip, pipeline) |
| `--bench-threads=N` | `4` | Epoll client threads (roundtrip only) |
| `--group-commit-us=N` | `0` | Journal fsync coalescing delay in microseconds (roundtrip, pipeline) |
| `<order_pairs>` | `1000000` | Number of order pairs (total orders = pairs x 2) |

## Feature Flags

| Feature | Effect |
|---------|--------|
| `no-fsync` | Skip fsync calls (journal still writes, but no durability guarantee) |
| `no-persist` | Skip all journal I/O (no writes, no fsync) |
| `io-uring` | Use io_uring for async fsync with group commit |
| `latency-trace` | Print per-stage latency histograms on shutdown |
| `pipeline-stats` | Print per-stage busy/idle utilization on shutdown |
