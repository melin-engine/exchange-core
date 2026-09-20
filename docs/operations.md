# Operations Runbook

Production operations guide for the trading engine. Written for the person running the server at 3 AM.

---

## Table of Contents

1. [Server Startup](#server-startup)
2. [Output Event Channel](#output-event-channel)
3. [Recovery on Startup](#recovery-on-startup)
4. [Journal Management](#journal-management)
5. [Scheduled Snapshots](#scheduled-snapshots)
6. [Log Levels](#log-levels)
7. [CPU Tuning](#cpu-tuning)
8. [Monitoring](#monitoring)
9. [Emergency Procedures](#emergency-procedures)
10. [Crash Recovery Scenarios](#crash-recovery-scenarios)
11. [Disk Failure](#disk-failure)
12. [Capacity Planning](#capacity-planning)

---

## Server Startup

### Binary

```sh
cargo build --release
./target/release/melin-ec-server [OPTIONS]
```

The server uses jemalloc by default (thread-local caches eliminate allocator lock contention).

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `127.0.0.1:9876` | TCP address to bind. Use `0.0.0.0:9876` for LAN access. |
| `--journal` | `melin.journal` | Path to the journal file. Use a dedicated NVMe for best latency. |
| `--snapshot` | (derived) | Path to the snapshot file. If omitted, defaults to `<journal>.snapshot` (e.g., `melin.snapshot`). |
| `--authorized-keys` | `authorized_keys` | Path to the Ed25519 authorized keys file. Every connection must authenticate before trading. Ignored in replica mode (`--replica-of`). |
| `--cores` | `journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11` | One `thread=core` entry per pipeline thread, in any order; every thread must be named. Each entry takes an optional wait-policy suffix: a bare core (or `7s`) busy-spins and needs the core to itself, `7y` spins briefly then yields and may share it; `journal-prep` blocks in file I/O and takes no suffix. `0` leaves a thread unpinned (and therefore yielding), and `none` unpins every thread. Core 0 should be reserved for OS/IRQ. The server refuses to start on a positional list, a missing, unknown or repeated name, or two threads on one core unless both entries carry `y`, and warns at boot when `journal-seq`, `matching`, `response`, `reader` or `journal-disk` has no core. See [Core Pinning](#core-pinning---cores). |
| `--max-journal-mib` | `256` | Live journal size in MiB above which the segment is archived and a fresh live file opens. Rotation runs online at the journal stage's fsync boundary. Set to `0` to disable. |
| `--max-journal-batch` | `4096` | Maximum events per journal fsync batch. Smaller values reduce tail latency; larger values improve throughput. |
| `--group-commit-us` | `0` | Group commit coalescing delay in microseconds. Keep at `0` for TCP transport. Only useful with UDS (see CLAUDE.md). |
| `--accounts` | `100000` | Number of accounts the node reserves memory for on every start, primary or replica. A build with the `synthetic-seed` feature also provisions that many funded test accounts on a fresh journal; see [Starting empty](#starting-empty). |
| `--instruments` | `100` | Number of instruments the node reserves memory for on every start. A build with the `synthetic-seed` feature also registers that many placeholder instruments on a fresh journal. |
| `--max-orders-per-account` | `10000` | Maximum simultaneously open orders per account (resting limits plus pending stops); beyond it, submissions reject with `ExceedsMaxOpenOrders`. `0` = unlimited. See [Per-account limits](#per-account-limits) for when a node's value takes effect. |
| `--max-orders-per-second` | `1000` | Per-account sustained order rate; beyond it, submissions reject with `ExceedsOrderRate`. `0` disables the limiter. See [Per-account limits](#per-account-limits). |
| `--max-orders-burst` | `5000` | Per-account burst allowance for the order-rate limiter. `0` disables the limiter. See [Per-account limits](#per-account-limits). |
| `--heartbeat-interval-secs` | `10` | Seconds between heartbeats to idle connections. `0` to disable. |
| `--connection-timeout-secs` | `30` | Seconds before disconnecting silent clients. `0` to disable. |
| `--max-connections` | `1024` | Maximum concurrent authenticated connections. `0` for unlimited. Rejects new connections at the limit. |
| `--journal-staging-mode` | `zero-fill` | How the background preparer stages the next journal segment. `zero-fill` pre-writes it, so appends never carry filesystem-metadata cost; `allocate` only reserves it, costing no device bandwidth while staging. Consider `allocate` on network-attached volumes (e.g. EBS), where the pre-write competes with the hot path for metered bandwidth. Which mode wins is a property of the volume; measure both. |
| `--health-bind` | `127.0.0.1:9878` | Address for the health/liveness TCP endpoint. Returns `OK\|ERR <conns> <seq> <lag>`. Served by primaries and replicas alike, so a primary and a replica on one host need distinct binds. Omit to disable. |
| `--event-bind` | (none) | Address for the output event publisher. Subscribers connect to receive all execution events in real time (market data, fills, cancellations). Ed25519 auth required. Omit to disable. See [Output Event Channel](#output-event-channel). |
| `--snapshot-interval-ms` | `3_000_000` (50 min) | Interval in milliseconds between snapshots written by the shadow exchange — the sole snapshot writer. Set to `0` to disable; recovery then falls back to full journal replay. The shadow replays events on a dedicated thread, so snapshot writes never pause the primary matching engine. See [Scheduled Snapshots](#scheduled-snapshots). |
| `--snapshot-path` | (derived) | Path for snapshot files. Defaults to journal path with `.snapshot` extension. **Recommended: place on the OS disk, not the journal NVMe, to avoid I/O jitter on the hot path.** |

#### Starting empty

A release build starts with no instrument and no account. On a fresh journal there is nothing to trade until an operator creates both, which is done at runtime through the admin client (`melin-ec-admin`): register each instrument, then provision the accounts.

Development builds can seed instead. The `synthetic-seed` build feature registers `--instruments` placeholder instruments and provisions `--accounts` accounts, each funded in every currency out of nothing, as the first events of a fresh journal — the fixture the benches and smoke tests trade from:

```sh
cargo build --release -p melin-ec-server --features synthetic-seed
```

The feature is off by default so that a production binary cannot create balances. A node built with it logs a warning at startup when it seeds, and the seed is permanent: it is journaled, replicated, and carried in every snapshot, so a journal created by a seeded build keeps those funded accounts for its lifetime. Never point one at a production journal.

#### Per-account limits

The three per-account limits (`--max-orders-per-account`, `--max-orders-per-second`, `--max-orders-burst`) are recorded in the journal, not read locally by every node. Each time a node becomes primary — at startup, and when it is promoted after a failover — it records its own flag values before serving its first client, and from then on those are the limits in force on every node:

- **A replica enforces the primary's limits, not its own flags.** Its flags matter only once it is promoted, and take effect then.
- **Changing a limit takes a primary restart or a failover.** Restart the primary with the new values, or promote a replica started with them. The change applies from that point on; decisions already made keep the limits they were made under, including on replay.
- **Nodes no longer need identical values to stay consistent.** Different values across nodes are safe. They just mean the limits change at the next failover, so keep them aligned unless that is what you want.

Changing the rate or burst while the limiter is active resets every account's order-rate allowance to a full burst, as any change of these values always has.

#### Replication Flags

The server supports synchronous replication. Exactly one of `--replication-bind`, `--standalone`, or `--replica-of` determines the replication mode. If none is specified, the server runs in implicit standalone mode (replication cursor at `u64::MAX`, responses gated only by the journal).

Under the default `disk+ram` ack policy the response stage releases an acknowledgement once the event is synced to the journal on at least one node — whichever of the primary or a replica finishes first — and held in memory on at least two. `ram` drops the sync requirement, taking NVMe fsync tail variance off the critical path entirely; `two-disks` requires a sync on two nodes. See `--ack-policy` below for the full menu.

> **Upgrading from a release before 0.15:** this flag was `--durability-mode`, and its values were `local`, `replicated`, `hybrid` and `durably-replicated`. It is now `--ack-policy` with `disk`, `ram`, `disk+ram` and `two-disks` respectively. The guarantee behind each is unchanged — only the names are — but a server started with the old flag will refuse to launch, so update your unit files and launch scripts before rolling out. Two related names changed with it: the admin `DURABILITY` command is now `ACK-POLICY` (see `--admin-bind`), and the `melin_durability_policy_degraded` gauge and `melin_durability_policy_degraded_seconds_total` counter are now `melin_ack_policy_degraded` and `melin_ack_policy_degraded_seconds_total`. Both old spellings still work in 0.15 but are removed in the next release — update runbooks and alert rules now, so a failover or an alert doesn't turn out to depend on a name that no longer exists.

| Flag | Default | Description |
|------|---------|-------------|
| `--replication-bind` | (none) | Address to listen for replica connections (enables primary mode with synchronous replication). |
| `--standalone` | `false` | Disable replication entirely (dev/test). Sets the replication cursor to `u64::MAX` so responses are gated only by the journal. |
| `--replica-of` | (none) | Run as a replica connected to the given primary address. The server does not accept client connections in this mode. |
| `--replication-key` | (none) | Path to the Ed25519 private key for replication authentication. Required when `--replica-of` is set. The corresponding public key must be in the primary's `authorized_keys` with `replication` permission. |
| `--replication-batch-size` | `32` | Maximum replication ring batches to coalesce into a single TCP write+flush. Higher values reduce syscall overhead but increase per-write latency. |
| `--replication-heartbeat-secs` | `5` | Seconds between primary-to-replica heartbeats. Used for disconnect detection. |
| `--replication-ring-size` | `256` | Slots in the replication ring buffer (must be power of two). Each slot holds up to 512 KiB. More slots = more buffering before the journal stage backpressures. Default: 256 (128 MiB). See [Replication Ring Sizing](#replication-ring-sizing). |
| `--ack-policy` | `disk+ram` | What a client acknowledgement guarantees. `disk`: synced to the journal on one node; single-node durability, required with `--standalone`. `ram`: held in memory on two nodes before the ack, with every journal sync trailing off the ack path (the journal still syncs every batch); lowest ack latency, survives any single node failure via failover, loses only the un-synced tail on a whole-cluster power loss — for slow-sync storage such as cloud volumes. `disk+ram` (default): synced on one node — whichever finishes first — **and** held in memory on two; single-failure-safe with a brief RAM-only window on the second copy, ~50–80 µs per fill faster than `two-disks`. `two-disks`: synced on two nodes before the ack; no RAM-only window — compliance-driven venues. In every policy but `disk` the gate stalls while no replica is connected. Named `--durability-mode` before 0.15. See the sequencer's [replication guide](https://github.com/melin-engine/melin/blob/main/docs/replication.md) for the operational menu. |
| `--admin-bind` | (none) | Address for the operator admin endpoint. Authenticated with operator keys; accepts `PROMOTE\n` (replica → primary, replica nodes only), `ROTATE\n` (archive the live journal segment — primaries only; replicas rotate where the primary announces) and `ACK-POLICY <policy>\n` (switch the ack policy at runtime without a restart, taking the same values as `--ack-policy` — on a primary or a promoted replica; the usual use is dropping a freshly promoted node that has no replicas yet to `disk` so it can acknowledge orders, then restoring the target policy once replicas reattach). |

### Startup Sequence

1. Load authorized keys from `--authorized-keys`.
2. Initialize or recover the exchange (see [Recovery on Startup](#recovery-on-startup)).
3. Reserve the exchange's memory and pre-fault it (avoids growth and page faults on the hot path). Every node reserves the same production capacity before its first event; the balance map alone is sized from `--accounts` and `--instruments`.
4. Build the disruptor pipeline (input ring + output ring).
5. Spawn I/O thread: in TCP mode, one io_uring reader thread that multiplexes every connection via multishot RECV; in DPDK mode, one poll thread per NIC queue.
6. Spawn the pipeline OS threads: journal-seq, journal-disk, journal-prep, matching, response, optionally event-publisher, optionally the shadow exchange, and the replication handlers when replication is on -- each pinned to its `--cores` entry.
7. Set listener to non-blocking mode.
8. Enter accept loop, authenticating connections via Ed25519 challenge-response.

### Minimal Production Launch (Standalone)

```sh
./target/release/melin-ec-server \
    --bind 0.0.0.0:9876 \
    --health-bind 0.0.0.0:9878 \
    --journal /mnt/nvme/melin.journal \
    --authorized-keys /etc/melin/authorized_keys \
    --cores journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11 \
    --max-journal-mib 512 \
    --standalone
```

### Production Launch with Replication

```sh
# Primary
./target/release/melin-ec-server \
    --bind 0.0.0.0:9876 \
    --health-bind 0.0.0.0:9878 \
    --journal /mnt/nvme/melin.journal \
    --authorized-keys /etc/melin/authorized_keys \
    --cores journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11 \
    --max-journal-mib 512 \
    --replication-bind 0.0.0.0:9877

# Replica (separate machine)
./target/release/melin-ec-server \
    --journal /mnt/nvme/melin.journal \
    --cores journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11 \
    --replica-of <primary-ip>:9877 \
    --replication-key /etc/melin/replication.key
```

## Output Event Channel

The event channel provides a real-time firehose of all execution events (fills, placements, cancellations, stats) to TCP subscribers. Enable it with `--event-bind`:

```sh
./target/release/melin-ec-server \
    --bind 0.0.0.0:9876 \
    --health-bind 0.0.0.0:9878 \
    --event-bind 0.0.0.0:9879 \
    --journal /mnt/nvme/melin.journal \
    --authorized-keys /etc/melin/authorized_keys \
    --cores journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11 \
    --standalone
```

When `--event-bind` is omitted, the output ring has a single consumer (the response stage) — identical to before, zero overhead.

### How it works

The matching stage publishes to an output disruptor ring. Without `--event-bind`, the ring has one consumer (response stage). With it, the builder adds a second consumer for the event publisher thread:

```
Matching Stage
    │
    │ ring::Producer::publish()
    ▼
Output Disruptor Ring (1M slots, multi-consumer)
    ├──► Consumer 0: Response Stage (per-client, gated on journal+repl cursors)
    └──► Consumer 1: Event Publisher (TCP broadcast to all subscribers)
```

Both consumers are parallel. The producer is gated on the **slowest** consumer. In practice, the response stage (which waits for journal fsync) will always be the bottleneck — the event publisher does non-blocking writes with no durability gating, so it runs faster.

### Subscriber protocol

Subscribers connect to the `--event-bind` port and authenticate with the standard Ed25519 challenge-response handshake (same as the main trading port). Any permission level (ReadOnly or above) is accepted.

After auth, the server sends a continuous stream of frames:

```
| ring_sequence (u64 LE) | length (u32 LE) | tag (u8) | payload (var) |
```

- **ring_sequence**: Monotonically increasing output ring sequence. Subscribers can detect gaps (missed events) if their last-seen sequence jumps by more than 1.
- **length + tag + payload**: Standard response codec (same as the per-client response frames). Decodable with the `melin-ec-protocol` crate's `codec::decode_response()`.

Every event the matching stage produces appears on the event channel — fills, placements, cancellations, batch-end markers, stats snapshots, and engine errors. There is no filtering; subscribers receive the full firehose.

### Slow subscriber policy

The event publisher uses non-blocking TCP writes. If a subscriber's TCP send buffer is full (the subscriber isn't reading fast enough), the publisher disconnects it immediately rather than blocking. This prevents a slow subscriber from backpressuring the entire pipeline.

Design your subscribers to read as fast as the publisher writes. If your subscriber does any processing, decouple ingestion from processing with an internal buffer.

### Failure mode

If the event publisher thread dies (panic), the server detects it in the accept loop's health check and initiates a full shutdown. This is necessary because a dead consumer stops advancing its ring progress counter, which would eventually cause the matching stage to backpressure and stall.

### When to use

| Use case | Description |
|----------|-------------|
| Market data gateway | Build L2/L3 order book snapshots, BBO feeds, trade tapes from the event stream |
| Audit logger | Write all execution events to a separate audit database or file for regulatory compliance |
| Analytics service | Real-time throughput counters, latency histograms, volume analytics |
| Monitoring | External health checks that verify events are flowing |

### When NOT to use

- **The submitting client already gets responses** via the response stage. The event channel is for *third-party observers*, not for the trading client itself.
- **For replay/recovery** use the journal file, not the event stream. The journal is the authoritative record.

---

## Recovery on Startup

The server automatically detects and handles all recovery scenarios. No manual intervention is needed for normal restarts.

### Decision Tree

The `init_engine` function checks the following conditions in order:

1. **Snapshot exists AND journal exists**: Recover from snapshot, then replay only journal entries after the snapshot's sequence number. This is the fast path -- avoids replaying the full history from genesis.

2. **Snapshot exists AND live journal is missing (no archives either)**: Loads the snapshot and creates a fresh journal continuing from the snapshot's sequence number. Logs: `recovering from snapshot only (journal missing)`. If archives are present, recovery falls into case 1 — it walks them from the snapshot's sequence and synthesizes a continuing live file if needed.

3. **Journal exists (no snapshot)**: Full replay from genesis. Every event in the journal is replayed to reconstruct exchange state.

4. **Neither exists**: Fresh start. Creates a new journal, empty — unless the node was built with `synthetic-seed`, which seeds test data per `--accounts` and `--instruments`. See [Starting empty](#starting-empty).

### Post-Recovery Rotation Check

After recovery, if `--max-journal-mib` is set (default 256) and the live segment exceeds that threshold, the server archives the segment to its next monotonic slot and opens a fresh live file before the pipeline starts. No snapshot is taken at this point — the shadow stage owns snapshot writes on its own cadence.

The same size-trigger also runs online during normal operation at the journal stage's fsync boundary, so the server doesn't depend on restarts to bound disk usage.

### Recovery Time

Recovery time is proportional to the number of journal entries replayed. With the shadow snapshot writer enabled (default), only entries since the last shadow snapshot are replayed. At ~80 bytes per event:

- 256 MiB segment = ~3.2M events to replay
- With shadow snapshot: only events since the last snapshot interval (typically minutes of traffic at default 50-minute cadence)

---

## Journal Management

### How Writes Reach Disk

The journal writes each batch with `pwrite` followed by `fdatasync`, so an acknowledged event is durable on any drive, with or without power-loss protection. The work is split across two threads: the journal stage orders and encodes events and feeds replicas, while a dedicated disk thread writes, syncs, and publishes the durable position that client responses wait on. The split keeps device latency off the sequencing thread; it does not change what "durable" means.

### How Rotation Works

Rotation runs online at the journal stage's fsync boundary. On primaries (and standalone nodes) two independent triggers fire it: the live segment crossing `--max-journal-mib` (default 256 MiB), or an operator `ROTATE` admin command. The boot path additionally checks the on-disk segment size on startup and rotates once before opening the pipeline if needed. Replicas have no local triggers — they rotate exactly where the primary announces over the replication stream, keeping their journals byte-for-byte mirrors of the primary's (see the sequencer's [replication guide](https://github.com/melin-engine/melin/blob/main/docs/replication.md)).

Each rotation is a single rename of the live file to its next monotonic archive slot, followed by opening a fresh live file that continues the sequence and BLAKE3 hash chain. No snapshot is written at rotation — snapshots are produced exclusively by the shadow exchange on its own cadence.

### Archive Naming

```
melin.journal           <-- current (active)
melin.journal.000001    <-- oldest archive
melin.journal.000002    <-- next archive
melin.journal.000003    <-- ...
```

Archive numbers are assigned monotonically and never renamed — each rotation is a single rename of the live file to the next free number. Archived journals are preserved indefinitely for audit purposes.

Snapshots (`melin.snapshot`, with `melin.snapshot.prev` as a one-deep rollback target) are written and rotated by the shadow exchange independently of journal rotation.

### Disk Space Planning

**Journal growth rate**: ~80 bytes per event (entry header + payload + CRC32C).

| Throughput | Per Hour | Per Day | Per Week |
|-----------|----------|---------|----------|
| 100K orders/sec | ~28 GiB | ~672 GiB | ~4.6 TiB |
| 1M orders/sec | ~280 GiB | ~6.7 TiB | ~47 TiB |
| 5M orders/sec | ~1.4 TiB | ~33 TiB | ~235 TiB |

The journal writer pre-allocates in 256 MiB chunks (`posix_fallocate`) to avoid filesystem metadata overhead during writes. The chunk size matches the default rotation threshold so a freshly created journal never needs mid-run extension. The on-disk file size will be larger than the valid data by up to one chunk.

**Action items**:

- Set `--max-journal-mib` to trigger rotation before disk fills. The default of 256 MiB is conservative.
- Periodically archive or delete old `.journal.N` files. They are only needed for audit replay with the matching engine version that produced them.
- Monitor disk free space. If the journal disk fills, writes will fail and the server will log errors but continue running (see [Disk Failure](#disk-failure)).

---

## Scheduled Snapshots

### Architecture

When `--snapshot-interval-ms` is non-zero (default: 3,000,000 — 50 minutes), the server spawns a **shadow exchange** on a dedicated thread. The shadow is the sole snapshot writer in the system. It is a third consumer on the input disruptor ring, gated on the journal cursor (it only processes events after the journal has confirmed durability), replays every event through its own independent copy of the exchange state, and periodically saves a snapshot to disk.

```
Input Disruptor Ring
    ├──► Consumer 0: Journal Stage (pwritev2 + RWF_DSYNC)
    ├──► Consumer 1: Matching Stage (primary Exchange)
    └──► Consumer 2: Shadow Stage (shadow Exchange, gated on journal cursor)
                          │
                          ▼
                    Periodic snapshot save
                    (every --snapshot-interval-ms)
```

### How It Works

1. The shadow stage consumes events from the input disruptor, always behind the journal cursor, ensuring it only processes durable events.
2. It replays each event through its own `Exchange` instance, maintaining identical state to the primary.
3. At each configured interval, the shadow exchange saves its state to the snapshot file. The snapshot is written atomically (`.tmp` + fsync + rename). Before each rename, the previous snapshot is rotated to `.snapshot.prev` as a rollback point.
4. Between snapshot writes, the shadow catches up to the current journal position before the next snapshot interval fires.

### Zero Impact on Matching Engine

The shadow exchange runs on its own dedicated core and has no interaction with the primary matching engine. It reads from the same input ring buffer but through its own independent cursor. The primary matching stage is never blocked or slowed by the shadow — they are fully parallel consumers.

The only state shared between stages is the BLAKE3 chain hash, published by the journal stage via a `SeqLock` after each fsync batch. The shadow reads it when taking a snapshot — one lock-free read per snapshot, no contention.

### Snapshot Placement

**Recommended: place the snapshot file on the OS disk, not the journal NVMe.** The snapshot write is a bulk I/O operation (tens of MiB) that could cause I/O jitter on the journal NVMe if co-located. Use `--snapshot-path` to specify an explicit path on a separate disk:

```sh
./target/release/melin-ec-server \
    --journal /mnt/nvme/melin.journal \
    --snapshot-path /var/lib/melin/melin.snapshot \
    --snapshot-interval-ms 60000 \
    --cores journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11 \
    ...
```

If `--snapshot-path` is omitted, the snapshot defaults to `<journal>.snapshot` (e.g., `/mnt/nvme/melin.snapshot`), which shares the journal NVMe.

### Snapshot Rotation

Each snapshot save rotates the previous snapshot to `<path>.prev` before writing the new one. This gives operators a one-deep rollback point:

- `melin.snapshot` — the latest snapshot (most recent interval).
- `melin.snapshot.prev` — the previous snapshot (one interval older).

If the latest snapshot is corrupt or contains undesired state (e.g., bad market data caused incorrect fills), operators can recover from the `.prev` snapshot by renaming it back:

```sh
mv melin.snapshot.prev melin.snapshot
```

The rotation is best-effort: if the `.prev` rename fails (e.g., permission error), the save proceeds anyway — losing the rollback point is preferable to failing the snapshot entirely. The server logs a warning when this happens. On first save after startup, there is no previous snapshot to rotate, so no `.prev` file is created.

#### Recovery from a Crash Mid-Save

If the server crashes between the `.prev` rotation and the final `.tmp → .snapshot` rename, the snapshot directory can be left with `melin.snapshot.prev` present, `melin.snapshot.tmp` present, and `melin.snapshot` missing. On the next startup, recovery from a fixed `<path>` fails because the path does not exist. Two manual recovery paths are available, depending on which copy you trust:

```sh
# Promote the rotated rollback target (loses the most recent save).
mv melin.snapshot.prev melin.snapshot

# Or accept the in-progress write (it was fully written and fsynced
# before the rotation; the rename is what didn't complete).
mv melin.snapshot.tmp melin.snapshot
```

Both files are complete, CRC-checked snapshots and will load normally. Pick whichever matches the journal state you intend to resume from.

### Catch-Up Behavior

After each snapshot write (which may take tens of milliseconds for a large exchange state), the shadow stage falls behind the journal cursor. It catches up by processing events at full speed until it reaches the current journal position. Under sustained high throughput, the shadow may never be fully "caught up" — it continuously trails the primary by some lag. This is expected and harmless; the snapshot reflects a consistent point-in-time state that is always more recent than the previous snapshot.

### Failure Mode

If the shadow thread panics or dies, the server detects this via the pipeline health check and initiates a **full shutdown**. This is necessary because a dead consumer on the input disruptor stops advancing its ring progress counter, which would eventually cause the ring to fill and stall the journal and matching stages.

If scheduled snapshots are not critical to your deployment, you can disable the shadow with `--snapshot-interval-ms 0` to eliminate this failure mode entirely. With the shadow disabled, no snapshots are written and recovery falls back to full journal replay from genesis (across all archived segments).

---

## Log Levels

Log output uses `tracing` with the `RUST_LOG` environment variable. The conventions are strict:

### `error` -- Server bugs and I/O failures only

Must never fire due to bad client input or client network issues. If you see an `error` log, something is wrong with the server itself.

Examples:
- `journal encode error` -- failed to encode a journal entry
- `journal flush_batch_sync error` -- fsync failed (disk problem)
- `accept error` -- listener socket error

**Action**: Investigate immediately. These indicate hardware failure, bugs, or resource exhaustion.

### `warn` -- Degraded operation

Not a bug, but needs attention. The server is still running but operating in a degraded state.

Examples:
- `core pinning failed` -- thread affinity could not be applied (performance impact)
- `connection rejected: max_connections reached` -- at the connection limit, new clients turned away
- `replica disconnected` -- replication link lost, degraded to local-only durability
- `replica connection error` -- replication connection failed
- `built with the synthetic-seed feature` -- this node is seeding funded test accounts into a fresh journal; not a production build

**Action**: Investigate promptly. These indicate resource pressure or infrastructure issues that could escalate.

### `info` -- Server lifecycle events

Normal operational events. Safe to monitor in production.

Examples:
- `loaded authorized keys` -- startup
- `recovering from snapshot + journal` -- recovery path taken
- `journal exceeds threshold, rotating` -- automatic rotation
- `listening` -- ready to accept connections
- `pinned to core` -- thread affinity applied
- `shutdown signal received` / `shutdown complete` -- orderly shutdown

### `debug` -- Client-caused events

High-volume in production. Enable only for debugging specific issues.

Examples:
- `new connection` -- client connected
- `authenticated` -- client passed auth
- `auth failed, dropping` -- bad credentials
- `failed to set auth timeout` -- socket option issue

### Configuration

```sh
# Production: info level (default)
RUST_LOG=info ./target/release/melin-ec-server ...

# Debugging client issues:
RUST_LOG=debug ./target/release/melin-ec-server ...

# Debugging specific crate:
RUST_LOG=melin_ec_server=debug,melin_ec=info ./target/release/melin-ec-server ...
```

---

## CPU Tuning

### Core Layout

The recommended core assignment for a production server:

| Core(s) | Assignment | `--cores` name |
|---------|-----------|------|
| 0 | OS, IRQs, RCU callbacks | (reserved, never assign pipeline work) |
| 1 | Journal sequencing thread | `journal-seq` |
| 2 | Matching stage | `matching` |
| 3 | Response stage | `response` |
| 4 | Reader thread (io_uring / DPDK poll) | `reader` |
| 5 | Unused by the default layout | -- |
| 6 | Event publisher | `event-publisher` |
| 7 | Shadow exchange (scheduled snapshots) | `shadow` |
| 8 | Replication handler 0 | `repl-handler-0` |
| 9 | Replication handler 1 | `repl-handler-1` |
| 10 | Journal segment preparer | `journal-prep` |
| 11 | Journal disk thread (writes, syncs, publishes durability) | `journal-disk` |
| 12+ | Available for other work (benchmarks, monitoring) | -- |

### Core Pinning (`--cores`)

Each pipeline thread pins itself to the core `--cores` gives it before entering its loop. If pinning fails, a warning is logged but the server continues.

`--cores` takes one `thread=core` entry per pipeline thread, in any order, and every thread must be named: `journal-seq`, `matching`, `response`, `reader`, `event-publisher`, `shadow`, `repl-handler-0`, `repl-handler-1`, `journal-prep` and `journal-disk`. A thread no other flag enables — the event publisher without `--event-bind`, the replication handlers on a standalone node — still takes an entry, so a layout reads the same whatever the node is doing. `0` leaves a thread unpinned, and `none` unpins every thread. The server refuses to start on a positional list, or on a missing, unknown or repeated name, and says which. To migrate a positional value, name the positions and drop the fifth: `1,2,3,4,0,6,7,8,9,10,11` becomes `journal-seq=1,matching=2,response=3,reader=4,event-publisher=6,shadow=7,repl-handler-0=8,repl-handler-1=9,journal-prep=10,journal-disk=11`, which is also the default. A nine- or ten-entry list adds `journal-prep=0` and `journal-disk=0` for the threads it left out, and an all-`0` list is `none`.

Each entry also states how the thread waits when it has nothing to do. A bare core (`7`, or `7s`) busy-spins and needs the core to itself. `7y` spins briefly, then yields to the scheduler, and may share the core with other yielding threads. `0` leaves the thread unpinned, which always yields. `journal-prep` blocks in file I/O rather than polling and takes no suffix. The server refuses to start when two entries name the same core and either thread busy-spins; the check covers every entry, including threads no other flag enables, and the error names both threads and the core. On DPDK the `reader` entry pins the NIC poll thread, which never yields, so it cannot take `y` — give it a core of its own. The boot log prints the resolved layout in `--cores` syntax.

An unpinned thread runs on the CPUs the server process was started with: the set that `taskset`, systemd's `CPUAffinity=` or a container cpuset narrows, and one that never includes an isolated core. Without isolated cores the scheduler moves it as load changes; with them, it shares the non-isolated cores with the OS and interrupt handling. `journal-seq`, `matching`, `response`, `reader` and `journal-disk` are on the path of every request, so the server warns at boot when any of them has no core, and calls `none` out as what it is: a development layout, under which latency figures say nothing about the server.

On a shared machine where every thread should yield, suffix every pinned entry: `--cores journal-seq=1y,matching=2y,response=3y,reader=4y,event-publisher=6y,shadow=7y,repl-handler-0=8y,repl-handler-1=9y,journal-prep=10,journal-disk=11y`. This replaces the former `--yield-idle` flag, which was removed; a `none` layout needs no change. A mixed layout keeps the acknowledgement path spinning on dedicated cores and packs the auxiliary threads onto one shared core: `--cores journal-seq=1,matching=2,response=3,reader=4,journal-disk=5,repl-handler-0=6,repl-handler-1=7,event-publisher=8y,shadow=8y,journal-prep=8` puts journal-seq, matching, response, reader and journal-disk on cores 1-5, the two replication handlers on 6 and 7, and the event publisher, shadow and segment preparer together on core 8. The replication handlers are on the acknowledgement path whenever the ack policy waits on a replica, so only a standalone node should treat them as auxiliary.

`journal-prep` is the journal segment preparer, which stages the next journal segment in the background so rotation doesn't stall the journal stage. `journal-disk` is the journal disk thread — the half of the journal that writes each batch, syncs it, and publishes the durability position every acknowledgement waits on. It busy-spins like the other stages, so give it a dedicated core, and keep it on the same CCD as `journal-seq`: the two exchange a cache line on every batch. Running either unpinned is a stated choice (`journal-disk=0`), and the disk thread is the costlier one to leave without a core: it syncs on every batch, so sharing a core with the OS puts that contention directly under the durability position clients wait on. The default layout takes cores 1-4 and 6-11 — budget for it when planning the layout.

### Kernel Boot Parameters (GRUB)

For lowest latency, configure kernel boot parameters. Add them through a drop-in rather than by editing `/etc/default/grub` directly — `grub-mkconfig` sources every `.cfg` in `/etc/default/grub.d` after the vendor file, so a drop-in survives image updates and cannot be silently lost:

```sh
sudo mkdir -p /etc/default/grub.d
printf 'GRUB_CMDLINE_LINUX="${GRUB_CMDLINE_LINUX} isolcpus=nohz,domain,1-9 nohz_full=1-9 rcu_nocbs=1-9"\n' | sudo tee /etc/default/grub.d/99-melin-manual.cfg
```

Append to `GRUB_CMDLINE_LINUX`, not `GRUB_CMDLINE_LINUX_DEFAULT`. Only the former is guaranteed to be defined — several hosting images ship without a `GRUB_CMDLINE_LINUX_DEFAULT` line at all, so an edit targeting it silently does nothing — and `GRUB_CMDLINE_LINUX` applies to the recovery entry too.

Note the filename. `scripts/server-setup.sh` owns `99-melin-ec-bench.cfg` in the same directory and rewrites it from scratch on every run, so anything you put there is lost the next time the script is used. Keep manual tuning in a drop-in of your own; both are sourced, and the parameters combine. If you are running `server-setup.sh` on this host, prefer editing its `KERNEL_PARAMS` list to hand-writing a second file — it applies a wider set of parameters than the three above and verifies afterwards that each one actually reached the boot config.

Then apply:

```sh
sudo update-grub
sudo reboot
```

Confirm the parameters actually reached the generated config *before* rebooting — this is the step that catches a lost edit:

```sh
grep -c isolcpus /boot/grub/grub.cfg    # must be non-zero
```

What each parameter does:

- **`isolcpus=nohz,domain,1-9`**: Removes cores 1-9 from the scheduler's load balancing and timer tick distribution. Only explicitly pinned threads run on these cores.
- **`nohz_full=1-9`**: Stops the timer tick on cores 1-9 when only one task is running. Eliminates ~1–10 µs jitter every 4ms (HZ=250).
- **`rcu_nocbs=1-9`**: Moves RCU callback processing off cores 1-9. Without this, RCU grace periods can still interrupt isolated cores.

Verify after reboot:

```sh
cat /sys/devices/system/cpu/isolated      # should print: 1-9
cat /sys/devices/system/cpu/nohz_full     # should print: 1-9
grep rcu_nocbs /proc/cmdline              # should show rcu_nocbs=1-9
```

To revert, remove the drop-in — the vendor's `/etc/default/grub` was never modified:

```sh
sudo rm /etc/default/grub.d/99-melin-manual.cfg && sudo update-grub && sudo reboot
```

On a host provisioned by `scripts/server-setup.sh`, remove `99-melin-ec-bench.cfg` the same way to undo the parameters it applied.

### Runtime Tuning (bench-isolate.sh)

The `scripts/bench-isolate.sh` script applies runtime tuning that does not require a reboot. It must run as root and automatically restores settings on exit:

1. **CPU governor**: Sets all cores to `performance` (locks max frequency, no scaling transitions).
2. **NMI watchdog**: Disables it (eliminates periodic non-maskable interrupts).
3. **IRQ affinity**: Pins all hardware interrupts to core 0.
4. **irqbalance**: Stops the daemon to prevent it from redistributing IRQs.

```sh
sudo ./scripts/bench-isolate.sh [bench args]
```

For production, apply these settings permanently. `scripts/server-setup.sh` installs the governor and IRQ pinning as `systemd` oneshot units (`melin-cpu-governor`, `melin-irq-pin`) so they survive reboots; the equivalent by hand is:

```sh
# CPU governor (install as a systemd unit, not a one-off shell loop)
for gov in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance > "$gov"
done

# NMI watchdog
echo 0 > /proc/sys/kernel/nmi_watchdog

# IRQ affinity (pin all IRQs to core 0)
for f in /proc/irq/*/smp_affinity; do
    echo 1 > "$f" 2>/dev/null
done

# Disable irqbalance
systemctl disable --now irqbalance
```

Set the governor at runtime even when `cpufreq.default_governor=performance` is on the kernel command line. That parameter only applies to policies created after it is parsed, and it does nothing at all on a host that has not yet rebooted into the tuned command line — so the two mechanisms cover each other. Check what is actually in effect with `cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor`; on `amd-pstate-epp` and `intel_pstate` hosts also confirm `energy_performance_preference` reads `performance`.

### Compact Layout for Smaller Hosts

The default core layout above assumes 11+ logical CPUs — i.e., a box where cores 1-10 are real physical cores and core 0 is reserved for OS work. On 8-core / 16-thread workstations and entry-level servers, cores 7-9 are hyperthread siblings of cores 0-2, so pinning the shadow / replication-handler threads there forces them to share execution units with the hot pipeline cores (journal, matching). Throughput collapses by 5-10x in that situation because the busy-spinning pipeline threads starve their own HT siblings.

For embedded benchmark mode (`melin-ec-bench --mode roundtrip`), the bench auto-detects host size and switches to a compact layout that fits inside 8 logical cores: journal-seq=1, matching=2, response=3, reader=4, event-publisher=5, shadow=6, bench client=7. The replication handlers, the segment preparer and the journal disk thread are left unpinned (replication is not used in embedded bench mode).

For production deployments on smaller hosts, pass the equivalent `--cores journal-seq=1,matching=2,response=3,reader=4,event-publisher=5,shadow=6,repl-handler-0=0,repl-handler-1=0,journal-prep=0,journal-disk=0` and accept that any non-pipeline work (replication, monitoring) competes with OS work on core 0. The `0` entries are deliberate: with no spare core to give them, the replication handlers, the segment preparer and the disk thread run unpinned, and the server warns at boot that `journal-disk` has no core. **An exchange operator should not run production matching on an 8-core host** — this layout exists for development and proof-of-concept deployments only.

### Private Network (802.1Q VLAN)

Replication and the LAN benchmarks expect a private network between hosts, separate from the public interface. On bare metal that network is usually a customer VLAN: the provider attaches each server's second NIC port to it and leaves the port link-up but unconfigured, carrying no traffic until the OS is told to tag frames. The addresses inside the VLAN are yours to choose.

`scripts/private-net-setup.sh` configures and then verifies one:

```sh
sudo ./scripts/private-net-setup.sh --vlan-id 2025 --address 10.8.0.1/24 --peer 10.8.0.2
```

Run it on each host with a different `--address` in the same prefix. `--peer` is what makes the run meaningful — without it only the local half is checked, and a VLAN that is configured correctly on this host but not carried by the switch looks identical to one that works.

Notes:

- **`--vlan-id` is the numeric 802.1Q tag**, not the `vlan_xxx` resource id the provider dashboard also displays.
- **The parent defaults to `eno2`**, the second port on Latitude.sh hosts. Use `--link` elsewhere. The script refuses to build on the interface holding the default route, since that is the public port and reconfiguring it would eventually cut your own SSH session.
- **Only pass `--mtu` if the switch passes jumbo frames.** A switch that does not blackholes large frames silently rather than reporting an error, so the script probes at full frame size with DF set and tells you which of the two is happening. The parent is raised to `--mtu` plus 4 for the tag.
- **Changing the MTU resets the adapter**, and 10GBASE-T copper can take 15-30 seconds to retrain. The script waits for carrier rather than reporting a dead peer.
- Configuration is persisted to `/etc/netplan/60-melin-private-<vlan>.yaml` and validated with `netplan generate`, so the host keeps its private network across reboots. It is deliberately not written into `50-cloud-init.yaml`, which cloud-init owns and may rewrite.
- `--down --vlan-id <n>` tears the interface down, removes the persisted file, and returns the parent MTU to 1500.

---

## Monitoring

### Health/Liveness Endpoint

Dedicated health port (default `127.0.0.1:9878`). Supports four modes:

1. **Plain TCP** (no data sent): writes a one-line status and closes — backward-compatible with `nc` and Kubernetes TCP probes.
2. **HTTP `GET /`**: wraps the one-line status in an HTTP 200 response.
3. **HTTP `GET /metrics`**: returns Prometheus text exposition format with all engine counters.
4. **HTTP `GET /stats-dump`**: returns the per-stage latency-histogram snapshot used by the bench's tick-to-trade decomposition. Tab-separated values, one record per stage. Body returns `# latency-trace disabled` when the server was built without `--features latency-trace`; servers with `latency-trace` only return the lighter 4-stage set, while `--features tick-to-trade` returns the full 9+ stage decomposition.

No authentication required.

```sh
# Quick liveness check (TCP connect succeeds = alive)
nc -z 127.0.0.1 9878

# Read status line (plain TCP)
nc 127.0.0.1 9878
OK 42 1234567 0 trading

# HTTP health check
curl http://127.0.0.1:9878/

# Prometheus metrics
curl http://127.0.0.1:9878/metrics

# Tick-to-trade per-stage histograms (bench-only, requires --features latency-trace)
curl http://127.0.0.1:9878/stats-dump
```

**Plain-text response format**: `OK|ERR <active_connections> <journal_seq> <replication_lag> trading|halted\n`

| Field | Description |
|-------|-------------|
| `OK` / `ERR` | `OK` when all pipeline threads are alive; `ERR` when a thread has died or the server is shutting down |
| `active_connections` | Currently authenticated client connections |
| `journal_seq` | Latest durable journal sequence number |
| `replication_lag` | `journal_seq - replication_cursor` (0 in standalone mode) |
| `trading` / `halted` | `trading` when accepting orders; `halted` when replica is disconnected (replication mode only) |

**Configuration**: `--health-bind <addr:port>` (default `127.0.0.1:9878`). Omit the flag to disable. Replicas serve the endpoint too, whether or not election is enabled (the election gauges are absent without it); a primary and a replica on the same host need distinct binds.

**Kubernetes**: Use as a TCP liveness probe on the health port. For basic liveness, check TCP connect success. For readiness, parse the first and last tokens and require `OK` + `trading`.

### Prometheus Metrics

The `/metrics` endpoint exposes counters in Prometheus text exposition format. Zero new dependencies — the response is built from a hardcoded template.

```sh
curl http://127.0.0.1:9878/metrics
# HELP melin_active_connections Current authenticated client connections.
# TYPE melin_active_connections gauge
melin_active_connections 42
# HELP melin_events_processed Total events processed by the matching engine.
# TYPE melin_events_processed counter
melin_events_processed 1234567
# HELP melin_journal_sequence Latest durable journal sequence number.
# TYPE melin_journal_sequence counter
melin_journal_sequence 1234567
# HELP melin_replication_lag Journal sequence minus replication cursor.
# TYPE melin_replication_lag gauge
melin_replication_lag 0
# HELP melin_pipeline_healthy Whether the pipeline is healthy (1) or degraded (0).
# TYPE melin_pipeline_healthy gauge
melin_pipeline_healthy 1
# HELP melin_input_queue_depth Items pending in the input disruptor.
# TYPE melin_input_queue_depth gauge
melin_input_queue_depth 128
# HELP melin_input_queue_capacity Total input ring buffer capacity.
# TYPE melin_input_queue_capacity gauge
melin_input_queue_capacity 1048576
# HELP melin_trading_active Whether the engine is accepting orders (1) or halted (0).
# TYPE melin_trading_active gauge
melin_trading_active 1
```

| Metric | Type | Description |
|--------|------|-------------|
| `melin_active_connections` | gauge | Currently authenticated client connections |
| `melin_events_processed` | counter | Total events processed by the matching engine |
| `melin_journal_sequence` | counter | Latest durable journal sequence number |
| `melin_replication_lag` | gauge | `journal_seq - replication_cursor` (0 in standalone) |
| `melin_pipeline_healthy` | gauge | 1 when all pipeline threads are alive, 0 otherwise |
| `melin_input_queue_depth` | gauge | Items pending in the input disruptor (`producer - matching`) |
| `melin_input_queue_capacity` | gauge | Total input ring buffer capacity (constant 1,048,576) |
| `melin_trading_active` | gauge | 1 when accepting orders, 0 when halted |
| `melin_writes_refused_total` | counter | Client writes rejected with `ReplicaDisconnected` while halted. A refused write is never journaled, so this counter is its only trace on the node |
| `melin_stage_busy_total{stage="..."}` | counter | Cumulative busy iterations per stage (journal/response: batches, matching: events) |
| `melin_stage_idle_total{stage="..."}` | counter | Cumulative idle iterations per stage |
| `melin_journal_rotations_total{path="..."}` | counter | Journal segment rotation attempts by outcome: `fast` adopted a pre-staged segment; `sync_fallback` allocated synchronously on the journal thread; `failed` left the current segment in place |
| `melin_replica_divergence_total` | counter | Divergent replica handshakes on this primary (journal chain failed validation; replica routed through archive + re-seed). Alert on any growth — outside an expected failover rejoin it indicates possible data corruption |

`melin_journal_rotations_total{path="sync_fallback"}` should stay flat in steady state — the first rotation after startup may take the synchronous path while the background staging warms up, but sustained growth means rotation stalls are landing on the order pipeline's critical path and is worth an alert. Any growth in `path="failed"` is alert-worthy on every configuration: rotation is failing (disk full, read-only filesystem) and the live segment keeps growing past its threshold. On a replica, a rotation that fails at a primary-announced boundary stalls the journal stage *at* that boundary and retries on the same backoff — the replica stops acking until the rotation succeeds (under `disk+ram`/`two-disks` gating the primary feels that as ack backpressure), but it does not exit and recovers without intervention once the disk condition clears.

Use `rate(melin_stage_busy_total) / (rate(melin_stage_busy_total) + rate(melin_stage_idle_total))` for per-stage utilization percentage. The matching stage counts events (not batches), so its utilization is directly proportional to throughput.

**Prometheus scrape config**:

```yaml
scrape_configs:
  - job_name: melin
    scrape_interval: 10s
    static_configs:
      - targets: ['127.0.0.1:9878']
```

### Halt on Replica Disconnect

When replication is enabled (`--replication-bind`), the engine automatically halts trading if the replica disconnects. All state-mutating requests (orders, deposits, admin operations) are rejected with `ReplicaDisconnected` until the replica reconnects. Heartbeats continue working.

**Queries are not answered while halted.** `QueryStats`, position queries and request-sequence queries get no reply until the halt clears, under every ack policy — the client sees only its own read timeout. Monitor a halted node through the health endpoint instead: `melin_trading_active` reports the halt itself, and `melin_journal_sequence`, `melin_replication_lag`, `melin_active_connections` and `melin_writes_refused_total` cover what the node is doing. Account positions and the request-sequence high-water mark are unavailable until trading resumes. A client that connects to a halted node also blocks, because connecting queries the request-sequence mark; point clients at the health endpoint, or at the operator admin endpoint, to tell a halted node from an unreachable one.

This preserves the durability guarantee: the engine never acks a response that isn't durable on both primary and replica. Without this, a primary crash after replica disconnect could lose acked events.

A refused request is turned away before it is journaled: it has no effect on the book or on balances, now or after a restart, a failover or a replica catch-up. It also consumes nothing, so a client may resend it with the same request sequence once trading resumes. Refusals are counted in `melin_writes_refused_total`.

A primary superseded by a newer one (after a failover) behaves differently: it is shutting down, so it closes client connections instead of rejecting requests. Clients reconnect and land on the new primary, as they would after a crash.

Trading resumes automatically when the replica reconnects — no operator intervention needed. In standalone mode (no `--replication-bind`), this check is disabled.

### Admin Dashboard (QueryStats)

The admin TUI (`melin-ec-admin`) connects to a running server and can send a `QueryStats` request. This returns a live snapshot of server state:

- **Active connections**: current authenticated client count
- **Events processed**: total events handled by the matching engine
- **Journal sequence**: current durable journal position

QueryStats is not journaled (no state change) and does not affect the hot path. It reads counters via relaxed atomics.

```sh
melin-ec-admin <server-addr> <admin-key-file>
```

### Compile Features

| Feature | Default | Description |
|---------|---------|-------------|
| `io-uring` | **yes** | Use io_uring for journal writes. Falls back to `pwritev2` if disabled. |
| `pipeline-stats` | no | Per-stage busy/idle counters for bottleneck analysis. |
| `latency-trace` | no | Per-stage HDR histograms (adds ~tens of ns overhead per event). |
| `no-persist` | no | Skip journal writes entirely. **Unsafe for production.** |

### Pipeline Utilization

Per-stage busy/idle counters are always exposed via the `/metrics` endpoint (`melin_stage_busy_total`, `melin_stage_idle_total`). These are zero-overhead on the hot path: each stage increments a thread-local `u64` and flushes to a shared atomic every 1024 idle spins or on batch boundaries.

The `pipeline-stats` feature adds a summary log line on shutdown with the final utilization percentages. Useful for quick single-run analysis without a Prometheus setup:

```sh
cargo build --release --features pipeline-stats
```

### Latency Trace Feature

Compile with the `latency-trace` feature for per-stage HDR histograms:

```sh
cargo build --release --features latency-trace
```

This records timestamps at each pipeline stage transition and builds histograms for:
- **Wakeup latency**: time from publish to stage pickup
- **Batch encode time**: journal encoding duration
- **Execute time**: matching engine execution duration
- **End-to-end server latency**: wire-receive to wire-send

Histograms are reported on shutdown. The bench crate passes these features through:

```sh
cargo run --release --bin melin-ec-bench --features latency-trace,pipeline-stats
```

**Warning**: Latency trace adds overhead (~tens of nanoseconds per event for `rdtsc` calls). Do not enable in production unless actively diagnosing a latency issue.

---

## Emergency Procedures

### Kill Switch: Cancel All Orders for an Account

Use the admin tool to send `CancelAll` for a specific account. This cancels all resting orders across all instruments for that account. The command is journaled before execution.

```
melin-ec-admin <server-addr> <admin-key-file>
# Select "Cancel All" from the menu
# Enter account ID
```

### Trading Halt: Circuit Breaker

Use the admin tool to set a circuit breaker with `halted=true` on a specific instrument. All new orders for that instrument will be rejected with `TradingHalted`. Existing resting orders remain on the book but will not match.

```
melin-ec-admin <server-addr> <admin-key-file>
# Select "Set Circuit Breaker" from the menu
# Enter symbol, set halted = true
```

The halt persists across restarts (it is journaled and included in snapshots).

To resume trading, send another `SetCircuitBreaker` with `halted=false`.

### Halt All Instruments

There is no single "halt everything" command. You must send `SetCircuitBreaker` with `halted=true` for each instrument individually.

### Graceful Shutdown (SIGINT / SIGTERM)

Send `SIGINT` (Ctrl-C) or `SIGTERM` to the server process. The shutdown sequence:

1. Accept loop exits (non-blocking check on shutdown flag).
2. Reader threads stop -- no new events enter the disruptor.
3. Pipeline shutdown signal is set.
4. Journal stage drains remaining events from the ring buffer and flushes to disk.
5. Matching stage drains remaining events and publishes responses.
6. Response stage exits.
7. Server logs `shutdown complete` and exits with status 0.

**Second signal**: If you send SIGINT/SIGTERM again while shutdown is in progress, the server calls `_exit(1)` immediately (hard exit, no cleanup). Use this only if the graceful shutdown appears stuck.

**All events that entered the disruptor before shutdown will be journaled and responded to.** The ordered shutdown ensures no data loss.

---

## Crash Recovery Scenarios

### 1. Clean Shutdown

No action needed. The journal is fully synced. On next startup, the server recovers from the journal (or snapshot + journal) and resumes from where it left off.

### 2. Crash Mid-Write (Partial Entry)

The journal uses CRC32C checksums on every entry. If the server crashes during a write:

- The partially written entry will fail CRC validation on recovery.
- The `JournalReader` detects the truncated/corrupt entry and stops replaying at the last valid entry.
- The journal reopens the file for appending at the valid data boundary, effectively truncating the garbage.
- **One event may be lost** (the one being written at crash time). All prior events are intact.

This is handled automatically. No manual intervention required.

### 3. Crash During Rotation

Rotation is a single rename of the live segment to its next monotonic archive slot followed by opening a fresh live file (no snapshot involvement — snapshots are written separately by the shadow). A crash between these two steps leaves the just-archived segment intact and no live file present. Recovery walks the archive chain and synthesizes a fresh live segment continuing from the last archive's tail (see `docs/journal-rotation.md` scenario #2). No acknowledged event is lost.

### Crash During Snapshot Write

Snapshots are written atomically via `.tmp` file + rename. If the crash happens during the `.tmp` write, the rename never occurs and the previous snapshot (`melin.snapshot`, or `melin.snapshot.prev` if the rename happened but the next save failed) remains valid. If there is no prior snapshot, the server falls back to full journal replay across all segments.

### 4. Snapshot-Only Recovery (No Journal)

If only a snapshot file exists (journal deleted or on a different disk that failed), the server loads the snapshot and creates a fresh journal. State is restored to the point of the last snapshot. Events between the snapshot and the crash are lost.

### 5. Complete Data Loss

If both the journal and snapshot are gone, the server starts fresh with empty state: no instrument, no account, no balance. A build with `synthetic-seed` re-seeds test data per `--accounts`/`--instruments` instead — which is a fixture, not a recovery. Real state only comes back from a journal or a snapshot.

---

## Disk Failure

### What Happens When Journal Writes Fail

The journal stage logs an `error` on write/sync failure:

```
journal encode error: ...
journal flush_batch_sync error: ...
```

The pipeline does **not** crash on journal I/O errors. The journal stage logs the error and continues processing. However:

- Events that failed to persist will NOT have their responses gated by the journal cursor (the cursor does not advance past them).
- Depending on the failure mode, the response stage may stall waiting for the journal cursor to advance, causing client timeouts.

**This is a critical situation.** The persist-before-ack guarantee is broken if journal writes fail silently.

### Detection

1. **Monitor for `error` level log messages.** Any `error` log in production indicates a server-level problem. Journal I/O failures will appear as `journal flush_batch_sync error` or `journal encode error`.
2. **Monitor journal file growth.** If the journal stops growing while the server is receiving traffic, writes are failing.
3. **Monitor disk health.** Use `smartctl`, NVMe health counters, and filesystem error counts.

### When to Intervene

- **Single transient error** (e.g., momentary disk stall): The server self-recovers on the next successful write. Monitor closely.
- **Repeated errors**: The journal disk is failing. **Stop the server immediately** (SIGINT). Investigate the disk. Replace if necessary. Restore the journal and snapshot to a healthy disk and restart.
- **Disk full**: Clear space (delete old `.journal.N` archives) or increase `--max-journal-mib` to trigger more frequent rotation. Restart the server.

### NVMe-Specific Considerations

For best journal performance, use an NVMe drive with:

- **Power Loss Protection (PLP)**: Required. The journal relies on PLP capacitors to flush controller DRAM to NAND on power loss. Consumer SSDs and drives without confirmed PLP support are not safe for production use.
- **Dedicated journal disk**: Avoids contention with OS I/O. Sharing the disk with the OS or other workloads increases p99 latency.
- **Use xfs, not ext4**: ext4's jbd2 batches metadata commits at internal extent-group boundaries (~256 MiB on the layouts we see), which adds a ~1–2 ms `fdatasync` stall every ~10 s of sustained writes. Under the `disk+ram` ack policy with ≥ 2 replicas the stalls usually mask each other, but because the bench's event stream is byte-deterministic the same offset triggers the same stall on every node simultaneously — defeating the masking. xfs allocates extents through a different bookkeeping path that doesn't fire at write-time, so the periodic spike vanishes. Mount xfs with `noatime,logbsize=256k,logbufs=8`. `scripts/server-setup.sh` does this automatically.

---

## Capacity Planning

### Journal Size

Each journal entry is approximately **90 bytes** (20-byte header + 16 bytes of dedup metadata + variable payload + 4-byte CRC32C). The exact size depends on the event type:

- Limit order submit: ~80-100 bytes
- Cancel: ~50-60 bytes
- Deposit: ~50-60 bytes

The entry stream contains only input events — hash-chain metadata lives in each segment's file header and adds no per-event or periodic disk overhead.

The journal writer pre-allocates in **256 MiB chunks**. The on-disk file size jumps in 256 MiB increments.

### Snapshot Size

Snapshot size depends on the number of accounts, instruments, and resting orders:

- Base: ~50 bytes (header + sequence + chain hash + CRC)
- Per account: ~16 bytes per currency balance
- Per instrument: ~100 bytes (spec + circuit breaker + risk limits + fee schedule)
- Per resting order: ~40 bytes

A server with 10K accounts, 100 instruments, and 50K resting orders uses approximately:
- 10K accounts * 200 currencies * 16 bytes = ~32 MiB
- 50K orders * 40 bytes = ~2 MiB
- Total: ~34 MiB

### Ring Buffer Memory

The input and output ring buffers are allocated at startup:

| Buffer | Capacity | Slot Size | Memory |
|--------|----------|-----------|--------|
| Input disruptor | 2^20 = 1,048,576 slots | ~72 bytes | ~72 MiB |
| Output SPSC | 2^20 = 1,048,576 slots | ~varies | ~72 MiB |

Total ring buffer memory: approximately **144 MiB**. This is fixed regardless of throughput.

### Total Memory Budget

| Component | Estimate |
|-----------|----------|
| Ring buffers | ~144 MiB |
| Exchange state (order books, accounts) | 10-500 MiB (depends on active orders) |
| Journal pre-allocation | 256 MiB chunk |
| Replication ring (if enabled) | 128 MiB (256 slots × 512 KiB, tunable via `--replication-ring-size`) |
| Connection state | ~4 KiB per connection |
| jemalloc overhead | ~10-50 MiB |
| **Total (typical)** | **300-800 MiB** (add replication ring if enabled) |

### Replication Ring Sizing

When replication is enabled, the journal stage publishes encoded batches to a pre-allocated ring buffer. The replication sender thread consumes the ring and writes batches over TCP to the replica. If the sender can't keep up (network congestion, replica GC pause), the ring fills and the journal stage **spin-waits** — stalling the entire pipeline.

The default ring (256 slots × 512 KiB = 128 MiB) buffers approximately `256 × --max-journal-batch` events before backpressure:

| Throughput | Events buffered (batch=4096) | Wall-clock headroom |
|-----------|----------------------------|-------------------|
| 100K orders/sec | ~1M events | ~10 s |
| 1M orders/sec | ~1M events | ~1 s |
| 5M orders/sec | ~1M events | ~200 ms |
| 10M orders/sec | ~1M events | ~100 ms |

**When the default is sufficient**: same-rack replica on a dedicated NIC with sub-ms RTT. Under normal conditions the sender drains faster than the producer fills, and transient stalls are absorbed by the buffer.

**When to increase**: cross-AZ replication, shared or congested networks, or very high throughput where 100 ms of headroom is tight (a single TCP retransmit timeout is typically 200 ms+). Doubling to 512 slots (256 MiB) provides proportionally more jitter absorption.

Increasing `--replication-ring-size` only helps with **transient** slowness. If the replica is persistently slower than the primary, no buffer size prevents backpressure — the replica must keep up at steady state.

Note: when a replica **disconnects**, its acknowledgement slot is parked and the quorum position degrades to the *remaining* connected replica rather than resetting — the departed replica stops applying backpressure from the ring, and durability degrades to fsync-gated (if fewer than 2 replicas remain connected). Only when no replica at all is connected does the quorum report no acknowledged position.

This matters for alerting: losing one of two replicas does **not** stall or reset the quorum position, which keeps advancing on the survivor's acks. Alert on `melin_replicas_connected` (and on `melin_ack_policy_degraded`), not on the quorum position stalling — a single-replica loss is invisible in the latter.

### Throughput vs. Disk Bandwidth

At 5M orders/sec with ~80 bytes/event, the journal writes **~400 MB/s** sustained. Ensure the journal disk can sustain this write rate. Modern PLP NVMe drives typically support 1–3 GB/s sequential write throughput.

At 10M orders/sec (engine-only rate), you would need ~800 MB/s sustained write bandwidth. In practice, the TCP network stack is the bottleneck before journal bandwidth becomes limiting.
