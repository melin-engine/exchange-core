# Replication Design Document

## Overview

Synchronous journal replication from a primary server to a replica, providing zero-data-loss failover capability. The primary streams journal entries to the replica over TCP; the replica persists them locally and acknowledges. Client responses are gated on **both** local journal durability and replica acknowledgement — a client never learns about an event that the replica hasn't durably stored.

## Architecture

```
Primary:
  Readers → Disruptor → JournalStage       (consumer 0, parallel) → disk
                       → MatchingStage      (consumer 1, parallel) → OutputSPSC
                       → ReplicationStage   (consumer 2, parallel) → TCP → Replica

  ResponseStage gates on min(journal_cursor, replication_cursor)

Replica:
  TCP → ReplicationReceiver → decode + verify CRC + chain hash
      → local JournalWriter (durability) → ack sequence back to primary
      → replay into Exchange (state)
```

### Pipeline integration

Replication is integrated into the `JournalStage` rather than running as a separate disruptor consumer. Before each `flush_batch_sync()`, the journal stage copies its pending batch buffer into a pre-allocated slot in a lock-free replication ring (64 slots × 128 KiB = 8 MiB). The replication sender thread consumes from this ring and streams batches to the replica. This guarantees the replicated bytes are **identical** to what was written to disk — same sequences, timestamps, CRC checksums, and checkpoint entries. No heap allocation on the journal thread — just a flat memcpy into the ring.

This design avoids a class of bugs where a separate replication consumer would re-encode events independently, producing different timestamps, missing auto-emitted checkpoint entries, and diverging BLAKE3 chain hashes.

### Ack-after-replicate

The response stage gates on `min(journal_cursor, replication_cursor)` instead of just `journal_cursor`. This ensures:

- A client only receives a response once the event is **locally durable** AND **replicated**.
- On failover, the replica has every event the client was told about.
- No data loss window — same guarantee as Raft commit.

**Latency impact**: adds ~100-200 µs (LAN round-trip) to client-perceived latency. Throughput is unaffected — batching amortizes the round-trip across many events.

### Replication cursor behavior

| Scenario | `replication_cursor` | Response gate effect |
|---|---|---|
| `--standalone` (dev/test) | `u64::MAX` | `min(journal, MAX) = journal` — no replication |
| Replica connected, acking | Latest acked seq | Waits for both journal + replica |
| Replica disconnects | `u64::MAX` | Degrades to local-only, operator alerted |
| Replica reconnects | Resumes from ack | Gate re-engages |

When the replica disconnects, the cursor is set to `u64::MAX` so the primary degrades gracefully to local-only durability. This is a deliberate design choice: a disconnected replica should not halt the exchange. The operator is alerted via `error!` log and must reconnect the replica.

## Wire Protocol

Length-prefixed frames, little-endian. Runs over a dedicated TCP connection separate from the client protocol.

### Replica → Primary

| Message | Layout | Purpose |
|---|---|---|
| Handshake | `[len:u32][type=0x01][last_sequence:u64][chain_hash:[u8;32]]` | Initial connection: replica reports its last durable sequence and chain hash |
| Ack | `[len:u32][type=0x02][acked_sequence:u64]` | Replica confirms durable write up to this sequence |

### Primary → Replica

| Message | Layout | Purpose |
|---|---|---|
| StreamStart | `[len:u32][type=0x10][start_sequence:u64][genesis_len:u32][genesis_entry_bytes...]` | Confirms handshake, includes raw genesis entry for byte-identical hash chain |
| NeedSnapshot | `[len:u32][type=0x11]` | Replica is too far behind; needs a snapshot transfer (future, not implemented) |
| HashMismatch | `[len:u32][type=0x12]` | Chain hash doesn't match at the replica's reported sequence (not yet validated) |
| DataBatch | `[len:u32][type=0x20][end_sequence:u64][chain_hash:[u8;32]][journal_bytes...]` | Batch of encoded journal entries with trailing chain hash |
| Heartbeat | `[len:u32][type=0x30][sequence:u64][chain_hash:[u8;32]]` | Periodic idle keepalive with current state |

### Design rationale

- **Journal wire format reuse**: DataBatch payloads contain raw journal-encoded bytes (same as on-disk format). This avoids a second serialization format, ensures the replica can write directly to its journal, and inherits CRC32C integrity checks.
- **Chain hash in DataBatch**: The replica verifies the chain hash after processing each batch, catching any corruption in transit that CRC alone might miss (CRC protects individual entries; the chain hash protects ordering and completeness).
- **Single replica (v1)**: The primary accepts one replica connection. Multi-replica support is a future extension.

## Catch-up

When a replica connects with `last_sequence < primary.current_sequence`, the primary reads historical entries from the journal file(s) using `JournalReader` and streams them as `DataBatch` frames before switching to the live feed from the disruptor.

The chain hash in the handshake is validated against the journal at the replica's reported sequence. A mismatch indicates divergent history (e.g., the replica was connected to a different primary) and is rejected with `HashMismatch`.

## Replica Mode

A server started with `--replica-of <primary_addr>` runs in replica mode:

- Connects to the primary and sends a `Handshake`.
- Receives `DataBatch` frames, decodes entries, verifies CRC and chain hash.
- Writes entries to a local `JournalWriter` for durability.
- Replays entries into a local `Exchange` for state.
- Sends `Ack` frames after each durable write.
- Does **not** accept client connections (read-only state).

## CLI Flags

| Flag | Required | Default | Purpose |
|---|---|---|---|
| `--replication-bind <addr>` | Yes (primary mode) | — | Address to listen for replica connections |
| `--standalone` | No | — | Disable replication (dev/test); sets cursor to `u64::MAX` |
| `--replica-of <addr>` | No | — | Run as a replica connected to the given primary |

`--replication-bind` and `--standalone` are mutually exclusive. `--replica-of` is mutually exclusive with both.

## Future Work (not in this branch)

These are known limitations of the current implementation. Each is documented here with the reason it was deferred and the plan for resolution.

### No catch-up from journal files

**What**: When a replica connects with `last_sequence` behind the primary's live stream, the primary does NOT read historical entries from the journal file. It starts streaming from the live feed only.

**Impact**: A replica must be connected from the start (or from a point where the live stream covers all missed events). A late-joining replica that missed events while disconnected will have a gap.

**Why deferred**: Catch-up requires reading from potentially rotated journal files (`.1`, `.2` archives) and coordinating the transition from historical to live streaming. This is substantial complexity that doesn't block the core replication mechanism.

**Resolution**: Read from `JournalReader` on the primary side during handshake when `replica.last_sequence < earliest_live_sequence`. Stream historical entries as DataBatch frames, then switch to the live channel.

### No chain hash verification on received DataBatch

**What**: The `chain_hash` field in DataBatch frames is populated by the primary but **not verified** by the replica. The replica decodes entries and checks individual CRC32C checksums but does not verify that the batch-level chain hash matches.

**Impact**: Corruption that preserves individual entry CRCs but reorders or drops entries within a batch would go undetected. In practice, TCP ordering guarantees make this extremely unlikely.

**Why deferred**: Verifying the chain hash requires the replica to maintain its own running hash state and compare after each batch. The journal's per-entry CRC32C provides entry-level integrity, and TCP provides ordering. Adding chain verification is a defense-in-depth measure, not a correctness requirement for the common case.

**Resolution**: After decoding all entries in a DataBatch, compute the BLAKE3 chain hash over the raw bytes and compare against `batch_chain_hash`. Reject the batch and disconnect on mismatch.

### No handshake chain hash validation

**What**: The primary does not validate the replica's `chain_hash` from the Handshake against its own journal at the replica's reported `last_sequence`. It unconditionally sends `StreamStart`.

**Impact**: A replica with divergent history (e.g., connected to a different primary previously, or with a corrupted journal) will be accepted without warning. The `NeedSnapshot` and `HashMismatch` response types are defined in the protocol but never sent.

**Why deferred**: Validating the chain hash at an arbitrary historical sequence requires either keeping a mapping of sequence→chain_hash (expensive) or re-reading the journal from genesis (slow). For v1, the assumption is that replicas are fresh or were connected to this primary.

**Resolution**: Store periodic chain hash checkpoints in a side index, or validate by reading the journal file at the reported sequence.

### ~~Fresh replica genesis hash diverges from primary~~ (FIXED)

The primary's raw genesis entry bytes (including the original timestamp) are sent in the `StreamStart` response. Fresh replicas write these bytes directly to the journal file, producing a byte-identical genesis entry. The BLAKE3 hash chain starts from the exact same encoded bytes, so checkpoint entries from the primary verify correctly on replica replay.

### Single replica only

**What**: The primary accepts one replica connection at a time. A second connection replaces the first (the previous connection's cursor is set to `u64::MAX`).

**Impact**: No quorum-based replication. If the single replica fails, the primary degrades to local-only.

**Resolution**: Accept N connections, track per-replica cursors, gate on a configurable quorum (e.g., majority).

### Backpressure from replication channel can stall the pipeline

**What**: The journal stage publishes to a lock-free replication ring (64 slots × 128 KiB). If the sender thread is slow (network saturated, replica not acking), the ring fills and the journal stage spins in `try_claim()`. This blocks the journal stage, which blocks the disruptor, which blocks all reader threads.

**Impact**: Under extreme replication lag, client request processing stalls. The 1M-slot disruptor ring provides substantial buffering before this happens (~100ms at 10M events/sec), but a multi-second network partition would trigger it.

**Mitigation**: On replica disconnect, the sender thread drains the ring (discards batches) and the cursor resets to `u64::MAX`, unblocking the pipeline.

**Resolution**: Consider a non-blocking publish with overflow detection, or increasing the ring capacity (currently 64 slots = 8 MiB).

### `read_frame` partial read on timeout

**What**: The ack reader socket has a 1ms read timeout. If `read_exact` partially reads a frame header (e.g., 2 of 4 bytes) before the timeout fires, the next `read_frame` call starts mid-frame, permanently desynchronizing the stream.

**Impact**: Extremely unlikely with TCP (kernel buffers ensure complete small reads), but theoretically possible under extreme memory pressure or with pathological packet fragmentation.

**Mitigation**: The 1ms timeout is short enough that ack frames (9 bytes) arrive atomically in practice. If desync occurs, the decode will fail and the connection will be dropped and re-established.

**Resolution**: Use a `BufReader` wrapper that preserves partial reads across calls, or switch to non-blocking I/O with explicit read state tracking.

## Future Work

- **Catch-up from journal files** — see limitation above
- **Chain hash verification** — see limitation above
- **Snapshot transfer**: When a replica is too far behind for journal catch-up (journal rotated away), the primary transfers a snapshot + remaining journal.
- **Manual promotion**: An operator command to promote a replica to primary (stop replication, start accepting clients).
- **Automatic failover**: Leader election / consensus for automatic promotion. Requires fencing to prevent split-brain.
- **Multi-replica**: Accept N replica connections, gate on a quorum.
- **Async replication**: Optional mode where the response stage does not gate on the replication cursor (lower latency, data loss window).
