//! Replication wire protocol — message types, framing, encode/decode.
//!
//! Length-prefixed frames, little-endian, over a dedicated TCP connection
//! (or DPDK pipe). See `mod.rs` for the message catalogue.
//!
//! All items are `pub(super)` — the protocol is an internal contract
//! between the sender and receiver paths in the parent module.

use std::io::{self, Read};

use melin_app::AppEvent;
use melin_journal::JournalEvent;
use melin_trading::trading_event::TradingEvent;

use crate::InputSlot;

// --- Wire protocol message tags ---

pub(super) const MSG_HANDSHAKE: u8 = 0x01;
pub(super) const MSG_ACK: u8 = 0x02;
// Auth messages (exchanged before the handshake).
pub(super) const MSG_CHALLENGE: u8 = 0x03;
pub(super) const MSG_CHALLENGE_RESPONSE: u8 = 0x04;
pub(super) const MSG_AUTH_OK: u8 = 0x05;
pub(super) const MSG_AUTH_FAILED: u8 = 0x06;
pub(super) const MSG_STREAM_START: u8 = 0x10;
pub(super) const MSG_NEED_SNAPSHOT: u8 = 0x11;
pub(super) const MSG_HASH_MISMATCH: u8 = 0x12;
pub(super) const MSG_SNAPSHOT_BEGIN: u8 = 0x13;
pub(super) const MSG_SNAPSHOT_CHUNK: u8 = 0x14;
pub(super) const MSG_SNAPSHOT_END: u8 = 0x15;
pub(super) const MSG_DATA_BATCH: u8 = 0x20;
/// Carries `InputSlot` records directly so the receiver can push them into
/// its input ring without going through the journal codec on the wire.
/// Replaces `MSG_DATA_BATCH` once the runtime is migrated.
#[allow(dead_code)] // wired up in subsequent commits of feat/unified-pipeline
pub(super) const MSG_INPUT_BATCH: u8 = 0x21;
pub(super) const MSG_HEARTBEAT: u8 = 0x30;

// Per-slot event tags, numerically aligned with the journal codec's private
// constants. We don't pull them from the journal crate to keep the wire and
// on-disk formats decoupled — they happen to share values today, but they're
// independent contracts.
#[allow(dead_code)]
pub(super) const SLOT_TAG_GENESIS_HASH: u8 = 0x01;
#[allow(dead_code)]
pub(super) const SLOT_TAG_CHECKPOINT: u8 = 0x02;
#[allow(dead_code)]
pub(super) const SLOT_TAG_TICK: u8 = 0x03;
#[allow(dead_code)]
pub(super) const SLOT_TAG_APP: u8 = 0x80;

/// Maximum frame size for control messages (handshake, ack, etc.).
/// Data batches can be much larger (up to 128 KiB of journal data).
pub(super) const MAX_CONTROL_FRAME: usize = 256;

/// Maximum data batch frame size. Must be >= CHUNK_SIZE (512 KiB) in the
/// replication ring, plus header overhead (9 bytes). Ring batches can use
/// the full 512 KiB chunk, so the frame limit must accommodate that.
pub(super) const MAX_DATA_FRAME: usize = 768 * 1024;

// --- Message structs / enums ---

/// Handshake message sent by the replica on connection.
#[derive(Debug, Clone)]
pub struct Handshake {
    pub last_sequence: u64,
    pub chain_hash: [u8; 32],
}

/// Ack message sent by the replica after durable write.
#[derive(Debug, Clone, Copy)]
pub struct Ack {
    pub acked_sequence: u64,
}

/// Messages from primary to replica.
#[derive(Debug)]
pub enum PrimaryMessage {
    StreamStart {
        start_sequence: u64,
        /// Primary's raw genesis entry bytes — the replica writes these
        /// directly to its journal for a byte-identical hash chain start.
        genesis_entry: Vec<u8>,
    },
    NeedSnapshot,
    HashMismatch,
    /// Start of a snapshot transfer. Sent after NeedSnapshot.
    SnapshotBegin {
        /// Total snapshot file size in bytes.
        snapshot_len: u64,
        /// Journal sequence at which the snapshot was taken.
        snap_sequence: u64,
        /// BLAKE3 chain hash at the snapshot point.
        snap_chain_hash: [u8; 32],
    },
    /// A chunk of snapshot data. Sent repeatedly after SnapshotBegin.
    SnapshotChunk(Vec<u8>),
    /// End of snapshot transfer. Contains CRC32C of the entire snapshot file.
    SnapshotEnd {
        crc32c: u32,
    },
    DataBatch {
        end_sequence: u64,
        journal_bytes: Vec<u8>,
    },
    Heartbeat {
        sequence: u64,
    },
}

/// Messages from replica to primary.
#[derive(Debug)]
pub enum ReplicaMessage {
    Handshake(Handshake),
    Ack(Ack),
}

// --- Encoders ---

/// Encode a handshake message into a frame (length-prefixed).
pub(super) fn encode_handshake(h: &Handshake, buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8 + 32; // type + sequence + hash
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HANDSHAKE);
    buf.extend_from_slice(&h.last_sequence.to_le_bytes());
    buf.extend_from_slice(&h.chain_hash);
}

/// Encode an ack message into a frame.
pub(super) fn encode_ack(ack: &Ack, buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8; // type + sequence
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_ACK);
    buf.extend_from_slice(&ack.acked_sequence.to_le_bytes());
}

/// Encode a Challenge message (primary → replica).
pub(super) fn encode_challenge(nonce: &[u8; 32], buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 32; // type + nonce
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_CHALLENGE);
    buf.extend_from_slice(nonce);
}

/// Encode a ChallengeResponse message (replica → primary).
pub(super) fn encode_challenge_response(
    signature: &[u8; 64],
    pubkey: &[u8; 32],
    buf: &mut Vec<u8>,
) {
    let payload_len: u32 = 1 + 64 + 32; // type + signature + pubkey
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_CHALLENGE_RESPONSE);
    buf.extend_from_slice(signature);
    buf.extend_from_slice(pubkey);
}

/// Encode an AuthOk message (primary → replica).
pub(super) fn encode_auth_ok(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_OK);
}

/// Encode an AuthFailed message (primary → replica).
pub(super) fn encode_auth_failed(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_FAILED);
}

/// Encode a StreamStart message into a frame.
///
/// Includes the primary's raw genesis entry bytes so the replica can
/// write a byte-identical genesis to its journal. This ensures the
/// BLAKE3 hash chain starts from the exact same encoded bytes (including
/// the timestamp), so checkpoint verification works on the replica.
pub(super) fn encode_stream_start(
    start_sequence: u64,
    genesis_entry_bytes: &[u8],
    buf: &mut Vec<u8>,
) {
    // type(1) + sequence(8) + genesis_len(4) + genesis_bytes
    let payload_len: u32 = (1 + 8 + 4 + genesis_entry_bytes.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_STREAM_START);
    buf.extend_from_slice(&start_sequence.to_le_bytes());
    buf.extend_from_slice(&(genesis_entry_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(genesis_entry_bytes);
}

/// Encode a NeedSnapshot message.
pub(super) fn encode_need_snapshot(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_NEED_SNAPSHOT);
}

/// Encode a SnapshotBegin message.
pub(super) fn encode_snapshot_begin(
    snapshot_len: u64,
    snap_sequence: u64,
    snap_chain_hash: &[u8; 32],
    buf: &mut Vec<u8>,
) {
    // type(1) + snapshot_len(8) + snap_sequence(8) + snap_chain_hash(32)
    let payload_len: u32 = 1 + 8 + 8 + 32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_BEGIN);
    buf.extend_from_slice(&snapshot_len.to_le_bytes());
    buf.extend_from_slice(&snap_sequence.to_le_bytes());
    buf.extend_from_slice(snap_chain_hash);
}

/// Encode a SnapshotChunk message.
pub(super) fn encode_snapshot_chunk(data: &[u8], buf: &mut Vec<u8>) {
    // type(1) + data
    let payload_len: u32 = (1 + data.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_CHUNK);
    buf.extend_from_slice(data);
}

/// Encode a SnapshotEnd message.
pub(super) fn encode_snapshot_end(crc32c: u32, buf: &mut Vec<u8>) {
    // type(1) + crc32c(4)
    let payload_len: u32 = 1 + 4;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_END);
    buf.extend_from_slice(&crc32c.to_le_bytes());
}

/// Encode a HashMismatch message.
#[allow(dead_code)] // Used in future catch-up implementation.
pub(super) fn encode_hash_mismatch(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HASH_MISMATCH);
}

/// Encode a DataBatch message.
///
/// Carries only the end sequence and the encoded journal bytes. Per-batch
/// chain hashes are not transmitted: with input replication each replica
/// re-encodes its own journal, so the primary's per-batch hash would not
/// match the replica's. Divergence detection runs inside the replica's
/// JournalStage at Checkpoint events instead.
#[allow(dead_code)] // still used by the DPDK path; phase 4 removes it
pub(super) fn encode_data_batch(end_sequence: u64, journal_bytes: &[u8], buf: &mut Vec<u8>) {
    // type(1) + end_sequence(8) + journal_bytes
    let payload_len: u32 = (1 + 8 + journal_bytes.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_DATA_BATCH);
    buf.extend_from_slice(&end_sequence.to_le_bytes());
    buf.extend_from_slice(journal_bytes);
}

/// Encode a Heartbeat message. Carries only the last-acked sequence;
/// the chain hash is verified at Checkpoint events, not on every heartbeat.
pub(super) fn encode_heartbeat(sequence: u64, buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HEARTBEAT);
    buf.extend_from_slice(&sequence.to_le_bytes());
}

// --- Decoders / framing ---

/// Read a length-prefixed frame from a stream. Returns the payload (without length prefix).
pub(super) fn read_frame(reader: &mut impl Read, max_size: usize) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > max_size {
        return Err(io::Error::other(format!(
            "frame too large: {len} > {max_size}"
        )));
    }
    if len == 0 {
        return Err(io::Error::other("empty frame"));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Decode a Challenge payload → 32-byte nonce.
pub(super) fn decode_challenge(payload: &[u8]) -> io::Result<[u8; 32]> {
    if payload.len() < 1 + 32 {
        return Err(io::Error::other("challenge too short"));
    }
    if payload[0] != MSG_CHALLENGE {
        return Err(io::Error::other(format!(
            "expected Challenge (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE, payload[0]
        )));
    }
    let mut nonce = [0u8; 32];
    nonce.copy_from_slice(&payload[1..33]);
    Ok(nonce)
}

/// Decode a ChallengeResponse payload → (signature, pubkey).
pub(super) fn decode_challenge_response(payload: &[u8]) -> io::Result<([u8; 64], [u8; 32])> {
    if payload.len() < 1 + 64 + 32 {
        return Err(io::Error::other("challenge response too short"));
    }
    if payload[0] != MSG_CHALLENGE_RESPONSE {
        return Err(io::Error::other(format!(
            "expected ChallengeResponse (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE_RESPONSE, payload[0]
        )));
    }
    let mut signature = [0u8; 64];
    signature.copy_from_slice(&payload[1..65]);
    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(&payload[65..97]);
    Ok((signature, pubkey))
}

/// Decode an auth result payload → true if AuthOk, false if AuthFailed.
pub(super) fn decode_auth_result(payload: &[u8]) -> io::Result<bool> {
    if payload.is_empty() {
        return Err(io::Error::other("empty auth result"));
    }
    match payload[0] {
        MSG_AUTH_OK => Ok(true),
        MSG_AUTH_FAILED => Ok(false),
        other => Err(io::Error::other(format!(
            "expected AuthOk/AuthFailed, got 0x{other:02x}"
        ))),
    }
}

/// Decode a replica message from a frame payload.
pub(super) fn decode_replica_message(payload: &[u8]) -> io::Result<ReplicaMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_HANDSHAKE => {
            if payload.len() < 1 + 8 + 32 {
                return Err(io::Error::other("handshake too short"));
            }
            let last_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let mut chain_hash = [0u8; 32];
            chain_hash.copy_from_slice(&payload[9..41]);
            Ok(ReplicaMessage::Handshake(Handshake {
                last_sequence,
                chain_hash,
            }))
        }
        MSG_ACK => {
            if payload.len() < 1 + 8 {
                return Err(io::Error::other("ack too short"));
            }
            let acked_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            Ok(ReplicaMessage::Ack(Ack { acked_sequence }))
        }
        other => Err(io::Error::other(format!(
            "unknown replica message type: 0x{other:02x}"
        ))),
    }
}

/// Fast-path decoder for `DataBatch` frames. Returns a *borrowed* slice
/// into `payload` so the receiver hot path can copy journal bytes directly
/// into its accumulator without the `Vec<u8>` allocation that the general
/// [`decode_primary_message`] performs on the `MSG_DATA_BATCH` arm.
///
/// Returns `None` in two cases:
/// - the payload is not a `DataBatch` (different type tag) — the caller
///   should fall back to [`decode_primary_message`] to handle control
///   messages (heartbeats, hash-mismatch, etc.).
/// - the payload *is* tagged as a `DataBatch` but is shorter than the
///   fixed header — indistinguishable from the non-data case here, so the
///   caller's general-decoder fallback will surface the truncation as a
///   protocol error.
#[allow(dead_code)] // still used by the DPDK path; phase 4 removes it
pub(super) fn try_decode_data_batch(payload: &[u8]) -> Option<(u64, &[u8])> {
    // Layout: type(1) + end_sequence(8) + journal_bytes
    const HEADER: usize = 1 + 8;
    if payload.len() < HEADER || payload[0] != MSG_DATA_BATCH {
        return None;
    }
    let end_sequence = u64::from_le_bytes(payload[1..9].try_into().ok()?);
    let journal_bytes = &payload[HEADER..];
    Some((end_sequence, journal_bytes))
}

/// Decode a primary message from a frame payload.
pub(super) fn decode_primary_message(payload: &[u8]) -> io::Result<PrimaryMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_STREAM_START => {
            if payload.len() < 1 + 8 + 4 {
                return Err(io::Error::other("StreamStart too short"));
            }
            let start_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let genesis_len = u32::from_le_bytes(payload[9..13].try_into().unwrap()) as usize;
            if payload.len() < 13 + genesis_len {
                return Err(io::Error::other("StreamStart genesis truncated"));
            }
            let genesis_entry = payload[13..13 + genesis_len].to_vec();
            Ok(PrimaryMessage::StreamStart {
                start_sequence,
                genesis_entry,
            })
        }
        MSG_NEED_SNAPSHOT => Ok(PrimaryMessage::NeedSnapshot),
        MSG_HASH_MISMATCH => Ok(PrimaryMessage::HashMismatch),
        MSG_SNAPSHOT_BEGIN => {
            if payload.len() < 1 + 8 + 8 + 32 {
                return Err(io::Error::other("SnapshotBegin too short"));
            }
            let snapshot_len = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let snap_sequence = u64::from_le_bytes(payload[9..17].try_into().unwrap());
            let mut snap_chain_hash = [0u8; 32];
            snap_chain_hash.copy_from_slice(&payload[17..49]);
            Ok(PrimaryMessage::SnapshotBegin {
                snapshot_len,
                snap_sequence,
                snap_chain_hash,
            })
        }
        MSG_SNAPSHOT_CHUNK => {
            let data = payload[1..].to_vec();
            Ok(PrimaryMessage::SnapshotChunk(data))
        }
        MSG_SNAPSHOT_END => {
            if payload.len() < 1 + 4 {
                return Err(io::Error::other("SnapshotEnd too short"));
            }
            let crc32c = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            Ok(PrimaryMessage::SnapshotEnd { crc32c })
        }
        MSG_DATA_BATCH => {
            if payload.len() < 1 + 8 {
                return Err(io::Error::other("DataBatch too short"));
            }
            let end_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let journal_bytes = payload[9..].to_vec();
            Ok(PrimaryMessage::DataBatch {
                end_sequence,
                journal_bytes,
            })
        }
        MSG_HEARTBEAT => {
            if payload.len() < 1 + 8 {
                return Err(io::Error::other("Heartbeat too short"));
            }
            let sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            Ok(PrimaryMessage::Heartbeat { sequence })
        }
        other => Err(io::Error::other(format!(
            "unknown primary message type: 0x{other:02x}"
        ))),
    }
}

// --- InputBatch (new wire format for input replication) ---
//
// `MSG_INPUT_BATCH` carries `InputSlot` records directly so the receiver
// can push them into its input ring without round-tripping through the
// journal codec on the wire. Replaces `MSG_DATA_BATCH` once the runtime
// is migrated; for now both coexist.
//
// Wire layout (after the [length:u32] frame prefix):
//   [type:0x21] [count:u16]
//   for each slot:
//     [event_size:u16]   ← bytes of event_payload only (after tag byte)
//     [sequence:u64]
//     [timestamp_ns:u64]
//     [key_hash:u64]
//     [request_seq:u64]
//     [event_tag:u8]
//     [event_payload: event_size bytes]
//
// No per-entry magic or CRC32C — TCP handles framing and integrity.
// `connection_id`, `publish_ts`, `recv_ts` from `InputSlot` are not on
// the wire (primary-internal bookkeeping); replica reconstructs them
// with `Default::default()`.

/// Decode a journal-codec byte stream into `InputSlot` records.
///
/// The replication ring on the primary still carries journal-encoded bytes
/// (so `JournalStage::publish_to_replication_rings` is unchanged). The
/// sender uses this helper to convert those bytes into `InputSlot`s before
/// re-encoding them as an `InputBatch` for the wire. Phase 3 of the
/// unified-pipeline plan eliminates this round-trip by having the journal
/// stage publish `InputSlot`s directly to the ring; until then this helper
/// keeps the wire format change isolated.
pub(super) fn decode_journal_to_input_slots(journal_bytes: &[u8]) -> io::Result<Vec<InputSlot>> {
    let mut slots = Vec::with_capacity(64);
    let mut offset = 0;
    while offset < journal_bytes.len() {
        let (consumed, sequence, timestamp_ns, key_hash, request_seq, event) =
            melin_journal::codec::decode::<TradingEvent>(
                &journal_bytes[offset..],
                melin_journal::codec::FORMAT_VERSION,
            )
            .map_err(|e| io::Error::other(format!("journal decode at offset {offset}: {e:?}")))?;
        offset += consumed;
        slots.push(InputSlot {
            connection_id: 0,
            key_hash,
            request_seq,
            sequence,
            timestamp_ns,
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
    }
    Ok(slots)
}

/// Encode an `InputBatch` frame into `buf` (length-prefixed).
pub(super) fn encode_input_batch(slots: &[InputSlot], buf: &mut Vec<u8>) {
    // Reserve 4 bytes for the length prefix; back-fill after we know the
    // total payload size (count + variable-size slot encodings).
    let len_prefix_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);
    let payload_start = buf.len();

    buf.push(MSG_INPUT_BATCH);
    let count = u16::try_from(slots.len()).expect("InputBatch slot count exceeds u16");
    buf.extend_from_slice(&count.to_le_bytes());

    for slot in slots {
        // Reserve event_size; back-fill after event payload is written.
        let size_pos = buf.len();
        buf.extend_from_slice(&[0u8; 2]);
        buf.extend_from_slice(&slot.sequence.to_le_bytes());
        buf.extend_from_slice(&slot.timestamp_ns.to_le_bytes());
        buf.extend_from_slice(&slot.key_hash.to_le_bytes());
        buf.extend_from_slice(&slot.request_seq.to_le_bytes());

        // Tag + payload. Payload starts after the tag byte; we measure
        // event_size from there so the decoder doesn't double-count the tag.
        let payload_start_in_slot = buf.len() + 1;
        match &slot.event {
            JournalEvent::GenesisHash { hash } => {
                buf.push(SLOT_TAG_GENESIS_HASH);
                buf.extend_from_slice(hash);
            }
            JournalEvent::Checkpoint {
                chain_hash,
                events_since_checkpoint,
            } => {
                buf.push(SLOT_TAG_CHECKPOINT);
                buf.extend_from_slice(chain_hash);
                buf.extend_from_slice(&events_since_checkpoint.to_le_bytes());
            }
            JournalEvent::Tick { now_ns } => {
                buf.push(SLOT_TAG_TICK);
                buf.extend_from_slice(&now_ns.to_le_bytes());
            }
            JournalEvent::App(e) => {
                buf.push(SLOT_TAG_APP);
                let n = e.encoded_size();
                let start = buf.len();
                buf.resize(start + n, 0);
                let written = e.encode(&mut buf[start..start + n]);
                debug_assert_eq!(written, n, "AppEvent::encode disagrees with encoded_size");
            }
        }

        let event_size = u16::try_from(buf.len() - payload_start_in_slot)
            .expect("event payload exceeds u16 max");
        buf[size_pos..size_pos + 2].copy_from_slice(&event_size.to_le_bytes());
    }

    let payload_len =
        u32::try_from(buf.len() - payload_start).expect("InputBatch payload exceeds u32");
    buf[len_prefix_pos..len_prefix_pos + 4].copy_from_slice(&payload_len.to_le_bytes());
}

/// Decode an `InputBatch` frame payload (the bytes after the length prefix,
/// starting with the type byte). Returns the reconstructed `InputSlot`
/// vector with `connection_id`, `publish_ts`, `recv_ts` reset to defaults.
pub(super) fn try_decode_input_batch(payload: &[u8]) -> io::Result<Vec<InputSlot>> {
    if payload.len() < 1 + 2 {
        return Err(io::Error::other("InputBatch header truncated"));
    }
    if payload[0] != MSG_INPUT_BATCH {
        return Err(io::Error::other(format!(
            "expected InputBatch (0x{:02x}), got 0x{:02x}",
            MSG_INPUT_BATCH, payload[0]
        )));
    }
    let count = u16::from_le_bytes(payload[1..3].try_into().unwrap()) as usize;
    let mut slots = Vec::with_capacity(count);
    let mut offset = 3;

    // Per-slot fixed header: event_size(2) + sequence(8) + timestamp_ns(8)
    //                      + key_hash(8) + request_seq(8) + tag(1) = 35 bytes.
    const SLOT_HEADER: usize = 2 + 8 + 8 + 8 + 8 + 1;

    for _ in 0..count {
        if payload.len() < offset + SLOT_HEADER {
            return Err(io::Error::other("InputBatch slot header truncated"));
        }
        let event_size =
            u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        let sequence = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let timestamp_ns = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let key_hash = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let request_seq = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let event_tag = payload[offset];
        offset += 1;

        if payload.len() < offset + event_size {
            return Err(io::Error::other("InputBatch slot payload truncated"));
        }
        let event_payload = &payload[offset..offset + event_size];
        offset += event_size;

        let event = match event_tag {
            SLOT_TAG_GENESIS_HASH => {
                if event_payload.len() < 32 {
                    return Err(io::Error::other("GenesisHash payload too short"));
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&event_payload[..32]);
                JournalEvent::GenesisHash { hash }
            }
            SLOT_TAG_CHECKPOINT => {
                if event_payload.len() < 40 {
                    return Err(io::Error::other("Checkpoint payload too short"));
                }
                let mut chain_hash = [0u8; 32];
                chain_hash.copy_from_slice(&event_payload[..32]);
                let events_since_checkpoint =
                    u64::from_le_bytes(event_payload[32..40].try_into().unwrap());
                JournalEvent::Checkpoint {
                    chain_hash,
                    events_since_checkpoint,
                }
            }
            SLOT_TAG_TICK => {
                if event_payload.len() < 8 {
                    return Err(io::Error::other("Tick payload too short"));
                }
                let now_ns = u64::from_le_bytes(event_payload[..8].try_into().unwrap());
                JournalEvent::Tick { now_ns }
            }
            SLOT_TAG_APP => {
                let app = TradingEvent::decode(event_payload)
                    .map_err(|e| io::Error::other(format!("App event decode failed: {e:?}")))?;
                JournalEvent::App(app)
            }
            other => {
                return Err(io::Error::other(format!("unknown slot tag: 0x{other:02x}")));
            }
        };

        slots.push(InputSlot {
            connection_id: 0,
            key_hash,
            request_seq,
            sequence,
            timestamp_ns,
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
    }

    Ok(slots)
}

#[cfg(test)]
mod input_batch_tests {
    use super::*;

    fn sample_slot(sequence: u64, event: crate::JournalEvent) -> InputSlot {
        InputSlot {
            connection_id: 0,
            key_hash: 0xabcd_ef00_1234_5678,
            request_seq: 9_999,
            sequence,
            timestamp_ns: 1_700_000_000_000_000_000,
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    #[test]
    fn roundtrip_transport_variants() {
        let slots = vec![
            sample_slot(10, crate::JournalEvent::Tick { now_ns: 12_345_678 }),
            sample_slot(
                11,
                crate::JournalEvent::Checkpoint {
                    chain_hash: [0x42; 32],
                    events_since_checkpoint: 1_000_000,
                },
            ),
            sample_slot(12, crate::JournalEvent::GenesisHash { hash: [0x77; 32] }),
        ];

        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);

        // Strip the 4-byte length prefix to get the payload.
        let payload_len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        assert_eq!(buf.len(), 4 + payload_len);
        let payload = &buf[4..];

        let decoded = try_decode_input_batch(payload).expect("decode succeeds");
        assert_eq!(decoded.len(), 3);

        for (orig, dec) in slots.iter().zip(decoded.iter()) {
            assert_eq!(dec.sequence, orig.sequence);
            assert_eq!(dec.timestamp_ns, orig.timestamp_ns);
            assert_eq!(dec.key_hash, orig.key_hash);
            assert_eq!(dec.request_seq, orig.request_seq);
            assert_eq!(dec.connection_id, 0);
        }

        match decoded[0].event {
            crate::JournalEvent::Tick { now_ns } => assert_eq!(now_ns, 12_345_678),
            ref other => panic!("expected Tick, got {other:?}"),
        }
        match decoded[1].event {
            crate::JournalEvent::Checkpoint {
                chain_hash,
                events_since_checkpoint,
            } => {
                assert_eq!(chain_hash, [0x42; 32]);
                assert_eq!(events_since_checkpoint, 1_000_000);
            }
            ref other => panic!("expected Checkpoint, got {other:?}"),
        }
        match decoded[2].event {
            crate::JournalEvent::GenesisHash { hash } => assert_eq!(hash, [0x77; 32]),
            ref other => panic!("expected GenesisHash, got {other:?}"),
        }
    }

    #[test]
    fn empty_batch_roundtrips() {
        let slots: Vec<InputSlot> = Vec::new();
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..];
        let decoded = try_decode_input_batch(payload).expect("decode succeeds");
        assert!(decoded.is_empty());
    }

    #[test]
    fn rejects_wrong_type_tag() {
        let payload = [0xFF, 0x00, 0x00];
        assert!(try_decode_input_batch(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_header() {
        let payload = [MSG_INPUT_BATCH];
        assert!(try_decode_input_batch(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_slot_payload() {
        // Encode a valid batch, then truncate the last byte.
        let slots = vec![sample_slot(1, crate::JournalEvent::Tick { now_ns: 0 })];
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..buf.len() - 1];
        assert!(try_decode_input_batch(payload).is_err());
    }
}
