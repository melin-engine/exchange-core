//! Wire format for input replication (`InputBatch` frames).
//!
//! Replicas accept `InputSlot` records directly — no journal-codec
//! round-trip on the wire. The journal stage encodes each slot into both
//! the local journal-codec buffer (for disk) and a parallel
//! `InputBatch` buffer (for the replication ring + sender), so the sender
//! becomes a passthrough: ring chunk bytes are already wire-ready frames.
//!
//! Wire layout (after a `[length:u32]` frame prefix):
//! ```text
//! [type:0x21] [count:u16]
//! for each slot:
//!   [event_size:u16]   ← bytes of event_payload only (after tag byte)
//!   [sequence:u64]
//!   [timestamp_ns:u64]
//!   [key_hash:u64]
//!   [request_seq:u64]
//!   [event_tag:u8]
//!   [event_payload: event_size bytes]
//! ```
//!
//! No per-entry magic or CRC32C — TCP/DPDK handle framing and integrity.
//! `connection_id`, `publish_ts`, `recv_ts` from `InputSlot` are not on
//! the wire (primary-internal bookkeeping); the receiver reconstructs
//! them with `Default::default()`.

use std::io;

use melin_app::AppEvent;
use melin_journal::JournalEvent;

use crate::pipeline::InputSlot;

// --- Constants ---

pub const MSG_INPUT_BATCH: u8 = 0x21;

pub const SLOT_TAG_GENESIS_HASH: u8 = 0x01;
pub const SLOT_TAG_CHECKPOINT: u8 = 0x02;
pub const SLOT_TAG_TICK: u8 = 0x03;
pub const SLOT_TAG_APP: u8 = 0x80;

/// `[length:u32] [type:u8] [count:u16]`. Streaming encoders reserve this
/// up front and back-fill it at finalize time.
const FRAME_HEADER_LEN: usize = 4 + 1 + 2;

// --- Streaming encode (used by the journal stage on the hot path) ---

/// Reset `buf` and reserve placeholder bytes for the frame header.
/// Caller appends slots via [`append_input_slot`] and back-fills the
/// header with [`finalize_input_batch`] before publishing.
pub fn init_input_batch(buf: &mut Vec<u8>) {
    buf.clear();
    buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
}

/// Append one slot's wire bytes to a buffer initialized with
/// [`init_input_batch`]. `seq` is the sequence to encode — `slot.sequence`
/// may be zero on the primary (the journal stage allocates at encode
/// time), so callers pass the allocated value explicitly.
///
/// **Caller contract**: `buf` must already contain a frame header (i.e.
/// `init_input_batch` was called, or this is being called inside
/// `encode_input_batch`). The debug assertion catches the bare-empty
/// `Vec` misuse in tests.
pub fn append_input_slot<E: AppEvent>(buf: &mut Vec<u8>, slot: &InputSlot<E>, seq: u64) {
    debug_assert!(
        buf.len() >= FRAME_HEADER_LEN,
        "append_input_slot requires init_input_batch first (buf.len() = {})",
        buf.len()
    );
    // Reserve event_size; back-fill after the tag + payload are written.
    let size_pos = buf.len();
    buf.extend_from_slice(&[0u8; 2]);
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&slot.timestamp_ns.to_le_bytes());
    buf.extend_from_slice(&slot.key_hash.to_le_bytes());
    buf.extend_from_slice(&slot.request_seq.to_le_bytes());

    // Tag + payload. event_size measures from after the tag byte so the
    // decoder doesn't double-count it.
    let payload_start = buf.len() + 1;
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

    let event_size =
        u16::try_from(buf.len() - payload_start).expect("event payload exceeds u16 max");
    buf[size_pos..size_pos + 2].copy_from_slice(&event_size.to_le_bytes());
}

/// Back-fill the frame header so `buf` is wire-ready (length-prefixed,
/// type-tagged, count populated). `slot_count` is the number of slots
/// appended since the last [`init_input_batch`].
pub fn finalize_input_batch(buf: &mut [u8], slot_count: u16) {
    debug_assert!(buf.len() >= FRAME_HEADER_LEN);
    let payload_len = u32::try_from(buf.len() - 4).expect("InputBatch payload exceeds u32");
    buf[0..4].copy_from_slice(&payload_len.to_le_bytes());
    buf[4] = MSG_INPUT_BATCH;
    buf[5..7].copy_from_slice(&slot_count.to_le_bytes());
}

// --- One-shot encode (used by catch-up paths that already have a slot vec) ---

/// Encode a complete length-prefixed `InputBatch` frame into `buf`.
/// Equivalent to `init_input_batch` + `append_input_slot` per slot
/// (with `slot.sequence`) + `finalize_input_batch`. Use the streaming
/// API on the journal stage hot path; this helper is for catch-up that
/// already materialised a slot vector.
pub fn encode_input_batch<E: AppEvent>(slots: &[InputSlot<E>], buf: &mut Vec<u8>) {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
    for slot in slots {
        append_input_slot(buf, slot, slot.sequence);
    }
    let count = u16::try_from(slots.len()).expect("InputBatch slot count exceeds u16");
    finalize_input_batch(&mut buf[start..], count);
}

// --- Decode ---

/// Decode an `InputBatch` frame payload (the bytes after the length prefix,
/// starting with the type byte). Returns the reconstructed `InputSlot`
/// vector with `connection_id`, `publish_ts`, `recv_ts` reset to defaults.
pub fn try_decode_input_batch<E: AppEvent>(payload: &[u8]) -> io::Result<Vec<InputSlot<E>>> {
    if payload.len() < 1 + 2 {
        return Err(io::Error::other("InputBatch header truncated"));
    }
    if payload[0] != MSG_INPUT_BATCH {
        return Err(io::Error::other(format!(
            "expected InputBatch (0x{:02x}), got 0x{:02x}",
            MSG_INPUT_BATCH, payload[0]
        )));
    }
    let count =
        u16::from_le_bytes(payload[1..3].try_into().expect("2-byte slice into [u8; 2]")) as usize;
    let mut slots = Vec::with_capacity(count);
    let mut offset = 3;

    // Per-slot fixed header: event_size(2) + sequence(8) + timestamp_ns(8)
    //                      + key_hash(8) + request_seq(8) + tag(1) = 35 bytes.
    const SLOT_HEADER: usize = 2 + 8 + 8 + 8 + 8 + 1;

    for _ in 0..count {
        if payload.len() < offset + SLOT_HEADER {
            return Err(io::Error::other("InputBatch slot header truncated"));
        }
        let event_size = u16::from_le_bytes(
            payload[offset..offset + 2]
                .try_into()
                .expect("2-byte slice into [u8; 2]"),
        ) as usize;
        offset += 2;
        let sequence = u64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("8-byte slice into [u8; 8]"),
        );
        offset += 8;
        let timestamp_ns = u64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("8-byte slice into [u8; 8]"),
        );
        offset += 8;
        let key_hash = u64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("8-byte slice into [u8; 8]"),
        );
        offset += 8;
        let request_seq = u64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .expect("8-byte slice into [u8; 8]"),
        );
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
                let events_since_checkpoint = u64::from_le_bytes(
                    event_payload[32..40]
                        .try_into()
                        .expect("8-byte slice into [u8; 8]"),
                );
                JournalEvent::Checkpoint {
                    chain_hash,
                    events_since_checkpoint,
                }
            }
            SLOT_TAG_TICK => {
                if event_payload.len() < 8 {
                    return Err(io::Error::other("Tick payload too short"));
                }
                let now_ns = u64::from_le_bytes(
                    event_payload[..8]
                        .try_into()
                        .expect("8-byte slice into [u8; 8]"),
                );
                JournalEvent::Tick { now_ns }
            }
            SLOT_TAG_APP => {
                let app = E::decode(event_payload)
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
mod tests {
    use super::*;
    use melin_app::CodecError;

    /// Minimal AppEvent for round-trip tests. Encodes a single u32 payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u32);

    impl AppEvent for TestEvent {
        fn encoded_size(&self) -> usize {
            4
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[..4].copy_from_slice(&self.0.to_le_bytes());
            4
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            if buf.len() < 4 {
                return Err(CodecError::Truncated);
            }
            Ok(TestEvent(u32::from_le_bytes(
                buf[..4].try_into().expect("4-byte slice into [u8; 4]"),
            )))
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    fn sample_slot(sequence: u64, event: JournalEvent<TestEvent>) -> InputSlot<TestEvent> {
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
            sample_slot(10, JournalEvent::Tick { now_ns: 12_345_678 }),
            sample_slot(
                11,
                JournalEvent::Checkpoint {
                    chain_hash: [0x42; 32],
                    events_since_checkpoint: 1_000_000,
                },
            ),
            sample_slot(12, JournalEvent::GenesisHash { hash: [0x77; 32] }),
        ];

        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);

        let payload_len =
            u32::from_le_bytes(buf[..4].try_into().expect("4-byte slice into [u8; 4]")) as usize;
        assert_eq!(buf.len(), 4 + payload_len);
        let payload = &buf[4..];

        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert_eq!(decoded.len(), 3);

        for (orig, dec) in slots.iter().zip(decoded.iter()) {
            assert_eq!(dec.sequence, orig.sequence);
            assert_eq!(dec.timestamp_ns, orig.timestamp_ns);
            assert_eq!(dec.key_hash, orig.key_hash);
            assert_eq!(dec.request_seq, orig.request_seq);
            assert_eq!(dec.connection_id, 0);
        }

        match decoded[0].event {
            JournalEvent::Tick { now_ns } => assert_eq!(now_ns, 12_345_678),
            ref other => panic!("expected Tick, got {other:?}"),
        }
        match decoded[1].event {
            JournalEvent::Checkpoint {
                chain_hash,
                events_since_checkpoint,
            } => {
                assert_eq!(chain_hash, [0x42; 32]);
                assert_eq!(events_since_checkpoint, 1_000_000);
            }
            ref other => panic!("expected Checkpoint, got {other:?}"),
        }
        match decoded[2].event {
            JournalEvent::GenesisHash { hash } => assert_eq!(hash, [0x77; 32]),
            ref other => panic!("expected GenesisHash, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_app_variant() {
        let slots = vec![sample_slot(7, JournalEvent::App(TestEvent(0xdead_beef)))];
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..];
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert_eq!(decoded.len(), 1);
        match decoded[0].event {
            JournalEvent::App(TestEvent(v)) => assert_eq!(v, 0xdead_beef),
            ref other => panic!("expected App, got {other:?}"),
        }
    }

    #[test]
    fn empty_batch_roundtrips() {
        let slots: Vec<InputSlot<TestEvent>> = Vec::new();
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..];
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert!(decoded.is_empty());
    }

    #[test]
    fn rejects_wrong_type_tag() {
        let payload = [0xFF, 0x00, 0x00];
        assert!(try_decode_input_batch::<TestEvent>(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_header() {
        let payload = [MSG_INPUT_BATCH];
        assert!(try_decode_input_batch::<TestEvent>(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_slot_payload() {
        let slots = vec![sample_slot(1, JournalEvent::Tick { now_ns: 0 })];
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..buf.len() - 1];
        assert!(try_decode_input_batch::<TestEvent>(payload).is_err());
    }

    #[test]
    fn streaming_api_matches_one_shot() {
        let slots = vec![
            sample_slot(20, JournalEvent::Tick { now_ns: 100 }),
            sample_slot(21, JournalEvent::App(TestEvent(42))),
        ];

        let mut one_shot = Vec::new();
        encode_input_batch(&slots, &mut one_shot);

        let mut streaming = Vec::new();
        init_input_batch(&mut streaming);
        for slot in &slots {
            append_input_slot(&mut streaming, slot, slot.sequence);
        }
        finalize_input_batch(&mut streaming, slots.len() as u16);

        assert_eq!(one_shot, streaming);
    }
}
