//! Named sequence-space cursors for the durable pipeline.
//!
//! Four atomics answer "how far is the journal?" but they live in **two
//! different sequence spaces**, and historically that ambiguity has been a bug
//! factory — the pre-v14 durability gate read an allocator-space value through
//! a variable *named* `journal_persisted_wire_seq`. This module gives each
//! space its own type so the compiler rejects the mix-up:
//!
//! - [`WireSeq`] — the monotonic sequence the journal allocates per durable
//!   event. Shared with replica metrics and `OutputSlot.wire_seq`; comparable
//!   across nodes and stable across `starting_sequence` (a fresh vs recovered
//!   primary). This is what the durability gate compares.
//! - [`RingPos`] — a disruptor consumer's progress counter (slots read). Starts
//!   at `0` every process start and counts *every* input slot (orders, queries,
//!   ticks), so it is **not** comparable to a [`WireSeq`].
//!
//! [`PipelineCursors`] bundles the journal-progress cursors behind accessors
//! that name the space. Two of the four are `Arc<AtomicU64>` (wire-seq) and two
//! are `Arc<Sequence>` (ring-index, cache-padded); the type difference means a
//! ring cursor cannot even be wired into a wire-seq slot.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use melin_pipeline::padding::Sequence;

/// Wire-sequence space — see the module docs. A position, not a count;
/// subtract two of them with [`WireSeq::saturating_sub`] to get a lag.
///
/// `#[repr(transparent)]` so it is layout-identical to `u64` and can be a
/// field of `#[repr(C)]` structs (e.g. `FsyncState`) without changing their
/// layout.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Debug)]
pub struct WireSeq(u64);

impl WireSeq {
    #[inline]
    pub const fn new(seq: u64) -> Self {
        Self(seq)
    }

    /// Unwrap to the raw `u64` — used only at the wire-encode / health-format
    /// boundaries where the value leaves the type system.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Lag between two wire-seq positions, saturating at zero. Returns a raw
    /// `u64` because a lag is a count, not a position.
    #[inline]
    pub const fn saturating_sub(self, earlier: WireSeq) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

/// Ring-index space — see the module docs. A position, not a count.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Debug)]
pub struct RingPos(u64);

impl RingPos {
    #[inline]
    pub const fn new(pos: u64) -> Self {
        Self(pos)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Depth between two ring positions (e.g. producer − consumer), saturating
    /// at zero. Returns a raw `u64` because a depth is a count, not a position.
    #[inline]
    pub const fn saturating_sub(self, behind: RingPos) -> u64 {
        self.0.saturating_sub(behind.0)
    }
}

/// The journal-progress cursors, bundled with one space-typed accessor each.
///
/// All fields are `Arc`, so the struct is cheap to [`Clone`] for the readers
/// that need a handle (the response-stage gate, the health endpoint, the
/// replica orchestrator). Writers keep publishing through the same `Arc`s:
/// the journal stage Release-stores the durable wire seq after each fsync, and
/// the ring counters advance inside `ring::Consumer::commit`.
#[derive(Clone)]
pub struct PipelineCursors {
    /// Highest wire seq durably persisted on this node's journal — the gate's
    /// `persisted` cursor and the replica reconnect-handshake value.
    durable_wire_seq: Arc<AtomicU64>,
    /// Journal consumer's ring progress (slots read), for queue-depth monitoring.
    journal_ring: Arc<Sequence>,
    /// Matching consumer's ring progress (slots read), for queue-depth monitoring.
    matching_ring: Arc<Sequence>,
    /// Highest wire seq acked by the fastest replica. `u64::MAX` until a replica
    /// engages — `load_replica_acked` maps that sentinel to `None`. Always the
    /// sentinel on a replica node (no downstream replica to ack it).
    replica_acked_wire_seq: Arc<AtomicU64>,
}

impl PipelineCursors {
    /// Sentinel stored in `replica_acked_wire_seq` until a replica engages.
    /// `min(durable, MAX) == durable`, so a fresh primary gates on its journal
    /// alone.
    pub const NO_REPLICA: u64 = u64::MAX;

    pub fn new(
        durable_wire_seq: Arc<AtomicU64>,
        journal_ring: Arc<Sequence>,
        matching_ring: Arc<Sequence>,
        replica_acked_wire_seq: Arc<AtomicU64>,
    ) -> Self {
        Self {
            durable_wire_seq,
            journal_ring,
            matching_ring,
            replica_acked_wire_seq,
        }
    }

    // ── Typed reads (the safe interface) ───────────────────────────────

    /// Highest wire seq durably persisted. `Acquire` to pair with the journal
    /// stage's `Release` publish.
    #[inline]
    pub fn load_durable_wire_seq(&self) -> WireSeq {
        WireSeq(self.durable_wire_seq.load(Ordering::Acquire))
    }

    /// Publish the highest durably-persisted wire seq. Single-writer (journal
    /// stage), `Release` to pair with the readers' `Acquire`.
    #[inline]
    pub fn store_durable_wire_seq(&self, seq: WireSeq) {
        self.durable_wire_seq.store(seq.0, Ordering::Release);
    }

    /// Journal consumer's ring position. `Relaxed` — monitoring only.
    #[inline]
    pub fn load_journal_ring(&self) -> RingPos {
        RingPos(self.journal_ring.get().load(Ordering::Relaxed))
    }

    /// Matching consumer's ring position. `Relaxed` — monitoring only.
    #[inline]
    pub fn load_matching_ring(&self) -> RingPos {
        RingPos(self.matching_ring.get().load(Ordering::Relaxed))
    }

    /// Fastest replica's acked wire seq, or `None` while no replica has engaged.
    #[inline]
    pub fn load_replica_acked(&self) -> Option<WireSeq> {
        match self.replica_acked_wire_seq.load(Ordering::Relaxed) {
            Self::NO_REPLICA => None,
            seq => Some(WireSeq(seq)),
        }
    }

    // ── Raw `Arc` handles (for wiring only) ────────────────────────────
    //
    // These hand the underlying `Arc` to a stage that still reads/writes it
    // directly (the journal-stage publisher, the matching-stage gate handle,
    // the legacy halt-check). The return *type* encodes the space: the
    // wire-seq getters yield `Arc<AtomicU64>`, the ring getters yield
    // `Arc<Sequence>`, so a ring cursor cannot be passed where a wire-seq
    // `Arc` is expected.

    #[inline]
    pub fn durable_wire_seq_arc(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.durable_wire_seq)
    }

    #[inline]
    pub fn journal_ring_arc(&self) -> Arc<Sequence> {
        Arc::clone(&self.journal_ring)
    }

    #[inline]
    pub fn matching_ring_arc(&self) -> Arc<Sequence> {
        Arc::clone(&self.matching_ring)
    }

    #[inline]
    pub fn replica_acked_arc(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.replica_acked_wire_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursors() -> PipelineCursors {
        PipelineCursors::new(
            Arc::new(AtomicU64::new(0)),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(AtomicU64::new(PipelineCursors::NO_REPLICA)),
        )
    }

    #[test]
    fn durable_wire_seq_round_trips() {
        let c = cursors();
        assert_eq!(c.load_durable_wire_seq(), WireSeq::new(0));
        c.store_durable_wire_seq(WireSeq::new(42));
        assert_eq!(c.load_durable_wire_seq(), WireSeq::new(42));
        // The wiring getter observes the same store.
        assert_eq!(c.durable_wire_seq_arc().load(Ordering::Acquire), 42);
    }

    #[test]
    fn ring_cursors_read_through_the_shared_arc() {
        let c = cursors();
        c.journal_ring_arc().get().store(7, Ordering::Relaxed);
        c.matching_ring_arc().get().store(3, Ordering::Relaxed);
        assert_eq!(c.load_journal_ring(), RingPos::new(7));
        assert_eq!(c.load_matching_ring(), RingPos::new(3));
    }

    #[test]
    fn replica_acked_sentinel_maps_to_none() {
        let c = cursors();
        assert_eq!(c.load_replica_acked(), None);
        c.replica_acked_arc().store(100, Ordering::Relaxed);
        assert_eq!(c.load_replica_acked(), Some(WireSeq::new(100)));
    }

    #[test]
    fn lag_and_depth_saturate() {
        // Lag is a count, never negative.
        assert_eq!(WireSeq::new(100).saturating_sub(WireSeq::new(40)), 60);
        assert_eq!(WireSeq::new(40).saturating_sub(WireSeq::new(100)), 0);
        assert_eq!(RingPos::new(10).saturating_sub(RingPos::new(4)), 6);
    }

    // The spaces deliberately do not inter-convert. The following would not
    // compile, which is the whole point of the newtypes:
    //   let _ = WireSeq::new(1).saturating_sub(RingPos::new(1)); // mismatched types
    //   let _: WireSeq = RingPos::new(1);                        // mismatched types
}
