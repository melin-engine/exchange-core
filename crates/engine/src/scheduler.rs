//! Engine-internal scheduler for time-driven tasks.
//!
//! The scheduler is fed by `JournalEvent::Tick { now_ns }` events published
//! by a dedicated tick thread. Every event entering the matching stage —
//! tick or otherwise — first drains all due tasks, so the scheduler runs
//! deterministically in lockstep with the journal.
//!
//! Tasks are stored in a min-heap keyed on `fire_ns`. A binary heap is the
//! natural fit: peek-min and pop-min are both O(log n), and the heap never
//! needs ordered iteration outside of snapshot serialization.
//!
//! This stage of the substrate carries no concrete task variants — those are
//! added incrementally by features that need scheduling (GTD expiry,
//! volatility halt evaluation, session transitions). Until then the heap
//! is permanently empty and `drain_due` is effectively a no-op.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// A single scheduled task waiting in the engine's min-heap.
///
/// Ordered by `fire_ns` so that wrapping in `Reverse` turns the
/// max-heap (`BinaryHeap` default) into the min-heap we want.
// Derive `Ord` after `fire_ns` so the natural ordering matches the heap's
// scheduling intent — tasks compare by deadline, then by kind for stable
// ordering of co-firing tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScheduledTask {
    /// Wall-clock deadline in nanoseconds since epoch. The task fires when
    /// the engine processes any event whose `now_ns >= fire_ns`.
    pub fire_ns: u64,
    /// Task kind discriminant. Future variants attach payloads (order ref,
    /// symbol, etc.); for now the substrate carries a single `Reserved`
    /// placeholder so the type compiles and round-trips through snapshots.
    pub kind: ScheduledTaskKind,
}

/// Discriminator for what kind of work fires at `ScheduledTask::fire_ns`.
///
/// Reserved-only until feature-specific variants land. The variant exists
/// so the enum is constructible (an empty enum is uninhabited, which would
/// prevent tests from exercising the heap and snapshot round-trip).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScheduledTaskKind {
    /// Placeholder until concrete kinds are introduced. Drained on tick
    /// like any other task; no side effect.
    Reserved,
}

/// On-disk tag for `ScheduledTaskKind::Reserved`. Stable across versions.
const KIND_TAG_RESERVED: u8 = 0;

/// Encode a single task kind into its on-disk tag byte.
pub(crate) fn encode_kind(kind: ScheduledTaskKind) -> u8 {
    match kind {
        ScheduledTaskKind::Reserved => KIND_TAG_RESERVED,
    }
}

/// Decode a task kind tag byte. Returns `None` if the tag is unknown.
pub(crate) fn decode_kind(tag: u8) -> Option<ScheduledTaskKind> {
    match tag {
        KIND_TAG_RESERVED => Some(ScheduledTaskKind::Reserved),
        _ => None,
    }
}

/// Min-heap of pending scheduled tasks.
///
/// Wraps `BinaryHeap<Reverse<ScheduledTask>>` to keep the `Reverse` plumbing
/// out of every caller. `BinaryHeap` is preferred over `BTreeSet` here
/// because the only operations on the hot path are `peek-min`, `pop-min`,
/// and `push` — all O(log n) — and we never need ordered iteration outside
/// snapshot serialization, which sorts explicitly.
#[derive(Debug, Default)]
pub struct ScheduledTaskHeap {
    inner: BinaryHeap<Reverse<ScheduledTask>>,
}

impl ScheduledTaskHeap {
    /// Construct an empty heap.
    pub fn new() -> Self {
        Self {
            inner: BinaryHeap::new(),
        }
    }

    /// Total number of pending tasks.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True when no tasks are scheduled.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Push a new task onto the heap.
    pub fn push(&mut self, task: ScheduledTask) {
        self.inner.push(Reverse(task));
    }

    /// Pop the next task whose `fire_ns <= now_ns`, if any.
    /// Returns `None` once the head is in the future (or the heap is empty).
    pub fn pop_due(&mut self, now_ns: u64) -> Option<ScheduledTask> {
        match self.inner.peek() {
            Some(Reverse(task)) if task.fire_ns <= now_ns => self.inner.pop().map(|r| r.0),
            _ => None,
        }
    }

    /// Snapshot the heap as a sorted Vec for deterministic on-disk layout.
    /// Order is by `fire_ns` ascending, then by `kind`.
    pub fn snapshot(&self) -> Vec<ScheduledTask> {
        let mut out: Vec<ScheduledTask> = self.inner.iter().map(|Reverse(t)| *t).collect();
        // Sort by the ScheduledTask Ord (fire_ns, then kind) so two heaps
        // built from the same set of tasks produce byte-identical snapshots.
        out.sort();
        out
    }

    /// Restore a heap from its snapshot representation.
    pub fn restore(tasks: Vec<ScheduledTask>) -> Self {
        let mut inner = BinaryHeap::with_capacity(tasks.len());
        for task in tasks {
            inner.push(Reverse(task));
        }
        Self { inner }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(fire_ns: u64) -> ScheduledTask {
        ScheduledTask {
            fire_ns,
            kind: ScheduledTaskKind::Reserved,
        }
    }

    #[test]
    fn empty_heap_pops_nothing() {
        let mut heap = ScheduledTaskHeap::new();
        assert!(heap.pop_due(u64::MAX).is_none());
    }

    #[test]
    fn pop_due_fires_only_past_tasks() {
        let mut heap = ScheduledTaskHeap::new();
        heap.push(task(100));
        heap.push(task(50));
        heap.push(task(200));

        assert_eq!(heap.pop_due(40), None, "all tasks still in the future");

        let first = heap.pop_due(150).unwrap();
        assert_eq!(first.fire_ns, 50);
        let second = heap.pop_due(150).unwrap();
        assert_eq!(second.fire_ns, 100);
        assert_eq!(heap.pop_due(150), None, "200 is still in the future");

        let third = heap.pop_due(200).unwrap();
        assert_eq!(third.fire_ns, 200);
        assert!(heap.is_empty());
    }

    #[test]
    fn snapshot_round_trip_preserves_set() {
        let mut heap = ScheduledTaskHeap::new();
        heap.push(task(300));
        heap.push(task(100));
        heap.push(task(200));

        let snap = heap.snapshot();
        assert_eq!(
            snap.iter().map(|t| t.fire_ns).collect::<Vec<_>>(),
            vec![100, 200, 300]
        );

        let restored = ScheduledTaskHeap::restore(snap);
        let mut popped = Vec::new();
        let mut h = restored;
        while let Some(t) = h.pop_due(u64::MAX) {
            popped.push(t.fire_ns);
        }
        assert_eq!(popped, vec![100, 200, 300]);
    }

    #[test]
    fn snapshot_is_deterministic_for_same_set() {
        let mut a = ScheduledTaskHeap::new();
        let mut b = ScheduledTaskHeap::new();
        // Push in different orders — snapshots must still match.
        for f in [200, 100, 300] {
            a.push(task(f));
        }
        for f in [300, 100, 200] {
            b.push(task(f));
        }
        assert_eq!(a.snapshot(), b.snapshot());
    }

    #[test]
    fn kind_round_trip() {
        // Only one variant today — the assertion grows when more land.
        let reserved = ScheduledTaskKind::Reserved;
        assert_eq!(decode_kind(encode_kind(reserved)), Some(reserved));
        assert!(decode_kind(0xFF).is_none());
    }
}
