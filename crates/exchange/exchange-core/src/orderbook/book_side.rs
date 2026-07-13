//! Resting limit-order book side: sorted price levels backed by a slab +
//! intrusive doubly-linked FIFO per level. See `BookSide` for the full
//! storage rationale.

use std::num::NonZeroU64;

use crate::types::{AccountId, OrderId, Price, Quantity, ReservationSlot, Side, TimeInForce};

/// Sentinel for "no node" in the intrusive doubly-linked lists used by
/// `BookSide`. `u32::MAX` saves 4 bytes vs `Option<u32>` and keeps `OrderNode`
/// a tight 64 bytes (one cache line) on x86_64.
pub(super) const INVALID_NODE: u32 = u32::MAX;

/// Snapshot-restore output: `(account, order_id)` paired with the slab
/// index assigned to that resting order. `OrderBook::restore` consumes
/// this to populate `order_index` with valid node handles.
pub(crate) type SnapshotNodeMapping = Vec<((AccountId, OrderId), u32)>;

/// A resting order on the book (the unfilled portion of a limit order).
///
/// Carries the `ReservationSlot` so that fill and cancel paths can
/// resolve the balance reservation in O(1) without a separate HashMap
/// lookup (eliminates the old `order_info` map from Exchange).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestingOrder {
    pub(super) id: OrderId,
    pub(super) account: AccountId,
    pub(super) remaining: Quantity,
    /// Stored to support selective cancellation (e.g., EndOfDay cancels
    /// only Day orders, not GTC). IOC/FOK orders never rest, so this
    /// is always GTC, Day, or GTD in practice.
    pub(super) time_in_force: TimeInForce,
    /// Expiry time in nanoseconds (GTD orders). Zero for non-GTD.
    pub(super) expiry_ns: u64,
    /// Side of the order (Buy or Sell). Stored here so fill reports
    /// can determine buyer/seller without an external lookup.
    pub(super) side: Side,
    /// Handle into the reservation slab. Embedded here so fill and
    /// cancel paths can release/adjust the reservation in O(1) via
    /// direct Vec index, eliminating the per-order HashMap lookup that
    /// previously dominated the engine profile (~14% of cycles).
    pub(super) reservation: ReservationSlot,
}

impl RestingOrder {
    /// Create a new resting order (used by snapshot restore).
    pub(crate) fn new(
        id: OrderId,
        account: AccountId,
        remaining: Quantity,
        time_in_force: TimeInForce,
        expiry_ns: u64,
        side: Side,
        reservation: ReservationSlot,
    ) -> Self {
        Self {
            id,
            account,
            remaining,
            time_in_force,
            expiry_ns,
            side,
            reservation,
        }
    }

    pub(crate) fn id(&self) -> OrderId {
        self.id
    }

    pub(crate) fn account(&self) -> AccountId {
        self.account
    }

    pub(crate) fn remaining(&self) -> Quantity {
        self.remaining
    }

    pub(crate) fn time_in_force(&self) -> TimeInForce {
        self.time_in_force
    }

    pub(crate) fn expiry_ns(&self) -> u64 {
        self.expiry_ns
    }
}

/// One side of the order book (either all bids or all asks).
///
/// **Storage layout:** a sorted `Vec<(Price, LevelHead)>` of price levels,
/// each holding `(head, tail, len)` of an intrusive doubly-linked FIFO list
/// of `OrderNode`s. All nodes (across all price levels on this side) live in
/// a single slab `Vec<OrderNode>`; freed nodes form a singly-linked free
/// list via `next`. Indices (`u32`) are stable for the lifetime of an order
/// on the book, which lets `OrderBook::order_index` map an
/// `(AccountId, OrderId)` directly to its node — making cancel and amend
/// O(1) instead of O(level_depth).
///
/// **Level ordering — the BEST level lives at the Vec TAIL on both sides.**
/// Levels are sorted ascending by the side-relative key
/// `price ^ key_mask` (`key_mask` = 0 for bids, `u64::MAX` for asks; XOR
/// with all-ones is a monotonically order-reversing bijection on `u64`,
/// so asks are physically stored in descending price order). Matching
/// exhausts levels at the best price and new levels overwhelmingly appear
/// at or near the best price, so keeping the best at the tail makes the
/// common level birth/death a shift-free `Vec` push/pop. Profiling the
/// benchmark harness's swing scenarios (500+ live levels) showed the
/// previous both-sides-ascending layout spent ~24% of engine work
/// memmoving the ask array, whose best level sat at index 0. The mask is
/// one XOR per binary-search probe — branchless and effectively free.
///
/// **Why per-side and not a `BTreeMap`:** typical books have 5-20 active
/// levels — the sorted `Vec` fits in 1-3 L1 cache lines and binary search
/// has zero pointer-chasing. A `BTreeMap` would allocate a node per level.
///
/// **Time priority:** `head` is the oldest order at a price (matches
/// first); `tail` is the newest. Matching pops from `head`; new resting
/// orders splice onto `tail`.
#[derive(Debug)]
pub(crate) struct BookSide {
    /// Sorted ascending by `price ^ key_mask` — best level at the tail
    /// (see the struct doc). Binary search on the key for all lookups.
    levels: Vec<(Price, LevelHead)>,
    /// Slab of order nodes. Indices are stable; freed slots are recycled
    /// via the `free_head` free list.
    nodes: Vec<OrderNode>,
    /// Head of the free list, or `INVALID_NODE` if empty. Free nodes
    /// chain through `OrderNode::next`. `Default` on `u32` would give 0,
    /// which is a valid node index — so constructors initialize this to
    /// `INVALID_NODE` explicitly.
    free_head: u32,
    /// Sort-key mask: 0 for bids (ascending by price, best = highest =
    /// tail), `u64::MAX` for asks (descending by price, best = lowest =
    /// tail). XORed into every comparison key; never changes after
    /// construction.
    key_mask: u64,
}

/// Per-price-level head/tail of the intrusive list.
/// `len` lets `available_quantity` and balance audits skip walking
/// dead levels and gives O(1) "is this level empty?" checks.
#[derive(Debug, Clone, Copy)]
pub(super) struct LevelHead {
    /// Index of the oldest order (front of FIFO). `INVALID_NODE` only
    /// during transient unlink-then-relink sequences — invariant: a level
    /// in `levels` always has at least one node.
    pub(super) head: u32,
    /// Index of the newest order (back of FIFO).
    pub(super) tail: u32,
    /// Number of orders at this price. `u32` is plenty — even a pathological
    /// 4 billion-deep level would exhaust the slab first.
    pub(super) len: u32,
}

/// A node in the per-level intrusive doubly-linked list.
///
/// **Layout:** `RestingOrder` is 40 bytes plus two `u32` links — 48 bytes
/// total. Forcing 64-byte alignment was tested and *regressed* throughput
/// ~4% on the realistic-flow bench because sequential level walks
/// (`available_quantity`, `for_each_order`) lost cache density that
/// outweighed the per-node single-line read on cancel. The 48-byte
/// natural layout wins on this workload.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OrderNode {
    pub(crate) order: RestingOrder,
    /// Previous node in this level's FIFO, or `INVALID_NODE` at the head.
    /// On free, this is set to `INVALID_NODE` (the free list is singly
    /// linked through `next`).
    prev: u32,
    /// Next node in this level's FIFO, or `INVALID_NODE` at the tail.
    /// While freed, this points at the next free slot.
    next: u32,
}

impl BookSide {
    /// Sort-key mask for a side: bids ascend by price, asks descend, so
    /// both keep their best level at the Vec tail.
    #[inline]
    fn mask_for(side: Side) -> u64 {
        match side {
            Side::Buy => 0,
            Side::Sell => u64::MAX,
        }
    }

    /// Side-relative sort key. Levels are stored ascending by this key;
    /// a higher key is a better (closer to the top of book) price.
    #[inline]
    fn key(&self, price: Price) -> u64 {
        price.get() ^ self.key_mask
    }

    /// True if `price` is at or better than `limit` from this side's
    /// perspective (bids: `price >= limit`; asks: `price <= limit`).
    #[inline]
    pub(crate) fn at_or_better(&self, price: Price, limit: Price) -> bool {
        self.key(price) >= self.key(limit)
    }

    /// An empty side for the given book side.
    pub(super) fn new(side: Side) -> Self {
        Self {
            levels: Vec::new(),
            nodes: Vec::new(),
            free_head: INVALID_NODE,
            key_mask: Self::mask_for(side),
        }
    }

    /// Pre-allocate the slab. Used by `with_capacity` to avoid resize stalls
    /// once warm. The free list is left empty — `alloc_node` will push fresh
    /// entries until the Vec reaches its capacity, at which point freed
    /// nodes get reused in LIFO order.
    pub(super) fn with_capacity(side: Side, node_capacity: usize) -> Self {
        Self {
            levels: Vec::with_capacity(64),
            nodes: Vec::with_capacity(node_capacity),
            free_head: INVALID_NODE,
            key_mask: Self::mask_for(side),
        }
    }

    /// Touch every slab page so first-use page faults happen at startup
    /// rather than on the hot path. Mirrors the HashMap prefault on
    /// `OrderBook`. Pushes dummy nodes up to `capacity()` then clears
    /// the Vec — `Vec::clear` retains the allocation (and its physical
    /// pages), so subsequent `alloc_node` writes hit warm memory.
    ///
    /// **No-op when the slab is non-empty.** `Exchange::prefault` is
    /// called once at startup *after* snapshot restore has placed
    /// orders. Clearing a populated slab would leave dangling
    /// `LevelHead.head`/`tail` indices pointing at empty memory.
    /// Idempotent and safe to re-run on an empty book.
    pub(super) fn prefault(&mut self) {
        if !self.nodes.is_empty() {
            // Already has live orders → pages are faulted by the
            // existing nodes; touching them again would corrupt state.
            return;
        }
        // Build a dummy node once and reuse via `Copy`.
        let dummy = OrderNode {
            order: RestingOrder {
                id: OrderId(0),
                account: AccountId(0),
                remaining: Quantity(NonZeroU64::new(1).expect("non-zero literal")),
                time_in_force: TimeInForce::GTC,
                expiry_ns: 0,
                side: Side::Buy,
                reservation: ReservationSlot::DUMMY,
            },
            prev: INVALID_NODE,
            next: INVALID_NODE,
        };
        let cap = self.nodes.capacity();
        for _ in 0..cap {
            self.nodes.push(dummy);
        }
        self.nodes.clear();
        // Free list stays empty: subsequent `alloc_node` calls take the
        // fresh-push path, overwriting the warm pages from index 0.
        self.free_head = INVALID_NODE;
    }

    /// Binary search for a price level (by side-relative key — see the
    /// struct doc). Returns `Ok(index)` if found, `Err(index)` for the
    /// insertion point.
    ///
    /// **Neighbor-aware fast path:** the tail (best) level is checked
    /// before falling back to binary search. Matching consumes the best
    /// level, cancels and placements cluster at the top of book, and
    /// improving quotes insert past the tail — so most lookups resolve
    /// in one comparison. Non-tail lookups pay one extra compare before
    /// binary-searching the remaining prefix.
    #[inline]
    fn search(&self, price: Price) -> Result<usize, usize> {
        let target = self.key(price);
        let Some(&(last, _)) = self.levels.last() else {
            return Err(0);
        };
        let last_key = self.key(last);
        if target >= last_key {
            return if target == last_key {
                Ok(self.levels.len() - 1)
            } else {
                Err(self.levels.len())
            };
        }
        // Prefix excludes the tail we just rejected; indices returned by
        // the prefix search are valid for the full array (all < len - 1).
        self.levels[..self.levels.len() - 1].binary_search_by_key(&target, |(p, _)| self.key(*p))
    }

    /// Allocate a slab slot for `order`. Reuses a freed node if available,
    /// else grows the slab. Returns the stable node index. Caller must
    /// link the node into a level.
    #[inline]
    fn alloc_node(&mut self, order: RestingOrder) -> u32 {
        if self.free_head != INVALID_NODE {
            let idx = self.free_head;
            let node = &mut self.nodes[idx as usize];
            self.free_head = node.next;
            node.order = order;
            node.prev = INVALID_NODE;
            node.next = INVALID_NODE;
            idx
        } else {
            // Slab full — push a new entry. `as u32` is fine: the slab is
            // bounded by HashMap capacity (4K-ish) in practice.
            let idx = self.nodes.len() as u32;
            self.nodes.push(OrderNode {
                order,
                prev: INVALID_NODE,
                next: INVALID_NODE,
            });
            idx
        }
    }

    /// Return a node to the free list. Caller must have already unlinked
    /// it from its level. The freed node's `prev`/`next` are clobbered.
    #[inline]
    fn free_node(&mut self, idx: u32) {
        let node = &mut self.nodes[idx as usize];
        node.prev = INVALID_NODE;
        node.next = self.free_head;
        self.free_head = idx;
    }

    /// Push `order` onto the back (newest end) of the price level. Creates
    /// the level if it doesn't exist. Returns the stable slab index that
    /// the caller should store in `OrderBook::order_index` for O(1) cancel.
    pub(crate) fn add(&mut self, price: Price, order: RestingOrder) -> u32 {
        let new_idx = self.alloc_node(order);
        match self.search(price) {
            Ok(level_idx) => {
                // Splice onto the tail of an existing level.
                let old_tail = self.levels[level_idx].1.tail;
                self.levels[level_idx].1.tail = new_idx;
                self.levels[level_idx].1.len += 1;
                self.nodes[new_idx as usize].prev = old_tail;
                self.nodes[old_tail as usize].next = new_idx;
            }
            Err(level_idx) => {
                self.levels.insert(
                    level_idx,
                    (
                        price,
                        LevelHead {
                            head: new_idx,
                            tail: new_idx,
                            len: 1,
                        },
                    ),
                );
            }
        }
        new_idx
    }

    /// Splice `node_idx` out of the level at `level_idx`, free the slab
    /// slot, and remove the level from `levels` if it became empty.
    /// Returns the removed `RestingOrder`. Caller has already located the
    /// level — used by `remove_node` and `pop_front` to skip a redundant
    /// binary search on the hot path.
    fn unlink_node_at_level(&mut self, level_idx: usize, node_idx: u32) -> RestingOrder {
        let prev = self.nodes[node_idx as usize].prev;
        let next = self.nodes[node_idx as usize].next;

        // Splice out of the doubly-linked list.
        if prev != INVALID_NODE {
            self.nodes[prev as usize].next = next;
        }
        if next != INVALID_NODE {
            self.nodes[next as usize].prev = prev;
        }

        let head = &mut self.levels[level_idx].1;
        if head.head == node_idx {
            head.head = next;
        }
        if head.tail == node_idx {
            head.tail = prev;
        }
        head.len -= 1;
        let became_empty = head.len == 0;

        let order = self.nodes[node_idx as usize].order;
        self.free_node(node_idx);
        if became_empty {
            self.levels.remove(level_idx);
        }
        order
    }

    /// Remove a node from the book in O(1) given its slab index and the
    /// price level it belongs to. Frees the slab slot. Removes the price
    /// level from `levels` if it becomes empty. Returns the removed
    /// `RestingOrder`, or `None` if `price` doesn't match a live level.
    pub(crate) fn remove_node(&mut self, price: Price, node_idx: u32) -> Option<RestingOrder> {
        let level_idx = self.search(price).ok()?;
        Some(self.unlink_node_at_level(level_idx, node_idx))
    }

    /// Price and front (oldest, highest-priority) node of the BEST level,
    /// or `None` if the side is empty. O(1) tail peek — the matching
    /// loop's outer guard, replacing the old price-keyed
    /// `front_node_idx` and its per-maker binary search.
    #[inline]
    pub(crate) fn best_front(&self) -> Option<(Price, u32)> {
        self.levels.last().map(|&(price, head)| (price, head.head))
    }

    /// Pop the front (oldest) order of the BEST level. Frees the slab
    /// slot and removes the level if it becomes empty. O(1) — no price
    /// search (the best level is the tail) and a shift-free `Vec::pop`
    /// on level death. Matching only ever consumes the best level, so
    /// this replaces the old price-keyed `pop_front` there; when the
    /// level empties, the next-best level becomes the new tail.
    /// Returns `(node_idx, order)` so callers can clean up auxiliary
    /// state.
    pub(crate) fn pop_front_best(&mut self) -> Option<(u32, RestingOrder)> {
        let level_idx = self.levels.len().checked_sub(1)?;
        let head_idx = self.levels[level_idx].1.head;
        let order = self.unlink_node_at_level(level_idx, head_idx);
        Some((head_idx, order))
    }

    /// Borrow a node by slab index. Used by the matching loop to read the
    /// front maker's metadata without locking the borrow checker.
    #[inline]
    pub(crate) fn node(&self, idx: u32) -> &OrderNode {
        &self.nodes[idx as usize]
    }

    /// Mutably borrow a node by slab index. Used to apply partial fills
    /// in-place.
    #[inline]
    pub(crate) fn node_mut(&mut self, idx: u32) -> &mut OrderNode {
        &mut self.nodes[idx as usize]
    }

    /// Physical level index for the i-th level in ascending PRICE order.
    /// Identity for bids; reversed for asks (stored descending). Keeps
    /// every externally observable walk (snapshots, bulk cancels) in the
    /// same canonical ascending order regardless of the side's physical
    /// layout. Cold paths only.
    #[inline]
    fn ascending_idx(&self, i: usize) -> usize {
        if self.key_mask == 0 {
            i
        } else {
            self.levels.len() - 1 - i
        }
    }

    /// Iterate every order on this side, calling `f` with the price level
    /// and a reference to each order. Walks levels in ascending price
    /// order, and within a level walks oldest→newest. Used by snapshot
    /// and bulk-cancel paths.
    pub(crate) fn for_each_order<F: FnMut(Price, &RestingOrder)>(&self, mut f: F) {
        for i in 0..self.levels.len() {
            let (price, head) = self.levels[self.ascending_idx(i)];
            let mut cur = head.head;
            while cur != INVALID_NODE {
                let n = &self.nodes[cur as usize];
                f(price, &n.order);
                cur = n.next;
            }
        }
    }

    /// Mutable variant of `for_each_order`. Used by snapshot-restore slot
    /// injection to patch reservation slots in place.
    pub(crate) fn for_each_order_mut<F: FnMut(Price, &mut RestingOrder)>(&mut self, mut f: F) {
        for i in 0..self.levels.len() {
            let (price, head) = self.levels[self.ascending_idx(i)];
            let mut cur = head.head;
            while cur != INVALID_NODE {
                // Split borrow: read links before handing &mut order to `f`.
                let next = self.nodes[cur as usize].next;
                f(price, &mut self.nodes[cur as usize].order);
                cur = next;
            }
        }
    }

    /// Snapshot: walk every level in ascending PRICE order (canonical —
    /// independent of the side's physical layout, keeping the snapshot
    /// format stable), yielding `(price, ordered_orders)` where
    /// `ordered_orders` preserves time priority (oldest first). Used by
    /// the snapshot codec — not on the hot path, so the per-level `Vec`
    /// allocation is fine.
    pub(crate) fn levels_snapshot(&self) -> Vec<(Price, Vec<RestingOrder>)> {
        (0..self.levels.len())
            .map(|i| {
                let (price, head) = self.levels[self.ascending_idx(i)];
                let mut v = Vec::with_capacity(head.len as usize);
                let mut cur = head.head;
                while cur != INVALID_NODE {
                    let n = &self.nodes[cur as usize];
                    v.push(n.order);
                    cur = n.next;
                }
                (price, v)
            })
            .collect()
    }

    /// Reconstruct a `BookSide` from snapshot levels (canonical ascending
    /// price order). Returns `(side, mapping)` where `mapping` records the
    /// slab index assigned to each `(account, order_id)` so the caller can
    /// populate `OrderBook::order_index` with valid node indices.
    pub(crate) fn from_levels_snapshot(
        side: Side,
        mut levels: Vec<(Price, Vec<RestingOrder>)>,
    ) -> (Self, SnapshotNodeMapping) {
        // Pre-size the slab to the total order count to avoid re-allocations.
        let total: usize = levels.iter().map(|(_, v)| v.len()).sum();
        let mut out = Self::with_capacity(side, total.max(64));
        let mut mapping = Vec::with_capacity(total);
        // Insert levels in this side's physical order (worst first) so
        // every level append lands at the Vec tail — O(1) instead of a
        // front-shifting O(n²) restore for the descending side. In-place
        // reverse of the level Vec, not the orders within a level (FIFO
        // time priority is per-level and must be preserved).
        if out.key_mask != 0 {
            levels.reverse();
        }
        for (price, orders) in levels {
            for order in orders {
                let key = (order.account, order.id);
                let idx = out.add(price, order);
                mapping.push((key, idx));
            }
        }
        (out, mapping)
    }

    /// True if no resting orders remain on this side.
    pub(crate) fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Best price on this side: highest for bids, lowest for asks. The
    /// best level lives at the tail on both sides (see the struct doc),
    /// so this is `last()` unconditionally.
    pub(super) fn best_price(&self) -> Option<Price> {
        self.levels.last().map(|(p, _)| *p)
    }

    /// Total resting quantity at one exact price level, or 0 if the level
    /// does not exist. Read-only book introspection (market-data / audit
    /// queries); not on the matching hot path.
    pub(super) fn depth_at(&self, price: Price) -> u64 {
        let Ok(idx) = self.search(price) else {
            return 0;
        };
        let mut total: u64 = 0;
        let mut cur = self.levels[idx].1.head;
        while cur != INVALID_NODE {
            let n = &self.nodes[cur as usize];
            total = total.saturating_add(n.order.remaining.get());
            cur = n.next;
        }
        total
    }

    /// Total available quantity at prices that would match the given limit.
    /// If `exclude_account` is `Some`, orders from that account are skipped
    /// (used for FOK pre-check with STP CancelNewest/CancelBoth).
    ///
    /// Walks levels from best→worst (physical tail→front on both sides)
    /// until the level no longer satisfies `limit`; within a level, walks
    /// the linked list head→tail (which is order-agnostic for summing).
    pub(super) fn available_quantity(
        &self,
        limit: Option<Price>,
        exclude_account: Option<AccountId>,
    ) -> u64 {
        let mut total: u64 = 0;
        for (price, head) in self.levels.iter().rev() {
            if let Some(limit) = limit
                && !self.at_or_better(*price, limit)
            {
                break;
            }
            let mut cur = head.head;
            while cur != INVALID_NODE {
                let n = &self.nodes[cur as usize];
                if exclude_account.is_none_or(|acct| acct != n.order.account) {
                    total = total.saturating_add(n.order.remaining.get());
                }
                cur = n.next;
            }
        }
        total
    }
}

/// Direct tests for the side-relative level ordering contracts. The
/// matching/snapshot behavior built on top is covered by the orderbook
/// tests and proptests; these pin the layout invariants themselves so a
/// regression fails here with a precise message rather than surfacing as
/// a changed snapshot byte stream or report order.
#[cfg(test)]
mod tests {
    use super::*;

    fn p(v: u64) -> Price {
        Price(NonZeroU64::new(v).unwrap())
    }

    fn order(id: u64, qty: u64, side: Side) -> RestingOrder {
        RestingOrder {
            id: OrderId(id),
            account: AccountId(1),
            remaining: Quantity(NonZeroU64::new(qty).unwrap()),
            time_in_force: TimeInForce::GTC,
            expiry_ns: 0,
            side,
            reservation: ReservationSlot::DUMMY,
        }
    }

    /// Build a side with levels at 100/90/110 (inserted out of order),
    /// one unit-qty order per level.
    fn three_level_side(side: Side) -> BookSide {
        let mut s = BookSide::new(side);
        for (id, price) in [(1, 100), (2, 90), (3, 110)] {
            s.add(p(price), order(id, 1, side));
        }
        s
    }

    /// The physical `levels` Vec must be sorted ascending by the
    /// side-relative key — every search/insert/best-at-tail property
    /// rests on this.
    fn assert_key_sorted(s: &BookSide) {
        let keys: Vec<u64> = s.levels.iter().map(|(price, _)| s.key(*price)).collect();
        assert!(keys.is_sorted(), "levels not sorted by side key: {keys:?}");
    }

    #[test]
    fn best_level_lives_at_the_tail_on_both_sides() {
        let bids = three_level_side(Side::Buy);
        assert_key_sorted(&bids);
        assert_eq!(bids.best_price(), Some(p(110)));
        assert_eq!(bids.levels.last().map(|(price, _)| *price), Some(p(110)));

        let asks = three_level_side(Side::Sell);
        assert_key_sorted(&asks);
        assert_eq!(asks.best_price(), Some(p(90)));
        assert_eq!(asks.levels.last().map(|(price, _)| *price), Some(p(90)));
    }

    #[test]
    fn best_price_advances_to_next_level_after_exhaustion() {
        let mut bids = three_level_side(Side::Buy);
        bids.pop_front_best().unwrap();
        assert_eq!(bids.best_price(), Some(p(100)));

        let mut asks = three_level_side(Side::Sell);
        asks.pop_front_best().unwrap();
        assert_eq!(asks.best_price(), Some(p(100)));
        assert_key_sorted(&asks);
    }

    /// `pop_front_best` must drain FIFO within the best level (oldest
    /// first), then advance to the next-best level, on both sides —
    /// this is the exact walk the matching loop performs.
    #[test]
    fn pop_front_best_drains_best_to_worst_fifo() {
        for side in [Side::Buy, Side::Sell] {
            let mut s = BookSide::new(side);
            // Two orders per level; ids encode (price, arrival) so the
            // expected pop order is fully determined.
            let mut id = 0;
            for price in [90, 100, 110] {
                for _ in 0..2 {
                    id += 1;
                    s.add(p(price), order(id, 1, side));
                }
            }
            let expected_prices: Vec<u64> = match side {
                Side::Buy => vec![110, 110, 100, 100, 90, 90],
                Side::Sell => vec![90, 90, 100, 100, 110, 110],
            };
            let expected_ids: Vec<u64> = match side {
                Side::Buy => vec![5, 6, 3, 4, 1, 2],
                Side::Sell => vec![1, 2, 3, 4, 5, 6],
            };
            let mut popped_prices = Vec::new();
            let mut popped_ids = Vec::new();
            while let Some((price, _)) = s.best_front() {
                let (_, o) = s.pop_front_best().unwrap();
                popped_prices.push(price.get());
                popped_ids.push(o.id.0);
            }
            assert_eq!(popped_prices, expected_prices, "{side:?}");
            assert_eq!(popped_ids, expected_ids, "{side:?}");
            assert!(s.is_empty(), "{side:?}");
            assert_eq!(s.pop_front_best(), None, "{side:?}");
        }
    }

    #[test]
    fn best_front_returns_oldest_order_at_best_level() {
        for side in [Side::Buy, Side::Sell] {
            let mut s = BookSide::new(side);
            s.add(p(100), order(1, 1, side));
            s.add(p(100), order(2, 1, side));
            let (price, front_idx) = s.best_front().unwrap();
            assert_eq!(price, p(100), "{side:?}");
            assert_eq!(s.node(front_idx).order.id, OrderId(1), "{side:?}");
        }
        assert_eq!(BookSide::new(Side::Buy).best_front(), None);
    }

    /// The `search` tail fast path must agree with the fallback binary
    /// search for every relative position: better than best (new tail),
    /// equal to best, existing inner level, and missing inner/worst
    /// levels. Regressions here would corrupt level ordering silently.
    #[test]
    fn search_fast_path_and_fallback_agree() {
        for side in [Side::Buy, Side::Sell] {
            // Levels at 90/100/110; probe every price in and around them
            // by exercising `add` (search is private — `add` hits both
            // the Ok and Err paths) and checking the resulting order.
            let mut s = three_level_side(side);
            for probe in [80, 85, 95, 100, 105, 110, 115, 120] {
                s.add(p(probe), order(1000 + probe, 1, side));
            }
            assert_key_sorted(&s);
            let snapshot_prices: Vec<u64> = s
                .levels_snapshot()
                .into_iter()
                .map(|(price, _)| price.get())
                .collect();
            assert_eq!(
                snapshot_prices,
                vec![80, 85, 90, 95, 100, 105, 110, 115, 120],
                "{side:?}"
            );
        }
    }

    #[test]
    fn at_or_better_polarity() {
        let bids = BookSide::new(Side::Buy);
        assert!(bids.at_or_better(p(110), p(100)));
        assert!(bids.at_or_better(p(100), p(100)));
        assert!(!bids.at_or_better(p(90), p(100)));

        let asks = BookSide::new(Side::Sell);
        assert!(asks.at_or_better(p(90), p(100)));
        assert!(asks.at_or_better(p(100), p(100)));
        assert!(!asks.at_or_better(p(110), p(100)));
    }

    #[test]
    fn canonical_walks_are_ascending_by_price_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let s = three_level_side(side);

            let snapshot_prices: Vec<Price> = s
                .levels_snapshot()
                .into_iter()
                .map(|(price, _)| price)
                .collect();
            assert_eq!(snapshot_prices, vec![p(90), p(100), p(110)], "{side:?}");

            let mut walked = Vec::new();
            s.for_each_order(|price, _| walked.push(price));
            assert_eq!(walked, vec![p(90), p(100), p(110)], "{side:?}");
        }
    }

    #[test]
    fn from_levels_snapshot_round_trips_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            // Two orders per level so FIFO order within a level is observable.
            let mut original = BookSide::new(side);
            let mut id = 0;
            for price in [90, 100, 110] {
                for _ in 0..2 {
                    id += 1;
                    original.add(p(price), order(id, id, side));
                }
            }

            let snapshot = original.levels_snapshot();
            let (restored, mapping) = BookSide::from_levels_snapshot(side, snapshot.clone());

            assert_key_sorted(&restored);
            assert_eq!(restored.levels_snapshot(), snapshot, "{side:?}");
            assert_eq!(restored.best_price(), original.best_price(), "{side:?}");

            // Every mapping entry must point at the slab node holding
            // that exact order.
            assert_eq!(mapping.len(), 6, "{side:?}");
            for ((account, order_id), idx) in mapping {
                let node = restored.node(idx);
                assert_eq!((node.order.account, node.order.id), (account, order_id));
            }
        }
    }

    #[test]
    fn available_quantity_stops_at_the_limit_on_both_sides() {
        // Levels 90/100/110 with qty 1 each (from three_level_side).
        let bids = three_level_side(Side::Buy);
        assert_eq!(bids.available_quantity(Some(p(100)), None), 2); // 110 + 100
        assert_eq!(bids.available_quantity(None, None), 3);

        let asks = three_level_side(Side::Sell);
        assert_eq!(asks.available_quantity(Some(p(100)), None), 2); // 90 + 100
        assert_eq!(asks.available_quantity(None, None), 3);
    }
}
