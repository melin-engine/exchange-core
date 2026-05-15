//! Per-symbol partial book tracker driven by an ITCH event stream.
//!
//! Maintains enough state to:
//!   - Resolve 'E' Order Executed messages to the resting order's
//!     price and side (ITCH 'E' carries only `order_ref` and `shares`).
//!   - Report the best bid and best ask of any tracked symbol at any
//!     moment, so the stats layer can compute price-distance-from-BBO
//!     for each new add.
//!   - Surface order-level metadata (original size, current remaining)
//!     for partial-cancel fraction stats.
//!
//! Only price levels — not individual orders — live in the per-symbol
//! BTreeMaps. The order ↔ level relationship is reconstructed via the
//! flat order map: every order knows which level it sits on.
//!
//! Sizing notes for a full-venue ITCH 5.0 trading day (thousands of
//! symbols, ~100M adds): peak live-order count is on the order of 1–5M,
//! giving a working set of ~100–400 MB for `order_map`. The per-symbol
//! books together hold low-millions of distinct price levels.

use std::collections::{BTreeMap, HashMap};

use super::Side;

/// State of one live order on the book.
#[derive(Debug, Clone, Copy)]
pub struct OrderState {
    pub stock_locate: u16,
    pub side: Side,
    pub price: u32,
    /// Remaining shares after any partial cancels/executes. Drops to
    /// zero when the order is fully consumed (the tracker removes it
    /// at that point).
    pub remaining_shares: u32,
    /// Original shares when the order was added. Kept so partial-cancel
    /// fractions can be reported relative to the order's initial size.
    pub original_shares: u32,
}

/// Per-symbol level-aggregated book. Both sides keyed by price; the
/// value is the number of resting orders at that level. The level
/// disappears when the count drops to zero.
///
/// BTreeMap is used (vs HashMap) because best-bid / best-ask lookup
/// requires ordered access — `last_key_value` and `first_key_value`
/// give us O(log N) BBO without an extra cache. Per-symbol level
/// counts stay small (typically <2_000 distinct levels even for
/// mega-caps), so BTreeMap overhead is dominated by HashMap savings
/// elsewhere.
#[derive(Debug, Default)]
struct SymbolBook {
    bids: BTreeMap<u32, u64>,
    asks: BTreeMap<u32, u64>,
}

/// Errors that can surface while applying an event. None of them are
/// fatal — they almost always indicate state inherited from the
/// previous trading session (orders that existed before our parser
/// joined the stream) and the caller can choose to count and ignore.
#[derive(Debug)]
pub enum TrackerError {
    /// Cancel/Delete/Execute/Replace referenced an `order_ref` we
    /// never saw an Add for. Common at the start of a session for
    /// orders that carry over from the previous day.
    UnknownOrder { order_ref: u64 },
    /// Execute or Cancel asked for more shares than the order had
    /// remaining. Should not happen on well-formed ITCH but is
    /// surfaced so corruption shows up rather than silently produces
    /// negative-looking state.
    ShareUnderflow {
        order_ref: u64,
        remaining: u32,
        requested: u32,
    },
    /// Replace's new order ref collided with one we already track.
    /// Indicates either upstream data corruption or a parser bug.
    NewRefAlreadyExists { new_ref: u64 },
}

pub struct BookTracker {
    /// `order_ref → state`. HashMap (not Vec) because order_ref space
    /// is sparse and large (~120M distinct refs per session); a flat
    /// array would waste hundreds of MB.
    order_map: HashMap<u64, OrderState>,
    /// Indexed by `stock_locate`. `Vec<Option<_>>` over HashMap because
    /// stock_locate codes are dense (1..=N, N≈9000) so direct indexing
    /// is cheaper than a hash.
    books: Vec<Option<SymbolBook>>,
}

impl BookTracker {
    pub fn new() -> Self {
        Self {
            order_map: HashMap::new(),
            // Pre-size for a typical full-venue symbol count; grows on demand.
            books: Vec::with_capacity(10_000),
        }
    }

    pub fn live_order_count(&self) -> usize {
        self.order_map.len()
    }

    /// Best bid for `stock_locate`, or `None` if the bid side is empty
    /// or the symbol is untracked.
    pub fn best_bid(&self, stock_locate: u16) -> Option<u32> {
        self.books
            .get(stock_locate as usize)
            .and_then(|b| b.as_ref())
            .and_then(|b| b.bids.last_key_value().map(|(p, _)| *p))
    }

    /// Best ask for `stock_locate`, or `None` if the ask side is empty
    /// or the symbol is untracked.
    pub fn best_ask(&self, stock_locate: u16) -> Option<u32> {
        self.books
            .get(stock_locate as usize)
            .and_then(|b| b.as_ref())
            .and_then(|b| b.asks.first_key_value().map(|(p, _)| *p))
    }

    /// Look up an order without mutating state. Used by the stats
    /// layer to capture the resting price/side of an order before
    /// applying an Execute that would otherwise erase it.
    pub fn get(&self, order_ref: u64) -> Option<&OrderState> {
        self.order_map.get(&order_ref)
    }

    /// Insert a new order on the book.
    pub fn add(
        &mut self,
        order_ref: u64,
        stock_locate: u16,
        side: Side,
        price: u32,
        shares: u32,
    ) -> Result<(), TrackerError> {
        if self.order_map.contains_key(&order_ref) {
            return Err(TrackerError::NewRefAlreadyExists { new_ref: order_ref });
        }
        self.ensure_book(stock_locate);
        let book = self.books[stock_locate as usize]
            .as_mut()
            .expect("ensure_book just created it");
        let levels = match side {
            Side::Buy => &mut book.bids,
            Side::Sell => &mut book.asks,
        };
        *levels.entry(price).or_insert(0) += 1;
        self.order_map.insert(
            order_ref,
            OrderState {
                stock_locate,
                side,
                price,
                remaining_shares: shares,
                original_shares: shares,
            },
        );
        Ok(())
    }

    /// Fully remove an order. Returns its prior state so callers can
    /// account for it.
    pub fn delete(&mut self, order_ref: u64) -> Result<OrderState, TrackerError> {
        let state = self
            .order_map
            .remove(&order_ref)
            .ok_or(TrackerError::UnknownOrder { order_ref })?;
        self.decrement_level(state.stock_locate, state.side, state.price);
        Ok(state)
    }

    /// Reduce an order's remaining shares by `cancelled_shares` (ITCH
    /// 'X'). If the remaining reaches zero, the order is removed
    /// from the book.
    ///
    /// Returns the order state **after** the cancel was applied (or
    /// the last-known state if the order was fully removed).
    pub fn cancel_partial(
        &mut self,
        order_ref: u64,
        cancelled_shares: u32,
    ) -> Result<OrderState, TrackerError> {
        self.consume_shares(order_ref, cancelled_shares)
    }

    /// Apply an execute against a resting order (ITCH 'E' or 'C').
    /// Identical bookkeeping to a partial cancel.
    pub fn execute(
        &mut self,
        order_ref: u64,
        executed_shares: u32,
    ) -> Result<OrderState, TrackerError> {
        self.consume_shares(order_ref, executed_shares)
    }

    /// Atomic cancel-and-add (ITCH 'U'). Returns the prior state so
    /// the stats layer can compute price/size deltas.
    pub fn replace(
        &mut self,
        old_ref: u64,
        new_ref: u64,
        new_price: u32,
        new_shares: u32,
    ) -> Result<OrderState, TrackerError> {
        let prior = self.delete(old_ref)?;
        // Use prior order's side and symbol — Replace doesn't carry them.
        self.add(
            new_ref,
            prior.stock_locate,
            prior.side,
            new_price,
            new_shares,
        )?;
        Ok(prior)
    }

    fn consume_shares(&mut self, order_ref: u64, shares: u32) -> Result<OrderState, TrackerError> {
        let entry = self
            .order_map
            .get_mut(&order_ref)
            .ok_or(TrackerError::UnknownOrder { order_ref })?;
        if shares > entry.remaining_shares {
            return Err(TrackerError::ShareUnderflow {
                order_ref,
                remaining: entry.remaining_shares,
                requested: shares,
            });
        }
        entry.remaining_shares -= shares;
        if entry.remaining_shares == 0 {
            // Fully consumed: drop from book the same way Delete does.
            let state = *entry;
            self.order_map.remove(&order_ref);
            self.decrement_level(state.stock_locate, state.side, state.price);
            Ok(state)
        } else {
            Ok(*entry)
        }
    }

    fn ensure_book(&mut self, stock_locate: u16) {
        let idx = stock_locate as usize;
        if idx >= self.books.len() {
            self.books.resize_with(idx + 1, || None);
        }
        if self.books[idx].is_none() {
            self.books[idx] = Some(SymbolBook::default());
        }
    }

    fn decrement_level(&mut self, stock_locate: u16, side: Side, price: u32) {
        let Some(Some(book)) = self.books.get_mut(stock_locate as usize) else {
            // Symbol was never seen on the add side — should be unreachable
            // because order_map only has entries we Added. We avoid a panic
            // here so a malformed feed doesn't take down the extractor.
            return;
        };
        let levels = match side {
            Side::Buy => &mut book.bids,
            Side::Sell => &mut book.asks,
        };
        // entry().and_modify(|c| ...) would still leave a zero entry
        // around; we want the level to disappear when empty so best-bid
        // lookups remain meaningful.
        if let Some(count) = levels.get_mut(&price) {
            *count -= 1;
            if *count == 0 {
                levels.remove(&price);
            }
        }
    }
}

impl Default for BookTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_then_bbo() {
        let mut t = BookTracker::new();
        t.add(1, 7, Side::Buy, 10_000, 100).unwrap();
        t.add(2, 7, Side::Buy, 10_050, 50).unwrap();
        t.add(3, 7, Side::Sell, 10_100, 80).unwrap();
        t.add(4, 7, Side::Sell, 10_150, 30).unwrap();
        assert_eq!(t.best_bid(7), Some(10_050));
        assert_eq!(t.best_ask(7), Some(10_100));
        assert_eq!(t.live_order_count(), 4);
    }

    #[test]
    fn delete_updates_bbo_when_top_disappears() {
        let mut t = BookTracker::new();
        t.add(1, 1, Side::Buy, 10_000, 100).unwrap();
        t.add(2, 1, Side::Buy, 10_050, 100).unwrap();
        assert_eq!(t.best_bid(1), Some(10_050));
        let prior = t.delete(2).unwrap();
        assert_eq!(prior.price, 10_050);
        assert_eq!(t.best_bid(1), Some(10_000));
    }

    #[test]
    fn partial_cancel_keeps_order_on_book() {
        let mut t = BookTracker::new();
        t.add(1, 1, Side::Buy, 9_000, 500).unwrap();
        let state = t.cancel_partial(1, 200).unwrap();
        assert_eq!(state.remaining_shares, 300);
        assert_eq!(state.original_shares, 500);
        assert_eq!(t.best_bid(1), Some(9_000));
    }

    #[test]
    fn execute_drains_then_removes() {
        let mut t = BookTracker::new();
        t.add(1, 1, Side::Sell, 11_000, 100).unwrap();
        let s1 = t.execute(1, 60).unwrap();
        assert_eq!(s1.remaining_shares, 40);
        assert_eq!(t.best_ask(1), Some(11_000));
        let s2 = t.execute(1, 40).unwrap();
        assert_eq!(s2.remaining_shares, 0);
        assert_eq!(t.best_ask(1), None, "level should disappear when drained");
        assert!(t.get(1).is_none());
    }

    #[test]
    fn replace_carries_over_side_and_symbol() {
        let mut t = BookTracker::new();
        t.add(1, 5, Side::Buy, 9_000, 100).unwrap();
        let prior = t.replace(1, 2, 9_100, 80).unwrap();
        assert_eq!(prior.price, 9_000);
        assert_eq!(prior.original_shares, 100);
        let new_state = t.get(2).expect("new order present");
        assert_eq!(new_state.stock_locate, 5);
        assert_eq!(new_state.side, Side::Buy);
        assert_eq!(new_state.price, 9_100);
        assert_eq!(new_state.remaining_shares, 80);
        assert_eq!(t.best_bid(5), Some(9_100));
        assert!(t.get(1).is_none());
    }

    #[test]
    fn unknown_order_surfaces_error_without_panicking() {
        let mut t = BookTracker::new();
        match t.delete(999) {
            Err(TrackerError::UnknownOrder { order_ref: 999 }) => {}
            other => panic!("expected UnknownOrder, got {other:?}"),
        }
    }

    #[test]
    fn share_underflow_surfaces_error() {
        let mut t = BookTracker::new();
        t.add(1, 1, Side::Buy, 5_000, 100).unwrap();
        match t.execute(1, 200) {
            Err(TrackerError::ShareUnderflow {
                order_ref: 1,
                remaining: 100,
                requested: 200,
            }) => {}
            other => panic!("expected ShareUnderflow, got {other:?}"),
        }
    }

    #[test]
    fn level_count_tracks_multiple_orders_at_same_price() {
        let mut t = BookTracker::new();
        t.add(1, 1, Side::Buy, 10_000, 100).unwrap();
        t.add(2, 1, Side::Buy, 10_000, 200).unwrap();
        t.add(3, 1, Side::Buy, 9_900, 50).unwrap();
        assert_eq!(t.best_bid(1), Some(10_000));
        t.delete(1).unwrap();
        // Still one order at 10_000 — best bid should stay.
        assert_eq!(t.best_bid(1), Some(10_000));
        t.delete(2).unwrap();
        // Now best bid drops to next level.
        assert_eq!(t.best_bid(1), Some(9_900));
    }
}
