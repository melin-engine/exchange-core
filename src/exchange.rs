//! Exchange: dispatches orders to per-instrument order books.
//!
//! All order books run on a single thread (LMAX-style). This keeps event
//! ordering deterministic and allows portfolio-wide risk checks (margin,
//! exposure limits) without cross-thread coordination.
//!
//! If throughput exceeds a single core, shard by instrument — each shard
//! stays single-threaded. Note: portfolio risk checks then require
//! cross-shard message passing, adding latency and complexity.

use std::collections::HashMap;

use crate::orderbook::OrderBook;
use crate::types::{ExecutionReport, Order, OrderId, Symbol};

/// Top-level exchange managing multiple instruments.
pub struct Exchange {
    /// HashMap for symbol → order book dispatch. O(1) amortized lookup.
    // TODO: If profiling shows hashing overhead on the hot path, consider
    // replacing with a pre-allocated `OrderBook` array indexed by
    // Symbol(u32), giving true O(1) dispatch with no hashing.
    books: HashMap<Symbol, OrderBook>,
}

impl Exchange {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
        }
    }

    /// Register a new instrument. Must be called before submitting orders.
    pub fn add_instrument(&mut self, symbol: Symbol) {
        self.books.entry(symbol).or_default();
    }

    /// Submit an order to the matching engine for the given instrument.
    /// Returns `false` if the symbol is not registered.
    pub fn execute(
        &mut self,
        symbol: Symbol,
        order: Order,
        reports: &mut Vec<ExecutionReport>,
    ) -> bool {
        let Some(book) = self.books.get_mut(&symbol) else {
            return false;
        };
        book.execute(order, reports);
        true
    }

    /// Cancel a resting order on the given instrument.
    /// Returns `false` if the symbol is not registered.
    pub fn cancel(
        &mut self,
        symbol: Symbol,
        order_id: OrderId,
        reports: &mut Vec<ExecutionReport>,
    ) -> bool {
        let Some(book) = self.books.get_mut(&symbol) else {
            return false;
        };
        book.cancel(order_id, reports);
        true
    }
}

impl Default for Exchange {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::types::{OrderType, Price, Quantity, RejectReason, Side, TimeInForce};

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn limit_order(id: u64, side: Side, p: u64, q: u64, tif: TimeInForce) -> Order {
        Order {
            id: OrderId(id),
            side,
            order_type: OrderType::Limit { price: price(p) },
            time_in_force: tif,
            quantity: qty(q),
        }
    }

    fn market_order(id: u64, side: Side, q: u64) -> Order {
        Order {
            id: OrderId(id),
            side,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::IOC,
            quantity: qty(q),
        }
    }

    #[test]
    fn execute_on_unknown_symbol_returns_false() {
        let mut exchange = Exchange::new();
        let mut reports = Vec::new();

        let result = exchange.execute(
            Symbol(1),
            limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC),
            &mut reports,
        );

        assert!(!result);
        assert!(reports.is_empty());
    }

    #[test]
    fn orders_on_different_symbols_are_isolated() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        let eth = Symbol(2);
        exchange.add_instrument(btc);
        exchange.add_instrument(eth);

        let mut reports = Vec::new();

        // Place a sell on BTC.
        exchange.execute(btc, limit_order(1, Side::Sell, 100, 10, TimeInForce::GTC), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        // Market buy on ETH should find no liquidity — books are isolated.
        exchange.execute(eth, market_order(2, Side::Buy, 10), &mut reports);
        assert_eq!(reports[0], ExecutionReport::Rejected {
            order_id: OrderId(2),
            reason: RejectReason::NoLiquidity,
        });
        reports.clear();

        // Market buy on BTC should match.
        exchange.execute(btc, market_order(3, Side::Buy, 10), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Fill { .. }));
    }

    #[test]
    fn cancel_on_unknown_symbol_returns_false() {
        let mut exchange = Exchange::new();
        let mut reports = Vec::new();

        let result = exchange.cancel(Symbol(1), OrderId(1), &mut reports);

        assert!(!result);
        assert!(reports.is_empty());
    }

    #[test]
    fn cancel_across_symbols() {
        let mut exchange = Exchange::new();
        let btc = Symbol(1);
        exchange.add_instrument(btc);

        let mut reports = Vec::new();

        exchange.execute(btc, limit_order(1, Side::Buy, 100, 10, TimeInForce::GTC), &mut reports);
        reports.clear();

        exchange.cancel(btc, OrderId(1), &mut reports);
        assert!(matches!(reports[0], ExecutionReport::Cancelled { .. }));
    }
}
