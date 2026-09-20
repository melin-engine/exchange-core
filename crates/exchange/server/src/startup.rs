//! What a trading node hands the sequencer runtime at startup.
//!
//! Three things, deliberately kept apart because they travel differently:
//!
//! - **Genesis** — the bulk `AddInstrument` / `ProvisionAccount` seed.
//!   Journaled once, by the node that creates the journal as primary;
//!   every other node receives it in the history.
//! - **Account limits** — the SEC-03 open-order cap and SEC-04 rate
//!   limiter. Journaled as a `SetAccountLimits` event every time a node
//!   becomes primary, from that node's own flags, so the limits in force
//!   are always the serving primary's and replay reproduces the decisions
//!   they made. A node's own values take effect only once it is primary.
//! - **Sizing** — how many accounts and instruments to reserve memory for.
//!   Capacity, never state: it stays on the node and reaches the engine
//!   only through `Application::prefault`.
//!
//! Keeps the runtime free of trading event variants.

use melin_ec_trading::trading_event::TradingEvent;
use melin_ec_types::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};
use melin_server_runtime::StartupEvents;

/// The trading node's startup configuration, built from the command line.
#[derive(Debug, Clone, Copy)]
pub struct StartupConfig {
    /// Accounts to provision in genesis, and to size for.
    pub accounts: u32,
    /// Instruments to register in genesis, and to size for.
    pub instruments: u32,
    /// SEC-03: maximum simultaneously open orders per account. `0` means
    /// unlimited.
    pub max_orders_per_account: u32,
    /// SEC-04: token-bucket refill rate, orders per second. `0` disables
    /// the limiter.
    pub max_orders_per_second: u32,
    /// SEC-04: token-bucket capacity (max burst). `0` disables the
    /// limiter.
    pub max_orders_burst: u32,
}

/// What the node reserves memory for: `ServerApp`'s `Application::Sizing`.
/// Local to the node and never journaled, so it may differ between nodes
/// without any effect on what they decide.
///
/// `u32` counts, matching the `AccountId` and `Symbol` spaces they size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExchangeSizing {
    pub accounts: u32,
    pub instruments: u32,
}

impl StartupConfig {
    /// The events the runtime journals on this node's behalf: the genesis
    /// seed, then the account limits.
    pub fn startup_events(&self) -> StartupEvents<TradingEvent> {
        // The only trace of the values a node brings to the cluster: they
        // take effect once journaled, when the node becomes primary.
        tracing::info!(
            max_orders_per_account = self.max_orders_per_account,
            max_orders_per_second = self.max_orders_per_second,
            max_orders_burst = self.max_orders_burst,
            "per-account order limits to journal on becoming primary (SEC-03 cap, SEC-04 rate)"
        );
        StartupEvents {
            genesis: self.genesis(),
            on_primary: vec![self.account_limits()],
        }
    }

    /// The node's sizing, from the same counts genesis seeds.
    pub fn sizing(&self) -> ExchangeSizing {
        ExchangeSizing {
            accounts: self.accounts,
            instruments: self.instruments,
        }
    }

    /// Instruments first, then accounts: `ProvisionAccount` funds an
    /// account in every currency of every instrument registered so far.
    fn genesis(&self) -> Vec<TradingEvent> {
        let mut events = Vec::with_capacity(self.instruments as usize + self.accounts as usize);
        for i in 0..self.instruments {
            events.push(TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(i),
                    base: CurrencyId(i * 2),
                    quote: CurrencyId(i * 2 + 1),
                },
            });
        }
        for acct in 1..=self.accounts {
            events.push(TradingEvent::ProvisionAccount {
                account: AccountId(acct),
                amount: u64::MAX / 4,
            });
        }
        events
    }

    fn account_limits(&self) -> TradingEvent {
        TradingEvent::SetAccountLimits {
            max_open_orders_per_account: self.max_orders_per_account,
            max_orders_per_second: self.max_orders_per_second,
            max_orders_burst: self.max_orders_burst,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(accounts: u32, instruments: u32) -> StartupConfig {
        StartupConfig {
            accounts,
            instruments,
            max_orders_per_account: 100,
            max_orders_per_second: 1_000,
            max_orders_burst: 100,
        }
    }

    #[test]
    fn genesis_count_matches_config() {
        // 3 instruments + 5 accounts.
        assert_eq!(cfg(5, 3).startup_events().genesis.len(), 8);
    }

    #[test]
    fn genesis_order_is_instruments_then_accounts() {
        let events = cfg(2, 2).startup_events().genesis;
        assert!(matches!(events[0], TradingEvent::AddInstrument { .. }));
        assert!(matches!(events[1], TradingEvent::AddInstrument { .. }));
        assert!(matches!(events[2], TradingEvent::ProvisionAccount { .. }));
        assert!(matches!(events[3], TradingEvent::ProvisionAccount { .. }));
    }

    #[test]
    fn genesis_empty_when_no_accounts_or_instruments() {
        assert!(cfg(0, 0).startup_events().genesis.is_empty());
    }

    /// The limits travel as exactly one event, carrying the node's values,
    /// and never in genesis: genesis is journaled once, while the limits
    /// must be journaled again by every node that becomes primary.
    #[test]
    fn on_primary_carries_the_account_limits() {
        let events = cfg(2, 2).startup_events();
        assert_eq!(
            events.on_primary,
            vec![TradingEvent::SetAccountLimits {
                max_open_orders_per_account: 100,
                max_orders_per_second: 1_000,
                max_orders_burst: 100,
            }]
        );
        assert!(
            !events
                .genesis
                .iter()
                .any(|e| matches!(e, TradingEvent::SetAccountLimits { .. }))
        );
    }

    #[test]
    fn sizing_follows_the_seed_counts() {
        assert_eq!(
            cfg(5, 3).sizing(),
            ExchangeSizing {
                accounts: 5,
                instruments: 3
            }
        );
    }
}
