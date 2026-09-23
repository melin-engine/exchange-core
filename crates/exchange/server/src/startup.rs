//! What a trading node hands the sequencer runtime at startup.
//!
//! Three things, deliberately kept apart because they travel differently:
//!
//! - **Genesis** — the bulk `AddInstrument` / `ProvisionAccount` seed.
//!   Journaled once, by the node that creates the journal as primary;
//!   every other node receives it in the history. It is a development
//!   fixture: the accounts it provisions are funded out of nothing, so a
//!   stock build seeds nothing and only a build with the `synthetic-seed`
//!   feature emits it.
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

use melin_ec_trading::trading_event::{TradingEvent, TradingRequest};
use melin_ec_types::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};
use melin_server_runtime::StartupEvents;

/// The trading node's startup configuration: the node's own flags,
/// flattened into its command line next to the sequencer runtime's.
///
/// `u32` throughout: the counts match the `AccountId` and `Symbol` spaces
/// they size, and the limits are the engine's own field types.
///
/// The field docs are the `--help` text. [`Default`] gives the same values
/// as the flags' defaults, for a node started without a command line (the
/// bench's embedded server).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::Args)]
pub struct StartupConfig {
    /// Number of accounts to reserve memory for on every start, primary
    /// or replica. A build with the `synthetic-seed` feature also
    /// provisions that many funded accounts on a fresh journal, which
    /// costs O(accounts) (~0.5 s for 1M).
    #[arg(long, default_value_t = 100_000)]
    pub accounts: u32,
    /// Number of instruments to reserve memory for on every start. A
    /// build with the `synthetic-seed` feature also registers that many
    /// placeholder instruments on a fresh journal.
    #[arg(long, default_value_t = 100)]
    pub instruments: u32,
    /// Maximum open orders (resting limits + pending stops, across all
    /// instruments) per account (SEC-03). New submissions are rejected
    /// with `ExceedsMaxOpenOrders` once an account hits this cap. `0`
    /// means unlimited. Journaled when this node becomes primary, and in
    /// force on every node from then on.
    #[arg(long, default_value_t = 10_000)]
    pub max_orders_per_account: u32,
    /// Per-account sustained order-submission rate, orders per second
    /// (SEC-04). A token bucket refills at this rate; an account that has
    /// spent its burst is rejected with `ExceedsOrderRate`. `0` disables
    /// the limiter. Journaled like `--max-orders-per-account`. The
    /// default suits algorithmic and retail flow; raise it (and the
    /// burst) for market makers that re-quote faster.
    #[arg(long, default_value_t = 1_000)]
    pub max_orders_per_second: u32,
    /// Per-account burst capacity: the most consecutive orders allowed
    /// after a quiet period (SEC-04). Paired with
    /// `--max-orders-per-second`; `0` disables the limiter. Journaled
    /// like `--max-orders-per-account`.
    #[arg(long, default_value_t = 5_000)]
    pub max_orders_burst: u32,
}

/// The flags' defaults; `default_matches_the_flags` holds the two together.
impl Default for StartupConfig {
    fn default() -> Self {
        Self {
            accounts: 100_000,
            instruments: 100,
            max_orders_per_account: 10_000,
            max_orders_per_second: 1_000,
            max_orders_burst: 5_000,
        }
    }
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
    ///
    /// Genesis is empty unless the crate is built with `synthetic-seed`.
    /// A node that seeds nothing starts with no instrument and no account;
    /// both are created at runtime through the admin client.
    ///
    /// The node journals these itself, so they carry no request sequence
    /// (see [`TradingRequest::internal`]).
    pub fn startup_events(&self) -> StartupEvents<TradingRequest> {
        // The only trace of the values a node brings to the cluster: they
        // take effect once journaled, when the node becomes primary.
        tracing::info!(
            max_orders_per_account = self.max_orders_per_account,
            max_orders_per_second = self.max_orders_per_second,
            max_orders_burst = self.max_orders_burst,
            "per-account order limits to journal on becoming primary (SEC-03 cap, SEC-04 rate)"
        );
        self.startup_events_seeding(cfg!(feature = "synthetic-seed"))
    }

    /// `startup_events` with the build's choice made explicit, so both
    /// outcomes can be tested from one build: the crate's own tests always
    /// run with `synthetic-seed` on (the test targets depend on the
    /// feature), so a `cfg`-gated test of the stock build would never
    /// compile, let alone run.
    fn startup_events_seeding(&self, seed: bool) -> StartupEvents<TradingRequest> {
        StartupEvents {
            genesis: if seed { self.genesis() } else { Vec::new() },
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

    /// The same, always carrying the synthetic seed whatever the crate
    /// was built with. For a node driven by a bench or a test, which owns
    /// its journal and needs funded accounts to trade with.
    pub fn synthetic_startup_events(&self) -> StartupEvents<TradingRequest> {
        StartupEvents {
            genesis: self.synthetic_genesis(),
            on_primary: vec![self.account_limits()],
        }
    }

    /// Genesis under `synthetic-seed`: funded accounts and placeholder
    /// instruments, for development, benches and smoke tests. A stock
    /// build journals no genesis at all: instruments and accounts are
    /// created through the admin client, against a running node.
    fn genesis(&self) -> Vec<TradingRequest> {
        if self.accounts > 0 {
            // Not a production build: these accounts are funded from
            // nowhere, and the journal keeps that forever.
            tracing::warn!(
                accounts = self.accounts,
                instruments = self.instruments,
                "built with the synthetic-seed feature: genesis provisions funded test accounts"
            );
        }
        self.synthetic_genesis()
    }

    /// Instruments first, then accounts: `ProvisionAccount` funds an
    /// account in every currency of every instrument registered so far.
    pub fn synthetic_genesis(&self) -> Vec<TradingRequest> {
        let mut events = Vec::with_capacity(self.instruments as usize + self.accounts as usize);
        for i in 0..self.instruments {
            events.push(TradingRequest::internal(TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(i),
                    base: CurrencyId(i * 2),
                    quote: CurrencyId(i * 2 + 1),
                },
            }));
        }
        for acct in 1..=self.accounts {
            events.push(TradingRequest::internal(TradingEvent::ProvisionAccount {
                account: AccountId(acct),
                amount: u64::MAX / 4,
            }));
        }
        events
    }

    fn account_limits(&self) -> TradingRequest {
        TradingRequest::internal(TradingEvent::SetAccountLimits {
            max_open_orders_per_account: self.max_orders_per_account,
            max_orders_per_second: self.max_orders_per_second,
            max_orders_burst: self.max_orders_burst,
        })
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
    fn synthetic_genesis_count_matches_config() {
        // 3 instruments + 5 accounts.
        assert_eq!(cfg(5, 3).synthetic_startup_events().genesis.len(), 8);
    }

    #[test]
    fn synthetic_genesis_order_is_instruments_then_accounts() {
        let events = cfg(2, 2).synthetic_genesis();
        assert!(matches!(
            events[0].event,
            TradingEvent::AddInstrument { .. }
        ));
        assert!(matches!(
            events[1].event,
            TradingEvent::AddInstrument { .. }
        ));
        assert!(matches!(
            events[2].event,
            TradingEvent::ProvisionAccount { .. }
        ));
        assert!(matches!(
            events[3].event,
            TradingEvent::ProvisionAccount { .. }
        ));
    }

    /// Nothing the node journals itself carries a sequence: it is applied
    /// under key 0, and the engine's idempotency check exempts that key.
    #[test]
    fn startup_events_carry_no_request_sequence() {
        let events = cfg(2, 2).synthetic_startup_events();
        assert!(
            events
                .genesis
                .iter()
                .chain(&events.on_primary)
                .all(|e| e.request_seq == 0)
        );
    }

    #[test]
    fn synthetic_genesis_empty_when_no_accounts_or_instruments() {
        assert!(cfg(0, 0).synthetic_genesis().is_empty());
    }

    /// What the binary journals depends on how it was built, and on
    /// nothing else: the counts stay sizing either way.
    #[test]
    fn seeding_build_journals_the_seed() {
        assert_eq!(cfg(5, 3).startup_events_seeding(true).genesis.len(), 8);
    }

    #[test]
    fn stock_build_journals_no_genesis() {
        let events = cfg(5, 3).startup_events_seeding(false);
        assert!(events.genesis.is_empty());
        assert_eq!(cfg(5, 3).sizing().accounts, 5);
        // The limits are configuration, not a fixture: they stay.
        assert_eq!(events.on_primary.len(), 1);
    }

    /// The crate's tests always build with the feature on, so this is the
    /// one assertion about the real `startup_events` they can make.
    #[test]
    fn startup_events_follow_the_build_feature() {
        assert_eq!(
            cfg(5, 3).startup_events().genesis.len(),
            if cfg!(feature = "synthetic-seed") {
                8
            } else {
                0
            }
        );
    }

    /// The limits travel as exactly one event, carrying the node's values,
    /// and never in genesis: genesis is journaled once, while the limits
    /// must be journaled again by every node that becomes primary.
    #[test]
    fn on_primary_carries_the_account_limits() {
        let events = cfg(2, 2).synthetic_startup_events();
        assert_eq!(
            events.on_primary,
            vec![TradingRequest::internal(TradingEvent::SetAccountLimits {
                max_open_orders_per_account: 100,
                max_orders_per_second: 1_000,
                max_orders_burst: 100,
            })]
        );
        assert!(
            !events
                .genesis
                .iter()
                .any(|e| matches!(e.event, TradingEvent::SetAccountLimits { .. }))
        );
    }

    /// Parses the node's flags alone, as the binary does after its
    /// runtime flags.
    #[derive(clap::Parser)]
    struct Flags {
        #[command(flatten)]
        startup: StartupConfig,
    }

    fn parse(args: &[&str]) -> Result<StartupConfig, clap::Error> {
        use clap::Parser;
        Flags::try_parse_from(std::iter::once("melin-ec-server").chain(args.iter().copied()))
            .map(|f| f.startup)
    }

    /// A node started without flags and the bench's embedded node, which
    /// takes `Default`, must run under the same limits.
    #[test]
    fn default_matches_the_flags() {
        assert_eq!(parse(&[]).unwrap(), StartupConfig::default());
    }

    /// The node now owns these flags; the runtime no longer parses them.
    #[test]
    fn limit_flags_parse_into_the_journaled_event() {
        let startup = parse(&[
            "--max-orders-per-account",
            "2",
            "--max-orders-per-second",
            "30",
            "--max-orders-burst",
            "40",
        ])
        .unwrap();
        assert_eq!(
            startup.synthetic_startup_events().on_primary,
            vec![TradingRequest::internal(TradingEvent::SetAccountLimits {
                max_open_orders_per_account: 2,
                max_orders_per_second: 30,
                max_orders_burst: 40,
            })]
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
