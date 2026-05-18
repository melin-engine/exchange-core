//! Application construction + bulk-seed seam.
//!
//! The server runtime needs to construct application instances in
//! several contexts: a fresh primary at startup, a replica preparing
//! to receive a snapshot transfer, a replica catching up from genesis
//! via journal replay. All three want a clean `A`, but only the
//! "fresh primary at startup" path also publishes bulk-seed events
//! that the matching stage applies and the journal stage persists —
//! so replicas converge on the same state via standard journal
//! replay rather than via parallel out-of-band construction.
//!
//! Trading apps seed accounts and instruments. A different
//! application might seed nothing, or seed a currency table, or
//! initialize a ledger schema. This trait is the seam — the runtime
//! drives the construction and publishes whatever the factory hands
//! it, never touching application-shaped event variants directly.
//!
//! Operator-controlled policy (rate limits, caps, ...) is kept
//! separate from journaled state. [`AppFactory::apply_operator_policy`]
//! reapplies these knobs after snapshot restore so primary and
//! replica converge on matching values even though the journal
//! carries no record of them.

use crate::Application;

/// Build and configure application instances on behalf of the
/// runtime.
///
/// Implementors are typically construction-config holders (sizing
/// hints, operator knobs) rather than zero-sized — they capture the
/// CLI-level values needed to produce `A` instances. Stored as
/// `Arc<dyn AppFactory<App = ConcreteA>>` on the runtime config so
/// replication paths can construct fresh apps after their snapshot
/// transfers or catch-up scans.
pub trait AppFactory: Send + Sync {
    /// The concrete application this factory produces.
    type App: Application;

    /// Construct an empty application. Used by replication paths
    /// that need a clean state before receiving a snapshot or
    /// replaying the journal from genesis. The returned app has no
    /// operator policy applied — the caller pairs this with
    /// [`Self::apply_operator_policy`] when the policy matters
    /// (post-snapshot or pre-live-replay).
    fn empty(&self) -> Self::App;

    /// Construct an empty application pre-sized for the configured
    /// bulk-seed workload, with operator policy applied. The default
    /// impl is `self.empty()` + `self.apply_operator_policy()`;
    /// override only when the application can use the seed-size hint
    /// to pre-allocate internal collections (e.g. trading pre-sizes
    /// the account-balance map so the seed phase doesn't hit
    /// per-rehash hundred-millisecond stalls).
    fn empty_for_seed(&self) -> Self::App {
        let mut app = self.empty();
        self.apply_operator_policy(&mut app);
        app
    }

    /// Reapply operator-controlled policy (rate limits, caps, ...)
    /// to an existing app. The policy is NOT journaled — primary
    /// and replica must apply matching values independently — so
    /// this is called after every snapshot restore (which
    /// reconstructs state but not policy) and after every replica
    /// reconnect that reuses an existing pipeline. Default impl is
    /// a no-op for applications that have no operator policy.
    fn apply_operator_policy(&self, _app: &mut Self::App) {}

    /// Yield the bulk-seed events the runtime should journal at
    /// startup. Called once on a fresh primary (empty journal, no
    /// snapshot); replicas receive the same events through standard
    /// journal replay and never call this themselves. Default impl
    /// returns an empty `Vec` for applications that don't
    /// bulk-seed.
    ///
    /// Returning a `Vec` rather than streaming an iterator is a
    /// deliberate trade-off: seed sets are bounded by operator
    /// config (counts of accounts / instruments / similar) and run
    /// once at startup, so the allocation is not on any hot path
    /// and the simpler signature keeps the trait object-safe.
    fn seed_events(&self) -> Vec<<Self::App as Application>::Event> {
        Vec::new()
    }
}
