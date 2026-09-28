//! The exchange's client roles.
//!
//! The runtime owns `operator` and `replication`; every other role in
//! `authorized_keys` is the application's. The exchange separates duties
//! across three of its own, so a trading key cannot move funds and a
//! custody key cannot trade.

use melin_app::auth::Role;

/// A client role of the exchange's own, named in `authorized_keys` by its
/// token.
///
/// The request decoder receives one inside a
/// [`ClientRole`](melin_app::auth::ClientRole) with every request and
/// decides what it may do. The tokens are the ones key files have always
/// used, so existing files load unchanged.
///
/// A fieldless enum: the runtime carries a role as its index in
/// [`Role::ROLES`] and hands it back typed, and the decoder matches on it
/// exhaustively, so a role added here is a compile error at every access
/// check until someone decides what it may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeRole {
    /// Order submission, cancellation and amendment, and queries on the
    /// connection's own state.
    Trader,
    /// Fund management: deposits and withdrawals.
    Custodian,
    /// Connection-level messages and the event feed only.
    ReadOnly,
}

impl Role for ExchangeRole {
    const ROLES: &'static [(&'static str, Self)] = &[
        ("trader", ExchangeRole::Trader),
        ("custodian", ExchangeRole::Custodian),
        ("readonly", ExchangeRole::ReadOnly),
    ];
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_app::auth::validate_roles;

    /// The node validates the table when it loads a keys file; this fails
    /// the test suite instead of the node's startup. That each token still
    /// reaches the decoder as its role is pinned in `request_decoder`.
    #[test]
    fn the_role_table_is_valid() {
        validate_roles::<ExchangeRole>().unwrap();
    }
}
