//! The exchange's access model: its client roles, and which of them may
//! send each category of request.
//!
//! The node runtime owns two roles, `operator` and `replication`; every
//! other role in `authorized_keys` is the exchange's own. Duties are
//! separated: each category of request is open to exactly one role (the
//! connection-level messages excepted), so a trading key cannot move funds
//! and a custody key cannot trade.
//!
//! Both halves are exhaustive, with no wildcard anywhere: a new role or a
//! new category fails to compile in [`RequestCategory::admits`], and a new
//! request fails to compile in [`Request::category`](crate::message::Request::category),
//! until someone decides who may send it. Nothing is granted by default.
//!
//! A key is not tied to accounts: a `trader` key may trade, cancel and
//! query for any account.

use melin_app::auth::{ClientRole, Role};

/// A client role of the exchange's own, named in `authorized_keys` by its
/// token.
///
/// The node hands the request decoder one inside a [`ClientRole`] with
/// every request. The tokens are the ones key files have always used, so
/// existing files load unchanged.
///
/// A fieldless enum: the node carries a role as its index in
/// [`Role::ROLES`] and hands it back typed, and [`RequestCategory::admits`]
/// matches on it exhaustively.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeRole {
    /// Orders, cancels and amendments, and position and request-sequence
    /// queries.
    Trader,
    /// Fund management: deposits and withdrawals.
    Custodian,
    /// Heartbeats and the event feed only.
    ReadOnly,
}

impl Role for ExchangeRole {
    const ROLES: &'static [(&'static str, Self)] = &[
        ("trader", ExchangeRole::Trader),
        ("custodian", ExchangeRole::Custodian),
        ("readonly", ExchangeRole::ReadOnly),
    ];
}

/// The duty a request belongs to, which decides the roles that may send
/// it. Every request has exactly one (see
/// [`Request::category`](crate::message::Request::category)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestCategory {
    /// Heartbeats and subscription control. Handled by the node, never
    /// applied by the engine.
    Connection,
    /// Orders, cancels and amendments, and position and request-sequence
    /// queries.
    Trading,
    /// Deposits and withdrawals.
    FundManagement,
    /// Instruments, risk limits, circuit breakers, fee schedules,
    /// end-of-day and stats.
    Administration,
}

impl RequestCategory {
    /// Whether a connection with `role` may send a request of this
    /// category.
    ///
    /// One match over every pair, each row naming its roles: a role or a
    /// category added later is a compile error here, never a silent grant.
    #[inline]
    pub fn admits(self, role: ClientRole<ExchangeRole>) -> bool {
        use ClientRole::{App, Operator};
        use ExchangeRole::{Custodian, ReadOnly, Trader};
        use RequestCategory::{Administration, Connection, FundManagement, Trading};

        match (self, role) {
            // Any client role may keep its connection alive and subscribe.
            (Connection, Operator | App(Trader | Custodian | ReadOnly)) => true,
            (Trading, App(Trader)) => true,
            (FundManagement, App(Custodian)) => true,
            (Administration, Operator) => true,

            (Trading, Operator | App(Custodian | ReadOnly))
            | (FundManagement, Operator | App(Trader | ReadOnly))
            | (Administration, App(Trader | Custodian | ReadOnly)) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_app::auth::validate_roles;

    /// The node validates the table when it loads a keys file; this fails
    /// the test suite instead of the node's startup.
    #[test]
    fn the_role_table_is_valid() {
        validate_roles::<ExchangeRole>().unwrap();
    }

    /// The access rule, written out as the operator docs state it.
    #[test]
    fn each_category_admits_exactly_its_roles() {
        const OPERATOR: ClientRole<ExchangeRole> = ClientRole::Operator;
        const TRADER: ClientRole<ExchangeRole> = ClientRole::App(ExchangeRole::Trader);
        const CUSTODIAN: ClientRole<ExchangeRole> = ClientRole::App(ExchangeRole::Custodian);
        const READONLY: ClientRole<ExchangeRole> = ClientRole::App(ExchangeRole::ReadOnly);
        const ALL: [ClientRole<ExchangeRole>; 4] = [OPERATOR, TRADER, CUSTODIAN, READONLY];

        for (category, admitted) in [
            (RequestCategory::Connection, &ALL[..]),
            (RequestCategory::Trading, &[TRADER][..]),
            (RequestCategory::FundManagement, &[CUSTODIAN][..]),
            (RequestCategory::Administration, &[OPERATOR][..]),
        ] {
            for role in ALL {
                assert_eq!(
                    category.admits(role),
                    admitted.contains(&role),
                    "{category:?} for {role:?}"
                );
            }
        }
    }
}
