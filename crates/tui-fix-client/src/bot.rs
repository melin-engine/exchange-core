//! Pure helpers for the synthetic order-flow bot.
//!
//! Isolated from `run_bot_session` so the rate curve, RNG, order
//! parameter sampling, and FIX message construction can be unit tested
//! without opening a real gateway connection.

use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;

// --- Rate model ---

/// Sine wave period. 30 s makes the cycle visible in a short demo.
pub(crate) const PERIOD_SECS: f64 = 30.0;
/// Mean submission rate (orders/sec).
pub(crate) const RATE_MID: f64 = 40.0;
/// Peak-to-mean amplitude (orders/sec). Peak = MID + AMP = 75 ord/s.
pub(crate) const RATE_AMP: f64 = 35.0;

/// Target submission rate (orders/sec) `t` seconds after bot start.
///
/// Traces `MID + AMP · sin(2π · t / PERIOD)`, floored at 1.0 so that
/// the trough (t = 3·PERIOD/4, raw value 5.0) stays positive and the
/// bot never stalls. The floor never binds with the default constants
/// but guards against future tuning that pushes `AMP` above `MID`.
pub(crate) fn bot_rate(t: f64) -> f64 {
    let raw = RATE_MID + RATE_AMP * (std::f64::consts::TAU * t / PERIOD_SECS).sin();
    raw.max(1.0)
}

// --- RNG ---

/// xorshift64: ~1 ns/sample, single-u64 state, non-cryptographic —
/// adequate for bot order-parameter jitter.
pub(crate) fn xs64(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

// --- Order parameter sampling ---

/// Account pool: 2..=32. Fixed-size array (not Vec) since the pool is
/// known at compile time — avoids a heap allocation per thread start.
/// Accounts start at 2 to leave account 1 for the interactive trader,
/// so the user's balances/active-orders panels aren't polluted.
pub(crate) const BOT_ACCOUNTS: [u32; 31] = {
    let mut a = [0u32; 31];
    let mut i = 0;
    while i < 31 {
        a[i] = (i as u32) + 2;
        i += 1;
    }
    a
};
/// Matches the FIX symbols configured in the OE gateway.
pub(crate) const BOT_SYMBOLS: [&str; 2] = ["BTC/USD", "ETH/USD"];
/// FIX decimal mid-price; the gateway maps this to engine ticks via
/// tick_size_inverse (100 in the default config → 10,000 ticks).
pub(crate) const MID_PRICE: f64 = 100.0;
/// One FIX price tick is 1/tick_size_inverse = 0.01 at the default
/// config. Offsets are drawn uniformly in [1, 50] ticks.
pub(crate) const MAX_OFFSET_TICKS: u64 = 50;
/// Max order quantity in lots. Quantities are drawn uniformly in [1, 50].
pub(crate) const MAX_QTY: u64 = 50;

/// Parameters for a single synthetic order.
pub(crate) struct BotOrder {
    pub account_id: u32,
    pub symbol: &'static str,
    /// FIX 4.4: "1" = BUY, "2" = SELL.
    pub side_code: &'static str,
    pub price: f64,
    pub qty: u64,
}

/// Draw the next order's parameters from the RNG state.
///
/// Buys sit below mid, sells above — the bot never self-crosses. Prices
/// are aligned to the 0.01 tick grid (one decimal place of precision,
/// rounded via the `.2` formatter at send time).
pub(crate) fn next_bot_order(rng: &mut u64) -> BotOrder {
    let account_id = BOT_ACCOUNTS[(xs64(rng) as usize) % BOT_ACCOUNTS.len()];
    let symbol = BOT_SYMBOLS[(xs64(rng) as usize) % BOT_SYMBOLS.len()];
    let side_code = if xs64(rng) & 1 == 0 { "1" } else { "2" };
    let offset_ticks = (xs64(rng) % MAX_OFFSET_TICKS) + 1;
    let price = if side_code == "1" {
        MID_PRICE - (offset_ticks as f64) / 100.0
    } else {
        MID_PRICE + (offset_ticks as f64) / 100.0
    };
    let qty = (xs64(rng) % MAX_QTY) + 1;
    BotOrder {
        account_id,
        symbol,
        side_code,
        price,
        qty,
    }
}

// --- FIX construction ---

/// Build a FIX NewOrderSingle from a bot order and ClOrdID.
pub(crate) fn build_bot_nos(clord: &str, order: &BotOrder) -> FixMessageBuilder {
    FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
        .str_tag(tags::CL_ORD_ID, clord)
        .str_tag(tags::SYMBOL, order.symbol)
        .str_tag(tags::SIDE, order.side_code)
        .str_tag(tags::ORD_TYPE, "2") // Limit
        .str_tag(tags::PRICE, &format!("{:.2}", order.price))
        .str_tag(tags::ORDER_QTY, &format!("{}", order.qty))
        .str_tag(tags::TIME_IN_FORCE, "1") // GTC
        .str_tag(tags::ACCOUNT, &format!("{}", order.account_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_gateway_core::fix::parse::FixMessage;

    // --- bot_rate ---

    #[test]
    fn bot_rate_at_zero_equals_mid() {
        assert!((bot_rate(0.0) - RATE_MID).abs() < 1e-9);
    }

    #[test]
    fn bot_rate_at_quarter_period_is_peak() {
        let peak = bot_rate(PERIOD_SECS / 4.0);
        assert!((peak - (RATE_MID + RATE_AMP)).abs() < 1e-6);
    }

    #[test]
    fn bot_rate_at_three_quarter_period_is_trough() {
        let trough = bot_rate(PERIOD_SECS * 3.0 / 4.0);
        // With default constants the raw trough is RATE_MID - RATE_AMP = 5.
        assert!((trough - (RATE_MID - RATE_AMP)).abs() < 1e-6);
    }

    #[test]
    fn bot_rate_is_floored_at_one() {
        // The floor only engages with non-default constants, but must
        // keep the guarantee so `1.0 / rate` is always finite.
        // Synthesize a guaranteed-negative raw value by evaluating with a
        // hypothetical parameterization: we can't tweak constants from
        // here, so just assert the property across the whole cycle.
        for i in 0..=300 {
            let t = i as f64 * 0.1;
            assert!(bot_rate(t) >= 1.0, "rate at t={t} was {}", bot_rate(t));
        }
    }

    #[test]
    fn bot_rate_is_periodic() {
        // Periodicity is the basis of the "visible in a short demo"
        // claim. A drift here would indicate a unit-conversion bug.
        let t = 3.7;
        assert!((bot_rate(t) - bot_rate(t + PERIOD_SECS)).abs() < 1e-9);
    }

    // --- xs64 ---

    #[test]
    fn xs64_nonzero_from_nonzero_seed() {
        let mut s = 0xC0FF_EE00_DEAD_BEEF;
        for _ in 0..1000 {
            assert!(xs64(&mut s) != 0);
        }
    }

    #[test]
    fn xs64_is_deterministic() {
        let mut a = 42;
        let mut b = 42;
        for _ in 0..100 {
            assert_eq!(xs64(&mut a), xs64(&mut b));
        }
    }

    // --- next_bot_order ---

    #[test]
    fn next_bot_order_stays_in_account_pool() {
        let mut rng = 0xC0FF_EE00_DEAD_BEEF;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng);
            assert!(
                (2..=32).contains(&o.account_id),
                "account {} out of range",
                o.account_id
            );
        }
    }

    #[test]
    fn next_bot_order_uses_configured_symbols() {
        let mut rng = 1;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng);
            assert!(o.symbol == "BTC/USD" || o.symbol == "ETH/USD");
        }
    }

    #[test]
    fn next_bot_order_side_is_buy_or_sell() {
        let mut rng = 1;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng);
            assert!(o.side_code == "1" || o.side_code == "2");
        }
    }

    #[test]
    fn next_bot_order_price_does_not_cross_mid() {
        // Buys strictly below mid, sells strictly above — guarantees the
        // bot doesn't self-cross.
        let mut rng = 1;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng);
            match o.side_code {
                "1" => assert!(o.price < MID_PRICE, "buy at {}", o.price),
                "2" => assert!(o.price > MID_PRICE, "sell at {}", o.price),
                other => panic!("unexpected side {other}"),
            }
            // Offset bounded to [1, 50] ticks = [0.01, 0.50].
            let abs_offset = (o.price - MID_PRICE).abs();
            assert!(
                (0.01 - 1e-9..=0.50 + 1e-9).contains(&abs_offset),
                "offset {} out of range",
                abs_offset
            );
        }
    }

    #[test]
    fn next_bot_order_qty_in_range() {
        let mut rng = 1;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng);
            assert!((1..=MAX_QTY).contains(&o.qty), "qty {} out of range", o.qty);
        }
    }

    // --- build_bot_nos ---

    #[test]
    fn build_bot_nos_produces_parseable_fix_with_expected_fields() {
        let order = BotOrder {
            account_id: 7,
            symbol: "BTC/USD",
            side_code: "1",
            price: 99.37,
            qty: 12,
        };
        let raw = build_bot_nos("BOT42", &order).build("BOT", "MELIN-OE", 1);
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_NEW_ORDER_SINGLE);
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("BOT42"));
        assert_eq!(msg.get_str(tags::SYMBOL), Some("BTC/USD"));
        assert_eq!(msg.get_str(tags::SIDE), Some("1"));
        assert_eq!(msg.get_str(tags::ORD_TYPE), Some("2"));
        assert_eq!(msg.get_str(tags::PRICE), Some("99.37"));
        assert_eq!(msg.get_str(tags::ORDER_QTY), Some("12"));
        assert_eq!(msg.get_str(tags::TIME_IN_FORCE), Some("1"));
        assert_eq!(msg.get_str(tags::ACCOUNT), Some("7"));
    }

    #[test]
    fn build_bot_nos_rounds_price_to_two_decimals() {
        // Formatter rounds to 2 decimals. Values drawn by next_bot_order
        // already land on the 0.01 grid, but this guards against future
        // drift in the sampling logic producing non-grid prices.
        let order = BotOrder {
            account_id: 2,
            symbol: "ETH/USD",
            side_code: "2",
            price: 100.005_6,
            qty: 1,
        };
        let raw = build_bot_nos("X", &order).build("BOT", "MELIN-OE", 1);
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.get_str(tags::PRICE), Some("100.01"));
    }
}
