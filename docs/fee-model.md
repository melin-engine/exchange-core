# Maker/Taker Fee Model

Fees are configured per instrument in basis points (bps, where 100 bps = 1%). Both maker and taker fees are signed (`i16`, range -10000 to 10000): positive values are fees charged to the trader, negative values are rebates paid by the exchange. Schedules outside the ±10000 bps range are rejected by the engine and ignored with a warning in the server log — the requested change does not take effect. If no fee schedule is set, both rates default to zero. Fee schedules can be changed at runtime via `SetFeeSchedule`; changes apply to all subsequent fills, including fills on orders placed before the change.

Collected fees are credited to a dedicated **fee collection account** (`AccountId(0)`). This account is never evicted and always exists. Operators can withdraw accumulated fees via the Withdraw command.

## Maker vs Taker

- **Maker**: the resting order already on the book. Adds liquidity.
- **Taker**: the incoming order that crosses the spread. Removes liquidity.

## Fee Currency

Fees are charged in the **received asset**, matching common practice on major venues:

- **Buyer** pays their fee in **base** currency, deducted from the base amount they receive in the fill.
- **Seller** pays their fee in **quote** currency, deducted from the quote proceeds they receive.

Execution reports state both legs' fees in **quote currency** (the fee's economic value at the fill price), regardless of which asset the fee was settled in.

Because each fee is deducted from what the trader *receives* in that fill, a fee can never exceed the trader's receipt: fees are capped at the received amount. A fee that hits this cap indicates a fee-schedule misconfiguration (for example a rate set to 100%) and is flagged in the server log.

## No Reservation Cushion

Order reservations lock **pure notional** only — `price * quantity` in quote for limit buys, quantity in base for sells, the full available quote balance for market buys. No fee cushion is added at placement time: since fees come out of the fill's proceeds rather than the reservation, a reservation is always sufficient to settle its fills, by construction.

## Dynamic Fee Schedule Changes

Because reservations don't depend on the fee schedule, a schedule change simply takes effect on all subsequent fills — including fills of orders that were already resting when the schedule changed. There is no reservation recalculation, no shortfall, and no restriction on when schedules may be changed.

Rebates (negative fees) are funded from the fee collection account. If rebates exceed the account's accumulated fees, the account's balance for that currency goes negative on a signed ledger; subsequent fee revenue pays the deficit down first. Operators can monitor the fee account's signed balance to know when it needs funding.
