# Hedged LP on Uniswap: quick memo

Goal: show how to locally neutralize the delta of a DEX LP position using a short on a CEX, what costs matter, and how the logic changes for Uniswap v3 with a narrow range.

## Inputs
- Pool: Uniswap V2 ETH/USDT.
- Deposit: 50/50 by USD (half ETH, half USDT) at the current price `P` (USDT per 1 ETH).

## Entry hedge (V2, 50/50)
Key fact for Uniswap V2: the value of an LP share is `V(P) = 2 * sqrt(k * P)`, where `k = x * y` is the pool invariant and `P = y/x` is the price (USDT per ETH). The derivative `dV/dP = sqrt(k)/sqrt(P) = x` — i.e., the local delta of the LP in ETH units equals the current ETH amount in your share.

- With a “50/50 by USD” deposit, your share initially holds `X = N/(2*P)` ETH and `Y = N/2` USDT, where `N` is the total USD amount you deposit.
- To make the position locally delta-neutral near the current price, short exactly the ETH held by your share: `short_size_eth = X = N/(2*P)`.
- Intuition: you offset the LP’s price sensitivity (which equals “ETH content of the share”) with a short on the CEX perpetual.

Example: You deposit `N = 100,000 USDT` at `P = 4,000 USDT/ETH`. Then `X = 100,000 / (2 * 4,000) = 12.5 ETH`. Open a `12.5 ETH` short on the CEX perpetual.

## Costs to account for
- Fees:
  - DEX: pool fee (Uniswap v2 depends on pool; for v3 you select 0.01%/0.05%/0.3%).
  - CEX perpetual: taker/maker fees.
- Funding and borrow:
  - Perp funding (periodic payments between longs/shorts) — can be positive or negative.
  - Cost of capital/borrow (if margin is borrowed, or opportunity cost of capital).
- Slippage/impact and rebalancing:
  - Executing the deposit and opening the short.
  - Periodic short adjustments if you maintain “local” neutrality as price moves.
- Divergence loss (impermanent loss):
  - Along the curve, the LP is “long gamma and short vega”; fee revenue should compensate IL over time — but it’s not guaranteed.
- Operational risks:
  - Quote divergence/latency between DEX and CEX, potential liquidity gaps.
  - Stablecoin depeg risk (USDT/USDC) and market anomalies.

## Extension to Uniswap v3: ±10% band around the current price
In Uniswap v3 liquidity is concentrated in a range `[P_lower, P_upper] = [0.9*P0, 1.1*P0]` (for a symmetric choice). Share behavior:
- While price stays inside the range, the share has “virtual reserves” `x_v(P), y_v(P)` from liquidity `L` and `sqrtPrice`. Local delta again equals current ETH in the share: `delta_eth(P) = x_v(P)`.
- Near the center (`P0`) the share is close to 50/50; at the edges it becomes one-sided (mostly USDT as price rises; mostly ETH as price falls).

Implications for the hedge:
- Initial short at entry (near `P0`) mirrors V2: `short_size_eth ≈ x_v(P0)` — effectively the ETH held by your share at entry.
- But gamma is higher than V2: delta changes faster within a narrow band as price moves.
- If price leaves the band, the share becomes “mono-asset”, and local delta tends to 0 (outside the band it stops rebalancing).
- Important trade-off:
  - A narrow range increases fee APR per unit of capital, but requires more frequent dynamic short rebalancing to keep neutrality.
  - Short becomes “dynamic”: `short_size_eth(P) ≈ x_v(P)` updated on meaningful price shifts.
  - At band edges you decide: either relocate liquidity (move the range) or accept one-sidedness and revise the hedge.

High-level v3 formula (intuition):
- With liquidity `L` and current root price `S = sqrt(P)`, virtual reserves are `x_v = L / S`, `y_v = L * S`. Then `delta_eth = x_v`. At the center `S ≈ sqrt(P0)` and `x_v` approximates the “ETH deposit under 50/50”.

## Summary
- V2 (50/50): LP’s local delta equals the ETH amount in your share. At entry, short is `N/(2*P)` ETH.
- Costs: DEX/CEX fees, perp funding, rebalancing, IL, operational risks.
- V3 with ±10%: entry logic is the same, but delta changes faster; short is dynamic. Outside the band delta tends to 0; either reposition the range or adjust the strategy.