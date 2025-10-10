# DEX–CEX Market Analysis Challenge

This challenge consists of two parts. The objective of the first part of the challenge is to explore how a liquidity position on a decentralized exchange can be hedged on a centralized one, focusing on structure, balance, and market dynamics. The objective of the second part of the challenge is to examine how a stablecoin’s price deviates from its peg across decentralized and centralized venues over a set timeframe.

## Task 1 — Hedged LP on Uniswap

### Input

You open an LP position in an Uniswap V2 ETH/USDT pool, deposited as 50/50 (by USD value).
1. Calculate the amount of ETH to short on a CEX perpetual to make the LP position locally delta-neutral at entry.
2. What other costs should be accounted for?
3. How would your result change if, instead of V2, liquidity was provided in a Uniswap V3 pool within a ±10% range around the current price?

### Output

A short memo explaining:

- Entry hedge (how you computed the short size).
- Additional costs interplay at a high level.
- V3 (±10%) extension: key trade-offs and what changes in the hedge logic.

*Note: The exercise is theoretical in nature – you won’t need to execute any trades or deposit real funds, but rather demonstrate clear understanding and sound reasoning in your analysis.*

## Task 2 — USDC Peg Deviation: DEX vs CEX (Hourly)

### Input

- **Timeframe**: 2025-07-01 - 2025-09-30 (UTC).
- **DEX**: Uniswap v3 USDC/USDT 0.01% pool at 0x3416cf6c708da44db2624d63ea0aaef7113527c6 (Ethereum mainnet).
- **CEX**: Bybit USDC/USDT spot pair
- **Band**: Expected fair price = 1.0000 USDT per USDC.

**Task**:

1. Build a comparison table by hour (UTC) with total USDC volume traded outside the ±0.1% band on each venue.
2. For each hour where both venues have outside-band volume > 0, also report min and max executed prices (per venue) for that hour.
3. Use any free data sources (state which ones you used).

### Output

To submit your results, please create a pull request to this repository. The PR should include:

1. A CSV-file with columns (one row per hour):

     - time
     - uniswap_volume
     - bybit_volume
     - uniswap_min_price
     - uniswap_max_price
     - bybit_min_price
     - bybit_max_price

When a venue has no outside-band trades that hour, set its outside volumes to 0; min/max may be left blank/NA or based on your stated convention.

2. A Jupyter notebook (or equivalent) showing calculations and which data sources were used.

_Please also email your pull request link along with your resume to challenge-submission@blockshop.org._
