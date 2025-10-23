# ETH/BTC Market Data Analysis: Suspicious Patterns

This report presents a focused investigation into potential irregularities and manipulative behaviors in ETH/BTC market activity using provided trade and orderbook samples.

**Data Overview**
- Trades: 845 rows; timeframe: 2025-09-01 00:02:57+00:00 to 2025-09-03 23:51:34+00:00 (UTC)
- Orderbooks: 188 snapshots; timeframe: 2025-09-01 00:13:09.511132300+00:00 to 2025-09-03 20:50:36.802878100+00:00 (UTC)
- Aggregation: 1-minute bars for price, volume, and returns

**Key Findings**
- Volume spikes: Multiple 1-minute intervals exceed 3σ of rolling volume, indicating abnormal liquidity bursts possibly linked to coordinated activity.
- Return outliers: Statistically significant jumps/drops suggest potential price impact actions beyond typical volatility.
- Micro-trade bursts: Repetitive small trades at identical price within seconds (≥4 prints) likely reflect algorithmic pinging or quote-stuffing-like behavior.
- Wash-trading heuristic: No strong back-to-back opposite-side pairs detected under strict criteria; however, burst patterns warrant attention.
- Pump-and-dump signals: 1 sequences with strong run-up followed by sharp reversal under elevated volume were flagged.

**Orderbook Irregularities**
- Spread behavior: Median spread is 0.000359; outliers suggest transient liquidity withdrawal or aggressive step-function updates.
- Top-5 imbalance: Mean imbalance -0.590. Extreme imbalances may precede directional moves; correlation with future returns over 5 minutes: 0.188.
- Walls near best levels: 161 snapshots show 10× size walls within top-5 levels, indicative of potential spoof-like signaling.

**Charts**
- Price with anomalies: ![](./figures/price_with_anomalies.png)
- 1-min volume: ![](./figures/volume_spikes.png)
- Returns distribution: ![](./figures/returns_hist.png)
- Orderbook spread: ![](./figures/orderbook_spread.png)
- Orderbook imbalance: ![](./figures/orderbook_imbalance.png)

**Methodology and Limitations**
- The analysis uses rolling z-scores (30-minute window) for volume and returns to flag anomalies.
- Wash-trading detection relies on heuristic matching; exchange-level counterparty data is not available, so findings are indicative rather than definitive.
- Pump/dump signals require windowed trend and reversal under elevated volume; thresholds are conservative to minimize false positives.
- Orderbook parsing focuses on top-5 levels; deeper-book dynamics and cancellations are not directly observable from snapshots.
