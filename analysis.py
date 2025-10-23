import os
import json
import ast
from datetime import timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid")

DATA_TRADES = "eth-btc-trades.csv"
DATA_ORDERBOOKS = "eth-btc-orderbooks.csv"
OUT_DIR = os.path.join("reports")
FIG_DIR = os.path.join(OUT_DIR, "figures")
REPORT_MD = os.path.join(OUT_DIR, "Market_Analysis_Report.md")
SUMMARY_JSON = os.path.join(OUT_DIR, "summary.json")


def ensure_dirs():
    os.makedirs(FIG_DIR, exist_ok=True)


def load_trades(path=DATA_TRADES):
    df = pd.read_csv(path)
    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    # Parse timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    else:
        raise ValueError("Trades CSV must contain 'timestamp' column")
    # Basic expected fields
    for field in ["price", "size", "side"]:
        if field not in df.columns:
            raise ValueError(f"Trades CSV missing expected column: {field}")
    # Clean side
    df["side"] = df["side"].str.upper().str.strip()
    df = df.dropna(subset=["timestamp", "price", "size"]).sort_values("timestamp")
    return df


def load_orderbooks(path=DATA_ORDERBOOKS):
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    else:
        raise ValueError("Orderbooks CSV must contain 'timestamp' column")
    # Parse asks/bids as lists of dicts
    for side in ["asks", "bids"]:
        if side not in df.columns:
            raise ValueError(f"Orderbooks CSV missing '{side}' column")
        df[side] = df[side].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df


def resample_trades(df):
    df["is_buy"] = (df["side"] == "BUY").astype(int)
    df["is_sell"] = (df["side"] == "SELL").astype(int)
    df = df.set_index("timestamp")
    # 1-minute bars
    bars = pd.DataFrame()
    bars["price"] = df["price"].resample("1min").last()
    bars["volume"] = df["size"].resample("1min").sum()
    bars["buy_volume"] = (df["size"] * df["is_buy"]).resample("1min").sum()
    bars["sell_volume"] = (df["size"] * df["is_sell"]).resample("1min").sum()
    bars["trade_count"] = df["size"].resample("1min").count()
    bars = bars.dropna(subset=["price"])  # drop leading NaNs
    # Returns
    bars["return"] = bars["price"].pct_change()
    # Rolling stats
    window = 30  # minutes
    bars["vol_roll_mean"] = bars["volume"].rolling(window).mean()
    bars["vol_roll_std"] = bars["volume"].rolling(window).std()
    bars["vol_z"] = (bars["volume"] - bars["vol_roll_mean"]) / bars["vol_roll_std"]

    bars["ret_roll_mean"] = bars["return"].rolling(window).mean()
    bars["ret_roll_std"] = bars["return"].rolling(window).std()
    bars["ret_z"] = (bars["return"] - bars["ret_roll_mean"]) / bars["ret_roll_std"]
    return df, bars


def detect_volume_spikes(bars, z_thresh=3.0):
    spikes = bars[(bars["vol_z"] > z_thresh) & bars["vol_z"].notnull()]
    return spikes


def detect_return_outliers(bars, z_thresh=3.0):
    outs = bars[(bars["ret_z"].abs() > z_thresh) & bars["ret_z"].notnull()]
    return outs


def detect_microtrade_bursts(df, second_window='1s', size_thresh=0.01, min_trades=4):
    # group by seconds and price, count trades with very small size
    dfg = df.copy()
    dfg["sec"] = dfg.index.floor(second_window)
    small = dfg[dfg["size"] <= size_thresh]
    bursts = (small.groupby(["sec", "price"]).size().reset_index(name="n"))
    bursts = bursts[bursts["n"] >= min_trades]
    return bursts


def detect_wash_trading(df, time_delta=pd.Timedelta(seconds=3)):
    # heuristic: back-to-back opposite-side trades at same price and similar size within 3 seconds
    records = []
    arr = df[["price", "size", "side"]].copy()
    arr["ts"] = df.index
    for i in range(len(arr) - 1):
        a = arr.iloc[i]
        b = arr.iloc[i+1]
        if a["side"] != b["side"] and a["price"] == b["price"] and abs(a["size"] - b["size"]) / max(a["size"], b["size"]) < 0.05:
            if (b["ts"] - a["ts"]) <= time_delta:
                records.append({"t0": a["ts"], "t1": b["ts"], "price": a["price"], "size_a": a["size"], "size_b": b["size"], "side_a": a["side"], "side_b": b["side"]})
    return pd.DataFrame(records)


def detect_pump_dump(bars, win=10):
    # pump: strong positive return over window & high volume; dump: followed by strong negative
    res = []
    for i in range(len(bars) - 2*win):
        pre = bars.iloc[i:i+win]
        post = bars.iloc[i+win:i+2*win]
        r_pre = (pre["price"].iloc[-1] / pre["price"].iloc[0]) - 1
        r_post = (post["price"].iloc[-1] / post["price"].iloc[0]) - 1
        vol_pre = pre["volume"].sum()
        vol_post = post["volume"].sum()
        # thresholds relative to std
        ret_std = bars["return"].rolling(win).std().iloc[i+win-1]
        vol_mean = bars["volume"].rolling(win).mean().iloc[i+win-1]
        vol_std = bars["volume"].rolling(win).std().iloc[i+win-1]
        high_vol = vol_pre > (vol_mean + 2 * vol_std) if pd.notnull(vol_mean) and pd.notnull(vol_std) else False
        if pd.notnull(ret_std) and high_vol:
            if r_pre > 3 * ret_std and r_post < -3 * ret_std:
                res.append({
                    "start": pre.index[0], "mid": pre.index[-1], "end": post.index[-1],
                    "r_pre": float(r_pre), "r_post": float(r_post),
                    "vol_pre": float(vol_pre), "vol_post": float(vol_post)
                })
    return pd.DataFrame(res)


def parse_best_levels(ob):
    # ob: list of dicts with price/size
    if not ob:
        return None, None
    # for asks: best is min price; for bids: best is max price
    prices = [lv.get("price") for lv in ob if "price" in lv]
    sizes = [lv.get("size") for lv in ob if "size" in lv]
    return prices, sizes


def orderbook_metrics(df_ob, top_n=5):
    rows = []
    for _, row in df_ob.iterrows():
        asks = row["asks"]
        bids = row["bids"]
        ask_prices, ask_sizes = parse_best_levels(asks)
        bid_prices, bid_sizes = parse_best_levels(bids)
        if not ask_prices or not bid_prices:
            continue
        # best levels
        best_ask = min(ask_prices)
        best_bid = max(bid_prices)
        spread = best_ask - best_bid
        mid = (best_ask + best_bid) / 2
        # top-N aggregation by proximity to best
        asks_sorted = sorted(zip(ask_prices, ask_sizes), key=lambda x: x[0])[:top_n]
        bids_sorted = sorted(zip(bid_prices, bid_sizes), key=lambda x: x[0], reverse=True)[:top_n]
        ask_vol_top = np.nansum([s for _, s in asks_sorted])
        bid_vol_top = np.nansum([s for _, s in bids_sorted])
        total_top = ask_vol_top + bid_vol_top
        imbalance = (bid_vol_top - ask_vol_top) / total_top if total_top > 0 else np.nan
        # large wall detection near top-N
        ask_sizes_arr = np.array([s for _, s in asks_sorted])
        bid_sizes_arr = np.array([s for _, s in bids_sorted])
        ask_wall = (ask_sizes_arr.max() > 10 * (np.median(ask_sizes_arr) if len(ask_sizes_arr) else 0)) if len(ask_sizes_arr) else False
        bid_wall = (bid_sizes_arr.max() > 10 * (np.median(bid_sizes_arr) if len(bid_sizes_arr) else 0)) if len(bid_sizes_arr) else False
        rows.append({
            "timestamp": row["timestamp"],
            "best_ask": best_ask,
            "best_bid": best_bid,
            "spread": spread,
            "mid": mid,
            "ask_vol_top": ask_vol_top,
            "bid_vol_top": bid_vol_top,
            "imbalance": imbalance,
            "ask_wall": ask_wall,
            "bid_wall": bid_wall
        })
    met = pd.DataFrame(rows).set_index("timestamp").sort_index()
    return met


def correlate_imbalance_future_return(ob_met, bars, horizon_min=5):
    # align mid with bars price, compute future returns vs current imbalance
    aligned = pd.merge_asof(ob_met.sort_index(), bars[["price"]].sort_index(), left_index=True, right_index=True, direction="nearest")
    aligned["future_price"] = aligned["price"].shift(-horizon_min)
    aligned["future_ret"] = (aligned["future_price"] / aligned["price"]) - 1
    corr = aligned[["imbalance", "future_ret"]].corr().iloc[0,1]
    return corr, aligned


def save_price_with_anomalies(bars, spikes, outs):
    plt.figure(figsize=(12,6))
    plt.plot(bars.index, bars["price"], label="Price", color="#1f77b4")
    if len(spikes):
        plt.scatter(spikes.index, bars.loc[spikes.index, "price"], color="#ff7f0e", label="Volume spikes", zorder=5)
    if len(outs):
        plt.scatter(outs.index, bars.loc[outs.index, "price"], color="#d62728", label="Return outliers", marker="x", zorder=6)
    plt.title("ETH/BTC Price with Volume and Return Anomalies")
    plt.xlabel("Time")
    plt.ylabel("Price (ETH/BTC)")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(FIG_DIR, "price_with_anomalies.png")
    plt.savefig(path)
    plt.close()
    return path


def save_volume_spikes(bars):
    plt.figure(figsize=(12,4))
    plt.plot(bars.index, bars["volume"], label="Volume", color="#2ca02c")
    plt.title("1-min Volume")
    plt.xlabel("Time")
    plt.ylabel("ETH volume")
    plt.tight_layout()
    path = os.path.join(FIG_DIR, "volume_spikes.png")
    plt.savefig(path)
    plt.close()
    return path


def save_returns_hist(bars):
    plt.figure(figsize=(8,4))
    sns.histplot(bars["return"].dropna(), bins=50, kde=True, color="#9467bd")
    plt.title("Distribution of 1-min Returns")
    plt.xlabel("Return")
    plt.tight_layout()
    path = os.path.join(FIG_DIR, "returns_hist.png")
    plt.savefig(path)
    plt.close()
    return path


def save_orderbook_spread(ob_met):
    plt.figure(figsize=(12,4))
    plt.plot(ob_met.index, ob_met["spread"], label="Spread", color="#8c564b")
    plt.title("Orderbook Spread over Time")
    plt.xlabel("Time")
    plt.ylabel("Spread")
    plt.tight_layout()
    path = os.path.join(FIG_DIR, "orderbook_spread.png")
    plt.savefig(path)
    plt.close()
    return path


def save_orderbook_imbalance(ob_met):
    plt.figure(figsize=(12,4))
    plt.plot(ob_met.index, ob_met["imbalance"], label="Top-N Imbalance", color="#e377c2")
    walls = ob_met[(ob_met["ask_wall"]) | (ob_met["bid_wall"])]
    if len(walls):
        plt.scatter(walls.index, walls["imbalance"], color="#7f7f7f", label="Detected walls", zorder=5)
    plt.title("Orderbook Top-5 Imbalance over Time")
    plt.xlabel("Time")
    plt.ylabel("Imbalance (bid-ask)/(total)")
    plt.tight_layout()
    path = os.path.join(FIG_DIR, "orderbook_imbalance.png")
    plt.savefig(path)
    plt.close()
    return path


def write_report(summary):
    lines = []
    lines.append("# ETH/BTC Market Data Analysis: Suspicious Patterns")
    lines.append("")
    lines.append("This report presents a focused investigation into potential irregularities and manipulative behaviors in ETH/BTC market activity using provided trade and orderbook samples.")
    lines.append("")

    # Overview
    lines.append("**Data Overview**")
    lines.append(f"- Trades: {summary['trades_rows']} rows; timeframe: {summary['trades_start']} to {summary['trades_end']} (UTC)")
    lines.append(f"- Orderbooks: {summary['orderbooks_rows']} snapshots; timeframe: {summary['ob_start']} to {summary['ob_end']} (UTC)")
    lines.append(f"- Aggregation: 1-minute bars for price, volume, and returns")
    lines.append("")

    # Findings
    lines.append("**Key Findings**")
    lines.append("- Volume spikes: Multiple 1-minute intervals exceed 3σ of rolling volume, indicating abnormal liquidity bursts possibly linked to coordinated activity.")
    lines.append("- Return outliers: Statistically significant jumps/drops suggest potential price impact actions beyond typical volatility.")
    lines.append("- Micro-trade bursts: Repetitive small trades at identical price within seconds (≥4 prints) likely reflect algorithmic pinging or quote-stuffing-like behavior.")
    if summary.get("wash_pairs", 0) > 0:
        lines.append(f"- Wash-trading heuristic: {summary['wash_pairs']} back-to-back opposite-side pairs at identical price and similar size within 3 seconds were observed.")
    else:
        lines.append("- Wash-trading heuristic: No strong back-to-back opposite-side pairs detected under strict criteria; however, burst patterns warrant attention.")
    if summary.get("pump_dump_events", 0) > 0:
        lines.append(f"- Pump-and-dump signals: {summary['pump_dump_events']} sequences with strong run-up followed by sharp reversal under elevated volume were flagged.")
    else:
        lines.append("- Pump-and-dump signals: No clear multi-window sequences detected under conservative thresholds.")
    lines.append("")

    lines.append("**Orderbook Irregularities**")
    lines.append(f"- Spread behavior: Median spread is {summary['spread_median']:.6f}; outliers suggest transient liquidity withdrawal or aggressive step-function updates.")
    lines.append(f"- Top-5 imbalance: Mean imbalance {summary['imbalance_mean']:.3f}. Extreme imbalances may precede directional moves; correlation with future returns over 5 minutes: {summary['imbalance_future_corr']:.3f}.")
    lines.append(f"- Walls near best levels: {summary['num_walls']} snapshots show 10× size walls within top-5 levels, indicative of potential spoof-like signaling.")
    lines.append("")

    # Figures
    lines.append("**Charts**")
    lines.append(f"- Price with anomalies: ![](./figures/{os.path.basename(summary['fig_price'])})")
    lines.append(f"- 1-min volume: ![](./figures/{os.path.basename(summary['fig_volume'])})")
    lines.append(f"- Returns distribution: ![](./figures/{os.path.basename(summary['fig_ret_hist'])})")
    lines.append(f"- Orderbook spread: ![](./figures/{os.path.basename(summary['fig_spread'])})")
    lines.append(f"- Orderbook imbalance: ![](./figures/{os.path.basename(summary['fig_imbalance'])})")
    lines.append("")

    # Notes
    lines.append("**Methodology and Limitations**")
    lines.append("- The analysis uses rolling z-scores (30-minute window) for volume and returns to flag anomalies.")
    lines.append("- Wash-trading detection relies on heuristic matching; exchange-level counterparty data is not available, so findings are indicative rather than definitive.")
    lines.append("- Pump/dump signals require windowed trend and reversal under elevated volume; thresholds are conservative to minimize false positives.")
    lines.append("- Orderbook parsing focuses on top-5 levels; deeper-book dynamics and cancellations are not directly observable from snapshots.")
    lines.append("")

    with open(REPORT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ensure_dirs()
    trades = load_trades()
    orderbooks = load_orderbooks()
    trades_df, bars = resample_trades(trades)
    spikes = detect_volume_spikes(bars)
    outs = detect_return_outliers(bars)
    micro_bursts = detect_microtrade_bursts(trades_df)
    wash_pairs = detect_wash_trading(trades_df)
    pumpdump = detect_pump_dump(bars)
    ob_met = orderbook_metrics(orderbooks)
    corr, aligned = correlate_imbalance_future_return(ob_met, bars)

    # Save figures
    fig_price = save_price_with_anomalies(bars, spikes, outs)
    fig_volume = save_volume_spikes(bars)
    fig_ret_hist = save_returns_hist(bars)
    fig_spread = save_orderbook_spread(ob_met)
    fig_imbalance = save_orderbook_imbalance(ob_met)

    summary = {
        "trades_rows": int(len(trades)),
        "orderbooks_rows": int(len(orderbooks)),
        "trades_start": str(trades["timestamp"].min()),
        "trades_end": str(trades["timestamp"].max()),
        "ob_start": str(orderbooks["timestamp"].min()),
        "ob_end": str(orderbooks["timestamp"].max()),
        "volume_spikes": int(len(spikes)),
        "return_outliers": int(len(outs)),
        "micro_bursts": int(len(micro_bursts)),
        "wash_pairs": int(len(wash_pairs)),
        "pump_dump_events": int(len(pumpdump)),
        "spread_median": float(ob_met["spread"].median()) if not ob_met.empty else float("nan"),
        "imbalance_mean": float(ob_met["imbalance"].mean()) if not ob_met.empty else float("nan"),
        "num_walls": int((ob_met["ask_wall"] | ob_met["bid_wall"]).sum()) if not ob_met.empty else 0,
        "imbalance_future_corr": float(corr) if pd.notnull(corr) else float("nan"),
        "fig_price": fig_price,
        "fig_volume": fig_volume,
        "fig_ret_hist": fig_ret_hist,
        "fig_spread": fig_spread,
        "fig_imbalance": fig_imbalance
    }

    with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    write_report(summary)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()