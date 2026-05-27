"""
Hyperliquid Microstructure Signal EDA — Vectorized Version
============================================================
Tests 12 signal hypotheses on HL trade + L2 data.
Side convention: A = Ask aggressor (seller-initiated / taker sell), B = Bid aggressor (buyer-initiated / taker buy)
Data: ~7.1 days of 1s-resolution trade + L2 data for BTC, ETH, HYPE, SOL
Forward returns: 5min, 15min, 1h
Fees: 3bp round-trip
All signal computation is vectorized — no Python loops over rows.
"""

import os
import warnings
import numpy as np
import pandas as pd
from scipy import stats
from pymongo import MongoClient

warnings.filterwarnings("ignore")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB  = os.environ.get("MONGO_DATABASE", "quants_lab")
FEE_BPS   = 3
FEE       = FEE_BPS / 10_000
COINS     = ["BTC", "ETH", "HYPE", "SOL"]

# Horizons: name → seconds → how many 1-min bars / 5-min bars ahead
HORIZONS = {"5min": 300, "15min": 900, "1h": 3600}

client = MongoClient(MONGO_URI)
db     = client[MONGO_DB]


# ──────────────────────────────────────────────────────────────
# DATA LOADING
# ──────────────────────────────────────────────────────────────

def load_trades(coins=COINS):
    print(f"Loading trades for {coins}...")
    docs = list(db["hyperliquid_recent_trades_1s"].find(
        {"coin": {"$in": coins}},
        {"_id": 0, "coin": 1, "time": 1, "px": 1, "sz": 1, "side": 1},
    ))
    df = pd.DataFrame(docs)
    df["ts"]        = df["time"] / 1000          # ms → seconds
    df["is_buy"]    = (df["side"] == "B")         # B = buyer aggressor
    df["notional"]  = df["px"] * df["sz"]
    df = df.sort_values(["coin", "ts"]).reset_index(drop=True)
    print(f"  Loaded {len(df):,} trade rows")
    return df


def load_l2(coins=COINS):
    print(f"Loading L2 snapshots for {coins}...")
    docs = list(db["hyperliquid_l2_snapshots_1s"].find(
        {"coin": {"$in": coins}},
        {"_id": 0, "coin": 1, "timestamp_utc": 1,
         "best_bid": 1, "best_ask": 1,
         "bid_sz_topn": 1, "ask_sz_topn": 1,
         "imbalance_topn": 1, "spread_bps": 1, "mid_px": 1},
    ))
    df = pd.DataFrame(docs)
    df["ts"] = df["timestamp_utc"] / 1000
    df = df.sort_values(["coin", "ts"]).reset_index(drop=True)
    print(f"  Loaded {len(df):,} L2 rows")
    return df


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────

def vectorized_fwd_return(px: np.ndarray, ts: np.ndarray, hz_sec: int) -> np.ndarray:
    """
    For each bar i, find the bar whose timestamp is closest to ts[i]+hz_sec
    (must land within hz_sec/2 tolerance). Pure numpy, O(n log n).
    """
    target = ts + hz_sec
    # searchsorted gives first idx where ts >= target
    j = np.searchsorted(ts, target, side="left")
    j = np.clip(j, 0, len(ts) - 1)
    # tolerance: must be within hz_sec*0.5 of target
    valid = (ts[j] - target) <= hz_sec * 0.5
    fwd = np.where(valid, np.log(px[j] / px), np.nan)
    return fwd


def non_overlapping_eval(signal_mask: np.ndarray, direction: np.ndarray,
                          fwd_ret: np.ndarray, ts: np.ndarray,
                          cooldown_sec: int, label: str, hz: str) -> dict:
    """
    Apply non-overlapping cooldown and compute stats.
    All inputs are numpy arrays of same length.
    """
    idx = np.where(signal_mask & np.isfinite(fwd_ret))[0]
    if len(idx) == 0:
        return _empty(label, hz)

    # Non-overlapping: fast loop (only over triggered indices, not all rows)
    selected = []
    last_ts   = -np.inf
    for i in idx:
        if ts[i] - last_ts >= cooldown_sec:
            selected.append(i)
            last_ts = ts[i]

    if len(selected) < 10:
        return {"signal": label, "horizon": hz, "n": len(selected),
                "mean_ret_bps": np.nan, "t_stat": np.nan, "p_value": np.nan, "sharpe": np.nan}

    sel = np.array(selected)
    pnl = direction[sel] * fwd_ret[sel] - FEE
    n   = len(pnl)
    mu  = np.nanmean(pnl)
    sd  = np.nanstd(pnl, ddof=1)
    if sd == 0 or np.isnan(sd):
        return _empty(label, hz)
    t_stat  = mu / (sd / np.sqrt(n))
    p_value = float(2 * stats.t.sf(abs(t_stat), df=n - 1))
    # annualised Sharpe: assume hz_sec holds per signal; signals/year = 86400/hz_sec * 365
    ann_factor = np.sqrt(365 * 86400 / HORIZONS.get(hz, 300))
    sharpe  = (mu / sd) * ann_factor
    return {
        "signal":       label,
        "horizon":      hz,
        "n":            n,
        "mean_ret_bps": round(mu * 10_000, 3),
        "t_stat":       round(t_stat, 3),
        "p_value":      round(p_value, 4),
        "sharpe":       round(sharpe, 3),
    }


def _empty(label, hz):
    return {"signal": label, "horizon": hz, "n": 0,
            "mean_ret_bps": np.nan, "t_stat": np.nan, "p_value": np.nan, "sharpe": np.nan}


def build_1min_bars(df: pd.DataFrame) -> pd.DataFrame:
    """1-minute OHLCV-style aggregation from raw trades."""
    df = df.copy()
    df["min_bucket"] = (df["ts"] // 60).astype(np.int64)
    buy_mask  = df["is_buy"].values
    not_buy   = ~buy_mask
    grp       = df.groupby("min_bucket")
    buy_vol   = df[buy_mask].groupby("min_bucket")["notional"].sum()
    sell_vol  = df[not_buy].groupby("min_bucket")["notional"].sum()
    bars = grp.agg(
        total_vol  = ("notional", "sum"),
        n_trades   = ("ts", "count"),
        last_px    = ("px", "last"),
        ts         = ("ts", "first"),
    )
    bars["buy_vol"]   = buy_vol.reindex(bars.index, fill_value=0)
    bars["sell_vol"]  = sell_vol.reindex(bars.index, fill_value=0)
    denom = bars["buy_vol"] + bars["sell_vol"]
    bars["imbalance"] = (bars["buy_vol"] - bars["sell_vol"]) / denom.replace(0, np.nan)
    bars = bars.reset_index()
    bars["ts"] = bars["min_bucket"] * 60
    return bars


def build_5min_bars(df: pd.DataFrame) -> pd.DataFrame:
    """5-minute aggregation."""
    df = df.copy()
    df["bucket"] = (df["ts"] // 300).astype(np.int64)
    buy_mask = df["is_buy"].values
    not_buy  = ~buy_mask
    grp = df.groupby("bucket")
    buy_vol  = df[buy_mask].groupby("bucket")["notional"].sum()
    sell_vol = df[not_buy].groupby("bucket")["notional"].sum()
    small_ct = (df[df["notional"] < 1_000]).groupby("bucket")["notional"].count()
    large_ct = (df[df["notional"] > 10_000]).groupby("bucket")["notional"].count()
    bars = grp.agg(
        total_vol = ("notional", "sum"),
        n_trades  = ("ts", "count"),
        last_px   = ("px", "last"),
        ts        = ("ts", "first"),
    )
    bars["buy_vol"]         = buy_vol.reindex(bars.index, fill_value=0)
    bars["sell_vol"]        = sell_vol.reindex(bars.index, fill_value=0)
    bars["small_trades"]    = small_ct.reindex(bars.index, fill_value=0)
    bars["large_trades"]    = large_ct.reindex(bars.index, fill_value=0)
    denom = bars["buy_vol"] + bars["sell_vol"]
    bars["imbalance"]       = (bars["buy_vol"] - bars["sell_vol"]) / denom.replace(0, np.nan)
    bars["small_large_ratio"] = bars["small_trades"] / (bars["large_trades"] + 1)
    bars = bars.reset_index()
    bars["ts"] = bars["bucket"] * 300
    return bars


# ──────────────────────────────────────────────────────────────
# SIGNALS
# ──────────────────────────────────────────────────────────────

def signal_1_trade_imbalance(bars5: pd.DataFrame, coin: str, results: list):
    """S1: 5-min buy/sell imbalance > 70% → fade (crowd overextended)."""
    px = bars5["last_px"].values
    ts = bars5["ts"].values
    imb = bars5["imbalance"].fillna(0).values

    for hz, hz_sec in HORIZONS.items():
        n_bars = max(1, hz_sec // 300)
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal = (np.abs(imb) > 0.70)
        # Fade: buy crowd → short; sell crowd → long
        direction = np.where(imb > 0.70, -1, np.where(imb < -0.70, 1, 0)).astype(float)
        results.append(non_overlapping_eval(signal, direction, fwd, ts, hz_sec,
                                            f"S1_TradeImbalanceFade_{coin}", hz))


def signal_2_large_trade(t_coin: pd.DataFrame, coin: str, results: list):
    """S2: Trades > 90th pct size → test continuation (follow direction) — vectorized via merge_asof."""
    thr = t_coin["notional"].quantile(0.90)
    large = t_coin[t_coin["notional"] > thr].copy().sort_values("ts").reset_index(drop=True)
    if len(large) < 30:
        return

    # Price series for forward return lookup
    price_ts = t_coin[["ts", "px"]].sort_values("ts").drop_duplicates("ts").reset_index(drop=True)
    px_arr = price_ts["px"].values
    ts_arr = price_ts["ts"].values

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px_arr, ts_arr, hz_sec)
        # Map each large trade to its forward return
        # searchsorted to find large trade position in ts_arr
        pos = np.searchsorted(ts_arr, large["ts"].values, side="left")
        pos = np.clip(pos, 0, len(ts_arr) - 1)
        large_fwd = fwd[pos]
        direction = np.where(large["is_buy"].values, 1.0, -1.0)
        signal = np.ones(len(large), dtype=bool)
        results.append(non_overlapping_eval(
            signal, direction, large_fwd, large["ts"].values, hz_sec,
            f"S2_LargeTradeContinuation_{coin}", hz,
        ))


def signal_3_arrival_rate(bars1: pd.DataFrame, coin: str, results: list):
    """S3: Trade arrival rate spike > 2 std → fade."""
    px = bars1["last_px"].values
    ts = bars1["ts"].values
    n  = bars1["n_trades"].values.astype(float)

    roll_m = pd.Series(n).rolling(30, min_periods=10).mean().values
    roll_s = pd.Series(n).rolling(30, min_periods=10).std().values
    z = (n - roll_m) / (roll_s + 0.001)

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal    = np.abs(z) > 2.0
        direction = np.where(z > 2.0, -1.0, np.where(z < -2.0, 1.0, 0.0))
        results.append(non_overlapping_eval(signal, direction, fwd, ts, 60,
                                            f"S3_ArrivalRateFade_{coin}", hz))


def signal_4_vpin(t_coin: pd.DataFrame, coin: str, results: list):
    """S4: VPIN over 50 volume buckets → directional prediction."""
    df = t_coin.copy().sort_values("ts").reset_index(drop=True)
    if len(df) < 200:
        return
    V = df["sz"].sum() / 50
    if V <= 0:
        return
    df["cum_sz"] = df["sz"].cumsum()
    df["bucket"] = (df["cum_sz"] // V).astype(int)

    grp = df.groupby("bucket")
    buy_v  = df[df["is_buy"]].groupby("bucket")["sz"].sum()
    sell_v = df[~df["is_buy"]].groupby("bucket")["sz"].sum()
    bkt = grp.agg(total_vol=("sz", "sum"), last_px=("px", "last"), ts=("ts", "last"))
    bkt["buy_vol"]  = buy_v.reindex(bkt.index, fill_value=0)
    bkt["sell_vol"] = sell_v.reindex(bkt.index, fill_value=0)
    bkt["tau"]      = (bkt["buy_vol"] - bkt["sell_vol"]).abs() / bkt["total_vol"].replace(0, np.nan)
    bkt["vpin"]     = bkt["tau"].rolling(10, min_periods=5).mean()
    bkt             = bkt.reset_index().dropna(subset=["vpin"])
    if len(bkt) < 20:
        return

    thr       = bkt["vpin"].quantile(0.75)
    signal    = (bkt["vpin"] > thr).values
    buy_dom   = (bkt["buy_vol"] > bkt["sell_vol"]).values
    direction = np.where(buy_dom, 1.0, -1.0)
    px        = bkt["last_px"].values
    ts_b      = bkt["ts"].values

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts_b, hz_sec)
        results.append(non_overlapping_eval(signal, direction, fwd, ts_b, hz_sec,
                                            f"S4_VPIN_{coin}", hz))


def signal_5_small_large_ratio(bars5: pd.DataFrame, coin: str, results: list):
    """S5: Small/large trade ratio spike → retail frenzy → fade."""
    px  = bars5["last_px"].values
    ts  = bars5["ts"].values
    slr = bars5["small_large_ratio"].values.astype(float)
    imb = bars5["imbalance"].fillna(0).values

    roll_m = pd.Series(slr).rolling(12, min_periods=6).mean().values
    roll_s = pd.Series(slr).rolling(12, min_periods=6).std().values
    z = (slr - roll_m) / (roll_s + 0.01)

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal    = z > 2.0
        direction = np.where(imb > 0, -1.0, 1.0)   # fade dominant flow
        results.append(non_overlapping_eval(signal, direction, fwd, ts, 300,
                                            f"S5_SmallLargeRatioFade_{coin}", hz))


def signal_6_aggressive_flow(bars1: pd.DataFrame, coin: str, results: list):
    """S6: Rolling 5-min aggressive flow imbalance > 60% → direction."""
    px      = bars1["last_px"].values
    ts      = bars1["ts"].values
    buy_v   = bars1["buy_vol"].values
    sell_v  = bars1["sell_vol"].values

    b5 = pd.Series(buy_v).rolling(5, min_periods=3).sum().values
    s5 = pd.Series(sell_v).rolling(5, min_periods=3).sum().values
    roll_imb = (b5 - s5) / (b5 + s5 + 1e-9)

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal    = np.abs(roll_imb) > 0.60
        direction = np.where(roll_imb > 0.60, 1.0, np.where(roll_imb < -0.60, -1.0, 0.0))
        results.append(non_overlapping_eval(signal, direction, fwd, ts, 60,
                                            f"S6_AggressiveFlow_{coin}", hz))


def signal_7_volume_burst(bars1: pd.DataFrame, coin: str, results: list):
    """S7: 1-min volume > 3x rolling 30-min avg → follow burst direction."""
    px  = bars1["last_px"].values
    ts  = bars1["ts"].values
    vol = bars1["total_vol"].values.astype(float)
    imb = bars1["imbalance"].fillna(0).values

    roll_avg  = pd.Series(vol).rolling(30, min_periods=10).mean().values
    vol_ratio = vol / (roll_avg + 1)

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal    = vol_ratio > 3.0
        direction = np.where(imb > 0, 1.0, -1.0)
        results.append(non_overlapping_eval(signal, direction, fwd, ts, 60,
                                            f"S7_VolumeBurst_{coin}", hz))


def signal_8_cross_coin_flow(trades: pd.DataFrame, results: list):
    """S8: BTC 1-min imbalance at lag 1/3/5 → predict ETH direction."""
    btc = trades[trades["coin"] == "BTC"]
    eth = trades[trades["coin"] == "ETH"]
    if len(btc) < 100 or len(eth) < 100:
        return

    btc1 = build_1min_bars(btc)[["ts", "imbalance", "last_px"]].rename(
        columns={"imbalance": "btc_imb", "last_px": "btc_px"})
    eth1 = build_1min_bars(eth)[["ts", "imbalance", "last_px"]].rename(
        columns={"imbalance": "eth_imb", "last_px": "eth_px"})

    merged = btc1.merge(eth1, on="ts", how="inner")
    if len(merged) < 50:
        return

    px = merged["eth_px"].values
    ts = merged["ts"].values

    for lag in [1, 3, 5]:
        btc_imb_lag = pd.Series(merged["btc_imb"].values).shift(lag).values
        for hz, hz_sec in HORIZONS.items():
            fwd = vectorized_fwd_return(px, ts, hz_sec)
            signal    = np.abs(btc_imb_lag) > 0.70
            direction = np.where(btc_imb_lag > 0.70, 1.0, np.where(btc_imb_lag < -0.70, -1.0, 0.0))
            valid     = np.isfinite(btc_imb_lag)
            results.append(non_overlapping_eval(
                signal & valid, direction, fwd, ts, 60,
                f"S8_CrossCoin_BTClag{lag}m_ETH", hz,
            ))


def signal_9_spread_fade(l2: pd.DataFrame, coin: str, results: list):
    """S9: Spread widens > 2 std → fade the directional move."""
    px  = l2["mid_px"].values
    ts  = l2["ts"].values
    spd = l2["spread_bps"].values.astype(float)
    imb = l2["imbalance_topn"].fillna(0).values

    roll_m = pd.Series(spd).rolling(60, min_periods=20).mean().values
    roll_s = pd.Series(spd).rolling(60, min_periods=20).std().values
    z = (spd - roll_m) / (roll_s + 0.001)

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal    = z > 2.0
        direction = np.where(imb > 0, -1.0, 1.0)   # fade
        results.append(non_overlapping_eval(signal, direction, fwd, ts, hz_sec,
                                            f"S9_SpreadWidenFade_{coin}", hz))


def signal_10_l2_depth_imbalance(l2: pd.DataFrame, coin: str, results: list):
    """S10: Top-5 L2 depth imbalance > 30% → directional bet."""
    px  = l2["mid_px"].values
    ts  = l2["ts"].values
    imb = l2["imbalance_topn"].fillna(0).values

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal    = np.abs(imb) > 0.30
        direction = np.where(imb > 0.30, 1.0, np.where(imb < -0.30, -1.0, 0.0))
        results.append(non_overlapping_eval(signal, direction, fwd, ts, hz_sec,
                                            f"S10_L2DepthImbalance_{coin}", hz))


def signal_11_l2_absorption(l2: pd.DataFrame, coin: str, results: list):
    """S11: High depth-per-bps (thick book) with imbalance → reversal."""
    px  = l2["mid_px"].values
    ts  = l2["ts"].values
    spd = l2["spread_bps"].values.astype(float)
    bid = l2["bid_sz_topn"].values.astype(float)
    ask = l2["ask_sz_topn"].values.astype(float)
    imb = l2["imbalance_topn"].fillna(0).values

    depth_per_spd = (bid + ask) / (spd + 0.01)
    roll_m = pd.Series(depth_per_spd).rolling(60, min_periods=20).mean().values
    roll_s = pd.Series(depth_per_spd).rolling(60, min_periods=20).std().values
    z = (depth_per_spd - roll_m) / (roll_s + 0.01)

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)
        signal    = (z > 1.5) & (np.abs(imb) > 0.20)
        direction = np.where(imb > 0, -1.0, 1.0)   # fade the imbalance
        results.append(non_overlapping_eval(signal, direction, fwd, ts, hz_sec,
                                            f"S11_L2Absorption_{coin}", hz))


def signal_12_hurst_clustering(t_coin: pd.DataFrame, coin: str, results: list):
    """S12: Hurst exponent of inter-trade arrival times → momentum vs mean-reversion.
    Computed on rolling 100-trade windows, stride 50. Vectorized over windows.
    """
    df = t_coin.copy().sort_values("ts").reset_index(drop=True)
    if len(df) < 300:
        return

    iat = df["ts"].diff().fillna(0).values
    window = 100
    stride = 50

    starts = range(window, len(df), stride)
    hurst_vals, ts_vals, px_vals = [], [], []

    for i in starts:
        seg = iat[i - window:i]
        seg = seg[seg > 0]
        if len(seg) < 20:
            continue
        seg = np.log1p(seg)   # log-transform for better R/S
        mean = seg.mean()
        dev  = np.cumsum(seg - mean)
        R    = dev.max() - dev.min()
        S    = seg.std(ddof=1)
        if S == 0:
            h = 0.5
        else:
            h = np.log(R / S) / np.log(len(seg))
        hurst_vals.append(h)
        ts_vals.append(df["ts"].iloc[i])
        px_vals.append(df["px"].iloc[i])

    if len(hurst_vals) < 20:
        return

    hdf = pd.DataFrame({"hurst": hurst_vals, "ts": ts_vals, "px": px_vals})
    # Get imbalance from 5-min bars
    bars5 = build_5min_bars(t_coin)
    b5_ts  = bars5["ts"].values
    b5_imb = bars5["imbalance"].fillna(0).values

    # Assign imbalance to each hurst window via searchsorted
    pos = np.searchsorted(b5_ts, hdf["ts"].values, side="right") - 1
    pos = np.clip(pos, 0, len(b5_imb) - 1)
    hdf["imbalance"] = b5_imb[pos]

    px = hdf["px"].values
    ts = hdf["ts"].values
    h  = hdf["hurst"].values
    imb = hdf["imbalance"].values

    for hz, hz_sec in HORIZONS.items():
        fwd = vectorized_fwd_return(px, ts, hz_sec)

        # Momentum regime (H > 0.6): follow flow
        mom_mask  = h > 0.60
        mom_dir   = np.sign(imb)
        results.append(non_overlapping_eval(mom_mask, mom_dir, fwd, ts, hz_sec,
                                            f"S12_Hurst_Momentum_{coin}", hz))

        # Mean-rev regime (H < 0.40): fade flow
        mr_mask   = h < 0.40
        mr_dir    = -np.sign(imb)
        results.append(non_overlapping_eval(mr_mask, mr_dir, fwd, ts, hz_sec,
                                            f"S12_Hurst_MeanRev_{coin}", hz))


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("HYPERLIQUID MICROSTRUCTURE SIGNAL EDA")
    print("=" * 70)
    print(f"Coins: {COINS}  |  Fees: {FEE_BPS}bp round-trip")
    print()

    trades = load_trades(COINS)
    l2     = load_l2(COINS)
    print()

    all_results = []

    for coin in COINS:
        print(f"\n{'─'*55}")
        print(f"[{coin}] Building bars...")
        t = trades[trades["coin"] == coin].copy().sort_values("ts").reset_index(drop=True)
        l = l2[l2["coin"] == coin].copy().sort_values("ts").reset_index(drop=True)
        print(f"  Trades: {len(t):,}  |  L2: {len(l):,}")

        bars1 = build_1min_bars(t)
        bars5 = build_5min_bars(t)
        print(f"  1-min bars: {len(bars1):,}  |  5-min bars: {len(bars5):,}")

        print(f"  [S1]  Trade Imbalance Fade...")
        signal_1_trade_imbalance(bars5, coin, all_results)

        print(f"  [S2]  Large Trade Continuation...")
        signal_2_large_trade(t, coin, all_results)

        print(f"  [S3]  Arrival Rate Fade...")
        signal_3_arrival_rate(bars1, coin, all_results)

        print(f"  [S4]  VPIN...")
        signal_4_vpin(t, coin, all_results)

        print(f"  [S5]  Small/Large Ratio Fade...")
        signal_5_small_large_ratio(bars5, coin, all_results)

        print(f"  [S6]  Aggressive Flow...")
        signal_6_aggressive_flow(bars1, coin, all_results)

        print(f"  [S7]  Volume Burst...")
        signal_7_volume_burst(bars1, coin, all_results)

        print(f"  [S9]  Spread Widen Fade...")
        signal_9_spread_fade(l, coin, all_results)

        print(f"  [S10] L2 Depth Imbalance...")
        signal_10_l2_depth_imbalance(l, coin, all_results)

        print(f"  [S11] L2 Absorption...")
        signal_11_l2_absorption(l, coin, all_results)

        print(f"  [S12] Hurst Clustering...")
        signal_12_hurst_clustering(t, coin, all_results)

    print(f"\n  [S8]  Cross-coin flow BTC→ETH...")
    signal_8_cross_coin_flow(trades, all_results)

    # ──────────────────────────────────────────────────────────────
    # RESULTS
    # ──────────────────────────────────────────────────────────────
    df = pd.DataFrame(all_results)
    df = df.dropna(subset=["mean_ret_bps"])
    df = df.sort_values("t_stat", ascending=False).reset_index(drop=True)

    W = 105
    print("\n\n" + "=" * W)
    print("RESULTS — ALL SIGNALS")
    print("=" * W)
    hdr = f"{'Signal':<52} {'Horizon':<8} {'N':>6} {'MeanBps':>9} {'t-stat':>8} {'p-val':>8} {'Sharpe':>8}"
    print(hdr)
    print("-" * W)
    for _, r in df.iterrows():
        star = "***" if r["p_value"] < 0.01 else ("**" if r["p_value"] < 0.05 else ("*" if r["p_value"] < 0.10 else "   "))
        print(f"{r['signal']:<52} {r['horizon']:<8} {int(r['n']):>6} {r['mean_ret_bps']:>9.3f} "
              f"{r['t_stat']:>8.3f} {r['p_value']:>8.4f} {r['sharpe']:>8.3f} {star}")

    print("\n\n" + "=" * W)
    print("TOP SIGNALS — p < 0.05, t > 2, mean_ret > fee (>3bps)")
    print("=" * W)
    top = df[(df["p_value"] < 0.05) & (df["t_stat"] > 2) & (df["mean_ret_bps"] > FEE_BPS)]
    if top.empty:
        print("  NONE — no signal clears significance + fee hurdle.")
    else:
        print(hdr)
        print("-" * W)
        for _, r in top.iterrows():
            print(f"{r['signal']:<52} {r['horizon']:<8} {int(r['n']):>6} {r['mean_ret_bps']:>9.3f} "
                  f"{r['t_stat']:>8.3f} {r['p_value']:>8.4f} {r['sharpe']:>8.3f}")

    print("\n\n" + "=" * W)
    print("NEGATIVE EDGES — p < 0.10, t < -1.65 (opposite of hypothesis)")
    print("=" * W)
    neg = df[(df["p_value"] < 0.10) & (df["t_stat"] < -1.65)]
    if neg.empty:
        print("  NONE.")
    else:
        for _, r in neg.iterrows():
            print(f"{r['signal']:<52} {r['horizon']:<8} {int(r['n']):>6} {r['mean_ret_bps']:>9.3f} "
                  f"{r['t_stat']:>8.3f} {r['p_value']:>8.4f}")

    print("\n\n" + "=" * W)
    print("SUMMARY — Best horizon per signal base (ranked by |t-stat|)")
    print("=" * W)
    df["base"] = df["signal"].str.extract(r"^(S\d+_[A-Za-z]+(?:_[A-Za-z]+)?)")
    best = (df.assign(abst=df["t_stat"].abs())
             .sort_values("abst", ascending=False)
             .groupby("base").first()
             .reset_index()
             .sort_values("abst", ascending=False))

    print(f"{'Base Signal':<45} {'Horizon':<8} {'MeanBps':>9} {'t-stat':>8} {'p-val':>8} {'Verdict'}")
    print("-" * W)
    for _, r in best.iterrows():
        if r["p_value"] < 0.01 and abs(r["t_stat"]) > 3:
            verdict = "STRONG — develop controller"
        elif r["p_value"] < 0.05 and abs(r["t_stat"]) > 2:
            verdict = "MARGINAL — needs more data"
        else:
            verdict = "dead / noise"
        sign = "+" if r["mean_ret_bps"] > 0 else "-"
        print(f"{r['base']:<45} {r['horizon']:<8} {sign}{abs(r['mean_ret_bps']):>8.3f} "
              f"{r['t_stat']:>8.3f} {r['p_value']:>8.4f} {verdict}")

    # Save
    out = "/Users/hermes/quants-lab/app/research/microstructure_eda_results.csv"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False)
    print(f"\nFull results saved → {out}")


if __name__ == "__main__":
    main()
