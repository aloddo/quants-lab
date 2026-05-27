"""
Systematic EDA: 14 Signal Hypotheses
Whale Signals (hl_whale_positions) + Cross-Sectional Statistical Signals (hyperliquid_candles_1h)

Author: quant-engineer agent
Date: 2026-05-06

IMPORTANT CAVEATS:
- Whale data: only 11 days (Apr 21 – May 2, 2026). All whale signal stats have tiny sample sizes.
- Candle data: ~1yr (May 2025 – May 2026), 59 coins, 1h resolution.
- All net returns are GROSS - 3bp round-trip fee (0.0003) deducted.
- No actual order execution modeled; signals are indicative.
"""

import os
import sys
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from scipy import stats
from pymongo import MongoClient
from datetime import timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────
FEE_RT = 0.0003          # 3bp round-trip
WHALE_SNAPSHOT_FREQ = '15min'   # resample whale snapshots to ~15min bins
CANDLE_COINS_MIN = 200          # min candle rows to include a coin
MIN_TRADES_REPORT = 5           # suppress signals with fewer trades

client = MongoClient(os.environ['MONGO_URI'])
db = client[os.environ['MONGO_DATABASE']]


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_whale_data() -> pd.DataFrame:
    """Load all whale positions into a DataFrame."""
    print("Loading whale positions …", end=' ', flush=True)
    docs = list(db.hl_whale_positions.find(
        {},
        {"_id": 0, "timestamp_utc": 1, "address": 1, "coin": 1, "side": 1,
         "notional_usd": 1, "leverage": 1, "unrealized_pnl": 1, "account_value": 1,
         "size": 1}
    ))
    df = pd.DataFrame(docs)
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
    df['signed_notional'] = np.where(df['side'] == 'LONG', df['notional_usd'], -df['notional_usd'])
    print(f"{len(df):,} rows | {df['timestamp_utc'].min().date()} → {df['timestamp_utc'].max().date()}")
    return df


def load_candles() -> pd.DataFrame:
    """Load all 1h candles, keep only coins with sufficient history."""
    print("Loading 1h candles …", end=' ', flush=True)
    docs = list(db.hyperliquid_candles_1h.find(
        {},
        {"_id": 0, "coin": 1, "timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
    ))
    df = pd.DataFrame(docs)
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], unit='ms', utc=True)
    # Filter coins with enough rows
    counts = df.groupby('coin').size()
    good_coins = counts[counts >= CANDLE_COINS_MIN].index
    df = df[df['coin'].isin(good_coins)].copy()
    df = df.sort_values(['coin', 'timestamp_utc']).reset_index(drop=True)
    print(f"{len(df):,} rows | {df['coin'].nunique()} coins | {df['timestamp_utc'].min().date()} → {df['timestamp_utc'].max().date()}")
    return df


def load_funding() -> pd.DataFrame:
    """Load funding rates for whale coins (last 400 days)."""
    print("Loading funding rates …", end=' ', flush=True)
    whale_coins = db.hl_whale_positions.distinct("coin")
    docs = list(db.hyperliquid_funding_rates.find(
        {"coin": {"$in": whale_coins + ['BTC']}},
        {"_id": 0, "coin": 1, "timestamp_utc": 1, "funding_rate": 1}
    ))
    df = pd.DataFrame(docs)
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], unit='ms', utc=True)
    print(f"{len(df):,} rows")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY
# ══════════════════════════════════════════════════════════════════════════════

def signal_stats(returns: pd.Series, label: str) -> dict:
    """Compute signal statistics from a series of per-trade net returns."""
    r = returns.dropna()
    n = len(r)
    if n < MIN_TRADES_REPORT:
        return {"signal": label, "n_trades": n, "mean_return_bps": np.nan,
                "t_stat": np.nan, "p_value": np.nan, "sharpe": np.nan, "win_rate": np.nan}
    mean_r = r.mean()
    t, p = stats.ttest_1samp(r, 0)
    sr = mean_r / (r.std() + 1e-12) * np.sqrt(252 * 24)  # annualized (1h bars)
    wr = (r > 0).mean()
    return {
        "signal": label,
        "n_trades": n,
        "mean_return_bps": round(mean_r * 10000, 2),
        "t_stat": round(t, 3),
        "p_value": round(p, 4),
        "sharpe_annualized": round(sr, 2),
        "win_rate_pct": round(wr * 100, 1),
    }


def pivot_candles(df_candles: pd.DataFrame) -> pd.DataFrame:
    """Pivot candles to wide format: index=timestamp, columns=coin, values=close."""
    return df_candles.pivot_table(index='timestamp_utc', columns='coin', values='close')


# ══════════════════════════════════════════════════════════════════════════════
# WHALE SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

def build_whale_snapshots(whale: pd.DataFrame) -> pd.DataFrame:
    """Aggregate whale positions into regular ~15min snapshots per coin."""
    # Resample to 15min bins
    whale['ts_bin'] = whale['timestamp_utc'].dt.floor(WHALE_SNAPSHOT_FREQ)
    snap = whale.groupby(['ts_bin', 'coin']).agg(
        net_delta=('signed_notional', 'sum'),
        total_long=('notional_usd', lambda x: x[whale.loc[x.index, 'side'] == 'LONG'].sum()),
        total_short=('notional_usd', lambda x: x[whale.loc[x.index, 'side'] == 'SHORT'].sum()),
        n_longs=('side', lambda x: (x == 'LONG').sum()),
        n_shorts=('side', lambda x: (x == 'SHORT').sum()),
        avg_leverage=('leverage', 'mean'),
        avg_upnl=('unrealized_pnl', 'mean'),
        max_acct=('account_value', 'max'),
    ).reset_index()
    snap['n_total'] = snap['n_longs'] + snap['n_shorts']
    snap['long_pct'] = snap['n_longs'] / snap['n_total'].replace(0, np.nan)
    return snap.sort_values(['coin', 'ts_bin'])


def merge_candle_returns(snap: pd.DataFrame, candles: pd.DataFrame, fwd_bars: int = 4) -> pd.DataFrame:
    """Attach forward candle returns to each snapshot row."""
    # Build a close price map: coin → Series indexed by hour
    closes = candles.groupby('coin')['close'].last()  # placeholder
    # Full pivot
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    fwd_ret = close_pivot.pct_change(fwd_bars).shift(-fwd_bars)  # forward return

    results = []
    for coin, grp in snap.groupby('coin'):
        if coin not in fwd_ret.columns:
            continue
        grp = grp.copy()
        # Floor snap ts to 1h to align with candles
        grp['ts_1h'] = grp['ts_bin'].dt.floor('1h')
        fwd = fwd_ret[coin].rename('fwd_ret')
        grp = grp.merge(fwd.reset_index().rename(columns={'timestamp_utc': 'ts_1h'}),
                        on='ts_1h', how='left')
        results.append(grp)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def signal_1_whale_net_delta(snap: pd.DataFrame, candles: pd.DataFrame) -> dict:
    """S1: Change in whale net delta over 1 snapshot → 4h fwd return."""
    fwd_bars = 4
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    fwd_ret = close_pivot.pct_change(fwd_bars).shift(-fwd_bars)

    all_ret = []
    for coin, grp in snap.groupby('coin'):
        if coin not in fwd_ret.columns:
            continue
        grp = grp.copy().sort_values('ts_bin')
        grp['delta_change'] = grp['net_delta'].diff()
        grp['signal'] = np.sign(grp['delta_change'])
        grp['ts_1h'] = grp['ts_bin'].dt.floor('1h')
        fwd = fwd_ret[coin]
        for _, row in grp.iterrows():
            if pd.isna(row['signal']) or row['signal'] == 0:
                continue
            ret = fwd.get(row['ts_1h'], np.nan)
            if pd.notna(ret):
                all_ret.append(row['signal'] * ret - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S1: Whale Net Delta Change (4h fwd)")


def signal_2_whale_consensus_flip(snap: pd.DataFrame, candles: pd.DataFrame) -> dict:
    """S2: >60% whales on same side, then flip next snapshot → contrarian signal."""
    THRESHOLD = 0.60
    fwd_bars = 4
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    fwd_ret = close_pivot.pct_change(fwd_bars).shift(-fwd_bars)

    all_ret = []
    for coin, grp in snap.groupby('coin'):
        if coin not in fwd_ret.columns:
            continue
        grp = grp.copy().sort_values('ts_bin').reset_index(drop=True)
        grp['prev_long_pct'] = grp['long_pct'].shift(1)
        grp['was_consensus_long'] = grp['prev_long_pct'] >= THRESHOLD
        grp['was_consensus_short'] = grp['prev_long_pct'] <= (1 - THRESHOLD)
        # Flip: was consensus long, now long_pct < 0.5
        grp['flip_short'] = grp['was_consensus_long'] & (grp['long_pct'] < 0.5)
        grp['flip_long'] = grp['was_consensus_short'] & (grp['long_pct'] > 0.5)
        grp['signal'] = np.where(grp['flip_long'], 1, np.where(grp['flip_short'], -1, 0))
        grp['ts_1h'] = grp['ts_bin'].dt.floor('1h')
        fwd = fwd_ret[coin]
        for _, row in grp[grp['signal'] != 0].iterrows():
            ret = fwd.get(row['ts_1h'], np.nan)
            if pd.notna(ret):
                all_ret.append(row['signal'] * ret - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S2: Whale Consensus Flip (4h fwd)")


def signal_3_whale_leverage_change(snap: pd.DataFrame, candles: pd.DataFrame) -> dict:
    """S3: Increasing average leverage → continuation trade."""
    fwd_bars = 4
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    fwd_ret = close_pivot.pct_change(fwd_bars).shift(-fwd_bars)

    all_ret = []
    for coin, grp in snap.groupby('coin'):
        if coin not in fwd_ret.columns:
            continue
        grp = grp.copy().sort_values('ts_bin')
        grp['lev_delta'] = grp['avg_leverage'].diff()
        # direction = current net delta direction; magnitude = leverage change
        grp['direction'] = np.sign(grp['net_delta'])
        grp['signal'] = np.sign(grp['lev_delta']) * grp['direction']
        grp['ts_1h'] = grp['ts_bin'].dt.floor('1h')
        fwd = fwd_ret[coin]
        for _, row in grp[grp['signal'] != 0].iterrows():
            ret = fwd.get(row['ts_1h'], np.nan)
            if pd.notna(ret):
                all_ret.append(row['signal'] * ret - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S3: Whale Leverage Change (4h fwd)")


def signal_4_whale_pnl_sentiment(snap: pd.DataFrame, candles: pd.DataFrame) -> dict:
    """S4: Deeply negative avg unrealized PnL → liquidation risk → reversal."""
    # 'deeply negative' = avg_upnl < -2 std of its distribution
    fwd_bars = 4
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    fwd_ret = close_pivot.pct_change(fwd_bars).shift(-fwd_bars)

    all_ret = []
    for coin, grp in snap.groupby('coin'):
        if coin not in fwd_ret.columns or len(grp) < 10:
            continue
        grp = grp.copy().sort_values('ts_bin')
        mu, sigma = grp['avg_upnl'].mean(), grp['avg_upnl'].std()
        if sigma == 0:
            continue
        grp['upnl_z'] = (grp['avg_upnl'] - mu) / sigma
        # Deeply negative PnL (z < -2) → longs are hurting → fade the current direction
        # If net_delta > 0 (net long, hurting) → short signal
        grp['signal'] = np.where(grp['upnl_z'] < -2, -np.sign(grp['net_delta']), 0)
        grp['ts_1h'] = grp['ts_bin'].dt.floor('1h')
        fwd = fwd_ret[coin]
        for _, row in grp[grp['signal'] != 0].iterrows():
            ret = fwd.get(row['ts_1h'], np.nan)
            if pd.notna(ret):
                all_ret.append(row['signal'] * ret - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S4: Whale PnL Distress Reversal (4h fwd)")


def signal_5_top_whale_copy(whale: pd.DataFrame, candles: pd.DataFrame) -> dict:
    """S5: Copy top-5 whales by account_value; trade their new position changes."""
    fwd_bars = 4
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    fwd_ret = close_pivot.pct_change(fwd_bars).shift(-fwd_bars)

    # Identify top-5 whales by max account value seen
    top5 = (whale.groupby('address')['account_value'].max()
            .sort_values(ascending=False).head(5).index.tolist())

    all_ret = []
    for addr in top5:
        sub = whale[whale['address'] == addr].copy().sort_values('timestamp_utc')
        for coin, grp in sub.groupby('coin'):
            if coin not in fwd_ret.columns:
                continue
            grp = grp.copy().sort_values('timestamp_utc').reset_index(drop=True)
            grp['prev_side'] = grp['side'].shift(1)
            grp['ts_1h'] = grp['timestamp_utc'].dt.floor('1h')
            # Detect flip: LONG→SHORT or SHORT→LONG
            flips = grp[grp['side'] != grp['prev_side']].copy()
            fwd = fwd_ret[coin]
            for _, row in flips.iterrows():
                direction = 1 if row['side'] == 'LONG' else -1
                ret = fwd.get(row['ts_1h'], np.nan)
                if pd.notna(ret):
                    all_ret.append(direction * ret - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S5: Top-5 Whale Copy Trade (4h fwd)")


def signal_6_whale_hhi(snap: pd.DataFrame, candles: pd.DataFrame) -> dict:
    """S6: High HHI concentration → fragile position → mean reversion."""
    fwd_bars = 4
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    fwd_ret = close_pivot.pct_change(fwd_bars).shift(-fwd_bars)

    # Compute HHI per snapshot per coin from raw whale data
    whale_raw = whale_data  # global set in main

    all_ret = []
    for coin, grp in snap.groupby('coin'):
        if coin not in fwd_ret.columns:
            continue
        grp = grp.copy().sort_values('ts_bin')
        # HHI = sum of squared market shares
        # We approximate using per-snapshot total long/short as shares
        total = grp['net_delta'].abs() + 1e-6
        # Use ratio of largest single direction as proxy (can't recompute by-whale here)
        max_dir = grp[['total_long', 'total_short']].max(axis=1)
        total_notional = grp['total_long'] + grp['total_short']
        grp['concentration'] = max_dir / (total_notional + 1e-6)

        mu, sigma = grp['concentration'].mean(), grp['concentration'].std()
        if sigma == 0:
            continue
        grp['conc_z'] = (grp['concentration'] - mu) / sigma
        # High concentration + direction = fade signal
        grp['signal'] = np.where(grp['conc_z'] > 1.5, -np.sign(grp['net_delta']), 0)
        grp['ts_1h'] = grp['ts_bin'].dt.floor('1h')
        fwd = fwd_ret[coin]
        for _, row in grp[grp['signal'] != 0].iterrows():
            ret = fwd.get(row['ts_1h'], np.nan)
            if pd.notna(ret):
                all_ret.append(row['signal'] * ret - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S6: Whale HHI Concentration Fade (4h fwd)")


# ══════════════════════════════════════════════════════════════════════════════
# CROSS-SECTIONAL SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

def signal_7_xsect_momentum_4h(candles: pd.DataFrame) -> dict:
    """S7: Cross-sectional 4h momentum — long top quintile, short bottom quintile."""
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    ret_4h = close_pivot.pct_change(4)  # 4h return
    fwd_4h = ret_4h.shift(-4)           # 4h forward return

    all_ret = []
    for ts in ret_4h.index[4:]:
        row = ret_4h.loc[ts].dropna()
        if len(row) < 10:
            continue
        fwd_row = fwd_4h.loc[ts].dropna()
        quintile_size = max(1, len(row) // 5)
        sorted_coins = row.sort_values()
        bottom = sorted_coins.index[:quintile_size]
        top = sorted_coins.index[-quintile_size:]
        for c in top:
            if c in fwd_row:
                all_ret.append(fwd_row[c] - FEE_RT)
        for c in bottom:
            if c in fwd_row:
                all_ret.append(-fwd_row[c] - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S7: Cross-Sect Momentum 4h (long top/short bot Q)")


def signal_8_xsect_mean_reversion_24h(candles: pd.DataFrame) -> dict:
    """S8: Cross-sectional 24h mean reversion — short top quintile, long bottom quintile."""
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    ret_24h = close_pivot.pct_change(24)
    fwd_24h = ret_24h.shift(-24)

    all_ret = []
    for ts in ret_24h.index[24:]:
        row = ret_24h.loc[ts].dropna()
        if len(row) < 10:
            continue
        fwd_row = fwd_24h.loc[ts].dropna()
        quintile_size = max(1, len(row) // 5)
        sorted_coins = row.sort_values()
        bottom = sorted_coins.index[:quintile_size]
        top = sorted_coins.index[-quintile_size:]
        for c in top:
            if c in fwd_row:
                all_ret.append(-fwd_row[c] - FEE_RT)   # short leaders
        for c in bottom:
            if c in fwd_row:
                all_ret.append(fwd_row[c] - FEE_RT)     # long laggards
    return signal_stats(pd.Series(all_ret), "S8: Cross-Sect Mean Reversion 24h (fade extremes)")


def signal_9_intraday_seasonality(candles: pd.DataFrame) -> dict:
    """S9: Trade the best hour-of-day for each coin."""
    df = candles.copy()
    df['hour'] = df['timestamp_utc'].dt.hour
    df = df.sort_values(['coin', 'timestamp_utc'])
    df['ret_1h'] = df.groupby('coin')['close'].pct_change()

    # Use first 80% of data to estimate best hour, last 20% to test
    cutoff = df['timestamp_utc'].quantile(0.8)
    train = df[df['timestamp_utc'] <= cutoff]
    test  = df[df['timestamp_utc'] > cutoff]

    # Best hour per coin by mean return
    best_hour = (train.groupby(['coin', 'hour'])['ret_1h'].mean()
                 .reset_index().sort_values('ret_1h', ascending=False)
                 .drop_duplicates('coin').set_index('coin')['hour'])

    all_ret = []
    for coin, grp in test.groupby('coin'):
        if coin not in best_hour:
            continue
        bh = best_hour[coin]
        hits = grp[grp['hour'] == bh]['ret_1h'].dropna()
        for r in hits:
            all_ret.append(r - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S9: Intraday Seasonality Best Hour")


def signal_10_dispersion_fade(candles: pd.DataFrame) -> dict:
    """S10: High cross-coin dispersion → fade z-score extremes next 4h."""
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    ret_4h = close_pivot.pct_change(4)
    fwd_4h = ret_4h.shift(-4)

    all_ret = []
    for ts in ret_4h.index[4:]:
        row = ret_4h.loc[ts].dropna()
        if len(row) < 10:
            continue
        dispersion = row.std()
        if dispersion == 0:
            continue
        # Only trade when dispersion is high (> 1 std of rolling dispersion)
        # Use z-scores directly
        z = (row - row.mean()) / dispersion
        fwd_row = fwd_4h.loc[ts].dropna()
        for coin, z_val in z.items():
            if coin not in fwd_row:
                continue
            if abs(z_val) > 2.0:   # extreme outlier
                signal = -np.sign(z_val)
                all_ret.append(signal * fwd_row[coin] - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S10: Dispersion Fade (z>2 extremes, 4h)")


def signal_11_residual_momentum(candles: pd.DataFrame) -> dict:
    """S11: Regress each coin on BTC 4h return; trade positive residual coins."""
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    ret_4h = close_pivot.pct_change(4)
    fwd_4h = ret_4h.shift(-4)

    if 'BTC' not in ret_4h.columns:
        return signal_stats(pd.Series([]), "S11: Residual Momentum (BTC β adj)")

    all_ret = []
    # Rolling 30-bar window for beta estimation
    window = 48   # 2 days
    for i in range(window, len(ret_4h)):
        ts = ret_4h.index[i]
        btc = ret_4h.iloc[i-window:i]['BTC'].dropna()
        if len(btc) < 10:
            continue
        fwd_row = fwd_4h.loc[ts].dropna()
        row = ret_4h.loc[ts].dropna()
        btc_cur = row.get('BTC', np.nan)
        if pd.isna(btc_cur):
            continue

        residuals = {}
        for coin in row.index:
            if coin == 'BTC':
                continue
            coin_hist = ret_4h.iloc[i-window:i][coin].dropna()
            if len(coin_hist) < 10:
                continue
            aligned = pd.concat([btc, coin_hist], axis=1).dropna()
            if len(aligned) < 5:
                continue
            b, a, _, _, _ = stats.linregress(aligned.iloc[:, 0], aligned.iloc[:, 1])
            resid = row[coin] - (a + b * btc_cur)
            residuals[coin] = resid

        if not residuals:
            continue
        resid_s = pd.Series(residuals)
        q_top = resid_s.quantile(0.8)
        q_bot = resid_s.quantile(0.2)
        for coin, resid in resid_s.items():
            if coin not in fwd_row:
                continue
            if resid > q_top:
                all_ret.append(fwd_row[coin] - FEE_RT)
            elif resid < q_bot:
                all_ret.append(-fwd_row[coin] - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S11: Residual Momentum (BTC β adj, 4h)")


def signal_12_vol_breakout(candles: pd.DataFrame) -> dict:
    """S12: 1h realized vol > 2x 20-day avg → momentum trade in direction of move."""
    df = candles.copy().sort_values(['coin', 'timestamp_utc'])
    df['ret'] = df.groupby('coin')['close'].pct_change()

    all_ret = []
    for coin, grp in df.groupby('coin'):
        grp = grp.copy().sort_values('timestamp_utc').reset_index(drop=True)
        if len(grp) < 500:
            continue
        grp['vol_1h'] = grp['ret'].abs()
        grp['vol_20d_avg'] = grp['vol_1h'].rolling(480, min_periods=100).mean()
        grp['vol_ratio'] = grp['vol_1h'] / (grp['vol_20d_avg'] + 1e-10)
        grp['fwd_ret'] = grp['ret'].shift(-4)
        grp['direction'] = np.sign(grp['ret'])
        breakout = grp[grp['vol_ratio'] > 2.0]
        for _, row in breakout.iterrows():
            if pd.notna(row['fwd_ret']) and row['direction'] != 0:
                all_ret.append(row['direction'] * row['fwd_ret'] - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S12: Vol Breakout Momentum (4h fwd)")


def signal_13_day_of_week(candles: pd.DataFrame) -> dict:
    """S13: Best day-of-week effect per coin — train/test split."""
    df = candles.copy()
    df['dow'] = df['timestamp_utc'].dt.dayofweek
    df = df.sort_values(['coin', 'timestamp_utc'])
    # Use 1h return as outcome
    df['ret_1h'] = df.groupby('coin')['close'].pct_change()

    cutoff = df['timestamp_utc'].quantile(0.8)
    train = df[df['timestamp_utc'] <= cutoff]
    test  = df[df['timestamp_utc'] > cutoff]

    best_dow = (train.groupby(['coin', 'dow'])['ret_1h'].mean()
                .reset_index().sort_values('ret_1h', ascending=False)
                .drop_duplicates('coin').set_index('coin')['dow'])

    all_ret = []
    for coin, grp in test.groupby('coin'):
        if coin not in best_dow:
            continue
        bd = best_dow[coin]
        hits = grp[grp['dow'] == bd]['ret_1h'].dropna()
        for r in hits:
            all_ret.append(r - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S13: Day-of-Week Best Day Effect")


def signal_14_funding_price_divergence(candles: pd.DataFrame, funding: pd.DataFrame) -> dict:
    """S14: Funding direction disagrees with 24h price direction → mean-reversion."""
    close_pivot = candles.pivot_table(index='timestamp_utc', columns='coin', values='close').sort_index()
    ret_24h = close_pivot.pct_change(24)
    fwd_24h = ret_24h.shift(-24)

    # Aggregate funding to 1h per coin (mean of all rates in that hour)
    funding = funding.copy()
    funding['ts_1h'] = pd.to_datetime(funding['timestamp_utc'], utc=True, errors='coerce')
    funding['ts_1h'] = funding['ts_1h'].dt.floor('1h')
    fund_1h = funding.groupby(['coin', 'ts_1h'])['funding_rate'].mean().reset_index()
    fund_pivot = fund_1h.pivot_table(index='ts_1h', columns='coin', values='funding_rate')
    fund_sign = np.sign(fund_pivot.rolling(24).mean())  # 24h avg funding direction

    all_ret = []
    for ts in ret_24h.index[24:]:
        if ts not in fund_sign.index:
            continue
        price_dir = np.sign(ret_24h.loc[ts].dropna())
        f_dir = fund_sign.loc[ts].dropna()
        fwd_row = fwd_24h.loc[ts].dropna()
        # Divergence: funding positive (longs paying) but price fell
        for coin in price_dir.index:
            if coin not in f_dir or coin not in fwd_row:
                continue
            pd_val = price_dir[coin]
            fd_val = f_dir[coin]
            if pd.isna(pd_val) or pd.isna(fd_val) or pd_val == 0 or fd_val == 0:
                continue
            if pd_val != fd_val:  # divergence
                # Contrarian: price went against funding → revert toward funding direction
                signal = fd_val
                all_ret.append(signal * fwd_row[coin] - FEE_RT)
    return signal_stats(pd.Series(all_ret), "S14: Funding-Price Divergence (24h fwd)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*70)
    print("SYSTEMATIC EDA: 14 SIGNAL HYPOTHESES")
    print("="*70)

    # --- Load data ---
    whale_data = load_whale_data()
    candles    = load_candles()
    funding    = load_funding()

    # --- Build whale snapshots ---
    print("Building whale snapshots …", end=' ')
    snap = build_whale_snapshots(whale_data)
    print(f"{len(snap):,} rows | {snap['ts_bin'].min().date()} → {snap['ts_bin'].max().date()}")

    # --- Run signals ---
    results = []
    SIGNALS = [
        ("WHALE", [
            ("S1", signal_1_whale_net_delta,          (snap, candles)),
            ("S2", signal_2_whale_consensus_flip,     (snap, candles)),
            ("S3", signal_3_whale_leverage_change,    (snap, candles)),
            ("S4", signal_4_whale_pnl_sentiment,      (snap, candles)),
            ("S5", signal_5_top_whale_copy,           (whale_data, candles)),
            ("S6", signal_6_whale_hhi,                (snap, candles)),
        ]),
        ("CROSS-SECTIONAL", [
            ("S7",  signal_7_xsect_momentum_4h,       (candles,)),
            ("S8",  signal_8_xsect_mean_reversion_24h,(candles,)),
            ("S9",  signal_9_intraday_seasonality,     (candles,)),
            ("S10", signal_10_dispersion_fade,         (candles,)),
            ("S11", signal_11_residual_momentum,       (candles,)),
            ("S12", signal_12_vol_breakout,            (candles,)),
            ("S13", signal_13_day_of_week,             (candles,)),
            ("S14", signal_14_funding_price_divergence,(candles, funding)),
        ]),
    ]

    for group, sigs in SIGNALS:
        print(f"\n{'─'*70}")
        print(f"  GROUP: {group}")
        print(f"{'─'*70}")
        for sid, fn, args in sigs:
            print(f"  Running {sid} …", end=' ', flush=True)
            try:
                res = fn(*args)
                results.append(res)
                n = res['n_trades']
                mr = res['mean_return_bps']
                t  = res['t_stat']
                p  = res['p_value']
                print(f"n={n:>6} | mean={mr:>7.2f}bps | t={t:>6.3f} | p={p:.4f}")
            except Exception as e:
                print(f"ERROR: {e}")
                results.append({"signal": sid, "n_trades": 0, "mean_return_bps": np.nan,
                                "t_stat": np.nan, "p_value": np.nan})

    # ── SUMMARY TABLE ──────────────────────────────────────────────────────────
    df_res = pd.DataFrame(results)
    df_res['significant_5pct'] = df_res['p_value'] < 0.05
    df_res['significant_10pct'] = df_res['p_value'] < 0.10
    df_res = df_res.sort_values('p_value')

    print("\n\n" + "="*90)
    print("SUMMARY TABLE (sorted by p-value)")
    print("="*90)
    print(f"{'Signal':<55} {'N':>7} {'Mean(bps)':>10} {'t-stat':>8} {'p-val':>8} {'Sharpe':>8} {'WinRate':>8} {'Sig?'}")
    print("─"*90)
    for _, row in df_res.iterrows():
        sig_flag = "**" if row.get('significant_5pct') else ("*" if row.get('significant_10pct') else "")
        n  = int(row['n_trades']) if pd.notna(row['n_trades']) else 0
        mr = f"{row['mean_return_bps']:.2f}" if pd.notna(row['mean_return_bps']) else "N/A"
        t  = f"{row['t_stat']:.3f}" if pd.notna(row['t_stat']) else "N/A"
        p  = f"{row['p_value']:.4f}" if pd.notna(row['p_value']) else "N/A"
        sr = f"{row.get('sharpe_annualized', np.nan):.2f}" if pd.notna(row.get('sharpe_annualized', np.nan)) else "N/A"
        wr = f"{row.get('win_rate_pct', np.nan):.1f}%" if pd.notna(row.get('win_rate_pct', np.nan)) else "N/A"
        print(f"{row['signal']:<55} {n:>7} {mr:>10} {t:>8} {p:>8} {sr:>8} {wr:>8} {sig_flag}")

    print("─"*90)
    print("* p<0.10   ** p<0.05")

    sig_signals = df_res[df_res['significant_5pct'] == True]
    print(f"\nSignificant at 5%: {len(sig_signals)}/{len(df_res)} signals")

    # ── INTERPRETATION ─────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("INTERPRETATION & CAVEATS")
    print("="*70)
    print("""
WHALE SIGNALS (S1–S6):
  - Whale data spans only 11 days (Apr 21 – May 2, 2026)
  - Snapshots are irregular ~15min intervals (not clean hourly)
  - Very limited sample: most whale signals will have <200 trades total
  - Any 'significant' whale result should be treated as a hypothesis,
    NOT a deployable edge. Need ≥6 months of whale data to validate.

CROSS-SECTIONAL SIGNALS (S7–S14):
  - Candles: ~1yr of data, 59 coins, 1h bars (~8,760 per coin max)
  - S11 (Residual Momentum) is compute-heavy due to rolling OLS
  - S9/S13 use train/test split (80/20) to avoid pure in-sample overfitting
  - All returns are gross-of-3bp fees. Actual slippage on HL is typically
    1–3bp, so net edge must survive 3–6bp total costs for perps.
  - Cross-sectional signals assume simultaneous execution across coins
    (portfolio approach), not single-coin deployment.

NEXT STEPS FOR PROMISING SIGNALS (p < 0.10, mean > 0bps):
  1. Walk-forward validation (expand-window or rolling-window)
  2. Subperiod stability analysis (quarter by quarter)
  3. Capacity check: how much notional can the signal absorb?
  4. Build full controller via research-process skill
""")
