"""
Systematic EDA: 14 Signal Hypotheses
Options (Deribit), Macro (VIX/DXY/SPX), DeFi (TVL/Stablecoins), Sentiment (Fear & Greed)

Options data: ~24 days (Apr 12 – May 6 2026)
Fear & Greed: ~8 years (Feb 2018 – May 2026) — the deep dataset
Macro: ~2 years (May 2024 – May 2026)
DVOL: 1 year (May 2025 – May 2026)
HL candles 1h: ~7 months (Sep 2025 – May 2026)
HL funding: ~3 years (May 2023 – May 2026)

Fee assumption: 3 bps per side = 6 bps round-trip on BTC/ETH HL
Outcome: BTC forward return (1h, 4h, 24h depending on signal)
"""

import os, sys, warnings
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy import stats
from pymongo import MongoClient
import pymongo

warnings.filterwarnings("ignore")

# ── Connection ────────────────────────────────────────────────────────────────
client = MongoClient(os.environ["MONGO_URI"])
db = client[os.environ["MONGO_DATABASE"]]

FEES_RT = 0.0006  # 6 bps round trip

# ── Utilities ─────────────────────────────────────────────────────────────────
def ts_ms_to_dt(ts):
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=None)
    if isinstance(ts, (int, float)):
        if ts > 1e12:
            return datetime.utcfromtimestamp(ts / 1000)
        return datetime.utcfromtimestamp(ts)
    return ts

def evaluate_signal(returns_pct, fees_rt=FEES_RT, signal_name=""):
    """Given a series of forward returns (%), compute stats."""
    r = np.array(returns_pct)
    n = len(r)
    if n < 5:
        return dict(n_trades=n, mean_return=np.nan, t_stat=np.nan, p_value=np.nan,
                    sharpe=np.nan, win_rate=np.nan, net_return=np.nan, note="TOO FEW")
    net = r - fees_rt * 100  # fees in pct
    mean_net = np.mean(net)
    std = np.std(net, ddof=1)
    t, p = stats.ttest_1samp(net, 0)
    sharpe = mean_net / std * np.sqrt(252) if std > 0 else np.nan
    win_rate = np.mean(net > 0)
    return dict(n_trades=n, mean_return=np.mean(r), net_return=mean_net,
                t_stat=t, p_value=p, sharpe=sharpe, win_rate=win_rate)

results = {}  # signal_name -> stats dict

print("=" * 70)
print("SYSTEMATIC EDA — 14 SIGNAL HYPOTHESES")
print("=" * 70)

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

print("\n[1/5] Loading HL BTC 1h candles...")
hl_raw = list(db.hyperliquid_candles_1h.find(
    {"coin": "BTC"},
    {"timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "_id": 0},
    sort=[("timestamp_utc", pymongo.ASCENDING)]
))
hl = pd.DataFrame(hl_raw)
hl["dt"] = hl["timestamp_utc"].apply(ts_ms_to_dt)
hl = hl.sort_values("dt").set_index("dt")
hl = hl[~hl.index.duplicated(keep="last")]
# forward returns (%)
hl["fwd_1h"] = hl["close"].pct_change(1).shift(-1) * 100
hl["fwd_4h"] = hl["close"].pct_change(4).shift(-4) * 100
hl["fwd_24h"] = hl["close"].pct_change(24).shift(-24) * 100
print(f"  HL BTC 1h: {len(hl)} bars, {hl.index[0].date()} – {hl.index[-1].date()}")

print("[2/5] Loading Fear & Greed...")
fg_raw = list(db.fear_greed_index.find(
    {}, {"timestamp_utc": 1, "value": 1, "classification": 1, "_id": 0}
))
fg = pd.DataFrame(fg_raw)
fg["dt"] = fg["timestamp_utc"].apply(ts_ms_to_dt).dt.normalize()
fg = fg.sort_values("dt").drop_duplicates("dt").set_index("dt")
fg["value"] = pd.to_numeric(fg["value"], errors="coerce")
fg["fg_chg3d"] = fg["value"].diff(3)  # 3-day momentum
fg["fg_chg1d"] = fg["value"].diff(1)
# Daily BTC return from HL (or use close-to-close)
hl_daily = hl["close"].resample("1D").last()
hl_daily_ret = hl_daily.pct_change(1).shift(-1) * 100  # next-day return
fg["btc_fwd_1d"] = fg.index.map(hl_daily_ret)
# For years before HL (pre-Sep 2025), we won't have HL data — that's ok, NaN will be dropped
print(f"  Fear & Greed: {len(fg)} daily obs, {fg.index[0].date()} – {fg.index[-1].date()}")
print(f"  Fear & Greed WITH HL overlap: {fg['btc_fwd_1d'].notna().sum()} days")

print("[3/5] Loading Macro data (VIX, DXY, SPX)...")
macro_raw = list(db.macro_data.find(
    {}, {"ticker": 1, "date": 1, "close": 1, "open": 1, "_id": 0}
))
macro = pd.DataFrame(macro_raw)
macro["dt"] = pd.to_datetime(macro["date"]).dt.normalize()
macro_wide = macro.pivot_table(index="dt", columns="ticker", values="close", aggfunc="last")
macro_wide = macro_wide.sort_index()
# Daily changes
macro_wide["vix_chg"] = macro_wide["VIX"].diff(1)
macro_wide["vix_chg_z"] = (macro_wide["vix_chg"] - macro_wide["vix_chg"].rolling(60).mean()) / macro_wide["vix_chg"].rolling(60).std()
macro_wide["dxy_7d_chg"] = macro_wide["DXY"].pct_change(5) * 100  # ~1 week (5 trading days)
macro_wide["dxy_1d_chg"] = macro_wide["DXY"].pct_change(1) * 100
macro_wide["btc_fwd_1d"] = macro_wide.index.map(hl_daily_ret)
macro_wide["btc_fwd_2d"] = macro_wide.index.map(
    hl_daily.pct_change(2).shift(-2) * 100
)
print(f"  Macro: {len(macro_wide)} days, {macro_wide.index[0].date()} – {macro_wide.index[-1].date()}")

print("[4/5] Loading DeFi data (TVL, Stablecoins)...")
tvl_raw = list(db.defillama_tvl.find({}, {"date": 1, "tvl_usd": 1, "_id": 0}))
tvl = pd.DataFrame(tvl_raw)
tvl["dt"] = tvl["date"].apply(ts_ms_to_dt).pipe(pd.to_datetime).dt.normalize()
tvl = tvl.sort_values("dt").drop_duplicates("dt").set_index("dt")
tvl["tvl_7d_chg"] = tvl["tvl_usd"].pct_change(7) * 100

stable_raw = list(db.defillama_stablecoins.find({}, {"date": 1, "circulating_usd": 1, "symbol": 1, "_id": 0}))
stable = pd.DataFrame(stable_raw)
stable["dt"] = stable["date"].apply(ts_ms_to_dt).pipe(pd.to_datetime).dt.normalize()
stable_agg = stable.groupby("dt")["circulating_usd"].sum().reset_index().set_index("dt")
stable_agg = stable_agg.sort_index()
stable_agg["stable_1d_chg"] = stable_agg["circulating_usd"].diff(1)
stable_agg["stable_1d_chg_pct"] = stable_agg["circulating_usd"].pct_change(1) * 100
stable_agg["btc_fwd_1d"] = stable_agg.index.map(hl_daily_ret)
tvl["btc_fwd_1d"] = tvl.index.map(hl_daily_ret)
print(f"  TVL: {len(tvl)} days | Stablecoins: {len(stable_agg)} days")

print("[5/5] Loading Options surface + DVOL...")
# Only 24 days — load all BTC ATM data
print("  Loading options (BTC, all snapshots)...")
# Get distinct timestamps first (15-min snapshots)
opts_raw = list(db.deribit_options_surface.find(
    {"currency": "BTC", "mark_iv": {"$gt": 0}},
    {"timestamp_utc": 1, "strike": 1, "expiry": 1, "type": 1,
     "mark_iv": 1, "open_interest": 1, "volume_24h": 1,
     "underlying_price": 1, "mid_price": 1, "_id": 0},
    sort=[("timestamp_utc", pymongo.ASCENDING)]
))
opts = pd.DataFrame(opts_raw)
opts["dt"] = opts["timestamp_utc"].apply(ts_ms_to_dt)
opts = opts.sort_values("dt")
print(f"  Options: {len(opts):,} docs, {opts['dt'].min()} – {opts['dt'].max()}")

# DVOL (hourly, 1 year)
dvol_raw = list(db.deribit_dvol.find(
    {"currency": "BTC"},
    {"timestamp_utc": 1, "dvol_close": 1, "_id": 0},
    sort=[("timestamp_utc", pymongo.ASCENDING)]
))
dvol = pd.DataFrame(dvol_raw)
dvol["dt"] = dvol["timestamp_utc"].apply(ts_ms_to_dt)
dvol = dvol.sort_values("dt").drop_duplicates("dt").set_index("dt")
dvol["dvol_chg_1h"] = dvol["dvol_close"].diff(1)
dvol["dvol_rv_spread"] = np.nan  # will compute below
# Realized vol (24h rolling from HL 1h returns)
hl["log_ret"] = np.log(hl["close"] / hl["close"].shift(1))
hl["rv_24h"] = hl["log_ret"].rolling(24).std() * np.sqrt(8760) * 100  # annualized %
# Map RV onto DVOL index
dvol["rv_24h"] = dvol.index.map(hl["rv_24h"])
dvol["dvol_rv_spread"] = dvol["dvol_close"] - dvol["rv_24h"]  # IV - RV
# fwd returns for DVOL
dvol["fwd_4h"] = dvol.index.map(hl["fwd_4h"])
dvol["fwd_1h"] = dvol.index.map(hl["fwd_1h"])
print(f"  DVOL: {len(dvol)} hourly obs")

# HL funding for BTC (for Signal 10)
fund_raw = list(db.hyperliquid_funding_rates.find(
    {"coin": "BTC"},
    {"timestamp_utc": 1, "funding_rate": 1, "_id": 0},
    sort=[("timestamp_utc", pymongo.ASCENDING)]
))
fund = pd.DataFrame(fund_raw)
fund["dt"] = fund["timestamp_utc"].apply(ts_ms_to_dt).dt.normalize()
fund_daily = fund.groupby("dt")["funding_rate"].mean().reset_index().set_index("dt")
fund_daily.columns = ["btc_funding_mean"]
print(f"  BTC HL funding: {len(fund_daily)} daily obs")

print("\nAll data loaded. Running signals...\n")

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONS SIGNALS (Deribit, ~24 days)
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 50)
print("OPTIONS SIGNALS (24-day window — low N, directional only)")
print("=" * 50)

# ── Helper: build ATM snapshot time series ────────────────────────────────────
def get_atm_iv_series(opts_df, currency="BTC", days_to_expiry_min=7, days_to_expiry_max=45):
    """For each snapshot timestamp, find near-ATM options in 7-45 DTE window."""
    records = []
    for ts, grp in opts_df.groupby("dt"):
        # Parse expiry dates
        underlying = grp["underlying_price"].median()
        if pd.isna(underlying) or underlying <= 0:
            continue
        # Filter by DTE approximation (expiry field is string like '10MAY26')
        def parse_expiry(s):
            try:
                return datetime.strptime(s, "%d%b%y")
            except:
                try:
                    return datetime.strptime(s, "%d%b%Y")
                except:
                    return None
        grp = grp.copy()
        grp["expiry_dt"] = grp["expiry"].apply(parse_expiry)
        grp["dte"] = (grp["expiry_dt"] - ts).dt.total_seconds() / 86400
        front = grp[(grp["dte"] >= days_to_expiry_min) & (grp["dte"] <= days_to_expiry_max)]
        if front.empty:
            continue
        # ATM: strike closest to underlying
        front = front.copy()
        front["moneyness"] = (front["strike"] / underlying - 1).abs()
        atm = front.nsmallest(10, "moneyness")
        calls = atm[atm["type"] == "call"]["mark_iv"]
        puts = atm[atm["type"] == "put"]["mark_iv"]
        atm_iv = atm["mark_iv"].mean()
        call_iv = calls.mean()
        put_iv = puts.mean()
        # 25d put skew approximation: deep OTM puts vs calls
        # Without delta, use moneyness buckets: puts at 0.8-0.95 moneyness from ATM
        deep_puts = front[(front["type"] == "put") & (front["moneyness"].between(0.05, 0.25))]["mark_iv"]
        deep_calls = front[(front["type"] == "call") & (front["moneyness"].between(0.05, 0.25))]["mark_iv"]
        skew_25d = deep_puts.mean() - deep_calls.mean() if (len(deep_puts) > 0 and len(deep_calls) > 0) else np.nan
        # Put/call OI ratio
        pc_oi = grp[grp["type"] == "put"]["open_interest"].sum() / (grp[grp["type"] == "call"]["open_interest"].sum() + 1e-6)
        records.append({
            "dt": ts,
            "atm_iv": atm_iv,
            "call_iv": call_iv,
            "put_iv": put_iv,
            "skew_25d": skew_25d,
            "pc_oi_ratio": pc_oi,
            "underlying": underlying,
        })
    df = pd.DataFrame(records).set_index("dt")
    df = df.sort_index()
    return df

print("\nBuilding ATM IV time series from options surface...")
opts_btc = opts[opts["timestamp_utc"].notna()].copy()
# Parse expiry dates once
def parse_expiry(s):
    try:
        return datetime.strptime(s, "%d%b%y")
    except:
        try:
            return datetime.strptime(s, "%d%b%Y")
        except:
            return None

opts_btc["expiry_dt"] = opts_btc["expiry"].apply(parse_expiry)
opts_btc["dte"] = (opts_btc["expiry_dt"] - opts_btc["dt"]).dt.total_seconds() / 86400

# Work in 15-min snapshots
atm_records = []
for ts, grp in opts_btc.groupby("dt"):
    underlying = grp["underlying_price"].median()
    if pd.isna(underlying) or underlying <= 0:
        continue
    front = grp[(grp["dte"] >= 7) & (grp["dte"] <= 45)].copy()
    if front.empty:
        continue
    front["moneyness_abs"] = (front["strike"] / underlying - 1).abs()
    atm = front.nsmallest(10, "moneyness_abs")
    calls_atm = atm[atm["type"] == "call"]["mark_iv"]
    puts_atm = atm[atm["type"] == "put"]["mark_iv"]

    # Skew: OTM puts (0.05-0.25 moneyness) vs OTM calls
    otm_puts = front[(front["type"] == "put") & (front["moneyness_abs"].between(0.05, 0.25))]["mark_iv"]
    otm_calls = front[(front["type"] == "call") & (front["moneyness_abs"].between(0.05, 0.25))]["mark_iv"]
    skew = otm_puts.mean() - otm_calls.mean() if (len(otm_puts) > 1 and len(otm_calls) > 1) else np.nan

    # Term structure: short <14d vs long 30-60d
    short_iv = front[(front["dte"] < 14)]["mark_iv"].mean()
    long_iv = front[(front["dte"].between(30, 60))]["mark_iv"].mean()
    term_ratio = short_iv / long_iv if (long_iv > 0 and not pd.isna(short_iv)) else np.nan

    pc_oi = grp[grp["type"] == "put"]["open_interest"].sum() / (grp[grp["type"] == "call"]["open_interest"].sum() + 1e-6)
    pc_vol = grp[grp["type"] == "put"]["volume_24h"].sum() / (grp[grp["type"] == "call"]["volume_24h"].sum() + 1e-6)

    # Gamma exposure (sign convention: positive = long gamma / price pinning)
    # We don't have delta/gamma from Deribit so approximate via OI-weighted IV
    # High OI near ATM with low IV → dealers likely short gamma → explosive risk
    atm_oi = atm["open_interest"].sum()

    atm_records.append({
        "dt": ts,
        "atm_iv": atm["mark_iv"].mean(),
        "call_atm_iv": calls_atm.mean(),
        "put_atm_iv": puts_atm.mean(),
        "skew_25d": skew,
        "term_ratio": term_ratio,
        "pc_oi_ratio": pc_oi,
        "pc_vol_ratio": pc_vol,
        "atm_oi": atm_oi,
        "underlying": underlying,
    })

atm_ts = pd.DataFrame(atm_records).set_index("dt").sort_index()
# Resample to 1h (take last in each hour)
atm_1h = atm_ts.resample("1h").last().ffill(limit=2)
atm_1h["atm_iv_chg_1h"] = atm_1h["atm_iv"].diff(1)
atm_1h["skew_chg_4h"] = atm_1h["skew_25d"].diff(4)
# Merge HL BTC fwd returns
atm_1h["fwd_1h"] = atm_1h.index.map(hl["fwd_1h"])
atm_1h["fwd_4h"] = atm_1h.index.map(hl["fwd_4h"])
print(f"ATM 1h series: {len(atm_1h)} rows, {atm_1h['atm_iv'].notna().sum()} with ATM IV")

# ── SIGNAL 1: ATM IV Change Velocity ─────────────────────────────────────────
print("\n--- Signal 1: ATM IV Change Velocity (>2 vol pts / 1h) ---")
# IV spike → anticipate BOTH directions → we go long absolute move
# Test: if IV spikes, does BTC move more? Proxy: take abs(fwd_4h) vs baseline
df1 = atm_1h.dropna(subset=["atm_iv_chg_1h", "fwd_4h"])
threshold = 2.0  # vol points change
triggered = df1[df1["atm_iv_chg_1h"].abs() > threshold]
baseline = df1[df1["atm_iv_chg_1h"].abs() <= threshold]
# For a long vol proxy: long direction = sign of IV change (rising IV = expect up OR down)
# We'll test: long when IV rising (positive IV chg → long BTC), short when falling
signal_ret1 = triggered["fwd_4h"] * np.sign(triggered["atm_iv_chg_1h"])
stats1 = evaluate_signal(signal_ret1.dropna().values)
stats1["note"] = f"Triggered={len(triggered)}, Baseline_mean={baseline['fwd_4h'].mean():.3f}%"
stats1["window"] = "24d options only"
results["S01_ATM_IV_velocity"] = stats1
print(f"  Triggered: {len(triggered)}, Signal returns mean={signal_ret1.mean():.3f}%")

# ── SIGNAL 2: Put-Call Skew (absolute level) ──────────────────────────────────
print("--- Signal 2: Put-Call Skew Absolute Level ---")
df2 = atm_1h.dropna(subset=["skew_25d", "fwd_4h"])
# Skew > 0 means puts more expensive → bearish → SHORT signal
# We add cooldown by sampling every 4h
df2_4h = df2.resample("4h").last().dropna(subset=["skew_25d", "fwd_4h"])
# High positive skew → bearish → short → return = -fwd_4h
skew_median = df2_4h["skew_25d"].median()
bearish_skew = df2_4h[df2_4h["skew_25d"] > skew_median + 0.02]  # above median + 2pp
signal_ret2 = -bearish_skew["fwd_4h"]  # short signal
stats2 = evaluate_signal(signal_ret2.dropna().values)
stats2["note"] = f"Median skew={skew_median:.4f}, n_triggered={len(bearish_skew)}"
stats2["window"] = "24d options only"
results["S02_put_call_skew_level"] = stats2
print(f"  Bearish skew triggers: {len(bearish_skew)}, Signal mean={signal_ret2.mean():.3f}%")

# ── SIGNAL 3: Skew Change (4h momentum) ──────────────────────────────────────
print("--- Signal 3: Skew Change 4h (rapid bearish skew) ---")
df3 = atm_1h.dropna(subset=["skew_chg_4h", "fwd_4h"])
df3_4h = df3.resample("4h").last().dropna(subset=["skew_chg_4h", "fwd_4h"])
# Rapidly rising skew (puts getting more expensive) → bearish
skew_chg_thresh = df3_4h["skew_chg_4h"].quantile(0.75)
rapid_bear = df3_4h[df3_4h["skew_chg_4h"] > skew_chg_thresh]
signal_ret3 = -rapid_bear["fwd_4h"]  # short
stats3 = evaluate_signal(signal_ret3.dropna().values)
stats3["note"] = f"75th skew chg thresh={skew_chg_thresh:.4f}, n={len(rapid_bear)}"
stats3["window"] = "24d options only"
results["S03_skew_change_4h"] = stats3
print(f"  Rapid bearish triggers: {len(rapid_bear)}, Signal mean={signal_ret3.mean():.3f}%")

# ── SIGNAL 4: Term Structure Inversion ────────────────────────────────────────
print("--- Signal 4: Term Structure Inversion (7d IV > 30d IV) ---")
df4 = atm_1h.dropna(subset=["term_ratio", "fwd_4h"])
df4_4h = df4.resample("4h").last().dropna(subset=["term_ratio", "fwd_4h"])
# term_ratio > 1.05 = front-heavy = event risk → expect mean reversion after
inverted = df4_4h[df4_4h["term_ratio"] > 1.05]
# Mean reversion: after inversion, go AGAINST the recent move (neutral signal → long)
# Test: inversion followed by bounce → long BTC
signal_ret4 = inverted["fwd_4h"]  # long
stats4 = evaluate_signal(signal_ret4.dropna().values)
stats4["note"] = f"Inverted (>1.05): {len(inverted)}/{len(df4_4h)} snaps"
stats4["window"] = "24d options only"
results["S04_term_structure_inversion"] = stats4
print(f"  Inversions: {len(inverted)}, Signal mean={signal_ret4.mean():.3f}%")

# ── SIGNAL 5: IV-RV Spread (sell vol proxy) ───────────────────────────────────
print("--- Signal 5: IV-RV Spread (DVOL, 1yr) ---")
df5 = dvol.dropna(subset=["dvol_rv_spread", "fwd_4h"])
df5_4h = df5.resample("4h").last().dropna(subset=["dvol_rv_spread", "fwd_4h"])
spread_75 = df5_4h["dvol_rv_spread"].quantile(0.75)
spread_25 = df5_4h["dvol_rv_spread"].quantile(0.25)
# IV >> RV (high spread) → sell vol proxy → expect prices to be calm → short volatility proxy
# Translate to directional: no strong prior, but high IV/RV gap often precedes calm
# Use contrarian: when IV >> RV, mean reversion → long (calm market drifts up)
high_iv_rv = df5_4h[df5_4h["dvol_rv_spread"] > spread_75]
low_iv_rv = df5_4h[df5_4h["dvol_rv_spread"] < spread_25]
# High spread → long (calm expected)
signal_ret5a = high_iv_rv["fwd_4h"]
stats5a = evaluate_signal(signal_ret5a.dropna().values)
stats5a["note"] = f"IV>>RV (>75th={spread_75:.1f}), n={len(high_iv_rv)}"
stats5a["window"] = "1yr DVOL"
results["S05a_iv_rv_spread_high"] = stats5a

# Low spread → short (RV>IV, trending/noisy market → potential continuation)
signal_ret5b = -low_iv_rv["fwd_4h"]  # short when RV > IV
stats5b = evaluate_signal(signal_ret5b.dropna().values)
stats5b["note"] = f"RV>>IV (<25th={spread_25:.1f}), n={len(low_iv_rv)}"
stats5b["window"] = "1yr DVOL"
results["S05b_iv_rv_spread_low"] = stats5b
print(f"  High IV/RV: n={len(high_iv_rv)}, Low: n={len(low_iv_rv)}")

# ── SIGNAL 6: Put Volume Spike ────────────────────────────────────────────────
print("--- Signal 6: Put Volume Spike (P/C volume ratio) ---")
df6 = atm_1h.dropna(subset=["pc_vol_ratio", "fwd_4h"])
df6_4h = df6.resample("4h").last().dropna(subset=["pc_vol_ratio", "fwd_4h"])
pc_vol_thresh = df6_4h["pc_vol_ratio"].quantile(0.80)
put_vol_spike = df6_4h[df6_4h["pc_vol_ratio"] > pc_vol_thresh]
# Put vol spike → institutional hedging → bearish
signal_ret6 = -put_vol_spike["fwd_4h"]  # short
stats6 = evaluate_signal(signal_ret6.dropna().values)
stats6["note"] = f"P/C vol >80th={pc_vol_thresh:.2f}, n={len(put_vol_spike)}"
stats6["window"] = "24d options only"
results["S06_put_volume_spike"] = stats6
print(f"  Put vol spikes: {len(put_vol_spike)}, Signal mean={signal_ret6.mean():.3f}%")

# ── SIGNAL 7: ATM OI (gamma pinning proxy) ───────────────────────────────────
print("--- Signal 7: ATM OI (Gamma Exposure proxy) ---")
df7 = atm_1h.dropna(subset=["atm_oi", "fwd_1h"]).copy()
df7["atm_oi_chg"] = df7["atm_oi"].diff(4)
df7_4h = df7.resample("4h").last().dropna(subset=["atm_oi_chg", "fwd_4h"])
# Rising ATM OI → dealers accumulate gamma → price pinning → lower vol
# Falling ATM OI → gamma unwind → explosive move
oi_chg_thresh = df7_4h["atm_oi_chg"].quantile(0.20)  # sharp OI decline
low_oi = df7_4h[df7_4h["atm_oi_chg"] < oi_chg_thresh]
# OI unwind → follow momentum (long abs move proxy — test as long signal)
signal_ret7 = low_oi["fwd_4h"].abs()  # proxy for vol
# Actually test as directional: OI unwind → follow recent direction
signal_ret7_dir = low_oi["fwd_4h"]
stats7 = evaluate_signal(signal_ret7_dir.dropna().values)
stats7["note"] = f"OI decline <20th={oi_chg_thresh:.0f}, n={len(low_oi)}"
stats7["window"] = "24d options only"
results["S07_atm_oi_unwind"] = stats7
print(f"  OI unwinds: {len(low_oi)}, Signal mean={signal_ret7_dir.mean():.3f}%")

# ── SIGNAL 8: Max Pain Migration ──────────────────────────────────────────────
print("--- Signal 8: Max Pain migration ---")
# Max pain = strike where total options (OI-weighted) have min value at expiry
# Need to compute per snapshot, per expiry
def compute_max_pain(grp_exp, underlying):
    """For a given expiry group, compute max pain strike."""
    strikes = sorted(grp_exp["strike"].unique())
    if len(strikes) < 3:
        return np.nan
    pain = []
    for S in strikes:
        calls = grp_exp[(grp_exp["type"] == "call") & (grp_exp["strike"] <= S)]
        puts = grp_exp[(grp_exp["type"] == "put") & (grp_exp["strike"] >= S)]
        call_pain = ((S - calls["strike"]) * calls["open_interest"]).sum()
        put_pain = ((puts["strike"] - S) * puts["open_interest"]).sum()
        pain.append(call_pain + put_pain)
    return strikes[np.argmin(pain)]

# Use front-month expiry only, sample every 4h
mp_records = []
opts_4h_sample = opts_btc.copy()
opts_4h_sample["dt_4h"] = opts_4h_sample["dt"].dt.floor("4h")
for (dt4h, expiry), grp_exp in opts_4h_sample.groupby(["dt_4h", "expiry"]):
    if grp_exp["dte"].median() < 3 or grp_exp["dte"].median() > 14:
        continue
    underlying = grp_exp["underlying_price"].median()
    if pd.isna(underlying):
        continue
    mp = compute_max_pain(grp_exp, underlying)
    mp_records.append({"dt": dt4h, "expiry": expiry, "max_pain": mp, "underlying": underlying})

if mp_records:
    mp_df = pd.DataFrame(mp_records)
    # Take front-month (min DTE) per snapshot
    mp_front = mp_df.sort_values("dt").groupby("dt").first()
    mp_front["mp_distance"] = (mp_front["max_pain"] / mp_front["underlying"] - 1) * 100
    mp_front["mp_chg"] = mp_front["max_pain"].diff(1)
    mp_front["fwd_4h"] = mp_front.index.map(hl["fwd_4h"])
    # Signal: price tends to gravitate to max pain → if price above max pain, go short
    mp_front_valid = mp_front.dropna(subset=["mp_distance", "fwd_4h"])
    above_pain = mp_front_valid[mp_front_valid["mp_distance"] < -2]  # underlying > max_pain by >2%
    below_pain = mp_front_valid[mp_front_valid["mp_distance"] > 2]   # underlying < max_pain
    signal_ret8a = -above_pain["fwd_4h"]  # short when above max pain
    signal_ret8b = below_pain["fwd_4h"]   # long when below max pain
    all_s8 = pd.concat([signal_ret8a, signal_ret8b])
    stats8 = evaluate_signal(all_s8.dropna().values)
    stats8["note"] = f"Above+below max pain (>2% distance), n={len(all_s8)}"
else:
    stats8 = evaluate_signal([])
    stats8["note"] = "No max pain data"
stats8["window"] = "24d options only"
results["S08_max_pain"] = stats8
print(f"  Max pain trades: {stats8['n_trades']}")

print("\n" + "=" * 50)
print("MACRO / SENTIMENT SIGNALS")
print("=" * 50)

# ── SIGNAL 9: Fear & Greed Momentum (contrarian) ─────────────────────────────
print("\n--- Signal 9: Fear & Greed 3-day momentum (contrarian) ---")
df9 = fg.dropna(subset=["fg_chg3d", "btc_fwd_1d"])
# Rapid fear increase (drop in F&G) → contrarian LONG
# Rapid greed increase → contrarian SHORT
fear_surge = df9[df9["fg_chg3d"] < -10]   # dropped 10+ pts in 3 days → contrarian long
greed_surge = df9[df9["fg_chg3d"] > 10]   # rose 10+ pts → contrarian short
signal_ret9a = fear_surge["btc_fwd_1d"]         # long
signal_ret9b = -greed_surge["btc_fwd_1d"]       # short
signal_ret9 = pd.concat([signal_ret9a, signal_ret9b])
stats9 = evaluate_signal(signal_ret9.dropna().values)
stats9["note"] = f"Fear drop>10: n={len(fear_surge)}, Greed>10: n={len(greed_surge)}, total={len(signal_ret9.dropna())}"
stats9["window"] = "HL overlap (~7m)"
results["S09_fear_greed_contrarian"] = stats9
print(f"  Fear surge: {len(fear_surge)}, Greed surge: {len(greed_surge)}")

# Also test with longer F&G history (pre-HL, use 0 as proxy? No — stick to overlap)
# But use just the fear signal alone with HL
fear_only = fear_surge.dropna(subset=["btc_fwd_1d"])
stats9_fear = evaluate_signal(fear_only["btc_fwd_1d"].values)
stats9_fear["note"] = f"Fear drop only, n={len(fear_only)}"
stats9_fear["window"] = "HL overlap (~7m)"
results["S09b_extreme_fear_long"] = stats9_fear
print(f"  Fear-only n={len(fear_only)}, mean={fear_only['btc_fwd_1d'].mean():.3f}%")

# ── SIGNAL 10: F&G < 30 AND HL Funding > 0 ────────────────────────────────────
print("--- Signal 10: F&G < 30 AND positive HL funding ---")
df10 = fg.join(fund_daily, how="left").dropna(subset=["btc_fwd_1d", "btc_funding_mean"])
fear_and_bull_fund = df10[(df10["value"] < 30) & (df10["btc_funding_mean"] > 0)]
signal_ret10 = fear_and_bull_fund["btc_fwd_1d"]
stats10 = evaluate_signal(signal_ret10.dropna().values)
stats10["note"] = f"F&G<30 AND funding>0, n={len(fear_and_bull_fund)}"
stats10["window"] = "HL overlap (~7m)"
results["S10_fear_plus_funding"] = stats10
print(f"  F&G<30 AND funding>0: {len(fear_and_bull_fund)}, mean={signal_ret10.mean():.3f}%")

# ── SIGNAL 11: VIX Spike → Crypto Bounce 24h later ───────────────────────────
print("--- Signal 11: VIX spike → BTC bounce 24h ---")
df11 = macro_wide.dropna(subset=["vix_chg_z", "btc_fwd_1d"])
vix_spike = df11[df11["vix_chg_z"] > 2.0]  # VIX 1-day change > 2 std
signal_ret11 = vix_spike["btc_fwd_1d"]  # next-day BTC return (bounce)
stats11 = evaluate_signal(signal_ret11.dropna().values)
stats11["note"] = f"VIX daily chg >2std, n={len(vix_spike)}"
stats11["window"] = "2yr macro"
results["S11_vix_spike_bounce"] = stats11
print(f"  VIX spikes: {len(vix_spike)}, next-day BTC mean={signal_ret11.mean():.3f}%")

# Also test 2-day window
signal_ret11b = vix_spike["btc_fwd_2d"]
stats11b = evaluate_signal(signal_ret11b.dropna().values)
stats11b["note"] = f"VIX spike >2std, 2d fwd, n={len(vix_spike)}"
stats11b["window"] = "2yr macro"
results["S11b_vix_spike_2d"] = stats11b

# ── SIGNAL 12: DXY Weekly Drop ────────────────────────────────────────────────
print("--- Signal 12: DXY weekly drop → BTC rally ---")
df12 = macro_wide.dropna(subset=["dxy_7d_chg", "btc_fwd_1d"])
dxy_drop = df12[df12["dxy_7d_chg"] < -1.0]   # DXY down >1% in 5 trading days
signal_ret12 = dxy_drop["btc_fwd_1d"]  # next-day BTC
stats12 = evaluate_signal(signal_ret12.dropna().values)
stats12["note"] = f"DXY 5d chg <-1%, n={len(dxy_drop)}"
stats12["window"] = "2yr macro"
results["S12_dxy_drop"] = stats12
print(f"  DXY drops: {len(dxy_drop)}, next-day BTC mean={signal_ret12.mean():.3f}%")

# Also test DXY daily drop for intraday
dxy_1d_drop = df12[df12["dxy_1d_chg"] < -0.5]  # DXY down >0.5% on the day
signal_ret12b = dxy_1d_drop["btc_fwd_1d"]
stats12b = evaluate_signal(signal_ret12b.dropna().values)
stats12b["note"] = f"DXY 1d chg <-0.5%, n={len(dxy_1d_drop)}"
stats12b["window"] = "2yr macro"
results["S12b_dxy_1d_drop"] = stats12b

# ── SIGNAL 13: Stablecoin Supply Change ──────────────────────────────────────
print("--- Signal 13: Stablecoin supply inflow → BTC bullish ---")
df13 = stable_agg.dropna(subset=["stable_1d_chg_pct", "btc_fwd_1d"])
inflow = df13[df13["stable_1d_chg_pct"] > 0.1]   # >0.1% increase = inflow
outflow = df13[df13["stable_1d_chg_pct"] < -0.1]  # >0.1% decrease = outflow
signal_ret13a = inflow["btc_fwd_1d"]
signal_ret13b = -outflow["btc_fwd_1d"]
signal_ret13 = pd.concat([signal_ret13a, signal_ret13b])
stats13 = evaluate_signal(signal_ret13.dropna().values)
stats13["note"] = f"Inflow (>+0.1%): n={len(inflow)}, Outflow: n={len(outflow)}"
stats13["window"] = "2yr TVL data"
results["S13_stablecoin_flow"] = stats13
print(f"  Inflows: {len(inflow)}, Outflows: {len(outflow)}, mean={signal_ret13.mean():.3f}%")

# ── SIGNAL 14: TVL 7-day Drop ─────────────────────────────────────────────────
print("--- Signal 14: DeFi TVL 7d drop → bearish ---")
df14 = tvl.dropna(subset=["tvl_7d_chg", "btc_fwd_1d"])
tvl_crash = df14[df14["tvl_7d_chg"] < -5.0]  # TVL down >5% in 7 days
signal_ret14 = -tvl_crash["btc_fwd_1d"]  # short
stats14 = evaluate_signal(signal_ret14.dropna().values)
stats14["note"] = f"TVL 7d chg <-5%, n={len(tvl_crash)}"
stats14["window"] = "2yr TVL data"
results["S14_tvl_crash"] = stats14
print(f"  TVL crashes: {len(tvl_crash)}, short BTC mean={signal_ret14.mean():.3f}%")

# ── BONUS: DVOL momentum ──────────────────────────────────────────────────────
print("--- Bonus: DVOL momentum (hourly, 1yr) ---")
dvol_4h = dvol.resample("4h").last().copy()
dvol_4h["dvol_chg_4h"] = dvol_4h["dvol_close"].diff(4)
dvol_4h["fwd_4h"] = dvol_4h.index.map(hl["fwd_4h"])
df_dv = dvol_4h.dropna(subset=["dvol_chg_4h", "fwd_4h"])
dvol_spike = df_dv[df_dv["dvol_chg_4h"] > 3]   # DVOL up >3pts → fear → short
dvol_crush = df_dv[df_dv["dvol_chg_4h"] < -3]  # DVOL down → calm → long
signal_retdv = pd.concat([-dvol_spike["fwd_4h"], dvol_crush["fwd_4h"]])
stats_dv = evaluate_signal(signal_retdv.dropna().values)
stats_dv["note"] = f"DVOL spike>3: n={len(dvol_spike)}, DVOL crush<-3: n={len(dvol_crush)}"
stats_dv["window"] = "1yr DVOL"
results["S00_dvol_momentum"] = stats_dv
print(f"  DVOL spikes: {len(dvol_spike)}, crushes: {len(dvol_crush)}")

# ─────────────────────────────────────────────────────────────────────────────
# COMPOSITE SIGNAL: DVOL + F&G
# ─────────────────────────────────────────────────────────────────────────────
print("--- Composite: High DVOL + Extreme Fear → Contrarian Long ---")
dvol_daily = dvol["dvol_close"].resample("1D").last()
fg_dvol = fg.join(dvol_daily.rename("dvol"), how="left")
fg_dvol["btc_fwd_1d"] = fg_dvol.index.map(hl_daily_ret)
fg_dvol_valid = fg_dvol.dropna(subset=["dvol", "btc_fwd_1d"])
dvol_high_thresh = fg_dvol_valid["dvol"].quantile(0.75)
fear_dvol = fg_dvol_valid[(fg_dvol_valid["value"] < 30) & (fg_dvol_valid["dvol"] > dvol_high_thresh)]
signal_comp = fear_dvol["btc_fwd_1d"]
stats_comp = evaluate_signal(signal_comp.dropna().values)
stats_comp["note"] = f"F&G<30 AND DVOL>75th={dvol_high_thresh:.1f}, n={len(fear_dvol)}"
stats_comp["window"] = "1yr DVOL"
results["S_COMPOSITE_fear_dvol"] = stats_comp

# ─────────────────────────────────────────────────────────────────────────────
# RESULTS TABLE
# ─────────────────────────────────────────────────────────────────────────────

print("\n\n" + "=" * 100)
print("SIGNAL SUMMARY TABLE")
print("=" * 100)

# Sort by |t-stat|
def sort_key(item):
    v = item[1].get("t_stat", 0)
    return 0 if pd.isna(v) else abs(v)

sorted_results = sorted(results.items(), key=sort_key, reverse=True)

HEADER = f"{'Signal':<35} {'N':>6} {'Mean%':>8} {'Net%':>8} {'T-stat':>8} {'P-val':>8} {'Win%':>7} {'Sharpe':>8} {'Window':<18} {'Note'}"
print(HEADER)
print("-" * 160)

for name, r in sorted_results:
    n = r.get("n_trades", 0)
    mean = r.get("mean_return", np.nan)
    net = r.get("net_return", np.nan)
    t = r.get("t_stat", np.nan)
    p = r.get("p_value", np.nan)
    win = r.get("win_rate", np.nan)
    sh = r.get("sharpe", np.nan)
    note = r.get("note", "")
    window = r.get("window", "")

    sig_flag = ""
    if not pd.isna(p):
        if p < 0.01:
            sig_flag = "***"
        elif p < 0.05:
            sig_flag = "** "
        elif p < 0.10:
            sig_flag = "*  "
        else:
            sig_flag = "   "

    def fmt(v, digits=3):
        return f"{v:>{digits+5}.{digits}f}" if not pd.isna(v) else "   N/A"

    print(f"{name:<35} {n:>6} {fmt(mean,3)} {fmt(net,3)} {fmt(t,3)}{sig_flag} {fmt(p,4)} {fmt(win*100 if not pd.isna(win) else np.nan,1)} {fmt(sh,3)} {window:<18} {note[:60]}")

print("\nSignificance: *** p<0.01, ** p<0.05, * p<0.10")
print("\nFee assumption: 6 bps round-trip (BTC/ETH on HL)")
print("Options signals: 24-day window only — N too small for statistical conclusions")
print("Deep signals: F&G (8yr raw, 7m with HL), Macro (2yr), DVOL (1yr)")

# ─────────────────────────────────────────────────────────────────────────────
# DEEP DIVE: Fear & Greed (use all available HL data)
# ─────────────────────────────────────────────────────────────────────────────
print("\n\n" + "=" * 70)
print("DEEP DIVE: Fear & Greed (all ~7 months HL overlap)")
print("=" * 70)

fg_deep = fg.dropna(subset=["btc_fwd_1d"]).copy()
print(f"F&G with HL BTC 1d fwd: {len(fg_deep)} obs ({fg_deep.index[0].date()} – {fg_deep.index[-1].date()})")
print(f"\nF&G distribution:\n{fg_deep['classification'].value_counts().to_string()}")
print(f"\nMean next-day BTC return by F&G zone:")
for zone in ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]:
    subset = fg_deep[fg_deep["classification"] == zone]
    if len(subset) > 0:
        t, p = stats.ttest_1samp(subset["btc_fwd_1d"].dropna(), 0) if len(subset) > 4 else (np.nan, np.nan)
        print(f"  {zone:<18}: n={len(subset):>4}, mean={subset['btc_fwd_1d'].mean():>7.3f}%, "
              f"median={subset['btc_fwd_1d'].median():>7.3f}%, "
              f"win%={subset['btc_fwd_1d'].gt(0).mean()*100:>5.1f}%, "
              f"t={t:>6.2f}, p={p:>6.4f}")

print(f"\nMean next-day BTC return by F&G decile:")
fg_deep["fg_decile"] = pd.qcut(fg_deep["value"], 10, labels=False)
decile_stats = fg_deep.groupby("fg_decile").agg(
    n=("btc_fwd_1d", "count"),
    mean_ret=("btc_fwd_1d", "mean"),
    median_ret=("btc_fwd_1d", "median"),
    fg_mean=("value", "mean"),
).reset_index()
for _, row in decile_stats.iterrows():
    print(f"  Decile {int(row['fg_decile'])+1} (F&G~{row['fg_mean']:.0f}): n={int(row['n']):>4}, "
          f"mean_ret={row['mean_ret']:>7.3f}%, median={row['median_ret']:>7.3f}%")

# ─────────────────────────────────────────────────────────────────────────────
# DEEP DIVE: DVOL (1yr, hourly)
# ─────────────────────────────────────────────────────────────────────────────
print("\n\n" + "=" * 70)
print("DEEP DIVE: DVOL vs BTC returns (1yr hourly)")
print("=" * 70)

dvol_valid = dvol.dropna(subset=["dvol_close", "fwd_4h"])
dvol_valid["dvol_decile"] = pd.qcut(dvol_valid["dvol_close"], 10, labels=False)
print(f"DVOL range: {dvol_valid['dvol_close'].min():.1f} – {dvol_valid['dvol_close'].max():.1f}")
print(f"\nMean 4h BTC return by DVOL decile:")
for dec in range(10):
    sub = dvol_valid[dvol_valid["dvol_decile"] == dec]
    if len(sub) > 10:
        print(f"  Decile {dec+1} (DVOL~{sub['dvol_close'].mean():.0f}): n={len(sub):>5}, "
              f"mean_4h={sub['fwd_4h'].mean():>7.3f}%, "
              f"abs_move={sub['fwd_4h'].abs().mean():>6.3f}%")

print(f"\nIV-RV spread stats:")
dvol_rv = dvol.dropna(subset=["dvol_rv_spread", "fwd_4h"])
print(f"  IV-RV spread range: {dvol_rv['dvol_rv_spread'].min():.1f} to {dvol_rv['dvol_rv_spread'].max():.1f}")
print(f"  Mean spread: {dvol_rv['dvol_rv_spread'].mean():.1f}")
print(f"  Corr(IV-RV spread, |fwd_4h|): {dvol_rv[['dvol_rv_spread','fwd_4h']].corr().iloc[0,1]:.3f}")

print("\n\nDone.")
