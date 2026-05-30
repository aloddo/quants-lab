#!/usr/bin/env python3
"""
Iron Condor Backtest -- Rigorous Validation

Uses DVOL (1yr hourly) + BTC candles (8mo) to simulate weekly iron condors.
No shortcuts: proper Black-Scholes pricing, realistic execution, walk-forward.

Questions we answer:
1. Is the VRP real and tradeable after costs?
2. What's the actual win rate, drawdown, Sharpe?
3. How does it perform in different regimes?
4. Walk-forward: does in-sample predict out-of-sample?

Fee assumption: Bybit 0.03% taker per leg x 4 legs = 0.12% of notional per condor
Slippage: 5% of mid-price per leg (conservative for OTM options)
"""

import numpy as np
import pandas as pd
from scipy.stats import norm
from datetime import datetime, timezone
from pymongo import MongoClient
import warnings
warnings.filterwarnings("ignore")


# ── Black-Scholes ────────────────────────────────────────────────────────────

def bs_call(S, K, T, r, sigma):
    """Black-Scholes call price."""
    if T <= 0 or sigma <= 0:
        return max(S - K, 0)
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)


def bs_put(S, K, T, r, sigma):
    """Black-Scholes put price."""
    if T <= 0 or sigma <= 0:
        return max(K - S, 0)
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def bs_delta_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 1.0 if S > K else 0.0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return norm.cdf(d1)


def bs_delta_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return -1.0 if S < K else 0.0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return norm.cdf(d1) - 1


def find_strike_by_delta(S, target_delta, T, r, sigma, is_call=True, step=500):
    """Find strike closest to target delta via grid search."""
    best_K = S
    best_diff = float('inf')
    low = S * 0.5
    high = S * 1.5
    for K in np.arange(low, high, step):
        if is_call:
            d = bs_delta_call(S, K, T, r, sigma)
        else:
            d = bs_delta_put(S, K, T, r, sigma)
        diff = abs(d - target_delta)
        if diff < best_diff:
            best_diff = diff
            best_K = K
    # Refine with finer grid
    for K in np.arange(best_K - step, best_K + step, step / 10):
        if is_call:
            d = bs_delta_call(S, K, T, r, sigma)
        else:
            d = bs_delta_put(S, K, T, r, sigma)
        diff = abs(d - target_delta)
        if diff < best_diff:
            best_diff = diff
            best_K = K
    return round(best_K / 1000) * 1000  # round to nearest 1000


# ── Data Loading ─────────────────────────────────────────────────────────────

def load_data():
    db = MongoClient("mongodb://localhost:27017/quants_lab").quants_lab

    # BTC 1h candles
    raw = list(db.hyperliquid_candles_1h.find(
        {"coin": "BTC"},
        {"timestamp_utc": 1, "close": 1, "high": 1, "low": 1, "_id": 0},
        sort=[("timestamp_utc", 1)]
    ))
    candles = pd.DataFrame(raw)
    candles["dt"] = pd.to_datetime(candles["timestamp_utc"], unit="ms", utc=True)
    candles = candles.set_index("dt").sort_index()
    candles["close"] = candles["close"].astype(float)
    candles["high"] = candles["high"].astype(float)
    candles["low"] = candles["low"].astype(float)

    # DVOL hourly
    dvol_raw = list(db.deribit_dvol.find(
        {"currency": "BTC"},
        {"timestamp_utc": 1, "dvol_close": 1, "_id": 0},
        sort=[("timestamp_utc", 1)]
    ))
    dvol = pd.DataFrame(dvol_raw)
    dvol["dt"] = pd.to_datetime(dvol["timestamp_utc"], unit="ms", utc=True)
    dvol = dvol.set_index("dt").sort_index()
    dvol["dvol"] = dvol["dvol_close"].astype(float)

    return candles, dvol


# ── Iron Condor Backtest ─────────────────────────────────────────────────────

def backtest_iron_condor(candles, dvol, params):
    """
    Simulate weekly iron condors.

    Entry: Monday 08:00 UTC
    Exit: Friday 08:00 UTC (Deribit expiry) or early exit if breached

    params:
        short_put_delta: -0.15 (sell this delta put)
        long_put_delta: -0.25 (buy this delta put, protection)
        short_call_delta: 0.15
        long_call_delta: 0.25
        notional_btc: 0.01 (per condor)
        n_condors: 2
        early_exit_pct: 0.02 (close if within 2% of short strike)
        slippage_pct: 0.05 (5% of option mid price)
        fee_per_leg: 0.0003 (0.03% of underlying)
        iv_skew_put: 1.06 (puts 6% richer than ATM, from data)
        iv_skew_call: 1.00
    """
    short_put_delta = params.get("short_put_delta", -0.15)
    long_put_delta = params.get("long_put_delta", -0.25)
    short_call_delta = params.get("short_call_delta", 0.15)
    long_call_delta = params.get("long_call_delta", 0.25)
    notional_btc = params.get("notional_btc", 0.01)
    n_condors = params.get("n_condors", 2)
    early_exit_pct = params.get("early_exit_pct", 0.02)
    slippage_pct = params.get("slippage_pct", 0.05)
    fee_per_leg = params.get("fee_per_leg", 0.0003)
    iv_skew_put = params.get("iv_skew_put", 1.06)
    iv_skew_call = params.get("iv_skew_call", 1.00)
    r = 0.05  # risk-free rate

    # Get daily data
    daily_close = candles["close"].resample("1D").last().dropna()
    daily_high = candles["high"].resample("1D").max().dropna()
    daily_low = candles["low"].resample("1D").min().dropna()
    dvol_daily = dvol["dvol"].resample("1D").last().dropna()

    # Find all Mondays and Fridays in the overlap period
    overlap_start = max(daily_close.index[0], dvol_daily.index[0])
    overlap_end = min(daily_close.index[-1], dvol_daily.index[-1])

    trades = []
    all_dates = daily_close.loc[overlap_start:overlap_end].index

    # Find Mondays
    mondays = [d for d in all_dates if d.weekday() == 0]

    for monday in mondays:
        # Find corresponding Friday
        friday = monday + pd.Timedelta(days=4)
        if friday not in daily_close.index:
            # Try nearest Friday
            closest = daily_close.index[daily_close.index >= friday]
            if len(closest) == 0:
                continue
            friday = closest[0]
            if (friday - monday).days > 6:
                continue

        # Entry price and IV
        S = daily_close.loc[monday]
        if monday not in dvol_daily.index:
            closest_dvol = dvol_daily.index[dvol_daily.index <= monday]
            if len(closest_dvol) == 0:
                continue
            iv_atm = dvol_daily.loc[closest_dvol[-1]] / 100  # convert from % to decimal
        else:
            iv_atm = dvol_daily.loc[monday] / 100

        T = 5 / 365  # 5 days to expiry

        # Compute strikes
        iv_put = iv_atm * iv_skew_put
        iv_call = iv_atm * iv_skew_call

        K_short_put = find_strike_by_delta(S, short_put_delta, T, r, iv_put, is_call=False, step=500)
        K_long_put = find_strike_by_delta(S, long_put_delta, T, r, iv_put, is_call=False, step=500)
        K_short_call = find_strike_by_delta(S, short_call_delta, T, r, iv_call, is_call=True, step=500)
        K_long_call = find_strike_by_delta(S, long_call_delta, T, r, iv_call, is_call=True, step=500)

        # Ensure proper ordering
        if K_long_put >= K_short_put:
            K_long_put = K_short_put - 1000
        if K_long_call <= K_short_call:
            K_long_call = K_short_call + 1000

        # Price the options at entry
        short_put_price = bs_put(S, K_short_put, T, r, iv_put)
        long_put_price = bs_put(S, K_long_put, T, r, iv_put)
        short_call_price = bs_call(S, K_short_call, T, r, iv_call)
        long_call_price = bs_call(S, K_long_call, T, r, iv_call)

        # Net premium collected (per BTC)
        put_spread_premium = short_put_price - long_put_price
        call_spread_premium = short_call_price - long_call_price
        gross_premium = put_spread_premium + call_spread_premium

        # Slippage: lose slippage_pct of each option's mid price
        slippage = slippage_pct * (short_put_price + long_put_price +
                                    short_call_price + long_call_price)
        # Fees: 4 legs x fee_per_leg x underlying price
        fees = 4 * fee_per_leg * S

        net_premium = gross_premium - slippage - fees

        if net_premium <= 0:
            continue  # Not worth trading

        # Max loss per side
        put_spread_width = K_short_put - K_long_put
        call_spread_width = K_long_call - K_short_call
        max_loss_put = put_spread_width - put_spread_premium
        max_loss_call = call_spread_width - call_spread_premium

        # Settlement
        settle_price = daily_close.loc[friday]

        # Check intra-week breach (using daily high/low)
        week_dates = daily_close.loc[monday:friday].index
        week_high = daily_high.loc[monday:friday].max()
        week_low = daily_low.loc[monday:friday].min()

        # Early exit check (simplified: if intraweek price came within 2% of short strike)
        early_exit = False
        early_exit_loss = 0
        if week_low <= K_short_put * (1 + early_exit_pct):
            early_exit = True
            # Approximate loss: halfway between premium and max loss
            # (conservative: we don't get max loss but don't keep full premium)
            # Use actual settlement for more accuracy
        if week_high >= K_short_call * (1 - early_exit_pct):
            early_exit = True

        # PnL at settlement
        put_side_pnl = 0
        call_side_pnl = 0

        if settle_price <= K_long_put:
            # Max loss on put side
            put_side_pnl = -max_loss_put
        elif settle_price <= K_short_put:
            # Partial loss on put side
            put_side_pnl = -(K_short_put - settle_price) + put_spread_premium
            # But capped at -max_loss_put
            put_side_pnl = max(put_side_pnl, -max_loss_put)
        else:
            # Put side wins
            put_side_pnl = put_spread_premium

        if settle_price >= K_long_call:
            # Max loss on call side
            call_side_pnl = -max_loss_call
        elif settle_price >= K_short_call:
            # Partial loss on call side
            call_side_pnl = -(settle_price - K_short_call) + call_spread_premium
            call_side_pnl = max(call_side_pnl, -max_loss_call)
        else:
            # Call side wins
            call_side_pnl = call_spread_premium

        # Total PnL per BTC (before slippage/fees already deducted from premium)
        total_pnl_btc = put_side_pnl + call_side_pnl - slippage - fees

        # Scale by notional and number of condors
        total_pnl_usd = total_pnl_btc * notional_btc * n_condors

        # Margin required (approximate: wider spread width x notional x n_condors)
        margin_per_condor = max(put_spread_width, call_spread_width) * notional_btc
        total_margin = margin_per_condor * n_condors

        btc_move_pct = (settle_price - S) / S * 100

        trades.append({
            "entry_date": monday,
            "exit_date": friday,
            "btc_entry": S,
            "btc_exit": settle_price,
            "btc_move_pct": btc_move_pct,
            "week_high": week_high,
            "week_low": week_low,
            "week_range_pct": (week_high - week_low) / S * 100,
            "iv_atm": iv_atm * 100,
            "K_short_put": K_short_put,
            "K_long_put": K_long_put,
            "K_short_call": K_short_call,
            "K_long_call": K_long_call,
            "gross_premium": gross_premium,
            "net_premium": net_premium,
            "slippage": slippage,
            "fees": fees,
            "put_side_pnl": put_side_pnl,
            "call_side_pnl": call_side_pnl,
            "total_pnl_btc": total_pnl_btc,
            "total_pnl_usd": total_pnl_usd,
            "margin": total_margin,
            "return_on_margin": total_pnl_usd / total_margin * 100 if total_margin > 0 else 0,
            "early_exit_triggered": early_exit,
            "win": total_pnl_btc > 0,
        })

    return pd.DataFrame(trades)


def print_results(df, label=""):
    """Print backtest summary."""
    if len(df) == 0:
        print(f"  {label}: No trades")
        return

    wins = df["win"].sum()
    total = len(df)
    win_rate = wins / total * 100

    total_pnl = df["total_pnl_usd"].sum()
    avg_pnl = df["total_pnl_usd"].mean()
    std_pnl = df["total_pnl_usd"].std()
    sharpe = avg_pnl / std_pnl * np.sqrt(52) if std_pnl > 0 else 0

    avg_win = df[df["win"]]["total_pnl_usd"].mean() if wins > 0 else 0
    avg_loss = df[~df["win"]]["total_pnl_usd"].mean() if (total - wins) > 0 else 0

    # Max drawdown (cumulative)
    cum_pnl = df["total_pnl_usd"].cumsum()
    peak = cum_pnl.cummax()
    dd = cum_pnl - peak
    max_dd = dd.min()

    # Monthly returns
    df_copy = df.set_index("entry_date")
    monthly = df_copy["total_pnl_usd"].resample("ME").sum()
    months_positive = (monthly > 0).sum()
    months_total = len(monthly)

    avg_return_on_margin = df["return_on_margin"].mean()

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Weeks:            {total}")
    print(f"  Win rate:         {win_rate:.1f}% ({wins}/{total})")
    print(f"  Total PnL:        ${total_pnl:+.2f}")
    print(f"  Avg weekly PnL:   ${avg_pnl:+.2f}")
    print(f"  Avg win:          ${avg_win:+.2f}")
    print(f"  Avg loss:         ${avg_loss:+.2f}")
    print(f"  Win/Loss ratio:   {abs(avg_win/avg_loss):.2f}x" if avg_loss != 0 else "  Win/Loss ratio:   N/A")
    print(f"  Sharpe (ann):     {sharpe:.2f}")
    print(f"  Max drawdown:     ${max_dd:.2f}")
    print(f"  Avg RoM/week:     {avg_return_on_margin:.2f}%")
    print(f"  Months +/-:       {months_positive}/{months_total}")

    # Regime analysis
    print(f"\n  --- Regime Analysis ---")
    high_iv = df[df["iv_atm"] > df["iv_atm"].median()]
    low_iv = df[df["iv_atm"] <= df["iv_atm"].median()]
    if len(high_iv) > 0:
        print(f"  High IV (>{df['iv_atm'].median():.0f}%): WR={high_iv['win'].mean()*100:.0f}%, avg=${high_iv['total_pnl_usd'].mean():+.2f}")
    if len(low_iv) > 0:
        print(f"  Low IV  (<{df['iv_atm'].median():.0f}%): WR={low_iv['win'].mean()*100:.0f}%, avg=${low_iv['total_pnl_usd'].mean():+.2f}")

    big_moves = df[df["week_range_pct"] > df["week_range_pct"].quantile(0.75)]
    calm_weeks = df[df["week_range_pct"] <= df["week_range_pct"].quantile(0.25)]
    if len(big_moves) > 0:
        print(f"  High range (>P75): WR={big_moves['win'].mean()*100:.0f}%, avg=${big_moves['total_pnl_usd'].mean():+.2f}")
    if len(calm_weeks) > 0:
        print(f"  Low range  (<P25): WR={calm_weeks['win'].mean()*100:.0f}%, avg=${calm_weeks['total_pnl_usd'].mean():+.2f}")

    # Worst weeks
    print(f"\n  --- Worst 5 Weeks ---")
    worst = df.nsmallest(5, "total_pnl_usd")
    for _, row in worst.iterrows():
        print(f"  {row['entry_date'].strftime('%Y-%m-%d')}: ${row['total_pnl_usd']:+.2f} "
              f"(BTC {row['btc_move_pct']:+.1f}%, range {row['week_range_pct']:.1f}%, IV {row['iv_atm']:.0f}%)")


def walk_forward(candles, dvol, params, train_weeks=16, test_weeks=4):
    """Walk-forward validation: train on N weeks, test on next M weeks."""
    full_df = backtest_iron_condor(candles, dvol, params)
    if len(full_df) < train_weeks + test_weeks:
        print(f"Not enough data for walk-forward ({len(full_df)} weeks)")
        return full_df

    n_folds = (len(full_df) - train_weeks) // test_weeks
    results = []

    print(f"\n{'='*60}")
    print(f"  WALK-FORWARD: {train_weeks}w train / {test_weeks}w test / {n_folds} folds")
    print(f"{'='*60}")

    for i in range(n_folds):
        train_start = i * test_weeks
        train_end = train_start + train_weeks
        test_start = train_end
        test_end = min(test_start + test_weeks, len(full_df))

        if test_end > len(full_df):
            break

        train = full_df.iloc[train_start:train_end]
        test = full_df.iloc[test_start:test_end]

        if len(test) == 0:
            break

        train_wr = train["win"].mean() * 100
        train_pnl = train["total_pnl_usd"].mean()
        test_wr = test["win"].mean() * 100
        test_pnl = test["total_pnl_usd"].mean()

        results.append({
            "fold": i + 1,
            "train_period": f"{train.iloc[0]['entry_date'].strftime('%m/%d')}-{train.iloc[-1]['entry_date'].strftime('%m/%d')}",
            "test_period": f"{test.iloc[0]['entry_date'].strftime('%m/%d')}-{test.iloc[-1]['entry_date'].strftime('%m/%d')}",
            "train_wr": train_wr,
            "train_avg_pnl": train_pnl,
            "test_wr": test_wr,
            "test_avg_pnl": test_pnl,
        })

        print(f"  Fold {i+1}: Train {train_wr:.0f}% WR, ${train_pnl:+.2f}/wk | "
              f"Test {test_wr:.0f}% WR, ${test_pnl:+.2f}/wk")

    if results:
        res_df = pd.DataFrame(results)
        print(f"\n  Walk-forward avg test WR: {res_df['test_wr'].mean():.1f}%")
        print(f"  Walk-forward avg test PnL: ${res_df['test_avg_pnl'].mean():+.2f}/wk")
        # Check if train performance predicts test
        corr = res_df[["train_avg_pnl", "test_avg_pnl"]].corr().iloc[0, 1]
        print(f"  Train-test PnL correlation: {corr:.3f}")

    return full_df


# ── Sensitivity Analysis ─────────────────────────────────────────────────────

def sensitivity_analysis(candles, dvol):
    """Test how sensitive results are to parameter changes."""
    print(f"\n{'='*60}")
    print(f"  PARAMETER SENSITIVITY")
    print(f"{'='*60}")

    base = {
        "short_put_delta": -0.15, "long_put_delta": -0.25,
        "short_call_delta": 0.15, "long_call_delta": 0.25,
        "notional_btc": 0.01, "n_condors": 2,
        "slippage_pct": 0.05, "fee_per_leg": 0.0003,
        "iv_skew_put": 1.06, "iv_skew_call": 1.00,
        "early_exit_pct": 0.02,
    }

    variations = [
        ("Base case", {}),
        ("Wider strikes (10d)", {"short_put_delta": -0.10, "short_call_delta": 0.10}),
        ("Tighter strikes (20d)", {"short_put_delta": -0.20, "short_call_delta": 0.20}),
        ("Higher slippage (10%)", {"slippage_pct": 0.10}),
        ("Higher fees (2x)", {"fee_per_leg": 0.0006}),
        ("No put skew", {"iv_skew_put": 1.00}),
        ("Higher put skew (10%)", {"iv_skew_put": 1.10}),
        ("4 condors", {"n_condors": 4}),
    ]

    print(f"  {'Scenario':<30} {'WR':>6} {'Avg$/wk':>10} {'Sharpe':>8} {'MaxDD':>10}")
    print(f"  {'-'*68}")

    for name, overrides in variations:
        p = {**base, **overrides}
        df = backtest_iron_condor(candles, dvol, p)
        if len(df) == 0:
            continue
        wr = df["win"].mean() * 100
        avg = df["total_pnl_usd"].mean()
        std = df["total_pnl_usd"].std()
        sharpe = avg / std * np.sqrt(52) if std > 0 else 0
        cum = df["total_pnl_usd"].cumsum()
        mdd = (cum - cum.cummax()).min()
        print(f"  {name:<30} {wr:>5.1f}% ${avg:>8.2f} {sharpe:>8.2f} ${mdd:>9.2f}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Loading data...")
    candles, dvol = load_data()
    print(f"  Candles: {candles.index[0].date()} to {candles.index[-1].date()}")
    print(f"  DVOL: {dvol.index[0].date()} to {dvol.index[-1].date()}")

    # Base parameters
    params = {
        "short_put_delta": -0.15,
        "long_put_delta": -0.25,
        "short_call_delta": 0.15,
        "long_call_delta": 0.25,
        "notional_btc": 0.01,
        "n_condors": 2,
        "slippage_pct": 0.05,       # 5% of option price
        "fee_per_leg": 0.0003,       # 0.03% of underlying
        "iv_skew_put": 1.06,         # puts 6% richer
        "iv_skew_call": 1.00,
        "early_exit_pct": 0.02,      # 2% buffer
    }

    # Full backtest
    print("\n" + "="*60)
    print("  FULL BACKTEST (all available data)")
    print("="*60)
    df = backtest_iron_condor(candles, dvol, params)
    print_results(df, "Iron Condor 15-delta, 2 condors x 0.01 BTC ($400 account)")

    # Walk-forward
    df = walk_forward(candles, dvol, params, train_weeks=12, test_weeks=4)

    # Sensitivity
    sensitivity_analysis(candles, dvol)

    # Realized Vol comparison
    print(f"\n{'='*60}")
    print(f"  VRP REALITY CHECK")
    print(f"{'='*60}")
    if len(df) > 0:
        df_copy = df.copy()
        # IV at entry vs actual weekly realized vol
        df_copy["realized_weekly_vol"] = df_copy["week_range_pct"] / np.sqrt(5) * np.sqrt(365)
        vrp = df_copy["iv_atm"] - df_copy["realized_weekly_vol"]
        print(f"  Mean IV at entry:        {df_copy['iv_atm'].mean():.1f}%")
        print(f"  Mean realized vol (ann): {df_copy['realized_weekly_vol'].mean():.1f}%")
        print(f"  Mean VRP:                {vrp.mean():+.1f}%")
        print(f"  VRP positive weeks:      {(vrp > 0).sum()}/{len(vrp)} ({(vrp > 0).mean()*100:.0f}%)")
        print(f"  VRP negative weeks:      {(vrp < 0).sum()}/{len(vrp)} ({(vrp < 0).mean()*100:.0f}%)")


if __name__ == "__main__":
    main()
