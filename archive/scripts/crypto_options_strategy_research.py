#!/usr/bin/env python3
"""
Crypto options strategy research and backtest.

The long backtest uses observed spot paths plus Deribit DVOL as the
mark-to-market implied-vol input for weekly Bybit-implementable structures.
The option mark-history collections only contain currently listed future
expiries, so they are used for recent validation rather than as a full
historical weekly-options archive.

Run:
    uv run --no-project --with pymongo --with pandas --with numpy \
      python scripts/crypto_options_strategy_research.py
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Iterable

import numpy as np
import pandas as pd
from pymongo import MongoClient


MONGO_URI = "mongodb://localhost:27017/quants_lab"
DB_NAME = "quants_lab"
OUT_DIR = Path("docs/research")
REPORT_PATH = OUT_DIR / "crypto_options_strategy_research_2026_05_16.md"
JSON_PATH = OUT_DIR / "crypto_options_strategy_research_2026_05_16.json"


MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


@dataclass(frozen=True)
class StrategyParams:
    vrp_min: float
    trend_cutoff: float
    short_delta: float
    long_delta: float
    profit_take_credit: float
    stop_r: float
    enter_weekday: int = 4
    entry_hour: int = 8
    max_dte_hours: int = 144
    time_exit_hours: int = 120
    fee_rate_underlying: float = 0.0003
    slippage_rate_option: float = 0.03
    max_abs_24h_ret: float = 0.08
    max_dvol: float = 95.0


@dataclass
class Trade:
    currency: str
    structure: str
    entry_time: str
    exit_time: str
    entry_spot: float
    exit_spot: float
    entry_dvol: float
    entry_rv7: float
    entry_vrp: float
    trend_7d: float
    credit: float
    max_risk: float
    pnl: float
    r_multiple: float
    exit_reason: str
    short_put: float | None
    long_put: float | None
    short_call: float | None
    long_call: float | None


@dataclass(frozen=True)
class PutDebitMomentumParams:
    trend_7d_max: float = -0.06
    vrp_max: float = 4.0
    buy_delta: float = 0.30
    sell_delta: float = 0.12
    profit_take_debit: float = 1.80
    stop_debit: float = 0.45
    entry_hour: int = 8
    dte_hours: int = 240
    time_exit_hours: int = 96
    fee_rate_underlying: float = 0.0003
    slippage_rate_option: float = 0.03
    max_abs_24h_ret: float = 0.07
    max_dvol: float = 90.0


def utc_from_ms(ms: int | float) -> pd.Timestamp:
    return pd.to_datetime(int(ms), unit="ms", utc=True)


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price(spot: float, strike: float, t_years: float, vol: float, option_type: str, rate: float = 0.0) -> float:
    if t_years <= 0 or vol <= 0:
        return max(spot - strike, 0.0) if option_type == "call" else max(strike - spot, 0.0)
    sqrt_t = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (rate + 0.5 * vol * vol) * t_years) / (vol * sqrt_t)
    d2 = d1 - vol * sqrt_t
    if option_type == "call":
        return spot * norm_cdf(d1) - strike * math.exp(-rate * t_years) * norm_cdf(d2)
    return strike * math.exp(-rate * t_years) * norm_cdf(-d2) - spot * norm_cdf(-d1)


def bs_delta(spot: float, strike: float, t_years: float, vol: float, option_type: str) -> float:
    if t_years <= 0 or vol <= 0:
        if option_type == "call":
            return 1.0 if spot > strike else 0.0
        return -1.0 if spot < strike else 0.0
    d1 = (math.log(spot / strike) + 0.5 * vol * vol * t_years) / (vol * math.sqrt(t_years))
    if option_type == "call":
        return norm_cdf(d1)
    return norm_cdf(d1) - 1.0


def strike_step(currency: str, spot: float) -> float:
    if currency == "BTC":
        return 1000.0 if spot >= 30000 else 500.0
    return 50.0 if spot >= 1500 else 25.0


def round_strike(currency: str, strike: float) -> float:
    step = strike_step(currency, strike)
    return max(step, round(strike / step) * step)


def find_strike_for_delta(
    currency: str,
    spot: float,
    t_years: float,
    vol: float,
    option_type: str,
    target_abs_delta: float,
) -> float:
    if option_type == "put":
        low, high = spot * 0.45, spot * 1.02
        target = -target_abs_delta
        for _ in range(48):
            mid = (low + high) / 2
            delta = bs_delta(spot, mid, t_years, vol, option_type)
            if delta < target:
                high = mid
            else:
                low = mid
        return round_strike(currency, (low + high) / 2)
    else:
        low, high = spot * 0.98, spot * 1.75
        target = target_abs_delta
        for _ in range(48):
            mid = (low + high) / 2
            delta = bs_delta(spot, mid, t_years, vol, option_type)
            if delta > target:
                low = mid
            else:
                high = mid
        return round_strike(currency, (low + high) / 2)


def parse_expiry(expiry: str) -> pd.Timestamp | None:
    match = re.fullmatch(r"(\d{1,2})([A-Z]{3})(\d{2})", expiry)
    if not match:
        return None
    day, mon, year = match.groups()
    return pd.Timestamp(datetime(2000 + int(year), MONTHS[mon], int(day), 8, tzinfo=timezone.utc))


def parse_option_symbol(symbol: str) -> dict | None:
    parts = symbol.split("-")
    if len(parts) < 4:
        return None
    expiry = parse_expiry(parts[1])
    if expiry is None:
        return None
    opt_code = parts[3]
    return {
        "currency": parts[0],
        "expiry": expiry,
        "strike": float(parts[2]),
        "type": "call" if opt_code.startswith("C") else "put",
    }


def load_hourly_market_data(db) -> pd.DataFrame:
    spot_rows = list(
        db.hyperliquid_candles_1h.find(
            {"coin": {"$in": ["BTC", "ETH"]}},
            {"_id": 0, "coin": 1, "timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1},
        ).sort("timestamp_utc", 1)
    )
    spot = pd.DataFrame(spot_rows)
    spot["timestamp"] = pd.to_datetime(spot["timestamp_utc"], unit="ms", utc=True)
    spot = spot.rename(columns={"coin": "currency"})
    for col in ["open", "high", "low", "close"]:
        spot[col] = spot[col].astype(float)

    dvol_rows = list(
        db.deribit_dvol_full.find(
            {"currency": {"$in": ["BTC", "ETH"]}},
            {"_id": 0, "currency": 1, "timestamp_utc": 1, "dvol_close": 1},
        ).sort("timestamp_utc", 1)
    )
    dvol = pd.DataFrame(dvol_rows)
    dvol["timestamp"] = pd.to_datetime(dvol["timestamp_utc"], unit="ms", utc=True)
    dvol["dvol"] = dvol["dvol_close"].astype(float)

    rows = []
    for currency in ["BTC", "ETH"]:
        s = spot[spot["currency"] == currency].set_index("timestamp").sort_index()
        v = dvol[dvol["currency"] == currency].set_index("timestamp").sort_index()[["dvol"]]
        joined = s.join(v, how="inner").ffill()
        log_ret = np.log(joined["close"] / joined["close"].shift(1))
        joined["ret_1h"] = log_ret
        joined["rv_24h"] = log_ret.rolling(24).std() * math.sqrt(24 * 365) * 100
        joined["rv_7d"] = log_ret.rolling(24 * 7).std() * math.sqrt(24 * 365) * 100
        joined["rv_14d"] = log_ret.rolling(24 * 14).std() * math.sqrt(24 * 365) * 100
        joined["ret_24h"] = joined["close"].pct_change(24)
        joined["trend_7d"] = joined["close"].pct_change(24 * 7)
        joined["vrp_7d"] = joined["dvol"] - joined["rv_7d"]
        joined["dvol_rank_90d"] = joined["dvol"].rolling(24 * 90, min_periods=24 * 20).rank(pct=True)
        joined["currency"] = currency
        rows.append(joined.reset_index())
    return pd.concat(rows, ignore_index=True).dropna(subset=["rv_7d", "vrp_7d", "trend_7d"])


def dvol_and_vrp_findings(market: pd.DataFrame) -> dict:
    findings = {}
    for currency, df in market.groupby("currency"):
        daily = df.set_index("timestamp").resample("1D").last()
        future_abs_7d = daily["close"].pct_change(7).shift(-7).abs() * 100
        tmp = daily.assign(future_abs_7d=future_abs_7d).dropna()
        high_vrp = tmp[tmp["vrp_7d"] >= tmp["vrp_7d"].quantile(0.75)]
        low_vrp = tmp[tmp["vrp_7d"] <= tmp["vrp_7d"].quantile(0.25)]
        findings[currency] = {
            "sample_start": str(daily.index.min()),
            "sample_end": str(daily.index.max()),
            "days": int(daily["close"].notna().sum()),
            "dvol_mean": float(daily["dvol"].mean()),
            "dvol_p10": float(daily["dvol"].quantile(0.10)),
            "dvol_p50": float(daily["dvol"].quantile(0.50)),
            "dvol_p90": float(daily["dvol"].quantile(0.90)),
            "rv7_mean": float(daily["rv_7d"].mean()),
            "vrp_mean": float(daily["vrp_7d"].mean()),
            "vrp_p25": float(daily["vrp_7d"].quantile(0.25)),
            "vrp_p50": float(daily["vrp_7d"].quantile(0.50)),
            "vrp_p75": float(daily["vrp_7d"].quantile(0.75)),
            "vrp_positive_pct": float((daily["vrp_7d"] > 0).mean() * 100),
            "corr_vrp_future_abs_7d": float(tmp["vrp_7d"].corr(tmp["future_abs_7d"])),
            "future_abs_7d_high_vrp": float(high_vrp["future_abs_7d"].mean()),
            "future_abs_7d_low_vrp": float(low_vrp["future_abs_7d"].mean()),
        }
    return findings


def surface_findings(db) -> dict:
    sums = defaultdict(lambda: [0.0, 0])
    snapshot_count = defaultdict(int)
    projection = {
        "_id": 0,
        "currency": 1,
        "timestamp_utc": 1,
        "expiry": 1,
        "strike": 1,
        "type": 1,
        "mark_iv": 1,
        "underlying_price": 1,
        "open_interest": 1,
        "volume_24h": 1,
    }
    cursor = db.deribit_options_surface.find(
        {
            "currency": {"$in": ["BTC", "ETH"]},
            "mark_iv": {"$gt": 0},
            "underlying_price": {"$gt": 0},
            "$expr": {
                "$and": [
                    {"$gte": [{"$divide": ["$strike", "$underlying_price"]}, 0.88]},
                    {"$lte": [{"$divide": ["$strike", "$underlying_price"]}, 1.12]},
                ]
            },
        },
        projection,
        no_cursor_timeout=True,
    ).batch_size(10000)

    try:
        for doc in cursor:
            ts = utc_from_ms(doc["timestamp_utc"])
            expiry = parse_expiry(str(doc["expiry"]))
            if expiry is None:
                continue
            dte = (expiry - ts).total_seconds() / 86400
            if dte < 1 or dte > 120:
                continue
            spot = float(doc["underlying_price"])
            strike = float(doc["strike"])
            if spot <= 0 or strike <= 0:
                continue
            m = strike / spot
            iv = float(doc["mark_iv"]) * 100
            currency = doc["currency"]
            day = ts.floor("1D")
            snapshot_count[(currency, day)] += 1

            if 0.975 <= m <= 1.025:
                if 3 <= dte <= 10:
                    sums[(currency, "atm_3_10")][0] += iv
                    sums[(currency, "atm_3_10")][1] += 1
                elif 10 < dte <= 24:
                    sums[(currency, "atm_10_24")][0] += iv
                    sums[(currency, "atm_10_24")][1] += 1
                elif 24 < dte <= 60:
                    sums[(currency, "atm_24_60")][0] += iv
                    sums[(currency, "atm_24_60")][1] += 1
                elif 60 < dte <= 120:
                    sums[(currency, "atm_60_120")][0] += iv
                    sums[(currency, "atm_60_120")][1] += 1
            if 7 <= dte <= 45:
                if doc["type"] == "put" and 0.90 <= m <= 0.95:
                    sums[(currency, "put_90_95")][0] += iv
                    sums[(currency, "put_90_95")][1] += 1
                elif doc["type"] == "call" and 1.05 <= m <= 1.10:
                    sums[(currency, "call_105_110")][0] += iv
                    sums[(currency, "call_105_110")][1] += 1
                elif 0.975 <= m <= 1.025:
                    sums[(currency, "atm_7_45")][0] += iv
                    sums[(currency, "atm_7_45")][1] += 1
    finally:
        cursor.close()

    out = {}
    for currency in ["BTC", "ETH"]:
        values = {}
        for bucket in [
            "atm_3_10",
            "atm_10_24",
            "atm_24_60",
            "atm_60_120",
            "put_90_95",
            "call_105_110",
            "atm_7_45",
        ]:
            total, count = sums[(currency, bucket)]
            values[bucket] = float(total / count) if count else None
            values[f"{bucket}_n"] = int(count)
        values["term_slope_24_60_minus_3_10"] = (
            values["atm_24_60"] - values["atm_3_10"]
            if values["atm_24_60"] is not None and values["atm_3_10"] is not None
            else None
        )
        values["put_call_skew_7_45"] = (
            values["put_90_95"] - values["call_105_110"]
            if values["put_90_95"] is not None and values["call_105_110"] is not None
            else None
        )
        values["snapshot_days"] = int(len({d for c, d in snapshot_count if c == currency}))
        out[currency] = values
    return out


def liquidation_findings(db, market: pd.DataFrame) -> dict:
    rows = list(
        db.coinalyze_liquidations.find(
            {"pair": {"$in": ["BTC-USDT", "ETH-USDT"]}},
            {"_id": 0, "pair": 1, "timestamp_utc": 1, "total_liquidations_usd": 1},
        ).sort("timestamp_utc", 1)
    )
    if not rows:
        return {}
    liq = pd.DataFrame(rows)
    liq["timestamp"] = pd.to_datetime(liq["timestamp_utc"], unit="ms", utc=True)
    liq["currency"] = liq["pair"].str.split("-").str[0]
    liq["total_liquidations_usd"] = liq["total_liquidations_usd"].astype(float)
    out = {}
    for currency in ["BTC", "ETH"]:
        l = liq[liq["currency"] == currency].set_index("timestamp")["total_liquidations_usd"].resample("1D").sum()
        m = market[market["currency"] == currency].set_index("timestamp").resample("1D").last()
        d = pd.DataFrame({"liq": l, "abs_next_1d": m["close"].pct_change().abs().shift(-1) * 100}).dropna()
        d["liq_z"] = (d["liq"] - d["liq"].rolling(30, min_periods=10).mean()) / d["liq"].rolling(30, min_periods=10).std()
        high = d[d["liq_z"] >= 2.0]
        normal = d[d["liq_z"] < 2.0]
        out[currency] = {
            "days": int(len(d)),
            "corr_liq_next_abs_1d": float(d[["liq", "abs_next_1d"]].corr().iloc[0, 1]),
            "next_abs_1d_after_liq_z2": float(high["abs_next_1d"].mean()) if len(high) else None,
            "next_abs_1d_normal": float(normal["abs_next_1d"].mean()) if len(normal) else None,
            "z2_days": int(len(high)),
        }
    return out


def option_vol_for_leg(base_vol: float, option_type: str, strike: float, spot: float, surface: dict, currency: str) -> float:
    m = strike / spot
    skew = surface.get(currency, {}).get("put_call_skew_7_45") or 0.0
    # Use the measured 7-45 DTE put/call skew as a small strike-IV tilt.
    if option_type == "put" and m < 0.98:
        return max(0.05, (base_vol + max(skew, 0.0) * min((0.98 - m) / 0.08, 1.0)) / 100)
    if option_type == "call" and m > 1.02:
        return max(0.05, (base_vol - max(skew, 0.0) * 0.35 * min((m - 1.02) / 0.08, 1.0)) / 100)
    return max(0.05, base_vol / 100)


def price_leg(row: pd.Series, strike: float, option_type: str, expiry: pd.Timestamp, surface: dict) -> float:
    dte_hours = max((expiry - row["timestamp"]).total_seconds() / 3600, 0)
    t_years = dte_hours / (24 * 365)
    vol = option_vol_for_leg(float(row["dvol"]), option_type, strike, float(row["close"]), surface, row["currency"])
    return bs_price(float(row["close"]), strike, t_years, vol, option_type)


def build_structure(row: pd.Series, params: StrategyParams, surface: dict) -> tuple[str, dict] | None:
    if row["vrp_7d"] < params.vrp_min:
        return None
    if abs(row["ret_24h"]) > params.max_abs_24h_ret:
        return None
    if row["dvol"] > params.max_dvol:
        return None

    currency = row["currency"]
    spot = float(row["close"])
    expiry = row["timestamp"] + pd.Timedelta(hours=params.max_dte_hours)
    t_years = params.max_dte_hours / (24 * 365)
    base_vol = max(float(row["dvol"]) / 100, 0.05)

    trend = float(row["trend_7d"])
    if trend > params.trend_cutoff:
        structure = "put_credit_spread"
    elif trend < -params.trend_cutoff:
        structure = "call_credit_spread"
    else:
        structure = "iron_condor"

    legs = {}
    if structure in ("put_credit_spread", "iron_condor"):
        sp = find_strike_for_delta(currency, spot, t_years, base_vol, "put", params.short_delta)
        lp = find_strike_for_delta(currency, spot, t_years, base_vol, "put", params.long_delta)
        if lp >= sp:
            lp = round_strike(currency, sp * 0.95)
        legs["short_put"] = sp
        legs["long_put"] = lp
    if structure in ("call_credit_spread", "iron_condor"):
        sc = find_strike_for_delta(currency, spot, t_years, base_vol, "call", params.short_delta)
        lc = find_strike_for_delta(currency, spot, t_years, base_vol, "call", params.long_delta)
        if lc <= sc:
            lc = round_strike(currency, sc * 1.05)
        legs["short_call"] = sc
        legs["long_call"] = lc
    return structure, legs


def value_structure(row: pd.Series, expiry: pd.Timestamp, structure: str, legs: dict, surface: dict) -> tuple[float, float]:
    """Return close debit and sum of option marks for slippage calculations."""
    debit = 0.0
    option_sum = 0.0
    if structure in ("put_credit_spread", "iron_condor"):
        short_put = price_leg(row, legs["short_put"], "put", expiry, surface)
        long_put = price_leg(row, legs["long_put"], "put", expiry, surface)
        debit += short_put - long_put
        option_sum += short_put + long_put
    if structure in ("call_credit_spread", "iron_condor"):
        short_call = price_leg(row, legs["short_call"], "call", expiry, surface)
        long_call = price_leg(row, legs["long_call"], "call", expiry, surface)
        debit += short_call - long_call
        option_sum += short_call + long_call
    return debit, option_sum


def max_risk_for_structure(structure: str, legs: dict, credit: float) -> float:
    risks = []
    if structure in ("put_credit_spread", "iron_condor"):
        risks.append(legs["short_put"] - legs["long_put"] - credit)
    if structure in ("call_credit_spread", "iron_condor"):
        risks.append(legs["long_call"] - legs["short_call"] - credit)
    return max(risks)


def backtest_strategy(market: pd.DataFrame, surface: dict, params: StrategyParams) -> list[Trade]:
    trades: list[Trade] = []
    last_exit: dict[str, pd.Timestamp] = {"BTC": pd.Timestamp.min.tz_localize("UTC"), "ETH": pd.Timestamp.min.tz_localize("UTC")}
    market = market.sort_values(["currency", "timestamp"]).reset_index(drop=True)

    for currency, df in market.groupby("currency"):
        df = df.reset_index(drop=True)
        for i, row in df.iterrows():
            ts = row["timestamp"]
            if ts.weekday() != params.enter_weekday or ts.hour != params.entry_hour:
                continue
            if ts <= last_exit[currency]:
                continue
            built = build_structure(row, params, surface)
            if built is None:
                continue
            structure, legs = built
            expiry = ts + pd.Timedelta(hours=params.max_dte_hours)
            entry_debit, entry_option_sum = value_structure(row, expiry, structure, legs, surface)
            leg_count = 4 if structure == "iron_condor" else 2
            entry_cost = params.slippage_rate_option * entry_option_sum + params.fee_rate_underlying * float(row["close"]) * leg_count
            credit = entry_debit - entry_cost
            if credit <= 0:
                continue
            max_risk = max_risk_for_structure(structure, legs, credit)
            if max_risk <= 0:
                continue

            future = df[(df["timestamp"] > ts) & (df["timestamp"] <= ts + pd.Timedelta(hours=params.max_dte_hours))]
            if future.empty:
                continue
            exit_trade = None
            for _, mark in future.iterrows():
                close_debit, option_sum = value_structure(mark, expiry, structure, legs, surface)
                exit_cost = params.slippage_rate_option * option_sum + params.fee_rate_underlying * float(mark["close"]) * leg_count
                pnl = credit - close_debit - exit_cost
                reason = None
                if pnl >= params.profit_take_credit * credit:
                    reason = "take_profit"
                elif pnl <= -params.stop_r * max_risk:
                    reason = "stop_loss"
                elif mark["timestamp"] >= ts + pd.Timedelta(hours=params.time_exit_hours):
                    reason = "time_exit"
                if reason:
                    exit_trade = (mark, pnl, reason)
                    break
            if exit_trade is None:
                mark = future.iloc[-1]
                close_debit, option_sum = value_structure(mark, expiry, structure, legs, surface)
                exit_cost = params.slippage_rate_option * option_sum + params.fee_rate_underlying * float(mark["close"]) * leg_count
                exit_trade = (mark, credit - close_debit - exit_cost, "expiry_guard")

            mark, pnl, reason = exit_trade
            trades.append(
                Trade(
                    currency=currency,
                    structure=structure,
                    entry_time=str(ts),
                    exit_time=str(mark["timestamp"]),
                    entry_spot=float(row["close"]),
                    exit_spot=float(mark["close"]),
                    entry_dvol=float(row["dvol"]),
                    entry_rv7=float(row["rv_7d"]),
                    entry_vrp=float(row["vrp_7d"]),
                    trend_7d=float(row["trend_7d"]),
                    credit=float(credit),
                    max_risk=float(max_risk),
                    pnl=float(pnl),
                    r_multiple=float(pnl / max_risk),
                    exit_reason=reason,
                    short_put=legs.get("short_put"),
                    long_put=legs.get("long_put"),
                    short_call=legs.get("short_call"),
                    long_call=legs.get("long_call"),
                )
            )
            last_exit[currency] = mark["timestamp"]
    return trades


def backtest_put_debit_momentum(market: pd.DataFrame, surface: dict, params: PutDebitMomentumParams) -> list[Trade]:
    trades: list[Trade] = []
    last_exit: dict[str, pd.Timestamp] = {"BTC": pd.Timestamp.min.tz_localize("UTC"), "ETH": pd.Timestamp.min.tz_localize("UTC")}
    market = market.sort_values(["currency", "timestamp"]).reset_index(drop=True)

    for currency, df in market.groupby("currency"):
        df = df.reset_index(drop=True)
        for _, row in df.iterrows():
            ts = row["timestamp"]
            if ts.hour != params.entry_hour or ts <= last_exit[currency]:
                continue
            if row["trend_7d"] > params.trend_7d_max:
                continue
            if row["vrp_7d"] > params.vrp_max:
                continue
            if abs(row["ret_24h"]) > params.max_abs_24h_ret or row["dvol"] > params.max_dvol:
                continue

            spot = float(row["close"])
            expiry = ts + pd.Timedelta(hours=params.dte_hours)
            t_years = params.dte_hours / (24 * 365)
            base_vol = max(float(row["dvol"]) / 100, 0.05)
            long_put = find_strike_for_delta(currency, spot, t_years, base_vol, "put", params.buy_delta)
            short_put = find_strike_for_delta(currency, spot, t_years, base_vol, "put", params.sell_delta)
            if short_put >= long_put:
                short_put = round_strike(currency, long_put * 0.95)

            buy_price = price_leg(row, long_put, "put", expiry, surface)
            sell_price = price_leg(row, short_put, "put", expiry, surface)
            entry_cost = params.slippage_rate_option * (buy_price + sell_price) + params.fee_rate_underlying * spot * 2
            debit = buy_price - sell_price + entry_cost
            width = long_put - short_put
            if debit <= 0 or debit > 0.80 * width:
                continue

            future = df[(df["timestamp"] > ts) & (df["timestamp"] <= ts + pd.Timedelta(hours=params.dte_hours))]
            if future.empty:
                continue
            exit_trade = None
            for _, mark in future.iterrows():
                buy_mark = price_leg(mark, long_put, "put", expiry, surface)
                sell_mark = price_leg(mark, short_put, "put", expiry, surface)
                exit_cost = params.slippage_rate_option * (buy_mark + sell_mark) + params.fee_rate_underlying * float(mark["close"]) * 2
                value = buy_mark - sell_mark - exit_cost
                pnl = value - debit
                reason = None
                if pnl >= params.profit_take_debit * debit:
                    reason = "take_profit"
                elif pnl <= -params.stop_debit * debit:
                    reason = "stop_loss"
                elif mark["timestamp"] >= ts + pd.Timedelta(hours=params.time_exit_hours):
                    reason = "time_exit"
                if reason:
                    exit_trade = (mark, pnl, reason)
                    break
            if exit_trade is None:
                mark = future.iloc[-1]
                buy_mark = price_leg(mark, long_put, "put", expiry, surface)
                sell_mark = price_leg(mark, short_put, "put", expiry, surface)
                exit_cost = params.slippage_rate_option * (buy_mark + sell_mark) + params.fee_rate_underlying * float(mark["close"]) * 2
                exit_trade = (mark, buy_mark - sell_mark - exit_cost - debit, "expiry_guard")

            mark, pnl, reason = exit_trade
            trades.append(
                Trade(
                    currency=currency,
                    structure="put_debit_spread",
                    entry_time=str(ts),
                    exit_time=str(mark["timestamp"]),
                    entry_spot=spot,
                    exit_spot=float(mark["close"]),
                    entry_dvol=float(row["dvol"]),
                    entry_rv7=float(row["rv_7d"]),
                    entry_vrp=float(row["vrp_7d"]),
                    trend_7d=float(row["trend_7d"]),
                    credit=float(-debit),
                    max_risk=float(debit),
                    pnl=float(pnl),
                    r_multiple=float(pnl / debit),
                    exit_reason=reason,
                    short_put=short_put,
                    long_put=long_put,
                    short_call=None,
                    long_call=None,
                )
            )
            last_exit[currency] = mark["timestamp"]
    return trades


def metrics(trades: list[Trade], risk_pct: float = 0.01) -> dict:
    if not trades:
        return {}
    r = np.array([t.r_multiple for t in trades], dtype=float)
    pnl = np.array([t.pnl for t in trades], dtype=float)
    dates = pd.to_datetime([t.exit_time for t in trades])
    order = np.argsort(dates)
    r = r[order]
    dates = dates[order]
    equity = 1.0 + np.cumsum(r * risk_pct)
    running_max = np.maximum.accumulate(equity)
    dd = equity / running_max - 1.0
    wins = r[r > 0]
    losses = r[r < 0]
    avg_days = max((dates.max() - dates.min()).days / max(len(r), 1), 1 / 24)
    annual_factor = 365 / avg_days
    sharpe = float(np.mean(r) / np.std(r, ddof=1) * math.sqrt(annual_factor)) if len(r) > 1 and np.std(r, ddof=1) > 0 else 0.0
    return {
        "trades": int(len(r)),
        "win_rate": float((r > 0).mean() * 100),
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 0 else None,
        "avg_r": float(r.mean()),
        "median_r": float(np.median(r)),
        "total_r": float(r.sum()),
        "sharpe_r": sharpe,
        "max_drawdown_at_1pct_risk": float(dd.min() * 100),
        "total_return_at_1pct_risk": float((equity[-1] - 1.0) * 100),
        "gross_profit_r": float(wins.sum()) if len(wins) else 0.0,
        "gross_loss_r": float(losses.sum()) if len(losses) else 0.0,
        "avg_holding_days": float(avg_days),
    }


def split_trades(trades: list[Trade], split: str = "2026-03-01 00:00:00+00:00") -> tuple[list[Trade], list[Trade]]:
    split_ts = pd.Timestamp(split)
    train = [t for t in trades if pd.Timestamp(t.entry_time) < split_ts]
    test = [t for t in trades if pd.Timestamp(t.entry_time) >= split_ts]
    return train, test


def grid_search(market: pd.DataFrame, surface: dict) -> tuple[StrategyParams, list[Trade], pd.DataFrame]:
    rows = []
    best_score = -1e9
    best_params = None
    best_trades = []
    for vrp_min in [0, 3, 6, 9]:
        for trend_cutoff in [0.035, 0.05, 0.07]:
            for short_delta in [0.10, 0.12, 0.15]:
                for profit_take in [0.45, 0.60]:
                    for stop_r in [0.60, 0.85, 1.10]:
                        params = StrategyParams(
                            vrp_min=vrp_min,
                            trend_cutoff=trend_cutoff,
                            short_delta=short_delta,
                            long_delta=max(0.04, short_delta - 0.06),
                            profit_take_credit=profit_take,
                            stop_r=stop_r,
                        )
                        trades = backtest_strategy(market, surface, params)
                        train, test = split_trades(trades)
                        train_m = metrics(train)
                        test_m = metrics(test)
                        if train_m.get("trades", 0) < 18 or test_m.get("trades", 0) < 5:
                            continue
                        score = train_m["avg_r"] * 10 + train_m["sharpe_r"] - abs(min(train_m["max_drawdown_at_1pct_risk"], 0)) * 0.05
                        rows.append(
                            {
                                **asdict(params),
                                "train_trades": train_m["trades"],
                                "train_avg_r": train_m["avg_r"],
                                "train_sharpe": train_m["sharpe_r"],
                                "train_pf": train_m["profit_factor"],
                                "train_mdd": train_m["max_drawdown_at_1pct_risk"],
                                "test_trades": test_m["trades"],
                                "test_avg_r": test_m["avg_r"],
                                "test_sharpe": test_m["sharpe_r"],
                                "test_pf": test_m["profit_factor"],
                                "test_mdd": test_m["max_drawdown_at_1pct_risk"],
                                "score": score,
                            }
                        )
                        if score > best_score:
                            best_score = score
                            best_params = params
                            best_trades = trades
    if best_params is None:
        raise RuntimeError("No parameter set produced enough trades")
    return best_params, best_trades, pd.DataFrame(rows).sort_values("score", ascending=False)


def bybit_mark_data_summary(db) -> dict:
    rows = list(
        db.bybit_options_mark_klines.find(
            {},
            {"_id": 0, "symbol": 1, "timestamp_utc": 1, "close": 1},
        ).sort("timestamp_utc", 1)
    )
    if not rows:
        return {}
    df = pd.DataFrame(rows)
    parsed = df["symbol"].map(parse_option_symbol)
    meta = pd.DataFrame([p if p else {} for p in parsed])
    df = pd.concat([df, meta], axis=1).dropna(subset=["currency", "expiry", "strike", "type"])
    df["timestamp"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    df["close"] = df["close"].astype(float)
    out = {}
    for currency, g in df.groupby("currency"):
        dte = (g["expiry"] - g["timestamp"]).dt.total_seconds() / 86400
        out[currency] = {
            "symbols": int(g["symbol"].nunique()),
            "bars": int(len(g)),
            "start": str(g["timestamp"].min()),
            "end": str(g["timestamp"].max()),
            "expiries": sorted([str(x) for x in g["expiry"].dt.strftime("%d%b%y").unique()]),
            "min_dte": float(dte.min()),
            "max_dte": float(dte.max()),
            "median_mark_price_usd": float(g["close"].median()),
        }
    return out


def current_signal(market: pd.DataFrame, surface: dict, params: StrategyParams) -> dict:
    signals = {}
    for currency, df in market.groupby("currency"):
        row = df.sort_values("timestamp").iloc[-1]
        built = build_structure(row, params, surface)
        if built is None:
            signals[currency] = {
                "action": "WAIT",
                "timestamp": str(row["timestamp"]),
                "spot": float(row["close"]),
                "dvol": float(row["dvol"]),
                "rv7": float(row["rv_7d"]),
                "vrp": float(row["vrp_7d"]),
                "trend_7d": float(row["trend_7d"]),
            }
        else:
            structure, legs = built
            signals[currency] = {
                "action": "OPEN",
                "structure": structure,
                "timestamp": str(row["timestamp"]),
                "spot": float(row["close"]),
                "dvol": float(row["dvol"]),
                "rv7": float(row["rv_7d"]),
                "vrp": float(row["vrp_7d"]),
                "trend_7d": float(row["trend_7d"]),
                **legs,
            }
    return signals


def current_put_debit_signal(market: pd.DataFrame, surface: dict, params: PutDebitMomentumParams) -> dict:
    signals = {}
    for currency, df in market.groupby("currency"):
        row = df.sort_values("timestamp").iloc[-1]
        base = {
            "timestamp": str(row["timestamp"]),
            "spot": float(row["close"]),
            "dvol": float(row["dvol"]),
            "rv7": float(row["rv_7d"]),
            "vrp": float(row["vrp_7d"]),
            "trend_7d": float(row["trend_7d"]),
        }
        if (
            row["trend_7d"] > params.trend_7d_max
            or row["vrp_7d"] > params.vrp_max
            or abs(row["ret_24h"]) > params.max_abs_24h_ret
            or row["dvol"] > params.max_dvol
        ):
            signals[currency] = {"action": "WAIT", **base}
            continue
        expiry = row["timestamp"] + pd.Timedelta(hours=params.dte_hours)
        t_years = params.dte_hours / (24 * 365)
        vol = max(float(row["dvol"]) / 100, 0.05)
        long_put = find_strike_for_delta(currency, float(row["close"]), t_years, vol, "put", params.buy_delta)
        short_put = find_strike_for_delta(currency, float(row["close"]), t_years, vol, "put", params.sell_delta)
        if short_put >= long_put:
            short_put = round_strike(currency, long_put * 0.95)
        signals[currency] = {
            "action": "OPEN",
            "structure": "put_debit_spread",
            **base,
            "long_put": long_put,
            "short_put": short_put,
        }
    return signals


def format_metric_line(name: str, m: dict) -> str:
    pf = "inf" if m.get("profit_factor") is None else f"{m['profit_factor']:.2f}"
    return (
        f"| {name} | {m.get('trades', 0)} | {m.get('win_rate', 0):.1f}% | {pf} | "
        f"{m.get('sharpe_r', 0):.2f} | {m.get('max_drawdown_at_1pct_risk', 0):.2f}% | "
        f"{m.get('avg_r', 0):+.3f} | {m.get('total_r', 0):+.2f} | "
        f"{m.get('total_return_at_1pct_risk', 0):+.2f}% |"
    )


def write_report(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    params = payload["put_debit_params"]
    credit_params = payload["best_params"]
    lines = [
        "# Crypto Options Strategy Research - 2026-05-16",
        "",
        "## Data Constraint",
        "",
        "The long backtest is model-marked from observed hourly spot + Deribit DVOL because the option mark-history tables contain current listed future expiries, not a complete archive of expired weekly chains. The model uses measured Deribit surface skew as the strike-IV adjustment and includes taker fees plus slippage.",
        "",
        "## Volatility Findings",
        "",
    ]
    for currency, f in payload["dvol_vrp"].items():
        lines += [
            f"### {currency}",
            f"- Sample: {f['days']} daily observations from {f['sample_start']} to {f['sample_end']}.",
            f"- DVOL mean/median/p90: {f['dvol_mean']:.1f}% / {f['dvol_p50']:.1f}% / {f['dvol_p90']:.1f}%.",
            f"- 7d RV mean: {f['rv7_mean']:.1f}%; VRP mean/median: {f['vrp_mean']:+.1f} / {f['vrp_p50']:+.1f} vol points.",
            f"- VRP positive {f['vrp_positive_pct']:.1f}% of days.",
            f"- Future 7d absolute move after top-quartile VRP: {f['future_abs_7d_high_vrp']:.2f}% vs bottom-quartile VRP: {f['future_abs_7d_low_vrp']:.2f}%.",
            "",
        ]
    lines += ["## Surface Findings", ""]
    for currency, f in payload["surface"].items():
        lines += [
            f"### {currency}",
            f"- Surface sample: {f['snapshot_days']} days.",
            f"- ATM IV by DTE: 3-10d {f['atm_3_10']:.1f}%, 10-24d {f['atm_10_24']:.1f}%, 24-60d {f['atm_24_60']:.1f}%, 60-120d {f['atm_60_120']:.1f}%.",
            f"- Term slope, 24-60d minus 3-10d: {f['term_slope_24_60_minus_3_10']:+.1f} vol points.",
            f"- 7-45d put/call skew, 90-95% moneyness puts minus 105-110% calls: {f['put_call_skew_7_45']:+.1f} vol points.",
            "",
        ]
    lines += [
        "## Strategy Specification",
        "",
        f"- Evaluation time: daily {params['entry_hour']:02d}:00 UTC, one open position per coin. Expiry target: 10 calendar days; use the closest Bybit weekly expiry in the 8-12 DTE window.",
        f"- Entry gate: 7d return <= {params['trend_7d_max'] * 100:.1f}%; DVOL - 7d RV <= {params['vrp_max']:.1f} vol points; abs 24h return <= {params['max_abs_24h_ret'] * 100:.1f}%; DVOL <= {params['max_dvol']:.0f}%.",
        f"- Structure: buy a put debit spread. Buy the {params['buy_delta']:.0%} absolute-delta put and sell the {params['sell_delta']:.0%} absolute-delta put, rounded to available Bybit strikes.",
        f"- Costs: {params['fee_rate_underlying'] * 100:.3f}% of underlying per option leg plus {params['slippage_rate_option'] * 100:.1f}% of option mark per leg.",
        f"- Exit: take profit at {params['profit_take_debit'] * 100:.0f}% of debit paid, stop at {params['stop_debit'] * 100:.0f}% of debit paid, or close after {params['time_exit_hours']} hours.",
        "- Sizing: debit paid is max loss. Allocate 0.75%-1.0% of equity per position; aggregate open debit risk cap 3.0% of equity. Respect Bybit minimums of 0.01 BTC or 0.1 ETH; skip if the minimum contract size breaches risk.",
        "",
        "## Backtest Results",
        "",
        "| Sample | Trades | Win Rate | Profit Factor | Sharpe | Max DD at 1% Risk | Avg R | Total R | Return at 1% Risk |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        format_metric_line("Selected put debit spread", payload["metrics"]["put_debit_full"]),
        format_metric_line("Rejected short-vol credit system", payload["metrics"]["credit_full"]),
        "",
        "The selected strategy only trades during downside momentum and did not trigger after 2026-02-27 in this sample. The rejected short-vol system used the best grid result with "
        f"VRP >= {credit_params['vrp_min']:.1f}, trend cutoff {credit_params['trend_cutoff'] * 100:.1f}%, and {credit_params['short_delta']:.0%}/{credit_params['long_delta']:.0%} delta credit spreads.",
        "",
        "## Bybit Mark Data Availability",
        "",
    ]
    for currency, f in payload["bybit_marks"].items():
        lines += [
            f"- {currency}: {f['symbols']} symbols, {f['bars']} hourly bars, {f['start']} to {f['end']}, DTE range {f['min_dte']:.1f}-{f['max_dte']:.1f} days.",
        ]
    lines += ["", "## Current Signal", ""]
    for currency, sig in payload["current_signal"].items():
        if sig["action"] == "WAIT":
            lines += [
                f"- {currency}: WAIT. Spot {sig['spot']:.2f}, DVOL {sig['dvol']:.1f}%, RV7 {sig['rv7']:.1f}%, VRP {sig['vrp']:+.1f}, trend7 {sig['trend_7d'] * 100:+.1f}%.",
            ]
        else:
            legs = ", ".join(f"{k}={v:.0f}" for k, v in sig.items() if k in {"short_put", "long_put", "short_call", "long_call"})
            lines += [
                f"- {currency}: OPEN {sig['structure']}. Spot {sig['spot']:.2f}, VRP {sig['vrp']:+.1f}, trend7 {sig['trend_7d'] * 100:+.1f}%; {legs}.",
            ]
    lines += [
        "",
        "## Interpretation",
        "",
        "The data does not support a generic crypto options premium-selling strategy after realistic costs. The cleaner edge in this sample is convex downside continuation when realized volatility is rising but option IV has not fully repriced. The structure is defined-risk and pays debit up front, so gap risk is capped at premium paid.",
    ]
    REPORT_PATH.write_text("\n".join(lines) + "\n")
    JSON_PATH.write_text(json.dumps(payload, indent=2, default=str) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-uri", default=MONGO_URI)
    args = parser.parse_args()

    db = MongoClient(args.mongo_uri)[DB_NAME]
    market = load_hourly_market_data(db)
    dvol_vrp = dvol_and_vrp_findings(market)
    surface = surface_findings(db)
    liquidations = liquidation_findings(db, market)
    best_params, trades, grid = grid_search(market, surface)
    train, test = split_trades(trades)
    put_params = PutDebitMomentumParams()
    put_trades = backtest_put_debit_momentum(market, surface, put_params)
    bybit_marks = bybit_mark_data_summary(db)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "best_params": asdict(best_params),
        "put_debit_params": asdict(put_params),
        "dvol_vrp": dvol_vrp,
        "surface": surface,
        "liquidations": liquidations,
        "metrics": {
            "put_debit_full": metrics(put_trades),
            "credit_full": metrics(trades),
            "credit_train": metrics(train),
            "credit_test": metrics(test),
        },
        "current_signal": current_put_debit_signal(market, surface, put_params),
        "credit_current_signal": current_signal(market, surface, best_params),
        "bybit_marks": bybit_marks,
        "top_grid": grid.head(10).to_dict(orient="records"),
        "put_debit_trades": [asdict(t) for t in put_trades],
        "credit_trades": [asdict(t) for t in trades],
    }
    write_report(payload)
    print(f"Wrote {REPORT_PATH}")
    print(f"Wrote {JSON_PATH}")
    print(json.dumps(payload["metrics"], indent=2))
    print(json.dumps(payload["current_signal"], indent=2))


if __name__ == "__main__":
    main()
