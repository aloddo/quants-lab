#!/usr/bin/env python
"""
v18_validation_sim.py -- V18 cohort net-book strategy: VALIDATION-GRADE sim.

Upgrades research/v16/agents/agentB_v18_scout.py by closing its four named
holes. Output feeds a codex gate. Still NOT a deployment verdict: the V15
harness rebuild from raw fills + 6-criteria verdict remain after this.

THE FOUR FIXES vs the scout
---------------------------
1. PER-COIN NB NORMALIZATION (raw |NB|>=3 saturates on BTC 74-83% of hours):
   signal = causal trailing percentile rank of the coin's hourly NB.
   Window = trailing 30 days (720h) of that coin's own NB, expanding from a
   168h minimum warmup (no future data; midrank tie handling because NB is
   small integers). Enter LONG on genuine cross pct >= p90 (SHORT <= p10);
   exit when pct re-enters the p40-p60 band (long: pct <= 0.60, short:
   pct >= 0.40) OR 72h cap. Sweep {p85,p90,p95} x {band, cap48, cap72, base}.
2. NON-OVERLAPPING FOLDS: fold_A test 2026-03-15..2026-04-15 (fold1 cohort),
   fold_B test 2026-04-16..2026-05-23 (fold2 cohort). NB before each test
   window is used for percentile warmup ONLY. Data constraint: the trade
   table starts exactly at each cohort's window start, so warmup is carved
   from the front of each window (fold_A entries begin 2026-03-22, fold_B
   2026-04-22). Test windows are disjoint -> folds are independent samples.
3. NB REBUILT INDEPENDENTLY from app/data/v16/sprint_trades_enriched.parquet
   open intervals (entry_ts..exit_ts, dir), PLUS leave-one-cohort-decile-out:
   the whole pipeline (NB -> percentile -> trades) is rerun with the top-10
   ranked wallets removed. If the signal dies without them ->
   CONCENTRATION RISK flag.
4. EXECUTION + FUNDING: canonical constants (research/v15/execution_model.py
   numbers: taker one-way 4.32 bps; slippage one-way BTC 0.13 / ETH 0.47 /
   SOL 0.12 / others 4.7 bps; paid entry AND exit) PLUS hourly funding
   accrued from mongo quants_lab.hyperliquid_funding_rates over every hold
   (long pays positive rate; events in (entry_ts, exit_ts]; one mongo query
   per coin -> in-memory cumsum index, no per-row round trips).

CONTROLS (kept from the scout)
------------------------------
A. +24h staleness shift of the base trades (reprice AND re-accrue funding)
   -- must kill the edge.
B. random-direction null, 200 draws (same times/coins/costs/funding physics,
   direction random +/-1) -> percentile of the real pooled mean.
C. per-coin AND pooled per fold: n, mean/median net bps, total, maxDD of the
   hourly-MTM sleeve portfolio (funding-inclusive equity), avg hold, win%.

Marks: app/data/v15/assetctx_marks_sprint/{COIN}.npy (2,N)=[ts_ms; px], 1-min,
asof-backward with 10-min staleness guard.

Outputs: /tmp/agentE_v18_validation.md, /tmp/agentE_v18_base_trades.csv.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab")
try:  # mandatory memory-guard compliance (sim is small; guard is cheap)
    from research.v15._streaming_io import install_memory_guard

    install_memory_guard(soft_gb=12.0, label="v18_validation_sim")
except Exception:
    pass

TRADES_PQ = "/Users/hermes/quants-lab/app/data/v16/sprint_trades_enriched.parquet"
MARKS_DIR = "/Users/hermes/quants-lab/app/data/v15/assetctx_marks_sprint"
MONGO_URI = "mongodb://localhost:27017"
REPORT_MD = "/tmp/agentE_v18_validation.md"
TRADES_CSV = "/tmp/agentE_v18_base_trades.csv"

COINS = ["ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"]
HOUR_MS = 3_600_000

# fold -> (source fold label in parquet, NB grid span, test window for entries)
# grid starts at the cohort's data start (warmup-only zone), ends at test end.
FOLDS = {
    "fold_A": dict(src="fold1", grid=("2026-03-15", "2026-04-16"), test_start="2026-03-15"),
    "fold_B": dict(src="fold2", grid=("2026-04-15", "2026-05-23"), test_start="2026-04-16"),
}

FEE_ONEWAY_BPS = 4.32  # HL taker one-way (RT 8.64) -- execution_model.py canon
SLIP_ONEWAY_BPS = {"BTC": 0.13, "ETH": 0.47, "SOL": 0.12}
SLIP_DEFAULT_BPS = 4.7
STALE_MS = 10 * 60 * 1000  # mark older than this vs signal time = stale

PCT_WINDOW_H = 720  # trailing 30d
PCT_MINW_H = 168  # minimum NB history (7d) before any signal -- causal warmup
BAND_LO, BAND_HI = 0.40, 0.60  # neutral band: long exits at <=0.60, short >=0.40

BASE_P = 0.90
BASE_STYLE = "base"  # band exit OR 72h cap (headline config)
SENS_PS = [0.85, 0.90, 0.95]
SENS_STYLES = ["band", "cap48", "cap72", "base"]  # base = band|72h cap
N_NULL_DRAWS = 200
NULL_SEED = 42
SHIFT_H = 24
TOP_DECILE_MAX_RANK = 10  # leave-one-cohort-decile-out: drop rank <= 10


def oneway_cost_bps(coin: str) -> float:
    return FEE_ONEWAY_BPS + SLIP_ONEWAY_BPS.get(coin, SLIP_DEFAULT_BPS)


def rt_cost_bps(coin: str) -> float:
    return 2.0 * oneway_cost_bps(coin)


# ---------------------------------------------------------------- data layer
@dataclass
class Marks:
    ts: np.ndarray
    px: np.ndarray

    @classmethod
    def load(cls, coin: str) -> "Marks":
        a = np.load(f"{MARKS_DIR}/{coin}.npy")
        return cls(ts=a[0], px=a[1])

    def asof(self, t_ms: np.ndarray, stale_ms: float = STALE_MS) -> np.ndarray:
        """Most recent mark at or before t; NaN if none or staler than stale_ms."""
        t = np.atleast_1d(np.asarray(t_ms, dtype=np.float64))
        idx = np.searchsorted(self.ts, t, side="right") - 1
        out = np.full(t.shape, np.nan)
        ok = idx >= 0
        out[ok] = self.px[idx[ok]]
        if stale_ms is not None:
            stale = ok & ((t - self.ts[np.clip(idx, 0, None)]) > stale_ms)
            out[stale] = np.nan
        return out


@dataclass
class FundIdx:
    """Per-coin funding index: cumsum of hourly funding_rate, O(log n) windows."""

    ts: np.ndarray  # sorted event timestamps (ms)
    cum: np.ndarray  # len(ts)+1 prefix sums of funding_rate

    @classmethod
    def from_mongo(cls, coll, coin: str, t0_ms: int, t1_ms: int) -> "FundIdx":
        cur = coll.find(
            {"coin": coin, "timestamp_utc": {"$gte": t0_ms, "$lte": t1_ms}},
            {"timestamp_utc": 1, "funding_rate": 1, "_id": 0},
        ).sort("timestamp_utc", 1)
        rows = list(cur)
        ts = np.array([r["timestamp_utc"] for r in rows], dtype=np.int64)
        rates = np.array([r["funding_rate"] for r in rows], dtype=np.float64)
        return cls(ts=ts, cum=np.concatenate([[0.0], np.cumsum(rates)]))

    def wsum(self, t0, t1) -> np.ndarray | float:
        """Sum of funding_rate over events with ts in (t0, t1]."""
        i0 = np.searchsorted(self.ts, t0, side="right")
        i1 = np.searchsorted(self.ts, t1, side="right")
        return self.cum[i1] - self.cum[i0]


def fund_bps(fund: FundIdx, direction: int, t0: int, t1: int) -> float:
    """Funding PnL in bps of notional over (t0, t1]. Long pays positive rate."""
    return -direction * float(fund.wsum(t0, t1)) * 1e4


def hourly_grid(start: str, end: str) -> np.ndarray:
    t0 = int(pd.Timestamp(start, tz="UTC").value // 10**6)
    t1 = int(pd.Timestamp(end, tz="UTC").value // 10**6)
    return np.arange(t0, t1 + HOUR_MS, HOUR_MS, dtype=np.int64)


def net_book(trades: pd.DataFrame, grid: np.ndarray) -> np.ndarray:
    """NB[t] = sum(dir) over cohort trades with entry_ts <= t < exit_ts."""
    i0 = np.searchsorted(grid, trades["entry_ts"].values, side="left")
    i1 = np.searchsorted(grid, trades["exit_ts"].values, side="left")
    delta = np.zeros(len(grid) + 1, dtype=np.int64)
    np.add.at(delta, i0, trades["dir"].values)
    np.add.at(delta, i1, -trades["dir"].values)
    return np.cumsum(delta[:-1])


def trailing_pct(nb: np.ndarray, window: int = PCT_WINDOW_H, minw: int = PCT_MINW_H) -> np.ndarray:
    """Causal percentile rank of NB[i] within NB[max(0,i-window+1)..i].

    Midrank tie convention: (n_less + 0.5*n_equal) / n_window -- NB is small
    integers, so strict <= or < would whipsaw on ties (a constant-0 window
    must read 0.5 = neutral, not 1.0 or 0.0). NaN until minw samples exist.
    """
    n = len(nb)
    pct = np.full(n, np.nan)
    for i in range(minw - 1, n):
        w = nb[max(0, i - window + 1) : i + 1]
        v = nb[i]
        pct[i] = ((w < v).sum() + 0.5 * (w == v).sum()) / len(w)
    return pct


# ---------------------------------------------------------------- simulator
def simulate(
    pct: np.ndarray,
    grid: np.ndarray,
    px: np.ndarray,
    coin: str,
    p_hi: float,
    style: str,
    enter_from_i: int,
    fund: FundIdx,
) -> list[dict]:
    """One position per coin. Genuine-cross entries on the percentile signal
    (prev inside, cur beyond p_hi / 1-p_hi). Entries only at i >= enter_from_i
    (test window AND warmup satisfied). Fills at the mark asof the signal
    hour; taker fee + slippage one-way on entry AND exit; hourly funding
    accrued over (entry_ts, exit_ts]. Exit styles:
      band  : pct re-enters neutral band (long: pct<=0.60, short: pct>=0.40)
      cap48 : timed exit at 48h only
      cap72 : timed exit at 72h only
      base  : band OR 72h cap (headline config)
    All styles force-close at window end (flagged 'window_end')."""
    out: list[dict] = []
    rt = rt_cost_bps(coin)
    thr_hi, thr_lo = p_hi, 1.0 - p_hi
    pos = 0
    e_i = -1
    e_px = np.nan
    e_pct = np.nan
    last = len(grid) - 1
    for i in range(1, len(grid)):
        cur = pct[i]
        if pos != 0:
            hold_h = (grid[i] - grid[e_i]) / HOUR_MS
            band_hit = (pos > 0 and cur <= BAND_HI) or (pos < 0 and cur >= BAND_LO)
            reason = None
            if style == "band":
                if band_hit:
                    reason = "band"
            elif style == "cap48":
                if hold_h >= 48:
                    reason = "cap"
            elif style == "cap72":
                if hold_h >= 72:
                    reason = "cap"
            elif style == "base":
                if band_hit:
                    reason = "band"
                elif hold_h >= 72:
                    reason = "cap"
            else:
                raise ValueError(style)
            if reason is None and i == last:
                reason = "window_end"
            if reason is not None:
                x_px = px[i]
                if np.isnan(x_px):  # stale-mark hole: close on raw backward mark
                    j = i
                    while j > e_i and np.isnan(px[j]):
                        j -= 1
                    x_px = px[j]
                    reason += "_stalepx"
                gross = pos * (x_px / e_px - 1.0) * 1e4
                fnd = fund_bps(fund, pos, int(grid[e_i]), int(grid[i]))
                out.append(
                    dict(
                        coin=coin,
                        dir=pos,
                        entry_i=e_i,
                        exit_i=i,
                        entry_ts=int(grid[e_i]),
                        exit_ts=int(grid[i]),
                        entry_px=float(e_px),
                        exit_px=float(x_px),
                        entry_pct=float(e_pct),
                        hold_h=float(hold_h),
                        gross_bps=float(gross),
                        fund_bps=float(fnd),
                        net_bps=float(gross - rt + fnd),
                        exit_reason=reason,
                    )
                )
                pos = 0
        if pos == 0 and i != last and i >= enter_from_i:
            prev = pct[i - 1]
            if not (np.isnan(prev) or np.isnan(cur)):
                want = 0
                if prev < thr_hi <= cur:
                    want = 1
                elif prev > thr_lo >= cur:
                    want = -1
                if want != 0 and not np.isnan(px[i]):
                    pos = want
                    e_i = i
                    e_px = px[i]
                    e_pct = cur
    return out


def mtm_equity(
    trades: list[dict], px: np.ndarray, grid: np.ndarray, coin: str, fund: FundIdx
) -> np.ndarray:
    """Hourly mark-to-market equity (bps, 1x notional sleeve), funding-inclusive.
    While open: realized + unrealized - one-way entry cost + funding accrued
    so far; exit-side cost charged at close (inside net_bps)."""
    n = len(grid)
    eq = np.zeros(n)
    pxf = pd.Series(px).ffill().bfill().values
    ow = oneway_cost_bps(coin)
    realized = 0.0
    ptr = 0
    for t in trades:
        e, x = t["entry_i"], t["exit_i"]
        eq[ptr:e] = realized
        seg_px = t["dir"] * (pxf[e:x] / t["entry_px"] - 1.0) * 1e4 - ow
        i_g = np.searchsorted(fund.ts, grid[e:x], side="right")
        i_e = np.searchsorted(fund.ts, t["entry_ts"], side="right")
        seg_fund = -t["dir"] * (fund.cum[i_g] - fund.cum[i_e]) * 1e4
        eq[e:x] = realized + seg_px + seg_fund
        realized += t["net_bps"]
        ptr = x
    eq[ptr:] = realized
    return eq


def max_drawdown(eq: np.ndarray) -> float:
    return float(np.max(np.maximum.accumulate(eq) - eq)) if len(eq) else 0.0


def agg_stats(trades: list[dict]) -> dict:
    if not trades:
        return dict(
            n=0, mean=np.nan, median=np.nan, total=0.0, hold=np.nan, win=np.nan, fund=np.nan
        )
    nb = np.array([t["net_bps"] for t in trades])
    hh = np.array([t["hold_h"] for t in trades])
    fb = np.array([t["fund_bps"] for t in trades])
    return dict(
        n=len(nb),
        mean=float(nb.mean()),
        median=float(np.median(nb)),
        total=float(nb.sum()),
        hold=float(hh.mean()),
        win=float((nb > 0).mean()),
        fund=float(fb.mean()),
    )


# ---------------------------------------------------------------- controls
def run_null(trs: list[dict], rng: np.random.Generator) -> dict | None:
    """Random-direction null. net(dir) = dir*core - rt with
    core = pxmove_bps - funding_window_sum*1e4 (funding is dir-linear, so a
    flipped direction flips the funding PnL too)."""
    if not trs:
        return None
    core = np.array(
        [t["gross_bps"] * t["dir"] - t["dir"] * t["fund_bps"] for t in trs]
    )  # dir*fund_bps = -F*1e4 -> core = pxmove - F*1e4; real net = dir*core - rt
    cost = np.array([rt_cost_bps(t["coin"]) for t in trs])
    real_mean = float(np.mean([t["net_bps"] for t in trs]))
    signs = rng.choice([-1.0, 1.0], size=(N_NULL_DRAWS, len(core)))
    draws = (signs * core - cost).mean(axis=1)
    return dict(
        real=real_mean,
        null_mean=float(draws.mean()),
        null_std=float(draws.std(ddof=1)),
        pctile=100.0 * float((draws < real_mean).mean()),
        z=float((real_mean - draws.mean()) / draws.std(ddof=1)),
        null_p95=float(np.percentile(draws, 95)),
        null_max=float(draws.max()),
    )


def staleness_shift(trs: list[dict], marks: dict[str, Marks], funds: dict[str, FundIdx]) -> dict:
    """Same trades executed +24h late: reprice entry/exit and re-accrue funding."""
    shift_ms = SHIFT_H * HOUR_MS
    nets = []
    for t in trs:
        m = marks[t["coin"]]
        e2, x2 = t["entry_ts"] + shift_ms, t["exit_ts"] + shift_ms
        e_px = m.asof(np.array([e2]))[0]
        x_px = m.asof(np.array([x2]))[0]
        if np.isnan(e_px) or np.isnan(x_px):
            continue
        gross = t["dir"] * (x_px / e_px - 1.0) * 1e4
        fnd = fund_bps(funds[t["coin"]], t["dir"], e2, x2)
        nets.append(gross - rt_cost_bps(t["coin"]) + fnd)
    nets = np.array(nets)
    return dict(
        n=len(nets),
        mean=float(nets.mean()) if len(nets) else np.nan,
        median=float(np.median(nets)) if len(nets) else np.nan,
        total=float(nets.sum()) if len(nets) else 0.0,
    )


# ---------------------------------------------------------------- main run
def main() -> None:
    from pymongo import MongoClient

    df = pd.read_parquet(
        TRADES_PQ, columns=["fold", "wallet", "rank", "coin", "dir", "entry_ts", "exit_ts"]
    )
    marks = {c: Marks.load(c) for c in COINS}

    f_t0 = int(pd.Timestamp("2026-03-01", tz="UTC").value // 10**6)
    f_t1 = int(pd.Timestamp("2026-05-26", tz="UTC").value // 10**6)  # covers +24h shift
    coll = MongoClient(MONGO_URI)["quants_lab"]["hyperliquid_funding_rates"]
    funds = {c: FundIdx.from_mongo(coll, c, f_t0, f_t1) for c in COINS}
    for c in COINS:
        if len(funds[c].ts) == 0:
            raise RuntimeError(f"no funding events for {c} -- refusing to sim without funding")

    grids: dict[str, np.ndarray] = {}
    grid_px: dict[tuple[str, str], np.ndarray] = {}
    pcts: dict[tuple[str, str, str], np.ndarray] = {}  # (fold, coin, variant)
    enter_from: dict[str, int] = {}
    nb_diag_rows = []
    for fold, cfg in FOLDS.items():
        grids[fold] = hourly_grid(*cfg["grid"])
        test_start_ms = int(pd.Timestamp(cfg["test_start"], tz="UTC").value // 10**6)
        i_test = int(np.searchsorted(grids[fold], test_start_ms, side="left"))
        enter_from[fold] = max(PCT_MINW_H, i_test)  # warmup AND test window
        fdf = df[df["fold"] == cfg["src"]]
        fdf_ex = fdf[fdf["rank"] > TOP_DECILE_MAX_RANK]
        for c in COINS:
            grid_px[(fold, c)] = marks[c].asof(grids[fold])
            cdf = fdf[fdf["coin"] == c]
            nb_full = net_book(cdf, grids[fold])
            nb_ex = net_book(fdf_ex[fdf_ex["coin"] == c], grids[fold])
            pcts[(fold, c, "full")] = trailing_pct(nb_full)
            pcts[(fold, c, "ex_top10")] = trailing_pct(nb_ex)
            p = pcts[(fold, c, "full")]
            ok = ~np.isnan(p)
            nb_diag_rows.append(
                dict(
                    fold=fold,
                    coin=c,
                    nb_min=int(nb_full.min()),
                    nb_max=int(nb_full.max()),
                    pct_ge_p90=100 * float((p[ok] >= 0.90).mean()),
                    pct_le_p10=100 * float((p[ok] <= 0.10).mean()),
                    corr_ex=float(np.corrcoef(nb_full, nb_ex)[0, 1])
                    if nb_full.std() > 0 and nb_ex.std() > 0
                    else np.nan,
                )
            )
    nb_diag = pd.DataFrame(nb_diag_rows)

    def run_config(p_hi: float, style: str, variant: str) -> dict[str, list[dict]]:
        by_fold: dict[str, list[dict]] = {}
        for fold in FOLDS:
            trs: list[dict] = []
            for c in COINS:
                tr = simulate(
                    pcts[(fold, c, variant)],
                    grids[fold],
                    grid_px[(fold, c)],
                    c,
                    p_hi,
                    style,
                    enter_from[fold],
                    funds[c],
                )
                for t in tr:
                    t["fold"] = fold
                trs.extend(tr)
            by_fold[fold] = trs
        return by_fold

    # ---- BASE config (full NB): per coin + pooled with funding-inclusive MTM DD
    base = run_config(BASE_P, BASE_STYLE, "full")
    base_rows, pooled_rows = [], []
    for fold in FOLDS:
        port_eq = np.zeros(len(grids[fold]))
        for c in COINS:
            tr = [t for t in base[fold] if t["coin"] == c]
            eq = mtm_equity(tr, grid_px[(fold, c)], grids[fold], c, funds[c])
            port_eq += eq
            st = agg_stats(tr)
            st.update(fold=fold, coin=c, maxdd=max_drawdown(eq))
            base_rows.append(st)
        st = agg_stats(base[fold])
        st.update(fold=fold, coin="POOLED", maxdd=max_drawdown(port_eq))
        pooled_rows.append(st)
    all_tr = base["fold_A"] + base["fold_B"]
    st = agg_stats(all_tr)
    st.update(fold="both", coin="POOLED", maxdd=np.nan)
    pooled_rows.append(st)
    base_df = pd.DataFrame(base_rows)
    pooled_df = pd.DataFrame(pooled_rows)
    exit_mix = (
        pd.Series([t["exit_reason"] for t in all_tr]).value_counts().to_dict() if all_tr else {}
    )
    long_share = float(np.mean([t["dir"] == 1 for t in all_tr])) if all_tr else np.nan

    # ---- sensitivity sweep {p85,p90,p95} x {band,cap48,cap72,base}
    sens_rows = []
    for p_hi in SENS_PS:
        for style in SENS_STYLES:
            bf = base if (p_hi == BASE_P and style == BASE_STYLE) else run_config(p_hi, style, "full")
            for fold in FOLDS:
                st = agg_stats(bf[fold])
                st.update(p=p_hi, style=style, fold=fold)
                sens_rows.append(st)
    sens_df = pd.DataFrame(sens_rows)

    # ---- control A: +24h staleness shift of base trades
    shift_out = {fold: staleness_shift(base[fold], marks, funds) for fold in FOLDS}

    # ---- control B: random-direction null (200 draws)
    rng = np.random.default_rng(NULL_SEED)
    null_out: dict[str, dict] = {}
    for label, trs in (
        ("base fold_A", base["fold_A"]),
        ("base fold_B", base["fold_B"]),
        ("base both", all_tr),
    ):
        r = run_null(trs, rng)
        if r:
            null_out[label] = r

    # ---- fix 3 robustness: leave-one-cohort-decile-out (drop rank <= 10)
    ex10 = run_config(BASE_P, BASE_STYLE, "ex_top10")
    ex10_pooled = {}
    for fold in FOLDS:
        st = agg_stats(ex10[fold])
        r = run_null(ex10[fold], rng)
        st["null_pctile"] = r["pctile"] if r else np.nan
        ex10_pooled[fold] = st
    ex_all = ex10["fold_A"] + ex10["fold_B"]
    st = agg_stats(ex_all)
    r = run_null(ex_all, rng)
    st["null_pctile"] = r["pctile"] if r else np.nan
    ex10_pooled["both"] = st

    # concentration flag: signal "dies" without the top decile if either fold
    # goes non-positive, or pooled retention < 25% of the full-NB pooled mean
    full_both_mean = pooled_df[pooled_df["fold"] == "both"]["mean"].iloc[0]
    ex_both_mean = ex10_pooled["both"]["mean"]
    retention = (
        100.0 * ex_both_mean / full_both_mean
        if (ex10_pooled["both"]["n"] > 0 and full_both_mean and full_both_mean > 0)
        else np.nan
    )
    conc_flag = bool(
        ex10_pooled["both"]["n"] == 0
        or any(
            (ex10_pooled[f]["n"] == 0 or ex10_pooled[f]["mean"] <= 0) for f in FOLDS
        )
        or (not np.isnan(retention) and retention < 25.0)
    )

    # ---- staleness kill check: shifted mean <= 25% of real mean (or <= 0)
    stale_kill = {}
    for fold in FOLDS:
        real_m = pooled_df[pooled_df["fold"] == fold]["mean"].iloc[0]
        sh_m = shift_out[fold]["mean"]
        stale_kill[fold] = bool(sh_m <= max(0.25 * real_m, 0.0)) if real_m > 0 else bool(sh_m <= 0)

    # ---- verdict (mechanical, pre-committed rules; no optimism)
    mA = pooled_df[pooled_df["fold"] == "fold_A"]["mean"].iloc[0]
    mB = pooled_df[pooled_df["fold"] == "fold_B"]["mean"].iloc[0]
    pcA = null_out.get("base fold_A", {}).get("pctile", np.nan)
    pcB = null_out.get("base fold_B", {}).get("pctile", np.nan)
    pcBoth = null_out.get("base both", {}).get("pctile", np.nan)
    killed_all = all(stale_kill.values())
    if (full_both_mean <= 0) or (mA <= 0 and mB <= 0) or (pcBoth < 50):
        call = "DEAD"
    elif (mA > 0 and mB > 0) and (pcA >= 95 and pcB >= 95) and killed_all and not conc_flag:
        call = "INVEST-CONFIRMED"
    else:
        call = "WEAKENED"
    reasons = []
    reasons.append(f"fold_A {mA:+.1f} bps (null p{pcA:.1f})")
    reasons.append(f"fold_B {mB:+.1f} bps (null p{pcB:.1f})")
    sh = ", ".join(f"{f} {shift_out[f]['mean']:+.1f}" for f in FOLDS)
    reasons.append(
        (f"staleness KILLED ({sh})" if killed_all else f"staleness NOT killed ({sh})")
    )
    reasons.append("CONCENTRATION RISK" if conc_flag else f"ex-top10 retains {retention:.0f}%")
    verdict_line = f"{call} -- " + "; ".join(reasons)

    # ---- persist + report
    cols = [
        "fold", "coin", "dir", "entry_ts", "exit_ts", "entry_px", "exit_px",
        "entry_pct", "hold_h", "gross_bps", "fund_bps", "net_bps", "exit_reason",
    ]
    pd.DataFrame(all_tr)[cols].to_csv(TRADES_CSV, index=False)
    write_report(
        nb_diag, base_df, pooled_df, sens_df, shift_out, null_out,
        ex10_pooled, retention, conc_flag, stale_kill, exit_mix, long_share,
        verdict_line, enter_from, grids,
    )

    print(fmt_pooled(pooled_df))
    print()
    print("== VERDICT FRAME (base config: p90 entry, p40-60 band exit, 72h cap) ==")
    for fold in FOLDS:
        r = pooled_df[pooled_df["fold"] == fold].iloc[0]
        print(
            f"{fold}: n={r['n']:.0f} pooled mean net {r['mean']:+.1f} bps"
            f" (median {r['median']:+.1f}, total {r['total']:+.0f}, maxDD {r['maxdd']:.0f},"
            f" hold {r['hold']:.1f}h, win {100 * r['win']:.0f}%, funding {r['fund']:+.1f} bps/trade)"
        )
    for k in ("base fold_A", "base fold_B", "base both"):
        v = null_out.get(k)
        if v:
            print(
                f"null[{k}]: real {v['real']:+.1f} vs null {v['null_mean']:+.1f}"
                f" +/- {v['null_std']:.1f} -> pctile {v['pctile']:.1f}, z={v['z']:+.2f}"
            )
    for fold in FOLDS:
        s = shift_out[fold]
        print(
            f"staleness[{fold}]: real {pooled_df[pooled_df['fold'] == fold]['mean'].iloc[0]:+.1f}"
            f" -> +24h {s['mean']:+.1f} bps (n={s['n']}) kill={'YES' if stale_kill[fold] else 'NO'}"
        )
    for fold in ("fold_A", "fold_B", "both"):
        e = ex10_pooled[fold]
        print(
            f"ex-top10[{fold}]: n={e['n']} mean {e['mean']:+.1f} bps"
            f" (null pctile {e['null_pctile']:.1f})"
        )
    print(f"concentration flag: {'FLAGGED' if conc_flag else 'clear'} (retention {retention:.0f}%)")
    print(f"\nVERDICT: {verdict_line}")
    print(f"\nreport: {REPORT_MD}\ntrades: {TRADES_CSV}")


# ---------------------------------------------------------------- reporting
def f1(x) -> str:
    return "--" if (x is None or (isinstance(x, float) and np.isnan(x))) else f"{x:+.1f}"


def fmt_pooled(pooled_df: pd.DataFrame) -> str:
    lines = [
        "| fold | n | mean net bps | median | total bps | maxDD (bps) | hold h | win% | funding bps/tr |",
        "|------|---|--------------|--------|-----------|-------------|--------|------|----------------|",
    ]
    for _, r in pooled_df.iterrows():
        dd = "--" if np.isnan(r["maxdd"]) else f"{r['maxdd']:.0f}"
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        lines.append(
            f"| {r['fold']} | {r['n']:.0f} | {f1(r['mean'])} | {f1(r['median'])} |"
            f" {f1(r['total'])} | {dd} | {h} | {w} | {f1(r['fund'])} |"
        )
    return "\n".join(lines)


def write_report(
    nb_diag, base_df, pooled_df, sens_df, shift_out, null_out,
    ex10_pooled, retention, conc_flag, stale_kill, exit_mix, long_share,
    verdict_line, enter_from, grids,
) -> None:
    L: list[str] = []
    a = L.append
    a("# V18 validation sim: cohort net-book strategy (agent E)")
    a("")
    a("**Validation-grade upgrade of the agent-B scout** -- closes the scout's four")
    a("named holes. Feeds the codex gate; the V15 raw-fills rebuild + 6-criteria")
    a("verdict still follow. Run: `research/v18/v18_validation_sim.py`.")
    a("")
    a("## The four fixes")
    a("")
    a("1. **Per-coin NB normalization**: causal trailing percentile rank of each")
    a("   coin's own hourly NB (30d window, expanding from a 168h minimum, midrank")
    a("   ties). Entry on genuine cross of p90/p10; exit on re-entering the p40-p60")
    a("   band or 72h cap. No raw thresholds -> BTC saturation fixed.")
    a("2. **Non-overlapping folds**: fold_A test 2026-03-15..04-15 (fold1 cohort),")
    a("   fold_B test 2026-04-16..05-23 (fold2 cohort). Pre-test NB = warmup only.")
    a("   The trade table starts at each cohort window start, so warmup is carved")
    a(f"   from the window front: first entries fold_A bar {enter_from['fold_A']}"
      f" ({pd.to_datetime(grids['fold_A'][enter_from['fold_A']], unit='ms')}),")
    a(f"   fold_B bar {enter_from['fold_B']}"
      f" ({pd.to_datetime(grids['fold_B'][enter_from['fold_B']], unit='ms')}).")
    a("3. **NB rebuilt independently** from sprint_trades_enriched open intervals,")
    a("   plus leave-one-cohort-decile-out (drop rank <= 10 wallets, rerun the FULL")
    a("   pipeline NB -> percentile -> trades) for concentration risk.")
    a("4. **Execution + funding**: taker one-way 4.32 bps + slippage one-way BTC")
    a("   0.13 / ETH 0.47 / SOL 0.12 / others 4.7 bps, entry AND exit; hourly")
    a("   funding accrued from mongo hyperliquid_funding_rates over (entry, exit]")
    a("   (long pays positive rate). MTM equity and maxDD are funding-inclusive.")
    a("")
    a("RT cost: BTC 8.90, ETH 9.58, SOL 8.88, others 18.04 bps. Marks: 1-min asof")
    a("with 10-min staleness guard. One position per coin, 1x notional per sleeve.")
    a("")
    a("## Pooled results (BASE config: p90 entry, p40-60 band exit, 72h cap)")
    a("")
    a(fmt_pooled(pooled_df))
    a("")
    a(f"Exit mix (both folds): {exit_mix}. Long share: {long_share:.0%}." if not np.isnan(long_share) else "No trades.")
    a("maxDD pooled = drawdown of the 10-sleeve portfolio equity (sum of per-coin")
    a("funding-inclusive hourly MTM, bps per 1x sleeve). Folds are disjoint in time")
    a("-> 'both' pools two independent samples (different cohorts).")
    a("")
    a("## Per coin, base config")
    a("")
    a("| fold | coin | n | mean | median | total | maxDD | hold h | win% | fund bps |")
    a("|------|------|---|------|--------|-------|-------|--------|------|----------|")
    for _, r in base_df.iterrows():
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        a(
            f"| {r['fold']} | {r['coin']} | {r['n']:.0f} | {f1(r['mean'])} |"
            f" {f1(r['median'])} | {f1(r['total'])} | {r['maxdd']:.0f} | {h} | {w} |"
            f" {f1(r['fund'])} |"
        )
    a("")
    a("## Signal diagnostics (percentile, full NB)")
    a("")
    a("%h beyond p90/p10 is over DEFINED pct hours (post-warmup). corr_ex = corr of")
    a("full NB vs ex-top10 NB on the fold grid.")
    a("")
    a("| fold | coin | NB min | NB max | %h pct>=p90 | %h pct<=p10 | corr(NB, NB_ex10) |")
    a("|------|------|--------|--------|-------------|-------------|--------------------|")
    for _, r in nb_diag.iterrows():
        a(
            f"| {r['fold']} | {r['coin']} | {r['nb_min']} | {r['nb_max']} |"
            f" {r['pct_ge_p90']:.1f} | {r['pct_le_p10']:.1f} | {r['corr_ex']:.3f} |"
        )
    a("")
    a("## Sensitivity: entry percentile x exit style (pooled per fold)")
    a("")
    a("band = pure band exit; cap48/cap72 = timed only; base = band OR 72h cap.")
    a("All force-close at window end.")
    a("")
    a("| p | exit | fold | n | mean bps | median | total | hold h | win% | fund bps |")
    a("|---|------|------|---|----------|--------|-------|--------|------|----------|")
    for _, r in sens_df.iterrows():
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        a(
            f"| {r['p']:.2f} | {r['style']} | {r['fold']} | {r['n']:.0f} | {f1(r['mean'])} |"
            f" {f1(r['median'])} | {f1(r['total'])} | {h} | {w} | {f1(r['fund'])} |"
        )
    a("")
    a("## Control A: +24h staleness shift (base trades, repriced + re-funded)")
    a("")
    a("| fold | real mean | shifted mean | shifted median | shifted total | n | killed? |")
    a("|------|-----------|--------------|----------------|---------------|---|---------|")
    for fold in FOLDS:
        s = shift_out[fold]
        rm = pooled_df[pooled_df["fold"] == fold]["mean"].iloc[0]
        a(
            f"| {fold} | {rm:+.1f} | {f1(s['mean'])} | {f1(s['median'])} |"
            f" {f1(s['total'])} | {s['n']} | {'YES' if stale_kill[fold] else 'NO'} |"
        )
    a("")
    a("Kill rule: shifted mean <= 25% of real mean (or <= 0).")
    a("")
    a("## Control B: random-direction null (200 draws)")
    a("")
    a("Same entry/exit times, coins, costs; direction random +/-1 per trade;")
    a("funding flips with direction (dir-linear).")
    a("")
    a("| scope | real mean | null mean | null std | null p95 | null max | pctile | z |")
    a("|-------|-----------|-----------|----------|----------|----------|--------|---|")
    for k, v in null_out.items():
        a(
            f"| {k} | {v['real']:+.2f} | {v['null_mean']:+.2f} | {v['null_std']:.2f} |"
            f" {v['null_p95']:+.2f} | {v['null_max']:+.2f} | {v['pctile']:.1f} |"
            f" {v['z']:+.2f} |"
        )
    a("")
    a("## Concentration: leave-one-cohort-decile-out (drop wallet rank <= 10)")
    a("")
    a("Full pipeline rerun on ex-top10 NB (percentiles recomputed on the reduced")
    a("book). Flag rule: either fold mean <= 0, or pooled retention < 25%.")
    a("")
    a("| scope | n | mean bps | median | total | hold h | win% | null pctile |")
    a("|-------|---|----------|--------|-------|--------|------|-------------|")
    for fold in ("fold_A", "fold_B", "both"):
        e = ex10_pooled[fold]
        w = "--" if np.isnan(e["win"]) else f"{100 * e['win']:.0f}"
        h = "--" if np.isnan(e["hold"]) else f"{e['hold']:.1f}"
        a(
            f"| {fold} | {e['n']} | {f1(e['mean'])} | {f1(e['median'])} |"
            f" {f1(e['total'])} | {h} | {w} | {e['null_pctile']:.1f} |"
        )
    a("")
    ret_s = "--" if np.isnan(retention) else f"{retention:.0f}%"
    a(f"Pooled retention vs full NB: {ret_s}."
      f" **{'CONCENTRATION RISK FLAGGED' if conc_flag else 'No concentration flag.'}**")
    a("")
    a("## Caveats")
    a("")
    a("- Warmup is carved from each test window's front (the trade table has no")
    a("  pre-window cohort history): fold_A trades 03-22..04-15, fold_B 04-22..05-23.")
    a("  fold_A's percentile window is mostly sub-30d (expanding).")
    a("- Funding accrued in bps of entry notional (no price-drift correction on the")
    a("  funding leg; second-order).")
    a("- NB still derives from the cohort trade table (rebuilt independently here);")
    a("  the V15 harness must rebuild from raw fills and price through")
    a("  research/v15/execution_model.py (live calib), not constants.")
    a("- Window-end forced closes included in stats (flagged in trades CSV).")
    a("- Hourly fills at the signal bar's asof mark; no intra-hour timing.")
    a("")
    a("## Verdict frame (base config)")
    a("")
    for fold in FOLDS:
        r = pooled_df[pooled_df["fold"] == fold].iloc[0]
        nk = f"base {fold}"
        v = null_out.get(nk, {})
        a(
            f"- {fold}: pooled mean net **{r['mean']:+.1f} bps** (n={r['n']:.0f},"
            f" median {r['median']:+.1f}, total {r['total']:+.0f}, maxDD {r['maxdd']:.0f} bps,"
            f" hold {r['hold']:.1f}h, win {100 * r['win']:.0f}%), null pctile"
            f" {v.get('pctile', float('nan')):.1f} (z {v.get('z', float('nan')):+.2f})"
        )
    for fold in FOLDS:
        s = shift_out[fold]
        a(
            f"- staleness {fold}: {pooled_df[pooled_df['fold'] == fold]['mean'].iloc[0]:+.1f}"
            f" -> {s['mean']:+.1f} bps shifted ({'KILLED' if stale_kill[fold] else 'NOT KILLED'})"
        )
    a(
        f"- concentration: ex-top10 fold_A {ex10_pooled['fold_A']['mean']:+.1f} /"
        f" fold_B {ex10_pooled['fold_B']['mean']:+.1f} bps, retention {ret_s}"
        f" ({'FLAGGED' if conc_flag else 'clear'})"
    )
    a("")
    a(f"**{verdict_line}**")
    with open(REPORT_MD, "w") as fh:
        fh.write("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
