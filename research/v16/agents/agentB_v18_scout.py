#!/usr/bin/env python
"""
agentB_v18_scout.py -- V18 lead SCOUT: cohort NET BOOK standalone strategy sim.

SCOUT ANALYSIS ONLY. Output informs whether to invest in a full validation
cycle (/research-process + V15 harness + codex gate). It is NOT a deployment
verdict and must not be read as one.

Hypothesis (brain: projects/quant/research/2026-06-11-v18-lead-cohort-net-book):
the top-100 cohort's NET book direction predicts 72h returns on liquid majors
(aligned-entry 72h bench +87/+62 bps vs against-herd -37/-48, both folds).
Test: a standalone strategy holding each coin in the cohort's net direction,
after real execution costs.

Data:
  trades : app/data/v16/sprint_trades_enriched.parquet
           per-trade entry_ts/exit_ts (ms) per (wallet, coin, dir), per fold
  marks  : app/data/v15/assetctx_marks_sprint/{COIN}.npy  (2,N) row0=ts_ms
           row1=price, 1-min grid

Execution costs (constants from research/v15/execution_model.py -- numbers
only, no live-engine imports per scout guardrail):
  taker fee one-way 4.32 bps (RT 8.64); slippage one-way BTC 0.13 / ETH 0.47 /
  SOL 0.12 / others 4.7 bps. Paid on entry AND exit.

Strategy (base config):
  NB(t) per coin on a 1h grid = net count (long minus short) of cohort
  positions open at t. Enter LONG on genuine cross NB >= +3 (SHORT <= -3).
  Exit when NB sign flips or falls back inside |NB| < 1 (integer NB => NB*dir
  <= 0), or 72h cap, or fold window end (forced, flagged). One position per
  coin. Fills at the 1-min mark asof the signal hour, costs both ways.

Controls (what makes this a scout, not a fantasy):
  A. staleness: same trades executed +24h late (entry and exit shifted).
  B. random-direction null: same entry/exit times and coins, direction
     random +/-1, 200 draws -> null distribution of pooled mean net bps.

Outputs: /tmp/agentB_v18_scout.md (report), /tmp/agentB_v18_base_trades.csv.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab")
try:  # mandatory memory-guard compliance (cheap; this sim is small)
    from research.v15._streaming_io import install_memory_guard

    install_memory_guard(soft_gb=12.0, label="agentB_v18_scout")
except Exception:
    pass

TRADES_PQ = "/Users/hermes/quants-lab/app/data/v16/sprint_trades_enriched.parquet"
MARKS_DIR = "/Users/hermes/quants-lab/app/data/v15/assetctx_marks_sprint"
REPORT_MD = "/tmp/agentB_v18_scout.md"
TRADES_CSV = "/tmp/agentB_v18_base_trades.csv"

COINS = ["ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"]
FOLDS = {
    "fold1": ("2026-03-15", "2026-05-17"),
    "fold2": ("2026-04-15", "2026-05-23"),
}
HOUR_MS = 3_600_000
FEE_ONEWAY_BPS = 4.32  # HL taker, RT 8.64 bps
SLIP_ONEWAY_BPS = {"BTC": 0.13, "ETH": 0.47, "SOL": 0.12}
SLIP_DEFAULT_BPS = 4.7
STALE_MS = 10 * 60 * 1000  # mark older than this vs signal time = stale

BASE_THR = 3
BASE_STYLE = "base"  # band exit (covers sign flip) OR 72h cap
SENS_THRS = [2, 3, 5]
SENS_STYLES = ["flip", "band", "cap48", "cap72"]  # pure variants, see report
NULL_EXTRA_CELLS = [(3, "band"), (5, "band"), (5, "flip")]  # also null-test these
N_NULL_DRAWS = 200
NULL_SEED = 42
SHIFT_H = 24


def rt_cost_bps(coin: str) -> float:
    return 2.0 * (FEE_ONEWAY_BPS + SLIP_ONEWAY_BPS.get(coin, SLIP_DEFAULT_BPS))


def oneway_cost_bps(coin: str) -> float:
    return FEE_ONEWAY_BPS + SLIP_ONEWAY_BPS.get(coin, SLIP_DEFAULT_BPS)


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


# ---------------------------------------------------------------- simulator
def simulate(
    nb: np.ndarray,
    grid: np.ndarray,
    px: np.ndarray,
    coin: str,
    thr: int,
    style: str,
) -> list[dict]:
    """One position per coin. Genuine-cross entries (prev inside, cur beyond
    threshold). Entry/exit filled at the mark asof the signal hour. Returns
    one dict per closed trade. Styles:
      flip  : exit when NB sign strictly flips (NB*pos < 0)
      band  : exit when NB back inside |NB|<1 or flipped (NB*pos <= 0)
      cap48 : timed exit at 48h only
      cap72 : timed exit at 72h only
      base  : band OR 72h cap (the headline config)
    All styles force-close at window end (flagged 'window_end')."""
    out: list[dict] = []
    rt = rt_cost_bps(coin)
    pos = 0
    e_i = -1
    e_px = np.nan
    prev = nb[0]
    last = len(grid) - 1
    for i in range(1, len(grid)):
        cur = nb[i]
        if pos != 0:
            hold_h = (grid[i] - grid[e_i]) / HOUR_MS
            reason = None
            if style == "flip":
                if cur * pos < 0:
                    reason = "flip"
            elif style == "band":
                if cur * pos <= 0:
                    reason = "band"
            elif style == "cap48":
                if hold_h >= 48:
                    reason = "cap"
            elif style == "cap72":
                if hold_h >= 72:
                    reason = "cap"
            elif style == "base":
                if cur * pos <= 0:
                    reason = "band"
                elif hold_h >= 72:
                    reason = "cap"
            else:
                raise ValueError(style)
            if reason is None and i == last:
                reason = "window_end"
            if reason is not None:
                x_px = px[i]
                if np.isnan(x_px):  # stale-guard hole: close on raw backward mark
                    j = i
                    while j > e_i and np.isnan(px[j]):
                        j -= 1
                    x_px = px[j]
                    reason += "_stalepx"
                gross = pos * (x_px / e_px - 1.0) * 1e4
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
                        hold_h=float(hold_h),
                        gross_bps=float(gross),
                        net_bps=float(gross - rt),
                        exit_reason=reason,
                    )
                )
                pos = 0
        if pos == 0 and i != last:
            want = 0
            if prev < thr <= cur:
                want = 1
            elif prev > -thr >= cur:
                want = -1
            if want != 0 and not np.isnan(px[i]):
                pos = want
                e_i = i
                e_px = px[i]
        prev = cur
    return out


def mtm_equity(trades: list[dict], px: np.ndarray, n: int, coin: str) -> np.ndarray:
    """Hourly mark-to-market equity (bps, 1x notional sleeve). While open:
    realized + unrealized - one-way entry cost; exit cost charged at close."""
    eq = np.zeros(n)
    pxf = pd.Series(px).ffill().bfill().values
    ow = oneway_cost_bps(coin)
    realized = 0.0
    ptr = 0
    for t in trades:
        e, x = t["entry_i"], t["exit_i"]
        eq[ptr:e] = realized
        seg = t["dir"] * (pxf[e:x] / t["entry_px"] - 1.0) * 1e4 - ow
        eq[e:x] = realized + seg
        realized += t["net_bps"]
        ptr = x
    eq[ptr:] = realized
    return eq


def max_drawdown(eq: np.ndarray) -> float:
    return float(np.max(np.maximum.accumulate(eq) - eq)) if len(eq) else 0.0


def agg_stats(trades: list[dict]) -> dict:
    if not trades:
        return dict(n=0, mean=np.nan, median=np.nan, total=0.0, hold=np.nan, win=np.nan)
    nb = np.array([t["net_bps"] for t in trades])
    hh = np.array([t["hold_h"] for t in trades])
    return dict(
        n=len(nb),
        mean=float(nb.mean()),
        median=float(np.median(nb)),
        total=float(nb.sum()),
        hold=float(hh.mean()),
        win=float((nb > 0).mean()),
    )


# ---------------------------------------------------------------- main run
def main() -> None:
    df = pd.read_parquet(
        TRADES_PQ, columns=["fold", "wallet", "coin", "dir", "entry_ts", "exit_ts"]
    )
    marks = {c: Marks.load(c) for c in COINS}

    grids: dict[str, np.ndarray] = {}
    grid_px: dict[tuple[str, str], np.ndarray] = {}
    nbs: dict[tuple[str, str], np.ndarray] = {}
    for fold, (s, e) in FOLDS.items():
        grids[fold] = hourly_grid(s, e)
        fdf = df[df["fold"] == fold]
        for c in COINS:
            grid_px[(fold, c)] = marks[c].asof(grids[fold])
            nbs[(fold, c)] = net_book(fdf[fdf["coin"] == c], grids[fold])

    # ---- NB diagnostics
    nb_diag = []
    for fold in FOLDS:
        for c in COINS:
            nb = nbs[(fold, c)]
            nb_diag.append(
                dict(
                    fold=fold,
                    coin=c,
                    nb_min=int(nb.min()),
                    nb_max=int(nb.max()),
                    pct_abs_ge2=100 * float((np.abs(nb) >= 2).mean()),
                    pct_abs_ge3=100 * float((np.abs(nb) >= 3).mean()),
                    pct_abs_ge5=100 * float((np.abs(nb) >= 5).mean()),
                )
            )
    nb_diag = pd.DataFrame(nb_diag)

    # ---- base config per coin + pooled, with MTM DD
    base_trades: dict[str, list[dict]] = {f: [] for f in FOLDS}
    base_rows = []
    fold_port_eq: dict[str, np.ndarray] = {}
    for fold in FOLDS:
        port_eq = np.zeros(len(grids[fold]))
        for c in COINS:
            tr = simulate(
                nbs[(fold, c)], grids[fold], grid_px[(fold, c)], c, BASE_THR, BASE_STYLE
            )
            for t in tr:
                t["fold"] = fold
            base_trades[fold].extend(tr)
            eq = mtm_equity(tr, grid_px[(fold, c)], len(grids[fold]), c)
            port_eq += eq
            st = agg_stats(tr)
            st.update(fold=fold, coin=c, maxdd=max_drawdown(eq))
            base_rows.append(st)
        fold_port_eq[fold] = port_eq
    base_df = pd.DataFrame(base_rows)

    pooled_rows = []
    for fold in FOLDS:
        st = agg_stats(base_trades[fold])
        st.update(fold=fold, coin="POOLED", maxdd=max_drawdown(fold_port_eq[fold]))
        pooled_rows.append(st)
    all_tr = base_trades["fold1"] + base_trades["fold2"]
    st = agg_stats(all_tr)
    st.update(fold="both", coin="POOLED", maxdd=np.nan)
    pooled_rows.append(st)
    pooled_df = pd.DataFrame(pooled_rows)

    exit_mix = (
        pd.Series([t["exit_reason"] for t in all_tr]).value_counts().to_dict()
        if all_tr
        else {}
    )
    long_share = (
        float(np.mean([t["dir"] == 1 for t in all_tr])) if all_tr else np.nan
    )

    # ---- sensitivity grid (pure exit variants)
    sens_rows = []
    sens_trades: dict[tuple[int, str, str], list[dict]] = {}
    for thr in SENS_THRS:
        for style in SENS_STYLES:
            for fold in FOLDS:
                trs: list[dict] = []
                for c in COINS:
                    trs.extend(
                        simulate(
                            nbs[(fold, c)], grids[fold], grid_px[(fold, c)], c, thr, style
                        )
                    )
                if (thr, style) in NULL_EXTRA_CELLS:
                    sens_trades[(thr, style, fold)] = trs
                st = agg_stats(trs)
                st.update(thr=thr, style=style, fold=fold)
                sens_rows.append(st)
    sens_df = pd.DataFrame(sens_rows)

    # ---- control A: +24h staleness shift of base trades
    shift_rows = []
    shift_ms = SHIFT_H * HOUR_MS
    for fold in FOLDS:
        nets = []
        for t in base_trades[fold]:
            m = marks[t["coin"]]
            e_px = m.asof(np.array([t["entry_ts"] + shift_ms]))[0]
            x_px = m.asof(np.array([t["exit_ts"] + shift_ms]))[0]
            if np.isnan(e_px) or np.isnan(x_px):
                continue
            gross = t["dir"] * (x_px / e_px - 1.0) * 1e4
            nets.append(gross - rt_cost_bps(t["coin"]))
        nets = np.array(nets)
        shift_rows.append(
            dict(
                fold=fold,
                n=len(nets),
                mean=float(nets.mean()) if len(nets) else np.nan,
                median=float(np.median(nets)) if len(nets) else np.nan,
                total=float(nets.sum()),
            )
        )
    shift_df = pd.DataFrame(shift_rows)

    # ---- control B: random-direction null (200 draws)
    rng = np.random.default_rng(NULL_SEED)

    def run_null(trs: list[dict]) -> dict | None:
        if not trs:
            return None
        move = np.array([t["gross_bps"] * t["dir"] for t in trs])  # unsigned px move
        cost = np.array([rt_cost_bps(t["coin"]) for t in trs])
        real_mean = float(np.mean([t["net_bps"] for t in trs]))
        signs = rng.choice([-1.0, 1.0], size=(N_NULL_DRAWS, len(move)))
        draws = (signs * move - cost).mean(axis=1)
        return dict(
            real=real_mean,
            null_mean=float(draws.mean()),
            null_std=float(draws.std(ddof=1)),
            pctile=100.0 * float((draws < real_mean).mean()),
            z=float((real_mean - draws.mean()) / draws.std(ddof=1)),
            null_p95=float(np.percentile(draws, 95)),
            null_max=float(draws.max()),
        )

    null_out = {}
    for label, trs in (
        ("base fold1", base_trades["fold1"]),
        ("base fold2", base_trades["fold2"]),
        ("base both", all_tr),
    ):
        r = run_null(trs)
        if r:
            null_out[label] = r
    for thr, style in NULL_EXTRA_CELLS:
        for fold in FOLDS:
            r = run_null(sens_trades.get((thr, style, fold), []))
            if r:
                null_out[f"thr{thr} {style} {fold}"] = r

    # ---- persist + report
    pd.DataFrame(all_tr).to_csv(TRADES_CSV, index=False)
    write_report(
        nb_diag, base_df, pooled_df, sens_df, shift_df, null_out, exit_mix, long_share
    )

    print(fmt_pooled(pooled_df))
    print()
    for k, v in null_out.items():
        print(
            f"null[{k}]: real {v['real']:+.1f} vs null {v['null_mean']:+.1f}"
            f" +/- {v['null_std']:.1f} bps -> pctile {v['pctile']:.1f}, z={v['z']:+.2f}"
        )
    print(f"\nreport: {REPORT_MD}\ntrades: {TRADES_CSV}")


# ---------------------------------------------------------------- reporting
def f1(x) -> str:
    return "--" if (x is None or (isinstance(x, float) and np.isnan(x))) else f"{x:+.1f}"


def fmt_pooled(pooled_df: pd.DataFrame) -> str:
    lines = [
        "| fold | n | mean net bps | median net bps | total bps | maxDD (bps) | avg hold h |",
        "|------|---|--------------|----------------|-----------|-------------|------------|",
    ]
    for _, r in pooled_df.iterrows():
        dd = "--" if np.isnan(r["maxdd"]) else f"{r['maxdd']:.0f}"
        lines.append(
            f"| {r['fold']} | {r['n']} | {f1(r['mean'])} | {f1(r['median'])} |"
            f" {f1(r['total'])} | {dd} | {r['hold']:.1f} |"
        )
    return "\n".join(lines)


def write_report(
    nb_diag, base_df, pooled_df, sens_df, shift_df, null_out, exit_mix, long_share
) -> None:
    L: list[str] = []
    a = L.append
    a("# V18 scout: cohort net-book standalone strategy (agent B)")
    a("")
    a("**SCOUT ANALYSIS ONLY -- informs whether a full validation cycle is justified.")
    a("NOT a deployment verdict.** Run: `research/v16/agents/agentB_v18_scout.py`.")
    a("")
    a("Lead: brain `projects/quant/research/2026-06-11-v18-lead-cohort-net-book`.")
    a("Cohort trades: `app/data/v16/sprint_trades_enriched.parquet`. Marks: 1-min")
    a("`app/data/v15/assetctx_marks_sprint/{COIN}.npy`. Costs: taker RT 8.64 bps +")
    a("slippage one-way BTC 0.13 / ETH 0.47 / SOL 0.12 / others 4.7 bps (entry AND")
    a("exit). Net RT cost: BTC 8.90, ETH 9.58, SOL 8.88, others 18.04 bps.")
    a("")
    a("Base config: 1h NB grid, genuine-cross entry at |NB| >= 3, exit on NB*dir <= 0")
    a("(sign flip or back inside |NB|<1) OR 72h cap OR fold window end (forced).")
    a("One position per coin, 1x notional per sleeve, fills at 1-min mark asof the")
    a("signal hour.")
    a("")
    a("## Pooled results (base config)")
    a("")
    a(fmt_pooled(pooled_df))
    a("")
    a(f"Exit mix (both folds): {exit_mix}. Long share of trades: {long_share:.0%}.")
    a("maxDD pooled row = drawdown of the 10-sleeve portfolio equity (sum of per-coin")
    a("hourly MTM equity, bps per 1x sleeve). fold1/fold2 overlap 2026-04-15..05-17,")
    a("so 'both' is not two independent samples.")
    a("")
    a("## Per coin, base config")
    a("")
    a("| fold | coin | n | mean | median | total | maxDD | hold h | win% |")
    a("|------|------|---|------|--------|-------|-------|--------|------|")
    for _, r in base_df.iterrows():
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        a(
            f"| {r['fold']} | {r['coin']} | {r['n']} | {f1(r['mean'])} |"
            f" {f1(r['median'])} | {f1(r['total'])} | {r['maxdd']:.0f} | {h} | {w} |"
        )
    a("")
    a("## NB diagnostics (signal density)")
    a("")
    a("| fold | coin | NB min | NB max | %h |NB|>=2 | %h |NB|>=3 | %h |NB|>=5 |")
    a("|------|------|--------|--------|-----------|-----------|-----------|")
    for _, r in nb_diag.iterrows():
        a(
            f"| {r['fold']} | {r['coin']} | {r['nb_min']} | {r['nb_max']} |"
            f" {r['pct_abs_ge2']:.1f} | {r['pct_abs_ge3']:.1f} | {r['pct_abs_ge5']:.1f} |"
        )
    a("")
    a("## Sensitivity: threshold x exit style (pooled across coins, per fold)")
    a("")
    a("Exit styles are PURE variants: flip = exit only on NB sign flip; band = exit")
    a("on NB*dir <= 0 (the base NB-exit, no time cap); cap48/cap72 = timed exit only,")
    a("no NB exit. All force-close at window end. Base config = band + 72h cap.")
    a("")
    a("| thr | exit | fold | n | mean bps | median | total | hold h | win% |")
    a("|-----|------|------|---|----------|--------|-------|--------|------|")
    for _, r in sens_df.iterrows():
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        a(
            f"| {r['thr']} | {r['style']} | {r['fold']} | {r['n']} | {f1(r['mean'])} |"
            f" {f1(r['median'])} | {f1(r['total'])} | {h} | {w} |"
        )
    a("")
    a("## Control A: +24h staleness shift (base trades executed a day late)")
    a("")
    a("| fold | n | mean bps | median | total |")
    a("|------|---|----------|--------|-------|")
    for _, r in shift_df.iterrows():
        a(
            f"| {r['fold']} | {r['n']} | {f1(r['mean'])} | {f1(r['median'])} |"
            f" {f1(r['total'])} |"
        )
    a("")
    a("## Control B: random-direction null (200 draws, same times/coins/costs)")
    a("")
    a("Null = same entry/exit timestamps, same coins, same costs, direction random")
    a("+/-1 per trade. Run on the base config and on the strongest sensitivity")
    a("cells (to test whether fold2's base-config weakness is signal or exit-rule).")
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
    a("## Caveats")
    a("")
    a("- Scout-grade sim: hourly fills at the signal bar's asof mark, no intra-hour")
    a("  timing, no funding PnL, no position sizing beyond 1x per sleeve.")
    a("- Fold windows overlap; pooled 'both' double-counts 2026-04-15..05-17 under")
    a("  two different cohorts.")
    a("- NB is built from the same enriched trade file that defined the cohort; a")
    a("  full cycle must rebuild NB from raw fills via the V15 harness and price")
    a("  through research/v15/execution_model.py.")
    a("- Window-end forced closes are included in stats (flagged in trades CSV).")
    a("")
    a("## Verdict")
    a("")
    a("(filled by scout after reading numbers -- see final analysis message)")
    with open(REPORT_MD, "w") as fh:
        fh.write("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
