#!/usr/bin/env python
"""
v18_forward_holdout.py -- DECISIVE out-of-sample forward-holdout test of V18.

V18 = cohort net-book strategy (distinct from the live V17 copy-mirror). It was
INVEST-CONFIRMED on two non-overlapping in-sample folds (fold_A 2026-03-15..04-15
+77.2 bps null p99.5; fold_B 2026-04-16..05-23 +66.6 bps null p99.5). Both folds
END 2026-05-23. The strategy has NEVER been tested on the LIVE-FORWARD HOLDOUT
2026-05-23..06-10 -- the SAME OOS window where agentM killed 4 of 5 in-sample
copy edges (E2-E5). This forward test is decisive.

METHOD: held-out APPLICATION of the FIXED, validated config. Thresholds are NOT
re-optimized: p90/p10 entry, p40-p60 band exit, 72h cap -- exactly as validated.
This is a NEW file; it imports the validation sim's functions verbatim (same NB
build from cohort open intervals, same causal trailing-30d percentile with 168h
warmup, same execution model: taker 4.32 bps one-way + per-coin slippage entry
AND exit, hourly funding from mongo). NB is rebuilt from the forward cohort's
open intervals the SAME way the validation sim does for the in-sample folds.

PRE-REGISTERED GATE (locked before looking at the forward number):
  V18 HOLDS OOS iff
    (a) forward pooled mean net bps >= +20  (conservative survival bar, well
        below the +66-77 in-sample), AND
    (b) a 200-draw random-direction null shows the forward result at >= p95, AND
    (c) >= 30 trades in the forward window.
  WEAKENED if positive but below those bars; DEAD if <= 0 or null-insignificant
  (pctile < 95 with a positive mean but failing the gate => WEAKENED; mean <= 0
  or null pctile < 50 => DEAD).

Coverage (verified before run): forward trades 2026-05-23 00:22 .. 2026-06-10
23:19 (977 cohort trades, 10 coins); marks through 2026-06-11 08:54; funding
through 2026-06-11 23:00. Forward grid 2026-05-23..06-11 = 457 h; 168 h warmup
carved from the front (no pre-window cohort history, identical to validation) ->
first entry bar 2026-05-30 00:00, 288 entry-eligible hours.

Outputs: /tmp/v18_forward_holdout.md, /tmp/v18_forward_trades.csv. READ-ONLY on
all data/mongo. Does NOT modify the validation sim.
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab")

# Import the validated sim's functions VERBATIM so the forward NB/percentile/
# exit/execution logic is byte-for-byte the same code path as the in-sample run.
from research.v18.v18_validation_sim import (  # noqa: E402
    BASE_P,
    BASE_STYLE,
    COINS,
    HOUR_MS,
    N_NULL_DRAWS,
    NULL_SEED,
    PCT_MINW_H,
    SENS_PS,
    SENS_STYLES,
    TOP_DECILE_MAX_RANK,
    FundIdx,
    Marks,
    agg_stats,
    f1,
    fmt_pooled,
    hourly_grid,
    max_drawdown,
    mtm_equity,
    net_book,
    run_null,
    simulate,
    staleness_shift,
    trailing_pct,
)

MONGO_URI = "mongodb://localhost:27017"
FWD_PQ = "/Users/hermes/quants-lab/app/data/v16/forward_trades_enriched.parquet"
REPORT_MD = "/tmp/v18_forward_holdout.md"
TRADES_CSV = "/tmp/v18_forward_trades.csv"

# Single forward fold. grid spans the data start (warmup-only zone) -> just past
# the last forward exit. test_start = grid start (warmup carved from the front,
# identical to how validation handles fold_A / fold_B).
FWD = dict(src="forward", grid=("2026-05-23", "2026-06-11"), test_start="2026-05-23")

# Pre-registered gate thresholds (LOCKED before looking at the forward result).
GATE_MEAN_BPS = 20.0
GATE_NULL_PCTILE = 95.0
GATE_MIN_TRADES = 30


def main() -> None:
    from pymongo import MongoClient

    df = pd.read_parquet(
        FWD_PQ, columns=["fold", "wallet", "rank", "coin", "dir", "entry_ts", "exit_ts"]
    )
    marks = {c: Marks.load(c) for c in COINS}

    # Funding window covers the forward grid PLUS the +24h staleness shift.
    f_t0 = int(pd.Timestamp("2026-05-20", tz="UTC").value // 10**6)
    f_t1 = int(pd.Timestamp("2026-06-12", tz="UTC").value // 10**6)
    coll = MongoClient(MONGO_URI)["quants_lab"]["hyperliquid_funding_rates"]
    funds = {c: FundIdx.from_mongo(coll, c, f_t0, f_t1) for c in COINS}
    for c in COINS:
        if len(funds[c].ts) == 0:
            raise RuntimeError(f"no funding events for {c} -- refusing to sim without funding")

    grid = hourly_grid(*FWD["grid"])
    test_start_ms = int(pd.Timestamp(FWD["test_start"], tz="UTC").value // 10**6)
    i_test = int(np.searchsorted(grid, test_start_ms, side="left"))
    enter_from = max(PCT_MINW_H, i_test)  # warmup AND test window
    first_entry_dt = pd.to_datetime(grid[enter_from], unit="ms", utc=True)

    fdf = df[df["fold"] == FWD["src"]]
    fdf_ex = fdf[fdf["rank"] > TOP_DECILE_MAX_RANK]

    grid_px = {}
    pct = {}  # (coin, variant) -> percentile series
    nb_diag_rows = []
    for c in COINS:
        grid_px[c] = marks[c].asof(grid)
        cdf = fdf[fdf["coin"] == c]
        nb_full = net_book(cdf, grid)
        nb_ex = net_book(fdf_ex[fdf_ex["coin"] == c], grid)
        pct[(c, "full")] = trailing_pct(nb_full)
        pct[(c, "ex_top10")] = trailing_pct(nb_ex)
        p = pct[(c, "full")]
        ok = ~np.isnan(p)
        nb_diag_rows.append(
            dict(
                coin=c,
                n_trades=len(cdf),
                nb_min=int(nb_full.min()),
                nb_max=int(nb_full.max()),
                pct_ge_p90=100 * float((p[ok] >= 0.90).mean()) if ok.any() else np.nan,
                pct_le_p10=100 * float((p[ok] <= 0.10).mean()) if ok.any() else np.nan,
                corr_ex=float(np.corrcoef(nb_full, nb_ex)[0, 1])
                if (nb_full.std() > 0 and nb_ex.std() > 0)
                else np.nan,
            )
        )
    nb_diag = pd.DataFrame(nb_diag_rows)

    def run_config(p_hi: float, style: str, variant: str) -> list[dict]:
        trs: list[dict] = []
        for c in COINS:
            tr = simulate(
                pct[(c, variant)], grid, grid_px[c], c, p_hi, style, enter_from, funds[c]
            )
            for t in tr:
                t["fold"] = "forward"
            trs.extend(tr)
        return trs

    # ---- BASE config (full NB), FIXED validated thresholds
    base = run_config(BASE_P, BASE_STYLE, "full")

    # per coin + pooled with funding-inclusive MTM portfolio DD
    base_rows = []
    port_eq = np.zeros(len(grid))
    for c in COINS:
        tr = [t for t in base if t["coin"] == c]
        eq = mtm_equity(tr, grid_px[c], grid, c, funds[c])
        port_eq += eq
        st = agg_stats(tr)
        st.update(coin=c, maxdd=max_drawdown(eq))
        base_rows.append(st)
    pooled = agg_stats(base)
    pooled.update(coin="POOLED", maxdd=max_drawdown(port_eq))
    base_df = pd.DataFrame(base_rows)

    exit_mix = (
        pd.Series([t["exit_reason"] for t in base]).value_counts().to_dict() if base else {}
    )
    long_share = float(np.mean([t["dir"] == 1 for t in base])) if base else np.nan

    # ---- sensitivity sweep (REPORTED ONLY; headline verdict uses fixed base)
    sens_rows = []
    for p_hi in SENS_PS:
        for style in SENS_STYLES:
            trs = base if (p_hi == BASE_P and style == BASE_STYLE) else run_config(p_hi, style, "full")
            st = agg_stats(trs)
            st.update(p=p_hi, style=style)
            sens_rows.append(st)
    sens_df = pd.DataFrame(sens_rows)

    # ---- control A: +24h staleness shift of base trades
    shift_out = staleness_shift(base, marks, funds)

    # ---- control B: random-direction null (200 draws)
    rng = np.random.default_rng(NULL_SEED)
    null = run_null(base, rng)

    # ---- concentration: leave-one-cohort-decile-out (drop rank <= 10)
    ex10 = run_config(BASE_P, BASE_STYLE, "ex_top10")
    ex10_stats = agg_stats(ex10)
    ex10_null = run_null(ex10, rng)
    ex10_stats["null_pctile"] = ex10_null["pctile"] if ex10_null else np.nan
    retention = (
        100.0 * ex10_stats["mean"] / pooled["mean"]
        if (ex10_stats["n"] > 0 and pooled["mean"] and pooled["mean"] > 0)
        else np.nan
    )

    # ---- staleness kill check: shifted mean <= 25% of real mean (or <= 0)
    real_m = pooled["mean"]
    sh_m = shift_out["mean"]
    stale_killed = (
        bool(sh_m <= max(0.25 * real_m, 0.0)) if real_m > 0 else bool(sh_m <= 0)
    )

    # ---- PRE-REGISTERED GATE arithmetic
    n = pooled["n"]
    mean_bps = pooled["mean"]
    null_pctile = null["pctile"] if null else np.nan
    cond_mean = (not np.isnan(mean_bps)) and mean_bps >= GATE_MEAN_BPS
    cond_null = (not np.isnan(null_pctile)) and null_pctile >= GATE_NULL_PCTILE
    cond_n = n >= GATE_MIN_TRADES

    if (np.isnan(mean_bps)) or mean_bps <= 0 or (not np.isnan(null_pctile) and null_pctile < 50):
        verdict = "DEAD"
    elif cond_mean and cond_null and cond_n:
        verdict = "HOLDS"
    else:
        verdict = "WEAKENED"

    write_report(
        nb_diag, base_df, pooled, sens_df, shift_out, null, ex10_stats, retention,
        stale_killed, exit_mix, long_share, verdict, enter_from, first_entry_dt, grid,
        cond_mean, cond_null, cond_n,
    )

    if base:
        cols = [
            "coin", "dir", "entry_ts", "exit_ts", "entry_px", "exit_px",
            "entry_pct", "hold_h", "gross_bps", "fund_bps", "net_bps", "exit_reason",
        ]
        pd.DataFrame(base)[cols].to_csv(TRADES_CSV, index=False)

    # ---- console
    print("== V18 FORWARD HOLDOUT 2026-05-23..06-10 (FIXED validated config) ==")
    print(f"first entry bar {enter_from} = {first_entry_dt}")
    print(f"POOLED: n={n} mean {mean_bps:+.1f} bps median {pooled['median']:+.1f}"
          f" total {pooled['total']:+.0f} maxDD {pooled['maxdd']:.0f} hold {pooled['hold']:.1f}h"
          f" win {100 * pooled['win']:.0f}% funding {pooled['fund']:+.2f} bps/tr")
    print(f"exit mix {exit_mix}  long share {long_share:.0%}")
    if null:
        print(f"null: real {null['real']:+.1f} vs {null['null_mean']:+.1f} +/- {null['null_std']:.1f}"
              f" p95 {null['null_p95']:+.1f} max {null['null_max']:+.1f} -> pctile {null['pctile']:.1f}"
              f" z={null['z']:+.2f}")
    print(f"staleness +24h: real {real_m:+.1f} -> {sh_m:+.1f} (n={shift_out['n']}) kill={'YES' if stale_killed else 'NO'}")
    print(f"ex-top10: n={ex10_stats['n']} mean {ex10_stats['mean']:+.1f} retention {retention:.0f}%"
          f" null p{ex10_stats['null_pctile']:.1f}")
    print(f"\nGATE: mean>=+20 [{cond_mean}]  null>=p95 [{cond_null}]  n>=30 [{cond_n}]")
    print(f"VERDICT: {verdict}")
    print(f"\nreport: {REPORT_MD}\ntrades: {TRADES_CSV}")


def write_report(
    nb_diag, base_df, pooled, sens_df, shift_out, null, ex10_stats, retention,
    stale_killed, exit_mix, long_share, verdict, enter_from, first_entry_dt, grid,
    cond_mean, cond_null, cond_n,
) -> None:
    L = []
    a = L.append
    a("# V18 FORWARD HOLDOUT TEST -- cohort net-book strategy (decisive OOS)")
    a("")
    a("Out-of-sample forward-holdout test of V18, the cohort NET-BOOK strategy")
    a("(distinct from the live V17 copy-mirror). V18 was INVEST-CONFIRMED on two")
    a("non-overlapping IN-SAMPLE folds (fold_A 2026-03-15..04-15 +77.2 bps null")
    a("p99.5; fold_B 2026-04-16..05-23 +66.6 bps null p99.5). BOTH folds end")
    a("2026-05-23. This test runs the SAME pipeline on the LIVE-FORWARD HOLDOUT")
    a("2026-05-23..06-10 -- the same OOS window where agentM killed 4 of 5")
    a("in-sample copy edges (E2-E5). Decisive: does +66-77 bps survive true OOS")
    a("or was it fold-fit?")
    a("")
    a("Method: held-out APPLICATION of the FIXED validated config. Thresholds are")
    a("NOT re-optimized (p90/p10 entry, p40-p60 band exit, 72h cap). This file")
    a("imports the validation sim's functions VERBATIM (`net_book`, `trailing_pct`,")
    a("`simulate`, `mtm_equity`, `run_null`, `staleness_shift`) so the NB build,")
    a("causal trailing-30d percentile (168h warmup), exit logic, and execution")
    a("model (taker 4.32 bps one-way + per-coin slippage entry AND exit; hourly")
    a("funding from mongo) are byte-for-byte the in-sample code path.")
    a("")
    a("## Data coverage (verified before the run)")
    a("")
    a("- Forward cohort trades (`app/data/v16/forward_trades_enriched.parquet`,")
    a("  fold='forward'): 977 open intervals, 10 coins, entry_ts 2026-05-23 00:22 ..")
    a("  exit_ts 2026-06-10 23:19. dir +1: 557, -1: 420. ranks 1..100.")
    a("- Marks (`app/data/v15/assetctx_marks_sprint`): 1-min, through 2026-06-11")
    a("  08:54 UTC -- covers the full forward window. (+24h staleness shifts that")
    a("  land after the marks end are dropped by the asof NaN guard, same as in")
    a("  validation.)")
    a("- Funding (mongo `hyperliquid_funding_rates`): hourly, through 2026-06-11")
    a("  23:00 -- covers the forward window plus the +24h shift, all 10 coins.")
    a(f"- Forward grid {FWD['grid'][0]}..{FWD['grid'][1]} = {len(grid)} h; 168 h")
    a("  warmup carved from the front (no pre-window cohort history, identical to")
    a(f"  validation) -> first entry bar {enter_from} = {first_entry_dt}, then")
    a(f"  {len(grid) - enter_from - 1} entry-eligible hours.")
    a("")
    a("## Forward pooled result (BASE config: p90 entry, p40-60 band exit, 72h cap)")
    a("")
    a("| scope | n | mean net bps | median | total bps | maxDD (bps) | hold h | win% | funding bps/tr |")
    a("|-------|---|--------------|--------|-----------|-------------|--------|------|----------------|")
    w = "--" if np.isnan(pooled["win"]) else f"{100 * pooled['win']:.0f}"
    h = "--" if np.isnan(pooled["hold"]) else f"{pooled['hold']:.1f}"
    dd = "--" if np.isnan(pooled["maxdd"]) else f"{pooled['maxdd']:.0f}"
    a(f"| forward | {pooled['n']:.0f} | {f1(pooled['mean'])} | {f1(pooled['median'])} |"
      f" {f1(pooled['total'])} | {dd} | {h} | {w} | {f1(pooled['fund'])} |")
    a("")
    a(f"Exit mix: {exit_mix}. Long share: {long_share:.0%}."
      if not np.isnan(long_share) else "No trades.")
    a("maxDD = drawdown of the 10-sleeve funding-inclusive hourly-MTM portfolio")
    a("equity (bps per 1x sleeve).")
    a("")
    a("### In-sample reference (for context, NOT re-run here)")
    a("")
    a("| fold | n | mean net bps | null pctile |")
    a("|------|---|--------------|-------------|")
    a("| fold_A (03-15..04-15) | 72 | +77.2 | 99.5 |")
    a("| fold_B (04-16..05-23) | 85 | +66.6 | 99.5 |")
    a("")
    a("## Per coin, forward base config")
    a("")
    a("| coin | n | mean | median | total | maxDD | hold h | win% | fund bps |")
    a("|------|---|------|--------|-------|-------|--------|------|----------|")
    for _, r in base_df.iterrows():
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        a(f"| {r['coin']} | {r['n']:.0f} | {f1(r['mean'])} | {f1(r['median'])} |"
          f" {f1(r['total'])} | {r['maxdd']:.0f} | {h} | {w} | {f1(r['fund'])} |")
    a("")
    a("## Signal diagnostics (percentile, full NB)")
    a("")
    a("| coin | cohort trades | NB min | NB max | %h pct>=p90 | %h pct<=p10 | corr(NB, NB_ex10) |")
    a("|------|---------------|--------|--------|-------------|-------------|--------------------|")
    for _, r in nb_diag.iterrows():
        ge = "--" if np.isnan(r["pct_ge_p90"]) else f"{r['pct_ge_p90']:.1f}"
        le = "--" if np.isnan(r["pct_le_p10"]) else f"{r['pct_le_p10']:.1f}"
        ce = "--" if np.isnan(r["corr_ex"]) else f"{r['corr_ex']:.3f}"
        a(f"| {r['coin']} | {r['n_trades']} | {r['nb_min']} | {r['nb_max']} | {ge} | {le} | {ce} |")
    a("")
    a("## Sensitivity (REPORTED ONLY -- headline verdict uses the fixed base)")
    a("")
    a("band = pure band exit; cap48/cap72 = timed only; base = band OR 72h cap.")
    a("")
    a("| p | exit | n | mean bps | median | total | hold h | win% | fund bps |")
    a("|---|------|---|----------|--------|-------|--------|------|----------|")
    for _, r in sens_df.iterrows():
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        a(f"| {r['p']:.2f} | {r['style']} | {r['n']:.0f} | {f1(r['mean'])} |"
          f" {f1(r['median'])} | {f1(r['total'])} | {h} | {w} | {f1(r['fund'])} |")
    a("")
    a("## Control A: +24h staleness shift (base trades, repriced + re-funded)")
    a("")
    a("| real mean | shifted mean | shifted median | shifted total | n | killed? |")
    a("|-----------|--------------|----------------|---------------|---|---------|")
    a(f"| {pooled['mean']:+.1f} | {f1(shift_out['mean'])} | {f1(shift_out['median'])} |"
      f" {f1(shift_out['total'])} | {shift_out['n']} | {'YES' if stale_killed else 'NO'} |")
    a("")
    a("Kill rule: shifted mean <= 25% of real mean (or <= 0). A real real-time")
    a("signal should DIE under the shift; surviving = the edge was spurious/lagged.")
    a("")
    a("## Control B: random-direction null (200 draws)")
    a("")
    if null:
        a("| real mean | null mean | null std | null p95 | null max | pctile | z |")
        a("|-----------|-----------|----------|----------|----------|--------|---|")
        a(f"| {null['real']:+.2f} | {null['null_mean']:+.2f} | {null['null_std']:.2f} |"
          f" {null['null_p95']:+.2f} | {null['null_max']:+.2f} | {null['pctile']:.1f} |"
          f" {null['z']:+.2f} |")
    else:
        a("No trades -> null not computed.")
    a("")
    a("## Concentration: leave-one-cohort-decile-out (drop wallet rank <= 10)")
    a("")
    a("| n | mean bps | median | total | hold h | win% | null pctile | retention |")
    a("|---|----------|--------|-------|--------|------|-------------|-----------|")
    w = "--" if np.isnan(ex10_stats["win"]) else f"{100 * ex10_stats['win']:.0f}"
    h = "--" if np.isnan(ex10_stats["hold"]) else f"{ex10_stats['hold']:.1f}"
    np_ = "--" if np.isnan(ex10_stats["null_pctile"]) else f"{ex10_stats['null_pctile']:.1f}"
    ret_s = "--" if np.isnan(retention) else f"{retention:.0f}%"
    a(f"| {ex10_stats['n']} | {f1(ex10_stats['mean'])} | {f1(ex10_stats['median'])} |"
      f" {f1(ex10_stats['total'])} | {h} | {w} | {np_} | {ret_s} |")
    a("")
    a("## PRE-REGISTERED GATE (locked before looking at the forward number)")
    a("")
    a("V18 HOLDS OOS iff ALL THREE: (a) forward pooled mean >= +20 bps; (b) random")
    a("-direction null pctile >= p95; (c) >= 30 forward trades. WEAKENED if positive")
    a("but below; DEAD if mean <= 0 or null pctile < 50.")
    a("")
    null_pctile = null["pctile"] if null else float("nan")
    a(f"- (a) mean {pooled['mean']:+.1f} bps >= +20 ? **{'PASS' if cond_mean else 'FAIL'}**")
    a(f"- (b) null pctile {null_pctile:.1f} >= 95 ? **{'PASS' if cond_null else 'FAIL'}**")
    a(f"- (c) n {pooled['n']:.0f} >= 30 ? **{'PASS' if cond_n else 'FAIL'}**")
    a("")
    a(f"# VERDICT: {verdict}")
    a("")
    a("## Honest caveats")
    a("")
    a("- SINGLE 18-day forward window (2026-05-23..06-10). After the mandatory 168h")
    a("  warmup carved from the front (the forward cohort table has no pre-window")
    a("  history), only ~288 h / 12 days are entry-eligible. The forward sample is")
    a("  necessarily smaller than either in-sample fold.")
    a("- The forward cohort is HYPE/BTC/ETH/SOL-heavy (384/226/159/128 of 977")
    a("  trades); ADA/AVAX/BNB/CRV/DOGE/LINK have thin cohort books, so several")
    a("  coins may produce 0 or 1 NB-cross trades in the forward window.")
    a("- NB derives from the cohort trade table (rebuilt independently here, same")
    a("  code as validation); a deployment-grade verdict still needs the V15")
    a("  raw-fills rebuild priced through research/v15/execution_model.py.")
    a("- Warmup percentile window is sub-30d (expanding from 168h), same limitation")
    a("  as in-sample fold_A.")
    a("- Funding accrued in bps of entry notional (no price-drift correction on the")
    a("  funding leg; second-order). Window-end forced closes included in stats.")
    a("")
    with open(REPORT_MD, "w") as fh:
        fh.write("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
