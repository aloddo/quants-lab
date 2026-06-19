#!/usr/bin/env python
"""
scaling_verdict_computer.py -- makes the Monday (06-22) scaling verdict MECHANICAL. The sole blocker to Gate 2
($500 MRR) is validation confidence (n>=200 clean closes); the contingent actions (Scenario A/B/C) are already
specified in projects/quant/plans/2026-06-15-capital-scaling-readiness. This pulls the post-reset closes
(exchange truth: v17_exchange_fills closedPnl since reset), computes the $/mo run-rate WITH a day-block
bootstrap confidence interval, and auto-classifies into A/B/C using the CONSERVATIVE lower-CI bound (never
declare Gate 2 on a point estimate). Re-runnable every HB until the verdict lands.

Scenario map (lower-CI $/mo, at current HL equity):
  A  lower-CI >= $500/mo            -> robustly AT Gate 2: add modest buffer, confirm 2-3 wks, declare.
  B  lower-CI in [$260, $500)       -> close: stage equity to ~1.5-2x AND/OR size $150->$250 on liquid subset.
  C  lower-CI < $260 or point<=0    -> edge did not survive: STOP scaling, diagnose execution drag.
Caveat: until n>=200 the verdict is PRELIMINARY (flagged). Read-only.

Run: ~/miniforge3/envs/quants-lab/bin/python scripts/scaling_verdict_computer.py
"""
import numpy as np
from datetime import datetime, timezone
from collections import defaultdict
from pymongo import MongoClient

RESET_MS = 1781512437000   # 2026-06-15 08:33:57 UTC (gross-gate+trim deploy reset)
N_TARGET = 200
B = 5000                   # bootstrap resamples
HL_EQ = 498.0              # current HL spot equity (rule 16); update from portfolio_snapshot


def main():
    db = MongoClient("mongodb://localhost:27017").quants_lab
    fills = list(db.v17_exchange_fills.find({"time": {"$gte": RESET_MS}}, {"closedPnl": 1, "time": 1, "_id": 0}))
    closes = [(int(f["time"]), float(f.get("closedPnl", 0) or 0)) for f in fills if abs(float(f.get("closedPnl", 0) or 0)) > 1e-9]
    n = len(closes)
    now = datetime.now(timezone.utc).timestamp() * 1000
    days = max((now - RESET_MS) / 86400e3, 0.1)
    print(f"=== SCALING VERDICT COMPUTER (post-reset {datetime.fromtimestamp(RESET_MS/1000,tz=timezone.utc):%Y-%m-%d %H:%M}Z, {days:.1f}d) ===")
    print(f"closes n={n}/{N_TARGET} | HL equity ${HL_EQ:.0f}\n")
    if n < 5:
        print("insufficient closes."); return

    pnls = np.array([p for _, p in closes])
    total = pnls.sum()
    usd_mo_point = total / days * 30.4
    print(f"realized ${total:+.2f} | mean ${pnls.mean():+.3f}/close | win {(pnls>0).mean()*100:.0f}% | $/mo point = ${usd_mo_point:+.0f}")

    # close-rate + ETA to n>=200 (the timeline gate): use the RECENT rate (last 48h), not the overall, because
    # the rate decays/varies. Surfaces honestly WHEN the verdict becomes trustworthy (do not assume "Monday").
    ts = np.array([t for t, _ in closes])
    last48 = int((ts > now - 2 * 86400e3).sum())
    recent_rate = max(last48 / 2.0, 0.1)
    overall_rate = n / days
    eta_days = (N_TARGET - n) / recent_rate
    print(f"close-rate: overall {overall_rate:.1f}/day | recent(48h) {recent_rate:.1f}/day | "
          f"ETA to n={N_TARGET}: ~{eta_days:.0f}d (recent rate). {N_TARGET-n} closes to go.")

    # day-block bootstrap: resample whole days (handles intra-day correlation) -> $/mo distribution
    by_day = defaultdict(list)
    for t, p in closes:
        by_day[int((t - RESET_MS) // 86400e3)].append(p)
    day_keys = list(by_day.keys()); ndays_obs = len(day_keys)
    day_sums = {k: sum(v) for k, v in by_day.items()}
    rng = np.random.default_rng(42)
    boot = np.empty(B)
    for i in range(B):
        pick = rng.choice(day_keys, size=ndays_obs, replace=True)
        boot[i] = sum(day_sums[k] for k in pick) / ndays_obs * 30.4   # per-day mean * 30.4 = $/mo
    lo, hi = np.percentile(boot, [5, 95])
    print(f"$/mo day-block bootstrap 90% CI: [${lo:+.0f}, ${hi:+.0f}]  (median ${np.median(boot):+.0f}, {ndays_obs} obs-days)")

    # haircut table (plan)
    print("\nhaircut sensitivity ($/mo at current equity):")
    for h in [1.0, 0.5, 0.33, 0.25]:
        print(f"  {h*100:>3.0f}% of observed -> ${usd_mo_point*h:+.0f}")

    # scenario classification on the CONSERVATIVE lower-CI bound
    print()
    if usd_mo_point <= 0:
        sc, act = "C", "edge negative -> STOP scaling, diagnose execution drag (slippage vs leader fills)."
    elif lo >= 500:
        sc, act = "A", "robustly AT Gate 2 -> add modest equity buffer, confirm 2-3 wks, declare Gate 2."
    elif lo >= 260:
        sc, act = "B", "close -> stage equity to ~1.5-2x AND/OR size $150->$250 on the liquid subset, re-measure 2 wks."
    else:
        sc, act = "C", "lower-CI < $260 -> do NOT scale yet; gather more closes / diagnose if it stalls."
    flag = "PRELIMINARY (n<200, not yet trustworthy)" if n < N_TARGET else "TRUSTWORTHY (n>=200)"
    print(f"VERDICT [{flag}]: SCENARIO {sc} -- {act}")
    print(f"(Gate on the lower-CI bound ${lo:+.0f}, NOT the point ${usd_mo_point:+.0f}. {N_TARGET-n} closes to a trustworthy call.)")
    print("Plan: projects/quant/plans/2026-06-15-capital-scaling-readiness. Any scale = codex + Alberto (rule 13).")


if __name__ == "__main__":
    main()
