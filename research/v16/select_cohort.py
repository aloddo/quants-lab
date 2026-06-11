#!/usr/bin/env python3
"""V16 COHORT SELECTION -- the live wallet-ranking job (weekly re-rank).

Implements EXACTLY the validated fidelity_oos.py selection procedure (codex SHIP gate,
projects/quant/decisions/2026-06-11-copy-ship-to-shadow-codex-gate), but on a single TRAILING window
instead of a train/test split, because the OOS validation already proved that train-window rank carries
forward (fold1 +8.7 / fold2 +8.3 bps taker median vs random -4.7/-5.8).

Procedure (any deviation needs a new codex gate):
  1. Universe: m01_nonerroring_wallets.txt (same file the validation used).
  2. Fills: m02_actions.parquet, trailing --lookback-days ending at --end (default: m02 max date).
  3. Round-trips per wallet via fidelity_replay.roundtrips (position 0 -> nonzero -> 0, entry/exit VWAP).
  4. Edge per wallet via the validated calc: LIQUID majors only (l2_calib_10coin.json keys -- the live
     whitelist MUST equal this set), mark at entry/exit + latency 2s, per-trade net capped +/-500bps,
     taker fee (committed live path). Min --min-rt round-trips in window.
  5. Rank by trailing TAKER edge (identical ordering to maker: same trades, constant fee shift).
  6. Cohort = top decile, floored at MIN_COHORT (30), capped at MAX_COHORT (100).
     HARD BLOCK (exit 2) if fewer than MIN_COHORT qualify.
  7. Emit: config/copy_trader_wallets_v16.json (live engine format) + app/data/v16/cohort_<asof>.parquet
     (full ranked audit) + Mongo v16_cohort_history (rank-at-time, for the decision-clock audit).

Run: python research/v16/select_cohort.py            # defaults: trailing 75d to m02 max
"""
from __future__ import annotations
import argparse, json, sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))

import leadlag_clean_rank_sim as S                      # mark_at + _DATA (assetctx marks)
from fidelity_replay import roundtrips                  # validated round-trip reconstruction
from execution_model import fee_rt, set_latency_ms      # canonical fees (BINDING)
from _streaming_io import install_memory_guard

FEE_T = fee_rt(maker=False)
FEE_M = fee_rt(maker=True)
LIQUID = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())
CAP = 500.0 / 1e4                                       # per-trade net clip (validated)
MIN_COHORT = 30
MAX_COHORT = 100
M02 = S._DATA / "m02_actions.parquet"
OUT_DIR = _REPO / "app" / "data" / "v16"
CONFIG_OUT = _REPO / "config" / "copy_trader_wallets_v16.json"

# ── V16 live engine config (globals + defaults). Numbers reviewed at the codex gate. ────────────────
V16_GLOBAL = {
    "strategy": "v16_top_decile_faithful_copy",
    "sizing_mode": "fixed",            # fixed per-trade $ = matches the validated equal-weighted-trade edge
    "order_size_usd": 50.0,            # BOOK-SIM SIZED (2026-06-11): cohort herds -> natural book at
                                       # $50/trade = p50 $1.9k / p90 $3k gross; caps block only 4% of
                                       # flow (vs 31% at $100, a non-random distortion during max
                                       # herding). Bigger $/trade just hits the same ~$2.9k gross
                                       # envelope while distorting WHICH trades execute.
    "min_entry_notional": 10.0,
    "max_margin_util": 0.60,           # at 10x lev => gross envelope ~0.6*10*equity ~ $2.9k
    "max_daily_loss": -25.0,           # NOTE: inert in engine (validated at init only); real latches below
    "global_stop_pct": 0.12,           # latched flatten-all at -12% (-$58). Book sim: in-sample max DD
                                       # 6.8-7.8% nearly kissed -8%; OOS edge ~4x smaller, similar vol
                                       # -> -8% would trip on normal variance and kill the live test.
                                       # -12% ~ 3.4 daily sigma at $50/trade. Alberto + codex to bless.
    "max_leverage_cap": 10,            # Alberto: up to 10x on liquid majors
    "cooldown_s": 30,
    "exit_poll_s": 10,
    "exit_min_trim_pct": 0.05,
    "exit_min_trim_usd": 3.0,
    "max_chase_bps": 15,
    "max_spread_bps": 20,
    "min_book_depth_usd": 3000,
    "target_equity_max_age_s": 120,
    "mark_max_age_s": 30,
    "margin_reserve_max_lev": 10,
    "trim_over_frac": 0.2,
    "gross_backstop_x": 6.0,           # flatten-all if gross notional > 6x account equity (blow-up guard)
    # V16-specific (consumed by hl_copy_trader_v16.py, ignored by the base engine):
    "coin_whitelist": sorted(LIQUID),  # MUST equal the validated LIQUID set -- selector enforces this
}
V16_DEFAULTS = {
    "group": "v16_top_decile",
    "entry_mode": "instant",           # leader opens -> we enter immediately (taker path)
    "twap_window_s": 0,
    "min_twap_notional": 0,
    "max_addon_multiplier": 1,         # copy the round-trip OPEN only; no add-on stacking
    # PER-COIN CAPS OFF (Alberto msg 9214 + coin_concurrency.py): cohort herds -- median 8 same-coin
    # positions open at typical entry, 84% of flow on HYPE/ETH/SOL/BTC. Any per-coin cap blocks 50-88%
    # of the validated flow non-randomly. Risk is carried at BOOK level: margin util 0.60, gross
    # backstop 6x, latched global stop, per-position SL.
    "max_coin_concentration": 1.0,     # effectively off (book-level controls govern)
    "max_coin_notional_pct": 10.0,     # effectively off (fixed-mode cap disabled by construction)
    "exit_type": "FIRST_CLOSE",        # leader's first close -> we close (faithful exit)
    "sl_bps": -400,                    # protective floor, beyond validated cap; overlay-tested
    "trail_activate_bps": 150,         # rule #7 trailing TP: activate at +150bps...
    "trail_bps": 75,                   # ...give back max 75bps from peak. Overlay-tested.
    "max_hold_s": 172800,              # 48h safety valve (validated holds are multi-hour)
    "exit_twap_min_notional": 50,
}


def load_wallet_fills(uni: set, start_ms: int, end_ms: int) -> dict:
    """Stream m02 (86M rows) in bounded batches -> {wallet: [(ts, coin, signed_size, price), ...]}.
    Identical access pattern to the validated fidelity_oos.py loader (memory-proven on full 18k)."""
    import pyarrow.parquet as pq
    from collections import defaultdict
    pf = pq.ParquetFile(str(M02))
    wf = defaultdict(list)
    for b in pf.iter_batches(batch_size=1_000_000,
                             columns=["wallet", "coin", "ts", "signed_size", "price"]):
        d = b.to_pydict()
        for i in range(len(d["wallet"])):
            w = d["wallet"][i]; t = d["ts"][i]
            if w in uni and start_ms <= t <= end_ms:
                wf[w].append((t, d["coin"][i], d["signed_size"][i], d["price"][i]))
    return wf


def edge(rts, lo, hi, lat_ms, fee):
    """Validated per-wallet edge calc (verbatim from fidelity_oos.edge): mean net bps over round-trips
    entered in [lo,hi), LIQUID only, marks at entry/exit + latency, per-trade clip +/-CAP."""
    nets = []
    for c, dir_, ets, xts, evw, xvw, g in rts:
        if not (lo <= ets < hi) or c not in LIQUID:
            continue
        ent = S.mark_at(c, ets + lat_ms); ex = S.mark_at(c, xts + lat_ms)
        if ent is None or ex is None or ent <= 0:
            continue
        og = max(-CAP, min(CAP, dir_ * (ex - ent) / ent))
        nets.append(og - fee)
    if not nets:
        return (None, 0, None)
    a = np.array(nets)
    return (a.mean() * 1e4, len(a), a.std(ddof=1) * 1e4 if len(a) > 1 else None)


def m02_max_ts_ms() -> int:
    import pyarrow.parquet as pq
    md = pq.ParquetFile(str(M02)).metadata
    names = [md.schema.column(i).name for i in range(len(md.schema))]
    ci = names.index("ts")
    tmax = None
    for rg in range(md.num_row_groups):
        st = md.row_group(rg).column(ci).statistics
        if st and st.has_min_max:
            tmax = st.max if tmax is None else max(tmax, st.max)
    return int(tmax)


def main():
    install_memory_guard(soft_gb=12.0, label="v16_select")
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default=None, help="rank as-of (ISO); default: m02 max ts")
    ap.add_argument("--lookback-days", type=int, default=75)
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-rt", type=int, default=15)
    ap.add_argument("--universe-file", default=str(S._DATA / "m01_nonerroring_wallets.txt"))
    ap.add_argument("--dry-run", action="store_true", help="rank + audit only; no config/Mongo write")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    lat = args.latency_s * 1000

    end_ms = (int(pd.Timestamp(args.end, tz="UTC").timestamp() * 1000) if args.end else m02_max_ts_ms())
    start_ms = end_ms - args.lookback_days * 86_400_000
    asof = pd.Timestamp(end_ms, unit="ms", tz="UTC")
    print(f"V16 SELECT: window {pd.Timestamp(start_ms, unit='ms', tz='UTC')} -> {asof} "
          f"({args.lookback_days}d), latency {args.latency_s}s, min_rt {args.min_rt}, liquid={sorted(LIQUID)}")

    # DATA STALENESS GUARD: warn (not block) when ranking data is old; codex gate set the envelope at
    # ~8 weeks (validation carried edge over a 2-month test window). Block beyond that.
    age_days = (datetime.now(timezone.utc) - asof.to_pydatetime()).days
    if age_days > 56:
        print(f"BLOCK: ranking data is {age_days}d old (> 56d validated staleness envelope). "
              f"Refresh hl_s3_fills_v2 + m02 first.")
        sys.exit(2)
    if age_days > 21:
        print(f"WARNING: ranking data is {age_days}d old; inside the validated envelope but refresh soon.")

    uni = set(l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#"))
    print(f"loading m02 fills for {len(uni)} universe wallets ...")
    wf = load_wallet_fills(uni, start_ms, end_ms)
    print(f"{len(wf)} wallets with fills in window")

    rows = []
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0])
        rts = roundtrips(fl)
        t_edge, t_n, t_std = edge(rts, start_ms, end_ms, lat, FEE_T)
        if t_edge is None or t_n < args.min_rt:
            continue
        m_edge, _, _ = edge(rts, start_ms, end_ms, lat, FEE_M)
        tstat = (t_edge / t_std * np.sqrt(t_n)) if (t_std and t_std > 0) else 0.0
        rows.append({"wallet": w, "taker_bps": t_edge, "maker_bps": m_edge,
                     "n_rt": t_n, "std_bps": t_std, "tstat": tstat})
    df = pd.DataFrame(rows).sort_values("taker_bps", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    print(f"\n{len(df)} rankable wallets (>= {args.min_rt} liquid RTs in window)")
    if df.empty:
        print("BLOCK: no rankable wallets"); sys.exit(2)

    dec = max(MIN_COHORT, len(df) // 10)
    cohort = df.head(min(dec, MAX_COHORT)).copy()
    if len(cohort) < MIN_COHORT:
        print(f"BLOCK: cohort {len(cohort)} < {MIN_COHORT} minimum. Not writing config."); sys.exit(2)

    print(f"\nCOHORT: top {len(cohort)} of {len(df)} (decile={len(df)//10}, floor {MIN_COHORT}, cap {MAX_COHORT})")
    print(f"  cohort trailing taker edge: median {cohort.taker_bps.median():+.1f} mean {cohort.taker_bps.mean():+.1f} bps")
    print(f"  cohort RTs in window: total {int(cohort.n_rt.sum())}, median {int(cohort.n_rt.median())}/wallet")
    print(f"  universe trailing taker edge (all rankable): {df.taker_bps.mean():+.1f} bps mean")
    print("\ntop 10:")
    for r in cohort.head(10).itertuples():
        print(f"  #{r.rank:>3} {r.wallet} taker {r.taker_bps:+7.1f}bps n={r.n_rt} t={r.tstat:+.2f}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = OUT_DIR / f"cohort_{asof.strftime('%Y%m%d')}.parquet"
    df.to_parquet(audit_path)
    print(f"\naudit (full ranking) -> {audit_path}")

    if args.dry_run:
        print("dry-run: no config / Mongo write"); return

    cfg = {
        "global": dict(V16_GLOBAL),
        "defaults": dict(V16_DEFAULTS),
        "wallets": {
            r.wallet: {"group": "v16_top_decile", "rank": int(r.rank),
                       "trail_taker_bps": round(float(r.taker_bps), 2), "trail_n_rt": int(r.n_rt)}
            for r in cohort.itertuples()
        },
    }
    cfg["global"]["cohort_asof"] = asof.isoformat()
    cfg["global"]["cohort_window_days"] = args.lookback_days
    CONFIG_OUT.write_text(json.dumps(cfg, indent=2))
    print(f"config ({len(cohort)} wallets) -> {CONFIG_OUT}")

    try:
        from pymongo import MongoClient
        db = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=3000).quants_lab
        db.v16_cohort_history.insert_one({
            "asof": asof.to_pydatetime(), "created_at": datetime.now(timezone.utc),
            "window_days": args.lookback_days, "latency_s": args.latency_s, "min_rt": args.min_rt,
            "liquid": sorted(LIQUID), "n_rankable": len(df), "n_cohort": len(cohort),
            "cohort": cohort[["wallet", "rank", "taker_bps", "n_rt", "tstat"]].to_dict("records"),
        })
        print("rank-at-time -> mongo v16_cohort_history")
    except Exception as e:
        print(f"WARNING: mongo write failed ({e}); parquet audit is authoritative")


if __name__ == "__main__":
    main()
