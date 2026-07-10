#!/usr/bin/env python3
"""v27 Stage 0 — cohort viability screen per FROZEN prereg (projects/quant/v27/prereg-frozen,
sha d75cf501, incl. A1-A5). EXPLORATORY/DESCRIPTIVE ONLY — every number this stage emits.

Grid (A5): blocks 1-18 = 10-day UTC blocks from 2025-12-14 00:00 UTC; cutoff 2026-06-12 00:00 UTC.
G0 (A1.2/A2.2/A3.2/A4): cell survives iff at the initial boundary (end of block 3)
>= 25 RANKABLE entities (assignment minimums + >= 50 realized R2 trips), AND the G0 policy
(top-25 by R2-LCB, equal-weight, E1 mirror, taker, f=1%, g=3) has cross-fitted pooled-daily
LCB > 0 over evaluation blocks 4..18 (rank at boundary k, evaluate block k+1, k=3..17).
Post-initial boundaries: rankable in [15,25) -> THIN (top-rankable roster); < 15 -> cell killed.

PASS A (this file, --pass-a): ONE streaming pass over m02 actions (wallet-sorted).
Per wallet: closed journeys over the full window, entry events (entity clustering),
gate row at the initial boundary, and ONE full-window fixed-$150 FIRST_CLOSE cold-start
copy sim (v25 simulate_wallet_trips, BASE scenario). Boundary slicing is EXACT for
realized trips because the R2 sim is uncoupled (portfolio=False, no caps): a realized
trip's existence and PnL depend only on leader actions in [start, exit], so slicing
realized trips to exit < boundary (strict, half-open) reproduces the boundary-asof
sim's realized trip set.

PASS B (--pass-b): assignment stats per boundary from journeys (median hold via numpy
linear interpolation over closed train journeys, cadence = closed journeys / distinct
UTC exit dates, minimums 20 active days + 30 closed journeys evaluated per boundary);
entity aggregation (v25 cluster_entities at the initial boundary, held fixed thereafter —
declared in manifest); per-boundary rankability + R2-LCB ranking (v25 wallet_lcb frozen
constants); G0 policy evaluation per block k+1 with LINEAR sizing map ($150-trip PnL
scaled by fE/150, E = G0_EQUITY) plus an explicit gross-cap audit: daily gross notional
of the roster's concurrent open trips vs g x E; days where the cap binds are flagged and
the policy return recomputed with entries beyond the cap dropped (deterministic order:
entry ts, then wallet, then coin). Pooled r_d -> studentized stationary block bootstrap
LCB (block 10d, 10k resamples, seed 42, 95% one-sided).

Memory (CLAUDE.md Rule 8): ShardedParquetWriter streaming, install_memory_guard, no
per-row DB access; marks via v25 MarksIndex page cache.

Usage:
  python research/v27/v27_stage0.py --pass-a [--max-wallets N]
  python research/v27/v27_stage0.py --pass-b
  python research/v27/v27_stage0.py --smoke   (pass A limited to 200 wallets into /tmp)
"""
from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "research" / "v25"))
sys.path.insert(0, str(REPO / "research" / "v15"))

from _streaming_io import ShardedParquetWriter, install_memory_guard  # noqa: E402
from v25_common import (MS_DAY, SCENARIOS, MarksIndex, build_journeys,  # noqa: E402
                        iter_wallet_frames, ACTIONS_PARQUET)
from v25_gates import cluster_entities, extract_entries, wallet_gate_row  # noqa: E402
from v25_portfolio_sim import simulate_wallet_trips  # noqa: E402
from v25_r2_lcb import wallet_lcb  # noqa: E402
from v25_bootstrap import stationary_bootstrap_indices  # noqa: E402

# ---- FROZEN grid (prereg A5) ------------------------------------------------------------------
GRID_START_MS = 1765670400000            # 2025-12-14 00:00:00 UTC
BLOCK_MS = 10 * MS_DAY
N_BLOCKS = 18
CUTOFF_MS = GRID_START_MS + N_BLOCKS * BLOCK_MS   # 2026-06-12 00:00 UTC
assert CUTOFF_MS == 1781222400000, "grid arithmetic drifted from A5"

def block_end_ms(k: int) -> int:
    """End (exclusive) of block k, 1-indexed."""
    return GRID_START_MS + k * BLOCK_MS

# ---- FROZEN partition (prereg section 2) ------------------------------------------------------
HOLD_EDGES_H = [0.25, 2.0, 24.0]         # <15m, 15m-2h, 2h-24h, >=24h
CAD_EDGES = [2.0, 10.0]                  # <2, 2-10, >=10 per day
MIN_ACTIVE_DAYS = 20
MIN_CLOSED_JOURNEYS = 30
G0_MIN_RANKABLE_INITIAL = 25
G0_THIN_FLOOR = 15
R2_MIN_TRIPS_RANKABLE = 50
G0_TOP_N = 25
G0_F = 0.01
G0_G = 3.0
G0_EQUITY = 15000.0                      # f x E = $150 => exact reuse of $150 trip sims
BOOT_BLOCK_DAYS = 10
BOOT_RESAMPLES = 10_000
BOOT_SEED = 42

OUT_DIR = REPO / "app" / "data" / "research" / "v27"


def cell_of(median_hold_h: float, cadence: float) -> str:
    hb = int(np.searchsorted(HOLD_EDGES_H, median_hold_h, side="right"))
    cb = int(np.searchsorted(CAD_EDGES, cadence, side="right"))
    return f"H{hb}C{cb}"


# ================================= PASS A ======================================================
def pass_a(out_dir: Path, max_wallets=None, actions_path: Path = ACTIONS_PARQUET) -> dict:
    marks = MarksIndex()
    sc_base = SCENARIOS["BASE"]()
    b3 = block_end_ms(3)
    writers = {n: ShardedParquetWriter(out_dir / f"{n}_full.parquet", flush_rows=500_000)
               for n in ("journeys", "entries", "gates", "r2trips")}
    n = 0
    t0 = time.time()
    for wallet, wdf in iter_wallet_frames(actions_path, max_wallets=max_wallets):
        n += 1
        wdf = wdf.sort_values("ts")
        ts = wdf["ts"].to_numpy()
        tr = wdf.iloc[: int(np.searchsorted(ts, CUTOFF_MS, side="left"))]
        if tr.empty:
            continue
        j = build_journeys(tr)
        if len(j):
            closed = j[j["exit_ts"].notna() & (j["exit_ts"] <= CUTOFF_MS)]
            for r in closed.to_dict("records"):
                r["wallet"] = wallet
                writers["journeys"].add(r)
        # entries only up to the initial boundary (entity clustering is fixed there)
        tr_b3 = tr.iloc[: int(np.searchsorted(tr["ts"].to_numpy(), b3, side="left"))]
        if tr_b3.empty:
            continue  # no activity before the initial boundary: cannot be clustered,
                      # gated, or ranked at b3; journeys already written above
        for (coin, side, ets, w) in extract_entries(wallet, tr_b3):
            writers["entries"].add({"coin": coin, "side": side, "ts": ets, "wallet": w})
        row = wallet_gate_row(wallet, tr_b3, b3, marks, journeys=build_journeys(tr_b3))
        writers["gates"].add(row)
        if row["eligible"]:
            res = simulate_wallet_trips(tr, sc_base, marks, GRID_START_MS, CUTOFF_MS)
            if len(res["trips"]):
                for r in res["trips"].to_dict("records"):
                    r["wallet"] = wallet
                    writers["r2trips"].add(r)
        if n % 250 == 0:
            print(f"  pass A: {n} wallets, {time.time()-t0:.0f}s, "
                  f"rss={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/2**30:.2f}GB",
                  flush=True)
    counts = {k: w.close() for k, w in writers.items()}
    counts["n_wallets"] = n
    print(f"  pass A done: {n} wallets, {time.time()-t0:.0f}s", flush=True)
    return counts


# ================================= PASS B ======================================================
def _wallet_journey_index(journeys: pd.DataFrame) -> dict:
    """wallet -> (exit_ts sorted asc, duration_h aligned). Built ONCE (Rule 8: no
    repeated full-frame filtering per boundary)."""
    idx = {}
    for w, g in journeys.groupby("wallet", sort=False):
        g = g.sort_values("exit_ts")
        idx[w] = (g["exit_ts"].to_numpy(dtype="float64"),
                  g["duration_h"].to_numpy(dtype="float64"))
    return idx


def _assign_cell(jidx: dict, wallet: str, boundary_ms: int) -> str | None:
    """Cell at a boundary from closed journeys with exit_ts < boundary (half-open)."""
    if wallet not in jidx:
        return None
    ex, dur = jidx[wallet]
    n = int(np.searchsorted(ex, boundary_ms, side="left"))   # strict <
    if n == 0:
        return None
    d = dur[:n]
    days = np.unique((ex[:n] // MS_DAY).astype("int64"))
    if len(days) < MIN_ACTIVE_DAYS or n < MIN_CLOSED_JOURNEYS:
        return "INSUF"
    return cell_of(float(np.median(d)), n / len(days))


def _wallet_trip_index(trips: pd.DataFrame) -> dict:
    """wallet -> realized-trip arrays sorted by exit_fill_ts_last (terminal excluded)."""
    t = trips[~trips["terminal"].astype(bool)]
    idx = {}
    for w, g in t.groupby("wallet", sort=False):
        gx = g.sort_values("exit_fill_ts_last")
        ge = g.sort_values("entry_fill_ts")
        idx[w] = {
            # exit-sorted arrays: ranking (boundary slice on exit ts)
            "exit": gx["exit_fill_ts_last"].to_numpy(dtype="int64"),
            "net_bps": gx["net_bps"].to_numpy(dtype="float64"),
            # entry-sorted arrays: policy admission (block slice on entry ts)
            "entry_e": ge["entry_fill_ts"].to_numpy(dtype="int64"),
            "exit_e": ge["exit_fill_ts_last"].to_numpy(dtype="int64"),
            "net_pnl_e": ge["net_pnl"].to_numpy(dtype="float64"),
            "coin_e": ge["coin"].to_numpy(dtype=object),
        }
    return idx


def _rank_entities(lcb_df: pd.DataFrame, entity_map: dict, boundary_k: int,
                   wallets_in_cell: set) -> pd.DataFrame:
    """R2-LCB ranking from the precomputed (wallet x boundary) table
    (v27_stage0_lcb_table.py: same frozen wallet_lcb call path, parallel outer loop).
    Best wallet per entity -> ranked desc, tie lexicographic wallet."""
    d = lcb_df[(lcb_df["boundary_k"] == boundary_k)
               & (lcb_df["wallet"].isin(wallets_in_cell))]
    if d.empty:
        return pd.DataFrame(columns=["wallet", "entity", "lcb_bps", "n_trips"])
    d = d.assign(entity=[entity_map.get(w, w) for w in d["wallet"]])
    d = (d.sort_values(["lcb_bps", "wallet"], ascending=[False, True])
           .drop_duplicates("entity", keep="first"))
    return d.reset_index(drop=True)


def _policy_block_days(tidx: dict, roster_wallets: list, lo: int, hi: int,
                       equity: float, f: float, g: float) -> pd.DataFrame:
    """Daily policy r_d on [lo,hi). DECLARED SEMANTICS (descriptive, in report):
    - Cold-start at roster formation: ONLY trips ENTERED in [lo, hi) count (a real
      policy starting flat at lo cannot hold earlier entries).
    - Per-wallet uncoupled trips, NO cross-wallet coalescing (equal-weight defined as
      independent per-wallet mirroring at f x E each).
    - Gross-cap sweep over admitted entries in deterministic order (entry ts, wallet);
      entries that would exceed g x E are dropped and counted.
    - PnL attribution to exit day for exits < hi; trips entered in-block but still open
      at hi are counted/reported as open_at_end (their PnL is in no block; declared
      truncation, not silently ignored).
    - r_d denominator: fixed start equity E (start-of-day allocated equity, constant
      within the block); zero-trade days included as 0."""
    import heapq
    per_trip = f * equity
    cap = g * equity
    ev = []   # (entry_ts, wallet, i) admitted candidates
    for w in roster_wallets:
        a = tidx.get(w)
        if a is None:
            continue
        i0 = int(np.searchsorted(a["entry_e"], lo, side="left"))
        i1 = int(np.searchsorted(a["entry_e"], hi, side="left"))
        for i in range(i0, i1):
            ev.append((int(a["entry_e"][i]), w, str(a["coin_e"][i]), i))
    ev.sort()
    days = np.arange(lo, hi, MS_DAY)
    r_d = pd.Series(0.0, index=days)
    open_heap = []   # (exit_ts, notional)
    open_notional = 0.0
    dropped = 0
    open_at_end = 0
    for ets, w, _coin, i in ev:
        while open_heap and open_heap[0][0] <= ets:
            open_notional -= heapq.heappop(open_heap)[1]
        if open_notional + per_trip > cap:
            dropped += 1
            continue
        a = tidx[w]
        xts = int(a["exit_e"][i])
        open_notional += per_trip
        heapq.heappush(open_heap, (xts, per_trip))
        if xts < hi:
            dkey = (xts // MS_DAY) * MS_DAY
            if dkey in r_d.index:
                r_d.loc[dkey] += a["net_pnl_e"][i] * (per_trip / 150.0) / equity
        else:
            open_at_end += 1
    return pd.DataFrame({"day_ms": days, "r_d": r_d.to_numpy()}), dropped, open_at_end


def _batch_means_se(r: np.ndarray, block: int) -> float:
    nb = len(r) // block
    if nb < 2:
        return float("nan")
    bm = r[: nb * block].reshape(nb, block).mean(axis=1)
    return float(np.sqrt(bm.var(ddof=1) / nb))


def _studentized_lcb(r: np.ndarray, level=0.95) -> float:
    """One-sided studentized stationary block bootstrap LCB of mean daily r_d.
    Studentizer = batch-means SE (block = BOOT_BLOCK_DAYS) at top level AND inside each
    resample (serial-dependence-consistent). Zero/undefined-SE resamples are DROPPED
    (not mapped to t=0); count reported via nan policy."""
    n = len(r)
    mean = float(r.mean())
    se = _batch_means_se(r, BOOT_BLOCK_DAYS)
    if n < 2 * BOOT_BLOCK_DAYS or not np.isfinite(se) or se == 0:
        return float("nan")
    rng = np.random.default_rng(BOOT_SEED)
    tstats = []
    for _ in range(BOOT_RESAMPLES):
        idx = stationary_bootstrap_indices(n, BOOT_BLOCK_DAYS, rng)
        rb = r[idx]
        seb = _batch_means_se(rb, BOOT_BLOCK_DAYS)
        if np.isfinite(seb) and seb > 0:
            tstats.append((rb.mean() - mean) / seb)
    if len(tstats) < BOOT_RESAMPLES // 2:
        return float("nan")
    q = float(np.quantile(np.asarray(tstats), level))
    return float(mean - q * se)


def pass_b(out_dir: Path) -> dict:
    journeys = pd.read_parquet(out_dir / "journeys_full.parquet",
                               columns=["wallet", "exit_ts", "duration_h"])
    entries = pd.read_parquet(out_dir / "entries_full.parquet")
    gates = pd.read_parquet(out_dir / "gates_full.parquet",
                            columns=["wallet", "eligible"])
    trips = pd.read_parquet(out_dir / "r2trips_full.parquet",
                            columns=["wallet", "coin", "entry_fill_ts",
                                     "exit_fill_ts_last", "net_pnl", "net_bps",
                                     "terminal"])
    lcb_df = pd.read_parquet(out_dir / "lcb_table.parquet")
    eligible = set(gates.loc[gates["eligible"], "wallet"])
    entity_map, _edges = cluster_entities(entries, eligible)
    jidx = _wallet_journey_index(journeys)
    del journeys
    tidx = _wallet_trip_index(trips)
    del trips
    b3 = block_end_ms(3)

    all_wallets = sorted(set(jidx) & eligible)
    report = {"cells": {}, "prereg_sha": "d75cf501", "label": "DESCRIPTIVE",
              "declared_semantics": ["no cross-wallet coalescing (equal-weight = "
                                     "independent per-wallet mirroring)",
                                     "in-block cold start at roster formation",
                                     "open-at-block-end trips truncated (counted)"]}
    cells0 = {w: _assign_cell(jidx, w, b3) for w in all_wallets}
    for cell in sorted({c for c in cells0.values() if c not in (None, "INSUF")}):
        w0 = {w for w, c in cells0.items() if c == cell}
        rank0 = _rank_entities(lcb_df, entity_map, 3, w0)
        c = {"initial_rankable": int(len(rank0)),
             "survives_initial": bool(len(rank0) >= G0_MIN_RANKABLE_INITIAL),
             "boundaries": {}, "killed_at": None, "dropped_entries": 0,
             "open_at_end": 0}
        if not c["survives_initial"]:
            c["policy_lcb"] = None
            c["survives"] = False
            report["cells"][cell] = c
            continue
        rds = []
        for k in range(3, 18):
            bk = block_end_ms(k)
            wk = {w for w in all_wallets if _assign_cell(jidx, w, bk) == cell}
            rank = _rank_entities(lcb_df, entity_map, k, wk)
            nr = len(rank)
            if nr < G0_THIN_FLOOR:
                c["killed_at"] = k
                break
            top = rank.head(min(G0_TOP_N, nr))
            blk, dropped, open_end = _policy_block_days(
                tidx, top["wallet"].tolist(), bk, block_end_ms(k + 1),
                G0_EQUITY, G0_F, G0_G)
            rds.append(blk)
            c["dropped_entries"] += dropped
            c["open_at_end"] += open_end
            c["boundaries"][k] = {"rankable": nr, "thin": bool(nr < G0_TOP_N)}
        if c["killed_at"] is None and rds:
            allr = pd.concat(rds)["r_d"].to_numpy()
            c["policy_mean_daily"] = float(allr.mean())
            c["policy_lcb"] = _studentized_lcb(allr)
            c["survives"] = bool(c["policy_lcb"] is not None
                                 and np.isfinite(c["policy_lcb"])
                                 and c["policy_lcb"] > 0)
        else:
            c["policy_lcb"] = None
            c["survives"] = False
        report["cells"][cell] = c
    (out_dir / "stage0_report.json").write_text(json.dumps(report, indent=1))
    print(json.dumps({k: {"rankable0": v["initial_rankable"],
                          "lcb": v.get("policy_lcb"),
                          "survives": v.get("survives", False),
                          "killed_at": v.get("killed_at")}
                      for k, v in report["cells"].items()}, indent=1))
    return report


def main():
    install_memory_guard()
    ap = argparse.ArgumentParser()
    ap.add_argument("--pass-a", action="store_true")
    ap.add_argument("--pass-b", action="store_true")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--max-wallets", type=int, default=None)
    a = ap.parse_args()
    out = Path("/tmp/v27_smoke") if a.smoke else OUT_DIR
    out.mkdir(parents=True, exist_ok=True)
    print("v27 STAGE 0 -- EXPLORATORY/DESCRIPTIVE ONLY (prereg d75cf501 + A5)", flush=True)
    if a.smoke:
        c = pass_a(out, max_wallets=200)
        print(json.dumps(c, indent=1))
        pass_b(out)
    elif a.pass_a:
        c = pass_a(out, max_wallets=a.max_wallets)
        (out / "pass_a_counts.json").write_text(json.dumps(c, indent=1))
    elif a.pass_b:
        pass_b(out)


if __name__ == "__main__":
    main()
