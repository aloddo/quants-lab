#!/usr/bin/env python3
"""v27 STAGE 1 -- successive-halving config search inside the Stage-0 surviving cell.

DESCRIPTIVE ONLY (prereg d75cf501 + A1-A6). Frozen mechanics (A6 v3):
  cross = K {5,25,100} x exit {E1,E2,E3} x sizing (f,g) {(1%,3),(1%,10),(5%,3),(5%,10)}
        = 36 configs (S=1 surviving cell H2C1, K_valid = all).
  round 1: blocks 4-12, keep 9; round 2: blocks 13-15, keep 3; round 3: blocks 16-18,
  keep 2 finalists; finalists pooled on blocks 4-18 (+ G3 flags, separate runner).

  A6.1 cross-fit: evaluation block b ranks/assigns at boundary b-1 (strict half-open).
  A6.2 sizing: order = f x E, E = $15,000 FIXED; caps vs FIXED E (gross g x E,
       coin-side 2 x E, margin 0.7 @ 10x reserve); cold start per block.
  A6.3 estimand: r_d = (eq day-end slot - eq day-start slot) / eq day-start; S_0 = E.
  A6.4 roster: rankable entities (>=50 trips) at boundary, R2-LCB desc, best wallet
       per entity, ties lexicographic wallet; top-K.
  A6.5 streams: per (block, exit_style), taker, BASE scenario, top-100-entity union.
  A6.6 fees: FIXED base tier (BASE 4.32/8.64 per side), no volume-tier evolution.
  A6.7 assembly: FORK of v26_run_grid.assemble_config_fold, exactly 4 deltas
       (sizing, fees, r_d output, no dropout). Everything else verbatim.
  A6.8 bootstrap: stationary (exp block 10d, wrap-around), 10k resamples, seed 42,
       batch-means studentizer (batch 10), SHARED index draws; selection = max-t
       adjusted LCB (c* = q95 of family max t*), ties lexicographic config id;
       degeneracy rules pinned (constant series excluded from family, LCB = constant;
       per-resample SE*=0 -> t*=0 logged; residual non-finite = HARD ABORT).

USAGE
    python research/v27/v27_stage1.py --round 1            # full round (streams + sims)
    python research/v27/v27_stage1.py --round 1 --smoke    # 1 block, 6 configs, /tmp
Rounds 2/3 refuse to run unless the prior round's keeps file exists.

Rule 8: install_memory_guard in parent + workers; streams stream to per-block parquet;
no per-row DB access; marks via the v25 page-cached MarksIndex.
"""
from __future__ import annotations

import argparse
import heapq
import json
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "research" / "v25"))
sys.path.insert(0, str(REPO / "research" / "v26"))
sys.path.insert(0, str(REPO / "research" / "v27"))

from v25_common import MS_DAY, MS_MIN, MarksIndex, build_journeys, scenario_base  # noqa: E402
from v25_gates import cluster_entities  # noqa: E402
from v26_common import FoldMarks, install_memory_guard  # noqa: E402
from v26_fees import FeeEngine, load_snapshot  # noqa: E402
from v26_overlays import build_trip_stream, extract_candidate_journeys  # noqa: E402
from v26_run_grid import COUNTER_KEYS, collect_fold_actions  # noqa: E402
from v27_stage0 import (MIN_ACTIVE_DAYS, MIN_CLOSED_JOURNEYS, OUT_DIR,  # noqa: E402
                        block_end_ms, cell_of)


def block_start(b: int) -> int:
    """Start (inclusive) of 1-indexed block b = end of block b-1."""
    return block_end_ms(b - 1)

# ---- frozen Stage-1 constants (A6 v3) -------------------------------------------------------- #
LABEL = "DESCRIPTIVE -- v27 Stage 1 (prereg d75cf501 + A1-A6)"
SURVIVING_CELL = "H2C1"                     # Stage 0 verdict, stage0_report.json
E_FIXED = 15_000.0                          # A6.2
K_AXIS = [5, 25, 100]
EXIT_AXIS = ["E1", "E2", "E3"]
SIZING_AXIS = [(0.01, 3.0), (0.01, 10.0), (0.05, 3.0), (0.05, 10.0)]
MAX_COIN_SIDE_X = 2.0                       # x E_FIXED (A6.2)
MAX_MARGIN_UTIL = 0.7
RESERVE_LEV = 10.0
UNION_TOP = 100                             # max K
ROUND_BLOCKS = {1: list(range(4, 13)), 2: [13, 14, 15], 3: [16, 17, 18]}
ROUND_KEEP = {1: 9, 2: 3, 3: 2}
BOOT_N = 10_000
BOOT_SEED = 42
BOOT_BLOCK_D = 10                           # expected stationary-bootstrap block (days)
BATCH_D = 10                                # batch-means studentizer batch (days)
R2_MIN_TRIPS = 50                           # A2.2 rankable
S1_DIR = OUT_DIR / "stage1"


def config_id(K: int, exit_style: str, f: float, g: float) -> str:
    return f"{SURVIVING_CELL}-K{K}-{exit_style}-f{f:g}-g{g:g}"


def all_configs() -> list[dict]:
    return [{"config_id": config_id(K, e, f, g), "K": K, "exit": e, "f": f, "g": g}
            for K in K_AXIS for e in EXIT_AXIS for (f, g) in SIZING_AXIS]


# ---- fixed base-tier fee shim (A6.6): FeeEngine base rates, NO tier evolution ---------------- #
class FixedBaseFee:
    """A6.6 fixed base-tier fee source. record_volume is a no-op so no tier ever
    departs base; tier_departed_base / rate_departed_base stay 0 (the v26 fail-closed
    tier-departure assertion is retained verbatim in the fork and always passes here)."""
    def __init__(self, snapshot: dict):
        self._eng = FeeEngine(snapshot, mode="BASE")
        self.tier_departed_base = 0
        self.rate_departed_base = 0

    def rate(self, ts_ms: int, coin: str, maker: bool) -> float:
        return (self._eng.base_maker_rate(coin) if maker
                else self._eng.base_taker_rate(coin))

    def base_taker_rate(self, coin: str) -> float:
        return self._eng.base_taker_rate(coin)

    def base_maker_rate(self, coin: str) -> float:
        return self._eng.base_maker_rate(coin)

    def record_volume(self, ts_ms: int, notional: float) -> None:
        pass                                   # no causal tiers in Stage 1 (A6.6)


# ---- roster construction (A6.1 + A6.4) ------------------------------------------------------- #
def load_cell_index(out_dir: Path) -> dict:
    """Wallet -> (exit_ts array, dur array) journey index for _assign_cell."""
    j = pd.read_parquet(out_dir / "journeys_full.parquet",
                        columns=["wallet", "exit_ts", "duration_h"])
    j = j.sort_values(["wallet", "exit_ts"], kind="mergesort")
    return {w: (g["exit_ts"].to_numpy("int64"), g["duration_h"].to_numpy("float64"))
            for w, g in j.groupby("wallet", sort=False)}


def assign_cell(jidx: dict, wallet: str, boundary_ms: int) -> str | None:
    if wallet not in jidx:
        return None
    ex, dur = jidx[wallet]
    n = int(np.searchsorted(ex, boundary_ms, side="left"))     # strict <
    if n == 0:
        return None
    days = np.unique((ex[:n] // MS_DAY).astype("int64"))
    if len(days) < MIN_ACTIVE_DAYS or n < MIN_CLOSED_JOURNEYS:
        return "INSUF"
    return cell_of(float(np.median(dur[:n])), n / len(days))


def build_entity_map(out_dir: Path) -> dict:
    """v25 cluster_entities held fixed, EXACT Stage-0 parity (v27_stage0.pass_b):
    eligible set = gates_full.eligible == True (NOT every wallet in entries)."""
    entries = pd.read_parquet(out_dir / "entries_full.parquet")
    gates = pd.read_parquet(out_dir / "gates_full.parquet", columns=["wallet", "eligible"])
    eligible = set(gates.loc[gates["eligible"], "wallet"])
    entity_map, _ = cluster_entities(entries, eligible)
    return entity_map


def roster_for_block(lcb_df: pd.DataFrame, jidx: dict, entity_map: dict,
                     block_b: int, top_k: int) -> pd.DataFrame:
    """A6.1/A6.4: rank at boundary b-1, cell membership at b-1, best wallet/entity."""
    boundary_k = block_b - 1
    b_ms = block_end_ms(boundary_k)          # boundary k = end of block k = start of b
    d = lcb_df[lcb_df["boundary_k"] == boundary_k]
    cells = {w: assign_cell(jidx, w, b_ms) for w in d["wallet"].unique()}
    d = d[[cells.get(w) == SURVIVING_CELL for w in d["wallet"]]]
    d = d[d["n_trips"] >= R2_MIN_TRIPS]
    d = d.assign(entity=[entity_map.get(w, w) for w in d["wallet"]])
    d = (d.sort_values(["lcb_bps", "wallet"], ascending=[False, True])
           .drop_duplicates("entity", keep="first"))
    return d.head(top_k).reset_index(drop=True)


# ---- assembly: FAITHFUL FORK of v26_run_grid.assemble_config_fold (A6.7) --------------------- #
# The body below is the v26 function reproduced VERBATIM with EXACTLY four marked deltas:
#   [DELTA a] sizing: order = f x E_FIXED; caps compared to FIXED g x E / 2 x E / margin
#             (v26 divided by MTM eq and compared to x-multiples; here fixed-$ thresholds).
#             INITIAL_EQUITY baseline -> E_FIXED (the account equity for this stage).
#   [DELTA b] fees: FixedBaseFee (base tier, no causal evolution). record_volume no-op;
#             tier_departed_base stays 0 so the retained assertion always passes.
#   [DELTA c] output: r_d start-of-day-equity series replaces daily_pnl (equity path kept).
#   [DELTA d] no dropout branch (selection rounds); finalists' dropout is a separate runner.
# Everything else (dust skip, coalescing, maker branches, exit-fill clock, entity/coin
# aggregation via entity_map, concentration outputs, tier-departure assertion) is verbatim.
def assemble_config_block(trips: pd.DataFrame, fm: FoldMarks, fee: FixedBaseFee,
                          f: float, g: float, entity_map: dict,
                          fallback_lookup: dict | None = None) -> dict:
    start_ms, end_ms = fm.start_ms, fm.end_ms
    n_days = (end_ms - start_ms) // MS_DAY
    n_slots = fm.n_slots
    unreal = np.zeros(n_slots)
    gross_arr = np.zeros(n_slots)
    real_steps = np.zeros(n_slots)
    admitted_day = np.zeros(n_days)
    c = {k: 0 for k in COUNTER_KEYS}
    open_lots: dict[tuple, dict] = {}
    exit_heap: list = []
    realized = 0.0
    seq = 0
    ent_trips: dict[str, int] = {}
    ent_net: dict[str, float] = {}
    coin_net: dict[str, float] = {}
    n_long = n_short = 0
    sum_net_bps = 0.0
    order_usd = f * E_FIXED                                    # [DELTA a]
    gross_cap_usd = g * E_FIXED                                # [DELTA a]
    coin_side_cap_usd = MAX_COIN_SIDE_X * E_FIXED              # [DELTA a]
    # margin cap [DELTA a] inlined at admission: (gross+order)/RESERVE_LEV > 0.7 x E

    t = trips.sort_values(["entry_signal_ts", "wallet", "coin"], kind="mergesort")

    def _mark_now(coin: str, ts: int, fallback: float) -> float:
        i = min(max(fm.slot_of(ts), 0), n_slots - 1)
        v = fm.series(coin)[i]
        return float(v) if v == v else fallback

    def _step_slot(ts: int, inclusive: bool) -> int:
        s = fm.slot_of(ts) + (0 if inclusive and ts % MS_MIN == 0 else 1)
        return min(max(s, 0), n_slots - 1)

    def _pop_exit(key):
        nonlocal realized, n_long, n_short, sum_net_bps
        lot = open_lots.pop(key)
        r = lot["row"]
        notional = lot["notional"]
        ratio = r.exit_px / r.entry_px
        gain = notional * (ratio - 1.0) * r.side
        exit_fee = notional * ratio * fee.rate(int(r.exit_fill_ts), r.coin,
                                               bool(r.exit_is_maker))
        fee.record_volume(int(r.exit_fill_ts), notional * ratio)
        net = gain - exit_fee - lot["entry_fee"]
        realized += gain - exit_fee
        terminal = r.exit_reason == "TERMINAL"
        s_end = (n_slots - 1 if terminal
                 else _step_slot(int(r.exit_fill_ts), True))
        s0 = lot["s0"]
        if s0 < s_end:
            cl = fm.series(r.coin)[s0:s_end]
            px = np.where(np.isfinite(cl), cl, r.entry_px)
            sz = notional / r.entry_px
            unreal[s0:s_end] += sz * (px - r.entry_px) * r.side
            gross_arr[s0:s_end] += sz * px
        real_steps[s_end] += gain - exit_fee
        c["trips_terminal" if terminal else "trips_realized"] += 1
        if not terminal:
            if r.side > 0:
                n_long += 1
            else:
                n_short += 1
            sum_net_bps += net / notional * 1e4
        if r.exit_late:
            c["exits_late"] += 1
        if r.maker_exit_fallback:
            c["maker_exit_fallback"] += 1
        ent = entity_map.get(r.wallet, r.wallet)
        if not terminal:
            ent_trips[ent] = ent_trips.get(ent, 0) + 1
        ent_net[ent] = ent_net.get(ent, 0.0) + net
        coin_net[r.coin] = coin_net.get(r.coin, 0.0) + net

    def _flush_exits(upto_ts: int):
        while exit_heap and exit_heap[0][0] <= upto_ts:
            _, _, key = heapq.heappop(exit_heap)
            if key in open_lots:
                _pop_exit(key)

    for r in t.itertuples(index=False):
        ts = int(r.entry_signal_ts)
        _flush_exits(ts)
        if r.miss_reason == "dust":
            c["entries_dust_skipped"] += 1
            continue
        key = (r.wallet, r.coin)
        if key in open_lots:
            c["dup_coalesced"] += 1
            continue
        # [DELTA d]: no dropout branch (v26 had event_dropout gate here).
        # account caps (v25 _caps_allow order + semantics), thresholds per [DELTA a]
        eq = E_FIXED + realized                               # [DELTA a] baseline
        gross = 0.0
        coin_side = 0.0
        for k2, lot in open_lots.items():
            lr = lot["row"]
            m = _mark_now(lr.coin, ts, lr.entry_px)
            n = lot["notional"] / lr.entry_px * m
            gross += n
            if lot["fill_ts"] <= ts:
                eq += lot["notional"] / lr.entry_px * (m - lr.entry_px) * lr.side
            if lr.coin == r.coin and lr.side == r.side:
                coin_side += n
        if eq <= 0:
            c["entries_blocked_margin"] += 1
            continue
        if gross + order_usd > gross_cap_usd:                 # [DELTA a] fixed-$ cap
            c["entries_blocked_gross"] += 1
            continue
        if coin_side + order_usd > coin_side_cap_usd:         # [DELTA a] fixed-$ cap
            c["entries_blocked_coin_side"] += 1
            continue
        if (gross + order_usd) / RESERVE_LEV > MAX_MARGIN_UTIL * E_FIXED:  # [DELTA a]
            c["entries_blocked_margin"] += 1
            continue
        row = r
        if row.status == "miss":
            if fallback_lookup is not None and row.miss_reason.startswith("maker"):
                fb = fallback_lookup.get((row.wallet, row.coin, int(row.journey_id)))
                if fb is not None and fb.status == "ok":
                    row = fb           # all-missed-as-taker variant (decision D9)
                else:
                    c["entries_dropped_no_mark"] += 1
                    continue
            else:
                if row.miss_reason == "entry_no_mark":
                    c["entries_dropped_no_mark"] += 1
                else:
                    c["maker_missed"] += 1
                    mk = f"maker_{row.miss_reason.split('maker_')[-1]}"
                    if mk in c:
                        c[mk] += 1
                continue
        entry_fee = order_usd * fee.rate(int(row.entry_fill_ts), row.coin,
                                         bool(row.entry_is_maker))
        fee.record_volume(int(row.entry_fill_ts), order_usd)
        realized -= entry_fee
        real_steps[_step_slot(ts, False)] -= entry_fee
        s0 = min(max(fm.slot_ceil(int(row.entry_fill_ts)), 0), n_slots - 1)
        open_lots[key] = {"row": row, "notional": order_usd, "entry_fee": entry_fee,
                          "fill_ts": int(row.entry_fill_ts), "s0": s0}
        heapq.heappush(exit_heap, (int(row.exit_fill_ts), seq, key))
        seq += 1
        c["entries"] += 1
        if row.entry_is_maker:
            c["maker_fills"] += 1
        d = (int(row.entry_fill_ts) - start_ms) // MS_DAY
        if 0 <= d < n_days:
            admitted_day[d] += order_usd
    _flush_exits(end_ms + 1)
    c["tier_departed_base"] = fee.tier_departed_base
    if fee.rate_departed_base or fee.tier_departed_base:
        raise RuntimeError(
            f"tier-departure assertion (D5): rate_departed={fee.rate_departed_base} "
            f"tier_departed={fee.tier_departed_base} -- config fails closed")

    equity = E_FIXED + np.cumsum(real_steps) + unreal
    peak = np.maximum.accumulate(equity)
    with np.errstate(invalid="ignore", divide="ignore"):
        dd = np.where(peak > 0, (peak - equity) / peak, 0.0)
    # [DELTA c]: r_d on start-of-day equity, S_0 = E (A6.3), replaces daily_pnl
    day_slots = np.arange(0, n_days + 1) * 1440
    eq_bounds = equity[np.minimum(day_slots, n_slots - 1)].astype(float)
    eq_bounds[0] = E_FIXED
    r_d = np.diff(eq_bounds) / eq_bounds[:-1]
    avg_gross = gross_arr[:n_days * 1440].reshape(n_days, 1440).mean(axis=1)
    return {
        "r_d": r_d, "admitted": admitted_day, "avg_gross": avg_gross,
        "total_pnl": float(equity[-1] - E_FIXED),
        "max_dd": float(np.nanmax(dd)) if len(dd) else 0.0,
        "n_realized": c["trips_realized"], "n_terminal": c["trips_terminal"],
        "n_long": n_long, "n_short": n_short,
        "mean_net_bps": (sum_net_bps / c["trips_realized"]
                         if c["trips_realized"] else float("nan")),
        "counters": c, "entity_trips": ent_trips, "entity_net": ent_net,
        "coin_net": coin_net,
    }


# ---- bootstrap + max-t selection (A6.8) ------------------------------------------------------ #
def _batch_means_se(x: np.ndarray, batch: int = BATCH_D) -> float:
    n = len(x)
    nb = n // batch
    if nb < 2:
        return float(np.std(x, ddof=1) / np.sqrt(n)) if n > 1 else 0.0
    means = x[:nb * batch].reshape(nb, batch).mean(axis=1)
    return float(np.std(means, ddof=1) / np.sqrt(nb))


def stationary_boot_indices(n: int, n_boot: int, seed: int,
                            mean_block: int) -> np.ndarray:
    """Wrap-around stationary bootstrap index matrix (n_boot x n), one shared stream."""
    rng = np.random.default_rng(seed)
    p = 1.0 / mean_block
    starts = rng.integers(0, n, size=(n_boot, n))
    new_block = rng.random(size=(n_boot, n)) < p
    new_block[:, 0] = True
    idx = np.zeros((n_boot, n), dtype=np.int64)
    cur = starts[:, 0].copy()
    for j in range(n):
        if j > 0:
            stay = ~new_block[:, j]
            cur = np.where(stay, (cur + 1) % n, starts[:, j])
        idx[:, j] = cur
    return idx


def select_round(series: dict[str, np.ndarray], keep: int) -> dict:
    """A6.8: shared-draw studentized max-t adjusted LCB selection."""
    cids = sorted(series)
    n = len(next(iter(series.values())))
    assert all(len(v) == n for v in series.values()), "unequal pooled lengths"
    stats = {}
    family = []
    for cid in cids:
        x = series[cid]
        mean = float(np.mean(x))
        se = _batch_means_se(x)
        excluded = (se == 0.0)          # A6.8(a) operational criterion: SE == 0 exactly
        # per-config bootstrap 95th critical (A6.8): null for excluded configs
        stats[cid] = {"mean": mean, "se": se, "excluded_se0": excluded,
                      "crit_q95": None}
        if excluded:                    # LCB = mean (= mean - c* x 0); no c* contribution
            stats[cid]["lcb_adj"] = mean
        else:
            family.append(cid)
    degenerate_resamples = 0
    c_star = None
    if family:
        idx = stationary_boot_indices(n, BOOT_N, BOOT_SEED, BOOT_BLOCK_D)
        tmat = np.zeros((BOOT_N, len(family)))
        for j, cid in enumerate(family):
            x = series[cid]
            xs = x[idx]                                     # (BOOT_N, n)
            means_s = xs.mean(axis=1)
            nb = n // BATCH_D
            if nb >= 2:
                bm = xs[:, :nb * BATCH_D].reshape(BOOT_N, nb, BATCH_D).mean(axis=2)
                se_s = bm.std(axis=1, ddof=1) / np.sqrt(nb)
            else:
                se_s = xs.std(axis=1, ddof=1) / np.sqrt(n)
            t = (means_s - stats[cid]["mean"]) / se_s        # 0/0 -> nan, x/0 -> inf
            # A6.8(b): any resample with SE*=0 or non-finite t* -> t*=0, ALL logged
            bad = ~np.isfinite(t)
            degenerate_resamples += int(bad.sum())
            t[bad] = 0.0
            tmat[:, j] = t
            # per-config q95 critical (reporting, A6.8): unadjusted single-config value
            stats[cid]["crit_q95"] = float(np.quantile(t, 0.95))
        c_star = float(np.quantile(tmat.max(axis=1), 0.95))
        if not np.isfinite(c_star):
            raise RuntimeError("A6.8(c) HARD ABORT: non-finite c*")
        for cid in family:
            m, se = stats[cid]["mean"], stats[cid]["se"]
            if not (np.isfinite(m) and np.isfinite(se)):
                raise RuntimeError(f"A6.8(c) HARD ABORT: non-finite mean/SE {cid}")
            stats[cid]["lcb_adj"] = m - c_star * se
    # A6.8(c): residual non-finite adjusted LCB anywhere = HARD ABORT (bug, not data)
    for cid in cids:
        if not np.isfinite(stats[cid]["lcb_adj"]):
            raise RuntimeError(f"A6.8(c) HARD ABORT: non-finite adjusted LCB {cid}")
    order = sorted(cids, key=lambda c_: (-stats[c_]["lcb_adj"], c_))
    return {"kept": order[:keep], "order": order, "stats": stats,
            "c_star": c_star, "degenerate_resamples": degenerate_resamples}


# ---- per-block pipeline ---------------------------------------------------------------------- #
_W: dict = {}


def _block_worker_init(mem_gb: float):
    install_memory_guard(soft_gb=mem_gb, label="v27-s1-block")
    _W["marks"] = MarksIndex()
    _W["fee_snapshot"] = load_snapshot()
    _W["entity_map"] = build_entity_map(OUT_DIR)     # frozen entity dedup (Stage-0 parity)


def _block_worker(task: dict) -> dict:
    """One evaluation block: roster -> actions -> journeys -> 3 streams -> 36 sims.
    Returns {config_id: r_d list} + stream/assembly counters. Streams parquet saved."""
    b = task["block"]
    lo, hi = block_start(b), block_start(b + 1)
    marks: MarksIndex = _W["marks"]
    fee = FixedBaseFee(_W["fee_snapshot"])
    roster = pd.DataFrame(task["roster"])                   # top-100 union, ranked
    union_wallets = set(roster["wallet"])
    actions = collect_fold_actions(union_wallets, lo, hi)
    journeys = extract_candidate_journeys(actions, lo, hi)
    fm = FoldMarks(marks, lo, hi)
    sc = scenario_base()
    out = {"block": b, "series": {}, "counters": {}}
    bdir = task["out_dir"] / f"block{b:02d}"
    bdir.mkdir(parents=True, exist_ok=True)
    for exit_style in EXIT_AXIS:
        stream = build_trip_stream(journeys, exit_style, "taker", sc,
                                   fee.base_taker_rate, fee.base_maker_rate,
                                   marks, fm, lo, hi)
        stream.to_parquet(bdir / f"stream_{exit_style}.parquet", index=False)
        for cfg in task["configs"]:
            if cfg["exit"] != exit_style:
                continue
            kw = set(roster.head(cfg["K"])["wallet"])
            res = assemble_config_block(stream[stream["wallet"].isin(kw)], fm, fee,
                                        cfg["f"], cfg["g"], _W["entity_map"])
            out["series"][cfg["config_id"]] = res["r_d"].tolist()
            out["counters"][cfg["config_id"]] = {
                "entries": res["counters"]["entries"],
                "realized": res["counters"]["trips_realized"],
                "terminal": res["counters"]["trips_terminal"],
                "blocked_gross": res["counters"]["entries_blocked_gross"],
                "max_dd": res["max_dd"], "total_pnl": res["total_pnl"]}
    return out


def run_round(rnd: int, smoke: bool, procs: int, mem_gb: float) -> None:
    t0 = time.time()
    out_dir = Path("/tmp/v27_s1_smoke") if smoke else S1_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    configs = all_configs()
    if rnd > 1:
        prior = json.loads((S1_DIR / f"round{rnd-1}_result.json").read_text())
        kept = set(prior["kept"])
        configs = [c_ for c_ in configs if c_["config_id"] in kept]
        assert len(configs) == ROUND_KEEP[rnd - 1], "keeps mismatch"
    blocks = ROUND_BLOCKS[rnd]
    if smoke:
        blocks = blocks[:1]
        configs = configs[:6]
    print(f"{LABEL}\nround {rnd}: {len(configs)} configs x blocks {blocks}", flush=True)

    lcb_df = pd.read_parquet(OUT_DIR / "lcb_table.parquet")
    jidx = load_cell_index(OUT_DIR)
    entity_map = build_entity_map(OUT_DIR)
    tasks = []
    for b in blocks:
        roster = roster_for_block(lcb_df, jidx, entity_map, b, UNION_TOP)
        assert len(roster) >= UNION_TOP or smoke, \
            f"union undersupply at block {b}: {len(roster)} < {UNION_TOP}"
        tasks.append({"block": b, "roster": roster.to_dict("records"),
                      "configs": configs, "out_dir": out_dir})
    del jidx

    results = []
    with Pool(processes=min(procs, len(tasks)), initializer=_block_worker_init,
              initargs=(mem_gb,)) as pool:
        for res in pool.imap_unordered(_block_worker, tasks):
            results.append(res)
            print(f"  block {res['block']} done ({time.time()-t0:.0f}s)", flush=True)

    results.sort(key=lambda r_: r_["block"])                 # calendar order (A6.3)
    series = {c_["config_id"]: np.concatenate(
        [np.asarray(r_["series"][c_["config_id"]]) for r_ in results])
        for c_ in configs}
    sel = select_round(series, ROUND_KEEP[rnd])
    report = {"label": LABEL, "round": rnd, "blocks": blocks,
              "n_configs": len(configs), "kept": sel["kept"],
              "c_star": sel["c_star"],
              "degenerate_resamples": sel["degenerate_resamples"],
              "table": [{"config_id": cid, **sel["stats"][cid],
                         "pooled_days": int(len(series[cid])),
                         "blocks": {str(r_["block"]): r_["counters"].get(cid)
                                    for r_ in results}}
                        for cid in sel["order"]],
              "runtime_s": round(time.time() - t0)}
    path = out_dir / f"round{rnd}_result.json"
    path.write_text(json.dumps(report, indent=1, default=float))
    print(f"round {rnd} DONE -> {path} ({report['runtime_s']}s)", flush=True)
    for row in report["table"]:
        print(f"  {row['config_id']:32s} lcb_adj={row['lcb_adj']:+.6f} "
              f"mean={row['mean']:+.6f} "
              f"{'KEPT' if row['config_id'] in sel['kept'] else 'cut'}", flush=True)


FINALIST_BLOCKS = list(range(4, 19))            # A6.3/A6.9 finalists pooled on blocks 4-18


def run_finalists(procs: int, mem_gb: float) -> None:
    """A6.9 finalist DESCRIPTIVE eval: round-3 keeps pooled over blocks 4-18, max-t
    over the (small) finalist family. Reuses the CODE-GATED assemble_config_block +
    select_round verbatim; the only new logic is the driver + the G3 block-sign flag.
    G3 WORST/dropout and G2 capacity are additive follow-ons (G2 UNCALIBRATED until the
    fresh L2 books mature ~Jul 8); this stage does NOT emit a live GO."""
    t0 = time.time()
    prior = json.loads((S1_DIR / "round3_result.json").read_text())
    keep_ids = set(prior["kept"])
    configs = [c_ for c_ in all_configs() if c_["config_id"] in keep_ids]
    assert len(configs) == ROUND_KEEP[3], "finalist count mismatch"
    print(f"{LABEL}\nFINALISTS: {len(configs)} configs x blocks {FINALIST_BLOCKS}",
          flush=True)

    lcb_df = pd.read_parquet(OUT_DIR / "lcb_table.parquet")
    jidx = load_cell_index(OUT_DIR)
    entity_map = build_entity_map(OUT_DIR)
    tasks = []
    for b in FINALIST_BLOCKS:
        roster = roster_for_block(lcb_df, jidx, entity_map, b, UNION_TOP)
        assert len(roster) >= UNION_TOP, f"union undersupply at block {b}: {len(roster)}"
        tasks.append({"block": b, "roster": roster.to_dict("records"),
                      "configs": configs, "out_dir": S1_DIR})
    del jidx

    results = []
    with Pool(processes=min(procs, len(tasks)), initializer=_block_worker_init,
              initargs=(mem_gb,)) as pool:
        for res in pool.imap_unordered(_block_worker, tasks):
            results.append(res)
            print(f"  block {res['block']} done ({time.time()-t0:.0f}s)", flush=True)

    results.sort(key=lambda r_: r_["block"])
    series = {c_["config_id"]: np.concatenate(
        [np.asarray(r_["series"][c_["config_id"]]) for r_ in results])
        for c_ in configs}
    sel = select_round(series, len(configs))
    # G3 block-sign consistency (descriptive flag): fraction of blocks with positive mean r_d
    sign = {c_["config_id"]: float(np.mean(
        [np.mean(r_["series"][c_["config_id"]]) > 0 for r_ in results]))
        for c_ in configs}
    report = {"label": LABEL, "stage": "FINALISTS", "blocks": FINALIST_BLOCKS,
              "n_configs": len(configs), "c_star": sel["c_star"],
              "degenerate_resamples": sel["degenerate_resamples"],
              "g3_block_sign_consistency": sign,
              "g3_worst": "DEFERRED (WORST scenario re-run pending)",
              "g2_capacity": "UNCALIBRATED (fresh L2 books mature ~2026-07-08)",
              "verdict": "DESCRIPTIVE ONLY -- no live GO (G2 veto uncomputable now)",
              "table": [{"config_id": cid, **sel["stats"][cid],
                         "pooled_days": int(len(series[cid])),
                         "block_sign_consistency": sign[cid],
                         "blocks": {str(r_["block"]): r_["counters"].get(cid)
                                    for r_ in results}}
                        for cid in sel["order"]],
              "runtime_s": round(time.time() - t0)}
    path = S1_DIR / "finalists_result.json"
    path.write_text(json.dumps(report, indent=1, default=float))
    print(f"FINALISTS DONE -> {path} ({report['runtime_s']}s)", flush=True)
    for row in report["table"]:
        print(f"  {row['config_id']:32s} lcb_adj={row['lcb_adj']:+.6f} "
              f"mean={row['mean']:+.6f} sign_consist={row['block_sign_consistency']:.2f}",
              flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--round", type=int, choices=[1, 2, 3])
    ap.add_argument("--finalists", action="store_true")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--procs", type=int, default=6)          # weekend rails
    ap.add_argument("--mem-gb", type=float, default=2.0)     # per-worker cap
    a = ap.parse_args()
    install_memory_guard(soft_gb=a.mem_gb * 2, label="v27-s1-parent")
    if a.finalists:
        run_finalists(a.procs, a.mem_gb)
    else:
        assert a.round, "one of --round or --finalists required"
        run_round(a.round, a.smoke, a.procs, a.mem_gb)


if __name__ == "__main__":
    main()
