#!/usr/bin/env python3
"""v26 grid orchestrator: registry -> per-fold family selection -> trip-stream
precompute -> per-config roster assembly sims -> estimand + familywise corrections ->
frontier / kill ledger / marginals / verdict_grid.json.

USAGE
    # smoke (ALLOWED anytime fold-1 artifacts exist): 20 configs x fold 1,
    # outputs under app/data/research/v26/smoke/, labeled NOT DECISION EVIDENCE
    .../python research/v26/v26_run_grid.py --smoke

    # full grid: refuses while the v25 run pid is alive, while verdict.json lacks all
    # 5 folds, or without a validating FREEZE-GRID.json; requires --confirm-grid
    .../python research/v26/v26_run_grid.py --confirm-grid

EVERY report carries the EXPLORATORY label (frozen, codex r2 #1).

ASSEMBLY SIM (wraps the v25 semantics; v25 files are never edited): the per-config sim
consumes the precomputed per-fold trip streams (v26_overlays). Per-lot outcomes are
portfolio-independent and linear in entry notional, so assembly applies -- in v25
on_entry order -- dust skip, (wallet, coin) coalescing, event-hash dropout, account caps
(gross_cap axis; coin-side 2x and margin 0.7/10x inherited frozen), sizing (150 / 500 /
pct2 = 2% of MTM equity at signal), and causal-tier fees (v26_fees), then builds the
minute-grid equity / gross / drawdown series (decision D7) and the daily estimand
inputs. EXIT-FILL CLOCK (codex code-gate #4, v25 gate-b r3 parity): gross exposure and
the unrealized MTM series run to the DELAYED EXIT FILL slot -- not the exit signal --
and realized PnL lands on the minute grid at the fill slot, so time-weighted avg_gross,
DD, and daily attribution all use fill timestamps. Entry fees hit equity at the entry
signal -- v25 CopySim parity.
"""
from __future__ import annotations

import argparse
import heapq
import json
import resource
import sys
import time
import traceback
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from v26_common import (DROPOUT_SEEDS, EXPLORATORY_LABEL, FREEZE_GRID_PATH,
                        GRID_BLOCK_DAYS, GRID_BLOCK_DAYS_ROBUST, GRID_SEED,
                        INITIAL_EQUITY, MAKER_MIN_FILL_RATE, MAX_COIN_SIDE_X,
                        MAX_MARGIN_UTIL, MIN_NONZERO_DAYS, MS_DAY, MS_MIN, RESERVE_LEV,
                        FoldMarks, MarksIndex, ShardedParquetWriter, V26_DATA,
                        canonical_sha256, event_dropout, folds, git_commit,
                        install_memory_guard, iter_wallet_frames, k_real_tier,
                        scenario_base, scenario_worst, stamp_parquet_exploratory,
                        v25_run_alive, v25_verdict_complete,
                        write_exploratory_parquet)
from v26_families import FoldContext, allowed_bands, build_fold_rankings
from v26_fees import FeeEngine, load_snapshot
from v26_overlays import build_trip_stream, extract_candidate_journeys
from v26_registry import enumerate_registry, load_registry, write_registry

SCEN = {"BASE": scenario_base, "WORST": scenario_worst}
SMOKE_N_CONFIGS = 20

BANNER = (
    "=" * 88 + "\n"
    "  v26 GRID -- " + EXPLORATORY_LABEL + "\n" + "=" * 88)


# --------------------------------------------------------------------------------------------- #
# Per-config roster assembly sim
# --------------------------------------------------------------------------------------------- #
COUNTER_KEYS = ["entries", "entries_dropped_no_mark", "entries_dust_skipped",
                "entries_blocked_gross", "entries_blocked_coin_side",
                "entries_blocked_margin", "dup_coalesced", "dropout_dropped",
                "exits_late", "trips_realized", "trips_terminal", "maker_fills",
                "maker_missed", "maker_no_post", "maker_no_cross", "maker_cancelled",
                "maker_exit_fallback", "tier_departed_base"]


def assemble_config_fold(trips: pd.DataFrame, fm: FoldMarks, fee: FeeEngine,
                         gross_cap: float, sizing: str, entity_map: dict,
                         dropout_seed: int | None = None,
                         fallback_lookup: dict | None = None) -> dict:
    """One config x fold x scenario x variant assembly over the precomputed trip stream
    (already filtered to the config's roster wallets). Returns the daily estimand inputs,
    DD, counters and concentration aggregates."""
    start_ms, end_ms = fm.start_ms, fm.end_ms
    n_days = (end_ms - start_ms) // MS_DAY
    n_slots = fm.n_slots
    unreal = np.zeros(n_slots)
    gross_arr = np.zeros(n_slots)
    real_steps = np.zeros(n_slots)
    admitted_day = np.zeros(n_days)
    c = {k: 0 for k in COUNTER_KEYS}
    open_lots: dict[tuple, dict] = {}          # (wallet, coin) -> lot
    exit_heap: list = []                       # (exit_signal_ts, seq, key)
    realized = 0.0
    seq = 0
    ent_trips: dict[str, int] = {}
    ent_net: dict[str, float] = {}
    coin_net: dict[str, float] = {}
    n_long = n_short = 0
    sum_net_bps = 0.0

    t = trips.sort_values(["entry_signal_ts", "wallet", "coin"], kind="mergesort")

    def _mark_now(coin: str, ts: int, fallback: float) -> float:
        i = min(max(fm.slot_of(ts), 0), n_slots - 1)
        v = fm.series(coin)[i]
        return float(v) if v == v else fallback

    def _step_slot(ts: int, inclusive: bool) -> int:
        # realized-PnL visibility on the minute grid: an event at ts affects marks
        # AFTER ts (v25 advance order) unless it IS the mark itself (overlay trigger)
        s = fm.slot_of(ts) + (0 if inclusive and ts % MS_MIN == 0 else 1)
        return min(max(s, 0), n_slots - 1)

    def _pop_exit(key):
        nonlocal realized, n_long, n_short, sum_net_bps
        lot = open_lots.pop(key)
        r = lot["row"]
        notional = lot["notional"]
        ratio = r.exit_px / r.entry_px
        gain = notional * (ratio - 1.0) * r.side
        # TERMINAL rows reserve taker exit costs (exit_is_maker False by construction)
        exit_fee = notional * ratio * fee.rate(int(r.exit_fill_ts), r.coin,
                                               bool(r.exit_is_maker))
        fee.record_volume(int(r.exit_fill_ts), notional * ratio)
        net = gain - exit_fee - lot["entry_fee"]
        realized += gain - exit_fee
        terminal = r.exit_reason == "TERMINAL"
        # EXIT-FILL CLOCK (codex code-gate #4, v25 gate-b r3 / _fill_exit parity):
        # the position stays marked (unreal + gross) through every minute STRICTLY
        # before the delayed exit FILL; the fill mark itself is sampled post-mutation
        # (inclusive=True: a fill on a minute boundary closes AT that slot), and the
        # realized PnL lands at the fill slot -- daily attribution uses fill timestamps
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
        if dropout_seed is not None and event_dropout(
                dropout_seed, r.wallet, r.coin, int(r.journey_id)):
            c["dropout_dropped"] += 1
            continue
        # account caps (v25 _caps_allow order + semantics; gross cap from the axis)
        eq = INITIAL_EQUITY + realized
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
        order_usd = {"150": 150.0, "500": 500.0}.get(sizing) or 0.02 * eq
        if (gross + order_usd) / eq > gross_cap:
            c["entries_blocked_gross"] += 1
            continue
        if (coin_side + order_usd) / eq > MAX_COIN_SIDE_X:
            c["entries_blocked_coin_side"] += 1
            continue
        if (gross + order_usd) / RESERVE_LEV / eq > MAX_MARGIN_UTIL:
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
        # ---- admit the lot ---------------------------------------------------------- #
        entry_fee = order_usd * fee.rate(int(row.entry_fill_ts), row.coin,
                                         bool(row.entry_is_maker))
        fee.record_volume(int(row.entry_fill_ts), order_usd)
        realized -= entry_fee
        real_steps[_step_slot(ts, False)] -= entry_fee
        s0 = min(max(fm.slot_ceil(int(row.entry_fill_ts)), 0), n_slots - 1)
        open_lots[key] = {"row": row, "notional": order_usd, "entry_fee": entry_fee,
                          "fill_ts": int(row.entry_fill_ts), "s0": s0}
        # codex cg-r2 #1: the position occupies its slot (caps, dup-coalescing,
        # admissions) until the delayed exit FILL, not the exit signal
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
        # decision D5 assertion (codex code-gate #5 + cg-r2 #2, fail closed): overlay
        # trigger geometry prices accrued exit costs at BASE-TIER rates. ANY tier
        # departure fails the config loudly -- in BASE mode a departed tier changes
        # charged rates (rate_departed_base), and in WORST mode the floor masks the
        # charged rate but the geometry claim is still voided (tier_departed_base).
        raise RuntimeError(
            f"tier-departure assertion (D5): rate_departed={fee.rate_departed_base} "
            f"tier_departed={fee.tier_departed_base} fills while E2/E3 trigger "
            f"geometry assumes base-tier fees -- config fails closed")

    equity = INITIAL_EQUITY + np.cumsum(real_steps) + unreal
    peak = np.maximum.accumulate(equity)
    with np.errstate(invalid="ignore", divide="ignore"):
        dd = np.where(peak > 0, (peak - equity) / peak, 0.0)
    day_ends = np.arange(1, n_days + 1) * 1440
    eq_at = equity[np.minimum(day_ends, n_slots - 1)]
    daily_pnl = np.diff(np.concatenate(([INITIAL_EQUITY], eq_at)))
    avg_gross = gross_arr[:n_days * 1440].reshape(n_days, 1440).mean(axis=1)
    return {
        "daily_pnl": daily_pnl, "admitted": admitted_day, "avg_gross": avg_gross,
        "total_pnl": float(equity[-1] - INITIAL_EQUITY),
        "max_dd": float(np.nanmax(dd)) if len(dd) else 0.0,
        "n_realized": c["trips_realized"], "n_terminal": c["trips_terminal"],
        "n_long": n_long, "n_short": n_short,
        "mean_net_bps": (sum_net_bps / c["trips_realized"]
                         if c["trips_realized"] else float("nan")),
        "counters": c, "entity_trips": ent_trips, "entity_net": ent_net,
        "coin_net": coin_net,
    }


# --------------------------------------------------------------------------------------------- #
# Per-fold pipeline
# --------------------------------------------------------------------------------------------- #
def stream_path(out_dir: Path, fold: int, exit_style: str, execution: str,
                scen: str) -> Path:
    return out_dir / "streams" / f"fold{fold}_{exit_style}_{execution}_{scen}.parquet"


def collect_fold_actions(wallets: set[str], start_ms: int, end_ms: int) -> pd.DataFrame:
    """Bounded second pass over m02_actions for the fold's roster-union wallets,
    ts in [start, end)."""
    if not wallets:
        return pd.DataFrame(columns=["wallet", "coin", "ts", "action_type",
                                     "signed_size", "price", "position_after",
                                     "journey_id", "closing_journey_id",
                                     "is_liquidation"])
    hi = max(wallets)
    parts = []
    for wallet, wdf in iter_wallet_frames():
        if wallet in wallets:
            sl = wdf[(wdf["ts"] >= start_ms) & (wdf["ts"] < end_ms)]
            if len(sl):
                parts.append(sl)
        if wallet > hi:
            break
    return (pd.concat(parts, ignore_index=True) if parts
            else pd.DataFrame(columns=["wallet", "coin", "ts", "action_type",
                                       "signed_size", "price", "position_after",
                                       "journey_id", "closing_journey_id",
                                       "is_liquidation"]))


_W: dict = {}          # worker globals


def _stream_worker_init(fold_meta, candidates_path, out_dir, mem_gb):
    install_memory_guard(soft_gb=mem_gb, label="v26-stream")
    _W["fold"] = fold_meta
    _W["cand"] = pd.read_parquet(candidates_path)
    _W["marks"] = MarksIndex()
    _W["fm"] = FoldMarks(_W["marks"], fold_meta["asof_ms"], fold_meta["test_end_ms"])
    _W["out_dir"] = Path(out_dir)
    _W["snapshot"] = load_snapshot()


def _stream_worker(task):
    exit_style, execution, scen = task
    f = _W["fold"]
    eng = FeeEngine(_W["snapshot"], mode=scen)
    sc = SCEN[scen]()
    df = build_trip_stream(_W["cand"], exit_style, execution, sc,
                           eng.base_taker_rate, eng.base_maker_rate,
                           _W["marks"], _W["fm"], f["asof_ms"], f["test_end_ms"])
    p = stream_path(_W["out_dir"], f["fold"], exit_style, execution, scen)
    p.parent.mkdir(parents=True, exist_ok=True)
    write_exploratory_parquet(df, p)
    return (task, len(df))


def build_fold_streams(fold_meta, cfgs: pd.DataFrame, out_dir: Path,
                       rankings_all: dict, n_procs: int, mem_gb: float,
                       precomputed_ranks: dict | None = None) -> dict:
    """Family selection + candidate journeys + trip streams for one fold.
    Returns diagnostics. precomputed_ranks: reuse rankings already computed for this
    fold (registry build already ranked fold 1; identical deterministic output)."""
    k = fold_meta["fold"]
    t0 = time.time()
    if precomputed_ranks is not None:
        rank_map = precomputed_ranks
    else:
        ctx = FoldContext(k, fold_meta["train_start_ms"], fold_meta["asof_ms"])
        cells = sorted({(r.family, r.hold_band) for r in cfgs.itertuples(index=False)})
        rank_map, rank_df = build_fold_rankings(ctx, cells=cells)
        write_exploratory_parquet(rank_df, out_dir / f"rankings_fold{k}.parquet")
    rankings_all[k] = rank_map
    union = set()
    for roster in rank_map.values():
        if len(roster):
            union |= set(roster["wallet"])
    t1 = time.time()
    actions = collect_fold_actions(union, fold_meta["asof_ms"],
                                   fold_meta["test_end_ms"])
    cand = extract_candidate_journeys(actions, fold_meta["asof_ms"],
                                      fold_meta["test_end_ms"])
    cand_path = out_dir / f"candidates_fold{k}.parquet"
    write_exploratory_parquet(cand, cand_path)
    del actions
    t2 = time.time()
    combos = sorted({(r.exit_style, r.execution, s)
                     for r in cfgs.itertuples(index=False) for s in ("BASE", "WORST")})
    # maker dual-eval needs the matching taker stream as fallback (decision D9)
    extra = {(ex, "taker", s) for (ex, e, s) in combos if e != "taker"}
    combos = sorted(set(combos) | extra)
    if n_procs > 1:
        with Pool(n_procs, initializer=_stream_worker_init,
                  initargs=(fold_meta, cand_path, out_dir, mem_gb)) as pool:
            res = pool.map(_stream_worker, combos)
    else:
        _stream_worker_init(fold_meta, cand_path, out_dir, mem_gb)
        res = [_stream_worker(c) for c in combos]
    t3 = time.time()
    return {"fold": k, "n_union_wallets": len(union), "n_candidates": int(len(cand)),
            "n_streams": len(combos),
            "t_select_s": round(t1 - t0, 1), "t_actions_s": round(t2 - t1, 1),
            "t_streams_s": round(t3 - t2, 1)}


# --------------------------------------------------------------------------------------------- #
# Per-config evaluation across one fold (worker)
# --------------------------------------------------------------------------------------------- #
def _cfg_worker_init(fold_meta, out_dir, mem_gb):
    install_memory_guard(soft_gb=mem_gb, label="v26-cfg")
    _W.clear()
    _W["fold"] = fold_meta
    _W["marks"] = MarksIndex()
    _W["fm"] = FoldMarks(_W["marks"], fold_meta["asof_ms"], fold_meta["test_end_ms"])
    _W["out_dir"] = Path(out_dir)
    _W["snapshot"] = load_snapshot()
    _W["streams"] = {}
    _W["rank"] = pd.read_parquet(Path(out_dir) / f"rankings_fold{fold_meta['fold']}.parquet")


def _get_stream(exit_style, execution, scen) -> pd.DataFrame:
    key = (exit_style, execution, scen)
    df = _W["streams"].get(key)
    if df is None:
        p = stream_path(_W["out_dir"], _W["fold"]["fold"], *key)
        df = pd.read_parquet(p)
        if len(_W["streams"]) > 6:
            _W["streams"].pop(next(iter(_W["streams"])))
        _W["streams"][key] = df
    return df


def _roster(fam: str, band: str, K: int) -> pd.DataFrame:
    r = _W["rank"]
    sub = r[(r["family"] == fam) & (r["band"] == band) & (r["rank"] <= K)]
    return sub


def eval_config_fold(cfg: dict) -> dict:
    """All assemblies for ONE config on the worker's fold: BASE primary, WORST, 3
    dropout-seed stress runs (BASE), and -- for maker configs -- the all-missed-as-taker
    fallback variant of EVERY one of those (codex code-gate #6: the <50%-fill-rate dual
    evaluation applies to BASE, WORST and stress alike; both evaluations are persisted
    and the criteria use the worse). Fails closed: any exception => {'error': ...}
    (runtime_failure, decision D10)."""
    try:
        f = _W["fold"]
        fm = _W["fm"]
        roster = _roster(cfg["family"], cfg["hold_band"], cfg["K"])
        k_real = int(len(roster))
        wallets = set(roster["wallet"])
        emap = dict(zip(roster["wallet"], roster["entity"]))
        out = {"config_id": cfg["config_id"], "fold": f["fold"], "k_real": k_real,
               "variants": {}}
        # k_real == 0 (undersupplied beyond fold 1) still evaluates: an empty roster
        # trades nothing and yields all zero-exposure days (excluded by the estimand)
        is_maker = cfg["execution"] != "taker"
        fb = {"BASE": None, "WORST": None}      # per-scenario taker fallback lookups
        if is_maker:
            for sc in ("BASE", "WORST"):
                tk = _get_stream(cfg["exit_style"], "taker", sc)
                tk = tk[tk["wallet"].isin(wallets)]
                fb[sc] = {(r.wallet, r.coin, int(r.journey_id)): r
                          for r in tk.itertuples(index=False)}
        runs = [("BASE", None, None), ("WORST", None, None)]
        for s in DROPOUT_SEEDS:
            runs.append((f"BASE_seed{s}", s, None))
        if is_maker:
            runs += [(f"{name}_FB", seed, fb["WORST" if name.startswith("WORST")
                                              else "BASE"])
                     for name, seed, _ in list(runs)]
        for name, seed, fbl in runs:
            scen = "WORST" if name.startswith("WORST") else "BASE"
            df = _get_stream(cfg["exit_style"], cfg["execution"], scen)
            df = df[df["wallet"].isin(wallets)]
            eng = FeeEngine(_W["snapshot"], mode=scen)
            res = assemble_config_fold(df, fm, eng, cfg["gross_cap"], cfg["sizing"],
                                       emap, dropout_seed=seed, fallback_lookup=fbl)
            out["variants"][name] = res
        return out
    except Exception as e:
        return {"config_id": cfg["config_id"], "fold": _W["fold"]["fold"],
                "error": f"{type(e).__name__}: {e}",
                "traceback": traceback.format_exc()[-1500:]}


# --------------------------------------------------------------------------------------------- #
# Verdict stage
# --------------------------------------------------------------------------------------------- #
def _agg_config(per_fold: list[dict], fold_list: list[dict], cfg: dict) -> dict:
    """Pool one config's fold results into criteria inputs + the estimand series."""
    from v26_estimand import daily_excess
    agg = {"config_id": cfg["config_id"], "runtime_failure": False}
    if any("error" in r for r in per_fold):
        agg["runtime_failure"] = True
        agg["error"] = "; ".join(r.get("error", "") for r in per_fold if "error" in r)
        return agg
    k_reals = [r["k_real"] for r in per_fold]
    agg["k_real_min"] = int(min(k_reals)) if k_reals else 0
    agg["k_real_by_fold"] = k_reals
    agg["undersupplied"] = bool(any(kr < cfg["K"] for kr in k_reals))
    # maker pooled fill rate + dual-eval variant choice (decision D9)
    fills = missed = 0
    for r in per_fold:
        v = r["variants"].get("BASE")
        if v:
            fills += v["counters"]["maker_fills"]
            missed += v["counters"]["maker_missed"]
    denom = fills + missed
    fill_rate = (fills / denom) if denom else float("nan")
    agg["maker_fill_rate"] = fill_rate
    use_fb = (cfg["execution"] != "taker" and denom > 0
              and fill_rate < MAKER_MIN_FILL_RATE)
    primary, alt = "BASE", ("BASE_FB" if use_fb else None)

    def _series(variant):
        pnl, adm, gro = [], [], []
        for r in per_fold:
            v = r["variants"].get(variant) or r["variants"]["BASE"]
            pnl.append(v["daily_pnl"])
            adm.append(v["admitted"])
            gro.append(v["avg_gross"])
        return (np.concatenate(pnl), np.concatenate(adm), np.concatenate(gro))

    tier = k_real_tier(agg["k_real_min"])
    agg["tier"] = tier
    p1 = _series(primary)
    ex1 = daily_excess(*p1, delta_mult=tier["delta_mult"])
    chosen, chosen_name = ex1, primary
    if alt is not None:
        p2 = _series(alt)
        ex2 = daily_excess(*p2, delta_mult=tier["delta_mult"])
        m1 = np.nanmean(ex1) if np.isfinite(ex1).any() else -np.inf
        m2 = np.nanmean(ex2) if np.isfinite(ex2).any() else -np.inf
        if m2 <= m1:                    # criteria use the WORSE variant
            chosen, chosen_name = ex2, alt
        agg["fb_mean_excess"] = float(m2) if m2 == m2 else None
    agg["variant_used"] = chosen_name
    agg["excess_series"] = chosen
    agg["excess_series_base_hurdle"] = daily_excess(*p1, delta_mult=1.0)

    # criteria inputs (v25-inherited + K-scaled, over the CHOSEN variant where daily;
    # decision D9 scope per codex code-gate #6: the dual evaluation ALSO applies to
    # WORST and stress -- criteria use the worse, both evaluations are persisted)
    vname = chosen_name if chosen_name in ("BASE", "BASE_FB") else "BASE"
    fold_pnls, dds = [], []
    n_real = n_term = n_long = n_short = 0
    ent_trips: dict = {}
    ent_net: dict = {}
    coin_net: dict = {}
    for r in per_fold:
        v = r["variants"].get(vname) or r["variants"]["BASE"]
        fold_pnls.append(v["total_pnl"])
        dds.append(v["max_dd"])
        n_real += v["n_realized"]
        n_term += v["n_terminal"]
        n_long += v["n_long"]
        n_short += v["n_short"]
        for d, dst in ((v["entity_trips"], ent_trips), (v["entity_net"], ent_net),
                       (v["coin_net"], coin_net)):
            for kk, vv in d.items():
                dst[kk] = dst.get(kk, 0) + vv
    def _pooled(variant):
        if any(variant not in r["variants"] for r in per_fold):
            return None
        return sum(r["variants"][variant]["total_pnl"] for r in per_fold)

    def _worse(primary, fb):
        return primary if (fb is None or not use_fb) else min(primary, fb)

    worst_primary = _pooled("WORST")
    worst_fb = _pooled("WORST_FB")
    worst_pooled = _worse(worst_primary, worst_fb)
    stress_primary = [_pooled(f"BASE_seed{s}") for s in DROPOUT_SEEDS]
    stress_fb = [_pooled(f"BASE_seed{s}_FB") for s in DROPOUT_SEEDS]
    stress = [_worse(p, f) for p, f in zip(stress_primary, stress_fb)]
    total_days = sum(f["test_days"] for f in fold_list)
    n_folds = len(fold_list)
    crit = {}
    crit["folds_positive_4of5"] = sum(1 for p in fold_pnls if p > 0) >= min(4, n_folds)
    crit["worst_pooled_positive"] = worst_pooled > 0
    crit["stress_median_positive"] = float(np.median(stress)) > 0 if stress else False
    crit["no_fold_mtm_dd_gt_5pct"] = all(d <= 0.05 for d in dds)
    crit["min_test_days_60"] = total_days >= 60
    crit[f"min_trips_{tier['trips_min']}"] = n_real >= tier["trips_min"]
    crit["min_nonzero_days_30"] = int(np.isfinite(chosen).sum()) >= MIN_NONZERO_DAYS
    n_entities = len([e for e, n in ent_trips.items() if n > 0])
    crit[f"min_entities_{tier['entity_min']}"] = n_entities >= tier["entity_min"]
    if tier["entity_caps"] and n_real > 0:
        shares = np.array(list(ent_trips.values())) / n_real
        crit["entity_trip_conc_le_30pct"] = bool((shares <= 0.30).all())
        dene = sum(abs(v) for v in ent_net.values())
        crit["entity_pnl_conc_le_40pct"] = (dene == 0.0 or
                                            max(abs(v) for v in ent_net.values())
                                            / dene <= 0.40)
    if tier["coin_caps"]:
        denc = sum(abs(v) for v in coin_net.values())
        crit["coin_pnl_conc_le_40pct"] = (denc == 0.0 or (len(coin_net) > 0 and
                                          max(abs(v) for v in coin_net.values())
                                          / denc <= 0.40))
    agg.update({"criteria": crit, "fold_pnls": fold_pnls, "dds": dds,
                "n_realized": n_real, "n_terminal": n_term, "n_long": n_long,
                "n_short": n_short, "worst_pooled": worst_pooled,
                "worst_pooled_primary": worst_primary,
                "worst_pooled_fb": worst_fb,
                "dual_eval_applied": bool(use_fb),
                "stress_by_seed": dict(zip(map(str, DROPOUT_SEEDS), stress)),
                "stress_by_seed_primary": dict(zip(map(str, DROPOUT_SEEDS),
                                                   stress_primary)),
                "stress_by_seed_fb": dict(zip(map(str, DROPOUT_SEEDS), stress_fb)),
                "trips_per_day": n_real / total_days if total_days else 0.0,
                "n_entities_realized": n_entities})
    return agg


def corrections_and_verdict(aggs: list[dict], fold_list: list[dict],
                            family_size: int, out_dir: Path,
                            n_resamples: int, holm_resamples: int) -> dict:
    """Familywise corrections at BOTH block sizes (7d primary + inherited 14d
    robustness, codex code-gate #3). Runtime-failure configs STAY in the family at both
    block sizes (worst-case -inf observed / Holm p = 1, codex code-gate #2); any
    exception or non-finite LCB at EITHER block size triggers the frozen Holm fallback.
    A config whose 14d above/below-hurdle conclusion disagrees with 7d FAILS
    (criteria['block_robustness_agree'], v25 frozen criterion)."""
    from v26_estimand import holm_adjust, holm_fallback, joint_maxstat
    seg_lens = [f["test_days"] for f in fold_list]
    total_days = sum(seg_lens)
    ok = [a for a in aggs if not a["runtime_failure"] and "excess_series" in a]
    n_failures = sum(a["runtime_failure"] for a in aggs)
    M = (np.vstack([a["excess_series"] for a in ok]) if ok
         else np.empty((0, total_days)))
    verdict = {"label": EXPLORATORY_LABEL, "exploratory": True,
               "family_size_nonpruned": family_size,
               "n_evaluated": len(ok), "n_runtime_failures": n_failures}
    method = None
    lcb = lcb14 = mean = None
    pass7 = pass14 = None
    trigger = None
    if len(ok) or n_failures:
        try:
            res7 = joint_maxstat(M, seg_lens, n_resamples=n_resamples,
                                 block_days=GRID_BLOCK_DAYS, n_failures=n_failures)
            res14 = joint_maxstat(M, seg_lens, n_resamples=n_resamples,
                                  block_days=GRID_BLOCK_DAYS_ROBUST,
                                  n_failures=n_failures)
            method, mean, lcb, lcb14 = (res7["method"], res7["mean"], res7["lcb"],
                                        res14["lcb"])
            pass7 = np.isfinite(lcb) & (lcb > 0)
            pass14 = np.isfinite(lcb14) & (lcb14 > 0)
            verdict["c_maxstat"] = res7["c_maxstat"]
            verdict["c_maxstat_14d"] = res14["c_maxstat"]
        except Exception as e:
            trigger = {"type": "exception", "detail": f"{type(e).__name__}: {e}"}
        if method is None:
            trigger = trigger or {"type": "nonfinite_lcb"}
            trigger["fallback"] = "holm_bonferroni_0.05_one_sided"
            trigger["trigger_unix"] = time.time()
            with open(out_dir / "manifest_grid.json", "w") as fh:
                json.dump({"holm_trigger": trigger, "exploratory": True,
                           "label": EXPLORATORY_LABEL}, fh, indent=2)
            try:
                res7 = holm_fallback(M, seg_lens, family_size,
                                     n_resamples=holm_resamples,
                                     block_days=GRID_BLOCK_DAYS,
                                     n_failures=n_failures)
                res14 = holm_fallback(M, seg_lens, family_size,
                                      n_resamples=holm_resamples,
                                      block_days=GRID_BLOCK_DAYS_ROBUST,
                                      n_failures=n_failures)
                method, mean, lcb, lcb14 = (res7["method"], res7["mean"], res7["lcb"],
                                            res14["lcb"])
                # arrays include the appended runtime-failure rows (p = 1, lcb -inf):
                # the Holm adjustment sees the FULL family explicitly
                p7_adj = holm_adjust(res7["p_raw"], family_size)
                p14_adj = holm_adjust(res14["p_raw"], family_size)
                pass7 = p7_adj <= 0.05
                pass14 = p14_adj <= 0.05
                verdict["holm"] = {"family_size": family_size}
            except Exception as e2:
                verdict["fallback_failure"] = f"{type(e2).__name__}: {e2}"
                method = "FAILED"       # fail closed: batch has NO winner
    verdict["method"] = method
    for i, a in enumerate(ok):
        a["mean_excess"] = float(mean[i]) if mean is not None else float("nan")
        a["adjusted_lcb"] = float(lcb[i]) if lcb is not None else float("nan")
        a["adjusted_lcb_14d"] = float(lcb14[i]) if lcb14 is not None else float("nan")
        p7 = bool(pass7[i]) if pass7 is not None else False
        p14 = bool(pass14[i]) if pass14 is not None else False
        # inherited v25 frozen criterion: 14d-block conclusion must AGREE with 7d
        a["criteria"]["block_robustness_agree"] = (p7 == p14) and method not in (
            None, "FAILED")
        a["estimand_pass"] = p7 and method not in (None, "FAILED")
        a["PASS"] = bool(a["estimand_pass"] and all(a["criteria"].values()))
    for a in aggs:
        if a["runtime_failure"]:
            # codex code-gate #2: stays in the family with worst-case values
            a["mean_excess"] = float("-inf")
            a["adjusted_lcb"] = float("-inf")
            a["adjusted_lcb_14d"] = float("-inf")
            a["estimand_pass"] = False
            a["PASS"] = False
            a["holm_p"] = 1.0
    passers = sorted([a for a in aggs if a.get("PASS")],
                     key=lambda a: (-a["adjusted_lcb"], a["config_id"]))
    verdict["n_passers"] = len(passers)
    verdict["winner"] = ({"config_id": passers[0]["config_id"],
                          "adjusted_lcb": passers[0]["adjusted_lcb"],
                          "next_step": "SEALED HOLDOUT once, >= 2026-07-16 (v25 "
                                       "procedure); holdout fail => STOP, no runner-up"}
                         if passers else None)
    if not passers:
        verdict["recommendation"] = "NO-SHIP (no passer among RUN configs)"
    return verdict


def write_outputs(aggs: list[dict], registry: pd.DataFrame, verdict: dict,
                  out_dir: Path):
    rows, kill_rows = [], []
    reg_ix = registry.set_index("config_id")
    for a in aggs:
        cid = a["config_id"]
        r = reg_ix.loc[cid]
        labels = []
        if not a.get("runtime_failure"):
            if a.get("undersupplied"):
                labels.append(f"UNDERSUPPLIED(K_real={a.get('k_real_min')})")
            if a.get("tier", {}).get("label"):
                labels.append(a["tier"]["label"])
        labels.append("EXPLORATORY")
        rows.append({
            "config_id": cid, "family": r["family"], "K": int(r["K"]),
            "hold_band": r["hold_band"], "exit_style": r["exit_style"],
            "gross_cap": float(r["gross_cap"]), "execution": r["execution"],
            "sizing": r["sizing"],
            "k_real_min": a.get("k_real_min", 0),
            "mean_excess": a.get("mean_excess", float("nan")),
            "adjusted_lcb": a.get("adjusted_lcb", float("nan")),
            "adjusted_lcb_14d": a.get("adjusted_lcb_14d", float("nan")),
            "estimand_pass": bool(a.get("estimand_pass", False)),
            "PASS": bool(a.get("PASS", False)),
            "runtime_failure": bool(a.get("runtime_failure", False)),
            "n_realized_trips": a.get("n_realized", 0),
            "trips_per_day": a.get("trips_per_day", 0.0),
            "maker_fill_rate": a.get("maker_fill_rate", float("nan")),
            "variant_used": a.get("variant_used", ""),
            # dual evaluation (codex code-gate #6): BOTH evaluations persisted
            "dual_eval_applied": bool(a.get("dual_eval_applied", False)),
            "worst_pooled": a.get("worst_pooled"),
            # codex cg-r2 #3: BOTH dual evaluations persisted -- primary AND fb
            "worst_pooled_primary": a.get("worst_pooled_primary"),
            "worst_pooled_fb": a.get("worst_pooled_fb"),
            "fb_mean_excess": a.get("fb_mean_excess"),
            "stress_by_seed_json": json.dumps(a.get("stress_by_seed", {}),
                                              default=float),
            "stress_by_seed_primary_json": json.dumps(
                a.get("stress_by_seed_primary", {}), default=float),
            "stress_by_seed_fb_json": json.dumps(a.get("stress_by_seed_fb", {}),
                                                 default=float),
            "labels": "|".join(labels),
            "criteria_json": json.dumps(a.get("criteria", {}), default=bool),
        })
        if not a.get("PASS", False):
            if a.get("runtime_failure"):
                reason = f"runtime_failure: {a.get('error', '')[:300]}"
            elif not a.get("estimand_pass", False):
                reason = "estimand_lcb_not_positive"
            else:
                fails = [k for k, v in a.get("criteria", {}).items() if not v]
                reason = "criteria_fail: " + ",".join(fails)
            kill_rows.append({"config_id": cid, "reason": reason,
                              "label": "EXPLORATORY"})
    frontier = pd.DataFrame(rows).sort_values(
        ["PASS", "adjusted_lcb"], ascending=[False, False], kind="mergesort")
    write_exploratory_parquet(frontier, out_dir / "frontier.parquet")
    write_exploratory_parquet(pd.DataFrame(kill_rows),
                              out_dir / "kill_ledger.parquet")
    # marginals: DESCRIPTIVE, NON-DECISION-MAKING (codex #12): raw cell means with n
    marg_rows = []
    for axis in ["family", "K", "hold_band", "exit_style", "gross_cap", "execution",
                 "sizing"]:
        for val, g in frontier.groupby(axis):
            marg_rows.append({
                "axis": axis, "value": str(val), "n": int(len(g)),
                "mean_excess_cellmean": float(np.nanmean(g["mean_excess"])),
                "adjusted_lcb_cellmean": float(np.nanmean(g["adjusted_lcb"])),
                "pass_rate": float(g["PASS"].mean()),
                "label": "DESCRIPTIVE, NON-DECISION-MAKING (EXPLORATORY)"})
    write_exploratory_parquet(pd.DataFrame(marg_rows), out_dir / "marginals.parquet")
    verdict["exploratory"] = True
    with open(out_dir / "verdict_grid.json", "w") as fh:
        json.dump(verdict, fh, indent=2, default=float)


# --------------------------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------------------------- #
def _pick_smoke_configs(registry: pd.DataFrame, n: int = SMOKE_N_CONFIGS) -> pd.DataFrame:
    run = registry[registry["status"] == "RUN"].sort_values(
        "config_id", kind="mergesort").reset_index(drop=True)
    if len(run) <= n:
        return run
    idx = np.linspace(0, len(run) - 1, n).round().astype(int)
    return run.iloc[sorted(set(idx))].reset_index(drop=True)


def build_registry_from_fold1(out_dir: Path) -> tuple[pd.DataFrame, dict]:
    """Registry enumeration from fold-1 TRAIN candidate counts. Also persists the
    fold-1 rankings (rankings_fold1.parquet) so the run stage reuses them instead of
    recomputing (deterministic, identical output)."""
    f1 = folds()[0]
    ctx = FoldContext(1, f1["train_start_ms"], f1["asof_ms"])
    rank_map, rank_df = build_fold_rankings(ctx)          # all band-RUN cells
    write_exploratory_parquet(rank_df, out_dir / "rankings_fold1.parquet")
    counts = {cell: len(roster) for cell, roster in rank_map.items()}
    registry = enumerate_registry(counts)
    meta = write_registry(registry, out_dir)
    print(f"  registry: {meta['n_run']} RUN / {meta['n_pruned']} PRUNED "
          f"(sha {meta['registry_sha256'][:16]}...)")
    return registry, rank_map


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true",
                    help="20 configs x fold 1 only (ALLOWED pre-freeze)")
    ap.add_argument("--build-registry", action="store_true",
                    help="pre-freeze step: enumerate configs.parquet + prune ledger "
                         "from fold-1 TRAIN candidate counts, then exit")
    ap.add_argument("--confirm-grid", action="store_true")
    ap.add_argument("--procs", type=int, default=6)
    ap.add_argument("--mem-gb", type=float, default=10.0)
    ap.add_argument("--resamples", type=int, default=None,
                    help="resample-count override, ALLOWED ONLY under --smoke "
                         "(full runs use the frozen counts, codex code-gate #7)")
    args = ap.parse_args()
    if args.resamples is not None and not args.smoke:   # cg-r2 #5: 0 must not bypass
        raise SystemExit("--resamples is allowed ONLY under --smoke: full grid runs "
                         "use the FROZEN resample counts (100,000 joint max-stat / "
                         "200,000 Holm fallback)")
    print(BANNER)

    if args.build_registry:
        install_memory_guard(soft_gb=args.mem_gb + 2, label="v26-registry")
        build_registry_from_fold1(V26_DATA)
        print("  registry written; next: commit, run v26_freeze.py, then --confirm-grid")
        return

    if not args.smoke:
        if not args.confirm_grid:
            raise SystemExit("full grid requires --confirm-grid (plus a validating "
                             "FREEZE-GRID.json, a dead v25 runner, and a complete v25 "
                             "verdict). Smoke: --smoke")
        if v25_run_alive():
            raise SystemExit("REFUSING: the v25 fold runner is ALIVE and owns the "
                             "artifact dir; v26 must not consume artifacts mid-write")
        if not v25_verdict_complete():
            raise SystemExit("REFUSING: app/data/research/v25/verdict.json lacks all 5 "
                             "folds; the v25 run has not finished")
        from v26_freeze import validate_freeze_grid
        fz = validate_freeze_grid()
        print(f"  FREEZE-GRID validated (doc {fz['grid_prereg_sha256'][:16]}...)")
        out_dir = V26_DATA
        fold_list = folds()
        registry = load_registry(V26_DATA)
        cfgs = registry[registry["status"] == "RUN"].reset_index(drop=True)
        fold1_ranks = None
        n_res, n_holm = 100_000, 200_000
    else:
        out_dir = V26_DATA / "smoke"
        out_dir.mkdir(parents=True, exist_ok=True)
        fold_list = folds()[:1]
        print("  SMOKE: 20 configs x fold 1; outputs are NOT decision evidence")
        registry, fold1_ranks = build_registry_from_fold1(out_dir)
        cfgs = _pick_smoke_configs(registry)
        n_res, n_holm = 20_000, 40_000
    if args.resamples:
        n_res, n_holm = args.resamples, 2 * args.resamples

    install_memory_guard(soft_gb=args.mem_gb + 2, label="v26-main")
    t_start = time.time()
    rankings_all: dict = {}
    fold_diags = []
    for f in fold_list:
        print(f"  fold {f['fold']}: selection + streams ...")
        pre = fold1_ranks if (f["fold"] == 1 and fold1_ranks is not None) else None
        d = build_fold_streams(f, cfgs, out_dir, rankings_all, args.procs,
                               args.mem_gb, precomputed_ranks=pre)
        fold_diags.append(d)
        print(f"    {d}")

    # per-config evaluation, fold by fold (workers cache streams per fold)
    per_cfg: dict[str, list] = {r.config_id: [] for r in cfgs.itertuples(index=False)}
    cfg_dicts = cfgs.to_dict("records")
    t_eval0 = time.time()
    n_done = 0
    for f in fold_list:
        if args.procs > 1:
            with Pool(args.procs, initializer=_cfg_worker_init,
                      initargs=(f, out_dir, args.mem_gb)) as pool:
                for r in pool.imap_unordered(eval_config_fold, cfg_dicts,
                                             chunksize=4):
                    per_cfg[r["config_id"]].append(r)
                    n_done += 1
                    if n_done % 200 == 0:
                        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30
                        print(f"    {n_done} config-folds, "
                              f"{time.time()-t_eval0:.0f}s, rss={rss:.2f}GB")
        else:
            _cfg_worker_init(f, out_dir, args.mem_gb)
            for cfg in cfg_dicts:
                per_cfg[cfg["config_id"]].append(eval_config_fold(cfg))
                n_done += 1
    t_eval = time.time() - t_eval0
    n_cfg_folds = len(cfg_dicts) * len(fold_list)
    per_sim_s = t_eval / max(n_cfg_folds, 1)

    # daily series artifact (per config, chosen later; store BASE primary)
    dw = ShardedParquetWriter(out_dir / "daily_grid.parquet", flush_rows=500_000)
    for cid, lst in per_cfg.items():
        for r in lst:
            if "error" in r or not r.get("variants"):
                continue
            v = r["variants"]["BASE"]
            for i in range(len(v["daily_pnl"])):
                dw.add({"config_id": cid, "fold": r["fold"], "day_idx": i,
                        "daily_pnl": float(v["daily_pnl"][i]),
                        "admitted_entry_notional": float(v["admitted"][i]),
                        "avg_gross": float(v["avg_gross"][i]),
                        "exploratory": True})
    dw.close()
    stamp_parquet_exploratory(out_dir / "daily_grid.parquet")

    aggs = [_agg_config(per_cfg[cfg["config_id"]], fold_list, cfg)
            for cfg in cfg_dicts]
    family_size = int((registry["status"] == "RUN").sum())
    verdict = corrections_and_verdict(aggs, fold_list, family_size, out_dir,
                                      n_res, n_holm)
    verdict["mode"] = "smoke" if args.smoke else "full"
    verdict["git_commit"] = git_commit()
    verdict["fold_diags"] = fold_diags
    verdict["timing"] = {"total_s": round(time.time() - t_start, 1),
                         "eval_s": round(t_eval, 1),
                         "config_folds": n_cfg_folds,
                         "per_config_fold_s": round(per_sim_s, 3)}
    n_run_full = int((registry["status"] == "RUN").sum())
    verdict["timing"]["projected_full_grid_h"] = round(
        n_run_full * len(folds()) * per_sim_s / max(args.procs, 1) / 3600, 2)
    write_outputs(aggs, registry, verdict, out_dir)

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30
    print(f"\nDONE ({verdict['mode']}). {len(cfg_dicts)} configs x {len(fold_list)} "
          f"folds | eval {t_eval:.0f}s ({per_sim_s:.2f}s/config-fold) | "
          f"projected full grid ~{verdict['timing']['projected_full_grid_h']}h at "
          f"--procs {args.procs} | peak RSS {peak:.2f}GB")
    print(f"  method={verdict['method']} passers={verdict['n_passers']} "
          f"winner={verdict['winner']['config_id'] if verdict['winner'] else 'NONE'}")
    print(f"  {EXPLORATORY_LABEL}")


if __name__ == "__main__":
    main()
