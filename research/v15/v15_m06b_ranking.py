"""V15 M6b -- Copyability-Adjusted FINAL Ranking (post-engine).

Design (FROZEN): brain projects/quant/v15/modules/m06b (codex DESIGN-SHIP r3, Alberto greenlit
2026-06-01). This module is the ONLY ranking that locks the V15 investable pool. It re-ranks the
M6a high-recall shortlist on M7 AFTER-OUR-COST engine results computed FOLD-PRETEST-PURE on
[train_start[k], test_start[k]) -- no OOS leak -- via a fully pre-registered spec, and emits the
per-fold INVESTABLE POOL + a QUALITY-only weight. Raw source ROE never locks the pool (thesis 4b).

SIZING CHAIN (one multiplier per module, applied in M9):
  final_slice = quality_weight(M6b) x confidence(M4 alloc) x survival(M8 tier) x caps/anti-corr(M9)
M6b owns ONLY quality_weight (does NOT multiply M4/M8 -- no double-count).

PROVISIONAL vs FINAL (design §1): an M6b run on uncalibrated costs (slippage_uncalibrated or
fee_unversioned True on its M7 input) is stamped investable=False (pipeline dry-run only). The FINAL
investable pool reruns AFTER cost calibration, recording fee_schedule_version +
slippage_calibration_version in the manifest.

Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v15/v15_m06b_ranking.py \
      --m07-dir app/data/v15/m07_pretest --out app/data/v15
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("m06b")

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
MS_PER_DAY = 86_400_000
# HL round-trip taker fee (fraction of notional). mu_stress (§5 gate 3 soft) subtracts 0.5x this per position
# as a 1.5x-cost APPROXIMATION on the size-neutral r_i. TRUE stress needs a 1.5x-fee/slip M7 re-run (flagged).
_FEE_RT_STRESS = 0.000864

# Pre-registered min CLOSED round-trips for an entity to be RANKABLE / pool-eligible (codex
# finding): M7 emits realized_roe=0.0 / round_trip_win_rate=0.0 when n_round_trips==0, so an
# open-only entity (zero closed round-trips) would otherwise look valid and rank/pool. The rank
# basis is the after-cost REALIZED round-trip return; without enough closed round-trips that basis
# is undefined/meaningless. Require >= this many closed round-trips to enter in_pool/quality_weight.
MIN_ROUND_TRIPS = 5


# --------------------------------------------------------------------------- #
# FROZEN MANIFEST (design §3/§4/§5). Every constant here is pre-registered and
# written verbatim into m06b_manifest.json BEFORE any OOS/M9/M10 run.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class M6bManifest:
    manifest_version: str = "m06b-v2"
    # Score coefficients (relative importances; NOT required to sum to 1 -- mixed scales).
    # v2: return basis switched to M7 REALIZED round-trip ROE (was full-window MTM roe_engine);
    #     win-rate term ADDED. New positive-weight vector sums to 0.95 (1.00 - 0.15 - 0.10 net of
    #     the subtracted survivability penalty kept at its prior magnitude):
    #       0.25 z_realized_roe + 0.20 z_calmar + 0.15 z_win_rate + 0.15 consistency
    #       + 0.10 capacity_health + 0.10 fidelity - 0.15 survivability_penalty
    w_realized_roe: float = 0.25
    w_calmar: float = 0.20
    w_win_rate: float = 0.15
    w_consistency: float = 0.15
    w_capacity_health: float = 0.10
    w_fidelity: float = 0.10
    w_survivability_penalty: float = 0.15  # subtracted
    # === V2.2 PER-POSITION SCORE (2026-07-23, Alberto: unit = round-trip journey; drop roe/calmar/win-rate)
    # Active whenever m07_positions is present. Ranks on Alberto's WHO criteria, on the after-OUR-cost
    # per-position return r_i (win-rate KILLED - payoff-blind; calmar KILLED - short-window noise):
    #   +0.30 z(pp_mean_r)  consistently profitable (per-journey mean net return)
    #   +0.25 z(pp_t)       consistency = edge/noise (t-stat; a big mean with high variance is not an edge)
    #   -0.20 z(pp_std_r)   low risk (low std of per-journey return)
    #   -0.15 z(max_dd)     NO unrealized loss = mark-to-market drawdown (proper MTM, not a blowup proxy)
    #   +0.10 z(frac_quick) quick-to-close (fraction of journeys closed <=48h; not baggers)
    #   -0.15 survivability_penalty (kept). capacity_health/fidelity remain as reported diagnostics only.
    # FAT-EDGE re-weight (Alberto 2026-07-23: "if your preselection only has 30-50bps gross edge you're targeting
    # the WRONG wallets"). For the cost-barrier problem, EDGE MAGNITUDE that dwarfs the ~8.6bps cost is the target,
    # NOT thin-but-consistent edge. So: mean-return DOMINATES; the variance penalty is cut to near-zero (it was
    # burying fat-but-noisy wallets); consistency kept small; MTM-unrealized-loss + quick-close retained (don't
    # want baggers/multi-week holds even at fat edge).
    w_pp_mean_r: float = 0.45      # edge magnitude — DOMINANT
    w_pp_t: float = 0.15           # consistency (edge/noise) — reduced
    w_pp_std: float = 0.05         # subtracted — near-zero (do NOT penalize fat-edge variance)
    w_pp_mtm_dd: float = 0.20      # subtracted: penalize POSITION drawdown (MAE p90), not account-equity DD
    w_pp_quick: float = 0.15
    # V2.2 rankable gates (Alberto WHO + review): min journeys, net-edge lower bound, quick-close, MTM ceiling
    pp_min_positions: int = 25          # material support (round-trips)
    pp_min_lcb_mean_r: float = 0.0      # COST-FEASIBILITY: 95% one-sided lower bound on per-journey NET return > 0
    pp_max_med_hold_h: float = 48.0     # quick to close (median)
    pp_max_p90_hold_h: float = 168.0    # ... and not the occasional multi-week bag (p90 <= 7d)
    pp_max_mtm_dd: float = 0.15         # no meaningful unrealized loss (MTM drawdown ceiling)
    # walk-forward OOS confirmation (walk_forward_confirm) — POOLED multi-fold + BH-FDR (Fable+Codex 2026-07-23).
    # Codex STANDARD is oos_min_folds=4 / oos_min_journeys_pooled=50; defaults here are the FLOOR that the
    # currently-available 3 OOS folds (recency actions Apr06-Jul22) can support -> raise to the standard once
    # the FULL 12-fold action bootstrap exists. NEVER tune these to hit a target wallet count.
    oos_min_folds: int = 2              # min NON-OVERLAPPING held-out OOS folds a wallet must trade in
    oos_min_journeys_pooled: int = 30   # min POOLED OOS journeys across those folds (SE control)
    oos_min_frac_folds_pos: float = 0.5 # net-positive in a MAJORITY of OOS folds (regime stability)
    oos_margin: float = 0.0             # H0 margin on pooled mean NET per-journey return (0 = break-even; raise for slippage/model-risk buffer)
    fdr_q: float = 0.10                 # Benjamini-Hochberg false-discovery rate across all eligible wallets
    # calmar / winsor / fidelity
    dd_floor: float = 0.05            # calmar = realized_roe / max(max_dd, dd_floor)
    winsor_lo_pct: float = 1.0        # winsorize calmar + realized_roe at fold [p1, p99]
    winsor_hi_pct: float = 99.0
    fidelity_B: float = 0.25          # fidelity = 1 - clamp(tracking_error / B, 0, 1)  (M7 D5)
    # consistency sub-split geometry (design §3): consecutive 14d blocks FROM train_start; trailing
    # partial (<14d) DROPPED; sub-split ACTIVE iff >=1 journey opened AND >=5 fills in it.
    block_days: int = 14
    consistency_active_min_journeys: int = 1
    consistency_active_min_fills: int = 5
    consistency_min_active_subsplits: int = 2  # < this -> consistency = 0, not top-bucket eligible
    # min-support to be RANKABLE
    min_fills_pretest: int = 30
    min_exposure_days: float = 3.0
    min_active_subsplits_support: int = 2
    # pool selection + G5
    n_pool: int = 100
    g5_min_active_pretest_folds: int = 3
    g5_min_journeys_pretest: int = 5
    # allocation -- quality weight only
    bucket_weights: tuple = (5, 4, 3, 2, 1)   # quintile buckets, normalized to sum 1
    n_buckets: int = 5
    top_bucket_consistency_gate: float = 0.5  # bucket-1 entity with consistency < this -> demote 1
    per_entity_quality_ceiling: float = 0.10  # pre-M9 ceiling = 10% of pool total
    # provenance / provisional
    informed_by: str = "pretest-distribution-only"
    fee_schedule_version: Optional[str] = None
    slippage_calibration_version: Optional[str] = None


# --------------------------------------------------------------------------- #
# INPUT LOADING (design §2: fold-pretest-pure; provenance fail-closed)
# --------------------------------------------------------------------------- #
def _read_parquet_maybe_parts(path: Path) -> pd.DataFrame:
    """Read a parquet that may be a single file OR a `<name>.parts/` dir of shards (streaming I/O)."""
    parts = path.parent / (path.name + ".parts")
    if path.exists() and path.is_file():
        return pd.read_parquet(path)
    if parts.exists() and parts.is_dir():
        shards = sorted(parts.glob("*.parquet"))
        if not shards:
            return pd.DataFrame()
        return pd.concat((pd.read_parquet(s) for s in shards), ignore_index=True)
    raise FileNotFoundError(f"neither {path} nor {parts} exists")


def _load_m04_by_fold(m04_dir: Path, folds: pd.DataFrame, stem: str) -> pd.DataFrame:
    parts = []
    for fid in sorted(folds["fold_id"].astype(int).unique()):
        p = m04_dir / f"{stem}_f{fid}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"missing fold-pure M4 file for fold {fid}: {p}")
        df = pd.read_parquet(p).copy()
        df["fold_id"] = int(fid)
        parts.append(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _per_position_metrics(m07_dir: Path) -> pd.DataFrame | None:
    """V2.2 (2026-07-23): per-(entity,fold) metrics from M7's PER-POSITION emit (m07_positions.parquet;
    Increment-1). This is the NEW ranking basis replacing aggregate realized_roe/calmar/win-rate. Unit =
    the round-trip journey (Alberto: "the unit of evidence is the round-trip journey"). Computes, on the
    after-OUR-cost per-position return r_i = realized_pnl_after_cost / peak_notional:
      n_pos, mean_r, std_r, t_stat (mean/(std/sqrt(n)) = edge/noise = consistency), lcb_mean_r (one-sided
      95% lower bound, mean - 1.645*se = the cost-feasibility net-edge lower bound), med_hold_h + frac_quick
      (<=48h) = quick-to-close, uw_add_ratio (DCA, reported not gated). Returns None if the emit is absent
      (older M7 run) -> build_ranking falls back to the legacy summary scoring."""
    p = m07_dir / "m07_positions.parquet"
    df = _read_parquet_maybe_parts(p) if (p.exists() or (m07_dir / "m07_positions.parquet").is_dir()) else None
    if df is None or df.empty:
        return None
    df = df.copy()
    df["r_i"] = pd.to_numeric(df["r_i"], errors="coerce")
    df = df.dropna(subset=["r_i"])
    if df.empty:
        return None
    df["hold_h"] = (pd.to_numeric(df["exit_ts"], errors="coerce")
                    - pd.to_numeric(df["entry_ts"], errors="coerce")) / 3.6e6
    df["_blk14"] = (pd.to_numeric(df["entry_ts"], errors="coerce") // (14 * MS_PER_DAY)).astype("Int64")
    def _agg(g):
        n = len(g); mean_r = g["r_i"].mean(); std_r = g["r_i"].std(ddof=1) if n > 1 else np.nan
        se = (std_r / np.sqrt(n)) if (std_r == std_r and n > 1) else np.nan
        t = (mean_r / se) if (se and se == se and se > 0) else np.nan
        # §5 gate 2 (Codex fix): LCB_mu via 14-DAY BLOCK (circular/cluster) bootstrap, NOT the normal-IID SE
        # (serially-correlated returns -> IID SE too small = biased loose). effective-n = # active 14d blocks
        # (gate 1 requires >=8). Fail-closed (nan LCB) when <3 blocks so a thin wallet can't clear gate 2.
        blk = g["_blk14"].to_numpy()
        ublk = pd.unique(blk[~pd.isna(blk)])
        eff_blocks = int(len(ublk))
        r = g["r_i"].to_numpy("float64")
        if eff_blocks >= 3:
            _rng = np.random.default_rng(20260724)
            groups = [r[blk == b] for b in ublk]
            nb = len(groups)
            bm = np.array([np.concatenate([groups[j] for j in _rng.integers(0, nb, size=nb)]).mean()
                           for _ in range(1500)])
            lcb_boot = float(np.percentile(bm, 5))
        else:
            lcb_boot = np.nan
        # §5 gate 6 (Codex fix): POSITION concentration (largest single-position |$PnL| share), not coin.
        rz = pd.to_numeric(g.get("realized_pnl_after_cost"), errors="coerce")
        tot_abs_pos = rz.abs().sum()
        pos_conc = float(rz.abs().max() / tot_abs_pos) if tot_abs_pos > 0 else np.nan
        lcb = (mean_r - 1.645 * se) if (se == se) else np.nan
        uw = pd.to_numeric(g.get("underwater_add_ratio"), errors="coerce")
        # POSITION DRAWDOWN (Alberto 2026-07-24 "It should be position drawdown", "MAE and MFE are crucial"):
        # mae = worst underwater frac vs entry VWAP over a position's life (<=0). This is the TRUE per-position
        # drawdown replacing the account-equity max_dd proxy. |mae| aggregated as median (typical bag) + p90
        # (tail bag). A bag-holder = deep, frequent MAE. mfe = best in-money frac reached; giveback = mfe - r_i
        # (run-up surrendered at the leader's exit) -> where OUR trailing-TP can beat mirroring their exit.
        mae = pd.to_numeric(g.get("mae"), errors="coerce")
        mfe = pd.to_numeric(g.get("mfe"), errors="coerce")
        gb = pd.to_numeric(g.get("mfe_giveback"), errors="coerce")
        amae = mae.abs()
        # === m6_redesign_v2 §5 additional metrics ===
        realized_usd = pd.to_numeric(g.get("realized_pnl_after_cost"), errors="coerce")
        tuw = (pd.to_numeric(g["time_underwater"], errors="coerce")      # gate 5 (emitted 2026-07-24)
               if "time_underwater" in g.columns else pd.Series(dtype=float))
        # mu_stress (gate 3 soft): 1.5x fee/slip => subtract 0.5x RT fee per position from r_i (approx;
        # true stress needs a 1.5x-cost M7 re-run — flagged approximate).
        mu_stress = float((g["r_i"] - 0.5 * _FEE_RT_STRESS).mean())
        # Concentration C_position (gate 6): max single-coin |dollar PnL| / total |dollar PnL|.
        if "coin" in g.columns and realized_usd.notna().any():
            by_coin = realized_usd.groupby(g["coin"]).sum()
            tot_abs = by_coin.abs().sum()
            c_pos = float(by_coin.abs().max() / tot_abs) if tot_abs > 0 else np.nan
            # drop-best-position (by $ PnL) leaves net-positive
            drop_best_pos = float(realized_usd.sum() - realized_usd.max())
        else:
            c_pos = np.nan; drop_best_pos = np.nan
        # Realization (gate 4b/support): frac of positions opened >=7d before window end that are closed.
        # NOTE: the sim force-closes at fold end, so 'closed' is near-degenerate here -> realization from the
        # sim is UNRELIABLE; the true realization gate needs on-chain leader close-turnover. FLAGGED.
        closed = g.get("closed")
        realization = float(pd.to_numeric(closed, errors="coerce").fillna(1).mean()) if closed is not None else np.nan
        return pd.Series({
            "pp_n": n, "pp_mean_r": mean_r, "pp_std_r": std_r, "pp_t": t, "pp_lcb_mean_r": lcb,
            "pp_med_hold_h": g["hold_h"].median(), "pp_p90_hold_h": g["hold_h"].quantile(0.9),
            "pp_frac_quick": (g["hold_h"] <= 48).mean(),
            "pp_realized": realized_usd.sum() if realized_usd.notna().any() else np.nan,
            "pp_uw_add": (uw.mean() if uw.notna().any() else np.nan),
            # position-drawdown (MAE) block
            "pp_mae_med": (amae.median() if amae.notna().any() else np.nan),
            "pp_mae_p90": (amae.quantile(0.9) if amae.notna().any() else np.nan),
            "pp_frac_underwater": ((mae < 0).mean() if mae.notna().any() else np.nan),
            # favorable-excursion (MFE) block
            "pp_mfe_med": (mfe.median() if mfe.notna().any() else np.nan),
            "pp_giveback_med": (gb.median() if gb.notna().any() else np.nan),
            # §5 spec metrics
            "pp_mu_stress": mu_stress,                                    # gate 3 SOFT (not a hard gate)
            "pp_lcb_boot": lcb_boot,                                      # gate 2: 14d block-bootstrap LCB_mu
            "pp_eff_blocks": eff_blocks,                                  # gate 1: effective-n (active 14d blocks)
            "pp_time_uw_p90": (tuw.quantile(0.9) if tuw.notna().any() else np.nan),  # gate 5
            "pp_time_uw_med": (tuw.median() if tuw.notna().any() else np.nan),
            "pp_concentration": c_pos,                                    # coin-level (deployment, diagnostic)
            "pp_pos_concentration": pos_conc,                            # gate 6 POSITION-level $PnL concentration
            "pp_drop_best_pos": drop_best_pos,                            # gate 6 (net-positive after drop-best)
            "pp_realization": realization,                               # gate 4b (FLAGGED sim-degenerate)
        })
    out = df.groupby(["entity_id", "fold_id"]).apply(_agg).reset_index()
    return out


def _same_coin_alpha(m07_dir: Path, folds: "pd.DataFrame", n_boot: int = 2000, seed: int = 7) -> "pd.DataFrame | None":
    """m6_redesign_v2 §5 GATE 8 (#E-bench), FAITHFUL per §4/v2.2 (Codex 2026-07-24 caught my prior per-position
    same-timing version = the EXPLICITLY-KILLED degenerate benchmark). Correct benchmark = PASSIVE buy&hold of the
    same coin over the WHOLE pretest window [train_start, test_start), exposure-weighted to the wallet's exposure on
    that coin. Per (entity,fold,coin): wallet_ret_c = Σrealized_after_cost / Σpeak_notional (exposure-weighted
    realized return); passive_c = coin's window return (mark(win_end)-mark(win_start))/mark(win_start); alpha_c =
    wallet_ret_c − passive_c. Aggregate exposure-weighted across coins -> pp_alpha_mean. LCB via bootstrap over the
    per-coin alphas (cluster=coin, removes coin beta; not iid per-journey). Gate: pp_alpha_lcb > 0."""
    p = m07_dir / "m07_positions.parquet"
    df = _read_parquet_maybe_parts(p) if (p.exists() or (m07_dir / "m07_positions.parquet").is_dir()) else None
    if df is None or df.empty:
        return None
    df = df.copy()
    for c in ("realized_pnl_after_cost", "peak_notional"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    df = df.dropna(subset=["realized_pnl_after_cost", "peak_notional", "coin", "fold_id"])
    if df.empty:
        return None
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from v15_m07_engine import MarketData, build_ohlc_cache, coin_is_spot
    # fold pretest-window bounds [train_start, test_start) in ms
    win = {int(r.fold_id): (int(pd.Timestamp(r.train_start).timestamp() * 1000),
                            int(pd.Timestamp(r.test_start).timestamp() * 1000))
           for r in folds.itertuples()}
    coins = sorted({c for c in df["coin"].unique() if c and not coin_is_spot(c)})
    build_ohlc_cache(coins)
    md = MarketData(require_cache=True)
    df = df[~df["coin"].map(coin_is_spot)].copy()
    # per (entity, fold, coin) aggregate: exposure (Σpeak) + wallet realized return + passive-hold coin return
    grp = df.groupby(["entity_id", "fold_id", "coin"], sort=False)
    agg = grp.agg(realized=("realized_pnl_after_cost", "sum"),
                  exposure=("peak_notional", "sum")).reset_index()
    agg = agg[agg["exposure"] > 0]
    _pass_cache: dict = {}
    def _passive(coin, fid):
        k = (coin, fid)
        if k in _pass_cache:
            return _pass_cache[k]
        t0, t1 = win.get(int(fid), (None, None))
        v = np.nan
        if t0 is not None:
            p0 = md.mark(coin, t0, causal=True); p1 = md.mark(coin, t1, causal=True)
            if p0 and p1 and p0 > 0:
                v = (p1 - p0) / p0
        _pass_cache[k] = v
        return v
    agg["wallet_ret_c"] = agg["realized"] / agg["exposure"]
    agg["passive_c"] = [_passive(c, f) for c, f in zip(agg["coin"], agg["fold_id"])]
    agg["alpha_c"] = agg["wallet_ret_c"] - agg["passive_c"]
    agg = agg.dropna(subset=["alpha_c"])
    if agg.empty:
        return None

    def _agg_alpha(g):
        a = g["alpha_c"].to_numpy("float64"); w = g["exposure"].to_numpy("float64")
        wsum = w.sum()
        mean_a = float((a * w).sum() / wsum) if wsum > 0 else np.nan
        n = len(a)
        if n < 3:
            lcb = np.nan   # too few coins to bootstrap -> not gate-eligible (fail-closed)
        else:
            rng = np.random.default_rng(seed)
            idx = rng.integers(0, n, size=(n_boot, n))   # resample coins (cluster bootstrap)
            bw = w[idx]; ba = a[idx]
            means = (ba * bw).sum(axis=1) / bw.sum(axis=1)
            lcb = float(np.percentile(means, 5))
        return pd.Series({"pp_alpha_mean": mean_a, "pp_alpha_lcb": lcb, "pp_alpha_ncoins": n})
    return agg.groupby(["entity_id", "fold_id"]).apply(_agg_alpha).reset_index()


def load_inputs(m07_dir: Path, data_dir: Path = DATA_DIR, m04_dir: Path | None = None) -> dict:
    """Load the M7 pretest-window engine results + upstream M6a/M5/M4/M3 inputs.

    Returns a dict of DataFrames. Provenance fail-closed checks happen in build_ranking once we
    know each fold's test_start.
    """
    folds = pd.read_parquet(data_dir / "m03_folds.parquet")

    m07_summary = _read_parquet_maybe_parts(m07_dir / "m07_summary.parquet")
    m07_positions_agg = _per_position_metrics(m07_dir)   # V2.2 per-position ranking basis (None if absent)
    # m07_fills only needed for exposure_time (per (entity,fold) our_ts span). Read lazily/aggregated.
    fills_path = m07_dir / "m07_fills.parquet"

    m06a = pd.read_parquet(data_dir / "m06a_shortlist.parquet")
    m05 = pd.read_parquet(data_dir / "m05_eligibility.parquet")
    if m04_dir is None:
        logger.warning("LOUD WARNING: using single M4 authenticity/entities files for all folds; "
                       "M6b M4 consumption is not fold-pure. Pass --m04-dir.")
        m04_entities = pd.read_parquet(data_dir / "m04_entities.parquet")
        # wallet -> entity_id map (for per-block journey-open counts from m02 when equity is present)
        m04_auth = pd.read_parquet(data_dir / "m04_authenticity.parquet")
        m04_fold_pure = False
    else:
        m04_entities = _load_m04_by_fold(m04_dir, folds, "m04_entities")
        m04_auth = _load_m04_by_fold(m04_dir, folds, "m04_authenticity")
        m04_fold_pure = True
    m02_journeys_path = data_dir / "m02_journeys.parquet"

    # optional: per-action equity path (FINAL consistency source). Absent in current shipped runner.
    eq_path = m07_dir / "m07_equity.parquet"
    m07_equity = None
    try:
        m07_equity = _read_parquet_maybe_parts(eq_path)
        if m07_equity.empty:
            m07_equity = None
    except FileNotFoundError:
        m07_equity = None

    return {
        "folds": folds,
        "m07_summary": m07_summary,
        "m07_fills_path": fills_path,
        "m06a": m06a,
        "m05": m05,
        "m04_entities": m04_entities,
        "m04_auth": m04_auth,
        "m04_fold_pure": m04_fold_pure,
        "m02_journeys_path": m02_journeys_path,
        "m07_equity": m07_equity,
        "m07_positions_agg": m07_positions_agg,
    }


def _exposure_days_from_fills(fills_path: Path, m07_dir: Path) -> pd.DataFrame:
    """Per (entity_id, fold_id) exposure_time in days = (max our_ts - min our_ts)/day, from the M7
    fills stream. Single streamed groupby; reads only the needed columns."""
    df = _read_parquet_maybe_parts(fills_path)
    if df.empty:
        return pd.DataFrame(columns=["entity_id", "fold_id", "exposure_days"])
    g = df.groupby(["entity_id", "fold_id"])["our_ts"].agg(["min", "max"]).reset_index()
    g["exposure_days"] = (g["max"] - g["min"]) / MS_PER_DAY
    return g[["entity_id", "fold_id", "exposure_days"]]


# --------------------------------------------------------------------------- #
# CONSISTENCY (design §3): per-sub-split (14d block from train_start) ENGINE after-cost ROE.
# FINAL source = m07_equity per-block ROE. PROVISIONAL fallback = M6a source persistence (clearly
# flagged non-final; provisional outputs are investable=False anyway).
# --------------------------------------------------------------------------- #
def _blocks_activity(fills_path, m02_journeys_path: Path, m04_auth: pd.DataFrame,
                     folds: pd.DataFrame, m: M6bManifest) -> pd.DataFrame:
    """Per (entity_id, fold_id, block_idx) -> n_fills + n_journeys, for the consistency activeness
    test (active iff >= min_journeys opened AND >= min_fills in the block). Blocks are consecutive
    14d windows FROM train_start within the pretest [train_start, test_start); trailing partial
    (block_idx >= n_full) is excluded by the consumer. Only computed when m07_equity is present
    (FINAL path); the provisional path uses M6a source persistence and does not call this.

    fills_path: m07 fills (entity_id, fold_id, our_ts) -> per-block n_fills (engine fills).
    m02 journeys (wallet, entry_ts) mapped to entity via m04_auth (wallet->entity_id) -> per-block
    journey OPENS (entry_ts in block), assigned to every fold whose pretest window contains entry_ts.
    """
    block_ms = m.block_days * MS_PER_DAY
    fold_win = [(int(r.fold_id), pd.Timestamp(r.train_start).value // 1_000_000,
                 pd.Timestamp(r.test_start).value // 1_000_000) for r in folds.itertuples()]

    # --- fills per (entity, fold, block) ---
    fdf = fills_path if isinstance(fills_path, pd.DataFrame) else _read_parquet_maybe_parts(fills_path)
    f_rows = []
    if not fdf.empty:
        fdf = fdf[["entity_id", "fold_id", "our_ts"]].copy()
        for fid, t0, t1 in fold_win:
            sub = fdf[fdf.fold_id == fid]
            if sub.empty:
                continue
            blk = ((sub["our_ts"] - t0) // block_ms).astype(int)
            sub = sub.assign(block_idx=blk)
            sub = sub[(sub["our_ts"] >= t0) & (sub["our_ts"] < t1)]
            g = sub.groupby(["entity_id", "block_idx"]).size().reset_index(name="n_fills_block")
            g["fold_id"] = fid
            f_rows.append(g)
    fills_blocks = (pd.concat(f_rows, ignore_index=True) if f_rows
                    else pd.DataFrame(columns=["entity_id", "block_idx", "n_fills_block", "fold_id"]))

    # --- journey opens per (entity, fold, block) from m02 entry_ts ---
    has_fold_m04 = "fold_id" in m04_auth.columns
    if has_fold_m04:
        w2e_by_fold = {int(fid): g[["wallet", "entity_id"]].drop_duplicates("wallet")
                       for fid, g in m04_auth.groupby("fold_id")}
        w2e = None
    else:
        w2e = m04_auth[["wallet", "entity_id"]].drop_duplicates("wallet")
        w2e_by_fold = {}
    j_rows = []
    try:
        jdf = _read_parquet_maybe_parts(Path(m02_journeys_path)) if not isinstance(
            m02_journeys_path, pd.DataFrame) else m02_journeys_path
        validity = {"lifecycle_valid", "stream_replay_valid"}
        if not validity.issubset(jdf.columns):
            if not isinstance(m02_journeys_path, pd.DataFrame):
                raise ValueError(
                    "M02 journeys lack lifecycle/stream replay validity; rebuild M2"
                )
        else:
            jdf = jdf[
                jdf["lifecycle_valid"].fillna(False).astype(bool)
                & jdf["stream_replay_valid"].fillna(False).astype(bool)
            ].copy()
        jdf = jdf[["wallet", "entry_ts"]].copy()
        for fid, t0, t1 in fold_win:
            mapper = w2e_by_fold.get(int(fid), pd.DataFrame(columns=["wallet", "entity_id"])) \
                if has_fold_m04 else w2e
            sub = jdf[(jdf["entry_ts"] >= t0) & (jdf["entry_ts"] < t1)].merge(
                mapper, on="wallet", how="inner")
            if sub.empty:
                continue
            blk = ((sub["entry_ts"] - t0) // block_ms).astype(int)
            sub = sub.assign(block_idx=blk, fold_id=fid)
            g = sub.groupby(["entity_id", "fold_id", "block_idx"]).size().reset_index(
                name="n_journeys_block")
            j_rows.append(g)
    except (FileNotFoundError, KeyError):
        pass
    journeys_blocks = (pd.concat(j_rows, ignore_index=True) if j_rows
                       else pd.DataFrame(columns=["entity_id", "fold_id", "block_idx", "n_journeys_block"]))

    out = fills_blocks.merge(journeys_blocks, on=["entity_id", "fold_id", "block_idx"], how="outer")
    out["n_fills_block"] = out["n_fills_block"].fillna(0).astype(int)
    out["n_journeys_block"] = out["n_journeys_block"].fillna(0).astype(int)
    return out


def _consistency_from_equity(equity: pd.DataFrame, folds: pd.DataFrame,
                             m: M6bManifest, blocks_activity: Optional[pd.DataFrame]) -> pd.DataFrame:
    """consistency = (# active sub-splits with block roe_adj>0) / (# active sub-splits).
    A sub-split is ACTIVE iff >= min_journeys opened AND >= min_fills in it. Block roe_adj = engine
    after-cost ROE on that block = eq[block_end]/eq[block_start] - 1.

    NOTE: activeness needs per-block journey + fill counts. fills_blocks (per entity,fold,block ->
    n_fills, n_journeys) supplies them; when unavailable we fall back to fill-count-only activeness
    from the equity sample density (conservative: still requires the block to carry >=min_fills
    equity-action samples).
    """
    rows = []
    fold_win = {int(r.fold_id): (pd.Timestamp(r.train_start).value // 1_000_000,
                                 pd.Timestamp(r.test_start).value // 1_000_000)
                for r in folds.itertuples()}
    block_ms = m.block_days * MS_PER_DAY
    # (entity, fold, block) -> (n_fills_block, n_journeys_block) for the activeness test.
    ba = {}
    keys_act = set()
    if blocks_activity is not None and not blocks_activity.empty:
        for r in blocks_activity.itertuples():
            ba[(int(r.entity_id), int(r.fold_id), int(r.block_idx))] = (
                int(r.n_fills_block), int(r.n_journeys_block))
            keys_act.add((int(r.entity_id), int(r.fold_id)))
    # equity grouped for O(1) lookup; iterate the UNION of (entity,fold) from blocks_activity AND
    # equity (codex r3 #1: an entity active by fills+journeys but with NO equity rows must still get
    # active=N, positive=0, consistency=0 -- fail-closed, never silently absent).
    eq_groups = {(int(k[0]), int(k[1])): g for k, g in equity.groupby(["entity_id", "fold_id"])} \
        if not equity.empty else {}
    all_keys = sorted(keys_act | set(eq_groups.keys()))  # sorted -> deterministic row order
    for (eid, fid) in all_keys:
        if int(fid) not in fold_win:
            continue
        t0, t1 = fold_win[int(fid)]
        grp = eq_groups.get((int(eid), int(fid)))
        anchor_ts = anchor_eq = None
        if grp is not None and not grp.empty:
            grp = grp.sort_values("ts")
            ts = grp["ts"].to_numpy()
            eq = grp["subaccount_equity"].to_numpy()
            # D4: per-block ROE uses the EXACT boundary anchors (fold_start/block_boundary/fold_end),
            # via nearest-at-or-before lookup -- NOT within-block first/last (which excludes the exact
            # block-end sample). Fall back to within-block only when no anchors exist (synthetic equity).
            if "event_flag" in grp.columns:
                am = grp["event_flag"].isin(["fold_start", "block_boundary", "fold_end"]).to_numpy()
                if am.any():
                    anchor_ts = ts[am]
                    anchor_eq = eq[am]
        else:
            ts = np.empty(0, dtype="int64")
            eq = np.empty(0, dtype="float64")
        # consecutive 14d blocks FROM train_start; drop trailing partial (<14d). The ACTIVE
        # denominator is driven by _blocks_activity (fills+journeys), NOT by equity-sample presence
        # (codex r2 #A: skipping equity-empty blocks biased consistency upward). A block that is
        # active by nf/nj counts in the denominator; it is POSITIVE only if equity actually covers
        # it AND its after-cost block ROE > 0 (an active block with no equity coverage = no
        # measurable positive ROE = NOT positive; conservative, no leak).
        n_full = int((t1 - t0) // block_ms)
        active = 0
        positive = 0
        for b in range(n_full):
            nf, nj = ba.get((int(eid), int(fid), b), (0, 0))
            if nf < m.consistency_active_min_fills or nj < m.consistency_active_min_journeys:
                continue  # block not active -> excluded from BOTH numerator and denominator
            active += 1
            b0 = t0 + b * block_ms
            b1 = t0 + (b + 1) * block_ms
            if anchor_ts is not None and anchor_ts.size:
                # D4: anchor-to-anchor block ROE = eq_anchor(b1)/eq_anchor(b0) - 1.
                i0 = int(np.searchsorted(anchor_ts, b0, side="right")) - 1
                i1 = int(np.searchsorted(anchor_ts, b1, side="right")) - 1
                if i0 >= 0 and i1 >= 0:
                    e0, e1 = float(anchor_eq[i0]), float(anchor_eq[i1])
                    if e0 > 0 and (e1 / e0 - 1.0) > 0:
                        positive += 1
            else:
                mask = (ts >= b0) & (ts < b0 + block_ms)
                if mask.any():
                    eq_in = eq[mask]
                    if eq_in[0] > 0 and (eq_in[-1] / eq_in[0] - 1.0) > 0:
                        positive += 1
        cons = 0.0 if active < m.consistency_min_active_subsplits else positive / active
        rows.append({"entity_id": int(eid), "fold_id": int(fid),
                     "consistency": cons, "n_active_subsplits": active,
                     "consistency_source": "m07_equity_block_roe"})
    return pd.DataFrame(rows)


def _consistency_provisional(m06a: pd.DataFrame) -> pd.DataFrame:
    """PROVISIONAL fallback when m07_equity is absent. Uses M6a source persistence
    (active_blocks/possible_blocks) as a SOURCE proxy for cross-sub-split repeatability. Flagged
    non-final: provisional M6b is investable=False, so this never reaches LP/live evidence."""
    df = m06a[["entity_id", "fold_id", "active_blocks", "possible_blocks"]].copy()
    poss = df["possible_blocks"].clip(lower=1)
    df["consistency"] = (df["active_blocks"] / poss).clip(0, 1).fillna(0.0)
    df["n_active_subsplits"] = df["active_blocks"].fillna(0).astype(int)
    df["consistency_source"] = "m6a_source_persistence_PROVISIONAL"
    return df[["entity_id", "fold_id", "consistency", "n_active_subsplits", "consistency_source"]]


# --------------------------------------------------------------------------- #
# SCORING
# --------------------------------------------------------------------------- #
def _winsorize(s: pd.Series, lo_pct: float, hi_pct: float) -> pd.Series:
    if s.notna().sum() == 0:
        return s
    lo = np.nanpercentile(s, lo_pct)
    hi = np.nanpercentile(s, hi_pct)
    return s.clip(lower=lo, upper=hi)


def _zscore(s: pd.Series) -> pd.Series:
    mu = s.mean()
    sd = s.std(ddof=0)
    if not np.isfinite(sd) or sd == 0:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - mu) / sd


def build_ranking(inputs: dict, m: M6bManifest) -> tuple[pd.DataFrame, dict]:
    """Core M6b pipeline. Returns (pool_df, manifest_dict)."""
    folds = inputs["folds"]
    summ = inputs["m07_summary"].copy()
    m06a = inputs["m06a"].copy()
    m05 = inputs["m05"].copy()
    m04e = inputs["m04_entities"].copy()

    # --- provenance: per-fold pretest-pure. M6a/M5 carry as_of_ms; refuse any input stamped AFTER
    #     test_start[k] (fixes r1#9 -- no inherited survivor bias / no leak). ---
    fold_test_start_ms = {int(r.fold_id): pd.Timestamp(r.test_start).value // 1_000_000
                          for r in folds.itertuples()}
    provenance_fail = []
    prov_inputs = [("m06a", m06a), ("m05", m05)]
    if inputs.get("m04_fold_pure"):
        prov_inputs.extend([("m04_entities", m04e), ("m04_auth", inputs["m04_auth"])])
    for nm, df in prov_inputs:
        if "as_of_ms" not in df.columns:
            provenance_fail.append(f"{nm}:no_as_of_ms_column")
            continue
        if nm.startswith("m04") and "fold_id" not in df.columns:
            provenance_fail.append(f"{nm}:no_fold_id_column")
            continue
        # require PRESENT + non-null as_of_ms for every row (missing provenance is not provenance-OK)
        n_null = int(df["as_of_ms"].isna().sum())
        if n_null:
            provenance_fail.append(f"{nm}:{n_null}_rows_null_as_of_ms")
        ts_start = df["fold_id"].map(fold_test_start_ms)
        if ts_start.isna().any():
            provenance_fail.append(f"{nm}:{int(ts_start.isna().sum())}_rows_unknown_fold")
        if nm.startswith("m04"):
            bad = (df["as_of_ms"].astype("float64") != ts_start.astype("float64")).fillna(False)
            if bad.any():
                provenance_fail.append(f"{nm}:{int(bad.sum())}_rows_as_of_not_equal_test_start")
        else:
            bad = (df["as_of_ms"].astype("float64") > ts_start.astype("float64")).fillna(False)
            if bad.any():
                provenance_fail.append(f"{nm}:{int(bad.sum())}_rows_post_test_start")
    # AUDIT 2026-07-10 (codex m06b P0#1, enabled by m07 P0#2 window metadata): PROVE the M7 metrics we rank on
    # were generated for the fold's PRETEST window [train_start, test_start), not a stale/test-window run. Every
    # m07_summary row must carry window_start_ms == train_start and window_end_ms == test_start for its fold.
    fold_train_start_ms = {int(r.fold_id): pd.Timestamp(r.train_start).value // 1_000_000 for r in folds.itertuples()}
    if "window_start_ms" not in summ.columns or "window_end_ms" not in summ.columns:
        provenance_fail.append("m07_summary:no_window_provenance(regenerate with current m07 engine)")
    else:
        _ws = pd.to_numeric(summ["window_start_ms"], errors="coerce")
        _we = pd.to_numeric(summ["window_end_ms"], errors="coerce")
        _ews = summ["fold_id"].map(fold_train_start_ms).astype("float64")
        _ewe = summ["fold_id"].map(fold_test_start_ms).astype("float64")
        _bad = ((_ws.astype("float64") != _ews) | (_we.astype("float64") != _ewe)).fillna(True)
        if _bad.any():
            provenance_fail.append(f"m07_summary:{int(_bad.sum())}_rows_window!=[train_start,test_start) "
                                   f"(test-window or stale m07 run -> look-ahead)")
    if provenance_fail:
        raise ValueError(f"M6b PROVENANCE FAIL-CLOSED: {provenance_fail}")

    # --- engine after-cost results per (entity,fold) ---
    summ = summ.rename(columns={"max_dd": "max_dd_pretest"})
    summ["capacity_capped_frac"] = summ["n_capacity_capped"] / summ["n_actions"].clip(lower=1)
    summ["account_ruin"] = summ["ruin"].astype(int)
    # uncalibrated-cost flags -> drives provisional/FINAL
    uncalibrated = bool(summ["slippage_uncalibrated"].any() or summ["fee_unversioned"].any())

    # --- RETURN BASIS (v2): rank on M7 REALIZED round-trip ROE, NOT full-window MTM roe_engine.
    #     The M7 engine emits realized_roe over CLEAN round-trips (realized_pnl_total / start_equity).
    #     Fall back to roe_engine ONLY when realized_roe is absent, flagged per-run + per-row so a
    #     fallback never silently masquerades as the realized basis. ---
    has_realized = "realized_roe" in summ.columns
    if has_realized:
        realized = pd.to_numeric(summ["realized_roe"], errors="coerce")
        summ["roe_adj"] = realized
        # rows where realized_roe is NaN but roe_engine exists fall back per-row (flagged)
        if "roe_engine" in summ.columns:
            fb_mask = summ["roe_adj"].isna() & summ["roe_engine"].notna()
            summ.loc[fb_mask, "roe_adj"] = pd.to_numeric(
                summ.loc[fb_mask, "roe_engine"], errors="coerce")
        summ["return_basis"] = np.where(realized.notna(), "realized_roe", "roe_engine_fallback")
        return_basis_fallback = bool((summ["return_basis"] == "roe_engine_fallback").any())
    else:
        summ["roe_adj"] = pd.to_numeric(summ.get("roe_engine"), errors="coerce")
        summ["return_basis"] = "roe_engine_fallback"
        return_basis_fallback = True
    # round-trip win-rate term (v2). Absent -> NaN (treated as missing realized metrics downstream).
    if "round_trip_win_rate" in summ.columns:
        summ["round_trip_win_rate"] = pd.to_numeric(summ["round_trip_win_rate"], errors="coerce")
    else:
        summ["round_trip_win_rate"] = np.nan
    # whether the engine actually reported closed round-trips (drives the MIN_ROUND_TRIPS gate).
    # When the column is entirely absent (legacy / pure roe_engine provisional input -- no realized
    # round-trip accounting at all), the gate cannot apply; an open-only entity in a REAL M7 v2 run
    # always carries n_round_trips (0 for open-only) and is therefore gated.
    has_n_round_trips = "n_round_trips" in summ.columns
    for c in ("n_round_trips", "n_round_trip_wins", "realized_pnl_total"):
        if c not in summ.columns:
            summ[c] = np.nan

    # --- exposure_time from fills (min-support) ---
    exposure = _exposure_days_from_fills(inputs["m07_fills_path"], None)

    # --- consistency. FINAL path = M7 equity per-block ROE (activeness from per-block fills+journeys).
    #     PROVISIONAL path (no equity) = M6a source persistence, flagged non-final. ---
    if inputs["m07_equity"] is not None:
        blocks_act = _blocks_activity(inputs["m07_fills_path"], inputs["m02_journeys_path"],
                                      inputs["m04_auth"], folds, m)
        cons = _consistency_from_equity(inputs["m07_equity"], folds, m, blocks_act)
    else:
        cons = _consistency_provisional(m06a)

    # --- eligibility (M5 pass as-of k) ---
    elig = m05[["entity_id", "fold_id", "eligible"]].rename(columns={"eligible": "m5_eligible"})

    # --- M6a provenance/copyability + n_journeys + tier + alloc + G5 fields.
    #     active_pretest_folds = # active 14d pretest sub-splits (m06a active_blocks), NOT
    #     active_test_folds (post-test_start info -> look-ahead). G5 uses active_pretest_folds. ---
    m6a_cols = m06a[["entity_id", "fold_id", "copyable", "rankable", "n_journeys_pretest",
                     "m4_tier", "entity_alloc_weight", "active_blocks",
                     "g5_pool_candidate_pass"]].copy()
    m6a_cols = m6a_cols.rename(columns={"active_blocks": "active_pretest_folds"})

    # --- assemble: UNIVERSE = M6a (every shortlist row), LEFT-join M7 engine results. A M6a row with
    #     no M7 row stays in the output with roe_adj NaN -> excluded (missing_m07_row), never dropped. ---
    # AUDIT 2026-07-10 (codex P0#3): validate="one_to_one" on every fold-keyed merge. A duplicate
    # (entity_id, fold_id) row in ANY input (m6a/summ/elig/cons/exposure) would multiply the universe row and
    # could hand one entity TWO pool slots via head(n_pool), displacing the legitimate #N. Fail closed on dups.
    df = m6a_cols.merge(summ, on=["entity_id", "fold_id"], how="left", validate="one_to_one")
    df = df.merge(elig, on=["entity_id", "fold_id"], how="left", validate="one_to_one")
    df = df.merge(cons, on=["entity_id", "fold_id"], how="left", validate="one_to_one")
    df = df.merge(exposure, on=["entity_id", "fold_id"], how="left", validate="one_to_one")
    # V2.2: merge per-position metrics (from M7's per-position emit) if present -> switches ranking basis.
    pp_agg = inputs.get("m07_positions_agg")
    use_pp = pp_agg is not None and not pp_agg.empty
    if use_pp:
        for c in pp_agg.columns:
            if c not in ("entity_id", "fold_id"):
                pp_agg[c] = pd.to_numeric(pp_agg[c], errors="coerce")
        df = df.merge(pp_agg, on=["entity_id", "fold_id"], how="left", validate="one_to_one")
    df["exposure_days"] = df["exposure_days"].fillna(0.0)
    df["consistency"] = df["consistency"].fillna(0.0)
    df["n_active_subsplits"] = df["n_active_subsplits"].fillna(0).astype(int)
    df["consistency_source"] = df["consistency_source"].fillna(
        "unavailable" if inputs["m07_equity"] is None else "m07_equity_block_roe")

    # --- M4 copyable from entities (KILL/no-primary -> not copyable). m6a copyable already encodes it
    #     but cross-check against m04_entities as the authority. ---
    if inputs.get("m04_fold_pure") and "fold_id" in m04e.columns:
        ent_copy = m04e[["entity_id", "fold_id", "copyable"]].rename(columns={"copyable": "entity_copyable"})
        df = df.merge(ent_copy, on=["entity_id", "fold_id"], how="left", validate="one_to_one")  # codex P0#3
    else:
        ent_copy = m04e[["entity_id", "copyable"]].rename(columns={"copyable": "entity_copyable"})
        # global M4 has one row per entity; df is many folds per entity -> many_to_one (fails on dup m04e).
        df = df.merge(ent_copy, on="entity_id", how="left", validate="many_to_one")  # codex P0#3
    df["entity_copyable"] = df["entity_copyable"].fillna(False)

    # --- fidelity: needs tracking_error from M7 summary. Absent column OR NaN/inf value -> UNKNOWN.
    #     Real fidelity only where tracking_error is finite (codex r2 #B: a present-but-NaN column
    #     must not pass as real; flagged PER ROW). FIX (v2): an unknown-fidelity row is filled with the
    #     FOLD MEAN of the known fidelities -- NOT the max (1.0). NaN->1.0 advantaged unknown rows
    #     (best-possible fidelity, a free top-bucket boost). Fold-mean is neutral within the fold (no
    #     advantage, no penalty). When a fold has NO known fidelities the term contributes 0 there.
    #     Provisional rows still block investability via fidelity_source regardless of the fill value. ---
    if "tracking_error" in df.columns:
        te = pd.to_numeric(df["tracking_error"], errors="coerce")
        te_finite = np.isfinite(te.to_numpy(dtype="float64"))  # rejects NaN AND +/-inf
        real_fid = (1.0 - (te / m.fidelity_B).clip(0, 1)).where(te_finite)
        df["fidelity"] = real_fid
        df["fidelity_source"] = np.where(te_finite, "m07_tracking_error", "unavailable_provisional")
        # fill unknown rows with the per-fold mean of KNOWN fidelities (neutral within fold)
        fold_mean = df.groupby("fold_id")["fidelity"].transform("mean")
        df["fidelity"] = df["fidelity"].fillna(fold_mean).fillna(0.0)
    else:
        # no tracking_error column at all -> no known fidelity anywhere -> term contributes 0.
        df["fidelity"] = 0.0
        df["fidelity_source"] = "unavailable_provisional"

    df["capacity_health"] = 1.0 - df["capacity_capped_frac"].clip(0, 1)
    nj = df["n_journeys_pretest"].clip(lower=1)
    df["survivability_penalty"] = ((df["n_backstop_transfer"] + 2 * df["account_ruin"]) / nj).clip(0, 1)
    df["calmar"] = df["roe_adj"] / df["max_dd_pretest"].clip(lower=m.dd_floor)

    # --- RANKABLE eligibility (design §3): M5-pass + M4-copyable + provenance OK + MIN-SUPPORT. ---
    # n_round_trips gate (codex finding): an entity with < MIN_ROUND_TRIPS CLOSED round-trips has
    # no meaningful realized after-cost return basis (M7 emits 0.0 realized metrics when
    # n_round_trips==0), so it must NOT be rankable / enter the pool. Applies whenever the engine
    # reported round-trip accounting (column present in the M7 summary); for entities present in M7
    # the NaN-or-below case fails. When the column is entirely absent (legacy/provisional input with
    # no round-trip accounting) the gate is a no-op (all True) so dry-runs still rank.
    n_round_trips = pd.to_numeric(df["n_round_trips"], errors="coerce")
    round_trip_gate = (n_round_trips >= MIN_ROUND_TRIPS) if has_n_round_trips \
        else pd.Series(True, index=df.index)
    rankable = (
        df["m5_eligible"].fillna(False)
        & df["entity_copyable"]
        & (df["n_fills"] >= m.min_fills_pretest)
        & (df["exposure_days"] >= m.min_exposure_days)
        & (df["n_active_subsplits"] >= m.min_active_subsplits_support)
        & round_trip_gate
        & df["roe_adj"].notna() & df["max_dd_pretest"].notna()
        # AUDIT 2026-07-10 (codex P1#5): ALL score inputs must be finite. capacity_health / survivability_penalty
        # derive from capacity_capped_frac / n_backstop_transfer / account_ruin; a NaN there makes m6b_score NaN,
        # and the pool cut (`& m6b_score.notna()`) would SILENTLY drop a "rankable" row with a blank reason.
        # Require them finite here so such a row is explicitly excluded (data-gap), not vanished.
        & np.isfinite(pd.to_numeric(df["capacity_health"], errors="coerce").to_numpy(dtype="float64"))
        & np.isfinite(pd.to_numeric(df["survivability_penalty"], errors="coerce").to_numpy(dtype="float64"))
    )
    # V2.2 PER-POSITION GATES (Alberto WHO + strategist review): material support, COST-FEASIBILITY
    # (95% lower bound on per-journey NET return > floor), quick-to-close, NO unrealized loss (MTM ceiling).
    if use_pp:
        # POSITION-DRAWDOWN basis (Alberto 2026-07-24 "It should be position drawdown"): gate + score the
        # unrealized-loss ceiling on the per-position MAE p90 (true position drawdown), NOT max_dd_pretest
        # (account-equity DD of our $10k copy — meaningless as a per-wallet bag signal). Fall back to
        # max_dd_pretest only when the MAE emit is absent (older M7 run) so the module stays backward-safe.
        _mae_p90 = pd.to_numeric(df.get("pp_mae_p90"), errors="coerce") if "pp_mae_p90" in df.columns else pd.Series(np.nan, index=df.index)
        df["mtm_dd_basis"] = _mae_p90.where(_mae_p90.notna(), pd.to_numeric(df["max_dd_pretest"], errors="coerce"))
        rankable = rankable & (
            (pd.to_numeric(df["pp_n"], errors="coerce") >= m.pp_min_positions)
            & (pd.to_numeric(df["pp_lcb_mean_r"], errors="coerce") > m.pp_min_lcb_mean_r)
            & (pd.to_numeric(df["pp_med_hold_h"], errors="coerce") <= m.pp_max_med_hold_h)
            & (pd.to_numeric(df["pp_p90_hold_h"], errors="coerce") <= m.pp_max_p90_hold_h)
            & (pd.to_numeric(df["mtm_dd_basis"], errors="coerce") <= m.pp_max_mtm_dd)
            & pd.to_numeric(df["pp_mean_r"], errors="coerce").notna()
            & pd.to_numeric(df["pp_std_r"], errors="coerce").notna()
        )
    df["m6b_rankable"] = rankable
    excl = []
    for _, r in df.iterrows():
        if r["m6b_rankable"]:
            excl.append("")
            continue
        reasons = []
        if not bool(r["m5_eligible"]):
            reasons.append("m5_ineligible")
        if not bool(r["entity_copyable"]):
            reasons.append("not_copyable")
        if r["n_fills"] < m.min_fills_pretest:
            reasons.append(f"n_fills<{m.min_fills_pretest}")
        if r["exposure_days"] < m.min_exposure_days:
            reasons.append(f"exposure<{m.min_exposure_days}d")
        if r["n_active_subsplits"] < m.min_active_subsplits_support:
            reasons.append("active_subsplits<2")
        if has_n_round_trips:
            _nrt = pd.to_numeric(pd.Series([r["n_round_trips"]]), errors="coerce").iloc[0]
            if pd.isna(_nrt) or _nrt < MIN_ROUND_TRIPS:
                reasons.append(f"n_round_trips<{MIN_ROUND_TRIPS}")
        if pd.isna(r["roe_adj"]) or pd.isna(r["max_dd_pretest"]):
            reasons.append("missing_m07_row")
        if not np.isfinite(pd.to_numeric(pd.Series([r["capacity_health"]]), errors="coerce").iloc[0]) \
                or not np.isfinite(pd.to_numeric(pd.Series([r["survivability_penalty"]]), errors="coerce").iloc[0]):
            reasons.append("nonfinite_score_component")  # codex P1#5: was a silent blank-reason drop
        excl.append("|".join(reasons) or "excluded")
    df["excluded_reason"] = excl

    # --- per-fold winsorize + z-score over RANKABLE entities, then frozen score ---
    out_parts = []
    for fid, g in df.groupby("fold_id"):
        g = g.copy()
        rk = g[g["m6b_rankable"]].copy()
        if rk.empty:
            g["m6b_score"] = np.nan
            out_parts.append(g)
            continue
        if use_pp:
            # === V2.2 PER-POSITION SCORE (Alberto: unit = round-trip journey) ===
            # z-scored per fold over rankable entities; KILL roe/calmar/win-rate. mean+consistency dominate;
            # subtract std (risk) + MTM drawdown (unrealized loss); reward quick-close.
            rk["z_pp_mean_r"] = _zscore(_winsorize(pd.to_numeric(rk["pp_mean_r"], errors="coerce"),
                                                   m.winsor_lo_pct, m.winsor_hi_pct))
            rk["z_pp_t"] = _zscore(pd.to_numeric(rk["pp_t"], errors="coerce"))
            rk["z_pp_std"] = _zscore(pd.to_numeric(rk["pp_std_r"], errors="coerce"))
            rk["z_pp_mtm"] = _zscore(pd.to_numeric(rk["mtm_dd_basis"], errors="coerce"))  # position DD (MAE p90), not equity DD
            rk["z_pp_quick"] = _zscore(pd.to_numeric(rk["pp_frac_quick"], errors="coerce"))
            rk["m6b_score"] = (
                m.w_pp_mean_r * rk["z_pp_mean_r"].fillna(0.0)
                + m.w_pp_t * rk["z_pp_t"].fillna(0.0)
                - m.w_pp_std * rk["z_pp_std"].fillna(0.0)
                - m.w_pp_mtm_dd * rk["z_pp_mtm"].fillna(0.0)
                + m.w_pp_quick * rk["z_pp_quick"].fillna(0.0)
                - m.w_survivability_penalty * rk["survivability_penalty"]
            )
            g = g.merge(rk[["entity_id", "fold_id", "z_pp_mean_r", "z_pp_t", "z_pp_std",
                            "z_pp_mtm", "z_pp_quick", "m6b_score"]],
                        on=["entity_id", "fold_id"], how="left")
            out_parts.append(g)
            continue
        rk["roe_adj_w"] = _winsorize(rk["roe_adj"], m.winsor_lo_pct, m.winsor_hi_pct)
        rk["calmar_w"] = _winsorize(rk["calmar"], m.winsor_lo_pct, m.winsor_hi_pct)
        rk["z_realized_roe"] = _zscore(rk["roe_adj_w"])
        rk["z_calmar"] = _zscore(rk["calmar_w"])
        # win-rate term (v2): z-scored per fold like the others. Unknown win-rate -> fold mean
        # (z=0, neutral) so a missing-realized row neither gains nor loses on this term.
        wr = pd.to_numeric(rk["round_trip_win_rate"], errors="coerce")
        wr = wr.fillna(wr.mean())
        rk["z_win_rate"] = _zscore(wr) if wr.notna().any() else 0.0
        rk["m6b_score"] = (
            m.w_realized_roe * rk["z_realized_roe"]
            + m.w_calmar * rk["z_calmar"]
            + m.w_win_rate * rk["z_win_rate"]
            + m.w_consistency * rk["consistency"]
            + m.w_capacity_health * rk["capacity_health"]
            + m.w_fidelity * rk["fidelity"]
            - m.w_survivability_penalty * rk["survivability_penalty"]
        )
        g = g.merge(rk[["entity_id", "fold_id", "z_realized_roe", "z_calmar", "z_win_rate",
                        "roe_adj_w", "calmar_w", "m6b_score"]],
                    on=["entity_id", "fold_id"], how="left")
        out_parts.append(g)
    df = pd.concat(out_parts, ignore_index=True)

    # --- POOL SELECTION + G5 (design §4) + allocation buckets (design §5) ---
    pool_parts = []
    for fid, g in df.groupby("fold_id"):
        g = g.copy()
        g["rank_in_fold"] = np.nan
        g["in_pool"] = False
        g["bucket"] = np.nan
        g["quality_weight"] = 0.0
        rk = g[g["m6b_rankable"] & g["m6b_score"].notna()].copy()
        if rk.empty:
            pool_parts.append(g)
            continue
        # deterministic ordering: score desc, then tie-break consistency desc, n_fills desc, entity asc
        rk = rk.sort_values(
            ["m6b_score", "consistency", "n_fills", "entity_id"],
            ascending=[False, False, False, True]).reset_index(drop=True)
        rk["rank_in_fold"] = np.arange(1, len(rk) + 1)
        # G5 enforced HERE: active_pretest_folds>=3 AND n_journeys_pretest>=5 (pretest-only; no leak)
        g5 = (rk["active_pretest_folds"].fillna(0) >= m.g5_min_active_pretest_folds) & \
             (rk["n_journeys_pretest"].fillna(0) >= m.g5_min_journeys_pretest)
        rk_g5 = rk[g5].copy()
        pooled = rk_g5.head(m.n_pool).copy()
        pooled["in_pool"] = True
        # quintile buckets by score (equal-count). bucket 1 = top.
        n = len(pooled)
        if n > 0:
            # qcut into n_buckets equal-count groups; labels 1..5 with 1 = highest score.
            try:
                q = pd.qcut(pooled["m6b_score"].rank(method="first"), m.n_buckets,
                            labels=list(range(m.n_buckets, 0, -1)))
                pooled["bucket"] = q.astype(int)
            except ValueError:
                # too few distinct -> linear split
                pooled["bucket"] = (np.floor(
                    (pooled["rank_in_fold"].rank(method="first") - 1) / max(n / m.n_buckets, 1)
                ).astype(int) + 1).clip(1, m.n_buckets)
            # TOP-BUCKET CONSISTENCY GATE: bucket-1 entity with consistency < gate -> demote 1 bucket
            # (to 2). No backfill of bucket 1; no boundary recompute (deterministic, non-circular).
            demote = (pooled["bucket"] == 1) & (pooled["consistency"] < m.top_bucket_consistency_gate)
            pooled.loc[demote, "bucket"] = 2
            # bucket weights [5,4,3,2,1] -> normalized to sum 1
            bw = {i + 1: m.bucket_weights[i] for i in range(m.n_buckets)}
            pooled["raw_w"] = pooled["bucket"].map(bw).astype(float)
            tot = pooled["raw_w"].sum()
            pooled["quality_weight"] = pooled["raw_w"] / tot if tot > 0 else 0.0
            # per-entity ceiling = 10% of pool total; clip + renormalize remaining mass. For small
            # pools where n * ceiling < 1 the nominal 10% is INFEASIBLE (weights could never sum to
            # 1) -> use an effective ceiling = max(ceiling, 1/n) so a normalized solution always
            # exists. Flagged in the manifest when the ceiling was relaxed.
            ceil = max(m.per_entity_quality_ceiling, 1.0 / n)
            ceiling_relaxed = ceil > m.per_entity_quality_ceiling + 1e-12
            qw = np.array(pooled["quality_weight"].to_numpy(dtype=float), copy=True)
            for _ in range(200):
                over = qw > ceil + 1e-12
                if not over.any():
                    break
                excess = (qw[over] - ceil).sum()
                qw[over] = ceil
                under = ~over
                if qw[under].sum() <= 0:
                    break
                qw[under] += excess * qw[under] / qw[under].sum()
            pooled["quality_weight"] = qw
            # invariants: normalized to 1 and no weight exceeds the effective ceiling.
            assert abs(qw.sum() - 1.0) < 1e-6, f"fold {fid}: quality_weight sum {qw.sum()} != 1"
            assert qw.max() <= ceil + 1e-9, f"fold {fid}: weight {qw.max()} > ceiling {ceil}"
            pooled["ceiling_relaxed"] = bool(ceiling_relaxed)
        # merge pooled fields back
        keep = ["entity_id", "fold_id", "rank_in_fold", "in_pool", "bucket", "quality_weight"]
        g = g.drop(columns=["rank_in_fold", "in_pool", "bucket", "quality_weight"]).merge(
            pooled[keep], on=["entity_id", "fold_id"], how="left")
        g["in_pool"] = g["in_pool"].fillna(False)
        g["quality_weight"] = g["quality_weight"].fillna(0.0)
        # rank_in_fold for non-pooled rankable: keep from rk
        g = g.merge(rk[["entity_id", "fold_id", "rank_in_fold"]].rename(
            columns={"rank_in_fold": "_rk2"}), on=["entity_id", "fold_id"], how="left")
        g["rank_in_fold"] = g["rank_in_fold"].fillna(g["_rk2"])
        g = g.drop(columns=["_rk2"])
        pool_parts.append(g)
    out = pd.concat(pool_parts, ignore_index=True)

    # --- investable flag (provisional vs FINAL). FINAL (investable=True) requires ALL of:
    #     calibrated costs (no uncalibrated flags) + fee/slippage calibration versions set +
    #     REAL fidelity from M7 tracking_error (no neutral default) + REAL consistency from M7
    #     after-cost equity block ROE (no M6a source-persistence proxy). Any provisional input ->
    #     non-investable (the proxies are gameable / not after-our-cost). ---
    # source flags gate FINAL on a PER-ROW basis over the RANKABLE rows (mode is fragile: a few
    # provisional rows must still block). FINAL requires EVERY rankable row to carry real M7 sources.
    rk_mask = out["m6b_rankable"].fillna(False)
    # report the source over RANKABLE rows (the rows that gate FINAL); the global mode is dominated by
    # non-rankable rows (no M7 row -> provisional default) and is misleading.
    _src_rows = out.loc[rk_mask] if rk_mask.any() else out
    consistency_source = _src_rows["consistency_source"].mode().iat[0] if len(_src_rows) else "unknown"
    fidelity_source = _src_rows["fidelity_source"].mode().iat[0] if len(_src_rows) else "unknown"
    fidelity_final = bool(rk_mask.any() and out.loc[rk_mask, "fidelity_source"].eq("m07_tracking_error").all())
    consistency_final = bool(rk_mask.any() and out.loc[rk_mask, "consistency_source"].eq("m07_equity_block_roe").all())
    # v2: FINAL additionally requires the REALIZED round-trip metrics to be present on every rankable
    #     row -- a realized_roe (no roe_engine fallback) AND a finite round_trip_win_rate AND a
    #     non-null n_round_trips. The rank now depends on realized round-trips; a fallback/missing
    #     basis means the pool was not locked on the after-cost realized return.
    if rk_mask.any():
        realized_final = bool(
            out.loc[rk_mask, "return_basis"].eq("realized_roe").all()
            and pd.to_numeric(out.loc[rk_mask, "round_trip_win_rate"], errors="coerce").notna().all()
            and pd.to_numeric(out.loc[rk_mask, "n_round_trips"], errors="coerce").notna().all()
        )
    else:
        realized_final = False
    # AUDIT 2026-07-10 (codex P0#2): FINAL/investable additionally REQUIRES fold-pure M4. The global
    # m04_entities.parquet maps entity->copyable with FULL-history (post-decision) knowledge = look-ahead; a run
    # that fell back to it must NEVER be stamped investable (it could admit an entity not copyable at test_start,
    # or drop a historically-valid one). Fold-pure per-fold M4 (--m04-dir, provenance-checked in load_inputs) only.
    m04_fold_pure = bool(inputs.get("m04_fold_pure"))
    # AUDIT 2026-07-10 (codex P1#4): the CLI-claimed slippage_calibration_version must be CROSS-CHECKED against
    # what M7 actually priced. Require every RANKABLE m07 row to carry slippage_calibration_version equal to the
    # manifest version -> a run stamped with a version M7 did not use (None/mixed) cannot be marked investable.
    if rk_mask.any() and "slippage_calibration_version" in out.columns:
        slippage_version_final = bool(
            out.loc[rk_mask, "slippage_calibration_version"].eq(m.slippage_calibration_version).all())
    else:
        slippage_version_final = False
    investable = (
        (not uncalibrated)
        and (m.slippage_calibration_version is not None)
        and (m.fee_schedule_version is not None)
        and fidelity_final
        and consistency_final
        and realized_final
        and m04_fold_pure
        and slippage_version_final
    )
    out["investable"] = bool(investable)
    out["slippage_uncalibrated"] = summ["slippage_uncalibrated"].any() if "slippage_uncalibrated" in summ else True

    return_basis = _src_rows["return_basis"].mode().iat[0] if (
        "return_basis" in _src_rows and len(_src_rows)) else "unknown"
    manifest = asdict(m)
    manifest["uncalibrated_input"] = uncalibrated
    manifest["investable"] = bool(investable)
    manifest["consistency_source"] = consistency_source
    manifest["fidelity_source"] = fidelity_source
    manifest["return_basis"] = return_basis
    manifest["return_basis_fallback"] = bool(return_basis_fallback)
    if not investable:
        reasons = []
        if uncalibrated:
            reasons.append("uncalibrated_costs")
        if m.slippage_calibration_version is None:
            reasons.append("no_slippage_calibration_version")
        if m.fee_schedule_version is None:
            reasons.append("no_fee_schedule_version")
        if not fidelity_final:
            reasons.append(f"fidelity_provisional({fidelity_source})")
        if not consistency_final:
            reasons.append(f"consistency_provisional({consistency_source})")
        if not realized_final:
            reasons.append(f"realized_metrics_missing({return_basis})")
        if not m04_fold_pure:
            reasons.append("m04_not_fold_pure(look_ahead_global_m04)")
        if not slippage_version_final:
            reasons.append("slippage_version_mismatch(m07 rows != claimed calibration)")
        manifest["non_investable_reasons"] = reasons
    return out, manifest


# --------------------------------------------------------------------------- #
# OUTPUT
# --------------------------------------------------------------------------- #
OUT_COLS = [
    "entity_id", "fold_id", "m6b_score", "roe_adj", "return_basis", "round_trip_win_rate",
    "n_round_trips", "calmar", "consistency", "n_active_subsplits",
    "consistency_source", "capacity_health", "capacity_capped_frac", "fidelity", "fidelity_source",
    "survivability_penalty", "max_dd_pretest", "n_fills", "exposure_days", "n_journeys_pretest",
    "n_backstop_transfer", "account_ruin", "m4_tier", "entity_alloc_weight",
    "m5_eligible", "entity_copyable", "m6b_rankable", "rank_in_fold", "bucket", "in_pool",
    "quality_weight", "excluded_reason", "slippage_uncalibrated", "investable",
    # V2.2 per-position basis (ranking unit = round-trip journey) + position drawdown/run-up (MAE/MFE)
    "pp_n", "pp_mean_r", "pp_std_r", "pp_t", "pp_lcb_mean_r", "pp_med_hold_h", "pp_p90_hold_h",
    "pp_frac_quick", "pp_uw_add", "pp_mae_med", "pp_mae_p90", "pp_frac_underwater",
    "pp_mfe_med", "pp_giveback_med", "mtm_dd_basis",
]


def write_outputs(out: pd.DataFrame, manifest: dict, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    cols = [c for c in OUT_COLS if c in out.columns]
    pool_path = out_dir / "m06b_pool.parquet"
    out[cols].to_parquet(pool_path, index=False)
    man_path = out_dir / "m06b_manifest.json"
    man_path.write_text(json.dumps(manifest, indent=2, default=str))
    n_pooled = int(out["in_pool"].sum())
    logger.info("M6b wrote %d rows (%d pooled across folds) -> %s; manifest -> %s",
                len(out), n_pooled, pool_path, man_path)
    return {"pool_rows": len(out), "pooled": n_pooled, "investable": manifest["investable"]}


def _bh_fdr_mask(pvals: np.ndarray, q: float) -> np.ndarray:
    """Benjamini-Hochberg step-up: boolean discoveries at FDR level q. Standard BH (independence/PRDS)."""
    p = np.asarray(pvals, dtype="float64")
    n = p.size
    if n == 0:
        return np.zeros(0, dtype=bool)
    order = np.argsort(p)
    thresh = q * (np.arange(1, n + 1) / n)
    below = p[order] <= thresh
    if not below.any():
        return np.zeros(n, dtype=bool)
    kmax = int(np.max(np.where(below)[0]))
    cutoff = p[order][kmax]
    return p <= cutoff


def _boot_p_mean_gt(x: np.ndarray, margin: float, n_boot: int = 4000, seed: int = 12345) -> float:
    """One-sided bootstrap p-value for H0: mean(x) <= margin (small p = evidence the NET per-journey return
    exceeds the cost/model-risk margin). Resamples the H0-centered data (shift so mean==margin) and returns
    P(bootstrap mean >= observed mean). Bootstrap (not a normal SE) handles the fat-tailed, small-n journey
    returns the strategist review flagged. Journeys are treated i.i.d. here; a moving-block variant over
    calendar-day blocks is the hardening for time-clustering (TODO with the full multi-fold pool)."""
    x = np.asarray(x, dtype="float64")
    x = x[np.isfinite(x)]
    n = x.size
    if n < 5:
        return 1.0
    obs = float(x.mean())
    if obs <= margin:
        return 1.0
    rng = np.random.default_rng(seed)
    null = x - obs + margin                       # H0: mean == margin
    idx = rng.integers(0, n, size=(n_boot, n))
    boot_means = null[idx].mean(axis=1)
    return float((boot_means >= obs).mean())


def walk_forward_confirm(inputs_pretest: dict, m07_test_dir: Path, m: M6bManifest) -> tuple[pd.DataFrame, dict]:
    """OUT-OF-SAMPLE CONFIRMATION (2026-07-23, Fable+Codex UNANIMOUS after Alberto's push). The pretest ranking
    is IN-SAMPLE (rho~0 forward). A single-window OOS gate is either too strict (lower-bound, ~3 wallets) or
    ~72% false-discoveries (mean>0, ~15). The SOUND gate = POOL a wallet's OOS journeys across its NON-OVERLAPPING
    held-out TEST folds -> BOOTSTRAP one-sided p-value that pooled mean NET return > margin -> BENJAMINI-HOCHBERG
    FDR at q across all eligible wallets. NO forcing a target count. Requires >= oos_min_folds folds and
    >= oos_min_journeys_pooled pooled OOS journeys per wallet (Codex: 4 folds / 50 journeys is the standard;
    fewer available until the FULL 12-fold action bootstrap exists). Also require net-positive in a majority of
    the wallet's OOS folds (stability). Ranking still comes from build_ranking (the module)."""
    pool, mani = build_ranking(inputs_pretest, m)
    pre = pool[pool["m6b_rankable"] & pool["m6b_score"].notna()].copy()
    # per-ENTITY best pretest score (pool the pretest-rankable across folds)
    pre_entity = pre.sort_values("m6b_score", ascending=False).drop_duplicates("entity_id")
    cand = set(pre_entity["entity_id"].astype(int))
    # RAW per-journey OOS returns from the held-out test window(s)
    tpos = _read_parquet_maybe_parts(Path(m07_test_dir) / "m07_positions.parquet")
    if tpos is None or tpos.empty:
        raise ValueError("walk_forward_confirm: no m07_positions in the TEST dir (need per-position emit).")
    tpos = tpos.copy()
    tpos["r_i"] = pd.to_numeric(tpos["r_i"], errors="coerce")
    tpos = tpos[tpos["entity_id"].astype(int).isin(cand)].dropna(subset=["r_i"])
    rows = []
    for eid, g in tpos.groupby("entity_id"):
        r = g["r_i"].to_numpy("float64")
        folds = g["fold_id"].nunique()
        fold_means = g.groupby("fold_id")["r_i"].mean()
        frac_pos = float((fold_means > 0).mean()) if len(fold_means) else 0.0
        rows.append({"entity_id": int(eid), "oos_n": len(r), "oos_folds": int(folds),
                     "oos_mean_r": float(r.mean()), "oos_frac_folds_pos": frac_pos,
                     "p_boot": _boot_p_mean_gt(r, m.oos_margin)})
    stats = pd.DataFrame(rows)
    if stats.empty:
        return stats, {"pretest_rankable_entities": len(cand), "eligible": 0, "confirmed": 0}
    eligible = stats[(stats["oos_folds"] >= m.oos_min_folds)
                     & (stats["oos_n"] >= m.oos_min_journeys_pooled)
                     & (stats["oos_frac_folds_pos"] >= m.oos_min_frac_folds_pos)].copy()
    if eligible.empty:
        confirmed = eligible
    else:
        disc = _bh_fdr_mask(eligible["p_boot"].to_numpy("float64"), m.fdr_q)
        eligible["bh_discovery"] = disc
        confirmed = eligible[eligible["bh_discovery"]].copy()
    _wcol = [c for c in ("primary_wallet", "m6b_score") if c in pre_entity.columns]
    confirmed = confirmed.merge(pre_entity[["entity_id"] + _wcol], on="entity_id", how="left")
    summ = {
        "pretest_rankable_entities": len(cand),
        "eligible_for_fdr": int(len(eligible)),
        "confirmed": int(len(confirmed)),
        "fdr_q": m.fdr_q, "oos_min_folds": m.oos_min_folds,
        "oos_min_journeys_pooled": m.oos_min_journeys_pooled,
        "note": ("only 3 OOS folds available (recency actions Apr06-Jul22); Codex standard is >=4 folds/>=50 "
                 "journeys -> needs the FULL 12-fold action bootstrap. FREEZE this gate + validate on an "
                 "untouched future window before capital."),
    }
    logger.info("WALK-FORWARD CONFIRM (pooled+BH-FDR q=%.2f): cand_entities=%d eligible=%d CONFIRMED=%d",
                m.fdr_q, len(cand), summ["eligible_for_fdr"], summ["confirmed"])
    return confirmed, summ


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--m07-dir", default=str(DATA_DIR / "m07_pretest"),
                    help="M7 PRETEST-window run dir (m07_summary/fills/[equity].parquet)")
    ap.add_argument("--out", default=str(DATA_DIR))
    ap.add_argument(
        "--data-dir", default=str(DATA_DIR),
        help="Directory containing this lane's M2-M6a artifacts.",
    )
    ap.add_argument("--m04-dir", default=None,
                    help="directory with m04_authenticity_f{fold_id}.parquet and m04_entities_f{fold_id}.parquet")
    ap.add_argument("--m07-test-dir", default=None,
                    help="M7 TEST-window run dir (held-out OOS). If set, runs pooled walk_forward_confirm "
                         "(BH-FDR) and writes m06b_confirmed.parquet. Needs the full-fold action bootstrap.")
    ap.add_argument("--fee-schedule-version", default=None)
    ap.add_argument("--slippage-calibration-version", default=None)
    # OOS-gate overrides (2026-07-28). The M6bManifest defaults are the temporary FLOOR (2 folds /
    # 30 journeys) chosen when only 3 OOS folds existed; the docstring says the CODEX STANDARD is
    # 4 folds / 50 journeys "once the FULL 12-fold action bootstrap exists". The 13-fold calendar now
    # exists, so the standard must be settable WITHOUT editing the dataclass (which would silently
    # re-gate every other caller). Defaults None -> manifest defaults unchanged.
    ap.add_argument("--oos-min-folds", type=int, default=None)
    ap.add_argument("--oos-min-journeys-pooled", type=int, default=None)
    ap.add_argument("--oos-min-frac-folds-pos", type=float, default=None)
    ap.add_argument("--oos-margin", type=float, default=None)
    ap.add_argument("--fdr-q", type=float, default=None)
    args = ap.parse_args()
    _over = {k: v for k, v in (
        ("oos_min_folds", args.oos_min_folds),
        ("oos_min_journeys_pooled", args.oos_min_journeys_pooled),
        ("oos_min_frac_folds_pos", args.oos_min_frac_folds_pos),
        ("oos_margin", args.oos_margin),
        ("fdr_q", args.fdr_q),
    ) if v is not None}
    m = M6bManifest(fee_schedule_version=args.fee_schedule_version,
                    slippage_calibration_version=args.slippage_calibration_version, **_over)
    if _over:
        logger.info("M6b OOS-gate overrides: %s", _over)
    inputs = load_inputs(
        Path(args.m07_dir), data_dir=Path(args.data_dir),
        m04_dir=Path(args.m04_dir) if args.m04_dir else None,
    )
    out, manifest = build_ranking(inputs, m)
    res = write_outputs(out, manifest, Path(args.out))
    logger.info("M6b done: %s", res)
    # OOS CONFIRMATION (pooled multi-fold + BH-FDR). Only meaningful with the full 12-fold action bootstrap;
    # writes the confirmed set + its summary alongside the pool so the roster rests on OOS, not in-sample rank.
    if args.m07_test_dir:
        confirmed, wf_summ = walk_forward_confirm(inputs, Path(args.m07_test_dir), m)
        out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
        confirmed.to_parquet(out_dir / "m06b_confirmed.parquet", index=False)
        (out_dir / "m06b_walkforward_summary.json").write_text(json.dumps(wf_summ, indent=2, default=str))
        logger.info("M6b walk-forward: %s -> m06b_confirmed.parquet", wf_summ)


if __name__ == "__main__":
    main()
