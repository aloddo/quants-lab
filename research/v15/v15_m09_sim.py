"""V15 M9 -- Portfolio simulation. M9-v1-CORE = the ALLOCATION engine (the fold-decision-point logic):
sizing chain + cap-aware water-filling + anti-corr selection + MIN-NOTIONAL feasibility at the
configured capital (Alberto delta) + the fixed-bankroll ledger primitives. The full CHAINED portfolio
path (per-subaccount M7 test-window runs merged on the deterministic event clock + global-DD/G4 kill +
cold-start) is M9-v2, wired once the pretest re-run + test-window M7 paths are available.

Design: brain projects/quant/v15/modules/m09 (codex DESIGN-SHIP r3) + the 2026-06-01 manifest freeze
(decisions/2026-06-01-m9-m10-manifest-freeze). Frozen knobs are pre-registered here.

ROLE: M9 owns anti-corr, portfolio caps, the global-DD circuit breaker + G4 kill, the capital ledger,
cold-start. Sizing chain (one multiplier per module): quality(M6b) x confidence(M4) x survival(M8) ->
M9 caps/anti-corr/min-notional. Caps bind at ALLOCATION (decision points); intra-fold drift is
tolerated+flagged, NOT force-trimmed (winners compound untouched; strategy §7).
"""
from __future__ import annotations

import copy
import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from _streaming_io import ShardedParquetWriter, install_memory_guard

logger = logging.getLogger("m09")


@dataclass(frozen=True)
class M9Manifest:
    manifest_version: str = "m09-v1"
    b0: float = 500.0                      # live-small bankroll (Alberto: $500); also report at 5000
    rho_max: float = 0.70                  # anti-corr: drop pair > this, KEEP higher-ranked
    target_count: int = 40                 # NON-BINDING ceiling (real cap = capital/min-size)
    gross_cap: float = 3.0                 # portfolio gross exposure ceiling (revisable)
    global_dd_derisk: float = 0.35         # global DD circuit-breaker de-risk trigger
    g4_intrafold_kill: float = 0.50        # intra-fold -50% portfolio kill
    per_entity_cap: float = 0.40           # G7 per-entity concentration (allocation-time)
    suspicious_cohort_cap: float = 0.10    # SUSPICIOUS tier cohort <= 10% of portfolio
    idle_cash_return: float = 0.0          # idle cash earns 0
    min_order_notional: float = 10.0       # HL $10 min
    # MIN-NOTIONAL FEASIBILITY (Alberto delta): an entity needs a slice large enough that a typical
    # action clears min-notional. viable_slice = min_order_notional / min_action_exposure_frac, with a
    # floor so a tiny exposure_frac doesn't demand an absurd slice. accessible_frac (M5) refines it.
    min_accessible_frac: float = 0.50      # need >=50% of actions to clear min-notional to be feasible
    sizing_mode: str = "leader_equity"      # leader_equity | fixed_position
    fixed_target_exposure: float = 0.10


def sizing_chain_weight(quality_weight: float, m4_confidence: float, survival_mult: float) -> float:
    """Desired weight = quality(M6b) x confidence(M4) x survival(M8). One multiplier per module."""
    return float(quality_weight) * float(m4_confidence) * float(survival_mult)


def anti_corr_select(cand: pd.DataFrame, corr: dict, m: M9Manifest) -> pd.DataFrame:
    """Greedy diversification (design §4): process entities by DESCENDING priority (m6b_score x
    survival); add an entity unless its max pairwise corr with the already-selected set > rho_max
    (KEEP the higher-ranked, drop the correlated lower-ranked one). Stop at target_count (non-binding
    ceiling). `corr` = {(a,b): rho}. Returns selected rows with `anti_corr_selected` + `dropped_reason`."""
    c = cand.sort_values("select_priority", ascending=False).reset_index(drop=True)
    selected: list = []
    out_rows = []
    for r in c.itertuples():
        eid = int(r.entity_id)
        # codex r1#1: drop only on POSITIVE corr > rho_max. A negative correlation is DIVERSIFYING ->
        # keep it (no abs()).
        maxrho = max((corr.get((eid, s), corr.get((s, eid), 0.0)) for s in selected), default=0.0)
        if len(selected) >= m.target_count:
            out_rows.append({**r._asdict(), "anti_corr_selected": False, "dropped_reason": "target_count_ceiling"})
        elif maxrho > m.rho_max:
            out_rows.append({**r._asdict(), "anti_corr_selected": False, "dropped_reason": f"corr>{m.rho_max}"})
        else:
            selected.append(eid)
            out_rows.append({**r._asdict(), "anti_corr_selected": True, "dropped_reason": ""})
    df = pd.DataFrame(out_rows)
    return df.drop(columns=[c for c in df.columns if c.startswith("Index")], errors="ignore")


def min_notional_feasible(min_action_exposure_frac: float, accessible_frac: Optional[float],
                          slice_capital: float, m: M9Manifest) -> tuple:
    """MIN-NOTIONAL FEASIBILITY at the configured capital (Alberto delta). An entity is feasible IFF its
    slice clears HL min-notional for a typical action AND a KNOWN accessible_frac >= min_accessible_frac.
    Returns (feasible, viable_slice_needed, reason). codex r1#2: exact viable_slice = $10/frac (no floor);
    frac<=0 -> infeasible (inf). codex r1#3: missing/non-finite accessible_frac -> infeasible (can't
    prove enough actions clear min-notional)."""
    frac = float(min_action_exposure_frac)
    if not (frac > 0):
        return False, float("inf"), "min_action_frac<=0"
    viable_slice_needed = m.min_order_notional / frac          # slice s.t. the min action clears $10
    if slice_capital < viable_slice_needed:
        return False, viable_slice_needed, "slice_below_min_notional_viable"
    if accessible_frac is None or accessible_frac != accessible_frac:
        return False, viable_slice_needed, "accessible_frac_unknown"
    if accessible_frac < m.min_accessible_frac:
        return False, viable_slice_needed, f"accessible_frac<{m.min_accessible_frac}"
    return True, viable_slice_needed, ""


def cap_aware_waterfill(desired: dict, carried_exposure: dict, caps: dict, m: M9Manifest,
                        cash_available: float, portfolio_equity: float) -> dict:
    """Cap-aware water-filling (design §2). Allocate `cash_available` to entities by desired weight,
    but only into RESIDUAL CAPACITY = per-entity cap - carried_exposure (an entity already at/over a cap
    gets NO new capital). Iterate: normalize desired over entities with headroom, clip to residual,
    redistribute freed cash pro-rata to entities still with headroom; repeat until no cap can be filled.
    NEVER renormalize past a cap -> residual stays CASH. Returns per-entity {target_weight, cash_funded,
    capped_by} + cash_funded_total + cash_shortfall (desired but un-fundable due to caps).

    caps[eid] = the per-entity HARD ceiling as an ABSOLUTE notional = min(G7 40% x equity,
    M8.max_survivable_slice, bucket residual, gross residual). carried_exposure[eid] = current live
    notional. desired[eid] = desired weight (relative)."""
    eps = 1e-9
    eids = [e for e in desired if desired[e] > 0]
    residual = {e: max(0.0, caps.get(e, 0.0) - carried_exposure.get(e, 0.0)) for e in eids}
    funded = {e: 0.0 for e in eids}
    capped_by = {e: "" for e in eids}
    remaining_cash = float(cash_available)
    desired_total0 = sum(desired[e] for e in eids) or 1.0
    for _ in range(200):
        open_e = [e for e in eids if (residual[e] - funded[e]) > eps]
        if not open_e or remaining_cash <= eps:
            break
        dtot = sum(desired[e] for e in open_e)
        if dtot <= 0:
            break
        any_capped = False
        alloc = {}
        for e in open_e:
            want = remaining_cash * (desired[e] / dtot)
            room = residual[e] - funded[e]
            alloc[e] = min(want, room)
            if want >= room - eps:                 # this entity hits its cap this round
                any_capped = True
                capped_by[e] = "residual_cap"
        for e in open_e:
            funded[e] += alloc[e]
        remaining_cash -= sum(alloc.values())
        if not any_capped:                          # every want fit -> done (residual cash stays cash)
            break
    cash_funded_total = sum(funded.values())
    cash_shortfall = max(0.0, float(cash_available) - cash_funded_total)
    return {
        "per_entity": {e: {"cash_funded": round(funded[e], 6),
                           "target_weight": round(desired[e] / desired_total0, 6),
                           "capped_by": capped_by[e]} for e in eids},
        "cash_funded_total": round(cash_funded_total, 6),
        "cash_shortfall": round(cash_shortfall, 6),
        "deployed_frac": round(cash_funded_total / portfolio_equity, 6) if portfolio_equity > 0 else 0.0,
    }


def expected_leverage(
    adf: Optional[pd.DataFrame], sizing_mode: str = "leader_equity",
    fixed_target_exposure: float = 0.10,
) -> float:
    """Decision-time proxy for the leverage the FOLLOWER will run for an entity = the leader's typical
    absolute target exposure (target_exposure_pct is SIGNED gross exposure as a fraction of equity; M7
    sizes notional = target_exposure_pct x subaccount_equity, so |target_exposure_pct| IS the per-leader
    leverage we copy). Uses the 75th percentile of |target_exposure_pct| over the fold window (a
    conservative, representative peak rather than the mean) so the levered-margin budget reserves enough
    headroom for the gross the leader actually runs. Floors at 1.0 (a copied position is at least its
    margin in notional). Returns 1.0 if no actions / column missing (degrade to unlevered budgeting)."""
    if adf is None or len(adf) == 0:
        return 1.0
    if "stream_replay_valid" in adf.columns:
        adf = adf[adf["stream_replay_valid"].fillna(False).astype(bool)]
    if sizing_mode == "fixed_position":
        if not {"coin", "position_after"}.issubset(adf.columns):
            return 1.0
        open_coins: set[str] = set()
        gross = []
        order = [c for c in ("ts", "event_order") if c in adf.columns]
        rows = adf.sort_values(order).itertuples() if order else adf.itertuples()
        for row in rows:
            pa = pd.to_numeric(
                pd.Series([getattr(row, "position_after")]), errors="coerce"
            ).iloc[0]
            coin = str(getattr(row, "coin"))
            if pd.notna(pa) and abs(float(pa)) > 1e-12:
                open_coins.add(coin)
            else:
                open_coins.discard(coin)
            gross.append(len(open_coins) * abs(float(fixed_target_exposure)))
        return float(max(gross)) if gross else 1.0
    if sizing_mode != "leader_equity":
        raise ValueError(f"unknown sizing_mode {sizing_mode!r}")
    if "target_exposure_pct" not in adf.columns:
        return 1.0
    te = pd.to_numeric(adf["target_exposure_pct"], errors="coerce").abs().dropna()
    if te.empty:
        return 1.0
    return float(max(1.0, te.quantile(0.75)))


def apply_gross_budget(funded: dict, carried_exposure: dict, lev: dict, gross_budget_notional: float,
                       eps: float = 1e-9) -> tuple:
    """ENFORCE the AGGREGATE levered-margin budget (manifest gross_cap): the total resulting NOTIONAL
    across all copied positions must not exceed `gross_budget_notional` (= bankroll x gross_cap). Each
    leader's leverage is COPIED untouched (per-leader leverage is NOT capped); only the AGGREGATE gross
    is constrained. resulting notional per entity = leverage x margin: carried entities contribute
    lev_e x carried_exposure_e (already live, NOT trimmed -- winners compound); NEW margin from the
    water-fill contributes lev_e x funded_e. If the implied aggregate notional exceeds the budget, scale
    the NEW funded margins down PRO-RATA (by their notional contribution) so the aggregate lands exactly
    at the budget; the freed cash is returned. Returns (funded_scaled, returned_cash, gross_notional).

    Carried notional alone could already exceed the budget after a winners-compound fold; in that case
    NO new margin is funded (scale = 0) and we do NOT force-trim carried positions (strategy: caps bind
    at allocation, intra-fold drift is tolerated, never force-trimmed)."""
    carried_notional = sum(lev.get(e, 1.0) * carried_exposure.get(e, 0.0) for e in carried_exposure)
    new_notional = {e: lev.get(e, 1.0) * f for e, f in funded.items()}
    new_notional_total = sum(new_notional.values())
    budget_for_new = max(0.0, gross_budget_notional - carried_notional)
    if new_notional_total <= budget_for_new + eps or new_notional_total <= eps:
        return dict(funded), 0.0, carried_notional + new_notional_total
    scale = budget_for_new / new_notional_total            # in (0,1): trim NEW margin pro-rata
    scaled = {e: f * scale for e, f in funded.items()}
    returned = sum(funded.values()) - sum(scaled.values())
    gross = carried_notional + sum(lev.get(e, 1.0) * f for e, f in scaled.items())
    return scaled, returned, gross


def measure_gross_notional(state, marks: dict) -> float:
    """Realized gross notional of an engine AccountState = sum_coin |szi| x mark (the resulting copied
    exposure). Used to VERIFY the post-M7 aggregate against the budget (the gross_cap is enforced at
    allocation; this confirms the realization)."""
    g = 0.0
    for coin, p in getattr(state, "positions", {}).items():
        mk = marks.get(coin)
        if mk is not None and mk == mk:
            g += abs(p.szi) * mk
    return g


# --------------------------------------------------------------------------- #
# M9-v2 chained-sim CORE: deterministic event clock + global-DD circuit breaker + G4 intra-fold kill
# (design §5/§6 -- the only portfolio interventions; the "no hindsight recovery" guards). Pure on
# per-subaccount equity series; the M7 chaining + ledger integration wraps these once re-run data lands.
# --------------------------------------------------------------------------- #
def portfolio_path(subaccount_equity: dict, event_priority: Optional[dict] = None) -> pd.DataFrame:
    """Merge per-subaccount equity paths on the deterministic portfolio event clock (design §5).
    subaccount_equity = {entity_id: DataFrame[ts, equity]} (each a step function; equity holds between
    its own samples). At each distinct event ts (sorted by (ts, event_priority, entity_id)) MARK every
    subaccount = its last equity at-or-before ts (0 before its first sample), and portfolio_equity =
    sum. Order-independent of subaccount iteration. Returns DataFrame[ts, portfolio_equity, n_open]."""
    if not subaccount_equity:
        return pd.DataFrame(columns=["ts", "portfolio_equity", "n_open"])
    # build sorted (ts, eid) arrays per entity for as-of lookup
    series = {}
    all_ts = set()
    for eid, df in subaccount_equity.items():
        # codex r1#1: collapse same-ts duplicate samples (e.g. an action + a boundary anchor at the
        # same ts) DETERMINISTICALLY to the MIN equity at that ts -> order-independent AND conservative
        # for the causal G4 guard (a same-ts dip is never hidden behind a higher same-ts sample).
        d = df.groupby("ts", as_index=False)["equity"].min().sort_values("ts")
        series[eid] = (d["ts"].to_numpy(), d["equity"].to_numpy())
        all_ts.update(d["ts"].tolist())
    clock = sorted(all_ts)
    rows = []
    for t in clock:
        tot = 0.0
        nopen = 0
        for eid, (ts_arr, eq_arr) in series.items():
            i = int(np.searchsorted(ts_arr, t, side="right")) - 1
            if i >= 0:
                tot += float(eq_arr[i])
                if eq_arr[i] > 0:
                    nopen += 1
        rows.append({"ts": int(t), "portfolio_equity": tot, "n_open": nopen})
    return pd.DataFrame(rows)


def running_dd(portfolio_df: pd.DataFrame) -> pd.DataFrame:
    """Causal running peak + dd_from_peak on the chained portfolio equity (design §6)."""
    df = portfolio_df.sort_values("ts").copy()
    peak = df["portfolio_equity"].cummax()
    df["running_peak"] = peak
    df["dd_from_peak"] = np.where(peak > 0, (peak - df["portfolio_equity"]) / peak, 0.0)
    return df


def detect_global_dd_derisk(portfolio_df: pd.DataFrame, m: M9Manifest) -> Optional[int]:
    """First ts where chained portfolio DD from the running peak exceeds the global de-risk threshold
    (35%). Returns the ts (a de-risk intervention marker) or None. CAUSAL (running peak only)."""
    df = running_dd(portfolio_df)
    breach = df[df["dd_from_peak"] > m.global_dd_derisk]
    return int(breach["ts"].iloc[0]) if len(breach) else None


def g4_intrafold_kill(portfolio_df: pd.DataFrame, fold_initial_equity: float, m: M9Manifest) -> dict:
    """G4 intra-fold kill (design §6): at the FIRST ts portfolio equity breaches `level` x fold-initial
    intra-fold, FLATTEN all -> the fold is KILLED. fold_end equity is taken at the kill ts (no hindsight
    recovery): a fold that dipped < level then recovered still FAILS G4. Returns
    {killed, kill_ts, fold_end_equity, g4_pass}. g4_pass = fold-end equity >= level x fold-initial AND
    not killed."""
    df = portfolio_df.sort_values("ts")
    thresh = m.g4_intrafold_kill * float(fold_initial_equity)
    breach = df[df["portfolio_equity"] <= thresh]
    if len(breach):
        kill_ts = int(breach["ts"].iloc[0])
        fold_end_equity = float(breach["portfolio_equity"].iloc[0])   # frozen at the kill (no recovery)
        return {"killed": True, "kill_ts": kill_ts, "fold_end_equity": fold_end_equity, "g4_pass": False}
    fold_end_equity = float(df["portfolio_equity"].iloc[-1]) if len(df) else float(fold_initial_equity)
    return {"killed": False, "kill_ts": None, "fold_end_equity": fold_end_equity,
            "g4_pass": fold_end_equity >= thresh}


def _reconstruct_account_state(eng, ending: dict):
    """Rebuild an engine AccountState from a step_subaccount ending_account_state dict (for chaining)."""
    st = eng.AccountState(cross_collateral=dict(ending.get("cross_collateral", {})),
                          cooldown_until_ms=int(ending.get("cooldown_until_ms", 0)))
    for c, pv in ending.get("positions", {}).items():
        st.positions[c] = eng.Position(**pv)
    return st


def run_m09_chained(m06b_pool: pd.DataFrame, m08_tiers: pd.DataFrame, m04_entities: pd.DataFrame,
                    folds: pd.DataFrame, eng, md, acts_loader, m: M9Manifest, b0: float,
                    pool_provider: str = "ranked", seed: Optional[int] = None,
                    corr: Optional[dict] = None, out_dir: Optional[str] = None,
                    flush_rows: int = 1_000_000, mem_soft_gb: float = 12.0) -> dict:
    """M9-v2 fixed-bankroll CHAINED portfolio sim over the 8 contiguous test folds. Per fold: select
    (M6b in_pool, drop M8 KILL), size via the sizing chain, run M7 per subaccount on that fold's TEST
    actions (carried state for continuing entities = winners compound untouched; new entities cold-start
    from cash), aggregate on the event clock, apply global-DD/G4, chain ending states to the next fold.
    Returns the chained ROE/DD path + per-fold PnL + G4 + top-entity share (the M10 inputs).

    MEMORY-SAFETY (CLAUDE.md Rule 8): install_memory_guard() at entry; the chained portfolio-equity
    series + per-fold ledger stream to disk via ShardedParquetWriter in bounded chunks (NO giant in-RAM
    frame + final pd.concat). running-DD is computed on a single streamed pass over the equity parts. If
    `out_dir` is None a temp dir is used (cleaned up). acts_loader(wallet, t0, t1) -> actions.

    ALLOCATION (wired here, not just defined): per fold -> M6b in_pool, drop M8 KILL -> anti-corr prune
    (rho_max, KEEP higher-ranked) -> target_count ceiling -> sizing chain -> cap-aware water-fill into
    effective_caps (G7 40% + M8 max_survivable_slice) -> SUSPICIOUS-cohort cap (<=10% of equity) ->
    AGGREGATE levered-margin / gross cap (total resulting notional <= bankroll x gross_cap; per-leader
    leverage copied untouched). Cash from selected-but-unrunnable entities (no wallet / funded<=0) is
    RETURNED to cash (no leak). Fixed bankroll, no-hindsight rebalance, causal, G4-DD-kill intact."""
    # Matched-null pool construction is not wired yet. Previously the argument
    # was ignored, so a caller could request ``matched_null`` and receive the
    # ranked strategy path mislabeled as a null sample. Fail closed until M10's
    # quality-matched provider is implemented and tested.
    if pool_provider != "ranked":
        raise NotImplementedError(f"unsupported M9 pool_provider: {pool_provider!r}")
    install_memory_guard(soft_gb=mem_soft_gb, label="m09_chained")
    conf_map = {"CLEAN": 1.0, "UNCERTAIN": 0.25, "SUSPICIOUS": 0.10, "KILL": 0.0}
    corr = corr or {}
    if "fold_id" in m04_entities.columns:
        ent_fold = m04_entities.set_index(["entity_id", "fold_id"])[
            ["primary_wallet", "entity_tier"]
        ].to_dict("index")
        ent_global = None
    else:
        logger.warning(
            "M9 received a single global M4 entity map; this is not fold-pure and is provisional"
        )
        ent_global = m04_entities.set_index("entity_id")[[
            "primary_wallet", "entity_tier"
        ]].to_dict("index")
        ent_fold = None

    def _entity(eid: int, fid: int) -> dict:
        if ent_fold is not None:
            return ent_fold.get((int(eid), int(fid)), {})
        return ent_global.get(int(eid), {}) if ent_global is not None else {}
    tiers = m08_tiers.set_index(["entity_id", "fold_id"]).to_dict("index")
    fold_rows = folds.sort_values("oos_chain_order")
    pool = m06b_pool[m06b_pool["in_pool"]]

    _tmp = tempfile.mkdtemp(prefix="m09_") if out_dir is None else None
    base = Path(out_dir) if out_dir is not None else Path(_tmp)
    base.mkdir(parents=True, exist_ok=True)
    eq_writer = ShardedParquetWriter(base / "m09_chained_equity.parquet", flush_rows=flush_rows)
    fold_writer = ShardedParquetWriter(base / "m09_per_fold.parquet", flush_rows=max(1, flush_rows))

    carried: dict = {}            # entity_id -> {"state": AccountState, "equity": float, "wallet": str}
    cash = float(b0)
    per_fold = []                 # bounded (<= n_folds rows) summary kept in RAM for the M10-input return
    entity_pnl = {}               # entity_id -> cumulative realized+unrealized PnL contribution
    # streamed running-DD accumulators (NO in-RAM equity concat): causal peak + max DD across the chain.
    run_peak = 0.0
    max_dd = 0.0
    last_equity = float(b0)
    fold_caps_applied = []        # diagnostics: which aggregate constraints bound, per fold

    def _flatten_at_boundary(eid: int, record: dict, ts_ms: int, fid: int) -> float:
        """Close a dropped carried state through M7 execution mechanics."""
        state = record["state"]
        if not getattr(state, "positions", None):
            return float(record["equity"])
        rows = []
        for order, (coin, pos) in enumerate(sorted(state.positions.items())):
            rows.append({
                "coin": coin, "ts": int(ts_ms), "event_order": order,
                "action_type": "EXIT", "signed_size": -float(pos.szi),
                "position_after": 0.0, "target_exposure_pct": 0.0,
                "is_liquidation": False, "carry_in_status": "SEEDED",
                "lifecycle_valid": True, "stream_replay_valid": True,
            })
        params = eng.EngineParams(slippage_band="base", start_policy="future_delta_only")
        params.copy_latency_ms = 0
        params.sizing_mode = m.sizing_mode
        params.fixed_target_exposure = m.fixed_target_exposure
        res = eng.step_subaccount(
            pd.DataFrame(rows), md, float(record["equity"]), params,
            end_ts_ms=int(ts_ms), start_ts_ms=int(ts_ms),
            start_state=state, entity_id=eid, fold_id=fid,
        )
        return float(res["summary"]["final_equity"])

    for fr in fold_rows.itertuples():
        fid = int(fr.fold_id)
        t0 = pd.Timestamp(fr.test_start).value // 1_000_000
        t1 = pd.Timestamp(fr.test_end_excl).value // 1_000_000
        # PRETEST (causal) window [train_start_k, test_start_k) -- the SAME look-ahead-safe window M5/M6
        # use. Allocation/sizing decisions may ONLY use data known at test_start_k; never the test fold.
        pt0 = pd.Timestamp(getattr(fr, "pretest_start", fr.train_start)).value // 1_000_000
        pt1 = pd.Timestamp(getattr(fr, "pretest_end_excl", fr.test_start)).value // 1_000_000
        fold_pool = pool[pool["fold_id"] == fid]
        # desired weights (drop KILL) + select_priority for anti-corr (m6b quality x survival).
        desired = {}
        eweight = {}
        tier_of = {}
        prio_rows = []
        for r in fold_pool.itertuples():
            eid = int(r.entity_id)
            tk = tiers.get((eid, fid), {})
            surv = float(tk.get("survival_multiplier", 1.0))
            if surv <= 0 or tk.get("tier") == "kill":
                continue
            etier = str(_entity(eid, fid).get("entity_tier", "UNCERTAIN"))
            conf = conf_map.get(etier, 0.25)
            w = sizing_chain_weight(r.quality_weight, conf, surv)
            if w > 0:
                desired[eid] = w
                eweight[eid] = {"max_surv": tk.get("max_survivable_slice", np.inf)}
                tier_of[eid] = etier
                prio_rows.append({"entity_id": eid, "select_priority": float(r.quality_weight) * surv})

        # ANTI-CORR prune + target_count ceiling (design §4): drop lower-ranked of a >rho_max pair.
        if prio_rows:
            ac = anti_corr_select(pd.DataFrame(prio_rows), corr, m)
            keep_ids = set(int(e) for e in ac[ac["anti_corr_selected"]]["entity_id"])
            for eid in list(desired):
                if eid not in keep_ids:
                    desired.pop(eid, None); eweight.pop(eid, None); tier_of.pop(eid, None)
        selected = set(desired)

        # DROP: carried entities not reselected (incl. anti-corr-pruned) -> flatten to cash at carried eq.
        for eid in list(carried):
            if eid not in selected:
                before = float(carried[eid]["equity"])
                after = _flatten_at_boundary(eid, carried[eid], t0, fid)
                cash += after
                entity_pnl[eid] = entity_pnl.get(eid, 0.0) + (after - before)
                del carried[eid]

        # SIZE new/top-up via cap-aware water-filling against the running portfolio equity.
        kept_equity = sum(c["equity"] for c in carried.values())
        portfolio_equity = kept_equity + cash
        cap_df = pd.DataFrame([{"entity_id": e, "max_survivable_slice": eweight[e]["max_surv"]} for e in selected])
        caps = effective_caps(cap_df, m, portfolio_equity) if len(cap_df) else {}
        # SUSPICIOUS-cohort cap: the SUSPICIOUS tier's combined NEW margin <= suspicious_cohort_cap x
        # equity. Enforced as a per-entity ceiling tightened so the cohort's residual sums to the cap.
        susp = [e for e in selected if tier_of.get(e) == "SUSPICIOUS"]
        if susp:
            cohort_budget = m.suspicious_cohort_cap * portfolio_equity
            susp_carried = sum(carried[e]["equity"] for e in susp if e in carried)
            cohort_new_room = max(0.0, cohort_budget - susp_carried)
            cohort_resid = sum(max(0.0, caps.get(e, 0.0) - carried.get(e, {}).get("equity", 0.0)) for e in susp)
            if cohort_resid > cohort_new_room + 1e-9 and cohort_resid > 0:
                shrink = cohort_new_room / cohort_resid          # scale each SUSPICIOUS cap's residual
                for e in susp:
                    ce = carried.get(e, {}).get("equity", 0.0)
                    caps[e] = ce + max(0.0, caps.get(e, 0.0) - ce) * shrink
        carried_exposure = {e: carried[e]["equity"] for e in selected if e in carried}
        wf = cap_aware_waterfill(desired, carried_exposure, caps, m, cash, portfolio_equity)
        funded = {e: wf["per_entity"].get(e, {}).get("cash_funded", 0.0) for e in selected}

        # AGGREGATE LEVERED-MARGIN / GROSS CAP: total resulting NOTIONAL <= bankroll x gross_cap. Build
        # per-entity expected leverage (leader's copied target_exposure) STRICTLY from the PRETEST/causal
        # window (data known at test_start_k); NEVER from the test fold (that would be OOS look-ahead --
        # sizing the fold off the fold's own future behavior). The TEST-fold actions are loaded separately
        # below ONLY to RUN M7; they never feed an allocation decision.
        adf_cache = {}
        lev = {}
        for eid in selected:
            wallet = _entity(eid, fid).get("primary_wallet")
            test_adf = acts_loader(wallet, t0, t1) if wallet is not None else None
            adf_cache[eid] = (wallet, test_adf)
            pre_adf = acts_loader(wallet, pt0, pt1) if wallet is not None else None
            lev[eid] = expected_leverage(
                pre_adf, m.sizing_mode, m.fixed_target_exposure
            )  # causal: pretest leverage, no look-ahead
        # FIXED-BANKROLL thesis: the gross budget keys off the FIXED bankroll b0, NOT live portfolio
        # equity -- winners must NOT expand portfolio capacity fold-to-fold (no equity-following leverage).
        gross_budget = m.gross_cap * float(b0)
        funded, gross_returned, implied_gross = apply_gross_budget(funded, carried_exposure, lev, gross_budget)
        cash -= sum(funded.values())                       # only the gross/cap-trimmed margin leaves cash
        fold_caps_applied.append({"fold_id": fid, "gross_budget": gross_budget,
                                  "implied_gross_notional": implied_gross,
                                  "gross_trimmed_cash": gross_returned, "n_suspicious": len(susp)})

        # RUN M7 per selected subaccount on this fold's TEST actions (carried or new).
        params = eng.EngineParams(slippage_band="base", start_policy="causal_carry_in")
        params.sizing_mode = m.sizing_mode
        params.fixed_target_exposure = m.fixed_target_exposure
        sub_eq = {}
        new_carried = {}
        deployed_new = 0.0                                 # NEW margin that actually reached the engine
        for eid in selected:
            wallet, adf = adf_cache[eid]
            if wallet is None:                             # unrunnable: RETURN any funded cash (no leak)
                cash += funded.get(eid, 0.0)
                continue
            if eid in carried:                         # continuing winner: carried state + any top-up
                start_state = copy.deepcopy(carried[eid]["state"])
                # CASH CONSERVATION: a carried entity can receive a water-fill TOP-UP (funded[eid]); that
                # margin was already debited from cash, so it MUST enter the carried subaccount's starting
                # equity. Dropping it (start from old equity only) silently vanishes that cash.
                topup = funded.get(eid, 0.0)
                start_state.cross_collateral["main"] = (
                    start_state.cross_collateral.get("main", 0.0) + topup
                )
                start_eq = carried[eid]["equity"] + topup
                deployed_new += topup
            else:                                       # new entity: cold-start from the funded slice
                if funded.get(eid, 0.0) <= 0:
                    cash += funded.get(eid, 0.0)           # RETURN unfunded-but-selected cash (no leak)
                    continue
                start_state = None; start_eq = funded[eid]
                deployed_new += funded[eid]
            res = eng.step_subaccount(adf, md, start_eq, params, end_ts_ms=t1, start_ts_ms=t0,
                                      start_state=start_state, entity_id=eid, fold_id=fid)
            eqdf = pd.DataFrame(res["equity"])
            if not eqdf.empty:
                sub_eq[eid] = eqdf[["ts", "subaccount_equity"]].rename(columns={"subaccount_equity": "equity"})
            end_eq = float(res["summary"]["final_equity"])
            new_carried[eid] = {"state": _reconstruct_account_state(eng, res["ending_account_state"]),
                                "equity": end_eq, "wallet": wallet}
            entity_pnl[eid] = entity_pnl.get(eid, 0.0) + (end_eq - start_eq)

        # AGGREGATE this fold on the event clock + add cash -> chained portfolio equity.
        fold_port = portfolio_path(sub_eq) if sub_eq else pd.DataFrame(columns=["ts", "portfolio_equity"])
        fold_port = fold_port.copy()
        if len(fold_port):
            fold_port["portfolio_equity"] = fold_port["portfolio_equity"] + cash      # idle cash (earns 0)
        fold_initial = kept_equity + cash + deployed_new   # cash now holds returned (unrunnable) margin
        g4 = g4_intrafold_kill(fold_port, fold_initial, m) if len(fold_port) else \
            {"killed": False, "kill_ts": None, "fold_end_equity": fold_initial, "g4_pass": True}

        # GLOBAL-DD circuit breaker (design §6): causal running-DD on the CHAINED equity (this fold's
        # portfolio path concatenated after the prior peak). If DD from the running peak breaches the
        # de-risk threshold intra-fold, that is a portfolio-level intervention. Detect on the chained
        # series so the breaker sees the carried peak, not just this fold.
        gdd_ts = None
        if len(fold_port):
            chained_fp = fold_port[["ts", "portfolio_equity"]].copy()
            ddf = running_dd(chained_fp)
            ddf["running_peak"] = np.maximum(ddf["running_peak"], run_peak)   # carry the chain peak in
            ddf["dd_from_peak"] = np.where(ddf["running_peak"] > 0,
                                           (ddf["running_peak"] - ddf["portfolio_equity"]) / ddf["running_peak"], 0.0)
            gbreach = ddf[ddf["dd_from_peak"] > m.global_dd_derisk]
            if len(gbreach):
                gdd_ts = int(gbreach["ts"].iloc[0])

        # APPLY the interventions (Fix: previously recorded but never enforced). On the FIRST of {G4 kill,
        # global-DD breach}, FLATTEN every carried position to cash at the breach equity -- NO post-kill
        # recovery is carried forward, and the chained equity is truncated at the breach ts (causal).
        intervention_ts = None
        intervention_kind = ""
        if g4["killed"] and gdd_ts is not None:
            if g4["kill_ts"] <= gdd_ts:
                intervention_ts, intervention_kind = g4["kill_ts"], "g4_kill"
            else:
                intervention_ts, intervention_kind = gdd_ts, "global_dd_derisk"
        elif g4["killed"]:
            intervention_ts, intervention_kind = g4["kill_ts"], "g4_kill"
        elif gdd_ts is not None:
            intervention_ts, intervention_kind = gdd_ts, "global_dd_derisk"

        # CAUSAL G4 diagnostic: g4["killed"] is computed on the FULL untruncated fold path, so it can flag
        # a G4 breach that occurs AFTER an earlier global-DD flatten (which would have already exited -- so
        # that G4 event never causally happens). The diagnostic must reflect ONLY the causal path up to the
        # first intervention: G4 is "killed" iff it is the intervention that fires first.
        g4_killed_causal = (intervention_kind == "g4_kill")

        if intervention_ts is not None and len(fold_port):
            # portfolio equity at-or-before the breach ts (already includes idle cash); FLATTEN to cash.
            at = fold_port[fold_port["ts"] <= intervention_ts]
            breach_equity = float(at["portfolio_equity"].iloc[-1]) if len(at) else fold_initial
            fold_port = at.copy()                          # truncate the streamed/DD path at the breach
            fold_end_equity = breach_equity
            new_carried = {}                               # flattened: no positions carried past the breach
            cash = breach_equity                           # all equity -> cash (de-risked)
        else:
            fold_end_equity = g4["fold_end_equity"]

        per_fold.append({"fold_id": fid, "fold_initial": fold_initial, "fold_end_equity": fold_end_equity,
                         "fold_pnl": fold_end_equity - fold_initial, "g4_killed": g4_killed_causal,
                         "g4_pass": g4["g4_pass"], "n_selected": len(selected),
                         "intervention": intervention_kind, "intervention_ts": intervention_ts})
        fold_writer.add({"fold_id": fid, "fold_initial": float(fold_initial),
                         "fold_end_equity": float(fold_end_equity),
                         "fold_pnl": float(fold_end_equity - fold_initial), "g4_killed": bool(g4_killed_causal),
                         "g4_pass": bool(g4["g4_pass"]), "n_selected": int(len(selected)),
                         "intervention": str(intervention_kind)})
        # STREAM the fold's portfolio equity rows to disk + fold the running-DD pass inline (bounded RAM).
        if len(fold_port):
            for ts, eq in zip(fold_port["ts"].to_numpy(), fold_port["portfolio_equity"].to_numpy()):
                eqf = float(eq)
                run_peak = max(run_peak, eqf)
                if run_peak > 0:
                    max_dd = max(max_dd, (run_peak - eqf) / run_peak)
                last_equity = eqf
                eq_writer.add({"fold_id": fid, "ts": int(ts), "portfolio_equity": eqf,
                               "running_peak": float(run_peak),
                               "dd_from_peak": float((run_peak - eqf) / run_peak) if run_peak > 0 else 0.0})
        carried = new_carried
        # cash already net of funded; carried holds the rest of equity (flattened to cash on intervention)

    n_equity_rows = eq_writer.close()
    fold_writer.close()
    final_equity = sum(c["equity"] for c in carried.values()) + cash
    chained_roe = final_equity / b0 - 1.0 if b0 > 0 else 0.0
    n_pos = sum(1 for f in per_fold if f["fold_pnl"] > 0)
    total_pnl = sum(entity_pnl.values())
    if total_pnl > 0:
        top_share = max((max(v, 0.0) / total_pnl for v in entity_pnl.values()), default=0.0)
    else:
        # A losing/flat strategy cannot satisfy a positive-PnL concentration
        # gate.  Avoid nonsensical positive shares created by dividing losses
        # by a negative total.
        top_share = float("inf") if entity_pnl else 0.0
    max_chained_calmar = chained_roe / max(max_dd, 1e-9)
    if _tmp is not None:                                   # temp output: clean up the streamed parts
        import shutil
        shutil.rmtree(_tmp, ignore_errors=True)
        equity_path = None
    else:
        equity_path = str(base / "m09_chained_equity.parquet")
    return {
        "b0": b0, "pool_provider": pool_provider, "seed": seed,
        "chained_roe": chained_roe, "max_chained_dd": max_dd, "chained_calmar": max_chained_calmar,
        "final_equity": final_equity, "n_positive_folds": n_pos, "n_equity_rows": n_equity_rows,
        "g4_no_kill": all(not f["g4_killed"] for f in per_fold),
        "g4_all_folds_above_floor": all(f["fold_end_equity"] >= m.g4_intrafold_kill * f["fold_initial"] for f in per_fold),
        "n_g4_kills": sum(1 for f in per_fold if f.get("intervention") == "g4_kill"),
        "n_global_dd_derisks": sum(1 for f in per_fold if f.get("intervention") == "global_dd_derisk"),
        "any_intervention": any(f.get("intervention") for f in per_fold),
        "top_entity_pnl_share": top_share, "per_fold": per_fold, "fold_caps_applied": fold_caps_applied,
        "equity_path": equity_path,
        "_simplifications": "dropped entities exit through M7 at fold boundary; "
                            "G4/global-DD breach uses marked breach equity because historical "
                            "per-subaccount state snapshots are not yet emitted",
    }


def effective_caps(entities: pd.DataFrame, m: M9Manifest, portfolio_equity: float) -> dict:
    """Per-entity absolute-notional cap = min(G7 40% x equity, M8 max_survivable_slice). Bucket/gross
    residuals are applied in the v2 chained sim (need live state); here we set the allocation-time
    per-entity ceiling. SUSPICIOUS-cohort cap is enforced as a separate group constraint in v2."""
    caps = {}
    for r in entities.itertuples():
        g7 = m.per_entity_cap * portfolio_equity
        m8 = getattr(r, "max_survivable_slice", None)
        # codex r1#4: ONLY missing/NaN defaults to inf (G7 binds). PRESERVE 0 (M8 capped out ->
        # min(G7,0)=0, no new cash); clamp negatives to 0.
        if m8 is None or m8 != m8:
            m8 = np.inf
        else:
            m8 = max(0.0, float(m8))
        caps[int(r.entity_id)] = float(min(g7, m8))
    return caps
