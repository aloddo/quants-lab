#!/usr/bin/env python3
"""v26 family selectors (amendment codex #7/#11, frozen; decisions D1-D3 in v26_common).

Every family selects WALLETS at the fold asof from CLOSED train journeys only
(exit_ts <= asof -- v25 journeys_foldK artifacts already contain exactly those), over the
v25 gate-ELIGIBLE pool, deduped to one wallet per entity (v25 entities_foldK), ranked to
a list of up to MAX_K=1000 entities; a config's roster is the top-K prefix and
K_real(fold) = min(K, list length).

Hold-band binding (codex #7, frozen): band statistic = median hold duration over closed
train journeys, linear-interpolated quantile (numpy default), intervals half-open
[lo, hi); < 30 closed train journeys => wallet fails the band filter (counted). Cadence =
closed train journeys / distinct active days (distinct UTC exit dates of closed train
journeys -- decision D2). TEST trades are never filtered by their own hold duration; the
band filters wallets at selection time only.

Families:
- F1a swing-R1: v25 score_r1 (causal v16 cohort scoring, unchanged) over band-passing
  wallets. R1's own hold window [2h,48h) is the family's definitional band (D1).
- F1b swing-R2: v25 score_r2 (objective-aligned LCB) over wallets passing the swing
  definitional band [2h,48h) intersected with the axis band (D1).
- F2 scalp-LCB: closed-train median hold in [2,15)min, >= 30 closed journeys, cadence
  >= 5/day; rank R2-LCB.
- F3 tail-asym (codex #11, frozen formula): >= 100 closed journeys AND >= 20 losing
  (losing = net bps STRICTLY < 0; zero-return journeys are NOT losses);
  R_unit_w = |p25 of losing journeys' net bps| (losses only; 0 => wallet fails,
  counted); tail_asym = p95(net bps) / max(|p5(net bps)|, 1e-9); big_run_count =
  #journeys with net bps >= 10 x R_unit_w, REQUIRE >= max(3, 1% of n). Rank: tail_asym
  desc, secondary R2-style LCB desc (missing LCB = -inf), tie lexicographic wallet.
  All on NET (canonical leader-fee model) bps.
- F4a active-concentrated: cadence >= 10/day; F4b active-diluted: cadence <= 2/day;
  rank R2-LCB (D3).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v26_common import (BAND_BOUNDS_MS, F2_MIN_CADENCE, F2_MIN_CLOSED, F3_BIG_RUN_R,
                        F3_MIN_JOURNEYS, F3_MIN_LOSSES, F4A_MIN_CADENCE, F4B_MAX_CADENCE,
                        FAMILY_DEF_BAND_MS, FAMILY_VARIANTS, HOLD_BANDS, MAX_K,
                        MIN_CLOSED_FOR_BAND, MS_DAY, load_artifact, scenario_base)
from v25_r1_causal import score_r1
from v25_r2_lcb import R2_MIN_TRIPS, wallet_lcb


# ---- band algebra (decision D1) -------------------------------------------------------------- #
def effective_band(fam: str, band: str):
    """Intersection of the family's definitional band with the axis band.
    Returns (lo, hi) half-open ms interval, None (unconstrained), or 'EMPTY'."""
    d = FAMILY_DEF_BAND_MS.get(fam)
    b = BAND_BOUNDS_MS[band]
    if d is None and b is None:
        return None
    if d is None:
        return b
    if b is None:
        return d
    lo, hi = max(d[0], b[0]), min(d[1], b[1])
    return (lo, hi) if lo < hi else "EMPTY"


def allowed_bands(fam: str) -> dict[str, str]:
    """Per band: 'RUN' or the prune reason 'family_forces_band' (empty intersection OR
    dedup-collapse onto an earlier identical cell, per the amendment's dedup rule)."""
    out = {}
    seen: list = []
    for band in HOLD_BANDS:
        eff = effective_band(fam, band)
        if eff == "EMPTY" or eff in seen:
            out[band] = "family_forces_band"
        else:
            out[band] = "RUN"
            seen.append(eff)
    return out


# ---- per-wallet train stats ------------------------------------------------------------------ #
def wallet_band_stats(journeys: pd.DataFrame) -> pd.DataFrame:
    """Per wallet over CLOSED train journeys: n_closed, median hold (ms, linear interp),
    cadence (n_closed / distinct UTC exit dates). journeys = v25 journeys_foldK rows."""
    if journeys.empty:
        return pd.DataFrame(columns=["wallet", "n_closed", "median_hold_ms", "cadence"]
                            ).set_index("wallet")
    j = journeys
    hold_ms = j["duration_h"].to_numpy(dtype="float64") * 3.6e6
    exit_day = (j["exit_ts"].to_numpy(dtype="float64") // MS_DAY).astype("int64")
    df = pd.DataFrame({"wallet": j["wallet"].to_numpy(), "hold_ms": hold_ms,
                       "exit_day": exit_day})
    g = df.groupby("wallet")
    out = g.agg(n_closed=("hold_ms", "size"),
                median_hold_ms=("hold_ms", "median"),          # numpy linear interpolation
                active_days=("exit_day", "nunique"))
    out["cadence"] = out["n_closed"] / out["active_days"].clip(lower=1)
    return out


def band_pass_wallets(stats: pd.DataFrame, interval) -> tuple[set, int]:
    """Wallets whose median hold lies in the half-open interval; < MIN_CLOSED_FOR_BAND
    closed journeys fails the band filter (counted). interval None = no filter."""
    if interval is None:
        return set(stats.index), 0
    lo, hi = interval
    enough = stats["n_closed"] >= MIN_CLOSED_FOR_BAND
    n_failed_min = int((~enough).sum())
    ok = stats[enough & (stats["median_hold_ms"] >= lo)
               & (stats["median_hold_ms"] < hi)]
    return set(ok.index), n_failed_min


# ---- F3 tail scoring (frozen formula) -------------------------------------------------------- #
def f3_wallet_row(bps: np.ndarray) -> dict | None:
    """Frozen F3 filter+stats for one wallet's closed-train net bps array.
    Returns None if the wallet fails any F3 requirement; else the stats row."""
    n = int(bps.size)
    if n < F3_MIN_JOURNEYS:
        return None
    # losing journey = net bps STRICTLY < 0 (codex code-gate #1: zero-return journeys
    # are NOT losses). A wallet whose "losses" are all exactly 0 has NO losing journeys
    # and fails the >= F3_MIN_LOSSES minimum below (R_unit undefined -> wallet fails,
    # the amendment's frozen fail-closed behavior). The zero_r_unit branch stays as a
    # defensive fail-closed guard.
    losses = bps[bps < 0]
    if losses.size < F3_MIN_LOSSES:
        return None
    r_unit = float(abs(np.percentile(losses, 25)))     # p25 of losses only, linear interp
    if r_unit == 0.0:
        return {"fail": "zero_r_unit"}
    tail_asym = float(np.percentile(bps, 95) / max(abs(np.percentile(bps, 5)), 1e-9))
    big_runs = int((bps >= F3_BIG_RUN_R * r_unit).sum())
    if big_runs < max(3.0, 0.01 * n):
        return None
    return {"n": n, "r_unit": r_unit, "tail_asym": tail_asym, "big_runs": big_runs}


def score_f3(journeys: pd.DataFrame, lcb_by_wallet: dict, entity_map: dict,
             top_n: int = MAX_K) -> tuple:
    """F3 ranking. journeys: closed train journeys of the candidate wallets (net bps via
    the canonical leader-fee model already inside net_realized_pnl). Secondary key =
    R2-style LCB of the wallet's train copy trips (missing => -inf)."""
    diag = {"n_zero_r_unit": 0}
    j = journeys[journeys["max_notional"] > 0]
    rows = []
    for w, g in j.groupby("wallet"):
        bps = (g["net_realized_pnl"].to_numpy(dtype="float64")
               / g["max_notional"].to_numpy(dtype="float64") * 1e4)
        r = f3_wallet_row(bps)
        if r is None:
            continue
        if r.get("fail") == "zero_r_unit":
            diag["n_zero_r_unit"] += 1
            continue
        lcb = lcb_by_wallet.get(w, -np.inf)
        r["wallet"] = w
        r["lcb_bps"] = lcb if lcb == lcb else -np.inf
        rows.append(r)
    scored = pd.DataFrame(rows)
    if scored.empty:
        return pd.DataFrame(), scored, diag
    # frozen rank: tail_asym desc, LCB desc, wallet lexicographic asc
    scored = scored.sort_values(["tail_asym", "lcb_bps", "wallet"],
                                ascending=[False, False, True],
                                kind="mergesort").reset_index(drop=True)
    roster_rows = []
    seen = set()
    for _, r in scored.iterrows():
        ent = entity_map.get(r["wallet"], r["wallet"])
        if ent in seen:
            continue
        seen.add(ent)
        roster_rows.append({"wallet": r["wallet"], "entity": ent,
                            "rank": len(roster_rows) + 1,
                            "score": float(r["tail_asym"]),
                            "lcb_bps": float(r["lcb_bps"]),
                            "n_trips": int(r["n"])})
        if len(roster_rows) >= top_n:
            break
    return pd.DataFrame(roster_rows), scored, diag


# ---- fold context + family dispatch ---------------------------------------------------------- #
class FoldContext:
    """Loaded v25 fold artifacts, filtered to gate-eligible wallets."""

    def __init__(self, fold: int, train_start_ms: int, asof_ms: int):
        self.fold = fold
        self.train_start_ms = train_start_ms
        self.asof_ms = asof_ms
        gates = load_artifact("gates", fold)
        self.eligible = set(gates[gates["eligible"]]["wallet"])
        ent = load_artifact("entities", fold)
        self.entity_map = dict(zip(ent["wallet"], ent["entity"]))
        j = load_artifact("journeys", fold)
        self.journeys = j[j["wallet"].isin(self.eligible)]
        t = load_artifact("r2trips", fold)
        self.r2trips = t[t["wallet"].isin(self.eligible)]
        eq_p = load_artifact("equity", fold)
        self.equity = eq_p
        self.band_stats = wallet_band_stats(self.journeys)
        self._scenario = scenario_base()
        self._r2_scored: pd.DataFrame | None = None

    def subset(self, wallets: set):
        return (self.journeys[self.journeys["wallet"].isin(wallets)],
                self.r2trips[self.r2trips["wallet"].isin(wallets)])

    @property
    def r2_scored(self) -> pd.DataFrame:
        """Per-wallet one-sided 90% LCB of mean net bps (v25 wallet_lcb, frozen params),
        computed ONCE per fold over ALL eligible wallets with >= 50 realized train copy
        trips. The per-wallet LCB is cell-independent, so every R2-ranked family/band
        cell filters this shared table (identical semantics to calling v25 score_r2 per
        cell, computed once)."""
        if self._r2_scored is None:
            t = self.r2trips[~self.r2trips["terminal"].astype(bool)]
            rows = []
            for w, g in t.groupby("wallet"):
                if len(g) < R2_MIN_TRIPS:
                    continue
                r = wallet_lcb(g, self.train_start_ms, self.asof_ms)
                r["wallet"] = w
                rows.append(r)
            s = pd.DataFrame(rows, columns=["lcb_bps", "mean_bps", "n_trips", "wallet"])
            if len(s):
                s = s[np.isfinite(s["lcb_bps"])]
                s = s.iloc[np.lexsort((s["wallet"].to_numpy(),
                                       -s["lcb_bps"].to_numpy()))]
            self._r2_scored = s.reset_index(drop=True)
        return self._r2_scored

    def rank_r2(self, wallets: set, top_n: int = MAX_K) -> pd.DataFrame:
        """v25 score_r2 semantics over a candidate wallet subset using the cached
        per-wallet LCB table: LCB desc, wallet-lex ties, one wallet per entity."""
        s = self.r2_scored
        s = s[s["wallet"].isin(wallets)] if len(s) else s
        roster_rows = []
        seen = set()
        for r in s.itertuples(index=False):
            ent = self.entity_map.get(r.wallet, r.wallet)
            if ent in seen:
                continue
            seen.add(ent)
            roster_rows.append({"wallet": r.wallet, "entity": ent,
                                "rank": len(roster_rows) + 1,
                                "score": float(r.lcb_bps),
                                "mean_bps": float(r.mean_bps),
                                "n_trips": int(r.n_trips)})
            if len(roster_rows) >= top_n:
                break
        return pd.DataFrame(roster_rows)


def family_ranking(fam: str, band: str, ctx: FoldContext, top_n: int = MAX_K
                   ) -> tuple[pd.DataFrame, dict]:
    """Ranked entity roster (up to top_n) for one (family_variant, band) cell on one
    fold's TRAIN data. Returns (roster df [wallet, entity, rank, score, ...], diag)."""
    assert fam in FAMILY_VARIANTS and band in HOLD_BANDS
    diag: dict = {}
    eff = effective_band(fam, band)
    if eff == "EMPTY":
        return pd.DataFrame(), {"reason": "family_forces_band"}
    wallets, n_failed_min = band_pass_wallets(ctx.band_stats, eff)
    diag["n_band_min_closed_failed"] = n_failed_min
    st = ctx.band_stats

    if fam == "F2":
        wallets = {w for w in wallets
                   if st.loc[w, "n_closed"] >= F2_MIN_CLOSED
                   and st.loc[w, "cadence"] >= F2_MIN_CADENCE}
    elif fam == "F4a":
        wallets = {w for w in wallets if w in st.index
                   and st.loc[w, "cadence"] >= F4A_MIN_CADENCE}
    elif fam == "F4b":
        wallets = {w for w in wallets if w in st.index
                   and st.loc[w, "cadence"] <= F4B_MAX_CADENCE}
    diag["n_candidates_after_filters"] = len(wallets)
    if not wallets:
        return pd.DataFrame(), diag

    if fam == "F1a":
        j, _t = ctx.subset(wallets)
        roster, _s, d = score_r1(j, None, ctx.entity_map, ctx.equity, ctx._scenario,
                                 ctx.asof_ms, top_n=top_n) \
            if len(j) else (pd.DataFrame(), None, {})
    elif fam == "F3":
        j, _t = ctx.subset(wallets)
        lcb_by_w = dict(zip(ctx.r2_scored["wallet"], ctx.r2_scored["lcb_bps"])) \
            if len(ctx.r2_scored) else {}
        roster, _s, d = score_f3(j, lcb_by_w, ctx.entity_map, top_n=top_n)
    else:                                   # F1b, F2, F4a, F4b: R2-LCB ranking
        roster, d = ctx.rank_r2(wallets, top_n=top_n), {}
    diag.update(d or {})
    diag["n_ranked_entities"] = len(roster)
    return roster, diag


def build_fold_rankings(ctx: FoldContext, cells: list[tuple[str, str]] | None = None
                        ) -> tuple[dict, pd.DataFrame]:
    """All (family, band) rankings for one fold. cells: optional restriction.
    Returns ({(fam, band): roster df}, long-form rankings DataFrame)."""
    out: dict = {}
    rows = []
    for fam in FAMILY_VARIANTS:
        for band, status in allowed_bands(fam).items():
            if status != "RUN":
                continue
            if cells is not None and (fam, band) not in cells:
                continue
            roster, diag = family_ranking(fam, band, ctx)
            out[(fam, band)] = roster
            for _, r in (roster if len(roster) else pd.DataFrame()).iterrows():
                rows.append({"fold": ctx.fold, "family": fam, "band": band,
                             "rank": int(r["rank"]), "wallet": r["wallet"],
                             "entity": r["entity"], "score": float(r["score"])})
    cols = ["fold", "family", "band", "rank", "wallet", "entity", "score"]
    return out, (pd.DataFrame(rows, columns=cols) if rows
                 else pd.DataFrame(columns=cols))
