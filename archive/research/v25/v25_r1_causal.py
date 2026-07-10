#!/usr/bin/env python3
"""v25 R1: v16-cohort-causal rule -- research/v16/build_skill_cohort.py scoring reimplemented
API-FREE and CAUSAL (spec-frozen; gate-b blocker #4 fidelity requirements).

Ported from build_skill_cohort.py (V17-on basis, COST=exec analog), source-parity:
- journeys reconstructed via opening_journey_id / closing_journey_id (v25_common.
  build_journeys; grouping by back-compat journey_id is FORBIDDEN)
- skill scored on NET realized PnL: ret = net_realized_pnl / max_notional in [-1, 2],
  max_notional > 10 (build_skill_cohort.py:353 basis; net = realized - canonical leader
  fees, funding not reconstructible from m02_actions -- documented in v25_common)
- net_ret = ret - per-coin RT execution cost (from the frozen scenario, canonical model)
- eligibility: n >= 40 journeys, median hold in [2h, 48h] (activity is enforced upstream
  by the frozen common gate: last fill <= 7d + >= 20 active days)
- HARD martingale veto (codex 2026-06-05 rule), on net_realized_pnl exactly as the
  source (build_skill_cohort.py:75)
- consistency axis: green-calendar-week fraction on net_ret
- skill = z(win) + z(sharpe) + z(-maxdd) + z(consistency)

DD GATE REIMPLEMENTED CAUSALLY (the committed v16 version queries the live Hyperliquid
API; that call MUST NOT and does NOT exist here): the same drop criterion (MTM maxDD
>= 70%, or 60-70% AND window return <= -40%) is evaluated on the wallet's OWN m02 equity
history (source_equity_post) over [asof - 30d, asof], daily-sampled, for ALL
score-eligible wallets (the arbitrary top_n*1.8*3 candidate pool is REMOVED -- gate-b
blocker #4). FAIL-CLOSED: fewer than 5 daily equity samples in the window => the wallet
is EXCLUDED (the v16 live-API version silently passed such wallets; the spec requires
fail-closed on missing data).

Output: top 25 entities at asof (one wallet per entity; entity rank = its best wallet's
skill; ties broken by lexicographically smaller wallet).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v25_common import TOP_N_ENTITIES, MS_DAY, ExecScenario

MIN_J = 40
HOLD_MIN_H = 2.0
HOLD_MAX_H = 48.0
DD_WINDOW_DAYS = 30
DD_HARD = 70.0
DD_SOFT_LO = 60.0
DD_SOFT_RET = -40.0
DD_MIN_SAMPLES = 5


def max_dd(r: np.ndarray) -> float:
    eq = np.cumsum(r)
    return float((np.maximum.accumulate(eq) - eq).max()) if len(eq) else 0.0


def z(x: pd.Series) -> pd.Series:
    return (x - x.mean()) / (x.std() + 1e-9)


def martingale_flags(df: pd.DataFrame) -> pd.Series:
    """Port of build_skill_cohort.martingale_flags on the SAME basis as the source:
    net_realized_pnl + max_notional (build_skill_cohort.py:75)."""
    out = {}
    for w, g in df.sort_values("entry_ts").groupby("wallet"):
        pnl = g["net_realized_pnl"].to_numpy()
        ntl = g["max_notional"].to_numpy()
        dur = g["duration_h"].to_numpy()
        n = len(pnl)
        if n < 10:
            out[w] = False
            continue
        win = pnl > 0
        loss = pnl < 0
        nloss = int(loss.sum())
        nal = ntl[1:][loss[:-1]]
        naw = ntl[1:][win[:-1]]
        su = (np.mean(nal) / np.mean(naw)) if (len(nal) and len(naw) and np.mean(naw) > 0) else np.nan
        ha = (np.mean(dur[loss]) / np.mean(dur[win])) if (nloss > 0 and win.any() and np.mean(dur[win]) > 0) else np.nan
        wm = (np.mean(pnl[win]) / np.mean(np.abs(pnl[loss]))) if (nloss > 0 and win.any() and np.mean(np.abs(pnl[loss])) > 0) else np.inf
        lf = nloss / n
        extreme = ((su == su and su > 3.0) or (ha == ha and ha > 5.0) or (lf < 0.02))
        mild = int((su == su and su > 1.3) + (ha == ha and ha > 2.5) + (wm < 0.6) + (lf < 0.05))
        out[w] = bool(extreme or mild >= 2)
    return pd.Series(out, dtype=bool)


def weekly_consistency(df: pd.DataFrame, ret_col: str = "net_ret") -> pd.Series:
    g = df.copy()
    g["wk"] = pd.to_datetime(g["entry_ts"], unit="ms").dt.to_period("W")
    wk = g.groupby(["wallet", "wk"])[ret_col].sum().reset_index()
    return wk.groupby("wallet")[ret_col].agg(lambda x: float((x > 0).mean()))


def causal_dd_exclude(equity_daily: pd.DataFrame, wallets: list[str], asof_ms: int) -> tuple:
    """API-free reimplementation of v16 mtm_dd_exclude on m02 equity history.
    equity_daily: DataFrame [wallet, day_ms, equity] (last source_equity_post per day).
    Returns (exclude_set, diagnostics dict). FAIL-CLOSED: <5 samples in window => exclude."""
    lo = asof_ms - DD_WINDOW_DAYS * MS_DAY
    excl = set()
    diag = {}
    e = equity_daily[(equity_daily["day_ms"] >= lo) & (equity_daily["day_ms"] <= asof_ms)
                     & (equity_daily["equity"] > 0)]
    by_w = dict(tuple(e.groupby("wallet")))
    for w in wallets:
        g = by_w.get(w)
        if g is None or len(g) < DD_MIN_SAMPLES:
            excl.add(w)
            diag[w] = {"reason": "insufficient_equity_history",
                       "n_samples": 0 if g is None else int(len(g))}
            continue
        a = g.sort_values("day_ms")["equity"].to_numpy()
        peak = np.maximum.accumulate(a)
        dd = float(((peak - a) / peak).max() * 100)
        ret = float((a[-1] / a[0] - 1) * 100)
        if (dd >= DD_HARD) or (DD_SOFT_LO <= dd < DD_HARD and ret <= DD_SOFT_RET):
            excl.add(w)
            diag[w] = {"reason": "near_ruin_dd", "dd_pct": dd, "ret_pct": ret}
    return excl, diag


def score_r1(journeys: pd.DataFrame, gates: pd.DataFrame, entity_map: dict,
             equity_daily: pd.DataFrame, scenario: ExecScenario, asof_ms: int,
             top_n: int = TOP_N_ENTITIES) -> tuple:
    """R1 scoring over the ELIGIBLE pool. journeys: closed train journeys (exit_ts < asof)
    of eligible wallets, with net_realized_pnl. Returns (roster, scored, diagnostics)."""
    diag = {}
    j = journeys[journeys["max_notional"] > 10].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_notional"]
    j = j[j["ret"].between(-1.0, 2.0)].copy()
    rt = {c: scenario.rt_cost(c) for c in j["coin"].unique()}
    j["net_ret"] = j["ret"] - j["coin"].map(rt)
    if j.empty:
        return pd.DataFrame(), pd.DataFrame(), {"reason": "no_journeys"}

    g = j.groupby("wallet")
    s = g.agg(n=("net_ret", "size"), mean=("net_ret", "mean"), std=("net_ret", "std"),
              win=("net_ret", lambda x: float((x > 0).mean())),
              hold=("duration_h", "median"))
    s["sharpe"] = s["mean"] / (s["std"] + 1e-9)
    s["maxdd"] = g["net_ret"].apply(lambda x: max_dd(x.to_numpy()))
    s = s[(s["n"] >= MIN_J) & (s["hold"] >= HOLD_MIN_H) & (s["hold"] <= HOLD_MAX_H)].copy()
    diag["n_after_sample_hold"] = int(len(s))
    if s.empty:
        return pd.DataFrame(), s, diag

    cons = weekly_consistency(j[j["wallet"].isin(s.index)])
    s["consistency"] = s.index.map(cons).fillna(0.0)
    mart = martingale_flags(j[j["wallet"].isin(s.index)])
    s["martingale"] = s.index.map(mart).fillna(False)
    diag["n_martingale_vetoed"] = int(s["martingale"].sum())
    s = s[~s["martingale"]].copy()
    if s.empty:
        return pd.DataFrame(), s, diag
    s["skill"] = z(s["win"]) + z(s["sharpe"]) + z(-s["maxdd"]) + z(s["consistency"])

    # DD gate over ALL score-eligible wallets (frozen; no candidate-pool multiplier)
    excl, dd_diag = causal_dd_exclude(equity_daily, list(s.index), asof_ms)
    diag["n_dd_excluded"] = len(excl)
    diag["dd_fail_closed"] = sum(1 for v in dd_diag.values()
                                 if v.get("reason") == "insufficient_equity_history")
    s = s[~s.index.isin(excl)].copy()

    # deterministic ties: skill desc, then wallet lexicographic asc (frozen)
    s = s.iloc[np.lexsort((s.index.to_numpy(), -s["skill"].to_numpy()))]
    roster_rows = []
    seen_entities = set()
    for w, r in s.iterrows():
        ent = entity_map.get(w, w)
        if ent in seen_entities:
            continue
        seen_entities.add(ent)
        roster_rows.append({"wallet": w, "entity": ent, "rank": len(roster_rows) + 1,
                            "score": float(r["skill"]), "n_journeys": int(r["n"]),
                            "win": float(r["win"]), "sharpe": float(r["sharpe"])})
        if len(roster_rows) >= top_n:
            break
    return pd.DataFrame(roster_rows), s, diag
