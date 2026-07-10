#!/usr/bin/env python3
"""v25 common eligibility gates + entity clustering (spec: /tmp/v25_prereg_v3.md, frozen).

All gates use asof-only data (the caller passes each wallet's actions sliced to ts < asof,
half-open train window [train_start, asof)).

Gates (each failure counted; a wallet can fail several):
- activity (frozen; codex vetoed resolution 4): last perp fill <= 7 DAYS before asof
  AND >= 20 distinct active days (UTC dates with >= 1 perp action) in train
- coverage (frozen, journey-level 95%): wallet EXCLUDED if > 5% of its CLOSED train
  journeys (exit_ts <= asof; train stats use only closed journeys) are unmarkable
  (a journey is unmarkable iff ANY of its actions lacks a valid mark); zero closed
  train journeys => trivially passes (R1/R2 sample minima handle empty wallets)
- liquidation: NO is_liquidation action in train
- exit_ts <= asof: enforced structurally -- train slice is ts < asof, so every closed
  train journey has exit_ts < asof
- open-bag: open-position MTM at asof >= -10% x trailing-30d realized |PnL|;
  ZERO open positions PASS by definition (>= semantics, frozen blocker fix).
  Fail-closed: open position with no available mark at asof => EXCLUDED (counted).

Entity clustering (frozen; gate-b blocker #3): edges = pairwise wallets (i, j) where
matched / min(n_i, n_j) > 0.30; matched = STRICT ONE-TO-ONE greedy matching in time
order of same-coin same-side entry events with |dt| <= 60s -- an event matches at most
one counterpart event TOTAL (two-pointer greedy per pair; a rolling-window count that
reuses one event against several later events is forbidden). Ratio capped at 1.0.
Connected components via union-find; representative = lexicographically smallest
address. Computed per fold from data < asof ONLY, over the gate-passing wallets.
"""
from __future__ import annotations

from collections import defaultdict, deque

import numpy as np
import pandas as pd

from v25_common import (ACTIVITY_LAST_FILL_DAYS, CLUSTER_OVERLAP_FRAC, CLUSTER_WINDOW_MS,
                        COVERAGE_MAX_UNMARKABLE_FRAC, MIN_ACTIVE_DAYS, MS_DAY,
                        OPEN_BAG_FRAC, OPEN_BAG_TRAIL_DAYS, MarksIndex, build_journeys,
                        coin_is_spot)

GATE_NAMES = ["activity", "coverage", "liquidation", "open_bag", "open_bag_missing_mark",
              "no_perp_actions"]


class UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        p = self.parent.setdefault(x, x)
        while p != self.parent[p]:
            self.parent[p] = self.parent[self.parent[p]]
            p = self.parent[p]
        self.parent[x] = p
        return p

    def union(self, a: str, b: str):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # deterministic: smaller root wins (lexicographic)
            if rb < ra:
                ra, rb = rb, ra
            self.parent[rb] = ra


def wallet_gate_row(wallet: str, wdf: pd.DataFrame, asof_ms: int, marks: MarksIndex,
                    journeys: pd.DataFrame | None = None) -> dict:
    """Evaluate the common gates for ONE wallet on its train slice (wdf must be ts < asof).
    journeys: optional precomputed build_journeys(train perp slice) to avoid recomputing.
    Returns a report row with per-gate pass flags and journey stats."""
    p = wdf[~wdf["coin"].map(coin_is_spot)].sort_values("ts")
    row = {"wallet": wallet, "n_actions": int(len(wdf)), "n_perp_actions": int(len(p))}
    if p.empty:
        row.update({g: False for g in ["pass_activity", "pass_coverage", "pass_liquidation",
                                       "pass_open_bag"]})
        row.update({"fail_no_perp_actions": True, "eligible": False, "n_closed_journeys": 0,
                    "n_train_journeys": 0, "n_active_days": 0, "unmarkable_frac": 0.0,
                    "open_mtm_usd": 0.0, "trail30_abs_pnl": 0.0, "last_ts": 0,
                    "fail_open_bag_missing_mark": False})
        return row
    row["fail_no_perp_actions"] = False
    last_ts = int(p["ts"].max())
    row["last_ts"] = last_ts
    n_active_days = int(pd.Series(p["ts"].to_numpy() // MS_DAY).nunique())
    row["n_active_days"] = n_active_days
    # frozen activity gate: last fill <= 7d before asof AND >= 20 distinct active days
    row["pass_activity"] = bool(last_ts >= asof_ms - ACTIVITY_LAST_FILL_DAYS * MS_DAY
                                and n_active_days >= MIN_ACTIVE_DAYS)
    row["pass_liquidation"] = not bool(p["is_liquidation"].any())

    j = build_journeys(p) if journeys is None else journeys
    closed = j[j["exit_ts"].notna() & (j["exit_ts"] <= asof_ms)] if len(j) else j
    row["n_closed_journeys"] = int(len(closed))
    row["n_train_journeys"] = int(len(j))
    # frozen coverage gate: journey-level -- > 5% unmarkable CLOSED train journeys
    # (exit_ts <= asof; consistent with "train stats use only journeys with
    # exit_ts <= asof") => EXCLUDED; zero-closed-journey denominator passes trivially
    if len(closed):
        unm_frac = float(closed["unmarkable"].mean())
    else:
        unm_frac = 0.0
    row["unmarkable_frac"] = unm_frac
    row["pass_coverage"] = bool(unm_frac <= COVERAGE_MAX_UNMARKABLE_FRAC)

    # open-bag gate: MTM of open positions at asof vs trailing 30d realized |PnL|
    open_j = j[np.abs(j["open_size"]) > 1e-12] if len(j) else j
    trail_lo = asof_ms - OPEN_BAG_TRAIL_DAYS * MS_DAY
    trail = closed[(closed["exit_ts"] >= trail_lo)] if len(closed) else closed
    trail_abs = float(np.abs(trail["realized_pnl"]).sum()) if len(trail) else 0.0
    row["trail30_abs_pnl"] = trail_abs
    row["fail_open_bag_missing_mark"] = False
    if open_j is None or len(open_j) == 0:
        # zero open positions PASS by definition (frozen: >= semantics)
        row["open_mtm_usd"] = 0.0
        row["pass_open_bag"] = True
    else:
        mtm = 0.0
        missing = False
        for _, r in open_j.iterrows():
            m = marks.asof_mark(r["coin"], asof_ms)
            if m is None:
                missing = True
                break
            mtm += r["open_size"] * (m - r["open_basis"])
        if missing:
            row["open_mtm_usd"] = float("nan")
            row["pass_open_bag"] = False
            row["fail_open_bag_missing_mark"] = True   # fail-closed on missing data
        else:
            row["open_mtm_usd"] = float(mtm)
            row["pass_open_bag"] = bool(mtm >= OPEN_BAG_FRAC * trail_abs)
    row["eligible"] = bool(row["pass_activity"] and row["pass_coverage"]
                           and row["pass_liquidation"] and row["pass_open_bag"])
    return row


def extract_entries(wallet: str, wdf: pd.DataFrame) -> list[tuple]:
    """(coin, side, ts, wallet) entry events (ENTRY + REVERSE) on perp coins, for clustering."""
    p = wdf[(~wdf["coin"].map(coin_is_spot))
            & (wdf["action_type"].isin(["ENTRY", "REVERSE"]))]
    out = []
    for coin, ts, pafter in zip(p["coin"].to_numpy(), p["ts"].to_numpy(),
                                p["position_after"].to_numpy()):
        side = 1 if pafter > 0 else (-1 if pafter < 0 else 0)
        if side == 0:
            continue
        out.append((coin, side, int(ts), wallet))
    return out


def one_to_one_matched(ts_a: np.ndarray, ts_b: np.ndarray, window_ms: int) -> int:
    """STRICT ONE-TO-ONE greedy matching in time order (gate-b blocker #3): walk both
    sorted timestamp arrays with two pointers; when the heads are within window_ms they
    match and BOTH are consumed; otherwise the earlier head advances unmatched. An event
    therefore matches at most one counterpart event total."""
    i = j = m = 0
    la, lb = len(ts_a), len(ts_b)
    while i < la and j < lb:
        d = int(ts_a[i]) - int(ts_b[j])
        if abs(d) <= window_ms:
            m += 1
            i += 1
            j += 1
        elif d < 0:
            i += 1
        else:
            j += 1
    return m


def cluster_entities(entries: pd.DataFrame, eligible_wallets: set[str],
                     overlap_frac: float = CLUSTER_OVERLAP_FRAC,
                     window_ms: int = CLUSTER_WINDOW_MS) -> tuple[dict, pd.DataFrame]:
    """Union-find entity clustering over eligible wallets from their train entry events.

    entries: DataFrame [coin, side, ts, wallet]. Edge (i, j) iff
        sum over (coin, side) of one_to_one_matched(i, j) / min(n_i, n_j) > overlap_frac
    (ratio capped at 1.0). Candidate pairs are discovered with a rolling 60s sweep, then
    the EXACT one-to-one greedy count is computed per pair per (coin, side) group.

    Returns ({wallet: entity_rep}, edges DataFrame). Representative = lexicographically
    smallest wallet in the component. Singletons map to themselves."""
    e = entries[entries["wallet"].isin(eligible_wallets)]
    n_entries = e.groupby("wallet").size().to_dict()
    pair_counts: dict[tuple, int] = defaultdict(int)
    for (_coin, _side), g in e.groupby(["coin", "side"], sort=False):
        g = g.sort_values("ts", kind="mergesort")
        ts_arr = g["ts"].to_numpy()
        w_arr = g["wallet"].to_numpy()
        # 1) rolling sweep discovers candidate pairs only (no counting here)
        cand: set[tuple] = set()
        window: deque = deque()
        for t, w in zip(ts_arr, w_arr):
            while window and t - window[0][0] > window_ms:
                window.popleft()
            for (_t2, w2) in window:
                if w2 != w:
                    cand.add((w, w2) if w < w2 else (w2, w))
            window.append((t, w))
        if not cand:
            continue
        # 2) exact one-to-one greedy count per candidate pair
        by_w = {w: np.sort(ts_arr[w_arr == w]) for w in
                {x for pair in cand for x in pair}}
        for (a, b) in cand:
            pair_counts[(a, b)] += one_to_one_matched(by_w[a], by_w[b], window_ms)
    uf = UnionFind()
    for w in eligible_wallets:
        uf.find(w)
    edge_rows = []
    for (a, b), m in sorted(pair_counts.items()):
        denom = min(n_entries.get(a, 0), n_entries.get(b, 0))
        frac = (m / denom) if denom > 0 else 0.0
        frac = min(frac, 1.0)
        is_edge = frac > overlap_frac
        edge_rows.append({"wallet_a": a, "wallet_b": b, "matched": int(m),
                          "min_entries": int(denom), "overlap_frac": float(frac),
                          "edge": bool(is_edge)})
        if is_edge:
            uf.union(a, b)
    # component -> lexicographically smallest member
    comps: dict[str, list[str]] = defaultdict(list)
    for w in eligible_wallets:
        comps[uf.find(w)].append(w)
    mapping = {}
    for members in comps.values():
        rep = min(members)
        for w in members:
            mapping[w] = rep
    return mapping, pd.DataFrame(edge_rows)


def exclusion_summary(report: pd.DataFrame) -> dict:
    """ALL exclusions counted (spec). Keys are counts of wallets failing each gate."""
    if report.empty:
        return {"n_wallets": 0, "n_eligible": 0}
    return {
        "n_wallets": int(len(report)),
        "n_eligible": int(report["eligible"].sum()),
        "fail_no_perp_actions": int(report.get("fail_no_perp_actions", pd.Series(dtype=bool)).sum()),
        "fail_activity": int((~report["pass_activity"]).sum()),
        "fail_coverage": int((~report["pass_coverage"]).sum()),
        "fail_liquidation": int((~report["pass_liquidation"]).sum()),
        "fail_open_bag": int((~report["pass_open_bag"]).sum()),
        "fail_open_bag_missing_mark": int(report["fail_open_bag_missing_mark"].sum()),
    }
