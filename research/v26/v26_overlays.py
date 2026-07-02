#!/usr/bin/env python3
"""v26 exit-overlay state machines + per-fold trip-stream precompute (codex #8, frozen).

Executable state machines, evaluated at EVERY 1m mark of a held coin (the v25 per-mark
DD clock), fills priced via the v25 repricing rule, max_hold 7d, no re-entry into the
same leader journey after any overlay exit, terminal MTM at window end for open lots.

- E1 mirror: v25 FIRST_CLOSE unchanged (plus common max_hold, decision D4).
- E2 tight-stop: R_unit = 1% of entry notional. STOP: cumulative net MTM <= -R_unit =>
  full exit. Leader FIRST_CLOSE mirror stays ACTIVE above the stop (whichever triggers
  first wins; the stop merely dominates the downside).
- E3 trail-the-tail: E2's stop, plus once cum net MTM >= +3R the leader-close mirror is
  REPLACED by a trail: exit when cum net MTM <= 0.70 x running peak since activation
  (peak inclusive of the activation mark; comparison uses <=).
- Trigger PnL (frozen): per-lot CUMULATIVE net MTM = size x (mark - entry_px) x side
  - fees_paid_to_date - accrued_exit_cost, accrued_exit_cost = current scenario TAKER
  fee + scenario slippage priced at the current mark on the remaining size.
- Priority at the same mark (frozen): STOP > TRAIL > LEADER-MIRROR > MAX-HOLD. The 1m
  mark at ts m is processed BEFORE any leader action at ts >= m (v25 advance semantics),
  which realizes STOP/TRAIL > MIRROR; MIRROR > MAXHOLD is an explicit tie-break.

PRECOMPUTE ARCHITECTURE (performance mandate): per-lot outcomes are independent of the
portfolio state and scale linearly in entry notional, so each fold precomputes ONE trip
stream per (exit_style, execution, scenario): per candidate leader journey, the entry
fill, the overlay exit reason/fill, and the price ratio -- all per $1 of entry notional.
The per-config assembly (v26_run_grid) then applies roster filtering, coalescing, caps,
sizing, dropout, and causal-tier fees without touching the marks again.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v26_common import (DUST_USD, EPS, EXIT_TRIGGER_FRAC, MAX_HOLD_MS, MS_MIN,
                        R_UNIT_FRAC, TRAIL_ACT_R, TRAIL_GIVEBACK, ExecScenario,
                        FoldMarks, MarksIndex, coin_is_spot, taker_entry_fill,
                        taker_exit_fill)
from v26_maker import maker_entry, maker_exit

TRIP_COLS = ["wallet", "coin", "journey_id", "side", "leader_notional",
             "entry_signal_ts", "entry_fill_ts", "entry_px", "entry_is_maker",
             "exit_reason", "exit_signal_ts", "exit_fill_ts", "exit_px",
             "exit_is_maker", "exit_late", "maker_exit_fallback", "status",
             "miss_reason", "mirror_ts"]


# --------------------------------------------------------------------------------------------- #
# Candidate leader journeys (cold-start, FIRST_CLOSE mirror signal extraction)
# --------------------------------------------------------------------------------------------- #
def extract_candidate_journeys(actions: pd.DataFrame, start_ms: int, end_ms: int
                               ) -> pd.DataFrame:
    """Per-journey leader events from (multi-)wallet m02 action rows, replaying the v25
    CopySim FIRST_CLOSE lifecycle WITHOUT the account: cold-start (only ENTRY actions at
    ts in [start, end) open a candidate; REVERSE never opens), ADDONs grow the trim
    denominator, cumulative reverse flow >= 85% of accumulated leader notional OR leader
    residual < $1 triggers the mirror close; a REVERSE closes unconditionally.

    Returns DataFrame [wallet, coin, journey_id, side, entry_ts, leader_notional,
    mirror_ts] with mirror_ts = NaN when the leader never closes inside the window."""
    rows: list[dict] = []
    a = actions[~actions["coin"].map(coin_is_spot)]
    a = a.sort_values(["ts", "wallet", "coin"], kind="mergesort")
    state: dict[tuple, dict] = {}          # (wallet, coin) -> open candidate
    has_cids = "closing_journey_id" in a.columns

    def _close(key, ts):
        st = state.pop(key, None)
        if st is not None:
            st["mirror_ts"] = float(ts)
            rows.append(st)

    for r in a.itertuples(index=False):
        ts = int(r.ts)
        if ts >= end_ms:
            break
        key = (r.wallet, r.coin)
        px = float(r.price) if r.price == r.price else 0.0
        notional = abs(float(r.signed_size)) * px
        after_notional = abs(float(r.position_after)) * px
        at = r.action_type
        if at == "ENTRY":
            if ts < start_ms:
                continue                    # cold-start: pre-window open never copied
            if key in state:                # desync fail-safe: emit old leg unclosed
                st = state.pop(key)
                st["mirror_ts"] = float("nan")
                rows.append(st)
            state[key] = {"wallet": r.wallet, "coin": r.coin,
                          "journey_id": int(r.journey_id),
                          "side": 1 if float(r.position_after) > 0 else -1,
                          "entry_ts": ts, "leader_notional": notional,
                          "acc": max(notional, EPS), "rev": 0.0,
                          "mirror_ts": float("nan")}
        elif at == "REVERSE":
            cjid = None
            if has_cids and r.closing_journey_id == r.closing_journey_id:
                cjid = int(r.closing_journey_id)
            st = state.get(key)
            if st is not None and (cjid is None or st["journey_id"] == cjid):
                _close(key, ts)             # reversal closes, NEVER opens (frozen)
        elif at == "ADDON":
            st = state.get(key)
            if st is not None and st["journey_id"] == int(r.journey_id):
                st["acc"] += notional       # grows the trim denominator only
        elif at in ("TRIM", "EXIT"):
            st = state.get(key)
            if st is None or st["journey_id"] != int(r.journey_id):
                continue
            st["rev"] += notional
            frac = st["rev"] / max(st["acc"], EPS)
            if frac + EPS >= EXIT_TRIGGER_FRAC or after_notional < DUST_USD:
                _close(key, ts)
    for key in sorted(state.keys()):        # leader never closed inside the window
        st = state.pop(key)
        st["mirror_ts"] = float("nan")
        rows.append(st)
    cols = ["wallet", "coin", "journey_id", "side", "entry_ts", "leader_notional",
            "acc", "rev", "mirror_ts"]
    df = pd.DataFrame(rows, columns=cols)
    return df.drop(columns=["acc", "rev"])


# --------------------------------------------------------------------------------------------- #
# Overlay exit plan (per lot, per $1 entry notional; vectorized minute scan)
# --------------------------------------------------------------------------------------------- #
def overlay_exit_plan(fm: FoldMarks, coin: str, side: int, entry_fill_ts: int,
                      entry_px: float, entry_fee_rate: float, exit_cost_rate: float,
                      mirror_ts: float, exit_style: str) -> tuple[str, int]:
    """(exit_reason, exit_signal_ts) for one lot under the frozen state machines.
    exit_cost_rate = scenario taker fee + scenario slippage (one-way fractions summed);
    accrued_exit_cost per $1 = ratio x exit_cost_rate. entry_fee_rate per $1 = the entry
    leg's actual fee fraction. reason TERMINAL => exit_signal_ts = window end."""
    end_ms = fm.end_ms
    mirror = None
    if mirror_ts == mirror_ts and mirror_ts is not None:
        mirror = int(mirror_ts)
    due = entry_fill_ts + MAX_HOLD_MS
    # first mark at/after the max-hold due time, must be readable (< end)
    mh_slot = fm.slot_ceil(due)
    maxhold_ts = None
    if mh_slot < fm.n_slots and fm.slot_ts[mh_slot] < end_ms:
        maxhold_ts = int(fm.slot_ts[mh_slot])

    if exit_style == "E1":
        # MIRROR > MAXHOLD at the same mark (frozen tie-break)
        if mirror is not None and (maxhold_ts is None or mirror <= maxhold_ts):
            return "MIRROR", mirror
        if maxhold_ts is not None:
            return "MAXHOLD", maxhold_ts
        return "TERMINAL", end_ms

    # ---- E2/E3: per-mark cumulative net MTM scan over the held window ---------------- #
    i0 = fm.slot_ceil(entry_fill_ts)
    hi_ts = maxhold_ts if maxhold_ts is not None else end_ms - 1
    i1 = min(fm.slot_of(hi_ts), fm.n_slots - 1)
    if i0 > i1:
        # no readable mark before max-hold/window end: only mirror/maxhold can fire
        if mirror is not None and (maxhold_ts is None or mirror <= maxhold_ts):
            return "MIRROR", mirror
        if maxhold_ts is not None:
            return "MAXHOLD", maxhold_ts
        return "TERMINAL", end_ms
    ts = fm.slot_ts[i0:i1 + 1]
    closes = fm.series(coin)[i0:i1 + 1]
    ratio = np.where(np.isfinite(closes), closes / entry_px, 1.0)  # v25 _lot_mark parity
    cum = (ratio - 1.0) * side - entry_fee_rate - ratio * exit_cost_rate
    R = R_UNIT_FRAC
    stop = cum <= -R

    def _first_ts(mask, lo_idx=0, ts_cap=None):
        idx = np.nonzero(mask[lo_idx:])[0]
        if idx.size == 0:
            return None
        t = int(ts[lo_idx + idx[0]])
        if ts_cap is not None and t > ts_cap:
            return None
        return t

    act_idx = None
    if exit_style == "E3":
        ai = np.nonzero(cum >= TRAIL_ACT_R * R)[0]
        if ai.size:
            act_idx = int(ai[0])
            act_ts = int(ts[act_idx])
            # activation only while the lot is open: a mirror strictly before the
            # activation mark closes first (mark at ts m processes before action at m)
            if mirror is not None and mirror < act_ts:
                act_idx = None

    if act_idx is not None:
        act_ts = int(ts[act_idx])
        # pre-activation STOP (must also precede any mirror; stop wins mark ties)
        t_stop_pre = _first_ts(stop[:act_idx], 0, None)
        if t_stop_pre is not None:
            t_stop_pre = int(ts[np.nonzero(stop[:act_idx])[0][0]])
            if mirror is None or t_stop_pre <= mirror:
                return "STOP", t_stop_pre
        # trail regime: mirror REPLACED; STOP still dominates at the same mark
        post = cum[act_idx:]
        peak = np.maximum.accumulate(post)
        hit = post <= TRAIL_GIVEBACK * peak
        hidx = np.nonzero(hit)[0]
        if hidx.size:
            j = act_idx + int(hidx[0])
            t = int(ts[j])
            return ("STOP" if cum[j] <= -R else "TRAIL"), t
        if maxhold_ts is not None:
            return "MAXHOLD", maxhold_ts
        return "TERMINAL", end_ms

    # E2 path (also E3 before/without activation): STOP vs MIRROR vs MAXHOLD
    t_stop = _first_ts(stop)
    if t_stop is not None and (mirror is None or t_stop <= mirror):
        return "STOP", t_stop
    if mirror is not None and (maxhold_ts is None or mirror <= maxhold_ts):
        return "MIRROR", mirror
    if maxhold_ts is not None:
        return "MAXHOLD", maxhold_ts
    return "TERMINAL", end_ms


# --------------------------------------------------------------------------------------------- #
# Trip-stream builder: one row per candidate journey, per $1 of entry notional
# --------------------------------------------------------------------------------------------- #
def build_trip_stream(journeys: pd.DataFrame, exit_style: str, execution: str,
                      scenario: ExecScenario, base_taker_fee, base_maker_fee,
                      marks: MarksIndex, fm: FoldMarks, start_ms: int, end_ms: int
                      ) -> pd.DataFrame:
    """Precompute entry + overlay exit + pricing for every candidate journey under one
    (exit_style, execution, scenario). base_taker_fee/base_maker_fee: callables
    coin -> base-tier fee fraction (trigger geometry, decision D5). Terminal rows carry
    exit_px = slip-adjusted end mark with reserved TAKER fee (v25 finish parity)."""
    rows = []
    entry_maker = execution in ("maker_entry", "maker_both")
    exit_maker = execution == "maker_both"
    for r in journeys.itertuples(index=False):
        side = int(r.side)
        coin = r.coin
        row = {"wallet": r.wallet, "coin": coin, "journey_id": int(r.journey_id),
               "side": side, "leader_notional": float(r.leader_notional),
               "entry_signal_ts": int(r.entry_ts), "mirror_ts": float(r.mirror_ts),
               "entry_is_maker": entry_maker, "exit_is_maker": False,
               "exit_late": False, "maker_exit_fallback": False, "miss_reason": ""}
        if float(r.leader_notional) < DUST_USD:
            row.update({"status": "miss", "miss_reason": "dust", "entry_fill_ts": -1,
                        "entry_px": np.nan, "exit_reason": "", "exit_signal_ts": -1,
                        "exit_fill_ts": -1, "exit_px": np.nan})
            rows.append(row)
            continue
        # ---- entry leg -------------------------------------------------------------- #
        if entry_maker:
            me = maker_entry(marks, coin, int(r.entry_ts), side, end_ms,
                             mirror_ts=r.mirror_ts)
            if not me["filled"]:
                row.update({"status": "miss", "miss_reason": me["reason"],
                            "entry_fill_ts": -1, "entry_px": np.nan, "exit_reason": "",
                            "exit_signal_ts": -1, "exit_fill_ts": -1,
                            "exit_px": np.nan})
                rows.append(row)
                continue
            entry_fill_ts, entry_px = me["fill_ts"], me["fill_px"]
            entry_fee_rate = base_maker_fee(coin)
        else:
            fill_ts, mark = taker_entry_fill(marks, coin, int(r.entry_ts), end_ms)
            if fill_ts is None:
                row.update({"status": "miss", "miss_reason": "entry_no_mark",
                            "entry_fill_ts": -1, "entry_px": np.nan, "exit_reason": "",
                            "exit_signal_ts": -1, "exit_fill_ts": -1,
                            "exit_px": np.nan})
                rows.append(row)
                continue
            entry_fill_ts = fill_ts
            entry_px = scenario.entry_px(coin, mark, side > 0)
            entry_fee_rate = base_taker_fee(coin)
        # ---- exit plan (overlay state machine) --------------------------------------- #
        exit_cost_rate = base_taker_fee(coin) + scenario.slip_oneway(coin)
        reason, exit_signal_ts = overlay_exit_plan(
            fm, coin, side, int(entry_fill_ts), float(entry_px), float(entry_fee_rate),
            float(exit_cost_rate), r.mirror_ts, exit_style)
        # ---- exit leg pricing --------------------------------------------------------- #
        if reason == "TERMINAL":
            m = fm.asof(coin, end_ms)
            mark = m if m == m else float(entry_px)   # fail-closed: flat mark
            exit_px = scenario.exit_px(coin, mark, side > 0)
            row.update({"status": "ok", "exit_reason": "TERMINAL",
                        "exit_signal_ts": end_ms, "exit_fill_ts": end_ms,
                        "entry_fill_ts": int(entry_fill_ts),
                        "entry_px": float(entry_px), "exit_px": float(exit_px)})
            rows.append(row)
            continue
        if exit_maker:
            mx = maker_exit(marks, coin, int(exit_signal_ts), side, end_ms)
            if mx["fill_ts"] is None:
                reason2, exit_px, fill_ts = "TERMINAL", None, end_ms
            else:
                reason2 = reason
                fill_ts = mx["fill_ts"]
                if mx["is_maker"]:
                    exit_px = float(mx["fill_px_mark"])   # at post, no slippage
                    row["exit_is_maker"] = True
                else:
                    exit_px = scenario.exit_px(coin, mx["fill_px_mark"], side > 0)
                    row["maker_exit_fallback"] = True
                    row["exit_late"] = bool(mx["late"])
        else:
            fill_ts, mark, late = taker_exit_fill(marks, coin, int(exit_signal_ts),
                                                  end_ms)
            if fill_ts is None:
                reason2, exit_px = "TERMINAL", None
            else:
                reason2 = reason
                exit_px = scenario.exit_px(coin, mark, side > 0)
                row["exit_late"] = bool(late)
        if reason2 == "TERMINAL":                      # unpriced exit -> terminal MTM
            m = fm.asof(coin, end_ms)
            mark = m if m == m else float(entry_px)
            exit_px = scenario.exit_px(coin, mark, side > 0)
            fill_ts = end_ms
            row["exit_is_maker"] = False
        row.update({"status": "ok", "exit_reason": reason2,
                    "exit_signal_ts": int(exit_signal_ts), "exit_fill_ts": int(fill_ts),
                    "entry_fill_ts": int(entry_fill_ts), "entry_px": float(entry_px),
                    "exit_px": float(exit_px)})
        rows.append(row)
    return pd.DataFrame(rows, columns=TRIP_COLS)
