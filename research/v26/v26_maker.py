#!/usr/bin/env python3
"""v26 CLOSE-ONLY structural maker fill model (amendment codex #9, frozen).

Deterministic, seedless, structural (cross/no-cross) -- calibrated on nothing from test
windows. No partial fills, no queue model, no intrabar logic.

ENTRY (maker_entry / maker_both):
- Post price = the mark at signal+2s (the SAME v25 repriced entry mark). No post mark
  within the v25 repricing window => maker_no_post (journey not copied).
- Timeout 60s from the post timestamp. Fill rule: BUY fills iff a 1m mark CLOSE <= post
  occurs STRICTLY AFTER the post time and within the timeout (SELL: close >= post).
  Fill timestamp = that mark's close time; fill price = the post price (no slippage --
  adverse selection is embedded by construction: we only fill when price comes TO us).
- No cross within the timeout => journey NOT copied (maker_no_cross); its exits ignored.
- Leader exit signal before our entry fill => order cancelled, journey not copied
  (maker_cancelled). All three miss modes count into the maker_missed fill-rate.

EXIT (maker_both): identical mechanics on the exit side (exiting a long posts a SELL:
fills iff close >= post; exiting a short posts a BUY). On timeout: fall back to TAKER
priced by the standard v25 repricing chain anchored at the TIMEOUT EXPIRY timestamp
(fee = taker; counted maker_exit_fallback). If no post mark exists at all, the taker
fallback anchors at the original exit signal (counted maker_exit_fallback).

Fill-rate consequence (frozen): any config with pooled maker entry fill rate < 50% is
additionally evaluated with ALL missed fills as taker; both are reported and the
criteria use the worse (decision D9, orchestrated in v26_run_grid).
"""
from __future__ import annotations

import numpy as np

from v26_common import (MAKER_TIMEOUT_MS, MS_MIN, MarksIndex, taker_entry_fill,
                        taker_exit_fill)


def _closes_between(marks: MarksIndex, coin: str, t0_excl: int, t1_incl: int,
                    cap_ms: int | None = None):
    """(close_ts, close) arrays for 1m closes in (t0_excl, t1_incl], optionally capped
    strictly below cap_ms (half-open window isolation)."""
    mins, closes = marks._series(coin)
    if mins.size == 0:
        return np.empty(0, "int64"), np.empty(0, "float64")
    ct = np.asarray(mins, dtype="float64") + MS_MIN
    lo = int(np.searchsorted(ct, float(t0_excl), side="right"))
    hi = int(np.searchsorted(ct, float(t1_incl), side="right"))
    ts = ct[lo:hi].astype("int64")
    px = np.asarray(closes, dtype="float64")[lo:hi]
    if cap_ms is not None:
        keep = ts < cap_ms
        ts, px = ts[keep], px[keep]
    valid = np.isfinite(px) & (px > 0)
    return ts[valid], px[valid]


def maker_entry(marks: MarksIndex, coin: str, signal_ts: int, side: int, end_ms: int,
                mirror_ts: float | None = None) -> dict:
    """Maker entry attempt. side: +1 our BUY (copying a long), -1 our SELL.
    Returns {filled, reason, post_ts, post_px, fill_ts, fill_px}."""
    post_ts, post_px = taker_entry_fill(marks, coin, signal_ts, end_ms)
    if post_ts is None:
        return {"filled": False, "reason": "maker_no_post", "post_ts": None,
                "post_px": None, "fill_ts": None, "fill_px": None}
    ts, px = _closes_between(marks, coin, post_ts, post_ts + MAKER_TIMEOUT_MS,
                             cap_ms=end_ms)
    cross = (px <= post_px) if side > 0 else (px >= post_px)
    hit = np.nonzero(cross)[0]
    if hit.size == 0:
        return {"filled": False, "reason": "maker_no_cross", "post_ts": int(post_ts),
                "post_px": float(post_px), "fill_ts": None, "fill_px": None}
    fill_ts = int(ts[hit[0]])
    if mirror_ts is not None and mirror_ts == mirror_ts and mirror_ts < fill_ts:
        # leader exited before our resting order filled: cancelled, journey not copied
        return {"filled": False, "reason": "maker_cancelled", "post_ts": int(post_ts),
                "post_px": float(post_px), "fill_ts": None, "fill_px": None}
    return {"filled": True, "reason": "", "post_ts": int(post_ts),
            "post_px": float(post_px), "fill_ts": fill_ts, "fill_px": float(post_px)}


def maker_exit(marks: MarksIndex, coin: str, anchor_ts: int, lot_side: int,
               end_ms: int) -> dict:
    """Maker exit attempt anchored at the exit trigger. Exiting a long (lot_side +1)
    posts a SELL (fills iff close >= post); exiting a short posts a BUY.
    Returns {fill_ts, fill_px_mark, is_maker, fallback, late} or
    {fill_ts: None} (terminal MTM settles it). fill_px_mark is the MARK to price at:
    maker fills price AT the mark (post) with no slippage; taker fallback marks get the
    scenario exit slippage applied by the caller."""
    post_ts, post_px = taker_entry_fill(marks, coin, anchor_ts, end_ms)
    if post_ts is None:
        # no post possible: straight taker fallback anchored at the original signal
        fill_ts, mark, late = taker_exit_fill(marks, coin, anchor_ts, end_ms)
        return {"fill_ts": fill_ts, "fill_px_mark": mark, "is_maker": False,
                "fallback": True, "late": late}
    ts, px = _closes_between(marks, coin, post_ts, post_ts + MAKER_TIMEOUT_MS,
                             cap_ms=end_ms)
    cross = (px >= post_px) if lot_side > 0 else (px <= post_px)
    hit = np.nonzero(cross)[0]
    if hit.size:
        return {"fill_ts": int(ts[hit[0]]), "fill_px_mark": float(post_px),
                "is_maker": True, "fallback": False, "late": False}
    # timeout: taker via the v25 repricing chain anchored at the TIMEOUT EXPIRY
    expiry = int(post_ts) + MAKER_TIMEOUT_MS
    fill_ts, mark, late = taker_exit_fill(marks, coin, expiry, end_ms)
    return {"fill_ts": fill_ts, "fill_px_mark": mark, "is_maker": False,
            "fallback": True, "late": late}
