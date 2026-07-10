#!/usr/bin/env python3
"""V15 M02 — Causal fill lifecycles with optional equity enrichment.

Converts each source wallet's Hyperliquid PERP fills into:
  (1) PRIMARY: an equity-independent per-ACTION stream (one row per ordered
      fill event) suitable for lifecycle and fixed-notional research.
  (2) DERIVED: a per-JOURNEY summary (groupby journey_id) driving ranking (M04).

M01 is NOT a prerequisite for the default M02 path.  ``--equity-enrichment``
is an optional lane that adds causal whole-account equity-% sizing targets for
strategies that explicitly require leader-equity-relative sizing.

This is the V15 port of research/v13/v13_journey_trace.py. It REUSES V13's proven
ENTRY/ADDON/TRIM/EXIT/REVERSE state machine and its realized-PnL / fee / funding /
cost-basis accounting VERBATIM in spirit, and REPLACES exactly two things that made
V13 look-ahead-contaminated:

  (A) EQUITY BASIS. V13 sizes ``max_position_pct_equity`` by a single present-day
      scalar (``spot_usdc_today``) — a look-ahead defect. V15 sizes EVERY action by
      ``source_equity_post`` from M01's per-event causal equity series:
          target_exposure_pct = position_after * mark / source_equity_post
      where source_equity_post uses ONLY events with event-order <= k (POST-fill,
      consistent with position_after). The equity basis is chosen per-event from a
      4-mode HYBRID (INTRAWEEK / PARTIAL_MTM / ANCHOR_FALLBACK / NO_ANCHOR) decided
      ONLY from data <= k — never from per-segment drift or whole-window recon-PASS.

  (B) CARRY-IN. V13 back-walks carry-in from CURRENT position state (look-ahead).
      V15 uses the causal ``startPosition`` field on the FIRST in-window fill per
      coin (HL reports the position held JUST BEFORE each fill = known at that fill's
      time). carry_in_status SEEDED / UNKNOWN.

NON-LOOK-AHEAD INVARIANTS (asserted in tests/v15/test_m02.py):
  anchor_ts < ts (strict) ; equity_ts <= ts ; mark_ts <= ts ;
  event_order strictly monotone per wallet ; NO_ANCHOR ⇒ target_exposure_pct null.

CLI:
    python v15_m02_journey_trace.py --wallets-file W.txt \
        --start 2025-12-01 --end 2026-05-23 \
        --actions-out actions.parquet --journeys-out journeys.parquet [--procs N]
        [--equity-enrichment]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# Import M01 (same dir) — reuse loaders, cash deltas, marks, and the additive
# per-event equity bridge. NO M01 reconstruction MATH is changed here.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_m01_equity_reconstruct as m01  # noqa: E402
from _streaming_io import ShardedParquetWriter, install_memory_guard, plan_memory_budget  # noqa: E402  MANDATORY (decisions/2026-05-31-mandatory-streaming-io)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("v15_m02")

EPS = 1e-9

# Journey classification thresholds (inherited from V13).
SCALP_THRESHOLD_S = 30 * 60
SWING_THRESHOLD_S = 24 * 60 * 60

# Equity-basis hybrid tunables (spec). STALE_CAP = one weekly anchor cycle.
STALE_CAP_MS = m01.STALE_CAP_MS
# Force ANCHOR_FALLBACK when the frozen unmarkable component exceeds this
# fraction of equity (material unmarkable exposure) OR is staler than STALE_CAP.
FROZEN_MATERIALITY_CAP = 0.10
# Causal equity-floor (codex/subagent gap 2026-05-31): a reconstructed source_equity_post
# that is <= 0 or implausibly small vs the last EXACT past anchor is a degenerate sizing
# denominator (produces absurd target_exposure_pct, e.g. 200000x on near-zero equity). This
# is a per-event, LOOK-AHEAD-FREE red flag (uses only equity_post(k) + the past anchor at
# anchor_ts < ts). When tripped, fall back to the last exact anchor equity (sane denominator)
# or mark unsizeable if the anchor is also degenerate.
EQUITY_FLOOR_ABS = 10.0    # below HL min-order scale -> sizing meaningless
EQUITY_FLOOR_FRAC = 0.02   # < 2% of the last exact anchor -> reconstruction lost the plot


# --------------------------------------------------------------------------- #
# Equity-basis mode selection — causal, per-event (codex r1 Q1/Q2/Q3/Q4)
# --------------------------------------------------------------------------- #


@dataclass
class EquityBasis:
    mode: str                 # INTRAWEEK | PARTIAL_MTM | ANCHOR_FALLBACK | NO_ANCHOR
    equity: Optional[float]   # source_equity_post (None for NO_ANCHOR)
    equity_ts: Optional[int]
    anchor_ts: Optional[int]
    mark_ts: Optional[int]
    degraded: bool
    frozen_value: float
    frozen_age_s: float
    frozen_materiality_frac: float


@dataclass
class LifecycleFillEvent:
    """Minimal M02 event contract; deliberately contains no leader equity."""

    ts: int
    event_order: int
    fill: dict
    type: str = "fill"


def select_equity_basis(ee: "m01.EventEquity") -> EquityBasis:
    """Pick the causal equity basis for one event, using ONLY data <= the event.

    Per-segment drift and whole-window recon-PASS are NEVER consulted (look-ahead).
    """
    if not ee.has_past_anchor:
        return EquityBasis(
            mode="NO_ANCHOR", equity=None, equity_ts=None,
            anchor_ts=None, mark_ts=None, degraded=True,
            frozen_value=0.0, frozen_age_s=0.0, frozen_materiality_frac=float("nan"),
        )

    eq = ee.equity_post
    frozen = ee.frozen_component_value
    frozen_age_ms = ee.frozen_component_age_ms
    materiality = (abs(frozen) / abs(eq)) if (eq and abs(eq) > EPS) else float("nan")
    age = ee.age_since_anchor_ms or 0
    anchor_eq = ee.anchor_equity

    # EQUITY-FLOOR (causal): a non-positive or implausibly-small reconstructed equity is a
    # degenerate denominator -> never emit a huge target off it. Uses only equity_post(k)
    # and the last PAST exact anchor (anchor_ts < ts), so no look-ahead. Falls back to the
    # exact anchor equity when sane, else marks the event unsizeable.
    eq_floor = EQUITY_FLOOR_ABS
    if anchor_eq is not None and anchor_eq == anchor_eq:
        eq_floor = max(EQUITY_FLOOR_ABS, EQUITY_FLOOR_FRAC * abs(anchor_eq))
    if eq is None or eq != eq or eq <= eq_floor:
        if anchor_eq is not None and anchor_eq == anchor_eq and anchor_eq > EQUITY_FLOOR_ABS:
            return EquityBasis(
                mode="ANCHOR_FALLBACK", equity=anchor_eq, equity_ts=ee.anchor_ts,
                anchor_ts=ee.anchor_ts, mark_ts=ee.mark_ts, degraded=True,
                frozen_value=frozen, frozen_age_s=frozen_age_ms / 1000.0,
                frozen_materiality_frac=materiality,
            )
        return EquityBasis(
            mode="NO_ANCHOR", equity=None, equity_ts=None, anchor_ts=ee.anchor_ts,
            mark_ts=ee.mark_ts, degraded=True, frozen_value=frozen,
            frozen_age_s=frozen_age_ms / 1000.0, frozen_materiality_frac=materiality,
        )

    # ANCHOR_FALLBACK: base anchor too stale AND material unmarkable exposure, OR
    # the frozen component is itself stale/material. Hold last PAST anchor flat.
    stale_anchor = age > STALE_CAP_MS
    material_frozen = (materiality == materiality and materiality > FROZEN_MATERIALITY_CAP)
    stale_frozen = frozen_age_ms > STALE_CAP_MS
    if (stale_anchor and ee.n_unmarkable > 0) or material_frozen or stale_frozen:
        return EquityBasis(
            mode="ANCHOR_FALLBACK", equity=ee.anchor_equity,
            equity_ts=ee.anchor_ts, anchor_ts=ee.anchor_ts, mark_ts=ee.mark_ts,
            degraded=True, frozen_value=frozen,
            frozen_age_s=frozen_age_ms / 1000.0,
            frozen_materiality_frac=materiality,
        )

    if ee.markable_all and ee.no_extradex_without_anchor:
        # INTRAWEEK: reconstruction fully reliable at this event.
        return EquityBasis(
            mode="INTRAWEEK", equity=eq, equity_ts=ee.ts,
            anchor_ts=ee.anchor_ts, mark_ts=ee.mark_ts, degraded=False,
            frozen_value=0.0, frozen_age_s=0.0, frozen_materiality_frac=0.0,
        )

    # PARTIAL_MTM: some positions unmarkable → equity_post already marks the
    # markable book + cash/funding/realized and FREEZES the unmarkable component
    # at its last known value (handled in M01). Flag degraded.
    return EquityBasis(
        mode="PARTIAL_MTM", equity=eq, equity_ts=ee.ts,
        anchor_ts=ee.anchor_ts, mark_ts=ee.mark_ts, degraded=True,
        frozen_value=frozen, frozen_age_s=frozen_age_ms / 1000.0,
        frozen_materiality_frac=materiality,
    )


# --------------------------------------------------------------------------- #
# Fee / funding helpers (ported from V13; actual v2-fills fees preferred)
# --------------------------------------------------------------------------- #

SOURCE_ASSUMED_TAKER_FEE_BPS = 4.32
FEE_RATE = SOURCE_ASSUMED_TAKER_FEE_BPS / 10000.0


def _fill_fee_usd_actual(f: dict) -> Optional[float]:
    """Return the signed total wallet fee reported by Hyperliquid.

    ``builderFee`` and ``deployerFee`` are component disclosures already
    included in ``fee``. They are deliberately ignored here.
    """
    v = f.get("fee")
    if v is None:
        return None
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return None
    # codex m02 r3: reject NaN AND inf (a non-finite reported fee must fall back to the computed fee, never
    # flow inf into fees_paid). np.isfinite catches both.
    return fv if np.isfinite(fv) else None


def _fill_fee_usd(notional: float) -> float:
    return abs(notional) * FEE_RATE


def _classify_fee_scope(dir_val: str, coin: str) -> str:
    if isinstance(coin, str) and coin.startswith("xyz:"):
        return "builder_perp"
    if ":" in coin:
        return "builder_perp"
    if dir_val in ("Buy", "Sell"):
        return "spot"
    if dir_val in (
        "Open Long", "Close Long", "Open Short", "Close Short",
        "Long > Short", "Short > Long",
    ):
        return "standard_perp"
    return "other"


def _funding_for_interval(
    funding: list[dict], coin: str, entry_ts: int, exit_ts: int
) -> float:
    """Sum funding usdc for (coin, (entry_ts, exit_ts]) half-open (codex r16 #6)."""
    if not funding or not coin:
        return 0.0
    total = 0.0
    for e in funding:
        d = e.get("delta", {})
        if d.get("type") != "funding" or str(d.get("coin", "")) != coin:
            continue
        t = int(e.get("time", 0))
        if entry_ts < t <= exit_ts:
            # codex m02 r4: finite-guard funding usdc so an inf/NaN payload cannot make funding_net (and
            # net_realized_pnl) non-finite in a trusted journey. Non-finite funding is dropped (garbage).
            try:
                u = float(d.get("usdc", 0) or 0)
            except (TypeError, ValueError):
                continue
            if np.isfinite(u):
                total += u
    return total


def _classify_duration(duration_s: float, n_addon: int, n_trim: int) -> str:
    if duration_s < SCALP_THRESHOLD_S:
        return "scalp"
    if duration_s < SWING_THRESHOLD_S:
        return "swing"
    if n_trim == 0 and n_addon == 0:
        return "fast-flip"
    if n_trim == 0:
        return "accumulation"
    if n_addon == 0:
        return "scale-out"
    return "position"


# --------------------------------------------------------------------------- #
# Per-wallet causal journey tracer (per-action stream + derived journeys)
# --------------------------------------------------------------------------- #


def trace_wallet(
    wallet: str,
    events: list["m01.EventEquity"],
    fills: list[dict],
    funding: list[dict],
    *,
    equity_enriched: bool = True,
    end_ms: Optional[int] = None,
) -> tuple[list[dict], list[dict]]:
    """Walk the ordered FILL events for one wallet and emit (actions, journeys).

    In the default/core lane, ``events`` contains only ``LifecycleFillEvent``
    rows and no M01 reconstruction is consulted.  In the optional enriched
    lane it is M01's total event stream so each fill can receive its causal
    source-equity target. Funding/ledger rows never emit actions.
    """
    # Fill events in `events` carry type=='fill' AND the EXACT fill payload on
    # ee.fill (codex r4 BUG 2). We consume that payload DIRECTLY off the ordered
    # stream — never rematching by (ts, tid), which collided for same-ms fills
    # lacking a tid (S3 by-wallet partition has none) and silently overwrote
    # duplicates. Funding/ledger advance equity but emit no action rows.
    funding_payloads = funding

    actions: list[dict] = []
    journeys: list[dict] = []

    # Per-coin state machines (independent books per coin).
    coin_state: dict[str, dict] = {}
    coin_journey_counter: dict[str, int] = {}
    coin_seen_first_fill: dict[str, bool] = {}

    def _new_state() -> dict:
        return {
            "position": 0.0, "cost_basis": 0.0, "open": False, "side": 0,
            "journey_id": None, "open_ts": None, "peak_ts": None,
            "max_notional": 0.0, "n_entry": 0, "n_addon": 0, "n_trim": 0,
            "n_exit": 0, "n_reverse": 0, "n_carry_in": 0,
            "realized_pnl": 0.0, "fees_paid": 0.0, "fee_scope": "standard_perp",
            "carry_in_status": None, "liq_closed": False,
            "lifecycle_valid": True, "state_discontinuity": False,
        }

    def _finalize(coin: str, st: dict, close_ts: int) -> Optional[dict]:
        if not st["open"] or st["open_ts"] is None:
            return None
        duration_s = max(0, (close_ts - st["open_ts"]) / 1000)
        max_notional = max(st["max_notional"], EPS)
        funding_net = _funding_for_interval(funding_payloads, coin, st["open_ts"], close_ts)
        net_realized = st["realized_pnl"] - st["fees_paid"] + funding_net
        return {
            "wallet": wallet, "coin": coin, "journey_id": st["journey_id"],
            "side": "long" if st["side"] > 0 else "short",
            "entry_ts": st["open_ts"], "exit_ts": close_ts, "peak_ts": st["peak_ts"],
            "duration_h": duration_s / 3600.0,
            "n_entry_fills": st["n_entry"], "n_addon_fills": st["n_addon"],
            "n_trim_fills": st["n_trim"], "n_exit_fills": st["n_exit"],
            "n_reverse_fills": st["n_reverse"], "n_carry_in_seeds": st["n_carry_in"],
            "max_position_notional": max_notional,
            "realized_pnl": st["realized_pnl"], "fees": st["fees_paid"],
            "funding_net": funding_net, "net_realized_pnl": net_realized,
            "journey_class": _classify_duration(duration_s, st["n_addon"], st["n_trim"]),
            "liq_closed": st["liq_closed"],
            "open_at_window_end": False,
            "carry_in_status": st["carry_in_status"] or "SEEDED",
            "lifecycle_valid": bool(st.get("lifecycle_valid", True)),
            "state_discontinuity": bool(st.get("state_discontinuity", False)),
        }

    # Walk events in event_order (already sorted).
    for ee in events:
        if ee.type != "fill":
            continue
        f = ee.fill  # exact payload for THIS ordered fill event (no rematch)
        if f is None:
            continue

        coin = f["coin"]
        # Defensive parity with M1 and M7/live: the research strategy copies
        # perps only. Spot must not affect journey/ranking metrics even if an
        # externally supplied event stream contains it.
        if m01.coin_is_spot(coin):
            continue
        st = coin_state.setdefault(coin, _new_state())
        signed = f["signed_sz"]
        if abs(signed) <= EPS:
            continue
        price = float(f["price"]) or 0.0
        ts = ee.ts
        is_liq = bool(f.get("is_liquidation"))
        row_scope = _classify_fee_scope(str(f.get("dir", "") or ""), coin)
        actual_fee = _fill_fee_usd_actual(f)

        position = st["position"]
        cost_basis = st["cost_basis"]

        # CAUSAL CARRY-IN: first fill for a coin reveals startPosition (pre-fill).
        first_fill = not coin_seen_first_fill.get(coin, False)
        sp = float(f.get("startPosition", 0.0) or 0.0)
        # AUDIT 2026-07-10 (codex m02 P1): a NON-FINITE startPosition/size/price must NOT fabricate a clean
        # lifecycle. A NaN startPosition made `abs(sp) > EPS` False -> seeded FLAT SEEDED with lifecycle_valid
        # True (fabricated pre-position). Fail CLOSED: non-finite inputs invalidate the lifecycle so downstream
        # (stream_replay_valid) excludes them; a non-finite carry-in is UNKNOWN, never a clean flat seed.
        sp_finite = bool(np.isfinite(sp))
        signed_finite = bool(np.isfinite(signed))
        price_finite = bool(np.isfinite(price))
        inputs_finite = sp_finite and signed_finite and price_finite
        if not inputs_finite:
            st["lifecycle_valid"] = False
            st["state_discontinuity"] = True
            if not sp_finite:
                sp = 0.0   # neutralize the denominator math; carry_in tagged UNKNOWN below (not SEEDED)
            if not (signed_finite and price_finite):
                # codex r2/r3: a NaN/inf SIZE poisons new_pos (crashes REVERSE via coin_journey_counter
                # KeyError); a NaN/inf PRICE poisons notional/cost_basis/fee arithmetic (inf into fees_paid /
                # max_notional). DROP the garbage fill; the coin lifecycle stays invalid so every journey on
                # it is stream_replay_valid=False (fail closed). Only a lone non-finite startPosition still
                # emits an invalid audit row (sp neutralized to 0, carry_in UNKNOWN).
                continue
        state_resynced = False
        if not first_fill and not np.isclose(position, sp, rtol=1e-9, atol=1e-9):
            # Missing/ambiguous fills: never continue a fabricated lifecycle.
            # Close the interrupted journey as INVALID, then causally reseed
            # from the current fill's authoritative pre-position.
            if st["open"]:
                st["lifecycle_valid"] = False
                st["state_discontinuity"] = True
                j = _finalize(coin, st, ts)
                if j is not None:
                    journeys.append(j)
            st = _new_state()
            coin_state[coin] = st
            position = 0.0
            cost_basis = 0.0
            first_fill = True
            state_resynced = True
        if first_fill:
            coin_seen_first_fill[coin] = True
            if abs(sp) > EPS:
                # Open an already-in-progress journey, sized causally.
                coin_journey_counter[coin] = coin_journey_counter.get(coin, 0) + 1
                st.update(
                    position=sp, cost_basis=price,  # best causal cost proxy = first observed px
                    open=True, side=(1 if sp > 0 else -1),
                    journey_id=coin_journey_counter[coin], open_ts=ts, peak_ts=ts,
                    max_notional=abs(sp) * price, n_carry_in=1,
                    carry_in_status="SEEDED", fee_scope=row_scope,
                )
                position = sp
                cost_basis = price
            else:
                # flat carry-in = cleanly known -- UNLESS startPosition was non-finite (unknown pre-position).
                st["carry_in_status"] = "SEEDED" if sp_finite else "UNKNOWN"

        if not bool(f.get("causal_order_ok", True)):
            st["lifecycle_valid"] = False

        # closedPnl ground truth (fallback to cost-basis derived).
        # codex m02 r3: a reported closedPnl that is NaN OR inf must NOT flow into realized_pnl. Treat any
        # non-finite reported pnl as MISSING (fall back to the cost-basis-derived value, always finite here
        # since price is finite) AND invalidate the lifecycle (corrupt ground-truth -> exclude downstream).
        # codex m02 r4: coerce BEFORE the finite check so a string "inf"/"nan" (direct callers; the production
        # loader already normalizes) is caught, not just numeric non-finite.
        raw_pnl = f.get("closedPnl", None)
        if raw_pnl is not None:
            try:
                raw_pnl = float(raw_pnl)
            except (TypeError, ValueError):
                raw_pnl = None   # unparseable -> treat as missing (derived path)
        _raw_pnl_nonfinite = raw_pnl is not None and not np.isfinite(raw_pnl)
        if _raw_pnl_nonfinite:
            st["lifecycle_valid"] = False
        new_pos_preview = position + signed
        if raw_pnl is None or _raw_pnl_nonfinite:
            if (position > 0 and signed < 0) or (position < 0 and signed > 0):
                if abs(new_pos_preview) < EPS:
                    closed_qty = position
                elif (position > 0 and new_pos_preview > 0) or (position < 0 and new_pos_preview < 0):
                    closed_qty = -signed
                else:
                    closed_qty = position
                closed_pnl = float((price - cost_basis) * closed_qty)
            else:
                closed_pnl = 0.0
        else:
            closed_pnl = float(raw_pnl)

        # PRE-fill peak capture.
        pre_notional = abs(position) * price
        if abs(position) > EPS and pre_notional > st["max_notional"]:
            st["max_notional"] = pre_notional
            st["peak_ts"] = ts

        new_pos = position + signed
        notional_after = abs(new_pos) * price
        fill_notional = abs(signed) * price
        fill_fee = actual_fee if actual_fee is not None else _fill_fee_usd(fill_notional)

        # ---- state machine ----
        action_type = None
        opening_jid = None
        closing_jid = None

        if abs(position) < EPS and abs(new_pos) > EPS:
            # ENTRY
            coin_journey_counter[coin] = coin_journey_counter.get(coin, 0) + 1
            st.update(
                journey_id=coin_journey_counter[coin], open=True,
                side=(1 if new_pos > 0 else -1), open_ts=ts, peak_ts=ts,
                n_entry=1, n_addon=0, n_trim=0, n_exit=0, n_reverse=0, n_carry_in=0,
                realized_pnl=0.0, max_notional=notional_after, cost_basis=price,
                fees_paid=fill_fee, fee_scope=row_scope, liq_closed=False,
                carry_in_status=st["carry_in_status"] or "SEEDED",
            )
            action_type = "ENTRY"
            opening_jid = st["journey_id"]
            closing_jid = None

        elif (position > 0 and signed > 0) or (position < 0 and signed < 0):
            # ADDON
            st["n_addon"] += 1
            st["cost_basis"] = (cost_basis * abs(position) + price * abs(signed)) / abs(new_pos)
            if notional_after > st["max_notional"]:
                st["max_notional"] = notional_after
                st["peak_ts"] = ts
            st["fees_paid"] += fill_fee
            action_type = "ADDON"
            opening_jid = st["journey_id"]
            closing_jid = st["journey_id"]

        elif abs(new_pos) < EPS:
            # EXACT EXIT
            st["n_exit"] += 1
            st["realized_pnl"] += closed_pnl
            st["fees_paid"] += fill_fee
            if is_liq:
                st["liq_closed"] = True
            action_type = "EXIT"
            opening_jid = st["journey_id"]
            closing_jid = st["journey_id"]
            j = _finalize(coin, st, ts)
            if j is not None:
                journeys.append(j)
            st.update(_new_state())
            coin_state[coin] = st

        elif (position > 0 and new_pos > 0 and signed < 0) or (position < 0 and new_pos < 0 and signed > 0):
            # TRIM
            st["n_trim"] += 1
            st["realized_pnl"] += closed_pnl
            st["fees_paid"] += fill_fee
            action_type = "TRIM"
            opening_jid = st["journey_id"]
            closing_jid = st["journey_id"]

        else:
            # REVERSE (crosses zero): one action row, two journey ids.
            st["n_reverse"] += 1
            st["realized_pnl"] += closed_pnl
            if is_liq:
                st["liq_closed"] = True
            split_total = abs(position) + abs(new_pos)
            if actual_fee is not None and split_total > EPS:
                closing_fee = actual_fee * (abs(position) / split_total)
                opening_fee = actual_fee * (abs(new_pos) / split_total)
            else:
                closing_fee = abs(position) * price * FEE_RATE
                opening_fee = abs(new_pos) * price * FEE_RATE
            st["fees_paid"] += closing_fee
            closing_jid = st["journey_id"]
            j = _finalize(coin, st, ts)
            if j is not None:
                journeys.append(j)
            # open the new (reversed) leg
            coin_journey_counter[coin] += 1
            st.update(_new_state())
            st.update(
                journey_id=coin_journey_counter[coin], open=True,
                side=(1 if new_pos > 0 else -1), open_ts=ts, peak_ts=ts,
                n_reverse=1, max_notional=abs(new_pos) * price, cost_basis=price,
                fees_paid=opening_fee, fee_scope=row_scope,
                carry_in_status="SEEDED",
            )
            coin_state[coin] = st
            action_type = "REVERSE"
            opening_jid = st["journey_id"]

        st["position"] = new_pos

        # ---- optional equity basis + action row ----
        # Core M02 deliberately does not read M01, anchors, or market marks.
        if equity_enriched:
            basis = select_equity_basis(ee)
            # SIZING signal: use only a bar that has closed by ts.
            _raw_mark = m01.get_mark(coin, ts, causal=True)
            # AUDIT 2026-07-10 (codex m02 P1): a NaN/inf mark (or NaN/inf equity) must NOT survive into
            # target_exposure_pct -- `mark is not None` alone let NaN/inf through and produced a NaN/inf target.
            # Require FINITE mark AND finite non-trivial equity, else the action is unsizeable (target=None).
            mark_ok = _raw_mark is not None and np.isfinite(_raw_mark)
            mark = float(_raw_mark) if mark_ok else None
            sizing_mark_ts = (ts // 60_000) * 60_000 - 60_000 if mark_ok else None
            eq_finite = basis.equity is not None and np.isfinite(basis.equity)
            eq_ok = eq_finite and abs(basis.equity) > EPS
            if basis.mode == "NO_ANCHOR" or not eq_ok or not mark_ok:
                target_pct = None
            else:
                target_pct = (new_pos * mark) / basis.equity
        else:
            basis = EquityBasis(
                mode="NOT_REQUESTED", equity=None, equity_ts=None,
                anchor_ts=None, mark_ts=None, degraded=False,
                frozen_value=0.0, frozen_age_s=0.0,
                frozen_materiality_frac=float("nan"),
            )
            mark = None
            sizing_mark_ts = None
            target_pct = None

        # codex m02 r5: NEVER write a non-finite source_equity_post / frozen diagnostic into the action row
        # (M01 can emit inf/NaN equity via cash reconstruction). Null non-finite equity-basis fields so no
        # trusted row carries a non-finite equity value; the row stays lifecycle-valid, just unsizeable.
        source_equity_out = (float(basis.equity)
                             if (basis.equity is not None and np.isfinite(basis.equity)) else None)
        frozen_value_out = (basis.frozen_value
                            if (basis.frozen_value is not None and np.isfinite(basis.frozen_value)) else None)
        frozen_mat_out = (basis.frozen_materiality_frac
                          if (basis.frozen_materiality_frac is not None
                              and np.isfinite(basis.frozen_materiality_frac)) else None)
        frozen_age_out = (basis.frozen_age_s
                          if (basis.frozen_age_s is not None and np.isfinite(basis.frozen_age_s)) else None)

        actions.append({
            "wallet": wallet, "coin": coin, "ts": ts,
            "fill_id": int(f.get("tid", 0) or 0),
            "event_order": ee.event_order,
            "action_type": action_type,
            "signed_size": signed, "price": price,
            "position_after": new_pos,
            "mark": mark, "mark_ts": sizing_mark_ts,
            "source_equity_post": source_equity_out, "equity_ts": basis.equity_ts,
            "anchor_ts": basis.anchor_ts,
            "equity_basis_mode": basis.mode,
            "equity_degraded": basis.degraded,
            "frozen_component_value": frozen_value_out,
            "frozen_component_age_s": frozen_age_out,
            "frozen_materiality_frac": frozen_mat_out,
            "target_exposure_pct": target_pct,
            "is_liquidation": is_liq,
            "opening_journey_id": opening_jid,
            "closing_journey_id": closing_jid,
            "journey_id": opening_jid,  # back-compat = opening side
            "carry_in_status": st["carry_in_status"],
            "state_resynced": state_resynced,
            "causal_order_ok": bool(f.get("causal_order_ok", True)),
            "lifecycle_valid": bool(st.get("lifecycle_valid", True)),
            # Final value is recomputed after every journey has been finalized.
            # A future gap can retroactively invalidate earlier actions in the
            # interrupted journey; a raw live trade stream also cannot repair
            # the resync row because it does not carry startPosition.
            "stream_replay_valid": bool(
                st.get("lifecycle_valid", True)
                and bool(f.get("causal_order_ok", True))
                and not state_resynced
            ),
        })

    # Positions still open at window end → flag (not finalized).
    # AUDIT 2026-07-10 (codex m02 P1): finalize an OPEN journey through the WINDOW END (end_ms), not peak_ts.
    # Using peak_ts truncated duration to 0 and dropped funding accrued between peak and window end. exit_ts
    # stays None (still open). Falls back to the old peak_ts behavior only when end_ms is not supplied.
    for coin, st in coin_state.items():
        if st["open"] and st["open_ts"] is not None:
            _close_ts = end_ms if end_ms is not None else (st["peak_ts"] or st["open_ts"])
            _close_ts = max(_close_ts, st["open_ts"])   # never negative duration
            j = _finalize(coin, st, _close_ts)
            if j is not None:
                j["open_at_window_end"] = True
                j["exit_ts"] = None
                journeys.append(j)

    # Lifecycle validity is only known once the whole wallet has been walked.
    # If a later position discontinuity interrupts a journey, propagate that
    # failure back to every already-emitted action belonging to that journey.
    # Without this pass, downstream code can filter lifecycle_valid=True and
    # still ingest the beginning of a journey proven broken later.
    invalid_journeys = {
        (str(j["coin"]), int(j["journey_id"]))
        for j in journeys
        if not bool(j.get("lifecycle_valid", True)) and j.get("journey_id") is not None
    }
    invalid_stream_journeys = set(invalid_journeys)
    for action in actions:
        if action.get("state_resynced", False):
            invalid_stream_journeys.update(
                (str(action["coin"]), int(jid))
                for jid in (action.get("opening_journey_id"), action.get("closing_journey_id"))
                if jid is not None
            )
    for action in actions:
        involved = {
            (str(action["coin"]), int(jid))
            for jid in (action.get("opening_journey_id"), action.get("closing_journey_id"))
            if jid is not None
        }
        lifecycle_valid = bool(action.get("causal_order_ok", True)) and not bool(
            involved & invalid_journeys
        )
        action["lifecycle_valid"] = lifecycle_valid
        action["stream_replay_valid"] = bool(
            lifecycle_valid
            and not action.get("state_resynced", False)
            and not bool(involved & invalid_stream_journeys)
        )
    for journey in journeys:
        key = (str(journey["coin"]), int(journey["journey_id"]))
        journey["stream_replay_valid"] = bool(
            journey.get("lifecycle_valid", True)
            and key not in invalid_stream_journeys
        )

    return actions, journeys


# --------------------------------------------------------------------------- #
# Worker
# --------------------------------------------------------------------------- #

_ANCHOR_DF = None


def _init_worker(
    anchor_parquet: str, worker_mem_gb: float = 3.0,
    equity_enriched: bool = False,
) -> None:
    global _ANCHOR_DF
    _ANCHOR_DF = pd.read_parquet(anchor_parquet) if equity_enriched else None
    # codex perf-r1 #5: guard WORKERS too (per-wallet reconstruction + per-process mark-series cache
    # live here). Backstop: abort a runaway worker loudly instead of a silent aggregate OS OOM.
    install_memory_guard(soft_gb=worker_mem_gb, label=f"m02-worker-{os.getpid()}")


def process_wallet(args: tuple) -> dict:
    wallet, start_ms, end_ms, equity_enriched = args
    try:
        if equity_enriched:
            anchor = m01.load_wallet_anchor(wallet, _ANCHOR_DF)
            res = m01.reconstruct_wallet_event_equity((wallet, anchor, start_ms, end_ms))
            if "error" in res:
                return {"wallet": wallet, "error": res["error"]}
            events = res["events"]
            fills = res["fills"]
            funding = res["funding"]
            n_anchors = len(res["weekly_anchors"])
            inter_drift = res["inter_anchor_drift"]
        else:
            fills = m01.load_wallet_fills(wallet, start_ms, end_ms)
            funding = m01.load_wallet_funding(wallet, start_ms, end_ms)
            events = [
                LifecycleFillEvent(
                    ts=int(fill["time"]),
                    event_order=int(fill.get("fill_seq", i)),
                    fill=fill,
                )
                for i, fill in enumerate(fills)
            ]
            n_anchors = None
            inter_drift = None
        actions, journeys = trace_wallet(
            wallet, events, fills, funding, equity_enriched=equity_enriched, end_ms=end_ms
        )
        return {
            "wallet": wallet, "actions": actions, "journeys": journeys,
            "n_anchors": n_anchors,
            "inter_drift": inter_drift,
        }
    except Exception as e:  # noqa: BLE001
        return {"wallet": wallet, "error": f"exception:{e!r}"}


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> None:
    ap = argparse.ArgumentParser(description="V15 M02 causal journey_trace")
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-23")
    ap.add_argument("--actions-out", required=True)
    ap.add_argument("--journeys-out", required=True)
    ap.add_argument("--procs", type=int, default=4,
                    help="REQUESTED workers (ceiling; auto-capped to fit RAM by the aggregate budget).")
    ap.add_argument(
        "--per-worker-gb", type=float, default=None,
        help="Per-worker peak RSS. Default: 0.5GB for core fill lifecycles; 2.0GB "
             "for optional M01 equity reconstruction/mark caches.",
    )
    ap.add_argument(
        "--headroom-gb", type=float, default=None,
        help="RAM reserved outside M02. Default: 2GB core; 6GB equity-enriched.",
    )
    ap.add_argument("--anchor-parquet", default=str(m01.ANCHOR_PARQUET))
    ap.add_argument(
        "--equity-enrichment", action="store_true",
        help="Opt in to M01 causal equity targets. Core actions/journeys do not require M01.",
    )
    ap.add_argument("--flush-rows", type=int, default=2_000_000,
                    help="MANDATORY streaming: flush a parquet part + free RAM every N buffered rows.")
    ap.add_argument("--mem-soft-gb", type=float, default=12.0,
                    help="Memory-guard soft cap (GB); abort loud above this instead of silent OOM kill.")
    ap.add_argument("--skip-marks-cache", action="store_true",
                    help="Skip the one-time marks-cache precompute (assumes app/data/v15/marks_cache is built+fresh).")
    ap.add_argument("--rebuild-marks-cache", action="store_true",
                    help="Force full rebuild of every coin's marks cache (use after a candle backfill).")
    args = ap.parse_args()

    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    with open(args.wallets_file) as fh:
        wallets = [l.strip().lower() for l in fh if l.strip() and not l.startswith("#")]
    logger.info(f"Loaded {len(wallets):,} wallets")

    tasks = [(w, start_ms, end_ms, args.equity_enrichment) for w in wallets]
    t0 = time.time()

    # PRECOMPUTE marks cache ONCE (perf: avoids N workers each re-scanning Mongo for coin price
    # series, which left the pool ~74% I/O-idle). Freshness-checked (codex perf-r1 #3): rebuild if
    # the coin-set or latest candle changed since the cache was built.
    if args.equity_enrichment and not args.skip_marks_cache:
        tmc = time.time()
        st = m01.marks_cache_status()
        force = args.rebuild_marks_cache or not st["fresh"]
        if force:
            logger.info(f"marks cache rebuild (fresh={st['fresh']}, reason={st['reason']}, "
                        f"age_days={st['age_days']})")
            # force a full refresh when explicitly requested OR when stale due to NEW candles /
            # changed coin-set (else force=False would skip the already-present-but-stale coins).
            m01.build_marks_cache(force=True)
        else:
            logger.info(f"marks cache FRESH (age_days={st['age_days']:.1f}); reusing")
        logger.info(f"marks cache ready in {time.time()-tmc:.0f}s")

    # MANDATORY memory-safe streaming (decisions/2026-05-31-mandatory-streaming-io): never hold the
    # full result set in RAM. Stream per-wallet actions/journeys to disk in bounded chunks; memory
    # stays flat regardless of universe size. Backstop watchdog aborts loud (not silent SIGKILL).
    # AGGREGATE memory budget (2026-06-10 OOM fix): per-process guards do not compose; cap worker
    # count from ACTUAL free RAM so N x per_worker + baseline cannot blow past physical RAM.
    # --mem-soft-gb acts as the upper cap on the main-process guard.
    per_worker_gb = args.per_worker_gb
    if per_worker_gb is None:
        per_worker_gb = 2.0 if args.equity_enrichment else 0.5
    headroom_gb = args.headroom_gb
    if headroom_gb is None:
        headroom_gb = 6.0 if args.equity_enrichment else 2.0
    budget = plan_memory_budget(requested_procs=args.procs, per_worker_gb=per_worker_gb,
                                headroom_gb=headroom_gb, main_soft_cap=args.mem_soft_gb)
    install_memory_guard(soft_gb=budget.main_soft_gb, label="m02")
    Path(args.actions_out).parent.mkdir(parents=True, exist_ok=True)
    aw = ShardedParquetWriter(args.actions_out, flush_rows=args.flush_rows)
    jw = ShardedParquetWriter(args.journeys_out, flush_rows=args.flush_rows)
    errors: list[tuple] = []

    def _consume(r: dict) -> None:
        if "error" in r:
            errors.append((r["wallet"], r["error"]))
        else:
            aw.add_many(r.get("actions"))
            jw.add_many(r.get("journeys"))
        _log_one(r, len(wallets))

    # ALWAYS run via a Pool (even procs=1) with maxtasksperchild so the worker process is RECYCLED
    # periodically -> the per-coin marks cache (m01._coin_series, which grows unbounded as a long-lived
    # worker touches more of the 1830-coin universe) is RESET, keeping RSS under worker_soft_gb. Without
    # this the procs=1 in-process loop blew the 2.5G soft cap at ~153 wallets (memory-guard abort, exit
    # 137) once the marks cache grew (1830 coins to 06-24). maxtasksperchild forks a fresh worker every
    # N wallets; on macOS spawn this re-imports m01 -> empty cache. (Fix 2026-06-25, codex marks-rebuild.)
    MAXTASKS_PER_CHILD = 40
    with Pool(max(1, budget.procs), initializer=_init_worker,
              initargs=(args.anchor_parquet, budget.worker_soft_gb, args.equity_enrichment),
              maxtasksperchild=MAXTASKS_PER_CHILD) as pool:
        # AUDIT 2026-07-10 (codex m02 P2): ORDERED imap so physical output row/shard order is deterministic
        # run-to-run (same wallets file -> byte-stable parts). Rows are identical either way; determinism only.
        for r in pool.imap(process_wallet, tasks):
            _consume(r)

    n_actions = aw.close()
    n_journeys = jw.close()
    logger.info(f"Wrote {args.actions_out}: {n_actions:,} actions")
    logger.info(f"Wrote {args.journeys_out}: {n_journeys:,} journeys")
    logger.info(f"Wall: {(time.time()-t0)/60:.2f} min")
    # AUDIT 2026-07-10 (codex m02 P1): a worker exception used to be logged and swallowed -> a PARTIAL wallet
    # universe was written and the process exited 0, so downstream consumed an incomplete set as if complete.
    # Fail CLOSED: write an explicit error manifest and exit non-zero so the pipeline halts on any wallet error.
    if errors:
        manifest = str(Path(args.actions_out).with_suffix(".errors.json"))
        try:
            Path(manifest).write_text(json.dumps(
                {"n_errors": len(errors), "n_wallets": len(wallets),
                 "errors": [{"wallet": w, "error": e} for w, e in errors]}, indent=2))
        except Exception as _me:  # noqa: BLE001
            logger.error(f"could not write error manifest {manifest}: {_me!r}")
        logger.error(f"{len(errors)}/{len(wallets)} wallet ERRORS -> partial universe. manifest={manifest}. "
                     f"first: {errors[:10]}")
        raise SystemExit(1)


def _log_one(r: dict, n: int) -> None:
    if "error" in r:
        logger.warning(f"  {r['wallet'][:12]} -> {r['error']}")
    else:
        logger.info(
            f"  {r['wallet'][:12]} -> {len(r['actions'])} actions, "
            f"{len(r['journeys'])} journeys"
            + (f", {r['n_anchors']} anchors" if r.get("n_anchors") is not None else "")
        )


if __name__ == "__main__":
    main()
