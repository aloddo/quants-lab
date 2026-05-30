#!/usr/bin/env python3
"""V13 Module 05 — Execution Realism.

Per spec: projects/quant/v13/modules/05-execution-realism

Defines the SINGLE simulator path used by:
- Module 04 (per-wallet copy ranking sim)
- Module 08 (aggregate portfolio sim)
- Live shadow engine (same code)

Core entry point: `execute_or_skip(coin, copy_exec_ts, delta_usd, mark_at_exec, ledger, coin_info)`
Returns: `ExecutionResult(executed, qty_before, qty_after, leg, reject_reason)`

Rejection reasons:
- 'ineligible_tier' — thin/illiquid (v0 excludes from copy pool)
- 'ioc_cap'         — proposed slip would breach MAX_SLIPPAGE_BPS
- 'min_order'       — leg notional < coin's min_order_usd
- 'partial_zero'    — after partial fill rate, qty rounds to 0
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


def _is_finite_numeric(x) -> bool:
    """Safe finite-check that rejects bool, accepts int/float, catches OverflowError
    on huge Python ints. Mirrors Module 07 _is_valid_finite helper.
    """
    if isinstance(x, bool):
        return False
    if not isinstance(x, (int, float)):
        return False
    try:
        return math.isfinite(float(x))
    except (OverflowError, ValueError):
        return False


# === Constants (Module 05 spec) ===
MAX_SLIPPAGE_BPS = 15
FEE_TAKER_PER_LEG_PCT = 0.000432   # 4.32 bps HL taker × 4% referral

# v0 slip tiers (Module 05 spec v0 starter)
SLIP_TIERS = {
    "tier_thresholds_usd": {"majors": 5_000_000, "liquid": 500_000, "mid": 100_000, "thin": 10_000},
    "tier_slip_bps":       {"majors": 2, "liquid": 5, "mid": 12, "thin": 30, "illiquid": None},
    "tier_fill_rate":      {"majors": 0.95, "liquid": 0.80, "mid": 0.60, "thin": 0.40},
    "pool_eligible_tiers": {"majors", "liquid", "mid"},
    "pinned_majors":       {"BTC", "ETH", "SOL", "HYPE"},
}


def classify_coin_tier(coin: str, volume_30d_median_1m_usd: float) -> str:
    """Per Module 05 spec: BTC/ETH/SOL/HYPE always majors; else volume-bucketed.

    codex m05 r1 fix: case-normalize coin name for pinned-majors check.
    """
    coin_upper = coin.upper() if isinstance(coin, str) else coin
    if coin_upper in SLIP_TIERS["pinned_majors"]:
        return "majors"
    if not isinstance(volume_30d_median_1m_usd, (int, float)) or volume_30d_median_1m_usd <= 0:
        return "illiquid"
    if volume_30d_median_1m_usd > 5_000_000:
        return "majors"
    if volume_30d_median_1m_usd > 500_000:
        return "liquid"
    if volume_30d_median_1m_usd > 100_000:
        return "mid"
    if volume_30d_median_1m_usd > 10_000:
        return "thin"
    return "illiquid"


@dataclass
class CoinInfo:
    coin: str
    tier: str
    tick_size: float
    qty_step: float
    min_order_usd: float


@dataclass
class Leg:
    exec_ts: int
    coin: str
    side: int               # +1 buy, -1 sell
    qty: float              # unsigned magnitude
    executable_px: float    # what we filled at (slip baked in)
    mark_at_exec: float     # for slip_attribution diagnostic
    fee_usd: float
    cashflow_usd: float     # -side × qty × executable_px
    slip_attribution_usd: float  # diagnostic ONLY; NOT subtracted from net_pnl


@dataclass
class ExecutionResult:
    executed: bool
    qty_before: float
    qty_after: float
    leg: Optional[Leg]
    reject_reason: Optional[str]


def _rejected(qty_before: float, reason: str) -> ExecutionResult:
    return ExecutionResult(
        executed=False, qty_before=qty_before, qty_after=qty_before,
        leg=None, reject_reason=reason,
    )


def execute_or_skip(coin: str, copy_exec_ts: int, delta_usd: float, mark_at_exec: float,
                    ledger, coin_info: CoinInfo) -> ExecutionResult:
    """Module 05 spec entry point. Mutates `ledger` via `on_leg_executed` ON SUCCESS ONLY.

    Args:
        coin: coin symbol
        copy_exec_ts: our fill timestamp (poll_ts + latency_s)
        delta_usd: signed target USD change (+ = buy, - = sell)
        mark_at_exec: candle close at copy_exec_ts (caller provides)
        ledger: Module 07 CopyLedger (or backward-compat wrapper)
        coin_info: CoinInfo for this coin
    """
    qty_before = ledger.position_qty.get(coin, 0.0)

    # codex m05 r1+r4 fix: validate delta_usd finite using safe helper (catches huge int OverflowError).
    if not _is_finite_numeric(delta_usd):
        return _rejected(qty_before, "invalid_delta")
    # Side
    if delta_usd == 0:
        return _rejected(qty_before, "zero_delta")
    side = +1 if delta_usd > 0 else -1

    # codex m05 r1+r2+r4 fix: validate coin_info metadata via safe helper.
    if not _is_finite_numeric(coin_info.qty_step) or coin_info.qty_step <= 0:
        return _rejected(qty_before, "invalid_coin_info_qty_step")
    if not _is_finite_numeric(coin_info.min_order_usd) or coin_info.min_order_usd < 0:
        return _rejected(qty_before, "invalid_coin_info_min_order")

    # Tier eligibility
    slip_bps = SLIP_TIERS["tier_slip_bps"].get(coin_info.tier)
    if slip_bps is None:
        return _rejected(qty_before, "ineligible_tier")
    if coin_info.tier not in SLIP_TIERS["pool_eligible_tiers"]:
        return _rejected(qty_before, "ineligible_tier")

    # Mark sanity (codex m05 r3+r4 fix: bool rejection + safe huge-int handling)
    if not _is_finite_numeric(mark_at_exec) or mark_at_exec <= 0:
        return _rejected(qty_before, "invalid_mark")

    # Proposed executable price with adverse slippage
    proposed_executable_px = mark_at_exec * (1 + side * slip_bps / 10000)

    # Side-aware IOC cap: REJECT not clamp
    cap_px = mark_at_exec * (1 + side * MAX_SLIPPAGE_BPS / 10000)
    if side > 0 and proposed_executable_px > cap_px:
        return _rejected(qty_before, "ioc_cap")
    if side < 0 and proposed_executable_px < cap_px:
        return _rejected(qty_before, "ioc_cap")

    executable_px = proposed_executable_px  # DO NOT clamp

    # Lot rounding (round DOWN to qty_step)
    qty_raw = abs(delta_usd) / executable_px
    qty_rounded = math.floor(qty_raw / coin_info.qty_step) * coin_info.qty_step
    leg_notional = qty_rounded * executable_px
    if leg_notional < coin_info.min_order_usd:
        return _rejected(qty_before, "min_order")

    # Partial fill (post-round)
    tier_rate = SLIP_TIERS["tier_fill_rate"].get(coin_info.tier, 0.5)
    actual_qty = math.floor((qty_rounded * tier_rate) / coin_info.qty_step) * coin_info.qty_step
    if actual_qty <= 0:
        return _rejected(qty_before, "partial_zero")

    # Sign-correct cashflow + fee + slip attribution
    cashflow_usd = -side * actual_qty * executable_px
    fee_usd = actual_qty * executable_px * FEE_TAKER_PER_LEG_PCT
    slip_attribution_usd = (executable_px - mark_at_exec) * side * actual_qty

    # Execute via Module 07 ledger (validated mutation)
    ledger.on_leg_executed(coin, side, actual_qty, executable_px, fee_usd)
    qty_after = ledger.position_qty.get(coin, 0.0)

    leg = Leg(
        exec_ts=copy_exec_ts, coin=coin, side=side, qty=actual_qty,
        executable_px=executable_px, mark_at_exec=mark_at_exec,
        fee_usd=fee_usd, cashflow_usd=cashflow_usd,
        slip_attribution_usd=slip_attribution_usd,
    )
    return ExecutionResult(executed=True, qty_before=qty_before, qty_after=qty_after, leg=leg, reject_reason=None)
