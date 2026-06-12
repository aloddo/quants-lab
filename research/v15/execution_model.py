#!/usr/bin/env python3
"""V15 CANONICAL EXECUTION MODEL -- the SINGLE source of truth for execution realism.

BINDING (2026-06-10, Alberto): every V15 backtest / copy-sim / shadow MUST price fills through THIS
module. No script may hardcode its own slippage / fee / latency. The recurring "is 7bps too harsh"
fight happened precisely because each sim invented its own numbers. This module ends that: change the
assumption HERE, once, and every consumer moves together.

Three components, each grounded in MEASURED data, not guesses:

1. SLIPPAGE (cross-spread cost, ONE WAY, fraction of price)
   Source: our own L2 order-book snapshots, app/data/v15/l2_calib_10coin.json
     slip_oneway(coin) = (half_spread_bps + impact_at_size_bps) / 1e4
   Measured majors (one-way): BTC 0.13bps, ETH 0.47bps, SOL 0.12bps, ... (vs the old flat 7bps guess).
   Our copy size is $10-300 (<< $10k) so half_spread + impact_10k is a CONSERVATIVE one-way estimate.
   Uncalibrated tail: a single liquidity-class default (DEFAULT_SLIP_BPS, tune via set_slip_default_bps).
   LIMITATION: per-coin liquidity-class mapping for the tail is not yet wired (l2_calib covers 10
   majors; v11 per-coin calib is keyed by HL #N codes, not tickers). Consumers should report the
   calibrated-coin share and run a default sensitivity. TODO: build ticker->ADV->class map.

2. FEES (round-trip, fraction)
   Source: real HL userFees, app/data/v15/hl_fee_schedule.json
     taker one-way 4.5bps, w/ 4% referral discount -> 4.32bps -> RT 8.64bps
     maker one-way 1.5bps -> RT 3.0bps
   fee_rt(maker=False).

3. LATENCY (ms from leader fill to our fill)
   Default LATENCY_MS below. With 1-minute marks this rarely changes the entry bar, but it is here so
   it is explicit and measurable. set_latency_ms() to override from a real WS-feed measurement
   (leader-fill ts vs our-order-accepted ts). TODO: wire the measured value from live fills.

USAGE
    from execution_model import slip_oneway, fee_rt, LATENCY_MS, apply_entry, apply_exit
    entry_px = apply_entry(coin, mark, is_long)          # crosses the spread to enter
    exit_px  = apply_exit(coin, mark, is_long)           # crosses the spread to exit
    net = gross_return(entry_px, exit_px, is_long) - fee_rt()
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger("execution_model")

_DATA = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
L2_CALIB = _DATA / "l2_calib_10coin.json"
FEE_SCHEDULE = _DATA / "hl_fee_schedule.json"

# ---- tunables (the ONLY place these live) --------------------------------------------------- #
DEFAULT_SLIP_BPS = 4.7          # one-way slippage for coins absent from l2_calib (~midcap class comp)
LATENCY_MS = 1000               # leader-fill -> our-fill; TODO replace with measured WS value

# ---- internal caches ----------------------------------------------------------------------- #
_SLIP_ONEWAY: dict[str, float] = {}        # coin -> one-way slippage (fraction)
_BASE_CALIB_COINS: set[str] = set()        # coins loaded from the committed l2_calib (the base 10);
                                           # register_slip_oneway must NOT silently overwrite these
_FEES: dict[str, float] | None = None
_HITS = {"calib": 0, "default": 0}


def _load_slip():
    if _SLIP_ONEWAY:
        return
    try:
        l2 = json.load(open(L2_CALIB))
        for c, v in l2.items():
            hs = float(v.get("half_spread_bps") or 0.0)
            imp = float(v.get("impact_10k_bps") or v.get("impact_50k_bps") or 0.0)
            _SLIP_ONEWAY[c] = (hs + imp) / 10_000.0
            _BASE_CALIB_COINS.add(c)
        logger.info(f"[exec] loaded L2 slippage for {len(_SLIP_ONEWAY)} coins from {L2_CALIB.name}")
    except Exception as e:
        logger.warning(f"[exec] l2_calib load failed ({e}); flat default {DEFAULT_SLIP_BPS}bps one-way")


def _load_fees():
    global _FEES
    if _FEES is not None:
        return
    try:
        f = json.load(open(FEE_SCHEDULE))
        taker_ow = float(f["base_taker_oneway"]) * (1 - float(f.get("referral_discount", 0.0)))
        maker_ow = float(f["base_maker_oneway"]) * (1 - float(f.get("referral_discount", 0.0)))
        _FEES = {"taker_rt": taker_ow * 2, "maker_rt": maker_ow * 2}
        logger.info(f"[exec] fees from {FEE_SCHEDULE.name}: taker_rt={_FEES['taker_rt']*1e4:.2f}bps "
                    f"maker_rt={_FEES['maker_rt']*1e4:.2f}bps")
    except Exception as e:
        _FEES = {"taker_rt": 0.000864, "maker_rt": 0.00030}     # documented HL fallback
        logger.warning(f"[exec] fee schedule load failed ({e}); fallback taker_rt=8.64bps")


def set_slip_default_bps(bps: float):
    """Override the uncalibrated-coin one-way slippage (sensitivity analysis)."""
    global DEFAULT_SLIP_BPS
    DEFAULT_SLIP_BPS = bps


def register_slip_oneway(coin: str, oneway_frac: float, allow_override: bool = False):
    """ADDITIVE (agent H, 2026-06-12, V17 universe-expansion backtest): merge a per-coin one-way
    slippage (fraction of price) into _SLIP_ONEWAY WITHOUT changing any existing coin's value or the
    10-coin l2_calib load path. Used to admit expansion coins (crypto + xyz perps) into the canonical
    execution model so the expanded backtest pays a MEASURED per-coin cost rather than the flat default.

    `oneway_frac` is the TOTAL one-way cost as a fraction (e.g. agentC impact_1k_bps/1e4, which already
    includes the half-spread -- do NOT double-add it here). No-op-safe: ensures the base calib is loaded
    first so this merges on top rather than being clobbered by a later lazy _load_slip().

    CALIB-OVERRIDE GUARD (agent J, 2026-06-12, codex V17 go-live req #1): a coin that is part of the
    committed base 10-coin l2_calib must NOT be silently overwritten by a registration call. With
    allow_override=False (default) such a call is REJECTED (the committed value stands) and a warning is
    logged -- a fat-fingered expansion mapping cannot quietly redefine BTC/ETH/etc. slippage and void the
    cancellation against the skill benchmark. Pass allow_override=True to intentionally replace a base
    value (logged loudly). Registering a brand-new (expansion) coin, or re-registering a previously
    registered EXTRA coin, is unaffected."""
    _load_slip()                       # ensure base 10-coin calib present so we merge, not get clobbered
    if coin in _BASE_CALIB_COINS and not allow_override:
        cur = _SLIP_ONEWAY.get(coin)
        logger.warning(
            f"[exec] register_slip_oneway IGNORED for base-calib coin {coin}: refusing to overwrite the "
            f"committed l2_calib value ({(cur or 0)*1e4:.2f}bps one-way) with {oneway_frac*1e4:.2f}bps "
            f"(pass allow_override=True to force). Base 10-coin slippage is immutable by default.")
        return
    if coin in _BASE_CALIB_COINS and allow_override:
        logger.warning(
            f"[exec] register_slip_oneway OVERRIDE base-calib coin {coin}: "
            f"{(_SLIP_ONEWAY.get(coin) or 0)*1e4:.2f} -> {oneway_frac*1e4:.2f}bps one-way (allow_override=True)")
    _SLIP_ONEWAY[coin] = float(oneway_frac)


def load_extra_calib(mapping: dict[str, float], allow_override: bool = False):
    """Bulk register_slip_oneway: {coin: oneway_frac}. ADDITIVE, same semantics + base-calib override
    guard as register_slip_oneway (agent J, codex req #1)."""
    for c, ow in mapping.items():
        register_slip_oneway(c, ow, allow_override=allow_override)


def set_latency_ms(ms: int):
    global LATENCY_MS
    LATENCY_MS = ms


def slip_oneway(coin: str) -> float:
    """One-way cross-spread slippage (fraction) for a taker order on `coin` at our size."""
    _load_slip()
    v = _SLIP_ONEWAY.get(coin)
    if v is None:
        _HITS["default"] += 1
        return DEFAULT_SLIP_BPS / 10_000.0
    _HITS["calib"] += 1
    return v


def fee_rt(maker: bool = False) -> float:
    """Round-trip fee (fraction). Taker by default (copy needs speed = crossing)."""
    _load_fees()
    return _FEES["maker_rt"] if maker else _FEES["taker_rt"]


def apply_entry(coin: str, mark: float, is_long: bool, realistic: bool = True) -> float:
    """Entry fill price: cross the spread (buy at ask, sell at bid)."""
    s = slip_oneway(coin) if realistic else 0.0
    return mark * (1 + s) if is_long else mark * (1 - s)


def apply_exit(coin: str, mark: float, is_long: bool, realistic: bool = True) -> float:
    """Exit fill price: cross the spread (sell a long into bid, buy back a short at ask)."""
    s = slip_oneway(coin) if realistic else 0.0
    return mark * (1 - s) if is_long else mark * (1 + s)


def gross_return(entry_px: float, exit_px: float, is_long: bool) -> float:
    return (exit_px - entry_px) / entry_px if is_long else (entry_px - exit_px) / entry_px


def calibrated_share() -> tuple[float, int, int]:
    """(% of slip lookups that hit measured calibration, n_calib, n_default). Report this per run so a
    result that rides mostly on the uncalibrated default is flagged (codex)."""
    tot = _HITS["calib"] + _HITS["default"]
    return ((_HITS["calib"] / tot * 100) if tot else 0.0, _HITS["calib"], _HITS["default"])


def reset_hits():
    _HITS["calib"] = 0
    _HITS["default"] = 0
