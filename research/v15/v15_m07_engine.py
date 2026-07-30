#!/usr/bin/env python3
"""V15 M7 — Execution-Reality Engine.

Design: brain projects/quant/v15/modules/m07 (codex DESIGN loop r1->r5 SHIP, 2026-05-31).
Greenlit by Alberto 2026-05-31 ("Go build"). Codex CODE loop r1 NO-SHIP (12 findings) -> this
revision addresses all 12 (final-horizon risk, isolated margin+liq, 3-valued breach, causal
prior-minute marks, HL liquidation ladder, no in-engine stress-mult, runner pre-shard, versioned
fee, start_policy, purity, determinism).

WHAT THIS IS
  A deterministic, PURE-per-step LIBRARY that steps ONE Hyperliquid subaccount (one source entity)
  through its fold-frozen M2 action stream under OUR costs, with zero look-ahead, and emits our
  replica's fills, typed state-transition events, a bounded-cadence equity path, the full ending
  account state (for M9 chaining), and a counterfactual-survival verdict. Consumed by M6b / M8 / M9.

BOUNDARY (design §0/D8): M7 copies RAW source exposure on a CALLER-SET slice (start_equity). NO
  allocation / tier / portfolio-cap / stress-sizing math lives here (that is M9; M8 stress = a bigger
  caller slice). The ONLY size adjustment M7 makes is the EXECUTION-reality capacity cap (depth
  limit, recorded as unfilled tracking error), which is a fill constraint, not an allocation choice.

MANDATORY (CLAUDE.md Key Rule 8): streaming sharded output, memory guard, NO per-row DB in the inner
  loop (market/funding/liquidity/tier precomputed once into shared in-memory asof indices; the runner
  pre-shards actions by wallet so no per-seat full-file scan). Pre-run smoke with /usr/bin/time -l.
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import urllib.parse as _ulib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("m07_engine")

# --------------------------------------------------------------------------- #
# Paths / constants
# --------------------------------------------------------------------------- #
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
OHLC_CACHE_DIR = DATA_DIR / "ohlc_cache"
HL_META_DIR = DATA_DIR / "hl_meta"
FEE_SCHEDULE_PATH = DATA_DIR / "hl_fee_schedule.json"

MS_MIN = 60_000
MS_HOUR = 3_600_000

# Fee fallback (design §1.7/D11/D20). Versioned schedule (FEE_SCHEDULE_PATH) overrides; absent ->
# conservative fallback + summary flag fee_unversioned=True. ONE-WAY taker. Build-time re-confirm.
DEFAULT_TAKER_FEE_ONEWAY = 0.00045          # 4.5 bps base perps taker
HIP3_FEE_MULT_FALLBACK = 2.0                # conservative HIP-3 per-market modifier until read live

# Liquidation (HL liquidations doc; design §3.3/D12/D15)
PARTIAL_LIQ_NOTIONAL = 100_000.0            # >100k -> first liq order is 20%, then 30s cooldown
PARTIAL_LIQ_FRACTION = 0.20
LIQ_COOLDOWN_MS = 30_000
BACKSTOP_MAINT_FRACTION = 2.0 / 3.0         # below 2/3 maintenance -> backstop transfer to vault
FORCED_LIQ_SLIP_BPS = 30.0                  # forced market-liq order execution penalty (legit, not a fee)

MIN_ORDER_NOTIONAL = 10.0                   # HL ~$10 min order

# Slippage (design §4; D1/D13). Calibrated artifact overrides per bucket; absent -> conservative prior
# + summary flag slippage_uncalibrated=True.
SLIP_BANDS = {"low": 0.5, "base": 1.0, "high": 2.0}
DEFAULT_IMPACT_K_BPS = 8.0
DEFAULT_IMPACT_ALPHA = 0.5
BLOCK_MS = 14 * 86_400_000        # CHANGE 1: 14d consistency sub-split block (M6b D4 anchor cadence)
DEFAULT_HALF_SPREAD_BPS = 1.0
CAPACITY_PARTICIPATION_CAP = 0.05           # execution-reality depth cap (NOT allocation)
MARK_MAX_AGE_MS = 15 * MS_MIN               # reject stale/delisted 1m execution marks

# CHANGE B: latency adverse-drift haircut tunable (latency_model="bar_drift_v1"). Multiplies the
# fraction-of-a-1m-bar our copy latency spans by the bar's |directional move|, applied ALWAYS-adverse
# to a late taker (a BUY in an up-bar / a SELL in a down-bar fills worse). 1m-RESOLUTION APPROXIMATION
# pending real sub-minute (HL trades / L2 book) data -- see _apply_order.
LAT_DRIFT_K = 1.0

ISOLATED_ONLY_DEXES = {"xyz", "flx", "vntl", "hyna", "km", "abcd", "cash", "para"}


# --------------------------------------------------------------------------- #
# Coin / dex helpers
# --------------------------------------------------------------------------- #
def coin_dex(coin: str) -> str:
    """Margin-sharing SCOPE key (design D19): the DEX. main perps share cross within main; each HIP-3
    dex is its own scope."""
    return coin.split(":", 1)[0] if ":" in coin else "main"


def coin_is_spot(coin: str) -> bool:
    # ``#`` assets are HIP-4 outcome markets with settlement semantics, not
    # perpetuals. The historical engine is perp-only.
    return coin.startswith(("@", "#")) or "/" in coin or coin == "USDC"


def default_margin_mode(coin: str) -> str:
    """Design D3: main -> cross; HIP-3 (onlyIsolated) dexes -> isolated."""
    return "isolated" if coin_dex(coin) in ISOLATED_ONLY_DEXES else "cross"


def _f(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def build_ohlc_cache(coins: list[str], out_dir: Path = OHLC_CACHE_DIR,
                     mongo_uri: str = "mongodb://localhost:27017", force: bool = False) -> int:
    """Precompute per-coin 1m (minute, open, high, low, close, volume) arrays to .npy (page-cached, shared,
    read with mmap) so the engine inner loop reads marks/extremes with NO per-row Mongo (CLAUDE.md
    Key Rule 8). Legacy 5-row OHLC caches are rebuilt automatically because
    capacity requires dollar volume. Returns #coins written."""
    import pymongo
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    db = pymongo.MongoClient(mongo_uri)["quants_lab"]
    n = 0
    for coin in coins:
        p = out_dir / f"{_ulib.quote(coin, safe='')}.npy"
        if p.exists() and not force:
            try:
                if np.load(p, mmap_mode="r").shape[0] >= 6:
                    continue
            except Exception:
                pass
        cur = db.hyperliquid_candles.find(
            {"coin": coin, "interval": "1m"},
            projection={"timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1,
                        "volume": 1, "_id": 0},
        ).sort("timestamp_utc", 1)
        mins, o, h, lo, c, volume = [], [], [], [], [], []
        for d in cur:
            t = d.get("timestamp_utc")
            if t is None:
                continue
            mins.append(int(t)); o.append(_f(d.get("open"))); h.append(_f(d.get("high")))
            lo.append(_f(d.get("low"))); c.append(_f(d.get("close")))
            volume.append(_f(d.get("volume")))
        arr = np.vstack([np.asarray(mins, "float64"), np.asarray(o, "float64"), np.asarray(h, "float64"),
                         np.asarray(lo, "float64"), np.asarray(c, "float64"),
                         np.asarray(volume, "float64")]) if mins else np.empty((6, 0), "float64")
        tmp = p.with_name(f"{p.name}.{os.getpid()}.tmp")
        with open(tmp, "wb") as fh:
            np.save(fh, arr)
        tmp.replace(p)
        n += 1
    logger.info("build_ohlc_cache: wrote %d coin OHLC series to %s", n, out_dir)
    return n


# --------------------------------------------------------------------------- #
# HL metadata (margin tiers) — design §1.6/§3.2
# --------------------------------------------------------------------------- #
class HLMeta:
    """Per-coin maxLeverage + margin-tier table from cached HL `meta`. HL maintenance margin = HALF
    the initial margin at the tier's max leverage; the tier is the largest lowerBound <= notional
    (flat per-tier lookup, NOT marginal/incremental). maint_rate = 1/(2*tier_maxLev). Build-time
    constraint (codex DESIGN r5 #3): re-confirm vs HL margin-tiers doc; add a deduction term if HL
    publishes one."""

    def __init__(self, meta_dir: Path = HL_META_DIR):
        self.coin_maxlev: dict[str, float] = {}
        self.coin_szdec: dict[str, int] = {}
        self.coin_tiers: dict[str, list[tuple[float, float]]] = {}
        self._loaded = False
        self.meta_dir = Path(meta_dir)

    def load(self) -> "HLMeta":
        if self._loaded:
            return self
        for f in sorted(self.meta_dir.glob("meta_*.json")):
            m = json.loads(f.read_text())
            tables: dict[int, list[tuple[float, float]]] = {}
            for entry in m.get("marginTables", []):
                tid, body = entry[0], entry[1]
                tiers = sorted((float(t["lowerBound"]), float(t["maxLeverage"]))
                               for t in body.get("marginTiers", []))
                tables[int(tid)] = tiers
            for u in m.get("universe", []):
                name = u["name"]
                ml = float(u.get("maxLeverage", 3))
                self.coin_maxlev[name] = ml
                self.coin_szdec[name] = int(u.get("szDecimals", 2))
                tid = u.get("marginTableId")
                self.coin_tiers[name] = tables.get(int(tid), [(0.0, ml)]) if tid is not None else [(0.0, ml)]
        if not self.coin_maxlev:
            logger.warning("HLMeta: no cached meta in %s; conservative fallback tiers in use", self.meta_dir)
        self._loaded = True
        return self

    def tier_maxlev(self, coin: str, notional: float) -> float:
        tiers = self.coin_tiers.get(coin)
        if not tiers:
            return 3.0
        lev = tiers[0][1]
        for lb, ml in tiers:
            if notional >= lb:
                lev = ml
            else:
                break
        return lev

    def maint_rate(self, coin: str, notional: float) -> float:
        return 1.0 / (2.0 * self.tier_maxlev(coin, abs(notional)))

    def init_margin_rate(self, coin: str, notional: float) -> float:
        return 1.0 / self.tier_maxlev(coin, abs(notional))

    def max_leverage(self, coin: str) -> float:
        return self.coin_maxlev.get(coin, 3.0)

    def szdec(self, coin: str) -> int:
        return self.coin_szdec.get(coin, 4)

    def has(self, coin: str) -> bool:
        return coin in self.coin_maxlev


# --------------------------------------------------------------------------- #
# Fee schedule (versioned — design §1.7/D11/D20)
# --------------------------------------------------------------------------- #
class FeeSchedule:
    """Versioned one-way taker + per-market HIP-3 modifier. Loads FEE_SCHEDULE_PATH if present, else
    conservative fallback (flagged). Build-time: populate from the master account's userFees tier +
    HIP-3 per-market fee args."""

    def __init__(self, path: Path = FEE_SCHEDULE_PATH):
        self.versioned = False
        self.base_taker = DEFAULT_TAKER_FEE_ONEWAY
        self.hip3_mult = HIP3_FEE_MULT_FALLBACK
        self.per_market: dict[str, float] = {}
        if Path(path).exists():
            d = json.loads(Path(path).read_text())
            self.versioned = True
            self.base_taker = float(
                d.get(
                    "effective_subaccount_taker_oneway",
                    d.get("base_taker_oneway", DEFAULT_TAKER_FEE_ONEWAY),
                )
            )
            self.hip3_mult = float(d.get("hip3_mult", HIP3_FEE_MULT_FALLBACK))
            self.per_market = {k: float(v) for k, v in d.get("per_market", {}).items()}

    def taker(self, coin: str) -> float:
        if coin in self.per_market:
            return self.per_market[coin]
        if coin_dex(coin) != "main":
            return self.base_taker * self.hip3_mult
        return self.base_taker


# --------------------------------------------------------------------------- #
# Market data indices (precomputed, shared, in-memory asof) — design §1.3-§1.5/§8
# --------------------------------------------------------------------------- #
class MarketData:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017", allow_mongo: bool = True,
                 require_cache: bool = False):
        self._mongo_uri = mongo_uri
        self._allow_mongo = allow_mongo
        self._require_cache = require_cache
        self._db = None
        self._ohlc: dict[str, tuple] = {}
        self._volume: dict[str, np.ndarray] = {}
        self._funding: dict[str, tuple] = {}
        self.meta = HLMeta().load()
        self.fees = FeeSchedule()
        # D1: active per-coin slippage calibration table (set per-fold by the runner; default empty ->
        # prior + uncalibrated for every coin = the shipped behavior).
        self.slip_calib_table: dict = {}
        self.slip_calib_version: Optional[str] = None

    def set_slip_calib(self, table: Optional[dict], version: Optional[str]) -> None:
        """Install the active per-coin calib table {coin -> {base_half_spread_bps, impact_k_bps,
        impact_alpha, covered}} for the fold being stepped. None -> clears (prior-only)."""
        self.slip_calib_table = table or {}
        self.slip_calib_version = version

    def _mongo(self):
        if self._db is None:
            import pymongo
            self._db = pymongo.MongoClient(self._mongo_uri)["quants_lab"]
        return self._db

    def _ohlc_path(self, coin: str) -> Path:
        return OHLC_CACHE_DIR / f"{_ulib.quote(coin, safe='')}.npy"

    def _load_ohlc(self, coin: str) -> tuple:
        p = self._ohlc_path(coin)
        if p.exists():
            try:
                arr = np.load(p, mmap_mode="r")
                self._volume[coin] = (
                    np.asarray(arr[5], dtype="float64")
                    if arr.shape[0] >= 6 else np.empty(0, "float64")
                )
                return tuple(np.asarray(arr[i], dtype=("int64" if i == 0 else "float64")) for i in range(5))
            except Exception:
                pass
        if self._require_cache or not self._allow_mongo:
            return (np.empty(0, "int64"),) + tuple(np.empty(0, "float64") for _ in range(4))
        cur = self._mongo().hyperliquid_candles.find(
            {"coin": coin, "interval": "1m"},
            projection={"timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1,
                        "volume": 1, "_id": 0},
        ).sort("timestamp_utc", 1)
        mins, o, h, lo, c, volume = [], [], [], [], [], []
        for d in cur:
            t = d.get("timestamp_utc")
            if t is None:
                continue
            mins.append(int(t)); o.append(_f(d.get("open"))); h.append(_f(d.get("high")))
            lo.append(_f(d.get("low"))); c.append(_f(d.get("close")))
            volume.append(_f(d.get("volume")))
        self._volume[coin] = np.asarray(volume, "float64")
        return (np.asarray(mins, "int64"), np.asarray(o, "float64"), np.asarray(h, "float64"),
                np.asarray(lo, "float64"), np.asarray(c, "float64"))

    def ohlc(self, coin: str) -> tuple:
        s = self._ohlc.get(coin)
        if s is None:
            s = self._load_ohlc(coin)
            self._ohlc[coin] = s
        return s

    def volume(self, coin: str) -> np.ndarray:
        if coin not in self._ohlc:
            self.ohlc(coin)
        return self._volume.get(coin, np.empty(0, "float64"))

    def mark(self, coin: str, ts_ms: int, causal: bool = True) -> Optional[float]:
        """Close of the last bar whose CLOSE time <= ts (causal: timestamp_utc is the bar OPEN, so the
        bar closes one minute later). For a decision at ts we may only use a bar that has closed:
        bar_open <= ts - 60s. Set causal=False only for realized-price MTM where the price genuinely
        occurred. None if uncovered."""
        mins, _o, _h, _l, c = self.ohlc(coin)
        if mins.size == 0:
            return None
        key = (ts_ms // MS_MIN) * MS_MIN - (MS_MIN if causal else 0)
        i = int(np.searchsorted(mins, key, side="right")) - 1
        if i < 0:
            return None
        # An unbounded as-of lookup can reuse a hours-old/delisted candle as an
        # executable price. Fail closed when the last available bar is stale.
        if key - int(mins[i]) > MARK_MAX_AGE_MS:
            return None
        v = c[i]
        return None if v != v else float(v)

    def funding_series(self, coin: str) -> tuple:
        # NOTE (codex code-r3): funding is NOT gated by require_cache. It is loaded ONCE per coin from
        # Mongo and memoized -- a per-coin PRECOMPUTE (the runner warms it before stepping), NOT a
        # per-row inner-loop DB hit. require_cache only governs the hot OHLC/mark path.
        s = self._funding.get(coin)
        if s is not None:
            return s
        if not self._allow_mongo:
            s = (np.empty(0, "int64"), np.empty(0, "float64"))
        else:
            cur = self._mongo().hyperliquid_funding_rates.find(
                {"coin": coin}, projection={"timestamp_utc": 1, "funding_rate": 1, "_id": 0},
            ).sort("timestamp_utc", 1)
            ts, r = [], []
            for d in cur:
                t = d.get("timestamp_utc")
                if t is None:
                    continue
                ts.append(int(t)); r.append(_f(d.get("funding_rate")))
            s = (np.asarray(ts, "int64"), np.asarray(r, "float64"))
        self._funding[coin] = s
        return s

    def funding_rate_at(self, coin: str, ts_ms: int) -> float:
        ts, r = self.funding_series(coin)
        if ts.size == 0:
            return 0.0
        i = int(np.searchsorted(ts, ts_ms, side="right")) - 1
        if i < 0:
            return 0.0
        v = r[i]
        return 0.0 if v != v else float(v)

    def liquidity(self, coin: str, ts_ms: int) -> dict:
        """Strictly-trailing liquidity as of ts (only COMPLETED bars, i.e. bar_open <= ts-60s).
        Calibrated impact artifact would override k/alpha per bucket; absent -> conservative prior +
        uncalibrated flag (design D13)."""
        # D1: a calibrated coin (covered in the active fold's calib table) overrides the prior
        # half-spread + impact params and CLEARS uncalibrated. ADV/capacity stays bar-derived (V11
        # fills only calibrate the spread INTERCEPT, not ADV/the impact slope -> those stay prior).
        cal = self.slip_calib_table.get(coin)
        cal_cov = bool(cal and cal.get("covered"))
        mins, _o, h, low, c = self.ohlc(coin)
        if mins.size == 0:
            return self._liq_prior(0.0, cal, cal_cov)
        end_key = (ts_ms // MS_MIN) * MS_MIN - MS_MIN     # last completed bar
        i = int(np.searchsorted(mins, end_key, side="right"))   # exclusive upper on completed bars
        # ADV is a wall-clock daily quantity. ``i - 1440`` is only valid for a
        # dense 24/7 series; on sparse HIP-3 markets it can span many days and
        # materially overstate capacity.
        lo = int(np.searchsorted(mins, end_key - 24 * MS_HOUR, side="left"))
        if i <= lo:
            return self._liq_prior(0.0, cal, cal_cov)
        cc, hh, ll = c[lo:i], h[lo:i], low[lo:i]
        rng = np.nanmean((hh - ll) / np.where(cc == 0, np.nan, cc)) if cc.size else float("nan")
        bar_half_spread = float(np.clip((rng * 1e4) / 2.0 if rng == rng else DEFAULT_HALF_SPREAD_BPS,
                                        DEFAULT_HALF_SPREAD_BPS, 50.0))
        volume = self.volume(coin)
        if volume.size != c.size:
            return self._liq_prior(0.0, cal, cal_cov, adv_unavailable=True)
        vv = volume[lo:i]
        valid = np.isfinite(vv) & np.isfinite(cc) & (vv >= 0.0) & (cc > 0.0)
        adv = float(np.sum(vv[valid] * cc[valid])) if valid.any() else 0.0
        if not np.isfinite(adv) or adv <= 0.0:
            return self._liq_prior(0.0, cal, cal_cov, adv_unavailable=True)
        if cal_cov:
            return {"adv": adv, "half_spread_bps": float(cal["base_half_spread_bps"]),
                    "impact_k_bps": float(cal.get("impact_k_bps", DEFAULT_IMPACT_K_BPS)),
                    "impact_alpha": float(cal.get("impact_alpha", DEFAULT_IMPACT_ALPHA)),
                    "uncalibrated": False, "adv_unavailable": False}
        return {"adv": adv, "half_spread_bps": bar_half_spread, "uncalibrated": True,
                "adv_unavailable": False}

    def _liq_prior(self, adv: float, cal: Optional[dict], cal_cov: bool,
                   adv_unavailable: bool = True) -> dict:
        """No-bar fallback. CALIBRATION is a per-coin structural property (v2: every coin has a class
        comp), independent of whether trailing bars exist at this ts -> a COVERED coin still returns
        its calibrated half-spread with uncalibrated=False (adv=0 disables the bar-derived capacity/
        impact, which stays prior). Only a coin with NO calibration flags uncalibrated."""
        if cal_cov:
            return {"adv": adv, "half_spread_bps": float(cal["base_half_spread_bps"]),
                    "impact_k_bps": float(cal.get("impact_k_bps", DEFAULT_IMPACT_K_BPS)),
                    "impact_alpha": float(cal.get("impact_alpha", DEFAULT_IMPACT_ALPHA)),
                    "uncalibrated": False, "adv_unavailable": adv_unavailable}
        return {"adv": adv, "half_spread_bps": DEFAULT_HALF_SPREAD_BPS, "uncalibrated": True,
                "adv_unavailable": adv_unavailable}


# --------------------------------------------------------------------------- #
# Account state (design §1.2/§3.1) — DEX-scoped cross buckets + per-isolated buckets
# --------------------------------------------------------------------------- #
@dataclass
class Position:
    coin: str
    szi: float
    entry_px: float
    mode: str                   # "cross" | "isolated"
    leverage: float
    cum_funding: float = 0.0
    isolated_margin: float = 0.0    # collateral posted for isolated positions (cross posts 0)


@dataclass
class AccountState:
    cross_collateral: dict[str, float] = field(default_factory=dict)   # scope(dex) -> cross cash
    positions: dict[str, Position] = field(default_factory=dict)       # coin -> Position
    cooldown_until_ms: int = 0

    def cross_scopes(self) -> list[str]:
        scopes = set(self.cross_collateral) | {coin_dex(c) for c, p in self.positions.items() if p.mode == "cross"}
        return sorted(scopes)

    def equity(self, marks: dict[str, float]) -> float:
        eq = sum(self.cross_collateral.values())
        for coin in sorted(self.positions):
            p = self.positions[coin]
            m = marks.get(coin)
            upnl = (p.szi * (m - p.entry_px)) if (m is not None and m == m) else 0.0
            eq += (p.isolated_margin + upnl) if p.mode == "isolated" else upnl
        return eq


# --------------------------------------------------------------------------- #
# Engine params (caller-set; design §1.8-§1.9) — NO stress-mult (boundary; M8 stress = bigger slice)
# --------------------------------------------------------------------------- #
@dataclass
class EngineParams:
    # MEASURED LIVE 2026-07-27 from the V17 engine log (signal -> ENTRY FILLED, n=11): median 3.21s,
    # min 2.26s, MAX 3.76s. The previous 2_000 was asserted, never measured, and EVERY live fill
    # exceeded it -- so every sim run to date was optimistic about how much of a leader's move we can
    # actually capture. Kept in step with v15_m05_eligibility.P95_COPY_LATENCY_S (4.0s): if these two
    # diverge, replay stops matching runtime and the selection gate stops predicting live behaviour.
    # RE-MEASURE both when engine latency changes.
    copy_latency_ms: int = 4_000
    slippage_band: str = "base"
    adl_stress: bool = False
    start_policy: str = "future_delta_only"   # future_delta_only | causal_carry_in (design D9)
    follower_trail: Optional[float] = None     # FOLLOWER trailing exit ("exit before them"): if our copy
    # 2026-07-30: default was "leader_equity", which is BOTH an M1 remnant and a silent-zero-orders
    # trap (see _action_target_pct). Default is now the mode every real sim has actually used.
    sizing_mode: str = "fixed_position"        # fixed_position | leader_equity(DEPRECATED, refuses)
    fixed_target_exposure: float = 0.10         # signed direction gets this absolute follower exposure
    # COPY POLICY (2026-07-23, Alberto HOW: test each wallet BOTH ways):
    #   "full_mirror"  = copy every ENTRY/ADDON/TRIM/EXIT verbatim (default; byte-identical to prior engine).
    #   "entry_trail"  = mirror the ENTRY only (open one fixed-size leg), IGNORE their adds/trims/exits, and
    #                    exit each position on OUR per-position TRAILING take-profit (retrace `trail_pct` from
    #                    the position's peak favorable mark). Trailing checked at the leader's action
    #                    timestamps + fold-end (dense for quick-flip wallets); continuous/hourly check = TODO.
    copy_policy: str = "full_mirror"
    trail_pct: float = 0.15                      # entry_trail: exit on retrace >= this from peak favorable mark
    # equity draws down >= follower_trail from its running peak, FLATTEN all positions and sit out the rest
    # of the fold (independent of source). None = disabled (copy source exposure verbatim). Checked at
    # every action boundary + fold-end using REAL engine equity (fills/fees/funding/liq priced in).


def _action_target_pct(action: dict, params: EngineParams) -> float:
    """Return the declared source target under the selected sizing policy."""
    if params.sizing_mode == "leader_equity":
        # DEPRECATED (2026-07-30). Kept functional for the unit tests that exercise engine mechanics
        # (backstop, liquidation, follower-trail, min-order) through this path with a synthetic column.
        # The production trap -- an ALL-NULL column silently producing zero orders -- is caught ONCE at
        # load time by assert_sizing_input_usable(), not per action, because "all null" is only knowable
        # over the whole store. See that function for the full reasoning.
        return _f(action.get("target_exposure_pct"))
    if params.sizing_mode == "fixed_position":
        pa = _f(action.get("position_after"))
        if pa != pa:
            return float("nan")
        if abs(pa) <= 1e-12:
            return 0.0
        return float(np.sign(pa) * abs(params.fixed_target_exposure))
    raise ValueError(f"unknown sizing_mode {params.sizing_mode!r}")


# --------------------------------------------------------------------------- #
# The engine core — step_subaccount (pure function; design §0/§3-§7)
# --------------------------------------------------------------------------- #
def step_subaccount(actions: pd.DataFrame, md: MarketData, start_equity: float, params: EngineParams,
                    end_ts_ms: int, start_ts_ms: Optional[int] = None,
                    start_state: Optional[AccountState] = None,
                    entity_id=None, fold_id=None) -> dict:
    """Step ONE subaccount through its fold-frozen action stream to end_ts_ms. PURE: never mutates the
    caller's start_state (deep-copied). `start_ts_ms` anchors the risk cursor (fold start) so carried
    start_state positions accrue funding/MTM/liquidation even when there are no actions (M9 chaining).
    Returns fills/events/equity/ending_account_state/summary."""
    # Defensive contract for every caller, including M8/M9 direct calls that do
    # not pass through run_shortlist's parquet predicate.
    if not actions.empty and "stream_replay_valid" in actions.columns:
        actions = actions[actions["stream_replay_valid"].fillna(False).astype(bool)].copy()
    if not actions.empty and "lifecycle_valid" in actions.columns:
        actions = actions[actions["lifecycle_valid"].fillna(False).astype(bool)].copy()

    st = copy.deepcopy(start_state) if start_state is not None else AccountState(cross_collateral={"main": float(start_equity)})
    if not st.cross_collateral:
        st.cross_collateral = {"main": float(start_equity)}

    fills: list[dict] = []
    events: list[dict] = []
    equity_samples: list[dict] = []
    summary = _new_summary(entity_id, fold_id, start_equity, len(actions), params, md)
    summary["_fills_ref"] = fills    # internal: lets the liquidation path append market_liq_order fills

    band = SLIP_BANDS.get(params.slippage_band, 1.0)
    peak_equity = float(start_equity)
    follower_halted = [False]   # follower trailing-exit latch (list for closure mutation)

    def _flatten_all(ts):
        """GUARANTEED close-all for the breaker (codex review bug #5). Closes EVERY open position in
        full at ts via force_close (no min-notional / capacity skip). Pricing falls back causal mark ->
        realized mark -> entry_px so a momentarily-uncovered coin is still closed (never silently left
        open). Any sub-lot residual that force_close cannot book is dropped to cash at its entry value."""
        for c in list(st.positions.keys()):
            p = st.positions.get(c)
            if p is None or p.szi == 0.0:
                continue
            m = md.mark(c, ts, causal=True)
            if m is None or m != m or m <= 0:
                m = md.mark(c, ts, causal=False)
            if m is None or m != m or m <= 0:
                m = p.entry_px      # last-resort: realize 0 PnL at entry rather than leave it open
            if m is None or m != m or m <= 0:
                continue            # cannot price at all (handled by residual drop below)
            _apply_order(st, md, c, -p.szi, m, ts, {"action_type": "FOLLOWER_TRAIL_EXIT", "ts": ts},
                         params, band, fills, events, summary, force_close=True)
        # residual safety net: any position still open (unpriceable / sub-lot dust) -> drop to cash at
        # entry value so NO exposure survives the halt and equity stays well-defined.
        for c in list(st.positions.keys()):
            p = st.positions.get(c)
            if p is None:
                continue
            if p.mode == "isolated":
                st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) + p.isolated_margin
            del st.positions[c]
            _rt_close(summary, c, our_ts=ts, close_reason="trail_flatten")   # CHANGE A: residual flatten ends the round-trip

    def _trail_fire(cur_eq, ts):
        """FOLLOWER trailing-exit state machine. Updates the running peak from REAL equity at THIS
        observation point (codex bug #4/#8), then if drawdown-from-peak >= params.follower_trail: flatten
        ALL positions (guaranteed), latch halted (no un-halt this fold), record ruin if the post-flatten
        equity is non-positive (codex bug #6). Returns post-flatten equity. No-op (and does NOT touch the
        peak) when disabled -> the follower_trail=None path is byte-identical to pre-breaker."""
        nonlocal peak_equity
        if params.follower_trail is None or follower_halted[0]:
            return cur_eq
        if cur_eq > peak_equity:
            peak_equity = cur_eq
        if peak_equity <= 0 or (peak_equity - cur_eq) / peak_equity < params.follower_trail:
            return cur_eq
        _flatten_all(ts)
        follower_halted[0] = True
        post_eq = st.equity(_marks(st, md, ts))
        events.append({"ts": ts, "event_type": "follower_trail_exit", "entity_id": entity_id,
                       "fold_id": fold_id, "trail": params.follower_trail,
                       "peak_equity": float(peak_equity), "trigger_equity": float(cur_eq)})
        if post_eq <= 0 and not summary["ruin"]:
            _ruin(st, md, summary, events, ts)
        return post_eq

    # CHANGE 1: block-boundary anchors b_k = start_ts + k*14d in [start_ts, end_ts) (fold_start = b_0;
    # interior = block_boundary; the fold_end sample is the final anchor). `_emit` is a PURE-READ
    # observer threaded into _advance_between: it appends boundary equity samples ONLY (never touches
    # fills/events/summary/state) -> existing outputs byte-identical. Equity at b uses mark(b).
    if start_ts_ms is not None:
        _bstart = int(start_ts_ms)
    elif not actions.empty:
        _bstart = int(actions["ts"].min())
    else:
        _bstart = int(end_ts_ms)
    boundaries_q = []
    k = 0
    while True:
        b = _bstart + k * BLOCK_MS
        if b >= end_ts_ms:
            break
        boundaries_q.append(b)
        k += 1
    _bi = [0]

    def _emit(cursor_ts, state):
        if cursor_ts is None:
            return
        while _bi[0] < len(boundaries_q) and boundaries_q[_bi[0]] <= cursor_ts:
            b = boundaries_q[_bi[0]]; _bi[0] += 1
            eqb = state.equity(_marks(state, md, b))
            flag = "fold_start" if b == _bstart else "block_boundary"
            equity_samples.append(_eq_sample(entity_id, fold_id, b, eqb, flag, state, summary["max_dd"]))

    rows = actions.sort_values(["ts", "event_order"]).to_dict("records") if not actions.empty else []
    # AUDIT 2026-07-10 (codex P0#1): WINDOW PURITY. Defensively drop any SOURCE action outside [win_lo, end)
    # so a mis-scoped input can't pollute the metrics with test-window data (the runner filters, but M8/M9
    # call step_subaccount directly). The latency-pushed our_ts >= end guard is enforced in the loop below.
    _win_lo = int(start_ts_ms) if start_ts_ms is not None else _bstart
    rows = [r for r in rows if _win_lo <= int(r["ts"]) < int(end_ts_ms)]
    # AUDIT 2026-07-10 (codex P0#2): emit window provenance on the summary so a consumer (m06b) can PROVE the
    # row was generated for its fold's window, not a stale test run. window_end is the half-open exclusive bound.
    summary["window_start_ms"] = int(_win_lo)
    summary["window_end_ms"] = int(end_ts_ms)
    # count of COPYABLE (perp, non-spot) actions whose latency-pushed our_ts lands at/after the fold end
    # (codex P0#1 minor; spot rows are skipped by the loop so they must not inflate this reject count).
    summary["n_late_copy_skipped"] = sum(
        1 for r in rows if not coin_is_spot(r["coin"]) and int(r["ts"]) + params.copy_latency_ms >= int(end_ts_ms))
    # AUDIT 2026-07-10 (codex P0#7): adl_stress is accepted but NOT applied (n_adl_stress stays 0). A caller
    # (e.g. m08 stress_adl=True) that thinks it is getting ADL de-leverage stress is getting an unstressed run
    # -> ruin/max_dd/roe too favorable. Surface the gap LOUDLY on the summary instead of silently zeroing it,
    # until ADL is implemented or the stress model is accepted without it (Alberto decision).
    summary["adl_stress_requested"] = bool(params.adl_stress)
    summary["adl_stress_applied"] = False   # not implemented; do NOT read a survived-under-ADL claim from this run
    # DECISION 2026-07-10 (Alberto delegated -> codex ACCEPT): real ADL is NOT material beyond the existing wipe
    # signals for a pass/fail survival tier, and a faithful HL ADL needs observables m07 lacks (exchange-wide
    # liquidation shortfall, insurance/backstop state, ADL queue priority, bankruptcy prices). CONTRACT: "survives
    # stress" = survives high slippage + adverse latency drift + adverse breach marks + liquidation ladder +
    # forced-liq slippage + funding/MTM + backstop wipe; it EXCLUDES ADL/socialized deleveraging (a known, bounded
    # understatement of danger). If ever needed, implement a heuristic ADL haircut (medium change) later; do NOT
    # block the survival tier on it. Record: projects/quant/decisions/2026-07-10-m07-adl-accept.
    if params.adl_stress:
        logger.warning("m07: adl_stress=True but ADL NOT modeled (accepted 2026-07-10). Stress excludes ADL; "
                       "flagged adl_stress_applied=False. See decisions/2026-07-10-m07-adl-accept.")
    # risk cursor anchored at fold start (start_ts_ms) so start_state positions accrue risk even with
    # zero actions (codex code-r2 #2). Falls back to the first action time, then end_ts.
    if start_ts_ms is not None:
        prev_ts = int(start_ts_ms)
    elif rows:
        prev_ts = int(rows[0]["ts"]) + params.copy_latency_ms
    else:
        prev_ts = end_ts_ms

    # causal_carry_in: seed the opening position from the first action's pre-state (design D9).
    # AUDIT 2026-07-10 (codex P0#3): seed at the WINDOW START mark (prev_ts), NOT the first action's mark, so
    # the main loop's first _advance_between(start -> first action) captures any intra-window drawdown/
    # liquidation on the carried position (seeding at the first-action price skipped it = wrong max_dd/roe/ruin).
    if rows and params.start_policy == "causal_carry_in":
        _seed_carry_in(st, md, rows[0], params, summary, seed_ts=prev_ts)

    # FOLLOWER-TRAIL peak init (codex review bug #8): when carrying state into the fold, the breaker peak
    # must start from the REAL marked equity at fold start, not the caller's (possibly stale) start_equity
    # scalar -- otherwise a chained fold can fire/suppress wrongly at t0. Gated on follower_trail so the
    # disabled path keeps the original peak (max_dd byte-identical).
    if params.follower_trail is not None and start_state is not None:
        _pe = st.equity(_marks(st, md, prev_ts))
        if _pe == _pe and _pe > peak_equity:
            peak_equity = _pe

    # CHANGE 2 tracking_error state: the SOURCE signed target vector + the open interval's L1/active.
    src_target: dict = {}
    te_prev_ts = None
    te_prev_l1 = 0.0
    te_prev_active = False

    def _te_accrue(to_ts):
        """accrue the open interval [te_prev_ts, to_ts) at the held L1 (active intervals only)."""
        nonlocal te_prev_ts
        if te_prev_ts is not None and to_ts > te_prev_ts and te_prev_active:
            dt = to_ts - te_prev_ts
            summary["_te_weighted_sum"] += dt * te_prev_l1
            summary["tracking_error_active_ms"] += dt

    # entry_trail (policy b): per-position peak favorable mark, for OUR trailing-TP exit.
    trail_peak: dict = {}

    def _trail_step_all(ts):
        """Policy (b) entry_trail: update each open position's peak favorable (causal) mark and exit the
        position on OUR trailing take-profit when the mark retraces >= trail_pct from its peak. Conservative
        causal mark (prior completed bar). No-op unless copy_policy == 'entry_trail'."""
        if params.copy_policy != "entry_trail":
            return
        for c in list(st.positions.keys()):
            p = st.positions.get(c)
            if p is None or p.szi == 0.0:
                continue
            mk = md.mark(c, ts, causal=True)
            if mk is None or mk != mk or mk <= 0:
                continue
            is_long = p.szi > 0
            pk = trail_peak.get(c)
            pk = mk if pk is None else (max(pk, mk) if is_long else min(pk, mk))
            trail_peak[c] = pk
            breached = (mk <= pk * (1 - params.trail_pct)) if is_long else (mk >= pk * (1 + params.trail_pct))
            if breached:
                _apply_order(st, md, c, -p.szi, mk, ts, {"action_type": "FOLLOWER_TRAIL_TP", "ts": ts},
                             params, band, fills, events, summary, force_close=True)
                trail_peak.pop(c, None)

    for a in rows:
        coin = a["coin"]
        if coin_is_spot(coin):
            continue
        our_ts = int(a["ts"]) + params.copy_latency_ms
        # AUDIT 2026-07-10 (codex P0#1): a latency-pushed fill at our_ts >= end_ts_ms would execute on
        # post-window (test) marks/liquidity -> look-ahead. Skip the fill; the post-loop advance still marks
        # + funds + liquidates risk to end_ts_ms. rows are ts-sorted so no later action can be in-window.
        if our_ts >= int(end_ts_ms):
            break   # n_late_copy_skipped already counted upfront (rows are ts-sorted -> nothing later is in-window)

        _advance_between(st, md, prev_ts, our_ts, params, events, summary, _emit=_emit, fold_end_ms=end_ts_ms)
        if summary["ruin"]:
            break

        marks = _marks(st, md, our_ts, extra=coin)
        cur_eq = st.equity(marks)
        if cur_eq <= 0:
            _ruin(st, md, summary, events, our_ts)
            break

        # FOLLOWER trailing exit: check on the freshly-advanced equity BEFORE copying this action. If we
        # breach, flatten + latch; once halted we stop copying source entries for the rest of the fold.
        cur_eq = _trail_fire(cur_eq, our_ts)
        if summary["ruin"]:
            break
        if follower_halted[0]:
            equity_samples.append(_eq_sample(entity_id, fold_id, our_ts, cur_eq, "action", st, summary["max_dd"]))
            summary["_core_final_eq"] = cur_eq
            prev_ts = our_ts
            continue

        mark = marks.get(coin)
        if mark is None or mark != mark or mark <= 0:
            summary["metadata_uncertain"] = True
            prev_ts = our_ts
            continue

        # tgt_pct + exit_cond computed for BOTH policies (post-order tracking-error uses them).
        tgt_pct = _action_target_pct(a, params)
        exit_cond = str(a.get("action_type", "")).upper() in ("EXIT", "CLOSE") or _f(a.get("position_after")) == 0.0
        if params.copy_policy == "entry_trail":
            # POLICY (b): first run OUR trailing-TP on every open position at this cursor; then MIRROR THE
            # ENTRY ONLY (open one fixed-size leg when the leader opens from flat and we're flat). ADDON/
            # TRIM/EXIT are IGNORED (our exit is the trailing stop, not theirs).
            _trail_step_all(our_ts)
            at = str(a.get("action_type", "")).upper()
            cur_szi = st.positions[coin].szi if coin in st.positions else 0.0
            if at == "ENTRY" and abs(cur_szi) < 10 ** (-md.meta.szdec(coin)) and tgt_pct == tgt_pct:
                side = 1.0 if _f(a.get("position_after")) >= 0 else -1.0
                target_szi = side * abs(params.fixed_target_exposure) * cur_eq / mark
                _apply_order(st, md, coin, target_szi - cur_szi, mark, our_ts, a, params, band, fills, events, summary)
                trail_peak[coin] = mark
            # else: ignore their add/trim/exit — the trailing stop above owns our exit.
        else:
            if tgt_pct != tgt_pct:
                prev_ts = our_ts
                continue
            target_notional = tgt_pct * cur_eq
            target_szi = target_notional / mark
            cur_szi = st.positions[coin].szi if coin in st.positions else 0.0
            if exit_cond:
                target_szi = 0.0
            delta_szi = target_szi - cur_szi

            _apply_order(st, md, coin, delta_szi, mark, our_ts, a, params, band, fills, events, summary)

        marks = _marks(st, md, our_ts, extra=coin)
        eq = st.equity(marks)
        peak_equity = max(peak_equity, eq)
        if peak_equity > 0:
            summary["max_dd"] = max(summary["max_dd"], (peak_equity - eq) / peak_equity)
        # FOLLOWER trailing exit POST-FILL (codex review bug #3): this action just changed exposure; the
        # fill's fee/slippage/marking can itself breach the trail -> check again immediately so we never
        # stay exposed past the threshold until the NEXT action.
        eq = _trail_fire(eq, our_ts)
        if peak_equity > 0:
            summary["max_dd"] = max(summary["max_dd"], (peak_equity - eq) / peak_equity)
        equity_samples.append(_eq_sample(entity_id, fold_id, our_ts, eq, "action", st, summary["max_dd"]))
        summary["_core_final_eq"] = eq
        # CHANGE 2: close the prior interval, fold in THIS action's source target, open a new interval.
        _te_accrue(our_ts)
        src_target[coin] = (0.0 if exit_cond else tgt_pct)
        te_prev_l1, te_prev_active = _te_l1_active(st, md, src_target, our_ts)
        te_prev_ts = our_ts
        if eq <= 0 and not summary["ruin"]:
            _ruin(st, md, summary, events, our_ts)
            break
        if summary["ruin"] or follower_halted[0]:
            prev_ts = our_ts
            continue
        prev_ts = our_ts

    # FINAL-HORIZON RISK (design §1.7/codex code-r1 #1): advance funding + liquidation to fold end.
    # Runs even with zero actions so carried start_state positions are marked/funded/liquidated.
    if not summary["ruin"] and end_ts_ms > prev_ts:
        _advance_between(st, md, prev_ts, end_ts_ms, params, events, summary, _emit=_emit, fold_end_ms=end_ts_ms)
        _trail_step_all(end_ts_ms)   # policy (b): final per-position trailing-TP check at fold end (no-op unless entry_trail)
        marks = _marks(st, md, end_ts_ms)
        eq = st.equity(marks)
        peak_equity = max(peak_equity, eq)
        if peak_equity > 0:
            summary["max_dd"] = max(summary["max_dd"], (peak_equity - eq) / peak_equity)
        # FOLLOWER trailing exit at FOLD END (codex review bug #2): catches a breach from a no-action
        # fold, a single-action fold, or a post-last-action drawdown via funding/MTM/liquidation that the
        # per-action checks cannot see. Flattens so the carried state into the next chained fold is flat.
        eq = _trail_fire(eq, end_ts_ms)
        if peak_equity > 0:
            summary["max_dd"] = max(summary["max_dd"], (peak_equity - eq) / peak_equity)
        equity_samples.append(_eq_sample(entity_id, fold_id, end_ts_ms, eq, "fold_end", st, summary["max_dd"]))
        summary["_core_final_eq"] = eq

    # D2 post-ruin closeout: after ruin, _ruin() cleared positions + zeroed cash -> equity 0. Emit
    # zero-equity samples at every REMAINING block boundary + a fold_end anchor so M6b block ROE has no
    # silent gap (any block at/after ruin reads eq=0 -> -100%, counts as NON-positive, never positive).
    if summary["ruin"]:
        while _bi[0] < len(boundaries_q):
            b = boundaries_q[_bi[0]]; _bi[0] += 1
            equity_samples.append(_eq_sample(entity_id, fold_id, b, 0.0, "block_boundary", st, summary["max_dd"]))
        equity_samples.append(_eq_sample(entity_id, fold_id, end_ts_ms, 0.0, "fold_end", st, summary["max_dd"]))

    # CHANGE 2 TE finalize.
    if not summary["ruin"]:
        _te_accrue(end_ts_ms)                          # flush the open interval to fold end
    else:
        # D3: flush the pre-ruin interval to the ruin instant (our tracked exposure), then the SOURCE
        # keeps trading while our exposure is forced 0 -> full penalty = sum|source target| accrued
        # time-weighted to fold_end, advancing the source target vector read-only over later actions.
        rt = int(summary.get("time_to_ruin_ms") or end_ts_ms)
        _te_accrue(rt)
        cur = rt
        for a in rows:
            ats = int(a["ts"]) + params.copy_latency_ms
            if ats <= rt or coin_is_spot(a["coin"]):
                continue
            pen = sum(abs(v) for v in src_target.values() if v == v)
            if ats > cur and pen > 0:
                summary["_te_weighted_sum"] += (ats - cur) * pen
                summary["tracking_error_active_ms"] += (ats - cur)
            tp = _action_target_pct(a, params)
            if tp == tp:
                ex = str(a.get("action_type", "")).upper() in ("EXIT", "CLOSE") or _f(a.get("position_after")) == 0.0
                src_target[a["coin"]] = (0.0 if ex else tp)
            cur = ats
        pen = sum(abs(v) for v in src_target.values() if v == v)
        if end_ts_ms > cur and pen > 0:
            summary["_te_weighted_sum"] += (end_ts_ms - cur) * pen
            summary["tracking_error_active_ms"] += (end_ts_ms - cur)

    return _finalize(st, fills, events, equity_samples, summary, md, start_equity)


def _new_summary(entity_id, fold_id, start_equity, n_actions, params, md):
    return {
        "entity_id": entity_id, "fold_id": fold_id, "start_equity": float(start_equity),
        "n_actions": int(n_actions), "n_fills": 0, "n_rejected": 0, "n_capacity_capped": 0,
        "n_market_liq_orders": 0, "n_backstop_transfer": 0, "n_adl_stress": 0,
        "total_fees": 0.0, "total_funding": 0.0, "slip_bps_notional_sum": 0.0, "notional_traded": 0.0,
        "outcome_states": set(), "n_indeterminate_minutes": 0, "max_dd": 0.0, "time_to_ruin_ms": None,
        "slippage_band": params.slippage_band, "start_policy": params.start_policy,
        "sizing_mode": params.sizing_mode,
        "fixed_target_exposure": (
            float(params.fixed_target_exposure) if params.sizing_mode == "fixed_position" else None
        ),
        "slippage_uncalibrated": False, "metadata_uncertain": False, "mode_uncertain": False,
        "adv_unavailable": False,
        "fee_unversioned": (not md.fees.versioned), "ruin": False,
        "slippage_calibration_version": None,
        # CHANGE 2 tracking_error accumulators (finalized in _finalize): time-weighted L1 signed
        # portfolio-vector error over ACTIVE intervals + the active-time denominator.
        "_te_weighted_sum": 0.0, "tracking_error_active_ms": 0,
        # CHANGE 1: final_equity/roe_engine come from the ORIGINAL sample sites (action + non-ruin
        # fold_end) ONLY -> the new boundary/ruin-drain samples never change them (byte-identical).
        "_core_final_eq": float(start_equity),
        # CHANGE A (round-trip aggregates for M6b): realized PnL after OUR costs per CLOSED round-trip
        # (a coin's position opening from flat and returning to ~0). `_rt` holds the per-coin open
        # accumulator (realized + funding - fees) since the position last opened from flat; finalized
        # into the n_round_trips/wins/realized_pnl_total totals when the position closes. Popped in
        # _finalize. Latency cost is ALREADY priced into each fill_px (CHANGE B), so the accumulated
        # realized PnL is genuinely after fees+funding+latency.
        "_rt": {}, "n_round_trips": 0, "n_round_trip_wins": 0, "realized_pnl_total": 0.0,
        # INCREMENT 1 (v2.2 per-position emit): _positions is the internal accumulator of PER-CLOSED-
        # ROUND-TRIP records (entry/exit/side/peak_notional/realized-after-cost/r_i + intra-journey
        # add/trim + underwater-add). Underscore = never written to the one-row summary parquet; popped
        # in _finalize into the return dict as "positions" (parallel to fills/events). Aggregates
        # (n_round_trips/realized_pnl_total) are UNCHANGED -> this is a pure superset. Open-at-cutoff
        # censoring marks, continuous-mark peak/MAE, and the sharded writer are follow-up sub-diffs.
        "_positions": [],
    }


def _eq_sample(entity_id, fold_id, ts, eq, flag, st, dd):
    return {"entity_id": entity_id, "fold_id": fold_id, "ts": ts, "subaccount_equity": eq,
            "event_flag": flag, "n_open_positions": len(st.positions), "dd_from_peak": dd}


def _bar_vol_proxy(md: MarketData, coin: str, ts_ms: int) -> float:
    """CHANGE B helper (CAUSAL FIX, codex finding #3): an UNSIGNED causal volatility proxy = the
    |return| of the PRIOR COMPLETED 1m bar (bar_open <= ts - 60s), i.e. the last bar that has closed
    BEFORE our fill timestamp. Using the CONTAINING bar's (close-open) was non-causal (within-minute
    future info / look-ahead). The prior bar is fully observable before our_ts, so it removes the
    look-ahead while preserving the discriminating power: high prior-bar vol = scalper/UHFT territory
    is still penalized. This is a conservative CAUSAL VOL-PROXY APPROXIMATION of the unknown realized
    within-minute drift our late fill eats, pending real sub-minute (HL trades / L2) data. Always >= 0
    (magnitude only); the adverse SIGN is applied by the caller via the order side. NaN if the prior
    bar is uncovered/zero-priced (-> no haircut)."""
    mins, o, h, low, c = md.ohlc(coin)
    if mins.size == 0:
        return float("nan")
    key = (ts_ms // MS_MIN) * MS_MIN - MS_MIN     # the bar that CLOSED before ts (prior completed bar)
    i = int(np.searchsorted(mins, key, side="right")) - 1
    if i < 0:
        return float("nan")
    op, cl = o[i], c[i]
    if op != op or cl != cl or op == 0.0:
        # fall back to the prior bar's high-low range as the vol proxy if open/close are degenerate
        hi, lo = h[i], low[i]
        if hi != hi or lo != lo or op == 0.0:
            return float("nan")
        return abs(float((hi - lo) / op)) if op != 0.0 else float("nan")
    return abs(float((cl - op) / op))


def _marks(st: AccountState, md: MarketData, ts_ms: int, extra: Optional[str] = None) -> dict:
    coins = set(st.positions) | ({extra} if extra else set())
    return {c: md.mark(c, ts_ms) for c in sorted(coins)}


def _te_l1_active(st, md, src_target: dict, ts_ms: int) -> tuple:
    """CHANGE 2: the L1 SIGNED portfolio-vector tracking error at ts + whether the interval is ACTIVE.
    our_pct_c = signed szi_c * mark_c / equity; target_pct_c = the source's signed target exposure.
    L1 = sum_c |our_pct_c - target_pct_c| (long-vs-short mismatch ADDITIVE, no cancel). active iff
    EITHER our or target exposure is non-flat (a fully-flat interval is excluded from the TE denom).
    On equity<=0 our_pct=0 (full penalty = sum|target|)."""
    marks = _marks(st, md, ts_ms)
    eq = st.equity(marks)
    our = {}
    if eq > 1e-9:
        for c, p in st.positions.items():
            m = marks.get(c)
            if m is not None and m == m:
                our[c] = p.szi * m / eq
    tgt = {c: v for c, v in src_target.items() if v == v and v != 0.0}
    coins = set(our) | set(tgt)
    l1 = sum(abs(our.get(c, 0.0) - tgt.get(c, 0.0)) for c in coins)
    return l1, bool(coins)


def _seed_carry_in(st, md, first_row, params, summary, seed_ts=None):
    """causal_carry_in: seed an opening position to match the source's exposure that pre-exists the
    window. We back-fill the position to (position_after - signed_size), sized to OUR equity by the same
    target fraction, and mark it at the WINDOW START (seed_ts) so the pre-existing position is exposed to
    the full in-window path (codex P0#3). Conservative: only seeds if the source enters already holding."""
    status = str(first_row.get("carry_in_status", "") or "")
    if "carry" not in status.lower():
        return
    coin = first_row["coin"]
    if coin_is_spot(coin):
        return
    # AUDIT 2026-07-10 (codex P0#3): mark the carried position at the window START (seed_ts), not at the first
    # action's latency-adjusted time -- otherwise the start->first-action drawdown/liquidation is invisible.
    ref_ts = int(seed_ts) if seed_ts is not None else int(first_row["ts"]) + params.copy_latency_ms
    mark = md.mark(coin, ref_ts, causal=True)
    if mark is None or mark != mark or mark <= 0:
        return
    post_pct = _action_target_pct(first_row, params)       # SIGNED exposure% AFTER the first action
    pa = _f(first_row.get("position_after"))               # source signed size after the first action
    ss = _f(first_row.get("signed_size"))                  # the first action's own signed delta
    if post_pct == post_pct and pa == pa and ss == ss and pa != 0 and pa != ss:  # had a prior position
        eq = sum(st.cross_collateral.values())
        pre_pos = pa - ss
        if params.sizing_mode == "fixed_position":
            pre_pct = float(np.sign(pre_pos) * abs(params.fixed_target_exposure))
        else:
            # source exposure% BEFORE the first observed action = post% scaled by the pre/post size ratio.
            # target_exposure_pct is already SIGNED, so do NOT multiply by sign(pa) (that double-flips
            # shorts -- codex code-r4 #3). The main loop's first order then trades to the post target.
            pre_pct = post_pct * pre_pos / pa
        seed_szi = pre_pct * eq / mark
        if abs(seed_szi) > 0:
            mode = default_margin_mode(coin)
            pos = Position(coin, seed_szi, mark, mode, md.meta.max_leverage(coin))
            if mode == "isolated":
                # post isolated IM from main at the causal mark (codex code-r2 #6) -- never seed an
                # isolated position with 0 margin (would liquidate instantly). Reject if main can't fund.
                im = abs(seed_szi) * mark * md.meta.init_margin_rate(coin, abs(seed_szi) * mark)
                if st.cross_collateral.get("main", 0.0) < im:
                    summary["outcome_states"].add("carry_in_rejected")
                    return
                st.cross_collateral["main"] -= im
                pos.isolated_margin = im
            st.positions[coin] = pos
            summary["outcome_states"].add("carry_in_seeded")


def _apply_order(st, md, coin, delta_szi, mark, our_ts, a, params, band, fills, events, summary,
                 force_close=False):
    """Order lifecycle (design §4.4): lot round -> min-notional skip -> leverage/IM check -> capacity
    cap (execution depth, recorded) -> fill at slippage price -> versioned fee -> position+bucket
    update.

    force_close (codex follower-trail review bug #5): a RISK exit (breaker flatten) must reliably close
    the full position. It still pays realistic fee + slippage, but is NOT blocked by the min-notional
    skip and is NOT downsized by the participation/capacity cap. Exit impact is charged on the FULL
    order/ADV ratio; capping impact at the entry participation limit would make emergency exits
    optimistically cheap exactly when capacity is breached."""
    if abs(delta_szi) < 1e-15:
        return
    szdec = md.meta.szdec(coin)
    delta_szi = round(delta_szi, szdec)
    if abs(delta_szi) < 10 ** (-szdec):
        return
    delta_notional = abs(delta_szi) * mark
    if delta_notional < MIN_ORDER_NOTIONAL and not force_close:
        return

    mode = default_margin_mode(coin)
    liq = md.liquidity(coin, our_ts)
    if liq.get("adv_unavailable") and not force_close:
        summary["adv_unavailable"] = True
        summary["n_rejected"] += 1
        return
    if liq.get("uncalibrated"):
        summary["slippage_uncalibrated"] = True
    elif getattr(md, "slip_calib_version", None) is not None:
        summary["slippage_calibration_version"] = md.slip_calib_version
    adv = liq.get("adv", 0.0)
    capacity_capped = False
    if adv > 0 and not force_close:
        max_notional = CAPACITY_PARTICIPATION_CAP * adv
        if delta_notional > max_notional:
            delta_szi *= max_notional / delta_notional
            delta_szi = round(delta_szi, szdec)
            delta_notional = abs(delta_szi) * mark
            capacity_capped = True
            summary["n_capacity_capped"] += 1
            if delta_notional < MIN_ORDER_NOTIONAL:
                return

    # If ADV is unavailable on a forced close, use a full-ADV conservative
    # fallback. New exposure already fails closed above.
    participation = (delta_notional / adv) if adv > 0 else (1.0 if force_close else CAPACITY_PARTICIPATION_CAP)
    impact_k = liq.get("impact_k_bps", DEFAULT_IMPACT_K_BPS)        # D1: calibrated-or-prior slope
    impact_alpha = liq.get("impact_alpha", DEFAULT_IMPACT_ALPHA)
    impact_bps = impact_k * (participation ** impact_alpha) * band
    half_spread_bps = liq.get("half_spread_bps", DEFAULT_HALF_SPREAD_BPS) * band
    slip_bps = half_spread_bps + impact_bps
    side = 1 if delta_szi > 0 else -1
    fill_px = mark * (1 + side * slip_bps / 1e4)

    # CHANGE B: explicit ADVERSE LATENCY DRIFT (latency_model="bar_drift_v1"). At a 1m mark resolution a
    # 2s copy latency resolves to the same bar-close price as 0s ~97% of the time, so the latency cost is
    # otherwise invisible. We charge a haircut = LAT_DRIFT_K * (latency as a fraction of a 1m bar) *
    # |causal vol proxy|, applied ALWAYS-adverse to a late taker: a BUY fills higher, a SELL fills lower.
    # CAUSAL FIX (codex finding #3): the vol proxy is the PRIOR COMPLETED bar's |return| (fully known
    # before our_ts), NOT the containing bar's (close-open) which was within-minute future info (look-
    # ahead). High prior-bar vol = scalper/UHFT territory is still penalized -> the discriminating power
    # is preserved while the look-ahead is removed. Conservative CAUSAL VOL-PROXY APPROXIMATION pending
    # real sub-minute (HL trades / L2 book) data. Determinism: derived from the frozen OHLC cache + the
    # fixed copy_latency_ms only.
    lat_frac = min(1.0, params.copy_latency_ms / 60_000.0)   # fraction of a 1m bar our latency spans
    vol_proxy = _bar_vol_proxy(md, coin, our_ts)             # UNSIGNED |return| of the PRIOR closed bar
    latency_drift_bps = 0.0
    if lat_frac > 0.0 and vol_proxy == vol_proxy:
        # ALWAYS a cost: magnitude scales with the prior-bar vol proxy, and `side` makes it adverse to
        # our trade. Large exactly on fast directional bars; charged on every copied fill so the latency
        # haircut is never silently zero.
        latency_drift = LAT_DRIFT_K * lat_frac * vol_proxy
        latency_drift_bps = latency_drift * 1e4   # for the slip diagnostic (codex finding #4)
        fill_px *= (1 + side * latency_drift)

    fee_rate = md.fees.taker(coin)
    fee = abs(delta_szi) * fill_px * fee_rate

    # pre-trade INITIAL-MARGIN admissibility (design §4.4; codex code-r2 #3). Only gate orders that
    # INCREASE exposure (reduces/closes free margin, always allowed). Reject if the required IM + fee
    # exceeds available bucket collateral -> prevents driving main negative on isolated opens and
    # caps cross leverage at the tier. Conservative: cross availability nets existing-position IM.
    cur_szi = st.positions[coin].szi if coin in st.positions else 0.0
    new_szi = cur_szi + delta_szi
    increasing = abs(new_szi) > abs(cur_szi) + 10 ** (-szdec)
    # AUDIT 2026-07-10 (codex P0#5 gate): a REVERSAL (flip through zero) is not "increasing" in magnitude, so it
    # skipped the IM gate and could open a residual isolated position that drives main NEGATIVE. Gate the flip:
    # after settling the old leg (main + old_im + realized_close - close_fee), the residual's fresh IM + open_fee
    # + slip must fit; else REJECT the copy (stay as-is; tracking_error captures the divergence).
    flipping = (cur_szi != 0.0 and abs(new_szi) > 10 ** (-szdec)
                and ((cur_szi >= 0) != (new_szi >= 0)))
    if flipping and mode == "isolated":
        p0 = st.positions[coin]
        realized_close = abs(cur_szi) * (fill_px - p0.entry_px) * (1.0 if cur_szi > 0 else -1.0)
        closed_sz, opened_sz = abs(cur_szi), abs(new_szi)
        total_sz = closed_sz + opened_sz
        close_fee = fee * (closed_sz / total_sz) if total_sz > 0 else fee
        open_fee = fee - close_fee
        slip_cost = abs(delta_szi) * abs(fill_px - mark)
        settled_main = st.cross_collateral.get("main", 0.0) + p0.isolated_margin + realized_close - close_fee
        new_notional = opened_sz * fill_px
        resid_im = new_notional * md.meta.init_margin_rate(coin, new_notional)
        if settled_main < resid_im + open_fee + slip_cost:
            summary["n_rejected"] += 1
            return
    # codex r2: a CROSS flip (same-or-smaller residual, so not "increasing") must ALSO pass the cross IM
    # admissibility gate below (else a reversal can realize loss + open a residual that leaves the scope under
    # IM). Route cross flips into the gate; validated isolated flips (handled above) are excluded.
    if (increasing or (flipping and mode == "cross")) and not (flipping and mode == "isolated"):
        # use the SLIPPAGE-ADJUSTED fill_px (codex code-r4 #2) so the gate matches what _book_fill
        # actually posts/debits; also charge the immediate slippage loss on the new size.
        new_notional = abs(new_szi) * fill_px
        new_im = new_notional * md.meta.init_margin_rate(coin, new_notional)   # TOTAL IM (tier-correct; codex code-r3)
        slip_cost = abs(delta_szi) * abs(fill_px - mark)
        if mode == "isolated":
            cur_im = st.positions[coin].isolated_margin if coin in st.positions else 0.0
            if (new_im - cur_im) + fee + slip_cost > max(0.0, st.cross_collateral.get("main", 0.0)):
                summary["n_rejected"] += 1
                return
        else:
            # TOTAL cross IM of the scope AFTER the fill (existing positions re-rated at their tier)
            # must not exceed scope equity (cash + uPnL) net of fee + immediate slippage cost.
            scope_k = coin_dex(coin)
            total_im = new_im
            equity = st.cross_collateral.get(scope_k, 0.0) - fee - slip_cost
            if coin in st.positions:          # uPnL on the pre-existing portion of this coin
                equity += cur_szi * (mark - st.positions[coin].entry_px)
            for cc in sorted(st.positions):
                pp = st.positions[cc]
                if pp.mode != "cross" or coin_dex(cc) != scope_k or cc == coin:
                    continue
                mm = md.mark(cc, our_ts)
                base = mm if (mm is not None and mm == mm) else pp.entry_px
                total_im += abs(pp.szi) * base * md.meta.init_margin_rate(cc, abs(pp.szi) * base)
                equity += pp.szi * (base - pp.entry_px)
            if total_im > max(0.0, equity):
                summary["n_rejected"] += 1
                return

    _book_fill(st, md, coin, delta_szi, fill_px, mode, fee, summary, our_ts=our_ts, mark=mark)

    summary["n_fills"] += 1
    summary["total_fees"] += fee
    summary["notional_traded"] += delta_notional
    # FIX (codex finding #4): include the latency-drift bps in the notional-weighted slip diagnostic so
    # the reported execution haircut isn't understated (the drift is a real per-fill execution cost
    # already priced into fill_px above; it must show up in slip_bps_notional_weighted too).
    summary["slip_bps_notional_sum"] += (slip_bps + latency_drift_bps) * delta_notional
    fills.append({
        "entity_id": summary["entity_id"], "fold_id": summary["fold_id"], "coin": coin,
        "our_ts": our_ts, "source_ts": int(a["ts"]), "side": "buy" if side > 0 else "sell",
        "our_fill_size": float(delta_szi), "our_fill_px": float(fill_px), "ref_mark": float(mark),
        "half_spread_bps": float(half_spread_bps), "impact_bps": float(impact_bps), "fee": float(fee),
        "fill_type": "normal", "capacity_capped": bool(capacity_capped), "margin_mode": mode,
    })


# --------------------------------------------------------------------------- #
# CHANGE A — realized round-trip accounting (per-coin accumulator -> M6b aggregates)
# --------------------------------------------------------------------------- #
def _rt_open(summary: dict, coin: str, our_ts=None, side: int = 0, entry_px: float = 0.0,
             open_notional: float = 0.0):
    """Start (or re-arm) the round-trip accumulator for `coin` when its position opens from flat.
    INCREMENT 1: also seeds the per-position fields (entry_ts/side/entry_px + intra-journey add/trim +
    peak-notional-at-fills + underwater-add numerator). NOTE: _seed_carry_in constructs the Position
    directly and does NOT call this -> carried-in positions have no accumulator and emit NO per-position
    record when later closed (exactly matching the legacy aggregate behavior, which also excludes them;
    their proper censoring/cutoff handling is a later v2.2 sub-diff). The our_ts=None defaults exist only
    as a defensive fallback; on the normal open-from-flat path our_ts is always supplied."""
    summary["_rt"][coin] = {
        "realized": 0.0, "fee": 0.0, "funding": 0.0,
        "entry_ts": (int(our_ts) if our_ts is not None else None), "side": int(side),
        "entry_px": float(entry_px), "peak_notional": float(open_notional),
        "n_addon": 0, "n_trim": 0, "addon_notional": 0.0, "underwater_add_notional": 0.0,
        "mae": 0.0,   # per-position MAX ADVERSE EXCURSION = worst underwater frac vs entry VWAP (<=0). True
                      # POSITION drawdown (Alberto 2026-07-24), updated at fills + hourly funding cursors.
        "mfe": 0.0,   # per-position MAX FAVORABLE EXCURSION = best in-the-money frac vs entry VWAP (>=0).
                      # "MAE and MFE are crucial" (Alberto 2026-07-24): MFE vs realized = give-back (informs
                      # whether OUR trailing-TP could capture more than the leader's exit).
        "n_samp": 0, "n_uw": 0,   # sample count + underwater-sample count -> time_underwater fraction
                                  # (m6_redesign_v2 gate 5 DCA-whale: time_underwater > ceiling). Fills + hourly marks.
    }


def _rt_update_excursions(acc: dict, mark: float, entry_px: float, side: int) -> None:
    """Update a position's MAE (worst underwater) + MFE (best in-money) vs its entry VWAP, as signed
    fractions. side=+1 long / -1 short. excursion = side*(mark-entry)/entry (>0 favorable, <0 adverse)."""
    if acc is None or entry_px is None or entry_px <= 0 or mark is None or mark != mark or mark <= 0:
        return
    exc = side * (mark - entry_px) / entry_px
    if exc < acc.get("mae", 0.0):
        acc["mae"] = exc
    if exc > acc.get("mfe", 0.0):
        acc["mfe"] = exc
    acc["n_samp"] = acc.get("n_samp", 0) + 1
    if exc < 0:
        acc["n_uw"] = acc.get("n_uw", 0) + 1


def _rt_add(summary: dict, coin: str, realized: float = 0.0, fee: float = 0.0, funding: float = 0.0):
    """Accumulate realized PnL / fee / funding onto the open round-trip for `coin` (no-op if no open
    round-trip is being tracked, e.g. a carried-in position seeded before tracking started)."""
    acc = summary["_rt"].get(coin)
    if acc is None:
        return
    acc["realized"] += realized
    acc["fee"] += fee
    acc["funding"] += funding


def _rt_realize_terminal(summary: dict, p: "Position", terminal_px: float, extra_loss: float = 0.0):
    """CHANGE A FIX (codex finding #1): book the FULL terminal loss of a force-closed position into its
    open round-trip BEFORE _rt_close, so a RUIN / BACKSTOP / LIQUIDATION wipe is correctly a LOSS in
    realized_pnl_total + round_trip_win_rate (otherwise the RT had only entry-fee/funding and could
    score as a win). terminal_px is the (adverse / causal) price the position is wiped at; the realized
    MTM = szi*(terminal_px - entry_px) (signed; deeply negative for the move that wiped it). extra_loss
    (>=0) is any additional penalty/fee the forced close eats (e.g. liquidation slippage already in
    terminal_px, so pass 0 there) charged as a positive cost. No-op if no RT is open for the coin."""
    acc = summary["_rt"].get(p.coin)
    if acc is None:
        return
    realized = p.szi * (terminal_px - p.entry_px)
    acc["realized"] += realized
    if extra_loss:
        acc["fee"] += float(extra_loss)


def _rt_close(summary: dict, coin: str, our_ts=None, close_reason: str = "normal"):
    """Finalize a CLOSED round-trip: realized PnL after OUR costs = realized - fees + funding (funding
    is already signed: negative = paid). Pushes into the summary totals and clears the accumulator.
    INCREMENT 1: also emits a PER-POSITION record into summary["_positions"] (aggregates unchanged).
    close_reason: "normal" (action-driven) | "liquidation" | "backstop" | "ruin" | "trail_flatten"
    -- so forced-loss records (the DCA-whale blowups this exists to catch) are distinguishable and
    carry their real exit_ts (codex+fable P1: forced closes were dropping the in-scope timestamp)."""
    acc = summary["_rt"].pop(coin, None)
    if acc is None:
        return
    pnl = acc["realized"] - acc["fee"] + acc["funding"]
    summary["n_round_trips"] += 1
    if pnl > 0:
        summary["n_round_trip_wins"] += 1
    summary["realized_pnl_total"] += pnl
    # INCREMENT 1 (v2.2): per-position record. peak_notional is the max |szi|*mark observed AT FILL
    # EVENTS (a lower bound; continuous-mark peak/MAE via the funding/scan cursor is the next sub-diff).
    # r_i = realized-after-cost / peak_notional (the size-neutral per-position return; NOTE v2.2 uses the
    # G*-standardized child peak notional -> that normalization lands with the sizing sub-diff, this emits
    # the raw follower peak). underwater_add_ratio = underwater_add_notional / addon_notional (DCA signal).
    peak = float(acc.get("peak_notional", 0.0) or 0.0)
    addon_notional = float(acc.get("addon_notional", 0.0) or 0.0)
    summary["_positions"].append({
        "entity_id": summary["entity_id"], "fold_id": summary["fold_id"], "coin": coin,
        "side": int(acc.get("side", 0)), "entry_ts": acc.get("entry_ts"),
        "exit_ts": (int(our_ts) if our_ts is not None else None),
        "peak_notional": peak, "realized_pnl_after_cost": float(pnl),
        "r_i": (pnl / peak if peak > 0 else None),
        "n_addon": int(acc.get("n_addon", 0)), "n_trim": int(acc.get("n_trim", 0)),
        "addon_notional": addon_notional,
        "underwater_add_notional": float(acc.get("underwater_add_notional", 0.0) or 0.0),
        "underwater_add_ratio": (
            float(acc.get("underwater_add_notional", 0.0) or 0.0) / addon_notional
            if addon_notional > 0 else 0.0),
        # POSITION drawdown/run-up (Alberto 2026-07-24 "MAE and MFE are crucial"): worst underwater
        # frac (<=0) and best in-money frac (>=0) vs entry VWAP over the position's life. mfe_giveback
        # = how much of the peak run-up was surrendered by exit (mfe - r_i); high giveback => leader's
        # own exit leaves money on the table => OUR trailing-TP (policy-b) may capture more.
        "mae": float(acc.get("mae", 0.0) or 0.0),
        "mfe": float(acc.get("mfe", 0.0) or 0.0),
        "mfe_giveback": (
            float(acc.get("mfe", 0.0) or 0.0) - (pnl / peak) if peak > 0 else None),
        # time_underwater = fraction of marks (fills + hourly) where the position was underwater vs entry
        # VWAP (m6_redesign_v2 gate 5 DCA-whale: time_underwater fraction > ceiling => reject).
        "time_underwater": (
            float(acc.get("n_uw", 0)) / float(acc.get("n_samp", 0)) if acc.get("n_samp", 0) else 0.0),
        "close_reason": close_reason, "closed": True,
    })


def _book_fill(st: AccountState, md: MarketData, coin: str, delta_szi: float, fill_px: float,
               mode: str, fee: float, summary: dict, our_ts=None, mark=None):
    """Update position + the right bucket. ISOLATED opens post initial margin from main cross bucket;
    isolated realized/funding/fees route to the position's isolated bucket; closing returns isolated
    margin + realized to main (design §1.2/§3.1; codex code-r1 #2).
    INCREMENT 1: our_ts/mark thread the fill time + ref mark so the per-position accumulator records
    entry/exit ts, peak notional (at fills), intra-journey add/trim, and underwater-add. Both default
    to None (older callers / no-mark paths) -> the per-position fields degrade gracefully, aggregates
    are byte-identical."""
    scope = coin_dex(coin)
    nmark = float(mark) if (mark is not None and mark == mark) else float(fill_px)  # notional valuation
    side_in = 1 if delta_szi > 0 else -1
    p = st.positions.get(coin)
    # INCREMENT 1 fix (codex P1): sample peak notional at THIS fill on the PRE-fill size so the mark at a
    # full-close / trim / flip fill is captured (the full-close branch returns before the tail update,
    # which previously left a run-up position's peak stuck at entry notional -> r_i overstated). The
    # between-fill continuous peak/MAE (via the funding/scan cursor) remains the next sub-diff.
    if p is not None:
        _acc0 = summary["_rt"].get(coin)
        if _acc0 is not None:
            _acc0["peak_notional"] = max(_acc0.get("peak_notional", 0.0), abs(p.szi) * nmark)
            _rt_update_excursions(_acc0, nmark, p.entry_px, 1 if p.szi > 0 else -1)  # MAE/MFE at pre-fill mark
    realized = 0.0
    if p is None:
        new = Position(coin, delta_szi, fill_px, mode, md.meta.max_leverage(coin))
        if mode == "isolated":
            im = abs(delta_szi) * fill_px * md.meta.init_margin_rate(coin, abs(delta_szi) * fill_px)
            st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) - im - fee
            new.isolated_margin = im
        else:
            st.cross_collateral[scope] = st.cross_collateral.get(scope, 0.0) - fee
        st.positions[coin] = new
        _rt_open(summary, coin, our_ts=our_ts, side=side_in, entry_px=fill_px,
                 open_notional=abs(delta_szi) * nmark)   # CHANGE A / INCREMENT 1: opens from flat
        _rt_add(summary, coin, fee=fee)    # entry fee is a round-trip cost
        return

    reducing = (p.szi > 0 and delta_szi < 0) or (p.szi < 0 and delta_szi > 0)
    if reducing:
        reduce = min(abs(delta_szi), abs(p.szi))
        realized = reduce * (fill_px - p.entry_px) * (1 if p.szi > 0 else -1)
    new_szi = p.szi + delta_szi

    if abs(new_szi) < 10 ** (-md.meta.szdec(coin)):
        # full close
        if p.mode == "isolated":
            st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) + p.isolated_margin + realized - fee
        else:
            st.cross_collateral[scope] = st.cross_collateral.get(scope, 0.0) + realized - fee
        del st.positions[coin]
        _rt_add(summary, coin, realized=realized, fee=fee)   # CHANGE A: book exit realized + fee
        _rt_close(summary, coin, our_ts)                     # ...and finalize the closed round-trip
        return

    if (p.szi >= 0) == (new_szi >= 0) and abs(new_szi) > abs(p.szi):
        # INCREMENT 1: an ADDON (same-side increase). Flag underwater (averaging-down) vs the entry VWAP
        # BEFORE it is re-averaged: long adding at mark < entry (or short at mark > entry) = a DCA add.
        _acc = summary["_rt"].get(coin)
        if _acc is not None:
            _acc["n_addon"] += 1
            _add_notional = abs(delta_szi) * nmark
            _acc["addon_notional"] += _add_notional
            _underwater = (p.szi > 0 and nmark < p.entry_px) or (p.szi < 0 and nmark > p.entry_px)
            if _underwater:
                _acc["underwater_add_notional"] += _add_notional
        p.entry_px = (p.entry_px * abs(p.szi) + fill_px * abs(delta_szi)) / abs(new_szi)
        if p.mode == "isolated":
            # post the TIER-CORRECT total IM for the resulting notional; draw the delta (incl any
            # tier-jump re-rate of the existing portion) from main (codex code-r3 consistency).
            new_notional = abs(new_szi) * fill_px
            target_im = new_notional * md.meta.init_margin_rate(coin, new_notional)
            st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) - (target_im - p.isolated_margin) - fee
            p.isolated_margin = target_im
        else:
            st.cross_collateral[scope] = st.cross_collateral.get(scope, 0.0) - fee
        _rt_add(summary, coin, fee=fee)    # CHANGE A: add into open round-trip (no realized)
    elif (p.szi >= 0) != (new_szi >= 0):
        # FLIPPED THROUGH ZERO. Split the flip-order fee by closing vs opening notional so each round-trip
        # bears its own fee (codex finding #2). |p.szi| closes, |new_szi| opens.
        closed_sz = abs(p.szi)
        opened_sz = abs(new_szi)
        total_sz = closed_sz + opened_sz
        close_fee = fee * (closed_sz / total_sz) if total_sz > 0 else fee
        open_fee = fee - close_fee
        if p.mode == "isolated":
            # AUDIT 2026-07-10 (codex P0#5): a reversal must CLOSE the old isolated leg (return its margin +
            # realized - close_fee to main) and OPEN a fresh isolated leg for the RESIDUAL with its OWN new IM
            # drawn from main. The old code did `isolated_margin += realized - fee`, leaving a live opposite
            # position with a stale/NEGATIVE isolated margin (e.g. -40) and breaking cash/IM conservation.
            st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) + p.isolated_margin + realized - close_fee
            new_notional = opened_sz * fill_px
            new_im = new_notional * md.meta.init_margin_rate(coin, new_notional)
            st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) - new_im - open_fee
            p.isolated_margin = new_im
        else:
            st.cross_collateral[scope] = st.cross_collateral.get(scope, 0.0) + realized - fee
        p.entry_px = fill_px      # residual opposite leg re-enters at the flip price
        # CHANGE A: flip CLOSES the prior round-trip (closed portion's realized + close_fee) and OPENS a new
        # one for the residual (open_fee).
        _rt_add(summary, coin, realized=realized, fee=close_fee)
        _rt_close(summary, coin, our_ts)                                    # INCREMENT 1: emit closed leg
        _rt_open(summary, coin, our_ts=our_ts, side=(1 if new_szi > 0 else -1),
                 entry_px=fill_px, open_notional=abs(new_szi) * nmark)      # ...open the residual leg
        _rt_add(summary, coin, fee=open_fee)   # the opposite-side RT bears its own entry fee
    else:
        # reducing (same side, smaller): realize to bucket
        _rt_add(summary, coin, realized=realized, fee=fee)   # CHANGE A: partial realized + fee
        _acc = summary["_rt"].get(coin)        # INCREMENT 1: a TRIM (partial scale-out)
        if _acc is not None:
            _acc["n_trim"] += 1
        if p.mode == "isolated":
            p.isolated_margin += realized - fee
        else:
            st.cross_collateral[scope] = st.cross_collateral.get(scope, 0.0) + realized - fee
    p.szi = new_szi
    # INCREMENT 1: update peak notional (max |szi|*mark seen at fills) on the surviving position.
    _acc = summary["_rt"].get(coin)
    if _acc is not None:
        _acc["peak_notional"] = max(_acc.get("peak_notional", 0.0), abs(new_szi) * nmark)
        _rt_update_excursions(_acc, nmark, p.entry_px, 1 if new_szi > 0 else -1)   # MAE/MFE at this fill


def _apply_funding(st, md, h, summary):
    """Apply the hourly funding settlement to every open position (design §5/D10)."""
    for coin in sorted(st.positions):
        p = st.positions[coin]
        rate = md.funding_rate_at(coin, h)
        px = md.mark(coin, h, causal=True)
        if px is None or px != px:
            continue
        # Continuous MTM cursor: update MAE/MFE on EVERY valid hourly mark, even zero-funding hours
        # (the between-fill drawdown/run-up that the fill-only samples miss). Independent of funding.
        _acc_f = summary["_rt"].get(coin)
        if _acc_f is not None:
            _rt_update_excursions(_acc_f, float(px), p.entry_px, 1 if p.szi > 0 else -1)
        if rate == 0.0:
            continue
        fund = -(p.szi * px * rate)         # long pays positive rate
        if p.mode == "cross":
            st.cross_collateral[coin_dex(coin)] = st.cross_collateral.get(coin_dex(coin), 0.0) + fund
        else:
            p.isolated_margin += fund
        p.cum_funding += -fund
        summary["total_funding"] += fund
        _rt_add(summary, coin, funding=fund)   # CHANGE A: funding is a round-trip cost (signed)


def _advance_between(st, md, t0_ms, t1_ms, params, events, summary, _emit=None, fold_end_ms=None):
    """CHRONOLOGICAL event loop over (t0,t1] (codex code-r2 #1/#5): interleave hourly funding
    settlements with repeated maintenance-breach scans + liquidations, in time order. A position
    liquidated early stops accruing later funding; a partial-liquidation survivor is rescanned to
    fold end; the >100k 20%-then-30s-cooldown ladder advances bar-by-bar (the cursor jumps past the
    cooldown so the next scan does the full close).

    `_emit(cursor_ts, st)` (CHANGE 1): a PURE-READ boundary-equity observer. Called at each cursor
    position; it appends to a NEW equity-samples list ONLY (never touches fills/events/summary/state)
    -> existing outputs are byte-identical. It records equity at any pending block boundary <= cursor."""
    if t0_ms is None or t1_ms <= t0_ms:
        if _emit is not None:
            _emit(t0_ms if t0_ms is not None else t1_ms, st)
        return
    cursor = t0_ms
    if _emit is not None:
        _emit(cursor, st)
    guard = 0
    while cursor < t1_ms and st.positions and not summary["ruin"]:
        guard += 1
        if guard > 200_000:                 # backstop against any pathological non-advance
            logger.error("m07 _advance_between guard tripped (entity=%s fold=%s)",
                         summary.get("entity_id"), summary.get("fold_id"))
            break
        next_funding = ((cursor // MS_HOUR) + 1) * MS_HOUR
        seg_end = min(next_funding, t1_ms)
        breach = _scan_breach(st, md, cursor, seg_end, summary)
        if breach is not None:
            _run_liquidation(st, md, breach["ts"], params, events, summary)
            # resume AFTER this breach minute (and past any liq cooldown just set) so survivors are
            # rescanned and the cooldown elapses before the next (full) liquidation step.
            cursor = max(breach["ts"] + MS_MIN, st.cooldown_until_ms)
            if _emit is not None:
                _emit(cursor, st)
        else:
            # no breach this segment -> account healthy. Once the cooldown has elapsed, reset it so a
            # later, independent breach starts fresh with a 20%-first step (not an immediate full close).
            if st.cooldown_until_ms and st.cooldown_until_ms <= cursor:
                st.cooldown_until_ms = 0
            cursor = seg_end
            # AUDIT 2026-07-10 (codex P0#4 + fix): the FOLD is half-open [start, fold_end). Funding stamped
            # exactly at the fold's exclusive end belongs to the NEXT fold -> skip it (was charging boundary/test
            # funding into pretest ROE). But do NOT skip funding at an INTERIOR action boundary that merely
            # coincides with an hourly stamp (t1_ms here is often an action time, not the fold end) -- only skip
            # when seg_end IS the fold exclusive end.
            if seg_end == next_funding and (fold_end_ms is None or seg_end != int(fold_end_ms)):
                _apply_funding(st, md, seg_end, summary)
                # POST-FUNDING solvency check (codex code-r4 #1): a settlement can push below
                # maintenance; liquidate now (the cooldown guard in _market_liquidate_scope prevents
                # any same-ts/within-30s double-fire on every caller path -- codex code-r5/r6/r7).
                _check_maint_at(st, md, seg_end, params, events, summary)
            if _emit is not None:
                _emit(cursor, st)
    # FOLD-BOUNDARY solvency: final point-in-time maintenance check at t1 (codex code-r4 #1). The
    # cooldown guard makes this safe even if a post-funding liquidation just fired within 30s of t1.
    if st.positions and not summary["ruin"]:
        _check_maint_at(st, md, t1_ms, params, events, summary)
    if _emit is not None:
        _emit(t1_ms, st)


def _scan_breach(st: AccountState, md: MarketData, t0_ms: int, t1_ms: int, summary: dict):
    """3-valued conservative breach scan (design §3.7/D7/D16; codex code-r1 #3). For each scope/iso
    position, test maintenance under BOTH the adverse (low-for-long/high-for-short) and favorable
    orderings over completed minutes in (t0,t1]. Returns the earliest minute that breaches under the
    ADVERSE ordering (the conservative breach) with a `resolved` flag: resolved=True if it also
    breaches under the favorable ordering (definite), False if only adverse breaches (INDETERMINATE,
    counted + treated as a breach conservatively)."""
    m0 = (t0_ms // MS_MIN) * MS_MIN
    m1 = (t1_ms // MS_MIN) * MS_MIN - MS_MIN     # only completed bars
    if m1 < m0:
        return None
    best = None

    def _consider(ts, resolved):
        nonlocal best
        if best is None or ts < best["ts"]:
            best = {"ts": int(ts), "resolved": bool(resolved)}

    # ISOLATED positions (independent)
    for coin in sorted(c for c, p in st.positions.items() if p.mode == "isolated"):
        p = st.positions[coin]
        mins, _o, h, low, _c = md.ohlc(coin)
        if mins.size == 0:
            continue
        lo = int(np.searchsorted(mins, m0, "left")); hi = int(np.searchsorted(mins, m1, "right"))
        if hi <= lo:
            continue
        adverse = low[lo:hi] if p.szi > 0 else h[lo:hi]
        favorable = h[lo:hi] if p.szi > 0 else low[lo:hi]
        notion_a = np.abs(p.szi) * adverse
        eq_a = p.isolated_margin + p.szi * (adverse - p.entry_px)
        maint_a = notion_a * md.meta.maint_rate(coin, float(np.nanmax(notion_a)) if notion_a.size else 0.0)
        bad_a = np.where(eq_a < maint_a)[0]
        if bad_a.size:
            k = lo + int(bad_a[0])
            eq_f = p.isolated_margin + p.szi * (favorable[int(bad_a[0])] - p.entry_px)
            resolved = eq_f < maint_a[int(bad_a[0])]
            if not resolved:
                summary["n_indeterminate_minutes"] += 1
                summary["outcome_states"].add("indeterminate")
            _consider(mins[k], resolved)

    # CROSS scopes (aggregate over coins on a union minute grid)
    scopes: dict[str, list[str]] = {}
    for coin in sorted(c for c, p in st.positions.items() if p.mode == "cross"):
        scopes.setdefault(coin_dex(coin), []).append(coin)
    for scope in sorted(scopes):
        coins = scopes[scope]
        grid = None
        for coin in coins:
            mins, *_ = md.ohlc(coin)
            if mins.size == 0:
                continue
            lo = int(np.searchsorted(mins, m0, "left")); hi = int(np.searchsorted(mins, m1, "right"))
            if hi > lo:
                grid = mins[lo:hi] if grid is None else np.union1d(grid, mins[lo:hi])
        if grid is None or grid.size == 0:
            continue
        cash = st.cross_collateral.get(scope, 0.0)
        adv_eq = np.full(grid.size, cash); fav_eq = np.full(grid.size, cash); maint = np.zeros(grid.size)
        for coin in coins:
            p = st.positions[coin]
            mins, _o, h, low, _c = md.ohlc(coin)
            if mins.size == 0:
                continue
            j = np.clip(np.searchsorted(mins, grid, "right") - 1, 0, mins.size - 1)
            adverse = low[j] if p.szi > 0 else h[j]
            favorable = h[j] if p.szi > 0 else low[j]
            adv_eq = adv_eq + p.szi * (adverse - p.entry_px)
            fav_eq = fav_eq + p.szi * (favorable - p.entry_px)
            notion = np.abs(p.szi) * adverse
            maint = maint + notion * md.meta.maint_rate(coin, float(np.nanmax(notion)) if notion.size else 0.0)
        bad = np.where(adv_eq < maint)[0]
        if bad.size:
            k = int(bad[0])
            resolved = fav_eq[k] < maint[k]
            if not resolved:
                summary["n_indeterminate_minutes"] += 1
                summary["outcome_states"].add("indeterminate")
            _consider(grid[k], resolved)
    return best


def _check_maint_at(st: AccountState, md: MarketData, ts_ms: int, params, events, summary):
    """Point-in-time maintenance solvency check at the CAUSAL CLOSE mark at ts (codex code-r4 #1):
    used after funding settlements and at the fold boundary, where a settled cash move (not a price
    wick) can drop a scope/isolated bucket below maintenance. If breached, run liquidation (which
    executes at the adverse mark, consistent + conservative). Detects via close; adverse <= close so
    a close breach always also breaches under the adverse extreme."""
    if not st.positions:
        return
    marks = _marks(st, md, ts_ms)
    breached = False
    for coin in sorted(c for c, p in st.positions.items() if p.mode == "isolated"):
        p = st.positions[coin]; m = marks.get(coin) or p.entry_px
        notion = abs(p.szi) * m
        if p.isolated_margin + p.szi * (m - p.entry_px) < notion * md.meta.maint_rate(coin, notion):
            breached = True; break
    if not breached:
        for scope in st.cross_scopes():
            coins = [c for c, p in st.positions.items() if p.mode == "cross" and coin_dex(c) == scope]
            if not coins:
                continue
            eq = st.cross_collateral.get(scope, 0.0) + sum(
                st.positions[c].szi * ((marks.get(c) or st.positions[c].entry_px) - st.positions[c].entry_px) for c in coins)
            maint = sum(abs(st.positions[c].szi) * (marks.get(c) or st.positions[c].entry_px)
                        * md.meta.maint_rate(c, abs(st.positions[c].szi) * (marks.get(c) or st.positions[c].entry_px)) for c in coins)
            if eq < maint:
                breached = True; break
    if breached:
        _run_liquidation(st, md, ts_ms, params, events, summary)


def _adverse_marks(st: AccountState, md: MarketData, ts_ms: int) -> dict:
    """Per-coin ADVERSE candle extreme at the minute covering ts (low for longs, high for shorts) —
    the realized worst price that drove the breach. Liquidation executes against THIS (consistent
    with _scan_breach), so a detected breach actually fires, and the forced fill is conservatively
    pessimistic (codex code-r1 #3 — scan and liquidation must agree)."""
    out = {}
    for coin in sorted(st.positions):
        p = st.positions[coin]
        mins, _o, h, low, c = md.ohlc(coin)
        if mins.size == 0:
            out[coin] = p.entry_px
            continue
        i = int(np.searchsorted(mins, (ts_ms // MS_MIN) * MS_MIN, "right")) - 1
        if i < 0:
            out[coin] = p.entry_px
            continue
        adverse = low[i] if p.szi > 0 else h[i]
        out[coin] = float(adverse) if adverse == adverse else float(c[i])
    return out


def _run_liquidation(st: AccountState, md: MarketData, ts_ms: int, params, events, summary):
    """Cross/isolated liquidation state machine (design §3.3/§3.4/D12/D15; codex code-r1 #2/#3/#5).
    Backstop = position+cross-margin TRANSFER to vault (event-only, NO fee). Market-liq orders are
    FILLS with the forced-liq slippage penalty. >100k: 20% first then (cooldown) full; <=100k full.
    Uses ADVERSE breach marks (consistent with _scan_breach) so detected breaches actually execute."""
    marks = _adverse_marks(st, md, ts_ms)

    # ISOLATED first (each independent)
    for coin in sorted(c for c, p in st.positions.items() if p.mode == "isolated"):
        p = st.positions[coin]
        m = marks.get(coin) or p.entry_px
        notion = abs(p.szi) * m
        iso_eq = p.isolated_margin + p.szi * (m - p.entry_px)
        maint = notion * md.meta.maint_rate(coin, notion)
        if iso_eq < BACKSTOP_MAINT_FRACTION * maint:
            events.append(_evt(summary, ts_ms, "backstop_transfer", coin, coin_dex(coin)))
            summary["n_backstop_transfer"] += 1
            summary["outcome_states"].add("backstop")
            _rt_realize_terminal(summary, p, m)   # CHANGE A FIX #1: realize the wipe loss FIRST
            del st.positions[coin]
            _rt_close(summary, coin, our_ts=ts_ms, close_reason="backstop")   # CHANGE A: backstop wipe ends the round-trip (now a LOSS)
        elif iso_eq < maint:
            _liq_close(st, md, coin, m, ts_ms, summary=summary)

    # AUDIT 2026-07-10 (codex P0#6 gap): if the isolated backstop(s) above left the account with no equity
    # (isolated-only account wiped), it is RUIN too -> finalize via _ruin (was leaving backstop w/o account_ruin/
    # time_to_ruin). Checked after the isolated loop to avoid mutating st.positions mid-iteration.
    if not summary["ruin"] and st.equity(_adverse_marks(st, md, ts_ms)) <= 1e-6:
        _ruin(st, md, summary, events, int(ts_ms))

    # CROSS scopes
    for scope in st.cross_scopes():
        coins = [c for c, p in st.positions.items() if p.mode == "cross" and coin_dex(c) == scope]
        if not coins:
            continue
        cash = st.cross_collateral.get(scope, 0.0)
        eq = cash + sum(st.positions[c].szi * ((marks.get(c) or st.positions[c].entry_px) - st.positions[c].entry_px) for c in coins)
        maint = sum(abs(st.positions[c].szi) * (marks.get(c) or st.positions[c].entry_px)
                    * md.meta.maint_rate(c, abs(st.positions[c].szi) * (marks.get(c) or st.positions[c].entry_px)) for c in coins)
        if eq < BACKSTOP_MAINT_FRACTION * maint:
            for c in sorted(coins):
                events.append(_evt(summary, ts_ms, "backstop_transfer", c, scope))
                _rt_realize_terminal(summary, st.positions[c],
                                     marks.get(c) or st.positions[c].entry_px)   # CHANGE A FIX #1
                del st.positions[c]
                _rt_close(summary, c, our_ts=ts_ms, close_reason="backstop")   # CHANGE A: backstop wipe ends the round-trip (now a LOSS)
            st.cross_collateral[scope] = 0.0
            summary["n_backstop_transfer"] += 1
            summary["outcome_states"].add("backstop")
            # AUDIT 2026-07-10 (codex P0#6): if this backstop leaves the account with no equity (total across
            # ALL remaining scopes+positions <= ~0), it is RUIN -> set ruin/time_to_ruin so `ruin`-based
            # downstream filters (m06b, etc.) don't treat a wiped account as non-ruined. A multi-scope account
            # whose OTHER scopes still hold collateral is NOT ruined by a single-scope backstop (n_backstop_transfer
            # still flags it; m08 counts any backstop as wiped).
            if not summary["ruin"] and st.equity(_adverse_marks(st, md, ts_ms)) <= 1e-6:
                _ruin(st, md, summary, events, int(ts_ms))   # full ruin finalization (zeros equity, no stale _core_final_eq)
        elif eq < maint:
            _market_liquidate_scope(st, md, scope, ts_ms, params, events, summary, marks)


def _market_liquidate_scope(st, md, scope, ts_ms, params, events, summary, marks):
    """ONE HL market-liquidation STEP for the breached cross scope (codex code-r2 #5): close the
    most-marginal position by 20% if its notional > 100k and not in cooldown (then sets the 30s
    cooldown), else full. The chronological event loop (_advance_between) re-invokes on the next bar
    after the cooldown elapses, so the >100k 20%-then-full ladder advances bar-by-bar (interval-
    bounded at 1m). Recorded as a market_liq_order FILL with the forced-liq slippage penalty."""
    # SINGLE-POINT COOLDOWN ENFORCEMENT (codex code-r5/r6/r7): never issue a liquidation order while
    # cooling down. This makes it IMPOSSIBLE for any caller path (price breach, post-funding, terminal
    # fold-boundary, or their combinations at the same/sub-minute ts) to fire two orders within 30s.
    # The position simply waits; the next eligible 1m bar (>=60s > 30s cooldown) issues the next order.
    if ts_ms < st.cooldown_until_ms:
        return
    coins = [c for c, p in st.positions.items() if p.mode == "cross" and coin_dex(c) == scope]
    if not coins:
        return
    c = max(sorted(coins), key=lambda x: abs(st.positions[x].szi) * (marks.get(x) or st.positions[x].entry_px))
    p = st.positions[c]
    m = marks.get(c) or p.entry_px
    notion = abs(p.szi) * m
    # HL ladder: the FIRST liquidation order on the account is 20% (if >100k) and sets the 30s
    # cooldown; SUBSEQUENT orders (after the cooldown elapses) are FULL. cooldown_until_ms==0 marks
    # "no prior liquidation pending" (reset once the account is healthy for a full segment).
    first_step = (st.cooldown_until_ms == 0)
    if notion > PARTIAL_LIQ_NOTIONAL and first_step:
        close_szi = -p.szi * PARTIAL_LIQ_FRACTION
        st.cooldown_until_ms = ts_ms + LIQ_COOLDOWN_MS
    else:
        close_szi = -p.szi
    _liq_close(st, md, c, m, ts_ms, summary=summary, close_szi=close_szi, scope=scope)


def _liq_close(st, md, coin, mark, ts_ms, summary, close_szi=None, scope=None):
    """Close via a forced market-liq order with uncapped full-order impact.

    Hyperliquid sends these orders to the book and charges no clearance fee.
    Thirty bps is a conservative floor, not a size-independent execution
    price: larger orders also pay the normal half-spread + full order/ADV impact
    curve with no participation cap.
    """
    p = st.positions.get(coin)
    if p is None:
        return
    if close_szi is None:
        close_szi = -p.szi
    side = 1 if close_szi > 0 else -1
    close_notional = abs(close_szi) * mark
    liq = md.liquidity(coin, ts_ms)
    adv = float(liq.get("adv", 0.0) or 0.0)
    participation = close_notional / adv if adv > 0 else 1.0
    half_spread_bps = float(liq.get("half_spread_bps", DEFAULT_HALF_SPREAD_BPS))
    impact_k = float(liq.get("impact_k_bps", DEFAULT_IMPACT_K_BPS))
    impact_alpha = float(liq.get("impact_alpha", DEFAULT_IMPACT_ALPHA))
    curve_slip_bps = half_spread_bps + impact_k * (participation ** impact_alpha)
    forced_slip_bps = max(FORCED_LIQ_SLIP_BPS, curve_slip_bps)
    fill_px = mark * (1 + side * forced_slip_bps / 1e4)
    # realized PnL on the closed portion (forced-liq order; no fee, design D12)
    realized = min(abs(close_szi), abs(p.szi)) * (fill_px - p.entry_px) * (1 if p.szi > 0 else -1)
    if p.mode == "cross":
        sc = scope or coin_dex(coin)
        st.cross_collateral[sc] = st.cross_collateral.get(sc, 0.0) + realized
    else:
        p.isolated_margin += realized
    _rt_add(summary, coin, realized=realized)   # CHANGE A: forced-liq realized (no fee on liq orders)
    new_szi = p.szi + close_szi
    fl = summary.get("_fills_ref")
    if fl is not None:
        fl.append({"entity_id": summary["entity_id"], "fold_id": summary["fold_id"], "coin": coin,
                   "our_ts": ts_ms, "source_ts": ts_ms, "side": "buy" if side > 0 else "sell",
                   "our_fill_size": float(close_szi), "our_fill_px": float(fill_px), "ref_mark": float(mark),
                   "half_spread_bps": half_spread_bps,
                   "impact_bps": float(forced_slip_bps - half_spread_bps), "fee": 0.0,
                   "fill_type": "market_liq_order", "capacity_capped": False, "margin_mode": p.mode})
    summary["n_market_liq_orders"] += 1
    summary["outcome_states"].add("position_liquidated")
    if abs(new_szi) < 10 ** (-md.meta.szdec(coin)):
        if p.mode == "isolated":
            st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) + max(0.0, p.isolated_margin)
        del st.positions[coin]
        _rt_close(summary, coin, our_ts=ts_ms, close_reason="liquidation")   # CHANGE A: forced full close ends the round-trip
    else:
        p.szi = new_szi


def _evt(summary, ts_ms, etype, coin, scope):
    return {"entity_id": summary["entity_id"], "fold_id": summary["fold_id"], "ts": ts_ms,
            "event_type": etype, "coin": coin, "scope": scope}


def _ruin(st: AccountState, md: "MarketData", summary: dict, events: list, ts_ms: int):
    summary["ruin"] = True
    summary["outcome_states"].add("account_ruin")
    if summary["time_to_ruin_ms"] is None:
        summary["time_to_ruin_ms"] = ts_ms
    events.append(_evt(summary, ts_ms, "account_ruin", "", ""))
    for c in list(st.positions):           # CHANGE A: close any still-open round-trips on ruin
        p = st.positions[c]
        # CHANGE A FIX #1: realize the FULL terminal MTM loss vs entry before closing so a ruined
        # round-trip is a LOSS, not a near-flat win. Price at the causal mark at the ruin instant;
        # fall back to entry_px only if uncovered (degenerate -> 0 realized, still not a spurious win).
        m = md.mark(c, ts_ms, causal=True)
        if m is None or m != m or m <= 0:
            m = md.mark(c, ts_ms, causal=False)
        if m is None or m != m or m <= 0:
            m = p.entry_px
        _rt_realize_terminal(summary, p, m)
        _rt_close(summary, c, our_ts=ts_ms, close_reason="ruin")
    st.positions.clear()
    for k in list(st.cross_collateral):
        st.cross_collateral[k] = 0.0
    # FIX (codex regression): the action/non-ruin sample sites stamped _core_final_eq with the LAST
    # positive pre-ruin equity. After ruin the post-ruin closeout emits equity=0 samples, but _finalize
    # reads _core_final_eq -> summary.final_equity/roe_engine were left STALE (positive). Stamp the
    # post-ruin equity (0) here so final_equity~=0 and roe_engine~=-1.0, consistent with the realized
    # round-trip terminal loss already booked. Non-ruin path is untouched (this runs only on ruin).
    summary["_core_final_eq"] = 0.0


def _finalize(st, fills, events, equity_samples, summary, md, start_equity):
    summary.pop("_fills_ref", None)     # internal handle; never written to the summary parquet
    # CHANGE 2: finalize tracking_error = active-time-weighted mean L1 vector error. None when no
    # active exposure time accrued (-> M6b fidelity stays provisional, never a misleading 0).
    te_active = summary.get("tracking_error_active_ms", 0)
    te_sum = summary.pop("_te_weighted_sum", 0.0)
    summary["tracking_error"] = (te_sum / te_active) if te_active > 0 else None
    # final_eq from the ORIGINAL sample sites only (byte-identical with the shipped path); the new
    # boundary/ruin-drain samples never participate.
    final_eq = float(summary.pop("_core_final_eq", float(start_equity)))
    final_eq = max(0.0, final_eq)   # isolation invariant: equity floored at 0
    summary["final_equity"] = float(final_eq)
    summary["roe_engine"] = (final_eq / start_equity - 1.0) if start_equity > 0 else 0.0
    summary["slip_bps_notional_weighted"] = (
        summary["slip_bps_notional_sum"] / summary["notional_traded"] if summary["notional_traded"] > 0 else 0.0)
    # CHANGE A: finalize realized round-trip aggregates (consumed by M6b). Any round-trip still OPEN at
    # fold end (an un-closed carried position) is intentionally NOT counted -- only CLOSED round-trips.
    summary.pop("_rt", None)
    # INCREMENT 1 (v2.2 per-position emit): lift the per-CLOSED-round-trip records out of the summary
    # (never written to the one-row summary parquet) and return them as a parallel "positions" stream
    # (like fills/events). Aggregates above are untouched -> pure superset.
    positions = summary.pop("_positions", [])
    nrt = summary["n_round_trips"]
    summary["round_trip_win_rate"] = (summary["n_round_trip_wins"] / nrt) if nrt > 0 else 0.0
    summary["realized_roe"] = (summary["realized_pnl_total"] / start_equity) if start_equity > 0 else 0.0
    # CHANGE B: declare the latency-cost model used to price every fill (see _apply_order).
    summary["latency_model"] = "bar_drift_v1"
    if not summary["outcome_states"]:
        summary["outcome_states"].add("survived")
    summary["outcome_states"] = sorted(summary["outcome_states"])
    ending_state = {"cross_collateral": dict(st.cross_collateral),
                    "positions": {c: vars(p) for c, p in st.positions.items()},
                    "cooldown_until_ms": st.cooldown_until_ms}
    return {"fills": fills, "events": events, "equity": equity_samples, "positions": positions,
            "ending_account_state": ending_state, "summary": summary}


# --------------------------------------------------------------------------- #
# Runner — pre-shard actions by wallet (no per-seat full-file scan), streaming out (design §8)
# --------------------------------------------------------------------------- #
def _require_action_schema(dataset) -> None:
    """Reject pre-fix M2 artifacts that cannot prove lifecycle observability."""
    required = {
        "wallet", "coin", "ts", "event_order", "action_type", "signed_size",
        "position_after", "target_exposure_pct", "is_liquidation",
        "carry_in_status", "lifecycle_valid", "stream_replay_valid",
    }
    missing = required - set(dataset.schema.names)
    if missing:
        raise ValueError(
            "M7 requires a rebuilt causal M2 action artifact; missing columns: "
            f"{sorted(missing)}"
        )


class _NoStats(Exception):
    """Internal: parquet column statistics unavailable -> fall back to a bounded data sample."""


def assert_sizing_input_usable(dataset, sizing_mode: str, actions_path) -> None:
    """FAIL LOUD if the selected sizing mode cannot size anything from this store (2026-07-30).

    THE BUG THIS CLOSES: `sizing_mode="leader_equity"` sizes from `target_exposure_pct`, which is 100%
    NULL in every m02 actions store ever built -- m02_journey_trace derives it from `source_equity_post`,
    and with M1 out of scope (Alberto 2026-07-17, reconfirmed 2026-07-30 "No M1 no equity") that anchor
    never exists, so the column is permanently NO_ANCHOR/null. Sampled 8 of 1,137 row groups on the 20k
    store: 904,494 values, 904,494 null. Under that mode EVERY target resolves to NaN, so the engine
    emitted ZERO orders and STILL REPORTED SUCCESS -- one documented run read 46,180,870 actions and
    produced 0 fills / 0 positions. A verdict of "this cohort has no edge" that actually means "no input".

    Checked ONCE here rather than per action, because "entirely null" is only knowable over the store.
    Uses parquet column statistics where available so it costs no data scan.

    Deliberately NOT raising for `leader_equity` unconditionally: the unit tests exercise engine
    mechanics (backstop, liquidation, follower-trail, min-order) through that path with a synthetic
    non-null column, and those are legitimate. It is the ALL-NULL store that is the wrong answer.
    """
    if sizing_mode != "leader_equity":
        return
    col = "target_exposure_pct"
    if col not in set(dataset.schema.names):
        raise ValueError(f"sizing_mode='leader_equity' needs {col!r}, absent from {actions_path}")
    # Prefer parquet column statistics (free). If they are ABSENT we must NOT assume usable -- the real
    # 20k store has no stats on this column, so an assume-usable fallback left this guard inert on
    # exactly the file it exists to catch (verified 2026-07-30). Fall back to reading a BOUNDED sample
    # of the column instead: enough to prove "entirely null", cheap enough to never matter.
    # codex 2026-07-30 #3 and #15 rewrote this. Three bugs in the previous version, all of which made a
    # SAFETY CHECK PASS when it should have refused:
    #   #3 a no-stats row group reset the counters and raised _NoStats but left used_stats=True, so the
    #      fallback scan was skipped and `total == 0` suppressed the rejection -> a MIXED-statistics
    #      all-null store sailed through and still emitted zero orders.
    #   #15 the bounded 200k prefix could FALSELY REJECT a store whose nulls happen to be at the front,
    #      and any metadata/scanner exception `return`ed, i.e. turned an inspection failure into
    #      PERMISSION. A fail-loud check must never fail open.
    # Correct semantics: the question is only "does even ONE non-null value exist?". Stats can answer it
    # definitively when present for EVERY row group; otherwise scan and STOP AT THE FIRST non-null value.
    # That is cheap for usable stores (early exit) and only walks the whole column for the all-null case,
    # which is exactly the case being rejected.
    def _stats_say_all_null():
        """True/False from row-group stats, or None if any row group lacks them."""
        total = nulls = 0
        for frag in dataset.get_fragments():
            md = frag.metadata
            if md is None or col not in md.schema.names:
                return None
            idx = md.schema.names.index(col)
            for rg in range(md.num_row_groups):
                c = md.row_group(rg).column(idx)
                if c.statistics is None:
                    return None          # ANY gap invalidates the whole stats path
                total += c.num_values
                nulls += c.statistics.null_count
        return (total > 0 and nulls >= total, total, nulls) if total else None

    verdict = None
    try:
        verdict = _stats_say_all_null()
    except Exception as e:               # inspection failure must NOT become permission
        logger.warning("[sizing-guard] stats inspection failed on %s (%s); falling back to a scan",
                       actions_path, e)
        verdict = None

    if verdict is None:                  # scan: stop at the FIRST non-null; no prefix guessing
        seen = 0
        try:
            for batch in dataset.scanner(columns=[col], batch_size=65_536).to_batches():
                arr = batch.column(0)
                seen += len(arr)
                if arr.null_count < len(arr):
                    return               # a real value exists -> the mode can size. Done.
        except Exception as e:
            raise ValueError(
                f"sizing_mode='leader_equity' refuses to run: could not verify {col} in "
                f"{actions_path} ({e}). A safety check that cannot inspect its input must refuse, not "
                f"assume. Use sizing_mode='fixed_position'."
            ) from e
        if seen == 0:
            return                       # genuinely empty dataset; nothing to size either way
        verdict = (True, seen, seen)

    all_null, total, nulls = verdict
    if all_null:
        raise ValueError(
            f"sizing_mode='leader_equity' refuses to run: {col} is 100% NULL in {actions_path} "
            f"({nulls:,}/{total:,} values). It sizes from leader equity (M1, out of scope), so this "
            f"store can never populate it -- the run would emit ZERO orders and report success. "
            f"Use sizing_mode='fixed_position' with --fixed-target-exposure. "
            f"See card/quant-engineer/canonical-pipeline-and-engine."
        )


try:
    import pyarrow as _pa_mod
    _PA_VER = _pa_mod.__version__
except Exception:  # noqa: BLE001
    _PA_VER = "na"
# bump the literal on any change to the shard cols/filter/partitioning; the pyarrow version is folded in
# because pyarrow does the filtering/decoding/partition-encoding (codex P2: an upgrade must invalidate).
_M07_SHARD_BUILD_VER = f"shard-v1|pa{_PA_VER}"
_M07_SHARD_CACHE_DIR = Path(__file__).resolve().parents[1] / "app" / "data" / "v15" / "m07_shard_cache"
# SOUNDNESS CONTRACT (same as m06a): m07 runs as a SEQUENTIAL recal_pipeline stage (one run_shortlist at a
# time; m02_actions is not rewritten while it runs). Under that contract the content-hash key + atomic
# marker-last publish make the shard cache sound. Concurrent run_shortlist into a shared cache is out of
# contract (a per-run UUID tmp dir still prevents staging collisions; hit-time manifest validation of the
# cached partitions is a deferred hardening).


def _m07_shard_key(wallets, limit_entities, cols, actions_path) -> str:
    """CONTENT-HASH key for the wallet pre-shard: the wallet SET + limit + projected cols + build version +
    the byte-content of m02_actions (streamed in 1MB chunks -> memory-bounded). Pure, no side effects. A change
    to any of these MISSes; an unchanged rerun (even with a new mtime) HITs."""
    import hashlib as _hl
    h = _hl.sha256()
    h.update("\x00".join(wallets).encode()); h.update(f"|lim={limit_entities}|".encode())
    h.update("\x00".join(cols).encode()); h.update(_M07_SHARD_BUILD_VER.encode())
    af = _hl.sha256()
    with open(actions_path, "rb") as f:
        for ck in iter(lambda: f.read(1 << 20), b""):
            af.update(ck)
    h.update(af.hexdigest().encode())
    return h.hexdigest()


def run_shortlist(actions_path: Path, shortlist_path: Path, folds_path: Path, out_dir: Path,
                  band: str = "base", limit_entities: Optional[int] = None, start_equity: float = 10_000.0,
                  flush_rows: int = 250_000, require_cache: bool = True, window: str = "test",
                  slip_calib_path: Optional[str] = None, follower_trail: Optional[float] = None,
                  copy_latency_ms: int = 4_000, sizing_mode: str = "fixed_position",  # latency measured 2026-07-27;
                  # sizing default flipped off leader_equity 2026-07-30 (silent-zero-orders trap + M1 remnant)
                  fixed_target_exposure: float = 0.10,
                  copy_policy: str = "full_mirror", trail_pct: float = 0.15):
    import shutil
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _streaming_io import ShardedParquetWriter, install_memory_guard
    import pyarrow.dataset as ds

    install_memory_guard(soft_gb=12, label="m07")
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    folds = pd.read_parquet(folds_path)
    # window selects the simulated span per fold: "test" = OOS [test_start,test_end) (M9 eval input);
    # "pretest" = [train_start,test_start) (M6b fold-pure ranking input). Same shipped engine; only the
    # window differs. start_ts_ms anchors risk at the window start.
    if window == "pretest":
        fold_win = {int(r.fold_id): (int(pd.Timestamp(r.train_start).timestamp() * 1000),
                                     int(pd.Timestamp(r.test_start).timestamp() * 1000))
                    for r in folds.itertuples()}
    else:
        fold_win = {int(r.fold_id): (int(pd.Timestamp(r.test_start).timestamp() * 1000),
                                     int(pd.Timestamp(r.test_end_excl).timestamp() * 1000))
                    for r in folds.itertuples()}

    sl = pd.read_parquet(shortlist_path, columns=["entity_id", "primary_wallet", "fold_id", "in_shortlist"])
    sl = sl[sl.in_shortlist].copy()
    if limit_entities:
        keep = sl.entity_id.drop_duplicates().head(limit_entities)
        sl = sl[sl.entity_id.isin(keep)]
    wallets = sorted(sl.primary_wallet.unique())
    seats_by_wallet: dict[str, list] = {}
    for r in sl.itertuples(index=False):
        seats_by_wallet.setdefault(r.primary_wallet, []).append((int(r.entity_id), int(r.fold_id)))
    logger.info("M7 runner: %d seats over %d wallets", len(sl), len(wallets))

    # PRE-SHARD actions by wallet ONCE via pyarrow write_dataset (codex code-r2 #4): streaming,
    # memory-bounded (no per-wallet writer held open buffering rows). Hive-partitioned by wallet.
    dataset = ds.dataset(actions_path, format="parquet")
    _require_action_schema(dataset)
    assert_sizing_input_usable(dataset, sizing_mode, actions_path)
    cols = ["wallet", "coin", "ts", "event_order", "action_type", "signed_size", "position_after",
            "target_exposure_pct", "is_liquidation", "carry_in_status",
            "lifecycle_valid", "stream_replay_valid"]
    replayable = ds.field("wallet").isin(wallets) & (ds.field("stream_replay_valid") == True)  # noqa: E712
    # CONTENT-HASH CACHE (Fable+codex-gated pattern, 2026-07-17): the pre-shard is a pure function of the wallet
    # SET + m02_actions content + the projected cols + the build logic. recal_pipeline rmtree-rebuilt it fresh
    # every run (a full ~4.4GB scan+filter+write); cache it keyed by that content so unchanged reruns reuse the
    # shards (biggest single reshard win).
    shard_key = _m07_shard_key(wallets, limit_entities, cols, actions_path)
    shard_dir = _M07_SHARD_CACHE_DIR / shard_key
    if (shard_dir / "._complete").exists():
        logger.info("M7 runner: reshard CACHE HIT %s (skipped the %s scan+shard)", shard_key[:12], actions_path)
    else:
        # build into a UNIQUE private tmp dir (tempfile.mkdtemp -> no PID-collision, codex P1), marker LAST,
        # atomic rename -> never serve a partial/corrupt shard set.
        import tempfile
        _M07_SHARD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _tmp = Path(tempfile.mkdtemp(prefix=f".tmp_{shard_key}_", dir=str(_M07_SHARD_CACHE_DIR)))
        scanner = dataset.scanner(columns=cols, filter=replayable, batch_size=200_000)
        ds.write_dataset(scanner, _tmp, format="parquet", partitioning=["wallet"],
                         partitioning_flavor="hive", existing_data_behavior="overwrite_or_ignore",
                         max_rows_per_file=2_000_000, max_rows_per_group=200_000)
        # TOCTOU guard (codex P1): only PUBLISH to the keyed cache if m02_actions is STILL what the key hashed
        # -- a mid-build atomic replace of the actions file would otherwise store new-file shards under the old
        # key. If it changed, use THIS run's freshly-built shards (correct for this run) but do NOT cache them.
        if _m07_shard_key(wallets, limit_entities, cols, actions_path) != shard_key:
            logger.warning("M7 runner: m02_actions changed during shard build -> using this run's shards, NOT caching")
            shard_dir = _tmp   # valid for this run; left uncached (key would be wrong)
        else:
            (_tmp / "._complete").write_text(shard_key)
            try:
                os.replace(_tmp, shard_dir)   # atomic; fails if a concurrent run already won
            except OSError:
                shutil.rmtree(_tmp, ignore_errors=True)
                if not (shard_dir / "._complete").exists():
                    raise
            logger.info("M7 runner: reshard CACHE MISS -> built %s", shard_key[:12])
    shard_ds = ds.dataset(shard_dir, format="parquet", partitioning="hive")

    # PRELOAD market caches for the shortlist coin set so the inner loop never hits Mongo (codex
    # code-r2 #7). Build the OHLC cache (close-only marks_cache is insufficient) if missing.
    coins = set()
    for b in dataset.scanner(columns=["coin", "wallet"], filter=replayable,
                             batch_size=500_000).to_batches():
        coins.update(c for c in b.column("coin").to_pylist() if c and not coin_is_spot(c))
    logger.info("M7 runner: preloading OHLC+funding caches for %d coins", len(coins))
    build_ohlc_cache(sorted(coins))
    md = MarketData(require_cache=require_cache)
    for c in sorted(coins):           # warm funding series into memo (one Mongo pass each, precompute)
        md.funding_series(c)

    # D1: optional slippage calibration (per-fold as-of). Installed per seat by fold_id.
    calib_per_fold: dict = {}
    calib_version = None
    if slip_calib_path:
        cal = json.loads(Path(slip_calib_path).read_text())
        calib_version = cal.get("version")
        calib_per_fold = {int(k): v for k, v in cal.get("per_fold_asof", {}).items()}
        logger.info("M7 slippage calib loaded: version=%s, %d folds", calib_version, len(calib_per_fold))

    fw = ShardedParquetWriter(out_dir / "m07_fills.parquet", flush_rows=flush_rows)
    ew = ShardedParquetWriter(out_dir / "m07_events.parquet", flush_rows=flush_rows)
    sw = ShardedParquetWriter(out_dir / "m07_summary.parquet", flush_rows=200_000)
    qw = ShardedParquetWriter(out_dir / "m07_equity.parquet", flush_rows=flush_rows)  # CHANGE 1
    pw = ShardedParquetWriter(out_dir / "m07_positions.parquet", flush_rows=flush_rows)  # INCREMENT 1
    params = EngineParams(
        slippage_band=band, follower_trail=follower_trail,
        copy_latency_ms=copy_latency_ms, sizing_mode=sizing_mode,
        fixed_target_exposure=fixed_target_exposure,
        copy_policy=copy_policy, trail_pct=trail_pct,
    )

    for w in wallets:
        try:
            wdf = shard_ds.to_table(filter=ds.field("wallet") == w).to_pandas()
        except Exception:
            continue
        if wdf.empty:
            continue
        for entity_id, fold_id in sorted(seats_by_wallet.get(w, [])):
            t0ms, t1ms = fold_win[fold_id]
            md.set_slip_calib(calib_per_fold.get(int(fold_id)), calib_version)   # D1 per-fold as-of
            adf = wdf[(wdf.ts >= t0ms) & (wdf.ts < t1ms)]
            res = step_subaccount(adf, md, start_equity, params, end_ts_ms=t1ms, start_ts_ms=t0ms,
                                  entity_id=entity_id, fold_id=fold_id)
            fw.add_many(res["fills"]); ew.add_many(res["events"]); qw.add_many(res["equity"])
            pw.add_many(res["positions"])   # INCREMENT 1: per-position records (streamed, bounded)
            sm = res["summary"]; sm["outcome_states"] = ",".join(sm["outcome_states"])
            sw.add(sm)
    nf, ne, ns, nq, npos = fw.close(), ew.close(), sw.close(), qw.close(), pw.close()
    logger.info("M7 runner done: %d fills, %d events, %d summaries, %d equity, %d positions",
                nf, ne, ns, nq, npos)
    return {"fills": nf, "events": ne, "summaries": ns, "equity": nq, "positions": npos}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--actions", default=str(DATA_DIR / "m02_actions.parquet"))
    ap.add_argument("--shortlist", default=str(DATA_DIR / "m06a_shortlist.parquet"))
    ap.add_argument("--folds", default=str(DATA_DIR / "m03_folds.parquet"))
    ap.add_argument("--out", default=str(DATA_DIR))
    ap.add_argument("--band", default="base", choices=list(SLIP_BANDS))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--start-equity", type=float, default=10_000.0)
    ap.add_argument("--require-cache", action="store_true")
    ap.add_argument("--window", default="test", choices=["test", "pretest"])
    ap.add_argument("--slip-calib", default=None, help="path to slippage_calib_v11.json (D1)")
    ap.add_argument("--follower-trail", type=float, default=None,
                    help="follower trailing-exit threshold (e.g. 0.07 = flatten+halt on 7% copy-equity DD)")
    ap.add_argument("--copy-latency-ms", type=int, default=2_000,
                    help="copy entry lag in ms (2000=typical, 15000=P95 tail stress)")
    ap.add_argument(
        "--sizing-mode", choices=("leader_equity", "fixed_position"),
        default="fixed_position",
        help="fixed_position (default) = signed direction x --fixed-target-exposure. "
             "leader_equity is DEPRECATED and refuses to run (M1 remnant; the column it reads is "
             "100%% NULL in every store, so it silently emitted zero orders).",
    )
    ap.add_argument(
        "--fixed-target-exposure", type=float, default=0.10,
        help="Absolute follower exposure per open leader position in fixed_position mode.",
    )
    ap.add_argument("--copy-policy", choices=("full_mirror", "entry_trail"), default="full_mirror",
                    help="(a) full_mirror = copy every add/trim; (b) entry_trail = mirror ENTRY only, exit on OUR trailing-TP.")
    ap.add_argument("--trail-pct", type=float, default=0.15, help="entry_trail: trailing-TP retrace-from-peak threshold.")
    args = ap.parse_args()
    run_shortlist(Path(args.actions), Path(args.shortlist), Path(args.folds), Path(args.out),
                  band=args.band, limit_entities=args.limit, start_equity=args.start_equity,
                  require_cache=args.require_cache, window=args.window, slip_calib_path=args.slip_calib,
                  follower_trail=args.follower_trail, copy_latency_ms=args.copy_latency_ms,
                  sizing_mode=args.sizing_mode,
                  fixed_target_exposure=args.fixed_target_exposure,
                  copy_policy=args.copy_policy, trail_pct=args.trail_pct)


if __name__ == "__main__":
    main()
