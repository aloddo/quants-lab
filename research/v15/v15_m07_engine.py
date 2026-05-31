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
DEFAULT_HALF_SPREAD_BPS = 1.0
CAPACITY_PARTICIPATION_CAP = 0.05           # execution-reality depth cap (NOT allocation)

ISOLATED_ONLY_DEXES = {"xyz", "flx", "vntl", "hyna", "km", "abcd", "cash", "para"}


# --------------------------------------------------------------------------- #
# Coin / dex helpers
# --------------------------------------------------------------------------- #
def coin_dex(coin: str) -> str:
    """Margin-sharing SCOPE key (design D19): the DEX. main perps share cross within main; each HIP-3
    dex is its own scope."""
    return coin.split(":", 1)[0] if ":" in coin else "main"


def coin_is_spot(coin: str) -> bool:
    return coin.startswith("@") or coin == "USDC"


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
    """Precompute per-coin 1m (minute, open, high, low, close) arrays to .npy (page-cached, shared,
    read with mmap) so the engine inner loop reads marks/extremes with NO per-row Mongo (CLAUDE.md
    Key Rule 8). Idempotent: skips coins already cached unless force. Returns #coins written."""
    import pymongo
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    db = pymongo.MongoClient(mongo_uri)["quants_lab"]
    n = 0
    for coin in coins:
        p = out_dir / f"{_ulib.quote(coin, safe='')}.npy"
        if p.exists() and not force:
            continue
        cur = db.hyperliquid_candles.find(
            {"coin": coin, "interval": "1m"},
            projection={"timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1, "_id": 0},
        ).sort("timestamp_utc", 1)
        mins, o, h, lo, c = [], [], [], [], []
        for d in cur:
            t = d.get("timestamp_utc")
            if t is None:
                continue
            mins.append(int(t)); o.append(_f(d.get("open"))); h.append(_f(d.get("high")))
            lo.append(_f(d.get("low"))); c.append(_f(d.get("close")))
        arr = np.vstack([np.asarray(mins, "float64"), np.asarray(o, "float64"), np.asarray(h, "float64"),
                         np.asarray(lo, "float64"), np.asarray(c, "float64")]) if mins else np.empty((5, 0), "float64")
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
            self.base_taker = float(d.get("base_taker_oneway", DEFAULT_TAKER_FEE_ONEWAY))
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
        self._funding: dict[str, tuple] = {}
        self.meta = HLMeta().load()
        self.fees = FeeSchedule()

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
                return tuple(np.asarray(arr[i], dtype=("int64" if i == 0 else "float64")) for i in range(5))
            except Exception:
                pass
        if self._require_cache or not self._allow_mongo:
            return (np.empty(0, "int64"),) + tuple(np.empty(0, "float64") for _ in range(4))
        cur = self._mongo().hyperliquid_candles.find(
            {"coin": coin, "interval": "1m"},
            projection={"timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1, "_id": 0},
        ).sort("timestamp_utc", 1)
        mins, o, h, lo, c = [], [], [], [], []
        for d in cur:
            t = d.get("timestamp_utc")
            if t is None:
                continue
            mins.append(int(t)); o.append(_f(d.get("open"))); h.append(_f(d.get("high")))
            lo.append(_f(d.get("low"))); c.append(_f(d.get("close")))
        return (np.asarray(mins, "int64"), np.asarray(o, "float64"), np.asarray(h, "float64"),
                np.asarray(lo, "float64"), np.asarray(c, "float64"))

    def ohlc(self, coin: str) -> tuple:
        s = self._ohlc.get(coin)
        if s is None:
            s = self._load_ohlc(coin)
            self._ohlc[coin] = s
        return s

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
        mins, _o, h, low, c = self.ohlc(coin)
        if mins.size == 0:
            return {"adv": 0.0, "half_spread_bps": DEFAULT_HALF_SPREAD_BPS, "uncalibrated": True}
        end_key = (ts_ms // MS_MIN) * MS_MIN - MS_MIN     # last completed bar
        i = int(np.searchsorted(mins, end_key, side="right"))   # exclusive upper on completed bars
        lo = max(0, i - 1440)
        if i <= lo:
            return {"adv": 0.0, "half_spread_bps": DEFAULT_HALF_SPREAD_BPS, "uncalibrated": True}
        cc, hh, ll = c[lo:i], h[lo:i], low[lo:i]
        mean_px = np.nanmean(cc) if cc.size else float("nan")
        rng = np.nanmean((hh - ll) / np.where(cc == 0, np.nan, cc)) if cc.size else float("nan")
        half_spread_bps = float(np.clip((rng * 1e4) / 2.0 if rng == rng else DEFAULT_HALF_SPREAD_BPS,
                                        DEFAULT_HALF_SPREAD_BPS, 50.0))
        adv = float(mean_px) * 1440.0 if mean_px == mean_px else 0.0
        return {"adv": adv, "half_spread_bps": half_spread_bps, "uncalibrated": True}


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
    copy_latency_ms: int = 2_000
    slippage_band: str = "base"
    adl_stress: bool = False
    start_policy: str = "future_delta_only"   # future_delta_only | causal_carry_in (design D9)


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

    rows = actions.sort_values(["ts", "event_order"]).to_dict("records") if not actions.empty else []
    # risk cursor anchored at fold start (start_ts_ms) so start_state positions accrue risk even with
    # zero actions (codex code-r2 #2). Falls back to the first action time, then end_ts.
    if start_ts_ms is not None:
        prev_ts = int(start_ts_ms)
    elif rows:
        prev_ts = int(rows[0]["ts"]) + params.copy_latency_ms
    else:
        prev_ts = end_ts_ms

    # causal_carry_in: seed the opening position from the first action's pre-state (design D9)
    if rows and params.start_policy == "causal_carry_in":
        _seed_carry_in(st, md, rows[0], params, summary)

    for a in rows:
        coin = a["coin"]
        if coin_is_spot(coin):
            continue
        our_ts = int(a["ts"]) + params.copy_latency_ms

        _advance_between(st, md, prev_ts, our_ts, params, events, summary)
        if summary["ruin"]:
            break

        marks = _marks(st, md, our_ts, extra=coin)
        cur_eq = st.equity(marks)
        if cur_eq <= 0:
            _ruin(st, summary, events, our_ts)
            break

        mark = marks.get(coin)
        if mark is None or mark != mark or mark <= 0:
            summary["metadata_uncertain"] = True
            prev_ts = our_ts
            continue

        tgt_pct = _f(a.get("target_exposure_pct"))
        if tgt_pct != tgt_pct:
            prev_ts = our_ts
            continue
        target_notional = tgt_pct * cur_eq
        target_szi = target_notional / mark
        cur_szi = st.positions[coin].szi if coin in st.positions else 0.0
        if str(a.get("action_type", "")).upper() in ("EXIT", "CLOSE") or _f(a.get("position_after")) == 0.0:
            target_szi = 0.0
        delta_szi = target_szi - cur_szi

        _apply_order(st, md, coin, delta_szi, mark, our_ts, a, params, band, fills, events, summary)

        marks = _marks(st, md, our_ts, extra=coin)
        eq = st.equity(marks)
        peak_equity = max(peak_equity, eq)
        if peak_equity > 0:
            summary["max_dd"] = max(summary["max_dd"], (peak_equity - eq) / peak_equity)
        equity_samples.append(_eq_sample(entity_id, fold_id, our_ts, eq, "action", st, summary["max_dd"]))
        if eq <= 0:
            _ruin(st, summary, events, our_ts)
            break
        prev_ts = our_ts

    # FINAL-HORIZON RISK (design §1.7/codex code-r1 #1): advance funding + liquidation to fold end.
    # Runs even with zero actions so carried start_state positions are marked/funded/liquidated.
    if not summary["ruin"] and end_ts_ms > prev_ts:
        _advance_between(st, md, prev_ts, end_ts_ms, params, events, summary)
        marks = _marks(st, md, end_ts_ms)
        eq = st.equity(marks)
        peak_equity = max(peak_equity, eq)
        if peak_equity > 0:
            summary["max_dd"] = max(summary["max_dd"], (peak_equity - eq) / peak_equity)
        equity_samples.append(_eq_sample(entity_id, fold_id, end_ts_ms, eq, "fold_end", st, summary["max_dd"]))

    return _finalize(st, fills, events, equity_samples, summary, md, start_equity)


def _new_summary(entity_id, fold_id, start_equity, n_actions, params, md):
    return {
        "entity_id": entity_id, "fold_id": fold_id, "start_equity": float(start_equity),
        "n_actions": int(n_actions), "n_fills": 0, "n_rejected": 0, "n_capacity_capped": 0,
        "n_market_liq_orders": 0, "n_backstop_transfer": 0, "n_adl_stress": 0,
        "total_fees": 0.0, "total_funding": 0.0, "slip_bps_notional_sum": 0.0, "notional_traded": 0.0,
        "outcome_states": set(), "n_indeterminate_minutes": 0, "max_dd": 0.0, "time_to_ruin_ms": None,
        "slippage_band": params.slippage_band, "start_policy": params.start_policy,
        "slippage_uncalibrated": False, "metadata_uncertain": False, "mode_uncertain": False,
        "fee_unversioned": (not md.fees.versioned), "ruin": False,
    }


def _eq_sample(entity_id, fold_id, ts, eq, flag, st, dd):
    return {"entity_id": entity_id, "fold_id": fold_id, "ts": ts, "subaccount_equity": eq,
            "event_flag": flag, "n_open_positions": len(st.positions), "dd_from_peak": dd}


def _marks(st: AccountState, md: MarketData, ts_ms: int, extra: Optional[str] = None) -> dict:
    coins = set(st.positions) | ({extra} if extra else set())
    return {c: md.mark(c, ts_ms) for c in sorted(coins)}


def _seed_carry_in(st, md, first_row, params, summary):
    """causal_carry_in: seed an opening position to match the source's exposure that pre-exists the
    window. We back-fill the position to (position_after - signed_size) at the first action, sized to
    OUR equity by the same target fraction. Conservative: only seeds if the source enters the window
    already holding (carry_in_status present)."""
    status = str(first_row.get("carry_in_status", "") or "")
    if "carry" not in status.lower():
        return
    coin = first_row["coin"]
    if coin_is_spot(coin):
        return
    our_ts = int(first_row["ts"]) + params.copy_latency_ms
    mark = md.mark(coin, our_ts)
    if mark is None or mark != mark or mark <= 0:
        return
    post_pct = _f(first_row.get("target_exposure_pct"))    # SIGNED exposure% AFTER the first action
    pa = _f(first_row.get("position_after"))               # source signed size after the first action
    ss = _f(first_row.get("signed_size"))                  # the first action's own signed delta
    if post_pct == post_pct and pa == pa and ss == ss and pa != 0 and pa != ss:  # had a prior position
        eq = sum(st.cross_collateral.values())
        # source exposure% BEFORE the first observed action = post% scaled by the pre/post size ratio.
        # target_exposure_pct is already SIGNED, so do NOT multiply by sign(pa) (that double-flips
        # shorts -- codex code-r4 #3). The main loop's first order then trades to the post target.
        pre_pct = post_pct * (pa - ss) / pa
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


def _apply_order(st, md, coin, delta_szi, mark, our_ts, a, params, band, fills, events, summary):
    """Order lifecycle (design §4.4): lot round -> min-notional skip -> leverage/IM check -> capacity
    cap (execution depth, recorded) -> fill at slippage price -> versioned fee -> position+bucket
    update."""
    if abs(delta_szi) < 1e-15:
        return
    szdec = md.meta.szdec(coin)
    delta_szi = round(delta_szi, szdec)
    if abs(delta_szi) < 10 ** (-szdec):
        return
    delta_notional = abs(delta_szi) * mark
    if delta_notional < MIN_ORDER_NOTIONAL:
        return

    mode = default_margin_mode(coin)
    liq = md.liquidity(coin, our_ts)
    if liq.get("uncalibrated"):
        summary["slippage_uncalibrated"] = True
    adv = liq.get("adv", 0.0)
    capacity_capped = False
    if adv > 0:
        max_notional = CAPACITY_PARTICIPATION_CAP * adv
        if delta_notional > max_notional:
            delta_szi *= max_notional / delta_notional
            delta_szi = round(delta_szi, szdec)
            delta_notional = abs(delta_szi) * mark
            capacity_capped = True
            summary["n_capacity_capped"] += 1
            if delta_notional < MIN_ORDER_NOTIONAL:
                return

    participation = (delta_notional / adv) if adv > 0 else CAPACITY_PARTICIPATION_CAP
    impact_bps = DEFAULT_IMPACT_K_BPS * (participation ** DEFAULT_IMPACT_ALPHA) * band
    half_spread_bps = liq.get("half_spread_bps", DEFAULT_HALF_SPREAD_BPS) * band
    slip_bps = half_spread_bps + impact_bps
    side = 1 if delta_szi > 0 else -1
    fill_px = mark * (1 + side * slip_bps / 1e4)

    fee_rate = md.fees.taker(coin)
    fee = abs(delta_szi) * fill_px * fee_rate

    # pre-trade INITIAL-MARGIN admissibility (design §4.4; codex code-r2 #3). Only gate orders that
    # INCREASE exposure (reduces/closes free margin, always allowed). Reject if the required IM + fee
    # exceeds available bucket collateral -> prevents driving main negative on isolated opens and
    # caps cross leverage at the tier. Conservative: cross availability nets existing-position IM.
    cur_szi = st.positions[coin].szi if coin in st.positions else 0.0
    new_szi = cur_szi + delta_szi
    increasing = abs(new_szi) > abs(cur_szi) + 10 ** (-szdec)
    if increasing:
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

    _book_fill(st, md, coin, delta_szi, fill_px, mode, fee, summary)

    summary["n_fills"] += 1
    summary["total_fees"] += fee
    summary["notional_traded"] += delta_notional
    summary["slip_bps_notional_sum"] += slip_bps * delta_notional
    fills.append({
        "entity_id": summary["entity_id"], "fold_id": summary["fold_id"], "coin": coin,
        "our_ts": our_ts, "source_ts": int(a["ts"]), "side": "buy" if side > 0 else "sell",
        "our_fill_size": float(delta_szi), "our_fill_px": float(fill_px), "ref_mark": float(mark),
        "half_spread_bps": float(half_spread_bps), "impact_bps": float(impact_bps), "fee": float(fee),
        "fill_type": "normal", "capacity_capped": bool(capacity_capped), "margin_mode": mode,
    })


def _book_fill(st: AccountState, md: MarketData, coin: str, delta_szi: float, fill_px: float,
               mode: str, fee: float, summary: dict):
    """Update position + the right bucket. ISOLATED opens post initial margin from main cross bucket;
    isolated realized/funding/fees route to the position's isolated bucket; closing returns isolated
    margin + realized to main (design §1.2/§3.1; codex code-r1 #2)."""
    scope = coin_dex(coin)
    p = st.positions.get(coin)
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
        return

    if (p.szi >= 0) == (new_szi >= 0) and abs(new_szi) > abs(p.szi):
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
    elif (p.szi >= 0) != (new_szi >= 0):
        p.entry_px = fill_px      # flipped through zero
        if p.mode == "isolated":
            p.isolated_margin += realized - fee
        else:
            st.cross_collateral[scope] = st.cross_collateral.get(scope, 0.0) + realized - fee
    else:
        # reducing (same side, smaller): realize to bucket
        if p.mode == "isolated":
            p.isolated_margin += realized - fee
        else:
            st.cross_collateral[scope] = st.cross_collateral.get(scope, 0.0) + realized - fee
    p.szi = new_szi


def _apply_funding(st, md, h, summary):
    """Apply the hourly funding settlement to every open position (design §5/D10)."""
    for coin in sorted(st.positions):
        p = st.positions[coin]
        rate = md.funding_rate_at(coin, h)
        px = md.mark(coin, h, causal=True)
        if px is None or px != px or rate == 0.0:
            continue
        fund = -(p.szi * px * rate)         # long pays positive rate
        if p.mode == "cross":
            st.cross_collateral[coin_dex(coin)] = st.cross_collateral.get(coin_dex(coin), 0.0) + fund
        else:
            p.isolated_margin += fund
        p.cum_funding += -fund
        summary["total_funding"] += fund


def _advance_between(st, md, t0_ms, t1_ms, params, events, summary):
    """CHRONOLOGICAL event loop over (t0,t1] (codex code-r2 #1/#5): interleave hourly funding
    settlements with repeated maintenance-breach scans + liquidations, in time order. A position
    liquidated early stops accruing later funding; a partial-liquidation survivor is rescanned to
    fold end; the >100k 20%-then-30s-cooldown ladder advances bar-by-bar (the cursor jumps past the
    cooldown so the next scan does the full close)."""
    if t0_ms is None or t1_ms <= t0_ms:
        return
    cursor = t0_ms
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
        else:
            # no breach this segment -> account healthy. Once the cooldown has elapsed, reset it so a
            # later, independent breach starts fresh with a 20%-first step (not an immediate full close).
            if st.cooldown_until_ms and st.cooldown_until_ms <= cursor:
                st.cooldown_until_ms = 0
            cursor = seg_end
            if seg_end == next_funding and seg_end <= t1_ms:
                _apply_funding(st, md, seg_end, summary)
                # POST-FUNDING solvency check (codex code-r4 #1): a settlement can push below
                # maintenance; liquidate now (the cooldown guard in _market_liquidate_scope prevents
                # any same-ts/within-30s double-fire on every caller path -- codex code-r5/r6/r7).
                _check_maint_at(st, md, seg_end, params, events, summary)
    # FOLD-BOUNDARY solvency: final point-in-time maintenance check at t1 (codex code-r4 #1). The
    # cooldown guard makes this safe even if a post-funding liquidation just fired within 30s of t1.
    if st.positions and not summary["ruin"]:
        _check_maint_at(st, md, t1_ms, params, events, summary)


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
            del st.positions[coin]
        elif iso_eq < maint:
            _liq_close(st, md, coin, m, ts_ms, summary=summary)

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
                del st.positions[c]
            st.cross_collateral[scope] = 0.0
            summary["n_backstop_transfer"] += 1
            summary["outcome_states"].add("backstop")
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
    """Close (part of) a position as a forced market-liq order FILL with the forced-liq slippage.
    Appends the fill to summary['_fills_ref'] (the engine's fills list)."""
    p = st.positions.get(coin)
    if p is None:
        return
    if close_szi is None:
        close_szi = -p.szi
    side = 1 if close_szi > 0 else -1
    fill_px = mark * (1 + side * FORCED_LIQ_SLIP_BPS / 1e4)
    # realized PnL on the closed portion (forced-liq order; no fee, design D12)
    realized = min(abs(close_szi), abs(p.szi)) * (fill_px - p.entry_px) * (1 if p.szi > 0 else -1)
    if p.mode == "cross":
        sc = scope or coin_dex(coin)
        st.cross_collateral[sc] = st.cross_collateral.get(sc, 0.0) + realized
    else:
        p.isolated_margin += realized
    new_szi = p.szi + close_szi
    fl = summary.get("_fills_ref")
    if fl is not None:
        fl.append({"entity_id": summary["entity_id"], "fold_id": summary["fold_id"], "coin": coin,
                   "our_ts": ts_ms, "source_ts": ts_ms, "side": "buy" if side > 0 else "sell",
                   "our_fill_size": float(close_szi), "our_fill_px": float(fill_px), "ref_mark": float(mark),
                   "half_spread_bps": 0.0, "impact_bps": float(FORCED_LIQ_SLIP_BPS), "fee": 0.0,
                   "fill_type": "market_liq_order", "capacity_capped": False, "margin_mode": p.mode})
    summary["n_market_liq_orders"] += 1
    summary["outcome_states"].add("position_liquidated")
    if abs(new_szi) < 10 ** (-md.meta.szdec(coin)):
        if p.mode == "isolated":
            st.cross_collateral["main"] = st.cross_collateral.get("main", 0.0) + max(0.0, p.isolated_margin)
        del st.positions[coin]
    else:
        p.szi = new_szi


def _evt(summary, ts_ms, etype, coin, scope):
    return {"entity_id": summary["entity_id"], "fold_id": summary["fold_id"], "ts": ts_ms,
            "event_type": etype, "coin": coin, "scope": scope}


def _ruin(st: AccountState, summary: dict, events: list, ts_ms: int):
    summary["ruin"] = True
    summary["outcome_states"].add("account_ruin")
    if summary["time_to_ruin_ms"] is None:
        summary["time_to_ruin_ms"] = ts_ms
    events.append(_evt(summary, ts_ms, "account_ruin", "", ""))
    st.positions.clear()
    for k in list(st.cross_collateral):
        st.cross_collateral[k] = 0.0


def _finalize(st, fills, events, equity_samples, summary, md, start_equity):
    summary.pop("_fills_ref", None)     # internal handle; never written to the summary parquet
    final_eq = equity_samples[-1]["subaccount_equity"] if equity_samples else float(start_equity)
    final_eq = max(0.0, final_eq)   # isolation invariant: equity floored at 0
    summary["final_equity"] = float(final_eq)
    summary["roe_engine"] = (final_eq / start_equity - 1.0) if start_equity > 0 else 0.0
    summary["slip_bps_notional_weighted"] = (
        summary["slip_bps_notional_sum"] / summary["notional_traded"] if summary["notional_traded"] > 0 else 0.0)
    if not summary["outcome_states"]:
        summary["outcome_states"].add("survived")
    summary["outcome_states"] = sorted(summary["outcome_states"])
    ending_state = {"cross_collateral": dict(st.cross_collateral),
                    "positions": {c: vars(p) for c, p in st.positions.items()},
                    "cooldown_until_ms": st.cooldown_until_ms}
    return {"fills": fills, "events": events, "equity": equity_samples,
            "ending_account_state": ending_state, "summary": summary}


# --------------------------------------------------------------------------- #
# Runner — pre-shard actions by wallet (no per-seat full-file scan), streaming out (design §8)
# --------------------------------------------------------------------------- #
def run_shortlist(actions_path: Path, shortlist_path: Path, folds_path: Path, out_dir: Path,
                  band: str = "base", limit_entities: Optional[int] = None, start_equity: float = 10_000.0,
                  flush_rows: int = 1_000_000, require_cache: bool = True):
    import shutil
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _streaming_io import ShardedParquetWriter, install_memory_guard
    import pyarrow.dataset as ds

    install_memory_guard(soft_gb=12, label="m07")
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    folds = pd.read_parquet(folds_path)
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
    # memory-bounded (no per-wallet writer held open buffering rows), FRESH dir each run (no stale
    # shard contamination). Hive-partitioned by wallet.
    shard_dir = out_dir / "_m07_wallet_shards"
    if shard_dir.exists():
        shutil.rmtree(shard_dir)
    shard_dir.mkdir(parents=True, exist_ok=True)
    dataset = ds.dataset(actions_path, format="parquet")
    cols = ["wallet", "coin", "ts", "event_order", "action_type", "signed_size", "position_after",
            "target_exposure_pct", "is_liquidation", "carry_in_status"]
    scanner = dataset.scanner(columns=cols, filter=ds.field("wallet").isin(wallets), batch_size=200_000)
    ds.write_dataset(scanner, shard_dir, format="parquet", partitioning=["wallet"],
                     partitioning_flavor="hive", existing_data_behavior="overwrite_or_ignore",
                     max_rows_per_file=2_000_000, max_rows_per_group=200_000)
    shard_ds = ds.dataset(shard_dir, format="parquet", partitioning="hive")

    # PRELOAD market caches for the shortlist coin set so the inner loop never hits Mongo (codex
    # code-r2 #7). Build the OHLC cache (close-only marks_cache is insufficient) if missing.
    coins = set()
    for b in dataset.scanner(columns=["coin", "wallet"], filter=ds.field("wallet").isin(wallets),
                             batch_size=500_000).to_batches():
        coins.update(c for c in b.column("coin").to_pylist() if c and not coin_is_spot(c))
    logger.info("M7 runner: preloading OHLC+funding caches for %d coins", len(coins))
    build_ohlc_cache(sorted(coins))
    md = MarketData(require_cache=require_cache)
    for c in sorted(coins):           # warm funding series into memo (one Mongo pass each, precompute)
        md.funding_series(c)

    fw = ShardedParquetWriter(out_dir / "m07_fills.parquet", flush_rows=flush_rows)
    ew = ShardedParquetWriter(out_dir / "m07_events.parquet", flush_rows=flush_rows)
    sw = ShardedParquetWriter(out_dir / "m07_summary.parquet", flush_rows=200_000)
    params = EngineParams(slippage_band=band)

    for w in wallets:
        try:
            wdf = shard_ds.to_table(filter=ds.field("wallet") == w).to_pandas()
        except Exception:
            continue
        if wdf.empty:
            continue
        for entity_id, fold_id in sorted(seats_by_wallet.get(w, [])):
            t0ms, t1ms = fold_win[fold_id]
            adf = wdf[(wdf.ts >= t0ms) & (wdf.ts < t1ms)]
            res = step_subaccount(adf, md, start_equity, params, end_ts_ms=t1ms, start_ts_ms=t0ms,
                                  entity_id=entity_id, fold_id=fold_id)
            fw.add_many(res["fills"]); ew.add_many(res["events"])
            sm = res["summary"]; sm["outcome_states"] = ",".join(sm["outcome_states"])
            sw.add(sm)
    nf, ne, ns = fw.close(), ew.close(), sw.close()
    logger.info("M7 runner done: %d fills, %d events, %d summaries", nf, ne, ns)
    return {"fills": nf, "events": ne, "summaries": ns}


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
    args = ap.parse_args()
    run_shortlist(Path(args.actions), Path(args.shortlist), Path(args.folds), Path(args.out),
                  band=args.band, limit_entities=args.limit, start_equity=args.start_equity,
                  require_cache=args.require_cache)


if __name__ == "__main__":
    main()
