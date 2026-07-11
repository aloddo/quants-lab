#!/usr/bin/env python3
"""V15 M01 — Whole-account Hyperliquid PERP equity reconstructor.

This is the V15 foundation module. It reconstructs each source wallet's
WHOLE-ACCOUNT perp equity (summed across every markable perp dex), re-anchored
to HL's true weekly ``portfolio.perpAllTime`` accountValue, and decomposes the
result into a flow-neutral trading-ROE plus per-wallet accuracy diagnostics.

It is a NEW file: it does NOT modify any running strategy. It adapts the proven
re-anchoring forward walk and ledger-cash-delta logic from
``scripts/v13_equity_reconstruct_v8.py`` but corrects the two structural flaws
of that module for V15:

  (A) WHOLE-ACCOUNT instead of MAIN-only. perpAllTime is the SUM of per-dex
      clearinghouseState marginSummary.accountValue across every perp dex
      (verified live 2026-05-30 on 0x2fcb6898: main 591818 + xyz 2357394 =
      2949212 = last perpAllTime point). v8 reconstructed MAIN only while
      anchoring to a whole-account number -> drift for multi-dex wallets. Here
      we reconstruct ALL perp dexes (main + every "<dex>:" prefix incl xyz/cash/
      hyna/para/vntl/flx/km), value positions with per-coin marks, and only
      EXCLUDE spot (@-tokens, USDC). Unmarkable coins are flagged (residual
      notional) and gate the day, not silently dropped.

  (B) FLOW-NEUTRAL ROE. We emit a cumulative external-capital flow series
      (``ext_flow_cum``) so downstream modules can compute
      ``trading_pnl = equity_change - ext_flow`` and a flow-neutral return.

CORE MODEL (verified, brain projects/quant/v15/equity-model-verified):
    equity = cash + Sum_coin signed_pos * mark_at(t)
    perpAllTime(t) = sum over perp dexes of marginSummary.accountValue
    (collateral + uPnL, perp-only; spot + staking EXCLUDED).

METHOD (re-anchored forward walk, adapted from v8 compute_eq_at):
    For each weekly perpAllTime anchor (t_a, eq_a):
      seed pre-anchor positions; snap cash := eq_a - Sum pos*mark(t_a);
      walk the merged fill/ledger/funding event stream in (t_a, t]:
        fill   -> cash += -signed_sz*px - fee;
                  pos[coin] += signed_sz
        ledger -> cash += taxonomy_cash_delta; ext_flow_cum += neutralize_delta
        funding-> cash += usdc
      equity(t) = cash + Sum pos*mark(t).
    Emit an EOD series (one row per UTC day), using the latest weekly anchor
    at-or-before EOD. Days with any missing mark are flagged, not silently
    corrupted.

DATA SOURCES (all LOCAL except the API anchor + today cross-check):
  - Anchor truth (weekly): HL API portfolio.perpAllTime accountValueHistory.
  - Today cross-check: HL API clearinghouseState per dex (marginSummary +
    assetPositions).
  - Anchor positions seed: app/data/v13/wallet_anchor_state.parquet.
  - Fills: app/data/hl_s3_fills_v2/YYYYMMDD.parquet (full enriched schema).
  - Funding: app/data/v13/raw_funding_cache_20k/{wallet}_*.json (S3 bulk).
  - Ledger: app/data/v13/raw_ledger_cache_20k/{wallet}_*.json.
  - Marks: Mongo hyperliquid_candles (1m, s3_reconstructed); close at-or-before
    ts. Candles END 2026-05-23 13:55 UTC -> window end clamped to 2026-05-23.

OUTPUTS (parquet, one row per wallet-DAY):
    wallet, date, equity_usd, cash, position_value_usd,
    gross_position_notional_usd, n_positions,
    ext_flow_cum (true series cumulative), segment_ext_flow (per-anchor segment),
    n_unmarkable_positions, unmarkable_notional_usd, anchor_age_h,
    return_since_anchor_pct, recon_incomplete, has_liquidation_in_day,
    source_dexes
  Plus a per-wallet ``.audit.parquet``:
    wallet, n_fills, n_funding, n_ledger, n_weekly_anchors_in_window,
    max_inter_anchor_drift_pct, median_inter_anchor_drift_pct,
    n_segments_reconciled, max_segment_reconcile_err_usd, today_crosscheck_pct,
    n_position_mismatches, unknown_ledger_types, quarantined, ...

CLI:
    python v15_m01_equity_reconstruct.py --wallets-file W.txt \
        --start 2025-12-01 --end 2026-05-23 --output out.parquet [--validate]

The ``--validate`` mode prints a per-wallet accuracy table (inter-anchor drift,
segment realized-PnL reconcile error, today cross-check, position mismatches)
and does NOT require writing the full series.
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import math

import numpy as np
import pandas as pd
import pymongo
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _streaming_io import ShardedParquetWriter, install_memory_guard, plan_memory_budget  # noqa: E402

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("v15_m01")

HL_URL = "https://api.hyperliquid.xyz/info"
S3_FILLS_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_fills_v2")
S3_BY_WALLET_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_fills_v2_by_wallet")
FUNDING_DIR = Path("/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k")
LEDGER_DIR = Path("/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k")
# Self-healing parquet fallback sources (2026-07-10 fix): the JSON per-wallet caches above are written
# by v13_s3_ledger_downloader but were EMPTY, so m01 silently reconstructed with zero funding/ledger.
# When a wallet has no JSON cache we read the raw parquet directly (per-wallet funding_cache + the
# monolithic ledger table) and reshape to the SAME record contract the JSON loaders emit. This makes
# m01 self-sufficient: a dead JSON cache can never again silently zero the funding/ledger input.
FUNDING_PARQUET_DIR = Path("/Users/hermes/quants-lab/app/data/v13/funding_cache")
LEDGER_PARQUET = Path("/Users/hermes/quants-lab/app/data/raw/ledger/ledger_20k.parquet")
# Delta fields ledger_cash_delta() reads; the parquet 'coin' column carries the token (mapped to delta['token']).
_LEDGER_DELTA_FIELDS = (
    "usdc", "usdcValue", "amount", "fee", "user", "destination",
    "sourceDex", "destinationDex", "toPerp", "netWithdrawnUsd", "operation",
    "dex",  # ledger_cash_delta reads delta['dex'] for activateDexAbstraction (codex m01fix)
)
_LEDGER_PARQUET_CACHE = {"df": None}  # lazy single load of the 200MB monolith, filtered per wallet
ANCHOR_PARQUET = Path("/Users/hermes/quants-lab/app/data/v13/wallet_anchor_state.parquet")
MONGO_URI = "mongodb://localhost:27017/"


def _num(v, default: float = 0.0) -> float:
    """NaN-safe numeric coercion. The idiom float(x or 0) does NOT sanitize parquet nulls: np.nan is
    truthy so `nan or 0` -> nan -> float(nan)=nan, silently poisoning equity/PnL sums. Returns default
    for None/NaN/inf/unparseable; otherwise the finite float. (2026-07-10 fail-closed fix.)"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return f if math.isfinite(f) else default

# ALL perp dexes are in scope (Alberto 2026-05-30). Any coin with a "<dex>:" prefix
# (or unprefixed main) is reconstructed; flx and km are INCLUDED. Marks come from a
# prefix-agnostic Mongo lookup. Only spot (@-tokens, USDC) is excluded. Unmarkable
# coins are flagged (and gate the day), never silently dropped.
DROPPED_DEX_PREFIXES: tuple[str, ...] = ()  # ALL dexes in scope (Alberto 2026-05-30): nothing dropped, incl flx:

# Liquidation `dir` values in the S3 fills (brain s3-data-reference + spec).
LIQUIDATION_DIRS = frozenset(
    {
        "Liquidated Cross Long",
        "Liquidated Cross Short",
        "Liquidated Isolated Long",
        "Liquidated Isolated Short",
        "Backstop Borrow Liquidation",
        "Partial Borrow Liquidation",
        "Auto-Deleveraging",
    }
)

# Ledger delta types that represent EXTERNAL CAPITAL / non-copyable flow. These
# STILL move cash in the equity walk, but we ALSO accumulate them into
# ext_flow_cum so downstream can neutralize them out of trading ROE.
# (Per brain m01-codex-consensus LEDGER-DELTA FLOW TAXONOMY.)
EXT_FLOW_TYPES = frozenset(
    {
        "deposit",
        "withdraw",
        "accountClassTransfer",
        "send",
        "internalTransfer",
        "subAccountTransfer",
        "vaultDeposit",
        "vaultWithdraw",
        "vaultCreate",
        "vaultDistribution",
        "vaultLeaderCommission",
        "borrowLend",  # only the supply/withdraw legs carry cash (see delta fn)
        "rewardsClaim",
        "accountActivationGas",
        "activateDexAbstraction",
        "deployGasAuction",
        "gossipPriorityGasAuction",
    }
)

# Ledger delta types with NO perp-equity impact (zero cash delta). Listing them
# explicitly means they never fall through to the "unknown type" quarantine.
ZERO_LEDGER_TYPES = frozenset(
    {
        "spotTransfer",
        "cStakingTransfer",
        "spotGenesis",
        # Informational liquidation summary. Liquidation position/cash effects
        # are already represented by the corresponding fills and their fees.
        "liquidation",
        # staking / validator layer (do not touch USDC perp account):
        "CDeposit",
        "CWithdrawal",
        "Delegation",
        "ValidatorRewards",
        "GossipPriorityAuctionRestart",
    }
)


# --------------------------------------------------------------------------- #
# Worker-process globals (Mongo client + mark cache, lazily created per process)
# --------------------------------------------------------------------------- #

_mongo: Optional[pymongo.database.Database] = None
_mark_cache: dict[tuple[str, int], Optional[float]] = {}
# Per-coin in-memory 1m close series for ASOF lookups (replaces per-action Mongo round-trips, which
# were ~94% of M2 wall time: 1.3M find_one calls / 10 wallets). Each coin loaded ONCE (one indexed
# range query) into sorted (minutes, closes) numpy arrays; get_mark then does a binary-search asof.
# Bounded memory: only coins actually touched, one series each. (Perf decision 2026-05-31.)
_mark_series: dict[str, tuple] = {}


def get_mongo() -> pymongo.database.Database:
    global _mongo
    if _mongo is None:
        _mongo = pymongo.MongoClient(MONGO_URI)["quants_lab"]
    return _mongo


def get_mark(coin: str, ts_ms: int, causal: bool = False) -> Optional[float]:
    """1m candle close at-or-before ts_ms (latest). None if no candle covers it.

    Works for any prefix (main + xyz:/cash:/hyna:/para:/vntl:/flx:/km:) since the
    candle ``coin`` carries the full prefixed symbol.

    EXACT-MARK FIX (2026-06-04, Alberto "reconcile to the cent"): for EXOTIC HIP-3
    coins (prefixed dexes xyz:/cash:/flx:/vntl:/km:/para:/hyna:) the trade-candle close
    diverges materially from HL's setOracle mark (observed xyz:SILVER -12.8%, xyz:GOLD
    -4.7% on 2026-03-04) -> on leveraged positions this is a dominant equity-recon drift
    source. HL computes accountValue on the MARK (oracle), not the last trade. So we now
    PREFER the oracle mark (hyperliquid_oracle, from v13_extract_oracle_marks setOracle)
    for prefixed coins, with a candle fallback when oracle is absent/stale. Main-dex
    (unprefixed) coins keep the trade-candle close (their oracle is consensus-level, not
    in explorer_blocks; main mark precision tracked separately via asset_ctxs).

    Set causal=True for forward decisions: use the last one-minute bar whose
    close time is <= ts_ms, matching M07's convention (bar_open <= ts - 60s).
    Keep causal=False for point-in-time MTM where the mark genuinely existed at ts_ms.
    """
    lookup_ms = (ts_ms // 60_000) * 60_000 - 60_000 if causal else ts_ms
    # EXACT MARK first: exotic (prefixed) coins -> setOracle oracle; MAIN (unprefixed) coins ->
    # asset_ctxs mark_px (HL's exact per-minute mark, the price accountValue uses). Candle fallback.
    if ":" in coin:
        # HL values accountValue/uPnL at the MARK price (markPxs[0]); ORACLE px is FUNDING-ONLY
        # and is WRONG for m2m (robust-price-indices doc; Alberto, repeatedly). markPx ONLY here --
        # NO oracle in the m2m path. If markPx is absent, fall through to the trade-candle last
        # resort (still imperfect on thin books, but never the funding oracle).
        mkpx = _markpx_lookup(coin, lookup_ms, ORACLE_MAX_AGE_MS)
        if mkpx is not None:
            return mkpx
    else:
        mpx = _assetctx_lookup(coin, lookup_ms, ASSETCTX_MAX_AGE_MS)
        if mpx is not None:
            return mpx
    minute_key = lookup_ms // 60_000 * 60_000
    mins, closes = _coin_series(coin)
    if mins.size == 0:
        return None
    i = int(np.searchsorted(mins, minute_key, side="right")) - 1  # latest candle minute <= minute_key
    if i < 0:
        return None
    # STALENESS CAP (codex marks-gate 2026-06-24): the candle fallback is at-or-before with NO age
    # bound, so a candle from hours ago was returned as if current -> silently-stale MTM that callers
    # treat as a live mark (markable_all=True). Cap the as-of age: if the nearest prior 1m bar is older
    # than CANDLE_MAX_AGE_MS, return None so the caller flags it (degraded / NO_ANCHOR) instead of
    # marking on a stale price. With dense all-market candles this rarely triggers; it only catches the
    # genuinely-dormant-coin case the cap is meant to surface.
    if minute_key - int(mins[i]) > CANDLE_MAX_AGE_MS:
        return None
    px = closes[i]
    return None if not np.isfinite(px) else float(px)  # NaN/inf-safe (codex m01: inf mark -> None)


# Candle-fallback staleness cap (codex marks-gate 2026-06-24). The 1m candle close is an at-or-before
# lookup; without a bound a stale bar (delisted/dormant coin, or a data gap) is returned as a live mark.
# Set to 15min to MATCH ASSETCTX_MAX_AGE_MS (codex recheck 2026-06-24: prefer 15min for a deploy-driving
# reconstruction; 60min still allows materially-stale MTM on a fast-moving thin coin). With dense
# all-market 1m candles a held coin has a bar most minutes, so this fires only on genuine dormancy/gaps
# -> caller flags degraded/NO_ANCHOR. The pre/post-05-24 coverage+staleness AUDIT validates the cap does
# not over-flag thin HIP-3 instruments; bump back toward 60min only if the audit shows false positives.
CANDLE_MAX_AGE_MS = 15 * 60 * 1000  # 15min (matches ASSETCTX_MAX_AGE_MS)

# Equity-dependent selection requires repeated out-of-sample anchor checks.
# One or zero segments is not enough evidence that the reconstruction is
# stable; such wallets remain usable only in the equity-independent lane.
MIN_INTER_ANCHOR_CHECKS = 2


# Oracle/setOracle series, per-coin in-memory asof cache. ``mark_px`` is the
# exact account-value mark when observed, but a sparse historical extraction is
# not permission to reuse it for hours. The 30-wallet multi-dex sensitivity
# showed 15 minutes weakly dominated 6 hours on inter-anchor drift (6 improved,
# 0 worsened for median drift; 7 improved, 0 worsened for max drift). Older
# observations therefore fall back to the contemporaneous trade candle.
_oracle_series: dict[str, tuple] = {}
ORACLE_MAX_AGE_MS = 15 * 60 * 1000


def _load_oracle_from_mongo(coin: str) -> tuple:
    db = get_mongo()
    cur = db.hyperliquid_oracle.find(
        {"coin": coin}, projection={"timestamp_utc": 1, "oracle_px": 1, "_id": 0}
    )
    pts: list[tuple[int, float]] = []
    for d in cur:
        t = d.get("timestamp_utc")
        p = d.get("oracle_px")
        if t is None or p is None:
            continue
        try:
            pts.append((int(t), float(p)))
        except (TypeError, ValueError):
            continue
    pts.sort(key=lambda x: x[0])
    if not pts:
        return (np.empty(0, dtype="int64"), np.empty(0, dtype="float64"))
    return (np.asarray([x[0] for x in pts], dtype="int64"),
            np.asarray([x[1] for x in pts], dtype="float64"))


def _oracle_lookup(coin: str, ts_ms: int, max_age_ms: int) -> Optional[float]:
    s = _oracle_series.get(coin)
    if s is None:
        s = _load_oracle_from_mongo(coin)
        _oracle_series[coin] = s
    ts_arr, px = s
    if ts_arr.size == 0:
        return None
    i = int(np.searchsorted(ts_arr, ts_ms, side="right")) - 1  # latest oracle point <= ts_ms
    if i < 0:
        return None
    if ts_ms - int(ts_arr[i]) > max_age_ms:  # too stale -> let caller fall back to candle
        return None
    v = px[i]
    return None if not np.isfinite(v) else float(v)


# setOracle MARK price (markPxs[0]) -- the price HL uses for accountValue/uPnL (oracle is funding-only).
# Mirrors _oracle_lookup; reads the mark_px field written by scripts/pull_oracle_targeted.py.
_markpx_series: dict[str, tuple] = {}


def _load_markpx_from_mongo(coin: str) -> tuple:
    db = get_mongo()
    cur = db.hyperliquid_oracle.find(
        {"coin": coin, "mark_px": {"$ne": None}},
        projection={"timestamp_utc": 1, "mark_px": 1, "_id": 0},
    )
    pts: list[tuple[int, float]] = []
    for d in cur:
        t = d.get("timestamp_utc")
        p = d.get("mark_px")
        if t is None or p is None:
            continue
        try:
            pts.append((int(t), float(p)))
        except (TypeError, ValueError):
            continue
    pts.sort(key=lambda x: x[0])
    if not pts:
        return (np.empty(0, dtype="int64"), np.empty(0, dtype="float64"))
    return (np.asarray([x[0] for x in pts], dtype="int64"),
            np.asarray([x[1] for x in pts], dtype="float64"))


def _markpx_lookup(coin: str, ts_ms: int, max_age_ms: int) -> Optional[float]:
    s = _markpx_series.get(coin)
    if s is None:
        s = _load_markpx_from_mongo(coin)
        _markpx_series[coin] = s
    ts_arr, px = s
    if ts_arr.size == 0:
        return None
    i = int(np.searchsorted(ts_arr, ts_ms, side="right")) - 1
    if i < 0:
        return None
    if ts_ms - int(ts_arr[i]) > max_age_ms:
        return None
    v = px[i]
    return None if not np.isfinite(v) else float(v)


# asset_ctxs EXACT mark for MAIN coins: per-coin (minute_ms, mark_px) .npy written by
# scripts/extract_asset_ctx_marks.py. 1-min granularity -> tight staleness cap. Mirrors _coin_series.
_assetctx_series: dict[str, tuple] = {}
ASSETCTX_MAX_AGE_MS = 15 * 60 * 1000  # 15min (asset_ctxs is 1-min; small cap)
ASSETCTX_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15" / "assetctx_marks"


def _assetctx_lookup(coin: str, ts_ms: int, max_age_ms: int) -> Optional[float]:
    s = _assetctx_series.get(coin)
    if s is None:
        p = ASSETCTX_DIR / f"{_ulib.quote(coin, safe='')}.npy"
        if p.exists():
            try:
                arr = np.load(p, mmap_mode="r")
                s = (np.asarray(arr[0], dtype="int64"), np.asarray(arr[1], dtype="float64"))
            except Exception:  # noqa: BLE001
                s = (np.empty(0, dtype="int64"), np.empty(0, dtype="float64"))
        else:
            s = (np.empty(0, dtype="int64"), np.empty(0, dtype="float64"))
        _assetctx_series[coin] = s
    ts_arr, px = s
    if ts_arr.size == 0:
        return None
    i = int(np.searchsorted(ts_arr, ts_ms, side="right")) - 1
    if i < 0:
        return None
    if ts_ms - int(ts_arr[i]) > max_age_ms:
        return None
    v = px[i]
    return None if not np.isfinite(v) else float(v)


# Earliest ms at which a coin has ANY price (candle minute, else oracle ts). A coin cannot be
# held before it has a price (its listing) -> used to bound phantom position back-projection in
# seed_positions. None = no price source ever. Cached.
_coin_first_price: dict[str, Optional[int]] = {}


def _coin_first_price_ms(coin: str) -> Optional[int]:
    if coin in _coin_first_price:
        return _coin_first_price[coin]
    fp: Optional[int] = None
    mins, _ = _coin_series(coin)
    if mins.size:
        fp = int(mins[0])
    if fp is None:  # no candle -> try oracle series
        oser = _oracle_series.get(coin)
        if oser is None:
            oser = _load_oracle_from_mongo(coin)
            _oracle_series[coin] = oser
        ts_arr, _ = oser
        if ts_arr.size:
            fp = int(ts_arr[0])
    _coin_first_price[coin] = fp
    return fp


import urllib.parse as _ulib

MARKS_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15" / "marks_cache"


def _coin_cache_path(coin: str) -> Path:
    return MARKS_CACHE_DIR / f"{_ulib.quote(coin, safe='')}.npy"


def _load_coin_from_mongo(coin: str) -> tuple:
    db = get_mongo()
    cur = db.hyperliquid_candles.find(
        {"coin": coin, "interval": "1m"},
        projection={"timestamp_utc": 1, "close": 1, "_id": 0},
    ).sort("timestamp_utc", 1)
    mins_l: list[int] = []
    closes_l: list[float] = []
    for d in cur:
        t = d.get("timestamp_utc")
        if t is None:
            continue
        c = d.get("close")
        mins_l.append(int(t))
        closes_l.append(float(c) if c is not None else float("nan"))
    return (np.asarray(mins_l, dtype="int64"), np.asarray(closes_l, dtype="float64"))


def _marks_manifest_path(out_dir: Path) -> Path:
    return out_dir / "_manifest.json"


def marks_cache_status(out_dir: Path | None = None) -> dict:
    """Cheap freshness check: compares the cached coin-set against Mongo's current 1m coin-set + the
    latest 1m candle minute (indexed). Returns {fresh, age_days, reason}. (codex perf-r1 #3)"""
    import json as _json
    out_dir = Path(out_dir) if out_dir else MARKS_CACHE_DIR
    mp = _marks_manifest_path(out_dir)
    if not mp.exists():
        return {"fresh": False, "age_days": None, "reason": "no manifest"}
    man = _json.loads(mp.read_text())
    db = get_mongo()
    cur_coins = set(db.hyperliquid_candles.distinct("coin", {"interval": "1m"}))
    # latest 1m candle minute overall (index-backed via the coin+interval+ts index is per-coin; use a
    # bounded find sorted desc which the planner serves from the compound index).
    latest = db.hyperliquid_candles.find_one({"interval": "1m"}, sort=[("timestamp_utc", -1)],
                                             projection={"timestamp_utc": 1, "_id": 0})
    cur_max = int(latest["timestamp_utc"]) if latest else 0
    cur_total = int(db.hyperliquid_candles.estimated_document_count())  # O(1) metadata; catches backfills
    import time as _t
    age_days = (_t.time() - man.get("built_unix", 0)) / 86400.0
    if set(man.get("coins", [])) != cur_coins:
        return {"fresh": False, "age_days": age_days, "reason": f"coin set changed ({len(cur_coins)} now vs {len(man.get('coins', []))} cached)"}
    if cur_max > int(man.get("max_minute", 0)):
        return {"fresh": False, "age_days": age_days, "reason": f"new candles since build (max {cur_max} > {man.get('max_minute')})"}
    # codex perf-r2 #3: total-count signature catches INTERIOR backfills (gap fills / corrections that
    # add docs) that do NOT advance the global max minute. (In-place value overwrites that preserve
    # doc count remain undetectable cheaply -> use --rebuild-marks-cache after any such correction.)
    if "total_candles" not in man:
        return {"fresh": False, "age_days": age_days, "reason": "manifest missing total_candles; rebuild required"}
    if cur_total != int(man["total_candles"]):
        return {"fresh": False, "age_days": age_days, "reason": f"candle count changed ({cur_total} vs {man['total_candles']}) -> possible backfill"}
    return {"fresh": True, "age_days": age_days, "reason": "coin-set + max-minute + count match"}


def build_marks_cache(out_dir: Path | None = None, force: bool = False) -> int:
    """PRECOMPUTE ONCE (single process, one Mongo pass per coin): write each coin's 1m (minute,close)
    series to a local .npy so the worker pool reads marks from local files instead of each of N
    workers re-scanning Mongo (the 74%-idle I/O stall). Returns #coins written. Idempotent: skips
    coins already cached unless force. Writes a manifest for freshness validation. Unique temp names
    so concurrent builds never clobber. (Perf decision 2026-05-31; mandatory streaming-io sibling.)"""
    import json as _json
    import time as _t
    out_dir = Path(out_dir) if out_dir else MARKS_CACHE_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    db = get_mongo()
    coins = db.hyperliquid_candles.distinct("coin", {"interval": "1m"})
    n = 0
    max_minute = 0
    for coin in coins:
        p = out_dir / f"{_ulib.quote(coin, safe='')}.npy"
        if p.exists() and not force:
            continue
        mins, closes = _load_coin_from_mongo(coin)
        if mins.size:
            max_minute = max(max_minute, int(mins[-1]))
        arr = np.vstack([mins.astype("float64"), closes]) if mins.size else np.empty((2, 0), dtype="float64")
        # codex perf-r1 #4: unique tmp (pid) so concurrent builds/runs never race on one tmp name.
        tmp = p.with_name(f"{p.name}.{os.getpid()}.tmp")
        with open(tmp, "wb") as fh:   # file handle: np.save must NOT re-append .npy to the name
            np.save(fh, arr)
        tmp.replace(p)
        n += 1
    # manifest: record the full coin set + the latest candle minute observed (freshness signature).
    latest = db.hyperliquid_candles.find_one({"interval": "1m"}, sort=[("timestamp_utc", -1)],
                                             projection={"timestamp_utc": 1, "_id": 0})
    cur_max = int(latest["timestamp_utc"]) if latest else max_minute
    _marks_manifest_path(out_dir).write_text(_json.dumps({
        "built_unix": _t.time(), "coins": list(coins), "n_coins": len(coins), "max_minute": cur_max,
        "total_candles": int(db.hyperliquid_candles.estimated_document_count()),
    }))
    logger.info(f"build_marks_cache: wrote {n} coin series to {out_dir} ({len(coins)} coins total)")
    return n


def _coin_series(coin: str) -> tuple:
    """Lazily load+cache a coin's FULL 1m (minute, close) series as sorted numpy arrays. Prefers the
    local precomputed .npy (page-cached, shared across workers, NO Mongo); falls back to a one-shot
    Mongo range query if the cache file is absent. Equivalent to the prior per-call
    `timestamp_utc <= minute_key, sort desc, limit 1` lookup (proven byte-identical in validation)."""
    s = _mark_series.get(coin)
    if s is not None:
        return s
    p = _coin_cache_path(coin)
    if p.exists():
        try:
            arr = np.load(p, mmap_mode="r")
            s = (np.asarray(arr[0], dtype="int64"), np.asarray(arr[1], dtype="float64"))
        except Exception:  # noqa: BLE001  corrupt cache -> Mongo fallback
            s = _load_coin_from_mongo(coin)
    else:
        s = _load_coin_from_mongo(coin)
    _mark_series[coin] = s
    return s


# --------------------------------------------------------------------------- #
# Coin / dex helpers
# --------------------------------------------------------------------------- #


def coin_is_spot(coin: str) -> bool:
    """Return true for non-perp trade assets excluded from reconstruction.

    ``@`` and slash-delimited symbols denote spot pairs; ``#`` denotes HIP-4
    outcome markets. Outcome fills have Buy/Sell/Settlement semantics, not perp
    position semantics.
    """
    return coin.startswith(("@", "#")) or "/" in coin or coin == "USDC"


def coin_is_dropped(coin: str) -> bool:
    """Return whether a configured perp-dex prefix is excluded."""
    return any(coin.startswith(p) for p in DROPPED_DEX_PREFIXES)


def coin_dex(coin: str) -> str:
    """Return the perp dex name for a coin. Unprefixed -> 'main'."""
    if ":" in coin:
        return coin.split(":", 1)[0]
    return "main"


# ALL DEXES IN SCOPE (Alberto 2026-05-30: "ALL dexes become IN SCOPE").
# The only equity history HL gives is whole-account perpAllTime (the portfolio
# `dex` param is IGNORED -> verified 2026-05-30, it returns the same whole-account
# number for dex=main/xyz/default). So reconstructing EVERY perp dex makes our
# equity match the anchor instead of fighting it. Any perp coin (any "<dex>:"
# prefix, incl flx:, or unprefixed main) is in scope; spot (@-tokens,
# slash-delimited pairs, USDC) and HIP-4 outcome markets (#-tokens) are excluded.
# (USDC capital-flow dex scoping is handled by dex_in_scope(): any non-spot dex.)
# Dexes with a row in wallet_anchor_state.parquet (position seed source). main +
# xyz + flx are seedable from the parquet; cash/hyna/para/vntl seed from fills
# only (still IN SCOPE, just no static-anchor seed -> rely on fills + drift gate).
ANCHOR_COVERED_DEXES = frozenset({"main", "xyz", "flx"})


def coin_is_allowed_perp(coin: str) -> bool:
    """True for any in-scope perp; excludes spot and HIP-4 outcomes."""
    return not coin_is_spot(coin)


def dex_in_scope(dex: str) -> bool:
    """A dex name (coin_dex output / ledger sourceDex) is in scope if it is any
    perp dex. Spot markers are out. All perp dexes are in scope."""
    d = (dex or "").strip().lower()
    return d not in ("spot", "@")


# --------------------------------------------------------------------------- #
# HL API: weekly anchor + today cross-check
# --------------------------------------------------------------------------- #


PERP_ANCHOR_CACHE = Path(
    "/Users/hermes/quants-lab/app/data/v15/perp_anchor_cache"
)


def get_portfolio_perp(wallet: str, retries: int = 4) -> list[tuple[int, float]]:
    """perpAllTime accountValueHistory as (ts_ms, value), whole-account perp.

    DISK-CACHED (2026-05-30): the per-wallet HL `portfolio` API call was the M01
    bottleneck (~0.3-0.7s each, network-bound, 20k wallets sequential = hours).
    We now persist each wallet's perpAllTime history to
    app/data/v15/perp_anchor_cache/{wallet}.json on first fetch and read it back
    on every subsequent run -> future M01 runs do ZERO API calls for anchors.
    Raw data, never auto-deleted. Delete the dir to force a refresh.
    """
    wallet_lc = wallet.lower()
    cache_fp = PERP_ANCHOR_CACHE / f"{wallet_lc}.json"
    if cache_fp.exists():
        try:
            with open(cache_fp) as f:
                return [(int(t), float(v)) for t, v in json.load(f)]
        except Exception:
            pass  # corrupt cache -> refetch

    result: list[tuple[int, float]] = []
    fetched = False
    for i in range(retries):
        try:
            r = requests.post(
                HL_URL, json={"type": "portfolio", "user": wallet}, timeout=20
            )
            if r.status_code == 429:
                time.sleep(2**i)
                continue
            if r.status_code != 200:
                fetched = False
                break
            for window_name, wd in r.json():
                if window_name == "perpAllTime":
                    result = [
                        (int(x[0]), float(x[1]))
                        for x in wd.get("accountValueHistory", [])
                    ]
                    break
            fetched = True
            break
        except Exception:
            time.sleep(1)

    # Persist only a genuine API response (incl. legitimately-empty histories) so
    # we never cache a transient network failure as "no anchors".
    if fetched:
        try:
            PERP_ANCHOR_CACHE.mkdir(parents=True, exist_ok=True)
            tmp = cache_fp.with_suffix(".json.tmp")
            with open(tmp, "w") as f:
                json.dump([[int(t), float(v)] for t, v in result], f)
            tmp.replace(cache_fp)
        except Exception:
            pass
    return result


WHOLE_ANCHOR_CACHE = Path(
    "/Users/hermes/quants-lab/app/data/v15/whole_anchor_cache"
)


def get_portfolio_all(wallet: str, retries: int = 4) -> list[tuple[int, float]]:
    """`allTime` WHOLE-ACCOUNT accountValueHistory as (ts_ms, value).

    2026-06-08 (SOLVED -- spot inclusion): the perpAllTime anchor is PERP-ONLY, but many wallets hold
    most of their equity in SPOT (e.g. 0x292ae77a: $10,941 total / $877 perp). Reconstructing perp-only
    against a perp-only anchor while the spot side dominates produced the large inter-anchor drift. We now
    reconstruct the WHOLE ACCOUNT (perp + spot, both marked) and anchor to HL's `allTime` series, which
    sits in the SAME portfolio response next to perpAllTime. Cached separately so perpAllTime is preserved.
    Raw data, never auto-deleted. Delete the dir to force a refresh.
    """
    wallet_lc = wallet.lower()
    cache_fp = WHOLE_ANCHOR_CACHE / f"{wallet_lc}.json"
    if cache_fp.exists():
        try:
            with open(cache_fp) as f:
                return [(int(t), float(v)) for t, v in json.load(f)]
        except Exception:
            pass
    result: list[tuple[int, float]] = []
    fetched = False
    for i in range(retries):
        try:
            r = requests.post(HL_URL, json={"type": "portfolio", "user": wallet}, timeout=20)
            if r.status_code == 429:
                time.sleep(2**i)
                continue
            if r.status_code != 200:
                fetched = False
                break
            for window_name, wd in r.json():
                if window_name == "allTime":
                    result = [(int(x[0]), float(x[1])) for x in wd.get("accountValueHistory", [])]
                    break
            fetched = True
            break
        except Exception:
            time.sleep(1)
    if fetched:
        try:
            WHOLE_ANCHOR_CACHE.mkdir(parents=True, exist_ok=True)
            tmp = cache_fp.with_suffix(".json.tmp")
            with open(tmp, "w") as f:
                json.dump([[int(t), float(v)] for t, v in result], f)
            tmp.replace(cache_fp)
        except Exception:
            pass
    return result


def get_clearinghouse(wallet: str, dex: Optional[str], retries: int = 3) -> Optional[dict]:
    """Per-dex clearinghouseState snapshot (today cross-check). dex=None -> main."""
    body: dict = {"type": "clearinghouseState", "user": wallet}
    if dex and dex != "main":
        body["dex"] = dex
    for i in range(retries):
        try:
            r = requests.post(HL_URL, json=body, timeout=20)
            if r.status_code == 429:
                time.sleep(2**i)
                continue
            if r.status_code == 200:
                return r.json()
            return None
        except Exception:
            time.sleep(1)
    return None


# --------------------------------------------------------------------------- #
# Local data loaders
# --------------------------------------------------------------------------- #


def _position_key(value: float) -> float:
    """Stable key for decimal position values represented as float."""
    return round(float(value), 10)


def _order_fill_burst(group: list[dict]) -> tuple[list[dict], bool]:
    """Order one same-wallet/coin/millisecond burst by position continuity.

    ``tid`` identifies a trade but is not a causal sequence number. Each fill's
    startPosition and signed size define an edge in the actual position path.
    """
    if len(group) <= 1:
        return list(group), True
    from collections import Counter

    starts = [_position_key(f["startPosition"]) for f in group]
    ends = [_position_key(f["startPosition"] + f["signed_sz"]) for f in group]
    start_counts, end_counts = Counter(starts), Counter(ends)
    path_starts = [k for k, n in start_counts.items() if n > end_counts.get(k, 0)]
    if len(path_starts) == 1:
        current = path_starts[0]
    elif all(f["signed_sz"] > 0 for f in group):
        current = min(starts)
    elif all(f["signed_sz"] < 0 for f in group):
        current = max(starts)
    else:
        current = min(starts, key=lambda x: (abs(x), x))

    unused = set(range(len(group)))
    ordered: list[dict] = []
    complete = True
    while unused:
        candidates = [i for i in unused if starts[i] == current]
        if not candidates:
            complete = False
            candidates = list(unused)
        i = min(candidates, key=lambda j: (int(group[j].get("tid", 0) or 0), j))
        ordered.append(group[i])
        unused.remove(i)
        current = ends[i]
    return ordered, complete


def order_wallet_fills_causally(fills: list[dict]) -> list[dict]:
    """Causally order fills and attach a stable per-wallet ``fill_seq``.

    DEDUP (2026-07-10 fix): funding/ledger loaders dedup but fills did not, so a duplicated fill row in
    an overlapping S3 partition double-counted size + closedPnl + fee into equity. Drop exact duplicates:
    prefer the trade id (tid) when present (unique per HL fill); else the full identifying tuple. This is
    order-independent and only removes rows that are byte-identical on their accounting fields."""
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for f in fills:
        tid = int(f.get("tid", 0) or 0)
        if tid:
            key = ("tid", tid)
        else:
            key = ("row", int(f["time"]), str(f["coin"]), str(f.get("side", "")),
                   float(f.get("size", 0.0)), float(f.get("price", 0.0)),
                   float(f.get("startPosition", 0.0)), float(f.get("closedPnl", 0.0)))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(f)
    fills = deduped
    groups: dict[tuple[int, str], list[dict]] = {}
    for f in fills:
        groups.setdefault((int(f["time"]), str(f["coin"])), []).append(f)
    out: list[dict] = []
    for key in sorted(groups):
        burst, complete = _order_fill_burst(groups[key])
        for f in burst:
            f["causal_order_ok"] = bool(complete)
            out.append(f)
    for seq, f in enumerate(out):
        f["fill_seq"] = seq
    return out


def load_wallet_fills(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load ALL markable perp fills for one wallet in [t0, t1].

    Includes every perp dex prefix except explicitly dropped prefixes. Spot
    fills (@-prefix, USDC) are excluded: M1 is anchored to ``perpAllTime`` and
    M7/live copy execution is perp-only.
    Casts string numerics, computes signed_sz (+size if side=='B' else -size),
    and tags liquidation fills via the `dir` column.

    Fast path: per-wallet partitioned parquet if present; else scan dailies
    filtering wallet (lowercased compare).
    """
    wallet_lc = wallet.lower()
    cols = [
        "wallet", "coin", "side", "size", "price", "time", "dir",
        "closedPnl", "startPosition", "fee", "builderFee", "deployerFee",
        "tid",
    ]

    def _normalize(df: pd.DataFrame) -> list[dict]:
        out: list[dict] = []
        for rec in df.to_dict("records"):
            coin = str(rec["coin"])
            if not coin_is_allowed_perp(coin):
                continue
            size = _num(rec["size"], default=float("nan"))
            price = _num(rec["price"], default=float("nan"))
            # Fail CLOSED on a malformed fill: a NaN price/size cannot be booked (0 would be worse than
            # dropping). Skip it so a corrupt row never poisons position/cash. (2026-07-10 fix.)
            if not (math.isfinite(size) and math.isfinite(price)):
                continue
            try:
                tid = int(rec.get("tid", 0) or 0)
            except (TypeError, ValueError):
                tid = 0
            d = {
                "coin": coin,
                "side": rec["side"],
                "size": size,
                "price": price,
                "time": int(rec["time"]),
                "tid": tid,
                "dir": str(rec.get("dir", "") or ""),
                # NaN-safe: parquet nulls survive `float(x or 0)` as NaN and poison equity/PnL sums.
                "closedPnl": _num(rec.get("closedPnl", 0)),
                "startPosition": _num(rec.get("startPosition", 0)),
                "fee": _num(rec.get("fee", 0)),
                "builderFee": _num(rec.get("builderFee", 0)),
                "deployerFee": _num(rec.get("deployerFee", 0)),
            }
            d["signed_sz"] = size if rec["side"] == "B" else -size
            d["is_liquidation"] = d["dir"] in LIQUIDATION_DIRS
            d["is_spot"] = False
            out.append(d)
        return out

    fills: list[dict] = []
    by_wallet_path = S3_BY_WALLET_DIR / f"{wallet_lc}.parquet"
    if by_wallet_path.exists():
        try:
            df = pd.read_parquet(by_wallet_path)
            df = df[[c for c in cols if c in df.columns]].copy()
            df["time"] = df["time"].astype("int64")
            df = df[(df["time"] >= t0) & (df["time"] <= t1)]
            return order_wallet_fills_causally(_normalize(df))
        except Exception:
            fills = []

    for ff in sorted(glob.glob(str(S3_FILLS_DIR / "*.parquet"))):
        try:
            # Read only columns that actually exist (read_parquet with a fixed
            # `columns=` list raises if ANY is absent, and the bare `except`
            # below would then silently drop the WHOLE day of fills). Require
            # the core accounting columns; tolerate missing optional fee cols.
            avail = set(pd.ParquetFile(ff).schema_arrow.names)
            required = {"wallet", "coin", "side", "size", "price", "time"}
            missing_required = required - avail
            if missing_required:
                logger.warning(
                    f"  skip {Path(ff).name}: missing required cols {sorted(missing_required)}"
                )
                continue
            read_cols = [c for c in cols if c in avail]
            df = pd.read_parquet(ff, columns=read_cols)
            df["time"] = df["time"].astype("int64")
            df["_w"] = df["wallet"].astype(str).str.lower()
            m = df[(df["_w"] == wallet_lc) & (df["time"] >= t0) & (df["time"] <= t1)]
            if not m.empty:
                fills.extend(_normalize(m))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"  skip {Path(ff).name}: {e!r}")
            continue
    return order_wallet_fills_causally(fills)


def load_wallet_funding(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load + merge + dedup funding events from the local S3 bulk cache.

    Multiple {wallet}_{start}_{end}.json files may exist; merge them and dedup
    by (time, coin) since overlapping windows can repeat the same record.
    Records: {time, hash, delta:{type:'funding', coin, usdc(signed), ...}}.
    """
    wallet_lc = wallet.lower()
    seen: set[tuple[int, str]] = set()
    out: list[dict] = []
    json_files = sorted(glob.glob(str(FUNDING_DIR / f"{wallet_lc}_*.json")))
    for fp in json_files:
        try:
            with open(fp) as f:
                data = json.load(f)
        except Exception:
            continue
        for rec in data:
            t = int(rec.get("time", 0))
            if t < t0 or t > t1:
                continue
            d = rec.get("delta", {})
            if d.get("type") != "funding":
                continue
            coin = str(d.get("coin", ""))
            key = (t, coin)
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
    # Self-healing fallback: JSON path produced NO records (dir missing, OR a dead/empty JSON file
    # exists) -> read the raw parquet funding cache and reshape to the SAME {time,hash,delta} contract.
    # Gated on `not out` (not `not json_files`) so an empty JSON file can't skip the heal. (2026-07-10.)
    if not out:
        for pf in sorted(FUNDING_PARQUET_DIR.glob(f"{wallet_lc}_*.parquet")):
            try:
                fd = pd.read_parquet(pf)
            except Exception:
                continue
            for r in fd.itertuples(index=False):
                t = int(r.time_ms)
                if t < t0 or t > t1:
                    continue
                coin = str(r.coin)
                key = (t, coin)
                if key in seen:
                    continue
                seen.add(key)
                out.append({"time": t, "hash": "",
                            "delta": {"type": "funding", "coin": coin, "usdc": str(r.usdc_signed)}})
    out.sort(key=lambda x: int(x["time"]))
    return out


def load_wallet_ledger(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load ledger entries from the local cache, dedup on the FULL delta payload.

    Dedup key includes a canonical serialization of the whole delta (not just
    its type), so two distinct deltas of the same type in one tx (same time +
    hash) are NOT collapsed into one. Overlapping cache windows still dedup
    identical records.
    """
    wallet_lc = wallet.lower()
    seen: set[tuple] = set()
    out: list[dict] = []
    json_files = sorted(glob.glob(str(LEDGER_DIR / f"{wallet_lc}_*.json")))
    for lf in json_files:
        try:
            with open(lf) as f:
                data = json.load(f)
        except Exception:
            continue
        for e in data:
            t = int(e.get("time", 0))
            if t < t0 or t > t1:
                continue
            d = e.get("delta", {})
            # Canonical full-delta key: distinct same-type deltas in one tx differ
            # in their payload (amount/coin/counterparty) and survive dedup.
            key = (
                t,
                e.get("hash", ""),
                json.dumps(d, sort_keys=True, default=str),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(e)
    # Self-healing fallback: JSON path produced NO records (dir missing OR dead/empty JSON) -> reshape
    # rows from the monolithic ledger parquet into the SAME {time, hash, delta:{...}} contract. The
    # parquet 'coin' column carries the token, mapped to delta['token'] (ledger_cash_delta branches on
    # delta['token']). Gated on `not out` so an empty JSON file can't skip the heal. (2026-07-10 fix.)
    if not out:
        lg = _LEDGER_PARQUET_CACHE["df"]
        if lg is None and LEDGER_PARQUET.exists():
            lg = pd.read_parquet(LEDGER_PARQUET)
            _LEDGER_PARQUET_CACHE["df"] = lg
        if lg is not None:
            sub = lg[lg["wallet"].astype(str).str.lower() == wallet_lc]
            for r in sub.itertuples(index=False):
                t = int(r.time)
                if t < t0 or t > t1:
                    continue
                delta = {"type": str(r.type)}
                coin = getattr(r, "coin", None)
                if coin is not None and not (isinstance(coin, float) and math.isnan(coin)) and str(coin) != "":
                    delta["token"] = str(coin)
                for fld in _LEDGER_DELTA_FIELDS:
                    v = getattr(r, fld, None)
                    if v is None or (isinstance(v, float) and math.isnan(v)):
                        continue
                    delta[fld] = v
                h = getattr(r, "hash", "")
                h = "" if (h is None or (isinstance(h, float) and math.isnan(h))) else str(h)
                e = {"time": t, "hash": h, "delta": delta}
                key = (t, h, json.dumps(delta, sort_keys=True, default=str))
                if key in seen:
                    continue
                seen.add(key)
                out.append(e)
    out.sort(key=lambda x: int(x["time"]))
    return out


@dataclass
class AnchorState:
    """Per-wallet anchor snapshot aggregated across markable perp dexes."""

    positions: dict[str, float]  # coin -> szi (markable dexes, flx dropped)
    entry_px: dict[str, float]  # coin -> entryPx from anchor (notional proxy)
    fetched_ms: int
    dexes_seen: set[str]
    has_flx_anchor: bool
    has_unmarkable_dex_anchor: bool
    # per-dex accountValue at fetch time (today cross-check reference)
    acct_value_by_dex: dict[str, float]
    aggregate_acct_value: float  # sum over parquet dex rows (main+xyz+flx)


def load_wallet_anchor(wallet: str, anchor_df: pd.DataFrame) -> Optional[AnchorState]:
    """Build the seed AnchorState from wallet_anchor_state.parquet.

    The parquet only carries main/xyz/flx rows. We seed positions from ALL three
    (flx is IN SCOPE). Positions on other in-scope dexes (cash/hyna/para/vntl/km)
    have no parquet row and seed from fills instead.
    """
    wallet_lc = wallet.lower()
    wa = anchor_df[anchor_df["wallet"].str.lower() == wallet_lc]
    if wa.empty:
        return None
    positions: dict[str, float] = {}
    entry_px: dict[str, float] = {}
    fetched_ms = 0
    dexes_seen: set[str] = set()
    acct_value_by_dex: dict[str, float] = {}
    aggregate_acct_value = 0.0
    has_flx_anchor = False
    has_unmarkable_dex_anchor = False
    for _, row in wa.iterrows():
        if not row["ok"]:
            continue
        dex = row["dex"]
        dexes_seen.add(dex)
        acct_val = float(row["accountValue"])
        acct_value_by_dex[dex] = acct_val
        ts_ms = int(float(row["fetched_at_ts"]) * 1000)
        fetched_ms = max(fetched_ms, ts_ms)
        if dex == "flx":
            # flx is now IN SCOPE (all dexes). Record presence AND seed its
            # positions from the anchor row (flx has a parquet row + marks).
            has_flx_anchor = acct_val > 1.0 or int(row["n_positions"]) > 0
        # An anchor row on a dex without a parquet seed source (outside main/xyz/
        # flx) is informational only now (all dexes in scope; such positions seed
        # from fills, not the parquet).
        if dex not in ANCHOR_COVERED_DEXES:
            if acct_val > 1.0 or int(row["n_positions"]) > 0:
                has_unmarkable_dex_anchor = True
        aggregate_acct_value += acct_val
        for p in json.loads(row["positions_json"]):
            coin = p["coin"]
            szi = float(p["szi"])
            if not coin_is_allowed_perp(coin):
                continue
            positions[coin] = szi
            try:
                entry_px[coin] = abs(float(p.get("entryPx", 0.0)))
            except (TypeError, ValueError):
                entry_px[coin] = 0.0
    return AnchorState(
        positions=positions,
        entry_px=entry_px,
        fetched_ms=fetched_ms,
        dexes_seen=dexes_seen,
        has_flx_anchor=bool(has_flx_anchor),
        has_unmarkable_dex_anchor=bool(has_unmarkable_dex_anchor),
        acct_value_by_dex=acct_value_by_dex,
        aggregate_acct_value=aggregate_acct_value,
    )


# --------------------------------------------------------------------------- #
# Cash-delta functions
# --------------------------------------------------------------------------- #


def fill_cash_delta(f: dict) -> float:
    """Per fill: ``-signed_sz * price - fee``.

    Hyperliquid's API defines ``fee`` as the total wallet charge (signed;
    negative means maker rebate). ``builderFee`` and ``deployerFee`` are
    informational components of that total and must not be added again. Spot
    and dropped markets are excluded at load time, but guard defensively.
    """
    if not coin_is_allowed_perp(f["coin"]):
        return 0.0
    return -f["signed_sz"] * f["price"] - f["fee"]


@dataclass
class LedgerDelta:
    """Result of classifying a ledger event: cash impact + external-flow tag."""

    cash: float  # delta to perp cash (affects equity walk)
    ext_flow: float  # portion to accumulate into ext_flow_cum (neutralizable)
    unknown: bool = False  # True if the type is unrecognized -> quarantine


def ledger_cash_delta(e: dict, wallet_lc: str) -> LedgerDelta:
    """Classify a ledger event into (cash_delta, ext_flow_delta, unknown).

    Cash signs mirror v8's proven mapper. The NEW V15 behaviour is that every
    EXTERNAL-CAPITAL / non-copyable type ALSO reports its cash impact as
    ext_flow so downstream can neutralize it:
        trading_pnl = equity_change - ext_flow_cum.
    Trading results (fills, funding, liquidation) are NOT in ext_flow.

    Unknown ledger types return unknown=True (caller quarantines the wallet);
    they are never silently zeroed.
    """
    d = e.get("delta", {})
    k = d.get("type", "")

    def _f(*keys, default=0.0) -> float:
        for key in keys:
            v = d.get(key)
            if v is not None:
                return float(v)
        return float(default)

    # --- External capital / non-copyable flows (cash == ext_flow) ----------- #
    if k == "deposit":
        c = _f("usdc")
        return LedgerDelta(c, c)
    if k == "withdraw":
        c = -(_f("usdc") + _f("fee"))
        return LedgerDelta(c, c)
    if k == "accountClassTransfer":
        # Perp-account model: spot -> perp is an inflow and perp -> spot an
        # outflow. Neutralize both as external/non-copyable capital flow.
        amt = abs(_f("usdc", "amount"))
        c = amt if bool(d.get("toPerp")) else -amt
        return LedgerDelta(c, c)
    if k == "send":
        # Perp-account model: only a leg touching an in-scope perp dex changes
        # reconstructed perp cash. HIP-3 dexes can use non-USDC collateral
        # (e.g. USDH on flx/km, USDT0 on cash); ``usdcValue`` is the canonical
        # USD value for those sends. Spot-only token transfers remain zero
        # because neither leg is in scope. Self-transfers spot<->perp remain
        # perp inflows/outflows and are neutralized from trading ROE.
        user = (d.get("user") or "").lower()
        dest = (d.get("destination") or "").lower()
        src_dex = str(d.get("sourceDex", "")).strip().lower()
        dst_dex = str(d.get("destinationDex", "")).strip().lower()
        if d.get("usdcValue") is not None:
            amt = _f("usdcValue")
        elif d.get("token") == "USDC":
            amt = _f("amount")
        else:
            # A non-USDC perp transfer without a USD valuation cannot be reconstructed safely.
            # Quarantine rather than treating token quantity as dollars. NOTE the asymmetry vs the
            # cash path above is INTENTIONAL and both directions are fail-closed: the cash path uses
            # _perp_leg (empty dex -> DON'T move cash, avoid fabricating a move); this quarantine path
            # keeps dex_in_scope (empty/unknown dex -> COULD be perp -> quarantine, avoid trusting a
            # move we cannot value). Empty dex is treated as maybe-perp here on purpose. (codex m01fix.)
            touches_perp = dex_in_scope(src_dex) or dex_in_scope(dst_dex)
            return LedgerDelta(0.0, 0.0, unknown=touches_perp)
        fee = _f("fee")
        # 2026-07-10 fix: an EMPTY/unknown dex is not PROVABLY an in-scope perp dex. dex_in_scope("")
        # returned True (only "spot"/"@" are out), so an empty-sourceDex USDC send wrongly moved perp
        # cash. Require a non-empty, in-scope dex before adjusting the reconstructed perp cash.
        def _perp_leg(dex: str) -> bool:
            return bool(dex) and dex_in_scope(dex)
        c = 0.0
        if user == wallet_lc and _perp_leg(src_dex):
            c -= amt + fee
        if dest == wallet_lc and _perp_leg(dst_dex):
            c += amt
        return LedgerDelta(c, c)
    if k == "spotTransfer":
        # Spot-side balance movement; accountClassTransfer/send carry any
        # corresponding movement into or out of perp cash.
        return LedgerDelta(0.0, 0.0)
    if k == "internalTransfer":
        if d.get("token") not in (None, "USDC"):
            return LedgerDelta(0.0, 0.0)
        user = (d.get("user") or "").lower()
        dest = (d.get("destination") or "").lower()
        amt = _f("usdc", "amount")
        fee = _f("fee")
        c = 0.0
        if user == wallet_lc and dest != wallet_lc:
            c = -(amt + fee)
        elif dest == wallet_lc and user != wallet_lc:
            c = amt
        return LedgerDelta(c, c)
    if k == "subAccountTransfer":
        user = (d.get("user") or "").lower()
        dest = (d.get("destination") or "").lower()
        amt = _f("usdc", "amount")
        c = 0.0
        if user == wallet_lc:
            c = -amt
        elif dest == wallet_lc:
            c = amt
        return LedgerDelta(c, c)
    if k == "vaultDeposit":
        c = -_f("usdc", "amount")
        return LedgerDelta(c, c)
    if k == "vaultWithdraw":
        c = _f("netWithdrawnUsd", "usdc", "amount")
        return LedgerDelta(c, c)
    if k == "vaultCreate":
        c = -(_f("usdc", "amount") + _f("fee"))
        return LedgerDelta(c, c)
    if k in ("vaultDistribution", "vaultLeaderCommission"):
        c = _f("usdc", "amount")
        return LedgerDelta(c, c)
    if k == "rewardsClaim":
        if d.get("token") != "USDC":
            return LedgerDelta(0.0, 0.0)
        c = _f("usdc", "amount")
        return LedgerDelta(c, c)
    if k == "borrowLend":
        if d.get("token") != "USDC":
            return LedgerDelta(0.0, 0.0)
        amt = abs(_f("usdc", "amount"))
        op = (d.get("operation") or "").lower()
        if op in ("supply", "lend", "deposit"):
            return LedgerDelta(-amt, -amt)
        if op in ("withdraw", "redeem"):
            return LedgerDelta(amt, amt)
        # borrow / repay -> debt offsets cash, no equity change, no ext flow.
        return LedgerDelta(0.0, 0.0)
    if k == "activateDexAbstraction":
        ev_dex = str(d.get("dex", "")).strip().lower()
        if not dex_in_scope(ev_dex):  # all perp dexes in scope (was main-only)
            return LedgerDelta(0.0, 0.0)
        token = d.get("token")
        if token == "USDC" or token is None:
            c = -abs(_f("usdc", "amount"))
        else:
            c = -_f("fee")
        return LedgerDelta(c, c)
    if k in ("accountActivationGas", "deployGasAuction", "gossipPriorityGasAuction"):
        # Operational cost; neutralize as external (non-trading).
        c = -abs(_f("usdc", "amount", "fee"))
        return LedgerDelta(c, c)

    # --- Zero / ignore (no perp-equity impact) ------------------------------ #
    if k in ZERO_LEDGER_TYPES:
        return LedgerDelta(0.0, 0.0)

    # --- Unknown type -> quarantine ----------------------------------------- #
    if k:
        return LedgerDelta(0.0, 0.0, unknown=True)
    return LedgerDelta(0.0, 0.0)


def funding_cash_delta(e: dict) -> float:
    """Funding USDC (signed, wallet perspective) for all included perp dexes.
    """
    d = e.get("delta", {})
    if d.get("type") != "funding":
        return 0.0
    coin = str(d.get("coin", ""))
    # Only funding on an included perp coin counts (parity with fills/positions).
    if not coin_is_allowed_perp(coin):
        return 0.0
    return float(d.get("usdc", 0))


# --------------------------------------------------------------------------- #
# Position seeding + reconstruction
# --------------------------------------------------------------------------- #


def positions_at(fills: list[dict], t_ms: int) -> dict[str, float]:
    """Position per coin at time t = (EARLIEST fill's startPosition) + cumulative
    sum of all signed sizes at-or-before t.

    CRITICAL (root-cause of large drift, fixed 2026-05-30): do NOT use the LAST
    fill's startPosition + signed_sz. Large orders fill as a SAME-MILLISECOND
    BURST against many makers, and the S3 by-wallet partition carries no `tid`,
    so same-ms fills are in arbitrary order. Picking the "last" one grabs a
    mid-burst startPosition and invents a phantom open position (observed: a flat
    wallet reconstructed as -90,492 MERL because the last-by-order fill in a
    closing burst had startPosition=-105,010). The cumulative sum from the
    EARLIEST fill's startPosition is ORDER-INDEPENDENT and exact regardless of
    intra-ms ordering or a missing tid. (fills are time-sorted on load.)"""
    by_coin: dict[str, list[dict]] = {}
    for f in fills:
        if f["time"] > t_ms:
            break
        by_coin.setdefault(f["coin"], []).append(f)
    positions: dict[str, float] = {}
    for coin, fs in by_coin.items():
        pos = fs[0]["startPosition"] + sum(x["signed_sz"] for x in fs)
        if abs(pos) > 1e-9:
            positions[coin] = pos
    return positions


@dataclass
class WalkResult:
    cash: float
    positions: dict[str, float]
    equity: float
    position_value: float  # independent mark-derived sum
    gross_position_notional: float  # sum(abs(szi * mark)); never net long/short legs
    n_unmarkable: int
    unmarkable_notional: float  # |szi * entry-ish proxy| for coins lacking a mark
    ext_flow_cum: float  # ext flow within (anchor_ms, t_ms] (per-SEGMENT, not series)
    had_unknown_ledger: bool
    anchor_unmarkable: int = 0  # seeded positions with NO mark at anchor_ms

    @property
    def recon_incomplete(self) -> bool:
        """True if equity for this walk is NOT trustworthy: a seeded position
        lacked an anchor mark (corrupted cash snap) or a terminal mark was missing
        -> we cannot value the book, so the day must not be trusted. (Hidden
        no-anchor-dex exposure is caught at the wallet level by the inter-anchor
        DRIFT gate, not here, since by definition we cannot see it per-row.)"""
        return self.anchor_unmarkable > 0 or self.n_unmarkable > 0


def seed_positions(
    fills: list[dict],
    anchor: AnchorState,
    anchor_ms: int,
    causal_cutoff: bool = False,
) -> dict[str, float]:
    """Seed positions held at anchor_ms.

    TWO paths:
    * causal_cutoff=True (per-EVENT walk): fully causal -- positions_at(fills<=anchor)
      + the anchor's OWN near-fetch snapshot. MUST NOT use any fill with time>anchor_ms
      (look-ahead). Unchanged.
    * causal_cutoff=False (daily reconstruction + cent reconciliation): EX-POST
      BURST-AWARE seed. pos = P0 + cumsum(signed_sz of fills<=anchor), where P0 (the
      pre-first-fill carry) is taken from the clean earliest-fill startPosition, EXCEPT
      when that earliest fill is a same-millisecond BURST (no tid -> arbitrary order ->
      mid-burst garbage startPosition), in which case P0 is anchored to the authoritative
      fetch-time snapshot: P0 = snapshot_position - sum(ALL signed_sz). This kills the
      phantom-seed leak (e2af8f54's 83-fill same-ms first DOGE fill read startPosition
      1.79M -> 1.79M DOGE seeded at a Dec anchor where the wallet held zero). Cumulative
      signed_sz is order-independent so only P0 needed the fix. Coins in the snapshot but
      never traded in-window (Case A) are seeded at the snapshot size at every anchor.

    NOTE: this CHANGES the daily reconstruction output (it was wrong: phantom positions).
    Re-run M2->M10 on the corrected equity. The per-event causal path is byte-unchanged.
    """
    near_fetch = abs(anchor_ms - anchor.fetched_ms) <= 86_400_000

    if causal_cutoff:
        # FULLY-CAUSAL seed for the per-event walk (codex r2 BUG): the seed serves
        # every event k in this anchor's segment and MUST NOT depend on any fill with
        # time > anchor_ms (look-ahead). Causal seed = positions from fills <= anchor_ms
        # + the anchor's OWN near-fetch snapshot, full stop. A far-from-fetch snapshot is
        # not causal evidence; any genuine unseen exposure surfaces as inter-anchor DRIFT.
        start_positions = positions_at(fills, anchor_ms)
        if near_fetch:
            for coin, szi in anchor.positions.items():
                if coin not in start_positions and abs(szi) >= 1e-9:
                    start_positions[coin] = float(szi)
        return start_positions

    # ----- EX-POST BURST-AWARE seed (causal_cutoff=False; daily + reconciliation) ----- #
    # pos(anchor) = P0 + cumulative signed_sz of fills at-or-before the anchor. The
    # cumulative sum is order-independent and robust. P0 (pre-first-fill carry) was the
    # leak: M1 took it from the earliest fill's startPosition, but large orders fill as
    # SAME-MILLISECOND bursts and the S3 by-wallet partition carries no tid -> the fills
    # are in arbitrary order, so a burst startPosition is mid-burst GARBAGE. Observed
    # (Alberto "reconcile to the cent", 2026-06-04): e2af8f54's first DOGE fill is an
    # 83-fill same-ms burst reading startPosition 1,792,138 -> the old seed put 1.79M DOGE
    # at a December anchor where the wallet held ZERO (phantom; its mark drift == the
    # inter-anchor residual to the dollar). Same for SOL/kPEPE/MON. FIX: when the earliest
    # fill is a same-ms burst, take P0 from the AUTHORITATIVE fetch-time snapshot instead:
    # P0 = snapshot_position(coin) - sum(ALL signed_sz) (order-independent, ground-truth-
    # anchored). reconstruct_wallet loads fills through fetched_ms so the sum reaches the
    # snapshot. A CLEAN earliest fill keeps the forward startPosition (the snapshot carries
    # minor fetch-instant noise that would otherwise leak into every anchor, e.g. the
    # main-only wallet a9881f6f: backward-everywhere regressed it from $0.04 to $272 median,
    # burst-aware preserves $0.04). Validated: a9881f6f $0.04 max $2.10 PRESERVED; e2af8f54
    # median $18,628 -> $2.28; daf50e49 $4,787 -> $710; 4ac21adc $9,311 -> $99.
    by_coin: dict[str, list[dict]] = {}
    for f in fills:
        by_coin.setdefault(f["coin"], []).append(f)  # already time-sorted on load
    cur = anchor.positions
    start_positions = {}
    for coin, fs in by_coin.items():
        if not coin_is_allowed_perp(coin):
            continue
        t0 = fs[0]["time"]
        burst = sum(1 for f in fs if f["time"] == t0) > 1
        net_all = 0.0
        cum_le = 0.0
        for f in fs:
            net_all += f["signed_sz"]
            if f["time"] <= anchor_ms:
                cum_le += f["signed_sz"]
        if burst:
            p0 = float(cur.get(coin, 0.0)) - net_all  # backward: perp snapshot-anchored
        else:
            p0 = float(fs[0]["startPosition"])         # forward: clean earliest fill
        pos = p0 + cum_le
        if abs(pos) > 1e-9:
            start_positions[coin] = pos
    # Case A: coins in the fetch snapshot but NEVER traded in-window -> established
    # before the window and held constant (no trades = no size change) -> seed the
    # snapshot size at every anchor.
    for coin, szi in cur.items():
        if coin not in by_coin and coin_is_allowed_perp(coin) and abs(szi) >= 1e-9:
            start_positions[coin] = float(szi)
    return start_positions


def compute_eq_at(
    stream: list[tuple[int, str, dict]],
    fills: list[dict],
    anchor: AnchorState,
    wallet_lc: str,
    t_ms: int,
    anchor_ms: int,
    anchor_eq: float,
    causal_seed: bool = False,
) -> WalkResult:
    """Re-anchored forward walk from anchor_ms to t_ms.

    cash := anchor_eq - Sum pos*mark(anchor_ms); then walk the merged event
    stream in (anchor_ms, t_ms]. equity(t) = cash + Sum pos*mark(t_ms).

    Missing marks are COUNTED (n_unmarkable) and their notional estimated, but
    not silently zeroed into equity; the caller decides whether to flag the day.
    """
    start_positions = seed_positions(fills, anchor, anchor_ms, causal_cutoff=causal_seed)

    # Snap cash to the anchor equity using marked position value at anchor_ms.
    # If a SEEDED position has no mark at anchor_ms, the cash snap silently
    # absorbs its (unknown) value -> equity is corrupted. Count those so the
    # caller can flag/quarantine the day instead of trusting it (P0).
    anchor_pos_value = 0.0
    anchor_unmarkable = 0
    for c, sz in start_positions.items():
        if abs(sz) < 1e-9:
            continue
        mark = get_mark(c, anchor_ms)
        if mark is None and coin_is_spot(c):
            mark = _last_fill_price(fills, c, anchor_ms) or None  # spot: candle may be absent -> fill price
        if mark is not None:
            anchor_pos_value += sz * mark
        else:
            anchor_unmarkable += 1
    cash = anchor_eq - anchor_pos_value

    positions = dict(start_positions)
    # Coins present in the anchor seed are already folded into both position
    # value and the cash snap. A coin first observed AFTER the anchor can still
    # reveal a pre-anchor carry via its authoritative startPosition. In causal
    # mode we fold that carry only when its first fill becomes observable; this
    # mirrors compute_event_equity and avoids both future leakage and the old
    # error of treating a trim as a fresh short/open.
    carried_in = set(start_positions)
    ext_flow_cum = 0.0
    had_unknown = False
    for ts, typ, ev in stream:
        if ts <= anchor_ms or ts > t_ms:
            continue
        if typ == "fill":
            if not coin_is_allowed_perp(ev["coin"]):
                continue
            coin = ev["coin"]
            if causal_seed and coin not in carried_in:
                carried_in.add(coin)
                sp = float(ev.get("startPosition", 0.0) or 0.0)
                if abs(sp) > 1e-9:
                    positions[coin] = positions.get(coin, 0.0) + sp
                    anchor_mark = get_mark(coin, anchor_ms)
                    if anchor_mark is None:
                        # The anchor cash allocation for this newly revealed
                        # carry is unknowable. Flag the walk incomplete rather
                        # than silently absorbing it into cash.
                        anchor_unmarkable += 1
                    else:
                        cash -= sp * anchor_mark
            cash += fill_cash_delta(ev)
            positions[coin] = positions.get(coin, 0.0) + ev["signed_sz"]
            if abs(positions[coin]) < 1e-9:
                positions.pop(coin, None)
        elif typ == "ledger":
            ld = ledger_cash_delta(ev, wallet_lc)
            cash += ld.cash
            ext_flow_cum += ld.ext_flow
            had_unknown = had_unknown or ld.unknown
        elif typ == "funding":
            cash += funding_cash_delta(ev)

    # Mark-to-market at t_ms (independent of cash).
    pos_value = 0.0
    gross_position_notional = 0.0
    n_unmarkable = 0
    unmarkable_notional = 0.0
    for c, sz in positions.items():
        if abs(sz) < 1e-9:
            continue  # dust; not a real position
        mark = get_mark(c, t_ms)
        if mark is None and coin_is_spot(c):
            mark = _last_fill_price(fills, c, t_ms) or None  # spot: candle may be absent -> fill price
        if mark is None:
            n_unmarkable += 1
            # Best-effort notional proxy: last in-window fill price for the coin;
            # fall back to the anchor parquet entryPx for statically-held coins
            # that were never traded in-window (and thus have no fill price).
            proxy_px = _last_fill_price(fills, c, t_ms)
            if proxy_px <= 0.0:
                proxy_px = anchor.entry_px.get(c, 0.0)
            unmarkable_notional += abs(sz) * proxy_px
            continue
        pos_value += sz * mark
        gross_position_notional += abs(sz * mark)
    equity = cash + pos_value
    # 2026-07-10 fix (daily path non-finite guard): the per-EVENT path fails closed on non-finite
    # cash/equity, but this daily path did not -> a NaN/inf ledger/funding delta (e.g. usdc="NaN") could
    # flow a NaN equity into the emitted EOD parquet with recon_incomplete=False. Force the day INCOMPLETE
    # (via anchor_unmarkable, which drives recon_incomplete) so it is flagged, never trusted.
    if not (np.isfinite(cash) and np.isfinite(equity)):
        anchor_unmarkable = max(anchor_unmarkable, 1)
    return WalkResult(
        cash=cash,
        positions=positions,
        equity=equity,
        position_value=pos_value,
        gross_position_notional=gross_position_notional,
        n_unmarkable=n_unmarkable,
        unmarkable_notional=unmarkable_notional,
        anchor_unmarkable=anchor_unmarkable,
        ext_flow_cum=ext_flow_cum,
        had_unknown_ledger=had_unknown,
    )


def _last_fill_price(fills: list[dict], coin: str, t_ms: int) -> float:
    """Notional-proxy price for an unmarkable coin position.

    Prefers the last traded price at-or-before t_ms. If the position was carried
    in from pre-window history (seeded via startPosition, no fill at-or-before
    t_ms), fall back to the FIRST fill price after t_ms so we still produce a
    sane notional estimate. 0.0 only if the coin never appears in the fills.
    """
    px_before = 0.0
    px_after = 0.0
    for f in fills:
        if f["coin"] != coin:
            continue
        if f["time"] <= t_ms:
            px_before = f["price"]
        elif px_after == 0.0:
            px_after = f["price"]
    return px_before if px_before > 0.0 else px_after


# --------------------------------------------------------------------------- #
# Per-wallet reconstruction + accuracy diagnostics
# --------------------------------------------------------------------------- #


def _liquidation_days(fills: list[dict]) -> set:
    """UTC dates on which a liquidation fill occurred."""
    days = set()
    for f in fills:
        if f.get("is_liquidation"):
            days.add(pd.Timestamp(f["time"], unit="ms", tz="UTC").floor("D").date())
    return days


def reconstruct_wallet(args: tuple) -> dict:
    """Reconstruct one wallet's whole-account perp equity EOD series + audit.

    Returns a dict with either {'error': ...} or {'series': df, 'audit': {...}}.
    In validate mode (validation_only=True) the series is still built (needed
    for the accuracy table) but accuracy functions are computed and surfaced.
    """
    if len(args) == 5:
        wallet, anchor, start_ms, end_ms, validation_only = args
        causal_seed = True
    else:
        wallet, anchor, start_ms, end_ms, validation_only, causal_seed = args
    wallet_lc = wallet.lower()

    if anchor is None:
        return {"wallet": wallet, "error": "no_anchor"}

    # 1) Weekly anchor truth: all included perp dexes, excluding spot.
    avh = get_portfolio_perp(wallet)
    n_sentinel_zeros = sum(1 for _, v in avh if v == 0.0)
    valid_anchors = [(t, v) for t, v in avh if v > 0.01]
    if not valid_anchors:
        return {"wallet": wallet, "error": "no_valid_anchors"}

    # Reconstruct (EMIT) only up to the anchor fetch time. But LOAD events through
    # the anchor fetch time so a coin opened in the gap between mark-coverage-end
    # (end_ms) and the later anchor fetch is NOT mis-classified as "never traded"
    # and silently backfilled into history (codex P0). The walk filters events by
    # t_ms <= walk_end_ms, so loading past walk_end_ms changes seed CLASSIFICATION
    # only, never the emitted equity.
    walk_end_ms = min(end_ms, anchor.fetched_ms)
    load_end_ms = max(walk_end_ms, int(anchor.fetched_ms))

    # 2) Load local event sources (through fetch time; see note above).
    fills = load_wallet_fills(wallet, start_ms, load_end_ms)
    funding = load_wallet_funding(wallet, start_ms, load_end_ms)
    ledger = load_wallet_ledger(wallet, start_ms, load_end_ms)

    stream: list[tuple[int, str, dict]] = []
    for f in fills:
        stream.append((f["time"], "fill", f))
    for e in ledger:
        stream.append((int(e["time"]), "ledger", e))
    for e in funding:
        stream.append((int(e["time"]), "funding", e))
    stream.sort(key=lambda x: x[0])

    # In-window weekly anchors (bound to >= start_ms so the walk never picks a
    # pre-start anchor and walk through unloaded events).
    window_anchors = [(t, v) for t, v in valid_anchors if start_ms <= t <= walk_end_ms]

    liq_days = _liquidation_days(fills)
    source_dexes = sorted({coin_dex(f["coin"]) for f in fills} | {
        coin_dex(c) for c in anchor.positions
    })
    # Perp dexes the wallet touches that have NO row in the anchor parquet
    # (cash/hyna/para/vntl). All dexes are IN SCOPE and reconstructed from fills;
    # this is now an INFORMATIONAL flag (not a hard quarantine), since a position
    # we cannot see at all only mis-states equity if it MOVES -> which surfaces as
    # inter-anchor drift and quarantines via the drift/missing-mark gate anyway.
    extradex_no_anchor = sorted(
        d for d in source_dexes
        if dex_in_scope(d) and d not in ANCHOR_COVERED_DEXES
    )
    has_extradex_no_anchor = bool(extradex_no_anchor)

    # True series-start cumulative external flow (P1 fix: wr.ext_flow_cum is only
    # the per-SEGMENT flow since the active anchor and resets each anchor). We
    # accumulate ledger ext_flow over (effective_start, eod] independently.
    ledger_ext_events = sorted(
        (int(e["time"]), ledger_cash_delta(e, wallet_lc).ext_flow) for e in ledger
    )

    # 3) Build the EOD series.
    earliest_anchor_ms = window_anchors[0][0] if window_anchors else valid_anchors[0][0]
    effective_start_ms = max(earliest_anchor_ms, start_ms)
    current_day = pd.Timestamp(effective_start_ms, unit="ms", tz="UTC").floor("D").date()
    end_day = pd.Timestamp(walk_end_ms, unit="ms", tz="UTC").floor("D").date()

    rows: list[dict] = []
    had_unknown_any = False
    n_incomplete_rows = 0
    while current_day <= end_day:
        eod_ms = int(pd.Timestamp(current_day, tz="UTC").timestamp() * 1000 + 86_399_999)
        eod_ms = min(eod_ms, walk_end_ms)
        before = [(t, v) for t, v in window_anchors if t <= eod_ms]
        if not before:
            current_day = current_day + pd.Timedelta(days=1)
            continue
        anchor_t, anchor_v = before[-1]
        # SELECTION series: causal seed by default for downstream M5 eligibility.
        # Anchor cash-snap marks remain point-in-time MTM via get_mark(..., causal=False).
        wr = compute_eq_at(
            stream, fills, anchor, wallet_lc, eod_ms, anchor_t, anchor_v,
            causal_seed=causal_seed,
        )
        had_unknown_any = had_unknown_any or wr.had_unknown_ledger
        # Row is incomplete only if we genuinely cannot value the book this day
        # (missing anchor/terminal mark). Touching a no-anchor-row dex is NOT an
        # incompleteness by itself (all dexes in scope) -> reconstruct it; hidden
        # exposure is caught by the inter-anchor drift quarantine below.
        row_incomplete = wr.recon_incomplete
        if row_incomplete:
            n_incomplete_rows += 1

        # True cumulative external flow from series start through this EOD.
        ext_flow_cum_total = sum(
            fl for ts, fl in ledger_ext_events
            if effective_start_ms < ts <= eod_ms
        )

        # return since the at-or-before weekly anchor for this day (NOT recon
        # drift: drift is only meaningful at the validation anchor checks).
        ret_pct = (wr.equity - anchor_v) / anchor_v if abs(anchor_v) > 0.01 else np.nan
        rows.append(
            {
                "wallet": wallet,
                "date": current_day,
                "equity_usd": wr.equity,
                "cash": wr.cash,
                "position_value_usd": wr.position_value,
                "gross_position_notional_usd": wr.gross_position_notional,
                "n_positions": len(wr.positions),
                "ext_flow_cum": ext_flow_cum_total,
                "segment_ext_flow": wr.ext_flow_cum,
                "n_unmarkable_positions": wr.n_unmarkable,
                "unmarkable_notional_usd": wr.unmarkable_notional,
                "anchor_age_h": (eod_ms - anchor_t) / 3_600_000,
                "return_since_anchor_pct": ret_pct,
                "recon_incomplete": row_incomplete,
                "has_liquidation_in_day": current_day in liq_days,
                "source_dexes": ",".join(source_dexes),
            }
        )
        current_day = current_day + pd.Timedelta(days=1)

    if not rows:
        return {"wallet": wallet, "error": "no_rows"}

    df_out = pd.DataFrame(rows)

    # 4) Accuracy diagnostics MUST validate the exact seed mode used by the
    # emitted series.  Quarantine consumes inter-anchor drift, so using a later
    # snapshot-backed ex-post seed here while emitting causal rows would make
    # the acceptance test look better/different than its actual input artifact.
    inter = inter_anchor_drift(
        stream, fills, anchor, wallet_lc, window_anchors, causal_seed=causal_seed
    )
    recon = segment_reconcile(
        stream, fills, anchor, wallet_lc, window_anchors, fills,
        causal_seed=causal_seed,
    )

    audit = {
        "n_fills": len(fills),
        "n_funding": len(funding),
        "n_ledger": len(ledger),
        "n_weekly_anchors_in_window": len(window_anchors),
        "n_sentinel_zeros": n_sentinel_zeros,
        "max_inter_anchor_drift_pct": inter["max_drift_pct"],
        "median_inter_anchor_drift_pct": inter["median_drift_pct"],
        "n_inter_anchor_checks": inter["n_checks"],
        "n_segments_reconciled": recon["n_segments"],
        "max_segment_reconcile_err_usd": recon["max_err_usd"],
        "median_segment_reconcile_err_usd": recon["median_err_usd"],
        "unknown_ledger_types": sorted(
            {
                e.get("delta", {}).get("type", "")
                for e in ledger
                if ledger_cash_delta(e, wallet_lc).unknown
            }
        ),
        "has_flx_anchor": anchor.has_flx_anchor,
        "has_unmarkable_dex_anchor": anchor.has_unmarkable_dex_anchor,
        "has_extradex_no_anchor": has_extradex_no_anchor,
        "extradex_no_anchor": ",".join(extradex_no_anchor),
        "has_unmarkable_in_series": bool(df_out["n_unmarkable_positions"].sum() > 0),
        "max_unmarkable_notional_usd": float(df_out["unmarkable_notional_usd"].max()),
        "n_incomplete_rows": n_incomplete_rows,
        "frac_incomplete_rows": float(n_incomplete_rows / len(df_out)) if len(df_out) else np.nan,
        "source_dexes": ",".join(source_dexes),
    }
    # Quarantine only when equity genuinely cannot be trusted: unknown ledger
    # types, OR a material fraction of rows had missing marks / incomplete seeding
    # (P0). All dexes are in scope now, so merely touching a no-anchor-row dex
    # (cash/hyna/para/vntl) is NOT a quarantine -> those positions reconstruct
    # from fills, and any unseen/moving exposure surfaces as inter-anchor drift.
    # Inter-anchor drift gate (codex backstop for hidden no-anchor-dex exposure):
    # an out-of-sample walk that lands >X% from the next weekly anchor means the
    # reconstruction is provably wrong for that wallet (e.g. a hidden position
    # absorbed into cash whose value moved). Quarantine on the proven error, not
    # on dex membership. NaN drift (all walks incomplete) is handled by frac below.
    _med = audit["median_inter_anchor_drift_pct"]
    _max = audit["max_inter_anchor_drift_pct"]
    drift_fail = (
        (_med == _med and _med > 0.10) or (_max == _max and _max > 0.50)
    )
    audit["quarantined"] = bool(
        had_unknown_any
        or audit["unknown_ledger_types"]
        or drift_fail
        or audit["n_inter_anchor_checks"] < MIN_INTER_ANCHOR_CHECKS
        or (audit["frac_incomplete_rows"] == audit["frac_incomplete_rows"]
            and audit["frac_incomplete_rows"] > 0.10)
    )
    audit["equity_validation_sufficient"] = bool(
        audit["n_inter_anchor_checks"] >= MIN_INTER_ANCHOR_CHECKS
    )

    if validation_only:
        # Today cross-check + position validation compare reconstruction AT the
        # anchor-parquet fetch time against the parquet's own snapshot (same
        # timestamp = apples-to-apples). Position state needs fills up to
        # fetched_ms (which may be AFTER walk_end / mark coverage); positions
        # are mark-INDEPENDENT, so we load an extended fill set just for this.
        ext_fills = load_wallet_fills(wallet, start_ms, anchor.fetched_ms)
        ext_funding = load_wallet_funding(wallet, start_ms, anchor.fetched_ms)
        ext_ledger = load_wallet_ledger(wallet, start_ms, anchor.fetched_ms)
        ext_stream: list[tuple[int, str, dict]] = (
            [(f["time"], "fill", f) for f in ext_fills]
            + [(int(e["time"]), "ledger", e) for e in ext_ledger]
            + [(int(e["time"]), "funding", e) for e in ext_funding]
        )
        ext_stream.sort(key=lambda x: x[0])
        cc = today_crosscheck(
            wallet, ext_stream, ext_fills, anchor, wallet_lc, window_anchors, df_out,
            causal_seed=causal_seed,
        )
        pv = position_validation(
            ext_stream, ext_fills, anchor, wallet_lc, window_anchors, causal_seed=causal_seed
        )
        audit.update(cc)
        audit.update(pv)

    return {"wallet": wallet, "series": df_out, "audit": audit}


def inter_anchor_drift(
    stream, fills, anchor: AnchorState, wallet_lc, window_anchors,
    causal_seed: bool = True,
) -> dict:
    """Accuracy #1: for each weekly anchor (except the first), reconstruct equity
    AT the current anchor's timestamp by walking forward from the PREVIOUS
    anchor, then compare to the current anchor's reported value. This is a
    genuine out-of-sample test of the walk: the measured point is the anchor
    itself, but the walk is seeded from the PRIOR anchor (no snap at cur_t).

    Walks flagged incomplete (missing seed/mark) are excluded from the drift
    distribution -- those wallets are quarantined separately, not silently
    counted as low-drift."""
    drifts: list[float] = []
    for i in range(1, len(window_anchors)):
        prev_t, prev_v = window_anchors[i - 1]
        cur_t, cur_v = window_anchors[i]
        if cur_t <= prev_t:
            continue  # anchors at/within the same instant; nothing to test
        wr = compute_eq_at(
            stream, fills, anchor, wallet_lc, cur_t, prev_t, prev_v,
            causal_seed=causal_seed,
        )
        # compute_eq_at includes events at ts == cur_t (ts > anchor_ms AND
        # ts <= t_ms), matching HL's accountValue snapshot which is post-event.
        if wr.recon_incomplete:
            continue
        if abs(cur_v) > 0.01:
            drifts.append((wr.equity - cur_v) / cur_v)
    if not drifts:
        return {"max_drift_pct": np.nan, "median_drift_pct": np.nan, "n_checks": 0}
    a = np.abs(drifts)
    return {
        "max_drift_pct": float(a.max()),
        "median_drift_pct": float(np.median(a)),
        "n_checks": len(drifts),
    }


def segment_reconcile(
    stream, fills, anchor: AnchorState, wallet_lc, window_anchors, all_fills,
    causal_seed: bool = True,
) -> dict:
    """Accuracy #2: realized-PnL reconciliation per inter-anchor segment.

    Self-financing identity (cross-check ONLY):
        (eq_{a+1} - eq_a) - ext_flow_segment
            ~= -sum(fees) + sum(funding)
                + delta(marked_position_value) - sum(signed_size * fill_price)

    ``delta(position value) - net traded`` is TOTAL trading PnL, including
    realized PnL. Adding ``closedPnl`` again double-counts every close (a
    buy-100/sell-110 round trip became $20 instead of $10 in the old audit).
    ``closedPnl`` remains source data but is not part of this independent cash+
    marked-book identity.
    """
    errs: list[float] = []
    for i in range(1, len(window_anchors)):
        a_t, a_v = window_anchors[i - 1]
        b_t, b_v = window_anchors[i]

        # ext_flow over the segment from the walk.
        ext = 0.0
        seg_fees = 0.0
        seg_funding = 0.0
        for ts, typ, ev in stream:
            if ts <= a_t or ts > b_t:
                continue
            if typ == "fill":
                # ``fee`` is the API total; builder/deployer fields are its
                # component breakdown and are not additional wallet charges.
                seg_fees += ev["fee"]
            elif typ == "funding":
                seg_funding += funding_cash_delta(ev)
            elif typ == "ledger":
                ext += ledger_cash_delta(ev, wallet_lc).ext_flow

        # Total trading PnL: marked position value change minus the value put on
        # / taken off by fills. This already includes both realized and
        # unrealized PnL and is independent of the source closedPnl field.
        wr_a = compute_eq_at(
            stream, fills, anchor, wallet_lc, a_t, a_t, a_v,
            causal_seed=causal_seed,
        )
        wr_b = compute_eq_at(
            stream, fills, anchor, wallet_lc, b_t, a_t, a_v,
            causal_seed=causal_seed,
        )
        # net cash paid into positions over the segment (sign: buying costs cash)
        net_traded = 0.0
        for ts, typ, ev in stream:
            if ts <= a_t or ts > b_t:
                continue
            if typ == "fill":
                net_traded += ev["signed_sz"] * ev["price"]
        trading_pnl = (wr_b.position_value - wr_a.position_value) - net_traded

        lhs = (b_v - a_v) - ext
        rhs = -seg_fees + seg_funding + trading_pnl
        errs.append(abs(lhs - rhs))

    if not errs:
        return {"n_segments": 0, "max_err_usd": np.nan, "median_err_usd": np.nan}
    return {
        "n_segments": len(errs),
        "max_err_usd": float(np.max(errs)),
        "median_err_usd": float(np.median(errs)),
    }


def today_crosscheck(
    wallet, ext_stream, ext_fills, anchor: AnchorState, wallet_lc, window_anchors, df_out,
    causal_seed: bool = True,
) -> dict:
    """Accuracy #3: cross-check the reconstruction against authoritative snapshots.

    Two references are reported:
      - main_xyz_anchor_snapshot_pct: WHOLE-ACCOUNT reconstruction AT
        anchor.fetched_ms vs the anchor parquet's aggregate accountValue. NOTE
        the parquet aggregate is MAIN+XYZ ONLY (it has no cash/hyna/para/vntl
        rows), so for any extra-dex wallet this intentionally compares a
        whole-account number to a main+xyz partial -> it is only meaningful for
        pure main/xyz wallets and is LABELLED as such. The whole-account clean
        check is today_crosscheck_pct below.
      - today_crosscheck_pct: same reconstruction vs the LIVE clearinghouseState
        sum-of-ALL-dexes accountValue (fetched now). This is the genuine
        whole-account cross-check; it carries whatever the account moved between
        the parquet snapshot and now, so it is time-stale but dex-complete.

    Note: marks past 2026-05-23 do not exist, so position value at fetched_ms is
    marked at the last available candle. For the anchor_snapshot cross-check this
    introduces a mark-staleness term equal to position drift over (markEnd ->
    fetch), which we surface but cannot remove without oracle marks (future work).
    """
    out: dict = {}
    # Reconstruction at the parquet fetch time from the last in-window anchor.
    recon_at_fetch = np.nan
    if window_anchors:
        a_t, a_v = window_anchors[-1]
        wr = compute_eq_at(
            ext_stream, ext_fills, anchor, wallet_lc, anchor.fetched_ms, a_t, a_v,
            causal_seed=causal_seed,
        )
        recon_at_fetch = wr.equity
    out["recon_equity_at_fetch_usd"] = recon_at_fetch
    out["anchor_aggregate_acct_value_usd"] = anchor.aggregate_acct_value
    if anchor.aggregate_acct_value and abs(anchor.aggregate_acct_value) > 0.01 and recon_at_fetch == recon_at_fetch:
        out["main_xyz_anchor_snapshot_pct"] = (
            recon_at_fetch - anchor.aggregate_acct_value
        ) / anchor.aggregate_acct_value
    else:
        out["main_xyz_anchor_snapshot_pct"] = np.nan

    # Live clearinghouse sum over ALL perp dexes (informational; time-stale by
    # design). Built from the known HIP-3 set UNION the dexes this wallet was
    # actually seen on (anchor parquet), so we never miss a dex it trades. main
    # is queried as dex=None.
    KNOWN_PERP_DEXES = {"xyz", "cash", "hyna", "para", "vntl", "flx", "km"}
    seen = {d for d in anchor.dexes_seen if d and d != "main"}
    dexes = [None] + sorted(KNOWN_PERP_DEXES | seen)
    total = 0.0
    got_any = False
    for dex in dexes:
        cs = get_clearinghouse(wallet, dex)
        if not cs:
            continue
        av = cs.get("marginSummary", {}).get("accountValue")
        if av is not None:
            total += float(av)
            got_any = True
    out["today_live_equity_usd"] = total if got_any else np.nan
    if got_any and recon_at_fetch == recon_at_fetch and abs(total) > 0.01:
        out["today_crosscheck_pct"] = (recon_at_fetch - total) / total
    else:
        out["today_crosscheck_pct"] = np.nan
    return out


def position_validation(
    stream, fills, anchor: AnchorState, wallet_lc, window_anchors, causal_seed: bool = True
) -> dict:
    """Accuracy #4: reconstructed per-coin positions AT the anchor-parquet fetch
    time vs the parquet positions_json (main+xyz). Both reference the SAME
    timestamp (anchor.fetched_ms), so the extended fill stream (loaded to
    fetched_ms) is required — positions are mark-independent.

    Reports per-coin mismatch count (rel error > 1% AND abs error > 1e-6) plus
    the worst absolute size error for diagnostics.
    """
    if not window_anchors:
        return {
            "n_position_mismatches": np.nan,
            "n_positions_checked": 0,
            "max_position_abs_err": np.nan,
            "position_validation_status": "no_anchor",
        }
    # The anchor parquet snapshots positions at anchor.fetched_ms. Our local S3
    # fills may end BEFORE fetched_ms (the daily parquets lag the anchor pull).
    # If so, reconstructed positions reflect an earlier moment than the parquet
    # snapshot and the comparison is cross-time / not meaningful. Detect and flag.
    last_fill_ms = max((f["time"] for f in fills), default=0)
    gap_h = (anchor.fetched_ms - last_fill_ms) / 3_600_000 if last_fill_ms else float("inf")
    if gap_h > 24:
        return {
            "n_position_mismatches": np.nan,
            "n_positions_checked": 0,
            "max_position_abs_err": np.nan,
            "position_validation_status": f"fills_end_{gap_h:.0f}h_before_anchor",
        }
    a_t, a_v = window_anchors[-1]
    wr = compute_eq_at(stream, fills, anchor, wallet_lc, anchor.fetched_ms, a_t, a_v,
                       causal_seed=causal_seed)
    recon = wr.positions
    mismatches = 0
    checked = 0
    max_abs_err = 0.0
    coins = set(recon) | set(anchor.positions)
    for c in coins:
        if coin_is_dropped(c) or coin_is_spot(c):
            continue
        r = recon.get(c, 0.0)
        a = float(anchor.positions.get(c, 0.0))
        checked += 1
        err = abs(r - a)
        max_abs_err = max(max_abs_err, err)
        denom = max(abs(a), abs(r), 1e-6)
        if err > 1e-6 and err / denom > 0.01:
            mismatches += 1
    return {
        "n_position_mismatches": mismatches,
        "n_positions_checked": checked,
        "max_position_abs_err": max_abs_err,
        "position_validation_status": "ok",
    }


# --------------------------------------------------------------------------- #
# M02 BRIDGE — additive per-event equity series (NO reconstruction-MATH change)
# --------------------------------------------------------------------------- #
#
# This block is ADDITIVE for V15 M02 (journey_trace). It does NOT touch the
# existing daily/audit reconstruction above; it reuses the SAME helpers
# (seed_positions, get_mark, fill/ledger/funding cash deltas) to emit, per
# wallet, a PER-EVENT equity_post series in the TOTAL EVENT ORDER required by
# the M02 design spec:
#
#   event_order key = (ts_ms, type_rank, source_seq)
#     type_rank: ledger=0, fill=1, funding=2   (ledger settles, then trade,
#                then funding accrues on the post-trade position)
#     source_seq: stable causal fill sequence; per-type append index otherwise.
#
# For each event k we re-anchor to the last weekly anchor with anchor_ts
# STRICTLY < ts(k) (codex r3: an anchor sharing the action's ms is NOT a valid
# base), walk forward applying every event with order <= k (incl. this one),
# then mark-to-market at ts(k). equity_post(k) therefore uses ONLY data with
# order <= k — no look-ahead. We also emit the per-event reliability fields M02
# needs to pick its equity-basis mode (markable_all, n_unmarkable, frozen
# component value/age, age_since_anchor, mark_ts, anchor_ts) — all derived from
# data <= k only.

# type_rank for the total event order (codex r3/r4).
_TYPE_RANK = {"ledger": 0, "fill": 1, "funding": 2}

# STALE_CAP: fall back if reconstruction base anchor is older than one anchor
# cycle (7d) while material unmarkable exposure exists. Tunable (spec).
STALE_CAP_MS = 7 * 86_400_000

SPOT_USDC_INVARIANT_NOTE = (
    "HL is a unified wallet: spot USDC IS perp margin. The weekly perpAllTime "
    "anchor (marginSummary.accountValue summed across perp dexes) already "
    "includes collateral (USDC) + uPnL. Re-anchoring snaps cash := anchor_eq - "
    "Sum pos*mark, so the spot-USDC/collateral component is captured inside "
    "`cash` and carried causally by ledger deposit/withdraw/transfer deltas. "
    "equity_post is therefore whole-account (collateral-inclusive) by "
    "construction; see test_m01_spot_collateral_invariant."
)


def build_event_stream(
    fills: list[dict],
    funding: list[dict],
    ledger: list[dict],
) -> list[dict]:
    """Merge fills+funding+ledger into the TOTAL EVENT ORDER (spec v4).

    Returns a list of event dicts each carrying:
        ts, type ('fill'|'funding'|'ledger'), type_rank, source_seq, ev (payload)
    sorted by (ts, type_rank, source_seq). source_seq is the causal per-wallet
    fill sequence, else a stable per-type append index so the order is deterministic
    and reproducible across M01 (equity) and M02 (actions).

    SAME-MS FILL ORDER: tid is identity, not sequence. Partial fills are ordered
    by chaining startPosition -> startPosition+signed_size. The fill payload is
    carried directly so M2 never rematches on a lossy key.
    """
    out: list[dict] = []
    ordered_fills = order_wallet_fills_causally(list(fills))
    for i, f in enumerate(ordered_fills):
        if not coin_is_allowed_perp(f["coin"]):
            continue
        seq = int(f.get("fill_seq", i))
        out.append(
            {
                "ts": int(f["time"]),
                "type": "fill",
                "type_rank": _TYPE_RANK["fill"],
                "source_seq": seq,
                "ev": f,
            }
        )
    for i, e in enumerate(ledger):
        out.append(
            {
                "ts": int(e["time"]),
                "type": "ledger",
                "type_rank": _TYPE_RANK["ledger"],
                "source_seq": i,
                "ev": e,
            }
        )
    for i, e in enumerate(funding):
        out.append(
            {
                "ts": int(e["time"]),
                "type": "funding",
                "type_rank": _TYPE_RANK["funding"],
                "source_seq": i,
                "ev": e,
            }
        )
    out.sort(key=lambda x: (x["ts"], x["type_rank"], x["source_seq"]))
    return out


@dataclass
class EventEquity:
    """Per-event reconstruction output (whole-account, causal)."""

    event_order: int           # strictly monotone index in the ordered stream
    ts: int
    type: str
    source_seq: int
    equity_post: float         # whole-account equity AFTER this event
    cash: float
    position_value: float
    anchor_ts: Optional[int]   # base anchor (strictly < ts); None if no past anchor
    anchor_equity: Optional[float]
    age_since_anchor_ms: Optional[int]
    mark_ts: Optional[int]     # latest mark <= ts across marked positions
    markable_all: bool         # every held nonzero position markable at ts
    n_unmarkable: int
    no_extradex_without_anchor: bool
    frozen_component_value: float   # last-known value of unmarkable positions
    frozen_component_age_ms: int    # staleness of the frozen value at ts
    has_past_anchor: bool
    fill: Optional[dict] = None     # for type=='fill': the EXACT fill payload of
                                    # this event (codex r4 BUG 2) — M02 consumes it
                                    # directly off the stream, never rematching by
                                    # (ts, tid). None for funding/ledger events.


def compute_event_equity(
    ordered: list[dict],
    fills: list[dict],
    anchor: "AnchorState",
    wallet_lc: str,
    window_anchors: list[tuple[int, float]],
    extradex_no_anchor: bool,
    causal_mark: bool = True,
) -> list[EventEquity]:
    """Walk the ordered event stream ONCE and emit equity_post per event.

    Causal contract (spec NON-LOOK-AHEAD INVARIANTS):
      * base anchor for event k = last weekly anchor with anchor_ts STRICTLY < ts(k).
      * equity_post(k) = cash + Sum pos*mark(ts(k)) using ONLY events order <= k.
        By default the terminal live-position MTM uses closed causal marks so
        M02's sizing denominator matches its causal numerator.
      * mark_ts <= ts(k); anchor_ts < ts(k); event_order strictly monotone.

    We re-seed/re-snap cash whenever the active base anchor changes (each weekly
    anchor cycle), exactly like compute_eq_at, then incrementally apply each
    event's cash/position delta. Mark-to-market is recomputed at each event ts
    over the live positions, tracking unmarkable positions' last-known value
    (the FROZEN component) so M02 can do PARTIAL_MTM without look-ahead.
    """
    results: list[EventEquity] = []

    # Active base-anchor state. Re-initialised lazily when the base anchor for
    # the current event differs from the one in force.
    active_anchor_ts: Optional[int] = None
    active_anchor_eq: Optional[float] = None
    cash = 0.0
    positions: dict[str, float] = {}
    # last-known mark value per coin (for the frozen unmarkable component)
    last_val: dict[str, float] = {}
    last_val_ts: dict[str, int] = {}
    # Coins whose carry-in (pre-anchor holding) has already been folded into the
    # book+cash under the ACTIVE anchor. Reset per anchor cycle (codex r4 BUG 1).
    carried_in: set[str] = set()
    # AUDIT 2026-07-10 (codex m01 P1a): the anchor cash SNAP (cash = a_v - Sum seed*mark(a_t)) OMITS any
    # seeded/carry-in position UNMARKABLE at the anchor time -> cash (and equity_post) overstated while
    # markable_all was falsely True. Track per-cycle incompleteness; when set, the denominator is unreliable
    # -> the event fails CLOSED (has_past_anchor=False -> M02 NO_ANCHOR).
    anchor_snap_incomplete = [False]   # list = mutable closure cell
    # AUDIT 2026-07-10 (codex m01 P1b): an UNKNOWN ledger type applies cash=0 (its real move is unknown), so
    # equity after it is silently wrong. Once one occurs at ts <= event, the cash is untrustworthy -> taint
    # every event at/after it (causal). Precompute the unknown-ledger timestamps once.
    import bisect as _bisect
    _unknown_ledger_ts = sorted(
        it["ts"] for it in ordered
        if it["type"] == "ledger" and ledger_cash_delta(it["ev"], wallet_lc).unknown
    )

    def _tainted_by_unknown_ledger(ts: int) -> bool:
        return _bisect.bisect_right(_unknown_ledger_ts, ts) > 0

    def _carry_in_fill(ev: dict) -> None:
        """CAUSAL replacement for seed_positions case 2 (codex r4 BUG 1).

        seed_positions(causal_cutoff=True) deliberately omits a coin first traded
        AFTER the active anchor, to avoid look-ahead at earlier events. But that
        coin DID hold a pre-anchor position (its first fill's startPosition), which
        the anchor equity a_v already includes. The first time we REACH that coin's
        fill in the ordered walk (order <= k = causal), we fold its carry-in in the
        SAME way case 2 + the cash snap would have: add startPosition to the book
        AND subtract its anchor-time value from cash (mark at active_anchor_ts < ts,
        causal) so equity is not double-counted. Coins already in the anchor seed,
        or already folded, are skipped. Identical end-state to compute_eq_at, but
        revealed only at the causal moment instead of at the anchor (no leak at
        earlier events)."""
        nonlocal cash
        coin = ev["coin"]
        if coin in carried_in or active_anchor_ts is None:
            return
        carried_in.add(coin)
        sp = float(ev.get("startPosition", 0.0) or 0.0)
        if abs(sp) <= 1e-9:
            return
        positions[coin] = positions.get(coin, 0.0) + sp
        m = get_mark(coin, active_anchor_ts)
        if m is not None:
            cash -= sp * m  # mirror the anchor cash snap for this carried-in coin
            last_val[coin] = m  # store PRICE (frozen re-values at current size)
            last_val_ts.setdefault(coin, active_anchor_ts)
        else:
            # codex m01 P1a: carry-in coin unmarkable at the anchor -> its anchor value was NOT removed from
            # cash -> the snap is incomplete -> equity denominator unreliable for this cycle. Fail closed.
            anchor_snap_incomplete[0] = True

    def _past_anchor(ts: int) -> Optional[tuple[int, float]]:
        chosen = None
        for a_t, a_v in window_anchors:
            if a_t < ts:            # STRICT (codex r3)
                chosen = (a_t, a_v)
            else:
                break
        return chosen

    for order, item in enumerate(ordered):
        ts = item["ts"]
        base = _past_anchor(ts)

        if base is None:
            # No past anchor → cannot reconstruct a causal denominator. Still
            # advance the position book from startPosition-derived seeding so
            # that once an anchor appears the book is correct, but emit
            # has_past_anchor=False (M02 maps this to NO_ANCHOR).
            # We seed positions at this ts from fills only (causal: positions_at
            # uses startPosition + cumulative signed_sz, all <= ts).
            ev = item["ev"]
            if item["type"] == "fill":
                positions[ev["coin"]] = positions.get(ev["coin"], 0.0) + ev["signed_sz"]
                if abs(positions[ev["coin"]]) < 1e-9:
                    positions.pop(ev["coin"], None)
            results.append(
                EventEquity(
                    event_order=order, ts=ts, type=item["type"],
                    source_seq=item["source_seq"], equity_post=float("nan"),
                    cash=float("nan"), position_value=float("nan"),
                    anchor_ts=None, anchor_equity=None, age_since_anchor_ms=None,
                    mark_ts=None, markable_all=False, n_unmarkable=0,
                    no_extradex_without_anchor=(not extradex_no_anchor),
                    frozen_component_value=0.0, frozen_component_age_ms=0,
                    has_past_anchor=False,
                    fill=(item["ev"] if item["type"] == "fill" else None),
                )
            )
            continue

        a_t, a_v = base
        if a_t != active_anchor_ts:
            # New base anchor cycle → re-seed positions at the anchor and snap
            # cash, identically to compute_eq_at. This rebuild uses only fills
            # (startPosition + cumulative) at/<= anchor_ts, which is < ts → causal.
            active_anchor_ts = a_t
            active_anchor_eq = a_v
            anchor_snap_incomplete[0] = False   # codex m01 P1a: fresh cycle, re-assess snap completeness
            # 2026-07-10 fix (frozen P1#4): the last-known-price map is CYCLE-SCOPED. A stale price for a
            # coin closed in a prior cycle must NOT value a same-named position reopened in this cycle.
            last_val.clear()
            last_val_ts.clear()
            seeded = seed_positions(fills, anchor, a_t, causal_cutoff=True)
            anchor_pos_value = 0.0
            for c, sz in seeded.items():
                if abs(sz) < 1e-9:
                    continue
                m = get_mark(c, a_t)
                if m is not None:
                    anchor_pos_value += sz * m
                    last_val[c] = m            # store PRICE (frozen re-values at current size)
                    last_val_ts[c] = a_t
                else:
                    # codex m01 P1a: seeded position unmarkable at the anchor -> its value is NOT subtracted
                    # from the cash snap -> incomplete denominator -> fail closed for this cycle.
                    anchor_snap_incomplete[0] = True
            cash = a_v - anchor_pos_value
            positions = {c: s for c, s in seeded.items() if abs(s) > 1e-9}
            # Coins already in the anchor seed are carried-in; their fills must NOT
            # re-fold a startPosition. Coins NOT seeded (first traded after anchor)
            # get folded causally at their first fill via _carry_in_fill.
            carried_in = set(seeded.keys())
            # Replay events strictly between the anchor and this event (anchor_ts
            # < e_ts < ts) so the book/cash reflect everything order <= current.
            for prev in ordered:
                p_ts = prev["ts"]
                if p_ts <= a_t:
                    continue
                # stop once we reach the current event's order position
                if (p_ts, prev["type_rank"], prev["source_seq"]) >= (
                    ts, item["type_rank"], item["source_seq"]
                ):
                    break
                if prev["type"] == "fill":
                    _carry_in_fill(prev["ev"])
                _apply(prev, positions, last_val, last_val_ts, _cash_ref=None)
                cash = _apply_cash(prev, cash, wallet_lc)

        # Apply THIS event (order == k included).
        if item["type"] == "fill":
            _carry_in_fill(item["ev"])
        _apply(item, positions, last_val, last_val_ts, _cash_ref=None)
        cash = _apply_cash(item, cash, wallet_lc)

        # Mark-to-market at ts over live positions; track frozen component.
        pos_value = 0.0
        n_unmarkable = 0
        mark_ts_latest: Optional[int] = None
        frozen_value = 0.0
        frozen_age = 0
        for c, sz in positions.items():
            if abs(sz) < 1e-9:
                continue
            m = get_mark(c, ts, causal=causal_mark)
            if m is None:
                n_unmarkable += 1
                # 2026-07-10 fix (frozen P0/P1): last_val stores the last known PRICE, so the frozen
                # value re-prices at the CURRENT size. Previously it stored the full VALUE at the last
                # markable size, so a size change during a mark gap double-counted (e.g. sell 9 of 10
                # while unmarkable -> frozen still showed all 10). A coin never priced in this cycle has
                # no last price -> contributes 0 to frozen (unchanged PARTIAL_MTM contract; disclosed via
                # markable_all=False), it is NOT fabricated into equity.
                lp = last_val.get(c)
                if lp is not None:
                    frozen_value += sz * lp
                    if c in last_val_ts:
                        frozen_age = max(frozen_age, ts - last_val_ts[c])
                continue
            pos_value += sz * m
            last_val[c] = m  # store PRICE, not value
            mk = ts // 60_000 * 60_000 - (60_000 if causal_mark else 0)
            last_val_ts[c] = mk
            mark_ts_latest = mk if mark_ts_latest is None else max(mark_ts_latest, mk)

        equity_post = cash + pos_value  # markable + cash; frozen excluded here
        whole_equity = equity_post + frozen_value

        # AUDIT 2026-07-10 (codex m01 P1a/P1b/P2): fail CLOSED when the reconstructed denominator is untrustworthy
        # -> emit the NO_ANCHOR form (M02 -> unsizeable), never a falsely-clean anchored equity:
        #   (a) anchor snap incomplete (a seed/carry-in coin was unmarkable at the anchor -> cash overstated),
        #   (b) an unknown ledger type at/before this ts (its real cash move was applied as 0),
        #   (c) any non-finite cash/equity (inf mark or NaN funding/ledger delta leaked through).
        _degraded = (anchor_snap_incomplete[0]
                     or _tainted_by_unknown_ledger(ts)
                     or not np.isfinite(cash) or not np.isfinite(whole_equity))
        if _degraded:
            results.append(
                EventEquity(
                    event_order=order, ts=ts, type=item["type"],
                    source_seq=item["source_seq"], equity_post=float("nan"),
                    cash=float("nan"), position_value=float("nan"),
                    anchor_ts=None, anchor_equity=None, age_since_anchor_ms=None,
                    mark_ts=None, markable_all=False, n_unmarkable=max(1, n_unmarkable),
                    no_extradex_without_anchor=(not extradex_no_anchor),
                    frozen_component_value=0.0, frozen_component_age_ms=0,
                    has_past_anchor=False,
                    fill=(item["ev"] if item["type"] == "fill" else None),
                )
            )
            continue
        results.append(
            EventEquity(
                event_order=order, ts=ts, type=item["type"],
                source_seq=item["source_seq"],
                equity_post=whole_equity,  # whole-account incl frozen
                cash=cash, position_value=pos_value,
                anchor_ts=a_t, anchor_equity=a_v,
                age_since_anchor_ms=ts - a_t,
                mark_ts=mark_ts_latest if mark_ts_latest is not None else (ts if not positions else None),
                markable_all=(n_unmarkable == 0),
                n_unmarkable=n_unmarkable,
                no_extradex_without_anchor=(not extradex_no_anchor),
                frozen_component_value=frozen_value,
                frozen_component_age_ms=frozen_age,
                has_past_anchor=True,
                fill=(item["ev"] if item["type"] == "fill" else None),
            )
        )
    return results


def _apply(item: dict, positions: dict, last_val: dict, last_val_ts: dict, _cash_ref=None) -> None:
    """Apply an event's POSITION delta (cash handled by _apply_cash)."""
    if item["type"] == "fill":
        ev = item["ev"]
        coin = ev["coin"]
        positions[coin] = positions.get(coin, 0.0) + ev["signed_sz"]
        if abs(positions[coin]) < 1e-9:
            positions.pop(coin, None)


def _apply_cash(item: dict, cash: float, wallet_lc: str) -> float:
    """Apply an event's CASH delta (mirrors compute_eq_at's walk)."""
    t = item["type"]
    ev = item["ev"]
    if t == "fill":
        return cash + fill_cash_delta(ev)
    if t == "ledger":
        return cash + ledger_cash_delta(ev, wallet_lc).cash
    if t == "funding":
        return cash + funding_cash_delta(ev)
    return cash


def reconstruct_wallet_event_equity(args: tuple) -> dict:
    """M02 bridge driver: emit the per-event equity series + anchors for one wallet.

    Returns {'wallet', 'events': [EventEquity...], 'weekly_anchors': [(ts,eq)],
    'fills': [...], 'funding': [...], 'inter_anchor_drift': {...} (diagnostic
    only, NEVER used to switch modes), 'extradex_no_anchor': bool}.
    Mirrors reconstruct_wallet's loading + ordering exactly so equity_post is
    consistent with the audited daily reconstruction.
    """
    wallet, anchor, start_ms, end_ms = args
    wallet_lc = wallet.lower()
    if anchor is None:
        return {"wallet": wallet, "error": "no_anchor"}

    avh = get_portfolio_perp(wallet)
    valid_anchors = [(t, v) for t, v in avh if v > 0.01]
    if not valid_anchors:
        return {"wallet": wallet, "error": "no_valid_anchors"}

    walk_end_ms = min(end_ms, anchor.fetched_ms)
    load_end_ms = max(walk_end_ms, int(anchor.fetched_ms))

    fills = load_wallet_fills(wallet, start_ms, load_end_ms)
    funding = load_wallet_funding(wallet, start_ms, load_end_ms)
    ledger = load_wallet_ledger(wallet, start_ms, load_end_ms)

    window_anchors = [(t, v) for t, v in valid_anchors if start_ms <= t <= walk_end_ms]
    if not window_anchors:
        return {"wallet": wallet, "error": "no_window_anchors"}

    source_dexes = sorted({coin_dex(f["coin"]) for f in fills} | {
        coin_dex(c) for c in anchor.positions
    })
    extradex_no_anchor = bool([
        d for d in source_dexes
        if dex_in_scope(d) and d not in ANCHOR_COVERED_DEXES
    ])

    ordered = build_event_stream(fills, funding, ledger)
    # Only emit events up to walk_end_ms (mark coverage); load_end may exceed it
    # but those tail events have no marks and are not sizing-relevant for M02.
    ordered = [e for e in ordered if e["ts"] <= walk_end_ms]

    events = compute_event_equity(
        ordered, fills, anchor, wallet_lc, window_anchors, extradex_no_anchor
    )

    # diagnostic only (never a mode switch): reuse the existing M01 drift calc.
    stream = [(e["ts"], e["type"], e["ev"]) for e in ordered]
    inter = inter_anchor_drift(stream, fills, anchor, wallet_lc, window_anchors)

    return {
        "wallet": wallet,
        "events": events,
        "weekly_anchors": window_anchors,
        "fills": fills,
        "funding": funding,
        "extradex_no_anchor": extradex_no_anchor,
        "inter_anchor_drift": inter,
    }


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def _print_accuracy_table(results: list[dict]) -> None:
    """Pretty-print the per-wallet accuracy table (validate mode)."""
    logger.info("\n" + "=" * 124)
    logger.info("PER-WALLET ACCURACY TABLE")
    logger.info(
        "  inter_drift = forward-walk recon vs NEXT weekly anchor (out-of-sample). "
        "snap% = recon@fetch vs anchor-parquet aggregate (same ts). "
        "today% = recon@fetch vs LIVE clearinghouse (time-stale, informational)."
    )
    logger.info("=" * 124)
    hdr = (
        f"{'wallet':<16} {'dexes':<22} {'#anch':>5} {'inter_drift med/max%':>22} "
        f"{'seg_err med/max $':>20} {'snap%':>8} {'today%':>8} {'pos_mis':>8} {'quar':>5}"
    )
    logger.info(hdr)
    logger.info("-" * 124)
    for r in results:
        w = r["wallet"]
        if "error" in r:
            logger.info(f"{w[:14]:<16} ERROR: {r['error']}")
            continue
        a = r["audit"]
        med = a.get("median_inter_anchor_drift_pct", np.nan)
        mx = a.get("max_inter_anchor_drift_pct", np.nan)
        smed = a.get("median_segment_reconcile_err_usd", np.nan)
        smx = a.get("max_segment_reconcile_err_usd", np.nan)
        snap = a.get("main_xyz_anchor_snapshot_pct", np.nan)
        today = a.get("today_crosscheck_pct", np.nan)
        posmis = a.get("n_position_mismatches", np.nan)
        dexes = a.get("source_dexes", "")

        def _p(x):
            return f"{x*100:.2f}" if x == x else "n/a"  # NaN check

        def _d(x):
            return f"{x:.2f}" if x == x else "n/a"

        logger.info(
            f"{w[:14]:<16} {dexes[:20]:<22} "
            f"{a.get('n_weekly_anchors_in_window', 0):>5} "
            f"{_p(med)+'/'+_p(mx):>22} "
            f"{_d(smed)+'/'+_d(smx):>20} "
            f"{_p(snap):>8} "
            f"{_p(today):>8} "
            f"{str(posmis):>8} "
            f"{('Y' if a.get('quarantined') else '-'):>5}"
        )
    logger.info("=" * 124)


def main() -> None:
    ap = argparse.ArgumentParser(description="V15 M01 whole-account equity reconstructor")
    ap.add_argument("--wallets-file", required=True, help="One wallet address per line")
    ap.add_argument("--start", default="2025-12-01", help="YYYY-MM-DD")
    ap.add_argument("--end", default="2026-05-23", help="YYYY-MM-DD (clamped to mark coverage)")
    ap.add_argument("--output", required=True, help="Output parquet path")
    ap.add_argument("--validate", action="store_true", help="Print accuracy table")
    ap.add_argument(
        "--ex-post-seed",
        action="store_true",
        help=(
            "Use legacy ex-post position seeding for the emitted EOD selection series "
            "(diagnostic studies only; not deployable). Default is causal seeding. "
            "Audit diagnostics always use the same mode as the emitted series."
        ),
    )
    ap.add_argument("--headroom-gb", type=float, default=6.0,
                    help="RAM reserved for the live baseline (agents + mongod + postgres); the main "
                         "memory-guard soft cap is lowered to (free - headroom) when RAM is tight.")
    args = ap.parse_args()

    # Rule 8 (mandatory streaming I/O): abort LOUDLY on runaway RSS instead of silent OS SIGKILL.
    # 2026-06-10 OOM fix: m01 is single-process, so model the process itself as the one "worker"
    # (main_reserve=0). Aborts BEFORE work if the box cannot fit it; else lowers the main guard to
    # what is grantable (<= free - headroom) so it cannot exhaust a box running the live baseline.
    _budget = plan_memory_budget(requested_procs=1, per_worker_gb=2.0, main_reserve_gb=0.0,
                                 headroom_gb=args.headroom_gb, main_soft_cap=12.0)
    install_memory_guard(soft_gb=_budget.main_soft_gb, label="m01")

    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    with open(args.wallets_file) as f:
        wallets = [
            line.strip().lower()
            for line in f
            if line.strip() and not line.startswith("#")
        ]
    logger.info(f"Loaded {len(wallets):,} wallets (lowercased)")

    anchor_df = pd.read_parquet(ANCHOR_PARQUET)
    logger.info(f"Loaded anchor parquet: {len(anchor_df):,} rows")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Rule 8: stream per-wallet series to disk in bounded chunks via ShardedParquetWriter; NEVER
    # concat all wallet-days into one DataFrame in RAM. We keep ONLY the lightweight per-wallet
    # audit/error records (one row per wallet, bounded) for the audit parquet + validate table.
    # Rule 8 (Alberto 2026-07-11 correction TG 11199): persist as it goes. 100k rows ~= every ~675
    # wallets at ~148 rows/wallet -> RAM bounded to tens of MB + progress durable on disk, never
    # buffer the full result (the OOM / lost-progress footgun we kept repeating with a 2M threshold).
    series_writer = ShardedParquetWriter(out_path, flush_rows=100_000)
    audits: list[dict] = []
    results_lite: list[dict] = []          # only wallet/error/audit keys (NO heavy series) for table
    n_with_series = 0
    uniq_wallets: set = set()
    uniq_dates: set = set()

    t0 = time.time()
    for j, w in enumerate(wallets, 1):
        anchor = load_wallet_anchor(w, anchor_df)
        try:
            res = reconstruct_wallet((w, anchor, start_ms, end_ms, args.validate, not args.ex_post_seed))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"  wallet exception {w[:12]}: {e!r}")
            res = {"wallet": w, "error": f"exception:{e!r}"}

        if "error" in res:
            logger.warning(f"  [{j}/{len(wallets)}] {w[:12]} -> {res['error']}")
            results_lite.append({"wallet": res["wallet"], "error": res["error"]})
        else:
            df_w = res["series"]
            logger.info(
                f"  [{j}/{len(wallets)}] {w[:12]} -> {len(df_w)} rows, "
                f"inter-drift med {res['audit'].get('median_inter_anchor_drift_pct', float('nan'))*100:.2f}%"
            )
            # stream this wallet's rows to disk, then drop the DataFrame (bounded RAM)
            if len(df_w):
                series_writer.add_many(df_w.to_dict("records"))
                n_with_series += 1
                uniq_wallets.add(w)
                uniq_dates.update(df_w["date"].tolist())
            audits.append({"wallet": res["wallet"], **res["audit"]})
            results_lite.append({"wallet": res["wallet"], "audit": res["audit"]})

    if args.validate:
        _print_accuracy_table(results_lite)

    if n_with_series:
        total_rows = series_writer.close()
        audit_df = pd.DataFrame(audits)
        audit_df.to_parquet(out_path.with_suffix(".audit.parquet"), index=False, compression="snappy")
        logger.info(
            f"\nWrote {out_path}: {total_rows:,} rows "
            f"({len(uniq_wallets)} wallets x {len(uniq_dates)} days)"
        )
        logger.info(f"Wrote audit: {out_path.with_suffix('.audit.parquet')}")
    else:
        # codex finding a: do NOT write args.output on this failure path. close() would stitch an
        # EMPTY no-schema parquet to out_path and CLOBBER a prior valid artifact. abort() discards the
        # (empty) staging parts and leaves out_path untouched -> the old artifact is preserved.
        series_writer.abort()
        logger.error("No series produced for any wallet. args.output left untouched (prior artifact preserved).")
        sys.exit(2)

    logger.info(f"Wall: {(time.time()-t0)/60:.2f} min")


if __name__ == "__main__":
    main()
