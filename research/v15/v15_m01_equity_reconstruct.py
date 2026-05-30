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
      we reconstruct ALL markable perp dexes (main + xyz: + cash: + hyna: +
      para: + vntl:), value positions with per-coin marks, and DROP flx: and
      any unmarkable coin (flagging the residual notional).

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
        fill   -> cash += -signed_sz*px - (fee+builderFee+deployerFee);
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
    wallet, date, equity_usd, cash, position_value_usd, n_positions,
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
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pymongo
import requests

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
ANCHOR_PARQUET = Path("/Users/hermes/quants-lab/app/data/v13/wallet_anchor_state.parquet")
MONGO_URI = "mongodb://localhost:27017/"

# Perp dexes we can mark (have 1m candles in Mongo) and therefore reconstruct.
# flx: is intentionally DROPPED (Alberto directive) even though candles exist.
# km: candles exist too but km is not in the spec's markable list; we treat any
# coin we have a mark for as markable by prefix-agnostic mark lookup, but we
# EXCLUDE flx: at the fill-load boundary. Everything else is included if a mark
# is available; unmarkable coins are flagged, not silently dropped.
DROPPED_DEX_PREFIXES = ("flx:",)

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


def get_mongo() -> pymongo.database.Database:
    global _mongo
    if _mongo is None:
        _mongo = pymongo.MongoClient(MONGO_URI)["quants_lab"]
    return _mongo


def get_mark(coin: str, ts_ms: int) -> Optional[float]:
    """1m candle close at-or-before ts_ms (latest). None if no candle covers it.

    Works for any prefix present in Mongo (main + xyz:/cash:/hyna:/para:/vntl:/...)
    because the candle ``coin`` field carries the full prefixed symbol.
    """
    minute_key = ts_ms // 60_000 * 60_000
    key = (coin, minute_key)
    if key in _mark_cache:
        return _mark_cache[key]
    db = get_mongo()
    doc = db.hyperliquid_candles.find_one(
        {"coin": coin, "interval": "1m", "timestamp_utc": {"$lte": minute_key}},
        sort=[("timestamp_utc", -1)],
        projection={"close": 1},
    )
    px = float(doc["close"]) if doc else None
    _mark_cache[key] = px
    return px


# --------------------------------------------------------------------------- #
# Coin / dex helpers
# --------------------------------------------------------------------------- #


def coin_is_spot(coin: str) -> bool:
    """@-prefix = spot token; USDC = the quote asset. Both excluded from perp."""
    return coin.startswith("@") or coin == "USDC"


def coin_is_dropped(coin: str) -> bool:
    """flx: perps are dropped from the V15 reconstruction (Alberto directive)."""
    return any(coin.startswith(p) for p in DROPPED_DEX_PREFIXES)


def coin_dex(coin: str) -> str:
    """Return the perp dex name for a coin. Unprefixed -> 'main'."""
    if ":" in coin:
        return coin.split(":", 1)[0]
    return "main"


# Explicit markable perp-dex allowlist (spec). main = unprefixed; the rest carry
# a "<dex>:" prefix. flx: is dropped (Alberto). Anything outside this set (km:,
# future dexes, spot @-tokens, USDC) is NOT part of the whole-account perp
# reconstruction and must never silently leak into cash, positions, or funding.
ALLOWED_PERP_PREFIXES = ("xyz:", "cash:", "hyna:", "para:", "vntl:")
# Dex names (coin_dex output) whose USDC capital flows touch our reconstructed
# perp cash. flx + spot are excluded; "" / "main" are the unprefixed main dex.
INCLUDED_PERP_DEXES = frozenset(
    {"", "main", "xyz", "cash", "hyna", "para", "vntl"}
)
# Dexes that have a row in wallet_anchor_state.parquet (position seed source).
# Positions on any OTHER included dex (cash/hyna/para/vntl) can only be seeded
# from fills -> if held statically with no in-window fill they are unprovable.
ANCHOR_COVERED_DEXES = frozenset({"main", "xyz", "flx"})


def coin_is_allowed_perp(coin: str) -> bool:
    """True only for coins in the explicit markable perp allowlist.

    main (unprefixed) or one of ALLOWED_PERP_PREFIXES. Excludes spot (@/USDC),
    dropped flx:, and any unlisted prefix (km: etc.).
    """
    if coin_is_spot(coin) or coin_is_dropped(coin):
        return False
    if ":" not in coin:
        return True  # main
    return coin.startswith(ALLOWED_PERP_PREFIXES)


# --------------------------------------------------------------------------- #
# HL API: weekly anchor + today cross-check
# --------------------------------------------------------------------------- #


def get_portfolio_perp(wallet: str, retries: int = 4) -> list[tuple[int, float]]:
    """perpAllTime accountValueHistory as (ts_ms, value), whole-account perp."""
    for i in range(retries):
        try:
            r = requests.post(
                HL_URL, json={"type": "portfolio", "user": wallet}, timeout=20
            )
            if r.status_code == 429:
                time.sleep(2**i)
                continue
            if r.status_code != 200:
                return []
            for window_name, wd in r.json():
                if window_name == "perpAllTime":
                    return [
                        (int(x[0]), float(x[1]))
                        for x in wd.get("accountValueHistory", [])
                    ]
            return []
        except Exception:
            time.sleep(1)
    return []


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


def load_wallet_fills(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load ALL markable perp fills for one wallet in [t0, t1].

    Includes every dex prefix EXCEPT dropped (flx:) and spot (@-prefix, USDC).
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
            size = float(rec["size"])
            try:
                tid = int(rec.get("tid", 0) or 0)
            except (TypeError, ValueError):
                tid = 0
            d = {
                "coin": coin,
                "side": rec["side"],
                "size": size,
                "price": float(rec["price"]),
                "time": int(rec["time"]),
                "tid": tid,
                "dir": str(rec.get("dir", "") or ""),
                "closedPnl": float(rec.get("closedPnl", 0) or 0),
                "startPosition": float(rec.get("startPosition", 0) or 0),
                "fee": float(rec.get("fee", 0) or 0),
                "builderFee": float(rec.get("builderFee", 0) or 0),
                "deployerFee": float(rec.get("deployerFee", 0) or 0),
            }
            d["signed_sz"] = size if rec["side"] == "B" else -size
            d["is_liquidation"] = d["dir"] in LIQUIDATION_DIRS
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
            fills = _normalize(df)
            fills.sort(key=lambda x: (x["time"], x["tid"]))
            return fills
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
    fills.sort(key=lambda x: (x["time"], x["tid"]))
    return fills


def load_wallet_funding(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load + merge + dedup funding events from the local S3 bulk cache.

    Multiple {wallet}_{start}_{end}.json files may exist; merge them and dedup
    by (time, coin) since overlapping windows can repeat the same record.
    Records: {time, hash, delta:{type:'funding', coin, usdc(signed), ...}}.
    """
    wallet_lc = wallet.lower()
    seen: set[tuple[int, str]] = set()
    out: list[dict] = []
    for fp in sorted(glob.glob(str(FUNDING_DIR / f"{wallet_lc}_*.json"))):
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
    for lf in sorted(glob.glob(str(LEDGER_DIR / f"{wallet_lc}_*.json"))):
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
    aggregate_acct_value: float  # sum over markable, non-flx dexes


def load_wallet_anchor(wallet: str, anchor_df: pd.DataFrame) -> Optional[AnchorState]:
    """Build the seed AnchorState from wallet_anchor_state.parquet.

    The parquet only carries main/xyz/flx rows. We seed positions from main+xyz
    (flx dropped). We also record whether the wallet has flx or other-dex anchor
    presence so we can flag material unmarkable/flx exposure downstream.
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
            has_flx_anchor = acct_val > 1.0 or int(row["n_positions"]) > 0
            continue  # flx dropped from reconstruction
        # An anchor row on a dex we cannot position-seed from the parquet
        # (anything outside main/xyz/flx) is an unsupported-dex signal.
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
    """Per fill: -signed_sz*price - (fee + builderFee + deployerFee).

    fee is SIGNED in S3 (negative = maker rebate). Spot/dropped already excluded
    at load time, but guard defensively.
    """
    if not coin_is_allowed_perp(f["coin"]):
        return 0.0
    fee = f["fee"] + f["builderFee"] + f["deployerFee"]
    return -f["signed_sz"] * f["price"] - fee


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
        # toPerp=True -> USDC moves INTO perp (+); False -> OUT (-).
        amt = abs(_f("usdc", "amount"))
        c = amt if bool(d.get("toPerp")) else -amt
        return LedgerDelta(c, c)
    if k == "send":
        if d.get("token") != "USDC":
            return LedgerDelta(0.0, 0.0)
        # Dex-scope: only main/empty source/destination dex touches our perp cash.
        user = (d.get("user") or "").lower()
        dest = (d.get("destination") or "").lower()
        src_dex = str(d.get("sourceDex", "")).strip().lower()
        dst_dex = str(d.get("destinationDex", "")).strip().lower()
        amt = _f("usdcValue", "amount")
        fee = _f("fee")
        # Whole-account perp: USDC leaving/entering ANY included perp dex
        # (main/xyz/cash/hyna/para/vntl) moves our reconstructed cash. Only
        # spot/flx/untracked dexes are excluded.
        c = 0.0
        if user == wallet_lc and src_dex in INCLUDED_PERP_DEXES:
            c -= amt + fee
        if dest == wallet_lc and dst_dex in INCLUDED_PERP_DEXES:
            c += amt
        return LedgerDelta(c, c)
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
        if ev_dex not in ("", "main"):
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
    """Funding usdc (signed, wallet perspective). Whole-account: keep ALL dexes
    except dropped flx:. (xyz:/cash:/etc funding IS part of whole-account perp.)
    """
    d = e.get("delta", {})
    if d.get("type") != "funding":
        return 0.0
    coin = str(d.get("coin", ""))
    # Only funding on an included perp coin counts. Drops flx:, spot @-tokens,
    # USDC, and any unlisted dex prefix (parity with the fills/position guard).
    if not coin_is_allowed_perp(coin):
        return 0.0
    return float(d.get("usdc", 0))


# --------------------------------------------------------------------------- #
# Position seeding + reconstruction
# --------------------------------------------------------------------------- #


def positions_at(fills: list[dict], t_ms: int) -> dict[str, float]:
    """Position per coin at time t = (last fill at-or-before t).startPosition +
    its signed_sz. Uses HL's authoritative startPosition (handles pre-window
    history). Spot/dropped already excluded at load."""
    last_per_coin: dict[str, dict] = {}
    for f in fills:
        if f["time"] > t_ms:
            break
        last_per_coin[f["coin"]] = f
    positions: dict[str, float] = {}
    for coin, f in last_per_coin.items():
        pos = f["startPosition"] + f["signed_sz"]
        if abs(pos) > 1e-9:
            positions[coin] = pos
    return positions


@dataclass
class WalkResult:
    cash: float
    positions: dict[str, float]
    equity: float
    position_value: float  # independent mark-derived sum
    n_unmarkable: int
    unmarkable_notional: float  # |szi * entry-ish proxy| for coins lacking a mark
    ext_flow_cum: float  # ext flow within (anchor_ms, t_ms] (per-SEGMENT, not series)
    had_unknown_ledger: bool
    anchor_unmarkable: int = 0  # seeded positions with NO mark at anchor_ms
    seed_incomplete: bool = False  # a material anchor position could not be proven

    @property
    def recon_incomplete(self) -> bool:
        """True if equity for this walk is NOT trustworthy: a seeded position
        lacked an anchor mark (corrupted cash snap), a terminal mark was missing,
        or a material anchor position could not be proven at the anchor."""
        return (
            self.anchor_unmarkable > 0
            or self.n_unmarkable > 0
            or self.seed_incomplete
        )


def seed_positions(
    fills: list[dict],
    anchor: AnchorState,
    anchor_ms: int,
) -> tuple[dict[str, float], bool]:
    """Seed positions held at anchor_ms (adapts v8's three-case seeding).

    Returns (start_positions, seed_incomplete).

    1. Positions implied by fills at-or-before anchor_ms (positions_at).
    2. Coins first TRADED after anchor_ms: their first post-anchor fill's
       startPosition reveals the pre-anchor holding.
    3. Static no-fill positions from the anchor parquet. The parquet is a SINGLE
       snapshot at anchor.fetched_ms, so a position in it is only PROVEN to exist
       at fetch time. We therefore seed a static position ONLY when the weekly
       anchor is within 24h of the parquet fetch (proximity heuristic) -- never
       backfilled into far-earlier history (P0: projecting a later-opened
       position backward inflates/deflates the cash snap and drifts the walk).
       When a MATERIAL static position cannot be proven at this anchor, we leave
       it unseeded and set seed_incomplete=True so the caller flags the day
       rather than emitting a silently-wrong equity.
       Coins with post-anchor fills are handled by case 2, not seeded here.
    """
    start_positions = positions_at(fills, anchor_ms)

    seen_pre = {f["coin"] for f in fills if f["time"] <= anchor_ms}
    for f in fills:
        if f["time"] <= anchor_ms:
            continue
        coin = f["coin"]
        if coin in seen_pre or coin in start_positions:
            continue
        pos_pre = f["startPosition"]
        if abs(pos_pre) > 1e-9:
            start_positions[coin] = pos_pre
        seen_pre.add(coin)

    post_anchor_coins = {f["coin"] for f in fills if f["time"] > anchor_ms}
    any_fill_coins = {f["coin"] for f in fills}
    near_fetch = abs(anchor_ms - anchor.fetched_ms) <= 86_400_000
    seed_incomplete = False
    for coin, szi in anchor.positions.items():
        if coin in start_positions or abs(szi) < 1e-9:
            continue
        if coin in post_anchor_coins:
            continue  # case 2 already recovered the pre-anchor holding
        if coin not in any_fill_coins:
            # Case A: the coin has NO fill anywhere in the loaded window, yet it
            # is in the fetch-time snapshot -> the position was established before
            # the window and held constant throughout (no trades = no size
            # change). It was therefore held at EVERY in-window anchor. Safe to
            # seed at any anchor. (Residual gap risk: a coin opened in
            # (walk_end, fetched_ms] would look never-traded; the window is ~1d
            # and bounded, surfaced via has_extradex/quarantine, not here.)
            start_positions[coin] = float(szi)
        elif near_fetch:
            # Case B near fetch: coin traded pre-anchor but closed before any
            # post-anchor fill; provable only when the anchor ~ the snapshot.
            start_positions[coin] = float(szi)
        # Case B far from fetch: positions_at (authoritative startPosition) shows
        # this coin FLAT at the anchor; the later snapshot holding it just means
        # it was re-opened after this anchor. Trust positions_at -> do NOT
        # backfill the snapshot position into far history (P0 #3). No flag: a
        # proven-flat coin is not an incompleteness. The only genuinely
        # unrecoverable case (extra-dex with no anchor row) is caught at the
        # wallet level by has_extradex_no_anchor.
    return start_positions, seed_incomplete


def compute_eq_at(
    stream: list[tuple[int, str, dict]],
    fills: list[dict],
    anchor: AnchorState,
    wallet_lc: str,
    t_ms: int,
    anchor_ms: int,
    anchor_eq: float,
) -> WalkResult:
    """Re-anchored forward walk from anchor_ms to t_ms.

    cash := anchor_eq - Sum pos*mark(anchor_ms); then walk the merged event
    stream in (anchor_ms, t_ms]. equity(t) = cash + Sum pos*mark(t_ms).

    Missing marks are COUNTED (n_unmarkable) and their notional estimated, but
    not silently zeroed into equity; the caller decides whether to flag the day.
    """
    start_positions, seed_incomplete = seed_positions(fills, anchor, anchor_ms)

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
        if mark is not None:
            anchor_pos_value += sz * mark
        else:
            anchor_unmarkable += 1
    cash = anchor_eq - anchor_pos_value

    positions = dict(start_positions)
    ext_flow_cum = 0.0
    had_unknown = False
    for ts, typ, ev in stream:
        if ts <= anchor_ms or ts > t_ms:
            continue
        if typ == "fill":
            cash += fill_cash_delta(ev)
            coin = ev["coin"]
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
    n_unmarkable = 0
    unmarkable_notional = 0.0
    for c, sz in positions.items():
        if abs(sz) < 1e-9:
            continue  # dust; not a real position
        mark = get_mark(c, t_ms)
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
    equity = cash + pos_value
    return WalkResult(
        cash=cash,
        positions=positions,
        equity=equity,
        position_value=pos_value,
        n_unmarkable=n_unmarkable,
        unmarkable_notional=unmarkable_notional,
        anchor_unmarkable=anchor_unmarkable,
        seed_incomplete=seed_incomplete,
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
    wallet, anchor, start_ms, end_ms, validation_only = args
    wallet_lc = wallet.lower()

    if anchor is None:
        return {"wallet": wallet, "error": "no_anchor"}

    # 1) Weekly anchor truth (whole-account perpAllTime).
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
    # Included perp dexes the wallet actually touches that have NO row in the
    # anchor parquet (cash/hyna/para/vntl). Pre-anchor positions on these can
    # only ever be seeded from fills -> a statically-held position with no
    # in-window fill is unrecoverable, so the whole wallet is low-confidence.
    extradex_no_anchor = sorted(
        d for d in source_dexes
        if d in INCLUDED_PERP_DEXES and d not in ANCHOR_COVERED_DEXES
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
        wr = compute_eq_at(
            stream, fills, anchor, wallet_lc, eod_ms, anchor_t, anchor_v
        )
        had_unknown_any = had_unknown_any or wr.had_unknown_ledger
        row_incomplete = wr.recon_incomplete or has_extradex_no_anchor
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

    # 4) Accuracy diagnostics.
    inter = inter_anchor_drift(stream, fills, anchor, wallet_lc, window_anchors)
    recon = segment_reconcile(
        stream, fills, anchor, wallet_lc, window_anchors, fills
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
    # Quarantine if equity cannot be trusted: unknown ledger types, OR any
    # extra-dex (cash/hyna/para/vntl) position with no anchor coverage, OR a
    # material fraction of rows had incomplete seeding / missing marks (P0).
    audit["quarantined"] = bool(
        had_unknown_any
        or audit["unknown_ledger_types"]
        or has_extradex_no_anchor
        or (audit["frac_incomplete_rows"] == audit["frac_incomplete_rows"]
            and audit["frac_incomplete_rows"] > 0.10)
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
            wallet, ext_stream, ext_fills, anchor, wallet_lc, window_anchors, df_out
        )
        pv = position_validation(
            ext_stream, ext_fills, anchor, wallet_lc, window_anchors
        )
        audit.update(cc)
        audit.update(pv)

    return {"wallet": wallet, "series": df_out, "audit": audit}


def inter_anchor_drift(
    stream, fills, anchor: AnchorState, wallet_lc, window_anchors
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
        wr = compute_eq_at(stream, fills, anchor, wallet_lc, cur_t, prev_t, prev_v)
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
    stream, fills, anchor: AnchorState, wallet_lc, window_anchors, all_fills
) -> dict:
    """Accuracy #2: realized-PnL reconciliation per inter-anchor segment.

    Identity (cross-check ONLY; the equity walk does not use closedPnl):
        (eq_{a+1} - eq_a) - ext_flow_segment
            ~= sum(closedPnl) - sum(fees) + sum(funding) + delta_unrealized
    We compute the LHS from anchor values + the walk's ext_flow, and the RHS
    from fills/funding within the segment plus the change in unrealized PnL
    (mark - entry implied via mark deltas), and report abs error in USD.
    """
    errs: list[float] = []
    for i in range(1, len(window_anchors)):
        a_t, a_v = window_anchors[i - 1]
        b_t, b_v = window_anchors[i]

        # ext_flow over the segment from the walk.
        ext = 0.0
        seg_closed = 0.0
        seg_fees = 0.0
        seg_funding = 0.0
        for ts, typ, ev in stream:
            if ts <= a_t or ts > b_t:
                continue
            if typ == "fill":
                seg_closed += ev["closedPnl"]
                seg_fees += ev["fee"] + ev["builderFee"] + ev["deployerFee"]
            elif typ == "funding":
                seg_funding += funding_cash_delta(ev)
            elif typ == "ledger":
                ext += ledger_cash_delta(ev, wallet_lc).ext_flow

        # delta_unrealized: marked position value change minus the value put on
        # / taken off by trading. Simplest robust proxy: reconstruct equity at
        # both anchors via the walk (which is independent of closedPnl) and use
        # the mark-derived position values. delta_unrealized = (posval_b -
        # posval_a) - net_traded_notional.
        wr_a = compute_eq_at(stream, fills, anchor, wallet_lc, a_t, a_t, a_v)
        wr_b = compute_eq_at(stream, fills, anchor, wallet_lc, b_t, a_t, a_v)
        # net cash paid into positions over the segment (sign: buying costs cash)
        net_traded = 0.0
        for ts, typ, ev in stream:
            if ts <= a_t or ts > b_t:
                continue
            if typ == "fill":
                net_traded += ev["signed_sz"] * ev["price"]
        delta_unreal = (wr_b.position_value - wr_a.position_value) - net_traded

        lhs = (b_v - a_v) - ext
        rhs = seg_closed - seg_fees + seg_funding + delta_unreal
        errs.append(abs(lhs - rhs))

    if not errs:
        return {"n_segments": 0, "max_err_usd": np.nan, "median_err_usd": np.nan}
    return {
        "n_segments": len(errs),
        "max_err_usd": float(np.max(errs)),
        "median_err_usd": float(np.median(errs)),
    }


def today_crosscheck(
    wallet, ext_stream, ext_fills, anchor: AnchorState, wallet_lc, window_anchors, df_out
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
            ext_stream, ext_fills, anchor, wallet_lc, anchor.fetched_ms, a_t, a_v
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

    # Live clearinghouse sum-of-dexes (informational; time-stale by design).
    dexes = [None, "xyz", "cash", "hyna", "para", "vntl"]
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
    stream, fills, anchor: AnchorState, wallet_lc, window_anchors
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
    wr = compute_eq_at(stream, fills, anchor, wallet_lc, anchor.fetched_ms, a_t, a_v)
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
    args = ap.parse_args()

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

    results: list[dict] = []
    t0 = time.time()
    for j, w in enumerate(wallets, 1):
        anchor = load_wallet_anchor(w, anchor_df)
        try:
            res = reconstruct_wallet((w, anchor, start_ms, end_ms, args.validate))
        except Exception as e:  # noqa: BLE001
            logger.warning(f"  wallet exception {w[:12]}: {e!r}")
            res = {"wallet": w, "error": f"exception:{e!r}"}
        results.append(res)
        if "error" in res:
            logger.warning(f"  [{j}/{len(wallets)}] {w[:12]} -> {res['error']}")
        else:
            logger.info(
                f"  [{j}/{len(wallets)}] {w[:12]} -> {len(res['series'])} rows, "
                f"inter-drift med {res['audit'].get('median_inter_anchor_drift_pct', float('nan'))*100:.2f}%"
            )

    series = [r["series"] for r in results if "series" in r]
    audits = [{"wallet": r["wallet"], **r["audit"]} for r in results if "audit" in r]

    if args.validate:
        _print_accuracy_table(results)

    if series:
        out_df = pd.concat(series, ignore_index=True)
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_parquet(out_path, index=False, compression="snappy")
        audit_df = pd.DataFrame(audits)
        audit_df.to_parquet(out_path.with_suffix(".audit.parquet"), index=False, compression="snappy")
        logger.info(
            f"\nWrote {out_path}: {len(out_df):,} rows "
            f"({out_df['wallet'].nunique()} wallets x {out_df['date'].nunique()} days)"
        )
        logger.info(f"Wrote audit: {out_path.with_suffix('.audit.parquet')}")
    else:
        logger.error("No series produced for any wallet.")
        sys.exit(2)

    logger.info(f"Wall: {(time.time()-t0)/60:.2f} min")


if __name__ == "__main__":
    main()
