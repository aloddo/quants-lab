#!/usr/bin/env python3
"""V13 Script 1/5 (v2): Wallet equity series reconstruction.

Per projects/quant/v13 Section 5.1 and remediation plan v2.

The equity series for each wallet is built as:

    equity[t] = today_api_equity
              - (cum_realized[end .. today]    - cum_realized[end .. t])
              - (cum_ledger_net[end .. today]  - cum_ledger_net[end .. t])
              - (mtm_unrealized[today]         - mtm_unrealized[t])

Equivalently, the daily delta is composed of realized PnL + signed ledger flow
+ change in unrealized PnL. Equity is ANCHORED on today's live API equity,
which is the only externally-verifiable ground truth.

Unrealized PnL at time t is computed against AVERAGE COST BASIS, not against
notional. Opening a position does not change equity; the position holds value
equal to its mark price minus its cost basis times quantity.

Carry-in handling: each (wallet, coin) is walked back through prior S3 fills
to the last point where net position was zero (last-flat). If we cannot reach
last-flat within available data, the pair is marked PRE_WINDOW_INCOMPLETE and
excluded from journey-level metrics and backtest signal replay.

Historical end-date handling: if --end < today, the equity series is back-
solved by walking activity from end to today via the same identity above. If
data between end and today is missing for any wallet, that wallet is marked
INCOMPLETE and skipped (not silently anchored on a future-leaking value).

Inputs:
    --wallets <path>        Newline-separated wallet addresses, OR
    --discover-from-fills   Use unique wallets in the S3 fill data
    --max-wallets N         Cap the wallet count (default: 200)
    --start YYYY-MM-DD      First date in the equity series (default: earliest fill)
    --end YYYY-MM-DD        Last date (default: today)
    --output <path>         app/data/v13/wallet_equity_series.parquet

Outputs:
    wallet_equity_series.parquet, columns:
        wallet, date,
        perp_account_value_usd,         # daily reconstructed perp series (PnL math)
        spot_usdc_today,                # wallet-level scalar, sizing per rule 16
        perp_acct_value_today,          # wallet-level scalar, current deployment
        realized_pnl_cum, ledger_net_cum,
        ledger_nonfunding_cum, funding_cum,
        mtm_unrealized, audit_today_diff_pct,
        audit_perp_anchor_zero,
        audit_min_reconstructed_perp_value,
        audit_perp_series_went_negative,
        audit_vault_flow_unverified,
        audit_unknown_ledger_type_count,
        audit_missing_field_count,
        audit_perp_series_min_abs_denominator,
        audit_perp_pct_return_bad_denominator_count,
        carry_in_status, pre_window_position_value

Semantics (Alberto rule 16 + decision A 2026-05-26 17:09 CEST):
- spot_usdc_today is the canonical HL EQUITY (sizing denominator) for downstream.
- perp_account_value_usd (daily) is the performance / PnL series, NOT equity.
- Downstream consumers MUST pick the right column for the semantic intent.
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import resource
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
import requests
from pymongo import MongoClient


# v3 OOM PREVENTION (Alberto-locked 2026-05-26): code-enforced memory guards.
# RLIMIT_DATA hard kills if process tries to alloc > 6GB. psutil monitor
# triggers graceful abort if RSS exceeds 4GB during run.
def install_memory_guards(rlimit_data_gb: float = 6.0, rss_abort_gb: float = 4.0) -> None:
    """Install OOM prevention guards. Call at start of main()."""
    # Hard cap via RLIMIT_DATA — kernel kills process if exceeded
    try:
        cap_bytes = int(rlimit_data_gb * 1024 ** 3)
        resource.setrlimit(resource.RLIMIT_DATA, (cap_bytes, cap_bytes))
        logging.getLogger(__name__).info(
            f"RLIMIT_DATA hard cap installed: {rlimit_data_gb:.1f}GB"
        )
    except (ValueError, OSError) as e:
        logging.getLogger(__name__).warning(
            f"RLIMIT_DATA install failed (likely macOS limit): {e}"
        )

    # Soft monitor via psutil — graceful abort at lower threshold
    pid = os.getpid()
    abort_bytes = int(rss_abort_gb * 1024 ** 3)

    def monitor():
        proc = psutil.Process(pid)
        peak = 0
        while True:
            try:
                rss = proc.memory_info().rss
                if rss > peak:
                    peak = rss
                if rss > abort_bytes:
                    logging.getLogger(__name__).error(
                        f"RSS abort: pid={pid} rss={rss/1024**3:.2f}GB > threshold={rss_abort_gb:.1f}GB. SIGTERM self."
                    )
                    os.kill(pid, signal.SIGTERM)
                    return
                time.sleep(10)
            except psutil.NoSuchProcess:
                return
            except Exception:
                pass

    t = threading.Thread(target=monitor, daemon=True, name="rss_monitor")
    t.start()
    logging.getLogger(__name__).info(
        f"psutil RSS monitor thread started: abort threshold {rss_abort_gb:.1f}GB"
    )

# v3 memory-safe column projection for fills queries.
# Per Alberto-locked spec 2026-05-26: avoid loading 174x ~225MB daily parquets
# fully into pandas before filtering; use pyarrow row-group filter pushdown
# via pd.read_parquet(p, columns=..., filters=...). Per codex r10 + smoke v3
# peak RSS dropped 320GB worst-case -> 1.34GB observed on 10-wallet × 175-day.
_FILLS_COLS_DEFAULT = [
    "wallet", "coin", "side", "size", "price", "time",
    "dir", "closedPnl", "hash", "source", "notional",
    # 2026-05-28: added enriched fee fields (per Alberto TG 7549 + 7565+).
    # Wallet realized PnL = closedPnl - (fee + builderFee + deployerFee).
    # Requires FILLS_DIR = hl_s3_fills_v2/ which contains these columns.
    "fee", "builderFee", "deployerFee",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_equity] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
# 2026-05-28: switched FILLS_DIR to hl_s3_fills_v2/ for enriched fee fields
# (fee, builderFee, deployerFee). Required for accurate wallet realized PnL
# = closedPnl - fees (Alberto TG 7549). v1 path was hl_s3_fills/.
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills_v2"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_equity_series.parquet"

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

EPS = 1e-9                                    # dust / zero comparison epsilon
VALID_SIDES = frozenset({"A", "B"})

# Ledger type accounting. Each entry maps a HL non-funding ledger `delta.type`
# to a classification handled in accumulate_ledger_flow.
#
# Classifications:
# - external_explicit: signed flow taken from delta.usdc as provided
# - external_vault_withdraw: vault withdraw — reads delta.netWithdrawnUsd
#   (HL docs); delta.usdc is NOT present. Codex r-correction 2026-05-26.
# - external_vault_create: vault create — outflow = delta.usdc + delta.fee.
#   Codex r-correction 2026-05-26 (was previously missing entirely; observed
#   in bucket 1 log 2026-05-26 13:41 CEST).
# - internal: net-zero for total-account equity (just shuffles within account)
# - income: positive flow (gains the wallet) read from delta.usdc
# - income_rewards_claim: rewards claim — reads delta.amount (HL docs); not usdc.
#   Codex r-correction 2026-05-26.
LEDGER_TYPE_MAP = {
    # CANONICAL SOURCE: nktkas/hyperliquid TypeScript SDK,
    # src/api/info/_methods/userNonFundingLedgerUpdates.ts — discriminated
    # union of 19 official types as of 2026-05-27. Validated empirically
    # against 50-wallet sample for 17 types (deployGasAuction never observed
    # in our universe; accountActivationGas not in TS union but WAS observed
    # in our run on 2026-05-26 — likely added post-SDK or rare admin event).
    # Codex sign-fix 2026-05-27 (round 3) addresses 7 handler bugs that
    # caused 62.5% audit_perp_series_went_negative in the v3-A.r2 run.
    #
    # --- Plain external flows (positive usdc field) ---
    # deposit: INFLOW to perp (+usdc).
    "deposit":             "external_deposit",
    # withdraw: positive usdc but OUTFLOW from perp; fee charged on top.
    # signed = -(usdc + fee). Sign INVERSION was the dominant bug.
    "withdraw":            "external_withdraw",
    #
    # --- Wallet-aware flows (user / destination fields) ---
    # internalTransfer: wallet-to-wallet USDC, has fee on sender side.
    "internalTransfer":    "external_wallet_aware",
    # subAccountTransfer: parent <-> sub-account USDC transfer. No fee.
    "subAccountTransfer":  "external_wallet_aware",
    #
    # --- Spot bag activity (no perp impact) ---
    # spotTransfer: token-level move between spot bags. Field has usdcValue
    # (USD equiv of the token), NOT usdc. Zero perp impact regardless of
    # token or direction.
    "spotTransfer":        "internal",
    # spotGenesis: spot token launch / genesis claim. No perp impact.
    "spotGenesis":         "internal",
    #
    # --- Cross-dex / sub-account / spot self-sends ---
    # send: complex. sourceDex / destinationDex govern which sub-account
    # USDC enters/leaves. Per-leg analysis in handler. Only USDC token
    # legs affect perp; only main-perp side ("") legs move perp equity.
    "send":                "external_send_full",
    #
    # --- Perp <-> spot self-transfer (accountClassTransfer) ---
    # accountClassTransfer: USDC moves between SPOT and PERP within the
    # same wallet. usdc is absolute, toPerp gives sign:
    #   toPerp=True:  +usdc (INFLOW to perp)
    #   toPerp=False: -usdc (OUTFLOW from perp)
    "accountClassTransfer": "perp_class_transfer",
    #
    # --- Sub-dex activation fee ---
    # activateDexAbstraction: USDC outflow to activate xyz/flx/etc dex.
    # signed = -amount (when token=USDC). Per-sample amount is NOT always
    # micro: observed up to $3642 in 50-wallet study.
    "activateDexAbstraction": "perp_class_fee",
    #
    # --- Vault flows (positive usdc means outflow from wallet → vault) ---
    # vaultCreate: -(usdc + fee). Wallet seeds new vault.
    "vaultCreate":         "external_vault_create",
    # vaultDeposit: -abs(usdc). Wallet adds to existing vault.
    "vaultDeposit":        "external_vault_deposit",
    # vaultDistribution: +abs(usdc). Vault pays out to depositor.
    "vaultDistribution":   "income",
    # vaultWithdraw: +abs(netWithdrawnUsd). Wallet pulls from vault back to
    # perp account. Do NOT fall back to requestedUsd (codex r2).
    "vaultWithdraw":       "external_vault_withdraw",
    # vaultLeaderCommission: +abs(usdc). Vault leader earns fee.
    "vaultLeaderCommission": "income",
    #
    # --- Rewards (token-scoped) ---
    # rewardsClaim: only token=USDC affects perp; other tokens (USDH etc.)
    # are airdrops to spot bag, no perp impact. signed = +abs(amount)
    # when token=USDC, else 0.
    "rewardsClaim":        "income_rewards_claim",
    #
    # --- Money market (token-scoped, operation-scoped) ---
    # borrowLend: HL lending pool. operation in {supply, withdraw, repay,
    # borrow}. Only USDC affects perp; only supply/withdraw move equity
    # directly (repay/borrow are accounting). signed:
    #   token=USDC, supply: -abs(amount) (USDC leaves perp into pool)
    #   token=USDC, withdraw: +abs(amount) (USDC returns; amount already
    #                                       includes accrued interest per
    #                                       sample observation — codex r3
    #                                       catch, do NOT double-count
    #                                       interestAmount)
    #   else: 0
    "borrowLend":          "borrow_lend",
    #
    # --- Other internal / no-impact types ---
    # liquidation: PnL captured in fills, ledger entry is metadata only.
    "liquidation":         "internal",
    # cStakingTransfer: HYPE token staking — no USDC, no perp impact.
    "cStakingTransfer":    "internal",
    # deployGasAuction: HL admin event when new perp asset is auctioned.
    # NOT observed in our universe. Per TS SDK fields {token, amount}.
    # Conservative: internal unless token=USDC (in which case treat as
    # outflow). Auditable.
    "deployGasAuction":    "deploy_gas_auction",
    # accountActivationGas: observed 2026-05-26 in bucket 1; not in nktkas
    # TS union (may be new). Fields {token, amount}. Tiny USDC outflow
    # at first-touch wallet activation. Conservative internal (negligible
    # amount empirically — sample was $1.0).
    "accountActivationGas": "internal",
    # NEW types discovered during 5K sample run 2026-05-23. Both are NOT
    # USDC-denominated and do NOT affect perp account_value:
    #   - borrowLend: HL money market activity (supply/withdraw of a token
}
# DROPPED 2026-05-27 codex r3: 13 fictional types that were in the old map
# but NOT in the official HL discriminated union (nktkas/hyperliquid SDK).
# These would never fire because HL never emits them. Removed to eliminate
# false confidence + dead code:
#   transferFromSubAccount, transferToSubAccount, cDeposit, cWithdraw,
#   perpDelist, rewards, staking, stakingDeposit, stakingWithdraw,
#   stakingUnlock, cStakingDeposit, cStakingWithdraw, receive
# The real types covering this functional space are:
#   subAccountTransfer (parent <-> sub-account)
#   rewardsClaim (NOT "rewards")
#   cStakingTransfer (HYPE staking; deposit/withdraw direction via isDeposit field)
#   send (incoming legs detected via destination==wallet AND destinationDex=="")

# Module-global counter for skipped (unknown) ledger types in lenient mode.
# Populated by accumulate_ledger_flow; summarized at end of main().
_skipped_ledger_types_counter: dict = {}


# ---------------------------------------------------------------------------
# HL info API helpers
# ---------------------------------------------------------------------------

def _hl_post(body: dict, max_retries: int = 5, base_sleep: float = 0.5) -> dict | list | None:
    for attempt in range(max_retries):
        try:
            r = requests.post(HL_INFO_URL, json=body, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(base_sleep * (2 ** attempt))
                continue
            logger.warning(f"HL {r.status_code}: {r.text[:200]}")
            return None
        except requests.RequestException as e:
            time.sleep(base_sleep * (2 ** attempt))
            logger.warning(f"HL request error: {e}")
    return None


def get_user_state(addr: str) -> dict | None:
    return _hl_post({"type": "clearinghouseState", "user": addr})


def get_spot_user_state(addr: str) -> dict | None:
    return _hl_post({"type": "spotClearinghouseState", "user": addr})


# v4 raw-ledger cache (Alberto direction 2026-05-27, TG 7432): persist HL
# ledger payloads to per-wallet json so future handler-fix re-runs reprocess
# locally in minutes instead of re-pulling 14h from HL.
_RAW_LEDGER_CACHE_DIR = Path("/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k")
_RAW_LEDGER_CACHE_LOCK = threading.Lock()


def _raw_ledger_cache_path(addr: str, start_ms: int, end_ms: int) -> Path:
    # Window-scoped cache key. If window changes, cache misses → re-pull.
    key = f"{addr.lower()}_{start_ms}_{end_ms}.json"
    return _RAW_LEDGER_CACHE_DIR / key


def get_non_funding_ledger_updates(addr: str, start_ms: int, end_ms: int) -> list:
    """Pull HL non-funding ledger entries for a wallet (start_ms..end_ms).

    v4 cache: results persisted at _RAW_LEDGER_CACHE_DIR. On cache hit, NO
    HL API call. On miss, query HL + cache. Subsequent re-runs (e.g. for
    handler bug fixes) skip the HL API entirely.
    """
    cache_path = _raw_ledger_cache_path(addr, start_ms, end_ms)
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                return json.load(f)
        except Exception:
            pass  # corrupt cache → re-pull

    body = {
        "type": "userNonFundingLedgerUpdates",
        "user": addr,
        "startTime": start_ms,
        "endTime": end_ms,
    }
    resp = _hl_post(body)
    entries = resp if isinstance(resp, list) else []

    # Persist (best-effort; lock prevents racing creates of the same wallet's
    # cache file across N=8 threads, but two wallets writing different paths
    # never collide).
    try:
        with _RAW_LEDGER_CACHE_LOCK:
            _RAW_LEDGER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            tmp = cache_path.with_suffix(".json.tmp")
            with open(tmp, "w") as f:
                json.dump(entries, f)
            tmp.replace(cache_path)
    except Exception as e:
        logger.warning(f"raw ledger cache write failed for {addr[:8]}: {e}")
    return entries


# Funding cache directory — analogous to raw ledger cache.
_RAW_FUNDING_CACHE_DIR = Path("/Users/hermes/quants-lab/app/data/v13/raw_funding_cache_20k")
_RAW_FUNDING_CACHE_LOCK = threading.Lock()


def _raw_funding_cache_path(addr: str, start_ms: int, end_ms: int) -> Path:
    key = f"{addr.lower()}_{start_ms}_{end_ms}.json"
    return _RAW_FUNDING_CACHE_DIR / key


def get_funding_updates(addr: str, start_ms: int, end_ms: int,
                        max_pages: int = 200) -> list:
    """Pull HL funding ledger entries (per-funding-period cashflows).

    Each entry has the shape:
        {
            "time": <ms>,
            "hash": "0x...",
            "delta": {
                "type": "funding",
                "coin": "BTC",
                "usdc": "-1.234",         # signed; negative = paid, positive = received
                "szi": "0.5",             # signed position size at funding time
                "fundingRate": "0.0001",
                "nSamples": ...,
            },
        }

    PAGINATION: HL caps each `userFunding` response at 500 entries. For
    active multi-coin wallets, a 30-day window can produce ~7K+ entries (one
    per funding period per held coin). We page by advancing startTime to
    `last_entry_time + 1` after each full-page response. Pagination stops on:
      - a non-full page (<500 entries) returned
      - max_pages reached (safety cap; default 200 -> 100K entries)
      - empty response

    De-duplicates by (time, coin, szi) since the boundary entry may otherwise
    appear in both pages.

    v4 cache (Alberto direction 2026-05-27): cached at _RAW_FUNDING_CACHE_DIR
    per (wallet, start_ms, end_ms). Cache hit skips HL API entirely.
    """
    cache_path = _raw_funding_cache_path(addr, start_ms, end_ms)
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                return json.load(f)
        except Exception:
            pass

    PAGE_CAP = 500
    out: list = []
    seen_keys: set = set()
    cur_start = start_ms
    for _ in range(max_pages):
        body = {
            "type": "userFunding",
            "user": addr,
            "startTime": cur_start,
            "endTime": end_ms,
        }
        resp = _hl_post(body)
        page = resp if isinstance(resp, list) else []
        if not page:
            break
        added = 0
        max_ts_in_page = cur_start
        for e in page:
            t = int(e.get("time", 0))
            d = e.get("delta") or {}
            key = (t, d.get("coin"), d.get("szi"), d.get("usdc"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(e)
            added += 1
            if t > max_ts_in_page:
                max_ts_in_page = t
        # If under page cap, we've drained the window.
        if len(page) < PAGE_CAP:
            break
        # Advance start to just past the latest entry seen.
        new_start = max_ts_in_page + 1
        if new_start <= cur_start:
            # Defensive: avoid infinite loop if the API returns stale data.
            break
        cur_start = new_start
        if cur_start >= end_ms:
            break

    # v4 cache write (best effort).
    try:
        with _RAW_FUNDING_CACHE_LOCK:
            _RAW_FUNDING_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            tmp = cache_path.with_suffix(".json.tmp")
            with open(tmp, "w") as f:
                json.dump(out, f)
            tmp.replace(cache_path)
    except Exception as e:
        logger.warning(f"raw funding cache write failed for {addr[:8]}: {e}")
    return out


def accumulate_funding_flow(entries: list) -> pd.DataFrame:
    """Returns DataFrame with columns: date, signed_flow_usd.

    Each entry's `delta.usdc` is signed (negative = wallet paid funding, positive
    = wallet received funding) and is in USDC. Same sign convention as the
    non-funding ledger `external_explicit` classification.
    """
    rows = []
    for entry in entries:
        ts = int(entry.get("time", 0))
        if ts == 0:
            continue
        delta = entry.get("delta") or {}
        if delta.get("type") != "funding":
            # Skip non-funding rows defensively; userFunding should only return funding.
            continue
        usdc_str = delta.get("usdc", "0")
        try:
            usdc = float(usdc_str)
        except (TypeError, ValueError):
            continue
        rows.append({
            "date": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date(),
            "signed_flow_usd": usdc,
            "classification": "funding",
            "raw_type": "funding",
            "coin": delta.get("coin"),
        })
    if not rows:
        return pd.DataFrame(columns=["date", "signed_flow_usd", "classification", "raw_type", "coin"])
    return pd.DataFrame(rows)


# Cache path (v3-A: stores BOTH spot_usdc and perp_acct_value separately, per
# Alberto rule 16 + codex r-correction 2026-05-26). Old single-equity cache
# (today_equity_cache.parquet) is OBSOLETE — different filename = automatic
# invalidation of pre-correction wrong-anchor data.
_HL_STATE_CACHE_PATH = Path("/Users/hermes/quants-lab/app/data/v13/today_hl_state_cache.parquet")
_HL_STATE_CACHE: dict[str, tuple[float, float, bool, float]] | None = None
# wallet -> (spot_usdc, perp_acct_value, ok, cached_at_ts)
_HL_STATE_CACHE_TTL_HOURS = 24  # re-query if older than this
# Lock protects concurrent reads + writes when running with --n-workers > 1.
# Without it, two threads racing the parquet-flush at the 25-entry boundary
# can corrupt the cache file. Alberto OK'd ThreadPool N=8 on 2026-05-26 18:23
# CEST for the v3-A.r2 restart.
_HL_STATE_CACHE_LOCK = threading.Lock()


def _load_hl_state_cache() -> dict[str, tuple[float, float, bool, float]]:
    """Load HL state cache from parquet (thread-safe).

    Returns dict {wallet: (spot_usdc, perp_acct_value, ok, ts)}.
    """
    global _HL_STATE_CACHE
    with _HL_STATE_CACHE_LOCK:
        if _HL_STATE_CACHE is not None:
            return _HL_STATE_CACHE
        _HL_STATE_CACHE = {}
        if _HL_STATE_CACHE_PATH.exists():
            try:
                df = pd.read_parquet(_HL_STATE_CACHE_PATH)
                cutoff = time.time() - _HL_STATE_CACHE_TTL_HOURS * 3600
                for _, row in df.iterrows():
                    if row["cached_at_ts"] >= cutoff:
                        _HL_STATE_CACHE[row["wallet"]] = (
                            float(row["spot_usdc"]),
                            float(row["perp_acct_value"]),
                            bool(row["ok"]),
                            float(row["cached_at_ts"]),
                        )
                logger.info(f"Loaded {len(_HL_STATE_CACHE)} cached HL states from {_HL_STATE_CACHE_PATH}")
            except Exception as e:
                logger.warning(f"Failed to load HL state cache: {e}")
        return _HL_STATE_CACHE


def _save_hl_state_cache_entry(wallet: str, spot_usdc: float, perp_acct_value: float, ok: bool):
    """Append-and-merge save to HL state cache parquet (thread-safe).

    Under ThreadPool concurrency, the read-modify-write on the in-memory dict
    AND the parquet flush both need locking. Without it, two threads racing
    the 25-entry boundary write would corrupt the file. Locked here.

    Codex r2 + 2026-05-26 20:35 smoke finding: DO NOT cache ok=False entries.
    The N=8 ThreadPool triggers HL rate-limit cascades on bursts; failures
    are typically transient (verified 5/5 retry success rate when queried
    sequentially). Caching the failure as ok=False would make a retry pass
    impossible (cache hit returns the stale failure). Only cache successes;
    failures are silently re-queryable on retry.
    """
    if not ok:
        return  # transient failure — do not poison the cache
    _HL_STATE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ts = time.time()
    # Load cache outside the lock (the load itself is locked internally).
    cache = _load_hl_state_cache()
    with _HL_STATE_CACHE_LOCK:
        cache[wallet] = (spot_usdc, perp_acct_value, ok, ts)
        # Periodically persist (every 25 new entries to avoid every-call IO)
        if len(cache) % 25 == 0:
            try:
                df = pd.DataFrame(
                    [(w, s, p, k, t) for w, (s, p, k, t) in cache.items()],
                    columns=["wallet", "spot_usdc", "perp_acct_value", "ok", "cached_at_ts"],
                )
                df.to_parquet(_HL_STATE_CACHE_PATH, index=False)
            except Exception as e:
                logger.warning(f"Failed to persist HL state cache: {e}")


def get_hl_state(addr: str) -> tuple[float, float, bool]:
    """Returns (spot_usdc, perp_acct_value, ok) for a wallet.

    - spot_usdc: HL spot account USDC balance. Per Alberto rule 16, this is
      the canonical HL EQUITY for sizing / capital references.
    - perp_acct_value: marginSummary.accountValue. This is the wallet's
      currently-deployed perp margin — NOT equity. Used as the anchor for the
      daily perp_account_value_usd back-solve series (performance / PnL math).
    - ok: False if EITHER API call returned None (true API failure). A
      successful API response with $0 balance is ok=True and yields 0.0 for
      that field — that is a valid signal, not an exclusion criterion.
      Codex r2 2026-05-26: prior version required BOTH calls to fail to
      surface ok=False, which silently coerced transient spot-API failures
      into "$0 spot equity" and corrupted the sizing anchor.

    Codex r-correction 2026-05-26: previously get_current_equity_usd returned
    only perp_acct_value mislabeled as "equity", violating rule 16. Now both
    fields are captured separately so downstream can pick the right one for
    its semantic intent (sizing -> spot, performance -> perp).

    Cache: 24h TTL on the joint (spot, perp) tuple. Reduces API pressure.
    """
    cache = _load_hl_state_cache()
    if addr in cache:
        spot, perp, ok, _ = cache[addr]
        return spot, perp, ok

    us = get_user_state(addr)
    sus = get_spot_user_state(addr)
    # Codex r2 2026-05-26: EITHER call failure = ok=False. Empty balances are
    # represented by a successful response with missing/zero USDC entries,
    # NOT by None — None is reserved for transport-level failures (timeouts,
    # 5xx, parse errors).
    if us is None or sus is None:
        _save_hl_state_cache_entry(addr, 0.0, 0.0, False)
        return 0.0, 0.0, False
    perp = float((us.get("marginSummary") or {}).get("accountValue", 0))
    spot = 0.0
    for bal in (sus.get("balances") or []):
        if bal.get("coin") == "USDC":
            spot += float(bal.get("total", 0))
    _save_hl_state_cache_entry(addr, spot, perp, True)
    return spot, perp, True


# Codex r2 2026-05-26: deprecated shim `get_current_equity_usd` REMOVED.
# `rg get_current_equity_usd` shows no migrated caller depends on it.
# Keeping it preserved the bad API name and even supported include_spot=True
# as `spot + perp` (a sum that double-counts per rule 16). Gone.


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_fills_for_dates(
    start: datetime, end: datetime, wallets: set[str] | None = None
) -> pd.DataFrame:
    """v3 memory-safe loader.

    Per-day pyarrow row-group filter pushdown. Drops daily buffer between
    iterations. Codex r10 + smoke v3 peak 1.34GB on 10w x 175d (vs prior
    impl worst-case 320GB on full window).
    """
    cols = _FILLS_COLS_DEFAULT
    if wallets is not None:
        wallets_lc = [w.lower() for w in wallets]
        filt = [("wallet", "in", wallets_lc)]
    else:
        filt = None

    frames = []
    cur = start
    while cur <= end:
        p = FILLS_DIR / f"{cur.strftime('%Y%m%d')}.parquet"
        if p.exists():
            try:
                df = pd.read_parquet(p, columns=cols, filters=filt)
            except Exception as e:
                logger.warning(f"load_fills_for_dates: skipped {p.name}: {e}")
                df = pd.DataFrame()
            if not df.empty:
                frames.append(df)
            gc.collect()  # release per-day buffer before next iteration
        cur += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if not out.empty and "wallet" in out.columns:
        out["wallet"] = out["wallet"].str.lower()
    return out


def load_daily_close_prices(coins: list[str], start: datetime, end: datetime) -> pd.DataFrame:
    """Returns wide DataFrame indexed by date (UTC) with columns=coins, values=close."""
    c = MongoClient("mongodb://localhost:27017")["quants_lab"]["hyperliquid_candles_1h"]
    start_ms = int(start.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp() * 1000)

    docs = list(c.find(
        {"coin": {"$in": list(coins)}, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
        {"coin": 1, "timestamp_utc": 1, "close": 1, "_id": 0},
    ))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df["dt"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    df["date"] = df["dt"].dt.floor("D")
    daily = df.sort_values("dt").groupby(["coin", "date"], as_index=False).last()
    pivot = daily.pivot(index="date", columns="coin", values="close")
    pivot.index = pivot.index.tz_convert("UTC").date
    return pivot


# ---------------------------------------------------------------------------
# Per-fill schema validation
# ---------------------------------------------------------------------------

REQUIRED_FILL_COLUMNS = {"wallet", "coin", "side", "size", "price", "time", "closedPnl", "dir"}


def validate_and_normalize_fills(df: pd.DataFrame) -> pd.DataFrame:
    """Strict validation: required columns present, side in {A,B}, time/price > 0.
    Skip rows with NaN coin or dust size. Hard fail on bad schema.
    """
    if df.empty:
        return df
    missing = REQUIRED_FILL_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"fills missing required columns: {missing}")
    # Hard fail on unknown side.
    bad_sides = df[~df["side"].isin(VALID_SIDES)]
    if not bad_sides.empty:
        raise ValueError(f"Unknown side enum found in {len(bad_sides)} fills, sample: {bad_sides['side'].unique()[:5]}")
    df = df.dropna(subset=["coin"])
    df = df[df["coin"].astype(str).str.len() > 0]
    df = df[df["size"].astype(float) > EPS]
    df = df[df["price"].astype(float) > EPS]
    df = df[df["time"].astype("int64") > 0]
    return df


# ---------------------------------------------------------------------------
# Ledger flow accumulator
# ---------------------------------------------------------------------------

def accumulate_ledger_flow(
    entries: list,
    wallet: str | None = None,
    lenient_unknown: bool = True,
) -> pd.DataFrame:
    """Returns DataFrame with columns: date, signed_flow_usd, classification,
    raw_type. Also attached as DataFrame attrs:
      - unknown_skip_count: int
      - missing_field_count: int

    `wallet`: REQUIRED for wallet-aware classifications (internalTransfer,
    subAccountTransfer, send). Direction is determined by whether the queried
    wallet appears as delta.user (sender, outflow) vs delta.destination
    (receiver, inflow). If None, wallet-aware handlers raise.

    `lenient_unknown` (default True): unknown delta.type is logged + skipped
    (codex r3 2026-05-27: LEDGER_TYPE_MAP now matches the OFFICIAL nktkas/
    hyperliquid TS SDK union of 19 types + accountActivationGas, so unknowns
    after this point are NEW HL types not deprecated/fictional ones).

    SIGN CONVENTION (codex r3 fix 2026-05-27):
    - deposit (+usdc): INFLOW
    - withdraw (positive usdc, OUTFLOW): -(usdc + fee). PRIOR BUG INVERTED.
    - internalTransfer / subAccountTransfer: wallet-aware. inflow when
      destination==wallet; outflow + fee (only internalTransfer has fee) when
      user==wallet.
    - spotTransfer / spotGenesis: ZERO perp impact.
    - send: per-leg analysis on sourceDex/destinationDex/token/user/destination.
    - accountClassTransfer: toPerp flag governs sign.
    - vaultCreate: -(usdc + fee). vaultDeposit: -abs(usdc). vaultDistribution: +abs(usdc).
      vaultWithdraw: +abs(netWithdrawnUsd) (no requestedUsd fallback).
      vaultLeaderCommission: +abs(usdc).
    - rewardsClaim: +abs(amount) only when token=USDC.
    - borrowLend: token=USDC + supply: -abs(amount); token=USDC + withdraw:
      +abs(amount) (amount INCLUDES interest per sample observation — do NOT
      add interestAmount separately). Other operations (repay/borrow) or
      non-USDC tokens: 0.
    - activateDexAbstraction: -abs(amount) when token=USDC.
    - liquidation / cStakingTransfer / spotGenesis / accountActivationGas /
      deployGasAuction: internal / no perp impact.
    """
    global _skipped_ledger_types_counter
    unknown_skip_count = 0
    missing_field_count = 0
    rows = []
    wallet_lc = wallet.lower() if isinstance(wallet, str) else None
    for entry in entries:
        ts = int(entry.get("time", 0))
        if ts == 0:
            continue
        delta = entry.get("delta") or {}
        kind = delta.get("type", "")
        if not kind:
            continue                                # malformed entry
        if kind not in LEDGER_TYPE_MAP:
            if lenient_unknown:
                if kind not in _skipped_ledger_types_counter:
                    logger.warning(
                        f"UNKNOWN HL ledger type '{kind}' (sample entry: {entry}). "
                        f"Treating as zero-flow / internal. Add to LEDGER_TYPE_MAP if it carries USDC value."
                    )
                _skipped_ledger_types_counter[kind] = _skipped_ledger_types_counter.get(kind, 0) + 1
                unknown_skip_count += 1
                continue
            raise ValueError(f"Unknown HL ledger type: {kind} (entry: {entry})")
        classification = LEDGER_TYPE_MAP[kind]

        # Strong field-shape parse with defensive defaults.
        def _f(key: str, default: float = 0.0) -> float:
            try:
                return float(delta.get(key, default))
            except (TypeError, ValueError):
                return default

        if classification == "internal":
            signed = 0.0

        elif classification == "external_deposit":
            # deposit: +usdc INFLOW.
            signed = abs(_f("usdc"))

        elif classification == "external_withdraw":
            # withdraw: positive usdc represents OUTFLOW; +fee charged on top.
            # signed = -(usdc + fee). Codex r3 fix: prior code treated as
            # +inflow (the dominant bug producing 62.5% negative-series).
            signed = -(abs(_f("usdc")) + abs(_f("fee")))

        elif classification == "external_wallet_aware":
            # internalTransfer / subAccountTransfer: direction depends on
            # whether queried wallet is delta.user (sender) or delta.destination.
            if wallet_lc is None:
                raise ValueError(
                    f"wallet-aware classification {kind} requires accumulate_ledger_flow(..., wallet=addr)"
                )
            user = (delta.get("user") or "").lower()
            dest = (delta.get("destination") or "").lower()
            fee = abs(_f("fee"))
            amt = abs(_f("usdc"))
            if dest == wallet_lc and user != wallet_lc:
                # Inflow: wallet is the receiver.
                signed = +amt
            elif user == wallet_lc and dest != wallet_lc:
                # Outflow: wallet is the sender. Sender pays the fee on top.
                signed = -(amt + fee)
            elif user == wallet_lc and dest == wallet_lc:
                # Self-transfer (parent <-> own sub-account, same address).
                # Net zero, no fee charged on self-routes per HL conventions.
                signed = 0.0
            else:
                # Neither side matches — shouldn't happen if API filter is
                # working; flag for inspection.
                missing_field_count += 1
                signed = 0.0

        elif classification == "external_send_full":
            # send: cross-dex / cross-wallet move with sourceDex/destinationDex.
            # Perp-account impact only when:
            #   - token == "USDC" (other tokens don't move perp account_value)
            #   - either sourceDex == "" (main perp side) for outflows
            #     OR destinationDex == "" (main perp side) for inflows
            # Self-send (user == destination) between own sub-accounts:
            #   sourceDex="" → outflow from perp, signed = -amount-fee
            #   destinationDex="" → inflow to perp, signed = +amount (fee
            #     paid by sender on the SOURCE side, not receiver)
            # Cross-wallet send:
            #   user==wallet AND sourceDex=="": outflow from our perp
            #     signed = -(amount + fee)
            #   destination==wallet AND destinationDex=="": inflow to our perp
            #     signed = +amount  (sender pays the fee, not us)
            # Otherwise: 0.
            if wallet_lc is None:
                raise ValueError(
                    f"send classification requires accumulate_ledger_flow(..., wallet=addr)"
                )
            token = delta.get("token", "")
            if token != "USDC":
                signed = 0.0
            else:
                user = (delta.get("user") or "").lower()
                dest = (delta.get("destination") or "").lower()
                src_dex = delta.get("sourceDex", "")
                dst_dex = delta.get("destinationDex", "")
                amt = abs(_f("usdcValue") or _f("amount"))
                fee = abs(_f("fee"))
                is_self = (user == dest and user == wallet_lc)
                is_outbound_from_us = (user == wallet_lc and src_dex == "")
                is_inbound_to_us = (dest == wallet_lc and dst_dex == "")
                if is_self:
                    # Self-send between own sub-accounts.
                    if src_dex == "" and dst_dex != "":
                        signed = -(amt + fee)              # main perp → spot/xyz
                    elif dst_dex == "" and src_dex != "":
                        signed = +amt                      # spot/xyz → main perp
                    elif src_dex == "" and dst_dex == "":
                        # Same-dex self-send shouldn't happen; treat as 0.
                        signed = 0.0
                    else:
                        signed = 0.0                       # spot ↔ xyz, no perp impact
                elif is_outbound_from_us and is_inbound_to_us:
                    # User and destination both us, but is_self test would have caught it.
                    signed = 0.0
                elif is_outbound_from_us:
                    signed = -(amt + fee)                  # cross-wallet outbound from main perp
                elif is_inbound_to_us:
                    signed = +amt                          # cross-wallet inbound to main perp
                else:
                    # Neither main-perp leg involves our wallet (e.g. a
                    # spot→spot send or a cross-wallet leg routed via
                    # spot/xyz dex). No perp impact.
                    signed = 0.0

        elif classification == "external_vault_withdraw":
            net_withdrawn = _f("netWithdrawnUsd")
            if net_withdrawn == 0.0:
                missing_field_count += 1
            signed = abs(net_withdrawn)

        elif classification == "external_vault_create":
            signed = -(abs(_f("usdc")) + abs(_f("fee")))

        elif classification == "external_vault_deposit":
            signed = -abs(_f("usdc"))

        elif classification == "income":
            # vaultDistribution, vaultLeaderCommission — both use +usdc.
            signed = abs(_f("usdc"))

        elif classification == "income_rewards_claim":
            # rewardsClaim: only affects perp when token == "USDC".
            token = delta.get("token", "")
            amt_raw = delta.get("amount", None)
            if amt_raw is None:
                missing_field_count += 1
                amt = 0.0
            else:
                try:
                    amt = float(amt_raw)
                except (TypeError, ValueError):
                    missing_field_count += 1
                    amt = 0.0
            if token == "USDC":
                signed = abs(amt)
            else:
                signed = 0.0

        elif classification == "perp_class_transfer":
            # accountClassTransfer: usdc absolute, toPerp gives sign.
            to_perp = bool(delta.get("toPerp", False))
            signed = abs(_f("usdc")) if to_perp else -abs(_f("usdc"))

        elif classification == "perp_class_fee":
            # activateDexAbstraction: USDC outflow when token=USDC.
            if delta.get("token", "USDC") == "USDC":
                signed = -abs(_f("amount"))
            else:
                signed = 0.0

        elif classification == "borrow_lend":
            # borrowLend: only token=USDC affects perp.
            # operation in {supply, withdraw, repay, borrow}.
            #   supply:   USDC LEAVES perp → -amount
            #   withdraw: USDC RETURNS to perp → +amount (amount includes
            #             accrued interest already — DO NOT add interestAmount)
            #   repay / borrow: accounting only, no perp equity move.
            token = delta.get("token", "")
            op = delta.get("operation", "")
            if token != "USDC":
                signed = 0.0
            elif op == "supply":
                signed = -abs(_f("amount"))
            elif op == "withdraw":
                signed = +abs(_f("amount"))
            else:
                # repay or borrow: 0 (positions accounting, not equity move).
                signed = 0.0

        elif classification == "deploy_gas_auction":
            # deployGasAuction (HL admin event, never observed in our universe).
            # Per nktkas SDK: {token, amount}. Conservative: outflow when
            # token=USDC, 0 otherwise.
            if delta.get("token", "") == "USDC":
                signed = -abs(_f("amount"))
            else:
                signed = 0.0

        else:
            raise ValueError(f"Bad classification {classification} for kind {kind}")

        rows.append({
            "date": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date(),
            "signed_flow_usd": signed,
            "classification": classification,
            "raw_type": kind,
        })
    if not rows:
        out = pd.DataFrame(columns=["date", "signed_flow_usd", "classification", "raw_type"])
    else:
        out = pd.DataFrame(rows)
    out.attrs["unknown_skip_count"] = unknown_skip_count
    out.attrs["missing_field_count"] = missing_field_count
    return out


# ---------------------------------------------------------------------------
# Carry-in walk-back to last flat
# ---------------------------------------------------------------------------

def load_prior_fills_for_wallets(
    wallets: set[str],
    prior_fills_dir: Path,
    window_start: datetime,
    max_walkback_days: int = 90,
) -> pd.DataFrame:
    """v3 memory-safe walk-back loader.

    Per-day pyarrow filter pushdown for the wallet set. Drops daily buffer
    between iterations. Returns fills strictly before window_start, going
    back up to max_walkback_days.
    """
    cols = _FILLS_COLS_DEFAULT
    wallets_lc = [w.lower() for w in wallets]
    filt = [("wallet", "in", wallets_lc)]

    cur_date = window_start.date() - timedelta(days=1)
    days_walked = 0
    frames: list[pd.DataFrame] = []
    while days_walked < max_walkback_days:
        p = prior_fills_dir / f"{cur_date.strftime('%Y%m%d')}.parquet"
        if p.exists():
            try:
                df = pd.read_parquet(p, columns=cols, filters=filt)
            except Exception as e:
                logger.warning(f"load_prior_fills_for_wallets: skipped {p.name}: {e}")
                df = pd.DataFrame()
            if not df.empty:
                frames.append(df)
            gc.collect()  # release per-day buffer before next iteration
        cur_date -= timedelta(days=1)
        days_walked += 1

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if not out.empty and "wallet" in out.columns:
        out["wallet"] = out["wallet"].str.lower()
    return out


# ── Streaming bucket-prepass (Phase A) ────────────────────────────────────
#
# The chunked-batched approach we tried first re-reads ALL 174 daily fills
# parquets ONCE PER CHUNK, which made 300 chunks of 100 wallets take ~50h.
# This 2-phase streaming approach reads each daily parquet exactly ONCE,
# filters to the universe, and distributes per-wallet rows to per-bucket
# files on disk. Phase B then loads only one bucket's small files per chunk.
# Peak RSS per phase: ~1 GB regardless of universe size.

def _wallet_to_bucket(wallet: str, n_buckets: int) -> int:
    """Deterministic bucket for a wallet by leading hex chars."""
    # Use 6 hex chars after 0x → 24 bits of entropy. mod n_buckets.
    return int(wallet[2:8], 16) % n_buckets


def phase_a_bucket_fills(
    wallets: list[str],
    n_buckets: int,
    window_start: datetime,
    window_end: datetime,
    walkback_days: int,
    bucket_root: Path,
) -> None:
    """Single pass over all relevant daily parquets. Filters to wallet set,
    groups by wallet bucket, writes per-bucket-per-day parquets.

    Output layout:
        bucket_root/in_window/bucket_NNNN/YYYYMMDD.parquet
        bucket_root/prior/bucket_NNNN/YYYYMMDD.parquet

    Peak memory: one daily parquet at a time (~500 MB peak during read)
    plus per-bucket-group buffer for that one day (~per-day total / n_buckets).
    """
    wallets_set = {w.lower() for w in wallets}
    wallet_to_bucket = {w: _wallet_to_bucket(w, n_buckets) for w in wallets_set}

    in_root = bucket_root / "in_window"
    prior_root = bucket_root / "prior"
    in_root.mkdir(parents=True, exist_ok=True)
    prior_root.mkdir(parents=True, exist_ok=True)

    # Resolve the full date range we need: [window_start - walkback, window_end].
    full_start = window_start - timedelta(days=walkback_days)
    full_end = window_end

    cur = full_start
    days_total = (full_end - full_start).days + 1
    days_processed = 0
    total_rows_kept = 0
    while cur <= full_end:
        date_str = cur.strftime("%Y%m%d")
        p = FILLS_DIR / f"{date_str}.parquet"
        days_processed += 1
        if not p.exists():
            cur += timedelta(days=1)
            continue

        df = pd.read_parquet(p)
        df["wallet"] = df["wallet"].str.lower()
        df = df[df["wallet"].isin(wallets_set)]
        if df.empty:
            cur += timedelta(days=1)
            continue

        df["_bucket"] = df["wallet"].map(wallet_to_bucket)
        # Decide window vs prior by date
        if cur < window_start:
            target_root = prior_root
        else:
            target_root = in_root

        for b, g in df.groupby("_bucket", sort=False):
            bdir = target_root / f"bucket_{int(b):04d}"
            bdir.mkdir(parents=True, exist_ok=True)
            out_p = bdir / f"{date_str}.parquet"
            g.drop(columns=["_bucket"]).to_parquet(out_p, index=False, compression="snappy")
            total_rows_kept += len(g)

        # Hard-free per-day frame before next iteration
        del df
        if days_processed % 30 == 0:
            import gc
            gc.collect()
            logger.info(f"phase_a: {days_processed}/{days_total} days processed, {total_rows_kept:,} rows kept")
        cur += timedelta(days=1)

    logger.info(f"phase_a DONE: {days_processed} days, {total_rows_kept:,} rows kept across {n_buckets} buckets")


def load_bucket_fills(bucket_root: Path, bucket_idx: int, kind: str,
                      start: datetime, end: datetime) -> pd.DataFrame:
    """Load all per-day parquets for a single bucket. kind in {'in_window','prior'}."""
    assert kind in ("in_window", "prior")
    bdir = bucket_root / kind / f"bucket_{bucket_idx:04d}"
    if not bdir.exists():
        return pd.DataFrame()
    frames = []
    cur = start
    while cur <= end:
        p = bdir / f"{cur.strftime('%Y%m%d')}.parquet"
        if p.exists():
            frames.append(pd.read_parquet(p))
        cur += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def find_carry_in_state_from_prior(
    wallet: str,
    coin: str,
    prior_fills: pd.DataFrame,                       # already filtered to wallets
) -> tuple[float, float, str]:
    """Compute (position, cost_basis, status) at window_start from a pre-loaded
    prior_fills DataFrame.

    The walkback assumes the wallet was FLAT before the earliest loaded fill.
    Validity of that assumption is proven only if we observe a true zero
    crossing somewhere AFTER the first loaded fill. If position ends non-zero
    and we never saw such a crossing, the wallet had pre-load activity we
    cannot account for -> incomplete.

    status:
        "no_carry"       wallet had no prior fills OR ended flat by window_start
        "carry_resolved" passed through zero after first-loaded-fill; carry-in seed is valid
        "incomplete"     position non-zero, no proven zero-crossing within loaded history
    """
    if prior_fills.empty:
        return 0.0, 0.0, "no_carry"
    # Caller may pass a wallet-pre-grouped frame (only that wallet's rows). If
    # the frame still has multiple wallets, filter; otherwise just filter by
    # coin (O(N) of rows for this wallet).
    if (prior_fills["wallet"] != wallet).any():
        pf = prior_fills[(prior_fills["wallet"] == wallet) & (prior_fills["coin"] == coin)]
    else:
        pf = prior_fills[prior_fills["coin"] == coin]
    if pf.empty:
        return 0.0, 0.0, "no_carry"
    pf = validate_and_normalize_fills(pf)
    if pf.empty:
        return 0.0, 0.0, "no_carry"
    sort_keys = ["time", "side", "price", "size"]
    if "hash" in pf.columns:
        sort_keys = ["time", "hash", "side", "price", "size"]
    pf = pf.sort_values(sort_keys, kind="stable")
    prior_fills_sorted = pf

    # Walk forward through prior fills using itertuples for speed.
    position = 0.0
    cost_basis = 0.0
    saw_flat_after_first = False
    is_first = True
    for r in prior_fills_sorted.itertuples(index=False):
        size = float(r.size)
        signed = size if r.side == "B" else -size
        price = float(r.price)
        new_pos = position + signed

        if not is_first and abs(position) < EPS:
            saw_flat_after_first = True

        if abs(position) < EPS:
            cost_basis = price
        elif (position > 0 and signed > 0) or (position < 0 and signed < 0):
            total_qty = abs(new_pos)
            cost_basis = (cost_basis * abs(position) + price * abs(signed)) / total_qty
        elif abs(new_pos) < EPS:
            cost_basis = 0.0
        elif (position > 0 and new_pos > 0) or (position < 0 and new_pos < 0):
            pass
        else:
            cost_basis = price
            saw_flat_after_first = True

        position = new_pos
        is_first = False

    if abs(position) < EPS:
        return 0.0, 0.0, "no_carry"
    if saw_flat_after_first:
        return position, cost_basis, "carry_resolved"
    # Non-zero position with no proven zero crossing -> pre-load activity exists.
    return position, cost_basis, "incomplete"


# ---------------------------------------------------------------------------
# Per-wallet daily series builder
# ---------------------------------------------------------------------------

def reconstruct_one_wallet(
    wallet: str,
    in_window_fills: pd.DataFrame,
    daily_close: pd.DataFrame,
    date_range: list,
    start_ms: int,
    end_ms: int,
    today_api_equity: float,
    historical_anchor: bool,                       # True if end < today
    historical_gap_fills: pd.DataFrame | None,     # fills in [end, today] if historical
    historical_gap_ledger: pd.DataFrame | None,
    historical_gap_daily_close: pd.DataFrame | None,
    prior_fills: pd.DataFrame | None = None,       # pre-loaded prior fills bulk
    window_start: datetime | None = None,
) -> pd.DataFrame | None:
    """Build per-day equity series for one wallet."""
    wf = validate_and_normalize_fills(in_window_fills.copy())
    if wf.empty and not historical_anchor:
        # If end == today and there are no fills, the wallet may still hold
        # carry-in positions. Continue with carry-in only.
        pass

    # 1) Per-fill signed size + signed notional.
    if not wf.empty:
        wf["signed_size"] = wf.apply(
            lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1
        )
        wf["fill_price"] = wf["price"].astype(float)
        wf["dt"] = pd.to_datetime(wf["time"], unit="ms", utc=True)
        wf["date"] = wf["dt"].dt.floor("D").dt.date

    # 2) Carry-in seeds per coin. Use pre-loaded prior_fills (bulk-loaded once
    # at main() level). coins_seen = union(in_window_coins, prior_wallet_coins)
    # so wallets with prior-only-traded positions are covered.
    in_window_coins = set(wf["coin"].dropna().unique().tolist()) if not wf.empty else set()
    prior_for_wallet = prior_fills[prior_fills["wallet"] == wallet] if prior_fills is not None and not prior_fills.empty else pd.DataFrame()
    prior_coins = set(prior_for_wallet["coin"].dropna().unique().tolist()) if not prior_for_wallet.empty else set()
    coins_seen = sorted(in_window_coins | prior_coins)
    carry_in: dict[str, tuple[float, float, str]] = {}
    incomplete_pairs: set[str] = set()
    for coin in coins_seen:
        pos_in, cb_in, status = find_carry_in_state_from_prior(wallet, coin, prior_for_wallet)
        carry_in[coin] = (pos_in, cb_in, status)
        if status == "incomplete":
            incomplete_pairs.add(coin)

    # 3) Walk in-window fills forward; maintain per-coin (position, cost basis).
    # Build daily realized PnL and end-of-day unrealized PnL.
    position: dict[str, float] = {c: carry_in[c][0] for c in coins_seen}
    cost_basis: dict[str, float] = {c: carry_in[c][1] for c in coins_seen}

    daily_realized = pd.Series(0.0, index=date_range)
    daily_unrealized = pd.Series(0.0, index=date_range)
    # Walk per fill; bucket by day.
    if not wf.empty:
        wf_sorted = wf.sort_values(["time", "coin", "side", "price", "size"], kind="stable")
        for _, r in wf_sorted.iterrows():
            coin = r["coin"]
            signed = float(r["signed_size"])
            price = float(r["fill_price"])
            d = r["date"]
            pos = position.get(coin, 0.0)
            cb = cost_basis.get(coin, 0.0)
            new_pos = pos + signed

            # 2026-05-28 (Alberto TG 7568): use HL's per-fill closedPnl + fees
            # directly instead of recomputing realized via (price-cb) walk.
            # The recomputed walk under-counted realized PnL by ~89% on test
            # wallet 0xe3dff077 (HL says +$20,936 in window, walker said +$2,625)
            # because pre-window carry-in cost_basis defaulted to 0.
            # HL's closedPnl IS the realized PnL per fill; trust it.
            # Wallet net realized = closedPnl - (fee + builderFee + deployerFee).
            closed_pnl_raw = float(r.get("closedPnl", 0.0) or 0.0)
            fee_raw = float(r.get("fee", 0.0) or 0.0)
            builder_fee_raw = float(r.get("builderFee", 0.0) or 0.0)
            deployer_fee_raw = float(r.get("deployerFee", 0.0) or 0.0)
            realized_today = closed_pnl_raw - fee_raw - builder_fee_raw - deployer_fee_raw

            # Still maintain position + cost_basis for end-of-day MTM (unrealized PnL)
            # below. The cb walk is only used for unrealized; realized is now direct.
            if abs(pos) < EPS:
                cb = price
            elif (pos > 0 and signed > 0) or (pos < 0 and signed < 0):
                # Same-direction add — weighted-avg cost basis.
                total_qty = abs(new_pos)
                cb = (cb * abs(pos) + price * abs(signed)) / total_qty if total_qty > EPS else price
            elif abs(new_pos) < EPS:
                # Full close — cost basis irrelevant after flat.
                cb = 0.0
            elif (pos > 0 and new_pos > 0) or (pos < 0 and new_pos < 0):
                # Trim (partial close, same-side remains) — cost basis unchanged.
                pass
            else:
                # Reverse (sign flip): new leg starts at fill price.
                cb = price
            position[coin] = new_pos
            cost_basis[coin] = cb
            daily_realized[d] = daily_realized.get(d, 0.0) + realized_today

    # 4) Compute daily unrealized PnL using end-of-day positions + mark prices.
    # We need the position state at end of each day; walk fills again, but
    # bucket by date and snapshot.
    position2: dict[str, float] = {c: carry_in[c][0] for c in coins_seen}
    cost_basis2: dict[str, float] = {c: carry_in[c][1] for c in coins_seen}
    if not wf.empty:
        wf_grouped = wf.sort_values(["time", "coin", "side", "price", "size"], kind="stable").groupby("date")
        for d in date_range:
            if d in wf_grouped.groups:
                day_fills = wf_grouped.get_group(d)
                for _, r in day_fills.iterrows():
                    coin = r["coin"]
                    signed = float(r["signed_size"])
                    price = float(r["fill_price"])
                    pos = position2.get(coin, 0.0)
                    cb = cost_basis2.get(coin, 0.0)
                    new_pos = pos + signed
                    if abs(pos) < EPS:
                        cb = price
                    elif (pos > 0 and signed > 0) or (pos < 0 and signed < 0):
                        total_qty = abs(new_pos)
                        cb = (cb * abs(pos) + price * abs(signed)) / total_qty if total_qty > EPS else price
                    elif abs(new_pos) < EPS:
                        cb = 0.0
                    elif (pos > 0 and new_pos > 0) or (pos < 0 and new_pos < 0):
                        pass
                    else:
                        cb = price
                    position2[coin] = new_pos
                    cost_basis2[coin] = cb
            # End-of-day mark-to-market.
            mtm = 0.0
            for coin, pos in position2.items():
                if abs(pos) < EPS:
                    continue
                if daily_close is not None and not daily_close.empty and d in daily_close.index and coin in daily_close.columns:
                    px = daily_close.at[d, coin]
                    if not pd.isna(px):
                        mtm += pos * (float(px) - cost_basis2.get(coin, 0.0))
            daily_unrealized[d] = mtm
    else:
        # No in-window fills; just MTM the carry-in positions each day.
        for d in date_range:
            mtm = 0.0
            for coin, (pos, cb, _) in carry_in.items():
                if abs(pos) < EPS:
                    continue
                if daily_close is not None and not daily_close.empty and d in daily_close.index and coin in daily_close.columns:
                    px = daily_close.at[d, coin]
                    if not pd.isna(px):
                        mtm += pos * (float(px) - cb)
            daily_unrealized[d] = mtm

    # 5) Ledger flow = non-funding ledger (deposits/withdraws/etc.) + funding.
    # Funding is a SEPARATE HL endpoint (userFunding); non-funding ledger
    # endpoint (userNonFundingLedgerUpdates) explicitly excludes it. We pull
    # both and sum into one daily series; both use the same sign convention
    # (delta.usdc, negative = outflow / wallet pays, positive = inflow /
    # wallet receives). Funding can be the single largest unmodeled cost on
    # a perp strategy, so excluding it would systematically over-estimate
    # wallet edge.
    ledger_entries = get_non_funding_ledger_updates(wallet, start_ms, end_ms)
    try:
        ledger_df = accumulate_ledger_flow(ledger_entries, wallet=wallet)
    except ValueError as e:
        logger.error(f"[{wallet[:8]}] ledger reconstruction failed: {e}")
        return None
    funding_entries = get_funding_updates(wallet, start_ms, end_ms)
    funding_df = accumulate_funding_flow(funding_entries)

    daily_ledger = pd.Series(0.0, index=date_range)
    daily_funding = pd.Series(0.0, index=date_range)
    if not ledger_df.empty:
        daily_sum = ledger_df.groupby("date")["signed_flow_usd"].sum()
        for d, v in daily_sum.items():
            if d in daily_ledger.index:
                daily_ledger[d] = float(v)
    if not funding_df.empty:
        daily_sum_f = funding_df.groupby("date")["signed_flow_usd"].sum()
        for d, v in daily_sum_f.items():
            if d in daily_funding.index:
                daily_funding[d] = float(v)
    # Combined ledger flow used in the equity walk. Keep funding broken out
    # for diagnostics so we can report funding drag separately.
    daily_ledger_combined = daily_ledger + daily_funding

    cum_realized = daily_realized.cumsum()
    cum_ledger_nonfunding = daily_ledger.cumsum()
    cum_funding = daily_funding.cumsum()
    cum_ledger = daily_ledger_combined.cumsum()

    # 6) Historical anchor: backsolve equity[end] from today's API.
    # equity[end] = today_api - (realized_in_gap + ledger_in_gap + (mtm_today - mtm_end))
    # For in-window day t:
    #   equity[t] = equity[end] + (cum_realized[t] + cum_ledger[t] + mtm[t]) - (cum_realized[end] + cum_ledger[end] + mtm[end])
    # which simplifies, with end as last date in date_range, to:
    #   equity[t] = today_api - flow_gap - (flow[end] - flow[t]) - (mtm[today] - mtm[end])
    # If end == today, flow_gap = 0 and mtm[today] = mtm[end]; the original same-day identity holds.
    if historical_anchor:
        if historical_gap_fills is None or historical_gap_ledger is None:
            logger.warning(f"[{wallet[:8]}] historical anchor required but gap data missing; marking INCOMPLETE")
            return None
        # Compute realized + ledger flows + mtm[today] for this wallet in the gap.
        gap_wf = historical_gap_fills[historical_gap_fills["wallet"] == wallet]
        try:
            gap_wf = validate_and_normalize_fills(gap_wf)
        except ValueError as e:
            logger.error(f"[{wallet[:8]}] gap fills validation failed: {e}")
            return None
        # We don't fully simulate cost basis here; we use closedPnl from S3 as
        # realized PnL proxy in the gap (matches the V1 approach for the gap
        # window only; the in-window is properly reconstructed).
        gap_realized = float(gap_wf["closedPnl"].sum()) if "closedPnl" in gap_wf.columns else 0.0
        gap_ledger = float(historical_gap_ledger[historical_gap_ledger["wallet"] == wallet]["signed_flow_usd"].sum()) if not historical_gap_ledger.empty else 0.0
        # mtm[today] vs mtm[end]: positions evolved over gap. For v2, we
        # require mtm change = 0 ASSUMPTION (positions held the same value).
        # If positions changed materially, we need the full forward replay of
        # gap fills. Mark INCOMPLETE if the gap has fills that change net
        # position significantly.
        if not gap_wf.empty:
            net_size_change = gap_wf.apply(lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1).abs().sum()
            if net_size_change > EPS * 1e6:
                # Material activity in gap; can't anchor cleanly without full replay.
                logger.warning(f"[{wallet[:8]}] gap window has material activity ({net_size_change:.4f}); marking INCOMPLETE")
                return None
        # equity[end] approx = today_api - gap_realized - gap_ledger
        equity_end = today_api_equity - gap_realized - gap_ledger
    else:
        # end == today: anchor on today's API equity directly.
        equity_end = today_api_equity

    flow_end = cum_realized.iloc[-1] + cum_ledger.iloc[-1] + daily_unrealized.iloc[-1]
    flow_t = cum_realized + cum_ledger + daily_unrealized
    equity = equity_end - (flow_end - flow_t.values)

    # Reconstructed daily series. Per codex r-correction + Alberto decision A
    # (2026-05-26 17:09 CEST), the column is named `perp_account_value_usd` to
    # make the semantic explicit. This is the wallet's PERP marginSummary.
    # accountValue reconstructed back over time — it is NOT equity (rule 16).
    # Downstream sizing references must use spot_usdc_today (a separate
    # wallet-level column attached in the main bucket loop); only performance
    # / PnL references should consume this series.
    df = pd.DataFrame({
        "wallet": wallet,
        "date": list(date_range),
        "perp_account_value_usd": equity,
        "realized_pnl_cum": cum_realized.values,
        "ledger_net_cum": cum_ledger.values,
        "ledger_nonfunding_cum": cum_ledger_nonfunding.values,
        "funding_cum": cum_funding.values,
        "mtm_unrealized": daily_unrealized.values,
    })

    # 7) Audit + carry-in metadata.
    if df.empty:
        return df

    # audit_today_diff_pct is mostly tautological by construction (the last
    # row is anchored to today_api_equity directly when historical_anchor is
    # False; codex flagged this in the 2026-05-26 review). Kept for backward
    # compatibility but should not be used as a primary quality gate. Use the
    # audit_* flag columns below instead.
    diff_pct = 100 * (df.iloc[-1]["perp_account_value_usd"] - equity_end) / equity_end if equity_end != 0 else 0.0
    df["audit_today_diff_pct"] = None
    df.iloc[-1, df.columns.get_loc("audit_today_diff_pct")] = diff_pct

    # Carry-in summary: how many coins had incomplete carry-in?
    incomplete = sum(1 for c, (_, _, s) in carry_in.items() if s == "incomplete")
    df["carry_in_status"] = "ok" if incomplete == 0 else f"{incomplete}_incomplete"
    df["pre_window_position_value"] = sum(pos * cb for coin, (pos, cb, _) in carry_in.items())

    # Codex r-correction 2026-05-26 + r2 audit flags. Downstream filters/
    # stratifies on these rather than us pre-dropping wallets at the bucket
    # loop. Constant per wallet (one value attached to every row of this
    # wallet).
    perp_min = float(df["perp_account_value_usd"].min())
    df["audit_perp_anchor_zero"] = bool(equity_end <= EPS)
    df["audit_min_reconstructed_perp_value"] = perp_min
    df["audit_perp_series_went_negative"] = bool(perp_min < -EPS)

    # vault_flow_unverified: True if this wallet's ledger contains vault-class
    # events. The vault* mappings in LEDGER_TYPE_MAP were corrected on
    # 2026-05-26 (vaultCreate + vaultLeaderCommission added; vaultWithdraw
    # reads netWithdrawnUsd; vaultDeposit sign inverted to -outflow). Sign
    # conventions reasoned from HL docs but NOT validated against raw HL
    # samples for vaultDeposit. Downstream can stratify ranking on this flag.
    vault_types = {
        "vaultDeposit", "vaultWithdraw", "vaultCreate",
        "vaultLeaderCommission", "vaultDistribution",
    }
    has_vault_flow = False
    if 'ledger_df' in locals() and ledger_df is not None and not ledger_df.empty:
        if "raw_type" in ledger_df.columns:
            has_vault_flow = bool(ledger_df["raw_type"].isin(vault_types).any())
    df["audit_vault_flow_unverified"] = has_vault_flow

    # Per-wallet unknown ledger type count and missing-field count (codex r2
    # 2026-05-26). Surfaced from accumulate_ledger_flow via DataFrame.attrs.
    if 'ledger_df' in locals() and ledger_df is not None:
        df["audit_unknown_ledger_type_count"] = int(ledger_df.attrs.get("unknown_skip_count", 0))
        df["audit_missing_field_count"] = int(ledger_df.attrs.get("missing_field_count", 0))
    else:
        df["audit_unknown_ledger_type_count"] = 0
        df["audit_missing_field_count"] = 0

    # audit_perp_anchor_zero is the renamed/honest version of the prior
    # mis-named audit_pct_return_invalid. The REAL pct-return validity check
    # is computed against the DAILY series at consumer time (codex r2
    # 2026-05-26): any previous-day perp_account_value_usd <= EPS yields a
    # bad pct_change denominator. We surface both the floor AND a count of
    # bad-denominator days so consumers can stratify without a second pass.
    series = df["perp_account_value_usd"]
    df["audit_perp_series_min_abs_denominator"] = float(series.abs().min())
    # Bad-denominator days = days where the PREVIOUS day's value (the divisor
    # in pct_change) is <= EPS. The last day is excluded from the count
    # (it's only ever a divisor if computing forward returns from it).
    prev_day = series.shift(1)
    df["audit_perp_pct_return_bad_denominator_count"] = int(
        (prev_day.abs() <= EPS).sum()
    )

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def discover_wallets_from_fills(fills_df: pd.DataFrame, max_n: int) -> list[str]:
    counts = fills_df["wallet"].str.lower().value_counts()
    return counts.head(max_n).index.tolist()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets")
    ap.add_argument("--discover-from-fills", action="store_true")
    ap.add_argument("--max-wallets", type=int, default=200)
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--walkback-days", type=int, default=90,
                    help="How far to walk back for carry-in proof (default 90)")
    ap.add_argument("--chunk-size", type=int, default=100,
                    help="Wallets per Phase B chunk (memory bound). 0 disables chunking. Default 100.")
    ap.add_argument("--chunk-shards-dir", default=None,
                    help="Directory for intermediate per-chunk parquet shards. Default: <output>.shards/")
    ap.add_argument("--bucket-root", default=None,
                    help="Directory for Phase A per-bucket-per-day prepass parquets. Default: <output>.buckets/")
    ap.add_argument("--n-buckets", type=int, default=0,
                    help="Number of wallet buckets for Phase A. Default: max(50, ceil(n_wallets / chunk_size)).")
    ap.add_argument("--skip-phase-a", action="store_true",
                    help="Skip Phase A bucket-prepass (assume bucket_root is already populated).")
    ap.add_argument("--resume", action="store_true",
                    help="If set, skip chunks whose shard parquet already exists.")
    ap.add_argument("--bucket-start-id", type=int, default=None,
                    help="If set, only process buckets with id >= this. For parallel workers.")
    ap.add_argument("--bucket-end-id", type=int, default=None,
                    help="If set, only process buckets with id <= this. For parallel workers.")
    ap.add_argument("--rlimit-data-gb", type=float, default=6.0,
                    help="Hard memory cap via RLIMIT_DATA (kernel-enforced). Default 6.0GB.")
    ap.add_argument("--rss-abort-gb", type=float, default=4.0,
                    help="Soft RSS threshold (psutil monitor). Process self-SIGTERMs above. Default 4.0GB.")
    ap.add_argument("--n-workers", type=int, default=1,
                    help="ThreadPoolExecutor workers for the per-wallet loop in "
                         "Phase B. 1 = sequential (default, safest). 8 was approved "
                         "by Alberto for the v3-A.r2 restart on 2026-05-26 with "
                         "RSS budget proof (~+15MB per worker). HL API calls "
                         "release GIL during requests.post.")
    args = ap.parse_args()

    # Install OOM prevention guards BEFORE any heavy load
    install_memory_guards(args.rlimit_data_gb, args.rss_abort_gb)

    files = sorted(FILLS_DIR.glob("*.parquet"))
    if not files:
        logger.error("No S3 fills found.")
        sys.exit(1)

    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = datetime.strptime(files[0].stem, "%Y%m%d").replace(tzinfo=timezone.utc)
    if args.end:
        end_requested = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_requested = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Always reconstruct to today (the anchorable endpoint). If the user asked
    # for end < today, we trim the output to [start, end_requested] at the end.
    # This eliminates the gap-anchor complexity: equity[t] is fully forward-
    # reconstructed from window_start to today using all data on disk, then
    # anchored on today's API equity.
    end = today_utc
    output_trim = end_requested < today_utc
    historical_anchor = False                     # no longer needed; always anchor on today

    logger.info(f"Reconstruction range: {start.date()} -> {end.date()} (today-anchor); "
                f"output trimmed to {start.date()} -> {end_requested.date()}" if output_trim
                else f"Date range: {start.date()} to {end.date()} (today-anchor)")

    if args.wallets:
        with open(args.wallets) as f:
            wallets = [w.strip().lower() for w in f if w.strip()]
    elif args.discover_from_fills:
        logger.info("Discovering wallets from S3 fills...")
        all_fills = load_fills_for_dates(start, end)
        if all_fills.empty:
            logger.error("No fills loaded.")
            sys.exit(1)
        wallets = discover_wallets_from_fills(all_fills, args.max_wallets)
        logger.info(f"Discovered {len(wallets)} wallets")
    else:
        logger.error("Provide --wallets or --discover-from-fills")
        sys.exit(2)

    # Missing-data guard: if reconstruction runs to today and any daily parquet
    # in the [start, today] window is missing, the equity series silently
    # treats that day as zero activity. Hard-fail before reconstruction.
    #
    # Codex r3 fix 2026-05-27: TODAY is excluded from the check. Today's fills
    # may not be downloaded yet (S3 publishes hourly throughout the day; the
    # last few hours often missing). Today is the ANCHOR day — equity is set
    # from live HL state API, not from today's fills. The back-solve walks
    # from today's anchor BACKWARDS using prior days' fills. Missing today's
    # fills means only that the anchor's intra-day PnL since 00:00 UTC isn't
    # captured in the cum_realized series — acceptable for a backtest that
    # trims output to end_requested anyway.
    today_utc_date = datetime.now(timezone.utc).date()
    missing_days = []
    cur = start
    while cur <= end:
        if cur.date() == today_utc_date:
            cur += timedelta(days=1)
            continue
        if not (FILLS_DIR / f"{cur.strftime('%Y%m%d')}.parquet").exists():
            missing_days.append(cur.date())
        cur += timedelta(days=1)
    if missing_days:
        logger.error(f"Missing daily parquets in reconstruction window: {missing_days[:5]}{'...' if len(missing_days)>5 else ''}")
        logger.error("Reconstruction would silently zero-fill these days. Aborting.")
        sys.exit(1)

    # ── 2-phase streaming processing ────────────────────────────────────────
    # Phase A: single pass over ~265 daily parquets (175 in-window + 90 prior).
    #          Filter to wallet universe, group by wallet bucket, write per-
    #          bucket-per-day small parquets. Peak: one daily parquet at a
    #          time (~500 MB peak during read).
    # Phase B: per bucket, load that bucket's daily files (small ~10-50 MB
    #          total), run existing per-wallet pipeline, write shard.
    #          Peak: ~500 MB per bucket regardless of universe size.
    # Concat: read all shards, write final --output parquet.
    #
    # WHY: previous chunked-batched approach re-read all 174 in-window
    # parquets per chunk, so 300 chunks of 100 wallets = ~50 hours runtime,
    # mostly I/O. 2-phase reads each daily parquet ONCE total.

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.chunk_shards_dir:
        shards_dir = Path(args.chunk_shards_dir)
    else:
        shards_dir = out_path.with_suffix(".shards")
    shards_dir.mkdir(parents=True, exist_ok=True)
    if args.bucket_root:
        bucket_root = Path(args.bucket_root)
    else:
        bucket_root = out_path.with_suffix(".buckets")

    chunk_size = args.chunk_size if args.chunk_size and args.chunk_size > 0 else len(wallets)
    n_buckets = args.n_buckets if args.n_buckets and args.n_buckets > 0 \
        else max(50, (len(wallets) + chunk_size - 1) // chunk_size)
    n_days = (end - start).days + 1
    date_range = [(start + timedelta(days=i)).date() for i in range(n_days)]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)

    # ── Phase A: bucket-prepass ─────────────────────────────────────────────
    if args.skip_phase_a:
        logger.info(f"Skipping Phase A; assuming bucket_root={bucket_root} is populated")
    else:
        logger.info(f"Phase A: bucket-prepass into {bucket_root} with {n_buckets} buckets...")
        phase_a_bucket_fills(
            wallets, n_buckets, start, end,
            walkback_days=args.walkback_days,
            bucket_root=bucket_root,
        )

    # ── Phase B: per-bucket processing ──────────────────────────────────────
    # Group wallets by their bucket id; each Phase B iteration processes ONE bucket.
    wallet_to_bucket = {w: _wallet_to_bucket(w, n_buckets) for w in wallets}
    bucket_to_wallets: dict[int, list[str]] = {}
    for w, b in wallet_to_bucket.items():
        bucket_to_wallets.setdefault(b, []).append(w)
    active_buckets = sorted(bucket_to_wallets.keys())
    logger.info(f"Phase B: processing {len(active_buckets)} active buckets "
                f"(of {n_buckets} possible)")

    # Skip counters (codex r-correction 2026-05-26):
    # - api_call_failed: true HL API failure (both clearinghouseState and
    #   spotClearinghouseState returned None). Real exclusion.
    # - ledger_unknown_type: reconstruction raised on unknown ledger type (when
    #   not in lenient mode). Real exclusion.
    # - gap_material: positions evolved materially during today-vs-end gap so
    #   we can't anchor cleanly without full forward replay. Real exclusion.
    # The previous 'api_equity_failed' counter was a misleading conflation —
    # 99.4% of its count was "perp_acct_value <= 0" (valid signal, NOT failure).
    # Wallets with $0 perp anchor are NO LONGER dropped here; they flow through
    # with audit flags so downstream can stratify.
    total_skipped = {"api_call_failed": 0, "ledger_unknown_type": 0, "gap_material": 0}
    buckets_done = 0
    buckets_skipped_resume = 0
    buckets_empty = 0

    for bi, bucket_idx in enumerate(active_buckets, 1):
        # Bucket range filter for parallel workers
        if args.bucket_start_id is not None and bucket_idx < args.bucket_start_id:
            continue
        if args.bucket_end_id is not None and bucket_idx > args.bucket_end_id:
            continue
        bucket_wallets = bucket_to_wallets[bucket_idx]
        shard_path = shards_dir / f"bucket_{bucket_idx:04d}.parquet"
        if args.resume and shard_path.exists():
            buckets_skipped_resume += 1
            buckets_done += 1
            logger.info(f"[bucket {bi}/{len(active_buckets)} (id={bucket_idx})] RESUME: shard exists, skipping")
            continue

        logger.info(f"[bucket {bi}/{len(active_buckets)} (id={bucket_idx})] "
                    f"loading {len(bucket_wallets)} wallets...")

        # Load bucket's in-window + prior fills from Phase A output
        bucket_fills = load_bucket_fills(bucket_root, bucket_idx, "in_window", start, end)
        if not bucket_fills.empty:
            bucket_fills = validate_and_normalize_fills(bucket_fills)
        bucket_prior = load_bucket_fills(
            bucket_root, bucket_idx, "prior",
            start - timedelta(days=args.walkback_days),
            start - timedelta(days=1),
        )

        if bucket_fills.empty and bucket_prior.empty:
            logger.info(f"[bucket {bucket_idx:04d}] no fills; writing empty shard")
            # ATOMIC: write to .tmp then rename. Partial files from a crash are
            # never picked up by --resume because the final path only appears
            # on successful rename. Same idempotency principle as cloid pattern:
            # a "completed" marker is durable only after the work is done.
            tmp = shard_path.with_suffix(".parquet.tmp")
            pd.DataFrame().to_parquet(tmp, index=False)
            tmp.replace(shard_path)
            buckets_empty += 1
            buckets_done += 1
            continue

        in_win_coins = set(bucket_fills["coin"].dropna().unique().tolist()) if not bucket_fills.empty else set()
        prior_coins = set(bucket_prior["coin"].dropna().unique().tolist()) if not bucket_prior.empty else set()
        bucket_coins = sorted(in_win_coins | prior_coins)
        bucket_close = load_daily_close_prices(bucket_coins, start, end)

        bucket_rows = []
        bucket_skipped = {"api_call_failed": 0, "ledger_unknown_type": 0, "gap_material": 0}

        # Codex r2 + Alberto OK 2026-05-26 18:23 CEST: parallel wallet
        # processing with ThreadPoolExecutor. HL API calls (get_user_state,
        # spotClearinghouseState, get_non_funding_ledger_updates,
        # get_funding_updates) are I/O-bound and release GIL during
        # requests.post; reconstruct_one_wallet pandas work is per-wallet
        # independent. RSS budget: ~15MB additional per concurrent worker,
        # N=8 → +120MB peak vs sequential 3.44GB → expected ~3.6GB, well
        # under 4GB self-SIGTERM. Cache writes are now locked.
        def _process_one_wallet(w):
            try:
                wf = bucket_fills[bucket_fills["wallet"] == w] if not bucket_fills.empty else pd.DataFrame()
                spot_usdc_today, perp_acct_value_today, ok = get_hl_state(w)
                if not ok:
                    return ("api_call_failed", None, w)
                try:
                    df = reconstruct_one_wallet(
                        w, wf, bucket_close, date_range, start_ms, end_ms,
                        perp_acct_value_today, False,
                        None, None, None,
                        prior_fills=bucket_prior,
                        window_start=start,
                    )
                except ValueError as e:
                    logger.error(f"[{w[:8]}] reconstruction failed: {e}")
                    return ("ledger_unknown_type", None, w)
                if df is None:
                    return ("gap_material", None, w)
                if df.empty:
                    return (None, None, w)
                df = df.copy()
                df["spot_usdc_today"] = spot_usdc_today
                df["perp_acct_value_today"] = perp_acct_value_today
                return (None, df, w)
            except Exception as e:
                logger.exception(f"[{w[:8]}] worker unexpected error: {e}")
                return ("ledger_unknown_type", None, w)

        # Track api_call_failed wallets for a sequential retry pass.
        # Smoke 2026-05-26 20:25 CEST: N=8 ThreadPool triggered HL rate-limit
        # cascade on 82/411 wallets in bucket 0; sequential retry confirmed
        # ALL 5 sampled failures recover (transient). Retry pass after parallel
        # finishes naturally lowers concurrent API pressure.
        failed_api_wallets = []

        if args.n_workers and args.n_workers > 1:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
                for skip_reason, df, w in ex.map(_process_one_wallet, bucket_wallets):
                    if skip_reason == "api_call_failed":
                        failed_api_wallets.append(w)
                    elif skip_reason is not None:
                        bucket_skipped[skip_reason] += 1
                    elif df is not None:
                        bucket_rows.append(df)
        else:
            # Sequential fallback (n_workers=1 or unset).
            for w in bucket_wallets:
                skip_reason, df, _ = _process_one_wallet(w)
                if skip_reason == "api_call_failed":
                    failed_api_wallets.append(w)
                elif skip_reason is not None:
                    bucket_skipped[skip_reason] += 1
                elif df is not None:
                    bucket_rows.append(df)

        # Sequential retry pass for api_call_failed wallets. Codex r2 +
        # 2026-05-26 20:35 smoke finding: HL rate-limit transients clear once
        # parallel pressure ends. Cache no longer stores ok=False, so retries
        # naturally re-hit the API.
        if failed_api_wallets:
            logger.info(
                f"[bucket {bucket_idx:04d}] retrying {len(failed_api_wallets)} api_call_failed "
                f"wallets sequentially (post-parallel rate-limit recovery)..."
            )
            for w in failed_api_wallets:
                skip_reason, df, _ = _process_one_wallet(w)
                if skip_reason is not None:
                    bucket_skipped[skip_reason] += 1
                elif df is not None:
                    bucket_rows.append(df)

        # ATOMIC write: .tmp then rename. Crash mid-write leaves a .tmp file
        # that --resume ignores (only matches bucket_*.parquet, not .tmp).
        tmp = shard_path.with_suffix(".parquet.tmp")
        if bucket_rows:
            shard_df = pd.concat(bucket_rows, ignore_index=True)
            shard_df.to_parquet(tmp, index=False, compression="snappy")
            tmp.replace(shard_path)
            del shard_df
        else:
            pd.DataFrame().to_parquet(tmp, index=False)
            tmp.replace(shard_path)
            buckets_empty += 1

        for k, v in bucket_skipped.items():
            total_skipped[k] += v
        buckets_done += 1

        del bucket_fills, bucket_prior, bucket_close, bucket_rows
        import gc
        gc.collect()

        logger.info(
            f"[bucket {bi}/{len(active_buckets)} (id={bucket_idx})] DONE "
            f"wallets={len(bucket_wallets)} skipped={bucket_skipped}"
        )

    n_chunks = len(active_buckets)
    chunks_done = buckets_done
    chunks_skipped_resume = buckets_skipped_resume
    chunks_empty = buckets_empty

    # ── Concatenate shards into final output ────────────────────────────────
    logger.info(f"All chunks done. {chunks_done}/{n_chunks} (resume-skipped: {chunks_skipped_resume}, empty: {chunks_empty}).")
    # Phase B writes bucket_*.parquet shards.
    shard_paths = sorted(shards_dir.glob("bucket_*.parquet"))
    non_empty = []
    for sp in shard_paths:
        try:
            sdf = pd.read_parquet(sp)
            if not sdf.empty:
                non_empty.append(sdf)
        except Exception as e:
            logger.warning(f"Failed to read shard {sp.name}: {e}")

    if not non_empty:
        logger.error("Zero wallets reconstructed across all chunks.")
        sys.exit(1)

    out = pd.concat(non_empty, ignore_index=True)
    audits = out.dropna(subset=["audit_today_diff_pct"])
    if not audits.empty:
        median_abs = audits["audit_today_diff_pct"].abs().median()
        p90 = audits["audit_today_diff_pct"].abs().quantile(0.90)
        logger.info(f"Audit |diff| median={median_abs:.3f}%, p90={p90:.3f}% (computed pre-trim)")

    if output_trim:
        out = out[(out["date"] >= start.date()) & (out["date"] <= end_requested.date())]
        logger.info(f"Trimmed output to {start.date()} -> {end_requested.date()}: {len(out):,} rows")
    out.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(out):,} rows to {out_path}")
    logger.info(f"Skip totals: {total_skipped}")
    logger.info(f"Shards retained at {shards_dir} for resume/audit; delete manually after verifying output.")
    if _skipped_ledger_types_counter:
        logger.warning(
            f"LENIENT ledger types skipped (treated as zero-flow): "
            f"{dict(sorted(_skipped_ledger_types_counter.items(), key=lambda kv: -kv[1]))}"
        )


if __name__ == "__main__":
    main()
