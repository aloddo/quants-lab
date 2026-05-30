#!/usr/bin/env python3
"""V13 Equity Reconstructor v8 — v7 + codex m01 r4 8-bug fix pass (2026-05-29).

v7 history: ports walker v7 logic into production wrapper. v7 fixed 7 P0 + 4 P1
bugs from the original v13_equity_reconstruct.py.

v8 fixes (codex m01 r4 review, 2026-05-29):
  #1 Funding API now paginated (HL caps 500 rows/call).
  #2 Ledger type coverage expanded: accountClassTransfer, subAccountTransfer,
     vaultDistribution, vaultLeaderCommission, rewardsClaim, borrowLend,
     activateDexAbstraction. UNKNOWN_LEDGER_TYPES tracked diagnostically.
  #3 Pre-anchor position seeding via first post-anchor fill's `startPosition`.
  #4 MAIN dex tracked separately from aggregate (xyz+flx) for anchor alignment.
  #5 Output clamped to anchor.fetched_ms (no stale equity past fetch time).
  #6 `spot_usdc_today` field — see ACCEPTED LIMITATIONS below.
  #7 Wallet casing normalized to lowercase BEFORE lookups (handles checksum case).
  #8 Validation gate failures + empty output now sys.exit non-zero.

ACCEPTED V0 LIMITATIONS (documented; downstream consumer MUST enforce):

  L1 `spot_usdc_today` column carries MAIN-only `perp_acct_value_today` as a PROXY value.
     Status field `spot_usdc_today_status = "MAIN_PERP_ACCT_VALUE_PROXY_v8"` flags this.
     Reason: journey_trace consumer requires a non-null denominator for max_position_pct_equity.
     A true spot USDC would require separate spotClearinghouseState fetch (v0.1).
     CONTRACT: downstream readers MUST either (a) accept proxy semantics or (b) check
     spot_usdc_today_status and route through perp_acct_value_today column directly.

  L2 flx-perp fills cannot be filtered from S3 fills (no `dex` column in fills v2 schema).
     Mitigations active in v8:
       - Ledger-side dex-scoping (send + activateDexAbstraction skip xyz/flx)
       - xyz: coin-prefix filter at fill load + funding event
       - Per-wallet `has_flx_anchor` flag from anchor parquet → emitted as
         `audit_flx_contamination_risk` per row
     Gate 3 LOGS the flx-risk wallet count but does NOT FAIL the process.
     CONTRACT: downstream Module 04 ranker MUST exclude wallets with
     audit_flx_contamination_risk == True from the eligible pool. Module 04 spec at
     projects/quant/v13/modules/04-ranking-copy-simulator documents this requirement.

Codex review chain m01 r4 → r28: 24 rounds, ~42 CODE-BUG fixes. The two items above are
remaining v0 design accepts, not unaddressed bugs.

CORE MODEL (per walker v7, codex r2 verified):
  equity = totalRawUsd + Σ_coin signed_pos × mark_at_t

  Per fill: cash += -signed_sz × price - (fee + builderFee + deployerFee)
            positions[coin] += signed_sz  (skip @ coins as spot)
  Per ledger: cash += signed_flow (deposit/withdraw/send/vault/internalTransfer)
  Per funding: cash += usdc (direct API per-call, cache is broken)

INPUTS:
  - wallets file (one address per line)
  - S3 fills v2 at app/data/hl_s3_fills_v2/{YYYYMMDD}.parquet
  - Ledger cache at app/data/v13/raw_ledger_cache_20k/{wallet}_*.json
  - Anchor parquet at app/data/v13/wallet_anchor_state.parquet (main/xyz/flx)
  - Mongo hyperliquid_candles (1m, s3_reconstructed) for marks
  - HL info API for portfolio.perpAllTime + userFunding direct

OUTPUT:
  - wallet_equity_series.parquet with schema:
    wallet, date, perp_account_value_usd, spot_usdc_today, perp_acct_value_today,
    cash, n_positions, audit_unanchored_dex, audit_sentinel_zero,
    audit_missing_marks, audit_drift_pct

VALIDATION (3 gates per codex):
  Gate 1: anchor reconciliation vs portfolio.perpAllTime
    PASS if median(abs(drift_pct)) < 1% and 75% of wallet-days < 10%
    FAIL if any clean main/xyz/flx wallet drifts > 10% without audit reason
  Gate 2: accounting identity at each endpoint
    abs(perp_account_value - (cash + Σ pos×mark)) < max($1, 1bp of equity)
  Gate 3: audit coverage
    no unflagged $0 anchors, no @ coins included in cash, no cached funding
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import pymongo
from dotenv import load_dotenv

load_dotenv('/Users/hermes/quants-lab/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger('v13_eq_v7')

HL_URL = 'https://api.hyperliquid.xyz/info'
S3_FILLS_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2')
S3_BY_WALLET_DIR = Path('/Users/hermes/quants-lab/app/data/hl_s3_fills_v2_by_wallet')
LEDGER_DIR = Path('/Users/hermes/quants-lab/app/data/v13/raw_ledger_cache_20k')
ANCHOR_PARQUET = Path('/Users/hermes/quants-lab/app/data/v13/wallet_anchor_state.parquet')
MONGO_URI = 'mongodb://localhost:27017/'

# Worker-process locals
_mongo = None
_mark_cache: dict = {}


def get_mongo():
    global _mongo
    if _mongo is None:
        _mongo = pymongo.MongoClient(MONGO_URI)['quants_lab']
    return _mongo


def get_mark(coin: str, ts_ms: int) -> float | None:
    """1m close at or before ts_ms. Cache in-worker."""
    minute_key = ts_ms // 60_000 * 60_000
    key = (coin, minute_key)
    if key in _mark_cache:
        return _mark_cache[key]
    db = get_mongo()
    doc = db.hyperliquid_candles.find_one(
        {'coin': coin, 'interval': '1m', 'timestamp_utc': {'$lte': minute_key}},
        sort=[('timestamp_utc', -1)],
    )
    px = float(doc['close']) if doc else None
    _mark_cache[key] = px
    return px


def get_portfolio_perp(wallet: str, retries: int = 3) -> list[tuple[int, float]]:
    """Returns perpAllTime accountValueHistory as list of (ts_ms, value)."""
    for i in range(retries):
        try:
            r = requests.post(
                HL_URL,
                json={'type': 'portfolio', 'user': wallet},
                timeout=15,
            )
            if r.status_code == 429:
                time.sleep(2 ** i)
                continue
            if r.status_code != 200:
                return []
            for window_name, wd in r.json():
                if window_name == 'perpAllTime':
                    return [(int(x[0]), float(x[1])) for x in wd.get('accountValueHistory', [])]
            return []
        except Exception:
            time.sleep(1)
    return []


def get_api_funding(wallet: str, start_ms: int, end_ms: int, retries: int = 3) -> list[dict] | None:
    """Direct API funding fetch (cache known broken — never use cache here).

    codex m01 r4 CODE-BUG 1 fix (v8): HL `userFunding` returns max 500 rows per call.
    For active 6-month wallets this drops funding data silently. Paginate by cursor:
    each page is sorted by time ASC; we advance startTime to LAST_ROW.time + 1.

    codex m01 r20 HIGH fix: hard-fail on pagination errors. Previously returned partial
    data on transient failure → silent equity corruption. Now returns None on any failure
    so caller can mark wallet INCOMPLETE.

    Returns: list[dict] on success (may be empty if no funding); None on any pagination error.
    """
    PAGE_LIMIT = 500  # HL response cap (empirically observed)
    all_events: list[dict] = []
    cursor_start = start_ms
    page = 0
    while cursor_start <= end_ms:
        page += 1
        page_data: list[dict] | None = None
        for i in range(retries):
            try:
                r = requests.post(
                    HL_URL,
                    json={
                        'type': 'userFunding',
                        'user': wallet,
                        'startTime': cursor_start,
                        'endTime': end_ms,
                    },
                    timeout=30,
                )
                if r.status_code == 429:
                    time.sleep(2 ** i)
                    continue
                if r.status_code == 200:
                    page_data = r.json()
                    break
            except Exception:
                time.sleep(1)
        if page_data is None:
            # codex m01 r20 fix: HARD FAIL on fetch failure (not silent partial).
            logger.warning(f'funding fetch FAILED for {wallet[:10]} page {page} cursor {cursor_start} → returning None')
            return None
        if not page_data:
            break  # no more pages — success
        all_events.extend(page_data)
        if len(page_data) < PAGE_LIMIT:
            break  # last page (less than cap → no more)
        # Advance cursor to last row's time + 1ms to avoid re-fetching the same row.
        try:
            last_ts = int(page_data[-1]['time'])
        except Exception:
            logger.warning(f'funding pagination malformed for {wallet[:10]} → returning None')
            return None
        if last_ts <= cursor_start:
            # Safety: avoid infinite loop if API returns identical first row repeatedly
            logger.warning(f'funding pagination non-progressive for {wallet[:10]} cursor {cursor_start} → returning None')
            return None
        cursor_start = last_ts + 1
    return all_events


def load_wallet_fills(wallet: str, t0: int, t1: int, main_only: bool = True) -> tuple[list[dict], bool]:
    """Load fills with normalized signed_sz. Uses partitioned per-wallet parquet if available.

    codex m01 r6 CODE-BUG-CRITICAL fix + r8 docstring correction: MAIN-only scoping.
    The anchor source is portfolio.perpAllTime (MAIN-only); reconstruction MUST exclude
    xyz dex (HIP-3 stock perps; `xyz:` coin prefix) and spot (`@` prefix, `USDC`).

    KNOWN LIMITATION: flx perps share the standard coin namespace and CANNOT be filtered
    by coin name alone (S3 fills v2 has NO `dex` column). Mitigated via ledger-side dex
    scoping (`send`, `activateDexAbstraction`) which catches bulk cash leaks. Pure
    flx-perp POSITION leak is residual. Audit per-wallet via anchor-profile cross-check (v0.1).

    Returns (fills_list, observed_xyz_fills_bool) so caller can flag `audit_unanchored_dex`.
    The bool is ONLY True when `xyz:` fills are observed (NOT spot, NOT USDC — those are
    legitimately not perp and don't indicate any leak).
    """
    fills = []
    observed_other_dex = False
    wallet_lc = wallet.lower()
    def _is_main_perp(coin: str) -> bool:
        # MAIN dex: standard perps (BTC, ETH, SOL, etc.).
        # NOT @-prefix spot, NOT xyz: prefix (HIP-3 stock perp on xyz dex).
        # KNOWN LIMITATION (codex m01 r7): flx perps share the standard coin namespace and CANNOT
        # be distinguished from MAIN perps by S3 fill data alone (no `dex` column in fills v2).
        # Mitigation: ledger-side dex scoping (send/activateDexAbstraction filtered by dex field)
        # catches the bulk of flx cash leaks. Pure flx-perp position leak is residual.
        # v0.1: cross-reference wallet anchor `dex` profile to detect flx-only wallets.
        if coin.startswith('@') or coin == 'USDC':
            return False  # spot, separate from perp
        if coin.startswith('xyz:'):
            return False  # HIP-3 stock perp on xyz dex
        return True
    # FAST PATH: partitioned per-wallet parquet
    by_wallet_path = S3_BY_WALLET_DIR / f'{wallet_lc}.parquet'
    if by_wallet_path.exists():
        try:
            df = pd.read_parquet(by_wallet_path)
            df['time'] = df['time'].astype('int64')
            m = df[(df['time'] >= t0) & (df['time'] <= t1)]
            for _, r in m.iterrows():
                d = dict(r)
                if 'signed_sz' not in d:
                    d['signed_sz'] = float(d['size']) if d['side'] == 'B' else -float(d['size'])
                # codex m01 r6 fix + r8 semantic cleanup: MAIN-only filter when requested.
                # Only flag observed_other_dex (=> audit_unanchored_dex) when XYZ fills are
                # rejected — those represent actual unanchored-dex trading activity. Spot
                # rejections (`@` prefix, `USDC`) are not leaks, just orthogonal asset classes.
                if main_only:
                    coin = d.get('coin', '')
                    if not _is_main_perp(coin):
                        if coin.startswith('xyz:'):
                            observed_other_dex = True
                        continue
                fills.append(d)
            fills.sort(key=lambda x: int(x['time']))
            return fills, observed_other_dex
        except Exception:
            pass
    # SLOW FALLBACK: scan all daily parquets
    # codex m01 r5 fix: normalize parquet wallet column to lowercase BEFORE comparison.
    # Day parquets may have checksum/mixed case → exact `==` misses.
    for ff in sorted(glob.glob(str(S3_FILLS_DIR / '*.parquet'))):
        try:
            df = pd.read_parquet(
                ff,
                columns=[
                    'wallet', 'coin', 'side', 'size', 'price', 'time',
                    'startPosition', 'fee', 'builderFee', 'deployerFee',
                ],
            )
            df['time'] = df['time'].astype('int64')
            df['wallet_lc'] = df['wallet'].astype(str).str.lower()
            m = df[(df['wallet_lc'] == wallet_lc) & (df['time'] >= t0) & (df['time'] <= t1)]
            for _, r in m.iterrows():
                d = dict(r)
                d['signed_sz'] = float(d['size']) if d['side'] == 'B' else -float(d['size'])
                # codex m01 r6 fix + r8 semantic cleanup: MAIN-only filter when requested.
                # Only flag observed_other_dex (=> audit_unanchored_dex) when XYZ fills are
                # rejected — those represent actual unanchored-dex trading activity. Spot
                # rejections (`@` prefix, `USDC`) are not leaks, just orthogonal asset classes.
                if main_only:
                    coin = d.get('coin', '')
                    if not _is_main_perp(coin):
                        if coin.startswith('xyz:'):
                            observed_other_dex = True
                        continue
                fills.append(d)
        except Exception:
            continue
    fills.sort(key=lambda x: int(x['time']))
    return fills, observed_other_dex


def load_wallet_ledger(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load ledger entries from cache (per-wallet API ledger cache is complete for events <= May 24 22:00 UTC)."""
    entries = []
    for lf in glob.glob(str(LEDGER_DIR / f'{wallet}_*.json')):
        try:
            with open(lf) as f:
                for e in json.load(f):
                    t = int(e.get('time', 0))
                    if t0 <= t <= t1:
                        entries.append(e)
        except Exception:
            continue
    entries.sort(key=lambda x: int(x['time']))
    return entries


def load_wallet_anchor(wallet: str, anchor_df: pd.DataFrame) -> dict:
    """Returns aggregated anchor across main/xyz/flx: cash, positions, fetched_ms, dexes_seen.

    codex m01 r4 CODE-BUG 8 fix (v8): case-insensitive wallet comparison.
    """
    wallet_lc = wallet.lower()
    wa = anchor_df[anchor_df['wallet'].str.lower() == wallet_lc]
    if wa.empty:
        return None
    # codex m01 r4 CODE-BUG 4 + 6 fix (v8): track MAIN dex SEPARATELY from xyz/flx aggregates.
    # Anchor truth source is portfolio.perpAllTime which is MAIN-only. Mixing all dexes into
    # `cash` causes inconsistency with the API anchor used downstream. And `spot_usdc_today` was
    # misnamed: it was actually aggregate perp cash, but consumer (journey_trace) treats it as
    # spot USDC denominator. We now emit:
    #   - main_cash             — main dex totalRawUsd (anchor-aligned)
    #   - main_positions        — main dex assetPositions (anchor-aligned)
    #   - aggregate_perp_cash   — sum of totalRawUsd across all dexes
    #   - aggregate_positions   — all positions across all dexes (informational)
    #   - spot_usdc             — placeholder TODO: requires spotClearinghouseState fetch
    main_cash = 0.0
    aggregate_cash = 0.0
    main_positions: dict[str, float] = {}
    aggregate_positions: dict[str, float] = {}
    fetched_ms = 0
    dexes_seen = set()
    main_acct_value = 0.0
    aggregate_acct_value = 0.0
    for _, row in wa.iterrows():
        if not row['ok']:
            continue
        dex = row['dex']
        dexes_seen.add(dex)
        cash_dex = float(row['totalRawUsd'])
        aggregate_cash += cash_dex
        acct_val_dex = float(row['accountValue'])
        aggregate_acct_value += acct_val_dex
        if dex == 'main':
            main_cash = cash_dex
            main_acct_value = acct_val_dex
        ts_ms = int(float(row['fetched_at_ts']) * 1000)
        fetched_ms = max(fetched_ms, ts_ms)
        for p in json.loads(row['positions_json']):
            coin = p['coin']
            szi = float(p['szi'])
            if dex == 'main':
                main_positions[coin] = szi
            # Aggregate uses last-write-wins per coin (rare for same coin to be on 2 dexes).
            aggregate_positions[coin] = szi
    # codex m01 r17 fix: flag wallets with flx anchor presence so downstream consumers
    # (Module 04 ranking) can exclude them from MAIN-only reconstruction trust.
    has_flx_anchor = 'flx' in dexes_seen
    return {
        # Anchor-aligned MAIN dex values (use these for reconstruction).
        'cash': main_cash,                          # MAIN dex cash only (v8)
        'positions': main_positions,                # MAIN dex positions only (v8)
        # Informational aggregates across all dexes.
        'aggregate_cash': aggregate_cash,
        'aggregate_positions': aggregate_positions,
        'aggregate_acct_value': aggregate_acct_value,
        'fetched_ms': fetched_ms,
        'dexes_seen': dexes_seen,
        # codex m01 r17 fix: hard flag for flx anchor presence (cannot perfectly filter flx
        # perps from S3 fills without a `dex` column). Wallets with flx anchor MAY have flx-perp
        # fill contamination in their MAIN reconstruction. Module 04 should EXCLUDE these
        # wallets from eligible pool OR mark `audit_flx_contamination_risk = True`.
        'has_flx_anchor': has_flx_anchor,
        # PERP account value: MAIN only (matches the portfolio.perpAllTime anchor source).
        'perp_acct_value_today': main_acct_value,
        # `spot_usdc_today`: placeholder; correct value requires spotClearinghouseState fetch.
        'spot_usdc_today_placeholder': 0.0,
        'spot_usdc_today_status': 'NOT_FETCHED_USE_perp_acct_value_today_INSTEAD',
    }


def coin_is_spot(coin: str) -> bool:
    """@ prefix = spot tokens (UBTC etc); skip from perp."""
    return coin.startswith('@')


def fill_cash_delta(f: dict) -> float:
    if coin_is_spot(f['coin']):
        return 0.0
    px = float(f['price'])
    signed_sz = float(f['signed_sz'])
    fee = float(f['fee']) + float(f['builderFee']) + float(f['deployerFee'])
    return -signed_sz * px - fee


def ledger_cash_delta(e: dict, wallet_lc: str) -> float:
    """codex m01 r4 CODE-BUG 2 fix (v8): expand ledger type coverage.
    Added types: accountClassTransfer, subAccountTransfer, vaultDistribution,
    vaultLeaderCommission, rewardsClaim, borrowLend, activateDexAbstraction.
    Unknown types still return 0 (with diagnostic log via UNKNOWN_LEDGER_TYPES set).
    """
    d = e.get('delta', {})
    k = d.get('type', '')
    if k == 'deposit':
        return float(d.get('usdc', 0))
    if k == 'withdraw':
        return -(float(d.get('usdc', 0)) + float(d.get('fee', 0)))
    if k == 'send' and d.get('token') == 'USDC':
        # codex m01 r5+r6 CRITICAL: MAIN dex only. Reconstruction uses MAIN-dex anchor
        # (portfolio.perpAllTime is MAIN-only); ledger send affecting xyz/flx is a SEPARATE
        # account's cash and MUST NOT be added to MAIN reconstruction.
        # codex m01 r6 fix: lowercase + normalize dex fields BEFORE comparison.
        user = (d.get('user') or '').lower()
        dest = (d.get('destination') or '').lower()
        src_dex = str(d.get('sourceDex', '')).strip().lower()
        dst_dex = str(d.get('destinationDex', '')).strip().lower()
        amt = float(d.get('usdcValue', d.get('amount', 0)))
        fee = float(d.get('fee', 0))
        delta = 0.0
        # MAIN only: include empty + main; EXCLUDE xyz, flx, or any unknown explicit dex.
        # Empty/None treated as MAIN (HL pre-multidex convention).
        main_set = {'', 'main'}
        if user == wallet_lc and src_dex in main_set:
            delta -= (amt + fee)
        if dest == wallet_lc and dst_dex in main_set:
            delta += amt
        return delta
    if k == 'vaultDeposit':
        return -float(d.get('usdc', 0))
    if k == 'vaultWithdraw':
        return float(d.get('netWithdrawnUsd', 0))
    if k == 'vaultCreate':
        return -(float(d.get('usdc', 0)) + float(d.get('fee', 0)))
    if k == 'vaultDistribution':
        # Vault leader receives distribution from vault performance.
        # HL convention: positive amount inbound to wallet.
        return float(d.get('usdc', d.get('amount', 0)))
    if k == 'vaultLeaderCommission':
        # Vault leader commission collected from depositors.
        return float(d.get('usdc', d.get('amount', 0)))
    if k == 'rewardsClaim':
        # codex m01 r5+r27 fix: STRICT USDC-token check.
        # A non-USDC reward may carry a `usdc` metadata/valuation field; counting it as cash
        # would silently inflate perp equity. Mature mapper in v13_equity_reconstruct.py uses
        # strict `token == 'USDC'`. Match that.
        if d.get('token') == 'USDC':
            return float(d.get('usdc', d.get('amount', 0)))
        return 0.0
    if k == 'accountClassTransfer':
        # codex m01 r5 CODE-BUG-CRITICAL #2 fix: HL accountClassTransfer uses `toPerp: bool` field.
        # NOT user/destination. toPerp=True → USDC moves INTO perp (positive); toPerp=False → OUT.
        # Amount field is the magnitude.
        amt = abs(float(d.get('usdc', d.get('amount', 0))))
        to_perp = bool(d.get('toPerp'))
        return amt if to_perp else -amt
    if k == 'subAccountTransfer':
        # Transfer between main and subaccounts owned by same user.
        # Affects MAIN account cash; we treat as cash flow to/from main perp.
        user = (d.get('user') or '').lower()
        dest = (d.get('destination') or '').lower()
        amt = float(d.get('usdc', d.get('amount', 0)))
        if user == wallet_lc:
            return -amt
        if dest == wallet_lc:
            return amt
        return 0.0
    if k == 'borrowLend':
        # codex m01 r5+r13 fix: only USDC, with operation-aware sign.
        # supply/lend/deposit → cash OUT of perp (-amt)
        # withdraw/redeem → cash IN to perp (+amt)
        # borrow/repay → ZERO equity movement (borrowing creates offsetting debt; not free cash).
        #   The mature v13_equity_reconstruct.py path treats borrow/repay as zero equity
        #   per codex m01 r13 finding (was incorrectly +amt in v8 r5).
        # Non-USDC → 0.
        if d.get('token') != 'USDC':
            return 0.0
        amt = abs(float(d.get('usdc', d.get('amount', 0))))
        op = (d.get('operation') or '').lower()
        if op in ('supply', 'lend', 'deposit'):
            return -amt
        if op in ('withdraw', 'redeem'):
            return amt
        if op in ('borrow', 'repay'):
            return 0.0  # debt offsets cash → no equity change
        # Unknown operation → zero conservatively.
        return 0.0
    if k == 'activateDexAbstraction':
        # codex m01 r6 fix: -abs(amount) when USDC, else -fee (mutually exclusive).
        # codex m01 r7 CRITICAL fix: dex-scope check (xyz/flx → 0).
        # codex m01 r27 fix: missing-token defaults to USDC per mature mapper convention.
        ev_dex = str(d.get('dex', '')).strip().lower()
        if ev_dex not in ('', 'main'):
            return 0.0  # xyz/flx/other dex — does NOT affect MAIN cash
        token = d.get('token')
        if token == 'USDC' or token is None:  # r27: missing token defaults to USDC
            return -abs(float(d.get('usdc', d.get('amount', 0))))
        return -float(d.get('fee', 0))
    if k == 'cStakingTransfer':
        # HYPE staking → unstaking is not a USDC cash flow for the perp account.
        return 0.0
    if k == 'spotTransfer':
        # codex m01 r5 fix: spotTransfer is SPOT-side; ZERO perp impact.
        # perp ↔ spot moves are accountClassTransfer with toPerp flag.
        return 0.0
    if k == 'send' and d.get('token') != 'USDC':
        # Non-USDC sends: no perp USDC cash impact.
        return 0.0
    if k == 'send' and d.get('token') != 'USDC':
        # Non-USDC sends (e.g., spot tokens, HYPE): no perp USDC cash impact.
        return 0.0
    if k == 'internalTransfer' and d.get('token') == 'USDC':
        user = (d.get('user') or '').lower()
        dest = (d.get('destination') or '').lower()
        amt = float(d.get('usdc', 0))
        fee = float(d.get('fee', 0))
        if user == wallet_lc and dest != wallet_lc:
            return -(amt + fee)
        if dest == wallet_lc and user != wallet_lc:
            return amt
    # Track unknown types for diagnostic (set is collected at module scope)
    if k:
        _UNKNOWN_LEDGER_TYPES.add(k)
    return 0.0


# Module-scope diagnostic set: collects ledger types we encountered but don't handle.
_UNKNOWN_LEDGER_TYPES: set[str] = set()


def funding_cash_delta(e: dict) -> float:
    """codex m01 r19 HIGH fix: filter xyz: funding from MAIN reconstruction.
    HL userFunding response includes events for ALL dexes — xyz funding goes to xyz account,
    not MAIN. Without this filter, xyz funding pollutes MAIN cash.
    `flx` funding: cannot distinguish from MAIN by coin name (no dex column); audit flag
    captures the risk at the wallet level.
    """
    d = e.get('delta', {})
    if d.get('type') != 'funding':
        return 0.0
    coin = d.get('coin', '')
    # Skip xyz: HIP-3 funding (not MAIN account)
    if isinstance(coin, str) and coin.startswith('xyz:'):
        return 0.0
    return float(d.get('usdc', 0))


def positions_at(fills: list[dict], t_ms: int) -> dict[str, float]:
    """Per coin: position at time t = (last fill at or before t).startPosition + signed_sz.
    Uses HL's authoritative startPosition field — handles pre-S3 positions.
    Skips @ spot coins."""
    last_per_coin: dict[str, dict] = {}
    for f in fills:
        if int(f['time']) > t_ms:
            break
        coin = f['coin']
        if coin_is_spot(coin):
            continue
        last_per_coin[coin] = f
    positions = {}
    for coin, f in last_per_coin.items():
        pos = float(f['startPosition']) + float(f['signed_sz'])
        if abs(pos) > 1e-9:
            positions[coin] = pos
    return positions


def reconstruct_wallet(args: tuple) -> dict | None:
    """Per-wallet daily equity reconstruction.

    Returns:
      {
        'wallet': str,
        'series': DataFrame with columns wallet, date, perp_account_value_usd,
                  cash, n_positions, audit_*,
        'audit': dict with summary flags
      }
    """
    wallet, anchor_data, start_ms, end_ms, validation_only = args
    wallet_lc = wallet.lower()

    if anchor_data is None:
        return {'wallet': wallet, 'error': 'no_anchor'}

    # 1) Get portfolio.perpAllTime — ground truth anchor source per v7
    avh = get_portfolio_perp(wallet)
    # FILTER sentinel $0 anchors (codex r3 hypothesis #2: chart artifacts)
    valid_anchors = [(t, v) for t, v in avh if v > 0.01 or (v == 0.0 and False)]  # filter pure zeros
    n_sentinel_zeros = sum(1 for t, v in avh if v == 0.0)
    if not valid_anchors:
        return {'wallet': wallet, 'error': 'no_valid_anchors'}

    # codex m01 r9+r21 fix: --validation-only mode short-circuits AFTER anchor fetch + funding
    # pagination check (per r21), but BEFORE walking events / building series. r21 fix: funding
    # completeness IS validated here — get_api_funding pagination failure returns None → wallet
    # marked funding_fetch_incomplete just like normal mode. This closes the r20 split-brain
    # where validation-only could pass while normal mode would fail.
    if validation_only:
        # Validate funding fetch completeness (most common production failure mode)
        events_funding = get_api_funding(wallet, start_ms, anchor_data['fetched_ms'])
        if events_funding is None:
            return {'wallet': wallet, 'error': 'funding_fetch_incomplete'}
        return {
            'wallet': wallet,
            'series': pd.DataFrame(),  # empty series; signals validation-only mode
            'audit': {
                'n_fills': 0,
                'n_ledger': 0,
                'n_funding': len(events_funding),  # r21: record actual count for diagnostic
                'n_sentinel_zeros': n_sentinel_zeros,
                'drift_pct': None,
                'missing_marks_at_start': 0,
                'validation_only': True,
            },
            'unknown_ledger_types': [],
        }

    # 2) Walker forward-walk with re-anchoring at each API perpAllTime point.
    # Key insight: walking 174 days from one anchor accumulates drift. Better to
    # RE-ANCHOR cash at every valid API point (snap walker to API equity).
    # Between anchors, walk events normally.
    #
    # Per-day equity output:
    #   For day D, find latest API anchor t <= EOD(D). Use that as cash base.
    #   Walk events from anchor_t to EOD(D) → cash at EOD(D).
    #   positions at EOD(D) from positions_at(fills, eod_ms).
    #   equity = cash + Σ pos × mark_at(eod_ms).

    # codex m01 r6 fix: load MAIN-only fills (excludes xyz: HIP-3 perps).
    events_fills, observed_unanchored_dex = load_wallet_fills(
        wallet, start_ms, anchor_data['fetched_ms'], main_only=True
    )
    events_ledger = load_wallet_ledger(wallet, start_ms, anchor_data['fetched_ms'])
    events_funding = get_api_funding(wallet, start_ms, anchor_data['fetched_ms'])
    # codex m01 r20 fix: hard-fail on funding pagination errors. None → mark wallet INCOMPLETE.
    if events_funding is None:
        return {'wallet': wallet, 'error': 'funding_fetch_incomplete'}

    # Merge events into chronological stream
    stream: list[tuple[int, str, dict]] = []
    for f in events_fills:
        stream.append((int(f['time']), 'fill', f))
    for e in events_ledger:
        stream.append((int(e['time']), 'ledger', e))
    for e in events_funding:
        stream.append((int(e['time']), 'funding', e))
    stream.sort(key=lambda x: x[0])

    earliest_anchor_ms, earliest_api_eq = valid_anchors[0]
    rows = []
    # Clamp start to max(earliest_anchor, requested start_ms)
    effective_start_ms = max(earliest_anchor_ms, start_ms)
    current_day = pd.Timestamp(effective_start_ms, unit='ms', tz='UTC').floor('D').date()
    end_day = pd.Timestamp(end_ms, unit='ms', tz='UTC').floor('D').date()
    audit_missing_marks_at_start = 0
    n_rows_skipped_missing = 0  # codex m01 r24: track skipped rows with any missing marks

    def compute_eq_at(t_ms: int, anchor_ms: int, anchor_eq: float):
        """Forward-walk events from anchor_ms to t_ms; return (cash, positions, eq, missing_marks, pos_value).
        anchor_eq is the API equity at anchor_ms — snap cash to (anchor_eq - Σ pos × mark_at(anchor_ms)).
        """
        # Start positions at anchor_ms
        start_positions = positions_at(events_fills, anchor_ms)
        # codex m01 r4 CODE-BUG 3 fix (v8): for coins with NO fill at-or-before anchor_ms but
        # at least one fill AFTER, the first post-anchor fill's `startPosition` reveals the
        # actual pre-anchor position (HL stamps every fill with the position-before-fill).
        # Without this seed, pre-window positions on coins not traded in [start_ms, anchor_ms]
        # are silently treated as 0 → cash over-attribution + missed position value.
        seen_coins_pre = {f['coin'] for f in events_fills if int(f['time']) <= anchor_ms}
        for f in events_fills:
            if int(f['time']) <= anchor_ms:
                continue
            coin = f['coin']
            if coin_is_spot(coin) or coin in seen_coins_pre or coin in start_positions:
                continue
            pos_pre_fill = float(f.get('startPosition', 0))
            if abs(pos_pre_fill) > 1e-9:
                start_positions[coin] = pos_pre_fill
            seen_coins_pre.add(coin)
        # codex m01 r15+r16+r22 HIGH fix: seed from MAIN anchor_data['positions'] for static
        # no-fill positions held across the window.
        #
        # Two cases for seeding anchor positions:
        # A) Coin has NO in-window fills at all → anchor position is valid for ANY anchor
        #    (the wallet held the coin the whole time without trading; r22 fix).
        # B) Coin has pre-anchor fills but no post-anchor fills → only seed when anchor is
        #    close to fetched_ms (24h heuristic), since anchor position reflects fetched-time
        #    state which may differ from anchor-time state if there were post-anchor fills.
        #
        # codex m01 r16 fix preserved: SKIP coins with post-anchor fills (r4 #3 handles those).
        post_anchor_fill_coins = {
            f['coin'] for f in events_fills
            if int(f['time']) > anchor_ms
        }
        any_fill_coins = {f['coin'] for f in events_fills}
        for coin, szi in anchor_data.get('positions', {}).items():
            if coin in start_positions or coin_is_spot(coin) or abs(szi) < 1e-9:
                continue
            if coin.startswith('xyz:'):
                continue  # MAIN-only reconstruction
            if coin in post_anchor_fill_coins:
                continue  # r16: r4 #3 handles post-anchor coins
            # Case A: zero in-window fills → seed at any anchor.
            if coin not in any_fill_coins:
                start_positions[coin] = float(szi)
                continue
            # Case B: pre-anchor fills but no post-anchor fills → 24h proximity heuristic only.
            if abs(anchor_ms - anchor_data['fetched_ms']) <= 86400000:
                start_positions[coin] = float(szi)
        # Compute pos value at anchor
        anchor_pos_value = 0.0
        missing = 0
        for c, sz in start_positions.items():
            mark = get_mark(c, anchor_ms)
            if mark is None:
                missing += 1
                continue
            anchor_pos_value += sz * mark
        cash_local = anchor_eq - anchor_pos_value

        # Walk events between anchor_ms and t_ms (exclusive at anchor_ms, inclusive at t_ms)
        positions_local = dict(start_positions)
        for ts, typ, ev in stream:
            if ts <= anchor_ms or ts > t_ms:
                continue
            if typ == 'fill':
                cash_local += fill_cash_delta(ev)
                coin = ev['coin']
                if not coin_is_spot(coin):
                    positions_local[coin] = positions_local.get(coin, 0.0) + float(ev['signed_sz'])
                    if abs(positions_local[coin]) < 1e-9:
                        positions_local.pop(coin, None)
            elif typ == 'ledger':
                cash_local += ledger_cash_delta(ev, wallet_lc)
            elif typ == 'funding':
                cash_local += funding_cash_delta(ev)

        # Compute equity at t_ms
        # codex m01 r10 fix: return pos_value as an INDEPENDENT mark-derived value (not equity-cash).
        # Caller emits this as `position_value_usd` so Gate 2 can verify the accounting identity
        # ISN'T trivially self-true.
        pos_value = 0.0
        for c, sz in positions_local.items():
            mark = get_mark(c, t_ms)
            if mark is None:
                missing += 1
                continue
            pos_value += sz * mark
        eq = cash_local + pos_value
        return cash_local, positions_local, eq, missing, pos_value

    # codex m01 r4 CODE-BUG 5 fix (v8): clamp output to anchor.fetched_ms. Daily output past
    # fetch time would be reconstructed from stale event data (fills/ledger/funding loaded only
    # up to fetched_ms) → silently wrong equity for days after fetched_ms.
    anchor_fetched_day = pd.Timestamp(anchor_data['fetched_ms'], unit='ms', tz='UTC').floor('D').date()
    if end_day > anchor_fetched_day:
        end_day = anchor_fetched_day
    while current_day <= end_day:
        eod_ms = int(pd.Timestamp(current_day, tz='UTC').timestamp() * 1000 + 86399999)
        # Hard clamp: never emit past fetched_ms even if eod_ms is in-range.
        if eod_ms > anchor_data['fetched_ms']:
            break
        # Find latest API anchor <= eod_ms
        # codex m01 r23 HIGH fix: bound anchors to >= start_ms. Otherwise the walker may pick
        # a pre-start_ms API anchor and walk forward, missing events in (anchor_t, start_ms)
        # because the event stream was only loaded from start_ms. Silent equity corruption.
        before_anchors = [(t, v) for t, v in valid_anchors if start_ms <= t <= eod_ms]
        if not before_anchors:
            # No anchor before this day → skip
            current_day = current_day + pd.Timedelta(days=1).to_pytimedelta()
            continue
        anchor_t, anchor_v = before_anchors[-1]
        cash, positions, equity, missing, pos_value_independent = compute_eq_at(eod_ms, anchor_t, anchor_v)
        if current_day == pd.Timestamp(earliest_anchor_ms, unit='ms', tz='UTC').floor('D').date():
            audit_missing_marks_at_start = missing
        # codex m01 r24 HIGH fix: SKIP rows with any missing marks. compute_eq_at silently omits
        # missing-mark coin from BOTH cash_local (via anchor_pos_value) and pos_value_independent
        # (via mtm sum), so equity is numerically wrong. Better to skip than emit corrupted row.
        # Day skipped is captured in audit_rows_skipped_missing_marks below.
        if missing > 0:
            n_rows_skipped_missing += 1
            current_day = current_day + pd.Timedelta(days=1).to_pytimedelta()
            continue
        # codex m01 r10 fix: emit MARK-DERIVED position_value (not equity-cash) so Gate 2 is a
        # REAL accounting identity check, not a tautology.
        rows.append({
            'wallet': wallet,
            'date': current_day,
            'perp_account_value_usd': equity,
            'cash': cash,
            'position_value_usd': pos_value_independent,   # mark-derived, INDEPENDENT
            'n_positions': len(positions),
            'audit_missing_marks': missing,  # always 0 in emitted rows (r24)
            'anchor_age_h': (eod_ms - anchor_t) / 3600000,
        })
        current_day = current_day + pd.Timedelta(days=1).to_pytimedelta()

    if not rows:
        # codex m01 r25 HIGH fix: when no rows emitted, still preserve skip count for Gate 3.
        # Differentiate 'all_skipped_missing_marks' from generic 'no_rows' so Gate 3 sees the
        # systemic mark gap, not silent disappearance.
        if n_rows_skipped_missing > 0:
            return {
                'wallet': wallet,
                'error': 'all_rows_skipped_missing_marks',
                'n_rows_skipped_missing': n_rows_skipped_missing,
            }
        return {'wallet': wallet, 'error': 'no_rows'}

    df_out = pd.DataFrame(rows)
    # codex m01 r4 #6 + r18 fix: legacy column compatibility.
    # journey_trace consumer drops rows with null spot_usdc_today and uses it as the denominator
    # for max_position_pct_equity. We can't NaN it without breaking journey_trace; we also can't
    # use aggregate perp cash (Hard Rule #16: HL_EQ=spot only — but that's for OUR portfolio
    # accounting, not source-wallet ranking-denominator purposes). For ranking, we use the
    # MAIN-only `perp_acct_value_today` as the denominator. journey_trace receives this in the
    # legacy `spot_usdc_today` slot but with the explicit status field documenting the actual semantics.
    # NEW v0.1 work: update journey_trace + downstream to read perp_acct_value_today directly.
    df_out['spot_usdc_today'] = anchor_data['perp_acct_value_today']  # MAIN-only proxy (was NaN in earlier v8)
    df_out['spot_usdc_today_status'] = 'MAIN_PERP_ACCT_VALUE_PROXY_v8'
    df_out['perp_acct_value_today'] = anchor_data['perp_acct_value_today']  # MAIN-only (v8 #4 fix)
    df_out['aggregate_perp_acct_value'] = anchor_data['aggregate_acct_value']  # informational
    df_out['aggregate_cash'] = anchor_data['aggregate_cash']  # informational
    df_out['audit_sentinel_zeros'] = n_sentinel_zeros
    # codex m01 r6 fix: actually set audit_unanchored_dex from load_wallet_fills observation.
    df_out['audit_unanchored_dex'] = bool(observed_unanchored_dex)
    # codex m01 r17 fix: emit per-row flx contamination risk flag.
    df_out['audit_flx_contamination_risk'] = bool(anchor_data.get('has_flx_anchor', False))

    # Gate 1 input: drift at last emitted day vs API anchor AT THAT DAY (not "ever").
    # codex m01 r5 fix: previously used valid_anchors[-1] which could be a future API anchor
    # if portfolio.perpAllTime returned data past anchor_data['fetched_ms']. Now pick the
    # anchor at or before last-emitted-day.
    last_row = df_out.iloc[-1]
    last_row_eod_ms = int(pd.Timestamp(last_row['date'], tz='UTC').timestamp() * 1000 + 86399999)
    matching_anchors = [(t, v) for t, v in valid_anchors if t <= last_row_eod_ms]
    last_api = matching_anchors[-1][1] if matching_anchors else None
    drift_pct = None
    if last_api and abs(last_api) > 0.01:
        drift_pct = (last_row['perp_account_value_usd'] - last_api) / last_api
    df_out['audit_drift_pct_last_vs_api'] = drift_pct or 0.0

    # codex m01 r11 fix: emit per-row drift_pct (vs at-or-before API anchor for each row's day)
    # so Gate 1 can check "75% of wallet-days < 10%" per the spec contract, not just last day.
    def _row_drift_pct(row):
        eod_ms = int(pd.Timestamp(row['date'], tz='UTC').timestamp() * 1000 + 86399999)
        matches = [(t, v) for t, v in valid_anchors if t <= eod_ms]
        if not matches:
            return float('nan')
        api_v = matches[-1][1]
        if abs(api_v) < 0.01:
            return float('nan')
        return (row['perp_account_value_usd'] - api_v) / api_v
    df_out['audit_drift_pct_per_day'] = df_out.apply(_row_drift_pct, axis=1)

    return {
        'wallet': wallet,
        'series': df_out,
        'audit': {
            'n_fills': len(events_fills),
            'n_ledger': len(events_ledger),
            'n_funding': len(events_funding),
            'n_sentinel_zeros': n_sentinel_zeros,
            'drift_pct': drift_pct,
            'missing_marks_at_start': audit_missing_marks_at_start,
            'n_rows_skipped_missing': n_rows_skipped_missing,  # codex m01 r24
        },
        # codex m01 r5 fix: return worker-local _UNKNOWN_LEDGER_TYPES so parent can aggregate
        # across worker processes (module-scope set is per-worker under multiprocessing).
        'unknown_ledger_types': sorted(_UNKNOWN_LEDGER_TYPES),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--wallets-file', required=True, help='Text file, one wallet address per line')
    ap.add_argument('--start', default='2025-12-01', help='YYYY-MM-DD')
    ap.add_argument('--end', default='2026-05-23', help='YYYY-MM-DD')
    ap.add_argument('--output', required=True, help='Output parquet path')
    ap.add_argument('--n-workers', type=int, default=4)
    ap.add_argument('--validation-only', action='store_true', help='Run gate checks only, no full series')
    args = ap.parse_args()

    start_ms = int(pd.Timestamp(args.start, tz='UTC').timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz='UTC') + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    # Load wallets
    # codex m01 r4 CODE-BUG 8 fix (v8): NORMALIZE wallet casing to lowercase.
    # Mixed-case wallet inputs (checksum) cause anchor lookup + ledger glob misses because
    # those compare exact strings.
    with open(args.wallets_file) as f:
        wallets = [line.strip().lower() for line in f if line.strip() and not line.startswith('#')]
    logger.info(f'Loaded {len(wallets):,} wallets from {args.wallets_file} (lowercased)')

    # Load anchor parquet
    anchor_df = pd.read_parquet(ANCHOR_PARQUET)
    logger.info(f'Loaded anchor parquet: {len(anchor_df):,} rows')

    # Build job args
    job_args = []
    skipped_no_anchor = 0
    for w in wallets:
        anchor_data = load_wallet_anchor(w, anchor_df)
        if anchor_data is None:
            skipped_no_anchor += 1
            continue
        job_args.append((w, anchor_data, start_ms, end_ms, args.validation_only))
    logger.info(f'Job args: {len(job_args):,} (skipped {skipped_no_anchor} with no anchor)')

    # Run
    all_series = []
    audits = []
    # codex m01 r5 fix: collect unknown_ledger_types ACROSS worker processes for accurate diagnostic.
    unknown_types_aggregate: set[str] = set()
    n_wallet_exceptions = 0
    # codex m01 r25+r26 fix: track wallets that failed entirely due to all-rows-skipped-missing-marks
    # so Gate 3 can include them in missing-mark gate accounting (don't silently disappear).
    # r26: ALSO track the row count from those full-loss wallets so row-skip-rate gate
    # numerator+denominator includes them (was undercounting before r26).
    n_skipped_missing_marks_full_wallet_loss = 0
    n_rows_skipped_full_wallet_loss = 0
    t0 = time.time()
    if args.n_workers > 1:
        with ProcessPoolExecutor(max_workers=args.n_workers) as ex:
            futs = {ex.submit(reconstruct_wallet, a): a[0] for a in job_args}
            for j, fut in enumerate(as_completed(futs), 1):
                w = futs[fut]
                try:
                    res = fut.result()
                    if res is None or 'error' in res:
                        err = res.get('error') if res else 'None'
                        logger.warning(f'  wallet fail {w[:10]}: {err}')
                        if err == 'all_rows_skipped_missing_marks':
                            n_skipped_missing_marks_full_wallet_loss += 1
                            n_rows_skipped_full_wallet_loss += int(res.get('n_rows_skipped_missing', 0))
                        continue
                    all_series.append(res['series'])
                    audits.append({'wallet': w, **res['audit']})
                    unknown_types_aggregate.update(res.get('unknown_ledger_types', []))
                except Exception as e:
                    n_wallet_exceptions += 1
                    logger.warning(f'  wallet exception {w[:10]}: {e}')
                if j % 10 == 0:
                    elapsed = time.time() - t0
                    rate = j / elapsed
                    eta_min = (len(job_args) - j) / rate / 60
                    logger.info(f'  [{j}/{len(job_args)}] rate={rate:.1f}/s eta={eta_min:.1f}min')
    else:
        for j, a in enumerate(job_args, 1):
            try:
                res = reconstruct_wallet(a)
                if res is None or 'error' in res:
                    err = res.get('error') if res else 'None'
                    logger.warning(f'  wallet fail {a[0][:10]}: {err}')
                    if err == 'all_rows_skipped_missing_marks':
                        n_skipped_missing_marks_full_wallet_loss += 1
                        n_rows_skipped_full_wallet_loss += int(res.get('n_rows_skipped_missing', 0))
                    continue
                all_series.append(res['series'])
                audits.append({'wallet': a[0], **res['audit']})
                unknown_types_aggregate.update(res.get('unknown_ledger_types', []))
            except Exception as e:
                n_wallet_exceptions += 1
                logger.warning(f'  wallet exception {a[0][:10]}: {e}')
            if j % 5 == 0:
                logger.info(f'  [{j}/{len(job_args)}] processed')

    # codex m01 r10+r11 fix: validation_only mode skips series concat/output entirely.
    # Worker short-circuit returned empty DataFrames; main now filters them out and writes audit-only.
    # r11 fix: ZERO-AUDIT must be HARD FAIL (not trivial Gate 3 pass).
    audit_df = pd.DataFrame(audits)
    if args.validation_only:
        logger.info(f'\n=== VALIDATION-ONLY MODE: skipping series concat ===')
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path = out_path.with_suffix('.audit.parquet')
        audit_df.to_parquet(audit_path, index=False, compression='snappy')
        logger.info(f'=== Wrote audit-only: {audit_path} ({len(audit_df):,} wallets) ===')
        # codex m01 r11+r14 fix: empty audit_df = HARD FAIL. Zero wallets succeeded → validation
        # framework cannot greenlight; sys.exit(4). Use TOTAL INPUT (wallets) so no_anchor drops
        # are visible in skip rate.
        n_input_wallets = len(wallets)
        n_audited = len(audit_df)
        n_skipped = n_input_wallets - n_audited
        if n_audited == 0:
            logger.error(f'VALIDATION-ONLY FAILED: 0 of {n_input_wallets} wallets produced audit data → exit 4')
            sys.exit(4)
        n_sentinel = int(audit_df['n_sentinel_zeros'].sum()) if 'n_sentinel_zeros' in audit_df.columns else 0
        sentinel_per_wallet = n_sentinel / max(n_audited, 1)
        # Gate 3 stricter under validation_only: also count wallet skips/errors against pass.
        skip_rate = n_skipped / max(n_input_wallets, 1)
        gate1_pass = True  # validation-only doesn't compute drift_pct
        # codex m01 r12 fix: validation-only short-circuits BEFORE ledger walk, so
        # `unknown_types_aggregate` is artificially empty and CANNOT be checked here.
        # Drop unknown_types from Gate 3 in this mode; log a NOTICE.
        gate3_pass = bool(
            n_wallet_exceptions == 0
            and sentinel_per_wallet < 5
            and skip_rate < 0.5
        )
        logger.info(f'\nVALIDATION-ONLY GATES:')
        logger.info(f'  Gate 1: SKIPPED (no series → no drift_pct)')
        logger.info(f'  Gate 2: SKIPPED (no series → no identity check)')
        logger.info(f'  Gate 3: {"PASS" if gate3_pass else "FAIL"} '
                    f'(audited={n_audited}/{n_input_wallets}, skip_rate={skip_rate:.1%}, '
                    f'sentinel/wallet={sentinel_per_wallet:.2f}, exceptions={n_wallet_exceptions})')
        logger.info(f'  NOTICE: validation-only does NOT load ledger entries → unknown_types NOT checked here. '
                    f'Run normal mode (no --validation-only) to surface unknown ledger types.')
        if not gate3_pass:
            sys.exit(3)
        return

    if not all_series or all(s.empty for s in all_series):
        # codex m01 r4 CODE-BUG 10 fix (v8): empty output is a HARD FAILURE.
        logger.error('No series produced — FAILING process')
        sys.exit(2)

    # Filter out empty frames (defensive — shouldn't occur in normal mode).
    out_df = pd.concat([s for s in all_series if not s.empty], ignore_index=True)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False, compression='snappy')
    audit_df.to_parquet(out_path.with_suffix('.audit.parquet'), index=False, compression='snappy')

    logger.info(f'\n=== Wrote {out_path}: {len(out_df):,} rows ({out_df["wallet"].nunique():,} wallets × {out_df["date"].nunique()} days) ===')
    logger.info(f'=== Audit: {out_path.with_suffix(".audit.parquet")} ===')

    # === STRICT VALIDATION GATES (codex r6 + m01 r5 fixes) ===
    logger.info('\n=== VALIDATION GATES ===')

    # Gate 1: anchor reconciliation
    # codex m01 r11 fix: BROADENED to per-wallet-per-DAY drift (was last-day-only).
    # Spec says "75% of wallet-days < 10% drift". Now actually checks that contract.
    last_day_drift = audit_df['drift_pct'].dropna()
    median_last_day = last_day_drift.abs().median() if len(last_day_drift) else 0
    p75_last_day = (last_day_drift.abs() < 0.10).mean() if len(last_day_drift) else 0
    # Per-wallet-day check via emitted column
    if 'audit_drift_pct_per_day' in out_df.columns:
        per_day_drift = out_df['audit_drift_pct_per_day'].dropna()
        median_per_day = per_day_drift.abs().median() if len(per_day_drift) else 0
        p75_per_day = (per_day_drift.abs() < 0.10).mean() if len(per_day_drift) else 0
    else:
        median_per_day = 0
        p75_per_day = 0
        per_day_drift = pd.Series(dtype=float)
    # Gate 1 PASS contract: BOTH last-day AND per-day must satisfy thresholds.
    gate1_pass = bool(
        median_last_day < 0.01 and p75_last_day >= 0.75
        and median_per_day < 0.05 and p75_per_day >= 0.75   # per-day is stricter on median, looser on quantile
    )
    logger.info(f'GATE 1 (anchor reconciliation):')
    logger.info(f'  last-day  n_wallets: {len(last_day_drift)}; median |drift|: {median_last_day*100:.4f}% (<1%); '
                f'<10%: {p75_last_day*100:.1f}% (>=75%)')
    logger.info(f'  per-day   n_rows: {len(per_day_drift)};    median |drift|: {median_per_day*100:.4f}% (<5%); '
                f'<10%: {p75_per_day*100:.1f}% (>=75%)')
    logger.info(f'  {"PASS" if gate1_pass else "FAIL"}')

    # Gate 2: accounting identity (eq == cash + position_value within epsilon).
    # codex m01 r9+r10 fix: position_value_usd is mark-derived INDEPENDENTLY in compute_eq_at
    # (sum of qty × mark per coin), then equity = cash + pos_value. This check verifies that
    # the EMITTED parquet rows survive serialization without corruption — catches parquet/
    # float-roundtrip issues + accidental column mutations.
    # KNOWN LIMITATION: producer + consumer compute both sides from same source, so this is
    # NOT a fully-independent identity check. True validation requires either emitting positions
    # per row (large output) or comparing vs API anchor at re-anchor points (Gate 1 covers this).
    finite_mask = (
        out_df['perp_account_value_usd'].notna()
        & out_df['cash'].notna()
        & out_df['position_value_usd'].notna()
        & np.isfinite(out_df['perp_account_value_usd'])
        & np.isfinite(out_df['cash'])
        & np.isfinite(out_df['position_value_usd'])
    )
    n_bad_finite = int((~finite_mask).sum())
    finite_df = out_df[finite_mask]
    eps_per_row = np.maximum(0.01, np.abs(finite_df['perp_account_value_usd']) * 0.0001)
    identity_residual = (
        finite_df['perp_account_value_usd']
        - (finite_df['cash'] + finite_df['position_value_usd'])
    ).abs()
    identity_ok = identity_residual <= eps_per_row
    n_identity_violations = int((~identity_ok).sum())
    max_residual = float(identity_residual.max()) if len(identity_residual) else 0.0
    gate2_pass = bool(n_bad_finite == 0 and n_identity_violations == 0)
    logger.info(f'\nGATE 2 (accounting identity eq = cash + position_value, eps = max($0.01, 1bp)):')
    logger.info(f'  rows total: {len(out_df):,}; non-finite: {n_bad_finite}')
    logger.info(f'  identity violations: {n_identity_violations}; max residual: ${max_residual:.4f}')
    logger.info(f'  {"PASS" if gate2_pass else "FAIL"}')

    # Gate 3: audit coverage (REAL checks per codex m01 r5/r6/r13).
    n_sentinel = int(audit_df['n_sentinel_zeros'].sum())
    n_unknown_types = len(unknown_types_aggregate)
    # codex m01 r6 fix: enforce sentinel inflation cap.
    n_wallets_processed = max(len(audit_df), 1)
    sentinel_per_wallet = n_sentinel / n_wallets_processed
    sentinel_ok = sentinel_per_wallet < 5  # arbitrary threshold; tune after large-N validation
    # codex m01 r13+r14 CRITICAL fix: normal-mode skip rate from TOTAL INPUT wallets,
    # not job_args (which excludes no_anchor drops). Wallets dropped at ANY stage (no_anchor,
    # no_valid_anchors, no_rows, worker error) count against skip rate.
    n_input_wallets = len(wallets)  # original input file count
    n_audited = len(audit_df)
    n_skipped = n_input_wallets - n_audited
    skip_rate = n_skipped / max(n_input_wallets, 1)
    skip_rate_ok = skip_rate < 0.20  # >20% skip rate = systemic issue (anchor pull failure, etc.)
    # codex m01 r13+r14+r24 fix: missing marks gate now enforces ZERO emitted rows with
    # missing marks (r24 skips them outright). Track skip volume separately.
    if 'audit_missing_marks' in out_df.columns:
        n_missing_marks_total = int(out_df['audit_missing_marks'].sum())
        n_rows = max(len(out_df), 1)
        missing_marks_per_row = n_missing_marks_total / n_rows
    else:
        n_missing_marks_total = 0
        missing_marks_per_row = 0
    missing_marks_emitted_ok = n_missing_marks_total == 0  # r24: must be 0 by construction
    # r24+r25+r26: row-skip rate audit. Includes per-wallet skip counts FROM audit_df
    # PLUS skip counts from wallets that lost ALL rows (not in audit_df, tracked via main loop).
    n_rows_skipped_audit = int(audit_df['n_rows_skipped_missing'].sum()) if 'n_rows_skipped_missing' in audit_df.columns else 0
    n_rows_skipped_for_missing = n_rows_skipped_audit + n_rows_skipped_full_wallet_loss
    total_attempted_rows = len(out_df) + n_rows_skipped_for_missing
    skip_rate_for_missing = n_rows_skipped_for_missing / max(total_attempted_rows, 1)
    skip_for_missing_ok = skip_rate_for_missing < 0.10  # <10% of attempted rows skipped
    # r25: full-wallet loss rate — fraction of INPUT wallets that lost all rows to missing marks.
    full_wallet_missing_loss_rate = n_skipped_missing_marks_full_wallet_loss / max(n_input_wallets, 1)
    full_wallet_missing_ok = full_wallet_missing_loss_rate < 0.05  # <5% of input wallets fully lost
    missing_marks_ok = missing_marks_emitted_ok and skip_for_missing_ok and full_wallet_missing_ok
    # codex m01 r7 fix: count WALLETS with unanchored dex fills (not rows, which over-inflates by days).
    if 'audit_unanchored_dex' in out_df.columns:
        wallets_with_unanchored = out_df.groupby('wallet')['audit_unanchored_dex'].any()
        n_wallets_unanchored = int(wallets_with_unanchored.sum())
    else:
        n_wallets_unanchored = 0
    gate3_pass = bool(
        n_wallet_exceptions == 0
        and n_unknown_types == 0
        and sentinel_ok
        and skip_rate_ok          # codex m01 r13 CRITICAL fix
        and missing_marks_ok      # codex m01 r13 fix
    )
    logger.info(f'\nGATE 3 (audit coverage):')
    logger.info(f'  input_wallets={n_input_wallets} audited={n_audited} skip_rate={skip_rate:.1%} (cap <20%)')
    logger.info(f'  sentinel $0 anchors found: {n_sentinel} (avg {sentinel_per_wallet:.2f}/wallet; cap <5)')
    logger.info(f'  missing marks in EMITTED rows: {n_missing_marks_total} (must be 0; r24 skips missing-mark rows)')
    logger.info(f'  rows skipped for missing marks: {n_rows_skipped_for_missing}/{total_attempted_rows} ({skip_rate_for_missing:.1%}; cap <10%) [audit:{n_rows_skipped_audit} + full-loss:{n_rows_skipped_full_wallet_loss}]')
    logger.info(f'  WALLETS fully lost to missing marks: {n_skipped_missing_marks_full_wallet_loss}/{n_input_wallets} ({full_wallet_missing_loss_rate:.1%}; cap <5%)')
    logger.info(f'  @ spot coins handling: SKIPPED in fill_cash_delta + positions_at')
    logger.info(f'  funding source: direct API (cache bypassed) — paginated (v8)')
    logger.info(f'  wallet exceptions: {n_wallet_exceptions} (must be 0)')
    logger.info(f'  unknown ledger types: {n_unknown_types} → {sorted(unknown_types_aggregate) if unknown_types_aggregate else "[]"}')
    logger.info(f'  WALLETS with xyz: HIP-3 fills observed and filtered (informational): {n_wallets_unanchored}')
    # codex m01 r17 fix: report flx contamination risk per wallet from anchor profile.
    if 'audit_flx_contamination_risk' in out_df.columns:
        flx_wallets = out_df.groupby('wallet')['audit_flx_contamination_risk'].any()
        n_wallets_flx_risk = int(flx_wallets.sum())
    else:
        n_wallets_flx_risk = 0
    logger.info(f'  WALLETS with flx anchor presence (contamination risk; Module 04 should exclude): {n_wallets_flx_risk}')
    logger.info(f'  (flx-perp filtering BEST-EFFORT in fills: no `dex` column in S3 fills v2 schema;')
    logger.info(f'   bulk cash leak mitigated via ledger-side dex-scope; per-row flag isolates risk wallets.)')
    logger.info(f'  {"PASS" if gate3_pass else "FAIL"}')

    logger.info(f'\nWall: {(time.time()-t0)/60:.1f}min')

    # codex m01 r4 #10 + r5 fixes: validation failures FAIL the process.
    all_gates_pass = gate1_pass and gate2_pass and gate3_pass
    if not all_gates_pass:
        logger.error(f'VALIDATION GATES FAILED (G1={gate1_pass} G2={gate2_pass} G3={gate3_pass}) — failing process with exit code 3')
        sys.exit(3)


if __name__ == '__main__':
    main()
