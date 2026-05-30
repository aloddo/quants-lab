#!/usr/bin/env python3
"""V13 Script 2/5 (v3): Journey tracing with carry-in + REVERSE PnL fix + COST columns.

Per projects/quant/v13 Section 5.2 + bulletproof spec 2026-05-26 step B (codex r16 reviewed).

Per (wallet, coin), fills are walked in time order while maintaining the
running net position. Each fill is classified ENTRY / ADDON / TRIM / EXIT /
REVERSE. A journey is a contiguous sequence of fills bracketed by ENTRY (or
REVERSE_OPEN) and EXIT (or REVERSE_CLOSE).

Fixes from v1 (unchanged):

1. REVERSE PnL split (codex r1 #6): the `closedPnl` on a reverse fill
   belongs ENTIRELY to the closing leg. The new opposite-side journey
   starts at zero realized PnL.

2. Carry-in seed (codex r1 #7): bulk-load prior fills + walk back to last-flat.
   (wallet, coin) pairs whose carry-in cannot be proven are flagged INCOMPLETE.

3. Deterministic same-timestamp ordering (codex r1 #8).

4. Peak-date equity (codex r1 #9 P2).

5. Schema validation (codex r3 gotcha #8).

NEW Step B (codex r16, 2026-05-26):

6. Per-fill fee accumulation. SOURCE_ASSUMED_TAKER_FEE_BPS = 4.32 (HL base 4.5 -4%
   referral discount conservative). Source can't tell maker/taker from S3; we
   assume taker which overestimates fees if source is partly maker.

7. REVERSE fee splitting (codex r16 #6): a REVERSE fill closes |pos_before| AND
   opens |pos_after| at same instant. Closing fee = |pos_before| * price * rate
   (added to closing journey). Opening fee = |pos_after| * price * rate (added to
   new journey).

8. Per-journey funding via existing get_funding_updates (paginated, delta-shape).
   Half-open interval (entry_ts, exit_ts]. Per-wallet disk cache at
   app/data/v13/funding_cache/. Funding only applies to perps; spot/dust/other
   scope set to 0.

9. fee_assumption_scope classification: standard_perp | builder_perp | spot |
   other. xyz: prefix coins flagged as builder_perp (rates may differ but use
   placeholder rate; flag is diagnostic).

10. net_realized_pnl_usd = realized_pnl_usd - fees_paid_usd + funding_net_usd
    (funding_net_usd positive when source received).

Inputs:
    --start YYYY-MM-DD          Earliest fill date in window
    --end YYYY-MM-DD            Latest fill date in window
    --wallets <path>            Optional newline-separated wallet filter
    --equity-series <path>      wallet_equity_series.parquet (REQUIRED for peak-pct)
    --walkback-days N           Carry-in walkback (default 90)
    --output <path>             app/data/v13/wallet_journeys_costed.parquet

Outputs (parquet, one row per closed journey; B columns marked NEW):
    wallet, coin, journey_id, side,
    entry_ts, exit_ts, duration_hours,
    n_entry_fills, n_carry_in_seeds, n_addon_fills, n_trim_fills, n_exit_fills,
    n_reverse_fills, n_fills_total,
    max_position_notional_usd, max_position_pct_equity, peak_ts,
    avg_seconds_between_addons, avg_seconds_between_trims,
    realized_pnl_usd, pnl_bps_of_max,
    fees_paid_usd (NEW B1),
    funding_net_usd (NEW B2, positive = received),
    net_realized_pnl_usd (NEW B3),
    fee_assumption_scope (NEW B1 diagnostic),
    journey_class,
    carry_in_status
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Reuse equity_reconstruct's shared helpers.
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_equity_reconstruct import (    # noqa: E402
    EPS,
    REQUIRED_FILL_COLUMNS,
    validate_and_normalize_fills,
    load_fills_for_dates,
    load_prior_fills_for_wallets,
    find_carry_in_state_from_prior,
    get_funding_updates,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_journey] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills_v2"   # codex m02 r2 BLOCKER fix 2026-05-29: was hl_s3_fills (old), caused carry-in walkback to silently fail with KeyError on missing fee/builderFee/deployerFee columns. v2 has 22 cols including the fee fields that _FILLS_COLS_DEFAULT requires.
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_journeys_costed.parquet"
FUNDING_CACHE_DIR = ROOT / "app" / "data" / "v13" / "funding_cache"

SCALP_THRESHOLD_S = 30 * 60
SWING_THRESHOLD_S = 24 * 60 * 60

# Step B cost model (codex r16 approved 2026-05-26):
# - Source side: assume taker (conservative; can't tell maker/taker from S3).
# - 4.32 bps assumes HL base perp taker 4.5 bps with 4% referral discount.
# - Builder perps (xyz: prefix) may differ; flagged but use same rate.
# - Spot (Buy/Sell dir) may differ; flagged separately, same rate as placeholder.
# - REVERSE fills split fee between closing leg (|pos_before|) + opening leg (|pos_after|).
SOURCE_ASSUMED_TAKER_FEE_BPS = 4.32
FEE_RATE = SOURCE_ASSUMED_TAKER_FEE_BPS / 10000.0


def _classify_fee_scope(dir_val: str, coin: str) -> str:
    """Classify fill for fee_assumption_scope diagnostic.

    Returns one of: standard_perp, builder_perp, spot, other.
    Affects logging only; actual rate is currently SOURCE_ASSUMED_TAKER_FEE_BPS for all.
    """
    if isinstance(coin, str) and coin.startswith("xyz:"):
        return "builder_perp"
    if dir_val in ("Buy", "Sell"):
        return "spot"
    if dir_val in (
        "Open Long", "Close Long", "Open Short", "Close Short",
        "Long > Short", "Short > Long",
    ):
        return "standard_perp"
    return "other"


def _fill_fee_usd(notional: float) -> float:
    """LEGACY estimator: per-fill fee in USD = notional × FEE_RATE (4.32 bps source-assumed-taker).
    Used as fallback when v2 fee fields are unavailable. Per codex m02 r2 BLOCKER 2 (2026-05-29),
    when fee/builderFee/deployerFee are present in the fill row, use them directly via
    _fill_fee_usd_actual() instead — they reflect HL's actual charged fees (including referral
    discount, builder splits, deployer fees) rather than the assumed-taker conservative estimate.
    """
    return abs(notional) * FEE_RATE


def _fill_fee_usd_actual(row) -> float | None:
    """Per codex m02 r2 BLOCKER 2 (2026-05-29) + Alberto TG 7549: use v2-fills enriched fee fields.
    Wallet realized PnL = closedPnl - (fee + builderFee + deployerFee).
    Returns None if ANY fee field is missing/NaN (caller falls back to _fill_fee_usd estimator).
    codex m02 r3 CODE-BUG 5 fix (2026-05-29): use pd.isna check, not None/falsy.
    """
    import math as _m
    fee = getattr(row, "fee", None)
    builder_fee = getattr(row, "builderFee", None)
    deployer_fee = getattr(row, "deployerFee", None)
    # pd.isna for pandas missing-value detection (NaN is the actual missing sentinel)
    def _is_missing(x):
        if x is None: return True
        try:
            return bool(_m.isnan(float(x)))
        except (TypeError, ValueError):
            return False
    if _is_missing(fee) or _is_missing(builder_fee) or _is_missing(deployer_fee):
        return None
    return float(fee) + float(builder_fee) + float(deployer_fee)


def _fetch_funding_for_wallet(
    wallet: str, start_ms: int, end_ms: int,
) -> list[dict]:
    """Pull funding events for a wallet (with disk cache).

    Cache key = wallet/window full range. Cached as parquet per wallet.
    Returns list of {time_ms, coin, usdc_signed} sorted by time.
    """
    FUNDING_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = FUNDING_CACHE_DIR / f"{wallet}_{start_ms}_{end_ms}.parquet"
    if cache_file.exists():
        try:
            cached = pd.read_parquet(cache_file)
            return cached.to_dict("records")
        except Exception as e:
            logger.warning(f"funding cache read failed {cache_file.name}: {e}; refetching")

    try:
        entries = get_funding_updates(wallet, start_ms, end_ms)
    except Exception as e:
        logger.warning(f"funding fetch failed {wallet[:10]}: {e}; treating as no funding")
        return []

    # Normalize to flat records: [{time_ms, coin, usdc_signed}, ...]
    out = []
    for e in entries:
        t = int(e.get("time", 0))
        d = e.get("delta") or {}
        if not isinstance(d, dict):
            continue
        if d.get("type") != "funding":
            continue
        coin = d.get("coin")
        usdc_str = d.get("usdc")
        if coin is None or usdc_str is None:
            continue
        try:
            usdc = float(usdc_str)
        except (TypeError, ValueError):
            continue
        out.append({"time_ms": t, "coin": coin, "usdc_signed": usdc})
    out.sort(key=lambda r: r["time_ms"])

    # Cache to disk
    try:
        if out:
            pd.DataFrame(out).to_parquet(cache_file)
        else:
            # Sentinel empty cache so we don't re-fetch
            pd.DataFrame(columns=["time_ms", "coin", "usdc_signed"]).to_parquet(cache_file)
    except Exception as e:
        logger.warning(f"funding cache write failed {cache_file.name}: {e}")

    return out


def _funding_for_journey(
    funding_events: list[dict],
    coin: str,
    entry_ts: int,
    exit_ts: int,
) -> float:
    """Sum funding usdc for journey's (coin, (entry, exit]) half-open interval.

    Half-open per codex r16 #6: funding event timestamps are hourly so
    avoiding double-counting at boundaries with REVERSE fills.

    Returns positive = source received, negative = paid.
    """
    if not funding_events or not coin:
        return 0.0
    total = 0.0
    for e in funding_events:
        if e["coin"] != coin:
            continue
        t = e["time_ms"]
        if t > entry_ts and t <= exit_ts:
            total += e["usdc_signed"]
    return total


# ---------------------------------------------------------------------------
# Loader for equity series
# ---------------------------------------------------------------------------

def _fetch_current_positions(wallets: list[str]) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
    """codex m02 r2 BLOCKER 1 alternative-C (2026-05-29): fetch CURRENT perp position state per wallet
    from HL info.user_state(). Returns ({wallet → {coin → signed_qty}}, {wallet → snapshot_ms}).
    Per-wallet HTTP; sequential with backoff. ~1s per wallet for 1K-wallet chunk = ~17min naive.
    Cached at app/data/v13/current_positions_cache/ keyed by wallet (refreshed daily).

    codex m02 r4 CODE-BUG 2 fix: cache MUST record per-wallet snapshot_ms (instant of fetch).
    Caller bounds post-window fill load to MIN(snapshot_ms) so we never subtract a fill that
    occurred AFTER the snapshot (the snapshot already reflects it; subtracting again is double-count).

    codex m02 r4 CODE-BUG 3 fix: distinguish UNKNOWN (fetch failure) from EMPTY (real no-positions).
    Cache requires explicit `__fetch_ok__: true`; otherwise re-fetch. On permanent failure, wallet
    is OMITTED from the returned dict → caller treats carry-in as INCOMPLETE for that wallet.
    """
    import requests
    import json
    import time as _t
    cache_dir = ROOT / "app" / "data" / "v13" / "current_positions_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    out: dict[str, dict[str, float]] = {}
    snapshot_ts_by_wallet: dict[str, int] = {}
    for wallet in wallets:
        cache_path = cache_dir / f"{wallet}_{today_str}.json"
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text())
                # Cache schema v2 (m02 r5 fix): snapshot_ms MUST be from HL server `time` field,
                # not local pre-request ts. Old v1 caches are invalidated.
                if (cached.get("__fetch_ok__") is True
                        and isinstance(cached.get("snapshot_ms"), int)
                        and cached.get("__schema__") == "v2_hl_server_time"):
                    out[wallet] = cached.get("positions", {})
                    snapshot_ts_by_wallet[wallet] = cached["snapshot_ms"]
                    continue
            except Exception:
                pass
        positions: dict[str, float] = {}
        fetch_ok = False
        snapshot_ms = 0
        for retry in range(3):
            try:
                # codex m02 r5 CODE-BUG fix: use AUTHORITATIVE server-side `time` from
                # clearinghouseState response (HL stamps the snapshot moment). Local pre-request
                # ts was unsafe: any fill landing on HL between request-start and the actual
                # server snapshot could be IN positions but EXCLUDED from post_window_fills
                # capped at the local ts → carry-in under-subtracted. Server `time` removes
                # this uncertainty band.
                r = requests.post(
                    "https://api.hyperliquid.xyz/info",
                    json={"type": "clearinghouseState", "user": wallet},
                    timeout=10,
                )
                if r.status_code == 429:
                    _t.sleep(2 ** retry); continue
                if r.status_code != 200:
                    break  # non-200 → fetch_ok stays False
                state = r.json()
                # Server-side snapshot timestamp (ms). Required by m02 r5 fix.
                snapshot_ms = int(state.get("time") or 0)
                if snapshot_ms <= 0:
                    # No server time → cannot safely bound post-window load. Fail this wallet.
                    logger.warning(f"clearinghouseState for {wallet[:10]} missing 'time' field; marking incomplete")
                    break
                for asset_pos in state.get("assetPositions", []):
                    pos = asset_pos.get("position", {})
                    coin = pos.get("coin")
                    szi = float(pos.get("szi", 0))
                    if coin and abs(szi) > EPS:
                        positions[coin] = szi
                fetch_ok = True
                break
            except Exception:
                _t.sleep(1)
        if fetch_ok:
            out[wallet] = positions
            snapshot_ts_by_wallet[wallet] = snapshot_ms
            try:
                cache_path.write_text(json.dumps({
                    "__fetch_ok__": True,
                    "__schema__": "v2_hl_server_time",
                    "positions": positions,
                    "snapshot_ms": snapshot_ms,
                    "ts": today_str,
                }))
            except Exception:
                pass
        else:
            # CRITICAL: do NOT cache empty on failure. Leave wallet missing from out → caller treats as "unknown".
            logger.warning(f"current-position fetch FAILED for {wallet[:10]}; will treat carry-in as INCOMPLETE for this wallet")
    return out, snapshot_ts_by_wallet


def _compute_carry_in_via_backwalk(
    fills: pd.DataFrame,
    wallets: list[str],
    current_positions: dict[str, dict[str, float]],
    post_window_fills: pd.DataFrame | None = None,   # NEW per codex m02 r3 CODE-BUG 1
    snapshot_ts_by_wallet: dict[str, int] | None = None,   # NEW per codex m02 r6 CODE-BUG
) -> dict[tuple[str, str], tuple[float, float, str]]:
    """codex m02 r2 BLOCKER 1 alternative-C + codex m02 r3 CODE-BUGS 1+2 corrected:
    derive carry-in by walking BACKWARD from CURRENT position state.

    position_at_window_start[wallet, coin] = current_qty
                                              - sum(signed_sz of in-window fills)
                                              - sum(signed_sz of POST-window fills, if any)

    Post-window fills (between window_end and "now" when current_positions was fetched)
    are needed because current_qty reflects them, but our in-window fills exclude them.
    Without subtracting, position_at_start would be off by the post-window net.

    SPOT FILTER (codex m02 r3 CODE-BUG 2): _fetch_current_positions reads PERP only via
    clearinghouseState. Spot coins (`@` prefix, dir=Buy/Sell) have current_qty=0 by definition
    in our query. Iterating over them with nonzero in-window net would fabricate opposite
    carry-in. Filter them OUT — spot is not tracked by this carry-in mechanism (it's also
    excluded from journey trace main loop via coin_is_spot).

    Returns {(wallet, coin): (carry_in_position, carry_in_cost_basis, carry_in_status)}.

    carry_in_status:
        "carry_resolved"   wallet had position before window_start (verified)
        "no_carry"         wallet was flat at window_start (current - all_fills == 0)
    """
    def _is_spot_coin(c: str) -> bool:
        # HL spot coin encoding: @ prefix. Also coin == "USDC" treated as cash, not perp.
        return c.startswith("@") or c == "USDC"

    out: dict[tuple[str, str], tuple[float, float, str]] = {}
    wallets_set = set(wallets)

    # In-window net per (wallet, coin)
    fills_w = fills[fills["wallet"].isin(wallets_set)].copy()
    # Filter spot OUT (codex m02 r3 CODE-BUG 2)
    fills_w = fills_w[~fills_w["coin"].apply(_is_spot_coin)]

    # codex m02 r7 CODE-BUG-CRITICAL fix: PRECOMPUTE coins-per-wallet from UNFILTERED fills_w
    # BEFORE applying snapshot bound. The r6 fix dropped all fills from snapshot-failed wallets,
    # which broke downstream incomplete propagation (which derived coin set from in_window_net).
    # codex m02 r8 PERF: single groupby instead of per-wallet loc scan (O(F) vs O(W*F)).
    coins_by_wallet_all: dict[str, set[str]] = {
        w: set(g["coin"].unique().tolist())
        for w, g in fills_w.groupby("wallet", sort=False)
    }
    # Ensure every requested wallet has an entry (empty set if no fills loaded).
    for wallet in wallets:
        coins_by_wallet_all.setdefault(wallet, set())

    # codex m02 r6 CODE-BUG fix: BOUND in_window_net to per-wallet snapshot_ms. current_qty
    # reflects state AT snapshot_ms; fills with time > snapshot_ms are NOT in current_qty
    # (they happened after) → subtracting them from current_qty would OVER-subtract and
    # fabricate negative carry-in. Only fills with time <= snapshot_ms contribute to the
    # backwalk arithmetic.
    if snapshot_ts_by_wallet:
        snapshot_series = fills_w["wallet"].map(snapshot_ts_by_wallet)
        # Wallets without a snapshot_ts (fetch failed) are handled separately as incomplete;
        # their fills get dropped from in_window_net here (mapped to -1 → filtered out).
        carry_mask = fills_w["time"].astype("int64") <= snapshot_series.fillna(-1).astype("int64")
        pre_n = len(fills_w)
        fills_w_carry = fills_w[carry_mask].reset_index(drop=True)
        if pre_n != len(fills_w_carry):
            logger.info(
                f"  in_window snapshot filter: dropped {pre_n - len(fills_w_carry):,} fills past per-wallet snapshot"
            )
    else:
        fills_w_carry = fills_w
    fills_w_carry["signed_sz"] = fills_w_carry.apply(
        lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1
    )
    in_window_net = fills_w_carry.groupby(["wallet", "coin"])["signed_sz"].sum().to_dict()
    # First-fill price still uses the full fills_w (snapshot bound is for arithmetic only;
    # first-fill price is a seed for cost-basis, valid even if the fill is past snapshot —
    # though practically first fills are window-start, well before snapshot).

    # First-fill price per (wallet, coin) for cost-basis seed
    fills_w_sorted = fills_w.sort_values(["wallet", "coin", "time"], kind="stable")
    first_prices = fills_w_sorted.groupby(["wallet", "coin"]).first()["price"].to_dict()

    # POST-window net (codex m02 r3 CODE-BUG 1) — already snapshot-bounded by caller
    post_window_net = {}
    if post_window_fills is not None and not post_window_fills.empty:
        pwf = post_window_fills[post_window_fills["wallet"].isin(wallets_set)].copy()
        pwf = pwf[~pwf["coin"].apply(_is_spot_coin)]
        # Caller already bounded post_window_fills to per-wallet snapshot_ms; no additional filter.
        pwf["signed_sz"] = pwf.apply(
            lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1
        )
        post_window_net = pwf.groupby(["wallet", "coin"])["signed_sz"].sum().to_dict()

    for wallet in wallets:
        # codex m02 r4 CODE-BUG 3 fix + r7 regression fix: if wallet missing from current_positions,
        # fetch FAILED. Mark ALL pairs for this wallet as INCOMPLETE so downstream excludes them.
        # MUST use coins_by_wallet_all (precomputed from UNFILTERED fills) because r6 snapshot bound
        # drops failed-wallet fills from in_window_net.
        if wallet not in current_positions:
            coins_in_fills = coins_by_wallet_all.get(wallet, set())
            for coin in coins_in_fills:
                if not _is_spot_coin(coin):
                    out[(wallet, coin)] = (0.0, 0.0, "incomplete")
            continue
        current = current_positions[wallet]
        # Coins to consider: any with current_position OR any with in-window net OR any with post-window net
        coins = set(current.keys()) | {c for (w, c) in in_window_net if w == wallet} | \
                {c for (w, c) in post_window_net if w == wallet}
        coins = {c for c in coins if not _is_spot_coin(c)}
        for coin in coins:
            current_qty = current.get(coin, 0.0)
            net_in_window = in_window_net.get((wallet, coin), 0.0)
            net_post_window = post_window_net.get((wallet, coin), 0.0)
            position_at_start = current_qty - net_in_window - net_post_window
            if abs(position_at_start) < EPS:
                out[(wallet, coin)] = (0.0, 0.0, "no_carry")
            else:
                cost_basis_seed = float(first_prices.get((wallet, coin), 0.0))
                out[(wallet, coin)] = (position_at_start, cost_basis_seed, "carry_resolved")
    return out


def load_equity_series(path: Path | None) -> dict:
    """Returns {wallet_lower: spot_usdc_today}.

    Per Alberto rule 16 + decision A on 2026-05-26 17:09 CEST:
    `max_position_pct_equity` uses SPOT USDC (the wallet's HL equity per rule
    16), NOT the daily perp account value series. spot_usdc_today is a
    wallet-level scalar attached to every row of the upstream equity parquet
    (constant per wallet) — we collapse to {wallet: scalar} for lookup
    efficiency.

    Backwards-compat: if the file has only the legacy `equity_usd` column (no
    `spot_usdc_today`), raise — it means the upstream rebuild has not been
    re-run with the corrected anchor. Downstream must regenerate the equity
    parquet via the v3-A reconstruction script before proceeding.
    """
    if path is None or not path.exists():
        return {}
    eq = pd.read_parquet(path)
    if "wallet" not in eq.columns:
        raise ValueError(f"equity series at {path} missing 'wallet' column")
    if "spot_usdc_today" not in eq.columns:
        raise ValueError(
            f"equity series at {path} missing 'spot_usdc_today' column. "
            f"This script requires the v3-A reconstruction output (Alberto rule 16 + "
            f"decision A on 2026-05-26). Re-run scripts/v13_equity_reconstruct.py with "
            f"the current code, then re-run this script."
        )
    eq["wallet"] = eq["wallet"].str.lower()
    # spot_usdc_today is constant per wallet (attached at bucket loop in the
    # reconstruction script). Take any one value per wallet (first non-null).
    return (
        eq.dropna(subset=["spot_usdc_today"])
          .groupby("wallet")["spot_usdc_today"]
          .first()
          .astype(float)
          .to_dict()
    )


# ---------------------------------------------------------------------------
# Journey classifier
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Per-pair journey tracer
# ---------------------------------------------------------------------------

def trace_journeys_for_pair(
    wallet: str,
    coin: str,
    fills: pd.DataFrame,                          # pre-sorted, single (wallet, coin), in-window only
    equity_lookup: dict,
    carry_in_position: float,
    carry_in_cost_basis: float,
    carry_in_status: str,
    window_start_date,
    window_end_date,
    funding_events: list | None = None,          # per-wallet funding events from _fetch_funding_for_wallet
) -> list[dict]:
    """Walk fills + emit journey records.

    The carry-in seed determines the starting position. If carry_in_status is
    "incomplete", journeys touching the carry-in leg are flagged and any
    journey that closes within the window is emitted with the incomplete flag
    so downstream metrics can exclude them.
    """
    journeys: list[dict] = []
    journey_id = 0
    if funding_events is None:
        funding_events = []

    # Initial state seeded from carry-in.
    position = carry_in_position
    cost_basis = carry_in_cost_basis    # used for cost-basis-aware realized PnL (we still
                                        # also accept HL's closedPnl as the ground truth where present)

    # Journey-open state (may start non-empty if carry-in seed is non-zero).
    if abs(position) > EPS:
        journey_id += 1
        open_ts = int(datetime(window_start_date.year, window_start_date.month, window_start_date.day, tzinfo=timezone.utc).timestamp() * 1000)
        open_side = 1 if position > 0 else -1
        max_notional = abs(position) * cost_basis if cost_basis > 0 else 0.0
        peak_ts = open_ts
        n_entry = 0           # the carry-in opening fills happened pre-window; we don't count them
        n_carry_in = 1
        n_addon = 0
        n_trim = 0
        n_exit = 0
        n_reverse = 0
        realized_pnl = 0.0
        fees_paid_usd = 0.0
        # codex m02 r2 MED 8 fix (2026-05-29): infer fee_scope from coin, NOT default to standard_perp.
        # A carry-in for xyz: prefix is builder_perp; @ prefix coin is spot (but spot wouldn't make it
        # to journey trace per coin_is_spot exclusion); otherwise standard_perp.
        if coin.startswith("xyz:"):
            fee_scope = "builder_perp"
        elif coin.startswith("@"):
            fee_scope = "spot"            # defensive — should not reach here per spot exclusion
        else:
            fee_scope = "standard_perp"
        addon_times: list[int] = []
        trim_times: list[int] = []
        # carry-in incompleteness propagates to all carry-in-touching journeys.
        carry_taint = (carry_in_status == "incomplete")
    else:
        open_ts = None
        open_side = None
        n_entry = n_addon = n_trim = n_exit = n_reverse = n_carry_in = 0
        max_notional = 0.0
        peak_ts = None
        realized_pnl = 0.0
        fees_paid_usd = 0.0
        fee_scope = "standard_perp"
        addon_times = []
        trim_times = []
        carry_taint = False

    def _finalize_journey(close_ts: int) -> dict | None:
        if open_ts is None or open_side is None:
            return None
        duration_s = max(0, (close_ts - open_ts) / 1000)
        ad_gaps = np.diff(addon_times) / 1000 if len(addon_times) >= 2 else np.array([])
        tr_gaps = np.diff(trim_times) / 1000 if len(trim_times) >= 2 else np.array([])
        max_notional_v = max(max_notional, EPS)
        pnl_bps = (realized_pnl / max_notional_v) * 10000

        # max_position_pct_equity per Alberto decision A (2026-05-26 17:09):
        # equity = wallet's SPOT USDC TODAY (rule 16). Wallet-level scalar
        # (NOT per-date) because we don't reconstruct historical spot. This
        # measures "fraction of current dry powder a typical position would
        # consume" — a copy-targeting signal, not a historical sizing record.
        # Wallets with $0 spot today produce max_pct = None (no dry powder).
        eq = equity_lookup.get(wallet)
        max_pct = (max_notional_v / eq) if (eq and eq > 0) else None

        # B2 funding: sum funding events for journey's (coin, (entry, exit])
        # NOTE: spot/dust scopes have no perp funding by definition, set to 0
        funding_net_usd = 0.0
        if fee_scope not in ("spot", "other"):
            funding_net_usd = _funding_for_journey(
                funding_events, coin, open_ts, close_ts,
            )

        # B3: net realized pnl = realized - fees + funding_net (positive = received)
        net_realized_pnl_usd = realized_pnl - fees_paid_usd + funding_net_usd

        return {
            "wallet": wallet,
            "coin": coin,
            "journey_id": journey_id,
            "side": "long" if open_side > 0 else "short",
            "entry_ts": open_ts,
            "exit_ts": close_ts,
            "peak_ts": peak_ts,
            "duration_hours": duration_s / 3600.0,
            "n_entry_fills": n_entry,
            "n_carry_in_seeds": n_carry_in,
            "n_addon_fills": n_addon,
            "n_trim_fills": n_trim,
            "n_exit_fills": n_exit,
            "n_reverse_fills": n_reverse,
            "n_fills_total": n_entry + n_carry_in + n_addon + n_trim + n_exit + n_reverse,
            "max_position_notional_usd": max_notional_v,
            "max_position_pct_equity": max_pct,
            "avg_seconds_between_addons": float(ad_gaps.mean()) if ad_gaps.size else None,
            "avg_seconds_between_trims": float(tr_gaps.mean()) if tr_gaps.size else None,
            "realized_pnl_usd": realized_pnl,
            # B1+B2+B3 cost columns:
            "fees_paid_usd": fees_paid_usd,
            "funding_net_usd": funding_net_usd,
            "net_realized_pnl_usd": net_realized_pnl_usd,
            "fee_assumption_scope": fee_scope,
            "pnl_bps_of_max": pnl_bps,
            "journey_class": _classify_duration(duration_s, n_addon, n_trim),
            "carry_in_status": "incomplete" if carry_taint else "ok",
        }

    # Use itertuples for speed (avoid iterrows boxing).
    has_closed_pnl_col = "closedPnl" in fills.columns
    for row in fills.itertuples(index=False):
        size = float(row.size)
        if size <= EPS:
            continue
        side = row.side
        signed = size if side == "B" else (-size if side == "A" else 0.0)
        if signed == 0.0:
            continue
        price = float(row.price) or 0.0
        ts = int(row.time)
        raw_pnl = getattr(row, "closedPnl", None) if has_closed_pnl_col else None
        # closedPnl fallback: cost-basis-derived realized for trim/exit/reverse.
        # Compute fallback BEFORE we apply the fill (we still know position+cost_basis here).
        if raw_pnl is None or (isinstance(raw_pnl, float) and (raw_pnl != raw_pnl)):  # NaN check
            # signed_size is opposite to position for trim/exit; size of the closed portion
            # in coin units depends on position vs new_pos.
            new_pos_preview = position + signed
            if (position > 0 and signed < 0) or (position < 0 and signed > 0):
                if abs(new_pos_preview) < EPS:
                    closed_qty = position                       # signed; full close
                elif (position > 0 and new_pos_preview > 0) or (position < 0 and new_pos_preview < 0):
                    closed_qty = -signed                        # trim partial
                else:
                    closed_qty = position                       # reverse: closes whole old leg
                closed_pnl_fallback = (price - cost_basis) * closed_qty
                closed_pnl = float(closed_pnl_fallback)
            else:
                closed_pnl = 0.0
        else:
            closed_pnl = float(raw_pnl)

        # PRE-fill notional capture (the peak of the journey can occur at any
        # fill point, INCLUDING the moment just before a trim/exit/reverse).
        pre_notional = abs(position) * price
        if abs(position) > EPS and pre_notional > max_notional:
            max_notional = pre_notional
            peak_ts = ts

        new_pos = position + signed
        notional_after = abs(new_pos) * price

        # B1 fee: per-fill fee for this row.
        # For non-REVERSE: full fill belongs to current journey.
        # For REVERSE: split below (closing_fee + opening_fee).
        fill_notional = abs(signed) * price
        # codex m02 r2 BLOCKER 2 fix (2026-05-29): prefer actual v2-fills fee fields
        # over flat 4.32 bps estimate. Falls back to estimator if v2 fields absent.
        actual_fee = _fill_fee_usd_actual(row)
        fill_fee = actual_fee if actual_fee is not None else _fill_fee_usd(fill_notional)
        # Classify fee_scope from this row's dir/coin (codex r16 #7).
        row_dir = getattr(row, "dir", None) or ""
        row_scope = _classify_fee_scope(row_dir, coin)

        if abs(position) < EPS and abs(new_pos) > EPS:
            # ENTRY
            journey_id += 1
            open_ts = ts
            open_side = 1 if new_pos > 0 else -1
            n_entry = 1
            n_carry_in = 0
            n_addon = n_trim = n_exit = n_reverse = 0
            realized_pnl = 0.0
            max_notional = notional_after
            peak_ts = ts
            addon_times = []
            trim_times = []
            cost_basis = price
            carry_taint = False
            # B1: ENTRY fee fully belongs to new journey
            fees_paid_usd = fill_fee
            fee_scope = row_scope
        elif (position > 0 and signed > 0) or (position < 0 and signed < 0):
            # ADDON: same direction, position grows.
            n_addon += 1
            cost_basis = (cost_basis * abs(position) + price * abs(signed)) / abs(new_pos)
            if notional_after > max_notional:
                max_notional = notional_after
                peak_ts = ts
            addon_times.append(ts)
            # B1: ADDON fee belongs to current journey
            fees_paid_usd += fill_fee
        elif (position > 0 and abs(new_pos) < EPS) or (position < 0 and abs(new_pos) < EPS):
            # EXACT EXIT
            n_exit += 1
            realized_pnl += closed_pnl
            # B1: EXIT fee belongs to current (closing) journey
            fees_paid_usd += fill_fee
            j = _finalize_journey(ts)
            if j is not None:
                journeys.append(j)
            open_ts = None
            open_side = None
            cost_basis = 0.0
            fees_paid_usd = 0.0
            fee_scope = "standard_perp"
        elif (position > 0 and new_pos > 0 and signed < 0) or (position < 0 and new_pos < 0 and signed > 0):
            # TRIM
            n_trim += 1
            realized_pnl += closed_pnl
            trim_times.append(ts)
            # cost basis unchanged for the remaining portion
            # B1: TRIM fee belongs to current journey
            fees_paid_usd += fill_fee
        elif (position > 0 and new_pos < 0) or (position < 0 and new_pos > 0):
            # REVERSE: codex r16 #6 - split fee between closing leg and new (opening) leg
            # codex m02 r2 BLOCKER 2 fix (2026-05-29): use actual v2 fees with notional split.
            # Total fill notional = |signed| × price = (|position| + |new_pos|) × price (since signed crosses zero).
            # Split actual_fee proportionally: closing_fee = actual_fee × |position| / (|position| + |new_pos|).
            n_reverse += 1
            realized_pnl += closed_pnl
            split_total_notional = (abs(position) + abs(new_pos))
            if actual_fee is not None and split_total_notional > EPS:
                closing_fee = actual_fee * (abs(position) / split_total_notional)
                opening_fee = actual_fee * (abs(new_pos) / split_total_notional)
            else:
                # Fallback to estimator (FEE_RATE flat) per leg
                closing_fee = abs(position) * price * FEE_RATE
                opening_fee = abs(new_pos) * price * FEE_RATE
            fees_paid_usd += closing_fee
            j = _finalize_journey(ts)
            if j is not None:
                journeys.append(j)
            journey_id += 1
            open_ts = ts
            open_side = 1 if new_pos > 0 else -1
            n_entry = 0
            n_carry_in = 0
            n_addon = 0
            n_trim = 0
            n_exit = 0
            n_reverse = 1
            realized_pnl = 0.0
            max_notional = abs(new_pos) * price
            peak_ts = ts
            cost_basis = price
            addon_times = []
            trim_times = []
            carry_taint = False
            # B1: new leg starts with opening_fee accumulated
            fees_paid_usd = opening_fee
            fee_scope = row_scope

        position = new_pos

    # End of fills. If we end with an open position the journey is incomplete
    # in the future; do not emit it for v1 (matches spec: only closed-in-window
    # journeys count).

    return journeys


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--wallets", help="Optional newline wallet filter")
    ap.add_argument("--equity-series", required=True, help="wallet_equity_series.parquet")
    ap.add_argument("--walkback-days", type=int, default=90)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument(
        "--checkpoint-every",
        type=int,
        default=500,
        help="Flush partial parquet every N completed wallets (default 500). "
             "0 disables checkpointing.",
    )
    ap.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore any existing <output>.done_wallets.txt + .partial.parquet; "
             "start fresh.",
    )
    ap.add_argument(
        "--use-current-state-anchor",
        action="store_true",
        default=True,
        help="codex m02 r2 alternative-C (2026-05-29): derive carry-in by walking BACKWARD from "
             "current HL position state through in-window fills. No pre-window data dependency. "
             "Replaces pre-window walkback (which always returned 0 because archive starts "
             "2025-12-01). Default True. Pass --no-use-current-state-anchor to disable.",
    )
    ap.add_argument(
        "--no-use-current-state-anchor",
        dest="use_current_state_anchor",
        action="store_false",
        help="Disable current-state anchor; revert to pre-window walkback (only useful if you've "
             "extended the S3 archive to cover pre-window dates).",
    )
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    wallets_filter = None
    if args.wallets:
        with open(args.wallets) as f:
            wallets_filter = {w.strip().lower() for w in f if w.strip()}

    # ------------------------------------------------------------------
    # Checkpoint / resume (OOM hardening, 2026-05-29)
    # ------------------------------------------------------------------
    out_path = Path(args.output)
    partial_path = out_path.with_suffix(out_path.suffix + ".partial")
    done_wallets_path = out_path.with_suffix(out_path.suffix + ".done_wallets.txt")

    resumed_journeys: list[dict] = []
    done_wallets: set[str] = set()
    if not args.no_resume and done_wallets_path.exists() and partial_path.exists():
        try:
            with open(done_wallets_path) as f:
                done_wallets = {w.strip().lower() for w in f if w.strip()}
            partial_df = pd.read_parquet(partial_path)
            resumed_journeys = partial_df.to_dict("records")
            logger.info(
                f"RESUME: loaded {len(resumed_journeys):,} journeys for "
                f"{len(done_wallets):,} already-done wallets from {partial_path}"
            )
            if wallets_filter is None:
                # Need to know the universe to skip done ones; if no filter given,
                # we don't know yet; we'll handle skipping post-fill-load.
                pass
            else:
                # codex m02 r3 CODE-BUG 4 fix: detect "all done" BEFORE filter empties wallets list.
                # If supplied wallets are all in done_wallets, partial is the COMPLETE output; promote.
                supplied_wallets_lc = wallets_filter  # before filtering
                all_done = supplied_wallets_lc.issubset(done_wallets)
                wallets_filter = {w for w in wallets_filter if w not in done_wallets}
                if all_done:
                    logger.warning(
                        f"RESUME TERMINAL: all {len(supplied_wallets_lc):,} wallets in done list; "
                        f"promoting partial ({len(resumed_journeys):,} journeys) → final and exiting."
                    )
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    tmp_final = out_path.with_suffix(out_path.suffix + ".final.tmp")
                    pd.DataFrame(resumed_journeys).to_parquet(tmp_final, index=False, compression="snappy")
                    tmp_final.replace(out_path)
                    if partial_path.exists(): partial_path.unlink()
                    if done_wallets_path.exists(): done_wallets_path.unlink()
                    return
                logger.info(
                    f"RESUME: reduced wallet filter to {len(wallets_filter):,} "
                    f"(skipped {len(done_wallets):,} done)"
                )
        except Exception as e:
            logger.error(f"RESUME failed reading {partial_path}/{done_wallets_path}: {e}")
            logger.error("Refusing to start fresh on top of broken checkpoint. "
                         "Either fix files or pass --no-resume.")
            return
    elif not args.no_resume and (done_wallets_path.exists() ^ partial_path.exists()):
        logger.error(
            f"CHECKPOINT INCONSISTENT: one of {partial_path}, {done_wallets_path} "
            f"exists but not both. Refusing to proceed. Delete both or pass --no-resume."
        )
        return

    logger.info(f"Loading fills {start.date()} to {end.date()}")
    fills = load_fills_for_dates(start, end, wallets_filter)
    if fills.empty:
        logger.error("No in-window fills loaded.")
        return
    fills = validate_and_normalize_fills(fills)
    logger.info(f"Loaded + validated {len(fills):,} in-window fills")

    # Universe of wallets to process.
    if wallets_filter is not None:
        wallets = sorted(wallets_filter)
    else:
        wallets = sorted(fills["wallet"].unique().tolist())

    # If resume and no explicit filter, skip done wallets from the discovered
    # universe (the load above already paid the RAM cost; we still save compute).
    if done_wallets and wallets_filter is None:
        before = len(wallets)
        wallets = [w for w in wallets if w not in done_wallets]
        logger.info(
            f"RESUME: skipping {before - len(wallets):,} done wallets in discovered universe"
        )
        # Also drop their fills so the loop ignores them.
        fills = fills[~fills["wallet"].isin(done_wallets)].reset_index(drop=True)

    # codex m02 r2 BLOCKER 1 fix (2026-05-29) ALTERNATIVE-C approach: rather than walking
    # back through pre-window fills (which our S3 archive lacks — starts 2025-12-01), we
    # derive carry-in by walking BACKWARD from CURRENT position state through in-window
    # fills. position_at_window_start = current_position - sum(signed_sz of in-window fills).
    # This requires:
    #   - HL info.user_state(wallet) call per wallet (single HTTP per wallet)
    #   - In-window fills already loaded (we have them)
    # No pre-window data dependency.
    if args.use_current_state_anchor:
        logger.info(f"Using current-state anchor for carry-in (walks backward through in-window fills)...")
        current_positions_by_wallet, snapshot_ts_by_wallet = _fetch_current_positions(wallets)
        # codex m02 r3 CODE-BUG 1 fix + r4 CODE-BUG 1: load POST-window fills strictly AFTER end day.
        # load_fills_for_dates is inclusive [t0, t1], so passing (end, today) double-counts end-day
        # fills (they appear in both in_window AND post_window). Start at end + 1 day.
        # codex m02 r4 CODE-BUG 2 fix: BOUND post-window load to MIN(snapshot_ms) across wallets
        # so we never subtract a fill that occurred AFTER the snapshot (snapshot already reflects it).
        today = datetime.now(timezone.utc)
        post_window_start = end + timedelta(days=1)
        if snapshot_ts_by_wallet:
            min_snapshot_ms = min(snapshot_ts_by_wallet.values())
            anchor_dt = datetime.fromtimestamp(min_snapshot_ms / 1000, tz=timezone.utc)
            post_window_end = min(today, anchor_dt)
        else:
            post_window_end = today
            logger.warning("No snapshot timestamps captured; using `today` as post-window upper bound (LESS SAFE)")
        if post_window_end >= post_window_start:
            logger.info(f"Loading post-window fills {post_window_start.date()} → {post_window_end.date()} (snapshot-bounded) for carry-in correction...")
            post_window_fills = load_fills_for_dates(post_window_start, post_window_end, set(wallets))
            if not post_window_fills.empty:
                post_window_fills = validate_and_normalize_fills(post_window_fills)
                # Also strict-filter by per-wallet snapshot_ms (more precise than the loose min-snapshot bound).
                if snapshot_ts_by_wallet:
                    snapshot_series = post_window_fills["wallet"].map(snapshot_ts_by_wallet)
                    # Drop fills past per-wallet snapshot; also drop fills for wallets with no snapshot.
                    keep_mask = post_window_fills["time"].astype("int64") <= snapshot_series.fillna(-1).astype("int64")
                    pre_n = len(post_window_fills)
                    post_window_fills = post_window_fills[keep_mask].reset_index(drop=True)
                    if pre_n != len(post_window_fills):
                        logger.info(f"  dropped {pre_n - len(post_window_fills):,} fills past per-wallet snapshot")
                logger.info(f"  loaded {len(post_window_fills):,} post-window fills (bounded)")
        else:
            post_window_fills = pd.DataFrame()
        # Compute carry-in per (wallet, coin) by walking backward (snapshot-bounded — m02 r6 fix)
        carry_in_by_wallet_coin = _compute_carry_in_via_backwalk(
            fills, wallets, current_positions_by_wallet,
            post_window_fills=post_window_fills,
            snapshot_ts_by_wallet=snapshot_ts_by_wallet,
        )
        prior_fills = pd.DataFrame()  # not used in this path
        prior_by_wallet = {}
        logger.info(
            f"Carry-in derived for {len(carry_in_by_wallet_coin):,} (wallet, coin) pairs via current-state anchor"
        )
    else:
        logger.info(f"Bulk-loading prior fills for carry-in walkback ({args.walkback_days} days)...")
        prior_fills = load_prior_fills_for_wallets(set(wallets), FILLS_DIR, start, max_walkback_days=args.walkback_days)
        logger.info(f"Bulk-loaded {len(prior_fills):,} prior fills")
        carry_in_by_wallet_coin = None

    equity_lookup = load_equity_series(Path(args.equity_series))
    logger.info(f"Loaded {len(equity_lookup):,} equity-series rows")

    # Sort once with deterministic tie-breakers.
    sort_keys = ["wallet", "coin", "time", "side", "price", "size"]
    if "hash" in fills.columns:
        sort_keys = ["wallet", "coin", "time", "hash", "side", "price", "size"]
    fills = fills.sort_values(sort_keys, kind="stable").reset_index(drop=True)

    # Pre-group prior fills by wallet for O(1) lookup per pair.
    prior_by_wallet: dict = {}
    if not prior_fills.empty:
        for w, grp in prior_fills.groupby("wallet", sort=False):
            prior_by_wallet[w] = grp

    # B2 funding: fetch per-wallet funding events ONCE (cached on disk).
    # Compute window in ms for the funding API call.
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    logger.info(f"Fetching funding events per wallet (disk-cached at {FUNDING_CACHE_DIR})...")

    # Seed with resumed journeys (if any) so the final write is the full set.
    # codex m02 r4 CODE-BUG 4 fix: dedup the seed by (wallet, coin, journey_id).
    # If we crashed AFTER partial parquet rename but BEFORE done_wallets rename, the wallet's
    # journeys exist in partial yet wallet is missing from done. On restart, fills for that
    # wallet are re-processed and re-extended into all_journeys → duplicates. Dedup at seed time
    # keeps the FIRST occurrence (which is the prior-flushed version); the same wallet, if
    # re-processed below, will produce identical journey_ids that we drop on a second dedup pass
    # at the end before writing the final parquet (belt-and-suspenders).
    seen_keys: set[tuple[str, str, int]] = set()
    all_journeys: list[dict] = []
    n_dups_seed = 0
    for j in resumed_journeys:
        k = (str(j.get("wallet", "")).lower(), str(j.get("coin", "")), int(j.get("journey_id", -1)))
        if k in seen_keys:
            n_dups_seed += 1
            continue
        seen_keys.add(k)
        all_journeys.append(j)
    if n_dups_seed:
        logger.warning(
            f"RESUME DEDUP: dropped {n_dups_seed:,} duplicate journeys from partial parquet seed"
        )

    # codex m02 r4 CODE-BUG 5 fix: filter SPOT coins from the journey trace main loop.
    # Spot coins (@-prefix or "USDC") are NOT tracked in clearinghouseState (perps only) and
    # would otherwise be processed with carry_in=0, fabricating fake short journeys when a spot
    # sell from pre-window inventory shows up in in-window fills. Carry-in path already filters
    # spot; main loop must match. Excluded coins are out-of-scope for V13 (perp copy strategy).
    def _coin_is_spot(c: str) -> bool:
        return isinstance(c, str) and (c.startswith("@") or c == "USDC")
    pre_filter_n = len(fills)
    spot_mask = fills["coin"].apply(_coin_is_spot)
    if spot_mask.any():
        n_spot = int(spot_mask.sum())
        n_spot_wallets = int(fills.loc[spot_mask, "wallet"].nunique())
        n_spot_coins = int(fills.loc[spot_mask, "coin"].nunique())
        fills = fills[~spot_mask].reset_index(drop=True)
        logger.info(
            f"SPOT FILTER: dropped {n_spot:,} spot fills "
            f"({n_spot_wallets:,} wallets × {n_spot_coins:,} coins); "
            f"{pre_filter_n - n_spot:,} perp fills remain"
        )

    pair_groups = fills.groupby(["wallet", "coin"], sort=False)
    n_pairs = pair_groups.ngroups
    logger.info(
        f"Tracing journeys across {n_pairs:,} (wallet,coin) pairs "
        f"(checkpoint_every={args.checkpoint_every} wallets, "
        f"resumed={len(resumed_journeys):,} journeys)..."
    )

    processed = 0
    carry_in_incomplete_count = 0
    incomplete_journeys_excluded = 0
    funding_cache_per_wallet: dict[str, list] = {}
    # Track wallet completion for checkpointing.
    current_wallet: str | None = None
    new_done_wallets: list[str] = []
    journeys_at_checkpoint_start = len(all_journeys)

    def _flush_checkpoint(reason: str) -> None:
        nonlocal journeys_at_checkpoint_start
        if args.checkpoint_every <= 0:
            return
        if not new_done_wallets:
            return
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # codex m02 r3 CODE-BUG 3 fix (2026-05-29): atomic pair via .tmp + .tmp.done + double rename.
        # We collect ALL done wallets (prior + new) into a fresh tmp file, write fresh tmp partial,
        # then rename both. If crash between renames: the prior partial+done pair is still consistent.
        tmp_partial = partial_path.with_suffix(partial_path.suffix + ".tmp")
        tmp_done = done_wallets_path.with_suffix(done_wallets_path.suffix + ".tmp")
        # Write fresh done_wallets file with prior content + new
        prior_done: list[str] = []
        if done_wallets_path.exists():
            with open(done_wallets_path) as _f:
                prior_done = [l.strip() for l in _f if l.strip()]
        with open(tmp_done, "w") as _f:
            for w in prior_done:
                _f.write(f"{w}\n")
            for w in new_done_wallets:
                _f.write(f"{w}\n")
        # Write fresh tmp partial
        pd.DataFrame(all_journeys).to_parquet(
            tmp_partial, index=False, compression="snappy"
        )
        # Atomic rename: partial FIRST (consumers that resume read both; if we crash here,
        # done_wallets is stale-low, resume re-processes some wallets — duplicate-prevention
        # via dedup at the all_journeys side rather than missing data).
        tmp_partial.replace(partial_path)
        tmp_done.replace(done_wallets_path)
        added_journeys = len(all_journeys) - journeys_at_checkpoint_start
        logger.info(
            f"CHECKPOINT [{reason}]: {len(new_done_wallets)} new wallets, "
            f"+{added_journeys:,} journeys → {partial_path.name} "
            f"({len(all_journeys):,} total)"
        )
        new_done_wallets.clear()
        journeys_at_checkpoint_start = len(all_journeys)

    for (wallet, coin), grp in pair_groups:
        # Wallet boundary: mark previous wallet as done; checkpoint if needed.
        if current_wallet is not None and wallet != current_wallet:
            new_done_wallets.append(current_wallet)
            if (args.checkpoint_every > 0
                    and len(new_done_wallets) >= args.checkpoint_every):
                _flush_checkpoint(f"every {args.checkpoint_every} wallets")
        current_wallet = wallet
        # codex m02 r2 BLOCKER 1 alternative-C (2026-05-29): use current-state anchor when enabled
        if args.use_current_state_anchor and carry_in_by_wallet_coin is not None:
            pos_in, cb_in, ci_status = carry_in_by_wallet_coin.get((wallet, coin), (0.0, 0.0, "no_carry"))
        else:
            prior_for_wallet = prior_by_wallet.get(wallet, pd.DataFrame())
            pos_in, cb_in, ci_status = find_carry_in_state_from_prior(wallet, coin, prior_for_wallet)
        if ci_status == "incomplete":
            carry_in_incomplete_count += 1
        # Fetch funding once per wallet (memoized across coins of the same wallet)
        if wallet not in funding_cache_per_wallet:
            funding_cache_per_wallet[wallet] = _fetch_funding_for_wallet(
                wallet, start_ms, end_ms,
            )
        try:
            js = trace_journeys_for_pair(
                wallet, coin, grp, equity_lookup,
                pos_in, cb_in, ci_status,
                start.date(), end.date(),
                funding_events=funding_cache_per_wallet[wallet],
            )
        except Exception as e:
            logger.exception(f"trace failed for {wallet[:8]} / {coin}: {e}")
            js = []
        # Per remediation plan + R3 gotcha #3: incomplete carry-in journeys
        # are EXCLUDED from the output (not merely flagged). Downstream
        # consumers should never see them.
        if ci_status == "incomplete":
            # Drop journeys whose carry_in_status is incomplete.
            kept = [j for j in js if j.get("carry_in_status") != "incomplete"]
            incomplete_journeys_excluded += (len(js) - len(kept))
            js = kept
        # codex m02 r4 CODE-BUG 4 fix: per-pair dedup against the running seen_keys set
        # (catches duplicates between resumed seed and freshly-traced output).
        new_js = []
        for j in js:
            k = (str(j.get("wallet", "")).lower(), str(j.get("coin", "")), int(j.get("journey_id", -1)))
            if k in seen_keys:
                continue
            seen_keys.add(k)
            new_js.append(j)
        all_journeys.extend(new_js)
        processed += 1
        if processed % 5000 == 0:
            logger.info(f"  {processed:,}/{n_pairs:,} pairs, {len(all_journeys):,} journeys, carry_incomplete_pairs={carry_in_incomplete_count}, excluded_journeys={incomplete_journeys_excluded}")

    # Mark the last wallet as done.
    if current_wallet is not None:
        new_done_wallets.append(current_wallet)
    # Final flush (covers any remainder under checkpoint_every).
    if new_done_wallets and args.checkpoint_every > 0:
        _flush_checkpoint("final")

    # codex m02 r2 HIGH 4 fix (2026-05-29): handle resume terminal-state. If all wallets
    # were already done (checkpoint complete but final write failed), promote partial → final
    # without requiring any additional fills processing.
    if not all_journeys and partial_path.exists() and len(done_wallets) > 0:
        logger.warning(
            f"RESUME: all {len(done_wallets):,} wallets already in partial parquet; "
            f"promoting partial → final without re-tracing."
        )
        all_journeys = partial_df.to_dict("records") if 'partial_df' in dir() else []
        if not all_journeys:
            try:
                all_journeys = pd.read_parquet(partial_path).to_dict("records")
            except Exception as e:
                logger.error(f"Failed to re-load partial for terminal promotion: {e}")
                return

    if not all_journeys:
        logger.error("Zero journeys extracted.")
        return

    out = pd.DataFrame(all_journeys)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # codex m02 r2 HIGH 5 fix (2026-05-29): atomic write via tmp+rename to prevent
    # corrupt final parquet on crash mid-write.
    tmp_final = out_path.with_suffix(out_path.suffix + ".final.tmp")
    out.to_parquet(tmp_final, index=False, compression="snappy")
    tmp_final.replace(out_path)
    logger.info(f"Wrote {len(out):,} journeys to {out_path}")
    # Clean up checkpoint artifacts on successful completion.
    if partial_path.exists():
        partial_path.unlink()
    if done_wallets_path.exists():
        done_wallets_path.unlink()

    # Summary.
    logger.info("Journey class distribution:")
    for cls, n in out["journey_class"].value_counts().items():
        pct = 100 * n / len(out)
        logger.info(f"  {cls:>15}: {n:>8,} ({pct:5.1f}%)")
    logger.info(f"Total wallets: {out['wallet'].nunique():,}")
    logger.info(f"Incomplete-carry-in journeys excluded from output: {incomplete_journeys_excluded:,}")
    logger.info(f"Median pnl_bps_of_max: {out['pnl_bps_of_max'].median():.1f} bps")
    logger.info(f"Win rate (pnl > 0): {100 * (out['realized_pnl_usd'] > 0).mean():.1f}%")


if __name__ == "__main__":
    main()
