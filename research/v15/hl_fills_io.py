#!/usr/bin/env python3
"""V15 HL fills/funding I/O — standalone loaders on the LIVE hot stores.

Extracted VERBATIM from ``v15_m01_equity_reconstruct.py`` so M02 (and any other
CORE-lane consumer) no longer imports the archived M01 for its fills/funding
data source. The fill-normalization, causal-ordering, dedup, fail-closed NaN
handling, coin/spot/perp helpers, and STALE_CAP are byte-identical to M01. The
ONLY change is the storage backend:

  - Fills:   app/data/hl_s3_fills_v2_hot/YYYYMMDD.parquet  (fresh, day-partitioned)
  - Funding: app/data/hl_s3_funding_hot/YYYYMMDD.parquet    (fresh, day-partitioned)

The old dead stores (hl_s3_fills_v2/ ended 2026-06-25; v13/funding_cache/ frozen
~May) and the S3_BY_WALLET_DIR fast-path are intentionally NOT read here.

Output contracts are IDENTICAL to M01: load_wallet_fills returns causally-ordered
fill dicts; load_wallet_funding returns {time, hash, delta:{type,coin,usdc}} rows.
"""
from __future__ import annotations

import glob
import json
import logging
import math
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger("hl_fills_io")

# --------------------------------------------------------------------------- #
# Single consolidated stores (ONE store, Alberto TG11307 2026-07-14). The v2
# deep-history day-files (2025-07-27..2026-06-08) are hardlinked INTO the hot
# fills dir by scripts/consolidate_fills_store.py so this dir is the single
# canonical fills store spanning 2025-07-27..today. No two-dir read here.
# --------------------------------------------------------------------------- #
HOT_FILLS_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_fills_v2_hot")
HOT_FUNDING_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_funding_hot")
HOT_LEDGER_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_ledger_hot")


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

# STALE_CAP: fall back if reconstruction base anchor is older than one anchor
# cycle (7d) while material unmarkable exposure exists. Tunable (spec).
STALE_CAP_MS = 7 * 86_400_000

_DAY_MS = 86_400_000

# Core accounting columns a fills day-file MUST expose; optional fee/tid cols may be absent.
_FILLS_REQUIRED = frozenset({"wallet", "coin", "side", "size", "price", "time"})
_FUNDING_REQUIRED = frozenset({"wallet", "time", "coin", "usdc"})
_LEDGER_REQUIRED = frozenset({"wallet", "time", "delta_json", "hash"})
_FILLS_COLS = [
    "wallet", "coin", "side", "size", "price", "time", "dir",
    "closedPnl", "startPosition", "fee", "builderFee", "deployerFee", "tid",
]


class HotStoreReadError(RuntimeError):
    """Raised (fail-CLOSED) when a hot-store day parquet that OVERLAPS the requested [t0,t1]
    window is unreadable or missing a required accounting column. A caller (process_wallet)
    turns this into an {"error": ...} result so run_daily ABORTS before advancing the checkpoint
    instead of silently omitting a whole day of fills/funding (2026-07-12 fail-closed fix)."""


def _day_bounds_from_path(path: str) -> Optional[tuple[int, int]]:
    """(day_start_ms, day_end_ms) for a YYYYMMDD.parquet hot-store file, else None (non-day file)."""
    stem = Path(path).stem
    if len(stem) != 8 or not stem.isdigit():
        return None
    ds = int(pd.Timestamp(f"{stem[:4]}-{stem[4:6]}-{stem[6:]}", tz="UTC").timestamp() * 1000)
    return ds, ds + _DAY_MS - 1


def _overlaps_window(path: str, t0: int, t1: int) -> bool:
    """True if the day-file's [start,end] intersects the requested [t0,t1] (so its integrity matters)."""
    b = _day_bounds_from_path(path)
    if b is None:
        return False
    ds, de = b
    return de >= t0 and ds <= t1


def _normalize_fills_df(df: "pd.DataFrame") -> list[dict]:
    """Normalize a raw fills DataFrame slice into causal fill dicts (perp-only, NaN-safe, fail-closed).

    Extracted to module level so the per-wallet ``load_wallet_fills`` and the day-oriented bulk
    ``load_grouped_fills_funding`` produce BYTE-IDENTICAL rows for the same source rows (the journey
    math depends on it)."""
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


def coin_is_allowed_perp(coin: str) -> bool:
    """True for any in-scope perp; excludes spot and HIP-4 outcomes."""
    return not coin_is_spot(coin)


# --------------------------------------------------------------------------- #
# Causal fill ordering (verbatim from M01)
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


# --------------------------------------------------------------------------- #
# Local data loaders — LIVE hot stores only
# --------------------------------------------------------------------------- #


def load_wallet_fills(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load ALL markable perp fills for one wallet in [t0, t1] from the HOT store.

    Includes every perp dex prefix except explicitly dropped prefixes. Spot
    fills (@-prefix, USDC) are excluded: M1 is anchored to ``perpAllTime`` and
    M7/live copy execution is perp-only.
    Casts string numerics, computes signed_sz (+size if side=='B' else -size),
    and tags liquidation fills via the `dir` column.

    Backend: day-partitioned parquet under HOT_FILLS_DIR (no per-wallet fast-path).
    """
    wallet_lc = wallet.lower()

    fills: list[dict] = []
    for ff in sorted(glob.glob(str(HOT_FILLS_DIR / "*.parquet"))):
        in_window = _overlaps_window(ff, t0, t1)
        try:
            # Read only columns that actually exist (read_parquet with a fixed
            # `columns=` list raises if ANY is absent). Require the core accounting
            # columns; tolerate missing optional fee cols.
            avail = set(pq.ParquetFile(ff).schema_arrow.names)
            missing_required = _FILLS_REQUIRED - avail
            if missing_required:
                # FAIL CLOSED for any day OVERLAPPING the requested window: a missing accounting
                # column would silently omit that day and then advance the checkpoint (2026-07-12).
                if in_window:
                    raise HotStoreReadError(
                        f"fills {Path(ff).name}: missing required cols {sorted(missing_required)} "
                        f"(overlaps requested window [{t0},{t1}])")
                logger.warning(
                    f"  skip {Path(ff).name}: missing required cols {sorted(missing_required)} (out of window)")
                continue
            read_cols = [c for c in _FILLS_COLS if c in avail]
            df = pd.read_parquet(ff, columns=read_cols)
            df["time"] = df["time"].astype("int64")
            df["_w"] = df["wallet"].astype(str).str.lower()
            m = df[(df["_w"] == wallet_lc) & (df["time"] >= t0) & (df["time"] <= t1)]
            if not m.empty:
                fills.extend(_normalize_fills_df(m))
        except HotStoreReadError:
            raise
        except Exception as e:  # noqa: BLE001
            if in_window:
                raise HotStoreReadError(f"fills {Path(ff).name}: unreadable ({e!r}); overlaps window") from e
            logger.warning(f"  skip {Path(ff).name}: {e!r} (out of window)")
            continue
    return order_wallet_fills_causally(fills)


def load_wallet_funding(wallet: str, t0: int, t1: int) -> list[dict]:
    """Load + merge + dedup funding events from the LIVE hot funding store.

    Day-partitioned parquet under HOT_FUNDING_DIR; per-wallet rows with a signed
    ``usdc`` funding-usd string. Reshaped to the SAME {time, hash, delta} contract
    M02 consumes (identical to M01's parquet-fallback shape). Dedup by (time, coin).
    """
    wallet_lc = wallet.lower()
    seen: set[tuple[int, str]] = set()
    out: list[dict] = []
    for pf in sorted(glob.glob(str(HOT_FUNDING_DIR / "*.parquet"))):
        in_window = _overlaps_window(pf, t0, t1)
        try:
            avail = set(pq.ParquetFile(pf).schema_arrow.names)
            missing_required = _FUNDING_REQUIRED - avail
            if missing_required:
                if in_window:
                    raise HotStoreReadError(
                        f"funding {Path(pf).name}: missing required cols {sorted(missing_required)} "
                        f"(overlaps requested window [{t0},{t1}])")
                logger.warning(
                    f"  skip {Path(pf).name}: missing required cols {sorted(missing_required)} (out of window)")
                continue
            fd = pd.read_parquet(pf, columns=["wallet", "time", "coin", "usdc"])
            fd["time"] = fd["time"].astype("int64")
            fd["_w"] = fd["wallet"].astype(str).str.lower()
            m = fd[(fd["_w"] == wallet_lc) & (fd["time"] >= t0) & (fd["time"] <= t1)]
        except HotStoreReadError:
            raise
        except Exception as e:  # noqa: BLE001
            if in_window:
                raise HotStoreReadError(f"funding {Path(pf).name}: unreadable ({e!r}); overlaps window") from e
            logger.warning(f"  skip {Path(pf).name}: {e!r} (out of window)")
            continue
        for r in m.itertuples(index=False):
            t = int(r.time)
            coin = str(r.coin)
            key = (t, coin)
            if key in seen:
                continue
            seen.add(key)
            out.append({"time": t, "hash": "",
                        "delta": {"type": "funding", "coin": coin, "usdc": str(r.usdc)}})
    out.sort(key=lambda x: int(x["time"]))
    return out


# --------------------------------------------------------------------------- #
# Day-oriented BULK loader (first-run / catch-up fast path)
# --------------------------------------------------------------------------- #


def load_grouped_fills_funding(
    wallets: set[str], t0: int, t1: int
) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """Read each hot day-file ONCE and return per-wallet grouped fills + funding for a SET of wallets.

    The per-wallet ``load_wallet_fills``/``load_wallet_funding`` glob ALL day-files and filter to ONE
    wallet on EVERY call — O(wallets x days) full-day scans (fatal on the ~15k first-run catch-up).
    This reads every day-file exactly ONCE, filters to the wallet set, groups per wallet PRESERVING
    causal row order (day-file order, in-file row order), and finally applies the SAME
    ``order_wallet_fills_causally`` / funding dedup+sort. The result per wallet is BYTE-IDENTICAL to
    the per-wallet loaders (verified by the FIX-1 equivalence check). Fail-CLOSED: a day-file that
    OVERLAPS [t0,t1] but is unreadable / missing a required column raises ``HotStoreReadError``.

    Memory: fills for the window are ~17M rows (~2-3GB) held once as per-wallet lists; the caller is
    expected to POP entries as it streams journeys out (Rule-8 bounded)."""
    wl = {str(w).lower() for w in wallets}
    fills_by: dict[str, list[dict]] = {w: [] for w in wl}
    funding_by: dict[str, list[dict]] = {w: [] for w in wl}
    fund_seen: dict[str, set[tuple[int, str]]] = {w: set() for w in wl}

    # ---- fills: one pass over day-files, grouped per wallet (order preserved) ----
    for ff in sorted(glob.glob(str(HOT_FILLS_DIR / "*.parquet"))):
        in_window = _overlaps_window(ff, t0, t1)
        if not in_window:
            continue
        try:
            avail = set(pq.ParquetFile(ff).schema_arrow.names)
            missing_required = _FILLS_REQUIRED - avail
            if missing_required:
                raise HotStoreReadError(
                    f"fills {Path(ff).name}: missing required cols {sorted(missing_required)} "
                    f"(overlaps requested window [{t0},{t1}])")
            read_cols = [c for c in _FILLS_COLS if c in avail]
            df = pd.read_parquet(ff, columns=read_cols)
            df["time"] = df["time"].astype("int64")
            df["_w"] = df["wallet"].astype(str).str.lower()
            m = df[(df["_w"].isin(wl)) & (df["time"] >= t0) & (df["time"] <= t1)]
        except HotStoreReadError:
            raise
        except Exception as e:  # noqa: BLE001
            raise HotStoreReadError(f"fills {Path(ff).name}: unreadable ({e!r}); overlaps window") from e
        if m.empty:
            continue
        # groupby(sort=False) preserves first-appearance + within-group row order -> identical to the
        # per-wallet single-wallet filter over the same day-file.
        for w, g in m.groupby("_w", sort=False):
            fills_by[w].extend(_normalize_fills_df(g))

    for w in fills_by:
        fills_by[w] = order_wallet_fills_causally(fills_by[w])

    # ---- funding: one pass, per-wallet dedup by (time, coin), then sort by time ----
    for pf in sorted(glob.glob(str(HOT_FUNDING_DIR / "*.parquet"))):
        in_window = _overlaps_window(pf, t0, t1)
        if not in_window:
            continue
        try:
            avail = set(pq.ParquetFile(pf).schema_arrow.names)
            missing_required = _FUNDING_REQUIRED - avail
            if missing_required:
                raise HotStoreReadError(
                    f"funding {Path(pf).name}: missing required cols {sorted(missing_required)} "
                    f"(overlaps requested window [{t0},{t1}])")
            fd = pd.read_parquet(pf, columns=["wallet", "time", "coin", "usdc"])
            fd["time"] = fd["time"].astype("int64")
            fd["wlc"] = fd["wallet"].astype(str).str.lower()
            m = fd[(fd["wlc"].isin(wl)) & (fd["time"] >= t0) & (fd["time"] <= t1)]
        except HotStoreReadError:
            raise
        except Exception as e:  # noqa: BLE001
            raise HotStoreReadError(f"funding {Path(pf).name}: unreadable ({e!r}); overlaps window") from e
        if m.empty:
            continue
        for r in m.itertuples(index=False):
            w = r.wlc
            t = int(r.time)
            coin = str(r.coin)
            key = (t, coin)
            if key in fund_seen[w]:
                continue
            fund_seen[w].add(key)
            funding_by[w].append({"time": t, "hash": "",
                                  "delta": {"type": "funding", "coin": coin, "usdc": str(r.usdc)}})

    for w in funding_by:
        funding_by[w].sort(key=lambda x: int(x["time"]))

    return fills_by, funding_by


def load_grouped_ledger(wallets: set[str], t0: int, t1: int) -> dict[str, list[dict]]:
    """Read each hl_s3_ledger_hot day-file ONCE and return per-wallet ledger entries for a SET of
    wallets (O(days), not O(wallets x days)). Output per wallet matches the m01.load_wallet_ledger
    contract: a list of ``{"time": int, "hash": str, "delta": {...}}`` deduped on the FULL delta
    payload key ``(time, hash, canonical-json(delta))`` and sorted by time. delta comes from the
    hot store's ``delta_json`` column (the full ledger delta object). Fail-CLOSED: a day-file that
    OVERLAPS [t0,t1] but is unreadable / missing a required column raises HotStoreReadError."""
    wl = {str(w).lower() for w in wallets}
    out_by: dict[str, list[dict]] = {w: [] for w in wl}
    seen: dict[str, set[tuple]] = {w: set() for w in wl}
    for lf in sorted(glob.glob(str(HOT_LEDGER_DIR / "*.parquet"))):
        if not _overlaps_window(lf, t0, t1):
            continue
        try:
            avail = set(pq.ParquetFile(lf).schema_arrow.names)
            missing_required = _LEDGER_REQUIRED - avail
            if missing_required:
                raise HotStoreReadError(
                    f"ledger {Path(lf).name}: missing required cols {sorted(missing_required)} "
                    f"(overlaps requested window [{t0},{t1}])")
            ld = pd.read_parquet(lf, columns=["wallet", "time", "delta_json", "hash"])
            ld["time"] = ld["time"].astype("int64")
            ld["wlc"] = ld["wallet"].astype(str).str.lower()
            m = ld[(ld["wlc"].isin(wl)) & (ld["time"] >= t0) & (ld["time"] <= t1)]
        except HotStoreReadError:
            raise
        except Exception as e:  # noqa: BLE001
            raise HotStoreReadError(f"ledger {Path(lf).name}: unreadable ({e!r}); overlaps window") from e
        if m.empty:
            continue
        for r in m.itertuples(index=False):
            w = r.wlc
            t = int(r.time)
            h = r.hash
            h = "" if (h is None or (isinstance(h, float) and math.isnan(h))) else str(h)
            try:
                delta = json.loads(r.delta_json) if r.delta_json else {}
            except (TypeError, ValueError) as e:  # FAIL CLOSED (contract): a corrupt delta must not
                raise HotStoreReadError(  # silently drop capital-flow / entity-link info
                    f"ledger {Path(lf).name}: unparseable delta_json for {w} @ {t} ({e!r})") from e
            key = (t, h, json.dumps(delta, sort_keys=True, default=str))
            if key in seen[w]:
                continue
            seen[w].add(key)
            out_by[w].append({"time": t, "hash": h, "delta": delta})
    for w in out_by:
        out_by[w].sort(key=lambda x: int(x["time"]))
    return out_by
