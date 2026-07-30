#!/usr/bin/env python3
"""Candle-close mark source for the forward-OOS, sourced from the fills-reconstructed 1m candles
(app/data/hl_s3_candles_1m_hot, fresh to today-1d) instead of the LAGGING asset_ctxs exact-mark archive
(hyperliquid-archive, only published to ~06-29). Alberto 2026-07-09: the fills+candles were backfilled this
morning, so pricing the OOS off candle closes unblocks it now.

candle_mark_at(coin, ts_ms): last 1m candle CLOSE at/before ts_ms (asof-backward), None past a staleness cap.
Same asof-backward contract as leadlag_clean_rank_sim.mark_at, so forward_oos_hot can swap sources cleanly.

CAVEAT: candle close = last-TRADE price; asset_ctxs = exact oracle MARK. Small bps-level basis. Use
basis_report() to quantify it on the overlap window (both sources exist through ~06-29) before trusting the
OOS verdict. Read-only.
"""
from __future__ import annotations
import glob, sys
from bisect import bisect_right
from pathlib import Path
import numpy as np, pandas as pd

REPO = Path(__file__).resolve().parent.parent.parent
CANDLE_DIR = REPO / "app" / "data" / "hl_s3_candles_1m_hot"
MARK_MAX_AGE_MS = 5 * 60_000   # 1m candles: a close older than 5m => no coverage (matches mark_at spirit)

_idx: dict[str, tuple[np.ndarray, np.ndarray] | None] = {}


def _load(lo_ms: int | None = None, hi_ms: int | None = None) -> None:
    """Build per-coin (ts_ms, close) asof index from the candle parquets, once."""
    if _idx:
        return
    files = sorted(glob.glob(str(CANDLE_DIR / "20??????.parquet")))
    if not files:
        return
    frames = []
    for f in files:
        df = pd.read_parquet(f, columns=["coin", "timestamp_utc", "close"])
        frames.append(df)
    allc = pd.concat(frames, ignore_index=True)
    allc = allc.dropna(subset=["coin", "timestamp_utc", "close"])
    allc["timestamp_utc"] = allc["timestamp_utc"].astype("int64")
    allc = allc.sort_values(["coin", "timestamp_utc"], kind="mergesort")
    for coin, g in allc.groupby("coin", sort=False):
        _idx[coin] = (g["timestamp_utc"].to_numpy(), g["close"].to_numpy(dtype="float64"))


def coverage_ms() -> tuple[int, int] | None:
    """(min_ts_ms, max_ts_ms) actually available from this mark source, or None if it has no data.

    2026-07-30 FAIL-LOUD: added because choosing a mark source that does not cover the requested
    window returns a silent n=0 rather than an error. On 2026-07-30 this produced a false
    "0 of 53 wallets pass" OOS verdict: `--mark-source candles` was used for windows starting
    2026-05-18, but this store holds only 2026-06-24..07-29 (36 dailies, ZERO for May), so the first
    five windows priced nothing. The wallets were trading heavily throughout -- raw fills show 2,332
    on 05-19 and 3,022 on 06-02, MORE than in July. Callers must assert coverage BEFORE scoring;
    see forward_oos_hot.assert_mark_coverage.
    """
    if not _idx:
        _load()
    if not _idx:
        # codex 2026-07-30 #10: this used to return None, which the caller read as "source has no
        # coverage API" and merely WARNED about. For candles, empty means _load() found NO DATA -- the
        # exact all-unmarkable state that produces a silent n=0. Signal it distinctly so the caller can
        # refuse rather than shrug.
        return (0, 0)
    lo = min(int(ts[0]) for ts, _ in _idx.values())
    hi = max(int(ts[-1]) for ts, _ in _idx.values())
    return lo, hi


def per_coin_coverage_ms() -> dict[str, tuple[int, int]]:
    """Per-COIN (min,max) ts. codex 2026-07-30 #9: the global min/max is not a coverage guarantee --
    one coin can supply the early bound and a different coin the late bound while NEITHER covers the
    window, so the aggregate check can print OK while every relevant mark returns None."""
    if not _idx:
        _load()
    return {c: (int(ts[0]), int(ts[-1])) for c, (ts, _) in _idx.items()}


def candle_mark_at(coin: str, ts_ms: int):
    if not _idx:
        _load()
    m = _idx.get(coin)
    if m is None:
        return None
    ts, px = m
    i = bisect_right(ts, ts_ms) - 1
    if i < 0:
        return None
    if ts_ms - int(ts[i]) > MARK_MAX_AGE_MS:
        return None
    return float(px[i])


def basis_report(coins: list[str], lo_ms: int, hi_ms: int, step_ms: int = 15 * 60_000):
    """Compare candle_mark_at vs asset_ctxs mark_at across [lo,hi] for `coins`; return per-coin bps basis."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import leadlag_clean_rank_sim as S
    rows = []
    for c in coins:
        diffs = []
        t = lo_ms
        while t <= hi_ms:
            cm = candle_mark_at(c, t)
            am = S.mark_at(c, t)
            if cm and am and am > 0:
                diffs.append((cm - am) / am * 1e4)  # bps
            t += step_ms
        if diffs:
            a = np.array(diffs)
            rows.append({"coin": c, "n": len(a), "med_bps": float(np.median(a)),
                         "mean_abs_bps": float(np.abs(a).mean()), "p95_abs_bps": float(np.percentile(np.abs(a), 95))})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--basis", action="store_true", help="report candle-vs-mark basis on the overlap window")
    ap.add_argument("--coins", default="BTC,ETH,SOL,HYPE,XRP,SUI,ENA,ONDO")
    ap.add_argument("--lo", default="2026-06-24"); ap.add_argument("--hi", default="2026-06-29")
    args = ap.parse_args()
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    _load()
    ncoins = len(_idx)
    cov = [(k, len(v[0])) for k, v in list(_idx.items())[:1]]
    print(f"candle index: {ncoins} coins loaded")
    if args.basis:
        df = basis_report(args.coins.split(","), ms(args.lo), ms(args.hi))
        print(df.to_string(index=False) if len(df) else "no overlap (marks/candles missing in window)")
        if len(df):
            print(f"\nMEDIAN |basis|: {df['mean_abs_bps'].median():.2f} bps  | worst p95: {df['p95_abs_bps'].max():.2f} bps")
