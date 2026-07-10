#!/usr/bin/env python3
"""Real-time HL MARK collector (Alberto GO 2026-07-09): poll metaAndAssetCtxs every ~60s, timestamp it,
store per-coin markPx (+oraclePx, midPx). From now on we have continuous 1-min marks -> the OOS is never
blocked by the ~10-day asset_ctxs archive lag again.

Source: https://api.hyperliquid.xyz/info {"type":"metaAndAssetCtxs"} -- ONE low-weight call/min (231 main-dex
coins), zero 429 risk to the copy trader. (xyz/HIP-3 dexes need per-dex polls -> fast-follow, not here.)

Output: app/data/hl_mark_1m_hot/{YYYYMMDD}.parquet  cols: coin,timestamp_utc(ms),markPx,oraclePx,midPx,source
Resident loop; resumes the current UTC day on restart; flushes atomically every N polls + on day rollover +
on SIGTERM/SIGINT. Read-only w.r.t. everything else. Companion loader: research/v15/live_marks.live_mark_at.
"""
from __future__ import annotations
import argparse, logging, signal, sys, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "app" / "data" / "hl_mark_1m_hot"
HL_API = "https://api.hyperliquid.xyz/info"
# timestamp_utc is MINUTE-FLOORED (join key -> aligns with 1m candles/fills timestamp_utc, which are also
# minute-floored). poll_ts_utc keeps the exact wall-clock poll instant for audit. (Alberto 2026-07-09:
# "properly to the minute mark so it's joinable with the S3 backfill".)
COLS = ["coin", "timestamp_utc", "poll_ts_utc", "markPx", "oraclePx", "midPx", "source"]
LOG = logging.getLogger("hl_live_mark")

_stop = False


def _sig(*_a):
    global _stop
    _stop = True


def day_id(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")


def poll_once(session: requests.Session, ts_ms: int) -> list[dict[str, Any]]:
    r = session.post(HL_API, json={"type": "metaAndAssetCtxs"}, timeout=15).json()
    if not (isinstance(r, list) and len(r) == 2):
        raise RuntimeError("metaAndAssetCtxs: unexpected response shape")
    meta, ctxs = r[0], r[1]
    uni = meta.get("universe", [])
    minute_ms = (ts_ms // 60_000) * 60_000     # minute-floored join key (aligns with 1m candle grid)
    rows = []
    for u, c in zip(uni, ctxs):
        coin = u.get("name")
        if not coin or not isinstance(c, dict):
            continue
        mk = c.get("markPx")
        if mk is None:
            continue
        rows.append({"coin": coin, "timestamp_utc": minute_ms, "poll_ts_utc": ts_ms, "markPx": str(mk),
                     "oraclePx": str(c.get("oraclePx", "")), "midPx": str(c.get("midPx", "")),
                     "source": "hl_api_metaAndAssetCtxs_1m"})
    return rows


def load_day(day: str) -> list[dict[str, Any]]:
    fp = OUT_DIR / f"{day}.parquet"
    if not fp.exists():
        return []
    try:
        return pd.read_parquet(fp).to_dict("records")
    except Exception as e:
        # codex P1 (2026-07-10): do NOT swallow a read error and return [] — the next flush would then
        # overwrite an existing day's marks with only the new (partial) buffer, silently truncating live
        # history. Fail LOUD so KeepAlive surfaces it and an operator fixes/moves the bad file.
        raise RuntimeError(
            f"hl_live_mark_collector: existing day file {fp} is unreadable ({e!r}). Refusing to start empty "
            f"and overwrite it. Move/repair the file, then let KeepAlive relaunch."
        ) from e


def flush_day(day: str, buf: list[dict[str, Any]]) -> None:
    if not buf:
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(buf, columns=COLS).sort_values(["coin", "timestamp_utc"], kind="mergesort")
    # dedup exact (coin, ts) in case of a restart-overlap poll
    df = df.drop_duplicates(subset=["coin", "timestamp_utc"], keep="last")
    fp = OUT_DIR / f"{day}.parquet"
    tmp = fp.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    tmp.replace(fp)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                        stream=sys.stdout, force=True)
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=60, help="seconds between polls")
    ap.add_argument("--flush-every", type=int, default=5, help="flush the day parquet every N polls")
    ap.add_argument("--once", action="store_true", help="single poll then exit (testing)")
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    session = requests.Session()

    now_ms = int(time.time() * 1000)
    cur_day = day_id(now_ms)
    buf = load_day(cur_day)
    LOG.info("start: interval=%ss flush_every=%s resumed_day=%s rows=%d", args.interval, args.flush_every, cur_day, len(buf))

    if args.once:
        buf.extend(poll_once(session, now_ms)); flush_day(cur_day, buf)
        LOG.info("once: wrote %d rows to %s", len(buf), OUT_DIR / f"{cur_day}.parquet")
        return 0

    n = 0
    while not _stop:
        t0 = time.time()
        ts_ms = int(t0 * 1000)
        day = day_id(ts_ms)
        if day != cur_day:                       # UTC day rollover -> flush + start fresh
            flush_day(cur_day, buf)
            cur_day = day
            buf = load_day(cur_day)
            LOG.info("day rollover -> %s", cur_day)
        try:
            rows = poll_once(session, ts_ms)
            buf.extend(rows)
            n += 1
            if n % args.flush_every == 0:
                flush_day(cur_day, buf)
            if n % 30 == 0:
                LOG.info("polled %d times; day=%s buf_rows=%d last_coins=%d", n, cur_day, len(buf), len(rows))
        except Exception as exc:
            LOG.warning("poll failed (continuing): %s", exc)
        # sleep the remainder of the interval, but wake often to honor _stop
        elapsed = time.time() - t0
        remaining = max(0.0, args.interval - elapsed)
        while remaining > 0 and not _stop:
            step = min(2.0, remaining)
            time.sleep(step)
            remaining -= step

    flush_day(cur_day, buf)
    LOG.info("stopped: flushed day=%s rows=%d", cur_day, len(buf))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
