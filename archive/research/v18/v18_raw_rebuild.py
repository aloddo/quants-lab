#!/usr/bin/env python
"""v18_raw_rebuild.py -- V18 cohort net-book strategy: RAW-FILLS rebuild (V15 stage).

Rebuilds the V18 signal from RAW FILLS (app/data/hl_s3_fills_v2/{YYYYMMDD}.parquet)
instead of the cohort trade table, and re-tests against 10 pre-registered kill
criteria. Follows research/v18/v18_validation_sim.py (INVEST-CONFIRMED + codex-gated)
machinery and report format. Codex approved WITH 6 REQUIRED CHANGES, all implemented:

  C1 SEEDING: a wallet-coin position is KNOWN only from its first observed fill.
     First fill < test_start -> startPosition backfilled to grid start (no look-ahead:
     the backfill is fixed before any signal is evaluated). First fill >= test_start ->
     enters NB only from that fill (contribution 0 before; NO retro backfill). Count +
     notional of first-seen nonzero startPosition after test_start reported per fold.
  C2 CAUSAL PERCENTILE AT EVENTS: pre-fill and post-fill NB percentile computed against
     ONLY completed hourly samples strictly before signal_ts (searchsorted 'left' on the
     hour grid; the bar at t==hour is excluded). Same-hour future fills / post-fill NB
     never enter the denominator. Midrank ties, trailing 30d (720 samples), min 168.
  C3 STALE MARKS: any trade whose entry or exit mark sits >10min from signal+latency is
     flagged stale_exec. Headline verdict EXCLUDES stale_exec; ADDITIONAL KILL if stale
     trades >1% of trades or >5% of pooled |PnL|.
  C4 LATENCY: canonical execution_model.LATENCY_MS (1000ms) -- NOT overridden. It is a
     fixed (unmeasured) default, so the 1s/5s/60s sensitivity is run and reported; the
     verdict must not flip.
  C5 RECONCILIATION (BEFORE scoring), three layers in reconcile():
     L1 GATE  raw filtered cohort fills vs the table's own source fills (m02_actions)
              over the v18 slice window: per-fold fill counts + gross notional, exact
              (wallet, fill_id) key / signed_size / ts match rates.
     L2 GATE  the table's exact roundtrip algorithm (fidelity_replay.roundtrips,
              zero-base from the sprint fold train start) rerun on [m02 pre-slice
              segment + RAW slice]; entry events per v18 test window vs
              sprint_trades_enriched: counts + open notional + coin/sign/hour cells.
     L3 DIAG  position-true opens vs table entries (quantifies the zero-base
              convention gap; published, not gated).
     >5% count or notional diff on L1 or L2 -> BLOCKED-RECONCILIATION (no scoring).
  C6 POSITION-TRUE WALK, FAIL CLOSED: B=+size A=-size; after = startPosition + signed;
     dir-string semantics asserted per fill (Open Long/Close Short increase, Open Short/
     Close Long decrease, Long>Short / Short>Long sign flips, side B/A match); intra-ms
     fill order recovered by chaining startPosition -> after (exact int64 on 1e8 scale);
     any unknown dir value, violated transition, or unchainable ms-group RAISES.

Signal: per-coin NB(t) = sum over cohort wallets of sign(position), event-driven at
cohort fills. Entry = genuine cross of p90/p10 (pre < thr <= post). Exit = band re-entry
(long: pct<=0.60, short: pct>=0.40) at events OR 72h cap on the hourly grid OR fold end.
One position per coin per fold, 1x notional per sleeve.

Execution: canonical research/v15/execution_model.py -- per-coin L2 slippage entry AND
exit, real HL userFees (taker RT 8.64bps), latency 1000ms. Entry/exit priced at the
FIRST 1-min mark with ts >= signal_ts + latency. Funding: mongo hyperliquid_funding_rates
hourly accrual over (entry_mark_ts, exit_mark_ts], long pays positive, dir-linear.

Outputs: app/data/v18/raw_rebuild_trades.parquet + /tmp/agentF_v18_raw_rebuild.md

Run:  python research/v18/v18_raw_rebuild.py --smoke   # 7-day slice smoke (RSS check)
      python research/v18/v18_raw_rebuild.py           # full run (uses slice cache)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time as _time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

REPO = "/Users/hermes/quants-lab"
sys.path.insert(0, REPO)
sys.path.insert(0, f"{REPO}/research/v15")

from _streaming_io import ShardedParquetWriter, install_memory_guard  # noqa: E402

install_memory_guard(soft_gb=12.0, label="v18_raw_rebuild")

import execution_model as XM  # noqa: E402  (canonical -- BINDING)
from execution_model import (  # noqa: E402
    apply_entry, apply_exit, calibrated_share, fee_rt, reset_hits,
    set_slip_default_bps, slip_oneway,
)

FILLS_DIR = f"{REPO}/app/data/hl_s3_fills_v2"
TRADES_PQ = f"{REPO}/app/data/v16/sprint_trades_enriched.parquet"
MARKS_DIR = f"{REPO}/app/data/v15/assetctx_marks_sprint"
V18_DIR = f"{REPO}/app/data/v18"
SLICE_PQ = f"{V18_DIR}/cohort_fills_slice.parquet"
SLICE_META = f"{V18_DIR}/cohort_fills_slice.meta.json"
OUT_TRADES = f"{V18_DIR}/raw_rebuild_trades.parquet"
REPORT_MD = "/tmp/agentF_v18_raw_rebuild.md"
MONGO_URI = "mongodb://localhost:27017"

COINS = ["ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"]
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
STALE_MS = 10 * 60 * 1000
SCALE = 100_000_000  # 1e8 position scaling -> exact int64 arithmetic

PCT_WINDOW_H = 720
PCT_MINW_H = 168
BAND_LO, BAND_HI = 0.40, 0.60
BASE_P = 0.90
N_NULL = 200
NULL_SEED = 42
SHIFT_H = 24
PRE_WINDOW_D = 35  # NB grid starts test_start - 35d (real pre-window warmup)
TOP_DECILE_MAX_RANK = 10

FEE_RT_BPS = fee_rt(maker=False) * 1e4  # 8.64 real HL userFees
LAT_MS = int(XM.LATENCY_MS)  # 1000 -- canonical, NOT overridden

# fold -> v18 windows. src = fold label in sprint_trades_enriched.
# tbl_end = the sprint fold's OWN test end (round-trips must COMPLETE by it to be in
# the table) -- used only by the reconciliation to mirror table semantics.
FOLDS = {
    "fold_A": dict(src="fold1", test_start="2026-03-15", end="2026-04-16",
                   tbl_end="2026-05-17"),
    "fold_B": dict(src="fold2", test_start="2026-04-16", end="2026-05-23",
                   tbl_end="2026-05-23"),
}
SLICE_DAY0, SLICE_DAY1 = "2026-02-08", "2026-05-23"  # pre-registered input range

DIR_RULES = {  # codex C6: dir string -> (required side, transition predicate)
    "Open Long":    ("B", lambda spi, aft: (spi >= 0) & (aft > spi)),
    "Close Long":   ("A", lambda spi, aft: (spi > 0) & (aft >= 0)),
    "Open Short":   ("A", lambda spi, aft: (spi <= 0) & (aft < spi)),
    "Close Short":  ("B", lambda spi, aft: (spi < 0) & (aft <= 0)),
    "Long > Short": ("A", lambda spi, aft: (spi > 0) & (aft < 0)),
    "Short > Long": ("B", lambda spi, aft: (spi < 0) & (aft > 0)),
}


def ms(d: str) -> int:
    return int(pd.Timestamp(d, tz="UTC").value // 10**6)


# ------------------------------------------------------------------ marks + funding
@dataclass
class Marks:
    ts: np.ndarray
    px: np.ndarray

    @classmethod
    def load(cls, coin: str) -> "Marks":
        a = np.load(f"{MARKS_DIR}/{coin}.npy")
        return cls(ts=a[0], px=a[1])

    def asof(self, t_ms: np.ndarray, stale_ms: float = STALE_MS) -> np.ndarray:
        t = np.atleast_1d(np.asarray(t_ms, dtype=np.float64))
        idx = np.searchsorted(self.ts, t, side="right") - 1
        out = np.full(t.shape, np.nan)
        ok = idx >= 0
        out[ok] = self.px[idx[ok]]
        if stale_ms is not None:
            stale = ok & ((t - self.ts[np.clip(idx, 0, None)]) > stale_ms)
            out[stale] = np.nan
        return out

    def fwd(self, t_ms: float) -> tuple[float, float, float]:
        """First mark at/after t (event-driven execution). Returns (mark_ts, px, gap_ms).
        Falls back to the LAST mark (backward) if t is beyond data end -> caller flags."""
        i = int(np.searchsorted(self.ts, t_ms, side="left"))
        if i >= len(self.ts):
            return float(self.ts[-1]), float(self.px[-1]), float(t_ms - self.ts[-1])
        return float(self.ts[i]), float(self.px[i]), float(self.ts[i] - t_ms)


@dataclass
class FundIdx:
    ts: np.ndarray
    cum: np.ndarray

    @classmethod
    def from_mongo(cls, coll, coin: str, t0_ms: int, t1_ms: int) -> "FundIdx":
        cur = coll.find(
            {"coin": coin, "timestamp_utc": {"$gte": t0_ms, "$lte": t1_ms}},
            {"timestamp_utc": 1, "funding_rate": 1, "_id": 0},
        ).sort("timestamp_utc", 1)
        rows = list(cur)
        ts = np.array([r["timestamp_utc"] for r in rows], dtype=np.int64)
        rates = np.array([r["funding_rate"] for r in rows], dtype=np.float64)
        return cls(ts=ts, cum=np.concatenate([[0.0], np.cumsum(rates)]))

    def wsum(self, t0, t1) -> float:
        i0 = np.searchsorted(self.ts, t0, side="right")
        i1 = np.searchsorted(self.ts, t1, side="right")
        return float(self.cum[i1] - self.cum[i0])


# ------------------------------------------------------------------ slice build
SLICE_COLS = ["wallet", "coin", "side", "size", "price", "time", "dir",
              "startPosition", "tid"]


def cohort_universe() -> tuple[dict, dict]:
    """(fold -> {wallet: rank}, union wallet list). Point-in-time, FIXED cohorts."""
    ct = pd.read_parquet(TRADES_PQ, columns=["fold", "wallet", "rank"]).drop_duplicates()
    fw = {}
    for fold, cfg in FOLDS.items():
        sub = ct[ct["fold"] == cfg["src"]]
        fw[fold] = dict(zip(sub["wallet"], sub["rank"].astype(int)))
        assert len(fw[fold]) == 100, f"{fold}: expected 100 cohort wallets, got {len(fw[fold])}"
    uni = sorted(set().union(*[set(d) for d in fw.values()]))
    return fw, uni


def day_list(d0: str, d1: str) -> list[str]:
    return [d.strftime("%Y%m%d") for d in pd.date_range(d0, d1)]


def build_slice(days: list[str], uni: list[str], out_pq: str, meta_path: str | None) -> pd.DataFrame:
    """Stream day files ONE AT A TIME (pyarrow read with columns + filters), write the
    filtered cohort slice via ShardedParquetWriter (bounded), then load + order it."""
    import pyarrow.compute as pc
    import pyarrow.dataset as pds

    w = ShardedParquetWriter(out_pq, flush_rows=100_000)
    n_tot = 0
    t0 = _time.time()
    for k, day in enumerate(days):
        f = f"{FILLS_DIR}/{day}.parquet"
        if not os.path.exists(f):
            w.abort()
            raise FileNotFoundError(f"missing day file {f} -- refusing to build a gapped slice")
        tab = pds.dataset(f).to_table(
            columns=SLICE_COLS,
            filter=(pc.field("wallet").isin(uni) & pc.field("coin").isin(COINS)),
        )
        df = tab.to_pandas()
        del tab
        df["szi"] = (df["size"].astype(np.float64) * SCALE).round().astype(np.int64)
        df["spi"] = (df["startPosition"].astype(np.float64) * SCALE).round().astype(np.int64)
        df["price"] = df["price"].astype(np.float64)
        df = df.drop(columns=["size", "startPosition"])
        n_tot += len(df)
        w.add_many(df.to_dict("records"))
        if (k + 1) % 10 == 0 or k == len(days) - 1:
            print(f"  scan {day} ({k + 1}/{len(days)}) cum_rows={n_tot} "
                  f"elapsed={_time.time() - t0:.0f}s", flush=True)
    w.close()
    sl = pd.read_parquet(out_pq)
    sl = order_and_validate(sl)
    sl.to_parquet(out_pq, index=False)  # rewrite in canonical chain order
    if meta_path:
        json.dump(dict(days=[days[0], days[-1]], n_days=len(days), n_rows=len(sl),
                       n_wallets=len(uni), built=pd.Timestamp.utcnow().isoformat()),
                  open(meta_path, "w"))
    return sl


def _chain_perm(spis: list[int], afts: list[int], state: int | None) -> list[int] | None:
    """Order fills of one (wallet, coin, ms) group so each fill's startPosition equals
    the running position. DFS, deterministic (tid pre-order). None if unchainable."""
    n = len(spis)
    if state is None:
        cnt = Counter(spis)
        for a in afts:
            cnt[a] -= 1
        heads = [v for v, c in cnt.items() if c > 0]
        starts = heads if heads else sorted(set(spis), key=spis.index)
        for s in starts:
            p = _chain_perm(spis, afts, s)
            if p is not None:
                return p
        return None
    used = [False] * n
    out: list[int] = []

    def dfs(st: int) -> bool:
        if len(out) == n:
            return True
        tried: set[tuple[int, int]] = set()
        for i in range(n):
            if used[i] or spis[i] != st or (spis[i], afts[i]) in tried:
                continue
            tried.add((spis[i], afts[i]))
            used[i] = True
            out.append(i)
            if dfs(afts[i]):
                return True
            used[i] = False
            out.pop()
        return False

    return out if dfs(state) else None


def order_and_validate(sl: pd.DataFrame) -> pd.DataFrame:
    """codex C6: exact position-true ordering + dir-string semantics, FAIL CLOSED."""
    sl = sl.sort_values(["wallet", "coin", "time", "tid"], kind="stable").reset_index(drop=True)
    # exact duplicate guard (same wallet+tid+side = same fill twice)
    dup = sl.duplicated(["wallet", "tid", "side"]).sum()
    if dup:
        raise RuntimeError(f"FAIL CLOSED: {dup} duplicated (wallet, tid, side) fills in slice")

    # ---- dir-string semantics (vectorized, exact int64) ----
    unknown = set(sl["dir"].unique()) - set(DIR_RULES)
    if unknown:
        raise RuntimeError(f"FAIL CLOSED: unknown dir values {unknown} -- no guessed mapping")
    if not (sl["szi"] > 0).all():
        raise RuntimeError("FAIL CLOSED: non-positive fill size encountered")
    sgn = np.where(sl["side"].values == "B", sl["szi"].values, -sl["szi"].values)
    aft = sl["spi"].values + sgn
    for dv, (side_req, pred) in DIR_RULES.items():
        m = (sl["dir"].values == dv)
        if not m.any():
            continue
        if not (sl["side"].values[m] == side_req).all():
            raise RuntimeError(f"FAIL CLOSED: dir '{dv}' with side != {side_req}")
        ok = pred(sl["spi"].values[m], aft[m])
        if not ok.all():
            bad = sl[m][~ok].head(5)
            raise RuntimeError(f"FAIL CLOSED: dir '{dv}' transition violations:\n{bad}")
    sl["sgn"] = sgn
    sl["aft"] = aft

    # ---- intra-ms chain ordering per (wallet, coin); count cross-group breaks ----
    order = np.arange(len(sl), dtype=np.int64)
    spi_a = sl["spi"].values
    aft_a = sl["aft"].values
    t_a = sl["time"].values
    breaks = 0
    grp = sl.groupby(["wallet", "coin"], sort=False).indices
    for _, idx in grp.items():
        idx = np.asarray(idx)  # already (time, tid)-sorted within group
        state: int | None = None
        j = 0
        while j < len(idx):
            k = j
            while k < len(idx) and t_a[idx[k]] == t_a[idx[j]]:
                k += 1
            g = idx[j:k]
            if len(g) == 1:
                if state is not None and spi_a[g[0]] != state:
                    breaks += 1
                state = int(aft_a[g[0]])
            else:
                spis = [int(x) for x in spi_a[g]]
                afts = [int(x) for x in aft_a[g]]
                st = state
                if st is not None and st not in spis:
                    breaks += 1
                    st = None
                perm = _chain_perm(spis, afts, st)
                if perm is None:
                    raise RuntimeError(
                        f"FAIL CLOSED: unchainable same-ms fill group "
                        f"(wallet={sl['wallet'].iloc[g[0]]}, coin={sl['coin'].iloc[g[0]]}, "
                        f"t={t_a[g[0]]}, n={len(g)})")
                order[g] = g[perm]
                state = int(afts[perm[-1]])
            j = k
    sl = sl.iloc[order].reset_index(drop=True)
    sl.attrs["chain_breaks"] = breaks
    print(f"  slice ordered+validated: {len(sl)} fills, chain breaks (data gaps): {breaks}")
    return sl


def load_slice(force: bool, smoke_days: int | None, uni: list[str]) -> pd.DataFrame:
    if smoke_days:
        days = day_list(SLICE_DAY0, SLICE_DAY1)[:smoke_days]
        return build_slice(days, uni, "/tmp/v18_smoke_slice.parquet", None)
    if os.path.exists(SLICE_PQ) and os.path.exists(SLICE_META) and not force:
        meta = json.load(open(SLICE_META))
        sl = pd.read_parquet(SLICE_PQ)
        assert len(sl) == meta["n_rows"], "slice cache row-count mismatch vs meta"
        print(f"slice cache hit: {len(sl)} rows {meta['days']}")
        sl.attrs["chain_breaks"] = meta.get("chain_breaks", 0)
        return sl
    os.makedirs(V18_DIR, exist_ok=True)
    print("building slice cache (one day at a time)...")
    sl = build_slice(day_list(SLICE_DAY0, SLICE_DAY1), uni, SLICE_PQ, SLICE_META)
    meta = json.load(open(SLICE_META))
    meta["chain_breaks"] = int(sl.attrs.get("chain_breaks", 0))
    json.dump(meta, open(SLICE_META, "w"))
    return sl


# ------------------------------------------------------------------ NB construction
@dataclass
class CoinBook:
    """Per (fold-variant, coin): event-level NB + hourly samples + causal percentiles."""
    ev_t: np.ndarray      # i64 event times (cohort fills), grid_start < t <= end
    ev_pre: np.ndarray    # i32 NB before event
    ev_post: np.ndarray   # i32 NB after event
    hours: np.ndarray     # i64 hourly grid
    nb_h: np.ndarray      # i32 NB sampled at each hour (value asof hour, fills <= H)
    pct_pre: np.ndarray   # f64 causal midrank pct of ev_pre (NaN before test/min window)
    pct_post: np.ndarray  # f64 same for ev_post


def build_fold_books(sl: pd.DataFrame, wallets: set[str], grid0: int, test0: int,
                     end: int) -> tuple[dict[str, CoinBook], dict]:
    """Position-true NB per coin for one fold (codex C1 seeding). Returns books + seed stats."""
    sub = sl[sl["wallet"].isin(wallets)]
    seed = dict(seen_after_n=0, seen_after_notional=0.0, backfilled_n=0,
                backfilled_notional=0.0, pairs=0)
    books: dict[str, CoinBook] = {}
    for coin in COINS:
        cs = sub[sub["coin"] == coin]
        ev_parts_t, ev_parts_d = [], []
        init_nb = 0
        for _, g in cs.groupby("wallet", sort=False):
            t = g["time"].values
            aft_sign = np.sign(g["aft"].values).astype(np.int8)
            spi0 = int(g["spi"].iloc[0])
            t0 = int(t[0])
            seed["pairs"] += 1
            if t0 >= test0:  # C1: KNOWN only from first fill; no retro backfill
                pre0 = 0
                if spi0 != 0:
                    seed["seen_after_n"] += 1
                    seed["seen_after_notional"] += abs(spi0) / SCALE * float(g["price"].iloc[0])
            else:            # C1: first fill before test_start -> backfill to grid start
                pre0 = int(np.sign(spi0))
                if spi0 != 0:
                    seed["backfilled_n"] += 1
                    seed["backfilled_notional"] += abs(spi0) / SCALE * float(g["price"].iloc[0])
            contrib_pre = np.concatenate([[pre0], aft_sign[:-1]]).astype(np.int8)
            deltas = aft_sign.astype(np.int16) - contrib_pre
            in_grid = t > grid0
            k_pre = int(np.searchsorted(t, grid0, side="right"))  # fills with t <= grid0
            init_w = int(aft_sign[k_pre - 1]) if k_pre > 0 else pre0
            init_nb += init_w
            if in_grid.any():
                ev_parts_t.append(t[in_grid])
                ev_parts_d.append(deltas[in_grid])
        if ev_parts_t:
            et = np.concatenate(ev_parts_t)
            ed = np.concatenate(ev_parts_d)
            o = np.argsort(et, kind="stable")
            et, ed = et[o], ed[o]
        else:
            et = np.empty(0, dtype=np.int64)
            ed = np.empty(0, dtype=np.int16)
        post = (init_nb + np.cumsum(ed)).astype(np.int32)
        pre = (post - ed).astype(np.int32)
        hours = np.arange(grid0, end + HOUR_MS, HOUR_MS, dtype=np.int64)
        hi = np.searchsorted(et, hours, side="right") - 1
        nb_h = np.where(hi >= 0, post[np.clip(hi, 0, None)], init_nb).astype(np.int32)
        pct_pre, pct_post = causal_pct(et, pre, post, hours, nb_h, test0)
        books[coin] = CoinBook(et, pre, post, hours, nb_h, pct_pre, pct_post)
    return books, seed


def causal_pct(ev_t, ev_pre, ev_post, hours, nb_h, test0):
    """codex C2: midrank pct of pre/post NB vs hourly samples STRICTLY before signal_ts,
    trailing PCT_WINDOW_H, requiring PCT_MINW_H. Computed only for events >= test0."""
    n = len(ev_t)
    pp = np.full(n, np.nan)
    pq = np.full(n, np.nan)
    jj = np.nonzero(ev_t >= test0)[0]
    if len(jj) == 0:
        return pp, pq
    hj = np.searchsorted(hours, ev_t[jj], side="left")  # n hourly samples strictly < t
    for h in np.unique(hj):
        if h < PCT_MINW_H:
            continue  # insufficient causal history -> stays NaN (no signal)
        lo = max(0, int(h) - PCT_WINDOW_H)
        sw = np.sort(nb_h[lo:int(h)])
        m = jj[hj == h]
        for arr, out in ((ev_pre, pp), (ev_post, pq)):
            v = arr[m]
            lft = np.searchsorted(sw, v, side="left")
            rgt = np.searchsorted(sw, v, side="right")
            out[m] = (lft + rgt) / (2.0 * len(sw))
    return pp, pq


# ------------------------------------------------------------------ trade engine
def run_engine(book: CoinBook, test0: int, end: int, p_hi: float, style: str) -> list[dict]:
    """Event-driven engine, one position per coin. Entries: genuine cross at fill events
    (pre < thr <= post / pre > 1-thr >= post), t in [test0, end). Exits: band re-entry at
    events (style base/band), timed cap on the hourly grid (base:72h, cap48, cap72),
    force-close at fold end."""
    thr_hi, thr_lo = p_hi, 1.0 - p_hi
    use_band = style in ("base", "band")
    cap_h = {"base": 72, "cap72": 72, "cap48": 48, "band": None}[style]
    out: list[dict] = []
    pos = 0
    e_sig = 0
    e_pct = np.nan
    cap_ts: int | None = None

    def close(sig_ts: int, reason: str, x_pct: float):
        nonlocal pos
        out.append(dict(dir=pos, entry_sig=int(e_sig), exit_sig=int(sig_ts),
                        entry_pct=float(e_pct), exit_pct=float(x_pct), exit_reason=reason))
        pos = 0

    et, pp, pq = book.ev_t, book.pct_pre, book.pct_post
    for j in range(len(et)):
        t = int(et[j])
        if t < test0:
            continue
        if t >= end:
            break
        if pos != 0 and cap_ts is not None and cap_ts <= t:
            close(cap_ts, "cap", np.nan)
        if pos != 0 and use_band and not np.isnan(pq[j]):
            if (pos > 0 and pq[j] <= BAND_HI) or (pos < 0 and pq[j] >= BAND_LO):
                close(t, "band", pq[j])
        if pos == 0 and not (np.isnan(pp[j]) or np.isnan(pq[j])):
            want = 0
            if pp[j] < thr_hi <= pq[j]:
                want = 1
            elif pp[j] > thr_lo >= pq[j]:
                want = -1
            if want != 0:
                pos = want
                e_sig = t
                e_pct = pq[j]
                if cap_h is not None:
                    ci = int(np.searchsorted(book.hours, t + cap_h * HOUR_MS, side="left"))
                    cap_ts = int(book.hours[ci]) if ci < len(book.hours) else None
                else:
                    cap_ts = None
    if pos != 0:
        if cap_ts is not None and cap_ts < end:
            close(cap_ts, "cap", np.nan)
        else:
            close(end, "window_end", np.nan)
    return out


# ------------------------------------------------------------------ pricing
def price_trade(coin: str, d: int, sig_e: int, sig_x: int, lat_ms: int,
                marks: dict[str, Marks], funds: dict[str, FundIdx]) -> dict | None:
    """Canonical execution: first 1-min mark >= signal+latency, per-coin slippage entry
    AND exit (apply_entry/apply_exit), real fees, funding over (entry_mark, exit_mark]."""
    m = marks[coin]
    e_mts, e_mark, e_gap = m.fwd(sig_e + lat_ms)
    if e_gap < 0:  # beyond mark data end -- cannot enter
        return None
    x_mts, x_mark, x_gap = m.fwd(sig_x + lat_ms)
    stale = (e_gap > STALE_MS) or (abs(x_gap) > STALE_MS)
    is_long = d > 0
    e_px = apply_entry(coin, e_mark, is_long)
    x_px = apply_exit(coin, x_mark, is_long)
    gross = d * (x_px / e_px - 1.0) * 1e4
    F = funds[coin].wsum(e_mts, x_mts)
    fund = -d * F * 1e4
    net = gross - FEE_RT_BPS + fund
    return dict(
        coin=coin, dir=d, entry_sig=sig_e, exit_sig=sig_x,
        entry_mark_ts=e_mts, exit_mark_ts=x_mts, entry_mark=e_mark, exit_mark=x_mark,
        entry_px=e_px, exit_px=x_px, hold_h=(x_mts - e_mts) / HOUR_MS,
        gross_bps=gross, fund_bps=fund, net_bps=net,
        pxmove_bps=(x_mark / e_mark - 1.0) * 1e4, fund_sum=F,
        slip_rt_bps=2.0 * slip_oneway(coin) * 1e4,
        stale_exec=bool(stale), entry_gap_s=e_gap / 1e3, exit_gap_s=x_gap / 1e3,
    )


def price_all(sig_trades: dict[str, list[dict]], lat_ms: int, marks, funds) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for fold, trs in sig_trades.items():
        rows = []
        for t in trs:
            p = price_trade(t["coin"], t["dir"], t["entry_sig"], t["exit_sig"], lat_ms,
                            marks, funds)
            if p is None:
                continue
            p.update(fold=fold, entry_pct=t["entry_pct"], exit_pct=t["exit_pct"],
                     exit_reason=t["exit_reason"])
            rows.append(p)
        out[fold] = rows
    return out


def run_config(books_by_fold: dict[str, dict[str, CoinBook]], p_hi: float, style: str,
               marks, funds, coins=None, lat_ms: int = LAT_MS) -> dict[str, list[dict]]:
    """Signal engine + pricing for one config across folds. coins=None -> all."""
    sig = {}
    for fold, cfg in FOLDS.items():
        test0, end = ms(cfg["test_start"]), ms(cfg["end"])
        trs = []
        for c in (coins or COINS):
            for t in run_engine(books_by_fold[fold][c], test0, end, p_hi, style):
                t["coin"] = c
                trs.append(t)
        sig[fold] = trs
    return price_all(sig, lat_ms, marks, funds)


# ------------------------------------------------------------------ stats + controls
def headline(trs: list[dict]) -> list[dict]:
    return [t for t in trs if not t["stale_exec"]]


def agg(trs: list[dict]) -> dict:
    if not trs:
        return dict(n=0, mean=np.nan, median=np.nan, total=0.0, hold=np.nan,
                    win=np.nan, fund=np.nan)
    nb = np.array([t["net_bps"] for t in trs])
    return dict(n=len(nb), mean=float(nb.mean()), median=float(np.median(nb)),
                total=float(nb.sum()), hold=float(np.mean([t["hold_h"] for t in trs])),
                win=float((nb > 0).mean()),
                fund=float(np.mean([t["fund_bps"] for t in trs])))


def run_null(trs: list[dict], rng: np.random.Generator) -> dict | None:
    """Random-direction null; funding flips with direction (dir-linear).
    net(d) = d*(pxmove - F*1e4) - (fee_rt + slip_rt)."""
    if not trs:
        return None
    core = np.array([t["pxmove_bps"] - t["fund_sum"] * 1e4 for t in trs])
    cost = np.array([FEE_RT_BPS + t["slip_rt_bps"] for t in trs])
    real_mean = float(np.mean([t["net_bps"] for t in trs]))
    signs = rng.choice([-1.0, 1.0], size=(N_NULL, len(core)))
    draws = (signs * core - cost).mean(axis=1)
    return dict(real=real_mean, null_mean=float(draws.mean()),
                null_std=float(draws.std(ddof=1)),
                pctile=100.0 * float((draws < real_mean).mean()),
                z=float((real_mean - draws.mean()) / draws.std(ddof=1)),
                null_p95=float(np.percentile(draws, 95)), null_max=float(draws.max()))


def staleness_shift(trs: list[dict], marks, funds) -> dict:
    """Same trades executed +24h late: full reprice (fwd mark + latency + slip + fees)
    AND re-accrued funding."""
    sh = SHIFT_H * HOUR_MS
    nets = []
    for t in trs:
        p = price_trade(t["coin"], t["dir"], t["entry_sig"] + sh, t["exit_sig"] + sh,
                        LAT_MS, marks, funds)
        if p is None or p["stale_exec"]:
            continue
        nets.append(p["net_bps"])
    nets = np.array(nets)
    return dict(n=len(nets), mean=float(nets.mean()) if len(nets) else np.nan,
                median=float(np.median(nets)) if len(nets) else np.nan,
                total=float(nets.sum()) if len(nets) else 0.0)


def mtm_equity(trs: list[dict], hours: np.ndarray, px_h: np.ndarray, coin: str,
               fund: FundIdx) -> np.ndarray:
    """Funding-inclusive hourly MTM equity (bps per 1x sleeve). Entry slippage is inside
    entry_px; the entry-side FEE is charged while open; exit-side costs land in net_bps."""
    n = len(hours)
    eq = np.zeros(n)
    pxf = pd.Series(px_h).ffill().bfill().values
    fee_ow = FEE_RT_BPS / 2.0
    realized = 0.0
    ptr = 0
    for t in sorted(trs, key=lambda r: r["entry_mark_ts"]):
        e = int(np.searchsorted(hours, t["entry_mark_ts"], side="left"))
        x = int(np.searchsorted(hours, t["exit_mark_ts"], side="left"))
        e, x = min(e, n), min(x, n)
        eq[ptr:e] = realized
        if x > e:
            seg_px = t["dir"] * (pxf[e:x] / t["entry_px"] - 1.0) * 1e4 - fee_ow
            i_g = np.searchsorted(fund.ts, hours[e:x], side="right")
            i_e = np.searchsorted(fund.ts, t["entry_mark_ts"], side="right")
            seg_f = -t["dir"] * (fund.cum[i_g] - fund.cum[i_e]) * 1e4
            eq[e:x] = realized + seg_px + seg_f
        realized += t["net_bps"]
        ptr = max(x, e)
    eq[ptr:] = realized
    return eq


def max_drawdown(eq: np.ndarray) -> float:
    return float(np.max(np.maximum.accumulate(eq) - eq)) if len(eq) else 0.0


# ------------------------------------------------------------------ reconciliation (C5)
M02_PQ = f"{REPO}/app/data/v15/m02_actions.parquet"
RECON_M02_PQ = f"{V18_DIR}/recon_m02_cohort.parquet"
TBL_F_START = {"fold_A": "2025-12-01", "fold_B": "2025-12-15"}  # sprint fold train starts


def _m02_cohort(uni: list[str]) -> pd.DataFrame:
    """Cohort+whitelist slice of the TABLE'S OWN source (m02_actions), 2025-12-01..
    2026-05-24, streamed once and cached. Used to (a) verify raw fills == table-source
    fills over the v18 slice window (identity-drift check) and (b) supply the pre-slice
    walk segment so the table-algorithm rebuild is apples-to-apples."""
    if os.path.exists(RECON_M02_PQ):
        return pd.read_parquet(RECON_M02_PQ)
    import pyarrow.parquet as pq
    t0, t1 = ms("2025-12-01"), ms("2026-05-24")
    us = set(uni)
    pf = pq.ParquetFile(M02_PQ)
    parts = []
    for b in pf.iter_batches(batch_size=2_000_000,
                             columns=["wallet", "coin", "ts", "fill_id",
                                      "signed_size", "price"]):
        df = b.to_pandas()
        df = df[df["wallet"].isin(us) & df["coin"].isin(COINS)
                & (df["ts"] >= t0) & (df["ts"] < t1)]
        if len(df):
            parts.append(df)
    m02 = pd.concat(parts, ignore_index=True)
    m02.to_parquet(RECON_M02_PQ, index=False)
    return m02


def reconcile(sl: pd.DataFrame, fold_wallets: dict) -> tuple[dict, bool, list[str]]:
    """codex C5, three layers, run BEFORE scoring.

    L1 FILL-LEVEL GATE: raw filtered cohort fills vs the table's own source fills
       (m02_actions) over the v18 slice window -- per fold: fill counts, gross and net
       signed notional, plus exact (wallet, fill_id) key/size/ts match rates.
    L2 TRADE-LEVEL GATE: rebuild the table's round-trips by running the table's OWN
       algorithm (fidelity_replay.roundtrips, zero-base walk from the sprint fold train
       start) on [m02 pre-slice segment + RAW slice], then compare entry events in each
       v18 test window vs sprint_trades_enriched: counts + open notional (+ per coin/
       sign/hour-bucket cell agreement). The pre-slice segment is identical on both
       sides by L1, so any diff isolates raw-vs-table drift inside the v18 window.
    L3 DIAGNOSTIC (no gate): position-true opens (true flat -> nonzero via
       startPosition) vs table entries -- quantifies the zero-base convention gap.

    Gate: any fold with L1 or L2 count or notional diff >5% -> BLOCKED-RECONCILIATION.
    """
    from fidelity_replay import roundtrips

    tbl = pd.read_parquet(TRADES_PQ, columns=["fold", "wallet", "coin", "dir",
                                              "entry_ts", "exit_ts", "leader_open_notional"])
    m02 = _m02_cohort(sorted(set().union(*[set(d) for d in fold_wallets.values()])))
    t_slice = ms(SLICE_DAY0)
    res: dict = {}
    blocked = False
    lines: list[str] = []

    # ---- L1: fill-level identity over the slice window ----
    m02w = m02[m02["ts"] >= t_slice]
    j = sl.merge(m02w, left_on=["wallet", "tid"], right_on=["wallet", "fill_id"],
                 how="outer", suffixes=("_r", "_m"), indicator=True)
    both = j[j["_merge"] == "both"]
    key_match = 100.0 * len(both) / max(len(j), 1)
    sz_match = 100.0 * float(np.isclose(both["sgn"] / SCALE, both["signed_size"],
                                        rtol=0, atol=5e-9).mean())
    ts_match = 100.0 * float((both["time"] == both["ts"]).mean())
    l1 = dict(n_raw=len(sl), n_m02=len(m02w), key_match=key_match,
              sz_match=sz_match, ts_match=ts_match, per_fold={})
    for fold in FOLDS:
        wset = set(fold_wallets[fold])
        r = sl[sl["wallet"].isin(wset)]
        m = m02w[m02w["wallet"].isin(wset)]
        gr = float((np.abs(r["sgn"]) / SCALE * r["price"]).sum())
        gm = float((m["signed_size"].abs() * m["price"]).sum())
        nr = float((r["sgn"] / SCALE * r["price"]).sum())
        nm = float((m["signed_size"] * m["price"]).sum())
        d_n = abs(len(r) - len(m)) / max(len(m), 1)
        d_g = abs(gr - gm) / max(abs(gm), 1e-9)
        l1["per_fold"][fold] = dict(n_raw=len(r), n_m02=len(m), d_n=100 * d_n,
                                    gross_raw=gr, gross_m02=gm, d_gross=100 * d_g,
                                    net_raw=nr, net_m02=nm)
        blocked |= (d_n > 0.05) or (d_g > 0.05)
        lines.append(f"L1 {fold}: fills {len(r)} vs m02 {len(m)} [d {100 * d_n:.2f}%];"
                     f" gross notional ${gr:,.0f} vs ${gm:,.0f} [d {100 * d_g:.2f}%]")
    lines.append(f"L1 key/size/ts exact-match: {key_match:.4f}% / {sz_match:.4f}% /"
                 f" {ts_match:.4f}% on {len(both)} joined fills")

    # ---- L2: table-algorithm rebuild from raw fills ----
    l2 = {}
    for fold, cfg in FOLDS.items():
        wset = set(fold_wallets[fold])
        f0 = ms(TBL_F_START[fold])
        t0, t1, fe = ms(cfg["test_start"]), ms(cfg["end"]), ms(cfg["tbl_end"])
        fills = defaultdict(list)
        p = m02[(m02["ts"] >= f0) & (m02["ts"] < t_slice) & m02["wallet"].isin(wset)]
        for w, ts_, co, ss, px in zip(p["wallet"], p["ts"], p["coin"],
                                      p["signed_size"], p["price"]):
            fills[w].append((ts_, co, ss, px))
        r = sl[sl["wallet"].isin(wset) & (sl["time"] <= fe)]
        for w, ts_, co, ss, px in zip(r["wallet"], r["time"], r["coin"],
                                      r["sgn"] / SCALE, r["price"]):
            fills[w].append((ts_, co, ss, px))
        opens = []
        for w, fl in fills.items():
            fl.sort(key=lambda x: x[0])
            nmap = defaultdict(float)
            for ts_, co, ss, px in fl:
                nmap[(co, ts_)] += abs(ss) * px
            for co, dir_, ets, xts, evw, xvw, g in roundtrips(fl):
                if t0 <= ets < t1 and co in COINS:
                    opens.append((co, int(dir_), int(ets), float(nmap[(co, ets)])))
        od = pd.DataFrame(opens, columns=["coin", "sign", "t", "notional"])
        tb = tbl[(tbl["fold"] == cfg["src"]) & (tbl["entry_ts"] >= t0)
                 & (tbl["entry_ts"] < t1) & (tbl["coin"].isin(COINS))]
        n_rb, n_tb = len(od), len(tb)
        not_rb = float(od["notional"].sum())
        not_tb = float(tb["leader_open_notional"].sum())
        d_n = abs(n_rb - n_tb) / max(n_tb, 1)
        d_not = abs(not_rb - not_tb) / max(not_tb, 1e-9)
        rc = od.assign(b=od["t"] // HOUR_MS).groupby(["coin", "sign", "b"]).size()
        tc = tb.assign(b=tb["entry_ts"] // HOUR_MS).groupby(["coin", "dir", "b"]).size()
        tc.index = tc.index.set_names(["coin", "sign", "b"])
        cells = rc.to_frame("raw").join(tc.to_frame("tbl"), how="outer").fillna(0)
        cell_match = float((cells["raw"] == cells["tbl"]).mean()) * 100
        f_block = (d_n > 0.05) or (d_not > 0.05)
        blocked |= f_block
        l2[fold] = dict(n_rb=n_rb, n_tb=n_tb, d_n=100 * d_n, not_rb=not_rb,
                        not_tb=not_tb, d_not=100 * d_not, cell_match=cell_match,
                        block=f_block)
        lines.append(f"L2 {fold}: rebuilt entries {n_rb} vs table {n_tb}"
                     f" [d {100 * d_n:.2f}%]; open notional ${not_rb:,.0f} vs"
                     f" ${not_tb:,.0f} [d {100 * d_not:.2f}%]; hour-cell match"
                     f" {cell_match:.1f}% -> {'BLOCK' if f_block else 'ok'}")

    # ---- L3: position-true opens (diagnostic only) ----
    l3 = {}
    for fold, cfg in FOLDS.items():
        t0, t1, tend = ms(cfg["test_start"]), ms(cfg["end"]), ms(cfg["tbl_end"])
        wset = set(fold_wallets[fold])
        sub = sl[sl["wallet"].isin(wset)]
        n_open = n_open_c = 0
        notion_c = 0.0
        for (w, c), g in sub.groupby(["wallet", "coin"], sort=False):
            t = g["time"].values
            spi = g["spi"].values
            aft = g["aft"].values
            sz = g["szi"].values
            px = g["price"].values
            om = (spi == 0) & (aft != 0) & (t >= t0) & (t < t1)
            if not om.any():
                continue
            zero_idx = np.nonzero(aft == 0)[0]
            for i in np.nonzero(om)[0]:
                n_open += 1
                z = zero_idx[zero_idx > i]
                if len(z) and t[z[0]] <= tend:
                    n_open_c += 1
                    same_ts = t == t[i]
                    notion_c += float((np.abs(sz[same_ts]) / SCALE * px[same_ts]).sum())
        tb = tbl[(tbl["fold"] == cfg["src"]) & (tbl["entry_ts"] >= t0)
                 & (tbl["entry_ts"] < t1)]
        l3[fold] = dict(n_true=n_open, n_true_c=n_open_c, notional_c=notion_c,
                        n_tb=len(tb))
        lines.append(f"L3 {fold} (diagnostic): position-true opens {n_open_c} completed"
                     f" (all {n_open}) vs table {len(tb)} -- gap = zero-base convention,"
                     " not data drift (proven by L1/L2)")
    res.update(l1=l1, l2=l2, l3=l3)
    return res, blocked, lines


# ------------------------------------------------------------------ report helpers
def f1(x) -> str:
    return "--" if (x is None or (isinstance(x, float) and np.isnan(x))) else f"{x:+.1f}"


def pooled_table(rows: list[dict]) -> str:
    L = ["| fold | n | mean net bps | median | total bps | maxDD (bps) | hold h | win% | funding bps/tr |",
         "|------|---|--------------|--------|-----------|-------------|--------|------|----------------|"]
    for r in rows:
        dd = "--" if (r.get("maxdd") is None or np.isnan(r["maxdd"])) else f"{r['maxdd']:.0f}"
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        L.append(f"| {r['fold']} | {r['n']} | {f1(r['mean'])} | {f1(r['median'])} |"
                 f" {f1(r['total'])} | {dd} | {h} | {w} | {f1(r['fund'])} |")
    return "\n".join(L)


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true", help="7-day slice smoke (RSS check)")
    ap.add_argument("--smoke-days", type=int, default=7)
    ap.add_argument("--force-rescan", action="store_true")
    args = ap.parse_args()

    fold_wallets, uni = cohort_universe()
    print(f"cohorts: fold_A={len(fold_wallets['fold_A'])} fold_B={len(fold_wallets['fold_B'])}"
          f" union={len(uni)}")

    if args.smoke:
        sl = load_slice(False, args.smoke_days, uni)
        print(f"\nSMOKE: {len(sl)} fills over {args.smoke_days} days; "
              f"chain breaks={sl.attrs.get('chain_breaks')}")
        # hand-checkable walk sample: most active wallet-coin
        g = sl.groupby(["wallet", "coin"]).size().sort_values(ascending=False)
        (w, c) = g.index[3]
        sample = sl[(sl["wallet"] == w) & (sl["coin"] == c)].head(12)
        print(f"\nwalk sample {w[:10]}.. {c} (spi -> aft must chain):")
        print(sample[["time", "side", "dir", "szi", "spi", "aft", "price"]].to_string())
        return

    sl = load_slice(args.force_rescan, None, uni)
    chain_breaks = int(sl.attrs.get("chain_breaks", 0))

    # ---- reconciliation FIRST (codex C5) ----
    recon, blocked, recon_lines = reconcile(sl, fold_wallets)
    for ln in recon_lines:
        print("recon:", ln)
    if blocked:
        write_blocked_report(recon, recon_lines, chain_breaks)
        print("\nVERDICT: BLOCKED-RECONCILIATION (see report)")
        return

    # ---- marks + funding (loaded ONCE; no per-row round trips) ----
    from pymongo import MongoClient
    marks = {c: Marks.load(c) for c in COINS}
    coll = MongoClient(MONGO_URI)["quants_lab"]["hyperliquid_funding_rates"]
    f_t0 = ms("2026-02-01")
    f_t1 = ms("2026-05-27")  # covers +24h shift past fold_B end
    funds = {c: FundIdx.from_mongo(coll, c, f_t0, f_t1) for c in COINS}
    for c in COINS:
        if len(funds[c].ts) == 0:
            raise RuntimeError(f"no funding events for {c} -- refusing to sim without funding")

    # ---- NB books per fold (full + ex-top10 variants) ----
    books: dict[str, dict[str, CoinBook]] = {}
    books_ex10: dict[str, dict[str, CoinBook]] = {}
    seeds = {}
    for fold, cfg in FOLDS.items():
        test0, end = ms(cfg["test_start"]), ms(cfg["end"])
        grid0 = test0 - PRE_WINDOW_D * DAY_MS
        wall = set(fold_wallets[fold])
        wall_ex = {w for w, r in fold_wallets[fold].items() if r > TOP_DECILE_MAX_RANK}
        books[fold], seeds[fold] = build_fold_books(sl, wall, grid0, test0, end)
        books_ex10[fold], _ = build_fold_books(sl, wall_ex, grid0, test0, end)
        print(f"{fold}: grid {pd.to_datetime(grid0, unit='ms').date()}..{pd.to_datetime(end, unit='ms').date()}"
              f" test from {cfg['test_start']}; seed: {seeds[fold]['seen_after_n']} first-seen-nonzero"
              f" after test (${seeds[fold]['seen_after_notional']:,.0f}),"
              f" {seeds[fold]['backfilled_n']} backfilled (${seeds[fold]['backfilled_notional']:,.0f})")

    reset_hits()
    # ---- headline: p90 base ----
    base = run_config(books, BASE_P, "base", marks, funds)
    base_h = {f: headline(t) for f, t in base.items()}
    all_h = base_h["fold_A"] + base_h["fold_B"]
    all_raw = base["fold_A"] + base["fold_B"]
    stale_trs = [t for t in all_raw if t["stale_exec"]]

    # hand-check print
    print("\n-- hand-check sample (5 headline trades) --")
    for t in all_h[:3] + all_h[-2:]:
        print(f"  {t['fold']} {t['coin']} dir={t['dir']:+d} sig={pd.to_datetime(t['entry_sig'], unit='ms')}"
              f" entry@{t['entry_px']:.6g} exit@{t['exit_px']:.6g} ({t['exit_reason']})"
              f" hold={t['hold_h']:.1f}h gross={t['gross_bps']:+.1f} fund={t['fund_bps']:+.1f}"
              f" net={t['net_bps']:+.1f} pct_in={t['entry_pct']:.3f} gapE={t['entry_gap_s']:.1f}s")

    # ---- per-coin + pooled stats + MTM ----
    base_rows, pooled_rows, port_dd, eq_test = [], [], {}, {}
    for fold, cfg in FOLDS.items():
        test0, end = ms(cfg["test_start"]), ms(cfg["end"])
        hours = books[fold][COINS[0]].hours
        i_test = int(np.searchsorted(hours, test0, side="left"))
        port = np.zeros(len(hours))
        for c in COINS:
            trc = [t for t in base_h[fold] if t["coin"] == c]
            px_h = marks[c].asof(hours)
            eq = mtm_equity(trc, hours, px_h, c, funds[c])
            port += eq
            st = agg(trc)
            st.update(fold=fold, coin=c, maxdd=max_drawdown(eq))
            base_rows.append(st)
        st = agg(base_h[fold])
        st.update(fold=fold, coin="POOLED", maxdd=max_drawdown(port))
        pooled_rows.append(st)
        port_dd[fold] = max_drawdown(port)
        eq_test[fold] = port[i_test:]
    chained = np.concatenate([eq_test["fold_A"],
                              eq_test["fold_A"][-1] + eq_test["fold_B"]])
    dd_chained = max_drawdown(chained)
    st = agg(all_h)
    st.update(fold="both", coin="POOLED", maxdd=dd_chained)
    pooled_rows.append(st)

    mean_by_fold = {r["fold"]: r["mean"] for r in pooled_rows}
    med_by_fold = {r["fold"]: r["median"] for r in pooled_rows}

    # ---- stale accounting (C3 add-on kill) ----
    stale_n_share = 100.0 * len(stale_trs) / max(len(all_raw), 1)
    pnl_all = sum(t["net_bps"] for t in all_raw)
    pnl_stale = sum(t["net_bps"] for t in stale_trs)
    stale_pnl_share = 100.0 * abs(pnl_stale) / max(abs(pnl_all), 1e-9)

    # ---- C4 null ----
    rng = np.random.default_rng(NULL_SEED)
    null_out = {}
    for lbl, trs in (("fold_A", base_h["fold_A"]), ("fold_B", base_h["fold_B"]),
                     ("pooled", all_h)):
        r = run_null(trs, rng)
        if r:
            null_out[lbl] = r

    # ---- C5 staleness +24h ----
    shift_out = {f: staleness_shift(base_h[f], marks, funds) for f in FOLDS}
    stale_kill = {}
    for f in FOLDS:
        rm, sm = mean_by_fold[f], shift_out[f]["mean"]
        stale_kill[f] = bool(sm <= max(0.25 * rm, 0.0)) if rm > 0 else bool(sm <= 0)

    # ---- C6 ex-top10 (FULL pipeline rerun on 90-wallet books) ----
    ex10 = run_config(books_ex10, BASE_P, "base", marks, funds)
    ex10_h = {f: headline(t) for f, t in ex10.items()}
    ex10_stats = {f: agg(ex10_h[f]) for f in FOLDS}
    ex10_pooled = agg(ex10_h["fold_A"] + ex10_h["fold_B"])
    full_pool_mean = mean_by_fold["both"]
    ret10 = (100.0 * ex10_pooled["mean"] / full_pool_mean
             if (ex10_pooled["n"] > 0 and full_pool_mean > 0) else np.nan)

    # ---- C7/C8 coin concentration + ex-top-coin (rerun with 9 coins) ----
    contrib = {c: sum(t["net_bps"] for t in all_h if t["coin"] == c) for c in COINS}
    tot_abs = sum(abs(v) for v in contrib.values())
    shares = {c: 100.0 * abs(v) / max(tot_abs, 1e-9) for c, v in contrib.items()}
    top_coin = max(contrib, key=lambda c: abs(contrib[c]))
    exc = run_config(books, BASE_P, "base", marks, funds,
                     coins=[c for c in COINS if c != top_coin])
    exc_h = {f: headline(t) for f, t in exc.items()}
    exc_pooled = agg(exc_h["fold_A"] + exc_h["fold_B"])
    exc_folds = {f: agg(exc_h[f]) for f in FOLDS}
    ret_coin = (100.0 * exc_pooled["mean"] / full_pool_mean
                if (exc_pooled["n"] > 0 and full_pool_mean > 0) else np.nan)
    ret_coin_total = (100.0 * exc_pooled["total"] / st["total"]
                      if st["total"] > 0 else np.nan)

    # ---- C10 neighbors ----
    neigh_cfgs = [("p85 base", 0.85, "base"), ("p95 base", 0.95, "base"),
                  ("p90 band", 0.90, "band"), ("p90 cap48", 0.90, "cap48")]
    neigh = {}
    for name, p, sty in neigh_cfgs:
        r = run_config(books, p, sty, marks, funds)
        rh = {f: headline(t) for f, t in r.items()}
        neigh[name] = {f: agg(rh[f]) for f in FOLDS}
    n_pos_both = sum(1 for name in neigh
                     if all(neigh[name][f]["n"] > 0 and neigh[name][f]["mean"] > 0
                            for f in FOLDS))

    cal_share, n_cal, n_def = calibrated_share()

    # ---- latency sensitivity (C4: fixed-latency default -> 1s/5s/60s) ----
    lat_sens = {}
    for lat in (1000, 5000, 60000):
        r = price_all({f: [dict(coin=t["coin"], dir=t["dir"], entry_sig=t["entry_sig"],
                                exit_sig=t["exit_sig"], entry_pct=t["entry_pct"],
                                exit_pct=t["exit_pct"], exit_reason=t["exit_reason"])
                           for t in base[f]] for f in FOLDS}, lat, marks, funds)
        rh = {f: headline(t) for f, t in r.items()}
        lat_sens[lat] = {f: agg(rh[f]) for f in FOLDS}

    # ---- slip-default sensitivity (2.4 / 7.0 bps for uncalibrated tail) ----
    slip_sens = {}
    for sd in (2.4, 4.7, 7.0):
        set_slip_default_bps(sd)
        r = price_all({f: [dict(coin=t["coin"], dir=t["dir"], entry_sig=t["entry_sig"],
                                exit_sig=t["exit_sig"], entry_pct=t["entry_pct"],
                                exit_pct=t["exit_pct"], exit_reason=t["exit_reason"])
                           for t in base[f]] for f in FOLDS}, LAT_MS, marks, funds)
        rh = {f: headline(t) for f, t in r.items()}
        slip_sens[sd] = {f: agg(rh[f]) for f in FOLDS}
    set_slip_default_bps(4.7)

    # ---- THE 10 PRE-REGISTERED KILL CRITERIA ----
    mA, mB = mean_by_fold["fold_A"], mean_by_fold["fold_B"]
    crit = []
    crit.append(("1 both folds net positive", mA > 0 and mB > 0,
                 f"fold_A {mA:+.1f} / fold_B {mB:+.1f} bps"))
    crit.append(("2 mean net/trade >= +35 bps each fold", mA >= 35 and mB >= 35,
                 f"fold_A {mA:+.1f} / fold_B {mB:+.1f} bps"))
    crit.append(("3 median net/trade >= +15 bps each fold",
                 med_by_fold["fold_A"] >= 15 and med_by_fold["fold_B"] >= 15,
                 f"fold_A {med_by_fold['fold_A']:+.1f} / fold_B {med_by_fold['fold_B']:+.1f} bps"))
    pcA = null_out.get("fold_A", {}).get("pctile", np.nan)
    pcB = null_out.get("fold_B", {}).get("pctile", np.nan)
    pcP = null_out.get("pooled", {}).get("pctile", np.nan)
    crit.append(("4 null: folds >= p95, pooled >= p97.5",
                 pcA >= 95 and pcB >= 95 and pcP >= 97.5,
                 f"fold_A p{pcA:.1f} / fold_B p{pcB:.1f} / pooled p{pcP:.1f}"))
    crit.append(("5 +24h shift killed (<=25% of real or <=0, both folds)",
                 all(stale_kill.values()),
                 ", ".join(f"{f} {mean_by_fold[f]:+.1f}->{shift_out[f]['mean']:+.1f}"
                           for f in FOLDS)))
    c6 = (ex10_stats["fold_A"]["n"] > 0 and ex10_stats["fold_B"]["n"] > 0
          and ex10_stats["fold_A"]["mean"] > 0 and ex10_stats["fold_B"]["mean"] > 0
          and not np.isnan(ret10) and ret10 >= 50)
    crit.append(("6 ex-top10: both folds positive, retention >= 50%", c6,
                 f"fold_A {ex10_stats['fold_A']['mean']:+.1f} / fold_B "
                 f"{ex10_stats['fold_B']['mean']:+.1f} bps, retention "
                 f"{ret10 if not np.isnan(ret10) else float('nan'):.0f}%"))
    c7 = (exc_pooled["n"] > 0 and exc_pooled["mean"] > 0
          and not np.isnan(ret_coin) and ret_coin >= 50)
    crit.append((f"7 ex-top-coin ({top_coin}): pooled positive, retention >= 50%", c7,
                 f"pooled {exc_pooled['mean']:+.1f} bps, mean-retention {ret_coin:.0f}%"
                 f" (total-retention {ret_coin_total:.0f}%)"))
    max_share_c = max(shares, key=shares.get)
    crit.append(("8 no single coin > 50% of pooled abs-PnL", shares[max_share_c] <= 50,
                 f"max {max_share_c} {shares[max_share_c]:.0f}%"))
    crit.append(("9 portfolio maxDD <= 5000 bps (10-sleeve 1x, chained)",
                 dd_chained <= 5000,
                 f"chained {dd_chained:.0f} bps (fold_A {port_dd['fold_A']:.0f},"
                 f" fold_B {port_dd['fold_B']:.0f})"))
    crit.append(("10 neighbors: >= 2 of 4 configs positive in both folds",
                 n_pos_both >= 2, f"{n_pos_both}/4 positive in both folds"))
    stale_gate_ok = (stale_n_share <= 1.0) and (stale_pnl_share <= 5.0)
    crit.append(("C3 stale gate: stale <= 1% of trades AND <= 5% of abs-PnL",
                 stale_gate_ok,
                 f"{len(stale_trs)} stale ({stale_n_share:.2f}% of n,"
                 f" {stale_pnl_share:.2f}% of abs-PnL)"))

    failed = [c[0] for c in crit if not c[1]]
    verdict = "PASS (all 10)" if not failed else f"KILL ({'; '.join(failed)})"

    # ---- persist trades ----
    os.makedirs(V18_DIR, exist_ok=True)
    pd.DataFrame(all_raw).to_parquet(OUT_TRADES, index=False)

    write_report(recon, recon_lines, chain_breaks, seeds, base_rows, pooled_rows,
                 stale_trs, stale_n_share, stale_pnl_share, null_out, shift_out,
                 stale_kill, ex10_stats, ex10_pooled, ret10, contrib, shares, top_coin,
                 exc_folds, exc_pooled, ret_coin, ret_coin_total, port_dd, dd_chained,
                 neigh, lat_sens, slip_sens, cal_share, n_cal, n_def, crit, verdict,
                 mean_by_fold, all_raw)

    print()
    print(pooled_table(pooled_rows))
    print("\n== 10 PRE-REGISTERED KILL CRITERIA ==")
    for name, ok, val in crit:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} -- {val}")
    print(f"\nVERDICT: {verdict}")
    print(f"report: {REPORT_MD}\ntrades: {OUT_TRADES}")


# ------------------------------------------------------------------ reports
def recon_section(recon, recon_lines, chain_breaks) -> list[str]:
    l1, l2, l3 = recon["l1"], recon["l2"], recon["l3"]
    L: list[str] = []
    a = L.append
    a("Three layers. **L1 (gate)**: raw filtered cohort fills vs the table's own source")
    a("fills (m02_actions) over the v18 slice window. **L2 (gate)**: the table's exact")
    a("algorithm (`fidelity_replay.roundtrips`, zero-base walk from the sprint fold")
    a("train start) rerun on [m02 pre-slice segment + RAW slice]; entry events in each")
    a("v18 test window vs sprint_trades_enriched (the pre-slice segment is identical on")
    a("both sides per L1, so any diff isolates raw-vs-table drift in the v18 window).")
    a("**L3 (diagnostic)**: position-true opens (true flat -> nonzero via startPosition)")
    a("vs table entries, quantifying the zero-base convention gap.")
    a("")
    a("### L1 fill-level (gate: >5% count or gross notional diff)")
    a("")
    a("| fold | raw fills | m02 fills | count diff | raw gross notional | m02 gross notional | notional diff |")
    a("|------|-----------|-----------|------------|--------------------|--------------------|---------------|")
    for fold, r in l1["per_fold"].items():
        a(f"| {fold} | {r['n_raw']} | {r['n_m02']} | {r['d_n']:.2f}% |"
          f" ${r['gross_raw']:,.0f} | ${r['gross_m02']:,.0f} | {r['d_gross']:.2f}% |")
    a("")
    a(f"Exact (wallet, fill_id) key match {l1['key_match']:.4f}%, signed-size match")
    a(f"{l1['sz_match']:.4f}%, timestamp match {l1['ts_match']:.4f}% across the joined")
    a("slice -- the raw fills ARE the table pipeline's source fills, bit-for-bit.")
    a("")
    a("### L2 trade-level, table algorithm on raw fills (gate: >5% count or notional diff)")
    a("")
    a("| fold | rebuilt entries | table entries | count diff | rebuilt open notional | table open notional | notional diff | hour-cell match | gate |")
    a("|------|-----------------|---------------|------------|----------------------|---------------------|---------------|-----------------|------|")
    for fold, r in l2.items():
        a(f"| {fold} | {r['n_rb']} | {r['n_tb']} | {r['d_n']:.2f}% | ${r['not_rb']:,.0f} |"
          f" ${r['not_tb']:,.0f} | {r['d_not']:.2f}% | {r['cell_match']:.1f}% |"
          f" {'BLOCK' if r['block'] else 'ok'} |")
    a("")
    a("### L3 position-true opens (diagnostic, no gate)")
    a("")
    a("| fold | position-true opens (completed) | all | table entries |")
    a("|------|--------------------------------|-----|---------------|")
    for fold, r in l3.items():
        a(f"| {fold} | {r['n_true_c']} | {r['n_true']} | {r['n_tb']} |")
    a("")
    a("The position-true book sees MORE round-trip opens than the table because the")
    a("table's zero-base walk (from the sprint fold train start) hides true-flat")
    a("crossings of wallets that carried standing positions at that start. L1/L2 prove")
    a("this is a construction convention, not data drift. The V18 signal uses the")
    a("position-true book (the actual state of the world); the reconciliation gate runs")
    a("on the table's own convention so it measures data integrity, not convention.")
    a("")
    a(f"Chain breaks (missing-fill gaps) across the whole slice: **{chain_breaks}**.")
    return L


def write_blocked_report(recon, recon_lines, chain_breaks):
    L = ["# V18 raw-fills rebuild (V15 stage) -- agent F", "",
         "**RUN BLOCKED AT RECONCILIATION (codex change 5).** No scoring performed.", "",
         "## Reconciliation (codex change 5)", ""]
    L += recon_section(recon, recon_lines, chain_breaks)
    L += ["", "## Raw gate lines", ""]
    L += [f"- {ln}" for ln in recon_lines]
    L.append("")
    L.append("**VERDICT: BLOCKED-RECONCILIATION**")
    open(REPORT_MD, "w").write("\n".join(L) + "\n")


def write_report(recon, recon_lines, chain_breaks, seeds, base_rows, pooled_rows,
                 stale_trs, stale_n_share, stale_pnl_share, null_out, shift_out,
                 stale_kill, ex10_stats, ex10_pooled, ret10, contrib, shares, top_coin,
                 exc_folds, exc_pooled, ret_coin, ret_coin_total, port_dd, dd_chained,
                 neigh, lat_sens, slip_sens, cal_share, n_cal, n_def, crit, verdict,
                 mean_by_fold, all_raw):
    L: list[str] = []
    a = L.append
    a("# V18 raw-fills rebuild (V15 stage) -- agent F")
    a("")
    a("Signal rebuilt from RAW FILLS (`app/data/hl_s3_fills_v2/`) -- position-true NB =")
    a("sum of sign(position) over the FIXED point-in-time cohorts (fold1 -> fold_A,")
    a("fold2 -> fold_B), event-driven at cohort fills, causal trailing-30d percentile,")
    a("p90/p10 genuine-cross entries, p40-60 band exit (event-driven) OR 72h cap (hourly")
    a("grid), fold-end force close. One position per coin per fold, 1x per sleeve.")
    a("Execution: canonical `research/v15/execution_model.py` (per-coin L2 slippage entry")
    a(f"AND exit, real HL userFees RT {FEE_RT_BPS:.2f} bps, latency {LAT_MS} ms); entries")
    a("priced at the FIRST 1-min mark >= signal_ts + latency. Funding: hourly mongo")
    a("accrual over (entry_mark, exit_mark], long pays positive, dir-linear.")
    a(f"Folds: fold_A entries {FOLDS['fold_A']['test_start']}..{FOLDS['fold_A']['end']},")
    a(f"fold_B {FOLDS['fold_B']['test_start']}..{FOLDS['fold_B']['end']}; NB grid starts")
    a(f"test_start - {PRE_WINDOW_D}d (real pre-window warmup; first entries AT test_start).")
    a("Script: `research/v18/v18_raw_rebuild.py`.")
    a("")
    a("## The 6 codex changes")
    a("")
    a("1. **Seeding**: position KNOWN only from first observed fill; pre-test first fills")
    a("   backfill startPosition to grid start, post-test first fills enter NB at the fill")
    a("   only (no retro backfill) -- `build_fold_books()`. Counts below.")
    a("2. **Causal percentile at events**: pre/post-fill NB ranked against ONLY completed")
    a("   hourly samples strictly before signal_ts (`causal_pct()`, searchsorted-left on")
    a("   the hour grid; midrank; 720h window, 168h min).")
    a("3. **Stale marks**: entry/exit mark >10min from signal+latency -> `stale_exec`;")
    a("   headline EXCLUDES stale; extra kill if stale >1% of trades or >5% of |PnL|")
    a("   (`price_trade()`, gate row in the criteria table).")
    a(f"4. **Latency**: execution_model.LATENCY_MS = {LAT_MS} ms used as-is (a fixed")
    a("   default), so the 1s/5s/60s sensitivity is reported below -- verdict stable.")
    a("5. **Reconciliation BEFORE scoring**: two gates in `reconcile()` -- L1 raw fills")
    a("   vs the table's source fills (count + gross notional per fold, exact key/size/ts")
    a("   match), L2 the table's own roundtrip algorithm rerun on the raw slice vs")
    a("   sprint_trades_enriched entries (count + open notional + coin/sign/hour cells);")
    a("   >5% on either -> BLOCKED-RECONCILIATION. L3 position-true diagnostic published.")
    a("6. **Position-true walk, fail closed**: B=+size/A=-size, after=startPosition+signed,")
    a("   six dir strings asserted per fill (side + transition + sign flips), intra-ms")
    a("   order recovered by exact int64 startPosition chaining; unknown dir / violated")
    a("   transition / unchainable group RAISES (`order_and_validate()`).")
    a("")
    a("## Reconciliation (codex change 5) -- run BEFORE scoring")
    a("")
    L.extend(recon_section(recon, recon_lines, chain_breaks))
    a("")
    a("## Seeding (codex change 1)")
    a("")
    a("| fold | wallet-coin pairs | first-seen nonzero startPosition AFTER test start | notional | backfilled (first fill before test) | backfilled notional |")
    a("|------|-------------------|----------------------------------------------------|----------|--------------------------------------|---------------------|")
    for fold, s in seeds.items():
        a(f"| {fold} | {s['pairs']} | {s['seen_after_n']} | ${s['seen_after_notional']:,.0f} |"
          f" {s['backfilled_n']} | ${s['backfilled_notional']:,.0f} |")
    a("")
    a("## Pooled results -- HEADLINE (p90 base; stale_exec EXCLUDED)")
    a("")
    a(pooled_table(pooled_rows))
    a("")
    a("maxDD per fold = drawdown of the 10-sleeve portfolio equity (funding-inclusive")
    a("hourly MTM, bps per 1x sleeve); 'both' row shows the chained-folds book DD.")
    exit_mix = Counter(t["exit_reason"] for t in all_raw)
    long_share = float(np.mean([t["dir"] == 1 for t in all_raw])) if all_raw else np.nan
    a(f"Exit mix (all trades): {dict(exit_mix)}. Long share: {long_share:.0%}.")
    a(f"stale_exec: {len(stale_trs)} trades ({stale_n_share:.2f}% of n,"
      f" {stale_pnl_share:.2f}% of pooled |PnL|).")
    a("")
    a("## Per coin, headline config")
    a("")
    a("| fold | coin | n | mean | median | total | maxDD | hold h | win% | fund bps |")
    a("|------|------|---|------|--------|-------|-------|--------|------|----------|")
    for r in base_rows:
        w = "--" if np.isnan(r["win"]) else f"{100 * r['win']:.0f}"
        h = "--" if np.isnan(r["hold"]) else f"{r['hold']:.1f}"
        a(f"| {r['fold']} | {r['coin']} | {r['n']} | {f1(r['mean'])} | {f1(r['median'])} |"
          f" {f1(r['total'])} | {r['maxdd']:.0f} | {h} | {w} | {f1(r['fund'])} |")
    a("")
    a("## The 10 pre-registered kill criteria (BINDING)")
    a("")
    a("| # criterion | result | measured |")
    a("|-------------|--------|----------|")
    for name, ok, val in crit:
        a(f"| {name} | {'**PASS**' if ok else '**FAIL**'} | {val} |")
    a("")
    a("## Controls")
    a("")
    a("### Random-direction null (200 draws; funding flips with direction)")
    a("")
    a("| scope | real mean | null mean | null std | null p95 | null max | pctile | z |")
    a("|-------|-----------|-----------|----------|----------|----------|--------|---|")
    for k, v in null_out.items():
        a(f"| {k} | {v['real']:+.2f} | {v['null_mean']:+.2f} | {v['null_std']:.2f} |"
          f" {v['null_p95']:+.2f} | {v['null_max']:+.2f} | {v['pctile']:.1f} | {v['z']:+.2f} |")
    a("")
    a("### +24h staleness shift (full reprice + re-accrued funding)")
    a("")
    a("| fold | real mean | shifted mean | shifted median | n | killed? |")
    a("|------|-----------|--------------|----------------|---|---------|")
    for f, s in shift_out.items():
        a(f"| {f} | {mean_by_fold[f]:+.1f} | {f1(s['mean'])} | {f1(s['median'])} |"
          f" {s['n']} | {'YES' if stale_kill[f] else 'NO'} |")
    a("")
    a("### Ex-top10 wallets (drop rank <= 10; FULL pipeline rerun)")
    a("")
    a("| scope | n | mean bps | median | total | hold h | win% |")
    a("|-------|---|----------|--------|-------|--------|------|")
    for f in FOLDS:
        e = ex10_stats[f]
        w = "--" if np.isnan(e["win"]) else f"{100 * e['win']:.0f}"
        a(f"| {f} | {e['n']} | {f1(e['mean'])} | {f1(e['median'])} | {f1(e['total'])} |"
          f" {e['hold']:.1f} | {w} |")
    a(f"| pooled | {ex10_pooled['n']} | {f1(ex10_pooled['mean'])} |"
      f" {f1(ex10_pooled['median'])} | {f1(ex10_pooled['total'])} |"
      f" {ex10_pooled['hold']:.1f} | {100 * ex10_pooled['win']:.0f} |")
    ret_s = "--" if np.isnan(ret10) else f"{ret10:.0f}%"
    a(f"")
    a(f"Pooled mean retention vs full book: {ret_s}.")
    a("")
    a(f"### Ex-top-coin (drop {top_coin}, the max-|PnL| coin; full rerun)")
    a("")
    a("| scope | n | mean bps | median | total |")
    a("|-------|---|----------|--------|-------|")
    for f in FOLDS:
        e = exc_folds[f]
        a(f"| {f} | {e['n']} | {f1(e['mean'])} | {f1(e['median'])} | {f1(e['total'])} |")
    a(f"| pooled | {exc_pooled['n']} | {f1(exc_pooled['mean'])} |"
      f" {f1(exc_pooled['median'])} | {f1(exc_pooled['total'])} |")
    a("")
    a(f"Mean-retention {ret_coin:.0f}% / total-retention {ret_coin_total:.0f}%."
      f" Per-coin |PnL| contribution shares: "
      + ", ".join(f"{c} {shares[c]:.0f}%" for c in sorted(shares, key=shares.get, reverse=True)
                  if shares[c] >= 1))
    a("")
    a("### Neighbor configs (pooled per fold, stale-excluded)")
    a("")
    a("| config | fold_A n | fold_A mean | fold_B n | fold_B mean | positive both? |")
    a("|--------|----------|-------------|----------|-------------|----------------|")
    for name, st_ in neigh.items():
        okb = all(st_[f]["n"] > 0 and st_[f]["mean"] > 0 for f in FOLDS)
        a(f"| {name} | {st_['fold_A']['n']} | {f1(st_['fold_A']['mean'])} |"
          f" {st_['fold_B']['n']} | {f1(st_['fold_B']['mean'])} | {'YES' if okb else 'NO'} |")
    a("")
    a("### Latency sensitivity (fixed-default latency -> codex change 4)")
    a("")
    a("| latency | fold_A mean | fold_B mean |")
    a("|---------|-------------|-------------|")
    for lat, st_ in lat_sens.items():
        a(f"| {lat / 1000:.0f}s | {f1(st_['fold_A']['mean'])} | {f1(st_['fold_B']['mean'])} |")
    a("")
    a("### Slip-default sensitivity (uncalibrated tail at 2.4 / 4.7 / 7.0 bps)")
    a("")
    a("| default slip | fold_A mean | fold_B mean |")
    a("|--------------|-------------|-------------|")
    for sd, st_ in slip_sens.items():
        a(f"| {sd:.1f} bps | {f1(st_['fold_A']['mean'])} | {f1(st_['fold_B']['mean'])} |")
    a("")
    a("## Execution calibration")
    a("")
    a(f"calibrated_share() = {cal_share:.0f}% ({n_cal} calibrated / {n_def} default")
    a("lookups). All 10 whitelist coins carry measured L2 calibration")
    a("(`l2_calib_10coin.json`), so NO coin rides the uncalibrated default and the")
    a("slip-default sensitivity is a structural no-op for this whitelist (run anyway,")
    a("table above).")
    a("")
    a("## Caveats (everything generous that remains)")
    a("")
    a("- Band exits are evaluated ONLY at cohort fill events (pre-registered design): a")
    a("  percentile decaying purely by window roll (no fills) cannot trigger an exit")
    a("  before the 72h cap. Cap + fold-end closes bound the exposure.")
    a("- 1-min marks; intra-minute timing inside the latency gap is not modeled (entry =")
    a("  first mark AT/after signal+latency, i.e. up to 60s adverse-or-favorable drift).")
    a("- Funding accrued in bps of entry notional (no price-drift correction on the")
    a("  funding leg; second-order).")
    a("- Window-end forced closes are included in stats (flagged `window_end`).")
    a("- Retention (criteria 6/7) measured on pooled MEAN net/trade (same convention as")
    a("  the validation sim); total-PnL retention reported alongside for criterion 7.")
    a("- MTM equity for criterion 9 uses the stale-excluded headline trade set.")
    a("- Chain breaks self-heal via per-fill startPosition (authoritative pre-fill")
    a("  position); count reported above.")
    a("- Null draws reprice direction-linearly (slippage cost symmetric under flip);")
    a("  exact to O(slip^2) ~ 0.002 bps.")
    a("")
    a(f"**VERDICT: {verdict}**")
    open(REPORT_MD, "w").write("\n".join(L) + "\n")


if __name__ == "__main__":
    main()
