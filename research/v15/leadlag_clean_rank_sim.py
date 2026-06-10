#!/usr/bin/env python3
"""Lead-lag CLEAN-RANK salvage sim (codex loop-3 final test) -- memory-safe rebuild.

CONTEXT (brain projects/quant/v15/2026-06-10-leadlag-newtrade-failed): the short-horizon
wallet-momentum copy idea was killed once before. The cheap kill-test (+62bps) was realized-PnL
inventory-close leakage. The DECISIVE new-trade-only test FAILED all 5 codex criteria when wallets
were ranked by TOTAL trailing copy-net (inventory-contaminated). Codex loop-3 prescribed ONE more
test: rank by the CLEAN new-trade-only trailing edge (not total), then forward new-trade-only copy
vs a matched null AND a symbol/side-matched beta control (to separate wallet-skill from pure
trend-following). If this fails too -> clean kill of copy-momentum.

This is the rebuild of the sim that OOM-crashed the box on 2026-06-10 (it aggregated all rows in
RAM). This version is MANDATORY-streaming (decisions/2026-05-31) + aggregate-memory-budgeted
(decisions/2026-06-10): per-decision rows stream to disk; the wallet universe is bounded; the marks
index is page-cached once (no per-row DB round-trip).

SURVIVORSHIP CAVEAT (codex #6): the candidate universe is selected from m03 activity (wallets active
into the window, ranked by total_journeys), a FUTURE-KNOWN active-wallet set. Acceptable ONLY for a
conservative NEGATIVE kill: if the edge fails even on survivor-active wallets, the kill is stronger.
A POSITIVE result here is NOT deployable without a point-in-time-eligible universe and conservative
(sub-minute) execution marks.

STATUS: codex-reviewed SHIP-TO-RUN (3 rounds; the close-handling look-ahead + decision-anchored beta
were the key fixes). Run gated on free RAM (aggregate budget aborts if tight) + CoS sweep-hold.

Data sources (faithful to the prior validated sim):
  - Wallet fills: HL API userFillsByTime -> authoritative `dir` ("Open Long" / "Close Short" ...).
    This is the only source with Open/Close labeling (hl_wallet_trades lacks `dir`).
  - Marks: app/data/v15/assetctx_marks/COIN.npy, shape (2, N) = [ts_ms_row, price_row], 1-min.
  - Candidate universe: app/data/v15/m03_wallet_activity_summary.parquet (cap by activity to fit RAM).

Method per decision hour t (hourly grid, Apr1..May23 default):
  1. CLEAN trailing rank signal: for each wallet, copy ONLY the opens it makes in (t-trail, t],
     FIFO-close our own copied lots on the wallet's matching Close (else force-close at t), enter at
     fill + latency with adverse bps + RT fee. Sum net = clean_trailing_edge[w]. (NOT total PnL.)
  2. Rank wallets with >=1 trailing open by clean_trailing_edge; take the top fraction (decile).
  3. FORWARD (t, t+hold]: copy ONLY opens the top-decile wallets make after t, same lot mechanics,
     force-close survivors at t+hold. Aggregate net = top_fwd_edge (size-normalized, equal per lot).
  4. MATCHED-NULL: random wallets NOT in the top decile, matched on the same activity bucket; same
     forward procedure = null_fwd_edge.
  5. BETA control (codex required): for the SAME (coin, side, entry_ts, exit_ts) lots the top-decile
     opened forward, the pure market move (entry_mark -> exit_mark, no skill) = beta_edge. The
     skill estimate is top_fwd - beta; trend-following alone would make top_fwd ~= beta.
  Stream one row per decision hour. Aggregate (mean top-null, bootstrap, concentration, top-beta)
  is done offline on the tiny streamed parquet.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
import urllib.request
import json
from bisect import bisect_right
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _streaming_io import ShardedParquetWriter, install_memory_guard, plan_memory_budget  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [leadlag] %(levelname)s: %(message)s")
log = logging.getLogger("leadlag")

HL_API = "https://api.hyperliquid.xyz/info"
_DATA = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
ASSETCTX_DIR = _DATA / "assetctx_marks"
HOUR_MS = 3_600_000

# CANONICAL execution model is the SINGLE source of truth for slippage / fees / latency (Alberto
# 2026-06-10: no sim may hardcode its own). See research/v15/execution_model.py + the spec in brain
# projects/quant/v15/execution-model. FEE_RT kept as a module alias so default args resolve.
from execution_model import (  # noqa: E402
    slip_oneway, fee_rt, set_slip_default_bps, set_latency_ms,
    calibrated_share, reset_hits, LATENCY_MS,
)
FEE_RT = fee_rt()            # canonical HL round-trip taker (from hl_fee_schedule.json)
TRAIL_FRAC = 0.01            # exit_mode="trail" retracement (hyp #9); set via --trail-frac

# ----- marks: page-cached per-coin asof index (no per-row DB round-trip) ---------------------- #
_marks: dict[str, tuple] = {}   # coin -> (ts_array, px_array)


def _load_marks(coin: str):
    if coin in _marks:
        return _marks[coin]
    p = ASSETCTX_DIR / f"{coin}.npy"
    if not p.exists():
        _marks[coin] = None
        return None
    a = np.load(p, allow_pickle=False)            # (2, N): row0 ts_ms, row1 price
    ts = a[0].astype(np.int64)
    px = a[1].astype(np.float64)
    _marks[coin] = (ts, px)
    return _marks[coin]


def mark_at(coin: str, ts_ms: int):
    """Last mark at or before ts_ms (asof-backward). None if no coverage / coin absent."""
    m = _load_marks(coin)
    if m is None:
        return None
    ts, px = m
    i = bisect_right(ts, ts_ms) - 1
    if i < 0:
        return None
    return float(px[i])


def trail_exit(coin: str, entry_ts: int, hi_ts: int, is_long: bool, trail: float):
    """REALIZABLE trailing-stop exit (hyp #9): walk marks forward from entry; ride the favorable move,
    exit when price retraces `trail` (fraction) from the running peak/trough. Timeout at hi_ts.
    Returns (exit_ts, exit_mark). Causal (only uses marks after entry)."""
    m = _load_marks(coin)
    if m is None:
        return None
    ts, px = m
    lo = bisect_right(ts, entry_ts)
    hi = bisect_right(ts, hi_ts)
    if hi <= lo:
        return None
    seg_px = px[lo:hi]
    seg_ts = ts[lo:hi]
    if is_long:
        peak = seg_px[0]
        for j in range(len(seg_px)):
            p = seg_px[j]
            if p > peak:
                peak = p
            if p <= peak * (1 - trail):
                return int(seg_ts[j]), float(p)
    else:
        trough = seg_px[0]
        for j in range(len(seg_px)):
            p = seg_px[j]
            if p < trough:
                trough = p
            if p >= trough * (1 + trail):
                return int(seg_ts[j]), float(p)
    return int(seg_ts[-1]), float(seg_px[-1])     # timeout


def best_mark(coin: str, lo_ts: int, hi_ts: int, is_long: bool):
    """ORACLE exit (hyp #2): the most favorable mark in (lo_ts, hi_ts] -- max for a long, min for a
    short. This is the EX-POST upper bound on exit timing (not tradeable; it bounds whether copied
    entry timing has enough gross edge to ever cover execution)."""
    m = _load_marks(coin)
    if m is None:
        return None
    ts, px = m
    lo = bisect_right(ts, lo_ts)            # first idx with ts > lo_ts
    hi = bisect_right(ts, hi_ts)            # first idx with ts > hi_ts  (exclusive)
    if hi <= lo:
        return None
    seg = px[lo:hi]
    return float(seg.max()) if is_long else float(seg.min())


# ----- HL fills with authoritative Open/Close dir ------------------------------------------- #
class FetchError(RuntimeError):
    pass


def _post(payload, tries=5):
    body = json.dumps(payload).encode()
    last = None
    for k in range(tries):
        try:
            req = urllib.request.Request(HL_API, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except Exception as e:
            last = e
            time.sleep(1.5 * (k + 1))
    # codex #7: do NOT silently truncate a wallet's history on a transient API failure -> raise so the
    # caller records the wallet as INCOMPLETE rather than treating it as "no more fills".
    raise FetchError(f"HL /info failed after {tries} tries: {last}")


def load_wallet_opens_closes(w: str, start_ms: int, end_ms: int) -> list[dict]:
    """All fills in [start,end] as normalized lots: {ts, coin, is_open, is_long, px}. dir from HL API.
    Deduped by (time, oid, tid). Sorted by ts."""
    fills, s, seen = [], start_ms, set()
    for _ in range(60):
        rr = _post({"type": "userFillsByTime", "user": w, "startTime": s, "endTime": end_ms})
        if not isinstance(rr, list):
            # codex #7: a non-list 200 (error/rate-limit/malformed object) must NOT read as "done".
            raise FetchError(f"wallet {w}: unexpected HL response (non-list): {str(rr)[:200]}")
        if not rr:
            break
        fills += rr
        if len(rr) < 2000:
            break
        s = max(f["time"] for f in rr) + 1
    else:
        # hit the page cap -> history may be truncated; surface it (codex #7).
        raise FetchError(f"wallet {w}: paging cap hit, history likely truncated")
    out = []
    for f in fills:
        k = (f.get("time"), f.get("oid"), f.get("tid"))
        if k in seen:
            continue
        seen.add(k)
        d = str(f.get("dir", ""))
        coin = f.get("coin", "")
        if not coin or (not d.startswith("Open") and not d.startswith("Close")):
            continue
        try:
            px = float(f.get("px"))
        except Exception:
            continue
        out.append({"ts": int(f["time"]), "coin": coin, "is_open": d.startswith("Open"),
                    "is_long": ("Long" in d), "px": px})
    out.sort(key=lambda x: x["ts"])
    return out


M02_ACTIONS = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15" / "m02_actions.parquet"


def load_events_from_m02(wallet_set: set[str], start_ms: int, end_ms: int,
                         exclude_liq: bool = True) -> dict[str, list]:
    """Full-universe open/close events from the m02 journey-trace output (NO API). Reads only the
    needed columns, row-group by row-group (bounded RAM), filters to `wallet_set` and [start,end].
    Returns {wallet: [{ts, coin, is_open, is_long}, ...]} sorted by ts.

    action_type ENTRY=open, EXIT=close. Direction for FIFO matching is the POSITION's side:
      ENTRY -> is_long = signed_size > 0 ; EXIT -> is_long = signed_size < 0 (selling closes a long).
    Liquidations excluded by default (not copyable signal)."""
    import pyarrow.parquet as pq
    from array import array
    cols = ["wallet", "coin", "ts", "action_type", "signed_size", "is_liquidation"]
    pf = pq.ParquetFile(str(M02_ACTIONS))
    # MEMORY: m02 has ~86M rows. Accumulating per-wallet as Python tuple-lists peaked ~5.5GB (and
    # Python retains it as RSS -> tripped the guard). Instead accumulate into typed array.array
    # (raw C values, ~12B/event = same as the final numpy), so the load peak ~= the final size (~1GB).
    a_ts: dict[str, array] = {}
    a_c: dict[str, array] = {}
    a_o: dict[str, array] = {}
    a_l: dict[str, array] = {}
    for batch in pf.iter_batches(batch_size=1_000_000, columns=cols):
        d = batch.to_pydict()
        ws, cs, ts, at, sz, liq = (d["wallet"], d["coin"], d["ts"], d["action_type"],
                                   d["signed_size"], d["is_liquidation"])
        for i in range(len(ws)):
            w = ws[i]
            if w not in wallet_set:
                continue
            tt = ts[i]
            if tt < start_ms or tt > end_ms:
                continue
            if exclude_liq and liq[i]:
                continue
            is_open = (at[i] == "ENTRY")
            s = sz[i]
            if s is None or s == 0:
                continue
            is_long = (s > 0) if is_open else (s < 0)
            if w not in a_ts:
                a_ts[w] = array('q'); a_c[w] = array('h'); a_o[w] = array('b'); a_l[w] = array('b')
            a_ts[w].append(int(tt)); a_c[w].append(_coin_id(cs[i]))
            a_o[w].append(1 if is_open else 0); a_l[w].append(1 if is_long else 0)
    out: dict[str, WalletEvents] = {}
    for w in list(a_ts.keys()):
        ts_a = np.frombuffer(a_ts.pop(w), dtype=np.int64).copy()
        cid = np.frombuffer(a_c.pop(w), dtype=np.int16).copy()
        op = np.frombuffer(a_o.pop(w), dtype=np.int8).astype(np.bool_)
        lg = np.frombuffer(a_l.pop(w), dtype=np.int8).astype(np.bool_)
        order = np.argsort(ts_a, kind="stable")    # ts not guaranteed sorted across row groups
        out[w] = WalletEvents(ts_a[order], cid[order], op[order], lg[order])
    return out


# ----- compact per-wallet event store (memory + speed for the full 18k universe) ------------- #
# Storing ~30M events as list-of-dicts needs ~30GB. Compact numpy arrays per wallet need ~12B/event
# (~0.4GB total). Each decision then bisect-slices the [win_lo, win_hi] range (O(log n)) and
# materializes ONLY that small slice as dicts for the (unchanged, codex-approved) copy_edge. This
# avoids the old O(events-up-to-t) from-zero scan that made the full universe infeasible.
_COINS: list[str] = []          # id -> coin string
_COIN_ID: dict[str, int] = {}   # coin string -> id


def _coin_id(coin: str) -> int:
    i = _COIN_ID.get(coin)
    if i is None:
        i = len(_COINS)
        _COINS.append(coin)
        _COIN_ID[coin] = i
    return i


class WalletEvents:
    """Per-wallet open/close events as compact, ts-sorted parallel numpy arrays."""
    __slots__ = ("ts", "coin_id", "is_open", "is_long")

    def __init__(self, ts, coin_id, is_open, is_long):
        self.ts = ts            # int64[]
        self.coin_id = coin_id  # int16[]
        self.is_open = is_open  # bool[]
        self.is_long = is_long  # bool[]

    @classmethod
    def from_tuples(cls, tuples: list):
        """tuples: list of (ts, coin_id, is_open, is_long), sorted by ts ascending."""
        n = len(tuples)
        ts = np.empty(n, dtype=np.int64)
        cid = np.empty(n, dtype=np.int16)
        op = np.empty(n, dtype=np.bool_)
        lg = np.empty(n, dtype=np.bool_)
        for i, (t, c, o, l) in enumerate(tuples):
            ts[i] = t; cid[i] = c; op[i] = o; lg[i] = l
        return cls(ts, cid, op, lg)

    def slice_dicts(self, win_lo: int, win_hi: int) -> list[dict]:
        """Materialize ONLY events with ts in (win_lo, win_hi] as dicts for copy_edge. bisect-bounded;
        identical to what copy_edge would have iterated (it skips ts<=win_lo opens and breaks at
        ts>win_hi)."""
        from bisect import bisect_right
        lo = bisect_right(self.ts, win_lo)          # first idx with ts > win_lo
        hi = bisect_right(self.ts, win_hi)          # first idx with ts > win_hi
        out = []
        for i in range(lo, hi):
            out.append({"ts": int(self.ts[i]), "coin": _COINS[self.coin_id[i]],
                        "is_open": bool(self.is_open[i]), "is_long": bool(self.is_long[i])})
        return out


# ----- copy lot accounting ------------------------------------------------------------------ #
def copy_edge(fills: list[dict], win_lo: int, win_hi: int, latency_ms: int, adverse_bps: float,
              realistic_slip: bool = True, fee_rt: float = FEE_RT, exit_mode: str = "mirror"):
    """Copy ONLY opens whose ts is in (win_lo, win_hi]. Each open = one unit lot, entered at the
    mark `latency_ms` after the fill. Cross-spread slippage is applied on BOTH entry and exit. With
    realistic_slip=True (default), uses the per-coin measured half_spread+impact (l2_calib); else the
    flat `adverse_bps` on entry only (legacy A/B). Close our oldest matching lot (FIFO, per
    coin+direction) when the wallet emits a matching Close; force-close survivors at win_hi. Net of
    `fee_rt`. Returns (net_per_lot_mean, lots): {coin, is_long, entry_ts, entry_px, exit_ts, exit_px, net}."""
    open_lots: dict[tuple, list] = {}     # (coin,is_long) -> [lot,...] FIFO
    done = []
    adv = adverse_bps / 10_000.0
    for f in fills:
        # codex #1 (CRITICAL look-ahead): fills are sorted; once past win_hi, NOTHING in the window
        # may use it. A close after win_hi must NOT close an in-window lot (that was future leakage).
        # Survivors are force-closed at win_hi below.
        if f["ts"] > win_hi:
            break
        coin, is_long = f["coin"], f["is_long"]
        if f["is_open"]:
            # codex #2: entry mark is taken at fill+latency; for the rank to be causal at the decision
            # boundary, require the ENTRY mark time to be within the window too (drops the last
            # `latency` of opens, consistent across top/null/forward).
            if not (win_lo < f["ts"] and f["ts"] + latency_ms <= win_hi):
                continue
            ent = mark_at(coin, f["ts"] + latency_ms)
            if ent is None or ent <= 0:
                continue
            s = slip_oneway(coin) if realistic_slip else adv      # cross the spread to enter
            ent_adj = ent * (1 + s) if is_long else ent * (1 - s)
            open_lots.setdefault((coin, is_long), []).append(
                {"coin": coin, "is_long": is_long, "entry_ts": f["ts"] + latency_ms, "entry_px": ent_adj})
        elif exit_mode == "mirror":
            # wallet closes (ts <= win_hi only, guaranteed by the break above) -> FIFO-close our
            # oldest copied lot of the SAME coin+direction. (oracle mode ignores wallet closes:
            # every lot exits at its best ex-post mark below.)
            key = (coin, is_long)
            lots = open_lots.get(key)
            if lots:
                ex_ts = f["ts"] + latency_ms
                if ex_ts > win_hi:
                    continue   # exit mark would fall outside window; leave for force-close at win_hi
                lot = lots.pop(0)
                ex = mark_at(coin, ex_ts)
                if ex is None or ex <= 0:
                    lots.insert(0, lot)   # cannot price exit now; keep, force-close later
                    continue
                _finalize(lot, ex_ts, ex, done, realistic_slip, fee_rt)
    # close survivors: oracle = best ex-post mark in (entry, win_hi]; mirror = win_hi mark.
    for key, lots in open_lots.items():
        for lot in lots:
            ex_ts = win_hi
            if exit_mode == "oracle":
                ex = best_mark(lot["coin"], lot["entry_ts"], win_hi, lot["is_long"])
            elif exit_mode == "trail":
                r = trail_exit(lot["coin"], lot["entry_ts"], win_hi, lot["is_long"], TRAIL_FRAC)
                ex = None if r is None else r[1]
                if r is not None:
                    ex_ts = r[0]
            else:
                ex = mark_at(lot["coin"], win_hi)
            if ex is None or ex <= 0:
                continue
            _finalize(lot, ex_ts, ex, done, realistic_slip, fee_rt)
    if not done:
        return None, []
    net_mean = float(np.mean([l["net"] for l in done]))
    return net_mean, done


def _finalize(lot, exit_ts, exit_px, done, realistic_slip=True, fee_rt=FEE_RT):
    # cross the spread to EXIT too: sell a long into the bid, buy back a short at the ask.
    s = slip_oneway(lot["coin"]) if realistic_slip else 0.0
    exit_fill = exit_px * (1 - s) if lot["is_long"] else exit_px * (1 + s)
    if lot["is_long"]:
        gross = (exit_fill - lot["entry_px"]) / lot["entry_px"]
    else:
        gross = (lot["entry_px"] - exit_fill) / lot["entry_px"]
    lot["exit_ts"] = exit_ts
    lot["exit_px"] = exit_fill
    lot["net"] = gross - fee_rt
    done.append(lot)


def beta_edge_for_lots(lots: list[dict], t0: int, t1: int,
                       realistic_slip: bool = True, fee_rt: float = FEE_RT,
                       exit_mode: str = "mirror") -> float | None:
    """DECISION-ANCHORED beta (codex #3): for each top lot's (coin, side), the market return of just
    BEING in that coin/side over the WHOLE forward window [t0, t1] -- entries NOT at the wallet's
    chosen timestamp. top_fwd - beta isolates the wallet's entry-TIMING + exit-SELECTION skill above
    simply holding the basket the top decile traded.

    Beta pays the SAME execution (per-coin cross-spread slippage on entry+exit + fee) as the copied
    lots. codex fix: ONE beta obs PER TOP LOT (NOT deduped) so the execution frequency/weighting
    matches top exactly -> top_minus_beta cancels execution and churn, leaving a clean SKILL estimate.
    (A wallet that opens BTC 3x gets 3 identical t0->t1 BTC beta obs, matching its 3 copied lots.)"""
    rets = []
    for l in lots:
        e = mark_at(l["coin"], t0)
        # benchmark exit matches the strategy's exit mode (same privilege/policy), passive entry at t0.
        if exit_mode == "oracle":
            x = best_mark(l["coin"], t0, t1, l["is_long"])
        elif exit_mode == "trail":
            r = trail_exit(l["coin"], t0, t1, l["is_long"], TRAIL_FRAC)
            x = None if r is None else r[1]
        else:
            x = mark_at(l["coin"], t1)
        if e is None or x is None or e <= 0:
            continue
        s = slip_oneway(l["coin"]) if realistic_slip else 0.0
        e_fill = e * (1 + s) if l["is_long"] else e * (1 - s)     # enter crossing spread
        x_fill = x * (1 - s) if l["is_long"] else x * (1 + s)     # exit crossing spread
        g = (x_fill - e_fill) / e_fill if l["is_long"] else (e_fill - x_fill) / e_fill
        rets.append(g - fee_rt)
    return float(np.mean(rets)) if rets else None


# ----- main sim ----------------------------------------------------------------------------- #
def run(candidates: list[str], start_ms: int, end_ms: int, *, trail_min: int, hold_min: int, exit_mode: str = "mirror",
        latency_s: int, adverse_bps: float, top_frac: float, null_mult: int, seed: int,
        out_path: str, source: str = "m02", step_hours: int = 1) -> str:
    trail_ms, hold_ms, lat_ms = trail_min * 60_000, hold_min * 60_000, latency_s * 1000
    rng = np.random.default_rng(seed)
    load_lo, load_hi = start_ms - trail_ms, end_ms + hold_ms

    if source == "m02":
        # FULL-UNIVERSE open/close from the m02 journey-trace parquet (no API). One streamed pass.
        log.info(f"loading m02 events for {len(candidates)} wallets from {M02_ACTIONS.name} ...")
        wf = load_events_from_m02(set(candidates), load_lo, load_hi)
        log.info(f"{len(wf)} wallets with events (of {len(candidates)} candidates)")
    else:
        # legacy API path (per-wallet; slow, rate-limited). codex #7: failures DROP the wallet.
        log.info(f"loading fills via HL API for {len(candidates)} wallets ...")
        wf, n_incomplete = {}, 0
        for i, w in enumerate(candidates, 1):
            try:
                f = load_wallet_opens_closes(w, load_lo, load_hi)
            except FetchError as e:
                n_incomplete += 1
                log.warning(f"  DROP incomplete wallet {w[:10]}: {e}")
                f = None
            if f:
                wf[w] = WalletEvents.from_tuples(
                    [(d["ts"], _coin_id(d["coin"]), d["is_open"], d["is_long"]) for d in f])
            if i % 50 == 0:
                log.info(f"  fills {i}/{len(candidates)} ({len(wf)} ok, {n_incomplete} dropped)")
        log.info(f"{len(wf)} wallets with complete fills ({n_incomplete} dropped incomplete)")

    wallets = list(wf.keys())
    if len(wallets) < 20:
        raise SystemExit(f"too few wallets with events ({len(wallets)}); widen candidates/window")
    step_ms = step_hours * HOUR_MS

    aw = ShardedParquetWriter(out_path, flush_rows=200_000)
    by_wallet_path = out_path.replace(".parquet", "_bywallet.parquet")
    bw = ShardedParquetWriter(by_wallet_path, flush_rows=200_000)
    t = start_ms
    n_dec = 0
    while t + hold_ms <= end_ms:
        # 1) CLEAN trailing rank (causal: copy_edge only sees fills <= t). Track edge AND trailing lot
        #    count per wallet for a CAUSAL activity-matched null (codex #4/#5).
        trailing, trail_n = {}, {}
        for w in wallets:
            edge, lots = copy_edge(wf[w].slice_dicts(t - trail_ms, t), t - trail_ms, t, lat_ms, adverse_bps)
            if edge is not None and lots:
                trailing[w] = edge
                trail_n[w] = len(lots)
        if len(trailing) >= 10:
            ranked = sorted(trailing, key=lambda w: trailing[w], reverse=True)
            k = max(1, int(len(ranked) * top_frac))
            top = ranked[:k]
            top_set = set(top)
            # CAUSAL activity buckets over the RANK-ELIGIBLE set only, on trailing lot count at t.
            elig = list(trailing.keys())
            tn = pd.Series([trail_n[w] for w in elig])
            nq = min(5, max(1, tn.nunique()))
            bkt = pd.qcut(tn.rank(method="first"), q=nq, labels=False) if nq > 1 else pd.Series([0] * len(elig))
            w_bucket = {w: int(b) for w, b in zip(elig, bkt)}
            # 2) forward edge for top decile (causal: copy_edge only sees (t, t+hold]). Record
            #    PER-WALLET forward edge so concentration (codex criterion #5) is computable offline.
            top_lots = []
            top_wallet_rows = []
            for w in top:
                _, lots = copy_edge(wf[w].slice_dicts(t, t + hold_ms), t, t + hold_ms, lat_ms, adverse_bps, exit_mode=exit_mode)
                top_lots += lots
                if lots:
                    we = float(np.mean([l["net"] for l in lots]))
                    wb = beta_edge_for_lots(lots, t, t + hold_ms, exit_mode=exit_mode)
                    top_wallet_rows.append({
                        "decision_ts": t, "wallet": w, "n_lots": len(lots),
                        "fwd_edge_bps": we * 1e4,
                        "beta_bps": (wb * 1e4) if wb is not None else None,
                        "trailing_edge_bps": trailing[w] * 1e4})
            if top_wallet_rows:
                bw.add_many(top_wallet_rows)
            top_edge = float(np.mean([l["net"] for l in top_lots])) if top_lots else None
            beta = beta_edge_for_lots(top_lots, t, t + hold_ms, exit_mode=exit_mode) if top_lots else None
            # 3) matched null: rank-eligible NON-top wallets, matched on causal trailing-activity bucket
            null_lots = []
            top_bucket_counts = {}
            for w in top:
                top_bucket_counts[w_bucket[w]] = top_bucket_counts.get(w_bucket[w], 0) + 1
            pool_by_bucket = {}
            for w in elig:
                if w in top_set:
                    continue
                pool_by_bucket.setdefault(w_bucket[w], []).append(w)
            for b, cnt in top_bucket_counts.items():
                pool = pool_by_bucket.get(b, [])
                if not pool:
                    continue
                pick = rng.choice(pool, size=min(len(pool), cnt * null_mult), replace=False)
                for w in pick:
                    _, lots = copy_edge(wf[w].slice_dicts(t, t + hold_ms), t, t + hold_ms, lat_ms, adverse_bps, exit_mode=exit_mode)
                    null_lots += lots
            null_edge = float(np.mean([l["net"] for l in null_lots])) if null_lots else None
            # diagnostics: symbol/side overlap between top and null forward baskets (codex #5)
            top_keys = {(l["coin"], l["is_long"]) for l in top_lots}
            null_keys = {(l["coin"], l["is_long"]) for l in null_lots}
            overlap = len(top_keys & null_keys) / max(1, len(top_keys))
            aw.add_many([{
                "decision_ts": t, "n_eligible": len(trailing), "n_top": len(top),
                "n_top_lots": len(top_lots), "n_null_lots": len(null_lots),
                "top_null_symside_overlap": overlap,
                "top_fwd_edge_bps": (top_edge * 1e4) if top_edge is not None else None,
                "null_fwd_edge_bps": (null_edge * 1e4) if null_edge is not None else None,
                "beta_edge_bps": (beta * 1e4) if beta is not None else None,
                "top_minus_null_bps": ((top_edge - null_edge) * 1e4)
                    if (top_edge is not None and null_edge is not None) else None,
                "top_minus_beta_bps": ((top_edge - beta) * 1e4)
                    if (top_edge is not None and beta is not None) else None,
            }])
            n_dec += 1
            if n_dec % 50 == 0:
                log.info(f"  decisions {n_dec} (t={pd.to_datetime(t, unit='ms')})")
        t += step_ms
    n = aw.close()
    nb = bw.close()
    share, n_calib, n_def = calibrated_share()
    log.info(f"streamed {n} decision rows -> {out_path}; {nb} per-wallet rows -> {by_wallet_path}")
    log.info(f"[exec] calibrated-coin slip share: {share:.1f}% ({n_calib} calib / {n_def} default)")
    return out_path


def pick_candidates(max_wallets: int, start_ms: int, end_ms: int) -> list[str]:
    df = pd.read_parquet(ASSETCTX_DIR.parent / "m03_wallet_activity_summary.parquet")
    df = df[df["key_kind"] == "wallet"].copy()
    last = pd.to_datetime(df["last_seen_ts"], utc=True)
    cutoff = pd.Timestamp(start_ms, unit="ms", tz="UTC")
    df = df[last >= cutoff]                                     # active into the window
    df = df.sort_values("total_journeys", ascending=False).head(max_wallets)
    return df["key"].tolist()


def main():
    ap = argparse.ArgumentParser(description="Lead-lag clean-rank salvage sim (memory-safe)")
    ap.add_argument("--source", choices=["m02", "api"], default="m02",
                    help="m02 = full-universe open/close from m02_actions.parquet (no API, default); "
                         "api = legacy per-wallet HL API (slow, small scope only).")
    ap.add_argument("--start", default="2025-12-01", help="default Dec 1 (full m02 history).")
    ap.add_argument("--end", default="2026-05-23")
    ap.add_argument("--max-wallets", type=int, default=0,
                    help="candidate cap (0 = ALL wallets in the universe file; >0 caps, mainly for api).")
    ap.add_argument("--universe-file", default="app/data/v15/m01_nonerroring_wallets.txt",
                    help="full-universe wallet list for --source m02 (one address/line).")
    ap.add_argument("--candidates-file", default=None, help="override universe; one wallet/line.")
    ap.add_argument("--trail-min", type=int, default=60)
    ap.add_argument("--hold-min", type=int, default=60)
    ap.add_argument("--decision-step-hours", type=int, default=1,
                    help="hours between decision points (raise to coarsen the grid for tractability).")
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--adverse-bps", type=float, default=7.0)
    ap.add_argument("--top-frac", type=float, default=0.10)
    ap.add_argument("--null-mult", type=int, default=3, help="null wallets sampled per top wallet/bucket.")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--slip-default-bps", type=float, default=4.7,
                    help="one-way slippage for coins not in l2_calib (sensitivity knob; ~midcap=4.7).")
    ap.add_argument("--headroom-gb", type=float, default=6.0)
    ap.add_argument("--per-worker-gb", type=float, default=3.0,
                    help="main-process peak-RSS estimate. m02 full-universe load is ~2-3GB (events for "
                         "~18k wallets + 0.73GB marks-cache ceiling). The memory guard aborts loud above it.")
    ap.add_argument("--exit-mode", choices=["mirror", "oracle", "trail"], default="mirror",
                    help="mirror=follow wallet exits; oracle=best ex-post exit (#2 ceiling, not tradeable); "
                         "trail=realizable trailing-stop (#9).")
    ap.add_argument("--trail-frac", type=float, default=0.01, help="trailing-stop retracement for --exit-mode trail.")
    ap.add_argument("--out", default="app/data/v15/leadlag_clean_rank.parquet")
    args = ap.parse_args()

    # configure the CANONICAL execution model (single source of truth)
    set_slip_default_bps(args.slip_default_bps)
    set_latency_ms(args.latency_s * 1000)
    reset_hits()
    global TRAIL_FRAC
    TRAIL_FRAC = args.trail_frac

    # memory budget: serial (one process); aborts before work if the box cannot fit it.
    b = plan_memory_budget(requested_procs=1, per_worker_gb=args.per_worker_gb, main_reserve_gb=0.0,
                           headroom_gb=args.headroom_gb, main_soft_cap=12.0)
    install_memory_guard(soft_gb=b.main_soft_gb, label="leadlag")

    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)

    if args.candidates_file:
        cands = [l.strip().lower() for l in open(args.candidates_file)
                 if l.strip() and not l.startswith("#")]
    elif args.source == "m02":
        cands = [l.strip().lower() for l in open(args.universe_file)
                 if l.strip() and not l.startswith("#")]
    else:
        cands = pick_candidates(args.max_wallets or 300, start_ms, end_ms)
    if args.max_wallets and args.max_wallets > 0:
        cands = cands[:args.max_wallets]
    log.info(f"source={args.source} {len(cands)} candidate wallets; window {args.start}..{args.end}; "
             f"trail={args.trail_min}m hold={args.hold_min}m step={args.decision_step_hours}h "
             f"top_frac={args.top_frac}")
    run(cands, start_ms, end_ms, trail_min=args.trail_min, hold_min=args.hold_min, exit_mode=args.exit_mode,
        latency_s=args.latency_s, adverse_bps=args.adverse_bps, top_frac=args.top_frac,
        null_mult=args.null_mult, seed=args.seed, out_path=args.out,
        source=args.source, step_hours=args.decision_step_hours)


if __name__ == "__main__":
    main()
