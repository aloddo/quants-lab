#!/usr/bin/env python3
"""V15 biweekly copy-selector -- implements the PRE-REGISTERED frozen rule
(projects/quant/v15/2026-06-06-preregistered-shorthorizon-selector-test).

Built ready-to-run so the live pilot / forward test starts the instant Alberto says go (lesson from the
"8h idle" correction: have it ready to RUN). NOTHING here trades; it only SELECTS a wallet set from live
HL data. The live copy engine (hl_prop_copy.py) consumes the output config; it stays paused until Alberto deploys.

Frozen rule (params overridable so threshold tweaks are CONFIG, not code rework):
  - eligibility: round_trip_win_rate >= WR_MIN (0.70) AND max_dd_14d <= DD_MAX (0.15) AND n_round_trips >= RT_MIN (15)
  - rank: 0.5*z(win_rate) + 0.5*z(neg max_dd)   [NO return weight -- return does not predict next period]
  - select: top TOP_K (5), equal weight
  - regime guard: if BTC fell > REGIME_DROP (10%) in trailing REGIME_HRS (48h), SKIP this window (flat)

Features are trailing-LOOKBACK_DAYS, computed from LIVE HL fills + portfolio (flow-neutral), matching the
pipeline's definitions as closely as the live API allows.
"""
from __future__ import annotations
import argparse, time, json
from dataclasses import dataclass, asdict
import concurrent.futures as cf
import numpy as np
import pandas as pd
import requests

HL = "https://api.hyperliquid.xyz/info"


@dataclass
class SelParams:
    wr_min: float = 0.70
    dd_max: float = 0.15
    rt_min: int = 15
    top_k: int = 5
    w_winrate: float = 0.5
    w_negdd: float = 0.5
    lookback_days: int = 14
    regime_drop: float = 0.10     # BTC trailing-48h drop that triggers the flat/skip guard
    regime_hours: int = 48
    min_equity: float = 2000.0    # M5 base floor
    # MARTINGALE/BAG GUARD (pre-reg v2, 2026-06-06): realized win-rate+DD is GAMED by bag-holders that
    # close only winners and HOLD losers (100% win rate + 0 realized DD while sitting on a deep unrealized
    # loss -- e.g. a -18% ZEC bag). The live unrealized-loss check is the decisive martingale filter
    # (caught 0xae6b80's -$102k bag). EXCLUDE if holding a material open loss OR a sprawling un-copyable book.
    worst_pos_floor: float = -0.06   # exclude if ANY open position uPnL% <= -6% (holding a real loser)
    open_loss_floor: float = -0.03   # exclude if TOTAL open uPnL <= -3% of equity
    max_open_positions: int = 10     # exclude sprawling books (un-copyable at our size; diversified != edge)


def _zscore(s: pd.Series) -> pd.Series:
    sd = s.std(ddof=0)
    return (s - s.mean()) / sd if sd and sd == sd and sd > 0 else s * 0.0


def regime_guard_triggered(p: SelParams, now_ms: int | None = None) -> bool:
    """True if BTC dropped > regime_drop over the trailing regime_hours (the signal dies at regime
    transitions -> stay flat). Uses HL 1h candles."""
    now_ms = now_ms or int(time.time() * 1000)
    start = now_ms - (p.regime_hours + 2) * 3600 * 1000
    try:
        c = requests.post(HL, json={"type": "candleSnapshot",
                                    "req": {"coin": "BTC", "interval": "1h", "startTime": start, "endTime": now_ms}},
                          timeout=12).json()
        if not c:
            return True  # no data -> fail-safe (skip)
        closes = [float(x["c"]) for x in c]
        hi = max(closes)
        return (closes[-1] / hi - 1.0) <= -p.regime_drop
    except Exception:
        return True  # fail-safe: skip the window if we cannot assess regime


def compute_features(wallet: str, p: SelParams, now_ms: int | None = None) -> dict:
    """Trailing-lookback features from live HL fills + clearinghouse (flow-neutral)."""
    now_ms = now_ms or int(time.time() * 1000)
    start = now_ms - p.lookback_days * 86400 * 1000
    try:
        fills, s, seen = [], start, set()
        for _ in range(8):
            r = requests.post(HL, json={"type": "userFillsByTime", "user": wallet,
                                        "startTime": s, "endTime": now_ms}, timeout=12).json()
            if not isinstance(r, list) or not r:
                break
            fills += r
            if len(r) < 2000:
                break
            s = max(f["time"] for f in r) + 1
        rt_pnls = []
        for f in fills:
            k = (f.get("time"), f.get("oid"), f.get("tid"))
            if k in seen:
                continue
            seen.add(k)
            cp = float(f.get("closedPnl", 0) or 0)
            if cp != 0:                       # a closing fill = a realized round-trip leg
                rt_pnls.append(cp - float(f.get("fee", 0) or 0))
        n_rt = len(rt_pnls)
        win_rate = (sum(1 for x in rt_pnls if x > 0) / n_rt) if n_rt else 0.0
        # max_dd from the cumulative realized-PnL path (proxy; flow-neutral)
        cum = np.cumsum(rt_pnls) if n_rt else np.array([0.0])
        peak = np.maximum.accumulate(cum)
        ch = requests.post(HL, json={"type": "clearinghouseState", "user": wallet}, timeout=10).json()
        av = float(ch.get("marginSummary", {}).get("accountValue", 0) or 0)
        denom = max(av, 1.0)
        max_dd = float((-(cum - peak)).max() / denom) if n_rt else 0.0
        # LIVE-BAG metrics (martingale guard): open unrealized-loss exposure the realized stats hide.
        open_pos = [pp["position"] for pp in ch.get("assetPositions", []) if abs(float(pp["position"]["szi"])) > 0]
        tot_uupnl = sum(float(pp.get("unrealizedPnl", 0) or 0) for pp in open_pos)
        worst_pct = 0.0
        for pp in open_pos:
            pv = float(pp.get("positionValue", 0) or 0)
            up = float(pp.get("unrealizedPnl", 0) or 0)
            if pv > 0:
                worst_pct = min(worst_pct, up / pv)
        return {"wallet": wallet, "round_trip_win_rate": win_rate, "max_dd_14d": max_dd,
                "n_round_trips": n_rt, "equity": av,
                "open_loss_frac": (tot_uupnl / denom), "worst_pos_pct": worst_pct,
                "n_open_positions": len(open_pos)}
    except Exception as e:  # noqa: BLE001
        return {"wallet": wallet, "round_trip_win_rate": 0.0, "max_dd_14d": 9.99,
                "n_round_trips": 0, "equity": 0.0, "open_loss_frac": -9.99,
                "worst_pos_pct": -9.99, "n_open_positions": 999, "err": str(e)[:40]}


def select(cand: pd.DataFrame, p: SelParams) -> pd.DataFrame:
    """Apply the FROZEN rule to a candidates DataFrame (cols: wallet, round_trip_win_rate, max_dd_14d,
    n_round_trips, equity). Returns the selected top-K with a `score`, or empty if none eligible.
    Pure + deterministic (unit-tested)."""
    elig = cand[(cand["round_trip_win_rate"] >= p.wr_min)
                & (cand["max_dd_14d"] <= p.dd_max)
                & (cand["n_round_trips"] >= p.rt_min)
                & (cand["equity"] >= p.min_equity)
                # MARTINGALE/BAG guard: reject wallets holding a material open loss or a sprawling book
                & (cand["worst_pos_pct"] >= p.worst_pos_floor)
                & (cand["open_loss_frac"] >= p.open_loss_floor)
                & (cand["n_open_positions"] <= p.max_open_positions)].copy()
    if elig.empty:
        return elig.assign(score=pd.Series(dtype=float))
    elig["score"] = (p.w_winrate * _zscore(elig["round_trip_win_rate"])
                     + p.w_negdd * _zscore(-elig["max_dd_14d"]))
    return elig.sort_values("score", ascending=False).head(p.top_k).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True, help="candidate wallet universe (one addr/line)")
    ap.add_argument("--out", default="config/copy_trader_wallets_v15_selected.json")
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--wr-min", type=float, default=0.70)
    ap.add_argument("--dd-max", type=float, default=0.15)
    args = ap.parse_args()
    p = SelParams(top_k=args.top_k, wr_min=args.wr_min, dd_max=args.dd_max)
    if regime_guard_triggered(p):
        print("REGIME GUARD TRIGGERED (BTC trailing-48h drop > %.0f%%) -> SELECT NOTHING (stay flat)." % (p.regime_drop * 100))
        json.dump({"global": {"regime_guard": "triggered"}, "wallets": {}}, open(args.out, "w"), indent=2)
        return
    wl = [w.strip() for w in open(args.wallets_file) if w.strip()]
    print(f"computing trailing-{p.lookback_days}d features for {len(wl)} candidates...")
    rows = []
    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        rows = list(ex.map(lambda w: compute_features(w, p), wl))
    cand = pd.DataFrame(rows)
    sel = select(cand, p)
    print(f"eligible {((cand['round_trip_win_rate']>=p.wr_min)&(cand['max_dd_14d']<=p.dd_max)&(cand['n_round_trips']>=p.rt_min)&(cand['equity']>=p.min_equity)).sum()}; selected {len(sel)}")
    if len(sel):
        print(sel[["wallet", "round_trip_win_rate", "max_dd_14d", "n_round_trips", "equity", "score"]].to_string(index=False))
    cfg = {"global": {"selector_params": asdict(p), "regime_guard": "ok"},
           "wallets": {r.wallet: {"group": "v15_biweekly", "win_rate": r.round_trip_win_rate,
                                  "max_dd_14d": r.max_dd_14d} for r in sel.itertuples()}}
    json.dump(cfg, open(args.out, "w"), indent=2)
    print(f"wrote selection -> {args.out}")


if __name__ == "__main__":
    main()
