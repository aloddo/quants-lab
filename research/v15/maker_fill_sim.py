#!/usr/bin/env python3
"""Realistic MAKER passive-fill simulation -- the deployability gate for copy selection.

The persistence test showed a real but thin selection edge (+2.4bps OOS top decile) ASSUMING 100%
maker fills. Real maker orders only fill if the market trades to your posted price within a timeout,
and you fill disproportionately on adverse moves (you buy the dips that keep dipping). This models
that: post at bid/ask, fill only if the mark touches your price within fill_timeout, count PnL only on
FILLED orders. Reports fill rate + filled-only edge, and re-runs the train/test persistence under it.

Run: python research/v15/maker_fill_sim.py --start 2025-12-01 --split 2026-03-15 --end 2026-05-17
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, set_latency_ms

_DATA = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
_HALF = {}
POST_OFFSET_MULT = 1.0   # 1.0 = post at bid/ask (passive, adverse); 0.0 = post at mark (reactive/leader)
def half_spread(coin):
    if not _HALF:
        try:
            for c, v in json.load(open(_DATA / "l2_calib_10coin.json")).items():
                _HALF[c] = float(v.get("half_spread_bps") or 0.0) / 1e4
        except Exception:
            pass
    return _HALF.get(coin, 4.7 / 1e4) * POST_OFFSET_MULT   # scaled by posting aggression


def _seg(coin, lo_ts, hi_ts):
    m = S._load_marks(coin)
    if m is None:
        return None
    ts, px = m
    from bisect import bisect_right
    lo = bisect_right(ts, lo_ts); hi = bisect_right(ts, hi_ts)
    return px[lo:hi] if hi > lo else None


def score_wallet_filled(we, start_ms, end_ms, hold_ms, lat_ms, fee, fill_timeout_ms):
    """Maker passive-fill: post at bid (buy)/ask (sell); fill only if mark touches it within timeout;
    fill at the posted price; exit at entry+hold (maker, mark). Returns filled-only stats + fill rate."""
    ev = we.slice_dicts(start_ms, end_ms)
    nets = []; n_posts = 0; n_fills = 0
    for f in ev:
        if not f["is_open"]:
            continue
        pt = f["ts"] + lat_ms
        if pt + fill_timeout_ms + hold_ms > end_ms:
            continue
        mk = S.mark_at(f["coin"], pt)
        if mk is None or mk <= 0:
            continue
        n_posts += 1
        hs = half_spread(f["coin"])
        post = mk * (1 - hs) if f["is_long"] else mk * (1 + hs)   # post at bid (buy) / ask (sell)
        seg = _seg(f["coin"], pt, pt + fill_timeout_ms)
        if seg is None or len(seg) == 0:
            continue
        # fill if market trades to your price: buy fills if low<=post; sell fills if high>=post
        filled = (seg.min() <= post) if f["is_long"] else (seg.max() >= post)
        if not filled:
            continue
        n_fills += 1
        ex = S.mark_at(f["coin"], pt + fill_timeout_ms + hold_ms)
        if ex is None or ex <= 0:
            continue
        g = (ex - post) / post if f["is_long"] else (post - ex) / post
        nets.append(g - fee)
    if n_posts == 0:
        return None
    fr = n_fills / n_posts
    if not nets:
        return {"n_posts": n_posts, "fill_rate": fr, "n_fills": n_fills, "mean_bps": None}
    a = np.array(nets)
    return {"n_posts": n_posts, "fill_rate": fr, "n_fills": len(a),
            "mean_bps": float(a.mean() * 1e4), "win": float((a > 0).mean() * 100)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--split", default="2026-03-15")
    ap.add_argument("--end", default="2026-05-17")
    ap.add_argument("--hold-min", type=int, default=60)
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--fill-timeout-min", type=int, default=15)
    ap.add_argument("--post-offset-mult", type=float, default=1.0,
                    help="1.0=post at bid/ask (passive, adverse); 0.0=post at mark (reactive/join-leader).")
    ap.add_argument("--min-fills", type=int, default=20)
    ap.add_argument("--universe-file", default="app/data/v15/m01_universe_20k_wallets.txt")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    global POST_OFFSET_MULT
    POST_OFFSET_MULT = args.post_offset_mult
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, split, end = ms(args.start), ms(args.split), ms(args.end)
    hold_ms, lat_ms = args.hold_min * 60_000, args.latency_s * 1000
    fto, fee = args.fill_timeout_min * 60_000, fee_rt(maker=True)

    uni = [l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#")]
    print(f"loading {len(uni)} wallets; maker fill-sim (post@bid/ask, {args.fill_timeout_min}m timeout, "
          f"{args.hold_min}m hold) {args.start}..{args.end} ...")
    wf = S.load_events_from_m02(set(uni), start - hold_ms, end)
    rows = []
    for w, we in wf.items():
        tr = score_wallet_filled(we, start, split, hold_ms, lat_ms, fee, fto)
        te = score_wallet_filled(we, split, end, hold_ms, lat_ms, fee, fto)
        if (tr and te and tr["mean_bps"] is not None and te["mean_bps"] is not None
                and tr["n_fills"] >= args.min_fills and te["n_fills"] >= args.min_fills):
            rows.append({"wallet": w, "train_bps": tr["mean_bps"], "train_fr": tr["fill_rate"],
                         "test_bps": te["mean_bps"], "test_fr": te["fill_rate"], "test_fills": te["n_fills"]})
    df = pd.DataFrame(rows)
    if len(df) < 20:
        print(f"only {len(df)} wallets w/ enough FILLED trades both windows -> fills too rare at these params")
        if len(df): print(df.describe().round(1).to_string())
        return
    print(f"\n=== maker FILL-SIM persistence ({len(df)} wallets, >= {args.min_fills} fills both windows) ===")
    print(f"avg fill rate: train {df.train_fr.mean()*100:.0f}% test {df.test_fr.mean()*100:.0f}%")
    print(f"corr(train,test) filled edge: {df.train_bps.corr(df.test_bps):.3f}")
    df = df.sort_values("train_bps", ascending=False).reset_index(drop=True)
    dec = max(5, len(df) // 10)
    top, bot = df.head(dec), df.tail(dec)
    print(f"train-TOP decile: train {top.train_bps.mean():.1f} -> TEST {top.test_bps.mean():.1f}bps (filled-only)")
    print(f"train-BOTTOM decile: TEST {bot.test_bps.mean():.1f}bps | ALL test mean {df.test_bps.mean():.1f}bps")
    edge = top.test_bps.mean() - df.test_bps.mean()
    print(f"\nVERDICT: top-vs-random OOS (filled) = {edge:+.1f}bps -> "
          f"{'SURVIVES fills' if edge > 3 and top.test_bps.mean() > 0 else 'DOES NOT survive realistic fills'}")
    df.to_parquet("app/data/v15/maker_fill_persistence.parquet")


if __name__ == "__main__":
    main()
