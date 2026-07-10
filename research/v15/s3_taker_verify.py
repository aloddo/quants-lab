#!/usr/bin/env python3
"""VERIFY: does the TAKER roundtrip edge computed from LOCAL S3 fills reconcile with the API-taker edge
(revalidate_api_execmodel)? If yes, S3 fills are a faithful, LOCAL, live-safe source for the selection
consistency gate (fixes the m02 position-event infidelity, 2026-07-08).

Reads app/data/hl_s3_fills_v2_by_wallet/{w}.parquet -> (ts, coin, signed_sz, price) -> roundtrips() ->
execution_model taker edge (marks-repriced at entry/exit ts, same as revalidate.edge()). Compares against
the known API-train (Jun1-14) numbers from stage-2.
"""
import sys, json
from pathlib import Path
import numpy as np, pandas as pd, pyarrow.parquet as pq
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, apply_entry, apply_exit

S3_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "hl_s3_fills_v2_by_wallet"
CAP = 500.0 / 1e4
FEE_T = fee_rt(maker=False)


def load_fills_from_s3(w, lo_ms, hi_ms):
    """LOCAL S3 fills -> sorted (ts, coin, signed_sz, price), same shape revalidate builds from the API."""
    fp = S3_DIR / f"{w}.parquet"
    if not fp.exists():
        return [], (None, None)
    d = pq.read_table(str(fp), columns=["coin", "time", "signed_sz", "price"]).to_pydict()
    coins, times, ssz, px = d["coin"], d["time"], d["signed_sz"], d["price"]
    out = []
    tmin = tmax = None
    for i in range(len(times)):
        t = int(times[i])
        tmin = t if tmin is None else min(tmin, t)
        tmax = t if tmax is None else max(tmax, t)
        if t < lo_ms or t >= hi_ms:
            continue
        c = coins[i]
        if not c or ssz[i] is None or px[i] is None:
            continue
        try:
            s = float(ssz[i]); p = float(px[i])
        except (TypeError, ValueError):
            continue
        if s == 0 or p <= 0:
            continue
        out.append((t, c, s, p))
    out.sort(key=lambda x: x[0])
    return out, (tmin, tmax)


def taker_edge_s3(w, lo_ms, hi_ms, lat_ms):
    # P0 fix (codex 2026-07-09): load FULL history [0, hi) so roundtrips() tracks true carry-in position
    # from flat -- window-truncated loading fabricates a fake opposite-side entry from a carry-in close.
    # Then filter resulting trips by entry_ts in [lo, hi).
    fills, cov = load_fills_from_s3(w, 0, hi_ms)
    if not fills:
        return None, cov
    nets = []
    for c, dir_, ets, xts, evw, xvw, g in roundtrips(fills):
        if not (lo_ms <= ets < hi_ms):
            continue
        em = S.mark_at(c, ets + lat_ms); xm = S.mark_at(c, xts + lat_ms)
        if em is None or xm is None or em <= 0:
            continue
        ef = apply_entry(c, em, dir_ > 0); xf = apply_exit(c, xm, dir_ > 0)
        og = max(-CAP, min(CAP, dir_ * (xf - ef) / ef))
        nets.append(og - fee_rt(maker=False, coin=c))  # P1 fix: coin-specific RT fee (HIP-3/xyz undercharged by const)
    if not nets:
        return None, cov
    a = np.array(nets)
    return {"n": len(a), "bps": float(a.mean() * 1e4)}, cov


def consistency_gate_s3(wallets, breaks_ms, lat_ms, min_trades):
    """N-window LEADER-LIVENESS + TAKER-CONSISTENCY gate sourced from LOCAL S3 fills (faithful + live-safe).
    PASS iff LIVE (n>=min_trades in EVERY window) AND CONSISTENT (taker bps>0 in EVERY window).
    Codex 2026-07-09 fixes: (P0) load FULL history ONCE per wallet -> roundtrips() tracks true carry-in,
    then bucket trips by entry_ts window (no fabricated carry-in entries; also faster than 4 truncated loads);
    (P1) coin-specific fee; (P1) any_fill keyed on RAW fills so stale-marks wallets stay visible (n=0)."""
    from bisect import bisect_right
    nwin = len(breaks_ms) - 1
    lo0 = breaks_ms[0]; hi = breaks_ms[-1]
    rows = []
    for w in wallets:
        fills, _ = load_fills_from_s3(w, 0, hi)      # full history up to last break; correct carry-in state
        rec = {"wallet": w}
        # P2 (codex r2): key visibility on IN-WINDOW raw fills, not full history (a wallet with only
        # pre-lo0 carry-in fills should not inflate the "had fills" diagnostic).
        any_fill = any(lo0 <= t < hi for t, _c, _s, _p in fills)
        buckets = [[] for _ in range(nwin)]
        for c, dir_, ets, xts, evw, xvw, g in roundtrips(fills):
            if ets < lo0 or ets >= hi:
                continue
            k = bisect_right(breaks_ms, ets) - 1
            if k < 0 or k >= nwin:
                continue
            em = S.mark_at(c, ets + lat_ms); xm = S.mark_at(c, xts + lat_ms)
            if em is None or xm is None or em <= 0:
                continue
            ef = apply_entry(c, em, dir_ > 0); xf = apply_exit(c, xm, dir_ > 0)
            og = max(-CAP, min(CAP, dir_ * (xf - ef) / ef))
            buckets[k].append(og - fee_rt(maker=False, coin=c))
        live_all = consistent_all = True
        active = 0
        for k in range(nwin):
            b = buckets[k]; n = len(b)
            bps = float(np.mean(b) * 1e4) if n else float("nan")
            rec[f"n_{k}"] = n; rec[f"bps_{k}"] = bps
            if n >= min_trades:
                active += 1
            else:
                live_all = False
            if not (n >= min_trades and bps > 0):
                consistent_all = False
        rec["active_wins"] = active; rec["live_all"] = live_all; rec["pass"] = live_all and consistent_all
        if any_fill:
            rows.append(rec)
    return pd.DataFrame(rows)


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true", help="7-wallet reconciliation vs API-taker (Jun1-14)")
    ap.add_argument("--universe-file", default="/tmp/taker_verify.txt")
    ap.add_argument("--windows", default="2026-02-01,2026-03-06,2026-04-08,2026-05-11,2026-06-24")
    ap.add_argument("--min-trades", type=int, default=10)
    ap.add_argument("--out", default="/tmp/s3_consistency_gate.parquet")
    args = ap.parse_args()
    lat_s = 2
    set_latency_ms(lat_s * 1000); lat_ms = lat_s * 1000
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)

    if args.verify:
        lo, hi = ms("2026-06-01"), ms("2026-06-14")
        api_train = {"0x04aa3ce6": "+114/n158", "0x453a932e": "+47/n23", "0x731582a6": "+62/n17",
                     "0x67e2b0a7": "+17/n17", "0x809fb2bd": "-8/n848", "0x0526345b": "n/a(0 train)",
                     "0x6bc1341f": "-5/n230"}
        wallets = [l.strip() for l in open("/tmp/taker_verify.txt") if l.strip()]
        print(f"{'wallet':<16}{'S3cov':>26}{'S3-taker Jun1-14':>20}  API-train(Jun1-14)")
        for w in wallets:
            r, cov = taker_edge_s3(w, lo, hi, lat_ms)
            covs = "-" if not cov[0] else \
                f"{pd.Timestamp(cov[0],unit='ms',tz='UTC').date()}..{pd.Timestamp(cov[1],unit='ms',tz='UTC').date()}"
            s3s = f"{r['bps']:+.0f}/n{r['n']}" if r else "n/a"
            print(f"{w[:14]:<16}{covs:>26}{s3s:>20}  {api_train.get(w[:10],'?')}")
        return

    breaks = [b.strip() for b in args.windows.split(",") if b.strip()]
    breaks_ms = [ms(b) for b in breaks]
    nwin = len(breaks) - 1
    wallets = [l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#")]
    import time as _t
    t0 = _t.time()
    df = consistency_gate_s3(wallets, breaks_ms, lat_ms, args.min_trades)
    dt = _t.time() - t0
    print(f"\n=== S3 TAKER consistency gate ({nwin} windows {breaks[0]}..{breaks[-1]}, min-trades={args.min_trades}) ===")
    print(f"scored {len(df)} wallets (had fills) of {len(wallets)} in {dt:.1f}s ({dt/max(1,len(wallets))*1000:.1f}ms/wallet)")
    if len(df):
        pass_df = df[df["pass"]]
        print(f"LIVE in all {nwin}: {df['live_all'].sum()} | PASS (live+bps>0 all): {len(pass_df)}")
        df.to_parquet(args.out)
        print(f"wrote -> {args.out}")
        for _, r in pass_df.head(30).iterrows():
            cells = " ".join(f"[{int(r[f'n_{k}'])}/{r[f'bps_{k}']:+.0f}]" for k in range(nwin))
            print(f"  {r['wallet'][:16]}  {cells}")


if __name__ == "__main__":
    main()
