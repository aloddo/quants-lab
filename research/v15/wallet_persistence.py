#!/usr/bin/env python3
"""Wallet-selection PERSISTENCE test (is the maker copy edge real or in-sample luck?).

Rank wallets by maker copy edge on a TRAIN window, then measure the SAME wallets' edge on a later
TEST window. If train-top stays high in test (and beats random/train-bottom), selection is a real,
deployable WHO. If train and test edges are uncorrelated, the wide score distribution was luck.

Run: python research/v15/wallet_persistence.py --start 2025-12-01 --split 2026-03-15 --end 2026-05-17
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from wallet_maker_scorer import score_wallet
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, apply_entry, apply_exit

_TAKER_CAP = 500.0 / 1e4          # per-trade cap, matches revalidate_api_execmodel
_FEE_TAKER = fee_rt(maker=False)


def taker_score_wallet(we, start_ms, end_ms, hold_ms, lat_ms, fee):
    """TAKER copy edge on m02 events, matching v17 deployment + revalidate_api_execmodel.edge():
    the wallet's ACTUAL roundtrips (FIFO entry->exit), TAKER spread-cross both sides + real taker fee,
    marks re-priced at entry/exit ts (+latency). Signature mirrors score_wallet (maker fixed-hold) so it
    is a drop-in for the consistency gate. `hold_ms`/`fee` are accepted for interface parity and unused
    (taker uses actual hold + its own taker fee). Fixes the maker/taker mismatch: maker-consistent wallets
    can be taker-negative (2026-07-08 finding)."""
    ev = we.slice_dicts(start_ms, end_ms)
    if not ev:
        return None
    fills = []
    for f in ev:
        sign = 1.0 if (f["is_open"] == f["is_long"]) else -1.0    # buy(+)/sell(-) of the underlying
        px = S.mark_at(f["coin"], f["ts"])                        # any >0 price; edge re-prices from marks
        if px is None or px <= 0:
            continue
        fills.append((f["ts"], f["coin"], sign, px))
    if not fills:
        return None
    fills.sort(key=lambda x: x[0])
    nets = []
    for c, dir_, ets, xts, evw, xvw, g in roundtrips(fills):
        if not (start_ms <= ets < end_ms):
            continue
        em = S.mark_at(c, ets + lat_ms); xm = S.mark_at(c, xts + lat_ms)
        if em is None or xm is None or em <= 0:
            continue
        ef = apply_entry(c, em, dir_ > 0); xf = apply_exit(c, xm, dir_ > 0)
        og = max(-_TAKER_CAP, min(_TAKER_CAP, dir_ * (xf - ef) / ef))
        nets.append(og - _FEE_TAKER)
    if not nets:
        return None
    a = np.array(nets)
    return {"n": len(a), "mean_bps": float(a.mean() * 1e4), "win": float((a > 0).mean() * 100)}


def consistency_gate(wf, breaks_ms, hold_ms, lat_ms, fee, min_trades, scorer=score_wallet):
    """N-window LEADER-LIVENESS + CONSISTENCY selection gate (2026-07-08).

    Both fresh-15 and the expansion-13 pool failed forward-OOS because they were selected by ONE
    window's edge -> one-month flukes that go dormant/thin afterward. This gate scores every wallet in
    EACH of the N-1 non-overlapping windows defined by `breaks_ms` and PASSES a wallet only if it is
    (a) LIVE -- n>=min_trades in EVERY window (kills dormant leaders), AND
    (b) CONSISTENT -- mean_bps>0 in EVERY window (kills single-window flukes).
    Returns (pass_df, all_df). all_df carries per-window n_* and bps_* for diagnostics.
    """
    nwin = len(breaks_ms) - 1
    rows = []
    for w, we in wf.items():
        rec = {"wallet": w}
        live_all = consistent_all = True
        active_wins = 0
        for k in range(nwin):
            sc = scorer(we, breaks_ms[k], breaks_ms[k + 1], hold_ms, lat_ms, fee)
            n = sc["n"] if sc else 0
            bps = sc["mean_bps"] if sc else float("nan")
            rec[f"n_{k}"] = n
            rec[f"bps_{k}"] = bps
            if n >= min_trades:
                active_wins += 1
            else:
                live_all = False
            if not (sc and n >= min_trades and bps > 0):
                consistent_all = False
        rec["active_wins"] = active_wins
        rec["live_all"] = live_all
        rec["pass"] = live_all and consistent_all
        # only keep wallets active in >=1 window (skip the fully-dead universe tail = noise)
        if active_wins >= 1:
            rows.append(rec)
    all_df = pd.DataFrame(rows)
    pass_df = all_df[all_df["pass"]].copy() if len(all_df) else all_df
    return pass_df, all_df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--split", default="2026-03-15")
    ap.add_argument("--end", default="2026-05-17")
    ap.add_argument("--windows", default=None,
                    help="N comma-separated ISO breakpoints for the multi-window consistency gate "
                         "(e.g. 2026-02-01,2026-03-01,2026-04-01,2026-05-01,2026-06-01,2026-06-11). "
                         "When set, runs the leader-liveness+consistency SELECTION gate instead of the "
                         "2-window persistence test. Spans the m02-covered range only (Dec1..Jun11).")
    ap.add_argument("--taker", action="store_true",
                    help="score the TAKER roundtrip edge (execution_model, matches v17 deployment) instead "
                         "of the maker fixed-hold entry-quality score. Fixes the maker/taker mismatch.")
    ap.add_argument("--hold-min", type=int, default=60)
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-trades", type=int, default=30)
    ap.add_argument("--out", default="app/data/v15/wallet_consistency_gate.parquet")
    ap.add_argument("--universe-file", default="app/data/v15/m01_universe_20k_wallets.txt")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, split, end = ms(args.start), ms(args.split), ms(args.end)
    hold_ms, lat_ms, fee = args.hold_min * 60_000, args.latency_s * 1000, fee_rt(maker=True)

    uni = [l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#")]

    # ---- N-window LEADER-LIVENESS + CONSISTENCY SELECTION GATE ----
    if args.windows:
        breaks = [b.strip() for b in args.windows.split(",") if b.strip()]
        if len(breaks) < 3:
            raise SystemExit("--windows needs >=3 breakpoints (>=2 windows) for a consistency gate.")
        breaks_ms = [ms(b) for b in breaks]
        span_lo, span_hi = breaks_ms[0], breaks_ms[-1]
        M02_MAX = ms("2026-06-11")  # m02_actions.parquet coverage ceiling (verified 2026-07-08)
        if span_hi > M02_MAX + 86_400_000:
            raise SystemExit(f"--windows end {breaks[-1]} exceeds m02 coverage (Jun11). Refresh m02 first.")
        scorer = taker_score_wallet if args.taker else score_wallet
        edge_kind = "TAKER roundtrip" if args.taker else "MAKER fixed-hold"
        print(f"loading {len(uni)} wallets {breaks[0]}..{breaks[-1]} for {len(breaks)-1}-window gate "
              f"[{edge_kind} edge] ...")
        wf = S.load_events_from_m02(set(uni), span_lo - hold_ms, span_hi)
        pass_df, all_df = consistency_gate(wf, breaks_ms, hold_ms, lat_ms, fee, args.min_trades, scorer=scorer)
        nwin = len(breaks) - 1
        print(f"\n=== consistency gate ({nwin} windows, {edge_kind} edge, min-trades={args.min_trades}/window) ===")
        print(f"universe scored (active in >=1 window): {len(all_df)}")
        if len(all_df):
            live = all_df[all_df["live_all"]]
            print(f"LIVE in ALL {nwin} windows: {len(live)}  ({(all_df['active_wins']==nwin).mean()*100:.1f}% of scored)")
            print(f"PASS (live in all AND bps>0 in all): {len(pass_df)}")
            import collections
            hist = collections.Counter(all_df["active_wins"])
            print("active-window histogram: " + " ".join(f"{k}win={hist.get(k,0)}" for k in range(nwin+1)))
        if len(pass_df):
            pass_df = pass_df.sort_values([f"bps_{k}" for k in range(nwin)], ascending=False)
            print(f"\nPASS wallets (n/bps per window):")
            for _, r in pass_df.head(40).iterrows():
                cells = " ".join(f"[{int(r[f'n_{k}'])}/{r[f'bps_{k}']:+.0f}]" for k in range(nwin))
                print(f"  {r['wallet'][:16]}  {cells}")
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        all_df.to_parquet(args.out)
        print(f"\nwrote full scored set -> {args.out} ({len(all_df)} rows, {len(pass_df)} PASS)")
        return
    # ---- default: original 2-window persistence test ----
    print(f"loading {len(uni)} wallets {args.start}..{args.end} ...")
    wf = S.load_events_from_m02(set(uni), start - hold_ms, end)
    rows = []
    for w, we in wf.items():
        tr = score_wallet(we, start, split, hold_ms, lat_ms, fee)
        te = score_wallet(we, split, end, hold_ms, lat_ms, fee)
        if tr and te and tr["n"] >= args.min_trades and te["n"] >= args.min_trades:
            rows.append({"wallet": w, "train_bps": tr["mean_bps"], "train_n": tr["n"],
                         "test_bps": te["mean_bps"], "test_n": te["n"]})
    df = pd.DataFrame(rows)
    print(f"\n=== 2-window persistence ({len(df)} wallets with >={args.min_trades} trades in BOTH windows) ===")
    print(f"corr(train_bps, test_bps): pearson {df.train_bps.corr(df.test_bps):.3f} | "
          f"spearman {df.train_bps.corr(df.test_bps, method='spearman'):.3f}")
    df = df.sort_values("train_bps", ascending=False).reset_index(drop=True)
    n = len(df)
    dec = max(5, n // 10)
    top = df.head(dec); bot = df.tail(dec)
    print(f"\ntrain-TOP decile ({dec} wallets): train {top.train_bps.mean():.1f}bps -> TEST {top.test_bps.mean():.1f}bps "
          f"(test win-of-wallets>0: {(top.test_bps>0).mean()*100:.0f}%)")
    print(f"train-BOTTOM decile: train {bot.train_bps.mean():.1f}bps -> TEST {bot.test_bps.mean():.1f}bps")
    print(f"ALL wallets test mean (random baseline): {df.test_bps.mean():.1f}bps")
    print(f"\nVERDICT: selection {'PERSISTS (top-decile test >> random)' if top.test_bps.mean() > df.test_bps.mean()+5 else 'does NOT persist (in-sample luck)'}")
    # where do V11's 3 proven wallets land
    proven = ["0x53b63a30a688beb53b5dc7bd731c661d678c555c","0x9e897322ae0e75b1eb3d86668d34f2271260b706","0xbbf7d7a9d0eaeab4115f022a6863450296112422"]
    print("\nV11 'proven' 3 (train -> test):")
    for p in proven:
        r = df[df.wallet == p]
        if len(r): print(f"  {p[:12]} train {r.train_bps.iloc[0]:.1f} -> test {r.test_bps.iloc[0]:.1f}bps")
        else: print(f"  {p[:12]} not in both-window set (insufficient m02 trades)")
    df.to_parquet("app/data/v15/wallet_persistence.parquet")


if __name__ == "__main__":
    main()
