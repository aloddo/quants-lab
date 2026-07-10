#!/usr/bin/env python3
"""FORWARD-OOS taker holdout from LOCAL hot-daily S3 fills (2026-06-24 .. 2026-07-08), a window that is
STRICTLY AFTER the selection windows (which end 2026-06-24). Confirms whether the 376 taker-consistent +
not-a-bag-holder survivors STILL have taker edge out of sample -- the direct test of the one-month-fluke
root cause. Reuses fidelity_replay.roundtrips + execution_model taker pricing (canonical). No new edge math.

signed_sz from hot schema: side 'B'(buy)=+size, 'A'(sell)=-size. Same (ts,coin,signed_sz,price) shape the
by_wallet gate consumes -> stage-1 selection == stage-3 holdout pricing. Read-only. Codex-gated for live use.
"""
import sys, glob
from pathlib import Path
import numpy as np, pandas as pd, pyarrow.parquet as pq
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
_MARK_FN = S.mark_at   # swappable: asset_ctxs exact mark (default) or candle-close (fresh-to-07-08)
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, apply_entry, apply_exit

HOT = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "hl_s3_fills_v2_hot"
CAP = 500.0 / 1e4
FEE_T = fee_rt(maker=False)


def load_hot_fills(wallets, lo_ms, hi_ms):
    """Read hot dailies overlapping [lo,hi], filter to `wallets`, return {w: sorted [(ts,coin,ssz,px)]}."""
    want = set(w.lower() for w in wallets)
    per = {w: [] for w in want}
    files = sorted(glob.glob(str(HOT / "2026*.parquet")))
    for fp in files:
        day = Path(fp).stem  # YYYYMMDD
        d_ms = int(pd.Timestamp(f"{day[:4]}-{day[4:6]}-{day[6:]}", tz="UTC").timestamp() * 1000)
        if d_ms + 86_400_000 < lo_ms or d_ms >= hi_ms:
            continue
        t = pq.read_table(fp, columns=["wallet", "coin", "side", "size", "price", "time"]).to_pydict()
        w_, c_, sd_, sz_, px_, tm_ = t["wallet"], t["coin"], t["side"], t["size"], t["price"], t["time"]
        for i in range(len(tm_)):
            w = w_[i]
            if w not in want:
                continue
            ts = int(tm_[i])
            if ts < lo_ms or ts >= hi_ms:
                continue
            c = c_[i]
            if not c or sz_[i] is None or px_[i] is None:
                continue
            try:
                s = float(sz_[i]); p = float(px_[i])
            except (TypeError, ValueError):
                continue
            if s <= 0 or p <= 0:
                continue
            sd = sd_[i]
            if sd not in ("B", "A"):        # P2 fix (codex): don't silently short on a malformed side
                continue
            ssz = s if sd == "B" else -s
            per[w].append((ts, c, ssz, p))
    for w in per:
        per[w].sort(key=lambda x: x[0])
    return per


def roundtrips_boundary(fills, boundary_ms):
    """P1 fix (codex 2026-07-10, survivorship-by-non-closure): fidelity_replay.roundtrips emits ONLY on
    close/reverse, so a position ENTERED in a window and still OPEN at the window boundary is invisible ->
    in-window losers silently drop out and per-wallet PASS counts overstate. This is roundtrips() replayed
    over fills with ts < boundary_ms (no look-ahead past the boundary), yielding the SAME closed round-trips
    PLUS, for any residual OPEN leg as of the boundary, a VIRTUAL close at boundary_ms (marked-to-market by
    taker_edge). 8th tuple field is_open flags the virtual boundary closes. xvw=nan is intentional: taker_edge
    prices mark-to-mark (uses _MARK_FN at entry/exit ts), it never reads the round-trip vwaps.

    Local (not a change to the shared fidelity_replay.roundtrips, which ~20 selection/cohort callers depend on
    staying closed-only). Slicing at boundary_ms also removes a latent look-ahead the original had: closed
    round-trips entered in-window but closing AFTER the boundary were scored with their FUTURE exit price."""
    from collections import defaultdict
    bycoin = defaultdict(list)
    for ts, c, s, p in fills:
        if ts >= boundary_ms:
            continue
        bycoin[c].append((ts, s, p))
    out = []
    for c, fl in bycoin.items():
        pos = 0.0; en = es = ex = exs = 0.0; ets = None; cdir = 0
        for ts, s, p in fl:
            if p is None or p <= 0 or s is None or s == 0:
                continue
            if pos == 0 or (pos > 0) == (s > 0):           # open / add
                if pos == 0:
                    ets = ts; cdir = 1 if s > 0 else -1; en = es = ex = exs = 0.0
                en += abs(s) * p; es += abs(s); pos += s
            else:                                          # reduce / close / reverse
                cl = min(abs(s), abs(pos))
                ex += cl * p; exs += cl; pos += s
                closed_or_reversed = abs(pos) < 1e-9 or (pos > 0) != (cdir > 0)
                if closed_or_reversed and es > 0 and exs > 0:
                    evw = en / es; xvw = ex / exs
                    g = cdir * (xvw - evw) / evw
                    out.append((c, cdir, ets, ts, evw, xvw, g, False, 0.0))
                    residual = pos
                    en = es = ex = exs = 0.0
                    if abs(residual) < 1e-9:
                        pos = 0.0
                    else:
                        pos = residual; ets = ts; cdir = 1 if residual > 0 else -1
                        en = abs(residual) * p; es = abs(residual)
        if abs(pos) >= 1e-9 and es > 0 and ets is not None:   # residual OPEN leg -> virtual boundary MTM
            # 9th field = exit-so-far fraction (exs/es): >0 means an opposite-side partial reduce/scale-out
            # happened before the boundary whose realized PnL taker_edge cannot price (size-blind, vwap-ignoring;
            # SAME pre-existing limitation on CLOSED roundtrips). Surfaced so scale-out materiality is auditable
            # (codex 2026-07-10; codex r2 wording: the reduce is opposite-side against the open leg, not same-side).
            out.append((c, cdir, ets, boundary_ms, en / es, float("nan"), float("nan"), True, exs / es if es else 0.0))
    return out


# codex-2 fix (2026-07-10): PENALIZE_UNMARKABLE -> an open-at-boundary leg we cannot mark is scored as the
# worst-case clipped loss (-CAP - fee) instead of silently dropped (residual survivorship hole). Off by default;
# main() flips it for the sensitivity run so we can report whether PASS count holds under the conservative floor.
PENALIZE_UNMARKABLE = False


def taker_edge(fills, lo_ms, hi_ms, lat_ms):
    nets = []; n_open_marked = 0; n_open_unmarked = 0; n_open_reduced = 0
    for c, dir_, ets, xts, evw, xvw, g, is_open, exs_frac in roundtrips_boundary(fills, hi_ms):
        if not (lo_ms <= ets < hi_ms):
            continue
        if is_open and exs_frac > 0.01:
            n_open_reduced += 1
        # codex-2 fix: virtual boundary close marks AT the break (hi_ms), no +lat, so no post-boundary mark can leak.
        em = _MARK_FN(c, ets + lat_ms); xm = _MARK_FN(c, xts if is_open else xts + lat_ms)
        if em is None or xm is None or em <= 0:
            if is_open:
                n_open_unmarked += 1
                if PENALIZE_UNMARKABLE:
                    nets.append(-CAP - fee_rt(maker=False, coin=c))   # conservative worst-case floor
            continue
        ef = apply_entry(c, em, dir_ > 0); xf = apply_exit(c, xm, dir_ > 0)
        og = max(-CAP, min(CAP, dir_ * (xf - ef) / ef))
        nets.append(og - fee_rt(maker=False, coin=c))  # P1 fix (codex): coin-specific RT fee (xyz/HIP-3)
        if is_open:
            n_open_marked += 1
    if not nets:
        return None
    a = np.array(nets)
    return {"n": len(a), "bps": float(a.mean() * 1e4),
            "n_open_marked": n_open_marked, "n_open_unmarked": n_open_unmarked, "n_open_reduced": n_open_reduced}


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe-file", required=True)
    ap.add_argument("--windows", default="2026-06-24,2026-07-01,2026-07-08")
    ap.add_argument("--min-trades", type=int, default=5)
    ap.add_argument("--mark-source", choices=["asset_ctxs", "candles", "live"], default="asset_ctxs",
                    help="asset_ctxs = exact HL mark (lags ~10d); candles = fills-reconstructed 1m close (fresh, ~9bps basis); "
                         "live = real-time captured mark (exact, coverage from 2026-07-09 onward)")
    ap.add_argument("--out", default="/tmp/forward_oos_hot.parquet")
    ap.add_argument("--penalize-unmarkable", action="store_true",
                    help="score open-at-boundary legs we cannot mark as worst-case -CAP-fee (sensitivity, codex-2)")
    args = ap.parse_args()
    lat_ms = 2000
    set_latency_ms(lat_ms)
    global PENALIZE_UNMARKABLE
    PENALIZE_UNMARKABLE = args.penalize_unmarkable
    if PENALIZE_UNMARKABLE:
        print("[sensitivity] unmarkable open-at-boundary legs scored as worst-case -CAP-fee", flush=True)
    global _MARK_FN
    if args.mark_source == "candles":
        import candle_marks
        _MARK_FN = candle_marks.candle_mark_at
        print("[mark-source] candle-close (fills-reconstructed, fresh; ~9bps basis vs exact mark)", flush=True)
    elif args.mark_source == "live":
        import live_marks
        _MARK_FN = live_marks.live_mark_at
        print("[mark-source] live captured mark (exact; coverage from 2026-07-09 onward)", flush=True)
    else:
        print("[mark-source] asset_ctxs exact mark (lags ~10d, frontier 06-29)", flush=True)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    breaks = [b.strip() for b in args.windows.split(",") if b.strip()]
    bms = [ms(b) for b in breaks]
    nwin = len(breaks) - 1
    wallets = [l.strip().lower().split(",")[0] for l in open(args.universe_file)
               if l.strip() and not l.lower().startswith("wallet") and not l.startswith("#")]
    # P2 rule-8 guard (codex r2): the by_wallet carry-in stitch holds prehistory tuples for EVERY wallet in
    # RAM. Fine for survivor-sized inputs; a 20k universe can retain a large fraction of the 5.3GB corpus and
    # OOM. This script is a survivor-shortlist OOS scorer, not a full-universe gate -> abort LOUD (not silent SIGKILL).
    if len(wallets) > 2000:
        raise SystemExit(f"forward_oos_hot: {len(wallets)} wallets exceeds the 2000 survivor-shortlist cap "
                         f"(carry-in stitch is not streamed). Run the full-universe gate via s3_taker_verify.py "
                         f"(bounded per-wallet), then feed only its survivors here.")
    print(f"loading hot fills for {len(wallets)} wallets over {breaks[0]}..{breaks[-1]} ...", flush=True)
    per = load_hot_fills(wallets, bms[0], bms[-1])
    hot_present = {w: bool(per.get(w)) for w in wallets}
    # P0 carry-in fix (codex 2026-07-09): prepend by_wallet history [0, first-break) so roundtrips() tracks
    # the TRUE carry-in position at the holdout boundary (a pre-boundary open otherwise fabricates a fake entry).
    from s3_taker_verify import load_fills_from_s3 as _load_bywallet
    for w in wallets:
        pre, _ = _load_bywallet(w, 0, bms[0])
        if pre:
            merged = pre + per.get(w, [])
            merged.sort(key=lambda x: x[0])
            per[w] = merged
    rows = []
    for w in wallets:
        fills = per.get(w, [])
        rec = {"wallet": w}
        live_all = consistent_all = True
        any_fill = hot_present[w]      # visibility keyed on IN-WINDOW (hot) fills, not carry-in prehistory
        for k in range(nwin):
            r = taker_edge(fills, bms[k], bms[k + 1], lat_ms) if fills else None
            n = r["n"] if r else 0
            bps = r["bps"] if r else float("nan")
            rec[f"n_{k}"] = n; rec[f"bps_{k}"] = bps
            rec[f"open_marked_{k}"] = r["n_open_marked"] if r else 0
            rec[f"open_unmarked_{k}"] = r["n_open_unmarked"] if r else 0
            rec[f"open_reduced_{k}"] = r["n_open_reduced"] if r else 0
            if n < args.min_trades:
                live_all = False
            if not (r and n >= args.min_trades and bps > 0):
                consistent_all = False
        rec["live_all"] = live_all; rec["pass"] = live_all and consistent_all
        if any_fill:
            rows.append(rec)
    df = pd.DataFrame(rows)
    print(f"=== FORWARD-OOS ({nwin} win {breaks[0]}..{breaks[-1]}, min-trades={args.min_trades}) ===")
    print(f"scored {len(df)} of {len(wallets)} (had fills)")
    if len(df):
        pdf = df[df["pass"]]
        om = sum(int(df[f"open_marked_{k}"].sum()) for k in range(nwin))
        ou = sum(int(df[f"open_unmarked_{k}"].sum()) for k in range(nwin))
        orr = sum(int(df[f"open_reduced_{k}"].sum()) for k in range(nwin))
        print(f"boundary-MTM (P1 fix): {om} open-at-boundary legs marked, {ou} unmarkable (no candle) "
              f"across all wallets/windows -- these were invisible under closed-only scoring")
        print(f"scale-out materiality (codex Q5): {orr} of the {om} marked open legs had an opposite-side "
              f"partial reduce >1% before the boundary (realized PnL taker_edge cannot price; SAME on closed roundtrips)")
        print(f"LIVE all {nwin}: {int(df['live_all'].sum())} | PASS (live+bps>0 all): {len(pdf)}")
        df.to_parquet(args.out)
        print(f"wrote -> {args.out}")
        show = pdf.sort_values([f"bps_{nwin-1}"], ascending=False).head(40)
        for _, r in show.iterrows():
            cells = " ".join(f"[{int(r[f'n_{k}'])}/{r[f'bps_{k}']:+.0f}]" for k in range(nwin))
            print(f"  {r['wallet'][:16]}  {cells}")


if __name__ == "__main__":
    main()
