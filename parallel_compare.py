"""Parallel universe comparison: current synthetic-notional reconstruction vs closedPnl reconstruction.

Over a random sample of N wallets, compute per-wallet inter-anchor drift under both methods
(with an equity floor on the anchor denominator), and report quarantine rates for each.
Quarantine gate: median drift > 10% OR max drift > 50%.
"""
import sys, glob, os, random
import numpy as np
import pandas as pd
import multiprocessing as mp

sys.path.insert(0, "/Users/hermes/quants-lab")
import research.v15.v15_m01_equity_reconstruct as m1
from research.v15._streaming_io import install_memory_guard

S = int(pd.Timestamp("2025-12-01", tz="UTC").timestamp() * 1000)
E = int((pd.Timestamp("2026-05-23", tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)
FLOOR = 100.0
ANCHORS_DF = None


def avg_cost_state(fills, upto_ms):
    st = {}
    for f in fills:
        if f["time"] > upto_ms:
            break
        c = f["coin"]; sz = f["signed_sz"]; px = f["price"]
        pos, entry = st.get(c, (0.0, 0.0))
        if pos == 0 or (pos > 0) == (sz > 0):
            newpos = pos + sz
            if abs(newpos) > 1e-12:
                entry = (entry * pos + px * sz) / newpos
            pos = newpos
        else:
            newpos = pos + sz
            if (newpos > 0) != (pos > 0) and abs(newpos) > 1e-12:
                entry = px
            pos = newpos
            if abs(pos) < 1e-12:
                pos = 0.0; entry = 0.0
        st[c] = (pos, entry)
    return st


def upnl_at(fills, t_ms):
    st = avg_cost_state(fills, t_ms)
    u = 0.0
    for c, (pos, entry) in st.items():
        if abs(pos) < 1e-9:
            continue
        mk = m1.get_mark(c, t_ms) or m1._last_fill_price(fills, c, t_ms)
        if mk is None:
            continue
        u += pos * (mk - entry)
    return u


def closedpnl_drift(w, fund, led, fills, anchors):
    out = []
    for i in range(1, len(anchors)):
        pt, pv = anchors[i - 1]; ct, cv = anchors[i]
        if ct <= pt or cv < FLOOR:
            continue
        realized = sum(float(f.get("raw", f).get("closedPnl", 0) or 0) for f in fills if pt < f["time"] <= ct)
        fees = sum(f.get("fee", 0) for f in fills if pt < f["time"] <= ct)
        fundg = sum(m1.funding_cash_delta(x) for x in fund if pt < int(x["time"]) <= ct)
        ledg = sum(m1.ledger_cash_delta(x, w.lower()).cash for x in led if pt < int(x["time"]) <= ct)
        recon = pv + realized + fundg - fees + ledg + (upnl_at(fills, ct) - upnl_at(fills, pt))
        out.append(abs(recon - cv) / cv)
    return out


def score(w):
    try:
        anc = m1.load_wallet_anchor(w, ANCHORS_DF)
        if anc is None:
            return None
        fills = m1.load_wallet_fills(w, S, E)
        if not fills:
            return None
        fund = m1.load_wallet_funding(w, S, E); led = m1.load_wallet_ledger(w, S, E)
        wa = [(int(t), float(v)) for t, v in m1.get_portfolio_all(w) if v > FLOOR and S <= int(t) <= E]
        if len(wa) < 2:
            return None
        m1._markpx_series.clear()
        au = m1.reconstruct_wallet((w, anc, S, E, True, False))["audit"]
        cur_med = au.get("median_inter_anchor_drift_pct"); cur_max = au.get("max_inter_anchor_drift_pct")
        if cur_med is None:
            return None
        cur_q = bool(au.get("quarantined"))
        cp = np.abs(closedpnl_drift(w, fund, led, fills, wa))
        if len(cp) == 0:
            return None
        cp_med = float(np.median(cp)); cp_max = float(cp.max())
        cp_q = (cp_med > 0.10) or (cp_max > 0.50)
        return (cur_q, cp_q, cur_med, cp_med, cur_max, cp_max)
    except Exception:
        return None


def _init():
    global ANCHORS_DF
    install_memory_guard(soft_gb=10.0, label="cmp-worker")
    ANCHORS_DF = pd.read_parquet(m1.ANCHOR_PARQUET)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    cached = [os.path.basename(p).replace(".json", "") for p in glob.glob(str(m1.WHOLE_ANCHOR_CACHE) + "/*.json")]
    have = {os.path.basename(p).split(".")[0] for p in glob.glob(str(m1.S3_BY_WALLET_DIR) + "/*")}
    cand = [w for w in cached if w in have]
    # deterministic sample (no Math.random equivalent issue; fixed seed)
    rng = random.Random(42)
    rng.shuffle(cand)
    sample = cand[:n]
    print(f"scoring {len(sample)} wallets (of {len(cand)} eligible) on {mp.cpu_count()} cores, floor=${FLOOR}", flush=True)
    rows = []
    with mp.Pool(processes=max(1, mp.cpu_count() - 2), initializer=_init) as pool:
        for i, r in enumerate(pool.imap_unordered(score, sample, chunksize=8), 1):
            if r is not None:
                rows.append(r)
            if i % 250 == 0:
                print(f"  {i}/{len(sample)} done, {len(rows)} scored", flush=True)
    df = pd.DataFrame(rows, columns=["cur_q", "cp_q", "cur_med", "cp_med", "cur_max", "cp_max"])
    nn = len(df)
    print(f"\n=== RESULT (n={nn} scored, floor=${FLOOR}) ===")
    print(f"CURRENT (synthetic-notional): quarantined {df.cur_q.sum()} ({100*df.cur_q.mean():.1f}%)  median-of-medians {df.cur_med.median()*100:.2f}%")
    print(f"CLOSEDPNL reconstruction:     quarantined {df.cp_q.sum()} ({100*df.cp_q.mean():.1f}%)  median-of-medians {df.cp_med.median()*100:.2f}%")
    print(f"\nwallets FIXED by closedPnl (cur_q & ~cp_q): {((df.cur_q) & (~df.cp_q)).sum()}")
    print(f"wallets BROKEN by closedPnl (~cur_q & cp_q): {((~df.cur_q) & (df.cp_q)).sum()}")
    df.to_parquet("/tmp/compare_scores.parquet", index=False)
    print("saved /tmp/compare_scores.parquet")


if __name__ == "__main__":
    main()
