"""PROTOTYPE: validate backward-from-snapshot seeding vs current forward seeding.

Root cause (proven): M1 seeds a coin's historical position from the startPosition of
its first fill, but same-millisecond fill bursts (no tid in the S3 partition) make that
startPosition mid-burst garbage -> phantom positions back-projected to early anchors.

Backward seed: pos(coin, anchor) = current_snapshot_position(coin) - sum(signed_sz of
fills with time > anchor). Order-independent (sum commutes; immune to burst ordering),
anchored to the authoritative fetch-time snapshot. Valid for the EX-POST reconciliation
(uses future fills); the causal per-event walk keeps its causal seed untouched.

Reports per-wallet max|segment residual| under FORWARD (current) and BACKWARD seeds.
"""
import sys
sys.path.insert(0, "research/v15")
import numpy as np
import pandas as pd
import v15_m01_equity_reconstruct as m01

S = int(pd.Timestamp("2025-12-01", tz="UTC").timestamp() * 1000)
E = int(pd.Timestamp("2026-05-23", tz="UTC").timestamp() * 1000 + 86_399_999)

_orig_seed = m01.seed_positions


def seed_backward(fills, anchor, anchor_ms, causal_cutoff=False):
    if causal_cutoff:
        return _orig_seed(fills, anchor, anchor_ms, causal_cutoff=True)
    cur = {c: float(s) for c, s in anchor.positions.items()}
    after = {}
    for f in fills:
        if f["time"] > anchor_ms:
            after[f["coin"]] = after.get(f["coin"], 0.0) + f["signed_sz"]
    seed = {}
    for c in set(cur) | set(after):
        if not m01.coin_is_allowed_perp(c):
            continue
        pos = cur.get(c, 0.0) - after.get(c, 0.0)
        if abs(pos) > 1e-9:
            seed[c] = pos
    return seed


def seed_hybrid(fills, anchor, anchor_ms, causal_cutoff=False):
    """Forward positions_at for coins WITH a fill <= anchor (clean, order-independent
    via earliest startPosition + cumsum); backward-from-snapshot ONLY for coins with NO
    fill <= anchor (the phantom case: a future fill's mid-burst startPosition is garbage).
    Eliminates phantoms without disturbing coins the forward path already nails."""
    if causal_cutoff:
        return _orig_seed(fills, anchor, anchor_ms, causal_cutoff=True)
    seed = dict(m01.positions_at(fills, anchor_ms))  # forward base
    has_prior = {f["coin"] for f in fills if f["time"] <= anchor_ms}
    cur = {c: float(s) for c, s in anchor.positions.items()}
    after = {}
    for f in fills:
        if f["time"] > anchor_ms:
            after[f["coin"]] = after.get(f["coin"], 0.0) + f["signed_sz"]
    for c in set(cur) | set(after):
        if c in has_prior or not m01.coin_is_allowed_perp(c):
            continue  # forward already covered it (or disallowed)
        pos = cur.get(c, 0.0) - after.get(c, 0.0)  # backward = current - future fills
        if abs(pos) > 1e-9:
            seed[c] = pos
    return seed


def residuals(w, adf):
    w = w.lower()
    anchor = m01.load_wallet_anchor(w, adf)
    if anchor is None:
        return None, "no_anchor"
    avh = m01.get_portfolio_perp(w)
    wa = sorted((t, v) for t, v in avh if v > 0.01 and S <= t <= E)
    if len(wa) < 2:
        return None, "no_anchors"
    we = min(E, anchor.fetched_ms)
    le = max(we, int(anchor.fetched_ms))
    fills = m01.load_wallet_fills(w, S, le)
    fu = m01.load_wallet_funding(w, S, le)
    ld = m01.load_wallet_ledger(w, S, le)
    stream = ([(f["time"], "fill", f) for f in fills]
              + [(int(x["time"]), "ledger", x) for x in ld]
              + [(int(x["time"]), "funding", x) for x in fu])
    stream.sort(key=lambda x: x[0])
    wa = [(t, v) for t, v in wa if S <= t <= we]
    res = []
    for i in range(1, len(wa)):
        a_t, a_v = wa[i - 1]
        b_t, b_v = wa[i]
        if b_t <= a_t:
            continue
        wr = m01.compute_eq_at(stream, fills, anchor, w, b_t, a_t, a_v)
        if wr.recon_incomplete:
            res.append(b_v * 0.01)
            continue
        res.append(wr.equity - b_v)
    if not res:
        return None, "no_segs"
    a = np.abs(res)
    return (float(a.max()), float(np.median(a)), len(res), int((a > 1.0).sum())), None


def run(label):
    adf = pd.read_parquet(m01.ANCHOR_PARQUET)
    wallets = [l.strip() for l in open("app/data/v15/m01_validation_wallets.txt")
               if l.strip().startswith("0x")]
    print(f"\n===== {label} =====")
    print(f"{'wallet':14s} {'maxResid$':>12s} {'medResid$':>11s} {'segs':>5s} {'>$1':>4s}")
    for w in wallets:
        # reset per-process mark caches not needed (mongo-backed); clear nothing
        r, err = residuals(w, adf)
        if err:
            print(f"{w[:12]:14s} {err}")
            continue
        mx, med, n, ng = r
        print(f"{w[:12]:14s} {mx:>12.2f} {med:>11.2f} {n:>5d} {ng:>4d}")


def seed_burst_aware(fills, anchor, anchor_ms, causal_cutoff=False):
    """Order-independent shape (cumsum signed_sz) + per-coin pre-window carry P0 chosen
    by reliability of startPosition: clean earliest fill -> forward startPos0; same-ms
    BURST earliest fill (startPos0 is mid-burst garbage, no tid to order) -> backward
    P0 = snapshot_current - sum(all signed_sz). pos(anchor) = P0 + sum(signed_sz <= anchor)."""
    if causal_cutoff:
        return _orig_seed(fills, anchor, anchor_ms, causal_cutoff=True)
    by = {}
    for f in fills:
        if not m01.coin_is_allowed_perp(f["coin"]):
            continue
        by.setdefault(f["coin"], []).append(f)
    cur = {c: float(s) for c, s in anchor.positions.items()}
    seed = {}
    for c, fs in by.items():
        fs.sort(key=lambda x: x["time"])
        t0 = fs[0]["time"]
        burst = sum(1 for f in fs if f["time"] == t0) > 1
        net_all = sum(f["signed_sz"] for f in fs)
        if burst:
            p0 = cur.get(c, 0.0) - net_all          # backward (snapshot-anchored)
        else:
            p0 = fs[0]["startPosition"]              # forward (clean earliest fill)
        cum_le = sum(f["signed_sz"] for f in fs if f["time"] <= anchor_ms)
        pos = p0 + cum_le
        if abs(pos) > 1e-9:
            seed[c] = pos
    # coins held at fetch but never traded in-window (no fills) -> snapshot carry
    for c, s in cur.items():
        if c not in by and m01.coin_is_allowed_perp(c) and abs(s) > 1e-9:
            seed[c] = float(s)
    return seed


def seed_fwd_burstzero(fills, anchor, anchor_ms, causal_cutoff=False):
    """SNAPSHOT-FREE seed (does NOT use the fetch-time snapshot, so immune to the
    fill-data-end vs snapshot-time gap). Pure forward from fills: pos = P0 + cumsum(<=anchor),
    P0 = clean first-fill startPosition, but 0 when the first fill is a same-ms BURST (its
    startPosition is mid-burst garbage; default to no pre-window carry)."""
    if causal_cutoff:
        return _orig_seed(fills, anchor, anchor_ms, causal_cutoff=True)
    by = {}
    for f in fills:
        if not m01.coin_is_allowed_perp(f["coin"]):
            continue
        by.setdefault(f["coin"], []).append(f)
    seed = {}
    for c, fs in by.items():
        fs.sort(key=lambda x: (x["time"], x["tid"]))
        t0 = fs[0]["time"]
        burst = sum(1 for f in fs if f["time"] == t0) > 1
        p0 = 0.0 if burst else float(fs[0]["startPosition"])
        cum = sum(f["signed_sz"] for f in fs if f["time"] <= anchor_ms)
        pos = p0 + cum
        if abs(pos) > 1e-9:
            seed[c] = pos
    return seed


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "both"
    if mode in ("forward", "both"):
        m01.seed_positions = _orig_seed
        run("FORWARD seed (current M1)")
    if mode in ("backward", "both"):
        m01.seed_positions = seed_backward
        run("BACKWARD seed (snapshot - future fills)")
    if mode in ("hybrid", "both"):
        m01.seed_positions = seed_hybrid
        run("HYBRID seed (forward if prior fill, else backward)")
    if mode in ("burst", "both"):
        m01.seed_positions = seed_burst_aware
        run("BURST-AWARE seed (clean->forward startPos0, burst->backward P0)")
    if mode in ("fwdzero", "both"):
        m01.seed_positions = seed_fwd_burstzero
        run("FWD-BURSTZERO seed (snapshot-free: clean->startPos0, burst->P0=0)")
