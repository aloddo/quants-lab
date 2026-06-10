"""Test: does completing the fill cache through the snapshot time fix the residual?

Our fill cache lags the position snapshot (parquet fetched_ms) by hours. The backward
seed (cur_snapshot - sum(fills>anchor)) is then built on INCOMPLETE fills -> wrong seed.
Here we fetch the missing boundary fills from the HL API (userFillsByTime), merge into the
cache (dedup by tid), and re-run compute_eq_at over the validation anchors.
"""
import sys
sys.path.insert(0, "research/v15")
import time
import requests
import numpy as np
import pandas as pd
import v15_m01_equity_reconstruct as m01

HL = "https://api.hyperliquid.xyz/info"
S = int(pd.Timestamp("2025-12-01", tz="UTC").timestamp() * 1000)
E = int(pd.Timestamp("2026-05-23", tz="UTC").timestamp() * 1000 + 86_399_999)


def api_fills(wallet, start_ms, end_ms):
    out, seen, cur = [], set(), start_ms
    for _ in range(60):
        r = requests.post(HL, json={"type": "userFillsByTime", "user": wallet,
                                    "startTime": cur, "endTime": end_ms,
                                    "aggregateByTime": False}, timeout=30).json()
        if not r:
            break
        new = [f for f in r if f.get("tid") not in seen]
        for f in new:
            seen.add(f.get("tid"))
        out += new
        if len(r) < 2000:
            break
        cur = max(int(f["time"]) for f in r) + 1
        time.sleep(0.15)
    # normalize to our fill dict shape
    norm = []
    for f in out:
        if not m01.coin_is_allowed_perp(f["coin"]):
            continue
        sz = float(f["sz"])
        norm.append({
            "wallet": wallet.lower(), "coin": f["coin"], "side": f["side"],
            "size": sz, "price": float(f["px"]), "time": int(f["time"]),
            "dir": f.get("dir", ""), "closedPnl": float(f.get("closedPnl", 0) or 0),
            "startPosition": float(f.get("startPosition", 0) or 0),
            "fee": float(f.get("fee", 0) or 0), "builderFee": float(f.get("builderFee", 0) or 0),
            "deployerFee": float(f.get("deployerFee", 0) or 0),
            "tid": int(f.get("tid", 0) or 0),
            "signed_sz": sz if f["side"] == "B" else -sz,
            "is_liquidation": "Liquidat" in str(f.get("dir", "")),
        })
    return norm


def merged_fills(wallet, load_end):
    cache = m01.load_wallet_fills(wallet, S - 200 * 86_400_000, load_end)
    last = max((f["time"] for f in cache), default=S)
    # fetch from a bit before the cache end through load_end+1d to cover the boundary
    api = api_fills(wallet, last - 3_600_000, load_end + 86_400_000)
    by_tid = {}
    for f in cache + api:
        by_tid[f["tid"]] = f  # api overrides cache on same tid (richer fields)
    fills = sorted(by_tid.values(), key=lambda x: (x["time"], x["tid"]))
    return cache, api, fills


def residuals(wallet, fills, anchor):
    avh = m01.get_portfolio_perp(wallet)
    wa = sorted((t, v) for t, v in avh if v > 0.01 and S <= t <= E)
    we = min(E, anchor.fetched_ms)
    fu = m01.load_wallet_funding(wallet, S, int(anchor.fetched_ms))
    ld = m01.load_wallet_ledger(wallet, S, int(anchor.fetched_ms))
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
        wr = m01.compute_eq_at(stream, fills, anchor, wallet.lower(), b_t, a_t, a_v)
        res.append(wr.equity - b_v)
    a = np.abs(res)
    return a.max(), float(np.median(a)), len(res)


if __name__ == "__main__":
    adf = pd.read_parquet(m01.ANCHOR_PARQUET)
    wallets = sys.argv[1:] or ["0x3e516cf3c9d4f29fae6c1324c2414dc872fc9c09"]
    for w in wallets:
        anchor = m01.load_wallet_anchor(w, adf)
        cache, api, fills = merged_fills(w, int(anchor.fetched_ms))
        # offset check per coin: forward_final vs cur
        import collections
        by = collections.defaultdict(list)
        for f in fills:
            by[f["coin"]].append(f)
        big_off = []
        for c, fs in by.items():
            fs.sort(key=lambda x: (x["time"], x["tid"]))
            fwd = fs[0]["startPosition"] + sum(f["signed_sz"] for f in fs)
            cur = anchor.positions.get(c, 0.0)
            off = cur - fwd
            mk = m01.get_mark(c, int(anchor.fetched_ms)) or 0
            if abs(off * mk) > 5000:
                big_off.append((c, round(off, 2), round(off * mk)))
        # before (cache only) and after (merged)
        mx0, med0, n0 = residuals(w, sorted(cache, key=lambda x: (x["time"], x["tid"])), anchor)
        mx1, med1, n1 = residuals(w, fills, anchor)
        print(f"{w[:12]}: cache_fills={len(cache)} api_added={len(fills)-len(cache)} "
              f"| BEFORE max=${mx0:,.0f} med=${med0:,.2f} | AFTER max=${mx1:,.0f} med=${med1:,.2f}")
        print(f"   remaining big per-coin offsets (|off*mk|>$5k): {big_off}")
