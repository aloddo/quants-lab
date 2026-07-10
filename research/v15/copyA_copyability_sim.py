#!/usr/bin/env python3
"""Copy A L3 -- copyability follower-sim on IDENTIFIED leaderboard candidates.

For each candidate address: fetch its full fill history from the HL API (userFillsByTime, paginated),
classify market composition (PERP major = copyable; spot @N / HIP-3 xyz: / illiquid = NOT the target),
then replay its PERP entries+exits as a FOLLOWER through execution_model (real per-coin slippage + HL
taker fee) and measure the FOLLOWER's realized PnL + any open bag marked at last price.

The point (per the 5-angle KILL + Alberto's shallow-correction): do NOT trust the leaderboard PnL (spot
bags, HIP-3 stocks, MM churn all inflate it). Measure what WE would net copying their actual perp trades
with real execution. A bag-holder auto-fails: the follower mirrors the exit / carries the open bag.
"""
from __future__ import annotations
import sys, time, json
import requests
import numpy as np, pandas as pd
sys.path.insert(0, "research/v15")
import execution_model as EM

API = "https://api.hyperliquid.xyz/info"
START_MS = 1767225600000  # 2026-01-01 (recent regime; fewer pages -> less rate-limit truncation)


def is_perp_major(coin: str) -> bool:
    """Copyable = plain perp ticker (no '@' spot index, no ':' HIP-3 dex, not a dust conversion)."""
    if not coin or any(c in coin for c in ("@", ":", "/", "#")):
        return False
    return True


def _post(payload, tries=4):
    for k in range(tries):
        try:
            r = requests.post(API, json=payload, timeout=30)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(0.6 * (k + 1))  # backoff on rate-limit / transient
    return None


def fetch_fills(addr: str, start_ms: int, max_pages: int = 40):
    out, cur = [], start_ms
    for _ in range(max_pages):
        d = _post({"type": "userFillsByTime", "user": addr, "startTime": cur, "endTime": None})
        if not isinstance(d, list) or not d:
            break
        out.extend(d)
        if len(d) < 2000:
            break
        nxt = max(int(x["time"]) for x in d) + 1
        if nxt <= cur:
            break
        cur = nxt
        time.sleep(0.35)
    return out


def fetch_open_bag(addr: str):
    """Exact current open perp positions from HL: sum unrealizedPnl (the bag a follower still holds)."""
    d = _post({"type": "clearinghouseState", "user": addr})
    if not isinstance(d, dict):
        return 0.0, 0.0
    upnl, notl = 0.0, 0.0
    for p in d.get("assetPositions", []):
        pos = p.get("position", {})
        try:
            upnl += float(pos.get("unrealizedPnl", 0) or 0)
            notl += abs(float(pos.get("positionValue", 0) or 0))
        except Exception:
            pass
    return upnl, notl


def follower_sim(fills, bag_upnl, bag_notl):
    """Follower PnL from EXACT exchange data (no fragile cost-basis reconstruction):
    follower_realized = sum(leader closedPnl on PERP fills)  [exact realized, at leader prices]
                        - execution_drag [follower slippage + follower fees on every mirrored leg]
    follower_bag      = leader's current open perp unrealizedPnl (a follower still holds it), net of
                        one-way entry slippage on the bag notional.
    """
    df = pd.DataFrame(fills)
    if df.empty:
        return None
    df["perp"] = df["coin"].map(is_perp_major)
    n_all = len(df)
    perp = df[df["perp"]].copy()
    perp_share = len(perp) / n_all if n_all else 0.0
    if perp.empty:
        return {"n_fills": n_all, "perp_share": 0.0, "note": "no perp fills",
                "follower_realized": 0.0, "follower_bag": 0.0, "follower_total": 0.0}
    perp["px"] = pd.to_numeric(perp["px"], errors="coerce")
    perp["sz"] = pd.to_numeric(perp["sz"], errors="coerce")
    perp["closedPnl"] = pd.to_numeric(perp["closedPnl"], errors="coerce").fillna(0.0)
    realized_leader = float(perp["closedPnl"].sum())
    # execution drag: every mirrored leg pays one-way slippage + one-way taker fee on its notional
    drag = 0.0
    for coin, g in perp.groupby("coin"):
        notl = float((g["px"] * g["sz"]).abs().sum())
        drag += notl * (EM.slip_oneway(coin) + EM.fee_rt(coin=coin) / 2.0)
    follower_realized = realized_leader - drag
    # bag: leader current unrealized, minus follower entry-slip to have gotten into it (approx midcap)
    bag_slip = bag_notl * EM.DEFAULT_SLIP_BPS / 10_000.0
    follower_bag = bag_upnl - bag_slip
    return {
        "n_fills": n_all, "perp_share": round(perp_share, 3), "perp_fills": len(perp),
        "realized_leader": round(realized_leader, 0), "exec_drag": round(drag, 0),
        "follower_realized": round(follower_realized, 0), "follower_bag": round(follower_bag, 0),
        "follower_total": round(follower_realized + follower_bag, 0),
        "coins": ",".join(perp["coin"].value_counts().head(5).index.tolist()),
    }


def main():
    addrs = sys.argv[1:]
    if not addrs:
        # default: top-N directional candidates by leaderboard PnL
        c = pd.read_parquet("app/data/copyA/directional_candidates.parquet")
        addrs = c.sort_values("pnl_all", ascending=False)["wallet"].head(int(40)).tolist()
    print(f"copyability sim on {len(addrs)} candidates, perp-only, follower execution\n", flush=True)
    rows = []
    for i, a in enumerate(addrs):
        fills = fetch_fills(a, START_MS)
        if not fills:
            print(f"{i+1:2d} {a[:12]} no fills (after retry)", flush=True)
            continue
        bag_upnl, bag_notl = fetch_open_bag(a)
        m = follower_sim(fills, bag_upnl, bag_notl)
        if m is None:
            continue
        m["wallet"] = a
        rows.append(m)
        print(f"{i+1:2d} {a[:12]} perp={m.get('perp_share'):.2f} "
              f"foll_real=${m.get('follower_realized',0):>11,.0f} bag=${m.get('follower_bag',0):>11,.0f} "
              f"total=${m.get('follower_total',0):>11,.0f} {m.get('coins','')}", flush=True)
        time.sleep(0.3)
    out = pd.DataFrame(rows)
    out.to_parquet("app/data/copyA/copyability_results.parquet", index=False)
    print(f"\nwrote {len(out)} results")
    # copyable = perp-dominant AND follower makes money on LOCKED-IN realized (not just a fragile bag)
    good = out[(out["perp_share"] >= 0.5) & (out["follower_realized"] > 0)]
    print(f"\nCOPYABLE (perp_share>=0.5 AND follower_REALIZED>0, bag excluded): {len(good)}")
    if len(good):
        print(good.sort_values("follower_realized", ascending=False)[
            ["wallet", "perp_share", "realized_leader", "exec_drag", "follower_realized", "follower_bag", "follower_total", "coins"]
        ].to_string(index=False))


if __name__ == "__main__":
    main()
