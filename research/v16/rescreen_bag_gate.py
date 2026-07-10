#!/usr/bin/env python3
"""rescreen_bag_gate.py -- re-screen the live-10 + cohort candidates through the NO-BAG-HOLDING gates
that did NOT bind on the gate1_v4 roster (Alberto 2026-07-07: "never holds unrealized losses HAD ALWAYS
BEEN A REQUIREMENT").

Extends (does NOT rebuild) research/v16/build_skill_cohort.py:
  - REUSES martingale_flags(df): the HARD behavioral bag-holder veto (hold losers, size up after losses,
    realize only winners). This is the historical signature gate that was never applied to the OOS-holds
    path that picked the live 10.
  - ADDS current_bag_snapshot(addr): position-granularity CURRENT open bag from clearinghouseState
    (unrealizedPnl per position + held-in-loss), TIGHTER than build_skill_cohort.mtm_dd_exclude which
    only fires at near-ruin (account DD>=70%) -- the loophole that let 0x5a5ec18f (-$12k bag) through.
  - ADDS holds_losers_metrics(df): soft diagnostic (loss-hold-ratio, realized-loss fraction, worst
    open-journey mark) so we see WHY a wallet trips, not just a bool.

Read-only screen. Prints a PASS/FAIL table. NO live config change. Codex-gated for any roster swap.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/rescreen_bag_gate.py
"""
from __future__ import annotations
import json, sys, time, urllib.request
import numpy as np, pandas as pd

sys.path.insert(0, "research/v16")
from build_skill_cohort import martingale_flags, max_dd  # REUSE the existing veto

JOURNEYS = "app/data/v15/m02_journeys.parquet"
API = "https://api.hyperliquid.xyz/info"

# ---- roster: live 10 (full) + cohort candidates (prefixes, resolved from parquet) ----
LIVE10 = [
    "0x36c097864a03c7f0215c0d43165a734152a12e0b",
    "0x6f83ab8890ed38bf38a31010aa9a5e9ca743bfad",
    "0xe46eafafb60af2eea3a59768106a9342aec59ec3",
    "0x1404109f8cd4a79a0447365edbb7a13acd0b2f27",
    "0x760ec8576c2dc5dba2655f7b948c0689b02b6cb0",
    "0x36a60294f8b77e8ebe2ee32f3d3697952a379514",
    "0x03d8c9ce2a103a0094acc96520cf5eb87f85270c",
    "0xccf595171e2e56655fb4d386b7424da16be69d42",
    "0x5a5ec18fcf9db025d24c3674dd48ff40d5305204",
    "0x8c364082b2d8151ef4e06f6b6cef395030c9bc00",
]
CAND_PREFIX = [  # cohort-expansion-candidates-2026-07-06 (13 OOS-HOLDS), truncated in brain
    "0xb567367a97986d", "0xa55573fc0ba35d", "0xd2efdde0def642", "0x08e881cb053a76",
    "0x5c3f8fdc2c99cd", "0x83c4c5a492d77e", "0xffdb2d4eb40e3b", "0x8aa077f5998d23",
    "0x70f2470004a760", "0xb13dfc88a37e32", "0xaf266b453d153c", "0x25554a80781ee6",
    "0x1f15d5bb38f0d3",
]


def post(body, tries=4):
    for k in range(tries):
        try:
            req = urllib.request.Request(API, data=json.dumps(body).encode(),
                                         headers={"Content-Type": "application/json"})
            return json.load(urllib.request.urlopen(req, timeout=20))
        except Exception:
            time.sleep(0.8 * (k + 1))
    return None


def current_bag_snapshot(addr):
    """Position-granularity CURRENT open bag. Returns dict: agg_upnl, n_pos, n_red, worst_pos_pct,
    worst_coin. worst_pos_pct = min over positions of (unrealizedPnl / positionValue) = deepest loser."""
    d = post({"type": "clearinghouseState", "user": addr})
    if not isinstance(d, dict):
        return None
    aps = d.get("assetPositions", [])
    acct = float(d.get("marginSummary", {}).get("accountValue", 0) or 0)
    agg = 0.0; nred = 0; worst = 0.0; wcoin = ""
    for p in aps:
        pos = p.get("position", {})
        u = float(pos.get("unrealizedPnl", 0) or 0)
        pv = abs(float(pos.get("positionValue", 0) or 0))
        agg += u
        if u < 0:
            nred += 1
        pct = (u / pv) if pv > 0 else 0.0
        if pct < worst:
            worst = pct; wcoin = pos.get("coin", "")
    return {"agg_upnl": agg, "acct": acct, "n_pos": len(aps), "n_red": nred,
            "agg_upnl_pct_acct": (agg / acct * 100 if acct > 0 else 0.0),
            "worst_pos_pct": worst * 100, "worst_coin": wcoin}


def holds_losers_metrics(g):
    """Soft diagnostic per wallet journeys: realized-loss fraction, loss/win hold-duration ratio."""
    g = g.sort_values("entry_ts")
    pnl = g["net_realized_pnl"].to_numpy(); dur = g["duration_h"].to_numpy()
    n = len(pnl); win = pnl > 0; loss = pnl < 0; nloss = int(loss.sum())
    lf = nloss / n if n else np.nan
    ha = (np.mean(dur[loss]) / np.mean(dur[win])) if (nloss > 0 and win.any() and np.mean(dur[win]) > 0) else np.nan
    return n, lf, ha


def resolve_prefixes(all_wallets, prefixes):
    out = {}
    wset = list(all_wallets)
    for pfx in prefixes:
        hits = [w for w in wset if w.startswith(pfx)]
        out[pfx] = hits[0] if len(hits) == 1 else (hits if hits else None)
    return out


def main():
    print("Loading journey wallets index...", flush=True)
    wcol = pd.read_parquet(JOURNEYS, columns=["wallet"])
    uniq = set(wcol["wallet"].unique())
    print(f"  {len(uniq)} unique wallets in journeys", flush=True)

    resolved = resolve_prefixes(uniq, CAND_PREFIX)
    cand_full = {}
    for pfx, r in resolved.items():
        if isinstance(r, str):
            cand_full[pfx] = r
        else:
            print(f"  [candidate {pfx}] -> {'AMBIGUOUS ' + str(len(r)) if r else 'NOT FOUND'} in journeys", flush=True)

    target = set(LIVE10) | set(cand_full.values())
    cols = ["wallet", "coin", "entry_ts", "realized_pnl", "net_realized_pnl",
            "max_position_notional", "liq_closed", "duration_h"]
    print(f"Loading journeys for {len(target)} target wallets...", flush=True)
    j = pd.read_parquet(JOURNEYS, columns=cols, filters=[("wallet", "in", list(target))])
    j = j[j.max_position_notional > 10].copy()
    print(f"  {len(j)} journeys loaded", flush=True)

    mart = martingale_flags(j)  # bool Series index=wallet, True = martingale/bag-holder veto

    def label(addr):
        if addr in LIVE10:
            return "LIVE"
        for pfx, full in cand_full.items():
            if full == addr:
                return "cand:" + pfx[:10]
        return "?"

    print("\nQuerying CURRENT open bags (clearinghouseState, throttled)...", flush=True)
    rows = []
    for addr in list(LIVE10) + [cand_full[p] for p in CAND_PREFIX if p in cand_full]:
        g = j[j.wallet == addr]
        n, lf, ha = holds_losers_metrics(g) if len(g) else (0, np.nan, np.nan)
        mflag = bool(mart.get(addr, False))
        bag = current_bag_snapshot(addr)
        time.sleep(3.5)
        rows.append({"addr": addr, "kind": label(addr), "n_j": n,
                     "mart_veto": mflag, "loss_frac": lf, "hold_ratio": ha,
                     "cur_agg_upnl": None if bag is None else round(bag["agg_upnl"], 0),
                     "cur_n_red": None if bag is None else bag["n_red"],
                     "worst_pos_pct": None if bag is None else round(bag["worst_pos_pct"], 1),
                     "worst_coin": None if bag is None else bag["worst_coin"]})

    df = pd.DataFrame(rows)
    # ---- GATE definitions (proposed, codex-gated) ----
    # HARD historical: martingale/holds-losers behavioral veto.
    # HARD current: sitting in a deep open loser now (worst position <= -20% of its notional) OR
    #               realize-only-winners tell (loss_frac < 0.05 with material sample).
    def verdict(r):
        reasons = []
        if r["mart_veto"]:
            reasons.append("martingale/holds-losers veto")
        if r["worst_pos_pct"] is not None and r["worst_pos_pct"] <= -20:
            reasons.append(f"deep open loser {r['worst_coin']} {r['worst_pos_pct']}%")
        if r["loss_frac"] == r["loss_frac"] and r["n_j"] >= 20 and r["loss_frac"] < 0.05:
            reasons.append(f"realize-only-winners loss_frac={r['loss_frac']:.2f}")
        return "FAIL: " + "; ".join(reasons) if reasons else "PASS"

    df["verdict"] = df.apply(verdict, axis=1)
    pd.set_option("display.width", 200); pd.set_option("display.max_colwidth", 40)
    print("\n===== RE-SCREEN RESULT (no-bag gates) =====")
    print(df[["kind", "addr", "n_j", "mart_veto", "loss_frac", "hold_ratio",
              "cur_agg_upnl", "cur_n_red", "worst_pos_pct", "worst_coin", "verdict"]].to_string(index=False))
    live = df[df.kind == "LIVE"]
    print(f"\nLIVE 10: {int((live.verdict=='PASS').sum())} PASS / {int((live.verdict!='PASS').sum())} FAIL")
    for _, r in live[live.verdict != "PASS"].iterrows():
        print(f"  FAIL {r['addr'][:12]} -> {r['verdict']}")
    cand = df[df.kind.str.startswith("cand")]
    print(f"CANDIDATES: {int((cand.verdict=='PASS').sum())} PASS / {int((cand.verdict!='PASS').sum())} FAIL")
    df.to_json("/tmp/rescreen_bag_gate.json", orient="records", indent=2)
    print("\nwrote /tmp/rescreen_bag_gate.json")


if __name__ == "__main__":
    main()
