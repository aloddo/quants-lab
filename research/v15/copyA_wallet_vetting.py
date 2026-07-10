#!/usr/bin/env python3
"""Copy A -- per-wallet VETTING features (fills-only, NO MtM / NO equity reconstruction).

Alberto 10582/10584: "Copy A ... you are the CEO, feel it out." Mirror a small set of HAND-VETTED
specific wallets, judged individually on non-gameable evidence (NOT ranked by a statistic -- that failed
5 ways, see projects/quant/lessons/copy-selection-signal-absent).

KEY: HL fills carry `closedPnl` (exchange-provided realized PnL on each closing/reducing fill) and
`startPosition`. So realized PnL + the martingale/bag-holder signature come straight from fills -- no
mark-to-market, no position-equity reconstruction (the banned rabbit hole).

Per-wallet features (accumulated one fills-file at a time, per-wallet aggregates only -> memory-safe):
  survival_days, active_days, n_fills, last_fill_age_days
  realized_pnl        = sum(closedPnl)                     (large-PnL survivor)
  n_close             = count(closedPnl != 0)              (realized round-trips)
  win_rate            = mean(closedPnl > 0 | closedPnl!=0) (~1.0 = never-take-a-loss = DISQUALIFY)
  gross_win, gross_loss, profit_factor
  took_real_losses    = gross_loss material vs gross_win   (real drawdowns realized, not bag-held)
  avg_notional, n_coins, top_coin_share                    (capacity / copyability)
Filters live in the companion step (copyA_vet_filter). This script only emits the raw features.
"""
from __future__ import annotations
import glob, os, resource, sys
import numpy as np, pandas as pd

FILL_DIR = "app/data/hl_s3_fills_v2"
OUT = "app/data/copyA/wallet_vetting_features.parquet"
NOW_MS = 1783150000000  # ~2026-07-04; stamped constant (Date.now banned in some contexts; fine here)
MS_DAY = 86_400_000
MIN_FILLS = 30          # ignore near-inactive wallets outright


def _install_memory_guard(soft_gb=10):
    try:
        soft = int(soft_gb * 1024**3)
        resource.setrlimit(resource.RLIMIT_AS, (soft, soft))
    except Exception as e:
        print(f"[memguard] could not set limit: {e}", file=sys.stderr)


def main():
    _install_memory_guard(10)
    files = sorted(glob.glob(f"{FILL_DIR}/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit()]
    print(f"{len(files)} fills files {os.path.basename(files[0])}..{os.path.basename(files[-1])}", flush=True)

    # per-wallet accumulators
    agg = {}   # w -> dict of running sums
    for i, f in enumerate(files):
        try:
            df = pd.read_parquet(f, columns=["wallet", "coin", "size", "price", "time", "closedPnl", "notional"])
        except Exception:
            continue
        if df.empty:
            continue
        df = df[~df["coin"].astype(str).str.contains(":", na=False)]
        if df.empty:
            continue
        df["closedPnl"] = pd.to_numeric(df["closedPnl"], errors="coerce").fillna(0.0)
        df["notional"] = pd.to_numeric(df["notional"], errors="coerce")
        df["notional"] = df["notional"].fillna(pd.to_numeric(df["price"], errors="coerce")
                                               * pd.to_numeric(df["size"], errors="coerce")).fillna(0.0)
        for w, g in df.groupby("wallet"):
            a = agg.get(w)
            if a is None:
                a = agg[w] = {"first": None, "last": None, "n": 0, "rpnl": 0.0, "nclose": 0,
                              "nwin": 0, "gwin": 0.0, "gloss": 0.0, "notsum": 0.0, "notn": 0,
                              "coins": {}, "active_days": 0}
            t = g["time"].values
            tmin, tmax = int(t.min()), int(t.max())
            a["first"] = tmin if a["first"] is None else min(a["first"], tmin)
            a["last"] = tmax if a["last"] is None else max(a["last"], tmax)
            a["n"] += len(g)
            cp = g["closedPnl"].values
            closed = cp[cp != 0.0]
            a["rpnl"] += float(cp.sum())
            a["nclose"] += int((cp != 0.0).sum())
            a["nwin"] += int((cp > 0).sum())
            a["gwin"] += float(cp[cp > 0].sum())
            a["gloss"] += float(-cp[cp < 0].sum())
            nt = g["notional"].values
            a["notsum"] += float(np.abs(nt).sum())
            a["notn"] += len(g)
            for coin, cg in g.groupby("coin"):
                a["coins"][coin] = a["coins"].get(coin, 0.0) + float(np.abs(cg["notional"].values).sum())
            # active days: files are daily, so a wallet appearing in this file = +span of days it touched here
            a["active_days"] += int(len(np.unique(t // MS_DAY)))
        if (i + 1) % 40 == 0:
            print(f"  {i+1}/{len(files)} files, {len(agg)} wallets", flush=True)

    rows = []
    for w, a in agg.items():
        if a["n"] < MIN_FILLS:
            continue
        surv = (a["last"] - a["first"]) / MS_DAY
        last_age = (NOW_MS - a["last"]) / MS_DAY
        wr = a["nwin"] / a["nclose"] if a["nclose"] else np.nan
        pf = a["gwin"] / a["gloss"] if a["gloss"] > 0 else (np.inf if a["gwin"] > 0 else 0.0)
        coins = a["coins"]
        tot = sum(coins.values()) or 1.0
        top_share = max(coins.values()) / tot if coins else np.nan
        rows.append({
            "wallet": w, "survival_days": surv, "active_days": a["active_days"],
            "last_fill_age_days": last_age, "n_fills": a["n"],
            "realized_pnl": a["rpnl"], "n_close": a["nclose"], "win_rate": wr,
            "gross_win": a["gwin"], "gross_loss": a["gloss"], "profit_factor": pf,
            "avg_notional": a["notsum"] / a["notn"] if a["notn"] else np.nan,
            "n_coins": len(coins), "top_coin_share": top_share,
        })
    out = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    out.to_parquet(OUT, index=False)
    peak_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024**3 if sys.platform == "darwin" else 1024**2)
    print(f"\nWROTE {OUT}: {len(out)} wallets (>= {MIN_FILLS} fills). peak RSS ~{peak_gb:.2f} GB", flush=True)
    # quick sanity
    q = out[(out["survival_days"] >= 180) & (out["last_fill_age_days"] <= 7) & (out["realized_pnl"] > 0)]
    print(f"survivors (>=180d, active<=7d, rpnl>0): {len(q)}", flush=True)
    print(q.sort_values("realized_pnl", ascending=False)[
        ["wallet", "survival_days", "n_close", "win_rate", "realized_pnl", "profit_factor", "avg_notional", "n_coins"]
    ].head(15).to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
