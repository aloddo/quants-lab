#!/usr/bin/env python3
"""Copy A -- WIDE copyability + consistency screen over the LOCAL S3 fills universe.

Alberto correction 2026-07-04 (msg 10599): the full fills are ALREADY on disk at
app/data/hl_s3_fills_v2_by_wallet/{wallet}.parquet (20,378 wallets, downloaded
from the hl-mainnet-node S3 bucket). Do NOT hit the HL API to re-fetch what we
already have. This screener reads local parquet only -- ZERO network.

Local schema: coin, side, size, price, time, tid, dir, closedPnl, startPosition,
fee, builderFee, deployerFee, crossed, signed_sz.

Per wallet (PERP only, follower execution via execution_model), we compute:
  perp_share, maker_share (crossed==False = passive/MM), active_months, pos_months,
  pos_frac, worst_month_k, best_month_k, total_real_k, liquid_share, top coins.

follower-realized per month = sum(closedPnl on perp) - execution_drag(slip + fee/2).
maker_share high => MM/passive market-maker => a follower cannot copy spread capture.

Memory-safe: one JSON line per wallet appended to OUT_JSONL, resumable (skip done).
Parallel across wallets with a thread pool (pure local I/O, no rate limit).
"""
from __future__ import annotations
import sys, os, json, glob, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
sys.path.insert(0, "research/v15")
import execution_model as EM

FILLS_DIR = "app/data/hl_s3_fills_v2_by_wallet"
OUT_JSONL = "app/data/copyA/local_screen.jsonl"
WORKERS = int(os.environ.get("COPYA_WORKERS", "8"))
LIQUID = {"BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE", "SUI", "AVAX", "LINK", "LTC", "BCH", "AAVE",
          "ENA", "WLD", "ARB", "OP", "TON", "APT", "SEI", "kPEPE", "PEPE", "BNB", "TRUMP", "ADA",
          "NEAR", "TIA", "INJ", "DYDX", "ZEC", "XMR", "UNI", "FARTCOIN", "POPCAT", "WIF"}


def is_perp_major(coin: str) -> bool:
    return bool(coin) and not any(c in coin for c in ("@", ":", "/", "#"))


def screen_path(path: str):
    wallet = os.path.basename(path)[:-8]
    try:
        df = pd.read_parquet(
            path,
            columns=["coin", "size", "price", "time", "closedPnl", "crossed"],
        )
    except Exception as e:
        return {"wallet": wallet, "status": f"err:{type(e).__name__}"}
    if df.empty:
        return {"wallet": wallet, "status": "no_fills"}
    n_all = len(df)
    df["perp"] = df["coin"].map(is_perp_major)
    perp = df[df["perp"]].copy()
    if perp.empty:
        return {"wallet": wallet, "status": "no_perp", "n_fills": n_all, "perp_share": 0.0}
    perp["cp"] = pd.to_numeric(perp["closedPnl"], errors="coerce").fillna(0.0)
    perp["px"] = pd.to_numeric(perp["price"], errors="coerce")
    perp["sz"] = pd.to_numeric(perp["size"], errors="coerce")
    perp["notl"] = (perp["px"] * perp["sz"]).abs()
    perp["drag"] = perp["notl"] * perp["coin"].map(lambda c: EM.slip_oneway(c) + EM.fee_rt(coin=c) / 2.0)
    perp["m"] = pd.to_datetime(perp["time"], unit="ms").dt.strftime("%Y-%m")
    g = perp.groupby("m").apply(lambda x: (x["cp"].sum() - x["drag"].sum()) / 1000.0)
    active = len(g)
    pos = int((g > 0).sum())
    liq_notl = perp[perp["coin"].isin(LIQUID)]["notl"].sum()
    tot_notl = perp["notl"].sum() or 1.0
    # crossed==False => passive/maker fill; high maker share => MM => uncopyable
    maker_share = float((~perp["crossed"].astype(bool)).mean())
    coins = perp["coin"].value_counts().head(5).index.tolist()
    return {
        "wallet": wallet, "status": "ok", "n_fills": n_all,
        "perp_share": round(len(perp) / n_all, 3),
        "maker_share": round(maker_share, 3),
        "active_months": active, "pos_months": pos,
        "pos_frac": round(pos / active, 3) if active else 0.0,
        "worst_month_k": round(float(g.min()), 1), "best_month_k": round(float(g.max()), 1),
        "total_real_k": round(float(g.sum()), 1),
        "liquid_share": round(float(liq_notl / tot_notl), 3),
        "coins": ",".join(coins),
    }


def main():
    paths = sorted(glob.glob(f"{FILLS_DIR}/*.parquet"))
    done = set()
    if os.path.exists(OUT_JSONL):
        for line in open(OUT_JSONL):
            try:
                done.add(json.loads(line)["wallet"])
            except Exception:
                pass
    todo = [p for p in paths if os.path.basename(p)[:-8] not in done]
    print(f"{len(paths)} local wallets, {len(done)} done, {len(todo)} to screen, {WORKERS} workers", flush=True)
    lock = threading.Lock()
    counter = {"n": 0}
    with open(OUT_JSONL, "a") as f:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(screen_path, p): p for p in todo}
            for fut in as_completed(futs):
                r = fut.result()
                with lock:
                    f.write(json.dumps(r) + "\n")
                    f.flush()
                    counter["n"] += 1
                    if counter["n"] % 1000 == 0:
                        print(f"  {counter['n']}/{len(todo)} screened", flush=True)
    print("DONE local screen", flush=True)


if __name__ == "__main__":
    main()
