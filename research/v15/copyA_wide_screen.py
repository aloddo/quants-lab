#!/usr/bin/env python3
"""Copy A roster build -- WIDE copyability + consistency screen over ALL directional candidates.

Fixes the one-lucky-month bias: instead of ranking by leaderboard total PnL (biases to big one-time
winners), screen every directional candidate through the follower-sim WITH a per-month breakdown, and
rank by CONSISTENCY (fraction of active months positive, worst month, active-month count), not total.

Per wallet (real HL history, PERP only, follower execution via execution_model):
  active_months, pos_months, pos_frac, worst_month_k, total_real_k, perp_share, liquid_share, top coins.

RESUMABLE (appends one JSON line per wallet to OUT_JSONL; skips already-done on restart) and PACED with
retry/backoff so HL rate-limits truncate nothing. Run as a long background job; rank with the companion.
"""
from __future__ import annotations
import sys, os, time, json, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd, numpy as np

WORKERS = int(os.environ.get("COPYA_WORKERS", "6"))
sys.path.insert(0, "research/v15")
import execution_model as EM

API = "https://api.hyperliquid.xyz/info"
START_MS = 1767225600000  # 2026-01-01
OUT_JSONL = "app/data/copyA/wide_screen.jsonl"
LIQUID = {"BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE", "SUI", "AVAX", "LINK", "LTC", "BCH", "AAVE",
          "ENA", "WLD", "ARB", "OP", "TON", "APT", "SEI", "kPEPE", "PEPE", "BNB", "TRUMP", "ADA",
          "NEAR", "TIA", "INJ", "DYDX", "ZEC", "XMR", "UNI", "FARTCOIN", "POPCAT", "WIF"}


def is_perp_major(coin: str) -> bool:
    return bool(coin) and not any(c in coin for c in ("@", ":", "/", "#"))


def _post(payload, tries=5):
    for k in range(tries):
        try:
            r = requests.post(API, json=payload, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(1.0 * (k + 1))
                continue
        except Exception:
            pass
        time.sleep(0.7 * (k + 1))
    return None


def fetch_fills(addr, start_ms=START_MS, max_pages=50):
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
        time.sleep(0.4)
    return out


def screen(addr):
    fills = fetch_fills(addr)
    if not fills:
        return {"wallet": addr, "status": "no_fills"}
    df = pd.DataFrame(fills)
    df["perp"] = df["coin"].map(is_perp_major)
    n_all = len(df)
    perp = df[df["perp"]].copy()
    if perp.empty:
        return {"wallet": addr, "status": "no_perp", "n_fills": n_all, "perp_share": 0.0}
    perp["cp"] = pd.to_numeric(perp["closedPnl"], errors="coerce").fillna(0.0)
    perp["px"] = pd.to_numeric(perp["px"], errors="coerce")
    perp["sz"] = pd.to_numeric(perp["sz"], errors="coerce")
    perp["notl"] = (perp["px"] * perp["sz"]).abs()
    perp["drag"] = perp["notl"] * perp["coin"].map(lambda c: EM.slip_oneway(c) + EM.fee_rt(coin=c) / 2.0)
    perp["m"] = pd.to_datetime(perp["time"], unit="ms").dt.strftime("%Y-%m")
    g = perp.groupby("m").apply(lambda x: (x["cp"].sum() - x["drag"].sum()) / 1000.0)
    active = len(g)
    pos = int((g > 0).sum())
    liq_notl = perp[perp["coin"].isin(LIQUID)]["notl"].sum()
    tot_notl = perp["notl"].sum() or 1.0
    coins = perp["coin"].value_counts().head(5).index.tolist()
    return {
        "wallet": addr, "status": "ok", "n_fills": n_all,
        "perp_share": round(len(perp) / n_all, 3),
        "active_months": active, "pos_months": pos,
        "pos_frac": round(pos / active, 3) if active else 0.0,
        "worst_month_k": round(float(g.min()), 1), "best_month_k": round(float(g.max()), 1),
        "total_real_k": round(float(g.sum()), 1),
        "liquid_share": round(float(liq_notl / tot_notl), 3),
        "coins": ",".join(coins),
    }


def main():
    c = pd.read_parquet("app/data/copyA/directional_candidates.parquet")
    addrs = c.sort_values("pnl_all", ascending=False)["wallet"].tolist()
    done = set()
    if os.path.exists(OUT_JSONL):
        for line in open(OUT_JSONL):
            try:
                done.add(json.loads(line)["wallet"])
            except Exception:
                pass
    todo = [a for a in addrs if a not in done]
    print(f"{len(addrs)} candidates, {len(done)} done, {len(todo)} to screen, {WORKERS} workers", flush=True)

    lock = threading.Lock()
    counter = {"n": 0}

    def worker(a):
        try:
            return screen(a)
        except Exception as e:
            return {"wallet": a, "status": f"err:{e}"}

    with open(OUT_JSONL, "a") as f:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(worker, a): a for a in todo}
            for fut in as_completed(futs):
                r = fut.result()
                with lock:
                    f.write(json.dumps(r) + "\n")
                    f.flush()
                    counter["n"] += 1
                    if counter["n"] % 25 == 0:
                        print(f"  {counter['n']}/{len(todo)} screened", flush=True)
    print("DONE wide screen", flush=True)


if __name__ == "__main__":
    main()
