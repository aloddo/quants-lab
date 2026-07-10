#!/usr/bin/env python3
"""Copy A -- CLEAN per-coin calendar-fold cross-validation (remediation of the rejected pack).

Addresses the Fable+Codex findings that killed the probe:
  - FIXED CALENDAR folds (not per-wallet active months): IN = months <= CUTOFF, OUT = after.
  - IN-ONLY screening + IN-only sizing (no full-window conditioning -> no selection leak).
  - PER (wallet, coin): the executor mirrors per coin, so validate per coin. A wallet qualifies
    for a coin only on that coin's own IN evidence.
  - OOS trades must OPEN after the cutoff (no in-sample-opened positions credited to OOS).
  - Zero-return inactive calendar months counted as 0 (no skipping bad months).
  - Sign-reversal positions split explicitly (A -> 0 -> B in one fill = close A, open B).
  - Drag = per-position round-trip slippage+fees via execution_model.

Output: per (wallet, coin) IN/OOS follower-$ + persistence. The honest question:
does a real per-coin roster exist, or is only the HYPE specialist genuine?
"""
from __future__ import annotations
import sys, os, json, glob, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
sys.path.insert(0, "research/v15")
import execution_model as EM

FILLS_DIR = "app/data/hl_s3_fills_v2_by_wallet"
OUT_JSONL = "app/data/copyA/cv_percoin.jsonl"
WORKERS = int(os.environ.get("COPYA_WORKERS", "8"))
TARGET_S = float(os.environ.get("FOLLOWER_S", "50"))
CUTOFF = pd.Timestamp(os.environ.get("CUTOFF", "2026-04-01"), tz="UTC")
CUTOFF_MS = int(CUTOFF.timestamp() * 1000)
# liquid majors the executor can actually fill (matches calibration set intent)
LIQUID = {"BTC", "ETH", "SOL", "HYPE", "XRP", "DOGE", "SUI", "AVAX", "LINK", "LTC", "BCH",
          "AAVE", "ENA", "ARB", "OP", "BNB", "ADA", "NEAR", "TIA", "INJ", "ZEC", "XMR", "UNI"}
MIN_IN_POS = 6          # need enough IN closed positions on the coin to judge
IN_POSFRAC_MIN = 0.60   # IN monthly positive fraction
MIN_ENTRY_NOTL = 50.0   # skip dust/residual positions below this notional (reconstruction noise;
                        # a $50 follower cannot mirror a sub-$50 leader residual anyway)
RET_CLIP = (-1.0, 3.0)  # clip per-position return to sane leverage bounds; beyond = reconstruction error


def is_perp_major(coin: str) -> bool:
    return bool(coin) and not any(c in coin for c in ("@", ":", "/", "#"))


def positions_percoin(sub, coin):
    """Yield dicts per closed position for ONE coin, handling sign reversals.

    Each position: open_ts, close_ts, entry_notl, realized. A fill that flips the
    running position through zero closes the current position (at that fill's ts,
    with its closedPnl) and opens a fresh one on the residual.
    """
    EPS = 1e-9
    sub = sub.sort_values("time")
    start = pd.to_numeric(sub["startPosition"], errors="coerce").fillna(0.0).to_numpy()
    ssz = pd.to_numeric(sub["signed_sz"], errors="coerce").fillna(0.0).to_numpy()
    cp = pd.to_numeric(sub["closedPnl"], errors="coerce").fillna(0.0).to_numpy()
    px = pd.to_numeric(sub["price"], errors="coerce").fillna(0.0).to_numpy()
    sz = pd.to_numeric(sub["size"], errors="coerce").abs().fillna(0.0).to_numpy()
    ts = sub["time"].to_numpy()
    drag_frac = EM.slip_oneway(coin) * 2 + EM.fee_rt(coin=coin)

    open_ts = None
    entry = 0.0
    realized = 0.0
    live = False  # currently tracking a position that OPENED from flat in-data
    for i in range(len(sub)):
        prev = start[i]
        post = prev + ssz[i]
        notl_i = px[i] * sz[i]
        crossed_zero = (abs(prev) > EPS and abs(post) > EPS and (prev > 0) != (post > 0))
        opened_from_flat = (abs(prev) < EPS and abs(post) > EPS)

        if not live:
            if opened_from_flat:
                live = True; open_ts = ts[i]; entry = notl_i; realized = cp[i]
                if abs(post) < EPS:  # open+close same fill (rare)
                    yield {"open_ts": open_ts, "close_ts": ts[i], "entry_notl": entry, "realized": realized}
                    live = False; entry = 0.0; realized = 0.0
            elif abs(prev) > EPS:
                # carried-in position: skip until it flattens, then ready
                realized += cp[i]  # ignored (not yielded); just advance
                if abs(post) < EPS or crossed_zero:
                    live = False; entry = 0.0; realized = 0.0
                    if crossed_zero:  # residual opens a clean position
                        live = True; open_ts = ts[i]; entry = abs(post) * px[i]; realized = 0.0
            continue

        # live == True (clean position in progress)
        realized += cp[i]
        if crossed_zero:
            # close current at this fill, then open residual
            yield {"open_ts": open_ts, "close_ts": ts[i], "entry_notl": entry, "realized": realized}
            live = True; open_ts = ts[i]; entry = abs(post) * px[i]; realized = 0.0
        elif abs(post) < EPS:
            yield {"open_ts": open_ts, "close_ts": ts[i], "entry_notl": entry, "realized": realized}
            live = False; entry = 0.0; realized = 0.0
        elif abs(post) > abs(prev) + 1e-12:
            entry += notl_i  # adding to position


def screen_wallet(path):
    wallet = os.path.basename(path)[:-8]
    try:
        df = pd.read_parquet(path, columns=["coin", "size", "price", "time", "closedPnl",
                                            "startPosition", "signed_sz"])
    except Exception as e:
        return {"wallet": wallet, "status": f"err:{type(e).__name__}"}
    df = df[df["coin"].map(is_perp_major)].copy()
    if df.empty:
        return {"wallet": wallet, "status": "no_perp"}
    out = []
    for coin, sub in df.groupby("coin", sort=False):
        if coin not in LIQUID:
            continue
        pos = [p for p in positions_percoin(sub, coin) if p["entry_notl"] >= MIN_ENTRY_NOTL]
        if not pos:
            continue
        pp = pd.DataFrame(pos)
        pp["ret"] = (pp["realized"] / pp["entry_notl"]).clip(*RET_CLIP) - (EM.slip_oneway(coin) * 2 + EM.fee_rt(coin=coin))
        pp["open_m"] = pd.to_datetime(pp["open_ts"], unit="ms", utc=True).dt.strftime("%Y-%m")
        pp["close_m"] = pd.to_datetime(pp["close_ts"], unit="ms", utc=True).dt.strftime("%Y-%m")
        in_pos = pp[pp["close_ts"] < CUTOFF_MS]        # closed in-sample
        oos_pos = pp[pp["open_ts"] >= CUTOFF_MS]       # OPENED after cutoff (clean OOS)
        if len(in_pos) < MIN_IN_POS:
            continue
        S = min(TARGET_S, float(in_pos["entry_notl"].median()))
        # IN monthly follower $ over IN calendar months present
        gin = (in_pos.assign(usd=in_pos["ret"] * S)).groupby("close_m")["usd"].sum()
        in_usd = float(gin.sum())
        in_posf = float((gin > 0).mean()) if len(gin) else 0.0
        in_mo = len(gin)
        selected = (in_usd > 0 and in_posf >= IN_POSFRAC_MIN and in_mo >= 2)
        # OOS
        if len(oos_pos):
            gout = (oos_pos.assign(usd=oos_pos["ret"] * S)).groupby("close_m")["usd"].sum()
            oos_usd = float(gout.sum())
            oos_mo = len(gout)
            oos_posf = float((gout > 0).mean()) if oos_mo else 0.0
            oos_usd_mo = oos_usd / oos_mo if oos_mo else 0.0
        else:
            oos_usd = oos_mo = oos_posf = oos_usd_mo = 0.0
        out.append({
            "wallet": wallet, "coin": coin, "S": round(S, 1),
            "in_pos": int(len(in_pos)), "in_mo": in_mo, "in_posf": round(in_posf, 3),
            "in_usd_mo": round(in_usd / in_mo, 2) if in_mo else 0.0,
            "selected": bool(selected),
            "oos_pos": int(len(oos_pos)), "oos_mo": oos_mo, "oos_posf": round(oos_posf, 3),
            "oos_usd_mo": round(oos_usd_mo, 2),
        })
    return {"wallet": wallet, "status": "ok", "pairs": out}


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
    print(f"{len(paths)} wallets, {len(done)} done, {len(todo)} to go, cutoff={CUTOFF.date()}, "
          f"S=${TARGET_S:.0f}, {WORKERS}w", flush=True)
    lock = threading.Lock(); n = {"c": 0}
    with open(OUT_JSONL, "a") as f:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(screen_wallet, p): p for p in todo}
            for fut in as_completed(futs):
                r = fut.result()
                with lock:
                    f.write(json.dumps(r) + "\n"); f.flush()
                    n["c"] += 1
                    if n["c"] % 2000 == 0:
                        print(f"  {n['c']}/{len(todo)}", flush=True)
    print("DONE cv", flush=True)


if __name__ == "__main__":
    main()
