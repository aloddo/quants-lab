#!/usr/bin/env python3
"""V15 minimal-experiment: exact-live-policy replay of builder-perp TWAP-aggregated copy.

Implements the codex-designed pre-registered protocol
(projects/quant/copy/2026-06-07-v15-offline-validation-protocol):

  - copy leader OPEN/ADD fills on builder perps (coin contains ':')
  - aggregate by (wallet, coin, side) over rolling 600s window; trigger when cumulative
    qualifying notional crosses $1,000 (crossing fill time, NOT first slice)
  - one position per coin globally; suppress further same-(wallet,coin,side) slices while held
  - entry = first 1m candle OPEN at/after cross_time + 2s (leak-free, no intrabar look-ahead)
  - exit  = first 1m candle OPEN at/after entry_time + 3600s (60m hold)
  - 8% disaster stop on the candle path (adverse-first inside ambiguous bars)
  - caps/kills: max 4 concurrent, $600 gross, $250/coin, one/coin, daily -$25, exp -$60
  - costs: builder-perp RT taker from hl_fee_schedule (hip3_mult) + builder slippage per side
  - nulls: same-day random-time, sign-flip

Read-only (HL public info API + local fee schedule). Does NOT trade.
Decision uses FULL-COST NET bps at the 60m hold.
"""
from __future__ import annotations
import argparse, time, json, os, math, random
from dataclasses import dataclass, field
import numpy as np
import requests

HL = "https://api.hyperliquid.xyz/info"
FEE_FILE = "app/data/v15/hl_fee_schedule.json"
CACHE_DIR = "app/data/v15/copy_replay_cache"
SIZE_USD = 150.0
GROSS_CAP = 600.0
COIN_CAP = 250.0
MAX_CONC = 4
HOLD_S = 3600
DISASTER = 0.08
DAILY_MAX_LOSS = -25.0
EXP_MAX_LOSS = -60.0
TWAP_WINDOW_MS = 600_000
AGG_THRESH = 1000.0
LATENCY_MS = 2_000
SEED = 12345


def _post(payload, tries=6):
    for i in range(tries):
        try:
            r = requests.post(HL, json=payload, timeout=20)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(0.6 * (2 ** i)); continue
            return r.json()
        except Exception:
            time.sleep(0.6 * (2 ** i))
    return None


def pull_fills(wallet: str, start_ms: int, end_ms: int) -> list:
    """Paginate userFillsByTime (2000-row cap) forward by time. Cache raw to disk (never delete)."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache = f"{CACHE_DIR}/fills_{wallet}_{start_ms}_{end_ms}.json"
    if os.path.exists(cache):
        with open(cache) as f:
            return json.load(f)
    out, cur, seen = [], start_ms, set()
    while cur < end_ms:
        batch = _post({"type": "userFillsByTime", "user": wallet, "startTime": cur, "endTime": end_ms})
        if not isinstance(batch, list) or not batch:
            break
        new = [f for f in batch if f.get("tid") not in seen]
        for f in new:
            seen.add(f.get("tid"))
        out.extend(new)
        if len(batch) < 2000:
            break
        mx = max(f["time"] for f in batch)
        if mx <= cur:
            break
        cur = mx + 1
        time.sleep(0.15)
    out.sort(key=lambda f: f["time"])
    with open(cache, "w") as f:
        json.dump(out, f)
    return out


def pull_candles(coin: str, start_ms: int, end_ms: int) -> np.ndarray:
    """Paginate 1m candleSnapshot. Returns array of (t_open_ms, open, high, low) sorted by t."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe = coin.replace(":", "_").replace("/", "_")
    cache = f"{CACHE_DIR}/candles_{safe}_{start_ms}_{end_ms}.npy"
    if os.path.exists(cache):
        return np.load(cache)
    rows, cur = [], start_ms
    step = 4000 * 60_000  # ~4000 1m bars per call
    while cur < end_ms:
        ce = min(cur + step, end_ms)
        cand = _post({"type": "candleSnapshot", "req": {"coin": coin, "interval": "1m", "startTime": cur, "endTime": ce}})
        if isinstance(cand, list) and cand:
            for c in cand:
                rows.append((int(c["t"]), float(c["o"]), float(c["h"]), float(c["l"])))
            cur = max(int(c["t"]) for c in cand) + 60_000
        else:
            cur = ce + 1
        time.sleep(0.12)
    if not rows:
        arr = np.empty((0, 4))
    else:
        arr = np.array(sorted(set(rows)), dtype=float)
    np.save(cache, arr)
    return arr


@dataclass
class Trigger:
    wallet: str
    arm: str
    coin: str
    side: int          # +1 long, -1 short
    t_ms: int          # crossing fill time
    notional: float


def build_triggers(fills: list, wallet: str, arm: str) -> list:
    """Rolling 600s (wallet,coin,side) aggregation; trigger on cumulative open/add notional crossing $1k."""
    opens = []
    for f in fills:
        d = f.get("dir", "")
        if not d.startswith("Open"):       # opens/adds only (Open Long / Open Short); skip closes
            continue
        if ":" not in f.get("coin", ""):   # builder perps only
            continue
        side = 1 if d == "Open Long" else (-1 if d == "Open Short" else 0)
        if side == 0:
            continue
        opens.append((int(f["time"]), f["coin"], side, float(f["sz"]) * float(f["px"])))
    opens.sort(key=lambda x: x[0])
    from collections import deque, defaultdict
    win = defaultdict(deque)   # (coin,side) -> deque[(t, notional)]
    fired = set()              # (coin,side) currently in "fired" state (until decayed)
    triggers = []
    for t, coin, side, notion in opens:
        k = (coin, side)
        dq = win[k]
        dq.append((t, notion))
        while dq and dq[0][0] < t - TWAP_WINDOW_MS:
            dq.popleft()
        cum = sum(n for _, n in dq)
        if k not in fired and cum >= AGG_THRESH:
            triggers.append(Trigger(wallet, arm, coin, side, t, cum))
            fired.add(k)
        if k in fired and cum < AGG_THRESH:
            fired.discard(k)   # window decayed below threshold -> re-armable
    return triggers


def _mark_at(cand: np.ndarray, t_ms: int):
    """First candle OPEN at/after t_ms. Returns (idx, open) or (None, None)."""
    if cand.shape[0] == 0:
        return None, None
    ts = cand[:, 0]
    i = int(np.searchsorted(ts, t_ms, side="left"))
    if i >= cand.shape[0]:
        return None, None
    return i, cand[i, 1]


def sim_trade(cand: np.ndarray, t_entry_ms: int, side: int, fee_rt: float, slip_side: float):
    """Enter at first open>=entry; walk path for 8% disaster; exit at first open>=entry+3600s.
    Returns dict(net_bps, gross_bps, win, entry_px, exit_px, exited_disaster) or None."""
    ei, epx = _mark_at(cand, t_entry_ms)
    if ei is None or epx <= 0:
        return None
    xt = cand[ei, 0] + HOLD_S * 1000
    xi, xpx = _mark_at(cand, xt)
    end_i = xi if xi is not None else cand.shape[0] - 1
    # disaster stop on path (adverse-first): long->low, short->high
    disaster = False
    stop_px = epx * (1 - DISASTER) if side == 1 else epx * (1 + DISASTER)
    for j in range(ei, end_i + 1):
        lo, hi = cand[j, 3], cand[j, 2]
        if side == 1 and lo <= stop_px:
            xpx = stop_px; disaster = True; break
        if side == -1 and hi >= stop_px:
            xpx = stop_px; disaster = True; break
    if xpx is None or xpx <= 0:
        return None
    gross = side * (xpx / epx - 1) * 1e4
    net = gross - fee_rt * 1e4 - slip_side * 2 * 1e4
    return dict(net_bps=net, gross_bps=gross, win=net > 0, entry_px=epx, exit_px=xpx,
                exited_disaster=disaster, entry_t=int(cand[ei, 0]))


def apply_caps(triggers: list, candles: dict, fee_rt: float, slip_side: float):
    """Chronological cap/kill simulation. Returns (executed_trades, n_skipped)."""
    triggers = sorted(triggers, key=lambda tr: tr.t_ms)
    open_pos = {}     # coin -> exit_t_ms
    executed, skipped = [], 0
    daily_pnl = {}    # day -> realized $ (approx via net_bps * size)
    exp_pnl = 0.0
    for tr in triggers:
        # release expired
        for c in [c for c, xt in open_pos.items() if xt <= tr.t_ms]:
            del open_pos[c]
        day = time.strftime("%Y-%m-%d", time.gmtime(tr.t_ms / 1000))
        if daily_pnl.get(day, 0.0) <= DAILY_MAX_LOSS or exp_pnl <= EXP_MAX_LOSS:
            skipped += 1; continue
        if tr.coin in open_pos or len(open_pos) >= MAX_CONC or len(open_pos) * SIZE_USD >= GROSS_CAP:
            skipped += 1; continue
        cand = candles.get(tr.coin)
        if cand is None:
            skipped += 1; continue
        res = sim_trade(cand, tr.t_ms + LATENCY_MS, tr.side, fee_rt, slip_side)
        if res is None:
            skipped += 1; continue
        open_pos[tr.coin] = res["entry_t"] + HOLD_S * 1000
        pnl_usd = res["net_bps"] / 1e4 * SIZE_USD
        daily_pnl[day] = daily_pnl.get(day, 0.0) + pnl_usd
        exp_pnl += pnl_usd
        executed.append(dict(wallet=tr.wallet, arm=tr.arm, coin=tr.coin, side=tr.side,
                             t=tr.t_ms, **res))
    return executed, skipped


def stats(nets: list) -> dict:
    a = np.array(nets, dtype=float)
    n = len(a)
    if n == 0:
        return dict(n=0)
    mean = float(a.mean()); sd = float(a.std(ddof=1)) if n > 1 else float("nan")
    se = sd / math.sqrt(n) if n > 1 else float("nan")
    t = mean / se if se and not math.isnan(se) and se > 0 else float("nan")
    wins = a[a > 0]; losses = a[a < 0]
    pf = float(wins.sum() / -losses.sum()) if losses.sum() < 0 else float("inf")
    return dict(n=n, mean_net_bps=round(mean, 2), t_stat=round(t, 2),
                win_pct=round(100 * (a > 0).mean(), 1), pf=round(pf, 2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--fee-mode", choices=["builder", "main"], default="builder",
                    help="builder = hip3_mult applied (conservative); main = 8.64bps RT")
    ap.add_argument("--slip-bps-side", type=float, default=3.0, help="builder slippage per side, bps")
    args = ap.parse_args()
    random.seed(SEED); np.random.seed(SEED)

    with open(FEE_FILE) as f:
        fee = json.load(f)
    base_ow = fee["base_taker_oneway"]
    if args.fee_mode == "builder":
        fee_rt = base_ow * fee.get("hip3_mult", 2.0) * 2
    else:
        fee_rt = 0.000864
    slip_side = args.slip_bps_side / 1e4
    print(f"fee_mode={args.fee_mode} fee_rt={fee_rt*1e4:.2f}bps slip/side={args.slip_bps_side}bps "
          f"(total RT cost ~{(fee_rt + 2*slip_side)*1e4:.1f}bps)")

    arms = {"realpnl": "config/copy_live_realpnl_5.json", "backtest": "config/copy_live_backtest_5.json"}
    now = int(time.time() * 1000); start = now - args.days * 24 * 3600 * 1000

    all_trig = []
    for arm, cfgpath in arms.items():
        with open(cfgpath) as f:
            wallets = json.load(f)["wallets"]
        for w in wallets:
            fills = pull_fills(w, start, now)
            trig = build_triggers(fills, w, arm)
            all_trig.extend(trig)
            print(f"  {arm} {w[:10]} fills={len(fills)} triggers={len(trig)}")

    coins = sorted(set(tr.coin for tr in all_trig))
    print(f"triggers={len(all_trig)} across {len(coins)} builder coins: {coins}")
    candles = {}
    for c in coins:
        candles[c] = pull_candles(c, start, now + HOLD_S * 1000)
        print(f"  candles {c}: {candles[c].shape[0]} bars")

    executed, skipped = apply_caps(all_trig, candles, fee_rt, slip_side)
    nets = [e["net_bps"] for e in executed]
    grosses = [e["gross_bps"] for e in executed]
    print("\n=== PRIMARY (executed, full-cost net) ===")
    print(f"executed={len(executed)} skipped_by_caps={skipped}")
    print("ALL:", stats(nets))
    print("gross mean bps:", round(float(np.mean(grosses)), 2) if grosses else "NA")
    for arm in arms:
        an = [e["net_bps"] for e in executed if e["arm"] == arm]
        print(f"ARM {arm}:", stats(an))
    print("\n=== per-wallet ===")
    bw = {}
    for e in executed:
        bw.setdefault(e["wallet"], []).append(e["net_bps"])
    for w, ns in sorted(bw.items(), key=lambda x: -np.mean(x[1])):
        print(f"  {w[:10]} {e and ''}", stats(ns))

    # ---- NULLS ----
    print("\n=== NULLS ===")
    # sign-flip: same trades, opposite side
    flip = []
    for e in executed:
        cand = candles[e["coin"]]
        r = sim_trade(cand, e["t"] + LATENCY_MS, -e["side"], fee_rt, slip_side)
        if r:
            flip.append(r["net_bps"])
    print("sign-flip:", stats(flip))
    # same-day random-time: per executed trade, same coin/side, random ts same UTC day
    rnd = []
    for e in executed:
        cand = candles[e["coin"]]
        day0 = int(e["t"] // (86400_000)) * 86400_000
        for _ in range(3):
            rt = day0 + random.randint(0, 86400_000 - HOLD_S * 1000)
            r = sim_trade(cand, rt, e["side"], fee_rt, slip_side)
            if r:
                rnd.append(r["net_bps"]); break
    print("same-day random:", stats(rnd))
    sd_mean = stats(rnd).get("mean_net_bps", float("nan"))
    pr_mean = stats(nets).get("mean_net_bps", float("nan"))
    if isinstance(sd_mean, (int, float)) and isinstance(pr_mean, (int, float)):
        print(f"beats same-day null by: {round(pr_mean - sd_mean, 2)} bps")

    # ---- PRE-REGISTERED DECISION ----
    s = stats(nets)
    arm_means = {arm: stats([e["net_bps"] for e in executed if e["arm"] == arm]).get("mean_net_bps", 0)
                 for arm in arms}
    print("\n=== PRE-REGISTERED DECISION ===")
    print(f"N={s.get('n')} net={s.get('mean_net_bps')}bps t={s.get('t_stat')} "
          f"win%={s.get('win_pct')} arms={arm_means}")
    decisive_keep = (s.get("n", 0) >= 60 and s.get("mean_net_bps", -99) >= 15
                     and all(v > 0 for v in arm_means.values())
                     and stats(flip).get("mean_net_bps", 99) < 0
                     and isinstance(pr_mean, (int, float)) and isinstance(sd_mean, (int, float))
                     and (pr_mean - sd_mean) >= 10)
    kill = (s.get("n", 0) >= 60 and s.get("mean_net_bps", 99) <= 5)
    print("VERDICT:", "DECISIVE-KEEP" if decisive_keep else ("KILL" if kill else "INCONCLUSIVE -> upgrade to full run"))

    out = dict(fee_mode=args.fee_mode, fee_rt_bps=round(fee_rt*1e4, 2), slip_side_bps=args.slip_bps_side,
               n_triggers=len(all_trig), executed=len(executed), skipped=skipped,
               primary=s, arms=arm_means, sign_flip=stats(flip), same_day=stats(rnd),
               gross_mean_bps=round(float(np.mean(grosses)), 2) if grosses else None)
    os.makedirs("app/data/v15", exist_ok=True)
    with open(f"app/data/v15/copy_builder_replay_{args.fee_mode}.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nwrote app/data/v15/copy_builder_replay_{args.fee_mode}.json")


if __name__ == "__main__":
    main()
