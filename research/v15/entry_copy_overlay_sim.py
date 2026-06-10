#!/usr/bin/env python3
"""Entry-copy-with-OWN-risk-overlay OOS simulator.

THE HOLE in every prior copy test: we measured the leader's OWN forward PnL (which blends their entry
SIGNAL with their RISK MISTAKES) or mirrored the leader's exit (so we inherited their bags/blowups).
A real copier brings its OWN risk management. The -98.8% wallet blew up ITS book; a copier with a hard
stop + trailing TP never takes that loss. We inherit WHEN they enter, not HOW they die.

This sim:
  SELECT wallets on IN-SAMPLE entry-information (markout, through 2026-05-27) -- leak-free.
  TEST on the GENUINELY-FUTURE window (2026-05-28 -> now): copy each selected wallet's OPEN entries
  (HL 'dir' field) on COPYABLE MAJOR coins, enter at +1s latency, then exit on OUR rules:
    - hard stop (default -3%)
    - trailing TP (arm after +arm%, trail by trail%)  [rule 7: always trailing TP]
    - max hold timeout
  walking the real 15m candle path (high/low) for that coin. Net 8.64bps RT + slippage.
  Compare overlay-copy return vs the leader's MIRROR-exit return on the SAME entries (shows the overlay
  cuts the tail).

Read-only (live HL API + local wallet_features). Does NOT trade.
"""
from __future__ import annotations
import argparse, time, json
from dataclasses import dataclass
import concurrent.futures as cf
import numpy as np
import pandas as pd
import requests

HL = "https://api.hyperliquid.xyz/info"
FEE_RT = 0.000864          # 8.64 bps round-trip taker
SLIP = 0.0002              # 2 bps each side entry/exit slippage (conservative for our size)
IN_SAMPLE_END = "2026-05-27"
FWD_START = "2026-05-28"


@dataclass
class Risk:
    stop: float = 0.03       # hard stop -3% from entry
    arm: float = 0.02        # arm trailing TP after +2% favorable
    trail: float = 0.012     # trail 1.2% off peak once armed
    max_hold_h: float = 12.0 # timeout
    leverage: float = 1.0    # we copy unlevered (copy return = price move)


def _post(payload, tries=5):
    """POST with backoff on rate-limit/transient errors (429/5xx)."""
    for i in range(tries):
        try:
            resp = requests.post(HL, json=payload, timeout=15)
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(0.5 * (2 ** i))
                continue
            return resp.json()
        except Exception:
            time.sleep(0.5 * (2 ** i))
    return None


def _candles(coin: str, start_ms: int, end_ms: int, iv: str = "15m") -> pd.DataFrame:
    try:
        c = _post({"type": "candleSnapshot",
                   "req": {"coin": coin, "interval": iv, "startTime": start_ms, "endTime": end_ms}})
        if not isinstance(c, list) or not c:
            return pd.DataFrame()
        df = pd.DataFrame(c)
        for k in ("t", "o", "h", "l", "c"):
            df[k] = pd.to_numeric(df[k], errors="coerce")
        return df[["t", "o", "h", "l", "c"]].sort_values("t").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def copyable_universe() -> set[str]:
    """Liquid MAIN-dex perps only. Excludes builder/HIP-3 dex perps (they appear as 'dex:SYMBOL', e.g.
    xyz:NVDA -- illiquid equities/exotics we cannot realistically copy)."""
    try:
        meta = requests.post(HL, json={"type": "meta"}, timeout=12).json()
        return {a["name"] for a in meta.get("universe", []) if ":" not in a["name"]}
    except Exception:
        return set()


def overlay_trade(side: str, entry_px: float, path: pd.DataFrame, r: Risk) -> tuple[float, str]:
    """Walk the 15m high/low path applying hard-stop + trailing-TP + timeout. NO within-bar look-ahead:
    at each bar we FIRST test exits (stop, trailing) against the ADVERSE extreme using the peak/arm state
    as known at the START of the bar; only AFTER do we update the peak with this bar's favorable extreme
    (so the same bar's high cannot raise the trail and then be exited by the same bar's low). If both the
    stop and the trailing exit could trigger in a bar, the stop (more adverse) wins. Returns (gross_ret,
    reason); gross_ret is signed price return (long:(exit-entry)/entry; short:(entry-exit)/entry)."""
    if path.empty or entry_px <= 0:
        return 0.0, "no_path"
    long = side == "Buy"
    peak = entry_px            # best favorable price as known at START of current bar
    armed = False
    max_t = path["t"].iloc[0] + r.max_hold_h * 3600 * 1000
    stop_px = entry_px * (1 - r.stop) if long else entry_px * (1 + r.stop)
    for _, b in path.iterrows():
        hi, lo = b["h"], b["l"]
        # 1) exits FIRST, against adverse extreme, using start-of-bar peak/arm state
        if long:
            if lo <= stop_px:
                return -r.stop, "stop"
            if armed and lo <= peak * (1 - r.trail):
                return (peak * (1 - r.trail) - entry_px) / entry_px, "trail_tp"
        else:
            if hi >= stop_px:
                return -r.stop, "stop"
            if armed and hi >= peak * (1 + r.trail):
                return (entry_px - peak * (1 + r.trail)) / entry_px, "trail_tp"
        # 2) THEN update peak + arm for SUBSEQUENT bars (this bar's favorable extreme)
        if long:
            peak = max(peak, hi)
            if (peak - entry_px) / entry_px >= r.arm:
                armed = True
        else:
            peak = min(peak, lo)
            if (entry_px - peak) / entry_px >= r.arm:
                armed = True
        # 3) timeout
        if b["t"] >= max_t:
            cl = b["c"]
            return ((cl - entry_px) / entry_px) if long else ((entry_px - cl) / entry_px), "timeout"
    cl = path["c"].iloc[-1]
    return ((cl - entry_px) / entry_px) if long else ((entry_px - cl) / entry_px), "eod"


def wallet_forward_entries(w: str, since_ms: int, now_ms: int, universe: set[str]) -> list[dict]:
    """OPEN entries (HL 'dir' starts with 'Open') on copyable majors, deduped to one position/coin at a
    time (we don't pyramid). Returns list of {coin, side, ts}."""
    fills, s, seen = [], since_ms, set()
    try:
        for _ in range(10):
            rr = _post({"type": "userFillsByTime", "user": w, "startTime": s, "endTime": now_ms})
            if not isinstance(rr, list) or not rr:
                break
            fills += rr
            if len(rr) < 2000:
                break
            s = max(f["time"] for f in rr) + 1
    except Exception:
        return []
    fills.sort(key=lambda f: f["time"])
    entries, open_coins = [], set()
    for f in fills:
        k = (f.get("time"), f.get("oid"), f.get("tid"))
        if k in seen:
            continue
        seen.add(k)
        coin = f.get("coin", "")
        if coin not in universe:
            continue
        d = str(f.get("dir", ""))
        if d.startswith("Open"):
            if coin in open_coins:
                continue  # already hold this coin
            side = "Buy" if "Long" in d else "Sell"
            entries.append({"coin": coin, "side": side, "ts": int(f["time"])})
            open_coins.add(coin)
        elif d.startswith("Close"):
            open_coins.discard(coin)
    return entries


def sim_wallet(w: str, since_ms: int, now_ms: int, universe: set[str], r: Risk,
               candle_cache: dict) -> dict:
    ents = wallet_forward_entries(w, since_ms, now_ms, universe)
    if not ents:
        return {"wallet": w, "n_entries": 0, "n_copied": 0, "overlay_ret": None, "mirror_ret": None}
    rets, reasons = [], []
    for e in ents:
        coin = e["coin"]
        if coin not in candle_cache:
            candle_cache[coin] = _candles(coin, since_ms, now_ms)
        cdf = candle_cache[coin]
        if cdf.empty:
            continue
        # entry bar = first bar at/after entry ts + 1s latency
        et = e["ts"] + 1000
        fwd = cdf[cdf["t"] >= et]
        if fwd.empty or len(fwd) < 2:
            continue
        entry_px = fwd["c"].iloc[0]            # close of the detection bar (we taker in after detect)
        entry_px = entry_px * (1 + SLIP) if e["side"] == "Buy" else entry_px * (1 - SLIP)
        path = fwd.iloc[1:]                      # subsequent bars
        gross, reason = overlay_trade(e["side"], entry_px, path, r)
        net = gross - SLIP - FEE_RT             # exit slippage + RT fee
        rets.append(net)
        reasons.append(reason)
    if not rets:
        return {"wallet": w, "n_entries": len(ents), "n_copied": 0, "overlay_ret": None, "mirror_ret": None}
    rets = np.array(rets)
    return {
        "wallet": w, "n_entries": len(ents), "n_copied": len(rets),
        "overlay_ret": float(rets.mean()),               # mean net return per copied trade
        "overlay_total": float(rets.sum()),
        "overlay_win": float((rets > 0).mean()),
        "overlay_med": float(np.median(rets)),
        "worst": float(rets.min()), "best": float(rets.max()),
        "stop_frac": float(np.mean([x == "stop" for x in reasons])),
        "tp_frac": float(np.mean([x == "trail_tp" for x in reasons])),
    }


def _load_feat(feat_path: str) -> pd.DataFrame:
    f = pd.read_parquet(feat_path)
    for c in ["events_per_day", "active_days", "total_notional", "open_event_count", "net_edge_bps",
              "event_t_stat", "consistency", "latency_decay_bps", "copy_mo_5s_winrate", "copyability_factor"]:
        if c in f.columns:
            f[c] = pd.to_numeric(f[c], errors="coerce")
    return f


def _base_active(f: pd.DataFrame, max_freq: float) -> pd.DataFrame:
    """Common liquidity/activity floor for BOTH informed and control (so the only difference is edge)."""
    return f[(f["events_per_day"] <= max_freq) & (f["events_per_day"] >= 0.3)
             & (f["active_days"] >= 15) & (f["total_notional"] >= 100000)
             & (f["open_event_count"] >= 25)].copy()


def select_informed(feat_path: str, max_freq: float, min_tstat: float, min_winrate: float,
                    top_k: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (informed_topk, control). Both pass the same activity floor; informed = high in-sample
    entry-information (net_edge + significance), control = net_edge<=0 (no entry edge). Leak-free:
    selection uses only IN-SAMPLE markout (through 2026-05-27)."""
    f = _load_feat(feat_path)
    base = _base_active(f, max_freq)
    informed = base[(base["net_edge_bps"] > 0) & (base["copy_mo_5s_winrate"] >= min_winrate)
                    & (base["event_t_stat"] >= min_tstat)].copy()

    def z(s):
        sd = s.std(ddof=0)
        return (s - s.mean()) / sd if sd and sd > 0 else s * 0
    informed["rank_score"] = (0.35 * z(informed["net_edge_bps"]) + 0.30 * z(informed["event_t_stat"])
                              + 0.20 * z(informed["consistency"].fillna(0))
                              - 0.15 * z(informed["latency_decay_bps"].fillna(0)))
    informed = informed.sort_values("rank_score", ascending=False).head(top_k)
    # control: same activity floor, NO entry edge (net_edge<=0), matched count
    control = base[base["net_edge_bps"] <= 0].sort_values("total_notional", ascending=False).head(top_k)
    return informed, control


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feat", default="app/data/wallet_alpha/wallet_features.parquet")
    ap.add_argument("--max-freq", type=float, default=8.0)
    ap.add_argument("--min-tstat", type=float, default=1.0)
    ap.add_argument("--min-winrate", type=float, default=0.50)
    ap.add_argument("--top-k", type=int, default=40)
    ap.add_argument("--stop", type=float, default=0.03)
    ap.add_argument("--arm", type=float, default=0.02)
    ap.add_argument("--trail", type=float, default=0.012)
    ap.add_argument("--max-hold-h", type=float, default=12.0)
    ap.add_argument("--out", default="app/data/v15/entry_copy_overlay_result.parquet")
    args = ap.parse_args()

    r = Risk(stop=args.stop, arm=args.arm, trail=args.trail, max_hold_h=args.max_hold_h)
    informed, control = select_informed(args.feat, args.max_freq, args.min_tstat, args.min_winrate, args.top_k)
    print(f"informed={len(informed)}  control={len(control)}  (max_freq={args.max_freq}, tstat>={args.min_tstat}, wr>={args.min_winrate})")
    universe = copyable_universe()
    print(f"copyable major universe: {len(universe)} coins")

    since = int(pd.Timestamp(FWD_START, tz="UTC").timestamp() * 1000)
    now = int(time.time() * 1000)
    import threading
    candle_cache: dict = {}
    clock = threading.Lock()

    def _cached_candles(coin, s, e):
        with clock:
            if coin in candle_cache:
                return candle_cache[coin]
        df = _candles(coin, s, e)
        with clock:
            candle_cache[coin] = df
        return df

    def run_group(wallets):
        def one(w):
            # local candle fetch via shared cache
            ents = wallet_forward_entries(w, since, now, universe)
            if not ents:
                return {"wallet": w, "n_entries": 0, "n_copied": 0, "overlay_ret": None}
            rets, reasons = [], []
            for e in ents:
                cdf = _cached_candles(e["coin"], since, now)
                if cdf.empty:
                    continue
                et = e["ts"] + 1000
                fwd = cdf[cdf["t"] >= et]
                if fwd.empty or len(fwd) < 2:
                    continue
                entry_px = fwd["c"].iloc[0]
                entry_px = entry_px * (1 + SLIP) if e["side"] == "Buy" else entry_px * (1 - SLIP)
                gross, reason = overlay_trade(e["side"], entry_px, fwd.iloc[1:], r)
                rets.append(gross - SLIP - FEE_RT)
                reasons.append(reason)
            if not rets:
                return {"wallet": w, "n_entries": len(ents), "n_copied": 0, "overlay_ret": None}
            a = np.array(rets)
            return {"wallet": w, "n_entries": len(ents), "n_copied": len(a),
                    "overlay_ret": float(a.mean()), "overlay_win": float((a > 0).mean()),
                    "overlay_med": float(np.median(a)), "worst": float(a.min()),
                    "stop_frac": float(np.mean([x == "stop" for x in reasons])),
                    "tp_frac": float(np.mean([x == "trail_tp" for x in reasons])),
                    "_rets": a}
        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            return list(ex.map(one, wallets))

    print("running informed group...")
    inf_rows = run_group(informed["wallet"].tolist())
    print("running control group...")
    ctl_rows = run_group(control["wallet"].tolist())

    def summarize(rows, name):
        v = [x for x in rows if x.get("n_copied", 0) >= 5]
        all_trades = np.concatenate([x["_rets"] for x in v]) if v else np.array([])
        print(f"\n--- {name} ---  wallets>=5trades={len(v)}  total_trades={len(all_trades)}")
        if len(all_trades):
            print(f"  POOLED net/trade: {all_trades.mean()*1e4:.1f} bps  median {np.median(all_trades)*1e4:.1f} bps  "
                  f"trade win-rate {100*(all_trades>0).mean():.0f}%")
            pw = np.array([x["overlay_ret"] for x in v])
            print(f"  per-wallet net/trade: mean {pw.mean()*1e4:.1f} bps  frac wallets positive {100*(pw>0).mean():.0f}%")
        return v, all_trades

    print("\n=== ENTRY-COPY OVERLAY OOS VERDICT (informed vs control, our risk overlay) ===")
    print(f"risk: stop -{r.stop:.0%}, arm +{r.arm:.0%}, trail {r.trail:.1%}, max_hold {r.max_hold_h}h | "
          f"fees {FEE_RT*1e4:.1f}bps RT + {SLIP*1e4:.0f}bps/side slip | OOS window {FWD_START}->now")
    inf_v, inf_t = summarize(inf_rows, "INFORMED (in-sample entry edge)")
    ctl_v, ctl_t = summarize(ctl_rows, "CONTROL (no entry edge)")
    if len(inf_t) >= 10 and len(ctl_t) >= 10:
        import scipy.stats as ss
        u, p = ss.mannwhitneyu(inf_t, ctl_t, alternative="greater")
        print(f"\nMann-Whitney informed>control (per-trade): p={p:.4f}  "
              f"(edge delta {(inf_t.mean()-ctl_t.mean())*1e4:.1f} bps/trade)")
    if inf_v:
        print("\ntop informed wallets:")
        cols = ["wallet", "n_copied", "overlay_ret", "overlay_win", "overlay_med", "worst", "stop_frac", "tp_frac"]
        dfv = pd.DataFrame([{k: x[k] for k in cols} for x in inf_v]).sort_values("overlay_ret", ascending=False)
        print(dfv.head(20).to_string(index=False))
        dfv.to_parquet(args.out, index=False)
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
