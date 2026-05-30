#!/usr/bin/env python3
"""
Wallet Scanner V2: Copy Trading Target Selection (streaming version)

Processes 150M+ fills in ~20min using streaming episode reconstruction.
Never holds raw fills in memory -- only episode state + completed metrics.

Usage:
    python scripts/wallet_scanner_v2.py --phase train
    python scripts/wallet_scanner_v2.py --phase test
"""
import argparse
import json
import logging
import time as time_mod
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import lz4.frame
import numpy as np
from scipy import stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s [scanner] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

S3_RAW_DIR = Path("app/data/hl_s3_raw")

TRAIN_START = datetime(2026, 4, 9, tzinfo=timezone.utc)
TRAIN_END   = datetime(2026, 4, 29, tzinfo=timezone.utc)
TEST_START  = datetime(2026, 4, 29, tzinfo=timezone.utc)
TEST_END    = datetime(2026, 5, 12, tzinfo=timezone.utc)

# Simulated copy costs (FROZEN)
ENTRY_LAG_BPS = 5.0
EXIT_LAG_BPS = 5.0
SPREAD_BPS = {"BTC": 3.0, "ETH": 3.0}  # default 8 for alts
DEFAULT_SPREAD_BPS = 8.0
FEE_RT_BPS = 8.64

# Checklist thresholds (FROZEN)
M1_SIM_PNL_MIN = 0
M2_SIM_WR_MIN = 0.50
M3_ENTRIES_MEDIAN_MAX = 2
M4_EPISODES_MIN = 30
M6_HOLD_MIN_S = 300
M6_HOLD_MAX_S = 172800
M7_EXPECTANCY_MIN = 5
M8_DCA_LIFT_MAX = 0
P1_PF_MIN = 1.2
P2_POS_DAYS_MIN = 0.50
P3_TOP3_CONC_MAX = 0.50
P7_SHARPE_MIN = 0.5
R1_MAE95_MAX = 500
R2_CONSEC_MAX = 6
R3_WORST_MAX = -800
A1_FLIP_MAX = 0.30
FDR_Q = 0.10


def spread_bps(coin):
    return SPREAD_BPS.get(coin, DEFAULT_SPREAD_BPS)


def iter_files(start, end):
    current = start
    while current < end:
        day_dir = S3_RAW_DIR / current.strftime("%Y%m%d")
        if day_dir.exists():
            for h in range(24):
                f = day_dir / f"{h}.lz4"
                if f.exists():
                    yield f
        current += timedelta(days=1)


def iter_fills(fpath):
    with open(fpath, "rb") as f:
        raw = lz4.frame.decompress(f.read())
    for line in raw.decode("utf-8").strip().split("\n"):
        if not line:
            continue
        try:
            rec = json.loads(line)
            for ev in rec.get("events", []):
                if len(ev) >= 2:
                    yield ev[0].lower(), ev[1]
        except (json.JSONDecodeError, ValueError):
            continue


class WalletEpisodeTracker:
    """Streaming episode reconstruction. Maintains state per (wallet, coin)."""

    def __init__(self):
        # Per (wallet, coin) -> position state
        self.state = {}  # (wallet, coin) -> {pos, entries, first_px, first_time, dir, min_px, max_px, closed_pnl, fees}
        # Completed episodes per wallet
        self.episodes = defaultdict(list)  # wallet -> [episode_metrics]

    def process_fill(self, wallet, fill):
        coin = fill.get("coin", "")
        d = fill.get("dir", "")
        if not d or not coin:
            return

        px = float(fill.get("px", 0))
        sz = float(fill.get("sz", 0))
        ts = int(fill.get("time", 0))
        fee = float(fill.get("fee", 0))
        cpnl = float(fill.get("closedPnl", 0))

        key = (wallet, coin)
        st = self.state.get(key)

        if "Open" in d:
            if st is None or st["pos"] < 0.0001:
                # New episode
                direction = "LONG" if "Long" in d else "SHORT"
                self.state[key] = {
                    "pos": sz, "entries": [(px, sz)], "first_px": px,
                    "first_time": ts, "dir": direction,
                    "min_px": px, "max_px": px, "closed_pnl": 0, "fees": abs(fee),
                }
            else:
                st["entries"].append((px, sz))
                st["pos"] += sz
                st["min_px"] = min(st["min_px"], px)
                st["max_px"] = max(st["max_px"], px)
                st["fees"] += abs(fee)

        elif "Close" in d:
            if st is None or st["pos"] < 0.0001:
                return
            st["closed_pnl"] += cpnl
            st["fees"] += abs(fee)
            st["min_px"] = min(st["min_px"], px)
            st["max_px"] = max(st["max_px"], px)
            st["pos"] = max(0, st["pos"] - sz)

            if st["pos"] < 0.0001:
                # Episode complete -- compute metrics and discard state
                self._close_episode(wallet, coin, px, ts, st)
                del self.state[key]

    def _close_episode(self, wallet, coin, exit_px, exit_time, st):
        entries = st["entries"]
        if not entries or st["first_px"] <= 0:
            return

        first_px = st["first_px"]
        total_cost = sum(p * s for p, s in entries)
        total_sz = sum(s for _, s in entries)
        avg_entry = total_cost / total_sz if total_sz > 0 else first_px
        hold_s = (exit_time - st["first_time"]) / 1000 if st["first_time"] > 1e12 else exit_time - st["first_time"]

        if st["dir"] == "LONG":
            first_bps = (exit_px - first_px) / first_px * 10000
            avg_bps = (exit_px - avg_entry) / avg_entry * 10000
            mae = (first_px - st["min_px"]) / first_px * 10000
        else:
            first_bps = (first_px - exit_px) / first_px * 10000
            avg_bps = (avg_entry - exit_px) / avg_entry * 10000
            mae = (st["max_px"] - first_px) / first_px * 10000

        # Simulated copy PnL
        sp = spread_bps(coin)
        lag_entry = ENTRY_LAG_BPS + sp / 2
        lag_exit = EXIT_LAG_BPS + sp / 2
        if st["dir"] == "LONG":
            our_entry = first_px * (1 + lag_entry / 10000)
            our_exit = exit_px * (1 - lag_exit / 10000)
            sim_gross = (our_exit - our_entry) / our_entry * 10000
        else:
            our_entry = first_px * (1 - lag_entry / 10000)
            our_exit = exit_px * (1 + lag_exit / 10000)
            sim_gross = (our_entry - our_exit) / our_entry * 10000
        sim_pnl = sim_gross - FEE_RT_BPS

        self.episodes[wallet].append({
            "coin": coin, "dir": st["dir"], "n_entries": len(entries),
            "hold_s": hold_s, "first_bps": first_bps, "avg_bps": avg_bps,
            "sim_pnl": sim_pnl, "dca_lift": avg_bps - first_bps,
            "mae": mae, "closed_pnl": st["closed_pnl"], "notional": total_cost,
            "entry_time": st["first_time"], "exit_time": exit_time,
        })


def profile(episodes):
    if len(episodes) < 5:
        return None

    sim = [e["sim_pnl"] for e in episodes]
    n_ent = [e["n_entries"] for e in episodes]
    holds = [e["hold_s"] for e in episodes]
    maes = [e["mae"] for e in episodes]
    sa = np.array(sim)

    sim_mean = np.mean(sa)
    sim_wr = np.sum(sa > 0) / len(sa)
    first_mean = np.mean([e["first_bps"] for e in episodes])
    first_wr = sum(1 for e in episodes if e["first_bps"] > 0) / len(episodes)

    std = np.std(sa)
    sharpe = sim_mean / std * np.sqrt(252) if std > 0 else 0
    wins = sa[sa > 0]
    losses = sa[sa <= 0]
    gw = np.sum(wins) if len(wins) else 0
    gl = abs(np.sum(losses)) if len(losses) else 1
    pf = gw / gl if gl > 0 else 999
    aw = np.mean(wins) if len(wins) else 0
    al = abs(np.mean(losses)) if len(losses) else 0
    exp = aw * sim_wr - al * (1 - sim_wr)

    # Consistency
    day_pnl = defaultdict(float)
    for e in episodes:
        t = e["entry_time"]
        if t > 1e12: t /= 1000
        d = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
        day_pnl[d] += e["sim_pnl"]
    pos_days = sum(1 for v in day_pnl.values() if v > 0) / len(day_pnl) if day_pnl else 0

    cpnls = sorted([e["closed_pnl"] for e in episodes], reverse=True)
    top3 = sum(cpnls[:3])
    total_cpnl = sum(cpnls)
    top3_conc = top3 / total_cpnl if total_cpnl > 0 else 1

    # Remove best coin
    coin_pnl = defaultdict(float)
    for e in episodes:
        coin_pnl[e["coin"]] += e["sim_pnl"]
    best_coin = max(coin_pnl, key=coin_pnl.get)
    no_best_coin = sum(v for k, v in coin_pnl.items() if k != best_coin) > 0
    no_best_day = sum(v for k, v in day_pnl.items() if k != max(day_pnl, key=day_pnl.get)) > 0 if day_pnl else False
    no_best_trade = (np.sum(sa) - np.max(sa)) > 0

    # Max consec losses
    mc = 0; cur = 0
    for s in sim:
        if s <= 0: cur += 1; mc = max(mc, cur)
        else: cur = 0

    # Long/short
    leps = [e for e in episodes if e["dir"] == "LONG"]
    seps = [e for e in episodes if e["dir"] == "SHORT"]
    lwr = sum(1 for e in leps if e["sim_pnl"] > 0) / len(leps) if leps else 0
    swr = sum(1 for e in seps if e["sim_pnl"] > 0) / len(seps) if seps else 0

    # Flip rate (MM detection)
    se = sorted(episodes, key=lambda e: e["entry_time"])
    flips = 0
    for i in range(1, len(se)):
        gap = se[i]["entry_time"] - se[i-1]["exit_time"]
        if se[i-1]["exit_time"] > 1e12: gap_s = gap / 1000
        else: gap_s = gap
        if gap_s < 300 and se[i]["dir"] != se[i-1]["dir"]:
            flips += 1
    flip_rate = flips / len(se) if se else 0

    # t-test
    if len(sim) >= 10:
        ts, pv = stats.ttest_1samp(sim, 0)
        pv1 = pv / 2 if ts > 0 else 1.0
    else:
        pv1 = 1.0

    total_not = sum(e["notional"] for e in episodes)
    t_range = (max(e["entry_time"] for e in episodes) - min(e["entry_time"] for e in episodes))
    if episodes[0]["entry_time"] > 1e12: t_range /= 1000
    epd = len(episodes) / max(t_range / 86400, 1)

    return {
        "sim_pnl": sim_mean, "sim_wr": sim_wr,
        "first_pnl": first_mean, "first_wr": first_wr,
        "closed_pnl": total_cpnl, "pnl_per_dollar": total_cpnl / total_not * 10000 if total_not > 0 else 0,
        "sharpe": sharpe, "pf": pf, "expectancy": exp,
        "episodes": len(episodes), "pos_days": pos_days,
        "top3_conc": top3_conc, "max_consec_losses": mc,
        "no_best_trade": no_best_trade, "no_best_coin": no_best_coin, "no_best_day": no_best_day,
        "entries_median": np.median(n_ent), "entries_mean": np.mean(n_ent),
        "dca_lift": np.mean([e["dca_lift"] for e in episodes]),
        "hold_median": np.median(holds), "hold_mean": np.mean(holds),
        "long_wr": lwr, "short_wr": swr, "long_n": len(leps), "short_n": len(seps),
        "coins": len(set(e["coin"] for e in episodes)), "best_coin": best_coin,
        "coin_conc": coin_pnl.get(best_coin, 0) / sum(coin_pnl.values()) if sum(coin_pnl.values()) != 0 else 1,
        "mae_95": np.percentile(maes, 95) if maes else 0,
        "worst_trade": min(sim), "flip_rate": flip_rate,
        "p_value": pv1, "total_notional": total_not, "epd": epd,
    }


def check_mandatory(p):
    fails = []
    if p["sim_pnl"] <= M1_SIM_PNL_MIN: fails.append(f"M1:sim_pnl={p['sim_pnl']:.1f}")
    if p["sim_wr"] <= M2_SIM_WR_MIN: fails.append(f"M2:wr={p['sim_wr']:.0%}")
    if p["entries_median"] > M3_ENTRIES_MEDIAN_MAX: fails.append(f"M3:entries={p['entries_median']:.0f}")
    if p["episodes"] < M4_EPISODES_MIN: fails.append(f"M4:eps={p['episodes']}")
    if not (M6_HOLD_MIN_S < p["hold_median"] < M6_HOLD_MAX_S): fails.append(f"M6:hold={p['hold_median']:.0f}s")
    if p["expectancy"] <= M7_EXPECTANCY_MIN: fails.append(f"M7:exp={p['expectancy']:.1f}")
    if p["dca_lift"] > M8_DCA_LIFT_MAX: fails.append(f"M8:dca={p['dca_lift']:.1f}")
    return len(fails) == 0, fails


def check_robust(p):
    fails = []
    if p["pf"] < P1_PF_MIN: fails.append(f"P1:pf={p['pf']:.2f}")
    if p["pos_days"] < P2_POS_DAYS_MIN: fails.append(f"P2:days={p['pos_days']:.0%}")
    if p["top3_conc"] > P3_TOP3_CONC_MAX: fails.append(f"P3:conc={p['top3_conc']:.0%}")
    if not p["no_best_coin"]: fails.append("P4:best_coin")
    if not p["no_best_day"]: fails.append("P5:best_day")
    both = p["long_n"] > 5 and p["short_n"] > 5
    if both and (p["long_wr"] < 0.35 or p["short_wr"] < 0.35): fails.append(f"P6:sides")
    if p["sharpe"] < P7_SHARPE_MIN: fails.append(f"P7:sharpe={p['sharpe']:.2f}")
    return len(fails) == 0, fails


def check_risk(p):
    fails = []
    if p["mae_95"] > R1_MAE95_MAX: fails.append(f"R1:mae={p['mae_95']:.0f}")
    if p["max_consec_losses"] > R2_CONSEC_MAX: fails.append(f"R2:consec={p['max_consec_losses']}")
    if p["worst_trade"] < R3_WORST_MAX: fails.append(f"R3:worst={p['worst_trade']:.0f}")
    return len(fails) == 0, fails


def check_antigaming(p):
    fails = []
    if p["flip_rate"] > A1_FLIP_MAX: fails.append(f"A1:flip={p['flip_rate']:.0%}")
    if p["closed_pnl"] <= 0: fails.append(f"A2:pnl=${p['closed_pnl']:.0f}")
    return len(fails) == 0, fails


def apply_fdr(profiles, q=FDR_Q):
    items = sorted(profiles.items(), key=lambda x: x[1]["p_value"])
    n = len(items)
    passing = {}
    for rank, (w, p) in enumerate(items, 1):
        if p["p_value"] <= q * rank / n:
            p["fdr_q"] = p["p_value"] * n / rank
            passing[w] = p
        else:
            break
    return passing


def print_wallet(w, p, rank=0):
    print(f"\n{'='*80}")
    print(f"{'#'+str(rank)+' ' if rank else ''}{w}")
    print(f"{'='*80}")
    print(f"  Eps: {p['episodes']} | Coins: {p['coins']} | Best: {p['best_coin']} | {p['epd']:.1f}/day")
    print(f"  SimCopy: {p['sim_pnl']:+.1f}bps WR:{p['sim_wr']:.0%} | 1stEntry: {p['first_pnl']:+.1f}bps WR:{p['first_wr']:.0%}")
    print(f"  DCA Lift: {p['dca_lift']:+.1f}bps | Expectancy: {p['expectancy']:+.1f}bps")
    print(f"  Sharpe: {p['sharpe']:.2f} | PF: {p['pf']:.2f} | bps/$: {p['pnl_per_dollar']:.2f}")
    print(f"  Closed: ${p['closed_pnl']:+,.0f} | Notional: ${p['total_notional']:,.0f}")
    print(f"  Hold: {p['hold_median']/3600:.1f}h med | Entries/ep: {p['entries_median']:.0f} med ({p['entries_mean']:.1f} mean)")
    print(f"  L:{p['long_n']}eps {p['long_wr']:.0%}WR | S:{p['short_n']}eps {p['short_wr']:.0%}WR")
    print(f"  PosDays: {p['pos_days']:.0%} | Top3: {p['top3_conc']:.0%} | CoinConc: {p['coin_conc']:.0%}")
    print(f"  MAE95: {p['mae_95']:.0f}bps | Worst: {p['worst_trade']:.0f}bps | MaxConsecL: {p['max_consec_losses']}")
    print(f"  FlipRate: {p['flip_rate']:.0%} | p={p['p_value']:.4f}" + (f" | FDR q={p.get('fdr_q', 'N/A')}" if 'fdr_q' in p else ""))


def run_phase(start, end, label, target_wallets=None):
    files = list(iter_files(start, end))
    logger.info(f"=== {label}: {start.date()} to {end.date()} ({len(files)} files) ===")

    # Streaming parse + episode reconstruction
    tracker = WalletEpisodeTracker()
    t0 = time_mod.time()
    total = 0

    for i, fpath in enumerate(files):
        for addr, fill in iter_fills(fpath):
            if target_wallets and addr not in target_wallets:
                continue
            tracker.process_fill(addr, fill)
            total += 1
        if (i + 1) % 48 == 0:
            n_wallets = len(tracker.episodes)
            n_eps = sum(len(v) for v in tracker.episodes.values())
            logger.info(f"  {i+1}/{len(files)} files | {total:,} fills | {n_wallets:,} wallets | {n_eps:,} episodes")

    elapsed = time_mod.time() - t0
    logger.info(f"Parsed in {elapsed:.0f}s: {total:,} fills, {len(tracker.episodes):,} wallets with episodes")

    # Profile
    logger.info("Profiling wallets...")
    profiles = {}
    for w, eps in tracker.episodes.items():
        p = profile(eps)
        if p:
            profiles[w] = p
    logger.info(f"Profiled: {len(profiles)}")

    return profiles, tracker.episodes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["train", "test", "full"], default="train")
    args = parser.parse_args()

    if args.phase == "train":
        profiles, _ = run_phase(TRAIN_START, TRAIN_END, "TRAIN")

        # Filter funnel
        m_pass = {w: p for w, p in profiles.items() if check_mandatory(p)[0]}
        logger.info(f"Mandatory: {len(m_pass)} (from {len(profiles)})")

        fdr_pass = apply_fdr(m_pass)
        logger.info(f"FDR q<{FDR_Q}: {len(fdr_pass)} (from {len(m_pass)})")

        r_pass = {w: p for w, p in fdr_pass.items() if check_robust(p)[0]}
        logger.info(f"Robustness: {len(r_pass)} (from {len(fdr_pass)})")

        rk_pass = {w: p for w, p in r_pass.items() if check_risk(p)[0]}
        logger.info(f"Risk: {len(rk_pass)} (from {len(r_pass)})")

        ag_pass = {w: p for w, p in rk_pass.items() if check_antigaming(p)[0]}
        logger.info(f"Anti-gaming: {len(ag_pass)} (from {len(rk_pass)})")

        ranked = sorted(ag_pass.items(), key=lambda x: -x[1]["sharpe"])

        print(f"\n{'='*80}")
        print("SELECTION FUNNEL (TRAIN)")
        print(f"{'='*80}")
        print(f"  Total wallets with episodes: {len(profiles):>8,}")
        print(f"  Pass mandatory (M1-M8):      {len(m_pass):>8,}")
        print(f"  Pass FDR (q<{FDR_Q}):           {len(fdr_pass):>8,}")
        print(f"  Pass robustness (P1-P7):     {len(r_pass):>8,}")
        print(f"  Pass risk (R1-R3):           {len(rk_pass):>8,}")
        print(f"  Pass anti-gaming (A1-A2):    {len(ag_pass):>8,}")

        print(f"\nTOP WALLETS ({len(ranked)} candidates)")
        for i, (w, p) in enumerate(ranked[:30], 1):
            print_wallet(w, p, rank=i)

        # Save for test phase
        out = {"wallets": {w: p for w, p in ranked}}
        with open("/tmp/scanner_train.json", "w") as f:
            json.dump(out, f, indent=2, default=str)
        logger.info("Saved to /tmp/scanner_train.json")

        # Also print full addresses for easy copy
        if ranked:
            print(f"\n{'='*80}")
            print("FULL ADDRESSES")
            print(f"{'='*80}")
            for i, (w, p) in enumerate(ranked[:30], 1):
                print(f"  {i:2d}. {w}")

    elif args.phase == "test":
        with open("/tmp/scanner_train.json") as f:
            train = json.load(f)
        target = set(train["wallets"].keys())
        logger.info(f"Validating {len(target)} train wallets on test period")

        profiles, _ = run_phase(TEST_START, TEST_END, "TEST", target_wallets=target)

        validated = {}
        failed = {}
        for w in target:
            if w not in profiles:
                failed[w] = "no episodes in test"
                continue
            p = profiles[w]
            ok, reasons = check_mandatory(p)
            if ok:
                validated[w] = p
            else:
                failed[w] = reasons[0]

        print(f"\n{'='*80}")
        print("TEST VALIDATION")
        print(f"{'='*80}")
        print(f"  Train candidates: {len(target)}")
        print(f"  Pass test:        {len(validated)}")
        print(f"  Fail test:        {len(failed)}")

        for w, r in sorted(failed.items()):
            print(f"  FAIL {w[:16]}... {r}")

        ranked = sorted(validated.items(), key=lambda x: -x[1]["sharpe"])
        for i, (w, p) in enumerate(ranked[:20], 1):
            print_wallet(w, p, rank=i)

        if ranked:
            print(f"\n{'='*80}")
            print("FINAL VALIDATED WALLETS")
            print(f"{'='*80}")
            for i, (w, p) in enumerate(ranked, 1):
                print(f"  {i:2d}. {w}  Sharpe={p['sharpe']:.2f} SimPnL={p['sim_pnl']:+.1f}bps WR={p['sim_wr']:.0%} Eps={p['episodes']}")


if __name__ == "__main__":
    main()
