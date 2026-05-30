#!/usr/bin/env python3
"""
Deep wallet analysis from raw S3 LZ4 fills.
Processes 19 GB of HL exchange data: every wallet, every trade.

Output: /tmp/deep_wallet_analysis.json
- Per-wallet: style, raw sequences, parent/agent relationships, PnL
- Cross-wallet correlations
- Strategy reverse engineering
- Copy recommendations

No pre-selected wallets. No aggregates first. Raw sequences drive everything.
"""
import json
import lz4.frame
import os
import time
from collections import defaultdict, Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

DATA_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_raw")
OUTPUT = "/tmp/deep_wallet_analysis.json"

def process_file(filepath: str) -> dict:
    """Decompress one LZ4 file and extract per-wallet fills."""
    with open(filepath, "rb") as f:
        raw = lz4.frame.decompress(f.read())

    wallet_fills = defaultdict(list)
    for line in raw.decode("utf-8").strip().split("\n"):
        if not line:
            continue
        record = json.loads(line)
        for event in record.get("events", []):
            if len(event) >= 2:
                addr = event[0].lower()
                fill = event[1]
                wallet_fills[addr].append({
                    "coin": fill.get("coin", ""),
                    "side": fill.get("side", ""),
                    "sz": float(fill.get("sz", 0)),
                    "px": float(fill.get("px", 0)),
                    "time": int(fill.get("time", 0)),
                    "dir": fill.get("dir", ""),
                    "pnl": float(fill.get("closedPnl", 0)),
                })
    return wallet_fills


def classify_wallet(fills: list) -> dict:
    """Classify a wallet from its raw fill sequence."""
    if len(fills) < 20:
        return {"style": "inactive", "copyable": False}

    # Group by coin
    by_coin = defaultdict(list)
    for f in fills:
        by_coin[f["coin"]].append(f)

    # Position lifecycles: track when wallet goes flat
    position_durations = []
    flat_count = 0
    total_pnl = sum(f["pnl"] for f in fills)
    total_volume = sum(f["sz"] * f["px"] for f in fills)

    for coin, coin_fills in by_coin.items():
        if len(coin_fills) < 3:
            continue
        pos = 0
        pos_open_time = None
        for f in coin_fills:
            d = f.get("dir", "")
            if "Open Long" in d or "Short > Long" in d:
                if abs(pos) < 0.001:
                    pos_open_time = f["time"]
                pos += f["sz"]
            elif "Open Short" in d or "Long > Short" in d:
                if abs(pos) < 0.001:
                    pos_open_time = f["time"]
                pos -= f["sz"]
            elif "Close Long" in d:
                pos -= f["sz"]
            elif "Close Short" in d:
                pos += f["sz"]

            if abs(pos) < 0.001 and pos_open_time is not None:
                dur_s = (f["time"] - pos_open_time) / 1000
                if 1 < dur_s < 86400:
                    position_durations.append(dur_s)
                flat_count += 1
                pos_open_time = None

    if not position_durations:
        return {"style": "no_round_trips", "copyable": False, "fills": len(fills)}

    dur = np.array(position_durations)
    median_dur = float(np.median(dur))
    p90_dur = float(np.percentile(dur, 90))

    # Classify style
    if median_dur < 30:
        style = "HFT"
    elif median_dur < 120:
        style = "SCALPER"
    elif median_dur < 600:
        style = "SHORT_SWING"
    elif median_dur < 3600:
        style = "SWING"
    elif median_dur < 14400:
        style = "POSITION"
    else:
        style = "HOLDER"

    # Directionality per episode
    episode_dirs = []
    for coin, coin_fills in by_coin.items():
        if len(coin_fills) < 3:
            continue
        episodes = []
        current = [coin_fills[0]]
        for f in coin_fills[1:]:
            if f["time"] - current[-1]["time"] > 300000:  # 5 min in ms
                episodes.append(current)
                current = [f]
            else:
                current.append(f)
        episodes.append(current)

        for ep in episodes:
            if len(ep) < 2:
                continue
            buy_vol = sum(f["sz"] * f["px"] for f in ep if f["side"] == "B")
            sell_vol = sum(f["sz"] * f["px"] for f in ep if f["side"] == "A")
            total = buy_vol + sell_vol
            if total > 0:
                episode_dirs.append(abs(buy_vol - sell_vol) / total)

    avg_dir = float(np.mean(episode_dirs)) if episode_dirs else 0

    # Copyable?
    copyable = style in ("SWING", "POSITION", "HOLDER") and avg_dir > 0.6

    return {
        "style": style,
        "copyable": copyable,
        "fills": len(fills),
        "coins": len(by_coin),
        "round_trips": flat_count,
        "median_dur_s": median_dur,
        "p90_dur_s": p90_dur,
        "avg_directionality": avg_dir,
        "total_pnl": total_pnl,
        "total_volume": total_volume,
        "pnl_per_volume_bps": total_pnl / total_volume * 10000 if total_volume > 0 else 0,
    }


def main():
    start = time.time()

    # Pass 1: count per-wallet activity across all files
    print("PASS 1: Counting per-wallet activity across 30 days...")
    wallet_fill_counts = Counter()
    wallet_volume = defaultdict(float)
    wallet_pnl = defaultdict(float)
    total_files = 0

    for day_dir in sorted(DATA_DIR.iterdir()):
        if not day_dir.is_dir():
            continue
        for lz4_file in sorted(day_dir.glob("*.lz4")):
            wallet_fills = process_file(str(lz4_file))
            for addr, fills in wallet_fills.items():
                wallet_fill_counts[addr] += len(fills)
                wallet_volume[addr] += sum(f["sz"] * f["px"] for f in fills)
                wallet_pnl[addr] += sum(f["pnl"] for f in fills)
            total_files += 1
            if total_files % 48 == 0:
                days = total_files / 24
                print(f"  {total_files} files ({days:.0f} days), {len(wallet_fill_counts):,} unique wallets ({time.time()-start:.0f}s)")

    print(f"\nPass 1 complete: {total_files} files, {len(wallet_fill_counts):,} unique wallets")

    # Filter: wallets with significant activity
    significant = {addr for addr, count in wallet_fill_counts.items() if count >= 500 and wallet_volume[addr] >= 500000}
    print(f"Wallets with 500+ fills and $500K+ volume: {len(significant)}")

    # Pass 2: for significant wallets, collect full fill sequences and classify
    print(f"\nPASS 2: Classifying {len(significant)} wallets...")
    wallet_all_fills = defaultdict(list)

    for day_dir in sorted(DATA_DIR.iterdir()):
        if not day_dir.is_dir():
            continue
        for lz4_file in sorted(day_dir.glob("*.lz4")):
            wallet_fills = process_file(str(lz4_file))
            for addr, fills in wallet_fills.items():
                if addr in significant:
                    wallet_all_fills[addr].extend(fills)

    print(f"Collected fills for {len(wallet_all_fills)} wallets")

    # Classify each wallet
    results = {}
    for addr, fills in wallet_all_fills.items():
        fills.sort(key=lambda x: x["time"])
        profile = classify_wallet(fills)
        profile["address"] = addr
        profile["total_volume_M"] = wallet_volume[addr] / 1e6
        results[addr] = profile

    # Style distribution
    print(f"\nSTYLE DISTRIBUTION:")
    styles = Counter(r["style"] for r in results.values())
    for style, count in styles.most_common():
        copyable = sum(1 for r in results.values() if r["style"] == style and r.get("copyable"))
        print(f"  {style:>15}: {count:>4} wallets, {copyable} copyable")

    # Top copyable wallets
    copyable_wallets = {addr: r for addr, r in results.items() if r.get("copyable")}
    print(f"\nCOPYABLE WALLETS: {len(copyable_wallets)}")

    sorted_copyable = sorted(copyable_wallets.items(), key=lambda x: x[1]["total_pnl"], reverse=True)

    print(f"\n{'Addr':>18} {'Style':>10} {'Fills':>8} {'RTs':>6} {'MedDur':>8} {'Dir%':>5} {'Coins':>5} {'PnL':>12} {'Vol$M':>7}")
    for addr, r in sorted_copyable[:30]:
        dur_str = f"{r['median_dur_s']/60:.0f}min" if r['median_dur_s'] < 3600 else f"{r['median_dur_s']/3600:.1f}h"
        print(f"{addr[:16]}.. {r['style']:>10} {r['fills']:>8,} {r['round_trips']:>6,} {dur_str:>8} {r['avg_directionality']*100:>4.0f}% {r['coins']:>5} ${r['total_pnl']:>+10,.0f} {r['total_volume_M']:>6.1f}")

    # For top 10 copyable: extract raw trade sequences (first 50 fills per coin)
    print(f"\n{'='*80}")
    print(f"RAW SEQUENCES FOR TOP 10 COPYABLE WALLETS")
    print(f"{'='*80}")

    for addr, r in sorted_copyable[:10]:
        fills = wallet_all_fills[addr]
        by_coin = defaultdict(list)
        for f in fills:
            by_coin[f["coin"]].append(f)

        top_coin = max(by_coin.items(), key=lambda x: len(x[1]))

        print(f"\n--- {addr[:20]}... ({r['style']}, ${r['total_pnl']:+,.0f}, {r['coins']} coins) ---")
        print(f"Top coin: {top_coin[0]} ({len(top_coin[1])} fills)")
        print(f"First 30 fills:")
        for f in top_coin[1][:30]:
            ts = datetime.fromtimestamp(f["time"]/1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
            notional = f["sz"] * f["px"]
            print(f"  {ts} {f['dir']:>14} {f['side']} ${notional:>8,.0f} @ {f['px']:.4f}")

    # Save full results
    output = {
        "metadata": {
            "total_files": total_files,
            "total_wallets": len(wallet_fill_counts),
            "significant_wallets": len(significant),
            "analysis_time_s": time.time() - start,
        },
        "style_distribution": dict(styles),
        "copyable_wallets": [
            {**r, "address": addr}
            for addr, r in sorted_copyable
        ],
        "all_wallet_summary": [
            {"address": addr, "fills": wallet_fill_counts[addr], "volume": wallet_volume[addr], "pnl": wallet_pnl[addr]}
            for addr in sorted(wallet_fill_counts, key=lambda x: wallet_fill_counts[x], reverse=True)[:200]
        ],
    }

    with open(OUTPUT, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nResults saved to {OUTPUT}")
    print(f"Total time: {time.time()-start:.0f}s")


if __name__ == "__main__":
    main()
