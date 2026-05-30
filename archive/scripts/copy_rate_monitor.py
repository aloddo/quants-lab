#!/usr/bin/env /Users/hermes/miniforge3/envs/quants-lab/bin/python
"""Daily copy rate monitor for HL copy trading bots (V9 + V10).

Parses tmux scrollback from both sessions, counts signals vs entries,
breaks down skip reasons, and computes actionable copy rate.

Usage:
    python scripts/copy_rate_monitor.py              # print table
    python scripts/copy_rate_monitor.py --telegram    # print + send to TG
"""
import argparse
import os
import re
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta

TG_CHAT_ID = "-1003576397888"
SESSIONS = {"V9": "hl-copy", "V10": "v10-shadow"}
CUTOFF_HOURS = 24

# Log line timestamp pattern: 2026-05-13 01:45:32,057
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def capture_tmux(session: str, lines: int = 30000) -> list[str]:
    """Capture scrollback from a tmux pane."""
    try:
        result = subprocess.run(
            ["tmux", "capture-pane", "-t", session, "-p", "-S", f"-{lines}"],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout.splitlines() if result.returncode == 0 else []
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


def parse_ts(line: str) -> datetime | None:
    m = TS_RE.match(line)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return None


def analyze_logs(lines: list[str], cutoff: datetime) -> dict:
    counts = {
        "target_fills": 0,
        "entries_v9": 0,       # ENTRY FILLED (IOC)
        "entries_v10": 0,      # V10 ENTRY:
        "exits": 0,
        "skip_too_small": 0,
        "skip_ambiguous": 0,
        "skip_closing": 0,
        "skip_margin": 0,
        "skip_margin_util": 0,
        "skip_margin_coin": 0,
        "skip_margin_addon": 0,
        "skip_entry": 0,       # Entry skipped (no book data etc)
    }
    coins_entered = defaultdict(int)
    coins_blocked = defaultdict(int)

    for line in lines:
        ts = parse_ts(line)
        if ts and ts < cutoff:
            continue

        if ("TWAP FILL" in line or "FILL:" in line) and "ENTRY FILLED" not in line and "EXIT FILLED" not in line:
            counts["target_fills"] += 1
        elif "ENTRY FILLED" in line:
            counts["entries_v9"] += 1
            # Format: ENTRY FILLED (IOC): TAO BUY ...
            coin_m = re.search(r"ENTRY FILLED\s*\(\w+\):\s*(\w+)", line)
            if coin_m:
                coins_entered[coin_m.group(1)] += 1
        elif "V10 ENTRY:" in line:
            counts["entries_v10"] += 1
            # Format: V10 ENTRY: 0xe65b9c86 INJ BUY $11
            coin_m = re.search(r"V10 ENTRY:\s*0x\w+\s+(\w+)", line)
            if coin_m:
                coins_entered[coin_m.group(1)] += 1
        elif "EXIT FILLED" in line:
            counts["exits"] += 1
        elif "TWAP SKIP" in line:
            if "CLOSING" in line:
                counts["skip_closing"] += 1
            elif "ambiguous" in line:
                counts["skip_ambiguous"] += 1
            else:
                counts["skip_too_small"] += 1
        elif "Margin BLOCKED" in line:
            counts["skip_margin"] += 1
            if "total util" in line:
                counts["skip_margin_util"] += 1
            elif "coin concentration" in line:
                counts["skip_margin_coin"] += 1
            elif "notional" in line:
                counts["skip_margin_addon"] += 1
            # Extract blocked coin
            coin_m = re.search(r"Margin BLOCKED (\w+):", line)
            if coin_m:
                coins_blocked[coin_m.group(1)] += 1
        elif "Entry skipped" in line:
            counts["skip_entry"] += 1

    return counts, coins_entered, coins_blocked


def format_report(all_counts: dict, all_coins: dict, all_blocked: dict, lookback_hours: int = 24) -> str:
    # Aggregate across sessions
    c = defaultdict(int)
    coins = defaultdict(int)
    blocked = defaultdict(int)
    for session_counts in all_counts.values():
        for k, v in session_counts.items():
            c[k] += v
    for session_coins in all_coins.values():
        for k, v in session_coins.items():
            coins[k] += v
    for session_blocked in all_blocked.values():
        for k, v in session_blocked.items():
            blocked[k] += v

    total_entries = c["entries_v9"] + c["entries_v10"]
    actionable_skips = c["skip_too_small"] + c["skip_ambiguous"] + c["skip_margin"] + c["skip_entry"]
    denom = total_entries + actionable_skips
    copy_rate = (total_entries / denom * 100) if denom > 0 else 0.0

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"Copy Rate Monitor -- {now} (last {lookback_hours}h)",
        "=" * 48,
        "",
        "SIGNALS",
        f"  Target fills detected:    {c['target_fills']:>5}",
        "",
        "ENTRIES",
        f"  V9 entries (IOC):         {c['entries_v9']:>5}",
        f"  V10 entries (chase):      {c['entries_v10']:>5}",
        f"  Exits:                    {c['exits']:>5}",
        "",
        "SKIPS",
        f"  Too small (TWAP):         {c['skip_too_small']:>5}",
        f"  Ambiguous (net/gross):    {c['skip_ambiguous']:>5}",
        f"  Closing (not actionable): {c['skip_closing']:>5}",
        f"  Margin blocked (total):   {c['skip_margin']:>5}",
        f"    - util cap:             {c['skip_margin_util']:>5}",
        f"    - coin concentration:   {c['skip_margin_coin']:>5}",
        f"    - addon cap (3x base):  {c['skip_margin_addon']:>5}",
        f"  Entry skipped (no book):  {c['skip_entry']:>5}",
        "",
        "=" * 48,
        f"  COPY RATE:  {copy_rate:5.1f}%  ({total_entries}/{denom} actionable)",
        "=" * 48,
    ]

    if coins:
        lines.append("")
        lines.append("COINS ENTERED")
        for coin, cnt in sorted(coins.items(), key=lambda x: -x[1]):
            lines.append(f"  {coin:<8} {cnt:>3}")

    if blocked:
        lines.append("")
        lines.append("COINS BLOCKED (margin)")
        for coin, cnt in sorted(blocked.items(), key=lambda x: -x[1]):
            lines.append(f"  {coin:<8} {cnt:>3}")

    # Per-session breakdown
    for name, sc in all_counts.items():
        sess_entries = sc["entries_v9"] + sc["entries_v10"]
        sess_skips = sc["skip_too_small"] + sc["skip_ambiguous"] + sc["skip_margin"] + sc["skip_entry"]
        sess_denom = sess_entries + sess_skips
        sess_rate = (sess_entries / sess_denom * 100) if sess_denom > 0 else 0.0
        lines.append("")
        lines.append(f"[{name}] fills={sc['target_fills']} entries={sess_entries} "
                      f"skips={sess_skips} closing={sc['skip_closing']} rate={sess_rate:.1f}%")

    return "\n".join(lines)


def send_telegram(text: str):
    import requests
    token = os.environ.get("TG_BOT_TOKEN")
    if not token:
        print("ERROR: TG_BOT_TOKEN not set in environment")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": TG_CHAT_ID,
        "text": f"```\n{text}\n```",
        "parse_mode": "Markdown",
    }, timeout=10)
    if resp.ok:
        print("Sent to Telegram.")
    else:
        print(f"Telegram send failed: {resp.status_code} {resp.text}")


def main():
    parser = argparse.ArgumentParser(description="Copy rate monitor")
    parser.add_argument("--telegram", action="store_true", help="Send report to Telegram")
    parser.add_argument("--hours", type=int, default=CUTOFF_HOURS, help="Lookback hours")
    args = parser.parse_args()

    lookback = args.hours
    cutoff = datetime.now() - timedelta(hours=lookback)

    all_counts = {}
    all_coins = {}
    all_blocked = {}
    for name, session in SESSIONS.items():
        lines = capture_tmux(session)
        if not lines:
            print(f"WARNING: No output from tmux session '{session}'")
        counts, coins, blocked_coins = analyze_logs(lines, cutoff)
        all_counts[name] = counts
        all_coins[name] = coins
        all_blocked[name] = blocked_coins

    report = format_report(all_counts, all_coins, all_blocked, lookback)
    print(report)

    if args.telegram:
        send_telegram(report)


if __name__ == "__main__":
    main()
