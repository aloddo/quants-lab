#!/usr/bin/env python3
"""Current-state sanity check for Gate-2 leader candidates.

This is intentionally read-only. It uses only public clearinghouseState calls,
loads no private key, places no orders, and never edits live config.

Default scope is conservative: top accepted candidates only. Increase --limit or
include --statuses watch when preparing a full replacement roster.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

HL_API = "https://api.hyperliquid.xyz"
REPO = Path(__file__).resolve().parents[2]
V28 = REPO / "app" / "data" / "research" / "v28"
DOCS = REPO / "docs" / "research"
CACHE = V28 / "live_sanity_cache.json"
DEXES = ["", "xyz", "flx"]


def _now() -> int:
    return int(time.time())


def _float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def save_cache(path: Path, cache: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, sort_keys=True))
    tmp.replace(path)


def ch_state(
    wallet: str,
    dex: str,
    *,
    cache: dict[str, Any],
    cache_ttl_s: int,
    sleep_s: float,
    retries: int,
) -> dict[str, Any] | None:
    key = f"{wallet}|{dex or 'main'}"
    cached = cache.get(key)
    if cached and _now() - int(cached.get("ts", 0)) <= cache_ttl_s:
        return cached.get("data")

    payload: dict[str, Any] = {"type": "clearinghouseState", "user": wallet}
    if dex:
        payload["dex"] = dex

    data = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(f"{HL_API}/info", json=payload, timeout=10)
            if r.status_code == 429:
                wait = max(sleep_s, min(20.0, sleep_s * attempt * 2.0))
                print(f"429 {wallet[:10]} {dex or 'main'} attempt={attempt}; sleep {wait:.1f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            break
        except Exception as exc:
            wait = max(sleep_s, min(20.0, sleep_s * attempt * 2.0))
            print(f"fetch failed {wallet[:10]} {dex or 'main'} attempt={attempt}: {exc}; sleep {wait:.1f}s")
            time.sleep(wait)
    time.sleep(sleep_s)

    if not isinstance(data, dict) or "assetPositions" not in data:
        cache[key] = {"ts": _now(), "data": None}
        return None
    cache[key] = {"ts": _now(), "data": data}
    return data


def summarize_wallet(wallet: str, states: dict[str, dict[str, Any] | None]) -> dict[str, Any]:
    account_value = 0.0
    margin_used = 0.0
    total_upnl = 0.0
    positions = []
    ok = True

    for dex, state in states.items():
        if state is None:
            ok = False
            continue
        ms = state.get("marginSummary") or {}
        account_value += _float(ms.get("accountValue"))
        margin_used += _float(ms.get("totalMarginUsed"))
        for ap in state.get("assetPositions") or []:
            pos = (ap or {}).get("position") or {}
            coin = pos.get("coin")
            szi = _float(pos.get("szi"))
            if not coin or abs(szi) < 1e-12:
                continue
            upnl = _float(pos.get("unrealizedPnl"))
            notional = abs(_float(pos.get("positionValue")))
            total_upnl += upnl
            positions.append(
                {
                    "dex": dex or "main",
                    "coin": coin,
                    "szi": szi,
                    "side": "long" if szi > 0 else "short",
                    "notional": notional,
                    "upnl": upnl,
                    "upnl_pct_notional": upnl / notional if notional > 0 else 0.0,
                    "entry_px": _float(pos.get("entryPx")),
                    "leverage": ((pos.get("leverage") or {}).get("value") if isinstance(pos.get("leverage"), dict) else None),
                }
            )

    worst = min((p["upnl_pct_notional"] for p in positions), default=0.0)
    biggest_notional = max((p["notional"] for p in positions), default=0.0)
    biggest_share = biggest_notional / account_value if account_value > 0 else 0.0
    upnl_acct = total_upnl / account_value if account_value > 0 else 0.0
    margin_ratio = margin_used / account_value if account_value > 0 else 0.0

    vetoes = []
    if not ok:
        vetoes.append("fetch_incomplete")
    if account_value <= 100:
        vetoes.append(f"tiny_account=${account_value:.0f}")
    if upnl_acct < -0.05:
        vetoes.append(f"open_upnl_acct={upnl_acct:.1%}")
    if worst < -0.10:
        vetoes.append(f"worst_position={worst:.1%}")
    if margin_ratio > 0.60:
        vetoes.append(f"margin_ratio={margin_ratio:.1%}")
    if biggest_share > 0.50:
        vetoes.append(f"single_position_share={biggest_share:.1%}")
    if len(positions) > 12:
        vetoes.append(f"too_many_open_positions={len(positions)}")

    return {
        "wallet": wallet,
        "fetch_ok": ok,
        "live_account_value": account_value,
        "live_margin_used": margin_used,
        "live_margin_ratio": margin_ratio,
        "live_open_positions": len(positions),
        "live_total_upnl": total_upnl,
        "live_upnl_acct_pct": upnl_acct,
        "live_worst_pos_pct_notional": worst,
        "live_biggest_position_notional": biggest_notional,
        "live_biggest_position_share": biggest_share,
        "live_status": "veto" if vetoes else "clean",
        "live_vetoes": "; ".join(vetoes) if vetoes else "clean",
        "live_positions_json": json.dumps(sorted(positions, key=lambda p: p["upnl_pct_notional"])[:20]),
    }


def write_report(df: pd.DataFrame, out_csv: Path, report: Path) -> None:
    clean = df[df["live_status"].eq("clean")].sort_values("gate2_score", ascending=False)
    veto = df[df["live_status"].eq("veto")].sort_values("gate2_score", ascending=False)
    out_csv_abs = out_csv.resolve()
    try:
        out_csv_label = str(out_csv_abs.relative_to(REPO))
    except ValueError:
        out_csv_label = str(out_csv)
    lines = [
        "# Gate-2 Live Sanity",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Read-only current-state check over Gate-2 candidates. Uses public clearinghouseState only.",
        "",
        "## Summary",
        "",
        f"- candidates checked: {len(df):,}",
        f"- live clean: {len(clean):,}",
        f"- live veto: {len(veto):,}",
        f"- csv: `{out_csv_label}`",
        "",
        "## Clean Candidates",
        "",
    ]
    if clean.empty:
        lines.append("_None._")
    else:
        lines.append("| wallet | hist score | acct | upnl/acct | worst pos | open | margin |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for r in clean.head(30).itertuples(index=False):
            lines.append(
                f"| `{r.wallet}` | {r.gate2_score:.1f} | ${r.live_account_value:.0f} | "
                f"{r.live_upnl_acct_pct:.1%} | {r.live_worst_pos_pct_notional:.1%} | "
                f"{int(r.live_open_positions)} | {r.live_margin_ratio:.1%} |"
            )
    lines.extend(["", "## Vetoed Candidates", ""])
    if veto.empty:
        lines.append("_None._")
    else:
        lines.append("| wallet | hist score | acct | upnl/acct | worst pos | reasons |")
        lines.append("| --- | ---: | ---: | ---: | ---: | --- |")
        for r in veto.head(60).itertuples(index=False):
            lines.append(
                f"| `{r.wallet}` | {r.gate2_score:.1f} | ${r.live_account_value:.0f} | "
                f"{r.live_upnl_acct_pct:.1%} | {r.live_worst_pos_pct_notional:.1%} | {r.live_vetoes} |"
            )
    report.write_text("\n".join(lines) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--screen", default=str(V28 / "gate2_leader_screen.csv"))
    ap.add_argument("--statuses", default="accept", help="comma-separated gate2 statuses")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--sleep-s", type=float, default=2.0)
    ap.add_argument("--cache-ttl-s", type=int, default=3600)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--out", default=str(V28 / "gate2_live_sanity.csv"))
    ap.add_argument("--report", default=str(DOCS / f"gate2_live_sanity_{datetime.now(timezone.utc):%Y%m%d}.md"))
    args = ap.parse_args()

    statuses = {s.strip() for s in args.statuses.split(",") if s.strip()}
    screen = pd.read_csv(args.screen)
    candidates = screen[screen["gate2_status"].isin(statuses)].sort_values("gate2_score", ascending=False)
    if args.limit > 0:
        candidates = candidates.head(args.limit)
    if candidates.empty:
        raise SystemExit("no candidates selected")

    cache = load_cache(CACHE)
    rows = []
    for i, row in enumerate(candidates.itertuples(index=False), 1):
        wallet = row.wallet
        print(f"[{i}/{len(candidates)}] {wallet}")
        states = {
            dex: ch_state(
                wallet,
                dex,
                cache=cache,
                cache_ttl_s=args.cache_ttl_s,
                sleep_s=args.sleep_s,
                retries=args.retries,
            )
            for dex in DEXES
        }
        live = summarize_wallet(wallet, states)
        rows.append({**row._asdict(), **live})
        save_cache(CACHE, cache)

    out = pd.DataFrame(rows).sort_values(["live_status", "gate2_score"], ascending=[True, False])
    out_path = Path(args.out)
    report = Path(args.report)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    DOCS.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    write_report(out, out_path, report)

    print(out["live_status"].value_counts().to_string())
    print(f"wrote {out_path}")
    print(f"wrote {report}")
    print(out[["wallet", "gate2_score", "live_status", "live_account_value", "live_upnl_acct_pct", "live_worst_pos_pct_notional", "live_vetoes"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
