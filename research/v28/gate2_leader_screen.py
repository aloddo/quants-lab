#!/usr/bin/env python3
"""Gate-2 leader screen: copyable leaders with explicit bag-holder vetoes.

Offline by default. Reads v27 parquet outputs and writes:
  - app/data/research/v28/gate2_leader_screen.csv
  - docs/research/gate2_leader_screen_YYYYMMDD.md

The screen treats high win-rate as weak evidence. It ranks realized/copyable edge
only after penalizing loss holding, deep tails, add-on behavior, open bags, and
weak sample quality.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
V27 = REPO / "app" / "data" / "research" / "v27"
OUT = REPO / "app" / "data" / "research" / "v28"
DOCS = REPO / "docs" / "research"


def _safe_div(a, b):
    return np.where(np.asarray(b) != 0, np.asarray(a) / np.asarray(b), np.nan)


def _pct(x):
    return f"{x:.1%}" if pd.notna(x) else ""


def _fmt(x, nd=2):
    return f"{x:.{nd}f}" if pd.notna(x) else ""


def load_inputs():
    journeys = pd.read_parquet(
        V27 / "journeys_full.parquet",
        columns=[
            "wallet",
            "coin",
            "side",
            "entry_ts",
            "exit_ts",
            "net_realized_pnl",
            "max_notional",
            "duration_h",
            "liq_closed",
            "n_actions",
            "open_size",
        ],
    )
    journeys = journeys[(journeys["max_notional"] > 10) & journeys["exit_ts"].notna()].copy()
    journeys["net_bps"] = journeys["net_realized_pnl"] / journeys["max_notional"] * 1e4
    journeys["is_win"] = journeys["net_realized_pnl"] > 0
    journeys["is_loss"] = journeys["net_realized_pnl"] < 0
    journeys["is_big_loss_20pct"] = journeys["net_bps"] <= -2000
    journeys["addon_proxy"] = journeys["n_actions"] > 2

    gates = pd.read_parquet(V27 / "gates_full.parquet")
    lcb = pd.read_parquet(V27 / "lcb_table.parquet")
    return journeys, gates, lcb


def aggregate_journeys(j: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for wallet, g in j.groupby("wallet", sort=False):
        pnl = g["net_realized_pnl"]
        bps = g["net_bps"]
        wins = g[g["is_win"]]
        losses = g[g["is_loss"]]

        win_dur = wins["duration_h"]
        loss_dur = losses["duration_h"]
        n = len(g)
        n_loss = len(losses)
        n_win = len(wins)

        rows.append(
            {
                "wallet": wallet,
                "n_journeys": n,
                "net_pnl": float(pnl.sum()),
                "mean_bps": float(bps.mean()),
                "median_bps": float(bps.median()),
                "win_rate": n_win / n if n else np.nan,
                "loss_rate": n_loss / n if n else np.nan,
                "med_win_hold_h": float(win_dur.median()) if n_win else np.nan,
                "med_loss_hold_h": float(loss_dur.median()) if n_loss else np.nan,
                "avg_win_hold_h": float(win_dur.mean()) if n_win else np.nan,
                "avg_loss_hold_h": float(loss_dur.mean()) if n_loss else np.nan,
                "p05_bps": float(bps.quantile(0.05)),
                "p01_bps": float(bps.quantile(0.01)),
                "worst_bps": float(bps.min()),
                "med_win_bps": float(wins["net_bps"].median()) if n_win else np.nan,
                "med_loss_bps": float(losses["net_bps"].median()) if n_loss else np.nan,
                "addon_proxy_rate": float(g["addon_proxy"].mean()),
                "loss_addon_proxy_rate": float(losses["addon_proxy"].mean()) if n_loss else np.nan,
                "big_loss_rate_20pct": float(g["is_big_loss_20pct"].mean()),
                "liq_count": int(g["liq_closed"].sum()),
                "n_coins": int(g["coin"].nunique()),
                "first_ts": int(g["entry_ts"].min()),
                "last_ts": int(g["entry_ts"].max()),
            }
        )
    out = pd.DataFrame(rows)
    out["loss_win_med_hold_ratio"] = _safe_div(out["med_loss_hold_h"], out["med_win_hold_h"])
    out["loss_win_avg_hold_ratio"] = _safe_div(out["avg_loss_hold_h"], out["avg_win_hold_h"])
    return out


def latest_lcb(lcb: pd.DataFrame) -> pd.DataFrame:
    if lcb.empty:
        return pd.DataFrame(columns=["wallet", "latest_lcb_bps", "latest_lcb_trips", "best_lcb_bps"])
    latest_k = int(lcb["boundary_k"].max())
    latest = lcb[lcb["boundary_k"] == latest_k][["wallet", "lcb_bps", "n_trips"]].rename(
        columns={"lcb_bps": "latest_lcb_bps", "n_trips": "latest_lcb_trips"}
    )
    best = (
        lcb.groupby("wallet", as_index=False)
        .agg(best_lcb_bps=("lcb_bps", "max"), max_lcb_trips=("n_trips", "max"))
    )
    return latest.merge(best, on="wallet", how="outer")


def classify(df: pd.DataFrame) -> pd.DataFrame:
    reasons_col = []
    status_col = []
    score_col = []

    for r in df.itertuples(index=False):
        reasons = []

        if not bool(getattr(r, "eligible", False)):
            reasons.append("not_v27_eligible")
        if r.n_journeys < 50:
            reasons.append(f"small_sample_n={r.n_journeys}")
        if r.net_pnl <= 0:
            reasons.append("nonpositive_net_pnl")
        if pd.isna(r.latest_lcb_bps):
            reasons.append("missing_latest_lcb")
        elif r.latest_lcb_bps <= 0:
            reasons.append(f"latest_lcb<=0:{r.latest_lcb_bps:.1f}")
        if pd.notna(r.loss_win_avg_hold_ratio) and r.loss_win_avg_hold_ratio > 2.0:
            reasons.append(f"loss_avg_hold_ratio>{r.loss_win_avg_hold_ratio:.1f}")
        if pd.notna(r.loss_win_med_hold_ratio) and r.loss_win_med_hold_ratio > 2.5:
            reasons.append(f"loss_med_hold_ratio>{r.loss_win_med_hold_ratio:.1f}")
        if r.big_loss_rate_20pct > 0.03:
            reasons.append(f"big_loss_rate>{r.big_loss_rate_20pct:.1%}")
        if r.p01_bps < -2000:
            reasons.append(f"p01_tail_bps={r.p01_bps:.0f}")
        if r.worst_bps < -5000:
            reasons.append(f"catastrophic_worst_bps={r.worst_bps:.0f}")
        if pd.notna(r.loss_addon_proxy_rate) and r.loss_addon_proxy_rate > 0.80:
            reasons.append(f"loss_addon_rate>{r.loss_addon_proxy_rate:.1%}")
        if r.liq_count > 0:
            reasons.append(f"liquidations={r.liq_count}")
        if pd.notna(r.open_mtm_usd) and r.open_mtm_usd < -2500:
            reasons.append(f"open_bag_mtm=${r.open_mtm_usd:.0f}")
        if r.win_rate > 0.90 and r.loss_rate < 0.05 and r.n_journeys >= 50:
            reasons.append("realize_only_winners_shape")

        hard = any(
            x.startswith(
                (
                    "not_v27_eligible",
                    "small_sample",
                    "nonpositive",
                    "missing_latest_lcb",
                    "latest_lcb",
                    "loss_avg",
                    "loss_med",
                    "big_loss",
                    "p01_tail",
                    "catastrophic",
                    "loss_addon",
                    "liquidations",
                    "open_bag",
                    "realize_only",
                )
            )
            for x in reasons
        )

        if hard:
            status = "reject"
        else:
            watch_reasons = []
            if r.win_rate > 0.85:
                watch_reasons.append("very_high_win_rate_monitor")
            if r.addon_proxy_rate > 0.75:
                watch_reasons.append("high_addon_rate")
            if r.n_coins < 3:
                watch_reasons.append("narrow_coin_set")
            if pd.notna(r.open_mtm_usd) and r.open_mtm_usd < 0:
                watch_reasons.append(f"small_open_mtm_loss=${r.open_mtm_usd:.0f}")
            if watch_reasons:
                status = "watch"
                reasons.extend(watch_reasons)
            else:
                status = "accept"

        latest_lcb = getattr(r, "latest_lcb_bps", 0)
        edge = max(float(latest_lcb), 0.0) if pd.notna(latest_lcb) else 0.0
        score = (
            edge
            + 0.20 * max(float(r.mean_bps), 0.0)
            + 0.05 * max(float(r.median_bps), 0.0)
            - 1500.0 * float(r.big_loss_rate_20pct)
            - max(0.0, float(r.loss_win_avg_hold_ratio) - 1.0 if pd.notna(r.loss_win_avg_hold_ratio) else 0.0) * 80.0
            - max(0.0, float(r.loss_addon_proxy_rate) - 0.5 if pd.notna(r.loss_addon_proxy_rate) else 0.0) * 120.0
            - max(0.0, -float(r.open_mtm_usd) if pd.notna(r.open_mtm_usd) else 0.0) * 0.02
        )
        if status == "reject":
            score -= 10_000
        elif status == "watch":
            score -= 500

        reasons_col.append("; ".join(reasons) if reasons else "clean")
        status_col.append(status)
        score_col.append(score)

    df = df.copy()
    df["gate2_status"] = status_col
    df["gate2_reasons"] = reasons_col
    df["gate2_score"] = score_col
    return df


def write_report(df: pd.DataFrame, out_csv: Path, report_path: Path):
    top = df.sort_values(["gate2_status", "gate2_score"], ascending=[True, False])
    accepted = df[df["gate2_status"] == "accept"].sort_values("gate2_score", ascending=False)
    watch = df[df["gate2_status"] == "watch"].sort_values("gate2_score", ascending=False)
    rejected = df[df["gate2_status"] == "reject"]

    lines = []
    lines.append("# Gate-2 Leader Screen")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("Offline screen over v27 local parquet. No Hyperliquid REST calls.")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- wallets screened: {len(df):,}")
    lines.append(f"- accept: {len(accepted):,}")
    lines.append(f"- watch: {len(watch):,}")
    lines.append(f"- reject: {len(rejected):,}")
    lines.append(f"- csv: `{out_csv.relative_to(REPO)}`")
    lines.append("")
    lines.append("## Top Accepts")
    lines.append("")
    cols = [
        "wallet",
        "gate2_score",
        "n_journeys",
        "latest_lcb_bps",
        "net_pnl",
        "win_rate",
        "loss_win_avg_hold_ratio",
        "big_loss_rate_20pct",
        "loss_addon_proxy_rate",
        "open_mtm_usd",
    ]
    if accepted.empty:
        lines.append("_No accepted wallets under current hard gates._")
    else:
        lines.append("| wallet | score | n | lcb | net | win | loss/win hold avg | big loss | loss add | open mtm |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for r in accepted.head(40)[cols].itertuples(index=False):
            lines.append(
                f"| `{r.wallet}` | {_fmt(r.gate2_score,1)} | {int(r.n_journeys)} | {_fmt(r.latest_lcb_bps,1)} | "
                f"{_fmt(r.net_pnl,0)} | {_pct(r.win_rate)} | {_fmt(r.loss_win_avg_hold_ratio,2)} | "
                f"{_pct(r.big_loss_rate_20pct)} | {_pct(r.loss_addon_proxy_rate)} | {_fmt(r.open_mtm_usd,0)} |"
            )
    lines.append("")
    lines.append("## Top Watch")
    lines.append("")
    if watch.empty:
        lines.append("_No watch wallets._")
    else:
        lines.append("| wallet | score | n | lcb | net | reasons |")
        lines.append("| --- | ---: | ---: | ---: | ---: | --- |")
        for r in watch.head(40).itertuples(index=False):
            lines.append(
                f"| `{r.wallet}` | {_fmt(r.gate2_score,1)} | {int(r.n_journeys)} | "
                f"{_fmt(r.latest_lcb_bps,1)} | {_fmt(r.net_pnl,0)} | {r.gate2_reasons} |"
            )
    lines.append("")
    lines.append("## Reject Reason Counts")
    lines.append("")
    reason_counts = {}
    for rs in rejected["gate2_reasons"].dropna():
        for reason in rs.split("; "):
            key = reason.split(":")[0].split(">")[0].split("=")[0]
            reason_counts[key] = reason_counts.get(key, 0) + 1
    for k, v in sorted(reason_counts.items(), key=lambda kv: -kv[1])[:30]:
        lines.append(f"- {k}: {v:,}")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This is a first repeatable Gate-2 screen, not a final live roster.")
    lines.append("- Next step: live current-bag refresh only for accepted/watch finalists, then cohort correlation optimization.")
    report_path.write_text("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-journeys", type=int, default=50)
    ap.add_argument("--out-csv", default=str(OUT / "gate2_leader_screen.csv"))
    ap.add_argument("--report", default=str(DOCS / f"gate2_leader_screen_{datetime.now(timezone.utc):%Y%m%d}.md"))
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    DOCS.mkdir(parents=True, exist_ok=True)

    journeys, gates, lcb = load_inputs()
    metrics = aggregate_journeys(journeys)
    metrics = metrics[metrics["n_journeys"] >= args.min_journeys].copy()

    gate_cols = [
        "wallet",
        "eligible",
        "n_active_days",
        "n_closed_journeys",
        "unmarkable_frac",
        "open_mtm_usd",
        "trail30_abs_pnl",
        "pass_open_bag",
        "pass_liquidation",
    ]
    full = metrics.merge(gates[gate_cols], on="wallet", how="left")
    full = full.merge(latest_lcb(lcb), on="wallet", how="left")
    full["eligible"] = full["eligible"].fillna(False)
    full["open_mtm_usd"] = full["open_mtm_usd"].fillna(0.0)

    full = classify(full)
    full = full.sort_values(["gate2_status", "gate2_score"], ascending=[True, False])

    out_csv = Path(args.out_csv)
    report = Path(args.report)
    full.to_csv(out_csv, index=False)
    write_report(full, out_csv, report)

    print(f"screened {len(full):,} wallets")
    print(full["gate2_status"].value_counts().to_string())
    print(f"wrote {out_csv}")
    print(f"wrote {report}")
    print("\nTop accepts:")
    show = full[full["gate2_status"] == "accept"].head(20)
    if show.empty:
        print("  none")
    else:
        print(
            show[
                [
                    "wallet",
                    "gate2_score",
                    "n_journeys",
                    "latest_lcb_bps",
                    "net_pnl",
                    "win_rate",
                    "loss_win_avg_hold_ratio",
                    "big_loss_rate_20pct",
                    "gate2_reasons",
                ]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
