#!/usr/bin/env python3
"""V15 Step (a): source ROE enrichment for top 200 wallets.

Implements codex consult #7 (2026-05-30 07:43 CEST) ranking metric:

    source_score = source_6m_ROE
                   * min(1, active_folds / 6)
                   * log1p(n_journeys)
                   * clamp(1 - source_max_DD / 1.0, 0.25, 1.0)

Inputs (read-only):
  app/data/v13/wallet_dec1_portfolio_anchor.parquet  -- 200 wallets, dec1 + today equity
  app/data/wallet_profiles_v13.parquet               -- 331K rows, fills + closed_pnl
  /tmp/v13_4sharpe_hunt/V-H_WF_K3_PROP.json          -- per-fold scoring, source-perspective sims

Output:
  /tmp/v15/source_roe_top200.json
    { wallet: {
        source_6m_ROE,            # pnl_delta_usd / max(dec1_anchor_usd, $1000)
        dec1_anchor_usd,
        pnl_delta_usd,
        active_folds,             # count of folds where wallet has n_entries>0 in scores
        n_journeys,               # sum of n_entries across all folds where wallet appears
        source_max_DD,            # max max_dd across per-fold scores (proxy for source DD)
        source_score,             # codex #7 ranking metric
        in_v13_sweep,             # True if appears in V-H_WF_K3_PROP scores
      } }

Notes:
  - source_6m_ROE uses pnl_delta_usd (today - dec1 anchor PnL) which captures realized
    PnL change. Net deposits/withdrawals can contaminate this; we floor denominator at
    $1000 and surface dec1_anchor_usd so step (b) can filter tiny accounts.
  - source_max_DD from per-fold sim is a proxy. True source max DD requires equity time
    series; if needed we can pull from wallet_equity_history_cache.pkl later.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

ROOT = Path("/Users/hermes/quants-lab")
DEC1_ANCHOR = ROOT / "app/data/v13/wallet_dec1_portfolio_anchor.parquet"
WALLET_PROFILES = ROOT / "app/data/wallet_profiles_v13.parquet"
V13_SWEEP = Path("/tmp/v13_4sharpe_hunt/V-H_WF_K3_PROP.json")
OUT = Path("/tmp/v15/source_roe_top200.json")

DEC1_ANCHOR_FLOOR_USD = 1000.0  # floor denom for ROE to suppress micro-accounts
MIN_DEC1_FLOOR_FOR_VALID = 100.0  # truly tiny accounts are unrankable


def load_dec1() -> pd.DataFrame:
    df = pd.read_parquet(DEC1_ANCHOR)
    df = df[df["ok"]].copy()
    df["wallet"] = df["wallet"].str.lower()
    return df


def load_sweep_scores() -> dict:
    """Return dict: wallet -> {n_active_folds, n_journeys, source_max_DD, in_v13_sweep}.

    Note: V-H_WF_K3_PROP universe is top-30 per fold by training Sharpe; only a small
    subset of top-200 by PnL wallets appear here. We use it as supplementary; primary
    active_folds + n_journeys come from raw fills per wallet (see load_fill_activity).
    """
    data = json.loads(V13_SWEEP.read_text())
    sel = data["selections_per_fold"]  # dict fold_str -> {selected, n_eligible, scores: [...]}
    per_wallet: dict = {}
    for _fold_id, fold_data in sel.items():
        for s in fold_data["scores"]:
            w = s["wallet"].lower()
            n_entries = int(s.get("n_entries", 0))
            dd = float(s.get("max_dd", 0.0))
            rec = per_wallet.setdefault(
                w,
                {"folds_active": 0, "n_journeys": 0, "max_dd_per_fold": []},
            )
            if n_entries > 0:
                rec["folds_active"] += 1
                rec["n_journeys"] += n_entries
                rec["max_dd_per_fold"].append(dd)
    return per_wallet


# 8 walk-forward fold test windows (from V-H_WF_K3_PROP.json fold_summaries)
FOLD_WINDOWS_UTC = [
    ("2026-01-15", "2026-01-29"),
    ("2026-01-30", "2026-02-13"),
    ("2026-02-14", "2026-02-28"),
    ("2026-03-01", "2026-03-15"),
    ("2026-03-16", "2026-03-30"),
    ("2026-03-31", "2026-04-14"),
    ("2026-04-15", "2026-04-29"),
    ("2026-04-30", "2026-05-14"),
]


def _to_ms(date_str: str, end_of_day: bool = False) -> int:
    import datetime as dt

    d = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    if end_of_day:
        d = d.replace(hour=23, minute=59, second=59)
    return int(d.timestamp() * 1000)


def load_fill_activity(wallets: list[str], fills_dir: Path, min_fills_active: int = 10) -> dict:
    """Per-wallet fold activity from raw fills.

    Returns dict: wallet -> {active_folds, n_fills_total, n_fills_per_fold, n_journeys_est}
    n_journeys_est = total position-open events across all folds (signed_sz crosses prior pos).
    """
    fold_bounds_ms = [(_to_ms(s), _to_ms(e, end_of_day=True)) for s, e in FOLD_WINDOWS_UTC]
    out: dict = {}
    for w in wallets:
        p = fills_dir / f"{w}.parquet"
        if not p.exists():
            out[w] = {
                "active_folds": 0,
                "n_fills_total": 0,
                "n_fills_per_fold": [0] * 8,
                "n_journeys_est": 0,
                "has_fills": False,
            }
            continue
        try:
            df = pd.read_parquet(p, columns=["time", "coin", "signed_sz", "startPosition"])
        except Exception as e:
            print(f"  ! failed to read {w}: {e}")
            out[w] = {
                "active_folds": 0,
                "n_fills_total": 0,
                "n_fills_per_fold": [0] * 8,
                "n_journeys_est": 0,
                "has_fills": False,
            }
            continue
        n_per_fold = []
        n_journeys = 0
        active = 0
        total = 0
        for lo, hi in fold_bounds_ms:
            mask = (df["time"] >= lo) & (df["time"] <= hi)
            n = int(mask.sum())
            n_per_fold.append(n)
            total += n
            if n >= min_fills_active:
                active += 1
            if n > 0:
                # journey estimate: count fills where startPosition is near 0 (new entry)
                # or where signed_sz pushes position past 0 (flip = new journey)
                sub = df[mask]
                try:
                    sp = pd.to_numeric(sub["startPosition"], errors="coerce").fillna(0.0)
                    ss = sub["signed_sz"].astype(float)
                    # New journey if startPosition was 0 and we open; or we flip side
                    opens_from_flat = (sp.abs() < 1e-9) & (ss.abs() > 1e-9)
                    flips = (sp * (sp + ss) < 0)
                    n_journeys += int((opens_from_flat | flips).sum())
                except Exception:
                    n_journeys += int(n // 4)  # crude fallback
        out[w] = {
            "active_folds": active,
            "n_fills_total": total,
            "n_fills_per_fold": n_per_fold,
            "n_journeys_est": n_journeys,
            "has_fills": True,
        }
    return out


def load_wallet_profiles() -> pd.DataFrame:
    df = pd.read_parquet(WALLET_PROFILES, columns=["wallet", "fills", "closed_pnl", "days_active"])
    df["wallet"] = df["wallet"].str.lower()
    return df


def compute_source_score(roe: float, active_folds: int, n_journeys: int, max_dd: float) -> float:
    coverage = min(1.0, active_folds / 6.0)
    confidence = math.log1p(max(0, n_journeys))
    # clamp(1 - DD/1.0, 0.25, 1.0)
    dd_penalty = max(0.25, min(1.0, 1.0 - max(0.0, max_dd)))
    return float(roe) * coverage * confidence * dd_penalty


def main() -> None:
    dec1 = load_dec1()
    sweep = load_sweep_scores()
    profiles = load_wallet_profiles()
    prof_lookup = profiles.set_index("wallet")[["fills", "closed_pnl", "days_active"]].to_dict("index")

    fills_dir = ROOT / "app/data/hl_s3_fills_v2_by_wallet"
    wallets = dec1["wallet"].tolist()
    print(f"Computing fold activity from raw fills for {len(wallets)} wallets...")
    fold_activity = load_fill_activity(wallets, fills_dir)
    print(
        f"  fills loaded: has_fills={sum(1 for v in fold_activity.values() if v['has_fills'])}"
    )

    out: dict = {}
    for _i, row in dec1.iterrows():
        w = row["wallet"]
        dec1_usd = float(row["dec1_anchor_usd"])
        pnl_delta = float(row["pnl_delta_usd"])

        denom = max(dec1_usd, DEC1_ANCHOR_FLOOR_USD)
        source_6m_ROE = pnl_delta / denom

        sw = sweep.get(w)
        in_sweep = sw is not None
        source_max_dd_proxy = (
            max(sw["max_dd_per_fold"]) if sw and sw["max_dd_per_fold"] else 0.0
        )

        # active_folds + n_journeys from RAW FILLS (primary), not sim
        fa = fold_activity.get(w, {})
        active_folds = int(fa.get("active_folds", 0))
        n_journeys = int(fa.get("n_journeys_est", 0))
        n_fills_total_window = int(fa.get("n_fills_total", 0))

        prof = prof_lookup.get(w, {})
        fills_total_all = int(prof.get("fills", 0))

        source_score = compute_source_score(
            source_6m_ROE, active_folds, n_journeys, source_max_dd_proxy
        )

        out[w] = {
            "source_6m_ROE": round(source_6m_ROE, 4),
            "dec1_anchor_usd": round(dec1_usd, 2),
            "today_anchor_usd": round(float(row["today_anchor_usd"]), 2),
            "pnl_delta_usd": round(pnl_delta, 2),
            "active_folds": active_folds,
            "n_journeys": n_journeys,
            "n_fills_in_8folds": n_fills_total_window,
            "n_fills_lifetime": fills_total_all,
            "n_fills_per_fold": fa.get("n_fills_per_fold", [0] * 8),
            "source_max_DD_proxy": round(source_max_dd_proxy, 4),
            "source_score": round(source_score, 4),
            "in_v13_sweep": in_sweep,
            "has_fills": bool(fa.get("has_fills", False)),
            "valid_for_ranking": dec1_usd >= MIN_DEC1_FLOOR_FOR_VALID,
        }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2))
    n_valid = sum(1 for v in out.values() if v["valid_for_ranking"])
    n_in_sweep = sum(1 for v in out.values() if v["in_v13_sweep"])
    print(f"Wrote {OUT} | n_wallets={len(out)} valid={n_valid} in_sweep={n_in_sweep}")

    # Print top 25 by source_score (valid only)
    valid = [(w, v) for w, v in out.items() if v["valid_for_ranking"]]
    valid.sort(key=lambda x: x[1]["source_score"], reverse=True)
    print(f"\nTOP 25 by source_score (of {len(valid)} valid):")
    print(
        f"{'wallet':<44} {'ROE%':>8} {'dec1$':>10} {'pnl_d$':>10} "
        f"{'folds':>5} {'jrny':>5} {'score':>8} sweep"
    )
    for w, v in valid[:25]:
        print(
            f"{w:<44} {v['source_6m_ROE']*100:>7.1f}% {v['dec1_anchor_usd']:>10.0f} "
            f"{v['pnl_delta_usd']:>10.0f} {v['active_folds']:>5} {v['n_journeys']:>5} "
            f"{v['source_score']:>8.3f} {'Y' if v['in_v13_sweep'] else 'N'}"
        )

    print(f"\nG5 pre-check (ROE>=50%, folds>=3, journeys>=5):")
    g5_pass = [
        (w, v) for w, v in valid
        if v["source_6m_ROE"] >= 0.50 and v["active_folds"] >= 3 and v["n_journeys"] >= 5
    ]
    g5_pass.sort(key=lambda x: x[1]["source_score"], reverse=True)
    print(f"  G5 pass: {len(g5_pass)} wallets")
    for w, v in g5_pass[:15]:
        print(
            f"  {w} ROE={v['source_6m_ROE']*100:.1f}% folds={v['active_folds']} "
            f"jrny={v['n_journeys']} score={v['source_score']:.3f}"
        )


if __name__ == "__main__":
    main()
