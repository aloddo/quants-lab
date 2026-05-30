#!/usr/bin/env python3
"""V15 Step (b): apply G5 source-quality filter.

Codex #7 G5 spec:
  source_6m_ROE >= 50%
  active_folds >= 3
  n_journeys >= 5
  source_max_DD <= 100% (deferred — proxy not yet computed)

V15 additions (LP-grade discipline):
  dec1_anchor_usd >= $5,000   # filter "lucky small-account" effects
  has_fills == True            # data integrity

Outputs:
  /tmp/v15/g5_pass_wallets.json — structured list of qualifying wallets, sorted by
    source_score, with reasoning band:
      ELITE    : ROE >= 200% and dec1 >= $5K (LP-cant-ignore)
      STRONG   : ROE >= 100% and dec1 >= $5K (LP-preferred)
      QUALIFIED: ROE >= 50%  and dec1 >= $5K (G5 baseline)
      MICRO    : ROE >= 50%  and dec1 <  $5K (excluded — small account noise)
"""
from __future__ import annotations

import json
from pathlib import Path

SRC = Path("/tmp/v15/source_roe_top200.json")
OUT = Path("/tmp/v15/g5_pass_wallets.json")

MIN_DEC1_USD_LP = 5_000.0  # LP-grade minimum capital
G5_ROE_MIN = 0.50
G5_ACTIVE_FOLDS_MIN = 3
G5_N_JOURNEYS_MIN = 5
G5_SOURCE_MAX_DD_MAX = 1.00  # deferred check — proxy=0 currently for all


def band(roe: float, dec1: float) -> str:
    if roe >= 2.0 and dec1 >= MIN_DEC1_USD_LP:
        return "ELITE"
    if roe >= 1.0 and dec1 >= MIN_DEC1_USD_LP:
        return "STRONG"
    if roe >= 0.5 and dec1 >= MIN_DEC1_USD_LP:
        return "QUALIFIED"
    if roe >= 0.5 and dec1 < MIN_DEC1_USD_LP:
        return "MICRO"
    return "FAIL"


def main() -> None:
    raw = json.loads(SRC.read_text())
    results = {"ELITE": [], "STRONG": [], "QUALIFIED": [], "MICRO": [], "FAIL": []}

    for w, v in raw.items():
        if not v.get("valid_for_ranking", False) or not v.get("has_fills", False):
            continue
        g5_baseline = (
            v["source_6m_ROE"] >= G5_ROE_MIN
            and v["active_folds"] >= G5_ACTIVE_FOLDS_MIN
            and v["n_journeys"] >= G5_N_JOURNEYS_MIN
            and v["source_max_DD_proxy"] <= G5_SOURCE_MAX_DD_MAX
        )
        if not g5_baseline:
            continue

        b = band(v["source_6m_ROE"], v["dec1_anchor_usd"])
        rec = dict(v)
        rec["wallet"] = w
        rec["band"] = b
        results[b].append(rec)

    # Sort each band by source_score desc
    for k in results:
        results[k].sort(key=lambda r: r["source_score"], reverse=True)

    summary = {
        "spec": {
            "G5_ROE_MIN": G5_ROE_MIN,
            "G5_ACTIVE_FOLDS_MIN": G5_ACTIVE_FOLDS_MIN,
            "G5_N_JOURNEYS_MIN": G5_N_JOURNEYS_MIN,
            "G5_SOURCE_MAX_DD_MAX": G5_SOURCE_MAX_DD_MAX,
            "MIN_DEC1_USD_LP": MIN_DEC1_USD_LP,
        },
        "counts": {k: len(v) for k, v in results.items()},
        "results": results,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(summary, indent=2))
    print(f"Wrote {OUT}")
    print(f"\nBand counts: {summary['counts']}")

    for band_name in ["ELITE", "STRONG", "QUALIFIED"]:
        rows = results[band_name]
        if not rows:
            continue
        print(f"\n=== {band_name} ({len(rows)}) ===")
        print(
            f"{'wallet':<44} {'ROE%':>8} {'dec1$':>10} {'pnl_d$':>11} "
            f"{'folds':>5} {'jrny':>5} {'score':>8}"
        )
        for r in rows:
            print(
                f"{r['wallet']:<44} {r['source_6m_ROE']*100:>7.1f}% "
                f"{r['dec1_anchor_usd']:>10.0f} {r['pnl_delta_usd']:>11.0f} "
                f"{r['active_folds']:>5} {r['n_journeys']:>5} {r['source_score']:>8.3f}"
            )

    # Print MICRO for transparency
    print(f"\n=== MICRO (excluded, dec1 < ${MIN_DEC1_USD_LP:.0f}) ===")
    print("Excluded because LP would not credit ROE on tiny starting capital.")
    for r in results["MICRO"]:
        print(
            f"  {r['wallet']} ROE={r['source_6m_ROE']*100:.1f}% dec1=${r['dec1_anchor_usd']:.0f} "
            f"pnl=${r['pnl_delta_usd']:.0f} folds={r['active_folds']}"
        )


if __name__ == "__main__":
    main()
