#!/usr/bin/env python
"""
Copy A — consistency ranking over the wide copyability screen.

Reads app/data/copyA/wide_screen.jsonl (produced by copyA_wide_screen.py, pure
read, NO api calls so it is safe to run while the screen is still in flight) and
ranks surviving wallets by CONSISTENCY, not absolute PnL. The L3 lesson
(work/quant-engineer/copy-a-l3-results) is that ranking by absolute leaderboard
PnL biases the roster toward one-lucky-month winners; this screen fixes that.

Consistency gate (all must pass):
  perp_share   >= 0.50   directional-copyable (a follower cannot mirror MM spread capture)
  active_months>= 4      real ongoing trader, not a 1-2 month streak
  pos_frac     >= 0.60   majority of active months net-positive for the FOLLOWER
  total_real_k >  0      follower makes money net of real execution drag
  liquid_share >= 0.70   tradeable majors, not illiquid micro-caps a follower cannot fill

Survivability flag (not a hard gate; surfaced for the roster decision):
  dd_ratio = worst_month_k / best_month_k   how deep the worst month cuts vs the best

CAPITAL-MATH NOTE (Hard Rule 18): total_real_k here is the wallet's realized PnL
on THEIR notional. For the $500 MRR go/no-go the decisive number is the
FOLLOWER return-on-capital at our ~$924 equity, i.e. monthly % return applied to
what we can actually deploy per mirror. Absolute PnL scales linearly with size
and is NOT the ship criterion. This ranker produces the consistency shortlist;
the ROC conversion (needs each wallet's deployed notional per trade) is the
required next step before any roster is proposed.
"""
import json
import sys
from pathlib import Path

JSONL = Path("app/data/copyA/local_screen.jsonl")  # local S3 fills screen (zero-network)

PERP_MIN = 0.50
MONTHS_MIN = 4
POSFRAC_MIN = 0.60
LIQ_MIN = 0.70
MAKER_MAX = 0.60  # maker_share above this = MM/passive = uncopyable (drop)


def load(path: Path):
    rows = []
    if not path.exists():
        print(f"MISSING {path}", file=sys.stderr)
        return rows
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def passes(r):
    if r.get("status") != "ok":
        return False
    return (
        r.get("perp_share", 0) >= PERP_MIN
        and r.get("maker_share", 0) <= MAKER_MAX
        and r.get("active_months", 0) >= MONTHS_MIN
        and r.get("pos_frac", 0) >= POSFRAC_MIN
        and r.get("total_real_k", 0) > 0
        and r.get("liquid_share", 0) >= LIQ_MIN
    )


def dd_ratio(r):
    best = r.get("best_month_k", 0) or 0.0
    worst = r.get("worst_month_k", 0) or 0.0
    if best <= 0:
        return None
    return round(worst / best, 3)


def main():
    rows = load(JSONL)
    ok = [r for r in rows if r.get("status") == "ok"]
    survivors = [r for r in rows if passes(r)]
    # Rank: active_months desc, then pos_frac desc, then dd_ratio (shallower worst month first).
    survivors.sort(
        key=lambda r: (
            r.get("active_months", 0),
            r.get("pos_frac", 0),
            (dd_ratio(r) or -99),
        ),
        reverse=True,
    )
    print(f"screened={len(rows)} ok={len(ok)} survivors={len(survivors)}")
    print(
        f"gate: perp>={PERP_MIN} maker<={MAKER_MAX} months>={MONTHS_MIN} "
        f"posfrac>={POSFRAC_MIN} total>0 liq>={LIQ_MIN}\n"
    )
    hdr = f"{'wallet':<44} {'mo':>3} {'posf':>5} {'perp':>5} {'mkr':>5} {'liq':>5} {'total_k':>9} {'worst_k':>9} {'ddR':>6}  coins"
    print(hdr)
    print("-" * len(hdr))
    for r in survivors:
        print(
            f"{r['wallet']:<44} {r.get('active_months',0):>3} "
            f"{r.get('pos_frac',0):>5.2f} {r.get('perp_share',0):>5.2f} "
            f"{r.get('maker_share',0):>5.2f} "
            f"{r.get('liquid_share',0):>5.2f} {r.get('total_real_k',0):>9.1f} "
            f"{r.get('worst_month_k',0):>9.1f} "
            f"{str(dd_ratio(r)):>6}  {r.get('coins','')}"
        )
    if not survivors:
        print("(no survivors yet — screen may still be in flight)")


if __name__ == "__main__":
    main()
