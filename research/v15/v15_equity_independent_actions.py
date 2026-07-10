#!/usr/bin/env python3
"""Build equity-independent, causally ordered perp actions from enriched fills.

This lane is suitable for fixed-notional or direction-only copy research. It
does not reconstruct leader equity and never emits exposure percentages.

The default output is one row per source fill because the live trade stream is
also fill-by-fill.  Same-millisecond burst rows are available as a diagnostic,
but must not drive a fidelity replay: netting a burst can erase an intraburst
open, close, or reversal that the live engine would observe.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_m01_equity_reconstruct as m01  # noqa: E402
from _streaming_io import ShardedParquetWriter, install_memory_guard  # noqa: E402


EPS = 1e-9


def classify_transition(before: float, after: float) -> str:
    if abs(before) <= EPS and abs(after) <= EPS:
        return "FLAT"
    if abs(before) <= EPS:
        return "ENTRY"
    if abs(after) <= EPS:
        return "EXIT"
    if np.sign(before) != np.sign(after):
        return "REVERSE"
    if abs(after) > abs(before) + EPS:
        return "ADDON"
    if abs(after) < abs(before) - EPS:
        return "TRIM"
    return "NO_CHANGE"


def build_atomic_actions(wallet: str, fills: list[dict]) -> list[dict]:
    """Return one causally ordered action per perp fill, without leader equity.

    ``startPosition`` is authoritative for the current transition.  A gap from
    the prior observed position is nevertheless fail-closed for copy replay:
    it proves that this event stream cannot reproduce what a stateful live
    tracker would have known.  The current row reseeds state so later rows can
    become valid again.
    """
    ordered = m01.order_wallet_fills_causally(list(fills))
    prior_position: dict[str, float] = {}
    rows: list[dict] = []
    for fill in ordered:
        coin = str(fill["coin"])
        if not m01.coin_is_allowed_perp(coin):
            continue
        before = float(fill["startPosition"])
        signed = float(fill["signed_sz"])
        after = before + signed
        price = float(fill["price"])
        expected = prior_position.get(coin)
        continuity_ok = expected is None or np.isclose(before, expected, rtol=1e-9, atol=1e-9)
        order_ok = bool(fill.get("causal_order_ok", True))
        values_ok = bool(
            np.isfinite(before)
            and np.isfinite(signed)
            and np.isfinite(after)
            and np.isfinite(price)
            and price > 0
            and abs(signed) > EPS
        )
        transition_valid = bool(order_ok and values_ok)
        rows.append(
            {
                "wallet": wallet.lower(),
                "coin": coin,
                "ts": int(fill["time"]),
                "fill_seq": int(fill.get("fill_seq", len(rows))),
                "tid": fill.get("tid"),
                "action_type": classify_transition(before, after),
                "position_before": before,
                "position_after": after,
                "signed_size": signed,
                "source_notional": abs(signed) * price,
                "source_fee": float(fill.get("fee", 0.0) or 0.0),
                "price": price,
                # Mirrors V17's true-open dust convention at each source fill.
                "live_open_candidate": bool(
                    abs(before) * price < 1.0 and abs(after) * price >= 1.0
                ),
                "continuity_ok": bool(continuity_ok),
                "causal_order_ok": order_ok,
                "transition_valid": transition_valid,
                # Strict stream-replay validity.  A historical startPosition
                # can repair a gap; V17's raw websocket trade does not carry it.
                "copy_signal_valid": bool(transition_valid and continuity_ok),
                "state_resync": bool(expected is not None and not continuity_ok),
            }
        )
        prior_position[coin] = after
    return rows


def build_burst_actions(wallet: str, fills: list[dict]) -> list[dict]:
    """Net same-millisecond fills for diagnostics, not fidelity replay."""
    ordered = m01.order_wallet_fills_causally(list(fills))
    groups: dict[tuple[int, str], list[dict]] = {}
    for fill in ordered:
        if not m01.coin_is_allowed_perp(fill["coin"]):
            continue
        groups.setdefault((int(fill["time"]), str(fill["coin"])), []).append(fill)

    prior_position: dict[str, float] = {}
    rows: list[dict] = []
    for (ts, coin), burst in sorted(groups.items()):
        first, last = burst[0], burst[-1]
        before = float(first["startPosition"])
        after = float(last["startPosition"] + last["signed_sz"])
        expected = prior_position.get(coin)
        continuity_ok = expected is None or np.isclose(before, expected, rtol=1e-9, atol=1e-9)
        order_ok = all(bool(f.get("causal_order_ok", True)) for f in burst)
        total_signed = float(sum(float(f["signed_sz"]) for f in burst))
        # A complete chain must reconcile its aggregate delta as an additional
        # hard check; false means do not act on this burst.
        aggregate_ok = np.isclose(after - before, total_signed, rtol=1e-9, atol=1e-9)
        fee = float(sum(float(f.get("fee", 0.0) or 0.0) for f in burst))
        notional = float(sum(abs(float(f["signed_sz"])) * float(f["price"]) for f in burst))
        first_price = float(first["price"])
        last_price = float(last["price"])
        rows.append(
            {
                "wallet": wallet.lower(),
                "coin": coin,
                "ts": ts,
                "action_type": classify_transition(before, after),
                "position_before": before,
                "position_after": after,
                "signed_size": total_signed,
                "n_source_fills": len(burst),
                "source_notional": notional,
                "source_fee": fee,
                "first_price": first_price,
                "last_price": last_price,
                # Mirrors V17's true-open dust convention: pre-position below
                # $1 notional, post-position at or above $1.
                "live_open_candidate": bool(
                    abs(before) * first_price < 1.0 and abs(after) * last_price >= 1.0
                ),
                "continuity_ok": bool(continuity_ok),
                "causal_order_ok": bool(order_ok),
                "aggregate_delta_ok": bool(aggregate_ok),
                "transition_valid": bool(order_ok and aggregate_ok),
                "copy_signal_valid": bool(order_ok and aggregate_ok and continuity_ok),
                "state_resync": bool(expected is not None and not continuity_ok),
            }
        )
        prior_position[coin] = after
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--wallets-file", type=Path, required=True)
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-23")
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--memory-gb", type=float, default=3.0)
    ap.add_argument(
        "--granularity",
        choices=("atomic", "burst"),
        default="atomic",
        help="atomic is required for live-fidelity replay; burst is diagnostic only",
    )
    args = ap.parse_args()
    install_memory_guard(soft_gb=args.memory_gb, label="equity_independent_actions")
    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)
    wallets = [
        line.strip().lower()
        for line in args.wallets_file.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    writer = ShardedParquetWriter(args.output)
    for wallet in wallets:
        fills = m01.load_wallet_fills(wallet, start_ms, end_ms)
        builder = build_atomic_actions if args.granularity == "atomic" else build_burst_actions
        writer.add_many(builder(wallet, fills))
    writer.close()


if __name__ == "__main__":
    main()
