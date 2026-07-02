#!/usr/bin/env python3
"""Derive equity-independent copy lifecycles from authoritative fill state.

This module models only the leader-signal contract used by V17:

* enter once when the leader moves from <$1 dust to >=$1 notional;
* track, but do not copy, same-direction additions;
* exit the copied position once reverse flow reaches a configured fraction of
  the accumulated leader notional;
* never turn a reversal into a new entry while the leader was already open;
* invalidate a copied lifecycle on any unreplayable stream/state gap.

It deliberately does not use M1 equity. Execution prices, portfolio gates,
risk overlays, and wallet selection belong to later replay layers.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_m01_equity_reconstruct as m01  # noqa: E402
from _streaming_io import ShardedParquetWriter, install_memory_guard  # noqa: E402
from v15_equity_independent_actions import build_atomic_actions  # noqa: E402


EPS = 1e-9


def derive_copy_lifecycles(
    wallet: str,
    fills: list[dict],
    close_fraction: float = 0.85,
) -> tuple[list[dict], dict]:
    """Return lifecycle rows plus an audit dictionary for one wallet."""
    if not 0 < close_fraction <= 1:
        raise ValueError("close_fraction must be in (0, 1]")

    actions = build_atomic_actions(wallet, fills)
    held: dict[str, dict] = {}
    lifecycle_seq: dict[str, int] = {}
    rows: list[dict] = []
    audit = {
        "wallet": wallet.lower(),
        "n_actions": len(actions),
        "n_invalid_actions": 0,
        "n_entries": 0,
        "n_valid_closes": 0,
        "n_invalidated": 0,
        "n_leader_side_diverged": 0,
        "n_open_at_end": 0,
    }

    def finish(coin: str, ts: int | None, px: float | None, reason: str, valid: bool) -> None:
        st = held.pop(coin)
        rows.append(
            {
                "wallet": wallet.lower(),
                "coin": coin,
                "copy_lifecycle_id": st["copy_lifecycle_id"],
                "side": st["side"],
                "entry_ts": st["entry_ts"],
                "entry_leader_price": st["entry_leader_price"],
                "exit_ts": ts,
                "exit_leader_price": px,
                "exit_reason": reason,
                "lifecycle_valid": bool(valid),
                "leader_accumulated_notional": st["accumulated_notional"],
                "leader_reverse_notional": st["reverse_notional"],
                "reverse_fraction": (
                    st["reverse_notional"] / st["accumulated_notional"]
                    if st["accumulated_notional"] > EPS
                    else np.nan
                ),
                "n_leader_fills": st["n_leader_fills"],
                "leader_side_divergence_ts": st.get("leader_side_divergence_ts"),
            }
        )

    for action in actions:
        coin = str(action["coin"])
        ts = int(action["ts"])
        px = float(action["price"])
        if not bool(action["copy_signal_valid"]):
            audit["n_invalid_actions"] += 1
            if coin in held:
                finish(coin, ts, px, "stream_gap", False)
                audit["n_invalidated"] += 1
            continue

        before = float(action["position_before"])
        after = float(action["position_after"])
        signed = float(action["signed_size"])
        before_side = 0 if abs(before) <= EPS else (1 if before > 0 else -1)
        fill_side = 1 if signed > 0 else -1

        st = held.get(coin)
        if st is not None:
            # Once the leader has crossed to the opposite side without the
            # dollar-notional threshold firing, V17 can no longer faithfully
            # infer this copy's close from later leader flow. Keep it open for
            # a downstream risk-overlay replay; do not fabricate another entry.
            if st.get("leader_side_divergence_ts") is not None:
                st["n_leader_fills"] += 1
                continue
            # A valid held lifecycle must still refer to the same leader leg.
            if before_side != st["side"] and abs(before) * px >= 1.0:
                st["leader_side_divergence_ts"] = ts
                audit["n_leader_side_diverged"] += 1
                continue
            elif fill_side == st["side"]:
                st["accumulated_notional"] += float(action["source_notional"])
                st["n_leader_fills"] += 1
                continue
            else:
                closed_qty = min(abs(signed), abs(before))
                st["reverse_notional"] += closed_qty * px
                st["n_leader_fills"] += 1
                frac = (
                    st["reverse_notional"] / st["accumulated_notional"]
                    if st["accumulated_notional"] > EPS
                    else float("inf")
                )
                if frac + EPS >= close_fraction:
                    finish(coin, ts, px, "leader_close_threshold", True)
                    audit["n_valid_closes"] += 1
                elif abs(after) * px < 1.0 or (after > 0) != (st["side"] > 0):
                    # Quantity is flat/reversed but the runtime threshold is
                    # dollar-notional based. V17 does not have a separate flat
                    # override, so leader-driven exit signaling is now lost.
                    st["leader_side_divergence_ts"] = ts
                    audit["n_leader_side_diverged"] += 1
                continue

        # Match the runtime: a reversal/trim on an uncopied leader position is
        # tracking-only.  Only a true dust->open transition can create a copy.
        if coin not in held and bool(action["live_open_candidate"]):
            side = 1 if after > 0 else -1
            lifecycle_seq[coin] = lifecycle_seq.get(coin, 0) + 1
            held[coin] = {
                "copy_lifecycle_id": lifecycle_seq[coin],
                "side": side,
                "entry_ts": ts,
                "entry_leader_price": px,
                "accumulated_notional": float(action["source_notional"]),
                "reverse_notional": 0.0,
                "n_leader_fills": 1,
                "leader_side_divergence_ts": None,
            }
            audit["n_entries"] += 1

    for coin in sorted(list(held)):
        if held[coin].get("leader_side_divergence_ts") is not None:
            finish(coin, None, None, "leader_side_diverged", True)
        else:
            finish(coin, None, None, "open_at_window_end", True)
            audit["n_open_at_end"] += 1
    return rows, audit


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--wallets-file", type=Path, required=True)
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-23")
    ap.add_argument("--close-fraction", type=float, default=0.85)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--audit-output", type=Path)
    ap.add_argument("--memory-gb", type=float, default=3.0)
    args = ap.parse_args()
    install_memory_guard(soft_gb=args.memory_gb, label="fixed_notional_signals")
    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    end_ms = int((pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000 - 1)
    wallets = [
        line.strip().lower()
        for line in args.wallets_file.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    writer = ShardedParquetWriter(args.output)
    audits: list[dict] = []
    for wallet in wallets:
        fills = m01.load_wallet_fills(wallet, start_ms, end_ms)
        rows, audit = derive_copy_lifecycles(wallet, fills, args.close_fraction)
        writer.add_many(rows)
        audits.append(audit)
    writer.close()
    audit_output = args.audit_output or args.output.with_suffix(".audit.json")
    audit_output.write_text(json.dumps(audits, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
