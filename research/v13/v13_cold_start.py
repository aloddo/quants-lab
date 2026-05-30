#!/usr/bin/env python3
"""V13 Module 06 — Cold-Start State Machine.

Per spec: projects/quant/v13/modules/06-cold-start

When a wallet enters the V13 selected pool with an already-open position in coin X,
our copy bot does NOT enter. Wait until source's position in X reaches net flat,
THEN allow copy. Symmetric in backtest + live.

Used by:
- Module 04 (ranking sim) — per-wallet
- Module 08 (portfolio sim) — per-pool
- Live engine — persistent state via state file

State transitions per (wallet, coin):
    PENDING_FLAT → ALLOWED  (source size hits 0)
    ALLOWED      → ALLOWED  (terminal; no re-entry to PENDING)
"""
from __future__ import annotations

from dataclasses import dataclass, field

EPS = 1e-9

# State constants
PENDING_FLAT = "PENDING_FLAT"
ALLOWED = "ALLOWED"


@dataclass
class ColdStartState:
    """Per-wallet cold-start state. Coin → state string.

    Default for any coin not in dict is ALLOWED (implicit). Only PENDING_FLAT is
    explicitly tracked (created at pool entry for coins with open positions).
    """
    state: dict[str, str] = field(default_factory=dict)

    def is_allowed(self, coin: str) -> bool:
        """True iff coin is ALLOWED (default for any unseen coin)."""
        return self.state.get(coin, ALLOWED) == ALLOWED

    def initialize_pool_entry(self, source_positions_at_pool_entry: dict[str, float]) -> None:
        """At wallet pool entry: any coin with non-zero source position → PENDING_FLAT.
        Coins with zero/missing position remain implicit ALLOWED.

        codex m06 r1 fix: PRESERVE terminal ALLOWED. If coin already ALLOWED from prior
        cycle, do NOT overwrite back to PENDING.
        """
        for coin, size in source_positions_at_pool_entry.items():
            if self.state.get(coin) == ALLOWED:
                continue  # terminal — never regress
            if abs(size) > EPS:
                self.state[coin] = PENDING_FLAT

    def update_from_poll(self, source_state_at_poll: dict[str, float]) -> int:
        """At each poll: for coins in PENDING_FLAT, check if source is flat → transition.

        codex m06 r1 fix: use `<= EPS` (consistent with init's `> EPS` semantics). Avoids
        coins stuck at exactly EPS.
        """
        transitioned = 0
        for coin in list(self.state):
            if self.state[coin] == PENDING_FLAT:
                source_size = source_state_at_poll.get(coin, 0.0)
                if abs(source_size) <= EPS:
                    self.state[coin] = ALLOWED
                    transitioned += 1
        return transitioned

    def detect_intra_poll_flat_from_fills(self, coin: str, starting_position: float,
                                          fills_in_poll: list[dict]) -> bool:
        """BACKTEST ONLY: walk fills chronologically in (prev_poll_ts, poll_ts] to detect
        if source reached flat at any sub-poll time. If yes, transition to ALLOWED.

        codex m06 r1 fix: also detect SIGN CROSSING. A single fill that flips sign (e.g.,
        +0.5 → -0.5 via a -1.0 fill) necessarily crossed flat. Use prev-vs-new sign change
        as flat-detection signal in addition to magnitude check.

        codex m06 r2 fix: SORT fills_in_poll by time ascending BEFORE walking. Caller-provided
        order may be unsorted; trusting input order missed real intra-poll flats. Drop
        fills missing a 'time' key (defensive).

        Live engine CANNOT do this (no fill stream); poll-only path stays PENDING until
        next-poll-observed-flat. Metric `cold_start_intra_poll_miss_estimate` quantifies the gap.
        """
        if self.state.get(coin) != PENDING_FLAT:
            return False
        # codex m06 r2 fix: sort chronologically by time; drop fills missing time
        sorted_fills = sorted(
            [f for f in fills_in_poll if "time" in f and isinstance(f.get("time"), (int, float))],
            key=lambda f: f["time"],
        )
        running = starting_position
        for f in sorted_fills:
            new_running = running + float(f.get("signed_sz", 0))
            # Flat (within EPS)
            if abs(new_running) <= EPS:
                self.state[coin] = ALLOWED
                return True
            # Sign crossing (must have crossed flat)
            if abs(running) > EPS and abs(new_running) > EPS and (running > 0) != (new_running > 0):
                self.state[coin] = ALLOWED
                return True
            running = new_running
        return False
