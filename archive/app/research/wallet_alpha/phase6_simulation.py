#!/usr/bin/env python3
"""
Phase 6: Copy Simulation

Realistic copy trading simulation using only information available at decision time.
Simulates the full lifecycle: detect wallet fill -> wait 1s -> place taker order ->
manage position -> exit.

Key assumptions modeled:
- 1s detection + execution latency
- Taker fees (4.32 bps each side = 8.64 bps RT)
- L2 book slippage at entry time
- Position sizing at $20-50 per trade
- Concurrent position limits
- Missed fills (size unavailable)

Usage:
    python -m app.research.wallet_alpha.phase6_simulation
"""
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [sim] %(levelname)s: %(message)s",
)
logger = logging.getLogger("sim")

OUTPUT_DIR = Path("app/data/wallet_alpha")
EVENTS_PATH = OUTPUT_DIR / "events_with_markout.parquet"
SCORED_PATH = OUTPUT_DIR / "wallet_scored.parquet"
SIM_RESULTS_PATH = OUTPUT_DIR / "simulation_results.parquet"
SIM_SUMMARY_PATH = OUTPUT_DIR / "simulation_summary.csv"

# Simulation parameters
LATENCY_MS = 1000  # 1s detection delay
FEE_PER_SIDE_BPS = 4.32
SLIPPAGE_BPS = 1.0  # Conservative additional slippage
POSITION_SIZE_USD = 30.0  # $30 per position
MAX_CONCURRENT_POSITIONS = 15
INITIAL_EQUITY = 540.0  # Starting capital


@dataclass
class Position:
    """An open position in the simulation."""
    wallet: str
    coin: str
    side: str  # "Buy" or "Sell"
    entry_price: float
    size: float
    notional: float
    entry_ts: int
    entry_fee_usd: float
    event_id: int


@dataclass
class SimState:
    """Simulation state tracker."""
    equity: float = INITIAL_EQUITY
    positions: list = field(default_factory=list)
    trades: list = field(default_factory=list)
    n_signals: int = 0
    n_filled: int = 0
    n_missed_capacity: int = 0
    n_missed_size: int = 0


def simulate_copy_portfolio(
    events: pd.DataFrame,
    target_wallets: set[str],
    mid_prices: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Run copy simulation on target wallets.

    Only copies OPEN events (not closes). Uses mid_at_1s as entry price
    plus slippage and fees.

    Returns:
    - trades DataFrame with entry/exit details
    - summary dict with aggregate stats
    """
    state = SimState()

    # Filter to target wallet OPEN events, sorted by time
    # Filter to target wallet OPEN events with ANY mid-price (L2 or fill-derived)
    has_mid = events["mid_at_1s"].notna() if "mid_at_1s" in events.columns else events.get("has_l2", True)
    target_events = events[
        (events["wallet"].isin(target_wallets)) &
        (events["event_type"] == "OPEN") &
        (has_mid)
    ].sort_values("start_ts").reset_index(drop=True)

    logger.info(f"Simulating {len(target_events):,} open events from {len(target_wallets)} wallets")

    for _, event in target_events.iterrows():
        state.n_signals += 1

        # Check capacity
        if len(state.positions) >= MAX_CONCURRENT_POSITIONS:
            state.n_missed_capacity += 1
            continue

        # Check if we already have a position in this coin
        existing = [p for p in state.positions if p.coin == event["coin"]]
        if existing:
            continue  # Skip, already positioned

        # Entry price: mid at 1s (our detection point) + slippage
        entry_mid = event.get("mid_at_1s", np.nan)
        if pd.isna(entry_mid) or entry_mid <= 0:
            state.n_missed_size += 1
            continue

        # Add slippage
        if event["side"] == "Buy":
            entry_price = entry_mid * (1 + SLIPPAGE_BPS / 10000)
        else:
            entry_price = entry_mid * (1 - SLIPPAGE_BPS / 10000)

        # Position sizing
        size = POSITION_SIZE_USD / entry_price
        notional = size * entry_price
        entry_fee = notional * FEE_PER_SIDE_BPS / 10000

        # Check sufficient equity
        if state.equity - sum(p.notional for p in state.positions) < notional:
            state.n_missed_capacity += 1
            continue

        pos = Position(
            wallet=event["wallet"],
            coin=event["coin"],
            side=event["side"],
            entry_price=entry_price,
            size=size,
            notional=notional,
            entry_ts=event["start_ts"] + LATENCY_MS,
            entry_fee_usd=entry_fee,
            event_id=event["event_id"],
        )
        state.positions.append(pos)
        state.n_filled += 1

    # Build lookup: for each open position, find matching CLOSE event from same wallet+coin
    close_events = events[
        (events["wallet"].isin(target_wallets)) &
        (events["event_type"] == "CLOSE")
    ].copy()
    # Index by (wallet, coin) for fast lookup
    close_lookup = {}
    for _, ce in close_events.iterrows():
        key = (ce["wallet"], ce["coin"])
        if key not in close_lookup:
            close_lookup[key] = []
        close_lookup[key].append(ce)

    # Close positions: match with wallet's CLOSE event, or use markout for exit price
    for pos in state.positions:
        exit_price = pos.entry_price  # Fallback
        exit_ts = pos.entry_ts
        status = "open"

        # Try to find a matching close event after our entry
        key = (pos.wallet, pos.coin)
        if key in close_lookup:
            matching_closes = [
                c for c in close_lookup[key] if c["start_ts"] > pos.entry_ts - LATENCY_MS
            ]
            if matching_closes:
                close_event = min(matching_closes, key=lambda c: c["start_ts"])
                close_mid = close_event.get("mid_at_1s", np.nan)
                if not pd.isna(close_mid) and close_mid > 0:
                    if pos.side == "Buy":
                        exit_price = close_mid * (1 - SLIPPAGE_BPS / 10000)
                    else:
                        exit_price = close_mid * (1 + SLIPPAGE_BPS / 10000)
                    exit_ts = close_event["start_ts"] + LATENCY_MS
                    status = "closed"

        # If no close event found, use best markout estimate
        if status == "open":
            # Use the 3600s markout as exit estimate
            open_event_row = target_events[target_events["event_id"] == pos.event_id]
            if len(open_event_row) > 0:
                mo_3600 = open_event_row.iloc[0].get("copy_mo_3600s", np.nan)
                if not pd.isna(mo_3600):
                    if pos.side == "Buy":
                        exit_price = pos.entry_price * (1 + mo_3600 / 10000)
                    else:
                        exit_price = pos.entry_price * (1 - mo_3600 / 10000)
                    exit_ts = pos.entry_ts + 3600_000
                    status = "markout_exit"

        exit_fee = pos.notional * FEE_PER_SIDE_BPS / 10000

        if pos.side == "Buy":
            pnl = (exit_price - pos.entry_price) * pos.size - pos.entry_fee_usd - exit_fee
        else:
            pnl = (pos.entry_price - exit_price) * pos.size - pos.entry_fee_usd - exit_fee

        state.trades.append({
            "wallet": pos.wallet,
            "coin": pos.coin,
            "side": pos.side,
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "size": pos.size,
            "notional": pos.notional,
            "entry_ts": pos.entry_ts,
            "exit_ts": exit_ts,
            "pnl_usd": pnl,
            "pnl_bps": pnl / pos.notional * 10000,
            "entry_fee": pos.entry_fee_usd,
            "exit_fee": exit_fee,
            "status": status,
        })

    trades_df = pd.DataFrame(state.trades) if state.trades else pd.DataFrame()

    summary = {
        "n_wallets": len(target_wallets),
        "n_signals": state.n_signals,
        "n_filled": state.n_filled,
        "n_missed_capacity": state.n_missed_capacity,
        "n_missed_size": state.n_missed_size,
        "fill_rate": state.n_filled / max(1, state.n_signals),
        "n_trades": len(state.trades),
    }

    if len(trades_df) > 0:
        closed = trades_df[trades_df["status"] == "closed"]
        if len(closed) > 0:
            summary["total_pnl_usd"] = closed["pnl_usd"].sum()
            summary["total_fees_usd"] = closed["entry_fee"].sum() + closed["exit_fee"].sum()
            summary["avg_pnl_bps"] = closed["pnl_bps"].mean()
            summary["win_rate"] = (closed["pnl_usd"] > 0).mean()
            summary["sharpe"] = (
                closed["pnl_usd"].mean() / closed["pnl_usd"].std() * np.sqrt(252)
                if closed["pnl_usd"].std() > 0 else 0
            )
            # Max drawdown
            cum_pnl = closed["pnl_usd"].cumsum()
            dd = cum_pnl - cum_pnl.cummax()
            summary["max_drawdown_usd"] = dd.min()

    return trades_df, summary


def main():
    t0 = time.time()

    # Load scored wallets
    if not SCORED_PATH.exists():
        logger.error("Scored wallets not found. Run phase4 first.")
        return

    scored = pd.read_parquet(SCORED_PATH)
    logger.info(f"Loaded {len(scored):,} scored wallets")

    # Select top N wallets
    top_n = min(10, len(scored))
    target_wallets = set(scored.nlargest(top_n, "composite_score")["wallet"].values)
    logger.info(f"Selected top {top_n} wallets for simulation")

    # Load events (from daily markout files, filtered to target wallets)
    MARKOUT_DIR = OUTPUT_DIR / "events_markout_daily"
    if EVENTS_PATH.exists():
        events = pd.read_parquet(EVENTS_PATH)
        events = events[events["wallet"].isin(target_wallets)]
        logger.info(f"Loaded {len(events):,} events from single file")
    elif MARKOUT_DIR.exists():
        markout_files = sorted(MARKOUT_DIR.glob("*.parquet"))
        if not markout_files:
            logger.error("No markout event files found. Run phase3 first.")
            return
        chunks = []
        for mf in markout_files:
            df = pd.read_parquet(mf)
            df = df[df["wallet"].isin(target_wallets)]
            if len(df) > 0:
                chunks.append(df)
        events = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        del chunks
        logger.info(f"Loaded {len(events):,} events from {len(markout_files)} daily files")
    else:
        logger.error("Events file not found. Run phase3 first.")
        return

    # Run simulation
    trades_df, summary = simulate_copy_portfolio(events, target_wallets)

    logger.info(f"\nSimulation Results:")
    for k, v in summary.items():
        if isinstance(v, float):
            logger.info(f"  {k}: {v:.4f}")
        else:
            logger.info(f"  {k}: {v}")

    # Save results
    if len(trades_df) > 0:
        trades_df.to_parquet(SIM_RESULTS_PATH, index=False)
    pd.DataFrame([summary]).to_csv(SIM_SUMMARY_PATH, index=False)

    elapsed = time.time() - t0
    logger.info(f"\nPhase 6 complete in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
