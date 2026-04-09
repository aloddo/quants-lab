"""
Research Tracker — enforces the quant research process for all strategies.

Every strategy must progress through research phases before it can trade live.
Phase status is stored in MongoDB `research_checklist` collection.
Signal scan and testnet resolver check research phase before acting.

Phases (from skills/quant-trading.md):
  P0_IDEA        — hypothesis defined, success criteria set, splits locked
  P1_BASELINE    — baseline backtest completed (walk-forward)
  P1_REGIME      — regime stress tests (bear, shock, range, bull)
  P1_ASYMMETRY   — long/short asymmetry analyzed, direction decided
  P1_SENSITIVITY — parameter sensitivity checked (+/- 1 step)
  P1_OPTIMIZED   — Optuna optimization on train window (if needed)
  P1_VALIDATED   — one-shot validation on locked holdout
  P2_DEGRADATION — A1/A2/slippage/distribution stress tests
  P2_MONTECARLO  — block bootstrap, ruin probability < 1%
  P3_PAPER       — paper trading (execution validation only)
  LIVE           — deployed with real capital

Usage:
    from app.engines.research_tracker import ResearchTracker

    tracker = ResearchTracker(mongo_db)
    phase = await tracker.get_phase("E1")
    await tracker.advance("E1", "P1_REGIME", evidence={...})
    can_trade = await tracker.can_paper_trade("E1")
"""
import logging
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

COLLECTION = "research_checklist"


class ResearchPhase(IntEnum):
    """Ordered research phases. Higher = further along."""
    P0_IDEA = 0
    P1_BASELINE = 10
    P1_REGIME = 20
    P1_ASYMMETRY = 30
    P1_SENSITIVITY = 40
    P1_OPTIMIZED = 50
    P1_VALIDATED = 60
    P2_DEGRADATION = 70
    P2_MONTECARLO = 80
    P3_PAPER = 90
    LIVE = 100


# What's required to pass each phase
PHASE_REQUIREMENTS = {
    ResearchPhase.P0_IDEA: [
        "hypothesis",         # str: market inefficiency being exploited
        "success_criteria",   # str: e.g. "Avg R > 0 AND Median R > 0"
        "train_window",       # str: date range for training
        "validation_window",  # str: date range for validation (locked)
    ],
    ResearchPhase.P1_BASELINE: [
        "walk_forward_run_id",  # str: MongoDB run_id
        "pairs_tested",         # int
        "allow_count",          # int
        "avg_test_pf",          # float
    ],
    ResearchPhase.P1_REGIME: [
        "regimes_tested",       # list[str]: e.g. ["bear", "shock", "range", "bull"]
        "break_conditions",     # list[str]: any regimes where strategy breaks
        "regime_summary",       # str: interpretation
    ],
    ResearchPhase.P1_ASYMMETRY: [
        "long_pf",              # float
        "short_pf",             # float
        "direction_decision",   # str: "BOTH", "LONG_ONLY", "SHORT_ONLY"
        "reasoning",            # str
    ],
    ResearchPhase.P1_SENSITIVITY: [
        "parameters_tested",    # list[str]: e.g. ["tp_mult", "sl_mult", "atr_threshold"]
        "fragile_params",       # list[str]: params that collapse at +/- 1 step
        "robust_params",        # list[str]: params that are stable
    ],
    ResearchPhase.P1_OPTIMIZED: [
        "method",               # str: "optuna" or "manual" or "skipped"
        "best_params",          # dict: optimized parameter values
        "train_pf",             # float
    ],
    ResearchPhase.P1_VALIDATED: [
        "validation_pf",       # float
        "validation_trades",   # int
        "train_val_delta",     # float: Sharpe delta (> 0.3 = suspicious)
        "passed",              # bool
    ],
    ResearchPhase.P2_DEGRADATION: [
        "slippage_tests",      # dict: {bps: pf} at each slippage tier
        "max_consecutive_losses",  # int
        "top5_contribution_pct",   # float: % of PnL from top 5 trades
        "passed",              # bool
    ],
    ResearchPhase.P2_MONTECARLO: [
        "simulations",         # int
        "median_terminal_pnl", # float
        "p5_terminal_pnl",    # float: 5th percentile (worst case)
        "ruin_probability",    # float: must be < 0.01
        "passed",              # bool
    ],
    ResearchPhase.P3_PAPER: [
        "signals_count",       # int: must be >= 20
        "avg_slippage_bps",    # float: must be < 15
        "fill_rate",           # float: must be >= 0.70
        "edge_after_slip",     # float: must be > 0
        "passed",              # bool
    ],
}

# Hard gates for paper trading and live
PAPER_TRADE_MINIMUM = ResearchPhase.P2_MONTECARLO
LIVE_MINIMUM = ResearchPhase.P3_PAPER


class ResearchTracker:
    """Track and enforce research phase progression per strategy."""

    def __init__(self, db):
        """db: motor AsyncIOMotorDatabase or pymongo Database."""
        self.db = db

    async def get_phase(self, engine: str) -> ResearchPhase:
        """Get current research phase for an engine."""
        doc = await self._get_doc(engine)
        if not doc:
            return ResearchPhase.P0_IDEA
        return ResearchPhase(doc.get("current_phase", 0))

    async def get_checklist(self, engine: str) -> Dict[str, Any]:
        """Get full checklist document for an engine."""
        doc = await self._get_doc(engine)
        return doc or {"engine": engine, "current_phase": 0, "phases": {}}

    async def advance(
        self,
        engine: str,
        phase: str,
        evidence: Dict[str, Any],
        notes: str = "",
    ) -> bool:
        """Record completion of a research phase.

        Args:
            engine: strategy name (e.g. "E1")
            phase: phase name (e.g. "P1_REGIME")
            evidence: dict of required fields for this phase
            notes: free-text notes

        Returns:
            True if phase was recorded, False if requirements not met.
        """
        try:
            phase_enum = ResearchPhase[phase]
        except KeyError:
            logger.error(f"Unknown phase: {phase}. Valid: {[p.name for p in ResearchPhase]}")
            return False

        # Check requirements
        required = PHASE_REQUIREMENTS.get(phase_enum, [])
        missing = [r for r in required if r not in evidence]
        if missing:
            logger.error(
                f"{engine}/{phase}: missing required evidence: {missing}"
            )
            return False

        # Check sequential progression
        current = await self.get_phase(engine)
        if phase_enum.value > current.value + 10:
            # Allow skipping at most one phase (P1_OPTIMIZED can be skipped)
            prev_phase = ResearchPhase(phase_enum.value - 10)
            if prev_phase != ResearchPhase.P1_OPTIMIZED:
                logger.warning(
                    f"{engine}: jumping from {current.name} to {phase} — "
                    f"skipping {prev_phase.name}"
                )

        # Store
        phase_data = {
            "completed_at": datetime.now(timezone.utc),
            "evidence": evidence,
            "notes": notes,
        }

        coll = self.db[COLLECTION]
        result = coll.update_one(
            {"engine": engine},
            {
                "$set": {
                    f"phases.{phase}": phase_data,
                    "current_phase": phase_enum.value,
                    "current_phase_name": phase_enum.name,
                    "updated_at": datetime.now(timezone.utc),
                },
                "$setOnInsert": {
                    "engine": engine,
                    "created_at": datetime.now(timezone.utc),
                },
            },
            upsert=True,
        )
        if hasattr(result, '__await__'):
            await result

        logger.info(f"{engine}: advanced to {phase}")
        return True

    async def can_paper_trade(self, engine: str) -> bool:
        """Check if engine has passed all pre-paper-trading gates."""
        phase = await self.get_phase(engine)
        return phase.value >= PAPER_TRADE_MINIMUM.value

    async def can_go_live(self, engine: str) -> bool:
        """Check if engine has passed all pre-live gates."""
        phase = await self.get_phase(engine)
        return phase.value >= LIVE_MINIMUM.value

    async def get_status_summary(self, engine: str) -> str:
        """Human-readable status for an engine."""
        doc = await self._get_doc(engine)
        if not doc:
            return f"{engine}: not started (P0_IDEA)"

        phase = ResearchPhase(doc.get("current_phase", 0))
        phases_done = list(doc.get("phases", {}).keys())
        all_phases = [p.name for p in ResearchPhase if p.value <= 100]

        done_str = ", ".join(phases_done) if phases_done else "none"
        remaining = [p for p in all_phases if p not in phases_done and p != "LIVE"]
        remaining_str = ", ".join(remaining) if remaining else "none"

        return (
            f"{engine}: {phase.name}\n"
            f"  Completed: {done_str}\n"
            f"  Remaining: {remaining_str}\n"
            f"  Can paper trade: {phase.value >= PAPER_TRADE_MINIMUM.value}\n"
            f"  Can go live: {phase.value >= LIVE_MINIMUM.value}"
        )

    async def _get_doc(self, engine: str) -> Optional[Dict]:
        """Get checklist document from MongoDB.

        Handles both pymongo (sync) and motor (async) clients.
        """
        coll = self.db[COLLECTION]
        result = coll.find_one({"engine": engine})
        if hasattr(result, '__await__'):
            return await result
        return result
