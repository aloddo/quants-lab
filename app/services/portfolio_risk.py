"""
Portfolio-level risk management service.

Tracks aggregate exposure across all running strategies, enforces risk limits,
and provides sizing recommendations. Used by:
- cli.py deploy (pre-deploy exposure check)
- watchdog_task (continuous monitoring)
- telegram_bot (exposure commands)

NOT used directly by controllers (they're self-contained in Docker).
Controllers get their sizing config at deploy time.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from pymongo import MongoClient

logger = logging.getLogger(__name__)


@dataclass
class PositionRisk:
    """Risk metrics for a single position."""
    pair: str
    side: str
    size: Decimal
    entry_price: Decimal
    mark_price: Decimal
    leverage: int
    unrealised_pnl: Decimal
    strategy_type: str  # carry, directional, mean_reversion, volatility
    engine: str

    @property
    def notional_value(self) -> Decimal:
        return self.size * self.mark_price

    @property
    def margin_used(self) -> Decimal:
        return self.notional_value / Decimal(str(self.leverage)) if self.leverage > 0 else self.notional_value

    @property
    def pnl_pct(self) -> Decimal:
        if self.entry_price == 0:
            return Decimal("0")
        if self.side == "Buy":
            return (self.mark_price - self.entry_price) / self.entry_price
        else:
            return (self.entry_price - self.mark_price) / self.entry_price


@dataclass
class PortfolioRiskSnapshot:
    """Aggregate portfolio risk at a point in time."""
    timestamp: datetime
    total_capital: Decimal
    positions: List[PositionRisk]

    # Computed fields
    total_exposure: Decimal = Decimal("0")
    total_margin_used: Decimal = Decimal("0")
    total_unrealised_pnl: Decimal = Decimal("0")
    exposure_by_strategy: Dict[str, Decimal] = field(default_factory=dict)
    exposure_by_pair: Dict[str, Decimal] = field(default_factory=dict)
    position_count: int = 0

    def __post_init__(self):
        self.position_count = len(self.positions)
        self.total_exposure = sum(p.notional_value for p in self.positions)
        self.total_margin_used = sum(p.margin_used for p in self.positions)
        self.total_unrealised_pnl = sum(p.unrealised_pnl for p in self.positions)

        for p in self.positions:
            self.exposure_by_strategy[p.strategy_type] = (
                self.exposure_by_strategy.get(p.strategy_type, Decimal("0")) + p.notional_value
            )
            self.exposure_by_pair[p.pair] = (
                self.exposure_by_pair.get(p.pair, Decimal("0")) + p.notional_value
            )

    @property
    def exposure_pct(self) -> Decimal:
        if self.total_capital == 0:
            return Decimal("0")
        return self.total_exposure / self.total_capital

    @property
    def margin_usage_pct(self) -> Decimal:
        if self.total_capital == 0:
            return Decimal("0")
        return self.total_margin_used / self.total_capital

    def check_limits(self, limits: "RiskLimits") -> List[str]:
        """Check all risk limits. Returns list of violations (empty = all OK)."""
        violations = []

        if self.exposure_pct > limits.max_portfolio_exposure_pct:
            violations.append(
                f"Portfolio exposure {self.exposure_pct:.1%} > limit {limits.max_portfolio_exposure_pct:.1%}"
            )

        if self.margin_usage_pct > limits.max_margin_usage_pct:
            violations.append(
                f"Margin usage {self.margin_usage_pct:.1%} > limit {limits.max_margin_usage_pct:.1%}"
            )

        if self.position_count > limits.max_concurrent_positions:
            violations.append(
                f"Position count {self.position_count} > limit {limits.max_concurrent_positions}"
            )

        for pair, exp in self.exposure_by_pair.items():
            pair_pct = exp / self.total_capital if self.total_capital > 0 else Decimal("0")
            if pair_pct > limits.max_single_pair_pct:
                violations.append(
                    f"{pair} exposure {pair_pct:.1%} > single-pair limit {limits.max_single_pair_pct:.1%}"
                )

        for stype, exp in self.exposure_by_strategy.items():
            if stype in limits.max_strategy_exposure_pct:
                strat_pct = exp / self.total_capital if self.total_capital > 0 else Decimal("0")
                limit = limits.max_strategy_exposure_pct[stype]
                if strat_pct > limit:
                    violations.append(
                        f"{stype} exposure {strat_pct:.1%} > strategy limit {limit:.1%}"
                    )

        if self.total_unrealised_pnl < -self.total_capital * limits.max_portfolio_drawdown_pct:
            violations.append(
                f"Unrealised PnL {self.total_unrealised_pnl:.2f} exceeds drawdown limit "
                f"{-self.total_capital * limits.max_portfolio_drawdown_pct:.2f}"
            )

        return violations


@dataclass
class RiskLimits:
    """Portfolio-level risk limits."""
    max_portfolio_exposure_pct: Decimal = Decimal("0.20")  # 20% of AUM
    max_margin_usage_pct: Decimal = Decimal("0.50")  # 50% of capital in margin
    max_concurrent_positions: int = 15
    max_single_pair_pct: Decimal = Decimal("0.05")  # 5% per pair
    max_portfolio_drawdown_pct: Decimal = Decimal("0.10")  # 10% max drawdown
    max_strategy_exposure_pct: Dict[str, Decimal] = field(default_factory=lambda: {
        "carry": Decimal("0.15"),       # Carry can use 15% — large positions, low leverage
        "directional": Decimal("0.10"),  # Directional: 10%
        "mean_reversion": Decimal("0.05"),  # MR: 5% — many small positions
        "volatility": Decimal("0.05"),   # Vol: 5%
    })


class PortfolioRiskService:
    """
    Query exchange positions and compute portfolio risk.
    Uses BybitExchangeClient as source of truth.
    """

    def __init__(
        self,
        exchange_client,  # BybitExchangeClient instance
        mongo_uri: str = "mongodb://localhost:27017",
        mongo_db: str = "quants_lab",
        total_capital: Decimal = Decimal("100000"),
    ):
        self.exchange_client = exchange_client
        self.db = MongoClient(mongo_uri)[mongo_db]
        self.total_capital = total_capital
        self.limits = RiskLimits()

    async def get_snapshot(self) -> PortfolioRiskSnapshot:
        """Get current portfolio risk snapshot from exchange."""
        positions_raw = await self.exchange_client.get_positions()

        positions = []
        for p in positions_raw:
            size = Decimal(str(p.get("size", "0")))
            if size == 0:
                continue

            # Look up which engine/strategy owns this pair
            engine_info = self._get_engine_for_pair(p.get("symbol", ""))

            positions.append(PositionRisk(
                pair=p.get("symbol", ""),
                side=p.get("side", ""),
                size=size,
                entry_price=Decimal(str(p.get("avgPrice", "0"))),
                mark_price=Decimal(str(p.get("markPrice", "0"))),
                leverage=int(p.get("leverage", "1")),
                unrealised_pnl=Decimal(str(p.get("unrealisedPnl", "0"))),
                strategy_type=engine_info.get("strategy_type", "unknown"),
                engine=engine_info.get("engine", "unknown"),
            ))

        return PortfolioRiskSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_capital=self.total_capital,
            positions=positions,
        )

    def _get_engine_for_pair(self, symbol: str) -> dict:
        """Look up which engine is trading this pair from MongoDB."""
        # Check pair_historical for the most recent ALLOW verdict
        record = self.db.pair_historical.find_one(
            {"pair": symbol, "verdict": "ALLOW"},
            sort=[("_id", -1)]
        )
        if record:
            return {
                "engine": record.get("engine", "unknown"),
                "strategy_type": self._engine_to_strategy_type(record.get("engine", "")),
            }
        return {"engine": "untracked", "strategy_type": "unknown"}

    @staticmethod
    def _engine_to_strategy_type(engine: str) -> str:
        """Map engine name to strategy type."""
        mapping = {
            "X1": "mean_reversion",   # Cross-exchange funding divergence
            "X2": "carry",            # True carry
            "X3": "directional",      # IV regime signal
            "X4": "directional",      # OI conviction momentum
            "X5": "directional",      # Liquidation cascade
            "X6": "volatility",       # Fear & Greed overlay
            # Legacy (all retired)
            "E3": "directional",
        }
        return mapping.get(engine, "unknown")

    async def check_risk(self) -> dict:
        """Check current risk state. Returns violations and snapshot."""
        snapshot = await self.get_snapshot()
        violations = snapshot.check_limits(self.limits)

        return {
            "healthy": len(violations) == 0,
            "violations": violations,
            "snapshot": {
                "total_exposure": float(snapshot.total_exposure),
                "exposure_pct": float(snapshot.exposure_pct),
                "margin_usage_pct": float(snapshot.margin_usage_pct),
                "unrealised_pnl": float(snapshot.total_unrealised_pnl),
                "position_count": snapshot.position_count,
                "by_strategy": {k: float(v) for k, v in snapshot.exposure_by_strategy.items()},
                "by_pair": {k: float(v) for k, v in snapshot.exposure_by_pair.items()},
            },
        }

    def can_deploy(
        self,
        engine: str,
        proposed_amount_quote: Decimal,
        proposed_leverage: int,
        current_snapshot: Optional[PortfolioRiskSnapshot] = None,
    ) -> dict:
        """
        Pre-deploy check: would this new position violate risk limits?
        Called by cli.py deploy before launching a bot.
        """
        strategy_type = self._engine_to_strategy_type(engine)

        # Simulate adding the proposed position
        new_notional = proposed_amount_quote * Decimal(str(proposed_leverage))
        new_margin = proposed_amount_quote

        if current_snapshot is None:
            # Can't check without snapshot — pass through with warning
            return {
                "allowed": True,
                "warning": "No snapshot available, skipping risk check",
            }

        # Check exposure limit
        new_total_exposure = current_snapshot.total_exposure + new_notional
        new_exposure_pct = new_total_exposure / self.total_capital
        if new_exposure_pct > self.limits.max_portfolio_exposure_pct:
            return {
                "allowed": False,
                "reason": f"Would breach exposure limit: {new_exposure_pct:.1%} > {self.limits.max_portfolio_exposure_pct:.1%}",
            }

        # Check strategy exposure
        current_strat_exp = current_snapshot.exposure_by_strategy.get(strategy_type, Decimal("0"))
        new_strat_exp = current_strat_exp + new_notional
        strat_limit = self.limits.max_strategy_exposure_pct.get(strategy_type, Decimal("0.10"))
        new_strat_pct = new_strat_exp / self.total_capital
        if new_strat_pct > strat_limit:
            return {
                "allowed": False,
                "reason": f"Would breach {strategy_type} limit: {new_strat_pct:.1%} > {strat_limit:.1%}",
            }

        return {"allowed": True}
