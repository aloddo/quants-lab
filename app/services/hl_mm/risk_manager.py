"""
Risk Manager — Portfolio-level risk controls (Spec Section 6).

Portfolio rules ($54 HL, $480 Bybit):
  - Max 2 live pairs simultaneously
  - Max gross inventory: $150 notional
  - Max beta-adjusted net exposure: $70
  - Max gross resting order notional: $150 (across all pairs, since corrected from spec)

Stops:
  - Daily strategy stop: -$3 combined realized + unrealized
  - Hard stop: -$5 and full shutdown for UTC day

Correlation stop:
  - BTC/ETH > 1.5% in 5m: halve all sizes
  - BTC/ETH > 2.5% in 5m: pause all alt pairs 15m

Gap risk:
  - HL book freshness > 1.5s: cancel all quotes
  - HL freshness > 3s with inventory: hedge immediately
  - No order ack/cancel in 3s: assume live, full sync

Stale data:
  - HL stale > 1.5s: no quoting
  - Bybit stale > 500ms: reduced anchor, half size
  - Bybit stale > 2s: no new hedges except emergency
"""
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class RiskAction(Enum):
    """Actions the risk manager can mandate."""
    NONE = "none"
    HALVE_SIZES = "halve_sizes"             # correlation stop level 1
    PAUSE_ALL = "pause_all"                 # correlation stop level 2
    CANCEL_ALL_QUOTES = "cancel_quotes"     # stale data
    HEDGE_IMMEDIATELY = "hedge_immediately" # stale + inventory
    DAILY_STOP = "daily_stop"               # -$3 loss
    HARD_STOP = "hard_stop"                 # -$5 loss, shutdown
    FULL_SYNC = "full_sync"                 # OMS mismatch


@dataclass
class RiskConfig:
    """Risk parameters from spec."""
    max_live_pairs: int = 2
    max_gross_notional: float = 150.0      # max gross inventory
    max_net_exposure: float = 70.0         # max beta-adjusted net
    max_gross_resting: float = 150.0       # max notional in resting orders

    daily_stop_usd: float = 3.0            # -$3 combined
    hard_stop_usd: float = 5.0             # -$5 full shutdown

    # Correlation stop thresholds (5-minute returns)
    corr_halve_pct: float = 1.5            # BTC/ETH > 1.5%: halve
    corr_pause_pct: float = 2.5            # BTC/ETH > 2.5%: pause 15m
    corr_window_s: float = 300.0           # 5 minutes
    corr_pause_duration_s: float = 900.0   # 15 minutes

    # Gap risk
    hl_stale_cancel_ms: float = 1500.0     # > 1.5s: cancel quotes
    hl_stale_hedge_ms: float = 3000.0      # > 3s + inventory: hedge
    oms_timeout_ms: float = 3000.0         # no ack/cancel in 3s: full sync

    # Funding avoidance
    funding_avoidance_before_s: float = 180.0  # 3 min before settlement
    funding_avoidance_after_s: float = 120.0   # 2 min after settlement


@dataclass
class RiskState:
    """Current risk state."""
    daily_pnl: float = 0.0
    gross_notional: float = 0.0
    net_exposure: float = 0.0
    live_pair_count: int = 0
    is_daily_stopped: bool = False
    is_hard_stopped: bool = False
    halve_sizes: bool = False
    pause_all_until: float = 0.0
    actions: list = field(default_factory=list)
    reason: str = ""


class RiskManager:
    """Portfolio-level risk management."""

    def __init__(self, config: Optional[RiskConfig] = None):
        self.config = config or RiskConfig()

        # BTC/ETH price history for correlation stop
        self._btc_prices: deque = deque(maxlen=600)  # 5min at 2/sec
        self._eth_prices: deque = deque(maxlen=600)

        # State
        self._is_daily_stopped = False
        self._is_hard_stopped = False
        self._halve_until: float = 0.0
        self._pause_until: float = 0.0
        self._stopped_at: float = 0.0  # UTC day when we stopped

    def update_reference_prices(self, btc_mid: float, eth_mid: float) -> None:
        """Feed BTC/ETH prices for correlation stop. Call every tick."""
        now = time.time()
        if btc_mid > 0:
            self._btc_prices.append((now, btc_mid))
        if eth_mid > 0:
            self._eth_prices.append((now, eth_mid))

    def evaluate(
        self,
        daily_pnl: float,
        gross_notional: float,
        net_exposure: float,
        live_pair_count: int,
        hl_book_ages_ms: dict[str, float],
        has_inventory: dict[str, bool],
    ) -> RiskState:
        """Run all risk checks. Returns the current risk state + any actions.

        Args:
            daily_pnl: combined realized + unrealized across all pairs
            gross_notional: sum of abs(notional) across all pairs
            net_exposure: signed sum of notional
            live_pair_count: number of pairs currently quoting
            hl_book_ages_ms: per-coin HL book age in milliseconds
            has_inventory: per-coin whether we hold a position
        """
        now = time.time()
        cfg = self.config
        actions = []

        # Check if new UTC day -> reset daily stop
        if self._is_daily_stopped or self._is_hard_stopped:
            current_utc_day = datetime.now(timezone.utc).date()
            stopped_day = datetime.fromtimestamp(
                self._stopped_at, tz=timezone.utc
            ).date() if self._stopped_at > 0 else None
            if stopped_day and current_utc_day > stopped_day:
                self._is_daily_stopped = False
                self._is_hard_stopped = False
                logger.info("New UTC day: risk stops reset")

        # Hard stop: -$5
        if daily_pnl <= -cfg.hard_stop_usd:
            self._is_hard_stopped = True
            self._stopped_at = now
            actions.append(RiskAction.HARD_STOP)
            return RiskState(
                daily_pnl=daily_pnl, gross_notional=gross_notional,
                net_exposure=net_exposure, live_pair_count=live_pair_count,
                is_hard_stopped=True, actions=actions,
                reason=f"HARD STOP: PnL ${daily_pnl:.2f} <= -${cfg.hard_stop_usd}",
            )

        # Daily stop: -$3
        if daily_pnl <= -cfg.daily_stop_usd:
            self._is_daily_stopped = True
            self._stopped_at = now
            actions.append(RiskAction.DAILY_STOP)
            return RiskState(
                daily_pnl=daily_pnl, gross_notional=gross_notional,
                net_exposure=net_exposure, live_pair_count=live_pair_count,
                is_daily_stopped=True, actions=actions,
                reason=f"DAILY STOP: PnL ${daily_pnl:.2f} <= -${cfg.daily_stop_usd}",
            )

        if self._is_daily_stopped or self._is_hard_stopped:
            return RiskState(
                daily_pnl=daily_pnl, gross_notional=gross_notional,
                net_exposure=net_exposure, live_pair_count=live_pair_count,
                is_daily_stopped=self._is_daily_stopped,
                is_hard_stopped=self._is_hard_stopped,
                actions=actions, reason="stopped for UTC day",
            )

        # Correlation stop
        corr_action = self._check_correlation_stop(now)
        if corr_action:
            actions.append(corr_action)

        # Gap risk: HL staleness
        for coin, age_ms in hl_book_ages_ms.items():
            if age_ms > cfg.hl_stale_hedge_ms and has_inventory.get(coin, False):
                actions.append(RiskAction.HEDGE_IMMEDIATELY)
                break
            elif age_ms > cfg.hl_stale_cancel_ms:
                if RiskAction.CANCEL_ALL_QUOTES not in actions:
                    actions.append(RiskAction.CANCEL_ALL_QUOTES)

        # Portfolio limits
        halve = now < self._halve_until
        pause = now < self._pause_until

        return RiskState(
            daily_pnl=daily_pnl,
            gross_notional=gross_notional,
            net_exposure=net_exposure,
            live_pair_count=live_pair_count,
            is_daily_stopped=self._is_daily_stopped,
            is_hard_stopped=self._is_hard_stopped,
            halve_sizes=halve or RiskAction.HALVE_SIZES in actions,
            pause_all_until=self._pause_until if pause else 0.0,
            actions=actions,
            reason=self._summarize_actions(actions),
        )

    def is_funding_avoidance_window(self) -> bool:
        """Check if we are near an HL funding settlement (every hour).

        HL funding is hourly. From T-180s to T+120s around settlement:
        exit-only mode, no new same-direction entries.
        """
        now = datetime.now(timezone.utc)
        seconds_in_hour = now.minute * 60 + now.second
        cfg = self.config

        # Near the top of the hour
        if seconds_in_hour >= (3600 - cfg.funding_avoidance_before_s):
            return True
        if seconds_in_hour <= cfg.funding_avoidance_after_s:
            return True
        return False

    def can_add_pair(self, current_live: int) -> bool:
        """Check if we can add another live pair."""
        return current_live < self.config.max_live_pairs

    def check_notional_limit(self, current_gross: float, new_order_notional: float) -> bool:
        """Check if a new order would exceed gross notional limit."""
        return (current_gross + new_order_notional) <= self.config.max_gross_notional

    def check_net_exposure(self, current_net: float, new_order_exposure: float) -> bool:
        """Check if new exposure would exceed net limit."""
        return abs(current_net + new_order_exposure) <= self.config.max_net_exposure

    # ------------------------------------------------------------------
    # Correlation stop
    # ------------------------------------------------------------------

    def _check_correlation_stop(self, now: float) -> Optional[RiskAction]:
        """Check BTC/ETH 5-minute returns for correlation stop."""
        cfg = self.config
        window_start = now - cfg.corr_window_s

        for name, prices in [("BTC", self._btc_prices), ("ETH", self._eth_prices)]:
            if len(prices) < 10:
                continue

            # Find price at window start
            old_prices = [(t, p) for t, p in prices if t <= window_start + 5]
            if not old_prices:
                continue

            old_price = old_prices[-1][1]
            current_price = prices[-1][1]

            if old_price <= 0:
                continue

            ret_pct = abs(current_price - old_price) / old_price * 100

            if ret_pct >= cfg.corr_pause_pct:
                self._pause_until = now + cfg.corr_pause_duration_s
                logger.warning(
                    f"CORRELATION STOP L2: {name} moved {ret_pct:.2f}% in 5m, "
                    f"pausing all pairs for {cfg.corr_pause_duration_s/60:.0f}m"
                )
                return RiskAction.PAUSE_ALL

            if ret_pct >= cfg.corr_halve_pct:
                self._halve_until = now + 60.0  # halve for 60s, re-evaluate
                logger.info(
                    f"CORRELATION STOP L1: {name} moved {ret_pct:.2f}% in 5m, "
                    f"halving sizes"
                )
                return RiskAction.HALVE_SIZES

        return None

    @staticmethod
    def _summarize_actions(actions: list[RiskAction]) -> str:
        if not actions:
            return "all clear"
        return ", ".join(a.value for a in actions)
