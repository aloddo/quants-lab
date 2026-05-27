"""
arb_hlbb configuration — all tunable parameters in one place.
"""
from dataclasses import dataclass, field


@dataclass
class ArbConfig:
    """Configuration for the HL-Bybit perp/perp arb engine."""

    # ── Fees ──────────────────────────────────────────────
    hl_taker_bps: float = 4.32        # HL taker fee (one side)
    bb_taker_bps: float = 5.5         # Bybit taker fee (one side)

    @property
    def fee_rt_bps(self) -> float:
        """Total round-trip fee: 4 taker fills."""
        return 2 * (self.hl_taker_bps + self.bb_taker_bps)  # 19.64 bps

    # ── Thresholds ────────────────────────────────────────
    entry_min_spread_bps: float = 30.0   # Hard floor: never enter below 30bp
    entry_percentile: float = 0.90       # Adaptive: entry when spread > P90
    exit_percentile: float = 0.25        # Adaptive: exit when spread < P25
    min_excess_bps: float = 25.0         # P90 - P25 must exceed this
    threshold_window: int = 720          # Rolling window for percentiles
    min_warmup: int = 120                # Min observations before trading
    min_entry_ticks: int = 2             # Consecutive ticks above threshold
    subsample_rate: int = 4              # Update thresholds every Nth tick
    entry_requote_max_age_s: float = 1.0 # Live entry must reprice from quote newer than this
    min_requote_edge_bps: float = 2.0    # Extra edge required after requote vs fees
    order_aggression_bps: float = 15.0   # Add bps beyond TOB for IOC fills — must be high enough
                                         # to absorb ~1s of HL book movement. 3bp → 17% fill rate.
                                         # 15bp = sweet spot: covers NEAR (0bp move) + DYDX (5-10bp).
                                         # Aggression is a ceiling, not a cost — fills at best available.

    # ── Risk ──────────────────────────────────────────────
    stop_loss_multiple: float = 2.5      # Spread > 2.5x entry = stop
    max_hold_s: float = 300              # Max hold 5 min (median is 10s)
    max_concurrent: int = 3              # Max simultaneous positions
    max_daily_loss_usd: float = 50.0     # Daily loss limit
    max_leg_failures: int = 10           # Consecutive leg failures = pause (raised from 3 for soak test)
    position_usd: float = 100.0          # Per-side position size (USD)
    leverage: int = 3                    # Leverage on both venues
    reentry_cooldown_s: float = 30.0            # Per-pair cooldown after exit (prevents churn)
    reentry_cooldown_after_fail_s: float = 120.0  # Longer cooldown after stop-loss or leg failure
    qty_mismatch_tolerance_pct: float = 0.001  # Trim if fills differ by >0.1%
    recovery_qty_tolerance_pct: float = 0.05   # Crash recovery position match tolerance
    use_exchange_equity_risk: bool = True      # Live risk from venue balances, not model PnL
    bybit_fill_poll_attempts: int = 8          # Avoid premature NOT_FILLED after IOC submit
    bybit_fill_poll_delay_s: float = 0.15

    # ── Pairs ─────────────────────────────────────────────
    default_pairs: list[str] = field(default_factory=lambda: [
        "CHIP-USDT", "APE-USDT", "AXS-USDT", "FARTCOIN-USDT", "HYPE-USDT",
        "PENGU-USDT", "ORDI-USDT", "IP-USDT", "OP-USDT", "DYDX-USDT",
        "AAVE-USDT", "NEAR-USDT", "SUI-USDT", "ARB-USDT", "WLD-USDT",
        "DOT-USDT", "LINK-USDT", "UNI-USDT", "ADA-USDT", "SOL-USDT",
    ])

    # ── MongoDB ───────────────────────────────────────────
    mongo_uri: str = "mongodb://localhost:27017/quants_lab"
    positions_collection: str = "arb_hlbb_positions"
    trades_collection: str = "arb_hlbb_trades"
    snapshots_collection: str = "arb_hl_bybit_perp_snapshots"

    # ── WS URLs ───────────────────────────────────────────
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    hl_rest_url: str = "https://api.hyperliquid.xyz"
    bb_ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    bb_rest_url: str = "https://api.bybit.com"

    # ── Telegram ──────────────────────────────────────────
    telegram_enabled: bool = True
    telegram_chat_id: str = ""  # Set from TELEGRAM_CHAT_ID env var at init
