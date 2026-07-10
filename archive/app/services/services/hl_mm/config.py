"""
HL MM Configuration -- single source of truth for all tunable parameters.

Loads from config/hl_mm_config.yml with hardcoded defaults as fallback.
All modules import from here instead of scattering magic numbers.
"""
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-pair config
# ---------------------------------------------------------------------------

@dataclass
class PairConfig:
    """Per-pair tunable parameters."""
    tox_buffer_bps: float = 1.0
    gamma_target_shift_bps: float = 1.5
    q_soft: float = 60.0
    q_hard: float = 80.0
    q_emergency: float = 100.0
    child_size_notional: float = 50.0


# Default per-pair configs (from spec)
DEFAULT_PAIR_CONFIGS: dict[str, PairConfig] = {
    "ORDI": PairConfig(tox_buffer_bps=0.8, gamma_target_shift_bps=1.5, q_soft=60, q_hard=80, q_emergency=100, child_size_notional=50),
    "BIO": PairConfig(tox_buffer_bps=0.9, gamma_target_shift_bps=1.5, q_soft=60, q_hard=80, q_emergency=100, child_size_notional=50),
    "DASH": PairConfig(tox_buffer_bps=1.2, gamma_target_shift_bps=2.0, q_soft=50, q_hard=65, q_emergency=80, child_size_notional=40),
    "AXS": PairConfig(tox_buffer_bps=1.1, gamma_target_shift_bps=2.0, q_soft=50, q_hard=65, q_emergency=80, child_size_notional=40),
    "PNUT": PairConfig(tox_buffer_bps=1.4, gamma_target_shift_bps=2.0, q_soft=50, q_hard=65, q_emergency=80, child_size_notional=40),
    "APE": PairConfig(tox_buffer_bps=1.0, gamma_target_shift_bps=1.0, q_soft=50, q_hard=65, q_emergency=80, child_size_notional=50),
    "PENDLE": PairConfig(tox_buffer_bps=1.0, gamma_target_shift_bps=1.0, q_soft=40, q_hard=50, q_emergency=65, child_size_notional=40),
    "MEGA": PairConfig(tox_buffer_bps=1.2, gamma_target_shift_bps=1.5, q_soft=50, q_hard=65, q_emergency=80, child_size_notional=40),
}


# ---------------------------------------------------------------------------
# Fee config
# ---------------------------------------------------------------------------

@dataclass
class FeeConfig:
    """Fee parameters across venues."""
    hl_maker_fee_bps: float = 1.44
    hl_taker_fee_bps: float = 3.50
    bybit_maker_fee_bps: float = 1.00
    bybit_taker_fee_bps: float = 5.50


# ---------------------------------------------------------------------------
# Timing config
# ---------------------------------------------------------------------------

@dataclass
class TimingConfig:
    """Timing intervals."""
    requote_min_interval_s: float = 1.2
    hard_refresh_interval_s: float = 5.0
    screener_interval_s: float = 900.0       # 15 minutes
    reconcile_interval_s: float = 30.0
    fill_poll_interval_s: float = 30.0       # REST fill fallback
    mongo_flush_interval_s: float = 30.0
    bybit_ws_ping_interval_s: float = 20.0
    bybit_ws_reconnect_base_s: float = 1.0
    bybit_ws_reconnect_max_s: float = 30.0


# ---------------------------------------------------------------------------
# Risk config
# ---------------------------------------------------------------------------

@dataclass
class GlobalRiskConfig:
    """Global risk limits.

    Bug #4 (Codex R4): Updated to match spec for $54 capital:
    max_gross=20, max_net=14, max_resting=40.
    """
    max_live_pairs: int = 2
    max_gross_notional: float = 20.0       # was 150, spec says $20 for $54 capital
    max_net_exposure: float = 14.0         # was 70, spec says $14
    max_resting_notional: float = 40.0     # new: max total resting order notional
    daily_stop_usd: float = 3.0
    hard_stop_usd: float = 5.0


# ---------------------------------------------------------------------------
# Hedge config
# ---------------------------------------------------------------------------

@dataclass
class HedgeConfig:
    """Bybit hedge execution parameters."""
    direct_slippage_budget_bps: float = 6.5
    proxy_slippage_budget_bps: float = 8.5
    hedge_pct_min: float = 0.80          # hedge 80-100% of delta
    hedge_pct_max: float = 1.00


# ---------------------------------------------------------------------------
# Telegram notification config
# ---------------------------------------------------------------------------

@dataclass
class TelegramConfig:
    """Telegram notification parameters."""
    min_message_interval_s: float = 3.0   # rate limit: 1 msg per 3s
    daily_summary_hour_utc: int = 0       # 00:00 UTC
    enabled: bool = True


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

@dataclass
class HLMMConfig:
    """Top-level HL MM engine configuration."""
    pair_configs: dict[str, PairConfig] = field(default_factory=lambda: dict(DEFAULT_PAIR_CONFIGS))
    default_pair_config: PairConfig = field(default_factory=PairConfig)
    fees: FeeConfig = field(default_factory=FeeConfig)
    timing: TimingConfig = field(default_factory=TimingConfig)
    risk: GlobalRiskConfig = field(default_factory=GlobalRiskConfig)
    hedge: HedgeConfig = field(default_factory=HedgeConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)

    def get_pair_config(self, coin: str) -> PairConfig:
        """Get per-pair config, falling back to defaults."""
        return self.pair_configs.get(coin, self.default_pair_config)


def load_config(yaml_path: Optional[str] = None) -> HLMMConfig:
    """Load config from YAML file with hardcoded defaults as fallback.

    Args:
        yaml_path: path to YAML config file. If None, tries
                   config/hl_mm_config.yml relative to repo root.

    Returns:
        HLMMConfig with values from YAML overlaying defaults.
    """
    config = HLMMConfig()

    if yaml_path is None:
        # Try default location
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        yaml_path = os.path.join(repo_root, "config", "hl_mm_config.yml")

    if not os.path.exists(yaml_path):
        logger.info(f"No config file at {yaml_path}, using hardcoded defaults")
        return config

    try:
        import yaml
        with open(yaml_path) as f:
            raw = yaml.safe_load(f)

        if not raw or not isinstance(raw, dict):
            logger.warning(f"Empty or invalid YAML in {yaml_path}, using defaults")
            return config

        # Parse fees
        if "fees" in raw:
            for k, v in raw["fees"].items():
                if hasattr(config.fees, k):
                    setattr(config.fees, k, float(v))

        # Parse timing
        if "timing" in raw:
            for k, v in raw["timing"].items():
                if hasattr(config.timing, k):
                    setattr(config.timing, k, float(v))

        # Parse risk
        if "risk" in raw:
            for k, v in raw["risk"].items():
                if hasattr(config.risk, k):
                    if isinstance(v, int) and k in ("max_live_pairs",):
                        setattr(config.risk, k, int(v))
                    else:
                        setattr(config.risk, k, float(v))

        # Parse hedge
        if "hedge" in raw:
            for k, v in raw["hedge"].items():
                if hasattr(config.hedge, k):
                    setattr(config.hedge, k, float(v))

        # Parse telegram
        if "telegram" in raw:
            for k, v in raw["telegram"].items():
                if hasattr(config.telegram, k):
                    if k == "enabled":
                        setattr(config.telegram, k, bool(v))
                    elif k in ("daily_summary_hour_utc",):
                        setattr(config.telegram, k, int(v))
                    else:
                        setattr(config.telegram, k, float(v))

        # Parse per-pair configs
        if "pairs" in raw:
            for coin, pair_raw in raw["pairs"].items():
                base = config.pair_configs.get(coin, PairConfig())
                for k, v in pair_raw.items():
                    if hasattr(base, k):
                        setattr(base, k, float(v))
                config.pair_configs[coin] = base

        logger.info(f"Loaded HL MM config from {yaml_path}")

    except ImportError:
        logger.warning("PyYAML not installed, using hardcoded defaults")
    except Exception as e:
        logger.warning(f"Failed to load config from {yaml_path}: {e}, using defaults")

    return config
