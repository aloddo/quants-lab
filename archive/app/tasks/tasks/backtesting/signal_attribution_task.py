"""
Signal Attribution Task — counterfactual analysis for all candidates.

For every candidate with trigger_fired=True (whether placed or filtered),
loads price data from parquet and simulates the TP/SL/time_limit race to
compute what *would* have happened.

This answers: "Of 500 E1 breakouts filtered by volume floor, what was their
theoretical win rate?" — driving data-informed filter tuning.

Fields written to candidates collection:
    attr_would_have_won       bool
    attr_theoretical_pnl_pct  float  (% from entry)
    attr_theoretical_close    str    ("TP" / "SL" / "TIME_LIMIT" / "STALE_DATA")
    attr_exit_price           float
    attr_computed_at          datetime

Schedule: daily at 01:00 UTC.
"""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

from app.engines.strategy_registry import get_strategy, STRATEGY_REGISTRY

logger = logging.getLogger(__name__)

# How many hours of history to look back for un-attributed candidates
LOOKBACK_HOURS = 48

# Connector name used when building parquet path
DEFAULT_CONNECTOR = "bybit_perpetual"


def _load_candles(connector: str, pair: str, resolution: str) -> Optional[pd.DataFrame]:
    """Load a parquet file and return sorted DataFrame, or None if missing."""
    path = data_paths.candles_dir / f"{connector}|{pair}|{resolution}.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return None
        if "timestamp" not in df.columns:
            return None
        return df.sort_values("timestamp").reset_index(drop=True)
    except Exception as e:
        logger.warning(f"Cannot load parquet {path}: {e}")
        return None


def _simulate_trade(
    df: pd.DataFrame,
    entry_ts_s: float,
    entry_price: float,
    direction: str,
    tp_price: float,
    sl_price: float,
    time_limit_s: int,
) -> Tuple[str, float]:
    """
    Walk forward from entry_ts_s, checking each candle's high/low.

    Returns (close_type, exit_price).
    close_type: "TP" | "SL" | "TIME_LIMIT" | "STALE_DATA"
    """
    # Find first candle at or after entry
    mask = df["timestamp"] >= entry_ts_s
    if not mask.any():
        return "STALE_DATA", entry_price

    future = df[mask].copy()
    time_limit_end = entry_ts_s + time_limit_s

    for _, row in future.iterrows():
        candle_ts = float(row["timestamp"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if candle_ts > time_limit_end:
            return "TIME_LIMIT", close

        if direction == "LONG":
            if high >= tp_price:
                return "TP", tp_price
            if low <= sl_price:
                return "SL", sl_price
        else:  # SHORT
            if low <= tp_price:
                return "TP", tp_price
            if high >= sl_price:
                return "SL", sl_price

    # Ran out of candles before time limit
    last_close = float(future["close"].iloc[-1])
    return "STALE_DATA", last_close


def _resolve_exit_prices(
    doc: dict,
    engine: str,
) -> Tuple[Optional[float], Optional[float], int]:
    """
    Resolve TP/SL prices and time limit from candidate doc or registry.

    Returns (tp_price, sl_price, time_limit_seconds).
    Candidate-level tp_price/sl_price take precedence (dynamic ATR-based).
    Falls back to registry percentage-based exits.
    """
    entry = doc.get("decision_price")
    if entry is None:
        return None, None, 86400

    entry = float(entry)
    direction = doc.get("direction", "LONG")
    time_limit_s = 86400

    try:
        meta = get_strategy(engine)
        ep = meta.exit_params
        time_limit_s = int(ep.get("time_limit", 86400))

        tp_candidate = doc.get("tp_price")
        sl_candidate = doc.get("sl_price")

        if tp_candidate is not None and sl_candidate is not None:
            return float(tp_candidate), float(sl_candidate), time_limit_s

        # Fall back to percentage-based exits from registry
        tp_pct = float(ep.get("take_profit", Decimal("0.03")))
        sl_pct = float(ep.get("stop_loss", Decimal("0.015")))

        if direction == "LONG":
            tp_price = entry * (1 + tp_pct)
            sl_price = entry * (1 - sl_pct)
        else:
            tp_price = entry * (1 - tp_pct)
            sl_price = entry * (1 + sl_pct)

        return tp_price, sl_price, time_limit_s

    except (KeyError, Exception) as e:
        logger.debug(f"Could not resolve exit prices for {engine}: {e}")
        return None, None, 86400


class SignalAttributionTask(BaseTask):
    """
    Daily task: compute theoretical outcomes for all triggered candidates.

    Processes the last LOOKBACK_HOURS hours of candidates where
    trigger_fired=True and attr_computed_at doesn't exist yet.
    Updates each with attr_* fields.
    """

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.lookback_hours = task_config.get("lookback_hours", LOOKBACK_HOURS)
        self.connector_name = task_config.get("connector_name", DEFAULT_CONNECTOR)

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for SignalAttributionTask")

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        db = self.mongodb_client.get_database()

        # Find candidates to attribute: triggered + not yet attributed
        cutoff_ms = int(
            (start - timedelta(hours=self.lookback_hours)).timestamp() * 1000
        )

        cursor = db["candidates"].find({
            "trigger_fired": True,
            "attr_computed_at": {"$exists": False},
            "timestamp_utc": {"$gte": cutoff_ms},
        })
        docs: List[dict] = await cursor.to_list(length=10000)

        stats = {
            "candidates_found": len(docs),
            "attributed": 0,
            "tp": 0,
            "sl": 0,
            "time_limit": 0,
            "stale_data": 0,
            "errors": 0,
        }

        if not docs:
            logger.info("SignalAttribution: no candidates to attribute")
            return self._result_dict(context, stats, start)

        # Cache loaded DataFrames to avoid re-reading the same file repeatedly
        df_cache: Dict[str, Optional[pd.DataFrame]] = {}

        for doc in docs:
            engine = doc.get("engine", "")
            pair = doc.get("pair", "")
            direction = doc.get("direction", "LONG")
            entry = doc.get("decision_price")
            ts_ms = doc.get("timestamp_utc")

            if not all([engine, pair, direction, entry, ts_ms]):
                stats["errors"] += 1
                continue

            try:
                # Resolve exit params
                tp_price, sl_price, time_limit_s = _resolve_exit_prices(doc, engine)
                if tp_price is None or sl_price is None:
                    stats["errors"] += 1
                    continue

                # Get parquet resolution for this engine
                try:
                    meta = get_strategy(engine)
                    resolution = meta.backtesting_resolution
                except KeyError:
                    resolution = "1h"  # default

                # Load candles (cached)
                cache_key = f"{pair}|{resolution}"
                if cache_key not in df_cache:
                    df_cache[cache_key] = _load_candles(
                        self.connector_name, pair, resolution
                    )
                df = df_cache[cache_key]

                if df is None:
                    close_type = "STALE_DATA"
                    exit_price = float(entry)
                else:
                    entry_ts_s = ts_ms / 1000.0
                    close_type, exit_price = _simulate_trade(
                        df=df,
                        entry_ts_s=entry_ts_s,
                        entry_price=float(entry),
                        direction=direction,
                        tp_price=tp_price,
                        sl_price=sl_price,
                        time_limit_s=time_limit_s,
                    )

                entry_f = float(entry)
                if entry_f > 0:
                    pnl_mult = 1.0 if direction == "LONG" else -1.0
                    theoretical_pnl_pct = (
                        (exit_price - entry_f) / entry_f * 100 * pnl_mult
                    )
                else:
                    theoretical_pnl_pct = 0.0

                would_have_won = close_type == "TP"

                await db["candidates"].update_one(
                    {"candidate_id": doc["candidate_id"]},
                    {"$set": {
                        "attr_would_have_won": would_have_won,
                        "attr_theoretical_pnl_pct": round(theoretical_pnl_pct, 4),
                        "attr_theoretical_close": close_type,
                        "attr_exit_price": exit_price,
                        "attr_computed_at": start,
                    }},
                )

                stats["attributed"] += 1
                stats[close_type.lower()] = stats.get(close_type.lower(), 0) + 1
                logger.debug(
                    f"Attributed {engine}/{pair} {direction}: "
                    f"{close_type}, pnl_pct={theoretical_pnl_pct:.2f}%"
                )

            except Exception as e:
                stats["errors"] += 1
                logger.warning(f"Attribution error for {engine}/{pair}: {e}")

        logger.info(
            f"SignalAttribution: {stats['attributed']} attributed "
            f"(TP={stats['tp']}, SL={stats['sl']}, "
            f"TIME_LIMIT={stats['time_limit']}, STALE={stats['stale_data']}, "
            f"errors={stats['errors']})"
        )

        return self._result_dict(context, stats, start)

    def _result_dict(self, context, stats: dict, start: datetime) -> dict:
        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
            "duration_seconds": (datetime.now(timezone.utc) - start).total_seconds(),
        }
