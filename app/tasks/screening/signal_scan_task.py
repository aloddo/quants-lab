"""
Signal scan task — evaluates E1/E2 engines on eligible pairs.

Reads pre-computed features from FeatureStorage, builds DecisionSnapshots,
runs engine evaluation, stores candidates to MongoDB, sends Telegram alerts.
"""
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from core.data_paths import data_paths
from core.data_structures.candles import Candles
from core.tasks import BaseTask, TaskContext

from app.engines.e1_compression_breakout import evaluate_e1, E1Candidate
from app.engines.e2_range_fade import evaluate_e2, E2Candidate
from app.engines.models import DecisionSnapshot, FeatureRow
from app.engines.pair_selector import get_eligible_pairs

logger = logging.getLogger(__name__)


class SignalScanTask(BaseTask):
    """Evaluate E1/E2 engines on screened pairs and log candidates."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.engines = task_config.get("engines", ["E1", "E2"])
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")
        self.top_n = task_config.get("top_n", 10)
        self.telegram_chat_id = task_config.get("telegram_chat_id")

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for SignalScanTask")

    async def _build_feature_row(self, pair: str) -> Optional[FeatureRow]:
        """Assemble a FeatureRow from the latest features in MongoDB."""
        feature_names = ["atr", "range", "volume", "momentum", "derivatives", "market_regime"]
        feature_data = {}

        for fname in feature_names:
            docs = await self.mongodb_client.get_documents(
                "features",
                {"feature_name": fname, "trading_pair": pair},
                limit=1,
            )
            if docs:
                feature_data[fname] = docs[0].get("value", {})

        atr_d = feature_data.get("atr", {})
        range_d = feature_data.get("range", {})
        vol_d = feature_data.get("volume", {})
        mom_d = feature_data.get("momentum", {})
        deriv_d = feature_data.get("derivatives", {})

        close = None
        # Get close from momentum's ema_50 or load from parquet
        path = data_paths.candles_dir / f"{self.connector_name}|{pair}|1h.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            if not df.empty:
                close = float(df["close"].iloc[-1])

        if close is None:
            return None

        return FeatureRow(
            pair=pair,
            timestamp_utc=int(datetime.now(timezone.utc).timestamp() * 1000),
            close=close,
            return_1h=mom_d.get("return_1h"),
            return_4h=mom_d.get("return_4h"),
            return_24h=mom_d.get("return_24h"),
            atr_14_1h=atr_d.get("atr_14_1h"),
            atr_percentile_90d=atr_d.get("atr_percentile_90d"),
            range_high_20=range_d.get("range_high_20"),
            range_low_20=range_d.get("range_low_20"),
            range_compression_confirmed=atr_d.get("compression_flag", 0) > 0.5,
            volume_1h=vol_d.get("vol_avg_20"),
            volume_zscore_20=vol_d.get("vol_zscore_20"),
            volume_floor_passed=vol_d.get("vol_floor_passed", 0) > 0.5,
            funding_rate_current=deriv_d.get("funding_rate"),
            funding_neutral=deriv_d.get("funding_neutral", 0) > 0.5,
            oi_change_1h_pct=deriv_d.get("oi_change_1h_pct"),
            oi_increasing=deriv_d.get("oi_increasing", 0) > 0.5,
            rs_aligned=deriv_d.get("rs_aligned", 0) > 0.5,
            feature_staleness_ok=True,
            staleness_flags=[],
        )

    async def _get_market_state(self) -> str:
        """Read BTC regime from feature store."""
        docs = await self.mongodb_client.get_documents(
            "features",
            {"feature_name": "market_regime", "trading_pair": "BTC-USDT"},
            limit=1,
        )
        if docs:
            val = docs[0].get("value", {})
            if val.get("risk_off", 0) > 0.5:
                return "Risk-Off Contagion"
        return "Normal"

    async def _store_candidate(self, engine: str, cand) -> None:
        """Store candidate to MongoDB candidates collection."""
        doc = asdict(cand)
        doc["engine"] = engine
        await self.mongodb_client.insert_documents(
            collection_name="candidates",
            documents=[doc],
            index=[("engine", 1), ("pair", 1), ("timestamp_utc", 1)],
        )

    async def _send_telegram(self, engine: str, cand) -> None:
        """Send Telegram alert for CANDIDATE_READY signals."""
        if not self.notification_manager:
            return
        from core.notifiers.base import NotificationMessage
        msg = (
            f"<b>{engine} Signal — {cand.pair}</b>\n"
            f"Direction: {cand.direction}\n"
            f"Trigger: {cand.trigger_reason}\n"
            f"Score: {cand.composite_score:.2f}\n"
            f"Price: {cand.decision_price}"
        )
        try:
            notification = NotificationMessage(
                title=f"{engine} Signal — {cand.pair}",
                message=msg,
                level="warning",
            )
            await self.notification_manager.send_notification(notification)
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        market_state = await self._get_market_state()

        stats = {"scanned": 0, "candidates_ready": 0, "skipped": 0, "errors": 0}
        signals = []

        for engine in self.engines:
            try:
                eligible, rejections = await get_eligible_pairs(
                    self.mongodb_client, engine, top_n=self.top_n
                )
                logger.info(f"{engine}: {len(eligible)} eligible pairs, {len(rejections)} rejected")

                for pair_info in eligible:
                    pair = pair_info["pair"]
                    try:
                        fr = await self._build_feature_row(pair)
                        if fr is None:
                            stats["skipped"] += 1
                            continue

                        snap = DecisionSnapshot(
                            pair=pair,
                            features=fr,
                            market_state=market_state,
                            staleness_ok=fr.feature_staleness_ok,
                            staleness_flags=fr.staleness_flags,
                        )

                        if engine == "E1":
                            cand = evaluate_e1(snap)
                        elif engine == "E2":
                            # Get candle high/low for E2
                            path = data_paths.candles_dir / f"{self.connector_name}|{pair}|1h.parquet"
                            c_high, c_low, c_open = None, None, None
                            if path.exists():
                                df = pd.read_parquet(path)
                                if not df.empty:
                                    c_high = float(df["high"].iloc[-1])
                                    c_low = float(df["low"].iloc[-1])
                                    c_open = float(df["open"].iloc[-1])
                            # Range expanding from features
                            range_docs = await self.mongodb_client.get_documents(
                                "features",
                                {"feature_name": "range", "trading_pair": pair},
                                limit=1,
                            )
                            range_exp = None
                            if range_docs:
                                range_exp = range_docs[0].get("value", {}).get("range_expanding")
                            cand = evaluate_e2(snap, c_high, c_low, c_open, range_exp)
                        else:
                            continue

                        # Store candidate (always, even filtered)
                        await self._store_candidate(engine, cand)
                        stats["scanned"] += 1

                        if cand.disposition == "CANDIDATE_READY":
                            stats["candidates_ready"] += 1
                            signals.append({"engine": engine, "pair": pair, "direction": cand.direction})
                            await self._send_telegram(engine, cand)
                            logger.info(f"🔔 {engine} CANDIDATE_READY: {pair} {cand.direction}")
                        else:
                            logger.debug(f"{engine} {pair}: {cand.disposition}")

                    except Exception as e:
                        stats["errors"] += 1
                        logger.error(f"Error scanning {engine}/{pair}: {e}")

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error in {engine} scan: {e}")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"SignalScanTask: {stats} in {duration:.1f}s")

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "market_state": market_state,
            "stats": stats,
            "signals": signals,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        signals = result.result_data.get("signals", [])
        logger.info(f"SignalScanTask: {stats['scanned']} scanned, {stats['candidates_ready']} ready")
        for s in signals:
            logger.info(f"  Signal: {s['engine']} {s['pair']} {s['direction']}")

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"SignalScanTask failed: {result.error_message}")
