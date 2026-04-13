"""
Trade Recorder — persists exchange execution data + HB bot trades to MongoDB.

Polls three data sources every 5 minutes:
1. Bybit exchange /v5/execution/list — authoritative fill record (source of truth)
2. Bybit exchange /v5/position/closed-pnl — complete round-trip position results
3. HB API /bot-orchestration/{bot}/history — bot's view of fills (for correlation)

Also snapshots open positions from the exchange for time-series analysis.

Stores to:
- exchange_executions: individual fills from Bybit (dedup by exec_id) — SOURCE OF TRUTH
- exchange_closed_pnl: closed position round-trips (dedup by order_id+pair)
- paper_trades: HB API bot history (bot's view, for cross-reference)
- paper_position_snapshots: periodic exchange position snapshots
- paper_controller_stats: per-controller performance from HB orchestration

Strategy-agnostic: records ALL exchange activity, regardless of which bot placed orders.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp

from core.tasks import BaseTask, TaskContext
from app.services.hb_api_client import HBApiClient
from app.services.bybit_exchange_client import BybitExchangeClient

logger = logging.getLogger(__name__)


class TradeRecorderTask(BaseTask):
    """Record HB-native bot trades and position snapshots to MongoDB."""

    def __init__(self, config):
        super().__init__(config)

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        db = self.mongodb_client.get_database()
        stats = {
            "exchange_executions": 0, "exchange_closed_pnl": 0,
            "hb_trades": 0, "positions_snapshot": 0, "bots_polled": 0,
        }

        exchange = BybitExchangeClient()

        # 1. Record exchange executions (source of truth for all fills)
        if exchange.is_configured():
            try:
                # Get high-water mark to only fetch new executions
                last_exec = await db["exchange_executions"].find_one(
                    sort=[("exec_time", -1)],
                )
                start_ms = int(last_exec["exec_time"]) + 1 if last_exec else None

                async with aiohttp.ClientSession() as session:
                    executions = await exchange.fetch_executions(
                        session, limit=100, start_ms=start_ms,
                    )
                    for ex in executions:
                        ex["recorded_at"] = datetime.now(timezone.utc)
                        result = await db["exchange_executions"].update_one(
                            {"exec_id": ex["exec_id"]},
                            {"$setOnInsert": ex},
                            upsert=True,
                        )
                        if result.upserted_id:
                            stats["exchange_executions"] += 1

            except Exception as e:
                logger.warning(f"TradeRecorder: exchange executions failed: {e}")

            # 2. Record closed PnL (complete round-trip position results)
            try:
                last_closed = await db["exchange_closed_pnl"].find_one(
                    sort=[("updated_time", -1)],
                )
                start_ms = int(last_closed["updated_time"]) + 1 if last_closed else None

                async with aiohttp.ClientSession() as session:
                    closed = await exchange.fetch_closed_pnl(
                        session, limit=50, start_ms=start_ms,
                    )
                    for c in closed:
                        c["recorded_at"] = datetime.now(timezone.utc)
                        result = await db["exchange_closed_pnl"].update_one(
                            {"order_id": c["order_id"], "pair": c["pair"]},
                            {"$setOnInsert": c},
                            upsert=True,
                        )
                        if result.upserted_id:
                            stats["exchange_closed_pnl"] += 1

            except Exception as e:
                logger.warning(f"TradeRecorder: exchange closed PnL failed: {e}")

            # 3. Snapshot open positions from exchange
            try:
                async with aiohttp.ClientSession() as session:
                    positions = await exchange.fetch_positions(session)

                if positions:
                    now = datetime.now(timezone.utc)
                    snapshot = {
                        "timestamp": now,
                        "positions": positions,
                        "total_unrealised_pnl": sum(
                            p["unrealised_pnl"] for p in positions
                        ),
                        "total_realised_pnl": sum(
                            p["realised_pnl"] for p in positions
                        ),
                        "total_position_value": sum(
                            p["position_value"] for p in positions
                        ),
                        "position_count": len(positions),
                    }
                    await db["paper_position_snapshots"].insert_one(snapshot)
                    stats["positions_snapshot"] = len(positions)

            except Exception as e:
                logger.warning(f"TradeRecorder: position snapshot failed: {e}")
        else:
            logger.debug("TradeRecorder: Bybit credentials not configured, skipping exchange data")

        # 4. Record fills from HB bots (bot's view, for cross-reference)
        hb = HBApiClient()
        try:
            orch = await hb.get_bot_status()
            bots = orch.get("data", {}) if isinstance(orch, dict) else {}

            for bot_name, bot_data in bots.items():
                if not isinstance(bot_data, dict):
                    continue
                stats["bots_polled"] += 1

                try:
                    history = await hb._request(
                        "GET",
                        f"/bot-orchestration/{bot_name}/history",
                        params={"days": 1},
                    )
                    trades = self._extract_trades(history, bot_name)

                    for trade in trades:
                        result = await db["paper_trades"].update_one(
                            {"trade_id": trade["trade_id"]},
                            {"$setOnInsert": trade},
                            upsert=True,
                        )
                        if result.upserted_id:
                            stats["hb_trades"] += 1

                except Exception as e:
                    logger.warning(f"Failed to record trades for {bot_name}: {e}")

                # Record close events from orchestration performance
                perf = bot_data.get("performance", {})
                if isinstance(perf, dict):
                    for ctrl_name, ctrl_data in perf.items():
                        p = ctrl_data.get("performance", {})
                        closes = p.get("close_type_counts", {})
                        rpnl = p.get("realized_pnl_quote", 0)
                        vol = p.get("volume_traded", 0)

                        if closes or rpnl != 0:
                            await db["paper_controller_stats"].update_one(
                                {"bot_name": bot_name, "controller": ctrl_name},
                                {"$set": {
                                    "close_type_counts": closes,
                                    "realized_pnl_quote": rpnl,
                                    "volume_traded": vol,
                                    "updated_at": datetime.now(timezone.utc),
                                }},
                                upsert=True,
                            )

        except Exception as e:
            logger.warning(f"Failed to poll HB API for trades: {e}")
        finally:
            await hb.close()

        new_data = stats["exchange_executions"] + stats["exchange_closed_pnl"] + stats["hb_trades"]
        if new_data > 0:
            logger.info(
                f"TradeRecorder: {stats['exchange_executions']} exchange fills, "
                f"{stats['exchange_closed_pnl']} closed PnL, "
                f"{stats['hb_trades']} HB trades, "
                f"{stats['positions_snapshot']} positions"
            )

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
        }

    def _extract_trades(self, history_response: dict, bot_name: str) -> List[Dict]:
        """Parse trade fills from the HB API history response."""
        resp = history_response.get("response", history_response)
        inner = resp.get("data", resp) if isinstance(resp, dict) else resp
        inner2 = inner.get("data", inner) if isinstance(inner, dict) else inner
        raw_trades = inner2.get("trades", []) if isinstance(inner2, dict) else []

        trades = []
        for t in raw_trades:
            fee_info = t.get("raw_json", {}).get("trade_fee", {})
            flat_fees = fee_info.get("flat_fees", [])
            fee_amount = float(flat_fees[0].get("amount", 0)) if flat_fees else 0.0

            symbol = t.get("symbol", "")
            # Convert BTCUSDT -> BTC-USDT if needed
            pair = symbol if "-" in symbol else f"{symbol.replace('USDT', '')}-USDT"

            trades.append({
                "trade_id": t.get("trade_id", ""),
                "bot_name": bot_name,
                "pair": pair,
                "side": t.get("trade_type", ""),  # BUY or SELL
                "price": float(t.get("price", 0)),
                "quantity": float(t.get("quantity", 0)),
                "quote_value": float(t.get("price", 0)) * float(t.get("quantity", 0)),
                "fee": fee_amount,
                "exchange": t.get("market", ""),
                "trade_timestamp": t.get("trade_timestamp", 0),
                "recorded_at": datetime.now(timezone.utc),
            })
        return trades
