import asyncio
import gc
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict

import pandas as pd

from core.data_sources import CLOBDataSource
from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

logging.basicConfig(level=logging.INFO)
logging.getLogger("asyncio").setLevel(logging.CRITICAL)


from app.tasks.notifying_task import NotifyingTaskMixin


class CandlesDownloaderTask(NotifyingTaskMixin, BaseTask):
    """Download OHLC candles data from exchanges and store as parquet files."""
    
    def __init__(self, config):
        super().__init__(config)
        
        # Configuration with defaults
        task_config = self.config.config
        self.connector_name = task_config["connector_name"]
        self.days_data_retention = task_config.get("days_data_retention", 7)
        self.intervals = task_config.get("intervals", ["1m"])
        self.quote_asset = task_config.get("quote_asset", "USDT")
        self.min_notional_size = Decimal(str(task_config.get("min_notional_size", 10.0)))
        
        # Initialize CLOB data source (handles parquet caching automatically)
        self.clob = CLOBDataSource()

    async def setup(self, context: TaskContext) -> None:
        """Setup task before execution, including validation of prerequisites."""
        try:
            await super().setup(context)
            
            # Validate prerequisites
            if not self.connector_name:
                raise RuntimeError("connector_name not configured")
            
            logging.info(f"Setup completed for {context.task_name}")
            logging.info(f"Connector: {self.connector_name}")
            logging.info(f"Quote asset: {self.quote_asset}")
            logging.info(f"Intervals: {self.intervals}")
            logging.info(f"Data retention: {self.days_data_retention} days")
            
        except Exception as e:
            logging.error(f"Setup failed: {e}")
            raise
    
    async def cleanup(self, context: TaskContext, result) -> None:
        """Cleanup after task execution."""
        try:
            await super().cleanup(context, result)
            logging.info(f"Cleanup completed for {context.task_name}")
        except Exception as e:
            logging.warning(f"Cleanup error: {e}")

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        """Main execution logic."""
        start_execution = datetime.now(timezone.utc)
        logging.info(f"Starting candles downloader for {self.connector_name}")

        try:
            # MEMORY FIX (2026-06-10, Alberto/CoS): the OLD code did load_candles_cache(connector) ->
            # loaded the FULL history of EVERY pair x interval (1.87GB parquet -> ~7-8GB pandas) into the
            # long-lived orchestrator at once = the sudden RSS step that thrashed a 16GB/swap-0 box.
            # FIX: process ONE pair at a time (load_candles_cache supports a trading_pair filter), dump
            # it, then clear+gc. Peak RAM is bounded to a single pair's candles. Parquet is the truth.

            # Determine which pairs already have parquet WITHOUT loading them (list filenames only).
            candles_dir = data_paths.candles_dir
            cached_pairs = set()
            if candles_dir.exists():
                for fn in os.listdir(candles_dir):
                    if fn == ".gitignore":
                        continue
                    parts = fn.split(".")[0].split("|")
                    if len(parts) == 3 and parts[0] == self.connector_name:
                        cached_pairs.add(parts[1])
            logging.info(f"{len(cached_pairs)} pairs have existing parquet for {self.connector_name}")

            end_time = datetime.now(timezone.utc)
            start_time = pd.Timestamp(
                time.time() - self.days_data_retention * 24 * 60 * 60,
                unit="s"
            ).tz_localize(timezone.utc).timestamp()
            logging.info(f"Time range: {start_time} to {end_time}")

            trading_rules = await self.clob.get_trading_rules(self.connector_name)
            all_trading_pairs = trading_rules.get_all_trading_pairs()
            # Incremental: only update pairs that already have parquet (new pairs need a separate
            # backfill; a full year x 500 pairs would block the pipeline). First run: do all.
            if cached_pairs:
                trading_pairs = [p for p in all_trading_pairs if p in cached_pairs]
                skipped = len(all_trading_pairs) - len(trading_pairs)
                if skipped:
                    logging.info(f"Incremental mode: {len(trading_pairs)} cached pairs, skipping {skipped} new")
            else:
                trading_pairs = all_trading_pairs

            stats = {"pairs_processed": 0, "pairs_total": len(trading_pairs),
                     "intervals_processed": 0, "candles_downloaded": 0, "errors": 0}

            for i, trading_pair in enumerate(trading_pairs):
                # PER-PAIR: load only this pair's parquet so get_candles fetches just the incremental
                # gap; bounds peak RAM to one pair instead of the whole 1.87GB cache.
                self.clob._candles_cache.clear()
                self.clob.load_candles_cache(connector_name=self.connector_name, trading_pair=trading_pair)
                for interval in self.intervals:
                    try:
                        logging.info(f"Fetching candles for {trading_pair} [{i+1}/{len(trading_pairs)}] {interval}")
                        candles = await self.clob.get_candles(
                            self.connector_name, trading_pair, interval,
                            int(start_time), int(end_time.timestamp()))
                        if candles.data.empty:
                            logging.info(f"No new candles for {trading_pair} {interval}")
                            continue
                        stats["candles_downloaded"] += len(candles.data)
                        stats["intervals_processed"] += 1
                        await asyncio.sleep(1)   # rate limit
                    except Exception as e:
                        stats["errors"] += 1
                        logging.exception(f"Error processing {trading_pair} {interval}: {e}")
                        continue
                # dump THIS pair, then free it -> bounded RSS
                self.clob.dump_candles_cache()
                self.clob._candles_cache.clear()
                gc.collect()
                stats["pairs_processed"] += 1

            # Prepare result
            duration = datetime.now(timezone.utc) - start_execution
            result = {
                "status": "completed",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_id": context.execution_id,
                "connector": self.connector_name,
                "stats": stats,
                "duration_seconds": duration.total_seconds()
            }
            
            logging.info(f"Candles download completed: {stats}")
            return result
            
        except Exception as e:
            logging.error(f"Error executing candles downloader: {e}")
            raise
    
    async def on_success(self, context: TaskContext, result) -> None:
        """Handle successful execution."""
        stats = result.result_data.get("stats", {})
        logging.info(f"✓ CandlesDownloaderTask succeeded in {result.duration_seconds:.2f}s")
        logging.info(f"  - Pairs: {stats.get('pairs_processed', 0)}/{stats.get('pairs_total', 0)}")
        logging.info(f"  - Intervals: {stats.get('intervals_processed', 0)}")
        logging.info(f"  - Candles: {stats.get('candles_downloaded', 0)}")
        if stats.get('errors', 0) > 0:
            logging.warning(f"  - Errors: {stats.get('errors', 0)}")
    
    async def on_failure(self, context: TaskContext, result) -> None:
        """Handle failed execution."""
        logging.error(f"✗ CandlesDownloaderTask failed: {result.error_message}")
        logging.error(f"  Execution ID: {context.execution_id}")
    
    async def on_retry(self, context: TaskContext, attempt: int, error: Exception) -> None:
        """Handle retry attempt."""
        logging.warning(f"🔄 CandlesDownloaderTask retry attempt {attempt}: {error}")


async def main():
    """Standalone execution for testing."""
    from core.tasks.base import TaskConfig, ScheduleConfig
    
    # Create v2.0 TaskConfig
    config = TaskConfig(
        name="candles_downloader_test",
        enabled=True,
        task_class="tasks.data_collection.candles_downloader_task.CandlesDownloaderTask",
        schedule=ScheduleConfig(
            type="frequency",
            frequency_hours=1.0
        ),
        config={
            "connector_name": "binance_perpetual",
            "quote_asset": "USDT",
            "intervals": ["15m", "1h"],
            "days_data_retention": 30,
            "min_notional_size": 10
        }
    )
    
    # Create and run task
    task = CandlesDownloaderTask(config)
    result = await task.run()
    
    print(f"Task completed with status: {result.status}")
    if result.result_data:
        stats = result.result_data.get("stats", {})
        print(f"Downloaded {stats.get('candles_downloaded', 0)} candles")
        print(f"Processed {stats.get('pairs_processed', 0)} pairs")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())