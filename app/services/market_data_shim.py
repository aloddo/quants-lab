"""
MarketDataShim — drop-in replacement for HB's MarketDataProvider.

Provides get_candles_df() and time() backed by parquet files (same files
written by CandlesDownloaderTask). Passed as `market_data_provider` to
HB controller constructors so they run identically outside the HB event loop.

Design (from unified plan v4, eng review finding):
- HB controllers require market_data_provider in __init__
- This shim implements the subset of MarketDataProvider that controllers use:
  get_candles_df(), time(), ready property, initialize_candles_feed()
- Parquet files use pipe-delimited names: bybit_perpetual|BTC-USDT|1h.parquet
"""

import logging
import time as _time
from pathlib import Path
from typing import Optional, List

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CANDLE_DIR = "/Users/hermes/quants-lab/app/data/cache/candles"


class MarketDataShim:
    """
    Drop-in replacement for HB's MarketDataProvider.

    Implements the interface that controllers call:
    - get_candles_df(connector_name, trading_pair, interval, max_records)
    - time() -> float
    - ready -> bool (always True)
    - initialize_candles_feed() -> no-op

    Usage:
        shim = MarketDataShim()
        controller = X5Controller(config, market_data_provider=shim, actions_queue=queue)
        await controller.update_processed_data()
    """

    def __init__(self, candle_dir: str = DEFAULT_CANDLE_DIR):
        self.candle_dir = Path(candle_dir)
        self._cache = {}
        self._cache_ts = {}
        self._cache_ttl = 60  # seconds
        # HB's MarketDataProvider has these attributes
        self.connectors = {}
        self.candles_feeds = {}

    @property
    def ready(self) -> bool:
        """Always ready — data is local parquet."""
        return True

    def time(self) -> float:
        """Current time in seconds (HB convention)."""
        return _time.time()

    def initialize_candles_feed(self, candles_config: list = None):
        """No-op. HB uses this to start WS candle feeds. We read parquet."""
        pass

    def initialize_rate_sources(self, connector_pairs: list = None):
        """No-op. HB uses this for rate conversion. Not needed for perps."""
        pass

    def get_order_book(self, connector_name: str, trading_pair: str):
        """No-op stub. Some controllers check orderbook. Returns None."""
        return None

    def get_candles_df(
        self,
        connector_name: str,
        trading_pair: str,
        interval: str = "1h",
        max_records: int = 100,
    ) -> Optional[pd.DataFrame]:
        """Load candles from parquet. Returns DataFrame matching HB's format.

        Columns: timestamp, open, high, low, close, volume,
                 quote_asset_volume, n_trades, taker_buy_base_volume,
                 taker_buy_quote_volume

        Args:
            connector_name: Exchange connector (e.g., "bybit_perpetual").
                            Strips _testnet/_demo suffixes.
            trading_pair: Pair in HB format (e.g., "GALA-USDT").
            interval: Candle interval (e.g., "1h", "1m", "5m").
            max_records: Number of most recent candles to return.
        """
        # Normalize connector name (strip testnet/demo suffixes)
        connector = connector_name.replace("_testnet", "").replace("_demo", "")

        # Parquet files use pipe-delimited names
        filename = f"{connector}|{trading_pair}|{interval}.parquet"
        path = self.candle_dir / filename

        # Check cache
        cache_key = str(path)
        now = _time.time()
        if (
            cache_key in self._cache
            and now - self._cache_ts.get(cache_key, 0) < self._cache_ttl
        ):
            df = self._cache[cache_key]
            return df.tail(max_records).reset_index(drop=True)

        if not path.exists():
            logger.warning(f"MarketDataShim: parquet not found: {path}")
            return None

        try:
            df = pd.read_parquet(path)
            # Ensure proper column types
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            self._cache[cache_key] = df
            self._cache_ts[cache_key] = now

            return df.tail(max_records).reset_index(drop=True)

        except Exception as e:
            logger.error(f"MarketDataShim: failed to read {path}: {e}")
            return None

    def get_price(self, connector_name: str, trading_pair: str) -> float:
        """Latest close price from candles."""
        df = self.get_candles_df(connector_name, trading_pair, max_records=1)
        if df is not None and len(df) > 0:
            return float(df["close"].iloc[-1])
        return 0.0
