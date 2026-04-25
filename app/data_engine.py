"""
DataEngine — Multi-resolution feature computation engine.

The core API for requesting features at any interval from any data source.
Strategies declare what they need, DataEngine assembles it from raw data.

Usage:
    from app.data_engine import DataEngine, FeatureRequest

    engine = DataEngine()
    df = engine.get_features(
        pair="BTC-USDT",
        requests=[
            FeatureRequest("atr", "5m", "candles", {"period": 14}),
            FeatureRequest("funding_zscore", "1h", "derivatives", {"window": 30}),
            FeatureRequest("spread_p90", "1m", "arb_hl_bybit_spread_stats_1m"),
        ],
        start_ts=1744722000,
        end_ts=1776258000,
    )
    # Returns DataFrame at 1m resolution (finest requested) with all features aligned.

Used by: backtest tasks, research scripts, analysis notebooks.
NOT used by: live controllers (they use CandlesConfig + WebSocket).

For backtesting integration: use inject_into_backtest_engine() to populate
BacktestingEngine's candles_feeds dict with computed multi-resolution data.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
from pymongo import MongoClient

from app.utils.candle_aggregation import aggregate_candles, INTERVAL_TO_FREQ
from app.data_sources.merge import merge_source_sync, merge_composite_sync
from app.data_sources.registry import DATA_SOURCE_REGISTRY, get_sources_for_tag
from app.data_sources import DataSourceDescriptor, CompositeDataSourceDescriptor

logger = logging.getLogger(__name__)


@dataclass
class FeatureRequest:
    """What a strategy needs from the data engine.

    Attributes:
        name: Feature identifier (must be in FEATURE_COMPUTE or a raw column name).
        interval: Target interval ("1m", "5m", "1h", etc.).
        source: Where the data comes from:
            - "candles": OHLCV candle data (aggregated from 1m parquet)
            - A feature_tag from DATA_SOURCE_REGISTRY (e.g., "derivatives", "funding_spread")
            - A MongoDB collection name directly (e.g., "arb_hl_bybit_spread_stats_1m")
        params: Feature-specific parameters (e.g., {"period": 14} for ATR).
    """
    name: str
    interval: str
    source: str
    params: Dict[str, Any] = field(default_factory=dict)


# ── Feature compute functions ──────────────────────────────────────
# Pure functions: (DataFrame, **params) → Series.
# The DataFrame has OHLCV columns (for candle features) or merged MongoDB columns
# (for data source features). The returned Series is the computed feature.

def _rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    """Rolling z-score with NaN fill to 0."""
    mu = series.rolling(window, min_periods=max(5, window // 3)).mean()
    sd = series.rolling(window, min_periods=max(5, window // 3)).std().replace(0, np.nan)
    z = (series - mu) / sd
    return z.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
    return ta.atr(df["high"], df["low"], df["close"], length=period).fillna(0.0)


def _atr_percentile(df: pd.DataFrame, period: int = 14, pct_window: int = 2160) -> pd.Series:
    """ATR percentile rank over rolling window."""
    atr_val = _atr(df, period)
    return atr_val.rolling(pct_window, min_periods=100).rank(pct=True).fillna(0.5)


def _ema(df: pd.DataFrame, period: int = 50) -> pd.Series:
    """Exponential moving average of close."""
    return ta.ema(df["close"], length=period).fillna(df["close"])


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """RSI on any series."""
    return ta.rsi(series, length=period).fillna(50.0)


def _returns(df: pd.DataFrame, periods: int = 1) -> pd.Series:
    """Percentage returns over N bars."""
    return (df["close"] / df["close"].shift(periods) - 1).fillna(0.0) * 100


FEATURE_COMPUTE = {
    # Candle-based features
    "atr": lambda df, **p: _atr(df, p.get("period", 14)),
    "atr_percentile": lambda df, **p: _atr_percentile(df, p.get("period", 14), p.get("pct_window", 2160)),
    "ema": lambda df, **p: _ema(df, p.get("period", 50)),
    "rsi": lambda df, **p: ta.rsi(df["close"], length=p.get("period", 14)).fillna(50.0),
    "returns": lambda df, **p: _returns(df, p.get("periods", 1)),
    "volume_zscore": lambda df, **p: _rolling_zscore(df["volume"], p.get("window", 20)),
    # MongoDB-sourced features (computed on merged columns)
    "funding_zscore": lambda df, **p: _rolling_zscore(df["funding_rate"], p.get("window", 30)),
    "oi_rsi": lambda df, **p: _rsi(df["oi_value"], p.get("period", 14)),
    "oi_change_pct": lambda df, **p: (df["oi_value"] / df["oi_value"].shift(1) - 1).fillna(0.0) * 100,
    "ls_zscore": lambda df, **p: _rolling_zscore(df["buy_ratio"], p.get("window", 30)),
    "liq_zscore": lambda df, **p: _rolling_zscore(df.get("total_liquidations_usd", pd.Series(0, index=df.index)), p.get("window", 24)),
    "hl_funding_zscore": lambda df, **p: _rolling_zscore(df.get("hl_funding_rate", pd.Series(0, index=df.index)), p.get("window", 30)),
    "funding_spread": lambda df, **p: (df.get("hl_funding_rate", pd.Series(0, index=df.index)) - df.get("bybit_funding_rate", pd.Series(0, index=df.index))).fillna(0.0),
    # Arb spread stats (from 1m aggregated stats collection)
    "spread_p90": lambda df, **p: df.get("best_p90", pd.Series(0, index=df.index)),
    "spread_mean": lambda df, **p: df.get("best_mean", pd.Series(0, index=df.index)),
    "spread_exceedance": lambda df, **p: df.get(f"time_above_{p.get('threshold', 5)}bps", pd.Series(0, index=df.index)),
}


# Interval ordering for resolution comparison
_INTERVAL_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480, "12h": 720, "1d": 1440,
}


class DataEngine:
    """Multi-resolution feature computation engine.

    Loads 1m candles from parquet, aggregates to requested intervals,
    merges MongoDB data sources, computes features, aligns everything
    to the finest requested interval.
    """

    def __init__(
        self,
        mongo_uri: str = "mongodb://localhost:27017/quants_lab",
        candles_dir: Optional[Path] = None,
    ):
        self._mongo_uri = mongo_uri
        db_name = mongo_uri.rsplit("/", 1)[-1] or "quants_lab"
        self._client = MongoClient(mongo_uri)
        self._db = self._client[db_name]

        if candles_dir is None:
            from core.data_paths import data_paths
            self._candles_dir = data_paths.candles_dir
        else:
            self._candles_dir = candles_dir

    def close(self):
        self._client.close()

    def get_features(
        self,
        pair: str,
        requests: List[FeatureRequest],
        start_ts: int,
        end_ts: int,
        connector: str = "bybit_perpetual",
    ) -> pd.DataFrame:
        """Compute all requested features and return aligned DataFrame.

        Args:
            pair: Trading pair (e.g., "BTC-USDT").
            requests: List of FeatureRequest objects.
            start_ts: Start timestamp (unix seconds).
            end_ts: End timestamp (unix seconds).
            connector: Candle connector name for parquet loading.

        Returns:
            DataFrame indexed at the finest requested interval.
            Coarser features are forward-filled to the fine resolution.
            Columns: one per FeatureRequest.name.
        """
        if not requests:
            return pd.DataFrame()

        # 1. Determine the finest interval (output resolution)
        finest_interval = min(
            (r.interval for r in requests),
            key=lambda i: _INTERVAL_MINUTES.get(i, 9999),
        )
        finest_minutes = _INTERVAL_MINUTES.get(finest_interval, 1)

        # 2. Load 1m candles (base resolution)
        df_1m = self._load_1m_candles(pair, connector, start_ts, end_ts)
        if df_1m is None or len(df_1m) == 0:
            logger.warning(f"No 1m candles for {pair}")
            return pd.DataFrame()

        # 3. Group requests by interval
        by_interval: Dict[str, List[FeatureRequest]] = {}
        for req in requests:
            by_interval.setdefault(req.interval, []).append(req)

        # 4. For each interval: aggregate candles + merge MongoDB data + compute features
        feature_series: Dict[str, pd.Series] = {}  # keyed by output column name

        for interval, interval_requests in by_interval.items():
            # Aggregate candles to this interval
            if interval == "1m":
                df_interval = df_1m.copy()
            else:
                df_interval = aggregate_candles(df_1m, interval)

            if len(df_interval) == 0:
                continue

            # Build datetime index for this interval
            dt_idx = pd.to_datetime(df_interval["timestamp"], unit="s", utc=True)

            # Determine which MongoDB sources need merging
            tags_needed = set()
            direct_collections = []
            for req in interval_requests:
                if req.source == "candles":
                    pass  # Already have OHLCV
                elif req.source in {ds.feature_tag for ds in DATA_SOURCE_REGISTRY.values()}:
                    tags_needed.add(req.source)
                else:
                    # Direct collection name (e.g., arb stats)
                    direct_collections.append((req.source, req))

            # Merge registered data sources
            start_ms = int(start_ts * 1000) if start_ts else None
            end_ms = int(end_ts * 1000) if end_ts else None

            for tag in tags_needed:
                for source in get_sources_for_tag(tag):
                    if isinstance(source, DataSourceDescriptor):
                        df_interval = merge_source_sync(
                            self._db, source, df_interval, pair,
                            start_ts=start_ms, end_ts=end_ms,
                        )
                    elif isinstance(source, CompositeDataSourceDescriptor):
                        df_interval = merge_composite_sync(
                            self._db, source, df_interval, pair,
                            start_ts=start_ms, end_ts=end_ms,
                        )

            # Merge direct MongoDB collections (e.g., arb stats)
            for coll_name, req in direct_collections:
                df_interval = self._merge_direct_collection(
                    df_interval, pair, coll_name, start_ms, end_ms
                )

            # Compute each feature — output column is "{name}_{interval}" to avoid
            # collisions when the same feature is requested at different intervals
            for req in interval_requests:
                # Use explicit alias if same name requested at multiple intervals
                out_col = req.name if sum(1 for r in requests if r.name == req.name) == 1 \
                    else f"{req.name}_{interval}"

                compute_fn = FEATURE_COMPUTE.get(req.name)
                if compute_fn:
                    try:
                        series = compute_fn(df_interval, **req.params)
                        feature_series[out_col] = pd.Series(
                            series.values, index=dt_idx[:len(series)], name=out_col
                        )
                    except Exception as e:
                        logger.warning(f"Failed to compute {req.name}@{interval} for {pair}: {e}")
                        feature_series[out_col] = pd.Series(
                            0.0, index=dt_idx, name=out_col
                        )
                elif req.name in df_interval.columns:
                    # Raw column passthrough
                    feature_series[out_col] = pd.Series(
                        df_interval[req.name].values, index=dt_idx[:len(df_interval)], name=out_col
                    )
                else:
                    logger.warning(f"Unknown feature '{req.name}', filling with 0")
                    feature_series[out_col] = pd.Series(0.0, index=dt_idx, name=out_col)

        # 5. Align all features to finest interval
        if not feature_series:
            return pd.DataFrame()

        # Build the output index from the finest interval candles
        if finest_interval == "1m":
            output_idx = pd.to_datetime(df_1m["timestamp"], unit="s", utc=True)
        else:
            df_finest = aggregate_candles(df_1m, finest_interval)
            output_idx = pd.to_datetime(df_finest["timestamp"], unit="s", utc=True)

        result = pd.DataFrame(index=output_idx)
        result.index.name = "timestamp"

        for name, series in feature_series.items():
            if len(series.index) == len(output_idx) and series.index.equals(output_idx):
                result[name] = series.values
            else:
                # Align coarser series to finer index via merge_asof (forward fill)
                aligned = series.reindex(output_idx, method="ffill")
                result[name] = aligned.values

        result = result.fillna(0.0)
        return result

    def _load_1m_candles(
        self, pair: str, connector: str, start_ts: int, end_ts: int
    ) -> Optional[pd.DataFrame]:
        """Load 1m candles from parquet, filtered to time range."""
        path = self._candles_dir / f"{connector}|{pair}|1m.parquet"
        if not path.exists():
            # Try HL connector
            path = self._candles_dir / f"hyperliquid_perpetual|{pair}|1m.parquet"
        if not path.exists():
            return None

        df = pd.read_parquet(path)
        if "timestamp" not in df.columns:
            return None

        # Filter to time range
        mask = (df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)
        df = df[mask].reset_index(drop=True)
        return df

    def _merge_direct_collection(
        self, df: pd.DataFrame, pair: str, collection_name: str,
        start_ms: Optional[int], end_ms: Optional[int],
    ) -> pd.DataFrame:
        """Merge a MongoDB collection directly (not via registry)."""
        coll = self._db[collection_name]
        query = {}

        # Try pair field variants
        if coll.find_one({"pair": pair}):
            query["pair"] = pair
        elif coll.find_one({"symbol": pair.replace("-", "")}):
            query["symbol"] = pair.replace("-", "")

        # Time range
        ts_field = "timestamp"
        sample = coll.find_one()
        if sample:
            if "timestamp_utc" in sample:
                ts_field = "timestamp_utc"
            elif "timestamp" in sample:
                ts_field = "timestamp"

        if start_ms or end_ms:
            ts_filter = {}
            if start_ms:
                ts_filter["$gte"] = start_ms if isinstance(sample.get(ts_field), (int, float)) else \
                    pd.Timestamp(start_ms, unit="ms", tz="UTC").to_pydatetime()
            if end_ms:
                ts_filter["$lte"] = end_ms if isinstance(sample.get(ts_field), (int, float)) else \
                    pd.Timestamp(end_ms, unit="ms", tz="UTC").to_pydatetime()
            if ts_filter:
                query[ts_field] = ts_filter

        docs = list(coll.find(query).sort(ts_field, 1))
        if not docs:
            return df

        sdf = pd.DataFrame(docs)
        # Parse timestamps
        ts_val = sdf[ts_field].iloc[0]
        if isinstance(ts_val, (int, float)) and ts_val > 1e12:
            sdf["_ts"] = pd.to_datetime(sdf[ts_field], unit="ms", utc=True)
        elif isinstance(ts_val, (int, float)):
            sdf["_ts"] = pd.to_datetime(sdf[ts_field], unit="s", utc=True)
        else:
            sdf["_ts"] = pd.to_datetime(sdf[ts_field], utc=True)

        sdf = sdf.set_index("_ts").sort_index()
        sdf = sdf[~sdf.index.duplicated(keep="last")]

        # Merge numeric columns into candle df
        candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        numeric_cols = sdf.select_dtypes(include=[np.number]).columns
        skip_cols = {"_id", ts_field, "collected_at", "recorded_at"}

        for col in numeric_cols:
            if col in skip_cols or col.startswith("_"):
                continue
            if col not in df.columns:
                reindexed = sdf[[col]].reindex(candle_idx, method="ffill")
                df[col] = reindexed[col].fillna(0.0).values

        return df

    def inject_into_backtest_engine(
        self,
        bt_engine,
        pair: str,
        intervals: List[str],
        connector: str = "bybit_perpetual",
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ):
        """Pre-populate BacktestingEngine's candles_feeds with multi-resolution data.

        This solves the integration hole: BacktestingEngine loads from parquet cache,
        but computed interval candles don't exist as parquet. We inject them directly
        into the candles_feeds dict (same pattern as _backtest_worker merge).

        Also patches get_candles_feed to prevent HB from trying to fetch from
        exchange API (which fails for excluded connectors like hyperliquid_perpetual).

        Args:
            bt_engine: BacktestingEngine instance (after load_cached_data).
            pair: Trading pair.
            intervals: List of intervals to inject (e.g., ["5m", "15m"]).
            connector: Connector name.
            start_ts: Optional start (unix seconds) for time range.
            end_ts: Optional end (unix seconds) for time range.
        """
        provider = bt_engine._bt_engine.backtesting_data_provider

        # Load 1m base
        feed_key_1m = f"{connector}_{pair}_1m"
        df_1m = provider.candles_feeds.get(feed_key_1m)

        if df_1m is None or len(df_1m) == 0:
            # Try loading from parquet directly
            df_1m = self._load_1m_candles(pair, connector, start_ts or 0, end_ts or int(1e10))
            if df_1m is not None and len(df_1m) > 0:
                provider.candles_feeds[feed_key_1m] = df_1m

        if df_1m is None or len(df_1m) == 0:
            logger.warning(f"No 1m data for {pair}, cannot inject multi-resolution candles")
            return

        # Inject each requested interval
        for interval in intervals:
            feed_key = f"{connector}_{pair}_{interval}"
            if feed_key in provider.candles_feeds and len(provider.candles_feeds[feed_key]) > 0:
                continue  # Already loaded (e.g., 1h from parquet)

            df_agg = aggregate_candles(df_1m, interval)
            if len(df_agg) > 0:
                # Ensure datetime index (BacktestingEngine expects this)
                df_agg.index = pd.to_datetime(df_agg["timestamp"], unit="s")
                df_agg.index.name = None
                # Ensure numeric types
                for col in df_agg.columns:
                    if col != "timestamp":
                        df_agg[col] = pd.to_numeric(df_agg[col], errors="coerce")
                provider.candles_feeds[feed_key] = df_agg
                logger.info(f"Injected {feed_key}: {len(df_agg)} bars")

        # Patch fallback to prevent exchange API fetch for injected feeds
        _orig_get_feed = getattr(provider, "_orig_get_feed", None) or getattr(provider, "get_candles_feed", None)
        if _orig_get_feed and not hasattr(provider, "_patched_by_data_engine"):
            provider._orig_get_feed = _orig_get_feed

            def _patched_get_feed(config):
                from hummingbot.strategy_v2.backtesting.backtesting_data_provider import BacktestingDataProvider
                key = BacktestingDataProvider._generate_candle_feed_key(config)
                existing = provider.candles_feeds.get(key, pd.DataFrame())
                if len(existing) > 0:
                    return existing
                return _orig_get_feed(config)

            provider.get_candles_feed = _patched_get_feed
            provider.initialize_candles_feed = lambda config: _patched_get_feed(config)
            provider._patched_by_data_engine = True
