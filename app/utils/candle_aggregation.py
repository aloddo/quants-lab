"""
On-demand candle aggregation: 1m OHLCV → any higher interval.

Pure function, no side effects, no external imports beyond pandas/numpy.
This is the core building block for multi-resolution backtesting.

Usage:
    from app.utils.candle_aggregation import aggregate_candles

    df_5m = aggregate_candles(df_1m, "5m")
    df_1h = aggregate_candles(df_1m, "1h")
    df_4h = aggregate_candles(df_1m, "4h")

For controllers running in Docker: inline this function (it's pure pandas).
"""

import pandas as pd
import numpy as np

# Interval string → pandas frequency
INTERVAL_TO_FREQ = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1D",
}

# Standard OHLCV aggregation rules
_AGG_RULES = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
    "quote_asset_volume": "sum",
    "n_trades": "sum",
    "taker_buy_base_volume": "sum",
    "taker_buy_quote_volume": "sum",
}


def aggregate_candles(
    df_1m: pd.DataFrame,
    target_interval: str,
    ts_col: str = "timestamp",
    ts_unit: str = "s",
) -> pd.DataFrame:
    """Aggregate 1m OHLCV candles to any higher interval.

    Args:
        df_1m: DataFrame with at minimum [timestamp, open, high, low, close, volume].
            May also contain: quote_asset_volume, n_trades, taker_buy_base_volume,
            taker_buy_quote_volume (all aggregated by sum).
        target_interval: Target interval string ("5m", "15m", "1h", "4h", "1d").
        ts_col: Name of the timestamp column (default: "timestamp").
        ts_unit: Unit of the timestamp column ("s" for unix seconds, "ms" for millis).

    Returns:
        DataFrame with same schema as input, aggregated to target_interval.
        Timestamps are left-edge of each bar in the same format as input.
        Incomplete bars at the end are included (last bar may have fewer candles).

    Raises:
        ValueError: If target_interval is not recognized or is <= 1m.
    """
    if target_interval == "1m":
        return df_1m.copy()

    freq = INTERVAL_TO_FREQ.get(target_interval)
    if freq is None:
        raise ValueError(
            f"Unknown interval '{target_interval}'. "
            f"Supported: {sorted(INTERVAL_TO_FREQ.keys())}"
        )

    df = df_1m.copy()

    # Parse timestamps to DatetimeIndex for grouping
    if ts_unit == "ms":
        dt_index = pd.to_datetime(df[ts_col], unit="ms", utc=True)
    elif ts_unit == "s":
        dt_index = pd.to_datetime(df[ts_col], unit="s", utc=True)
    elif ts_unit == "datetime":
        dt_index = pd.to_datetime(df[ts_col], utc=True)
    else:
        raise ValueError(f"Unknown ts_unit: {ts_unit}")

    df["_dt"] = dt_index

    # Sort by timestamp to ensure first/last aggregation is correct
    # (unsorted data from backfill appends would corrupt open/close)
    df = df.sort_values("_dt").drop_duplicates(subset=["_dt"], keep="last")

    # Build aggregation rules for columns that exist
    agg = {}
    for col, rule in _AGG_RULES.items():
        if col in df.columns:
            agg[col] = rule

    # Drop the original timestamp from aggregation (we'll reconstruct from index)
    if ts_col in agg:
        del agg[ts_col]

    # Group by interval and aggregate
    grouped = df.set_index("_dt").groupby(pd.Grouper(freq=freq, label="left", closed="left"))
    result = grouped.agg(agg).dropna(subset=["open"])

    # Convert DatetimeIndex back to original timestamp format.
    # Use .to_series().apply(timestamp()) for reliable cross-resolution conversion.
    epoch_seconds = np.array([t.timestamp() for t in result.index])
    if ts_unit == "ms":
        result[ts_col] = (epoch_seconds * 1000).astype(float)
    elif ts_unit == "s":
        result[ts_col] = epoch_seconds.astype(float)
    else:
        result[ts_col] = result.index

    result = result.reset_index(drop=True)

    # Ensure column order matches input (timestamp first, then OHLCV, then extras)
    cols = [ts_col] + [c for c in df_1m.columns if c != ts_col and c in result.columns]
    result = result[[c for c in cols if c in result.columns]]

    return result
