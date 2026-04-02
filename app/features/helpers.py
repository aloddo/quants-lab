"""Shared computation helpers for features."""
import math
from typing import Optional

import pandas as pd


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Compute ATR using Wilder's smoothing."""
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """Rolling percentile rank (0-1) of the last value vs the window."""
    def _pct(s):
        if len(s) < 2:
            return float("nan")
        v = s.iloc[-1]
        return (s.iloc[:-1] < v).sum() / (len(s) - 1)
    return series.rolling(window, min_periods=max(100, window // 10)).apply(_pct, raw=False)


def zscore(series: pd.Series, window: int = 20) -> pd.Series:
    """Rolling z-score."""
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    return (series - mean) / std.replace(0, float("nan"))
