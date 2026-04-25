"""
Custom merge functions for CompositeDataSourceDescriptor sources.

Each function has the standard signature:
    (db, candle_df, pair, start_ts, end_ts) -> pd.DataFrame

Add new functions here as composite sources are added to the registry.
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def merge_options_surface_sync(
    db,
    candle_df: pd.DataFrame,
    pair: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> pd.DataFrame:
    """Merge Deribit options surface data into candle DataFrame.

    The options surface collection has multi-dimensional documents:
    each timestamp has multiple strikes × expiries × put/call. We aggregate
    across the surface to produce scalar features per timestamp:
    - 25-delta skew (call IV - put IV at 25-delta equivalent)
    - ATM implied volatility (nearest-to-ATM strike)
    - Put/call OI ratio
    - Term structure slope (front month vs back month ATM IV)

    Only BTC and ETH have options data. For other pairs, fills with neutral values.
    """
    # Map pair to Deribit currency
    pair_upper = pair.upper().replace("-USDT", "")
    if pair_upper not in ("BTC", "ETH"):
        # No options data for this pair
        for col in ["options_atm_iv", "options_skew_25d", "options_pc_oi_ratio", "options_term_slope"]:
            candle_df[col] = 0.0
        return candle_df

    candle_idx = pd.to_datetime(candle_df["timestamp"], unit="s", utc=True)

    # Build query
    query = {"currency": pair_upper}
    if start_ts is not None or end_ts is not None:
        ts_filter = {}
        if start_ts is not None:
            ts_filter["$gte"] = start_ts - 24 * 3600 * 1000
        if end_ts is not None:
            ts_filter["$lte"] = end_ts
        if ts_filter:
            query["timestamp_utc"] = ts_filter

    docs = list(db["deribit_options_surface"].find(query).sort("timestamp_utc", 1))

    if not docs:
        for col in ["options_atm_iv", "options_skew_25d", "options_pc_oi_ratio", "options_term_slope"]:
            candle_df[col] = 0.0
        return candle_df

    odf = pd.DataFrame(docs)
    odf["_ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)

    # Group by timestamp and compute aggregate features
    features_by_ts = {}

    for ts_val, group in odf.groupby("_ts"):
        underlying_price = group["underlying_price"].iloc[0] if "underlying_price" in group.columns else None
        if underlying_price is None or underlying_price == 0:
            continue

        # Detect type column (Deribit uses "type", some schemas use "instrument_type")
        type_col = "type" if "type" in group.columns else "instrument_type" if "instrument_type" in group.columns else None
        if type_col:
            calls = group[group[type_col] == "call"]
            puts = group[group[type_col] == "put"]
        else:
            calls = pd.DataFrame()
            puts = pd.DataFrame()

        # ATM IV: nearest strike to underlying price
        atm_iv = 0.0
        if "strike" in group.columns and "mark_iv" in group.columns:
            group_valid = group[group["mark_iv"] > 0]
            if len(group_valid) > 0:
                nearest_idx = (group_valid["strike"] - underlying_price).abs().idxmin()
                atm_iv = group_valid.loc[nearest_idx, "mark_iv"]

        # 25-delta skew proxy: OTM put IV - OTM call IV at ~25% away from ATM
        skew_25d = 0.0
        if len(calls) > 0 and len(puts) > 0 and "strike" in group.columns:
            otm_calls = calls[calls["strike"] > underlying_price * 1.05]
            otm_puts = puts[puts["strike"] < underlying_price * 0.95]
            if len(otm_calls) > 0 and len(otm_puts) > 0:
                call_iv = otm_calls["mark_iv"].median() if "mark_iv" in otm_calls.columns else 0
                put_iv = otm_puts["mark_iv"].median() if "mark_iv" in otm_puts.columns else 0
                if call_iv > 0 and put_iv > 0:
                    skew_25d = put_iv - call_iv

        # Put/call OI ratio
        pc_oi = 0.0
        if "open_interest" in group.columns:
            call_oi = calls["open_interest"].sum() if len(calls) > 0 else 0
            put_oi = puts["open_interest"].sum() if len(puts) > 0 else 0
            if call_oi > 0:
                pc_oi = put_oi / call_oi

        features_by_ts[ts_val] = {
            "options_atm_iv": atm_iv,
            "options_skew_25d": skew_25d,
            "options_pc_oi_ratio": pc_oi,
            "options_term_slope": 0.0,  # TODO: needs expiry grouping
        }

    if not features_by_ts:
        for col in ["options_atm_iv", "options_skew_25d", "options_pc_oi_ratio", "options_term_slope"]:
            candle_df[col] = 0.0
        return candle_df

    feat_df = pd.DataFrame.from_dict(features_by_ts, orient="index").sort_index()
    feat_df = feat_df[~feat_df.index.duplicated(keep="last")]

    for col in ["options_atm_iv", "options_skew_25d", "options_pc_oi_ratio", "options_term_slope"]:
        if col in feat_df.columns:
            reindexed = feat_df[[col]].reindex(candle_idx, method="ffill")
            candle_df[col] = reindexed[col].fillna(0.0).values
        else:
            candle_df[col] = 0.0

    return candle_df
