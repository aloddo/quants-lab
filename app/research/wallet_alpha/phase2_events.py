#!/usr/bin/env python3
"""
Phase 2: Event Construction and Position Inference

Reads per-day Parquet fills from Phase 1, groups fills into atomic trade events,
reconstructs positions, classifies opens vs closes, and builds round trips.

Atomic Trade Event: fills by the same wallet, same coin, same direction, within
EVENT_GAP_MS of each other (default 5000ms = 5s).

Position Inference: Running position per (wallet, coin) pair. A fill that increases
|position| is an OPEN; a fill that decreases |position| is a CLOSE.

Usage:
    python -m app.research.wallet_alpha.phase2_events
"""
import logging
import os
import resource
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Memory guard: limit this process to 8GB to prevent OOM-killing the server
_EIGHT_GB = 8 * 1024 * 1024 * 1024
try:
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (min(_EIGHT_GB, hard), hard))
except (ValueError, resource.error):
    pass  # Some platforms don't support RLIMIT_AS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [phase2] %(levelname)s: %(message)s",
)
logger = logging.getLogger("phase2")

FILLS_DIR = Path("app/data/wallet_alpha/fills")
OUTPUT_DIR = Path("app/data/wallet_alpha")
EVENTS_PATH = OUTPUT_DIR / "events.parquet"
POSITIONS_PATH = OUTPUT_DIR / "positions.parquet"
ROUND_TRIPS_PATH = OUTPUT_DIR / "round_trips.parquet"

EVENT_GAP_MS = 5000  # 5 seconds max gap between fills in same event


def build_events(df: pd.DataFrame) -> pd.DataFrame:
    """Group fills into atomic trade events.

    An event is a cluster of fills by the same wallet, same coin, same effective
    direction (Buy or Sell), within EVENT_GAP_MS of each other.
    """
    if len(df) == 0:
        return pd.DataFrame()

    # Sort by wallet, coin, timestamp
    df = df.sort_values(["wallet", "coin", "timestamp_ms"]).reset_index(drop=True)

    # Detect event boundaries: new wallet, new coin, new side, or time gap > threshold
    new_wallet = df["wallet"] != df["wallet"].shift(1)
    new_coin = df["coin"] != df["coin"].shift(1)
    new_side = df["side"] != df["side"].shift(1)
    time_gap = (df["timestamp_ms"] - df["timestamp_ms"].shift(1)) > EVENT_GAP_MS

    # Event boundary is any of these conditions
    event_boundary = new_wallet | new_coin | new_side | time_gap
    df["event_id"] = event_boundary.cumsum()

    # Aggregate fills into events
    events = df.groupby("event_id").agg(
        wallet=("wallet", "first"),
        coin=("coin", "first"),
        side=("side", "first"),
        start_ts=("timestamp_ms", "min"),
        end_ts=("timestamp_ms", "max"),
        n_fills=("wallet", "count"),
        total_size=("size", "sum"),
        total_notional=("notional", "sum"),
        total_fee=("fee", "sum"),
        total_closed_pnl=("closed_pnl", "sum"),
        # VWAP = sum(price * size) / sum(size)
        _weighted_price=("notional", "sum"),  # notional = price * size
        _total_size=("size", "sum"),
        maker_fills=("is_maker", "sum"),
    ).reset_index()

    events["vwap_price"] = events["_weighted_price"] / events["_total_size"].clip(lower=1e-12)
    events["duration_ms"] = events["end_ts"] - events["start_ts"]
    events["is_burst"] = events["n_fills"] > 3
    events = events.drop(columns=["_weighted_price", "_total_size"])

    return events


def _classify_group(group_df: pd.DataFrame) -> pd.DataFrame:
    """Classify events for a single (wallet, coin) group using numpy for speed."""
    n = len(group_df)
    sides = group_df["side"].values
    sizes = group_df["total_size"].values

    # Compute signed sizes
    signed = np.where(sides == "Buy", sizes, -sizes)

    # Cumulative position
    pos_after = np.cumsum(signed)
    pos_before = np.concatenate([[0.0], pos_after[:-1]])

    abs_before = np.abs(pos_before)
    abs_after = np.abs(pos_after)

    # Classify
    types = np.full(n, "OPEN", dtype=object)
    types[abs_after < 1e-10] = "CLOSE"  # Now flat
    types[(abs_before >= 1e-10) & (abs_after < abs_before) & (abs_after >= 1e-10)] = "CLOSE"
    # FLIP: crossed zero (pos_before * pos_after < 0 and started non-flat)
    flip_mask = (abs_before >= 1e-10) & (pos_before * pos_after < 0)
    types[flip_mask] = "FLIP"
    # OPEN: was flat or magnitude increased in same direction
    # (default is already OPEN, so just need to handle the overrides above)

    group_df = group_df.copy()
    group_df["event_type"] = types
    group_df["position_before"] = pos_before
    group_df["position_after"] = pos_after

    return group_df


def infer_positions_and_classify(events: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct positions per (wallet, coin) and classify events as OPEN or CLOSE.

    OPEN: increases |position| (the predictive signal for copy trading)
    CLOSE: decreases |position| (exit quality metric, not for entry alpha)
    FLIP: crosses zero (partial close + partial open)

    Processes per (wallet, coin) group with numpy for speed.
    """
    if len(events) == 0:
        return events

    events = events.sort_values(["wallet", "coin", "start_ts"]).reset_index(drop=True)

    # Process each (wallet, coin) group
    groups = []
    n_groups = events.groupby(["wallet", "coin"]).ngroups
    logger.info(f"  Processing {n_groups:,} (wallet, coin) groups...")

    for _, group in events.groupby(["wallet", "coin"], sort=False):
        groups.append(_classify_group(group))

    result = pd.concat(groups, ignore_index=True)
    # Restore original sort order
    result = result.sort_values(["wallet", "coin", "start_ts"]).reset_index(drop=True)

    return result


def build_round_trips(events: pd.DataFrame) -> pd.DataFrame:
    """Link OPEN events to their corresponding CLOSE events to form round trips.

    A round trip starts with an OPEN event and ends when position returns to zero
    (or near zero). Handles partial closes by tracking remaining size.
    """
    if len(events) == 0:
        return pd.DataFrame()

    events = events.sort_values(["wallet", "coin", "start_ts"]).reset_index(drop=True)

    round_trips = []
    # Track open positions per (wallet, coin)
    open_stack = {}  # (wallet, coin) -> list of {event_id, side, remaining_size, entry_vwap, entry_ts, notional}

    for _, row in events.iterrows():
        key = (row["wallet"], row["coin"])

        if row["event_type"] == "OPEN":
            if key not in open_stack:
                open_stack[key] = []
            open_stack[key].append({
                "event_id": row["event_id"],
                "side": row["side"],
                "remaining_size": row["total_size"],
                "entry_vwap": row["vwap_price"],
                "entry_ts": row["start_ts"],
                "entry_notional": row["total_notional"],
            })

        elif row["event_type"] == "CLOSE" and key in open_stack and open_stack[key]:
            close_size = row["total_size"]
            close_vwap = row["vwap_price"]
            close_ts = row["start_ts"]

            # Match against open stack (FIFO)
            while close_size > 1e-10 and open_stack[key]:
                entry = open_stack[key][0]
                matched_size = min(close_size, entry["remaining_size"])

                if matched_size > 1e-10:
                    # Compute PnL
                    if entry["side"] == "Buy":
                        pnl_bps = (close_vwap - entry["entry_vwap"]) / entry["entry_vwap"] * 10000
                    else:
                        pnl_bps = (entry["entry_vwap"] - close_vwap) / entry["entry_vwap"] * 10000

                    pnl_usd = matched_size * abs(close_vwap - entry["entry_vwap"])
                    if entry["side"] == "Sell":
                        pnl_usd = -pnl_usd if close_vwap > entry["entry_vwap"] else pnl_usd

                    round_trips.append({
                        "wallet": row["wallet"],
                        "coin": row["coin"],
                        "entry_event_id": entry["event_id"],
                        "exit_event_id": row["event_id"],
                        "side": entry["side"],
                        "entry_vwap": entry["entry_vwap"],
                        "exit_vwap": close_vwap,
                        "size": matched_size,
                        "entry_ts": entry["entry_ts"],
                        "exit_ts": close_ts,
                        "hold_duration_s": (close_ts - entry["entry_ts"]) / 1000.0,
                        "pnl_bps": pnl_bps,
                        "pnl_usd": pnl_usd,
                    })

                entry["remaining_size"] -= matched_size
                close_size -= matched_size

                if entry["remaining_size"] < 1e-10:
                    open_stack[key].pop(0)

    return pd.DataFrame(round_trips)


def main(skip_round_trips: bool = False, round_trips_only: bool = False):
    t0 = time.time()

    EVENTS_DIR = OUTPUT_DIR / "events_daily"
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)

    fill_files = sorted(FILLS_DIR.glob("*.parquet"))
    if not fill_files:
        logger.error("No fill files found. Run phase1 first.")
        return

    logger.info(f"Processing {len(fill_files)} daily fill files...")

    if round_trips_only:
        logger.info("Skipping Phase 2.1 + 2.2 (round_trips_only mode)")
        event_files = sorted(EVENTS_DIR.glob("*.parquet"))
        if not event_files:
            logger.error("No event files found. Run full phase2 first.")
            return
        # Jump straight to round trip building
        skip_round_trips = False
        # Fall through to Phase 2.3 below
        total_events = sum(
            pq.ParquetFile(f).metadata.num_rows for f in event_files
        )
        logger.info(f"Found {len(event_files)} event files, {total_events:,} events")
    else:
        event_files = None  # Will be set after Phase 2.2

    # Phase 2.1: Build events per day (memory-efficient)
    if not round_trips_only:
        total_events = 0
        total_fills = 0
    for i, ff in enumerate(fill_files if not round_trips_only else []):
        date_str = ff.stem
        out_path = EVENTS_DIR / f"{date_str}.parquet"

        if out_path.exists():
            logger.info(f"[{i+1}/{len(fill_files)}] {date_str}: exists, skipping")
            total_events += len(pd.read_parquet(out_path, columns=["wallet"]))
            continue

        logger.info(f"[{i+1}/{len(fill_files)}] Processing {date_str}...")
        t_day = time.time()

        df = pd.read_parquet(ff)
        total_fills += len(df)
        events = build_events(df)

        if len(events) > 0:
            events["date"] = date_str
            events.to_parquet(out_path, index=False)
            total_events += len(events)
            logger.info(
                f"  {len(df):,} fills -> {len(events):,} events "
                f"({(events['n_fills'] > 1).sum():,} multi-fill) "
                f"in {time.time() - t_day:.1f}s"
            )

        del df, events  # Free memory

    if not round_trips_only:
        logger.info(f"\nTotal: {total_fills:,} fills -> {total_events:,} events")

    # Phase 2.2: Position inference (streaming, per-day with carryover state)
    # Uses groupby + numpy for speed instead of row-by-row iteration
    if not round_trips_only:
        logger.info("\nInferring positions (streaming per day)...")
        positions = {}  # (wallet, coin) -> signed position (carries across days)
        event_files = sorted(EVENTS_DIR.glob("*.parquet"))

    total_open = 0
    total_close = 0
    total_flip = 0

    for i, ef in enumerate(event_files if not round_trips_only else []):
        date_str = ef.stem
        logger.info(f"  [{i+1}/{len(event_files)}] Classifying {date_str}...")
        t_cls = time.time()

        events = pd.read_parquet(ef)
        events = events.sort_values(["wallet", "coin", "start_ts"]).reset_index(drop=True)

        # Vectorized: compute signed sizes
        signed_sizes = np.where(
            events["side"].values == "Buy",
            events["total_size"].values,
            -events["total_size"].values,
        )

        # Group by (wallet, coin), process each group with numpy
        n = len(events)
        event_types = np.empty(n, dtype=object)
        pos_before_arr = np.zeros(n)
        pos_after_arr = np.zeros(n)

        # Build group indices
        group_keys = events["wallet"] + "|" + events["coin"]
        groups = events.groupby(group_keys, sort=False)

        for gk, idx in groups.groups.items():
            parts = gk.split("|", 1)
            wallet, coin = parts[0], parts[1]
            key = (wallet, coin)
            start_pos = positions.get(key, 0.0)

            indices = idx.values
            g_signed = signed_sizes[indices]

            # Cumulative position with carryover
            cum = np.cumsum(g_signed) + start_pos
            before = np.concatenate([[start_pos], cum[:-1]])

            pos_before_arr[indices] = before
            pos_after_arr[indices] = cum

            abs_b = np.abs(before)
            abs_a = np.abs(cum)

            types = np.full(len(indices), "OPEN", dtype=object)
            types[abs_a < 1e-10] = "CLOSE"
            close_mask = (abs_b >= 1e-10) & (abs_a < abs_b) & (abs_a >= 1e-10)
            types[close_mask] = "CLOSE"
            flip_mask = (abs_b >= 1e-10) & (before * cum < 0)
            types[flip_mask] = "FLIP"

            event_types[indices] = types
            positions[key] = cum[-1]

        events["event_type"] = event_types
        events["position_before"] = pos_before_arr
        events["position_after"] = pos_after_arr

        # Count
        unique, counts = np.unique(event_types, return_counts=True)
        type_dict = dict(zip(unique, counts))
        total_open += type_dict.get("OPEN", 0)
        total_close += type_dict.get("CLOSE", 0)
        total_flip += type_dict.get("FLIP", 0)

        events.to_parquet(ef, index=False)
        elapsed_cls = time.time() - t_cls
        logger.info(f"    {len(events):,} events classified in {elapsed_cls:.1f}s "
                     f"(O={type_dict.get('OPEN',0):,} C={type_dict.get('CLOSE',0):,} F={type_dict.get('FLIP',0):,})")

        del events

    if not round_trips_only:
        logger.info(f"Event types: OPEN={total_open:,} CLOSE={total_close:,} FLIP={total_flip:,}")

    if skip_round_trips:
        logger.info("\nSkipping round trips (--skip-round-trips)")
        elapsed = time.time() - t0
        logger.info(f"\nPhase 2 complete in {elapsed:.0f}s ({elapsed/60:.1f}min)")
        return

    # Phase 2.3: Build round trips (streaming, chunked writes to avoid OOM)
    # NOTE: This is the slowest part. Can be skipped for faster iteration.
    if round_trips_only:
        event_files = sorted(EVENTS_DIR.glob("*.parquet"))
    logger.info("\nBuilding round trips (streaming, chunked)...")
    t_rt = time.time()
    open_stack = {}  # (wallet, coin) -> deque of entries
    from collections import deque

    MAX_OPEN_PER_KEY = 500  # Cap open stack depth per (wallet, coin) to bound memory

    rt_cols = ["wallet", "coin", "side", "entry_vwap", "exit_vwap", "size",
               "entry_ts", "exit_ts", "hold_duration_s", "pnl_bps", "pnl_usd"]
    rt_schema = pa.schema([
        ("wallet", pa.string()), ("coin", pa.string()), ("side", pa.string()),
        ("entry_vwap", pa.float64()), ("exit_vwap", pa.float64()), ("size", pa.float64()),
        ("entry_ts", pa.int64()), ("exit_ts", pa.int64()),
        ("hold_duration_s", pa.float64()), ("pnl_bps", pa.float64()), ("pnl_usd", pa.float64()),
    ])
    rt_writer = pq.ParquetWriter(str(ROUND_TRIPS_PATH), rt_schema)
    chunk_buffer = []
    CHUNK_SIZE = 500_000  # Write every 500K round trips
    total_rt_count = 0

    for file_idx, ef in enumerate(event_files):
        # Only load columns needed for round trips + filter to OPEN/CLOSE
        events = pd.read_parquet(
            ef,
            columns=["wallet", "coin", "side", "event_type", "total_size", "vwap_price", "start_ts"],
        )
        events = events[events["event_type"].isin(["OPEN", "CLOSE"])]
        events = events.sort_values(["wallet", "coin", "start_ts"]).reset_index(drop=True)

        # Use numpy arrays for speed
        wallets = events["wallet"].values
        coins = events["coin"].values
        sides = events["side"].values
        types = events["event_type"].values
        sizes = events["total_size"].values
        vwaps = events["vwap_price"].values
        timestamps = events["start_ts"].values

        for j in range(len(events)):
            key = (wallets[j], coins[j])

            if types[j] == "OPEN":
                if key not in open_stack:
                    open_stack[key] = deque(maxlen=MAX_OPEN_PER_KEY)
                open_stack[key].append((sides[j], sizes[j], vwaps[j], timestamps[j]))

            elif types[j] == "CLOSE" and key in open_stack and open_stack[key]:
                close_size = sizes[j]
                close_vwap = vwaps[j]
                close_ts = timestamps[j]

                while close_size > 1e-10 and open_stack[key]:
                    e_side, e_rem, e_vwap, e_ts = open_stack[key][0]
                    matched = min(close_size, e_rem)

                    if matched > 1e-10:
                        if e_side == "Buy":
                            pnl_bps = (close_vwap - e_vwap) / e_vwap * 10000
                            pnl_usd = matched * (close_vwap - e_vwap)
                        else:
                            pnl_bps = (e_vwap - close_vwap) / e_vwap * 10000
                            pnl_usd = matched * (e_vwap - close_vwap)

                        chunk_buffer.append((
                            wallets[j], coins[j], e_side, e_vwap, close_vwap,
                            matched, e_ts, close_ts,
                            (close_ts - e_ts) / 1000.0, pnl_bps, pnl_usd,
                        ))
                        total_rt_count += 1

                        # Flush chunk to disk when buffer is full
                        if len(chunk_buffer) >= CHUNK_SIZE:
                            chunk_df = pd.DataFrame(chunk_buffer, columns=rt_cols)
                            rt_writer.write_table(pa.Table.from_pandas(chunk_df, schema=rt_schema))
                            chunk_buffer.clear()
                            logger.info(f"    Flushed chunk, total round trips: {total_rt_count:,}")

                    new_rem = e_rem - matched
                    close_size -= matched
                    if new_rem < 1e-10:
                        open_stack[key].popleft()
                    else:
                        open_stack[key][0] = (e_side, new_rem, e_vwap, e_ts)

        if (file_idx + 1) % 5 == 0:
            # Report memory usage and open stack size
            rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
            stack_keys = len(open_stack)
            stack_entries = sum(len(v) for v in open_stack.values())
            logger.info(
                f"    Day {file_idx+1}: {total_rt_count:,} RTs, "
                f"RSS={rss_mb:.0f}MB, stack={stack_keys:,} keys/{stack_entries:,} entries"
            )

        del events

        # Prune stale open_stack entries every 10 days to bound memory
        # Remove keys where the newest entry is >7 days old (likely abandoned positions)
        if (file_idx + 1) % 10 == 0 and open_stack:
            cutoff_ts = timestamps[-1] - 7 * 86400 * 1000  # 7 days in ms
            stale_keys = [
                k for k, v in open_stack.items()
                if v and v[-1][3] < cutoff_ts  # newest entry timestamp
            ]
            for k in stale_keys:
                del open_stack[k]
            if stale_keys:
                logger.info(f"    Pruned {len(stale_keys):,} stale open-stack keys")

    # Flush remaining buffer
    if chunk_buffer:
        chunk_df = pd.DataFrame(chunk_buffer, columns=rt_cols)
        rt_writer.write_table(pa.Table.from_pandas(chunk_df, schema=rt_schema))
        chunk_buffer.clear()

    rt_writer.close()
    logger.info(f"Round trips: {total_rt_count:,} in {time.time() - t_rt:.0f}s")

    if total_rt_count > 0:
        # Read back a sample for stats (avoid loading full file)
        rt_sample = pd.read_parquet(ROUND_TRIPS_PATH, columns=["hold_duration_s", "pnl_bps"])
        logger.info(f"Median hold duration: {rt_sample['hold_duration_s'].median():.0f}s")
        logger.info(f"Mean PnL: {rt_sample['pnl_bps'].mean():.2f} bps")
        logger.info(f"Round trips saved to {ROUND_TRIPS_PATH}")
        del rt_sample

    elapsed = time.time() - t0
    logger.info(f"\nPhase 2 complete in {elapsed:.0f}s ({elapsed/60:.1f}min)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-round-trips", action="store_true")
    parser.add_argument("--round-trips-only", action="store_true",
                        help="Skip event building + classification, only build round trips")
    args = parser.parse_args()
    main(skip_round_trips=args.skip_round_trips, round_trips_only=args.round_trips_only)
