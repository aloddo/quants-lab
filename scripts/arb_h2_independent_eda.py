from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "app/data/cache/arb_trades"
ANALYSIS_CSV = ROOT / "app/data/cache/arb_analysis.csv"
FUNDING_CSV = ROOT / "app/data/cache/arb_funding_summary.csv"
DOC_PATH = ROOT / "docs/arb_h2_independent_eda.md"
OUT_PREFIX = ROOT / "app/data/cache/arb_h2_eda"

POSITION_USD = 200.0
RT_FEE_BPS = 24.0
SPIKE_THRESHOLD_BPS = 150.0
BASELINE_WINDOWS = ("1h", "6h", "24h")
PRIMARY_BASELINE = "6h"
ENTRY_DELAYS_SEC = (0, 1, 5, 15, 30, 60, 300)
EXIT_HORIZONS_SEC = (300, 1800, 3600, 14400, 86400)
REVERT_TARGET_BPS = 25.0
EPISODE_BREAK_GAP_SEC = 60


@dataclass
class PairContext:
    symbol: str
    df: pd.DataFrame
    baseline: pd.Series
    abs_excess: pd.Series
    volume_p50: float
    volume_p90: float
    trades_p50: float
    trades_p90: float
    coverage_ratio: float
    calendar_days: float
    observed_days: float


def iter_pair_files() -> list[Path]:
    return sorted(DATA_DIR.glob("*/*_1s_merged_90d.parquet"))


def load_pair(path: Path) -> PairContext:
    symbol = path.name.split("_")[0]
    cols = [
        "spread_close_bps",
        "bn_volume_usd",
        "bb_volume_usd",
        "bn_trade_count",
        "bb_trade_count",
        "bn_close",
        "bb_close",
    ]
    df = pd.read_parquet(path, columns=cols).sort_index()
    df.index = pd.to_datetime(df.index, utc=True)

    df["total_volume_usd"] = df["bn_volume_usd"].astype(float) + df["bb_volume_usd"].astype(float)
    df["total_trade_count"] = df["bn_trade_count"].astype(float) + df["bb_trade_count"].astype(float)
    spread = df["spread_close_bps"].astype(float)
    baseline = spread.shift(1).rolling(PRIMARY_BASELINE, min_periods=30).median()
    abs_excess = (spread - baseline).abs()

    elapsed_sec = max((df.index.max() - df.index.min()).total_seconds(), 1.0)
    return PairContext(
        symbol=symbol,
        df=df,
        baseline=baseline,
        abs_excess=abs_excess,
        volume_p50=float(df["total_volume_usd"].median()),
        volume_p90=float(df["total_volume_usd"].quantile(0.90)),
        trades_p50=float(df["total_trade_count"].median()),
        trades_p90=float(df["total_trade_count"].quantile(0.90)),
        coverage_ratio=float(len(df) / (elapsed_sec + 1.0)),
        calendar_days=float(elapsed_sec / 86400.0),
        observed_days=float(len(df) / 86400.0),
    )


def baseline_sensitivity(context: PairContext) -> pd.DataFrame:
    out = []
    spread = context.df["spread_close_bps"].astype(float)
    for window in BASELINE_WINDOWS:
        baseline = spread.shift(1).rolling(window, min_periods=30).median()
        abs_excess = (spread - baseline).abs()
        gap_s = context.df.index.to_series().diff().dt.total_seconds().fillna(0)
        above = abs_excess >= SPIKE_THRESHOLD_BPS
        starts = above & (~above.shift(fill_value=False) | (gap_s > EPISODE_BREAK_GAP_SEC))
        out.append(
            {
                "symbol": context.symbol,
                "baseline_window": window,
                "event_bars": int(above.sum()),
                "episode_starts": int(starts.sum()),
                "events_per_calendar_day": float(starts.sum() / max(context.calendar_days, 1e-9)),
                "events_per_observed_day": float(starts.sum() / max(context.observed_days, 1e-9)),
                "baseline_p95_1h_drift_bps": float((baseline - baseline.shift(freq="1h")).abs().quantile(0.95)),
                "baseline_p99_1h_drift_bps": float((baseline - baseline.shift(freq="1h")).abs().quantile(0.99)),
            }
        )
    return pd.DataFrame(out)


def build_episodes(context: PairContext) -> pd.DataFrame:
    df = context.df.copy()
    df["baseline_bps"] = context.baseline
    df["excess_bps"] = df["spread_close_bps"] - df["baseline_bps"]
    df["abs_excess_bps"] = df["excess_bps"].abs()
    df = df.dropna(subset=["baseline_bps", "abs_excess_bps"])
    if df.empty:
        return pd.DataFrame()

    gap_s = df.index.to_series().diff().dt.total_seconds().fillna(0)
    above = df["abs_excess_bps"] >= SPIKE_THRESHOLD_BPS
    starts = above & (~above.shift(fill_value=False) | (gap_s > EPISODE_BREAK_GAP_SEC))
    start_positions = np.flatnonzero(starts.to_numpy())
    if len(start_positions) == 0:
        return pd.DataFrame()

    rows: list[dict] = []
    timestamps = df.index
    abs_excess_arr = df["abs_excess_bps"].to_numpy()
    excess_arr = df["excess_bps"].to_numpy()
    spread_arr = df["spread_close_bps"].to_numpy()
    baseline_arr = df["baseline_bps"].to_numpy()
    vol_arr = df["total_volume_usd"].to_numpy()
    trades_arr = df["total_trade_count"].to_numpy()

    for episode_id, start_pos in enumerate(start_positions, start=1):
        start_ts = timestamps[start_pos]
        end_pos = start_pos
        while end_pos + 1 < len(df):
            next_gap = (timestamps[end_pos + 1] - timestamps[end_pos]).total_seconds()
            if next_gap > EPISODE_BREAK_GAP_SEC or abs_excess_arr[end_pos + 1] < SPIKE_THRESHOLD_BPS:
                break
            end_pos += 1

        peak_slice = slice(start_pos, end_pos + 1)
        local_peak_pos = start_pos + int(np.argmax(abs_excess_arr[peak_slice]))
        peak_ts = timestamps[local_peak_pos]

        row = {
            "symbol": context.symbol,
            "episode_id": episode_id,
            "start_ts": start_ts,
            "end_ts": timestamps[end_pos],
            "peak_ts": peak_ts,
            "duration_sec": float((timestamps[end_pos] - start_ts).total_seconds()),
            "start_spread_bps": float(spread_arr[start_pos]),
            "start_baseline_bps": float(baseline_arr[start_pos]),
            "start_excess_bps": float(excess_arr[start_pos]),
            "start_abs_excess_bps": float(abs_excess_arr[start_pos]),
            "peak_abs_excess_bps": float(abs_excess_arr[local_peak_pos]),
            "start_total_volume_usd": float(vol_arr[start_pos]),
            "start_total_trade_count": float(trades_arr[start_pos]),
            "start_vol_vs_median": float(vol_arr[start_pos] / max(context.volume_p50, 1e-9)),
            "start_vol_vs_p90": float(vol_arr[start_pos] / max(context.volume_p90, 1e-9)),
            "start_trades_vs_median": float(trades_arr[start_pos] / max(context.trades_p50, 1e-9)),
            "start_trades_vs_p90": float(trades_arr[start_pos] / max(context.trades_p90, 1e-9)),
            "coverage_ratio": context.coverage_ratio,
            "calendar_days": context.calendar_days,
            "observed_days": context.observed_days,
        }

        for delay_sec in ENTRY_DELAYS_SEC:
            entry_target = start_ts + pd.Timedelta(seconds=delay_sec)
            entry_pos = int(df.index.searchsorted(entry_target, side="left"))
            prefix = f"d{delay_sec}s"
            if entry_pos >= len(df):
                row[f"{prefix}_entry_found"] = False
                continue

            entry_ts = timestamps[entry_pos]
            row[f"{prefix}_entry_found"] = True
            row[f"{prefix}_entry_ts"] = entry_ts
            row[f"{prefix}_actual_delay_sec"] = float((entry_ts - start_ts).total_seconds())
            row[f"{prefix}_entry_abs_excess_bps"] = float(abs_excess_arr[entry_pos])
            row[f"{prefix}_entry_excess_bps"] = float(excess_arr[entry_pos])
            row[f"{prefix}_entry_volume_usd"] = float(vol_arr[entry_pos])
            row[f"{prefix}_entry_trade_count"] = float(trades_arr[entry_pos])
            row[f"{prefix}_missed_threshold"] = bool(abs_excess_arr[entry_pos] < SPIKE_THRESHOLD_BPS)

            for horizon_sec in EXIT_HORIZONS_SEC:
                end_target = entry_ts + pd.Timedelta(seconds=horizon_sec)
                future_end = int(df.index.searchsorted(end_target, side="right"))
                hprefix = f"{prefix}_h{horizon_sec}s"
                if future_end <= entry_pos + 1:
                    row[f"{hprefix}_has_window"] = False
                    continue

                future_abs = abs_excess_arr[entry_pos:future_end]
                future_idx = np.arange(entry_pos, future_end)
                min_local = int(np.argmin(future_abs))
                best_pos = future_idx[min_local]
                best_abs = float(future_abs[min_local])
                entry_abs = float(abs_excess_arr[entry_pos])
                compression = entry_abs - best_abs
                entry_baseline = float(baseline_arr[entry_pos])
                fixed_abs = np.abs(spread_arr[entry_pos:future_end] - entry_baseline)
                min_fixed_local = int(np.argmin(fixed_abs))
                best_fixed_pos = future_idx[min_fixed_local]
                best_fixed_abs = float(fixed_abs[min_fixed_local])
                fixed_compression = entry_abs - best_fixed_abs

                row[f"{hprefix}_has_window"] = True
                row[f"{hprefix}_best_ts"] = timestamps[best_pos]
                row[f"{hprefix}_best_abs_excess_bps"] = best_abs
                row[f"{hprefix}_compression_bps"] = compression
                row[f"{hprefix}_capture_pct"] = float(compression / entry_abs) if entry_abs > 0 else np.nan
                row[f"{hprefix}_net_pnl_usd"] = float((compression - RT_FEE_BPS) * POSITION_USD / 10000.0)
                row[f"{hprefix}_reverted_25bp"] = bool(best_abs <= REVERT_TARGET_BPS)
                row[f"{hprefix}_best_fixed_ts"] = timestamps[best_fixed_pos]
                row[f"{hprefix}_best_fixed_abs_excess_bps"] = best_fixed_abs
                row[f"{hprefix}_fixed_compression_bps"] = fixed_compression
                row[f"{hprefix}_fixed_capture_pct"] = float(fixed_compression / entry_abs) if entry_abs > 0 else np.nan
                row[f"{hprefix}_fixed_net_pnl_usd"] = float((fixed_compression - RT_FEE_BPS) * POSITION_USD / 10000.0)
                row[f"{hprefix}_fixed_reverted_25bp"] = bool(best_fixed_abs <= REVERT_TARGET_BPS)

        rows.append(row)

    return pd.DataFrame(rows)


def summarize_delay(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for delay_sec in ENTRY_DELAYS_SEC:
        prefix = f"d{delay_sec}s"
        valid = events[events.get(f"{prefix}_entry_found", False).fillna(False)].copy()
        if valid.empty:
            continue
        for horizon_sec in EXIT_HORIZONS_SEC:
            hprefix = f"{prefix}_h{horizon_sec}s"
            usable = valid[valid.get(f"{hprefix}_has_window", False).fillna(False)].copy()
            if usable.empty:
                continue
            rows.append(
                {
                    "delay_sec": delay_sec,
                    "horizon_sec": horizon_sec,
                    "episodes": len(usable),
                    "threshold_missed_pct": float(usable[f"{prefix}_missed_threshold"].mean()),
                    "median_actual_delay_sec": float(usable[f"{prefix}_actual_delay_sec"].median()),
                    "median_entry_abs_excess_bps": float(usable[f"{prefix}_entry_abs_excess_bps"].median()),
                    "median_best_abs_excess_bps": float(usable[f"{hprefix}_best_abs_excess_bps"].median()),
                    "median_compression_bps": float(usable[f"{hprefix}_compression_bps"].median()),
                    "mean_compression_bps": float(usable[f"{hprefix}_compression_bps"].mean()),
                    "revert_25bp_rate": float(usable[f"{hprefix}_reverted_25bp"].mean()),
                    "positive_net_rate": float((usable[f"{hprefix}_net_pnl_usd"] > 0).mean()),
                    "mean_net_pnl_usd": float(usable[f"{hprefix}_net_pnl_usd"].mean()),
                    "median_net_pnl_usd": float(usable[f"{hprefix}_net_pnl_usd"].median()),
                    "fixed_median_best_abs_excess_bps": float(usable[f"{hprefix}_best_fixed_abs_excess_bps"].median()),
                    "fixed_median_compression_bps": float(usable[f"{hprefix}_fixed_compression_bps"].median()),
                    "fixed_mean_compression_bps": float(usable[f"{hprefix}_fixed_compression_bps"].mean()),
                    "fixed_revert_25bp_rate": float(usable[f"{hprefix}_fixed_reverted_25bp"].mean()),
                    "fixed_positive_net_rate": float((usable[f"{hprefix}_fixed_net_pnl_usd"] > 0).mean()),
                    "fixed_mean_net_pnl_usd": float(usable[f"{hprefix}_fixed_net_pnl_usd"].mean()),
                    "fixed_median_net_pnl_usd": float(usable[f"{hprefix}_fixed_net_pnl_usd"].median()),
                    "daily_net_pnl_calendar": float(usable[f"{hprefix}_net_pnl_usd"].sum() / events["calendar_days"].max()),
                    "fixed_daily_net_pnl_calendar": float(usable[f"{hprefix}_fixed_net_pnl_usd"].sum() / events["calendar_days"].max()),
                }
            )
    return pd.DataFrame(rows)


def summarize_pairs(events: pd.DataFrame, funding: pd.DataFrame | None) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    rows = []
    funding = funding.copy() if funding is not None else pd.DataFrame()
    if not funding.empty:
        funding["symbol"] = funding["symbol"].astype(str)

    for symbol, g in events.groupby("symbol"):
        row = {
            "symbol": symbol,
            "episodes": len(g),
            "events_per_calendar_day": float(len(g) / g["calendar_days"].max()),
            "events_per_observed_day": float(len(g) / g["observed_days"].max()),
            "median_peak_abs_excess_bps": float(g["peak_abs_excess_bps"].median()),
            "p95_peak_abs_excess_bps": float(g["peak_abs_excess_bps"].quantile(0.95)),
            "median_start_volume_usd": float(g["start_total_volume_usd"].median()),
            "median_start_trade_count": float(g["start_total_trade_count"].median()),
            "low_liquidity_start_pct": float(((g["start_vol_vs_median"] < 0.5) | (g["start_trades_vs_median"] < 0.5)).mean()),
            "ghost_start_pct": float(((g["start_total_trade_count"] <= 2) | (g["start_total_volume_usd"] < 25)).mean()),
            "coverage_ratio": float(g["coverage_ratio"].max()),
        }

        for delay_sec, horizon_sec in ((0, 300), (0, 3600), (5, 3600), (60, 3600), (300, 14400), (300, 86400)):
            prefix = f"d{delay_sec}s_h{horizon_sec}s"
            found_col = f"d{delay_sec}s_entry_found"
            has_col = f"d{delay_sec}s_h{horizon_sec}s_has_window"
            pnl_col = f"d{delay_sec}s_h{horizon_sec}s_net_pnl_usd"
            rev_col = f"d{delay_sec}s_h{horizon_sec}s_reverted_25bp"
            miss_col = f"d{delay_sec}s_missed_threshold"
            if found_col in g and has_col in g:
                usable = g[g[found_col].fillna(False) & g[has_col].fillna(False)]
                row[f"{prefix}_episodes"] = len(usable)
                row[f"{prefix}_revert_rate"] = float(usable[rev_col].mean()) if len(usable) else np.nan
                row[f"{prefix}_mean_net_pnl_usd"] = float(usable[pnl_col].mean()) if len(usable) else np.nan
                row[f"{prefix}_sum_net_pnl_usd"] = float(usable[pnl_col].sum()) if len(usable) else np.nan
                row[f"{prefix}_threshold_missed_pct"] = float(usable[miss_col].mean()) if len(usable) else np.nan
                row[f"{prefix}_fixed_revert_rate"] = float(usable[f"d{delay_sec}s_h{horizon_sec}s_fixed_reverted_25bp"].mean()) if len(usable) else np.nan
                row[f"{prefix}_fixed_mean_net_pnl_usd"] = float(usable[f"d{delay_sec}s_h{horizon_sec}s_fixed_net_pnl_usd"].mean()) if len(usable) else np.nan
                row[f"{prefix}_fixed_sum_net_pnl_usd"] = float(usable[f"d{delay_sec}s_h{horizon_sec}s_fixed_net_pnl_usd"].sum()) if len(usable) else np.nan

        if not funding.empty:
            match = funding[funding["symbol"] == symbol]
            if not match.empty:
                mean_rate = float(match["mean_rate_pct"].iloc[0])
                row["funding_mean_rate_pct"] = mean_rate
                row["funding_annual_pct"] = float(match["annual_pct"].iloc[0])
                long_perp = g["start_excess_bps"] < 0
                short_perp = g["start_excess_bps"] > 0
                carry_aligned = (long_perp & (mean_rate < 0)) | (short_perp & (mean_rate > 0))
                row["carry_aligned_pct"] = float(carry_aligned.mean())

        rows.append(row)

    return pd.DataFrame(rows).sort_values("d300s_h14400s_sum_net_pnl_usd", ascending=False, na_position="last")


def summarize_concurrency(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    starts = events.groupby("start_ts").size().rename("simultaneous_starts").reset_index()
    starts["minute"] = starts["start_ts"].dt.floor("min")
    minute = starts.groupby("minute")["simultaneous_starts"].sum().rename("starts_in_minute").reset_index()
    return starts.merge(minute, on="minute", how="left")


def summarize_capital(events: pd.DataFrame, calendar_days: float) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    rows = []
    for delay_sec, horizon_sec in ((300, 14400), (300, 86400)):
        has_col = f"d{delay_sec}s_h{horizon_sec}s_has_window"
        entry_col = f"d{delay_sec}s_entry_ts"
        exit_col = f"d{delay_sec}s_h{horizon_sec}s_best_fixed_ts"
        pnl_col = f"d{delay_sec}s_h{horizon_sec}s_fixed_net_pnl_usd"
        if has_col not in events or entry_col not in events or exit_col not in events:
            continue

        usable = events[events[has_col].fillna(False)].dropna(subset=[entry_col, exit_col, pnl_col]).copy()
        if usable.empty:
            continue

        holds = (pd.to_datetime(usable[exit_col], utc=True) - pd.to_datetime(usable[entry_col], utc=True)).dt.total_seconds()
        entries_per_day = len(usable) / max(calendar_days, 1e-9)
        avg_concurrent = float(entries_per_day * holds.mean() / 86400.0)
        max_concurrent = 0
        changes = []
        for start_ts, end_ts in usable[[entry_col, exit_col]].itertuples(index=False):
            start_ns = pd.Timestamp(start_ts).value
            end_ns = pd.Timestamp(end_ts).value
            changes.append((start_ns, 1))
            changes.append((end_ns, -1))
        changes.sort()
        cur = 0
        for _, delta in changes:
            cur += delta
            if cur > max_concurrent:
                max_concurrent = cur

        total_pnl = float(usable[pnl_col].sum())
        daily_pnl = total_pnl / max(calendar_days, 1e-9)
        rows.append(
            {
                "delay_sec": delay_sec,
                "horizon_sec": horizon_sec,
                "episodes": len(usable),
                "entries_per_day": entries_per_day,
                "mean_hold_sec": float(holds.mean()),
                "median_hold_sec": float(holds.median()),
                "avg_concurrent_positions": avg_concurrent,
                "max_concurrent_positions": int(max_concurrent),
                "avg_capital_required_usd": float(avg_concurrent * POSITION_USD),
                "max_capital_required_usd": float(max_concurrent * POSITION_USD),
                "fixed_daily_pnl_usd": daily_pnl,
                "fixed_daily_pnl_per_200usd_slot": float(daily_pnl / max(avg_concurrent, 1e-9)),
            }
        )
    return pd.DataFrame(rows)


def choose_worst_event(events: pd.DataFrame) -> pd.Series | None:
    if events.empty:
        return None
    candidates = events[events["d300s_h86400s_has_window"].fillna(False)].copy()
    if candidates.empty:
        return None
    sort_cols = ["d300s_h86400s_net_pnl_usd", "peak_abs_excess_bps"]
    return candidates.sort_values(sort_cols, ascending=[True, False]).iloc[0]


def build_report(
    coverage: pd.DataFrame,
    sensitivity: pd.DataFrame,
    events: pd.DataFrame,
    delay_summary: pd.DataFrame,
    pair_summary: pd.DataFrame,
    concurrency: pd.DataFrame,
    capital: pd.DataFrame,
    worst_event: pd.Series | None,
) -> str:
    total_event_bars = int(sensitivity.loc[sensitivity["baseline_window"] == PRIMARY_BASELINE, "event_bars"].sum())
    total_episodes = len(events)
    calendar_days = float(coverage["calendar_days"].median())
    observed_days = float(coverage["observed_days"].sum())

    primary_delay = delay_summary[(delay_summary["delay_sec"] == 300) & (delay_summary["horizon_sec"] == 14400)]
    immediate_delay = delay_summary[(delay_summary["delay_sec"] == 0) & (delay_summary["horizon_sec"] == 3600)]
    long_delay = delay_summary[(delay_summary["delay_sec"] == 300) & (delay_summary["horizon_sec"] == 86400)]

    lines: list[str] = []
    lines.append("# H2 Independent EDA")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    if not primary_delay.empty:
        row = primary_delay.iloc[0]
        cap = capital[(capital["delay_sec"] == 300) & (capital["horizon_sec"] == 14400)]
        cap_text = ""
        if not cap.empty:
            cap_row = cap.iloc[0]
            cap_text = (
                f" That headline also needs about `{cap_row['avg_concurrent_positions']:.1f}` overlapping `$200` slots on average "
                f"(`~${cap_row['avg_capital_required_usd']:.0f}` deployed), so it is only `{cap_row['fixed_daily_pnl_per_200usd_slot']:.2f}` dollars/day per `$200` capital slot."
            )
        lines.append(
            f"The raw `$185/day` claim is not robust. On the primary causal definition "
            f"(trailing `{PRIMARY_BASELINE}` median, episode-level counting, `5m` entry delay, `4h` exit horizon, `$200` notional, `{RT_FEE_BPS:.0f}`bp round-trip fees), "
            f"the study finds `{int(row['episodes'])}` executable episodes, `{row['fixed_mean_net_pnl_usd']:.2f}` mean net PnL per episode "
            f"when reversion is measured back to the frozen entry baseline, and `{row['fixed_daily_net_pnl_calendar']:.2f}` dollars/day across calendar time across the full overlapping book."
            f"{cap_text}"
        )
    else:
        lines.append("The raw `$185/day` claim is not robust. Under stricter causal rules the event set becomes too thin to support the headline.")
    lines.append("")
    lines.append("The biggest artifacts are:")
    lines.append("")
    lines.append(
        f"- Counting bars instead of episodes: `{total_event_bars}` threshold-hit bars collapse to `{total_episodes}` distinct spike episodes on the `{PRIMARY_BASELINE}` baseline."
    )
    lines.append(
        f"- Treating sparse event-time bars as continuous 1s data: median venue-pair coverage is only `{coverage['coverage_ratio'].median():.2%}` of clock seconds."
    )
    lines.append(
        f"- Entering at the peak: after a `5m` delay, the threshold is already gone on `{primary_delay.iloc[0]['threshold_missed_pct']:.1%}` of usable episodes."
        if not primary_delay.empty
        else "- Entering at the peak materially overstates capture; delayed entry misses a large fraction of spikes."
    )
    lines.append("")
    lines.append("## What Survived")
    lines.append("")
    if not immediate_delay.empty:
        row = immediate_delay.iloc[0]
        lines.append(
            f"Some spikes do compress. With zero delay and a `1h` exit horizon, the dynamic baseline shows `{row['revert_25bp_rate']:.1%}` reversion inside `{REVERT_TARGET_BPS:.0f}`bp, "
            f"but the stricter frozen-baseline rate is only `{row['fixed_revert_25bp_rate']:.1%}`."
        )
    if not long_delay.empty:
        row = long_delay.iloc[0]
        lines.append(
            f"But realism removes most of the edge. With `5m` delay and `24h` patience, the dynamic baseline still shows `{row['mean_net_pnl_usd']:.2f}` mean net PnL, "
            f"while the frozen-baseline version drops to `{row['fixed_mean_net_pnl_usd']:.2f}` with only `{row['fixed_positive_net_rate']:.1%}` of episodes above fees."
        )
    lines.append("")
    lines.append("## Failure Modes")
    lines.append("")

    worst_cov = coverage.sort_values("coverage_ratio").head(5)[["symbol", "coverage_ratio", "median_gap_s", "p95_gap_s"]]
    lines.append("Sparse coverage is severe in many names:")
    lines.append("")
    lines.append(worst_cov.to_markdown(index=False))
    lines.append("")

    stable = sensitivity[sensitivity["baseline_window"] == PRIMARY_BASELINE][
        ["symbol", "episode_starts", "events_per_calendar_day", "baseline_p95_1h_drift_bps", "baseline_p99_1h_drift_bps"]
    ].sort_values("baseline_p99_1h_drift_bps", ascending=False).head(10)
    lines.append(f"The structural level is not perfectly stable even after smoothing with `{PRIMARY_BASELINE}`:")
    lines.append("")
    lines.append(stable.to_markdown(index=False))
    lines.append("")

    ghost_pct = float(((events["start_total_trade_count"] <= 2) | (events["start_total_volume_usd"] < 25)).mean()) if len(events) else np.nan
    lowliq_pct = float(((events["start_vol_vs_median"] < 0.5) | (events["start_trades_vs_median"] < 0.5)).mean()) if len(events) else np.nan
    lines.append(
        f"`{ghost_pct:.1%}` of episode starts occur on effectively ghost bars (<=2 trades across both venues or < $25 total USD volume), "
        f"and `{lowliq_pct:.1%}` start in below-median liquidity."
    )
    lines.append("")

    if not concurrency.empty:
        lines.append("Spike clustering across pairs is real:")
        lines.append("")
        cluster_summary = pd.DataFrame(
            [
                {
                    "metric": "max simultaneous starts at same second",
                    "value": int(concurrency["simultaneous_starts"].max()),
                },
                {
                    "metric": "95th pct starts per minute",
                    "value": float(concurrency["starts_in_minute"].quantile(0.95)),
                },
                {
                    "metric": "max starts in one minute",
                    "value": int(concurrency["starts_in_minute"].max()),
                },
            ]
        )
        lines.append(cluster_summary.to_markdown(index=False))
        lines.append("")

    if not capital.empty:
        lines.append("Capital overlap is the real bottleneck:")
        lines.append("")
        lines.append(capital.to_markdown(index=False))
        lines.append("")

    if worst_event is not None:
        lines.append("## Worst Event")
        lines.append("")
        lines.append(
            f"`{worst_event['symbol']}` on `{pd.Timestamp(worst_event['start_ts']).isoformat()}` is the ugliest delayed-entry failure in the sample. "
            f"It started at `{worst_event['start_abs_excess_bps']:.1f}`bp excess, peaked at `{worst_event['peak_abs_excess_bps']:.1f}`bp, "
            f"and still produced `{worst_event['d300s_h86400s_net_pnl_usd']:.2f}` dollars net after a `5m` delayed entry and `24h` look-ahead."
        )
        lines.append("")

    if "carry_aligned_pct" in pair_summary.columns:
        carry = pair_summary["carry_aligned_pct"].dropna()
        if not carry.empty:
            lines.append("## Carry Check")
            lines.append("")
            lines.append(
                f"Funding alignment matters. Across pairs with cached funding summaries, the median share of episodes that are directionally aligned with funding carry is `{carry.median():.1%}`."
            )
            top_carry = pair_summary.dropna(subset=["carry_aligned_pct"])[
                ["symbol", "carry_aligned_pct", "funding_mean_rate_pct", "funding_annual_pct", "d300s_h14400s_sum_net_pnl_usd"]
            ].sort_values("carry_aligned_pct", ascending=False).head(10)
            lines.append("")
            lines.append(top_carry.to_markdown(index=False))
            lines.append("")

    top_pairs = pair_summary[
        [
            "symbol",
            "episodes",
            "events_per_calendar_day",
            "ghost_start_pct",
            "coverage_ratio",
            "d300s_h14400s_sum_net_pnl_usd",
            "d300s_h14400s_fixed_sum_net_pnl_usd",
            "d300s_h14400s_revert_rate",
            "d300s_h14400s_fixed_revert_rate",
        ]
    ].head(15)
    bottom_pairs = pair_summary[
        [
            "symbol",
            "episodes",
            "ghost_start_pct",
            "coverage_ratio",
            "d300s_h14400s_sum_net_pnl_usd",
            "d300s_h14400s_fixed_sum_net_pnl_usd",
            "d300s_h86400s_sum_net_pnl_usd",
            "d300s_h86400s_fixed_sum_net_pnl_usd",
        ]
    ].tail(15)

    lines.append("## Pair Cross-Section")
    lines.append("")
    lines.append("Best pairs under the strict `5m` delayed-entry / `4h` horizon lens:")
    lines.append("")
    lines.append(top_pairs.to_markdown(index=False))
    lines.append("")
    lines.append("Worst pairs under the same lens:")
    lines.append("")
    lines.append(bottom_pairs.to_markdown(index=False))
    lines.append("")
    lines.append("## Bottom Line")
    lines.append("")
    lines.append(
        "H2 is directionally true but economically fragile: extreme spikes often compress, yet much of the headline edge comes from non-causal baseline choices, bar-level double counting, sparse-clock sampling, and assuming fills at the peak."
    )
    lines.append(
        "Once the structural level is frozen at entry instead of allowed to drift, the broad `$185/day` machine disappears. What remains is a selective, liquidity-aware excursion trade in a handful of pairs."
    )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    coverage_rows = []
    sensitivity_frames = []
    event_frames = []

    funding = pd.read_csv(FUNDING_CSV) if FUNDING_CSV.exists() else pd.DataFrame()

    for path in iter_pair_files():
        context = load_pair(path)
        coverage_rows.append(
            {
                "symbol": context.symbol,
                "rows": len(context.df),
                "start": context.df.index.min(),
                "end": context.df.index.max(),
                "calendar_days": context.calendar_days,
                "observed_days": context.observed_days,
                "coverage_ratio": context.coverage_ratio,
                "median_gap_s": float(context.df.index.to_series().diff().dt.total_seconds().median()),
                "p95_gap_s": float(context.df.index.to_series().diff().dt.total_seconds().quantile(0.95)),
                "volume_p50": context.volume_p50,
                "volume_p90": context.volume_p90,
                "trades_p50": context.trades_p50,
                "trades_p90": context.trades_p90,
            }
        )
        sensitivity_frames.append(baseline_sensitivity(context))
        events = build_episodes(context)
        if not events.empty:
            event_frames.append(events)

    coverage = pd.DataFrame(coverage_rows).sort_values("coverage_ratio")
    sensitivity = pd.concat(sensitivity_frames, ignore_index=True) if sensitivity_frames else pd.DataFrame()
    events = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
    delay_summary = summarize_delay(events) if not events.empty else pd.DataFrame()
    pair_summary = summarize_pairs(events, funding)
    concurrency = summarize_concurrency(events)
    capital = summarize_capital(events, float(coverage["calendar_days"].median())) if not events.empty else pd.DataFrame()
    worst_event = choose_worst_event(events)

    coverage.to_csv(f"{OUT_PREFIX}_coverage.csv", index=False)
    sensitivity.to_csv(f"{OUT_PREFIX}_baseline_sensitivity.csv", index=False)
    events.to_csv(f"{OUT_PREFIX}_events.csv", index=False)
    delay_summary.to_csv(f"{OUT_PREFIX}_delay_summary.csv", index=False)
    pair_summary.to_csv(f"{OUT_PREFIX}_pair_summary.csv", index=False)
    concurrency.to_csv(f"{OUT_PREFIX}_concurrency.csv", index=False)
    capital.to_csv(f"{OUT_PREFIX}_capital.csv", index=False)

    report = build_report(coverage, sensitivity, events, delay_summary, pair_summary, concurrency, capital, worst_event)
    DOC_PATH.write_text(report)
    print(report)


if __name__ == "__main__":
    main()
