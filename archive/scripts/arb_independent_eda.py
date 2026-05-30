from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy.optimize import curve_fit
from scipy.stats import pearsonr, spearmanr


ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = ROOT / "app/data/cache/arb_backtest"
UNIVERSE_CSV = ROOT / "app/data/cache/arb_universe.csv"
REPORT_PATH = ROOT / "docs/arb_independent_eda.md"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "quants_lab"
LAGS = [1, 5, 12, 60, 288]
SIGNAL_THRESHOLD_BPS = 60.0


@dataclass
class FundingSummary:
    pair: str
    observations: int
    positive_count: int
    negative_count: int
    zero_count: int
    mean_rate: float
    median_abs_rate: float
    p90_abs_rate: float
    pearson_corr_abs_funding_abs_spread: float | None
    spearman_corr_abs_funding_abs_spread: float | None


def hurst_exponent(series: pd.Series, max_lag: int = 512) -> float:
    clean = series.dropna().astype(float)
    n = len(clean)
    if n < 128:
        return np.nan

    upper = min(max_lag, max(16, n // 8))
    lags = np.unique(np.round(np.logspace(np.log10(2), np.log10(upper), 20)).astype(int))
    tau = []
    valid_lags = []
    values = clean.to_numpy()

    for lag in lags:
        diff = values[lag:] - values[:-lag]
        std = np.std(diff)
        if np.isfinite(std) and std > 0:
            tau.append(std)
            valid_lags.append(lag)

    if len(valid_lags) < 6:
        return np.nan

    slope, _ = np.polyfit(np.log(valid_lags), np.log(tau), 1)
    return float(slope)


def decay_model(log_volume: np.ndarray, asymptote: float, scale: float, decay: float) -> np.ndarray:
    return asymptote + scale * np.exp(-np.maximum(log_volume, 0.0) / np.maximum(decay, 1e-9))


def pair_to_universe_symbol(pair: str) -> str:
    return pair.replace("-USDT", "USDT")


def load_backtest_pair(path: Path) -> tuple[str, pd.DataFrame]:
    pair = path.name.replace("_5m_spread.parquet", "")
    df = pd.read_parquet(path).copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
    df.index = pd.to_datetime(df.index, utc=True)
    return pair, df.sort_index()


def compute_spread_metrics(pair: str, df: pd.DataFrame) -> dict:
    spread = df["spread_bps"].astype(float)
    abs_spread = df["abs_spread"].astype(float)

    acf = {f"lag_{lag}": spread.autocorr(lag=lag) for lag in LAGS}
    hourly_abs = abs_spread.groupby(df.index.hour).mean().reindex(range(24))

    gt60 = spread[spread.abs() > SIGNAL_THRESHOLD_BPS]
    pos_gt60 = int((gt60 > 0).sum())
    neg_gt60 = int((gt60 < 0).sum())
    total_gt60 = len(gt60)

    wider_direction = "balanced"
    if total_gt60 > 0:
        diff = pos_gt60 - neg_gt60
        dominance = abs(diff) / total_gt60
        if dominance >= 0.10:
            wider_direction = "BUY_BN" if diff > 0 else "BUY_BB"

    return {
        "pair": pair,
        "rows": len(df),
        "start": df.index.min(),
        "end": df.index.max(),
        "mean_spread_bps": float(spread.mean()),
        "mean_abs_spread_bps": float(abs_spread.mean()),
        "p95_abs_spread_bps": float(abs_spread.quantile(0.95)),
        "max_abs_spread_bps": float(abs_spread.max()),
        "hurst": hurst_exponent(spread),
        **acf,
        "gt60_total": total_gt60,
        "gt60_pos": pos_gt60,
        "gt60_neg": neg_gt60,
        "gt60_pos_frac": float(pos_gt60 / total_gt60) if total_gt60 else np.nan,
        "wider_direction": wider_direction,
        "hourly_abs": hourly_abs,
    }


def fit_volume_curve(universe: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    viable = universe.loc[universe["viable"].astype(bool)].copy()
    viable = viable[(viable["min_volume_usd"] > 0) & viable["spread_bps"].notna()].copy()
    viable["log_min_volume"] = np.log10(viable["min_volume_usd"])

    x = viable["log_min_volume"].to_numpy(dtype=float)
    y = viable["spread_bps"].to_numpy(dtype=float)

    lower = max(0.0, float(np.percentile(y, 5) * 0.5))
    initial = [float(np.percentile(y, 15)), float(np.percentile(y, 95) - np.percentile(y, 15)), 0.75]
    bounds = ([0.0, 0.0, 0.05], [max(np.max(y), 500.0), max(np.max(y) * 2, 500.0), 10.0])
    params, _ = curve_fit(decay_model, x, y, p0=initial, bounds=bounds, maxfev=20000)
    asymptote, scale, decay = [float(v) for v in params]

    grid = np.linspace(x.min(), x.max(), 400)
    pred = decay_model(grid, *params)
    plateau_target = asymptote + max(5.0, 0.10 * scale)
    collapse_idx = int(np.argmax(pred <= plateau_target))
    if pred[collapse_idx] > plateau_target:
        collapse_log = float(grid[-1])
    else:
        collapse_log = float(grid[collapse_idx])
    collapse_volume = float(10 ** collapse_log)

    pearson = pearsonr(viable["log_min_volume"], viable["spread_bps"])
    spearman = spearmanr(viable["min_volume_usd"], viable["spread_bps"])

    fit = {
        "n": len(viable),
        "asymptote_bps": asymptote,
        "scale_bps": scale,
        "decay_log10": decay,
        "collapse_volume_usd": collapse_volume,
        "collapse_log10_volume": collapse_log,
        "pearson_r_log_volume_spread": float(pearson.statistic),
        "pearson_p": float(pearson.pvalue),
        "spearman_r_volume_spread": float(spearman.statistic),
        "spearman_p": float(spearman.pvalue),
        "plateau_target_bps": float(plateau_target),
    }
    return viable, fit


def load_funding_summaries(pair_metrics: pd.DataFrame) -> tuple[list[FundingSummary], str | None]:
    pair_index = {row["pair"]: row for _, row in pair_metrics.iterrows()}
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000, connectTimeoutMS=3000, socketTimeoutMS=3000)

    try:
        db = client[MONGO_DB]
        db.command("ping")
    except Exception as exc:
        return [], f"MongoDB unavailable from this sandbox: {type(exc).__name__}: {exc}"

    summaries: list[FundingSummary] = []

    for pair in pair_metrics["pair"]:
        row = pair_index[pair]
        query = {
            "pair": pair,
            "timestamp_utc": {
                "$gte": int(pd.Timestamp(row["start"]).timestamp() * 1000),
                "$lte": int(pd.Timestamp(row["end"]).timestamp() * 1000),
            },
        }
        cursor = db["bybit_funding_rates"].find(query, {"_id": 0, "pair": 1, "timestamp_utc": 1, "funding_rate": 1}).sort(
            "timestamp_utc", 1
        )
        docs = list(cursor)
        if not docs:
            summaries.append(
                FundingSummary(
                    pair=pair,
                    observations=0,
                    positive_count=0,
                    negative_count=0,
                    zero_count=0,
                    mean_rate=np.nan,
                    median_abs_rate=np.nan,
                    p90_abs_rate=np.nan,
                    pearson_corr_abs_funding_abs_spread=np.nan,
                    spearman_corr_abs_funding_abs_spread=np.nan,
                )
            )
            continue

        fdf = pd.DataFrame(docs)
        fdf["dt"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
        fdf = fdf.drop_duplicates("dt", keep="last").set_index("dt").sort_index()
        funding = fdf["funding_rate"].astype(float)

        spread_df = load_backtest_pair(BACKTEST_DIR / f"{pair}_5m_spread.parquet")[1]
        joined = spread_df[["abs_spread"]].join(funding.rename("funding_rate"), how="left")
        joined["funding_rate"] = joined["funding_rate"].ffill()
        joined = joined.dropna(subset=["funding_rate", "abs_spread"])

        pearson_corr = np.nan
        spearman_corr = np.nan
        if len(joined) >= 10 and joined["funding_rate"].abs().nunique() > 1 and joined["abs_spread"].nunique() > 1:
            pearson_corr = pearsonr(joined["funding_rate"].abs(), joined["abs_spread"]).statistic
            spearman_corr = spearmanr(joined["funding_rate"].abs(), joined["abs_spread"]).statistic

        summaries.append(
            FundingSummary(
                pair=pair,
                observations=int(len(funding)),
                positive_count=int((funding > 0).sum()),
                negative_count=int((funding < 0).sum()),
                zero_count=int((funding == 0).sum()),
                mean_rate=float(funding.mean()),
                median_abs_rate=float(funding.abs().median()),
                p90_abs_rate=float(funding.abs().quantile(0.90)),
                pearson_corr_abs_funding_abs_spread=float(pearson_corr) if np.isfinite(pearson_corr) else np.nan,
                spearman_corr_abs_funding_abs_spread=float(spearman_corr) if np.isfinite(spearman_corr) else np.nan,
            )
        )

    return summaries, None


def format_hour_profile(series: pd.Series) -> str:
    top = series.sort_values(ascending=False).head(5)
    return ", ".join(f"{int(hour):02d}:00={value:.1f}bp" for hour, value in top.items())


def markdown_table(df: pd.DataFrame, columns: Iterable[str]) -> str:
    view = df.loc[:, list(columns)].copy()
    for col in view.columns:
        if pd.api.types.is_float_dtype(view[col]):
            view[col] = view[col].map(lambda v: "" if pd.isna(v) else f"{v:.4f}")
        elif pd.api.types.is_datetime64_any_dtype(view[col]):
            view[col] = view[col].dt.strftime("%Y-%m-%d")
    return view.to_markdown(index=False)


def main() -> None:
    universe = pd.read_csv(UNIVERSE_CSV)
    pair_frames = [load_backtest_pair(path) for path in sorted(BACKTEST_DIR.glob("*.parquet"))]
    metrics = [compute_spread_metrics(pair, df) for pair, df in pair_frames]
    metrics_df = pd.DataFrame([{k: v for k, v in m.items() if k != "hourly_abs"} for m in metrics]).sort_values(
        "mean_abs_spread_bps", ascending=False
    )
    hour_profiles = {m["pair"]: m["hourly_abs"] for m in metrics}

    viable_df, volume_fit = fit_volume_curve(universe)
    funding_summaries, funding_error = load_funding_summaries(metrics_df)
    funding_df = pd.DataFrame([vars(s) for s in funding_summaries]) if funding_summaries else pd.DataFrame()

    avg_acf = {lag: float(metrics_df[f"lag_{lag}"].mean()) for lag in LAGS}
    avg_hurst = float(metrics_df["hurst"].mean())
    dominant_buy_bn = int((metrics_df["wider_direction"] == "BUY_BN").sum())
    dominant_buy_bb = int((metrics_df["wider_direction"] == "BUY_BB").sum())
    balanced_dirs = int((metrics_df["wider_direction"] == "balanced").sum())

    top_pairs = metrics_df.nlargest(5, "mean_abs_spread_bps")[["pair", "mean_abs_spread_bps", "p95_abs_spread_bps"]]
    persistent_pairs = metrics_df.nlargest(5, "lag_288")[["pair", "lag_288", "hurst"]]
    directional_pairs = metrics_df.sort_values("gt60_total", ascending=False)[
        ["pair", "gt60_total", "gt60_pos_frac", "wider_direction", "p95_abs_spread_bps"]
    ]

    print("Key findings")
    print("============")
    print(
        f"14-pair mean autocorrelation: lag1={avg_acf[1]:.3f}, lag5={avg_acf[5]:.3f}, "
        f"lag12={avg_acf[12]:.3f}, lag60={avg_acf[60]:.3f}, lag288={avg_acf[288]:.3f}"
    )
    print(f"Average Hurst exponent across pairs: {avg_hurst:.3f}")
    print("Top pairs by mean abs spread:")
    for _, row in top_pairs.iterrows():
        print(f"  {row['pair']}: mean_abs={row['mean_abs_spread_bps']:.2f}bp, p95_abs={row['p95_abs_spread_bps']:.2f}bp")
    print(
        f"Directionality split: BUY_BN-dominant={dominant_buy_bn}, BUY_BB-dominant={dominant_buy_bb}, balanced={balanced_dirs}"
    )
    print(
        f"Viable-universe spread vs min volume: pearson(log volume, spread)={volume_fit['pearson_r_log_volume_spread']:.3f}, "
        f"spearman(volume, spread)={volume_fit['spearman_r_volume_spread']:.3f}"
    )
    print(
        f"Estimated collapse threshold: ${volume_fit['collapse_volume_usd']:,.0f} min daily volume, "
        f"plateau near {volume_fit['asymptote_bps']:.2f}bp"
    )
    if funding_error:
        print(f"Funding analysis blocked: {funding_error}")
    elif not funding_df.empty:
        avg_funding_corr = funding_df["spearman_corr_abs_funding_abs_spread"].mean()
        print(f"Funding vs spread mean Spearman correlation across pairs: {avg_funding_corr:.3f}")

    report_lines: list[str] = []
    report_lines.append("# Independent EDA: Binance Spot x Bybit Perp Arb")
    report_lines.append("")
    report_lines.append("## Scope")
    report_lines.append("")
    report_lines.append("- Spread dynamics across the 14 cached backtest pairs (`5m` resolution).")
    report_lines.append("- Direction split of large opportunities (`|spread_bps| > 60`).")
    report_lines.append("- Funding-rate linkage for the same 14 pairs using `bybit_funding_rates` when MongoDB is reachable.")
    report_lines.append("- Cross-sectional relation between spread and minimum exchange volume from `arb_universe.csv`.")
    report_lines.append("")
    report_lines.append("## Executive Summary")
    report_lines.append("")
    report_lines.append(
        f"- Mean spread autocorrelation decays from `{avg_acf[1]:.3f}` at 5 minutes to `{avg_acf[288]:.3f}` at 24 hours, "
        "so spreads are sticky intraday but much less so on a full-day horizon."
    )
    report_lines.append(
        f"- The average Hurst exponent is `{avg_hurst:.3f}`. Values below `0.5` indicate mean reversion; above `0.5` indicate persistence."
    )
    report_lines.append(
        f"- Directional asymmetry is mixed: `{dominant_buy_bn}` pairs skew to `BUY_BN`, `{dominant_buy_bb}` skew to `BUY_BB`, and `{balanced_dirs}` are roughly balanced."
    )
    report_lines.append(
        f"- Across viable pairs in the universe, spread falls as liquidity rises: Pearson on `log10(min_volume_usd)` is "
        f"`{volume_fit['pearson_r_log_volume_spread']:.3f}` and Spearman is `{volume_fit['spearman_r_volume_spread']:.3f}`."
    )
    report_lines.append(
        f"- A simple exponential decay fit implies spreads largely collapse by about `${volume_fit['collapse_volume_usd']:,.0f}` "
        f"minimum daily volume, where the expected spread is within `{volume_fit['plateau_target_bps'] - volume_fit['asymptote_bps']:.2f}`bp of its asymptote."
    )
    report_lines.append("")
    report_lines.append("## Spread Dynamics")
    report_lines.append("")
    report_lines.append("### Per-Pair Summary")
    report_lines.append("")
    report_lines.append(
        markdown_table(
            metrics_df,
            [
                "pair",
                "rows",
                "mean_abs_spread_bps",
                "p95_abs_spread_bps",
                "hurst",
                "lag_1",
                "lag_5",
                "lag_12",
                "lag_60",
                "lag_288",
            ],
        )
    )
    report_lines.append("")
    report_lines.append("### Highest-Spread Pairs")
    report_lines.append("")
    for _, row in top_pairs.iterrows():
        report_lines.append(
            f"- `{row['pair']}`: mean absolute spread `{row['mean_abs_spread_bps']:.2f}`bp, 95th percentile `{row['p95_abs_spread_bps']:.2f}`bp."
        )
    report_lines.append("")
    report_lines.append("### Most Persistent Pairs")
    report_lines.append("")
    for _, row in persistent_pairs.iterrows():
        report_lines.append(f"- `{row['pair']}`: 24h autocorr `{row['lag_288']:.3f}`, Hurst `{row['hurst']:.3f}`.")
    report_lines.append("")
    report_lines.append("### UTC Hour-of-Day Absolute Spread")
    report_lines.append("")
    for pair in metrics_df["pair"]:
        report_lines.append(f"- `{pair}`: {format_hour_profile(hour_profiles[pair])}")
    report_lines.append("")
    report_lines.append("## Direction Analysis")
    report_lines.append("")
    report_lines.append(
        "Positive `spread_bps` means Binance spot is richer than Bybit perp (`BUY_BB`), and negative values mean the opposite (`BUY_BN`)."
    )
    report_lines.append("")
    report_lines.append(markdown_table(directional_pairs, ["pair", "gt60_total", "gt60_pos_frac", "wider_direction", "p95_abs_spread_bps"]))
    report_lines.append("")
    report_lines.append("## Funding Correlation")
    report_lines.append("")
    if funding_error:
        report_lines.append(
            f"Funding analysis could not be executed from this session because the local MongoDB endpoint at `{MONGO_URI}` is not reachable from the sandbox."
        )
        report_lines.append("")
        report_lines.append(
            "The repo code expects `bybit_funding_rates` documents keyed by `pair` and `timestamp_utc`, with the funding value stored in `funding_rate`."
        )
        report_lines.append(
            "This schema is referenced in [scripts/x1_backtest_eq.py](/Users/hermes/quants-lab/scripts/x1_backtest_eq.py:47) and [feature_assembler.py](/Users/hermes/quants-lab/app/ml/feature_assembler.py:365)."
        )
        report_lines.append("")
        report_lines.append(f"Observed error: `{funding_error}`")
    elif funding_df.empty:
        report_lines.append("MongoDB was reachable but returned no funding history for the 14 backtest pairs in the requested date range.")
    else:
        report_lines.append(markdown_table(funding_df, funding_df.columns))
        report_lines.append("")
        report_lines.append(
            f"Mean Spearman correlation between `|funding_rate|` and `abs_spread` across pairs: `{funding_df['spearman_corr_abs_funding_abs_spread'].mean():.3f}`."
        )
    report_lines.append("")
    report_lines.append("## Volume vs Spread")
    report_lines.append("")
    report_lines.append(
        "Model fit on viable pairs only: `spread_bps = asymptote + scale * exp(-log10(min_volume_usd) / decay)`."
    )
    report_lines.append("")
    report_lines.append(
        f"- Viable pairs used: `{volume_fit['n']}`"
    )
    report_lines.append(
        f"- Asymptotic spread: `{volume_fit['asymptote_bps']:.2f}`bp"
    )
    report_lines.append(
        f"- Decay scale: `{volume_fit['scale_bps']:.2f}`bp"
    )
    report_lines.append(
        f"- Decay constant in `log10(volume)`: `{volume_fit['decay_log10']:.3f}`"
    )
    report_lines.append(
        f"- Estimated collapse threshold: `${volume_fit['collapse_volume_usd']:,.0f}` minimum daily volume"
    )
    report_lines.append(
        f"- Pearson `r(log10(volume), spread)`: `{volume_fit['pearson_r_log_volume_spread']:.3f}` with `p={volume_fit['pearson_p']:.3g}`"
    )
    report_lines.append(
        f"- Spearman `rho(volume, spread)`: `{volume_fit['spearman_r_volume_spread']:.3f}` with `p={volume_fit['spearman_p']:.3g}`"
    )
    report_lines.append("")
    report_lines.append("### Highest-Spread Viable Pairs")
    report_lines.append("")
    report_lines.append(
        markdown_table(
            viable_df.sort_values("spread_bps", ascending=False).head(15),
            ["symbol", "spread_bps", "min_volume_usd", "tier", "direction"],
        )
    )
    report_lines.append("")
    report_lines.append("## Notes")
    report_lines.append("")
    report_lines.append("- Backtest sample lengths differ by pair; `ALICE` and `NOM` have materially shorter histories than the others.")
    report_lines.append("- Hour-of-day analysis uses UTC, matching the timestamp index embedded in the parquet files.")
    report_lines.append("- The collapse threshold is model-based and should be interpreted as a practical turnover region, not a sharp market microstructure boundary.")
    report_lines.append("")

    REPORT_PATH.write_text("\n".join(report_lines))
    print(f"Report written to {REPORT_PATH}")


if __name__ == "__main__":
    main()
