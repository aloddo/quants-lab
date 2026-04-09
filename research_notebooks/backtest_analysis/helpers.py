"""
Backtest analysis helpers — shared query and plotting functions.

Usage:
    from helpers import get_mongo, load_walk_forward, load_backtest_trades, plot_equity_curve
"""
import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pymongo import MongoClient


def get_mongo(
    uri: Optional[str] = None,
    db_name: Optional[str] = None,
):
    """Get MongoDB database handle."""
    uri = uri or os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    db_name = db_name or os.getenv("MONGO_DATABASE", "quants_lab")
    client = MongoClient(uri)
    return client[db_name]


# ── Walk-forward results ──────────────────────────────────

def load_walk_forward(
    db,
    engine: str,
    run_id: Optional[str] = None,
) -> pd.DataFrame:
    """Load walk-forward results into a DataFrame.

    If run_id is None, loads the latest run.
    """
    query = {"engine": engine}
    if run_id:
        query["run_id"] = run_id
    else:
        # Find latest run_id
        latest = db.walk_forward_results.find_one(
            {"engine": engine},
            sort=[("created_at", -1)],
        )
        if latest:
            query["run_id"] = latest["run_id"]

    docs = list(db.walk_forward_results.find(query))
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs)
    df = df.drop(columns=["_id"], errors="ignore")
    return df


def walk_forward_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot walk-forward results: one row per pair with train/test PF comparison."""
    if df.empty:
        return pd.DataFrame()

    train = df[df["period_type"] == "train"].groupby("pair").agg(
        train_pf=("profit_factor", "mean"),
        train_wr=("win_rate", "mean"),
        train_trades=("trades", "sum"),
    )
    test = df[df["period_type"] == "test"].groupby("pair").agg(
        test_pf=("profit_factor", "mean"),
        test_wr=("win_rate", "mean"),
        test_trades=("trades", "sum"),
    )
    summary = train.join(test, how="outer").reset_index()
    summary["overfit_ratio"] = summary["train_pf"] / summary["test_pf"].replace(0, float("nan"))
    summary = summary.sort_values("test_pf", ascending=False)
    return summary


# ── Pair historical (backtest verdicts) ──────────────────

def load_pair_historical(db, engine: str) -> pd.DataFrame:
    """Load pair_historical verdicts as a DataFrame."""
    docs = list(db.pair_historical.find({"engine": engine}))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs).drop(columns=["_id"], errors="ignore")
    return df.sort_values("pair")


# ── Backtest trades ──────────────────────────────────────

def load_backtest_trades(
    db,
    engine: str,
    run_id: Optional[str] = None,
    pair: Optional[str] = None,
) -> pd.DataFrame:
    """Load individual backtest trades from backtest_trades collection."""
    query = {"engine": engine}
    if run_id:
        query["run_id"] = run_id
    if pair:
        query["pair"] = pair

    docs = list(db.backtest_trades.find(query))
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs).drop(columns=["_id"], errors="ignore")
    if "timestamp" in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return df


# ── Live candidates ──────────────────────────────────────

def load_candidates(
    db,
    engine: Optional[str] = None,
    disposition: Optional[str] = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """Load candidates from the candidates collection."""
    query = {}
    if engine:
        query["engine"] = engine
    if disposition:
        query["disposition"] = disposition

    docs = list(db.candidates.find(query).sort("timestamp_utc", -1).limit(limit))
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs).drop(columns=["_id"], errors="ignore")
    return df


# ── Plotting helpers ─────────────────────────────────────

def plot_equity_curve(trades_df: pd.DataFrame, title: str = "Equity Curve"):
    """Plot cumulative PnL from a trades DataFrame."""
    import matplotlib.pyplot as plt

    if trades_df.empty or "net_pnl_quote" not in trades_df.columns:
        print("No trade data to plot")
        return

    df = trades_df.sort_values("timestamp").copy()
    df["cum_pnl"] = df["net_pnl_quote"].cumsum()

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(range(len(df)), df["cum_pnl"], linewidth=1.5)
    ax.axhline(y=0, color="gray", linestyle="--", alpha=0.5)
    ax.fill_between(range(len(df)), df["cum_pnl"], 0, alpha=0.1)
    ax.set_xlabel("Trade #")
    ax.set_ylabel("Cumulative PnL (Quote)")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    return fig


def plot_drawdown(trades_df: pd.DataFrame, title: str = "Drawdown"):
    """Plot drawdown curve from a trades DataFrame."""
    import matplotlib.pyplot as plt

    if trades_df.empty or "net_pnl_quote" not in trades_df.columns:
        print("No trade data to plot")
        return

    df = trades_df.sort_values("timestamp").copy()
    df["cum_pnl"] = df["net_pnl_quote"].cumsum()
    df["peak"] = df["cum_pnl"].cummax()
    df["drawdown"] = df["cum_pnl"] - df["peak"]

    fig, ax = plt.subplots(figsize=(14, 3))
    ax.fill_between(range(len(df)), df["drawdown"], 0, color="red", alpha=0.3)
    ax.plot(range(len(df)), df["drawdown"], color="red", linewidth=1)
    ax.set_xlabel("Trade #")
    ax.set_ylabel("Drawdown (Quote)")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    return fig


def plot_close_type_breakdown(trades_df: pd.DataFrame, title: str = "Close Types"):
    """Pie chart of close types."""
    import matplotlib.pyplot as plt

    if trades_df.empty or "close_type" not in trades_df.columns:
        print("No trade data to plot")
        return

    counts = trades_df["close_type"].value_counts()
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(counts.values, labels=counts.index, autopct="%1.0f%%", startangle=90)
    ax.set_title(title)
    plt.tight_layout()
    return fig


def plot_pnl_distribution(trades_df: pd.DataFrame, title: str = "PnL Distribution"):
    """Histogram of per-trade PnL."""
    import matplotlib.pyplot as plt

    if trades_df.empty or "net_pnl_quote" not in trades_df.columns:
        print("No trade data to plot")
        return

    fig, ax = plt.subplots(figsize=(10, 4))
    pnl = trades_df["net_pnl_quote"].dropna()
    ax.hist(pnl, bins=50, edgecolor="black", alpha=0.7)
    ax.axvline(x=0, color="red", linestyle="--")
    ax.axvline(x=pnl.mean(), color="blue", linestyle="--", label=f"Mean: {pnl.mean():.2f}")
    ax.set_xlabel("PnL (Quote)")
    ax.set_ylabel("Count")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    return fig


def plot_train_vs_test(summary_df: pd.DataFrame, title: str = "Train vs Test PF"):
    """Scatter plot of train PF vs test PF per pair."""
    import matplotlib.pyplot as plt

    if summary_df.empty:
        print("No data to plot")
        return

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.scatter(
        summary_df["train_pf"],
        summary_df["test_pf"],
        alpha=0.7,
        s=50,
    )

    # Label each point with pair name
    for _, row in summary_df.iterrows():
        ax.annotate(
            row["pair"].replace("-USDT", ""),
            (row["train_pf"], row["test_pf"]),
            fontsize=7,
            alpha=0.7,
        )

    # Reference lines
    max_val = max(
        summary_df["train_pf"].max(),
        summary_df["test_pf"].max(),
        2.0,
    )
    ax.plot([0, max_val], [0, max_val], "k--", alpha=0.3, label="train = test")
    ax.axhline(y=1.0, color="red", linestyle=":", alpha=0.5, label="test PF = 1.0")
    ax.axhline(y=1.3, color="green", linestyle=":", alpha=0.5, label="test PF = 1.3 (ALLOW)")

    ax.set_xlabel("Train PF (in-sample)")
    ax.set_ylabel("Test PF (out-of-sample)")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    return fig
