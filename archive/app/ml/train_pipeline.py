#!/usr/bin/env python3
"""
End-to-end ML training pipeline for Exotic Signal Factory v2.

Usage:
    set -a && source .env && set +a
    python -m app.ml.train_pipeline

Steps:
1. Assemble feature vectors from backtest_trades + all feature stores
2. Train entry classifier (P(profitable))
3. Train magnitude estimator (expected |move|)
4. Output SHAP analysis + governance report
"""
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient

from app.ml.feature_assembler import (
    FeatureAssembler,
    ALL_FEATURE_COLS,
    LABEL_COLS,
)
from app.ml.trainer import SignalModelTrainer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def run_training_pipeline(
    engines: list = None,
    n_optuna_trials: int = 50,
    min_trades: int = 100,
):
    """Run the full training pipeline."""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    mongo_db = os.environ.get("MONGO_DATABASE", "quants_lab")

    client = AsyncIOMotorClient(mongo_uri)
    db = client[mongo_db]

    logger.info("=" * 60)
    logger.info("EXOTIC SIGNAL FACTORY v2 — ML TRAINING PIPELINE")
    logger.info("=" * 60)

    # ── Step 1: Assemble training data ───────────────────────
    logger.info("\n[1/4] Assembling feature vectors...")
    assembler = FeatureAssembler(db=db)
    df = await assembler.build_training_dataset(engines=engines, min_trades=min_trades)

    if df.empty:
        logger.error("No training data assembled. Check backtest_trades collection.")
        return

    # Report data quality
    logger.info(f"\nDataset shape: {df.shape}")
    logger.info(f"Engines: {df['engine'].value_counts().to_dict()}")
    logger.info(f"Pairs: {df['pair'].nunique()}")
    logger.info(f"Profitable: {df['profitable'].mean()*100:.1f}%")
    logger.info(f"Close types: {df['close_type'].value_counts().to_dict()}")

    # Feature coverage report
    logger.info("\nFeature coverage (% non-null):")
    for col in ALL_FEATURE_COLS:
        if col in df.columns:
            coverage = df[col].notna().mean() * 100
            logger.info(f"  {col:30s}: {coverage:5.1f}%")
        else:
            logger.info(f"  {col:30s}: MISSING")

    # Filter to features with > 10% coverage
    available_features = [
        col for col in ALL_FEATURE_COLS
        if col in df.columns and df[col].notna().mean() > 0.10
    ]
    logger.info(f"\nUsable features ({len(available_features)} / {len(ALL_FEATURE_COLS)}):")
    for f in available_features:
        logger.info(f"  {f}")

    if len(available_features) < 3:
        logger.error("Fewer than 3 usable features. Need more data collection.")
        return

    # Feature selection: if prior SHAP rankings exist, keep only top features
    # to reduce dimensionality and overfitting
    MAX_FEATURES = 12
    if len(available_features) > MAX_FEATURES:
        # Check if we have a prior model with SHAP rankings
        prior_top = _get_prior_shap_features()
        if prior_top:
            # Keep features that were in prior top-10, plus any new ones up to MAX_FEATURES
            ranked = [f for f in prior_top if f in available_features]
            remaining = [f for f in available_features if f not in ranked]
            available_features = (ranked + remaining)[:MAX_FEATURES]
            logger.info(f"Feature selection: kept {len(available_features)} features (SHAP-ranked)")
        else:
            available_features = available_features[:MAX_FEATURES]
            logger.info(f"Feature selection: kept top {MAX_FEATURES} features (no prior SHAP)")

        for f in available_features:
            logger.info(f"  {f}")

    # ── Step 2: Train Entry Classifier ───────────────────────
    logger.info("\n[2/4] Training entry classifier...")
    trainer = SignalModelTrainer(n_optuna_trials=n_optuna_trials)

    entry_model, entry_meta = trainer.train_entry_classifier(
        df, feature_cols=available_features
    )

    # ── Step 3: Train Magnitude Estimator ────────────────────
    logger.info("\n[3/4] Training magnitude estimator...")
    if "net_pnl_pct" in df.columns:
        df["abs_return_pct"] = df["net_pnl_pct"].abs()
        mag_model, mag_meta = trainer.train_magnitude_estimator(
            df, feature_cols=available_features, target_col="abs_return_pct"
        )
    else:
        logger.warning("No net_pnl_pct column — skipping magnitude estimator")
        mag_meta = None

    # ── Step 4: Governance Report ────────────────────────────
    logger.info("\n[4/4] Governance report...")
    logger.info("=" * 60)
    logger.info("GOVERNANCE REPORT")
    logger.info("=" * 60)

    # Entry classifier gates
    logger.info(f"\nEntry Classifier:")
    logger.info(f"  Holdout AUC: {entry_meta.val_auc:.4f} (gate: > 0.55)")
    passed_auc = entry_meta.val_auc > 0.55
    logger.info(f"  AUC gate: {'PASS' if passed_auc else 'FAIL'}")

    logger.info(f"  Train/Val gap: {entry_meta.train_auc - entry_meta.val_auc:.4f} (gate: < 0.10)")
    passed_gap = (entry_meta.train_auc - entry_meta.val_auc) < 0.10
    logger.info(f"  Gap gate: {'PASS' if passed_gap else 'FAIL'}")

    logger.info(f"  Samples: {entry_meta.n_train_samples + entry_meta.n_val_samples} (gate: >= {min_trades})")
    passed_samples = (entry_meta.n_train_samples + entry_meta.n_val_samples) >= min_trades
    logger.info(f"  Sample gate: {'PASS' if passed_samples else 'FAIL'}")

    # Regime diversity
    regime_pass = True
    if entry_meta.regime_performance:
        positive_regimes = sum(1 for v in entry_meta.regime_performance.values() if v > 0.5)
        regime_pass = positive_regimes >= 2
        logger.info(f"  Regime performance: {entry_meta.regime_performance}")
        logger.info(f"  Regime diversity (>= 2 positive): {'PASS' if regime_pass else 'FAIL'}")
    else:
        logger.info("  Regime performance: insufficient data")

    # Top SHAP features
    logger.info(f"\nTop SHAP features:")
    for i, feat in enumerate(entry_meta.shap_top_features[:10], 1):
        logger.info(f"  {i:2d}. {feat}")

    # Overall verdict
    all_pass = passed_auc and passed_gap and passed_samples
    logger.info(f"\n{'=' * 60}")
    logger.info(f"OVERALL: {'PASS — ready for controller integration' if all_pass else 'FAIL — needs more data or feature engineering'}")
    logger.info(f"{'=' * 60}")

    # Save governance report to MongoDB
    report = {
        "timestamp": datetime.now(timezone.utc),
        "model_version": entry_meta.version,
        "holdout_auc": entry_meta.val_auc,
        "train_val_gap": entry_meta.train_auc - entry_meta.val_auc,
        "n_samples": entry_meta.n_train_samples + entry_meta.n_val_samples,
        "n_features": len(available_features),
        "top_features": entry_meta.shap_top_features[:10],
        "regime_performance": entry_meta.regime_performance,
        "passed": all_pass,
        "engines": engines or "all",
    }
    await db["ml_model_registry"].insert_one(report)
    logger.info("Governance report saved to ml_model_registry")

    client.close()


def _get_prior_shap_features() -> list:
    """Load SHAP feature rankings from the most recent model metadata."""
    from app.ml.trainer import SignalModelTrainer
    try:
        _, meta = SignalModelTrainer.load_model()
        return meta.shap_top_features
    except FileNotFoundError:
        return []


def main():
    """CLI entry point."""
    import argparse
    parser = argparse.ArgumentParser(description="Train ML models for signal factory v2")
    parser.add_argument("--engines", nargs="*", help="Filter to specific engines")
    parser.add_argument("--trials", type=int, default=50, help="Optuna trials")
    parser.add_argument("--min-trades", type=int, default=100, help="Minimum trades required")
    args = parser.parse_args()

    asyncio.run(run_training_pipeline(
        engines=args.engines,
        n_optuna_trials=args.trials,
        min_trades=args.min_trades,
    ))


if __name__ == "__main__":
    main()
