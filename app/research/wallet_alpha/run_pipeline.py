#!/usr/bin/env python3
"""
Wallet Alpha Research Pipeline Runner

Orchestrates all phases in sequence:
  Phase 1: Data preparation (S3 fills -> Parquet)
  Phase 2: Event construction + position inference
  Phase 3: Feature engineering (markout, edge decomposition)
  Phase 4-5: Scoring, ranking, validation

Usage:
    python -m app.research.wallet_alpha.run_pipeline
    python -m app.research.wallet_alpha.run_pipeline --phase 2  # start from phase 2
    python -m app.research.wallet_alpha.run_pipeline --phase 3 --skip-phase1
"""
import argparse
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pipeline] %(levelname)s: %(message)s",
)
logger = logging.getLogger("pipeline")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, default=1, help="Start from phase N")
    args = parser.parse_args()

    t0 = time.time()

    if args.phase <= 1:
        logger.info("=" * 60)
        logger.info("PHASE 1: Data Preparation (S3 fills -> Parquet)")
        logger.info("=" * 60)
        from app.research.wallet_alpha.phase1_data_prep import main as phase1
        phase1()

    if args.phase <= 1:
        logger.info("=" * 60)
        logger.info("PHASE 1b: Universe Summary")
        logger.info("=" * 60)
        from app.research.wallet_alpha.phase1_data_prep import compute_universe_summary, OUTPUT_DIR, FILLS_DIR, UNIVERSE_PATH
        import pandas as pd
        summary = compute_universe_summary(FILLS_DIR)
        summary.to_csv(UNIVERSE_PATH, index=False)
        logger.info(f"Universe: {len(summary):,} wallets")
        filtered = summary[
            (summary["fill_count"] >= 50) &
            (summary["coins_traded"] >= 5) &
            (summary["total_notional"] >= 10_000) &
            (summary["fills_per_day"] <= 10_000)
        ]
        filtered_path = OUTPUT_DIR / "universe_filtered.csv"
        filtered.to_csv(filtered_path, index=False)
        logger.info(f"Filtered: {len(filtered):,} wallets")

    if args.phase <= 1:
        logger.info("=" * 60)
        logger.info("PHASE 1c: Build fill-derived mid-prices (fallback)")
        logger.info("=" * 60)
        from app.research.wallet_alpha.build_fill_midprice import main as phase1b
        phase1b()

    if args.phase <= 1:
        logger.info("=" * 60)
        logger.info("PHASE 1c: BTC regime tagging")
        logger.info("=" * 60)
        from app.research.wallet_alpha.regime_tagger import main as phase1c
        phase1c()

    if args.phase <= 2:
        logger.info("=" * 60)
        logger.info("PHASE 2: Event Construction + Position Inference")
        logger.info("=" * 60)
        from app.research.wallet_alpha.phase2_events import main as phase2
        phase2()

    if args.phase <= 3:
        logger.info("=" * 60)
        logger.info("PHASE 3: Feature Engineering (markout + 40 features)")
        logger.info("=" * 60)
        from app.research.wallet_alpha.phase3_features import main as phase3
        phase3()

    if args.phase <= 4:
        logger.info("=" * 60)
        logger.info("PHASE 4-5: Scoring + Ranking + Validation")
        logger.info("=" * 60)
        from app.research.wallet_alpha.phase4_scoring import main as phase4
        phase4()

    if args.phase <= 6:
        logger.info("=" * 60)
        logger.info("PHASE 6: Copy Simulation")
        logger.info("=" * 60)
        from app.research.wallet_alpha.phase6_simulation import main as phase6
        phase6()

    elapsed = time.time() - t0
    logger.info(f"\n{'=' * 60}")
    logger.info(f"PIPELINE COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f}min)")
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
