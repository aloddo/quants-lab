"""
HL-Bybit Perp/Perp Spread Arb V2 — WebSocket-based, production-ready.

Delta-neutral spread arb with sub-100ms price detection via WebSocket feeds.
Both legs are perpetual futures on HL + Bybit. No spot inventory risk.

Modes:
  --dry-run  (default): WS price feeds + signal detection + virtual position tracking
  --paper:   Full execution flow with simulated fills
  --live:    Real orders on both exchanges (requires --confirm-live)

Usage:
    set -a && source .env && set +a
    python scripts/arb_hl_bybit_perp_v2.py --dry-run
    python scripts/arb_hl_bybit_perp_v2.py --dry-run --pairs CHIP-USDT,APE-USDT
    python scripts/arb_hl_bybit_perp_v2.py --live --confirm-live --position-usd 100
"""
import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Load .env
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

from app.services.arb_hlbb.config import ArbConfig
from app.services.arb_hlbb.orchestrator import Orchestrator, RunMode

# ── Logging ───────────────────────────────────────────────────────

logger = logging.getLogger("arb_hlbb")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
_fh = logging.FileHandler("/tmp/arb-hl-bybit-perp-v2.log", mode="a")
_fh.setFormatter(_fmt)
logger.addHandler(_sh)
logger.addHandler(_fh)

# Also set child loggers
for name in ["arb_hlbb", "app.services.arb_hlbb"]:
    lg = logging.getLogger(name)
    lg.setLevel(logging.INFO)
    lg.addHandler(_sh)
    lg.addHandler(_fh)


async def main(args):
    # Determine mode
    if args.live:
        if not args.confirm_live:
            logger.error("LIVE mode requires --confirm-live flag")
            sys.exit(1)
        mode = RunMode.LIVE
    elif args.paper:
        mode = RunMode.PAPER
    else:
        mode = RunMode.DRY_RUN

    # Build config
    config = ArbConfig(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"),
        position_usd=args.position_usd,
        entry_min_spread_bps=args.min_spread,
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "81062935"),
        telegram_enabled=bool(os.getenv("TELEGRAM_BOT_TOKEN", "")),
    )

    # Parse pairs
    pairs = args.pairs.split(",") if args.pairs else config.default_pairs

    logger.info("=" * 60)
    logger.info(f"HL-Bybit Perp/Perp Arb V2 — {mode.value.upper()}")
    logger.info(f"Pairs: {len(pairs)}")
    logger.info(f"Position: ${config.position_usd}/side")
    logger.info(f"Fees: {config.fee_rt_bps:.1f}bp RT")
    logger.info(f"Entry floor: {config.entry_min_spread_bps:.0f}bp")
    logger.info("=" * 60)

    # Create orchestrator
    orch = Orchestrator(config, mode)

    # Signal handling
    loop = asyncio.get_event_loop()

    def handle_signal(sig):
        logger.info(f"Signal {sig} received — shutting down")
        orch._running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal, sig)

    # Initialize and run
    await orch.initialize(pairs)
    await orch.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HL-Bybit Perp/Perp Spread Arb V2")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Signal detection only (default)")
    parser.add_argument("--paper", action="store_true",
                        help="Simulated execution")
    parser.add_argument("--live", action="store_true",
                        help="Real execution on both venues")
    parser.add_argument("--confirm-live", action="store_true",
                        help="Required with --live to confirm real money")
    parser.add_argument("--pairs", type=str, default=None,
                        help="Comma-separated pairs (e.g., CHIP-USDT,APE-USDT)")
    parser.add_argument("--position-usd", type=float, default=100.0,
                        help="Position size per side in USD (default: 100)")
    parser.add_argument("--min-spread", type=float, default=30.0,
                        help="Minimum entry spread in bps (default: 30)")
    args = parser.parse_args()

    asyncio.run(main(args))
