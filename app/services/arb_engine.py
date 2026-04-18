"""
Cross-exchange arbitrage engine — Binance SPOT ↔ Bybit PERP.

Production-ready main loop: screen → decide → execute → monitor → unwind.
Logs all decisions to MongoDB. Supports paper and live modes.

Optimal params (from 134-pair × 200-day backtest):
    entry=35bp, exit=5bp, hold=24h, cooldown=2h, stop=2.5x
    Expected: 95% WR, ~$42/day on $200/trade, funding is INCOME (~3bp)

Usage:
    # Weekend paper trading
    python -m app.services.arb_engine

    # Monday go-live
    python -m app.services.arb_engine --live
"""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

# Load .env
_env_file = Path(__file__).resolve().parents[2] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

from app.services.arb_screener import ArbScreener, ArbOpportunity
from app.services.arb_executor import (
    ArbExecutor, ArbDirection, ArbPosition, PositionStatus, RiskManager,
)
from app.services.binance_client import BinanceClient
from app.services.arb_safety import SymbolPrecision, SpreadTracker
from app.services.arb_notifier import notify_open, notify_close, notify_stats, poll_commands

logger = logging.getLogger(__name__)

# Optimal params from backtest
DEFAULT_ENTRY_BPS = 60.0
DEFAULT_EXIT_BPS = 5.0
DEFAULT_MAX_HOLD_H = 24
DEFAULT_COOLDOWN_H = 2
DEFAULT_STOP_MULT = 2.5
DEFAULT_POSITION_USD = 200.0
DEFAULT_MAX_EXPOSURE = 2000.0
DEFAULT_MAX_CONCURRENT = 10
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_MIN_VOL = 500_000


class ArbEngine:
    """Main arb engine: screen → decide → execute → monitor → unwind."""

    def __init__(
        self,
        paper_mode: bool = True,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        min_entry_spread_bps: float = DEFAULT_ENTRY_BPS,
        min_exit_spread_bps: float = DEFAULT_EXIT_BPS,
        max_hold_hours: float = DEFAULT_MAX_HOLD_H,
        cooldown_hours: float = DEFAULT_COOLDOWN_H,
        stop_loss_mult: float = DEFAULT_STOP_MULT,
        position_size_usd: float = DEFAULT_POSITION_USD,
        max_total_exposure_usd: float = DEFAULT_MAX_EXPOSURE,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        min_volume_24h: float = DEFAULT_MIN_VOL,
    ):
        self.paper_mode = paper_mode
        self.min_entry_spread_bps = min_entry_spread_bps
        self.min_exit_spread_bps = min_exit_spread_bps
        self.max_hold_seconds = max_hold_hours * 3600
        self.cooldown_seconds = cooldown_hours * 3600
        self.stop_loss_mult = stop_loss_mult
        self.position_size_usd = position_size_usd

        # Screener
        self.screener = ArbScreener(
            poll_interval=poll_interval,
            min_net_spread_bps=0,
            min_volume_24h=min_volume_24h,
        )

        # Binance client
        self.binance = BinanceClient()

        # Risk manager
        self.risk = RiskManager(
            max_position_usd=position_size_usd * 1.5,
            max_total_exposure_usd=max_total_exposure_usd,
            max_concurrent_positions=max_concurrent,
            max_position_per_symbol=1,
            min_net_spread_bps=min_entry_spread_bps,
        )

        # Executor
        self.executor = ArbExecutor(
            binance=self.binance,
            risk=self.risk,
            paper_mode=paper_mode,
        )

        # Safety: symbol precision + spread tracker
        try:
            self.precision = SymbolPrecision()
        except Exception:
            self.precision = None
            logger.warning("Symbol precision not loaded — run arb_safety first")

        self.spread_tracker = SpreadTracker()

        # Inventory cache
        self._inventory: dict[str, float] = {}
        self._inventory_last_refresh: float = 0
        self._INVENTORY_TTL = 30

        # Cooldown tracker: symbol → last exit timestamp
        self._cooldowns: dict[str, float] = {}

        # MongoDB for session stats
        from pymongo import MongoClient
        uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        db_name = os.getenv("MONGO_DATABASE", "quants_lab")
        self._db = MongoClient(uri)[db_name]
        self._session_id = f"arb_{int(time.time())}"
        self._stats_coll = self._db["arb_session_stats"]

        self._running = False
        self._start_time = 0
        self._iteration = 0

    async def _refresh_inventory(self):
        if self.paper_mode:
            return
        now = time.time()
        if now - self._inventory_last_refresh < self._INVENTORY_TTL:
            return
        try:
            balances = await self.binance.get_balances()
            self._inventory = {a: info["free"] for a, info in balances.items()}
            self._inventory_last_refresh = now
        except Exception as e:
            logger.error(f"Inventory refresh failed: {e}")

    def _has_inventory(self, symbol: str, direction: ArbDirection, usd_size: float, price: float) -> bool:
        if self.paper_mode:
            return True
        base = symbol.replace("USDT", "")
        if direction == ArbDirection.BUY_BB_SELL_BN:
            needed = usd_size / price
            return self._inventory.get(base, 0) >= needed * 0.95
        if direction == ArbDirection.BUY_BN_SELL_BB:
            return self._inventory.get("USDT", 0) >= usd_size * 0.95
        return True

    def _check_cooldown(self, symbol: str) -> bool:
        last_exit = self._cooldowns.get(symbol, 0)
        return (time.time() - last_exit) >= self.cooldown_seconds

    def _compute_current_spread(self, pos: ArbPosition, bn: dict, bb: dict) -> float:
        """Compute the DIRECTIONAL spread for an open position.

        For BUY_BN_SELL_BB: we bought BN spot, sold BB perp.
          Spread = (bb_bid - bn_ask) / bn_ask. Positive = still profitable to hold.
          When it drops to near zero, the basis has converged.

        For BUY_BB_SELL_BN: we bought BB perp, sold BN spot.
          Spread = (bn_bid - bb_ask) / bb_ask. Same logic.
        """
        if pos.direction == ArbDirection.BUY_BN_SELL_BB:
            return (bb["bid"] - bn["ask"]) / bn["ask"] * 10000
        else:
            return (bn["bid"] - bb["ask"]) / bb["ask"] * 10000

    async def _check_unwinds(self, bn_tickers: dict, bb_tickers: dict):
        """Check if any open positions should be unwound."""
        open_positions = self.executor.get_open_positions()
        if not open_positions:
            return

        # Fetch fresh tickers for any open positions missing from the scan
        missing = [p.symbol for p in open_positions
                   if p.symbol not in bn_tickers or p.symbol not in bb_tickers]
        if missing:
            try:
                all_bn = await self.screener._fetch_binance_spot()
                all_bb = await self.screener._fetch_bybit_perp()
                for sym in missing:
                    if sym in all_bn:
                        bn_tickers[sym] = all_bn[sym]
                    if sym in all_bb:
                        bb_tickers[sym] = all_bb[sym]
            except Exception as e:
                logger.error(f"Failed to fetch tickers for unwind check: {e}")

        for pos in open_positions:
            now = time.time()
            hold_time = now - pos.entry_time

            if pos.symbol not in bn_tickers or pos.symbol not in bb_tickers:
                # Can't check spread — force close if held too long
                if hold_time > self.max_hold_seconds:
                    logger.warning(f"Force closing {pos.symbol}: no price data + max hold exceeded")
                    pos.status = PositionStatus.CLOSED
                    pos.exit_time = now
                    pos.error = "No price data for unwind"
                    self.executor._save_position(pos)
                    self._cooldowns[pos.symbol] = now
                continue

            bn = bn_tickers[pos.symbol]
            bb = bb_tickers[pos.symbol]
            current_spread = self._compute_current_spread(pos, bn, bb)

            should_close = False
            reason = ""

            # 1. Spread converged
            if current_spread <= self.min_exit_spread_bps:
                should_close = True
                reason = f"REVERTED: {current_spread:.1f}bp"

            # 2. Max hold time
            elif hold_time > self.max_hold_seconds:
                should_close = True
                reason = f"MAX_HOLD: {hold_time/3600:.1f}h"

            # 3. Stop loss: spread widened beyond entry × multiplier
            elif current_spread > pos.entry_spread_bps * self.stop_loss_mult:
                should_close = True
                reason = f"STOP_LOSS: {current_spread:.1f}bp > {pos.entry_spread_bps * self.stop_loss_mult:.0f}bp"

            if should_close:
                bn_price = (bn["bid"] + bn["ask"]) / 2
                bb_price = (bb["bid"] + bb["ask"]) / 2
                logger.info(f"↩️  Unwinding {pos.symbol}: {reason}")
                await self.executor.close_position(
                    pos, bn_price, bb_price,
                    bn_ticker=bn, bb_ticker=bb,
                )
                self._cooldowns[pos.symbol] = time.time()

    async def run(self, max_iterations: int | None = None):
        mode = "📝 PAPER" if self.paper_mode else "🔥 LIVE"
        config = (
            f"\n{'='*60}\n"
            f"  ARB ENGINE — {mode}\n"
            f"  Session: {self._session_id}\n"
            f"  Entry:     >{self.min_entry_spread_bps}bp\n"
            f"  Exit:      <{self.min_exit_spread_bps}bp\n"
            f"  Hold:      {self.max_hold_seconds/3600:.0f}h\n"
            f"  Cooldown:  {self.cooldown_seconds/3600:.0f}h\n"
            f"  Stop:      {self.stop_loss_mult}x\n"
            f"  Size:      ${self.position_size_usd}\n"
            f"  Exposure:  ${self.risk.max_total_exposure_usd}\n"
            f"  Concurrent:{self.risk.max_concurrent_positions}\n"
            f"{'='*60}"
        )
        logger.info(config)
        print(config)

        self._running = True
        self._start_time = time.time()
        self._iteration = 0

        # Clean stale positions from previous test runs
        stale = self._db["arb_positions"].delete_many({"status": "FAILED"})
        if stale.deleted_count:
            logger.info(f"Cleaned {stale.deleted_count} stale FAILED positions")

        try:
            while self._running:
                self._iteration += 1

                # 1. Refresh inventory (live only)
                await self._refresh_inventory()

                # 2. Poll both exchanges
                opportunities = await self.screener.poll_once()
                self.screener._log_opportunities(opportunities)

                # Build ticker dicts for unwind checks
                # (screener already fetched these — reuse from state)
                bn_tickers = {}
                bb_tickers = {}
                for opp in opportunities:
                    bn_tickers[opp.symbol] = {"bid": opp.bn_bid, "ask": opp.bn_ask, "vol": opp.bn_vol_24h}
                    bb_tickers[opp.symbol] = {"bid": opp.bb_bid, "ask": opp.bb_ask, "vol": opp.bb_vol_24h}

                # 3. Track spread persistence
                for opp in opportunities:
                    self.spread_tracker.record(opp.symbol, opp.net_spread_bps)

                # 4. Check unwinds FIRST (free up capital + slots)
                await self._check_unwinds(bn_tickers, bb_tickers)

                # 5. Filter for actionable opportunities
                actionable = []
                for opp in opportunities:
                    if opp.net_spread_bps < self.min_entry_spread_bps:
                        continue
                    if not self._check_cooldown(opp.symbol):
                        continue
                    # Prefer persistent spreads (seen in >50% of recent polls)
                    persistence = self.spread_tracker.persistence_score(opp.symbol, self.min_entry_spread_bps)
                    if self.spread_tracker._history.get(opp.symbol) and len(self.spread_tracker._history[opp.symbol]) > 5:
                        if persistence < 0.3:
                            continue  # skip transient spikes
                    actionable.append(opp)

                # 6. Check inventory
                tradeable = []
                for opp in actionable:
                    direction = ArbDirection(opp.direction)
                    price = opp.bn_ask if direction == ArbDirection.BUY_BN_SELL_BB else opp.bn_bid
                    if self._has_inventory(opp.symbol, direction, self.position_size_usd, price):
                        tradeable.append(opp)

                # 7. Execute top opportunities (max 2 per cycle to avoid bursts)
                for opp in tradeable[:2]:
                    direction = ArbDirection(opp.direction)
                    if direction == ArbDirection.BUY_BN_SELL_BB:
                        bn_price, bb_price = opp.bn_ask, opp.bb_bid
                    else:
                        bn_price, bb_price = opp.bn_bid, opp.bb_ask

                    await self.executor.open_position(
                        symbol=opp.symbol,
                        direction=direction,
                        usd_size=self.position_size_usd,
                        entry_spread_bps=opp.net_spread_bps,
                        bn_price=bn_price,
                        bb_price=bb_price,
                    )

                # 8. Status
                self._print_status(opportunities, actionable, tradeable)

                # 9. Periodic stats save (every 60 iterations ~ 5 min)
                if self._iteration % 60 == 0:
                    self._save_session_stats()

                # 10. Check Telegram commands (every 6 iterations ~ 30s)
                if self._iteration % 6 == 0:
                    await poll_commands()

                # 11. Telegram stats (every 360 iterations ~ 30 min)
                if self._iteration % 360 == 0:
                    s = self.executor.get_position_summary()
                    uptime = (time.time() - self._start_time) / 3600
                    notify_stats(s['open'], s['closed'], s['total_pnl'],
                                s['win_rate'], uptime, paper=self.paper_mode)

                if max_iterations and self._iteration >= max_iterations:
                    break

                await asyncio.sleep(self.screener.poll_interval)

        except KeyboardInterrupt:
            logger.info("Engine stopped by user")
        except Exception as e:
            logger.error(f"Engine error: {e}", exc_info=True)
        finally:
            self._running = False
            self._save_session_stats()
            summary = self.executor.get_position_summary()
            logger.info(f"\n=== SESSION SUMMARY ===\n{summary}")
            print(f"\n{'='*60}")
            print(f"  SESSION COMPLETE — {self._session_id}")
            print(f"  Duration: {(time.time()-self._start_time)/3600:.1f}h")
            print(f"  Iterations: {self._iteration}")
            for k, v in summary.items():
                print(f"  {k}: {v}")
            print(f"{'='*60}")
            await self.executor.close()

    def _print_status(self, all_opps, actionable, tradeable):
        ts = datetime.now().strftime("%H:%M:%S")
        summary = self.executor.get_position_summary()
        open_pos = self.executor.get_open_positions()
        uptime = (time.time() - self._start_time) / 60

        # Only print full status every 12th iteration (~1 min) to reduce noise
        if self._iteration % 12 == 0 or summary['closed'] > 0:
            print(
                f"[{ts}] #{self._iteration} ({uptime:.0f}m) | "
                f"opps={len(all_opps)} act={len(actionable)} trd={len(tradeable)} | "
                f"open={summary['open']} closed={summary['closed']} "
                f"PnL=${summary['total_pnl']:.2f} "
                f"WR={summary['win_rate']:.0%}"
            )
            for p in open_pos[:5]:
                age_m = (time.time() - p.entry_time) / 60
                print(f"  📊 {p.symbol:<14} {p.direction.value} entry={p.entry_spread_bps:.0f}bp age={age_m:.0f}m")

    def _save_session_stats(self):
        summary = self.executor.get_position_summary()
        doc = {
            "session_id": self._session_id,
            "timestamp": datetime.now(timezone.utc),
            "uptime_hours": (time.time() - self._start_time) / 3600,
            "iterations": self._iteration,
            "paper_mode": self.paper_mode,
            **summary,
        }
        self._stats_coll.update_one(
            {"session_id": self._session_id},
            {"$set": doc},
            upsert=True,
        )

    def stop(self):
        self._running = False


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Cross-exchange arb engine")
    parser.add_argument("--live", action="store_true", help="Live trading mode")
    parser.add_argument("--interval", type=float, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument("--min-entry", type=float, default=DEFAULT_ENTRY_BPS)
    parser.add_argument("--min-exit", type=float, default=DEFAULT_EXIT_BPS)
    parser.add_argument("--max-hold", type=float, default=DEFAULT_MAX_HOLD_H, help="Hours")
    parser.add_argument("--cooldown", type=float, default=DEFAULT_COOLDOWN_H, help="Hours")
    parser.add_argument("--stop-mult", type=float, default=DEFAULT_STOP_MULT)
    parser.add_argument("--position-size", type=float, default=DEFAULT_POSITION_USD)
    parser.add_argument("--max-exposure", type=float, default=DEFAULT_MAX_EXPOSURE)
    parser.add_argument("--max-concurrent", type=int, default=DEFAULT_MAX_CONCURRENT)
    parser.add_argument("--min-vol", type=float, default=DEFAULT_MIN_VOL)
    parser.add_argument("--iterations", type=int, default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("/tmp/arb-engine.log"),
        ],
    )

    engine = ArbEngine(
        paper_mode=not args.live,
        poll_interval=args.interval,
        min_entry_spread_bps=args.min_entry,
        min_exit_spread_bps=args.min_exit,
        max_hold_hours=args.max_hold,
        cooldown_hours=args.cooldown,
        stop_loss_mult=args.stop_mult,
        position_size_usd=args.position_size,
        max_total_exposure_usd=args.max_exposure,
        max_concurrent=args.max_concurrent,
        min_volume_24h=args.min_vol,
    )

    await engine.run(max_iterations=args.iterations)


if __name__ == "__main__":
    asyncio.run(main())
