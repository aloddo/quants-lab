#!/usr/bin/env python3
"""
Copy Trade Backtester — replay 2.64M HL trades through copy trader logic.

Simulates the full copy trader with fixed TWAP detection, trade-stream exit,
and realistic fee assumptions. No lookahead, no survivorship bias.

Usage:
    python scripts/copy_trade_backtest.py --wallets 0xabc,0xdef --size 20

Reads from MongoDB: hl_wallet_trades
Outputs: per-wallet and per-trade results
"""
import argparse
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
from pymongo import MongoClient

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger("bt")

# --- Constants (match live script) ---
TWAP_WINDOW_S = 60
MIN_TWAP_NOTIONAL = 500
DIRECTIONALITY_MIN = 0.6
EXIT_TWAP_WINDOW_S = 60
EXIT_TWAP_MIN_NOTIONAL = 200
MAX_POSITIONS = 3
COOLDOWN_S = 30
TAKER_FEE_BPS = 4.32
MAKER_FEE_BPS = 1.44
SLIPPAGE_BPS = 1.0  # estimated market impact

# Fee scenarios
FEE_IOC_ENTRY_MAKER_EXIT = TAKER_FEE_BPS + MAKER_FEE_BPS  # 5.76bp
FEE_IOC_ENTRY_IOC_EXIT = TAKER_FEE_BPS + TAKER_FEE_BPS    # 8.64bp


class Position:
    def __init__(self, coin, side, entry_px, entry_ts, wallet, size_usd):
        self.coin = coin
        self.side = side  # 'BUY' or 'SELL'
        self.entry_px = entry_px
        self.entry_ts = entry_ts
        self.wallet = wallet
        self.size_usd = size_usd
        self.exit_px = None
        self.exit_ts = None
        self.pnl_bps = None

    def close(self, exit_px, exit_ts):
        self.exit_px = exit_px
        self.exit_ts = exit_ts
        if self.side == 'BUY':
            self.pnl_bps = (exit_px - self.entry_px) / self.entry_px * 10000
        else:
            self.pnl_bps = (self.entry_px - exit_px) / self.entry_px * 10000

    @property
    def hold_min(self):
        if self.exit_ts:
            return (self.exit_ts - self.entry_ts) / 60
        return 0


class CopyTradeBacktester:
    def __init__(self, target_wallets, order_size_usd=20.0):
        self.targets = {w.lower().strip() for w in target_wallets}
        self.order_size = order_size_usd

        # State
        self.positions = []       # open positions
        self.closed = []          # completed round trips
        self.last_entry = {}      # (wallet, coin) -> timestamp
        self.mid_prices = {}      # coin -> last known mid price

        # TWAP entry buffer: (wallet, coin) -> {...}
        self._twap_buffer = {}
        self._twap_entered = set()

        # TWAP exit buffer: (wallet, coin) -> {...}
        self._exit_buffer = {}

        # Target position tracking (from trade stream, not API)
        self._target_positions = defaultdict(lambda: defaultdict(float))

    def _process_trade(self, trade):
        """Process a single trade from the replay."""
        coin = trade['coin']
        buyer = trade.get('buyer', '').lower()
        seller = trade.get('seller', '').lower()
        px = trade['price']
        notional = trade['notional']
        ts = trade['timestamp']

        # Update mid price
        self.mid_prices[coin] = px

        # Check if trade is from a target wallet
        matched_wallet = None
        is_buy = None
        if buyer in self.targets:
            matched_wallet = buyer
            is_buy = True
        elif seller in self.targets:
            matched_wallet = seller
            is_buy = False

        if not matched_wallet:
            return

        # --- ENTRY TWAP ---
        twap_key = (matched_wallet, coin)
        if twap_key not in self._twap_buffer:
            self._twap_buffer[twap_key] = {
                'first_ts': ts, 'last_ts': ts,
                'buy_notional': 0, 'sell_notional': 0,
                'count': 0,
            }
        buf = self._twap_buffer[twap_key]
        buf['last_ts'] = ts
        if is_buy:
            buf['buy_notional'] += notional
        else:
            buf['sell_notional'] += notional
        buf['count'] += 1

        # --- EXIT TWAP: detect reverse flow for open positions ---
        for pos in self.positions:
            if pos.coin == coin and pos.wallet == matched_wallet:
                pos_is_long = pos.side == 'BUY'
                is_reverse = (pos_is_long and not is_buy) or (not pos_is_long and is_buy)
                if is_reverse:
                    exit_key = (matched_wallet, coin)
                    if exit_key not in self._exit_buffer:
                        self._exit_buffer[exit_key] = {
                            'first_ts': ts, 'last_ts': ts,
                            'reverse_notional': 0, 'count': 0,
                        }
                    ebuf = self._exit_buffer[exit_key]
                    ebuf['last_ts'] = ts
                    ebuf['reverse_notional'] += notional
                    ebuf['count'] += 1

    def _check_entries(self, current_ts):
        """Check if any entry TWAP windows have completed."""
        expired = []
        for twap_key, buf in list(self._twap_buffer.items()):
            wallet, coin = twap_key
            elapsed = current_ts - buf['first_ts']
            if elapsed < TWAP_WINDOW_S:
                continue

            expired.append(twap_key)

            # Dedup
            dedup_key = (wallet, coin, int(buf['first_ts']))
            if dedup_key in self._twap_entered:
                continue

            # NET direction
            net = buf['buy_notional'] - buf['sell_notional']
            gross = buf['buy_notional'] + buf['sell_notional']
            abs_net = abs(net)
            is_buy = net > 0

            # Filters
            if abs_net < MIN_TWAP_NOTIONAL:
                continue
            if gross > 0 and abs_net / gross < DIRECTIONALITY_MIN:
                continue

            # Opening check: compare against tracked position
            prev_sz = self._target_positions[wallet][coin]
            if abs(prev_sz) > 0.001:
                # Has existing position — check if this adds or reduces
                if prev_sz > 0 and not is_buy:
                    continue  # long + selling = closing
                if prev_sz < 0 and is_buy:
                    continue  # short + buying = closing

            # Max positions
            if len(self.positions) >= MAX_POSITIONS:
                continue

            # Already in this coin?
            if any(p.coin == coin for p in self.positions):
                continue

            # Cooldown
            if current_ts - self.last_entry.get((wallet, coin), 0) < COOLDOWN_S:
                continue

            # ENTRY: use current mid price + slippage
            entry_px = self.mid_prices.get(coin, 0)
            if entry_px <= 0:
                continue

            # Apply slippage
            if is_buy:
                entry_px *= (1 + SLIPPAGE_BPS / 10000)
            else:
                entry_px *= (1 - SLIPPAGE_BPS / 10000)

            pos = Position(coin, 'BUY' if is_buy else 'SELL', entry_px, current_ts, wallet, self.order_size)
            self.positions.append(pos)
            self.last_entry[(wallet, coin)] = current_ts

            # Update target position tracker
            trade_sz = abs_net / entry_px
            if not is_buy:
                trade_sz = -trade_sz
            self._target_positions[wallet][coin] += trade_sz

            self._twap_entered.add(dedup_key)

        for key in expired:
            del self._twap_buffer[key]

    def _check_exits(self, current_ts):
        """Check if any exit TWAP windows have completed."""
        still_open = []
        for pos in self.positions:
            exit_key = (pos.wallet, pos.coin)

            exited = False
            if exit_key in self._exit_buffer:
                ebuf = self._exit_buffer[exit_key]
                exit_elapsed = current_ts - ebuf['first_ts']

                if exit_elapsed >= EXIT_TWAP_WINDOW_S:
                    if ebuf['reverse_notional'] >= EXIT_TWAP_MIN_NOTIONAL:
                        # EXIT SIGNAL
                        exit_px = self.mid_prices.get(pos.coin, pos.entry_px)
                        # Apply slippage for exit
                        if pos.side == 'BUY':
                            exit_px *= (1 - SLIPPAGE_BPS / 10000)
                        else:
                            exit_px *= (1 + SLIPPAGE_BPS / 10000)

                        pos.close(exit_px, current_ts)
                        self.closed.append(pos)

                        # Update target position
                        rev_sz = ebuf['reverse_notional'] / exit_px
                        if self._target_positions[pos.wallet][pos.coin] > 0:
                            self._target_positions[pos.wallet][pos.coin] -= rev_sz
                        else:
                            self._target_positions[pos.wallet][pos.coin] += rev_sz

                        del self._exit_buffer[exit_key]
                        exited = True
                    else:
                        del self._exit_buffer[exit_key]

            if not exited:
                still_open.append(pos)

        self.positions = still_open

    def run(self, trades):
        """Replay trades chronologically."""
        start = time.time()
        n = len(trades)
        last_check = 0

        for i, trade in enumerate(trades):
            ts = trade['timestamp']
            self._process_trade(trade)

            # Throttle checks to every 1 second of simulated time
            if ts - last_check >= 1.0:
                last_check = ts
                self._check_entries(ts)
                self._check_exits(ts)

            if i % 500000 == 0 and i > 0:
                logger.warning(f"  Processed {i}/{n} trades ({i/n*100:.0f}%)... {len(self.closed)} closed, {len(self.positions)} open")

        # Force-close remaining positions at last known price
        for pos in self.positions:
            exit_px = self.mid_prices.get(pos.coin, pos.entry_px)
            pos.close(exit_px, trades[-1]['timestamp'] if trades else 0)
            self.closed.append(pos)
        self.positions = []

        elapsed = time.time() - start
        logger.warning(f"  Backtest complete in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", help="Comma-separated wallets (or 'top10' for auto-select)")
    parser.add_argument("--size", type=float, default=20.0)
    parser.add_argument("--top", type=int, default=0, help="Auto-select top N wallets by trade count")
    parser.add_argument("--min-trades", type=int, default=50)
    parser.add_argument("--min-notional", type=float, default=100000)
    args = parser.parse_args()

    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    col = db["hl_wallet_trades"]

    # Determine target wallets
    if args.top > 0:
        # Auto-select top wallets by total notional
        pipeline = [
            {"$group": {"_id": "$buyer", "total": {"$sum": "$notional"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gte": args.min_trades}, "total": {"$gte": args.min_notional}}},
            {"$sort": {"total": -1}},
            {"$limit": args.top * 3},  # get extra, will filter
        ]
        candidates = [r["_id"] for r in col.aggregate(pipeline, allowDiskUse=True) if r["_id"]]
        target_wallets = candidates[:args.top]
        print(f"Auto-selected top {len(target_wallets)} wallets")
    elif args.wallets:
        target_wallets = [w.strip() for w in args.wallets.split(",")]
    else:
        print("Specify --wallets or --top N")
        return

    for w in target_wallets:
        print(f"  {w[:20]}...")

    # Load ALL trades sorted by timestamp
    print(f"\nLoading trades from MongoDB...")
    trades = list(col.find(
        {},
        {"buyer": 1, "seller": 1, "coin": 1, "price": 1, "notional": 1, "timestamp": 1}
    ).sort("timestamp", 1))
    print(f"Loaded {len(trades):,} trades")

    if not trades:
        print("No trades found!")
        return

    ts_start = datetime.fromtimestamp(trades[0]["timestamp"], tz=timezone.utc)
    ts_end = datetime.fromtimestamp(trades[-1]["timestamp"], tz=timezone.utc)
    print(f"Period: {ts_start.strftime('%Y-%m-%d %H:%M')} → {ts_end.strftime('%Y-%m-%d %H:%M')} ({(ts_end - ts_start).days}d)")

    # Run backtest for each wallet individually
    print(f"\n{'='*70}")
    print(f"BACKTESTING {len(target_wallets)} wallets @ ${args.size}/trade")
    print(f"Fees: {FEE_IOC_ENTRY_MAKER_EXIT}bp (IOC+maker) / {FEE_IOC_ENTRY_IOC_EXIT}bp (IOC+IOC)")
    print(f"Slippage: {SLIPPAGE_BPS}bp each way")
    print(f"{'='*70}")

    all_results = []

    for wallet in target_wallets:
        bt = CopyTradeBacktester([wallet], order_size_usd=args.size)
        bt.run(trades)

        if not bt.closed:
            continue

        pnls_gross = [t.pnl_bps for t in bt.closed]
        pnls_net = [p - FEE_IOC_ENTRY_MAKER_EXIT - 2 * SLIPPAGE_BPS for p in pnls_gross]
        holds = [t.hold_min for t in bt.closed]
        coins = list(set(t.coin for t in bt.closed))

        result = {
            'wallet': wallet,
            'trips': len(bt.closed),
            'gross_avg': np.mean(pnls_gross),
            'net_avg': np.mean(pnls_net),
            'wr_net': np.mean(np.array(pnls_net) > 0) * 100,
            'hold_min': np.mean(holds),
            'total_net_bps': np.sum(pnls_net),
            'total_net_usd': np.sum(pnls_net) / 10000 * args.size,
            'coins': coins,
            'max_loss': np.min(pnls_net),
            'max_win': np.max(pnls_net),
        }
        all_results.append(result)

        print(f"\n{wallet[:20]}... ({len(bt.closed)} trades, {len(coins)} coins)")
        print(f"  Gross: avg={result['gross_avg']:+.1f}bp | Net: avg={result['net_avg']:+.1f}bp | WR={result['wr_net']:.0f}%")
        print(f"  Hold: avg={result['hold_min']:.0f}min | Total net: {result['total_net_bps']:+.0f}bp (${result['total_net_usd']:+.2f})")
        print(f"  Max win: {result['max_win']:+.1f}bp | Max loss: {result['max_loss']:+.1f}bp")

    # Summary
    if all_results:
        profitable = [r for r in all_results if r['net_avg'] > 0]
        print(f"\n{'='*70}")
        print(f"SUMMARY")
        print(f"  Wallets tested: {len(all_results)}")
        print(f"  Profitable (net > 0): {len(profitable)}")
        total_usd = sum(r['total_net_usd'] for r in all_results)
        total_trades = sum(r['trips'] for r in all_results)
        print(f"  Total trades: {total_trades}")
        print(f"  Total net PnL: ${total_usd:+.2f} (at ${args.size}/trade)")
        if total_trades > 0:
            days = (trades[-1]["timestamp"] - trades[0]["timestamp"]) / 86400
            print(f"  Daily: ${total_usd / max(days, 1):+.2f}/day")
            print(f"  Monthly: ${total_usd / max(days, 1) * 30:+.2f}/month")

        # Per-wallet ranking
        all_results.sort(key=lambda x: x['total_net_bps'], reverse=True)
        print(f"\n  RANKING (by total net PnL):")
        print(f"  {'Wallet':>22} {'Trips':>6} {'Net/trade':>10} {'WR':>5} {'Hold':>6} {'Total$':>8}")
        for r in all_results[:20]:
            s = "+" if r['net_avg'] > 0 else " "
            print(f"  {r['wallet'][:20]}.. {r['trips']:>6} {s}{r['net_avg']:>8.1f}bp {r['wr_net']:>4.0f}% {r['hold_min']:>5.0f}m ${r['total_net_usd']:>+7.2f}")


if __name__ == "__main__":
    main()
