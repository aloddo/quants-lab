"""
Liquidate all Binance alt holdings to USDT for arb deployment.

Run with --dry-run first to see what would happen.
Run with --execute to actually sell.

Usage:
    python scripts/arb_liquidate_alts.py --dry-run
    python scripts/arb_liquidate_alts.py --execute
    python scripts/arb_liquidate_alts.py --execute --keep BTC  # keep BTC, sell rest
"""
import asyncio
import argparse
import os
import sys
from pathlib import Path

# Load .env
env_file = Path(__file__).resolve().parents[1] / ".env"
for line in env_file.read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.binance_client import BinanceClient


# Assets to NEVER sell (stablecoins, dust)
SKIP_ASSETS = {'USDT', 'BUSD', 'USDC', 'BNB', 'LUNC', 'LUNA', 'ROSE', 'INJ', 'TIA'}
# BNB kept for potential fee discount later
# LUNC/LUNA/ROSE/INJ/TIA are dust

MIN_SELL_USD = 6  # Binance min notional is $5, buffer to $6


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Actually sell (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--keep", nargs="*", default=[], help="Assets to keep (e.g. --keep BTC)")
    args = parser.parse_args()

    execute = args.execute
    skip = SKIP_ASSETS | set(a.upper() for a in args.keep)

    client = BinanceClient()

    # Get balances
    balances = await client.get_balances()
    print(f"{'Asset':<8} {'Free':>12} {'Symbol':<12} {'Est USD':>10} {'Action'}")
    print("-" * 55)

    # Get prices
    prices_raw = await client.get_ticker_price()
    prices = {p['symbol']: float(p['price']) for p in prices_raw}

    to_sell = []
    total_expected = 0

    for asset, info in sorted(balances.items()):
        free = info['free']
        if free <= 0 or asset in skip:
            continue

        symbol = f"{asset}USDT"
        price = prices.get(symbol, 0)
        usd_val = free * price if price > 0 else 0

        if usd_val < MIN_SELL_USD:
            print(f"{asset:<8} {free:>12.4f} {symbol:<12} ${usd_val:>9.2f}  SKIP (< ${MIN_SELL_USD})")
            continue

        to_sell.append((asset, free, symbol, price, usd_val))
        total_expected += usd_val
        action = "SELL" if execute else "WOULD SELL"
        print(f"{asset:<8} {free:>12.4f} {symbol:<12} ${usd_val:>9.2f}  {action}")

    print(f"\n{'Total':>35} ${total_expected:>9.2f}")

    if not execute:
        print(f"\n  DRY RUN — pass --execute to sell. Add --keep BTC to preserve BTC.")
        await client.close()
        return

    # Execute sells
    print(f"\n  Executing {len(to_sell)} market sells...")
    total_received = 0

    for asset, qty, symbol, price, est_usd in to_sell:
        try:
            # Get exchange info for proper precision
            info = await client.get_exchange_info(symbol)
            filters = {f['filterType']: f for f in info['symbols'][0]['filters']}
            lot = filters.get('LOT_SIZE', {})
            step = float(lot.get('stepSize', 0.00000001))

            # Round qty down to step size
            import math
            rounded_qty = math.floor(qty / step) * step
            if rounded_qty * price < 5:  # below min notional
                print(f"  {symbol}: qty too small after rounding (${rounded_qty * price:.2f}), skipping")
                continue

            result = await client.place_market_order(symbol, "SELL", quantity=rounded_qty)
            filled_qty = float(result.get('executedQty', 0))
            filled_val = float(result.get('cummulativeQuoteQty', 0))
            total_received += filled_val
            print(f"  ✅ {symbol}: sold {filled_qty} for ${filled_val:.2f}")

        except Exception as e:
            print(f"  ❌ {symbol}: {e}")

    print(f"\n  Total USDT received: ${total_received:.2f}")

    # Final balance
    new_balances = await client.get_balances()
    usdt = new_balances.get('USDT', {}).get('free', 0)
    print(f"  New USDT balance: ${usdt:.2f}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
