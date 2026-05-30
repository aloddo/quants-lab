#!/usr/bin/env python3
"""
Bybit Options Trader -- Cash-Secured Put / Credit Spread / Iron Condor

Supports multiple strategies on Bybit USDT-settled BTC/ETH options.
Small capital test mode ($399 USDT on Bybit).

Usage:
    python scripts/bybit_options_trader.py scan          # Show best trades
    python scripts/bybit_options_trader.py trade --strategy put --strike 77000 --expiry 22MAY26
    python scripts/bybit_options_trader.py positions      # Show open options positions
    python scripts/bybit_options_trader.py close --symbol BTC-22MAY26-77000-P-USDT

Strategies:
    put     - Cash-secured put (sell OTM put)
    spread  - Put credit spread (sell higher, buy lower)
    ic      - Iron condor (put spread + call spread)
    strangle - Short strangle (sell OTM put + call)
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [options] %(levelname)s: %(message)s")
logger = logging.getLogger("options")

# Load env
for k, v in os.environ.items():
    pass  # already loaded by caller


def get_session():
    from pybit.unified_trading import HTTP
    return HTTP(
        api_key=os.environ.get("BYBIT_API_KEY"),
        api_secret=os.environ.get("BYBIT_API_SECRET"),
    )


def get_chain(session, base_coin="BTC", expiry=None, min_dte=3, max_dte=14):
    """Get options chain with live prices, filtered by DTE."""
    tickers = session.get_tickers(category="option", baseCoin=base_coin)
    now = datetime.now(timezone.utc)
    spot = None
    options = []

    for t in tickers["result"]["list"]:
        if expiry and expiry not in t["symbol"]:
            continue
        if not spot and t.get("underlyingPrice"):
            spot = float(t["underlyingPrice"])

        bid = float(t.get("bid1Price", 0))
        ask = float(t.get("ask1Price", 0))
        mark = float(t.get("markPrice", 0))
        iv = float(t.get("markIv", 0))

        parts = t["symbol"].split("-")
        if len(parts) < 5:
            continue
        strike = int(parts[2])
        opt_type = parts[3]  # C or P

        # Parse expiry for DTE
        exp_str = parts[1]
        try:
            exp_dt = datetime.strptime(exp_str, "%d%b%y").replace(tzinfo=timezone.utc)
            dte = (exp_dt - now).days
        except ValueError:
            continue

        if not expiry and (dte < min_dte or dte > max_dte):
            continue

        options.append({
            "symbol": t["symbol"],
            "strike": strike,
            "type": opt_type,
            "bid": bid,
            "ask": ask,
            "mark": mark,
            "iv": iv,
            "dte": dte,
            "expiry": exp_str,
        })

    return options, spot


def scan(args):
    """Show best available trades across strategies."""
    session = get_session()
    options, spot = get_chain(session, args.coin, args.expiry)

    if not spot:
        logger.error("Could not determine spot price")
        return

    SIZE = 0.01  # minimum BTC contract
    FEE_PER_LEG = 0.0003 * spot * SIZE

    import pandas as pd
    df = pd.DataFrame(options)
    if df.empty:
        logger.error("No options found")
        return

    # Group by expiry
    for exp, group in df.groupby("expiry"):
        dte = group["dte"].iloc[0]
        puts = group[group["type"] == "P"].sort_values("strike", ascending=False)
        calls = group[group["type"] == "C"].sort_values("strike")

        print(f"\n{'='*70}")
        print(f"Expiry: {exp} ({dte} DTE), Spot: ${spot:,.2f}, Size: {SIZE} BTC")
        print(f"{'='*70}")

        # Cash-secured puts (OTM only)
        otm_puts = puts[(puts["bid"] > 0) & (puts["strike"] < spot)].sort_values("strike", ascending=False)
        print(f"\n--- CASH-SECURED PUTS ---")
        for _, p in otm_puts.head(8).iterrows():
            credit = p["bid"] * SIZE - FEE_PER_LEG
            otm = (spot - p["strike"]) / spot * 100
            margin_est = p["strike"] * SIZE * 0.15
            weekly_yield = credit / margin_est * 100 if margin_est > 0 else 0
            if credit > 0:
                print(f"  Sell ${p['strike']:,}P: credit=${credit:.2f}, OTM={otm:.1f}%, IV={p['iv']*100:.1f}%, yield={weekly_yield:.1f}%/wk")

        # Best credit spreads (put side, OTM only)
        print(f"\n--- PUT CREDIT SPREADS ---")
        put_list = otm_puts
        shown = 0
        for i in range(min(6, len(put_list))):
            short = put_list.iloc[i]
            for j in range(i + 1, len(put_list)):
                long_leg = put_list.iloc[j]
                if long_leg["ask"] <= 0:
                    continue
                credit = (short["bid"] - long_leg["ask"]) * SIZE - 2 * FEE_PER_LEG
                width = (short["strike"] - long_leg["strike"]) * SIZE
                if credit <= 0 or width <= 0:
                    continue
                max_loss = width - credit
                rr = max_loss / credit
                otm = (spot - short["strike"]) / spot * 100
                if rr < 15 and shown < 5:
                    print(f"  ${short['strike']:,}/${long_leg['strike']:,}P: credit=${credit:.2f}, loss=${max_loss:.2f}, R:R={rr:.1f}:1, OTM={otm:.1f}%")
                    shown += 1
                break

        # Short strangles
        print(f"\n--- SHORT STRANGLES ---")
        for put_otm, call_otm in [(0.04, 0.04), (0.05, 0.05), (0.06, 0.06)]:
            put_k = round(spot * (1 - put_otm) / 1000) * 1000
            call_k = round(spot * (1 + call_otm) / 1000) * 1000
            p_row = puts[puts["strike"] == put_k]
            c_row = calls[calls["strike"] == call_k]
            if p_row.empty or c_row.empty:
                continue
            p_bid = p_row.iloc[0]["bid"]
            c_bid = c_row.iloc[0]["bid"]
            if p_bid <= 0 or c_bid <= 0:
                continue
            credit = (p_bid + c_bid) * SIZE - 2 * FEE_PER_LEG
            range_pct = (call_k - put_k) / spot * 100
            print(f"  ${put_k:,}P + ${call_k:,}C: credit=${credit:.2f}, range={range_pct:.0f}%")


def trade(args):
    """Place an options trade."""
    session = get_session()

    if args.strategy == "put":
        symbol = f"BTC-{args.expiry}-{args.strike}-P-USDT"
        logger.info(f"Selling {symbol} x {args.qty}")

        try:
            result = session.place_order(
                category="option",
                symbol=symbol,
                side="Sell",
                orderType="Limit",
                qty=str(args.qty),
                price=str(args.price) if args.price else None,
                timeInForce="GTC",
                reduceOnly=False,
            )
            logger.info(f"Order placed: {json.dumps(result['result'], indent=2)}")
        except Exception as e:
            logger.error(f"Order failed: {e}")

    elif args.strategy == "spread":
        # Put credit spread: sell higher strike, buy lower strike
        short_symbol = f"BTC-{args.expiry}-{args.strike}-P-USDT"
        long_symbol = f"BTC-{args.expiry}-{args.long_strike}-P-USDT"
        logger.info(f"Credit spread: SELL {short_symbol}, BUY {long_symbol}")

        try:
            # Sell short leg
            r1 = session.place_order(
                category="option", symbol=short_symbol,
                side="Sell", orderType="Market", qty=str(args.qty), timeInForce="IOC",
            )
            logger.info(f"Short leg: {r1['result']}")

            # Buy long leg
            r2 = session.place_order(
                category="option", symbol=long_symbol,
                side="Buy", orderType="Market", qty=str(args.qty), timeInForce="IOC",
            )
            logger.info(f"Long leg: {r2['result']}")
        except Exception as e:
            logger.error(f"Spread order failed: {e}")

    else:
        logger.error(f"Strategy '{args.strategy}' not yet implemented for trade")


def positions(args):
    """Show open options positions."""
    session = get_session()
    try:
        result = session.get_positions(category="option", settleCoin="USDT")
        pos_list = result["result"]["list"]
        if not pos_list:
            print("No open options positions.")
            return

        print(f"\n{'Symbol':40s} {'Side':>6s} {'Size':>8s} {'Entry':>10s} {'Mark':>10s} {'uPnL':>10s}")
        print("-" * 90)
        for p in pos_list:
            if float(p["size"]) == 0:
                continue
            print(f"{p['symbol']:40s} {p['side']:>6s} {p['size']:>8s} {p['avgPrice']:>10s} {p['markPrice']:>10s} ${float(p['unrealisedPnl']):>+9.2f}")
    except Exception as e:
        logger.error(f"Failed to get positions: {e}")


def main():
    parser = argparse.ArgumentParser(description="Bybit Options Trader")
    sub = parser.add_subparsers(dest="command")

    # Scan
    scan_p = sub.add_parser("scan", help="Show best available trades")
    scan_p.add_argument("--coin", default="BTC")
    scan_p.add_argument("--expiry", default=None)

    # Trade
    trade_p = sub.add_parser("trade", help="Place a trade")
    trade_p.add_argument("--strategy", required=True, choices=["put", "spread", "ic", "strangle"])
    trade_p.add_argument("--strike", type=int, required=True)
    trade_p.add_argument("--long-strike", type=int, help="Long leg strike (for spreads)")
    trade_p.add_argument("--expiry", required=True)
    trade_p.add_argument("--qty", type=float, default=0.01)
    trade_p.add_argument("--price", type=float, help="Limit price (omit for market)")

    # Positions
    sub.add_parser("positions", help="Show open positions")

    # Close
    close_p = sub.add_parser("close", help="Close a position")
    close_p.add_argument("--symbol", required=True)

    args = parser.parse_args()

    if args.command == "scan":
        scan(args)
    elif args.command == "trade":
        trade(args)
    elif args.command == "positions":
        positions(args)
    elif args.command == "close":
        logger.info(f"Close not yet implemented for {args.symbol}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
