"""
Options V4 Risk Engine -- Pre-trade checks, kill switch, liquidity filters.
Codex conditions: no naked short, exchange-aware kill, liquidity filters.
"""
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pymongo import MongoClient

logger = logging.getLogger("options_v4.risk")

MONGO_URI = "mongodb://localhost:27017/quants_lab"


@dataclass
class RiskConfig:
    max_risk_pct: float = 0.05        # 5% of capital per trade (BTC $2K grid requires this)
    equity_kill_pct: float = 0.90     # Kill at 90% of starting equity
    max_consecutive_losses: int = 3
    max_concurrent: int = 4           # 2 spreads (4 legs) allowed: BTC + ETH
    min_bid_depth: float = 5.0        # $5 min bid size for liquidity
    max_spread_pct: float = 0.20      # 20% max bid/ask spread as % of mid
    min_credit_usd: float = 0.50      # Min $0.50 credit after fees


@dataclass
class AccountState:
    equity: float = 0.0
    available: float = 0.0
    open_positions: int = 0
    open_risk: float = 0.0           # Total max loss of open intents
    consecutive_losses: int = 0
    starting_equity: float = 399.0


def fetch_account(session) -> AccountState:
    """Fetch Bybit account state."""
    state = AccountState()
    try:
        bal = session.get_wallet_balance(accountType="UNIFIED")
        for c in bal["result"]["list"][0]["coin"]:
            if c["coin"] == "USDT":
                state.equity = float(c["equity"])
                state.available = float(c.get("availableToWithdraw", 0) or 0)
                break
        pos = session.get_positions(category="option", settleCoin="USDT")
        state.open_positions = sum(1 for p in pos["result"]["list"] if float(p["size"]) > 0)
    except Exception as e:
        logger.error(f"Account fetch failed: {e}")

    # Open intents from MongoDB
    db = MongoClient(MONGO_URI)["quants_lab"]
    open_intents = list(db["options_v4_intents"].find({
        "status": {"$nin": ["closed", "failed_long", "killed"]}
    }))
    state.open_risk = sum(i.get("max_loss", 0) for i in open_intents)
    return state


def check_pretrade(config: RiskConfig, account: AccountState,
                   proposed_max_loss: float, proposed_credit: float) -> tuple[bool, str]:
    """Pre-trade risk gate. Returns (allowed, reason)."""
    # Kill switch: equity floor
    if account.equity < account.starting_equity * config.equity_kill_pct:
        return False, f"KILL: equity ${account.equity:.0f} < floor ${account.starting_equity * config.equity_kill_pct:.0f}"

    # Consecutive losses
    if account.consecutive_losses >= config.max_consecutive_losses:
        return False, f"KILL: {account.consecutive_losses} consecutive losses"

    # Concurrent positions
    if account.open_positions >= config.max_concurrent:
        return False, f"Max concurrent {config.max_concurrent} reached"

    # Per-trade risk limit
    max_allowed = account.equity * config.max_risk_pct
    if proposed_max_loss > max_allowed:
        return False, f"Max loss ${proposed_max_loss:.2f} > {config.max_risk_pct*100:.0f}% of equity (${max_allowed:.2f})"

    # Min credit
    if proposed_credit > 0 and proposed_credit < config.min_credit_usd:
        return False, f"Credit ${proposed_credit:.2f} < min ${config.min_credit_usd:.2f}"

    # Total exposure (10% cap across all positions)
    total_risk = account.open_risk + proposed_max_loss
    if total_risk > account.equity * 0.10:
        return False, f"Total risk ${total_risk:.2f} > 10% of equity"

    return True, "OK"


def check_liquidity(session, symbol: str, config: RiskConfig) -> tuple[bool, str]:
    """Check if option has sufficient liquidity to trade. Codex condition #6."""
    try:
        ob = session.get_orderbook(category="option", symbol=symbol, limit=5)
        bids = ob["result"].get("b", [])
        asks = ob["result"].get("a", [])

        if not bids or not asks:
            return False, f"No bids/asks for {symbol}"

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        bid_size = float(bids[0][1])

        if best_bid <= 0:
            return False, f"Zero bid for {symbol}"

        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid if mid > 0 else 1.0

        if spread_pct > config.max_spread_pct:
            return False, f"Spread {spread_pct*100:.1f}% > {config.max_spread_pct*100:.0f}% for {symbol}"

        if bid_size * best_bid < config.min_bid_depth:
            return False, f"Bid depth ${bid_size * best_bid:.2f} < ${config.min_bid_depth:.2f} for {symbol}"

        return True, "OK"
    except Exception as e:
        return False, f"Liquidity check failed: {e}"


def check_instrument(session, symbol: str, qty: float) -> tuple[bool, str]:
    """Verify min size from Bybit instrument metadata. Codex condition #5."""
    try:
        # Parse base coin from symbol
        base = symbol.split("-")[0]
        result = session.get_instruments_info(category="option", baseCoin=base)
        for inst in result["result"]["list"]:
            if inst["symbol"] == symbol:
                min_qty = float(inst["lotSizeFilter"]["minOrderQty"])
                qty_step = float(inst["lotSizeFilter"]["qtyStep"])
                if qty < min_qty:
                    return False, f"Qty {qty} < min {min_qty} for {symbol}"
                if inst.get("status") != "Trading":
                    return False, f"{symbol} not trading (status={inst['status']})"
                return True, "OK"
        return False, f"Instrument {symbol} not found"
    except Exception as e:
        return False, f"Instrument check failed: {e}"
