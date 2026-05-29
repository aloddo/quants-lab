#!/usr/bin/env python3
"""V14-shadow-A: V13 entry-only + source-proportional sizing → live HL executor.

Codex-validated spec (consult #6, 2026-05-30 00:21 CEST):
  - Pool: 3 source wallets (static, from latest WF fold 8 train+val ranking)
  - Sizing: min(source.pct_eq, 1/K) × follower_equity, $10 floor
  - Entry-only copying (NOT addons/trims)
  - Cooldown 1800s per (wallet, coin)
  - Exclude xyz:/flx: prefixes
  - Hard stops: daily -$15, total -$30, drawdown 30%
  - Source equity polling REQUIRED, NO equal-weight fallback
  - Telegram alerts every entry/exit/kill

Implementation notes:
- Polls every 60s (not WebSocket — simpler for shadow)
- Per (source_wallet, coin) state machine: FLAT → OPEN → CLOSED
- All trades IOC market (no limit orders)
- File-persisted state (JSON) — survives restart
- ABORT-only-on-failure: no silent degradation
"""
import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import eth_account
import requests
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [v14_shadow] %(levelname)s: %(message)s",
                    stream=sys.stdout)
logger = logging.getLogger("v14_shadow")

HL_API = "https://api.hyperliquid.xyz"
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1003576397888")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

EXCLUDE_PREFIXES = ("xyz:", "flx:", "vntl:")  # informational; we only trade main perps


def _tg(msg: str):
    if not TG_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": f"[V14-shadow] {msg}"}, timeout=5,
        )
    except Exception as e:
        logger.warning(f"TG failed: {e}")


# ============================================================
# State persistence
# ============================================================

@dataclass
class OpenLeg:
    source_wallet: str
    coin: str
    side: str             # "long" / "short"
    source_size: float    # |szi| at entry time
    source_pct_eq: float  # at entry
    follower_qty: float   # signed
    follower_entry_px: float
    follower_notional: float
    entry_ts: int
    journey_seq: int      # to detect re-entry vs same journey


@dataclass
class State:
    open_legs: dict       # key f"{wallet}|{coin}|{seq}" -> OpenLeg (serializable)
    cooldowns: dict       # key f"{wallet}|{coin}" -> ts ms
    source_prev_pos: dict # wallet -> {coin: szi} (previous poll state)
    source_journey_seq: dict  # wallet -> {coin: int sequence counter}
    daily_pnl: dict       # YYYY-MM-DD -> float
    total_realized_pnl: float
    starting_equity: float
    last_alert_dd_pct: float  # to throttle DD alerts
    killed: bool
    first_cycle_done: bool = False  # tracks "we've baselined the source state"

    @staticmethod
    def load(path: Path) -> "State":
        if not path.exists():
            return State(open_legs={}, cooldowns={}, source_prev_pos={},
                         source_journey_seq={}, daily_pnl={}, total_realized_pnl=0.0,
                         starting_equity=0.0, last_alert_dd_pct=0.0, killed=False,
                         first_cycle_done=False)
        d = json.loads(path.read_text())
        # Rebuild OpenLeg objects
        d["open_legs"] = {k: OpenLeg(**v) for k, v in d.get("open_legs", {}).items()}
        return State(**d)

    def save(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        out = {**asdict(self)}
        # OpenLeg -> dict
        out["open_legs"] = {k: asdict(v) for k, v in self.open_legs.items()}
        path.write_text(json.dumps(out, indent=2, default=str))


# ============================================================
# HL helpers
# ============================================================

def get_source_state(wallet: str) -> Optional[dict]:
    """Returns {coin: {"szi": float}} for source wallet's MAIN-dex positions only, plus equity."""
    try:
        r = requests.post(HL_API + "/info",
                          json={"type": "clearinghouseState", "user": wallet}, timeout=5)
        data = r.json()
        if data is None:
            return None
        positions = {}
        for p in data.get("assetPositions", []):
            pos = p.get("position", {})
            coin = pos.get("coin", "")
            szi = float(pos.get("szi", 0))
            if abs(szi) > 1e-10:
                positions[coin] = {
                    "szi": szi,
                    "positionValue": float(pos.get("positionValue", 0)),
                }
        margin = data.get("marginSummary", {})
        equity = float(margin.get("accountValue", 0))
        return {"positions": positions, "equity": equity}
    except Exception as e:
        logger.warning(f"source state {wallet[:14]} failed: {e}")
        return None


def get_follower_equity(info: Info, parent_address: str) -> Optional[float]:
    """Spot USDC balance (per HL_EQ rule #16: spot USDC only)."""
    try:
        r = requests.post(HL_API + "/info",
                          json={"type": "spotClearinghouseState", "user": parent_address},
                          timeout=5)
        data = r.json()
        for b in data.get("balances", []):
            if b.get("coin") == "USDC":
                return float(b.get("total", 0))
        return 0.0
    except Exception as e:
        logger.warning(f"follower equity fetch failed: {e}")
        return None


def place_ioc_market(exchange: Exchange, info: Info, coin: str, is_buy: bool,
                     notional_usd: float, sz_decimals: dict, max_slippage_bps: int = 15) -> Optional[dict]:
    """Place an IOC market order. Returns {"oid", "avg_px", "qty"} on fill, else None."""
    # Mid price for sizing
    try:
        mids = info.all_mids()
        mid = float(mids.get(coin, 0))
        if mid <= 0:
            logger.warning(f"no mid for {coin}")
            return None
    except Exception as e:
        logger.warning(f"mids fetch failed: {e}")
        return None
    sz = notional_usd / mid
    sz_dec = sz_decimals.get(coin, 2)
    sz_rounded = round(sz, sz_dec)
    if sz_rounded <= 0:
        logger.warning(f"qty rounded to 0 for {coin} (notional ${notional_usd:.2f} mid ${mid:.4f})")
        return None
    # IOC price = mid * (1 + slip * side)
    limit_px = mid * (1 + (1 if is_buy else -1) * max_slippage_bps / 10_000)
    try:
        resp = exchange.order(
            name=coin, is_buy=is_buy, sz=sz_rounded, limit_px=round(limit_px, 6),
            order_type={"limit": {"tif": "Ioc"}}, reduce_only=False,
        )
        if resp.get("status") != "ok":
            logger.warning(f"order rejected {coin}: {resp}")
            return None
        statuses = resp["response"]["data"]["statuses"]
        for s in statuses:
            if "filled" in s:
                f = s["filled"]
                return {"oid": f.get("oid"), "avg_px": float(f.get("avgPx", limit_px)),
                        "qty": float(f.get("totalSz", sz_rounded))}
            if "resting" in s:
                # IOC + resting = no fill (IOC cancels remainder)
                logger.warning(f"order {coin} resting (zero fill): {s}")
                return None
            if "error" in s:
                logger.warning(f"order error {coin}: {s['error']}")
                return None
        return None
    except Exception as e:
        logger.error(f"order exception {coin}: {e}")
        return None


# ============================================================
# Main loop
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", nargs="+", default=[
        "0x7bbfea8bcb34a30ead8a77bb207fa593e279a8d0",
        "0x86995f4a229b5a6512bf46b0a65df450f37de109",
        "0xb27bbaadcdfeab937069b4d966ee6bf5a32c999b",
    ], help="Source wallets to copy (static pool)")
    ap.add_argument("--K", type=int, default=3, help="Pool size for cap (1/K)")
    ap.add_argument("--poll-s", type=int, default=60)
    ap.add_argument("--cooldown-s", type=int, default=1800)
    ap.add_argument("--max-daily-loss", type=float, default=-15.0)
    ap.add_argument("--max-total-loss", type=float, default=-30.0)
    ap.add_argument("--alert-dd-pct", type=float, default=0.20)
    ap.add_argument("--pause-dd-pct", type=float, default=0.30)
    ap.add_argument("--min-notional", type=float, default=10.0)
    ap.add_argument("--max-slippage-bps", type=int, default=15)
    ap.add_argument("--state-file", default="/tmp/v14_shadow_state.json")
    ap.add_argument("--shadow", action="store_true",
                    help="Shadow mode: log decisions only, do not execute orders")
    ap.add_argument("--max-gross-leverage", type=float, default=1.0,
                    help="Max total open follower notional / follower equity (e.g. 1.0 = no leverage)")
    ap.add_argument("--max-entries-per-cycle", type=int, default=3,
                    help="Max NEW positions opened per poll cycle (burst protection)")
    args = ap.parse_args()

    K = max(1, args.K)
    pool = [w.lower() for w in args.pool]
    logger.info(f"V14-shadow-A: pool={pool} K={K} poll={args.poll_s}s cd={args.cooldown_s}s")
    logger.info(f"Stops: daily {args.max_daily_loss}, total {args.max_total_loss}, "
                f"alert dd {args.alert_dd_pct*100:.0f}%, pause dd {args.pause_dd_pct*100:.0f}%")
    if args.shadow:
        logger.warning("SHADOW MODE: no orders will be placed")

    # SDK
    if "HL_PRIVATE_KEY" not in os.environ:
        logger.error("HL_PRIVATE_KEY not set")
        sys.exit(1)
    parent = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
    agent_key = os.environ["HL_PRIVATE_KEY"]
    agent_addr = os.environ["HL_ADDRESS"]
    acct = eth_account.Account.from_key(agent_key)
    info = Info(HL_API, skip_ws=True)
    exchange = Exchange(acct, HL_API, account_address=agent_addr)

    # Meta for sz_decimals
    meta = info.meta_and_asset_ctxs()
    sz_decimals = {}
    for u in meta[0]["universe"]:
        sz_decimals[u["name"]] = u.get("szDecimals", 2)

    state_path = Path(args.state_file)
    state = State.load(state_path)
    if state.starting_equity == 0.0:
        eq = get_follower_equity(info, parent)
        if eq is None:
            logger.error("Cannot fetch follower equity at startup")
            sys.exit(1)
        state.starting_equity = eq
        logger.info(f"Starting equity: ${eq:.2f}")
        state.save(state_path)

    running = True
    def handle_sigterm(*_):
        nonlocal running
        logger.info("SIGTERM received, will exit after current cycle")
        running = False
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    _tg(f"BOOT pool={[w[:8] for w in pool]} K={K} start_eq=${state.starting_equity:.2f}")

    while running:
        cycle_t0 = time.time()
        try:
            run_cycle(args, K, pool, info, exchange, parent, sz_decimals, state, state_path)
        except Exception as e:
            logger.exception(f"cycle failed: {e}")
            _tg(f"CYCLE ERROR: {e}")
        elapsed = time.time() - cycle_t0
        sleep_s = max(0, args.poll_s - elapsed)
        time.sleep(sleep_s)

    state.save(state_path)
    logger.info("clean exit")


def run_cycle(args, K, pool, info, exchange, parent, sz_decimals, state: State, state_path: Path):
    """Single poll cycle: fetch source states, detect changes, act."""
    now_ms = int(time.time() * 1000)

    # 1. Kill switch
    if state.killed:
        return

    # 2. Follower equity
    eq = get_follower_equity(info, parent)
    if eq is None:
        logger.warning("equity fetch failed; skipping cycle")
        return
    drawdown_pct = (state.starting_equity - eq) / state.starting_equity if state.starting_equity > 0 else 0.0
    if drawdown_pct >= args.pause_dd_pct:
        logger.error(f"DD pause: {drawdown_pct:.1%} >= {args.pause_dd_pct:.0%}, killing")
        kill_switch(state, state_path, info, exchange, parent, sz_decimals, args,
                    reason=f"drawdown {drawdown_pct:.1%}")
        return
    if drawdown_pct >= args.alert_dd_pct and drawdown_pct - state.last_alert_dd_pct >= 0.05:
        _tg(f"DD ALERT {drawdown_pct:.1%} (eq ${eq:.2f} vs start ${state.starting_equity:.2f})")
        state.last_alert_dd_pct = drawdown_pct

    # Daily PnL stop
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = state.daily_pnl.get(today, 0.0)
    if daily <= args.max_daily_loss:
        logger.error(f"daily PnL stop: ${daily:.2f} <= ${args.max_daily_loss}, killing")
        kill_switch(state, state_path, info, exchange, parent, sz_decimals, args,
                    reason=f"daily PnL {daily:.2f}")
        return
    if state.total_realized_pnl <= args.max_total_loss:
        logger.error(f"total PnL stop: ${state.total_realized_pnl:.2f} <= ${args.max_total_loss}, killing")
        kill_switch(state, state_path, info, exchange, parent, sz_decimals, args,
                    reason=f"total PnL {state.total_realized_pnl:.2f}")
        return

    # 3. Compute current gross exposure (for leverage cap)
    current_gross_notional = sum(abs(leg.follower_notional) for leg in state.open_legs.values())
    max_gross = args.max_gross_leverage * eq
    entries_this_cycle = 0

    # 4. For each source wallet: fetch state, detect entry/exit
    for source in pool:
        src = get_source_state(source)
        if src is None:
            logger.warning(f"source {source[:14]} fetch failed; skip this wallet this cycle")
            continue
        src_eq = src["equity"]
        if src_eq <= 0:
            logger.warning(f"source {source[:14]} equity={src_eq}; skip")
            continue
        current_pos = {c: p["szi"] for c, p in src["positions"].items()
                       if not c.startswith(EXCLUDE_PREFIXES)}
        prev_pos = state.source_prev_pos.get(source, {})

        # FIRST-CYCLE BASELINE: don't trade pre-existing positions. Just snapshot them as prior state.
        if not state.first_cycle_done:
            state.source_prev_pos[source] = current_pos
            logger.info(f"BASELINE {source[:14]}: {len(current_pos)} pre-existing positions snapshotted (not traded)")
            continue

        # Detect transitions per coin
        all_coins = set(current_pos.keys()) | set(prev_pos.keys())
        for coin in sorted(all_coins):
            cur = current_pos.get(coin, 0.0)
            prv = prev_pos.get(coin, 0.0)
            cur_open = abs(cur) > 1e-10
            prv_open = abs(prv) > 1e-10

            if not prv_open and cur_open:
                # ENTRY detected — gate on cycle budget + gross cap
                if entries_this_cycle >= args.max_entries_per_cycle:
                    logger.info(f"CYCLE CAP: {entries_this_cycle} entries already, skip {source[:8]} {coin}")
                    continue
                if current_gross_notional >= max_gross:
                    logger.warning(f"GROSS CAP: ${current_gross_notional:.2f} >= ${max_gross:.2f}, skip {source[:8]} {coin}")
                    continue
                ok = handle_entry(args, K, source, coin, cur, src["positions"][coin]["positionValue"],
                                  src_eq, info, exchange, sz_decimals, state, state_path, now_ms,
                                  max_gross_remaining=max_gross - current_gross_notional)
                if ok:
                    entries_this_cycle += 1
                    leg = next((l for l in state.open_legs.values()
                                if l.source_wallet == source and l.coin == coin), None)
                    if leg:
                        current_gross_notional += abs(leg.follower_notional)

            elif prv_open and not cur_open:
                # EXIT detected
                handle_exit(args, source, coin, info, exchange, sz_decimals, state, state_path, now_ms)

            elif prv_open and cur_open and (prv * cur) < 0:
                # SIGN FLIP: close + open opposite (treated as exit then entry)
                handle_exit(args, source, coin, info, exchange, sz_decimals, state, state_path, now_ms)
                if entries_this_cycle < args.max_entries_per_cycle and current_gross_notional < max_gross:
                    ok = handle_entry(args, K, source, coin, cur, src["positions"][coin]["positionValue"],
                                      src_eq, info, exchange, sz_decimals, state, state_path, now_ms,
                                      max_gross_remaining=max_gross - current_gross_notional)
                    if ok:
                        entries_this_cycle += 1

        state.source_prev_pos[source] = current_pos

    if not state.first_cycle_done:
        state.first_cycle_done = True
        logger.info(f"BASELINE COMPLETE: tracking new entries only")
        _tg(f"BASELINE done — tracking new entries only from this cycle forward")
    state.save(state_path)


def handle_entry(args, K, source, coin, cur_szi, position_value, src_eq, info, exchange,
                  sz_decimals, state: State, state_path: Path, now_ms: int,
                  max_gross_remaining: float = None) -> bool:
    """Detected new entry by source. Open follower position via PROP sizing.
    Returns True if entry placed (or shadow-recorded), False if skipped."""
    cd_key = f"{source}|{coin}"
    if state.cooldowns.get(cd_key, 0) > now_ms:
        logger.info(f"COOLDOWN {source[:8]} {coin}: skip entry")
        return False
    side = "long" if cur_szi > 0 else "short"
    sign = 1 if side == "long" else -1
    src_pct = abs(position_value) / src_eq if src_eq > 0 else 0
    if src_pct <= 0:
        return False

    follower_eq = get_follower_equity(info, os.environ.get("HL_QUERY_ADDRESS",
                                                            "0x11ca20aeb7cd014cf8406560ae405b12601994b4"))
    if follower_eq is None or follower_eq <= 0:
        logger.warning(f"follower equity unknown; skip entry {coin}")
        return False
    pct_capped = min(src_pct, 1.0 / K)
    notional = pct_capped * follower_eq
    if max_gross_remaining is not None and notional > max_gross_remaining:
        notional = max_gross_remaining
        logger.info(f"GROSS-REMAINING reducing {coin} notional to ${notional:.2f}")
    if notional < args.min_notional:
        logger.info(f"SKIP {source[:8]} {coin} {side}: notional ${notional:.2f} < ${args.min_notional} "
                    f"(src_pct={src_pct:.3%} cap=1/{K})")
        return False

    # Bump journey seq
    seq_map = state.source_journey_seq.setdefault(source, {})
    seq = seq_map.get(coin, 0) + 1
    seq_map[coin] = seq

    leg_key = f"{source}|{coin}|{seq}"
    if leg_key in state.open_legs:
        logger.warning(f"leg {leg_key} already open; refusing dup")
        return False

    if args.shadow:
        logger.info(f"SHADOW ENTRY {source[:8]} {coin} {side} notional=${notional:.2f} src_pct={src_pct:.2%}")
        try:
            mid = float(info.all_mids().get(coin, 0))
        except Exception:
            mid = 0
        leg = OpenLeg(source_wallet=source, coin=coin, side=side,
                      source_size=abs(cur_szi), source_pct_eq=src_pct,
                      follower_qty=sign * (notional / mid if mid > 0 else 0),
                      follower_entry_px=mid, follower_notional=notional,
                      entry_ts=now_ms, journey_seq=seq)
        state.open_legs[leg_key] = leg
        state.cooldowns[cd_key] = now_ms + args.cooldown_s * 1000
        _tg(f"SHADOW ENTRY {source[:8]} {coin} {side} ${notional:.2f}")
        return True

    fill = place_ioc_market(exchange, info, coin, is_buy=(sign > 0), notional_usd=notional,
                             sz_decimals=sz_decimals, max_slippage_bps=args.max_slippage_bps)
    if fill is None:
        logger.warning(f"ENTRY FILL FAILED {source[:8]} {coin} {side}; not tracking")
        return False
    leg = OpenLeg(source_wallet=source, coin=coin, side=side,
                  source_size=abs(cur_szi), source_pct_eq=src_pct,
                  follower_qty=sign * fill["qty"], follower_entry_px=fill["avg_px"],
                  follower_notional=fill["qty"] * fill["avg_px"],
                  entry_ts=now_ms, journey_seq=seq)
    state.open_legs[leg_key] = leg
    state.cooldowns[cd_key] = now_ms + args.cooldown_s * 1000
    logger.info(f"ENTRY {leg_key} qty={leg.follower_qty:+.4f} px={leg.follower_entry_px:.4f} "
                f"notional=${leg.follower_notional:.2f}")
    _tg(f"ENTRY {source[:8]} {coin} {side} ${leg.follower_notional:.2f} @ {leg.follower_entry_px:.4f}")
    return True


def handle_exit(args, source, coin, info, exchange, sz_decimals, state: State, state_path: Path, now_ms: int):
    """Detected source position closed. Close any open follower legs for this (source, coin)."""
    keys_to_close = [k for k, leg in state.open_legs.items()
                     if leg.source_wallet == source and leg.coin == coin]
    for leg_key in keys_to_close:
        leg = state.open_legs[leg_key]
        close_qty = abs(leg.follower_qty)
        if close_qty <= 0:
            del state.open_legs[leg_key]
            continue
        is_buy_close = leg.follower_qty < 0  # short → buy to close

        if args.shadow:
            try:
                mid = float(info.all_mids().get(coin, 0))
            except Exception:
                mid = leg.follower_entry_px
            gross_pnl = leg.follower_qty * (mid - leg.follower_entry_px)
            logger.info(f"SHADOW EXIT {leg_key} mid=${mid:.4f} gross_pnl=${gross_pnl:+.2f}")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            state.daily_pnl[today] = state.daily_pnl.get(today, 0.0) + gross_pnl
            state.total_realized_pnl += gross_pnl
            del state.open_legs[leg_key]
            _tg(f"SHADOW EXIT {source[:8]} {coin} pnl=${gross_pnl:+.2f}")
            continue

        notional_close = close_qty * leg.follower_entry_px  # approximate; actual size via qty
        # Place close IOC: notional_usd = close_qty * mid (compute from qty directly)
        try:
            mid = float(info.all_mids().get(coin, 0))
            if mid <= 0:
                logger.warning(f"no mid for close {coin}; defer")
                continue
        except Exception:
            continue
        fill = place_ioc_market(exchange, info, coin, is_buy=is_buy_close,
                                 notional_usd=close_qty * mid,
                                 sz_decimals=sz_decimals,
                                 max_slippage_bps=args.max_slippage_bps)
        if fill is None:
            logger.warning(f"EXIT FILL FAILED {leg_key}; will retry next cycle")
            continue
        gross_pnl = leg.follower_qty * (fill["avg_px"] - leg.follower_entry_px)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        state.daily_pnl[today] = state.daily_pnl.get(today, 0.0) + gross_pnl
        state.total_realized_pnl += gross_pnl
        del state.open_legs[leg_key]
        logger.info(f"EXIT {leg_key} px={fill['avg_px']:.4f} gross_pnl=${gross_pnl:+.2f}")
        _tg(f"EXIT {source[:8]} {coin} pnl=${gross_pnl:+.2f} (total ${state.total_realized_pnl:+.2f})")


def kill_switch(state: State, state_path: Path, info, exchange, parent, sz_decimals, args, reason: str):
    """Close all open positions, mark killed."""
    state.killed = True
    state.save(state_path)
    _tg(f"KILL SWITCH: {reason}. Flattening {len(state.open_legs)} legs.")
    for leg_key, leg in list(state.open_legs.items()):
        if args.shadow:
            del state.open_legs[leg_key]
            continue
        try:
            mid = float(info.all_mids().get(leg.coin, 0))
            if mid <= 0:
                continue
            is_buy_close = leg.follower_qty < 0
            close_qty = abs(leg.follower_qty)
            fill = place_ioc_market(exchange, info, leg.coin, is_buy=is_buy_close,
                                     notional_usd=close_qty * mid,
                                     sz_decimals=sz_decimals,
                                     max_slippage_bps=args.max_slippage_bps)
            if fill:
                gross_pnl = leg.follower_qty * (fill["avg_px"] - leg.follower_entry_px)
                state.total_realized_pnl += gross_pnl
                logger.info(f"KILL CLOSE {leg_key} pnl=${gross_pnl:+.2f}")
            del state.open_legs[leg_key]
        except Exception as e:
            logger.error(f"kill close {leg_key} failed: {e}")
    state.save(state_path)


if __name__ == "__main__":
    main()
