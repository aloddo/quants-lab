#!/usr/bin/env python3
"""
Exchange-truth PnL tracker for V11/V12 copy trading.

Pulls fills from Hyperliquid exchange API (source of truth), matches to V11 order IDs,
computes per-trade/wallet/coin/day PnL. Outputs to TG and CLI.

Usage:
    python scripts/pnl_tracker.py --daily           # Today's summary
    python scripts/pnl_tracker.py --since 2026-05-17  # Since date
    python scripts/pnl_tracker.py --sync             # Sync fills only (no report)
    python scripts/pnl_tracker.py --tg               # Send daily report to TG
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pymongo
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [pnl_tracker] %(levelname)s: %(message)s")
logger = logging.getLogger("pnl_tracker")

HL_API = "https://api.hyperliquid.xyz"
PARENT_ADDRESS = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
DB_NAME = "quants_lab"
# V16 (2026-06-11, Alberto msg 9222/9226: updated labels + epoch start): track V16 collections.
FILLS_COLLECTION = "v16_exchange_fills"
OID_COLLECTION = "v16_order_ids"
STRATEGY_LABEL = "V16"
TG_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1003576397888")


class PnLTracker:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017")
        self.db = self.client[DB_NAME]
        # Cache OID -> wallet mapping
        self._oid_cache = {}

    def _load_oid_cache(self):
        """Load all V11 order IDs with wallet info."""
        self._oid_cache = {}
        for doc in self.db[OID_COLLECTION].find():
            self._oid_cache[doc["oid"]] = {
                "wallet": doc.get("wallet", ""),
                "wallet_group": doc.get("wallet_group", ""),
                "action": doc.get("action", ""),
                "coin": doc.get("coin", ""),
            }
        logger.info(f"Loaded {len(self._oid_cache)} OID records")

    def sync_fills(self, since_ms: int = None):
        """Pull fills from HL exchange API and store to MongoDB. Deduplicate by tid."""
        if since_ms is None:
            # Default: last 7 days
            since_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)

        try:
            fills = requests.post(f"{HL_API}/info", json={
                "type": "userFillsByTime",
                "user": PARENT_ADDRESS,
                "startTime": since_ms,
            }, timeout=15).json()
        except Exception as e:
            logger.error(f"Failed to fetch fills: {e}")
            return 0

        if not fills:
            logger.info("No fills returned")
            return 0

        new_count = 0
        for f in fills:
            tid = f.get("tid")
            oid = f.get("oid")
            time_ms = f.get("time")
            if not tid:
                continue
            doc = {
                "tid": tid,
                "fill_key": f"{oid}_{time_ms}" if oid and time_ms else None,
                "coin": f["coin"],
                "px": f["px"],
                "sz": f["sz"],
                "side": f["side"],
                "time": f["time"],
                "dir": f.get("dir", ""),
                "closedPnl": f.get("closedPnl", "0"),
                "oid": oid,
                "fee": f.get("fee", "0"),
                "feeToken": f.get("feeToken", "USDC"),
                "startPosition": f.get("startPosition", ""),
            }
            try:
                result = self.db[FILLS_COLLECTION].update_one(
                    {"tid": tid},
                    {"$set": doc},
                    upsert=True,
                )
                if result.upserted_id:
                    new_count += 1
            except pymongo.errors.DuplicateKeyError:
                pass  # Already exists

        logger.info(f"Synced {len(fills)} fills, {new_count} new")
        return new_count

    def compute_pnl(self, since_ms: int, until_ms: int = None) -> dict:
        """Compute PnL from exchange fills in a time window.

        Returns dict with:
        - total: total realized PnL
        - total_fees: total fees paid
        - net: total - fees
        - by_coin: {coin: {pnl, fees, fills, closes, wins}}
        - by_wallet_group: {group: {pnl, fees, fills, closes}}
        - engine_attributed: PnL from fills matched to V11 OIDs
        - unattributed: PnL from fills not matched (pre-epoch, manual, other)
        - open_positions: current positions with unrealized PnL
        """
        self._load_oid_cache()

        query = {"time": {"$gte": since_ms}}
        if until_ms:
            query["time"]["$lte"] = until_ms

        fills = list(self.db[FILLS_COLLECTION].find(query).sort("time", 1))

        result = {
            "period_start": since_ms,
            "period_end": until_ms or int(datetime.now(timezone.utc).timestamp() * 1000),
            "total_fills": len(fills),
            "total_pnl": 0.0,
            "total_fees": 0.0,
            "total_closes": 0,
            "total_wins": 0,
            "engine_pnl": 0.0,
            "engine_fees": 0.0,
            "engine_closes": 0,
            "engine_wins": 0,
            "unattributed_pnl": 0.0,
            "unattributed_closes": 0,
            "by_coin": defaultdict(lambda: {"pnl": 0.0, "fees": 0.0, "fills": 0, "closes": 0, "wins": 0}),
            "by_wallet_group": defaultdict(lambda: {"pnl": 0.0, "fees": 0.0, "fills": 0, "closes": 0, "wins": 0}),
        }

        for f in fills:
            pnl = float(f.get("closedPnl", 0))
            fee = float(f.get("fee", 0))
            coin = f.get("coin", "?")
            oid = f.get("oid")
            is_close = pnl != 0
            is_win = pnl > 0

            # Total stats
            result["total_pnl"] += pnl
            result["total_fees"] += fee
            if is_close:
                result["total_closes"] += 1
                if is_win:
                    result["total_wins"] += 1

            # By coin
            result["by_coin"][coin]["pnl"] += pnl
            result["by_coin"][coin]["fees"] += fee
            result["by_coin"][coin]["fills"] += 1
            if is_close:
                result["by_coin"][coin]["closes"] += 1
                if is_win:
                    result["by_coin"][coin]["wins"] += 1

            # Attribution
            oid_info = self._oid_cache.get(oid)
            if oid_info:
                result["engine_pnl"] += pnl
                result["engine_fees"] += fee
                if is_close:
                    result["engine_closes"] += 1
                    if is_win:
                        result["engine_wins"] += 1

                # By wallet group
                group = oid_info.get("wallet_group", "unknown") or "unknown"
                result["by_wallet_group"][group]["pnl"] += pnl
                result["by_wallet_group"][group]["fees"] += fee
                result["by_wallet_group"][group]["fills"] += 1
                if is_close:
                    result["by_wallet_group"][group]["closes"] += 1
                    if is_win:
                        result["by_wallet_group"][group]["wins"] += 1
            else:
                result["unattributed_pnl"] += pnl
                if is_close:
                    result["unattributed_closes"] += 1

        # Convert defaultdicts
        result["by_coin"] = dict(result["by_coin"])
        result["by_wallet_group"] = dict(result["by_wallet_group"])

        return result

    def get_open_positions(self) -> list:
        """Get current open positions from exchange (perps + builder coin DEXs).

        Retry on 429 / null response (HL rate limits during V11 heavy TWAP
        bursts produce null bodies that silently dropped main-dex positions
        from the report -- Alberto correction 2026-05-24 msg 6980).
        """
        import time as _time
        positions = []
        dex_status = []  # track per-dex success/failure for alerting
        for dex in ["", "xyz", "flx"]:
            payload = {"type": "clearinghouseState", "user": PARENT_ADDRESS}
            if dex:
                payload["dex"] = dex
            success = False
            for attempt in range(4):  # up to 4 retries with backoff
                try:
                    r = requests.post(f"{HL_API}/info", json=payload, timeout=10)
                    if r.status_code == 429:
                        wait = 2 ** attempt   # 1s, 2s, 4s, 8s
                        logger.warning(f"HL 429 dex={dex or 'main'}, retry {attempt+1}/4 in {wait}s")
                        _time.sleep(wait)
                        continue
                    if r.status_code != 200:
                        logger.warning(f"HL {r.status_code} dex={dex or 'main'}")
                        break
                    data = r.json()
                    if data is None:
                        wait = 2 ** attempt
                        logger.warning(f"HL null body dex={dex or 'main'}, retry {attempt+1}/4 in {wait}s")
                        _time.sleep(wait)
                        continue
                    for ap in data.get("assetPositions", []):
                        p = ap["position"]
                        if abs(float(p["szi"])) > 0:
                            positions.append({
                                "coin": p["coin"],
                                "size": float(p["szi"]),
                                "entry_px": float(p["entryPx"]),
                                "upnl": float(p["unrealizedPnl"]),
                                "notional": float(p["positionValue"]),
                                "margin_used": float(p.get("marginUsed", 0) or 0),
                            })
                    success = True
                    break
                except Exception as e:
                    logger.error(f"Failed to get positions (dex={dex or 'main'}, attempt {attempt+1}): {e}")
                    _time.sleep(2 ** attempt)
            dex_status.append((dex or "main", success))
        # If any dex failed all retries, raise so report includes a warning
        failed_dexes = [d for d, ok in dex_status if not ok]
        if failed_dexes:
            logger.error(f"POSITIONS REPORT INCOMPLETE: dex query failed for {failed_dexes} -- report will be missing these venues")
            # Attach flag to first position dict OR return wrapper -- for backwards compat, raise warning
            # Use a sentinel: append a stub with _incomplete flag the formatter can detect
            positions.append({
                "_incomplete": True,
                "failed_dexes": failed_dexes,
                "coin": "_INCOMPLETE_",
                "size": 0,
                "entry_px": 0,
                "upnl": 0,
                "notional": 0,
            })
        return positions

    def get_equity(self) -> float:
        """Get current USDC equity from exchange."""
        try:
            r = requests.post(f"{HL_API}/info", json={
                "type": "spotClearinghouseState", "user": PARENT_ADDRESS
            }, timeout=5)
            for bal in r.json().get("balances", []):
                if bal["coin"] == "USDC":
                    return float(bal["total"])
        except Exception:
            pass
        return 0.0

    def get_options_positions(self) -> dict:
        """Get Bybit options positions and compute spread PnL."""
        try:
            from pybit.unified_trading import HTTP
            session = HTTP(
                api_key=os.getenv("BYBIT_API_KEY", ""),
                api_secret=os.getenv("BYBIT_API_SECRET", ""),
            )
            bal = session.get_wallet_balance(accountType="UNIFIED")
            bybit_equity = float(bal["result"]["list"][0]["totalEquity"])

            spreads = []
            total_upnl = 0.0
            for coin in ["BTC", "ETH"]:
                pos_list = session.get_positions(category="option", baseCoin=coin)
                legs = []
                for p in pos_list["result"]["list"]:
                    if float(p.get("size", 0)) != 0:
                        upnl = float(p["unrealisedPnl"])
                        total_upnl += upnl
                        legs.append({
                            "symbol": p["symbol"],
                            "side": p["side"],
                            "size": p["size"],
                            "entry": float(p["avgPrice"]),
                            "mark": float(p["markPrice"]),
                            "upnl": upnl,
                        })
                if legs:
                    # Group into spread
                    credit = sum(l["entry"] * float(l["size"]) for l in legs if l["side"] == "Sell")
                    debit = sum(l["entry"] * float(l["size"]) for l in legs if l["side"] == "Buy")
                    net_credit = credit - debit
                    spread_upnl = sum(l["upnl"] for l in legs)
                    # Extract expiry from symbol (e.g. BTC-29MAY26-72000-P-USDT)
                    expiry = legs[0]["symbol"].split("-")[1] if legs else "?"
                    strikes = sorted(set(l["symbol"].split("-")[2] for l in legs))
                    spreads.append({
                        "coin": coin,
                        "expiry": expiry,
                        "strikes": "/".join(strikes),
                        "type": "put credit" if any("P" in l["symbol"] for l in legs) else "call credit",
                        "credit": net_credit,
                        "upnl": spread_upnl,
                        "legs": legs,
                    })

            return {"equity": bybit_equity, "spreads": spreads, "total_upnl": total_upnl}
        except Exception as e:
            logger.warning(f"Failed to get options: {e}")
            return {"equity": 0, "spreads": [], "total_upnl": 0}

    def format_daily_report(self, stats: dict) -> str:
        """Format PnL stats into a clean TG-ready text report."""
        equity = self.get_equity()
        positions_raw = self.get_open_positions()
        # Separate the _incomplete sentinel from real positions
        incomplete_info = [p for p in positions_raw if p.get("_incomplete")]
        positions = [p for p in positions_raw if not p.get("_incomplete")]
        total_upnl = sum(p["upnl"] for p in positions)
        total_notional = sum(abs(p["notional"]) for p in positions)

        wr = (stats["total_wins"] / stats["total_closes"] * 100) if stats["total_closes"] > 0 else 0
        net = stats["total_pnl"] - stats["total_fees"]

        lines = []
        lines.append(f"{STRATEGY_LABEL} PnL Report")
        lines.append("")

        # Strategy totals
        lines.append("STRATEGY (all fills, exchange truth):")
        lines.append(f"  Closes: {stats['total_closes']} | WR: {wr:.0f}% | Gross: ${stats['total_pnl']:.2f} | Fees: ${stats['total_fees']:.2f} | Net: ${net:.2f}")

        # Engine attribution
        if stats["engine_closes"] > 0:
            eng_wr = stats["engine_wins"] / stats["engine_closes"] * 100
            eng_net = stats["engine_pnl"] - stats["engine_fees"]
            lines.append("")
            lines.append(f"ENGINE AUTO-EXIT: {stats['engine_closes']} closes, ${eng_net:.2f} net")
            lines.append(f"UNATTRIBUTED: {stats['unattributed_closes']} closes, ${stats['unattributed_pnl']:.2f}")
            capture_pct = (stats["engine_closes"] / stats["total_closes"] * 100) if stats["total_closes"] > 0 else 0
            lines.append(f"CAPTURE RATE: {capture_pct:.0f}% of closes handled by engine")

        # Top/bottom coins
        if stats["by_coin"]:
            sorted_coins = sorted(stats["by_coin"].items(), key=lambda x: x[1]["pnl"], reverse=True)
            top = [(c, s) for c, s in sorted_coins if s["pnl"] > 0][:3]
            bottom = [(c, s) for c, s in sorted_coins if s["pnl"] < 0][-3:]

            if top:
                top_str = " | ".join(f"{c} +${s['pnl']:.2f}" for c, s in top)
                lines.append(f"\nBest: {top_str}")
            if bottom:
                bot_str = " | ".join(f"{c} ${s['pnl']:.2f}" for c, s in bottom)
                lines.append(f"Worst: {bot_str}")

        # Wallet groups (only if we have attribution data)
        if stats["by_wallet_group"]:
            lines.append("")
            lines.append("BY WALLET GROUP:")
            for group, gs in sorted(stats["by_wallet_group"].items(), key=lambda x: x[1]["pnl"], reverse=True):
                g_net = gs["pnl"] - gs["fees"]
                g_wr = (gs["wins"] / gs["closes"] * 100) if gs["closes"] > 0 else 0
                lines.append(f"  {group}: {gs['closes']} closes, ${g_net:.2f} net, {g_wr:.0f}% WR")

        # Open positions -- show ALL (was limited to top 5 which hid positions during many-open cycles)
        if positions:
            lines.append("")
            lines.append(f"OPEN: {len(positions)} pos, ${total_notional:.0f} notional, uPnL ${total_upnl:.2f}")
            for p in sorted(positions, key=lambda x: abs(x["notional"]), reverse=True):
                side = "LONG" if p["size"] > 0 else "SHORT"
                ep = p.get("entry_px", 0.0)
                eps = (f"{ep:.6g}" if ep else "?")          # avg entry / breakeven price
                lines.append(f"  {p['coin']} {side} ${abs(p['notional']):.0f} @ {eps} uPnL ${p['upnl']:.2f}")
        # Surface incomplete-report warning if any dex query failed all retries
        if incomplete_info:
            failed = incomplete_info[0].get("failed_dexes", [])
            lines.append("")
            lines.append(f"⚠ REPORT INCOMPLETE: dex query failed for {failed} (HL rate limit?). Re-run for full view.")

        # Equity (HL spot USDC only, rule 16). Report TRUE margin used (sum of per-position
        # HL marginUsed == marginSummary.totalMarginUsed), NOT notional/equity (that is gross
        # leverage, which was previously mislabeled "Margin"). Alberto correction 2026-06-02.
        if equity > 0:
            total_margin = sum(p.get("margin_used", 0) for p in positions)
            mu_pct = total_margin / equity * 100
            gross_x = total_notional / equity
            lines.append(f"\nEQUITY: ${equity:.2f} | Margin used: ${total_margin:.0f} ({mu_pct:.0f}%) | Gross: {gross_x:.1f}x")
            # Fold in the live copy-trading status (PnL%/peak/DD/stop/leaders) that the engine persists,
            # so this single PNG report carries everything the old (now-disabled) engine text report had.
            try:
                _repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                _sp = os.path.join(_repo, "app", "data", "v15", "v15_live_status.json")
                if os.path.exists(_sp):
                    with open(_sp) as _sf:
                        st = json.load(_sf)
                    if time.time() - st.get("ts", 0) <= 1200:  # fresh within a report cycle
                        dist = (st["total_value"] - st["stop_at"]) / st["total_value"] * 100 if st.get("total_value") else 0
                        lines.append(
                            f"Account ${st['total_value']:.2f} (spot+uPnL) | PnL {st['pnl_pct']:+.1f}% | "
                            f"peak ${st['peak']:.2f} | DD {st['dd_pct']:.1f}%")
                        lines.append(
                            f"Stop -{st['stop_pct']*100:.0f}% @ ${st['stop_at']:.2f} (dist {dist:.0f}%)"
                            + (" | HALTED" if st.get("halted") else "")
                            + f" | Leaders {st['n_active']}/{st['n_leaders']}")
                        # PER-LEADER breakdown (their live book + THEIR uPnL) -- Alberto 2026-06-02.
                        leaders = st.get("leaders") or []
                        if leaders:
                            lines.append("\nPER-LEADER — their book | contribution to OUR book:")
                            for L in leaders:
                                if not L.get("ok"):
                                    lines.append(f"  {L['addr']}: stale")
                                    continue
                                av = L["av"]
                                avs = f"${av/1000:.1f}k" if av >= 1000 else f"${av:.0f}"
                                coins = ",".join(L.get("coins") or []) or "flat"
                                ours = L.get("our_notional", 0)
                                ourup = L.get("our_upnl", 0.0)
                                lines.append(
                                    f"  {L['addr']}: {avs} their-uPnL ${L['upnl']:+.0f} ({L['upnl_pct']:+.0f}%) | "
                                    f"OURS ${ours:.0f} uPnL ${ourup:+.1f} | {coins}")
            except Exception as _e:
                logger.warning(f"status fold-in failed: {_e}")

        return "\n".join(lines)

    def format_options_report(self) -> str:
        """Separate report for Bybit options positions.

        Accounting (Alberto correction 2026-05-24 msg 6964):
          Credit was received upfront and is ALREADY in wallet equity.
          Bybit unrealisedPnl per leg = (entry - mark) * size for short,
          (mark - entry) * size for long. Sum across legs = P&L of the
          spread IF CLOSED NOW (already includes the credit collected).
          So uPnL is the answer to "what would I make/lose if I closed
          right now?" -- DO NOT add credit + uPnL (double count).

        Fields:
          Credit collected (cash, at entry) = sum of net premia received
          P&L if closed now                  = uPnL (Bybit unrealisedPnl)
          Max profit at expiry (if OTM)      = Credit collected (close cost goes to 0)
          Remaining decay to capture         = Credit - uPnL
        """
        opts = self.get_options_positions()
        if not opts["spreads"]:
            return ""

        lines = []
        lines.append("Options Report (Bybit)")
        lines.append("")

        total_credit = 0.0
        total_upnl = 0.0
        for sp in opts["spreads"]:
            total_credit += sp["credit"]
            total_upnl += sp["upnl"]
            remaining = sp["credit"] - sp["upnl"]
            lines.append(f"{sp['coin']} {sp['type']} {sp['strikes']} exp {sp['expiry']}:")
            lines.append(
                f"  Credit ${sp['credit']:.2f} (received at entry) | "
                f"Close-now P&L ${sp['upnl']:+.2f} | "
                f"Hold-to-expiry max ${sp['credit']:.2f} | "
                f"Remaining decay ${remaining:+.2f}"
            )
            for leg in sp["legs"]:
                side_label = "SOLD" if leg["side"] == "Sell" else "BOUGHT"
                strike = leg["symbol"].split("-")[2]
                lines.append(f"  {side_label} {strike}P x{leg['size']}: entry ${leg['entry']:.2f}, mark ${leg['mark']:.2f}, leg P&L ${leg['upnl']:+.2f}")

        remaining_total = total_credit - total_upnl
        progress = (total_upnl / total_credit * 100) if total_credit > 0 else 0
        lines.append("")
        lines.append(
            f"TOTAL: credit ${total_credit:.2f} | close-now P&L ${total_upnl:+.2f} | "
            f"max-at-expiry ${total_credit:.2f} | remaining decay ${remaining_total:+.2f} "
            f"({progress:.0f}% of max captured)"
        )
        lines.append(f"Bybit equity: ${opts['equity']:.2f}")
        # State annotation
        if total_upnl > 0:
            lines.append(f"State: profitable if closed now (+${total_upnl:.2f}); +${remaining_total:.2f} more upside if held to expiry OTM")
        elif total_upnl < 0:
            lines.append(f"State: closing now would book -${abs(total_upnl):.2f} loss; hold thesis intact -> wait for theta/recovery; cut only if max-loss risk approaching")
        else:
            lines.append("State: at break-even")

        return "\n".join(lines)

    def generate_equity_curve(self, since_ms: int, output_path: str = "/tmp/equity_curve.png") -> str:
        """Generate equity curve chart with wins/losses dots and unrealized PnL dashed line."""
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        fills = list(self.db[FILLS_COLLECTION].find(
            {"time": {"$gte": since_ms}},
            {"time": 1, "closedPnl": 1, "fee": 1}
        ).sort("time", 1))

        if not fills:
            logger.warning("No fills for equity curve")
            return ""

        # Build cumulative PnL curve with win/loss markers
        times = []
        cum_pnl = []
        win_times, win_pnl = [], []
        loss_times, loss_pnl = [], []
        running = 0.0
        closed_count = 0
        for f in fills:
            pnl = float(f.get("closedPnl", 0))
            fee = float(f.get("fee", 0))
            net = pnl - fee
            running += net
            dt = datetime.fromtimestamp(f["time"] / 1000, tz=timezone.utc)
            times.append(dt)
            cum_pnl.append(running)
            if pnl != 0:
                closed_count += 1
                if net >= 0:
                    win_times.append(dt)
                    win_pnl.append(running)
                else:
                    loss_times.append(dt)
                    loss_pnl.append(running)

        # Get current unrealized PnL for dashed extension
        open_positions = self.get_open_positions()
        total_upnl = sum(p["upnl"] for p in open_positions)
        open_count = len(open_positions)

        fig, ax = plt.subplots(figsize=(10, 4))

        # Realized PnL line
        ax.plot(times, cum_pnl, linewidth=1.5, color="#1565C0", label="Realized", zorder=2)
        ax.fill_between(times, cum_pnl, alpha=0.1, color="#2196F3")
        ax.axhline(y=0, color="#888", linewidth=0.5, linestyle="--")

        # Win/loss dots
        if win_times:
            ax.scatter(win_times, win_pnl, color="#4CAF50", s=20, zorder=3, label=None)
        if loss_times:
            ax.scatter(loss_times, loss_pnl, color="#F44336", s=20, zorder=3, label=None)

        # Unrealized PnL dashed extension
        if times and total_upnl != 0:
            unrealized_total = running + total_upnl
            ax.plot(
                [times[-1], times[-1]],
                [running, unrealized_total],
                linewidth=1.5, color="#7B1FA2", linestyle="--", label="+ Unrealized", zorder=2,
            )
            ax.scatter([times[-1]], [unrealized_total], color="#7B1FA2", s=40, marker="s", zorder=4)
            ax.annotate(
                f"${unrealized_total:.3f}\n{open_count} open",
                xy=(times[-1], unrealized_total),
                fontsize=9, fontweight="bold",
                color="#7B1FA2",
                xytext=(10, 0), textcoords="offset points",
            )

        # Realized value annotation
        if cum_pnl:
            ax.annotate(
                f"${cum_pnl[-1]:.2f}",
                xy=(times[-1], cum_pnl[-1]),
                fontsize=11, fontweight="bold",
                color="#1565C0" if cum_pnl[-1] >= 0 else "#F44336",
                xytext=(-50, 10), textcoords="offset points",
            )

        ax.set_title(
            f"Copy Trader {STRATEGY_LABEL} -- {closed_count} closed, {open_count} open",
            fontsize=13, fontweight="bold",
        )
        ax.set_ylabel("Cumulative PnL ($)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
        ax.tick_params(axis="x", rotation=30)
        ax.legend(loc="upper left", fontsize=9)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)
        logger.info(f"Equity curve saved to {output_path}")
        return output_path

    def send_tg(self, text: str, image_path: str = None):
        """Send message to Telegram, optionally with an image."""
        if not TG_BOT_TOKEN:
            logger.warning("No TG_BOT_TOKEN set, skipping TG send")
            print(text)
            return

        try:
            if image_path and os.path.exists(image_path):
                # Send photo with caption (TG caption limit: 1024 chars)
                # If text is too long, send photo first then text
                with open(image_path, "rb") as photo:
                    if len(text) <= 1024:
                        requests.post(
                            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto",
                            data={"chat_id": TG_CHAT_ID, "caption": text},
                            files={"photo": photo},
                            timeout=15,
                        )
                    else:
                        requests.post(
                            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto",
                            data={"chat_id": TG_CHAT_ID, "caption": f"{STRATEGY_LABEL} Equity Curve"},
                            files={"photo": photo},
                            timeout=15,
                        )
                        requests.post(
                            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                            json={"chat_id": TG_CHAT_ID, "text": text},
                            timeout=10,
                        )
            else:
                requests.post(
                    f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
                    json={"chat_id": TG_CHAT_ID, "text": text},
                    timeout=10,
                )
            logger.info("TG report sent")
        except Exception as e:
            logger.error(f"Failed to send TG: {e}")
            print(text)


def main():
    parser = argparse.ArgumentParser(description="Exchange-truth PnL tracker")
    parser.add_argument("--daily", action="store_true", help="Today's PnL summary")
    parser.add_argument("--since", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--sync", action="store_true", help="Sync fills only")
    parser.add_argument("--tg", action="store_true", help="Send report to TG with equity curve")
    parser.add_argument("--epoch", action="store_true", help="Since V12 epoch (May 17 11:42 UTC)")
    parser.add_argument("--loop", type=int, metavar="MINUTES", help="Run every N minutes (e.g. --loop 15)")
    parser.add_argument("--chart", type=str, metavar="PATH", help="Save equity curve to path")
    args = parser.parse_args()

    tracker = PnLTracker()

    def run_once():
        if args.sync:
            tracker.sync_fills()
            return

        # Determine time range
        if args.since:
            dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            since_ms = int(dt.timestamp() * 1000)
        elif args.epoch:
            # V16 epoch: persisted by hl_copy_trader_v16.py at FIRST LIVE START (mongo v16_meta).
            # Before launch (doc absent) fall back to 'now' so pre-launch reports show zero, never
            # legacy history (Alberto msg 9222: fresh epoch for V16).
            _epoch_doc = tracker.db.v16_meta.find_one({"_id": "epoch"})
            since_ms = int(_epoch_doc["epoch_ms"]) if _epoch_doc else int(
                datetime.now(timezone.utc).timestamp() * 1000)
        elif args.daily:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            since_ms = int(today.timestamp() * 1000)
        else:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            since_ms = int(today.timestamp() * 1000)

        # Sync first
        tracker.sync_fills(since_ms)

        # Compute
        stats = tracker.compute_pnl(since_ms)
        report = tracker.format_daily_report(stats)

        # Generate equity curve. CRITICAL: use the RETURN value -- when there are no fills yet the
        # generator returns "" and writes nothing; attaching the preset path would send a STALE chart
        # from a previous strategy (2026-06-11 launch-day incident: months-old V15 chart went to TG).
        chart_path = tracker.generate_equity_curve(since_ms, args.chart or "/tmp/v16_equity_curve.png")

        # Options report (separate)
        opts_report = tracker.format_options_report()

        if args.tg:
            tracker.send_tg(report, image_path=chart_path)
            if opts_report:
                tracker.send_tg(opts_report)
                logger.info("Options report sent")
        else:
            print(report)
            if os.path.exists(chart_path):
                print(f"\nEquity curve: {chart_path}")
            if opts_report:
                print(f"\n{'='*40}\n")
                print(opts_report)

    if args.loop:
        import time as _time
        logger.info(f"Starting PnL tracker loop every {args.loop} minutes")
        while True:
            try:
                run_once()
            except Exception as e:
                logger.error(f"Loop iteration failed: {e}")
            _time.sleep(args.loop * 60)
    else:
        run_once()


if __name__ == "__main__":
    main()
