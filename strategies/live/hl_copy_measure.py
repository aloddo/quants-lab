#!/usr/bin/env python3
"""hl_copy_measure.py -- LIVE MEASUREMENT bot for HL copy edges (v3, exchange-truth).

Measures whether copying selected wallets' large crossed opens is net-positive on REAL fills. NOT a PnL bot.
Spec: brain projects/quant/copy/2026-06-07-live-measurement-config-codex.
Fixes: .../2026-06-07-executor-codex-review-fix-first + .../2026-06-07-executor-v2-review-fixes.

DESIGN: EXCHANGE TRUTH is the source for positions/fills/flatten/kill. Confirm every fill; flatten reads
parent.assetPositions and only clears local state when CONFIRMED flat; HL-safe tick rounding; correct
crossing book side; caps vs worst-case fill; recheck halt/latency immediately before submit; fail-closed on
missing book/vol/equity; refuses to start unless parent flat; per-wallet bag-guard seeded before subscribe.

SAFETY: dry-run by DEFAULT. /tmp/v12_pause halts+flattens. Run with the quants-lab conda python.
  python strategies/live/hl_copy_measure.py            # dry-run (no orders)
  python strategies/live/hl_copy_measure.py --live      # real (tiny) orders
"""
from __future__ import annotations
import argparse, json, math, os, threading, time
from collections import deque
from pathlib import Path

import eth_account
import requests
import websocket
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

HL_API = "https://api.hyperliquid.xyz"
WS_URL = "wss://api.hyperliquid.xyz/ws"
LOG_PATH = Path("research/v15/out/live/copy_measure_log.jsonl")
PAUSE_FILE = "/tmp/v12_pause"  # the v12-copy-trader launchd daemon (KeepAlive) now runs THIS bot via
# v12_launcher.sh, gated on this file. Kill: touch /tmp/v12_pause (launcher won't relaunch + running bot
# flatten+exits). Terminal kill-switches also touch it so KeepAlive can't restart past a kill.
DIR_SIGN = {"Open Long": 1, "Open Short": -1, "Short > Long": 1, "Long > Short": -1}
BUILDER_DEXES = ["xyz", "flx"]   # HL builder/HIP-3 perp dexes -- where these wallets actually trade


def _coin_dex(coin):
    return coin.split(":", 1)[0] if ":" in coin else ""

CFG = dict(
    min_leader_notional=1000.0, size_cap_usd=150.0, size_vol_frac=0.005, size_floor_usd=75.0,
    max_concurrent=4, gross_cap_usd=600.0, per_coin_cap_usd=250.0, hold_s=3600, disaster_pct=0.08,
    daily_max_loss=-25.0, experiment_max_loss=-60.0, max_entries_per_hour=30,
    # 2026-06-08 (Alberto): loosened the THROTTLES (entries/hr 8->30, cooldowns 15m/60m -> 60s) to capture
    # more of the signal for the MEASUREMENT. Hard RISK caps unchanged (4 concurrent, $600 gross, $250/coin,
    # one-position-per-coin, daily -$25 / exp -$60 / -8% kills) -- those bound risk; the cooldowns only
    # bounded sample size and are redundant with per-coin cap + one-per-coin. 60s avoids literal dup-fills.
    coin_cooldown_s=60, wallet_coin_cooldown_s=60, max_spread_bps=8.0,
    max_fill_age_ms=5000, bag_worst_pos_pct=-0.06, bag_open_loss_frac=-0.03, bag_max_stale_s=240,
    fee_rt=0.001064, equity_fail_halt_s=60, mark_max_age_s=30,
    # liquidity floor (builder perps vary wildly): require take-side book depth within 25bps >= depth_mult x
    # intended notional AND last-closed-minute volume >= min_vol60s_usd. Skips thin perps where a copy eats slippage.
    depth_within_bps=25.0, depth_mult=3.0, min_vol60s_usd=50000.0,
    twap_window_s=600,   # rolling window to aggregate a leader's sliced/TWAP entry (same coin+side) into one signal
)


def _tg(msg):
    try:
        tok = os.environ.get("TELEGRAM_BOT_TOKEN")
        if tok:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": "-1003576397888", "text": msg}, timeout=8)
    except Exception:
        pass


def _post_info(payload, tries=3):
    for i in range(tries):
        try:
            r = requests.post(HL_API + "/info", json=payload, timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception:
            time.sleep(0.4 * (i + 1))
    return None


def hl_round_px(px, szd, is_buy):
    """HL perp px: <=5 sig figs AND <=(6-szDecimals) decimals. Round to the tick quantum in the crossing
    direction. Handles px>=1e5 (quantum>1, e.g. round to tens for BTC)."""
    if px <= 0:
        return px
    exp = math.floor(math.log10(abs(px)))
    sig_q = 10.0 ** (exp - 4)                 # 5 significant figures
    dec_q = 10.0 ** (-(max(0, 6 - szd)))      # decimal-place cap
    q = max(sig_q, dec_q)
    return (math.ceil(px / q) * q) if is_buy else (math.floor(px / q) * q)


def hl_floor_sz(sz, szd):
    f = 10 ** szd
    return math.floor(sz * f) / f


class CopyMeasure:
    def __init__(self, wallets_files, live):
        self.live = live
        self.arm = {}
        for wf in wallets_files:
            arm = Path(wf).stem
            for w in json.load(open(wf)).get("wallets", []):
                self.arm.setdefault(w.lower(), arm)
        self.wset = set(self.arm)
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

        self.private_key = os.environ["HL_PRIVATE_KEY"]
        self.agent_address = os.environ["HL_ADDRESS"]
        self.parent = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
        self.account = eth_account.Account.from_key(self.private_key)
        if self.account.address.lower() != self.agent_address.lower():
            raise RuntimeError(f"signer {self.account.address} != HL_ADDRESS {self.agent_address}")
        all_dexes = [""] + BUILDER_DEXES
        self.info = Info(HL_API, skip_ws=True, perp_dexs=all_dexes); self.info.timeout = 10
        self.exchange = Exchange(self.account, HL_API, account_address=self.parent, perp_dexs=all_dexes)
        self.exchange.timeout = 10
        self.sz_decimals = {}   # full coin name ("BTC", "xyz:CL") -> szDecimals, across main + builder dexes
        for dex in all_dexes:
            try:
                m = self.info.meta(dex=dex) if dex else self.info.meta()
                for a in m["universe"]:
                    self.sz_decimals[a["name"]] = a["szDecimals"]
            except Exception as e:
                raise RuntimeError(f"meta dex={dex or 'main'} failed at startup: {e}")
        self.mids = {}
        self._refresh_mids()
        self.start_equity = self._equity()
        if self.start_equity is None:
            raise RuntimeError("cannot read parent equity at startup")
        if live and self.start_equity < 10.0:
            raise RuntimeError(f"live start_equity ${self.start_equity:.2f} below $10 floor -- perp account "
                               f"unfunded (USDC likely in spot)? refusing to go live with disabled kill switches")
        # FAIL CLOSED: refuse to start unless parent is CONFIRMED flat
        pp = self._parent_positions()
        if pp is None:
            raise RuntimeError("cannot verify parent flat (user_state failed); refusing to start")
        if pp:
            raise RuntimeError(f"parent NOT FLAT at startup: {pp}; flatten or use a dedicated subaccount first")

        self.positions = {}      # coin -> dict(dir, entry_ts, entry_px, leader_px, size, wallet, arm) | {"reserving":True}
        self.seen = set()
        self.entry_times = deque()
        self.last_coin = {}
        self.flow = {}   # (wallet,coin,side) -> deque[(ts_ms, notional)] rolling window for TWAP/slice aggregation
        self.last_wallet_coin = {}
        self.bag = {}            # wallet -> (skip_bool, ts)
        self.realized = 0.0
        self.halted = False
        self.equity_fail_since = None
        self.lock = threading.Lock()
        self._seed_bags()        # seed BEFORE subscribing (fail-closed: unknown -> skip)
        self._log({"ev": "start", "live": live, "wallets": len(self.wset), "arms": sorted(set(self.arm.values())),
                   "start_equity": self.start_equity, "cfg": CFG})
        _tg(f"[copy-measure v3] START {'LIVE' if live else 'DRY'} | {len(self.wset)} wallets | eq ${self.start_equity:.2f} | parent FLAT ok")

    # ---------- truth helpers ----------
    def _log(self, d):
        d["t"] = int(time.time() * 1000)
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(d) + "\n")

    def _refresh_mids(self):
        now = time.time()
        for dex in [""] + BUILDER_DEXES:   # main + builder dex mids, merged by full coin name, timestamped
            try:
                mids = self.info.all_mids(dex=dex) if dex else self.info.all_mids()
                for k, v in (mids or {}).items():
                    try:
                        self.mids[k] = (float(v), now)
                    except (TypeError, ValueError):
                        pass
            except Exception:
                continue

    def _mark(self, coin):
        """Mark from cached mids; None if missing or STALE (> mark_max_age_s) -> callers fail safe (no bad px)."""
        v = self.mids.get(coin)
        if not v or (time.time() - v[1]) > CFG["mark_max_age_s"]:
            return None
        return v[0]

    def _equity(self):
        """UNIFIED-ACCOUNT equity = SPOT USDC (rule 16; matches the proven V11 bot). HL is unified: spot USDC
        IS the perp collateral/margin -- there is NO transfer. The perp marginSummary.accountValue is the
        risk metric (margin used + uPnL), NOT the deployable equity. Spot USDC is the equity for sizing + kill."""
        try:
            d = _post_info({"type": "spotClearinghouseState", "user": self.parent})
            if not isinstance(d, dict) or not isinstance(d.get("balances"), list):
                return None
            for b in d["balances"]:
                if b.get("coin") == "USDC":
                    return float(b.get("total", 0) or 0)
            return 0.0
        except Exception:
            return None

    def _perp_upnl(self):
        """Sum unrealized PnL across MAIN + BUILDER dex positions (mark-to-market for the kill switch).
        Returns None if ANY dex read is missing/malformed -> caller fails closed (never undercount builder loss)."""
        tot = 0.0
        for dex in [""] + BUILDER_DEXES:
            payload = {"type": "clearinghouseState", "user": self.parent}
            if dex:
                payload["dex"] = dex
            d = _post_info(payload)
            if not isinstance(d, dict) or not isinstance(d.get("assetPositions"), list):
                return None
            for ap in d["assetPositions"]:
                try:
                    tot += float(ap["position"].get("unrealizedPnl", 0) or 0)
                except (KeyError, TypeError, ValueError):
                    return None
        return tot

    def _parent_positions(self):
        """EXCHANGE TRUTH across MAIN + BUILDER dexes: {full_coin: szi} nonzero. None if ANY dex query fails
        (caller fails closed -- never assume flat on a failed read)."""
        out = {}
        for dex in [""] + BUILDER_DEXES:
            payload = {"type": "clearinghouseState", "user": self.parent}
            if dex:
                payload["dex"] = dex
            d = _post_info(payload)
            if not isinstance(d, dict) or not isinstance(d.get("assetPositions"), list):
                return None
            for ap in d["assetPositions"]:
                try:
                    p = ap["position"]; coin = p["coin"]; szi = float(p["szi"])
                    pv = float(p.get("positionValue", 0) or 0)
                except (KeyError, TypeError, ValueError):
                    return None
                if _coin_dex(coin) not in ([""] + BUILDER_DEXES):
                    return None  # unsupported dex -> fail closed
                if abs(szi) > 0:
                    # px = truth price from clearinghouse (positionValue/|szi|), used as flatten fallback
                    out[coin] = {"szi": szi, "px": (pv / abs(szi)) if abs(szi) > 0 and pv > 0 else None}
        return out

    def _book(self, coin):
        """Returns (bid, ask, spread_bps, depth_bid_usd, depth_ask_usd) where depth_* = notional resting
        within depth_within_bps of the touch on that side. (None,...) on failure -> caller fails closed."""
        b = _post_info({"type": "l2Book", "coin": coin})
        try:
            lv = b["levels"]; bid = float(lv[0][0]["px"]); ask = float(lv[1][0]["px"])
            mid = (bid + ask) / 2; band = CFG["depth_within_bps"] / 1e4
            dbid = sum(float(x["px"]) * float(x["sz"]) for x in lv[0] if float(x["px"]) >= bid * (1 - band))
            dask = sum(float(x["px"]) * float(x["sz"]) for x in lv[1] if float(x["px"]) <= ask * (1 + band))
            return bid, ask, 1e4 * (ask - bid) / mid, dbid, dask
        except Exception:
            return None, None, None, None, None

    def _vol60s(self, coin):
        now = int(time.time() * 1000)
        c = _post_info({"type": "candleSnapshot", "req": {"coin": coin, "interval": "1m",
                                                          "startTime": now - 180000, "endTime": now}})
        if not isinstance(c, list) or len(c) < 2:
            return None
        try:
            return float(c[-2]["v"]) * float(c[-2]["c"])
        except Exception:
            return None

    def _order_fill(self, r):
        try:
            st = r["response"]["data"]["statuses"][0]
            if "filled" in st:
                return float(st["filled"]["totalSz"]), float(st["filled"]["avgPx"])
        except Exception:
            pass
        return 0.0, None

    def _cross_px(self, coin, is_buy, ref_bid, ref_ask, mark, slip=0.003):
        """Crossing px for a marketable IOC: BUY lifts the ASK, SELL hits the BID. None if no reference px."""
        ref = (ref_ask if is_buy else ref_bid) or mark
        if not ref:
            return None
        return hl_round_px(ref * (1 + slip if is_buy else 1 - slip), self.sz_decimals.get(coin, 2), is_buy)

    # ---------- bag-guard ----------
    def _refresh_bag(self, w):
        """Bag guard across MAIN + BUILDER dexes (the wallets' builder book can be impaired too). Fail closed:
        if ANY dex read fails, leave the entry stale -> _bagged() returns True (skip the wallet)."""
        worst = 0.0; tot = 0.0; eqsum = 0.0
        for dex in [""] + BUILDER_DEXES:
            payload = {"type": "clearinghouseState", "user": w}
            if dex:
                payload["dex"] = dex
            d = _post_info(payload)
            if not isinstance(d, dict) or not isinstance(d.get("assetPositions"), list):
                self.bag[w] = (True, time.time()); return  # fail closed IMMEDIATELY: skip this wallet
            try:
                eqsum += float(d.get("marginSummary", {}).get("accountValue", 0) or 0)
            except (TypeError, ValueError):
                self.bag[w] = (True, time.time()); return
            for ap in d["assetPositions"]:
                try:
                    p = ap["position"]; up = float(p.get("unrealizedPnl", 0) or 0)
                    pv = float(p.get("positionValue", 0) or 0)
                except (KeyError, TypeError, ValueError):
                    self.bag[w] = (True, time.time()); return
                tot += up
                if pv > 0:
                    worst = min(worst, up / pv)
        eq = eqsum or 1.0
        skip = (worst <= CFG["bag_worst_pos_pct"]) or (tot / eq <= CFG["bag_open_loss_frac"])
        self.bag[w] = (skip, time.time())

    def _seed_bags(self):
        for w in list(self.wset):
            self._refresh_bag(w); time.sleep(0.15)

    def bag_loop(self):
        while True:
            for w in list(self.wset):
                self._refresh_bag(w); time.sleep(0.3)
            time.sleep(60)

    def _bagged(self, w):
        v = self.bag.get(w)
        if v is None or time.time() - v[1] > CFG["bag_max_stale_s"]:
            return True  # fail closed: unknown/stale -> skip
        return v[0]

    # ---------- signal ----------
    def on_fill(self, wallet, f):
        if self.halted:
            return
        key = (wallet, f.get("oid"), f.get("tid"), f.get("time"))
        if key in self.seen:
            return
        self.seen.add(key)
        coin = f.get("coin", "")
        # builder/HIP-3 perps ("xyz:CL") are now in sz_decimals and copyable; reject only spot ("#"/"@")
        # and anything not in the perp universe. Liquidity is gated later (spread + vol-based sizing).
        if coin not in self.sz_decimals or coin.startswith("#") or coin.startswith("@"):
            return
        if not bool(f.get("crossed", False)):
            return self._skip(wallet, coin, "not_taker")
        d = str(f.get("dir", ""))
        if d not in DIR_SIGN:
            return
        try:
            notional = float(f.get("sz", 0)) * float(f.get("px", 0))
        except Exception:
            return
        side = DIR_SIGN[d]
        now_ms = int(time.time() * 1000)
        fill_ms = int(f.get("time", 0))
        # reject STALE before touching the aggregate (codex: a delayed child must not pollute flow)
        if now_ms - fill_ms > CFG["max_fill_age_ms"]:
            return self._skip(wallet, coin, "stale_fill")
        # AGGREGATE sliced/TWAP entries: leaders build a big position via many small same-coin+side opens
        # (twapId is null in the live feed, so key on wallet+coin+side). Sum notional over a rolling window
        # keyed by FILL time; copy ONCE when the AGGREGATE crosses the threshold (per-child is often < $1k).
        # The one-position-per-coin rule then dedups the remaining slices into the single live position.
        key = (wallet, coin, side)
        dq = self.flow.setdefault(key, deque())
        dq.append((fill_ms, notional))
        cutoff = fill_ms - CFG["twap_window_s"] * 1000
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        agg = sum(n for _, n in dq)
        if agg < CFG["min_leader_notional"]:
            return self._skip(wallet, coin, f"agg_below_{agg:.0f}")
        if self._bagged(wallet):
            return self._skip(wallet, coin, "wallet_bag")
        self._try_enter(wallet, coin, side, float(f.get("px", 0)), f)

    def _skip(self, wallet, coin, reason):
        self._log({"ev": "skip", "wallet": wallet, "coin": coin, "reason": reason})

    def _try_enter(self, wallet, coin, sign, leader_px, f):
        now = time.time()
        with self.lock:
            if self.halted:
                return
            if coin in self.positions:
                return self._skip(wallet, coin, "coin_open")
            live_pos = [p for p in self.positions.values() if isinstance(p, dict) and not p.get("reserving")]
            if len(self.positions) >= CFG["max_concurrent"]:
                return self._skip(wallet, coin, "max_concurrent")
            while self.entry_times and now - self.entry_times[0] > 3600:
                self.entry_times.popleft()
            if len(self.entry_times) >= CFG["max_entries_per_hour"]:
                return self._skip(wallet, coin, "entries_per_hour")
            if now - self.last_coin.get(coin, 0) < CFG["coin_cooldown_s"]:
                return self._skip(wallet, coin, "coin_cooldown")
            if now - self.last_wallet_coin.get((wallet, coin), 0) < CFG["wallet_coin_cooldown_s"]:
                return self._skip(wallet, coin, "wallet_coin_cooldown")
            gross = sum(p["size"] * (self._mark(c) or p["entry_px"]) for c, p in self.positions.items()
                        if isinstance(p, dict) and not p.get("reserving"))
            gross_room = CFG["gross_cap_usd"] - gross
            if gross_room < CFG["size_floor_usd"]:
                return self._skip(wallet, coin, "gross_cap")
            self.positions[coin] = {"reserving": True}
        try:
            mark = self._mark(coin) or (self._refresh_mids() or self._mark(coin))
            if not mark:
                return self._abort_reserve(coin, wallet, "no_mark")
            bid, ask, sp, dbid, dask = self._book(coin)
            if sp is None:
                return self._abort_reserve(coin, wallet, "no_book_failclosed")
            if sp > CFG["max_spread_bps"]:
                return self._abort_reserve(coin, wallet, f"spread_{sp:.1f}")
            vol = self._vol60s(coin)
            if vol is None:
                return self._abort_reserve(coin, wallet, "vol_stale_failclosed")
            if vol < CFG["min_vol60s_usd"]:
                return self._abort_reserve(coin, wallet, f"thin_vol_{vol:.0f}")
            is_buy = sign > 0
            # LIQUIDITY FLOOR: the take-side (ask for buy, bid for sell) must hold depth_mult x our notional
            # within depth_within_bps, else a $150 copy eats slippage (esp. thin builder perps).
            take_depth = dask if is_buy else dbid
            if take_depth is None or take_depth < CFG["depth_mult"] * CFG["size_cap_usd"]:
                return self._abort_reserve(coin, wallet, f"thin_depth_{(take_depth or 0):.0f}")
            cross = self._cross_px(coin, is_buy, bid, ask, mark)
            # WORST-CASE fill price for NOTIONAL: a buy IOC fills <= our limit (cross=ask*1.003); a sell IOC
            # fills >= our limit but <= best bid, so the max notional uses the top BID. Sizing against the
            # worst-case price bounds notional <= target on BOTH sides (codex r9).
            size_px = cross if is_buy else (bid or mark)
            target = min(CFG["size_cap_usd"], CFG["size_vol_frac"] * vol, CFG["per_coin_cap_usd"], gross_room)
            szd = self.sz_decimals[coin]
            sz = hl_floor_sz(target / size_px, szd)
            if sz <= 0 or sz * size_px < CFG["size_floor_usd"]:
                return self._abort_reserve(coin, wallet, f"size<floor(vol{vol:.0f})")
            lat = now - int(f.get("time", now * 1000)) / 1000.0
            entry_px, filled = mark, sz
            if self.live:
                if int(time.time() * 1000) - int(f.get("time", 0)) > CFG["max_fill_age_ms"]:
                    return self._abort_reserve(coin, wallet, "stale_presubmit")
                aborted = False; capped = False; need_verify = None
                self._refresh_mids()  # fresh marks for the final gross recompute
                # SERIALIZE check + the only non-reduce order + record under ONE lock, so halt/flatten cannot
                # interleave with a live entry submit (codex r4). IOC w/ exchange.timeout=10 bounds the hold.
                with self.lock:
                    if self.halted or not (isinstance(self.positions.get(coin), dict) and self.positions[coin].get("reserving")):
                        aborted = True
                    elif (sum(p["size"] * (self._mark(c) or p["entry_px"]) for c, p in self.positions.items()
                              if isinstance(p, dict) and not p.get("reserving")) + sz * size_px) > CFG["gross_cap_usd"]:
                        capped = True   # marked exposure moved over cap during the book/vol calls
                    else:
                        try:
                            r = self.exchange.order(coin, is_buy, sz, cross, {"limit": {"tif": "Ioc"}}, reduce_only=False)
                            fz, avg = self._order_fill(r)
                            if fz > 0:
                                entry_px, filled = avg, fz
                                self.positions[coin] = {"dir": sign, "entry_ts": time.time(), "entry_px": avg,
                                                        "leader_px": leader_px, "size": fz, "wallet": wallet,
                                                        "arm": self.arm.get(wallet, "?")}
                                self.entry_times.append(now); self.last_coin[coin] = now
                                self.last_wallet_coin[(wallet, coin)] = now
                            else:
                                need_verify = f"not_filled:{str(r)[:100]}"
                        except Exception as e:
                            need_verify = f"order_exc:{str(e)[:80]}"
                if aborted:
                    return self._abort_reserve(coin, wallet, "halt_or_slot_lost_presubmit")
                if capped:
                    return self._abort_reserve(coin, wallet, "gross_cap_presubmit")
                if need_verify:
                    return self._verify_or_abort(coin, wallet, sign, need_verify)
            else:
                with self.lock:
                    self.positions[coin] = {"dir": sign, "entry_ts": time.time(), "entry_px": entry_px,
                                            "leader_px": leader_px, "size": filled, "wallet": wallet,
                                            "arm": self.arm.get(wallet, "?")}
                    self.entry_times.append(now); self.last_coin[coin] = now
                    self.last_wallet_coin[(wallet, coin)] = now
            self._log({"ev": "enter", "live": self.live, "wallet": wallet, "arm": self.arm.get(wallet),
                       "coin": coin, "dir": sign, "leader_px": leader_px, "entry_px": entry_px, "size": filled,
                       "notional": filled * entry_px, "latency_s": round(lat, 2), "spread_bps": round(sp, 2),
                       "slip_vs_leader_bps": round(1e4 * sign * (entry_px - leader_px) / leader_px, 2) if leader_px else None})
            if self.live:
                self._enforce_caps_postfill(coin)  # sell fills aren't pre-bounded; trim any cap overshoot
        except Exception as e:
            self._abort_reserve(coin, wallet, f"enter_exc:{str(e)[:80]}")

    def _enforce_caps_postfill(self, coin):
        """After a fill, if actual notional overshoots the per-trade or gross cap (sell IOC fill price is not
        bounded pre-trade), reduce-only TRIM the excess. Reduce-only -> can only shrink, never adds risk."""
        with self.lock:
            p = self.positions.get(coin)
            if not isinstance(p, dict) or p.get("reserving"):
                return
            notional = p["size"] * p["entry_px"]
            gross = sum(q["size"] * (self._mark(c) or q["entry_px"]) for c, q in self.positions.items()
                        if isinstance(q, dict) and not q.get("reserving"))
            over = max(notional - CFG["size_cap_usd"], notional - CFG["per_coin_cap_usd"],
                       gross - CFG["gross_cap_usd"], 0.0)
            dirn = p["dir"]; epx = p["entry_px"]
        if over <= 0:
            return
        szd = self.sz_decimals.get(coin, 2)
        lot_notional = epx * (10 ** -szd)
        if over < lot_notional:
            # untrimmable: smaller than one lot. Inherent to discrete lot sizes; economically immaterial
            # (sub-lot, here sub-dollar). Bounded by gross cap + daily kill. Accept + log, do not churn.
            self._log({"ev": "cap_sub_lot_residual", "coin": coin, "over_usd": round(over, 4)})
            return
        trim_sz = hl_floor_sz(over / epx, szd)
        is_buy = dirn < 0   # closing side
        bid, ask, _, _, _ = self._book(coin)
        cross = self._cross_px(coin, is_buy, bid, ask, self._mark(coin), slip=0.006)
        if trim_sz <= 0 or not cross:
            self._log({"ev": "cap_trim_no_ref_HALT", "coin": coin, "over_usd": round(over, 2)})
            with self.lock:
                self.halted = True
            self._flatten_truth("cap_enforce_no_ref"); return
        try:
            r = self.exchange.order(coin, is_buy, trim_sz, cross, {"limit": {"tif": "Ioc"}}, reduce_only=True)
            tz, _ = self._order_fill(r)
            with self.lock:
                if coin in self.positions and isinstance(self.positions[coin], dict) and not self.positions[coin].get("reserving"):
                    self.positions[coin]["size"] = max(0.0, self.positions[coin]["size"] - (tz or 0.0))
            self._log({"ev": "cap_trim", "coin": coin, "over_usd": round(over, 2), "trim_sz": trim_sz, "filled": tz})
            if (tz or 0.0) < trim_sz - 1e-9:   # ANY lot-level shortfall -> escalate (only float-eps tolerated)
                self._log({"ev": "cap_trim_short_HALT", "coin": coin, "trim_sz": trim_sz, "filled": tz})
                with self.lock:
                    self.halted = True
                self._flatten_truth("cap_enforce_trim_short")
        except Exception as e:
            self._log({"ev": "cap_trim_err_HALT", "coin": coin, "err": str(e)[:80]})
            with self.lock:
                self.halted = True
            self._flatten_truth("cap_enforce_err")

    def _abort_reserve(self, coin, wallet, reason):
        with self.lock:
            if isinstance(self.positions.get(coin), dict) and self.positions[coin].get("reserving"):
                self.positions.pop(coin, None)
        self._skip(wallet, coin, reason)

    def _verify_or_abort(self, coin, wallet, sign, reason):
        """Ambiguous submit: poll exchange truth with retries. If a position exists -> record it (entryPx
        from truth) so it's managed/flattened. If truth never reads cleanly -> FAIL CLOSED (halt + alert,
        keep reservation marker so gross still counts it). Only abort if CONFIRMED flat across reads."""
        pos = None; unknown = 0
        for _ in range(5):
            pp = self._parent_positions()   # ALL-DEX truth (builder coins included), fail-closed on any dex error
            if pp is None:
                unknown += 1; time.sleep(1); continue
            if coin in pp:
                pos = pp[coin]; break
            time.sleep(0.8)
        if pos is not None:
            szi = pos["szi"]; epx = pos.get("px") or self._mark(coin) or 1.0
            with self.lock:
                self.positions[coin] = {"dir": 1 if szi > 0 else -1, "entry_ts": time.time(), "entry_px": epx,
                                        "leader_px": 0.0, "size": abs(szi), "wallet": wallet,
                                        "arm": self.arm.get(wallet, "?"), "ambiguous": True}
                self.entry_times.append(time.time()); self.last_coin[coin] = time.time()
            self._log({"ev": "ambiguous_recorded", "coin": coin, "szi": szi, "entry_px": epx, "reason": reason})
            if self.live:
                self._enforce_caps_postfill(coin)   # apply the cap invariant to truth-recovered fills too
            return
        if unknown == 0:
            self._abort_reserve(coin, wallet, reason)  # ALL reads clean + flat -> safe to release
            return
        # any failed truth read before confirming flat -> could be exposed -> FAIL CLOSED: halt AND actively
        # attempt an exchange-truth flatten (retries truth reads + reduce-only) before falling back to manual.
        with self.lock:
            self.halted = True
        self._log({"ev": "ambiguous_unknown_HALT", "coin": coin, "reason": reason, "unknown_reads": unknown})
        _tg(f"[copy-measure] AMBIGUOUS submit + {unknown} unreadable truth reads {coin} -> halt+flatten attempt")
        self._flatten_truth("ambiguous_unknown")

    # ---------- exit / risk ----------
    def manage(self):
        threading.Thread(target=self.bag_loop, daemon=True).start()
        while True:
            time.sleep(3)
            if os.path.exists(PAUSE_FILE):
                if self.live:
                    with self.lock:
                        self.halted = True       # block on_fill BEFORE flattening
                    self._flatten_truth("pause_file"); _tg("[copy-measure] pause -> flatten attempt, exit"); os._exit(0)
                # dry-run places no orders: the global pause is a live kill switch; ignore it so the dry-run
                # can validate signal/sizing/logging on the live feed. (Use TaskStop/pkill to stop a dry-run.)
            self._refresh_mids()
            # GC the flow aggregator: prune each deque by window, drop empty keys (no unbounded growth)
            cut = int(time.time() * 1000) - CFG["twap_window_s"] * 1000
            for k in list(self.flow.keys()):
                dq = self.flow[k]
                while dq and dq[0][0] < cut:
                    dq.popleft()
                if not dq:
                    del self.flow[k]
            eq = self._equity()
            for coin, p in list(self.positions.items()):
                if not isinstance(p, dict) or p.get("reserving"):
                    continue
                mark = self._mark(coin)
                age = time.time() - p["entry_ts"]
                if mark:                              # disaster check needs a price; only when mark is fresh
                    ret = p["dir"] * (mark - p["entry_px"]) / p["entry_px"]
                    if ret <= -CFG["disaster_pct"]:
                        self._exit(coin, mark, "disaster_8pct"); continue
                if age >= CFG["hold_s"]:              # TIME-based -> fire even if mark is stale (_exit uses
                    self._exit(coin, mark or p["entry_px"], "hold_60m")   # clearinghouse truth px in live
            # kill switches on REAL equity; fail closed if equity unreadable too long.
            # ALWAYS set halted under self.lock first (waits behind any in-flight live entry submit), THEN
            # release and flatten -- so a halt cannot interleave with a non-reduce order (codex r5).
            upnl = self._perp_upnl() if eq is not None else None
            if eq is None or upnl is None:   # equity OR any-dex uPnL unreadable -> fail closed
                if self.equity_fail_since is None:
                    self.equity_fail_since = time.time()
                elif self.live and time.time() - self.equity_fail_since > CFG["equity_fail_halt_s"] and not self.halted:
                    with self.lock:
                        self.halted = True
                    self._flatten_truth("equity_or_upnl_unreadable_failclosed")
                    _tg("[copy-measure] equity/uPnL unreadable -> fail-closed halt+flat")
            else:
                self.equity_fail_since = None
                dd = (eq + upnl) - self.start_equity   # mark-to-market: spot USDC + perp uPnL (all dexes)
                if dd <= CFG["daily_max_loss"] and not self.halted:
                    with self.lock:
                        self.halted = True
                    self._flatten_truth("daily_max_loss")
                    _tg(f"[copy-measure] DAILY MAX LOSS dd={dd:.2f} -> halt+flat")
                if dd <= CFG["experiment_max_loss"]:
                    with self.lock:
                        self.halted = True
                    self._flatten_truth("experiment_max_loss")
                    open(PAUSE_FILE, "a").close()   # under launchd KeepAlive: set pause so relaunch stays DOWN
                    _tg(f"[copy-measure] EXPERIMENT MAX LOSS dd={dd:.2f} -> stop"); os._exit(0)

    def _exit(self, coin, mark, reason):
        with self.lock:
            p = self.positions.get(coin)
            if not isinstance(p, dict) or p.get("reserving"):
                return
        exit_px = mark
        if self.live:
            ok = False
            for _ in range(6):
                pp = self._parent_positions()
                if pp is None:
                    time.sleep(1); continue
                info = pp.get(coin)
                if not info or abs(info["szi"]) < 1e-12:
                    ok = True; break
                szi = info["szi"]; is_buy = szi < 0      # truth-derived side
                bid, ask, _, _, _ = self._book(coin)
                cross = self._cross_px(coin, is_buy, bid, ask, self._mark(coin), slip=0.006)
                if not cross and info.get("px"):         # builder market-data stale -> use clearinghouse truth price
                    cross = hl_round_px(info["px"] * (1.006 if is_buy else 0.994), self.sz_decimals.get(coin, 2), is_buy)
                if not cross:
                    time.sleep(1); continue              # no ref px at all -> retry, never crash
                try:
                    r = self.exchange.order(coin, is_buy, abs(szi), cross, {"limit": {"tif": "Ioc"}}, reduce_only=True)
                    _, avg = self._order_fill(r)
                    if avg:
                        exit_px = avg
                except Exception as e:
                    self._log({"ev": "exit_retry_err", "coin": coin, "err": str(e)[:100]})
                time.sleep(1)
            if not ok:
                pp = self._parent_positions()            # final check: last IOC may have filled
                if pp is not None and coin not in pp:
                    ok = True
            if not ok:
                with self.lock:
                    self.halted = True
                self._log({"ev": "exit_unconfirmed_HALT", "coin": coin, "reason": reason})
                _tg(f"[copy-measure] EXIT UNCONFIRMED {coin} -> halted, still exposed, manual check")
                return
        with self.lock:
            p = self.positions.pop(coin, None)
        if not isinstance(p, dict):
            return
        gross_ret = p["dir"] * (exit_px - p["entry_px"]) / p["entry_px"]
        net = gross_ret - CFG["fee_rt"]
        pnl = net * p["size"] * p["entry_px"]
        self.realized += pnl
        self._log({"ev": "exit", "live": self.live, "coin": coin, "reason": reason, "arm": p.get("arm"),
                   "dir": p["dir"], "entry_px": p["entry_px"], "exit_px": exit_px,
                   "hold_s": round(time.time() - p["entry_ts"]), "gross_bps": round(1e4 * gross_ret, 2),
                   "net_bps": round(1e4 * net, 2), "pnl": round(pnl, 4), "cum_realized": round(self.realized, 4)})

    def _flatten_truth(self, reason):
        """Flatten from EXCHANGE TRUTH; only clear local state when CONFIRMED flat."""
        for _ in range(10):
            pp = self._parent_positions()
            if pp is None:
                time.sleep(1.2); continue
            if not pp:
                self.positions.clear(); return True
            self._refresh_mids()
            for coin, info in pp.items():
                szi = info["szi"]; is_buy = szi < 0
                bid, ask, _, _, _ = self._book(coin)
                cross = self._cross_px(coin, is_buy, bid, ask, self._mark(coin), slip=0.01)
                if not cross and info.get("px"):   # market data stale -> clearinghouse truth price fallback
                    cross = hl_round_px(info["px"] * (1.01 if is_buy else 0.99), self.sz_decimals.get(coin, 2), is_buy)
                if self.live and cross:
                    try:
                        self.exchange.order(coin, is_buy, abs(szi), cross, {"limit": {"tif": "Ioc"}}, reduce_only=True)
                    except Exception as e:
                        self._log({"ev": "flatten_err", "coin": coin, "err": str(e)[:100]})
                self._log({"ev": "flatten", "coin": coin, "szi": szi, "reason": reason})
            time.sleep(1.5)
        # final verify: do NOT clear if still exposed
        pp = self._parent_positions()
        if pp == {}:
            self.positions.clear(); return True
        with self.lock:
            self.halted = True
        _tg(f"[copy-measure] FLATTEN UNCONFIRMED ({reason}) -> still exposed: {pp}; halted, manual check")
        self._log({"ev": "flatten_unconfirmed_HALT", "reason": reason, "remaining": pp})
        return False

    # ---------- feed ----------
    def run(self):
        threading.Thread(target=self.manage, daemon=True).start()
        while True:
            try:
                ws = websocket.create_connection(WS_URL, timeout=20)
                for w in self.wset:
                    ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": w}}))
                    time.sleep(0.1)
                last_ping = time.time(); subok = 0
                while True:
                    try:
                        ws.settimeout(5)
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        if time.time() - last_ping > 25:   # keepalive ping on idle; do NOT tear down
                            ws.send(json.dumps({"method": "ping"})); last_ping = time.time()
                        continue
                    if not raw:
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    ch = msg.get("channel")
                    if ch == "error":
                        self._log({"ev": "ws_error", "msg": str(msg.get("data"))[:140]}); continue
                    if ch == "subscriptionResponse":
                        subok += 1
                        if subok == len(self.wset):
                            self._log({"ev": "subscribed", "n": subok, "of": len(self.wset)})
                        continue
                    if ch != "userFills":
                        continue
                    data = msg.get("data", {})
                    user = (data.get("user") or "").lower()
                    if data.get("isSnapshot"):
                        for fl in data.get("fills", []):
                            self.seen.add((user, fl.get("oid"), fl.get("tid"), fl.get("time")))
                        continue
                    if user not in self.wset:
                        continue
                    for fl in data.get("fills", []):
                        try:
                            self.on_fill(user, fl)
                        except Exception as e:
                            self._log({"ev": "onfill_err", "err": str(e)[:120]})
            except Exception as e:
                self._log({"ev": "ws_reconnect", "err": str(e)[:120]})
                time.sleep(3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", nargs="+",
                    default=["config/copy_live_realpnl_5.json", "config/copy_live_backtest_5.json"])
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()
    CopyMeasure(args.wallets, args.live).run()


if __name__ == "__main__":
    main()
