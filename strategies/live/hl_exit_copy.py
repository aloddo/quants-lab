#!/usr/bin/env python3
"""hl_exit_copy.py -- LIVE-SMALL dust validator for the EXIT-COPY (commitment-following) strategy.

Strategy (codex DESIGN-SHIP projects/quant/copy/2026-06-08-exit-copy-strategy-design-codex-ship):
  Copy venue-native leaders FAST once their cumulative same-(coin,side) builder-perp open notional
  crosses $1k (COMMITMENT). Then FOLLOW the leader's position lifecycle out, UNLESS our risk overlay
  exits first. Liquid builder perps only (xyz:CL, xyz:SILVER). Dust size ($10-50) -- this run's ONLY
  purpose is to MEASURE real entry concession vs the leader commitment fill (the +58bps commit-entry
  arm vs the -16bps last-open arm). Decision is on the logged concession, not dust PnL.

Inherits the exchange-truth core of hl_copy_measure.py (confirm every fill; flatten reads clearinghouse
and only clears local state when CONFIRMED flat; HL-safe tick rounding; correct crossing book side; caps
vs worst-case fill; recheck halt/latency before submit; fail-closed on missing book/vol/equity/uPnL;
refuse to start unless parent flat; per-wallet bag-guard).

EXIT (new vs measure bot): primary = FOLLOW LEADER EXIT (when the triggering leader closes >=90% of the
position they held). Overlay overrides: disaster -150bps; trailing TP (arm +75bps, trail 35bps, tighten
to 20bps after +150bps; rule 7); funding stop (exit if our cumFunding cost since open >= 25bps of
notional); time stop 24h.

SAFETY: dry-run by DEFAULT. /tmp/v12_pause halts+flattens. Run with the quants-lab conda python.
  python strategies/live/hl_exit_copy.py            # dry-run (no orders)
  python strategies/live/hl_exit_copy.py --live      # real DUST orders
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
LOG_PATH = Path("research/v15/out/live/exit_copy_log.jsonl")
PAUSE_FILE = "/tmp/v12_pause"  # the v12-copy-trader launchd daemon (KeepAlive) runs THIS bot via
# v12_launcher.sh, gated on this file. Kill: touch /tmp/v12_pause (launcher won't relaunch + running bot
# flatten+exits). Terminal kill-switches also touch it so KeepAlive can't restart past a kill.
DIR_SIGN = {"Open Long": 1, "Open Short": -1, "Short > Long": 1, "Long > Short": -1}
BUILDER_DEXES = ["xyz", "flx"]
LIQ_COINS = {"xyz:CL", "xyz:SILVER"}   # liquid builder perps only (codex universe v1)


def _coin_dex(coin):
    return coin.split(":", 1)[0] if ":" in coin else ""

CFG = dict(
    min_leader_notional=1000.0,            # commitment threshold (cumulative same-coin/side opens)
    size_cap_usd=50.0, size_vol_frac=0.002, size_floor_usd=10.0,   # DUST
    max_concurrent=6, gross_cap_usd=200.0, per_coin_cap_usd=50.0,
    # exit overlay
    leader_exit_frac=0.10,                 # exit when leader holds <= 10% of their peak (i.e. >=90% closed)
    disaster_bps=150.0,                    # hard stop -1.5% from our fill
    tp_arm_bps=75.0, tp_trail_bps=35.0, tp_tighten_after_bps=150.0, tp_trail_tight_bps=20.0,
    funding_stop_bps=25.0,                 # exit if our cumFunding cost since open >= 25bps of notional
    max_hold_s=86400,                      # 24h time stop
    # risk kills (dust backstops)
    daily_max_loss=-15.0, experiment_max_loss=-40.0, max_entries_per_hour=60,
    coin_cooldown_s=30, wallet_coin_cooldown_s=30, max_spread_bps=8.0,
    max_fill_age_ms=5000, bag_worst_pos_pct=-0.06, bag_open_loss_frac=-0.03, bag_max_stale_s=240,
    fee_rt=0.000164,                       # real builder RT taker ~1.6bps (measured from leader fills)
    equity_fail_halt_s=60, mark_max_age_s=30,
    depth_within_bps=25.0, depth_mult=3.0, min_vol60s_usd=20000.0,
    twap_window_s=600,
    leader_poll_s=30,                      # backstop poll of leader position truth for coins we hold
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
    if px <= 0:
        return px
    exp = math.floor(math.log10(abs(px)))
    sig_q = 10.0 ** (exp - 4)
    dec_q = 10.0 ** (-(max(0, 6 - szd)))
    q = max(sig_q, dec_q)
    return (math.ceil(px / q) * q) if is_buy else (math.floor(px / q) * q)


def hl_floor_sz(sz, szd):
    f = 10 ** szd
    return math.floor(sz * f) / f


class ExitCopy:
    def __init__(self, wallets_files, live):
        self.live = live
        self.arm = {}
        for wf in wallets_files:
            arm = Path(wf).stem
            for w in json.load(open(wf)).get("wallets", []):
                self.arm.setdefault(w.lower(), arm)
        self.wset = set(self.arm)
        if not self.wset:
            raise RuntimeError("no wallets loaded")
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
        self.sz_decimals = {}
        for dex in all_dexes:
            try:
                m = self.info.meta(dex=dex) if dex else self.info.meta()
                for a in m["universe"]:
                    self.sz_decimals[a["name"]] = a["szDecimals"]
            except Exception as e:
                raise RuntimeError(f"meta dex={dex or 'main'} failed at startup: {e}")
        # require our liquid coins to be present in the perp universe
        missing = [c for c in LIQ_COINS if c not in self.sz_decimals]
        if missing:
            raise RuntimeError(f"liquid coins missing from universe: {missing}")
        self.mids = {}
        self._refresh_mids()
        self.start_equity = self._equity()
        if self.start_equity is None:
            raise RuntimeError("cannot read parent equity at startup")
        if live and self.start_equity < 10.0:
            raise RuntimeError(f"live start_equity ${self.start_equity:.2f} below $10 floor -- unfunded?")
        self.positions = {}      # coin -> dict(...) | {"reserving":True}
        self.seen = set()
        self.entry_times = deque()
        self.last_coin = {}
        self.flow = {}           # (wallet,coin,side) -> deque[(ts_ms, notional)] commitment aggregation
        self.leader_pos = {}     # (wallet,coin) -> signed position (post-fill truth)
        self.leader_fill_ms = {} # (wallet,coin) -> last applied fill time (drop out-of-order fills)
        self.journey_lock = {}   # (wallet,coin) -> True while a closed journey awaits leader-flat re-arm
        self.last_wallet_coin = {}
        self.bag = {}
        self.realized = 0.0
        self.n_entries = 0       # executed live cycles (the live-small counter)
        self.halted = False
        self.equity_fail_since = None
        self.postruth_fail_since = None   # position-truth (leader/parent) unreadable timer -> fail closed
        self.lock = threading.Lock()
        # STARTUP ORPHAN GUARD: never leave an unmanaged live position. We have no leader attribution on a
        # cold restart, so follow-exit cannot manage a recovered position -> safest is flatten to a clean
        # slate, then refuse to start only if STILL non-flat. (dry-run: just refuse on non-flat.)
        pp = self._parent_positions()
        if pp is None:
            raise RuntimeError("cannot verify parent flat (user_state failed); refusing to start")
        if pp:
            if not live:
                raise RuntimeError(f"parent NOT FLAT at startup: {pp}; flatten first (dry-run won't trade)")
            _tg(f"[exit-copy] startup NON-FLAT {list(pp)} -> flattening to clean slate before start")
            self._flatten_truth("startup_nonflat")
            pp2 = self._parent_positions()
            if pp2 is None or pp2:
                raise RuntimeError(f"startup flatten failed/unverified: {pp2}; refusing to start")
            self.positions = {}
        self._seed_bags()
        self._seed_leader_positions()
        self._log({"ev": "start", "live": live, "wallets": len(self.wset), "coins": sorted(LIQ_COINS),
                   "arms": sorted(set(self.arm.values())), "start_equity": self.start_equity, "cfg": CFG})
        _tg(f"[exit-copy] START {'LIVE' if live else 'DRY'} | {len(self.wset)} wallets | {sorted(LIQ_COINS)} | eq ${self.start_equity:.2f} | parent FLAT ok")

    # ---------- truth helpers ----------
    def _log(self, d):
        d["t"] = int(time.time() * 1000)
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(d) + "\n")

    def _refresh_mids(self):
        now = time.time()
        for dex in [""] + BUILDER_DEXES:
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
        v = self.mids.get(coin)
        if not v or (time.time() - v[1]) > CFG["mark_max_age_s"]:
            return None
        return v[0]

    def _equity(self):
        """UNIFIED-ACCOUNT equity = SPOT USDC (rule 16). Spot USDC IS the perp collateral; no transfer."""
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
        """EXCHANGE TRUTH across MAIN + BUILDER dexes: {coin: {szi, px, cum_funding}} nonzero. None on any
        dex failure (caller fails closed). cum_funding = position.cumFunding.sinceOpen (cost>0 = we paid)."""
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
                    cf = float((p.get("cumFunding") or {}).get("sinceOpen", 0) or 0)
                    epx = float(p.get("entryPx", 0) or 0)   # exchange-truth entry price (for recovery)
                except (KeyError, TypeError, ValueError):
                    return None
                if _coin_dex(coin) not in ([""] + BUILDER_DEXES):
                    return None
                if abs(szi) > 0:
                    out[coin] = {"szi": szi, "px": (pv / abs(szi)) if abs(szi) > 0 and pv > 0 else None,
                                 "entry_px": epx if epx > 0 else None, "cum_funding": cf}
        return out

    def _leader_pos_truth(self, wallet, coin):
        """Signed position of a leader on a coin from clearinghouse truth (backstop for missed ws fills).
        None on read failure (caller leaves ws-tracked value intact)."""
        dex = _coin_dex(coin)
        payload = {"type": "clearinghouseState", "user": wallet}
        if dex:
            payload["dex"] = dex
        d = _post_info(payload)
        if not isinstance(d, dict) or not isinstance(d.get("assetPositions"), list):
            return None
        for ap in d["assetPositions"]:
            try:
                p = ap["position"]
                if p["coin"] == coin:
                    return float(p["szi"])
            except (KeyError, TypeError, ValueError):
                return None
        return 0.0   # not in positions -> flat

    def _book(self, coin):
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
        ref = (ref_ask if is_buy else ref_bid) or mark
        if not ref:
            return None
        return hl_round_px(ref * (1 + slip if is_buy else 1 - slip), self.sz_decimals.get(coin, 2), is_buy)

    # ---------- bag-guard ----------
    def _refresh_bag(self, w):
        worst = 0.0; tot = 0.0; eqsum = 0.0
        for dex in [""] + BUILDER_DEXES:
            payload = {"type": "clearinghouseState", "user": w}
            if dex:
                payload["dex"] = dex
            d = _post_info(payload)
            if not isinstance(d, dict) or not isinstance(d.get("assetPositions"), list):
                self.bag[w] = (True, time.time()); return
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

    def _seed_leader_positions(self):
        """Seed leader_pos for every (wallet, LIQ coin) from clearinghouse TRUTH at startup (HIGH 5), so
        leader_peak at entry reflects real current exposure rather than arbitrary snapshot fill order."""
        now_ms = int(time.time() * 1000)
        for w in list(self.wset):
            for coin in LIQ_COINS:
                t = self._leader_pos_truth(w, coin)
                if t is not None:
                    self.leader_pos[(w, coin)] = t
                    self.leader_fill_ms[(w, coin)] = now_ms
                time.sleep(0.05)

    def bag_loop(self):
        while True:
            for w in list(self.wset):
                self._refresh_bag(w); time.sleep(0.3)
            time.sleep(60)

    def _bagged(self, w):
        v = self.bag.get(w)
        if v is None or time.time() - v[1] > CFG["bag_max_stale_s"]:
            return True
        return v[0]

    # ---------- leader position tracking (for follow-exit) ----------
    def _open_contrib(self, f):
        """(side, open_notional) of the NEWLY-OPENED exposure of this fill. Flips count ONLY the residual
        new-side exposure, not the closing leg (codex BLOCKER 2). Closes contribute nothing."""
        d = str(f.get("dir", ""))
        try:
            sz = float(f.get("sz", 0)); px = float(f.get("px", 0)); spos = float(f.get("startPosition", 0))
        except (TypeError, ValueError):
            return 0, 0.0
        if d == "Open Long":
            return 1, sz * px
        if d == "Open Short":
            return -1, sz * px
        if d == "Short > Long":          # start<0 -> ends>0; new long residual = post
            post = spos + sz
            return (1, post * px) if post > 0 else (0, 0.0)
        if d == "Long > Short":          # start>0 -> ends<0; new short residual = -post
            post = spos - sz
            return (-1, -post * px) if post < 0 else (0, 0.0)
        return 0, 0.0                    # Close Long / Close Short -> no new open

    def _fill_post(self, f):
        """Leader position AFTER this specific fill = startPosition + signed delta. Order-INDEPENDENT and
        absolute (startPosition is exchange truth before the fill) -> used to grow leader_peak from EVERY
        fill even if the current-position tracker drops it as stale (codex r4 BLOCKER)."""
        d = str(f.get("dir", ""))
        try:
            sz = float(f.get("sz", 0)); sp = float(f.get("startPosition", 0))
        except (TypeError, ValueError):
            return None
        if d in ("Open Long", "Close Short", "Short > Long"):
            delta = sz
        elif d in ("Open Short", "Close Long", "Long > Short"):
            delta = -sz
        else:
            delta = 0.0
        return sp + delta

    def _update_leader_pos(self, wallet, coin, f):
        """Maintain signed leader position per (wallet,coin) = startPosition + signed_delta (post-fill truth).
        Drops out-of-order fills (HIGH 5). Caller holds self.lock."""
        d = str(f.get("dir", ""))
        try:
            sz = float(f.get("sz", 0)); sp = float(f.get("startPosition", 0)); fms = int(f.get("time", 0))
        except (TypeError, ValueError):
            return
        k = (wallet, coin)
        if fms < self.leader_fill_ms.get(k, 0):
            return                        # stale/out-of-order: do not overwrite newer truth
        if d in ("Open Long", "Close Short", "Short > Long"):
            delta = sz
        elif d in ("Open Short", "Close Long", "Long > Short"):
            delta = -sz
        else:
            delta = 0.0
        self.leader_pos[k] = sp + delta
        self.leader_fill_ms[k] = fms

    # ---------- signal ----------
    def on_fill(self, wallet, f):
        if self.halted:
            return
        coin = f.get("coin", "")
        if coin not in LIQ_COINS:                 # we only track + trade liquid builder coins
            return
        # DEDUP FIRST (HIGH 5): never apply the same fill twice to leader_pos or the aggregate
        key = (wallet, f.get("oid"), f.get("tid"), f.get("time"))
        if key in self.seen:
            return
        self.seen.add(key)
        side, open_notional = self._open_contrib(f)   # BLOCKER 2: flips count only the new-side residual
        now_ms = int(time.time() * 1000)
        fill_ms = int(f.get("time", 0))
        # leader-position tracking for EVERY (incl. close) LIQ fill, under lock, time-ordered; re-arm a
        # journey lockout once the leader is confirmed FLAT or FLIPPED vs the locked side (HIGH 7)
        post = self._fill_post(f)   # this fill's absolute post-position (for order-independent peak growth)
        with self.lock:
            self._update_leader_pos(wallet, coin, f)
            jk = (wallet, coin)
            lp_now = self.leader_pos.get(jk, 0.0)
            # REAL-TIME leader_peak growth for a held position from THIS fill's own post (codex r4 BLOCKER:
            # use post, not current leader_pos, so a poll/stale-drop can never hide the true intratick peak).
            held = self.positions.get(coin)
            if (isinstance(held, dict) and not held.get("reserving") and held.get("wallet") == wallet
                    and post is not None):
                # FOLLOW-LEADER-EXIT off THIS fill's own post (codex r5/r6 HIGH): evaluate the exit BEFORE
                # peak growth, so a >=90% close that leaves a SAME-SIDE residual still triggers (e.g. peak
                # 100, post 5 long -> exit). Flip or >=90%-closed sets pending_exit; manage executes it next
                # tick. Only a genuine same-side ADD (above the threshold) grows the peak.
                peak = held.get("leader_peak", 1e-12)
                if (post * held["dir"] < 0) or abs(post) <= CFG["leader_exit_frac"] * peak:
                    held["pending_exit"] = "leader_exit"
                elif post * held["dir"] > 0:
                    held["leader_peak"] = max(peak, abs(post))
            # re-arm a journey lockout once the leader is confirmed FLAT or FLIPPED vs the locked side
            jdir = self.journey_lock.get(jk)
            if jdir is not None:
                if abs(lp_now) < 1e-12 or (lp_now * jdir < 0):
                    self.journey_lock.pop(jk, None)
        if coin not in self.sz_decimals:
            return
        if not bool(f.get("crossed", False)):
            return self._skip(wallet, coin, "not_taker")
        if side == 0 or open_notional <= 0:           # closes / pure reductions never trigger commitment
            return
        if now_ms - fill_ms > CFG["max_fill_age_ms"]:
            return self._skip(wallet, coin, "stale_fill")
        # COMMITMENT aggregation: journey lockout check AND flow append in ONE lock block so an overlay
        # exit cannot commit stale flow while locked (codex #7). _try_enter rechecks the lock too.
        akey = (wallet, coin, side)
        locked = False; held_already = False
        with self.lock:
            if self.journey_lock.get((wallet, coin)) is not None:
                locked = True
            elif coin in self.positions:
                # already holding/reserving this coin (one-position-per-coin): do NOT aggregate adds into
                # flow -- they would survive into a later journey and trigger below true commitment (codex r4)
                held_already = True
            else:
                dq = self.flow.setdefault(akey, deque())
                dq.append((fill_ms, open_notional))
                cutoff = fill_ms - CFG["twap_window_s"] * 1000
                while dq and dq[0][0] < cutoff:
                    dq.popleft()
                agg = sum(n for _, n in dq)
        if locked:
            return self._skip(wallet, coin, "journey_locked")
        if held_already:
            return self._skip(wallet, coin, "coin_open")
        if agg < CFG["min_leader_notional"]:
            return self._skip(wallet, coin, f"agg_below_{agg:.0f}")
        if self._bagged(wallet):
            return self._skip(wallet, coin, "wallet_bag")
        # commitment_px = px of THIS crossing fill; detect_ms = now (the concession reference)
        self._try_enter(wallet, coin, side, float(f.get("px", 0)), f, now_ms)

    def _skip(self, wallet, coin, reason):
        self._log({"ev": "skip", "wallet": wallet, "coin": coin, "reason": reason})

    def _try_enter(self, wallet, coin, sign, commit_px, f, detect_ms):
        now = time.time()
        with self.lock:
            if self.halted:
                return
            if self.journey_lock.get((wallet, coin)) is not None:   # HIGH 7: recheck under lock (overlay
                return self._skip(wallet, coin, "journey_locked")   # exit could have set it after on_fill)
            if coin in self.positions:
                return self._skip(wallet, coin, "coin_open")
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
            take_depth = dask if is_buy else dbid
            if take_depth is None or take_depth < CFG["depth_mult"] * CFG["size_cap_usd"]:
                return self._abort_reserve(coin, wallet, f"thin_depth_{(take_depth or 0):.0f}")
            cross = self._cross_px(coin, is_buy, bid, ask, mark)
            size_px = cross if is_buy else (bid or mark)
            target = min(CFG["size_cap_usd"], CFG["size_vol_frac"] * vol, CFG["per_coin_cap_usd"], gross_room)
            szd = self.sz_decimals[coin]
            sz = hl_floor_sz(target / size_px, szd)
            if sz <= 0 or sz * size_px < CFG["size_floor_usd"]:
                return self._abort_reserve(coin, wallet, f"size<floor(vol{vol:.0f})")
            lat = now - detect_ms / 1000.0
            leader_pk = abs(self.leader_pos.get((wallet, coin), 0.0))
            entry_px, filled = mark, sz
            submit_ms = int(time.time() * 1000)
            if self.live:
                if int(time.time() * 1000) - int(f.get("time", 0)) > CFG["max_fill_age_ms"]:
                    return self._abort_reserve(coin, wallet, "stale_presubmit")
                aborted = False; capped = False; need_verify = None
                self._refresh_mids()
                with self.lock:
                    if self.halted or not (isinstance(self.positions.get(coin), dict) and self.positions[coin].get("reserving")):
                        aborted = True
                    elif (sum(p["size"] * (self._mark(c) or p["entry_px"]) for c, p in self.positions.items()
                              if isinstance(p, dict) and not p.get("reserving")) + sz * size_px) > CFG["gross_cap_usd"]:
                        capped = True
                    else:
                        try:
                            r = self.exchange.order(coin, is_buy, sz, cross, {"limit": {"tif": "Ioc"}}, reduce_only=False)
                            fz, avg = self._order_fill(r)
                            if fz > 0:
                                entry_px, filled = avg, fz
                                self._record_position(coin, sign, avg, commit_px, fz, wallet, leader_pk, now)
                            else:
                                need_verify = f"not_filled:{str(r)[:100]}"
                        except Exception as e:
                            need_verify = f"order_exc:{str(e)[:80]}"
                if aborted:
                    return self._abort_reserve(coin, wallet, "halt_or_slot_lost_presubmit")
                if capped:
                    return self._abort_reserve(coin, wallet, "gross_cap_presubmit")
                if need_verify:
                    return self._verify_or_abort(coin, wallet, sign, need_verify, commit_px, leader_pk)
            else:
                with self.lock:
                    self._record_position(coin, sign, entry_px, commit_px, filled, wallet, leader_pk, now)
            fill_ms2 = int(time.time() * 1000)
            concession = 1e4 * sign * (entry_px - commit_px) / commit_px if commit_px else None
            self._log({"ev": "enter", "live": self.live, "wallet": wallet, "arm": self.arm.get(wallet),
                       "coin": coin, "dir": sign, "commit_px": commit_px, "entry_px": entry_px, "size": filled,
                       "notional": filled * entry_px, "detect_ms": detect_ms, "submit_ms": submit_ms,
                       "fill_ms": fill_ms2, "latency_s": round(lat, 2), "spread_bps": round(sp, 2),
                       "leader_peak_abs": leader_pk, "n_entries": self.n_entries,
                       "concession_vs_commit_bps": round(concession, 2) if concession is not None else None})
            if self.live:
                self._enforce_caps_postfill(coin)
        except Exception as e:
            self._abort_reserve(coin, wallet, f"enter_exc:{str(e)[:80]}")

    def _record_position(self, coin, sign, entry_px, commit_px, size, wallet, leader_pk, now):
        """Caller holds self.lock."""
        self.positions[coin] = {"dir": sign, "entry_ts": now, "entry_px": entry_px, "commit_px": commit_px,
                                "size": size, "wallet": wallet, "arm": self.arm.get(wallet, "?"),
                                "leader_peak": max(leader_pk, 1e-12), "peak_ret": 0.0}
        self.entry_times.append(now); self.last_coin[coin] = now
        self.last_wallet_coin[(wallet, coin)] = now
        self.flow.pop((wallet, coin, sign), None)   # HIGH 7: consume the aggregate so slices don't re-trigger
        self.n_entries += 1

    def _enforce_caps_postfill(self, coin):
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
            self._log({"ev": "cap_sub_lot_residual", "coin": coin, "over_usd": round(over, 4)})
            return
        trim_sz = hl_floor_sz(over / epx, szd)
        is_buy = dirn < 0
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
            # MED 9: reconcile from EXCHANGE TRUTH, not the order response
            pp = self._parent_positions()
            if pp is None:
                self._log({"ev": "cap_trim_truth_unreadable_HALT", "coin": coin})
                with self.lock:
                    self.halted = True
                self._flatten_truth("cap_enforce_truth_unreadable"); return
            true_szi = abs(pp.get(coin, {}).get("szi", 0.0))
            with self.lock:
                if coin in self.positions and isinstance(self.positions[coin], dict) and not self.positions[coin].get("reserving"):
                    self.positions[coin]["size"] = true_szi
            true_notional = true_szi * epx
            with self.lock:
                gross = sum(q["size"] * (self._mark(c) or q["entry_px"]) for c, q in self.positions.items()
                            if isinstance(q, dict) and not q.get("reserving"))
            tol = epx * (10 ** -szd)
            self._log({"ev": "cap_trim", "coin": coin, "over_usd": round(over, 2), "trim_sz": trim_sz,
                       "filled": tz, "true_size": true_szi, "true_notional": round(true_notional, 2),
                       "gross": round(gross, 2)})
            # if still over single/per-coin OR gross cap after the trim (confirmed by truth), escalate
            if (true_notional > max(CFG["size_cap_usd"], CFG["per_coin_cap_usd"]) + tol
                    or gross > CFG["gross_cap_usd"] + tol):
                self._log({"ev": "cap_trim_short_HALT", "coin": coin, "trim_sz": trim_sz,
                           "true_size": true_szi, "gross": round(gross, 2)})
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

    def _verify_or_abort(self, coin, wallet, sign, reason, commit_px, leader_pk):
        pos = None; unknown = 0
        for _ in range(5):
            pp = self._parent_positions()
            if pp is None:
                unknown += 1; time.sleep(1); continue
            if coin in pp:
                pos = pp[coin]; break
            time.sleep(0.8)
        if pos is not None:
            szi = pos["szi"]; epx = pos.get("entry_px") or pos.get("px") or self._mark(coin) or 1.0
            with self.lock:
                self._record_position(coin, 1 if szi > 0 else -1, epx, commit_px, abs(szi), wallet, leader_pk, time.time())
                self.positions[coin]["ambiguous"] = True
            self._log({"ev": "ambiguous_recorded", "coin": coin, "szi": szi, "entry_px": epx, "reason": reason})
            if self.live:
                self._enforce_caps_postfill(coin)
            return
        if unknown == 0:
            self._abort_reserve(coin, wallet, reason)
            return
        with self.lock:
            self.halted = True
        self._log({"ev": "ambiguous_unknown_HALT", "coin": coin, "reason": reason, "unknown_reads": unknown})
        _tg(f"[exit-copy] AMBIGUOUS submit + {unknown} unreadable truth reads {coin} -> halt+flatten attempt")
        self._flatten_truth("ambiguous_unknown")

    # ---------- exit / risk ----------
    def manage(self):
        threading.Thread(target=self.bag_loop, daemon=True).start()
        self._last_leader_poll = 0.0
        while True:
            time.sleep(3)
            try:
                self._manage_tick()
            except SystemExit:
                raise
            except Exception as e:
                # a manage-tick exception must NEVER silently stop all exits (codex BLOCKER 4)
                self._log({"ev": "manage_tick_err", "err": str(e)[:160]})
                if self.live and not self.halted:
                    try:
                        with self.lock:
                            self.halted = True
                        self._flatten_truth("manage_tick_exception")
                        _tg(f"[exit-copy] manage tick exception -> halt+flat: {str(e)[:100]}")
                    except Exception:
                        pass

    def _manage_tick(self):
        if os.path.exists(PAUSE_FILE):
            if self.live:
                with self.lock:
                    self.halted = True
                self._flatten_truth("pause_file"); _tg("[exit-copy] pause -> flatten attempt, exit"); os._exit(0)
        self._refresh_mids()
        # GC the commitment aggregator
        cut = int(time.time() * 1000) - CFG["twap_window_s"] * 1000
        with self.lock:
            for k in list(self.flow.keys()):
                dq = self.flow[k]
                while dq and dq[0][0] < cut:
                    dq.popleft()
                if not dq:
                    del self.flow[k]
        do_poll = time.time() - self._last_leader_poll > CFG["leader_poll_s"]
        if do_poll:
            self._last_leader_poll = time.time()
        truth = self._parent_positions() if self.live else None
        eq = self._equity()
        # RUNTIME ORPHAN SWEEP: any exchange position not tracked locally (missed fill / partial-state) is
        # unexpected on this dedicated account -> halt + flatten (fail-safe). Entries reserve the slot before
        # ordering and exits pop only after confirmed-flat, so this never fires on a normal in-flight trade.
        if self.live and truth:
            untracked = [c for c in truth if c not in self.positions]
            if untracked and not self.halted:
                self._log({"ev": "orphan_detected_HALT", "coins": untracked})
                with self.lock:
                    self.halted = True
                self._flatten_truth("orphan_untracked")
                _tg(f"[exit-copy] ORPHAN exchange position(s) {untracked} -> halt+flatten")
                return
        # POSITION-TRUTH fail-closed timer (HIGH 6): in live, if we hold positions but parent truth is
        # unreadable, we cannot trust funding/exits -> halt+flatten after a short grace.
        held = any(isinstance(p, dict) and not p.get("reserving") for p in list(self.positions.values()))
        if self.live and held and truth is None:
            if self.postruth_fail_since is None:
                self.postruth_fail_since = time.time()
            elif time.time() - self.postruth_fail_since > CFG["equity_fail_halt_s"] and not self.halted:
                with self.lock:
                    self.halted = True
                self._flatten_truth("position_truth_unreadable_failclosed")
                _tg("[exit-copy] position truth unreadable -> fail-closed halt+flat")
                return
        else:
            self.postruth_fail_since = None
        for coin, p in list(self.positions.items()):
            if not isinstance(p, dict) or p.get("reserving"):
                continue
            w = p["wallet"]
            mark = self._mark(coin)
            age = time.time() - p["entry_ts"]
            # pending follow-leader-exit flagged by on_fill off a close/flip fill's own post (codex r5/r6):
            # execute immediately, cannot be hidden by leader_pos staleness. Locked read (clean handoff).
            with self.lock:
                pend = p.get("pending_exit")
            if pend:
                self._exit(coin, mark or p["entry_px"], pend, overlay=False); continue
            if mark:
                ret = p["dir"] * (mark - p["entry_px"]) / p["entry_px"]
                with self.lock:
                    if ret > p.get("peak_ret", 0.0):
                        p["peak_ret"] = ret
                if ret <= -CFG["disaster_bps"] / 1e4:
                    self._exit(coin, mark, "disaster_150bps", overlay=True); continue
                peak = p.get("peak_ret", 0.0)
                if peak >= CFG["tp_arm_bps"] / 1e4:
                    trail = (CFG["tp_trail_tight_bps"] if peak >= CFG["tp_tighten_after_bps"] / 1e4
                             else CFG["tp_trail_bps"]) / 1e4
                    if ret <= peak - trail:
                        self._exit(coin, mark, "trailing_tp", overlay=True); continue
            # FOLLOW LEADER EXIT: update leader_peak (BLOCKER 1) then exit if leader closed >=90% of peak
            poll_failed = False
            with self.lock:
                lp = self.leader_pos.get((w, coin))
            if do_poll:
                t = self._leader_pos_truth(w, coin)
                if t is not None:
                    lp = t
                    with self.lock:
                        self.leader_pos[(w, coin)] = t
                        self.leader_fill_ms[(w, coin)] = int(time.time() * 1000)  # poll is freshest truth
                else:
                    poll_failed = True
                    self._log({"ev": "leader_truth_poll_fail", "coin": coin, "wallet": w})  # log regardless
            if lp is not None:
                with self.lock:
                    if lp * p["dir"] > 0:                       # same side -> grow the peak
                        p["leader_peak"] = max(p.get("leader_peak", 1e-12), abs(lp))
                    peak_abs = p.get("leader_peak", 1e-12)
                if (lp * p["dir"] < 0) or abs(lp) <= CFG["leader_exit_frac"] * peak_abs:
                    self._exit(coin, mark or p["entry_px"], "leader_exit", overlay=False); continue
            # funding stop (exchange-truth cumFunding cost since open; >0 = we paid)
            if truth and coin in truth:
                cf = truth[coin].get("cum_funding", 0.0)
                notl = p["size"] * p["entry_px"]
                if notl > 0 and (cf / notl) >= CFG["funding_stop_bps"] / 1e4:
                    self._exit(coin, mark or p["entry_px"], "funding_stop", overlay=True); continue
            if age >= CFG["max_hold_s"]:
                self._exit(coin, mark or p["entry_px"], "time_24h", overlay=True); continue
        # kill switches on REAL equity; fail closed if unreadable too long
        upnl = self._perp_upnl() if eq is not None else None
        if eq is None or upnl is None:
            if self.equity_fail_since is None:
                self.equity_fail_since = time.time()
            elif self.live and time.time() - self.equity_fail_since > CFG["equity_fail_halt_s"] and not self.halted:
                with self.lock:
                    self.halted = True
                self._flatten_truth("equity_or_upnl_unreadable_failclosed")
                _tg("[exit-copy] equity/uPnL unreadable -> fail-closed halt+flat")
        else:
            self.equity_fail_since = None
            dd = (eq + upnl) - self.start_equity
            if dd <= CFG["daily_max_loss"] and not self.halted:
                with self.lock:
                    self.halted = True
                self._flatten_truth("daily_max_loss")
                _tg(f"[exit-copy] DAILY MAX LOSS dd={dd:.2f} -> halt+flat")
            if dd <= CFG["experiment_max_loss"]:
                with self.lock:
                    self.halted = True
                self._flatten_truth("experiment_max_loss")
                open(PAUSE_FILE, "a").close()
                _tg(f"[exit-copy] EXPERIMENT MAX LOSS dd={dd:.2f} -> stop"); os._exit(0)

    def _exit(self, coin, mark, reason, overlay=False):
        with self.lock:
            p = self.positions.get(coin)
            if not isinstance(p, dict) or p.get("reserving"):
                return
            w = p.get("wallet")
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
                szi = info["szi"]; is_buy = szi < 0
                bid, ask, _, _, _ = self._book(coin)
                cross = self._cross_px(coin, is_buy, bid, ask, self._mark(coin), slip=0.006)
                if not cross and info.get("px"):
                    cross = hl_round_px(info["px"] * (1.006 if is_buy else 0.994), self.sz_decimals.get(coin, 2), is_buy)
                if not cross:
                    time.sleep(1); continue
                try:
                    r = self.exchange.order(coin, is_buy, abs(szi), cross, {"limit": {"tif": "Ioc"}}, reduce_only=True)
                    _, avg = self._order_fill(r)
                    if avg:
                        exit_px = avg
                except Exception as e:
                    self._log({"ev": "exit_retry_err", "coin": coin, "err": str(e)[:100]})
                time.sleep(1)
            if not ok:
                pp = self._parent_positions()
                if pp is not None and coin not in pp:
                    ok = True
            if not ok:
                with self.lock:
                    self.halted = True
                self._log({"ev": "exit_unconfirmed_HALT", "coin": coin, "reason": reason})
                _tg(f"[exit-copy] EXIT UNCONFIRMED {coin} -> halted, still exposed, manual check")
                return
        with self.lock:
            p = self.positions.pop(coin, None)
            # clear BOTH-side commitment flow for this (wallet,coin) so stale adds can't seed a later
            # journey below true commitment (codex r4 HIGH)
            if isinstance(p, dict) and p.get("wallet"):
                self.flow.pop((p["wallet"], coin, 1), None)
                self.flow.pop((p["wallet"], coin, -1), None)
            # HIGH 7: after an OVERLAY exit (leader still in position), lock this journey until the leader is
            # confirmed flat/flipped (re-armed in on_fill), so later slices don't re-enter the same journey.
            if overlay and isinstance(p, dict) and p.get("wallet"):
                self.journey_lock[(p["wallet"], coin)] = p["dir"]   # store side; re-arm on flat OR flip
        if not isinstance(p, dict):
            return
        gross_ret = p["dir"] * (exit_px - p["entry_px"]) / p["entry_px"]
        net = gross_ret - CFG["fee_rt"]
        pnl = net * p["size"] * p["entry_px"]
        self.realized += pnl
        self._log({"ev": "exit", "live": self.live, "coin": coin, "reason": reason, "arm": p.get("arm"),
                   "wallet": p.get("wallet"), "dir": p["dir"], "entry_px": p["entry_px"], "exit_px": exit_px,
                   "commit_px": p.get("commit_px"), "hold_s": round(time.time() - p["entry_ts"]),
                   "gross_bps": round(1e4 * gross_ret, 2), "net_bps": round(1e4 * net, 2),
                   "peak_ret_bps": round(1e4 * p.get("peak_ret", 0.0), 2),
                   "pnl": round(pnl, 4), "cum_realized": round(self.realized, 4)})

    def _flatten_truth(self, reason):
        for _ in range(10):
            pp = self._parent_positions()
            if pp is None:
                time.sleep(1.2); continue
            if not pp:
                with self.lock:
                    self.positions.clear()
                return True
            self._refresh_mids()
            for coin, info in pp.items():
                szi = info["szi"]; is_buy = szi < 0
                bid, ask, _, _, _ = self._book(coin)
                cross = self._cross_px(coin, is_buy, bid, ask, self._mark(coin), slip=0.01)
                if not cross and info.get("px"):
                    cross = hl_round_px(info["px"] * (1.01 if is_buy else 0.99), self.sz_decimals.get(coin, 2), is_buy)
                if self.live and cross:
                    try:
                        self.exchange.order(coin, is_buy, abs(szi), cross, {"limit": {"tif": "Ioc"}}, reduce_only=True)
                    except Exception as e:
                        self._log({"ev": "flatten_err", "coin": coin, "err": str(e)[:100]})
                self._log({"ev": "flatten", "coin": coin, "szi": szi, "reason": reason})
            time.sleep(1.5)
        pp = self._parent_positions()
        if pp == {}:
            with self.lock:
                self.positions.clear()
            return True
        with self.lock:
            self.halted = True
        _tg(f"[exit-copy] FLATTEN UNCONFIRMED ({reason}) -> still exposed: {pp}; halted, manual check")
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
                        if time.time() - last_ping > 25:
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
                        # snapshot fills only mark seen (dedup); leader positions are seeded from
                        # clearinghouse TRUTH at startup (HIGH 5), not from snapshot fill order.
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
    ap.add_argument("--wallets", nargs="+", default=["config/copy_exit_basket.json"])
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()
    ExitCopy(args.wallets, args.live).run()


if __name__ == "__main__":
    main()
