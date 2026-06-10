"""V15 LIVE proportional copy executor -- clean, spec-built (projects/quant/v15/modules/live-deployment
Part C). Replaces the V11-fork approach (which fought V11's fixed-size incremental machinery).

CORE DESIGN -- LEVEL-BASED NET-TARGET RECONCILER (idempotent by construction):
  For each coin, NET TARGET szi = sum over copied leaders of
      (leader_signed_notional_in_coin / leader_equity) x OUR_SLICE / mark
  where OUR_SLICE = our_sizing_equity / N  (EQUAL-SPLIT allocation across N leaders -- Alberto 2026-06-01:
  each leader mirrors its OWN leverage within its equal slice; no leader can crowd the book).
  Each reconcile cycle: read OUR ACTUAL exchange position per coin (exchange truth), compute delta =
  net_target - actual, and place ONE IOC order for the delta (reduce-only when shrinking). Because we
  reconcile to a LEVEL (not react to events) and read exchange truth every cycle with IOC orders, this is
  inherently idempotent and handles UP and DOWN convergence natively -- no add/trim/exit state machine,
  no per-leader attribution at execution, no partial-fill poison.

SAFETY (spec C2):
  - mark-age + leader-staleness gates: never size off stale data (skip).
  - liquidation-aware pre-trade: reject an increase that pushes margin util past max.
  - GLOBAL -15% STOP: drawdown of our account value from session baseline >= stop_pct -> market_close ALL
    (exchange truth) + halt (latched, manual re-arm).
  - reconcile-to-exchange-truth every cycle; IOC only (no resting orders to leak).
NO gross cap (Alberto): risk bounded by per-slice allocation + margin util + the -15% stop.

SCOPE: live-small -- ONE account, the configured leaders, equal-split. NOT the full spec's per-subaccount
isolation / real-time source-equity collector / L2 calibration (those are for scale + cost-accuracy).

Run:
  python strategies/live/hl_prop_copy.py --config config/copy_trader_wallets_v15_prop.json [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import threading
import time

import eth_account
import requests
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

try:
    from hl_ws_feed import HLWSFeed                 # launched as a script (sys.path[0]=strategies/live)
except ImportError:
    from strategies.live.hl_ws_feed import HLWSFeed  # imported as a package (tests / one-shot flatten)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("hl_prop_copy")

HL_API = "https://api.hyperliquid.xyz"
TG_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1003576397888")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
BUILDER_DEXES = ["xyz", "flx"]


def _tg(msg: str):
    if not TG_TOKEN:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT_ID, "text": f"[prop-copy] {msg}"}, timeout=5)
    except Exception:
        pass


def _coin_dex(coin: str) -> str:
    return coin.split(":", 1)[0] if ":" in coin else ""


_SPARK = "▁▂▃▄▅▆▇█"


def _sparkline(vals: list[float]) -> str:
    """Compact unicode equity curve."""
    if not vals:
        return ""
    lo, hi = min(vals), max(vals)
    if hi - lo < 1e-9:
        return _SPARK[0] * len(vals)
    return "".join(_SPARK[min(len(_SPARK) - 1, int((v - lo) / (hi - lo) * (len(_SPARK) - 1)))] for v in vals)


EQUITY_CURVE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                 "app", "data", "v15", "v15_live_equity.json")
STOP_PEAK_PATH = os.path.join(os.path.dirname(EQUITY_CURVE_PATH), "v15_stop_peak.json")


class PropCopy:
    def __init__(self, config_path: str, dry_run: bool = False):
        with open(config_path) as f:
            cfg = json.load(f)
        g = cfg["global"]
        self.dry_run = dry_run
        self.leaders = [a.lower() for a in cfg["wallets"].keys()]
        self.n = max(1, len(self.leaders))
        # params
        self.poll_s = float(g.get("reconcile_poll_s", 8))
        self.min_notional = float(g.get("min_entry_notional", 10.0))
        self.pos_trail_pct = float(g.get("pos_trailing_stop_pct", 0.15))  # per-position trailing stop (Alberto)
        # per-leader circuit breaker (Alberto): drop a leader from the copy when its OWN book uPnL% < drop;
        # re-include when it recovers above reinclude (hysteresis to avoid flicker).
        self.leader_drop_pct = float(g.get("leader_drop_upnl_pct", 0.0))       # drop if leader uPnL/eq < this
        self.leader_reinclude_pct = float(g.get("leader_reinclude_upnl_pct", 0.01))  # re-add if >= this
        self.rebalance_frac = float(g.get("rebalance_frac", 0.25))   # only act if |delta|/|target| or slice > this
        self.mark_max_age_s = float(g.get("mark_max_age_s", 30))
        self.leader_max_age_s = float(g.get("target_equity_max_age_s", 120))
        self.global_stop_pct = float(g.get("global_stop_pct", 0.15))
        # SIZING LEVERAGE (Alberto 2026-06-03): scale the sizing-equity base so the copied book carries
        # meaningful notional ($1.5-2k target) instead of sitting idle. our_eq = spot_eq * sizing_leverage.
        # Risk stays bounded by margin util + the -15% account stop + per-position trailing + per-leader breaker.
        self.sizing_leverage = float(g.get("sizing_leverage", 1.0))
        self.max_margin_util = float(g.get("max_margin_util", 0.95))
        self.max_leverage_cap = int(g.get("max_leverage_cap", 40))
        # MODE-AWARE leader equity (2026-06-05): per-leader HL account mode + TRUE equity for unified/PM leaders.
        self._leader_mode: dict[str, str] = {}
        self._leader_true_eq: dict[str, float] = {}
        self._leader_meta_ts = 0.0
        self._leader_meta_refresh_s = 300.0

        # SDK
        self.private_key = os.environ["HL_PRIVATE_KEY"]
        self.agent_address = os.environ["HL_ADDRESS"]
        self.parent_address = os.environ.get("HL_QUERY_ADDRESS",
                                             "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
        self.account = eth_account.Account.from_key(self.private_key)
        # codex r3 (authoritative): the PRIVATE KEY's address is the real signer; assert it matches the
        # declared HL_ADDRESS so a key/env mismatch can't silently sign for the wrong agent.
        if self.account.address.lower() != self.agent_address.lower():
            raise RuntimeError(f"HL_PRIVATE_KEY address {self.account.address} != HL_ADDRESS "
                               f"{self.agent_address}; refusing to start")
        all_dexes = [""] + BUILDER_DEXES
        # Retry SDK init with backoff -- a transient 429 at startup must NOT crash-loop the keep-alive
        # daemon (the clean engine had dropped V11's init-retry; that crash-looped on a rate limit).
        self.info = None
        for attempt in range(6):
            try:
                self.info = Info(HL_API, skip_ws=True, perp_dexs=all_dexes)
                break
            except Exception as e:
                wait = min(15 * (attempt + 1), 60)
                logger.warning(f"Info() init attempt {attempt+1} failed ({str(e)[:80]}); waiting {wait}s")
                time.sleep(wait)
        if self.info is None:
            raise RuntimeError("Info() init failed after retries (rate-limited?) -- daemon will re-throttle")
        # codex r2 (critical): the AGENT key (self.account) SIGNS, but orders + reduce_only must act on the
        # account that HOLDS the positions/funds = the PARENT (account_address=parent). Initializing with
        # the agent address (as the V11 fork did) makes reduce-only / market_close query the agent (no
        # positions) -> the flatten can silently no-op. Canonical HL agent pattern = account_address=master.
        if self.agent_address.lower() == self.parent_address.lower():
            logger.warning("agent == parent address; expected an agent (signer) distinct from the funded parent")
        self.exchange = Exchange(self.account, HL_API, account_address=self.parent_address,
                                 perp_dexs=all_dexes)
        # CRITICAL HANG FIX (2026-06-05): the HL SDK defaults API.timeout=None, so every SDK HTTP call
        # (info.all_mids/meta/order) can BLOCK FOREVER on a network/API stall -> the main loop froze
        # mid-cycle (the post-reboot + 3x-today WS-hang). post() reads self.timeout per call, so setting
        # it on the live objects enforces it everywhere. 10s -> a stall raises, the cycle try/except logs
        # "skip cycle" and retries instead of hanging.
        self.info.timeout = 10
        self.exchange.timeout = 10
        logger.info(f"signer(agent)={self.agent_address[:10]} trading account(parent)={self.parent_address[:10]} (SDK http timeout=10s)")
        self._refresh_leader_meta()   # detect leader modes + true equity for unified/PM leaders (mode-aware sizing)

        # meta: sz_decimals + per-coin max leverage (retry on transient 429)
        self.sz_decimals: dict[str, int] = {}
        self.max_leverage: dict[str, float] = {}
        meta = None
        for attempt in range(6):
            try:
                meta = self.info.meta_and_asset_ctxs()
                break
            except Exception as e:
                wait = min(15 * (attempt + 1), 60)
                logger.warning(f"meta init attempt {attempt+1} failed ({str(e)[:80]}); waiting {wait}s")
                time.sleep(wait)
        if meta is None:
            raise RuntimeError("meta_and_asset_ctxs failed after retries -- daemon will re-throttle")
        if meta and len(meta) == 2:
            for u in meta[0]["universe"]:
                self.sz_decimals[u["name"]] = u.get("szDecimals", 2)
                self.max_leverage[u["name"]] = min(u.get("maxLeverage", 3), self.max_leverage_cap)
        for dex in BUILDER_DEXES:
            try:
                for u in self.info.meta(dex=dex).get("universe", []):
                    self.sz_decimals[u["name"]] = u.get("szDecimals", 2)
                    self.max_leverage[u["name"]] = min(u.get("maxLeverage", 3), self.max_leverage_cap)
            except Exception as e:
                logger.warning(f"builder meta {dex} failed: {e}")

        # caches / state
        self._leader_cache: dict[str, dict] = {}     # addr -> {"eq":, "pos":{coin:szi}, "ts":}
        self._marks: dict[str, tuple[float, float]] = {}   # coin -> (mark, ts)
        self._pos_peak: dict[str, float] = {}        # coin -> peak-favorable mark (per-position trailing stop)
        self._pos_peak_sign: dict[str, bool] = {}     # coin -> side the peak was tracked for (True=long)
        self._copy_blacklist: set[str] = set()        # coins stopped out; do NOT re-copy until leader exits/flips
        self._blacklist_sign: dict[str, float] = {}   # coin -> leaders' net sign at blacklist time (flip clears)
        self._dropped_leaders: set[str] = set()       # leaders dropped by the per-leader circuit breaker
        self._peak_av: float | None = self._load_peak()   # trailing-stop HIGH-WATER mark = peak of account
        #   value (spot USDC + uPnL, rule-16-clean), persisted across restarts. stop = peak*(1-pct), trails up.
        self._halted = False
        self.running = True
        # professional Telegram reporting (exchange-truth)
        self.report_interval_s = float(g.get("report_interval_s", 900))    # periodic status cadence (15 min)
        self._last_report = 0.0
        self._cycle_actions: list[str] = []          # per-cycle order summary (flushed to TG each cycle)
        # WS data feed (replaces REST polling of marks + own/leader clearinghouse in the hot path; REST kept
        # for spot USDC + the stop/flatten confirm + the startup parity gate). Started lazily in run().
        self.ws_max_age_s = float(g.get("ws_max_age_s", 15))
        self._ws = HLWSFeed([self.parent_address] + self.leaders)
        self._ws_parity_gen = -1                      # WS generation the parity gate last passed for (codex #4):
        #   a reconnect bumps the feed's generation -> parity must re-pass before trading resumes.
        logger.info(f"prop-copy init: {self.n} leaders, equal-split, stop -{self.global_stop_pct:.0%}, "
                    f"dry_run={dry_run}")

    # ── pricing / rounding ──────────────────────────────────────────────────
    def _round_size(self, coin: str, sz: float) -> float:
        return round(sz, self.sz_decimals.get(coin, 2))

    def _round_price(self, px: float) -> float:
        if px <= 0:
            return 0.0
        mag = math.floor(math.log10(abs(px)))
        return round(px, max(min(4 - mag, 5), 0))

    def _refresh_marks(self, force: bool = False):
        """Refresh marks via all_mids (main + each builder dex), one batched call per dex. HONORS the mark
        cache: skips the fetch entirely while the last successful refresh is younger than mark_max_age_s, so
        the reconcile loop stops calling all_mids every poll (the dominant 429 source). Each dex is isolated:
        one dex's 429 no longer aborts the others; throttled coins just go stale and _mark() returns None
        (fail-safe -- never a bad price)."""
        now = time.time()
        if not force and (now - getattr(self, "_marks_refreshed_at", 0.0)) < self.mark_max_age_s:
            return
        got_any = False
        for dex in [""] + BUILDER_DEXES:
            try:
                mids = self.info.all_mids(dex=dex) if dex else self.info.all_mids()
            except Exception as e:
                logger.warning(f"all_mids dex={dex or 'main'} failed: {e}")
                continue
            for coin, px in (mids or {}).items():
                try:
                    self._marks[coin] = (float(px), now)
                    got_any = True
                except (TypeError, ValueError):
                    continue
        if got_any:
            self._marks_refreshed_at = now

    def _mark(self, coin: str) -> float | None:
        """Mark from the WS allMids feed (main dex). None if missing/stale -> the caller drops that coin
        (can't size/price it); _flatten_all falls back to positionValue/szi when this is None."""
        return self._ws.get_mid(coin, self.mark_max_age_s)

    def _rest_mark(self, coin: str) -> float | None:
        """One-off REST mid -- FALLBACK so de-risking/exits are never blocked by a missing WS mid (codex #8).
        Tries main + each builder dex (a held coin could be on xyz/flx). Rare path."""
        for dex in [""] + BUILDER_DEXES:
            try:
                mids = self.info.all_mids(dex=dex) if dex else self.info.all_mids()
                px = (mids or {}).get(coin)
                if px:
                    return float(px)
            except Exception as e:
                logger.warning(f"rest_mark {coin} dex={dex or 'main'} failed: {e}")
        return None

    # ── exchange-truth queries ──────────────────────────────────────────────
    def _strict_ch_query(self, addr: str, dex: str) -> dict | None:
        """Single STRICT clearinghouseState fetch+validate (codex r3: shared by _clearinghouse AND
        _flatten_all so the global-stop path can't fail open). Returns the validated dict, or None on ANY
        malformed/error/partial response. Requires: dict, no 'error', marginSummary dict with BOTH
        accountValue and totalMarginUsed, list assetPositions, and every entry a dict 'position' with a
        'coin' on an allowed dex. Caller treats None as FAIL-CLOSED (not flat)."""
        # 429-RESILIENT fetch: HL public /info rate-limits hard (shared IP with the collectors). A SINGLE
        # transient 429 / null body used to return None -> fail-closed -> the whole reconcile cycle skipped,
        # which froze the copy. Retry with backoff so a transient throttle no longer bricks the cycle.
        payload = {"type": "clearinghouseState", "user": addr}
        if dex:
            payload["dex"] = dex
        d = None
        for attempt in range(4):
            try:
                r = requests.post(f"{HL_API}/info", json=payload, timeout=6)
                if r.status_code == 429:
                    time.sleep(0.4 * (2 ** attempt))   # 0.4, 0.8, 1.6, 3.2s
                    continue
                d = r.json()
            except Exception as e:
                logger.warning(f"ch query {addr[:10]} dex={dex} attempt {attempt+1} exception: {e}")
                time.sleep(0.4 * (2 ** attempt))
                continue
            if d is None:                              # HL transient null body -> retry
                time.sleep(0.4 * (2 ** attempt))
                continue
            break
        if not isinstance(d, dict) or "error" in d:
            return None
        ms = d.get("marginSummary")
        aps = d.get("assetPositions")
        if not isinstance(ms, dict) or not isinstance(aps, list):
            return None
        # codex r4: do ALL numeric validation HERE (the safety contract). accountValue + totalMarginUsed
        # must parse as float; EVERY position must have a coin (allowed dex) AND a parseable numeric szi.
        # Any failure -> None (fail closed). Otherwise a missing szi reads as 0 -> phantom-flat fail-open.
        try:
            float(ms["accountValue"])
            float(ms["totalMarginUsed"])
            for ap in aps:
                p = ap.get("position") if isinstance(ap, dict) else None
                if not isinstance(p, dict) or not p.get("coin") or "szi" not in p:
                    return None
                if _coin_dex(p["coin"]) not in ([""] + BUILDER_DEXES):
                    logger.error(f"position on UNSUPPORTED dex: {p['coin']} ({addr[:10]}) -- fail closed")
                    return None
                float(p["szi"])
        except (TypeError, ValueError, KeyError):
            return None
        return d

    def _clearinghouse(self, addr: str) -> tuple[float, dict[str, float], float, float, bool]:
        """(account_value, {coin: szi}, total_margin_used, total_uPnL, ok). Summed across main + builder dexes.
        account_value = total perp account value incl uPnL (marginSummary.accountValue) -- the RISK metric
        (distinct from the rule-16 spot-USDC sizing/reporting equity). codex #3/#8: FAIL CLOSED -- if ANY
        required dex query fails, ok=False so the caller SKIPS (never assume cur=0 -> never double-open).
        codex #8: a position on a dex outside the allowed set -> ok=False (hard-fail, never silently miss)."""
        av = 0.0
        used = 0.0
        upnl = 0.0
        pos: dict[str, float] = {}
        ok = True
        for dex in [""] + BUILDER_DEXES:
            d = self._strict_ch_query(addr, dex)
            if d is None:
                ok = False
                continue
            ms = d["marginSummary"]
            av += float(ms["accountValue"])
            used += float(ms["totalMarginUsed"])
            for ap in d["assetPositions"]:
                p = ap["position"]
                pos[p["coin"]] = pos.get(p["coin"], 0.0) + float(p.get("szi", 0) or 0)
                upnl += float(p.get("unrealizedPnl", 0) or 0)
        return av, pos, used, upnl, ok

    def _equity(self) -> float | None:
        """UNIFIED-ACCOUNT equity = SPOT USDC (rule 16, Alberto repeated 10000+ times). HL is a unified
        wallet: the spot USDC IS the perp collateral/margin -- there is NO separate per-account/per-dex
        capital to transfer. Perp per-dex `accountValue` is NOT the deployable capital (it double-counts /
        only shows the allocated slice); spot USDC is the single equity for sizing, margin, AND the stop."""
        # 2026-06-09 (Alberto: WS-failure concern): cache the spot-USDC equity. A transient REST failure/429
        # on this poll used to return None -> the cycle skipped ("spot equity missing"), even though our
        # equity barely moves cycle-to-cycle. Serve a recent cached value (<=120s) on failure so an idle
        # account does not spuriously fail-closed; only go None when we have no recent value at all.
        val = None
        try:
            d = requests.post(f"{HL_API}/info",
                              json={"type": "spotClearinghouseState", "user": self.parent_address},
                              timeout=6).json()
            if isinstance(d, dict) and "error" not in d and isinstance(d.get("balances"), list):
                val = 0.0
                for b in d["balances"]:
                    if b.get("coin") == "USDC":
                        val = float(b.get("total", 0) or 0)
                        break
        except Exception as e:
            logger.warning(f"spot USDC equity poll failed: {e}")
        if val is not None:
            self._spot_eq_cache = (val, time.time())
            return val
        cached = getattr(self, "_spot_eq_cache", None)
        if cached and (time.time() - cached[1]) <= 120.0:
            return cached[0]
        return None

    def _leader_state(self, addr: str) -> tuple[dict | None, bool]:
        """Cached (ttl) leader equity+positions. Returns (state, query_ok). DISTINGUISHES a QUERY FAILURE
        (query_ok=False -> caller fails the cycle closed) from a leader that is legitimately INACTIVE/FLAT
        (query ok but eq<=0 -> state with eq=0/pos={}, contributes nothing, does NOT block the others).
        Dry-run found this: one dead leader (E544 $0) was wrongly fail-closing the WHOLE strategy."""
        c = self._leader_cache.get(addr)
        if c and (time.time() - c["ts"]) <= self.leader_max_age_s:
            return c, True
        av, pos, _used, _upnl, ok = self._clearinghouse(addr)
        if not ok:
            return None, False                       # transient query failure -> fail closed (skip cycle)
        if av <= 0:
            return {"eq": 0.0, "pos": {}, "ts": time.time()}, True   # inactive leader -> contributes 0
        c = {"eq": av, "pos": pos, "ts": time.time()}
        self._leader_cache[addr] = c
        return c, True

    # ── target + reconcile ──────────────────────────────────────────────────
    def _refresh_leader_meta(self):
        """Detect each leader's HL account mode (userAbstraction) and, for unified/portfolioMargin leaders,
        fetch their TRUE total equity from the `portfolio` endpoint. For those modes the perp
        clearinghouseState.accountValue is a meaningless slice (HL docs verified 2026-06-05), so sizing + the
        per-leader breaker must use the portfolio total. default/dexAbstraction keep the WS perp av. Periodic
        (total equity is stable); 10s timeouts (no hang). Never zeroes a leader: failures keep the prior value."""
        for a in self.leaders:
            try:
                mode = requests.post(f"{HL_API}/info", json={"type": "userAbstraction", "user": a},
                                     timeout=10).json()
                if isinstance(mode, str):
                    self._leader_mode[a] = mode   # codex P0: keep prior mode on a bad response (no downgrade)
                if self._leader_mode[a] in ("unifiedAccount", "portfolioMargin"):
                    pf = requests.post(f"{HL_API}/info", json={"type": "portfolio", "user": a}, timeout=10).json()
                    av = None
                    for win, d in pf:
                        if win == "day":
                            h = d.get("accountValueHistory", [])
                            if h:
                                av = float(h[-1][1])
                    if av and av > 0:
                        self._leader_true_eq[a] = av
            except Exception as e:
                logger.warning(f"leader meta refresh {a[:10]} failed: {e}")
        self._leader_meta_ts = time.time()
        if self._leader_true_eq:
            logger.info("leader true-equity (unified/PM): "
                        + str({k[:8]: round(v) for k, v in self._leader_true_eq.items()}))

    def _leader_equity(self, al: str, agg: dict) -> float:
        """SIZING equity. unified/portfolioMargin -> portfolio TRUE total (perp av is a meaningless slice).
        FAIL-CLOSED (codex P0): if a unified/PM leader has NO fresh portfolio total (fetch failed, or stale >
        2x the refresh interval), return 0.0 -> the leader is SKIPPED this cycle, never sized off the tiny perp
        slice (which would massively over-size). default/dexAbstraction -> WS perp av (correct)."""
        mode = self._leader_mode.get(al)
        if mode in ("unifiedAccount", "portfolioMargin"):
            te = self._leader_true_eq.get(al)
            if not te or (time.time() - self._leader_meta_ts) > 2 * self._leader_meta_refresh_s:
                return 0.0   # fail-closed: no trustworthy true equity -> do not copy this unified leader
            return te
        if mode in ("default", "dexAbstraction"):
            return agg.get("av", 0.0)   # perp av IS the real equity for these modes
        return 0.0   # UNKNOWN/unclassified mode (detection failed/pending) -> FAIL-CLOSED (codex), never perp-slice

    def _net_target_szi(self, our_slice: float, leader_aggs: dict[str, dict]) -> dict[str, float]:
        """NET target signed szi per coin = sum over leaders of (leader_signed_notional/leader_eq) x slice
        / mark. Consumes the ATOMIC WS snapshot (leader_aggs = {addr: {"av","upnl","pos"}}); the caller has
        already verified ALL leaders are present+fresh in one generation (no-partial-state), so there is no
        per-leader fail path here. An inactive leader (av<=0) contributes 0 and does not block. A missing
        MARK for a coin only drops that coin's contribution (can't size it)."""
        tgt: dict[str, float] = {}
        for addr in self.leaders:
            al = addr.lower()
            agg = leader_aggs.get(al)
            if agg is None:
                continue
            # MODE-AWARE leader equity (2026-06-05): unified/portfolioMargin -> portfolio TRUE total (the perp
            # accountValue is a meaningless slice for those modes per HL docs); default/dexAbstraction -> perp av.
            # Using the perp slice for a unified leader would massively OVER-size (position/$300 instead of /$60k).
            eq = self._leader_equity(al, agg)
            if eq <= 0:
                continue                              # inactive leader OR unified-without-true-eq: skip (fail-closed)
            # PER-LEADER CIRCUIT BREAKER (Alberto): drop a leader while its OWN book is in the red; re-include
            # after it recovers (hysteresis). Denominator = PERP book av (codex P1): agg["upnl"] is perp-only,
            # so % it against the perp av, NOT the portfolio total (else a unified leader's breaker never fires).
            perp_eq = agg.get("av", 0.0)
            upnl_pct = (agg.get("upnl", 0.0) / perp_eq) if perp_eq > 0 else 0.0
            if al in self._dropped_leaders:
                if upnl_pct >= self.leader_reinclude_pct:
                    self._dropped_leaders.discard(al)
                    logger.info(f"leader {al[:10]} recovered (uPnL {upnl_pct:+.1%}) -> re-included in copy")
                else:
                    continue                          # still in the red -> do not copy this leader
            elif upnl_pct < self.leader_drop_pct:
                self._dropped_leaders.add(al)
                logger.error(f"LEADER DROP {al[:10]}: own book uPnL {upnl_pct:+.1%} < {self.leader_drop_pct:+.0%} "
                             f"-> stop copying until recovered")
                _tg(f"LEADER DROP {al[:6]}: book {upnl_pct:+.1%} -> stop copying (re-add at {self.leader_reinclude_pct:+.0%})")
                continue
            for coin, szi in agg["pos"].items():
                if abs(szi) < 1e-12:
                    continue
                mark = self._mark(coin)
                if mark is None:
                    continue
                exposure_pct = (szi * mark) / eq           # signed
                tgt[coin] = tgt.get(coin, 0.0) + exposure_pct * our_slice / mark
        return tgt

    def _add_margin(self, add_notional: float, coin: str) -> float:
        """Conservative margin reservation for an increase (cap assumed leverage at 10x)."""
        lev = min(self.max_leverage.get(coin, 3), 10.0)
        return add_notional / max(lev, 1.0)

    def _margin_ok_for_increase(self, our_eq: float, used_now: float, pending_cycle: float,
                                add_notional: float, coin: str) -> bool:
        """codex #1: util check must include BOTH the live exchange margin used AND margin already reserved
        by orders placed earlier THIS reconcile cycle (pending_cycle), else several increases each pass
        against the same starting margin and collectively breach the cap."""
        add_margin = self._add_margin(add_notional, coin)
        return (used_now + pending_cycle + add_margin) <= self.max_margin_util * our_eq

    def _place(self, coin: str, delta_szi: float, mark: float, reduce_only: bool) -> bool:
        """IOC order for the delta at an aggressive marketable price (no resting order). Idempotent across
        cycles because each cycle re-derives delta from exchange truth. Returns True if an order was placed
        (or would be, in dry-run); False if the size rounded to zero (un-actionable dust) -- callers use this
        to avoid an infinite close-then-never-open loop on sub-min dust."""
        sz = abs(self._round_size(coin, delta_szi))
        if sz <= 0:
            return False
        is_buy = delta_szi > 0
        side = "BUY" if is_buy else "SELL"
        tag = "close" if reduce_only else "open"
        px = self._round_price(mark * (1.003 if is_buy else 0.997))   # cross the spread for IOC fill
        if self.dry_run:
            logger.info(f"[DRY] {side} {coin} sz={sz} ~${sz*mark:.0f} reduce_only={reduce_only}")
            self._cycle_actions.append(f"{side} {coin} ${sz*mark:.0f} ({tag})")
            return True
        try:
            r = self.exchange.order(coin, is_buy, sz, px, {"limit": {"tif": "Ioc"}}, reduce_only=reduce_only)
            logger.info(f"ORDER {side} {coin} sz={sz} ~${sz*mark:.0f} reduce_only={reduce_only} -> {str(r)[:160]}")
            self._cycle_actions.append(f"{side} {coin} ${sz*mark:.0f} ({tag})")
        except Exception as e:
            logger.error(f"order failed {coin}: {e}")
            self._cycle_actions.append(f"FAIL {side} {coin}: {str(e)[:40]}")
        return True

    def _flatten_all(self) -> int:
        """Dedicated exchange-truth flatten. codex #5: do NOT use SDK market_close (it queries the agent
        account internally -> can no-op when positions are on the PARENT). Instead read the PARENT's actual
        positions and submit an explicit REDUCE-ONLY IOC for each, priced from the position itself
        (positionValue/szi) so it needs no live mark (codex #7). No give-up. Returns count still open."""
        n = 0
        for dex in [""] + BUILDER_DEXES:
            d = self._strict_ch_query(self.parent_address, dex)
            if d is None:
                # codex r3: malformed/error in the STOP path must NOT read as flat. Count as not-flat so
                # we keep retrying (and never log "HALTED: flat" while exposure is unknown).
                logger.error(f"FLATTEN query malformed dex={dex} -> assume NOT flat, retry next poll")
                n += 1
                continue
            for ap in d["assetPositions"]:
                p = ap["position"]
                coin = p.get("coin")
                szi = float(p.get("szi", 0) or 0)
                if not coin or abs(szi) < 1e-12:
                    continue
                n += 1
                pv = abs(float(p.get("positionValue", 0) or 0))
                px = (pv / abs(szi)) if (pv > 0 and abs(szi) > 0) else self._mark(coin)
                if not px or px <= 0:
                    logger.error(f"FLATTEN {coin}: no price -> retry next poll")
                    continue
                is_buy = szi < 0  # close: buy back a short, sell a long
                sz = abs(self._round_size(coin, szi))
                cross = self._round_price(px * (1.01 if is_buy else 0.99))  # aggressive IOC to ensure fill
                if self.dry_run:
                    logger.info(f"[DRY] FLATTEN {'BUY' if is_buy else 'SELL'} {coin} sz={sz} @~{cross}")
                    continue
                try:
                    self.exchange.order(coin, is_buy, sz, cross, {"limit": {"tif": "Ioc"}}, reduce_only=True)
                    logger.error(f"FLATTEN {'BUY' if is_buy else 'SELL'} {coin} sz={sz} reduce_only")
                except Exception as e:
                    logger.error(f"flatten {coin} failed: {e}")
        return n

    def _load_peak(self) -> float | None:
        """Load the persisted trailing-stop high-water mark (survives restarts). BOUND to this account
        (codex #6): a peak file written for a different parent address is ignored, so a stale/foreign file
        cannot spuriously halt us or weaken the stop. None if absent/foreign/unreadable."""
        try:
            if os.path.exists(STOP_PEAK_PATH):
                with open(STOP_PEAK_PATH) as f:
                    d = json.load(f)
                if (d.get("account") or "").lower() != self.parent_address.lower():
                    logger.warning("peak file account mismatch -> ignoring (fresh arm)")
                    return None
                v = float(d.get("peak", 0) or 0)
                return v if v > 0 else None
        except Exception as e:
            logger.warning(f"peak load failed: {e}")
        return None

    def _persist_peak(self, peak: float):
        """Persist the high-water mark (account-bound, ATOMIC tmp+replace) so a restart never resets the
        trailing-stop reference and a half-written file can't corrupt it (codex #6)."""
        try:
            os.makedirs(os.path.dirname(STOP_PEAK_PATH), exist_ok=True)
            tmp = STOP_PEAK_PATH + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"peak": round(float(peak), 4), "ts": int(time.time()),
                           "account": self.parent_address.lower()}, f)
            os.replace(tmp, STOP_PEAK_PATH)
        except Exception as e:
            logger.warning(f"peak persist failed: {e}")

    def _check_global_stop(self, acct_value: float, ok: bool) -> bool:
        """TRAILING -X% stop on ACCOUNT VALUE = spot USDC + uPnL (rule-16-clean; NOT the locked-margin perp
        accountValue). The high-water mark trails UP only and is persisted across restarts, so a restart can
        never reset the drawdown reference (the old session-baseline bug). Trip when acct_value falls -X%
        below the peak. Only evaluate on a GOOD read (ok and acct_value>0) -- never arm/trip on garbage."""
        if not ok or acct_value <= 0:
            return self._halted
        if self._peak_av is None or acct_value > self._peak_av:
            self._peak_av = acct_value
            self._persist_peak(acct_value)
        stop_level = self._peak_av * (1 - self.global_stop_pct)
        if acct_value <= stop_level:
            if not self._halted:
                logger.error(f"TRAILING STOP: acct ${acct_value:.2f} <= -{self.global_stop_pct:.0%} of peak "
                             f"${self._peak_av:.2f} (= ${stop_level:.2f}). FLATTEN + halt.")
                _tg(f"TRAILING STOP -{self.global_stop_pct:.0%}: acct ${acct_value:.2f} vs peak "
                    f"${self._peak_av:.2f} -- flattening + halt")
            self._halted = True
        return self._halted

    def _startup_parity_ok(self) -> bool:
        """Gate before the FIRST live order: confirm the WS-derived state matches REST exchange truth for the
        parent + EVERY leader (accountValue within tolerance + identical open-coin set). Guards against
        trading off a misparsed/partial WS feed. Transient coin-set drift (a leader fills between the two
        reads) just returns False -> retried next cycle (eventually consistent). codex/Alberto: verify, not trust."""
        try:
            for addr in [self.parent_address] + self.leaders:
                ws_agg, _gen, _conn = self._ws.user_aggregate(addr, self.ws_max_age_s)
                if ws_agg is None:
                    logger.warning(f"parity: WS state missing/stale for {addr[:10]} -> retry")
                    return False
                rest_av, rest_pos, _u, _up, ok = self._clearinghouse(addr)
                if not ok:
                    logger.warning(f"parity: REST read failed for {addr[:10]} -> retry")
                    return False
                # MODE-AWARE av check (2026-06-05): for unified/portfolioMargin LEADERS the perp accountValue
                # is a meaningless, volatile slice (HL docs) -> WS vs REST legitimately disagree (this is what
                # blocked 0x7186d081). Skip the av tolerance for those leaders; the POSITION/coin-set check
                # below is what matters for copying them. Parent + default/dexAbstraction keep the av check.
                al = addr.lower()
                unified_leader = (al != self.parent_address.lower()
                                  and self._leader_mode.get(al) in ("unifiedAccount", "portfolioMargin"))
                if not unified_leader and abs(ws_agg["av"] - rest_av) > max(5.0, 0.02 * max(rest_av, 1.0)):
                    logger.error(f"parity FAIL {addr[:10]}: WS av=${ws_agg['av']:.2f} vs REST av=${rest_av:.2f}")
                    _tg(f"WS PARITY FAIL {addr[:10]}: WS ${ws_agg['av']:.0f} vs REST ${rest_av:.0f} -- trading blocked")
                    return False
                ws_coins = {c for c, s in ws_agg["pos"].items() if abs(s) > 1e-9}
                rest_coins = {c for c, s in rest_pos.items() if abs(s) > 1e-9}
                if ws_coins != rest_coins:
                    logger.warning(f"parity coin-set mismatch {addr[:10]}: WS={ws_coins} REST={rest_coins} -> retry")
                    return False
            logger.info("WS-vs-REST parity OK (parent + all leaders)")
            return True
        except Exception as e:
            logger.warning(f"parity check exception: {e}")
            return False

    def _apply_pos_trailing_stops(self, target: dict, our_pos: dict, leader_aggs: dict) -> dict:
        """PER-POSITION trailing stop (Alberto 2026-06-02): trail each held position's peak-favorable price;
        if it retraces pos_trail_pct from that peak, CLOSE it individually (target->0) instead of the global
        account stop nuking the whole book. COPY-ENGINE GUARD: a stopped coin is BLACKLISTED from re-copying
        until the leaders fully EXIT it (else the reconciler re-opens it next cycle); a fresh leader re-entry
        after that is copied again. The -15% ACCOUNT stop remains as the final backstop."""
        # leaders' net (signed) AND max-held (abs) exposure per coin. lead_held = does ANY leader still hold
        # this coin (codex fix: clear blacklist only when NONE do, not when raw nets offset to ~0).
        lead_net: dict[str, float] = {}
        lead_held: dict[str, float] = {}
        for a in self.leaders:
            ag = leader_aggs.get(a.lower())
            if ag:
                for c, s in ag["pos"].items():
                    lead_net[c] = lead_net.get(c, 0.0) + s
                    lead_held[c] = max(lead_held.get(c, 0.0), abs(s))
        for c in list(self._copy_blacklist):
            net_sign = (1.0 if lead_net.get(c, 0.0) > 0 else (-1.0 if lead_net.get(c, 0.0) < 0 else 0.0))
            exited = lead_held.get(c, 0.0) < 1e-9                       # NO leader holds it anymore
            _rec = self._blacklist_sign.get(c, 0.0)                     # leaders' net sign recorded at stop time
            # a true side FLIP needs BOTH the recorded and current net signs nonzero AND opposite. If the
            # recorded sign was 0 (offsetting leaders at stop time), a later nonzero net is NOT a flip ->
            # only a full leader exit clears it (codex re-review: prevents premature reopen).
            flipped = _rec != 0.0 and net_sign != 0.0 and net_sign != _rec
            if exited or flipped:                                       # codex fix: clear on full-exit OR flip
                self._copy_blacklist.discard(c)
                self._blacklist_sign.pop(c, None)
                self._pos_peak.pop(c, None)
                self._pos_peak_sign.pop(c, None)
        # update peak-favorable + detect trailing-stop triggers on OUR held positions
        for c, szi in list(our_pos.items()):
            if abs(szi) < 1e-12:
                continue
            mk = self._mark(c)
            if mk is None or mk <= 0:
                continue
            is_long = szi > 0
            pk = self._pos_peak.get(c)
            if pk is None or self._pos_peak_sign.get(c) != is_long:
                pk = mk                                 # init at current mark on first sight / after a flip
            pk = max(pk, mk) if is_long else min(pk, mk)
            self._pos_peak[c] = pk
            self._pos_peak_sign[c] = is_long
            tripped = (mk <= pk * (1 - self.pos_trail_pct)) if is_long else (mk >= pk * (1 + self.pos_trail_pct))
            if tripped and c not in self._copy_blacklist:
                logger.error(f"POS TRAILING STOP {c}: mark {mk:.6g} retraced >={self.pos_trail_pct:.0%} from "
                             f"peak {pk:.6g} ({'L' if is_long else 'S'}) -> close + blacklist until leader exits")
                _tg(f"POS STOP {c} {'L' if is_long else 'S'}: -{self.pos_trail_pct:.0%} from peak {pk:.6g} -> closing")
                self._copy_blacklist.add(c)
                ln = lead_net.get(c, 0.0)
                self._blacklist_sign[c] = 1.0 if ln > 0 else (-1.0 if ln < 0 else 0.0)   # clears on a later flip
        # blacklisted coins -> force target to 0 (close now, do NOT re-open until leader exits)
        for c in self._copy_blacklist:
            target[c] = 0.0
        # drop stale peaks for coins we no longer hold and aren't blacklisted
        for c in list(self._pos_peak):
            if abs(our_pos.get(c, 0.0)) < 1e-12 and c not in self._copy_blacklist:
                self._pos_peak.pop(c, None)
                self._pos_peak_sign.pop(c, None)
        return target

    def reconcile(self):
        # UNIFIED ACCOUNT (rule 16): equity = SPOT USDC (REST, not a WS sub) = sizing + margin base + stop
        # ref. POSITIONS + marks + LEADER states come from the WS feed (one persistent connection, no 429
        # polling). REST clearinghouse is kept only for the flatten/stop confirm + the startup parity gate.
        spot_eq = self._equity()
        eq_ok = spot_eq is not None and spot_eq > 0
        # STOP first, on OWN state ONLY (codex #2): the trailing stop must protect existing exposure even if a
        # LEADER feed is stale. Own uPnL from the WS feed, but FALL BACK to a REST clearinghouse read when own
        # WS is stale/disconnected (codex re-review blocker): the stop must NEVER fail open just because OUR
        # feed is stale. spot USDC via REST. acct_value is None only if BOTH WS and REST own reads fail.
        own_agg, _og, _oc = self._ws.user_aggregate(self.parent_address, self.ws_max_age_s)
        if own_agg is not None:
            our_upnl, own_ok = own_agg["upnl"], True
        else:
            _av, _pos, _mu, rest_upnl, rest_ok = self._clearinghouse(self.parent_address)  # 429-resilient
            our_upnl, own_ok = (rest_upnl if rest_ok else 0.0), rest_ok
        acct_value = (spot_eq + our_upnl) if (eq_ok and own_ok) else None
        if self._halted or (acct_value is not None and self._check_global_stop(acct_value, True)):
            remaining = self._flatten_all()
            if remaining == 0:
                logger.info("HALTED: flat. (manual re-arm to resume)")
            return
        # ATOMIC WS snapshot of own + ALL leaders (no-partial-state gate) for TRADING. None -> fail closed.
        # parent STRICT (stop safety), idle LEADERS feed-liveness (no false-close when quiet) -- 2026-06-05 WS fix
        snap, ws_gen, ws_conn = self._ws.snapshot_all([self.parent_address] + self.leaders, self.ws_max_age_s,
                                                      strict_addrs=frozenset({self.parent_address.lower()}))
        ws_ok = snap is not None
        own = snap[self.parent_address.lower()] if ws_ok else None
        our_pos = own["pos"] if own else {}
        used_now = own["mu"] if own else 0.0
        # fail CLOSED -- stale/incomplete WS state or missing spot equity must NOT proceed to sizing.
        if not ws_ok or not eq_ok:
            logger.warning(f"WS state stale/incomplete (conn={ws_conn} gen={ws_gen}) or spot equity missing "
                           f"-> skip cycle (fail closed)")
            return
        if self._peak_av is None:
            logger.warning("trailing-stop peak not armed yet -> skip trading this cycle")
            return
        # PARITY GATE per WS GENERATION (codex #4): re-confirm WS-vs-REST after EVERY (re)connect, not just
        # once -- a reconnect repopulates caches and must not resume trading on unverified state.
        if self._ws_parity_gen != ws_gen:
            if not self._startup_parity_ok():
                logger.warning(f"WS-vs-REST parity not confirmed for gen {ws_gen} -> skip trading this cycle")
                return
            self._ws_parity_gen = ws_gen
            logger.info(f"WS-vs-REST parity confirmed for gen {ws_gen} -> live trading enabled")
        # leader-meta refresh AFTER the stop+parity gates (codex P1: never delay the stop with these REST calls).
        if time.time() - self._leader_meta_ts > self._leader_meta_refresh_s:
            self._refresh_leader_meta()   # unified-leader true equity (stable; every ~5min)
        our_eq = spot_eq * self.sizing_leverage   # rule 16 spot-USDC base, scaled by sizing leverage (Alberto)
        our_slice = our_eq / self.n
        leader_aggs = {a.lower(): snap[a.lower()] for a in self.leaders}
        target = self._net_target_szi(our_slice, leader_aggs)
        target = self._apply_pos_trailing_stops(target, our_pos, leader_aggs)  # per-position trailing stop
        coins = set(target) | {c for c, s in our_pos.items() if abs(s) > 1e-12}
        pending_cycle = 0.0   # codex #1: margin reserved by orders placed earlier THIS cycle
        for coin in sorted(coins):
            tgt = target.get(coin, 0.0)
            cur = our_pos.get(coin, 0.0)
            # DE-RISKING must work even if the WS mid is momentarily missing -> REST mark fallback when we
            # hold a real position (codex #8). For a fresh OPEN a missing mark just defers it (no fallback).
            mark = self._mark(coin)
            if mark is None:
                mark = self._rest_mark(coin) if abs(cur) > 1e-12 else None
            if mark is None:
                continue
            delta = tgt - cur
            if abs(delta * mark) < self.min_notional:   # dust delta in EITHER direction -> skip
                continue
            cur_dust = abs(cur) * mark < self.min_notional
            # OPPOSITE SIDE: close the existing exposure reduce-only FIRST; NEVER cross zero in one
            # non-reduce-only order (codex #3 -- preserves the flip-closes-first safety + avoids reduce+open
            # churn). If `cur` is un-closeable sub-min dust the close no-ops -> fall through and open the
            # target (treat the residual dust as ~0). _place returns False only when size rounds to 0.
            if abs(tgt) > 1e-12 and abs(cur) > 1e-12 and (tgt > 0) != (cur > 0):
                if self._place(coin, -cur, mark, reduce_only=True):
                    continue
                cur, delta, cur_dust = 0.0, tgt, True
            # REDUCING (same-side shrink or full exit) of a REAL position: always de-risk (bypass band+margin).
            reducing = (not cur_dust) and (tgt == 0.0 or (abs(tgt) < abs(cur) and (tgt > 0) == (cur > 0)))
            if reducing:
                self._place(coin, delta, mark, reduce_only=True)
                continue
            # INCREASE (fresh open or same-side add). The rebalance band damps CHURN on an EXISTING position
            # only; it must NOT block the initial OPEN of a small-but-valid (>= min_notional) target. The HYPE
            # bug: base=max(tgt_notional, our_slice) made a small-allocation copy (~$21 vs a ~$118 slice) fall
            # under rebalance_frac*slice -> silently skipped -> small-allocation leader coins were never mirrored.
            base = max(abs(tgt) * mark, our_slice)
            if not cur_dust and abs(delta * mark) < self.rebalance_frac * base:
                continue
            if not self._margin_ok_for_increase(our_eq, used_now, pending_cycle, abs(delta) * mark, coin):
                logger.info(f"SKIP {coin}: margin-util cap would be breached")
                continue
            pending_cycle += self._add_margin(abs(delta) * mark, coin)
            self._place(coin, delta, mark, reduce_only=False)
        # flush this cycle's order actions to Telegram (concise, only when we actually traded).
        if self._cycle_actions:
            _tg("Trades: " + "; ".join(self._cycle_actions[:20])
                + (f" (+{len(self._cycle_actions)-20} more)" if len(self._cycle_actions) > 20 else ""))
            self._cycle_actions = []

    def _report(self):
        """Professional EXCHANGE-TRUTH status to Telegram: equity, session PnL, stop distance, every open
        position with notional + uPnL, gross/util, leaders active. Read-only; safe to call any time."""
        spot = self._equity()
        rich = []          # (coin, szi, notional, upnl)
        tot_upnl = 0.0
        gross = 0.0
        margin_used = 0.0
        read_ok = True
        for dex in [""] + BUILDER_DEXES:
            d = self._strict_ch_query(self.parent_address, dex)
            if d is None:
                read_ok = False
                continue
            margin_used += float(d["marginSummary"]["totalMarginUsed"])
            for ap in d["assetPositions"]:
                p = ap["position"]
                szi = float(p.get("szi", 0) or 0)
                if abs(szi) < 1e-12:
                    continue
                pv = abs(float(p.get("positionValue", 0) or 0))
                up = float(p.get("unrealizedPnl", 0) or 0)
                rich.append((p["coin"], szi, pv, up))
                tot_upnl += up
                gross += pv
        n_active = sum(1 for a in self.leaders
                       if (self._ws.user_aggregate(a, self.ws_max_age_s, strict=False)[0] or {}).get("av", 0) > 0)
        lines = ["V15 copy-trader" + (" [DRY]" if self.dry_run else "")]
        if spot is not None:
            total_value = spot + tot_upnl                 # account value (spot USDC + uPnL), rule-16-clean
            curve = self._update_equity_curve(total_value)   # persisted across restarts
            vals = [c[1] for c in curve]
            # peak = the persisted TRAILING-stop high-water mark (falls back to curve/current until armed).
            peak = self._peak_av or (max(vals) if vals else total_value)
            stop_at = peak * (1 - self.global_stop_pct)
            cur_dd = (peak - total_value) / peak * 100 if peak else 0.0
            base = vals[0] if vals else total_value          # first curve sample = session-start PnL ref
            pnl = total_value - base
            pnl_pct = pnl / base * 100 if base else 0.0
            lines.append(f"Equity ${total_value:,.2f} (spot ${spot:,.2f} +uPnL ${tot_upnl:+,.2f})")
            lines.append(f"PnL ${pnl:+,.2f} ({pnl_pct:+.1f}%) | peak ${peak:,.2f} | DD {cur_dd:.1f}%")
            if len(vals) >= 2:
                lines.append(f"Curve {_sparkline(vals[-40:])} (${vals[0]:,.0f}->${vals[-1]:,.0f}, {len(vals)} pts)")
            lines.append(f"Stop -{self.global_stop_pct:.0%} @ ${stop_at:,.2f} (trailing from peak)"
                         + (" | HALTED" if self._halted else ""))
            lines.append(f"Open {len(rich)} | gross ${gross:,.0f} ({gross/spot:.1f}x) | "
                         f"margin ${margin_used:,.0f} ({margin_used/spot*100:.0f}%)")
            # Persist the copy-trading status the PNG report (pnl_tracker) cannot compute itself, so the
            # single consolidated report keeps PnL%, peak/DD, stop distance and leaders. Alberto 2026-06-02.
            # PER-LEADER breakdown (Alberto 2026-06-02): each leader's live book + THEIR uPnL, so the report
            # shows which leaders are winning/losing and what we copy from them. From the WS feed (no REST).
            # CONTRIBUTION TO OUR BOOK (Alberto 2026-06-02): each leader's copied exposure in OUR book =
            # our_slice x (their signed position / their equity); summed |notional| = our_slice x their
            # leverage. Attributed uPnL = our per-coin uPnL split by each leader's signed share of the net
            # target on that coin. our_slice uses spot (rule 16); marks from the WS feed.
            our_slice = spot / self.n if (spot and self.n) else 0.0
            our_up = {coin: up for coin, szi, pv, up in rich}          # OUR per-coin uPnL (exchange truth)
            _laggs = {a: self._ws.user_aggregate(a, self.ws_max_age_s, strict=False)[0] for a in self.leaders}
            # net target szi per coin = sum over leaders of (szi/av)*our_slice  (signed)
            net_tgt = {}
            for _ag in _laggs.values():
                if _ag and _ag["av"] > 0:
                    for c, s in _ag["pos"].items():
                        net_tgt[c] = net_tgt.get(c, 0.0) + s * our_slice / _ag["av"]
            leaders_rep = []
            for _a in self.leaders:
                _agg = _laggs.get(_a)
                if _agg is None:
                    leaders_rep.append({"addr": _a[:6], "ok": False})
                    continue
                _coins = sorted([c for c, s in _agg["pos"].items() if abs(s) > 1e-9])
                contrib_notional = 0.0
                contrib_upnl = 0.0
                if _agg["av"] > 0:
                    for c, s in _agg["pos"].items():
                        mk = self._mark(c)
                        if mk is None:
                            continue
                        cs = s * our_slice / _agg["av"]               # our copied szi from THIS leader on c
                        contrib_notional += abs(cs * mk)
                        nt = net_tgt.get(c, 0.0)
                        if abs(nt) > 1e-12 and c in our_up:
                            contrib_upnl += our_up[c] * (cs / nt)     # our uPnL on c, this leader's share
                leaders_rep.append({
                    "addr": _a[:6], "ok": True, "av": round(_agg["av"], 0), "upnl": round(_agg["upnl"], 0),
                    # uPnL as % of COST-BASIS equity (av - upnl), NOT current equity. Dividing by current
                    # equity explodes toward -infinity as a losing book nears liquidation (the misleading
                    # "-800%"). Cost-basis equity is the capital actually at risk; bounded ~[-100%, +inf).
                    "upnl_pct": round(_agg["upnl"] / (_agg["av"] - _agg["upnl"]) * 100, 1) if (_agg["av"] - _agg["upnl"]) > 0 else 0.0,
                    "coins": _coins,
                    "our_notional": round(contrib_notional, 0),       # $ of OUR book from this leader
                    "our_upnl": round(contrib_upnl, 2),               # OUR uPnL attributed to this leader
                })
            try:
                status_path = os.path.join(os.path.dirname(EQUITY_CURVE_PATH), "v15_live_status.json")
                with open(status_path, "w") as _sf:
                    json.dump({
                        "ts": int(time.time()),
                        "spot": round(spot, 2), "total_value": round(total_value, 2),
                        "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                        "peak": round(peak, 2), "dd_pct": round(cur_dd, 2),
                        "stop_at": round(stop_at, 2), "stop_pct": self.global_stop_pct,
                        "n_active": n_active, "n_leaders": self.n, "halted": bool(self._halted),
                        "leaders": leaders_rep,
                    }, _sf)
            except Exception as _se:
                logger.warning(f"status json write failed: {_se}")
        lines.append(f"Leaders active {n_active}/{self.n}" + ("" if read_ok else " | READ INCOMPLETE"))
        for coin, szi, pv, up in sorted(rich, key=lambda x: -x[2])[:15]:
            lines.append(f"  {'L' if szi>0 else 'S'} {coin} ${pv:,.0f} uPnL ${up:+,.1f}")
        # Alberto 2026-06-02: SINGLE report only. The pnl-tracker PNG (spawned below) is the sole post -- it
        # now folds in this engine's status (PnL%/peak/DD/stop/leaders via v15_live_status.json) and has the
        # corrected margin. This engine's own text post was the duplicate (and its "curve" was ASCII, not the
        # real PNG), so it is disabled. Equity-curve + status json are still persisted above. Do NOT re-enable
        # this _tg without removing the pnl-tracker post, or the two reports overlap again.
        # _tg("\n".join(lines))
        # The engine drives the ONE report -- fire the pnl-tracker ONE-SHOT (syncs fills + posts the PNG
        # equity curve + status). The standalone pnl-tracker loop is disabled so there is no second post.
        try:
            import subprocess
            repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            subprocess.Popen([sys.executable, "tools/pnl_tracker.py", "--tg", "--epoch"],
                             cwd=repo, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            logger.warning(f"pnl-tracker chart trigger failed: {e}")

    def _update_equity_curve(self, total_value: float) -> list:
        """Append a (ts, total_value) sample to a persisted curve (survives daemon restarts), cap to 500,
        return the list. Best-effort -- never raises into the report."""
        try:
            curve = []
            if os.path.exists(EQUITY_CURVE_PATH):
                with open(EQUITY_CURVE_PATH) as f:
                    curve = json.load(f)
            curve.append([int(time.time()), round(float(total_value), 4)])
            curve = curve[-500:]
            os.makedirs(os.path.dirname(EQUITY_CURVE_PATH), exist_ok=True)
            tmp = EQUITY_CURVE_PATH + ".tmp"
            with open(tmp, "w") as f:
                json.dump(curve, f)
            os.replace(tmp, EQUITY_CURVE_PATH)
            return curve
        except Exception as e:
            logger.warning(f"equity curve update failed: {e}")
            return [[int(time.time()), round(float(total_value), 4)]]

    def _watchdog(self, stall_s: float = 90.0):
        """Outer safety net (codex 2026-06-05): the SDK http timeout bounds single calls, but total cycle
        time can still stretch (retries/backoff, many coins/dexes, future code). If the main loop hasn't
        completed a cycle in `stall_s`, force-exit so launchd KeepAlive respawns a fresh process. Converts
        any hang into a ~30s auto-restart instead of a multi-minute freeze holding an unmanaged position."""
        import os as _os
        while self.running:
            time.sleep(15)
            age = time.time() - self._last_cycle_ts
            if age > stall_s:
                logger.error(f"WATCHDOG: main loop stalled {age:.0f}s > {stall_s:.0f}s -> force-exit for launchd restart")
                try: _tg(f"WATCHDOG: copy loop stalled {age:.0f}s -> auto-restarting")
                except Exception: pass
                _os._exit(1)

    def run(self):
        _tg(f"V15 prop-copy starting: {self.n} leaders, equal-split, stop -{self.global_stop_pct:.0%}, "
            f"dry_run={self.dry_run}")
        self._last_cycle_ts = time.time()             # watchdog heartbeat (updated each completed cycle)
        threading.Thread(target=self._watchdog, name="copy-watchdog", daemon=True).start()
        self._ws.start()                              # spin up the WS feed thread; caches populate in ~1-2s
        time.sleep(3)                                 # let subscriptions populate before the first reconcile
        while self.running:
            try:
                self.reconcile()
                if time.time() - self._last_report >= self.report_interval_s:
                    self._report()
                    self._last_report = time.time()
            except Exception as e:
                logger.error(f"reconcile error: {e}")
            self._last_cycle_ts = time.time()         # heartbeat: a full cycle completed
            time.sleep(self.poll_s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/copy_trader_wallets_v15_prop.json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    PropCopy(args.config, dry_run=args.dry_run).run()


if __name__ == "__main__":
    main()
