"""
hl_ws_feed.py -- Hyperliquid WebSocket data feed for the V15 prop-copy engine.

WHY: REST /info polling (all_mids x3 every 8s + own/leader clearinghouse, shared IP w/ 2 collectors)
caused HL 429 storms -> fail-closed every cycle -> the copy engine FROZE (2026-06-02). This feed moves the
hot-path data to ONE persistent WS connection: zero per-call rate limit, real-time leader mirroring.

DESIGN (learn from V11, improve on it -- Alberto 2026-06-02):
- PORT from V11: raw `websockets` client (bypass the SDK ws-manager, which has no on_close/reconnect),
  reconnect-on-any-exception loop, CLEAR caches on (re)connect, ping keepalive.
- IMPROVE on V11: isolated module + IMMUTABLE snapshots (not a 3300-line entangled file); connection
  GENERATION counter so the reconciler can refuse to trade on partial/mixed post-reconnect state; per-entry
  freshness timestamps (not one global last_sync); exponential backoff.

EMPIRICALLY VERIFIED payloads (2026-06-02, brain: projects/quant/v15/ws-empirical-findings):
- allDexsClearinghouseState{user} -> data={user, clearinghouseStates:[[dex, chState], ...]} -- ALL perp
  dexes for a user in ONE message. chState has the REST clearinghouseState shape.
- allMids -> data={mids:{coin: px}} (main perp dex + spot "#NNNN"). Per-dex demux is BROKEN on one socket,
  so this feed uses the MAIN allMids only (our mirror coins are all main-dex). Builder-dex marks would need
  a separate connection -- add only when a builder-dex target appears.
- Spot USDC is NOT a WS sub -> stays on REST in the engine (rule-16-clean), not here.

This module owns NO trading logic. It only maintains read-only snapshots. The engine reads them behind a
freshness+generation gate and keeps REST as the authoritative confirm for the stop/flatten + a parity gate.
"""
import asyncio
import json
import logging
import threading
import time

import websockets

HL_WS = "wss://api.hyperliquid.xyz/ws"
logger = logging.getLogger("hl_ws_feed")


class HLWSFeed:
    """Background-thread HL WebSocket feed. Thread-safe read API returns immutable snapshots.

    Caches (guarded by self._lock):
      _user_state[addr] = {"states": {dex: {"av","mu","upnl","pos":{coin:szi}}}, "ts": float}
      _mids[coin]       = (px, ts)
      _generation       : incremented on every successful (re)connect AFTER caches are cleared. The engine
                          records the generation it first saw all users fresh in; if generation changes
                          (a reconnect happened), it must re-establish freshness before trading.
    """

    def __init__(self, users: list[str], mark_dexes: list[str] | None = None):
        self.users = [u.lower() for u in users]
        # builder dexes whose marks we ALSO need (copy_a copies xyz coins whose marks are NOT in the
        # main allMids). Each gets its OWN dedicated WS connection subscribing allMids?dex=<name> --
        # per-dex demux on ONE socket is broken (unlabeled allMids messages), so one socket per dex.
        self.mark_dexes = [d for d in (mark_dexes if mark_dexes is not None else ["xyz"]) if d]
        self._lock = threading.Lock()
        self._user_state: dict[str, dict] = {}
        self._mids: dict[str, tuple[float, float]] = {}
        # builder-dex marks live in a SEPARATE cache so they never collide with same-ticker main coins.
        # get_mid falls back to this only if the coin is absent from the main mids.
        self._dex_mids: dict[str, tuple[float, float]] = {}
        self._generation = 0
        self._connected = False
        self._last_msg_ts = 0.0
        self._stop = False
        self._thread: threading.Thread | None = None
        self._mark_threads: list[threading.Thread] = []

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self):
        self._thread = threading.Thread(target=self._run_forever, name="hl-ws-feed", daemon=True)
        self._thread.start()
        # one dedicated mark connection per builder dex (xyz) -- keeps xyz marks fresh for the disaster
        # stop + order crossing without a REST-poll 429 storm (HARD SAFETY requirement 5).
        for dex in self.mark_dexes:
            t = threading.Thread(target=self._run_mark_dex, args=(dex,),
                                 name=f"hl-ws-mark-{dex}", daemon=True)
            t.start()
            self._mark_threads.append(t)

    def stop(self):
        self._stop = True

    def _run_forever(self):
        try:
            asyncio.run(self._loop())
        except Exception as e:  # asyncio.run failure -> thread dies; engine staleness gate fails closed
            logger.error(f"WS feed thread died: {e}")
            with self._lock:
                self._connected = False

    def _run_mark_dex(self, dex: str):
        try:
            asyncio.run(self._loop_mark_dex(dex))
        except Exception as e:  # thread died -> per-entry freshness on _dex_mids fails closed (marks age out)
            logger.error(f"WS mark feed thread for dex={dex} died: {e}")

    async def _loop_mark_dex(self, dex: str):
        """Dedicated connection for ONE builder dex's allMids. Its messages are unambiguously this dex
        (own socket), so we store them in _dex_mids keyed by coin. On any disconnect we drop THIS dex's
        marks so a stale builder mark can never be served (get_mid then returns None -> fail-closed)."""
        backoff = 1.0
        while not self._stop:
            try:
                async with websockets.connect(HL_WS, ping_interval=20, ping_timeout=10,
                                               close_timeout=5, max_queue=512) as ws:
                    await ws.send(json.dumps({"method": "subscribe",
                                              "subscription": {"type": "allMids", "dex": dex}}))
                    logger.info(f"WS mark feed connected for dex={dex}")
                    backoff = 1.0
                    while not self._stop:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue
                        try:
                            msg = json.loads(raw)
                        except (TypeError, ValueError):
                            continue
                        if msg.get("channel") != "allMids":
                            continue
                        data = msg.get("data") or {}
                        mids = data.get("mids") if isinstance(data, dict) else None
                        if not isinstance(mids, dict):
                            continue
                        now = time.time()
                        with self._lock:
                            for coin, px in mids.items():
                                try:
                                    self._dex_mids[coin] = (float(px), now)
                                except (TypeError, ValueError):
                                    continue
            except Exception as e:
                logger.warning(f"WS mark feed error dex={dex}: {e}; reconnecting in {backoff:.0f}s")
            # drop THIS dex's marks on disconnect (best-effort: coins are namespaced by builder dex, so a
            # main-dex coin of the same ticker is unaffected in _mids).
            with self._lock:
                self._dex_mids = {}
            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _loop(self):
        backoff = 1.0
        while not self._stop:
            try:
                async with websockets.connect(HL_WS, ping_interval=20, ping_timeout=10,
                                               close_timeout=5, max_queue=512) as ws:
                    # ON (RE)CONNECT: clear caches first (never trade on pre-disconnect data), bump generation.
                    with self._lock:
                        self._user_state = {}
                        self._mids = {}
                        self._generation += 1
                        self._connected = True
                        gen = self._generation
                    logger.info(f"WS connected (generation {gen}); subscribing {len(self.users)} users + allMids")
                    for u in self.users:
                        await ws.send(json.dumps({"method": "subscribe",
                                                  "subscription": {"type": "allDexsClearinghouseState", "user": u}}))
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
                    backoff = 1.0
                    while not self._stop:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            # No message for 30s. ping_interval keeps the socket alive; the engine's
                            # staleness gate will block trading if data goes stale. Keep listening.
                            continue
                        self._handle(raw)
            except Exception as e:
                logger.warning(f"WS error: {e}; reconnecting in {backoff:.0f}s")
            # On ANY disconnect/exception: invalidate caches so the engine fails closed until re-populated.
            with self._lock:
                self._connected = False
                self._user_state = {}
                self._mids = {}
            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    # ── message handling ─────────────────────────────────────────────────────
    def _handle(self, raw: str):
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            return
        ch = msg.get("channel")
        if ch == "error":
            logger.warning(f"WS error message: {str(msg.get('data'))[:160]}")
            return
        if ch in ("subscriptionResponse", "pong"):
            return
        now = time.time()
        data = msg.get("data") or {}
        if ch == "allMids":
            mids = data.get("mids") if isinstance(data, dict) else None
            if not isinstance(mids, dict):
                return
            with self._lock:
                for coin, px in mids.items():
                    try:
                        self._mids[coin] = (float(px), now)
                    except (TypeError, ValueError):
                        continue
                self._last_msg_ts = now
        elif ch == "allDexsClearinghouseState":
            if not isinstance(data, dict):
                return
            addr = (data.get("user") or "").lower()
            parsed = self._parse_states(data.get("clearinghouseStates"))
            if addr:
                with self._lock:
                    if parsed is not None:
                        self._user_state[addr] = {"states": parsed, "ts": now}
                        self._last_msg_ts = now
                    else:
                        # MALFORMED update for a subscribed user -> INVALIDATE its cache (codex 2026-06-05 #1):
                        # under the relaxed leader feed-liveness gate, a stale cached state must NOT survive a
                        # bad update. Dropping it -> the gate returns None (fail-closed) until a clean update.
                        self._user_state.pop(addr, None)

    @staticmethod
    def _parse_states(states) -> dict | None:
        """[[dex, chState], ...] -> {dex: {av, mu, upnl, pos{coin:szi}}}. STRICT: any malformed entry -> None
        (the engine treats None/stale as fail-closed; never assume flat). Mirrors the REST _strict_ch_query
        validation contract."""
        if not isinstance(states, list):
            return None
        out: dict[str, dict] = {}
        try:
            for entry in states:
                dex = entry[0]
                ch = entry[1]
                if not isinstance(ch, dict):
                    return None
                ms = ch["marginSummary"]
                av = float(ms["accountValue"])
                mu = float(ms["totalMarginUsed"])
                aps = ch.get("assetPositions")
                if not isinstance(aps, list):     # FAIL-CLOSED: missing/non-list positions != flat (codex #1)
                    return None
                pos: dict[str, float] = {}
                upnl = 0.0
                for ap in aps:
                    p = ap["position"]
                    coin = p["coin"]
                    if not coin:
                        return None
                    pos[coin] = pos.get(coin, 0.0) + float(p["szi"])  # require szi present (no .get default)
                    upnl += float(p.get("unrealizedPnl", 0) or 0)
                out[str(dex)] = {"av": av, "mu": mu, "upnl": upnl, "pos": pos}
        except (KeyError, TypeError, ValueError, IndexError):
            return None
        return out

    # ── read API (thread-safe) ───────────────────────────────────────────────
    def user_aggregate(self, addr: str, max_age_s: float, strict: bool = True,
                       exclude_dexes: frozenset = frozenset(),
                       include_dexes: frozenset | None = None) -> tuple[dict | None, int, bool]:
        """Return (agg, generation, connected) for a user, aggregated across ALL dexes (matches REST
        _clearinghouse). agg = {"av","mu","upnl","pos":{coin:szi}} or None if missing/stale/disconnected.
        Fail-closed: caller treats None as 'do not trade this cycle'.
        strict=True (default, for OUR OWN account + the parity gate): per-user message freshness -- a state
          older than max_age_s is stale. This keeps the -15% stop reading fresh own state (codex 2026-06-05 #2).
        strict=False (for idle LEADERS): FEED-liveness -- HL only pushes a leader's state on CHANGE, so a
          quiet low-frequency leader sends no update; its cached state is still VALID (unchanged) as long as
          the FEED is alive (allMids ticks ~1/s -> _last_msg_ts fresh). Caches are cleared on any disconnect
          (v->None) + generation bumps, and a malformed update invalidates the cache (#1), so a present v in
          the current generation is trustworthy. Fixes the constant false 'WS state stale' on idle leaders."""
        now = time.time()
        with self._lock:   # aggregate UNDER the lock so the returned dict is a true immutable snapshot (codex #7)
            gen = self._generation
            connected = self._connected
            v = self._user_state.get(addr.lower())
            if not connected or v is None:
                return None, gen, connected
            age = (now - v["ts"]) if strict else (now - self._last_msg_ts)
            if age > max_age_s:
                return None, gen, connected
            av = mu = upnl = 0.0
            pos: dict[str, float] = {}
            # dex scoping for POSITIONS only (equity av/mu/upnl stays ACCOUNT-WIDE across all dexes):
            #   include_dexes (if not None): copy positions ONLY from these dexes (copy_a passes {"","xyz"}
            #     = main + xyz; every other dex, e.g. flx, is excluded).
            #   exclude_dexes (legacy): omit these dexes.
            # A copy_a builder-dex position on an unlisted dex thus never perturbs the before/after diff.
            for dex, s in v["states"].items():
                av += s["av"]
                mu += s["mu"]
                upnl += s["upnl"]
                if include_dexes is not None and dex not in include_dexes:
                    continue
                if dex in exclude_dexes:
                    continue
                for coin, szi in s["pos"].items():
                    pos[coin] = pos.get(coin, 0.0) + szi
            return {"av": av, "mu": mu, "upnl": upnl, "pos": pos}, gen, connected

    def snapshot_all(self, addrs: list[str], max_age_s: float,
                     strict_addrs: frozenset = frozenset()) -> tuple[dict | None, int, bool]:
        """ATOMIC multi-user snapshot. Returns ({addr: agg}, generation, connected) ONLY if EVERY addr is
        present, fresh (< max_age_s), and the socket is connected -- all read under one lock in the CURRENT
        generation. Otherwise ({None}, gen, connected). This is the no-partial-state gate: the engine never
        reconciles on a leader set that is partially updated or spans a reconnect (a V11 failure mode).
        agg = {"av","mu","upnl","pos":{coin:szi}} aggregated across all of that user's dexes."""
        now = time.time()
        strict_addrs = frozenset(s.lower() for s in strict_addrs)   # normalize (codex: mixed-case footgun)
        with self._lock:
            gen = self._generation
            connected = self._connected
            if not connected:
                return None, gen, connected
            result: dict[str, dict] = {}
            for a in addrs:
                al = a.lower()
                v = self._user_state.get(al)
                if v is None:
                    return None, gen, connected
                # OWN/parent (in strict_addrs) -> per-user freshness (stop safety); idle LEADERS -> feed-liveness.
                age = (now - v["ts"]) if al in strict_addrs else (now - self._last_msg_ts)
                if age > max_age_s:
                    return None, gen, connected
                av = mu = upnl = 0.0
                pos: dict[str, float] = {}
                for s in v["states"].values():
                    av += s["av"]
                    mu += s["mu"]
                    upnl += s["upnl"]
                    for coin, szi in s["pos"].items():
                        pos[coin] = pos.get(coin, 0.0) + szi
                result[a.lower()] = {"av": av, "mu": mu, "upnl": upnl, "pos": pos}
            return result, gen, connected

    def get_mid(self, coin: str, max_age_s: float) -> float | None:
        """Fresh mid for a coin. MAIN allMids first (gated on the main socket being alive -- a mid from a
        dead socket is not tradeable, codex #5). If the coin is absent from the main mids (a builder-dex
        coin, e.g. xyz), fall back to the dedicated builder-dex mark cache. Each source is freshness-gated
        by its own per-entry timestamp; a stale/absent mark returns None -> the runner fails closed."""
        now = time.time()
        with self._lock:
            v = self._mids.get(coin) if self._connected else None
            if v is None:
                # builder-dex coin: served from its own socket's cache (cleared on that socket's disconnect,
                # so a present entry is from a live builder connection). Independent of the main socket.
                v = self._dex_mids.get(coin)
        if not v:
            return None
        px, ts = v
        return px if (now - ts) <= max_age_s and px > 0 else None

    def health(self) -> dict:
        with self._lock:
            return {"connected": self._connected, "generation": self._generation,
                    "n_users": len(self._user_state), "n_mids": len(self._mids),
                    "last_msg_age_s": (time.time() - self._last_msg_ts) if self._last_msg_ts else None}
