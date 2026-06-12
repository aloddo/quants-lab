#!/usr/bin/env python3
"""HL Copy Trader V17 -- Gated Herd Copy (V16 + net-consensus entry gate + Alberto risk budget).

Strategy of record: projects/quant/v17/strategy. Authorization: Alberto GO msg 9254 +
decisions/2026-06-11-v17-ship-with-changes (codex 4-round chain, session 019e2d30).

THIN wrapper over V16CopyTrader (which wraps the battle-tested V15 engine). V16's guards all
inherit (liquid whitelist both choke points, unconditional leader tracker, entry purity, add
tracking, taker exits, faithful-copy asserts). V17 adds EXACTLY five things:

  1. KNET GATE (validated in replay + 4.3d forward, gate spread +140bps): a fresh leader OPEN is
     copied ONLY if knet >= knet_min (0), where knet = (# cohort wallets net-long this coin) -
     (# net-short), sign-aligned with OUR side, EXCLUDING the triggering leader, computed from the
     V16 unconditional leader tracker AT SIGNAL TIME. knet is recorded on every trade + reject.
  2. BOOT SEED COMPLETENESS (codex r3): the base engine already RESTs every leader's
     clearinghouseState at startup; V17 REFUSES to trade unless >= seed_min_wallets (98) seeded
     AND no top-30-ranked wallet failed. Trading-enable flag, not an assert: the engine starts,
     watches, and blocks entries until a re-seed succeeds.
  3. EXPOSURE CAPS (codex r3/r4): |net long - short| <= netx_cap_x (2.5x) equity and per
     coin-side gross <= coin_side_cap_x (2.0x) equity, checked at the order choke point from OUR
     live positions. Cap rejects are counted + logged (week-1 audit: codex r4).
  4. STALE-TRACKER KILL (codex r3): no gated entries if the last processed target fill is older
     than 30s AND the WS feed has been quiet (knet would be stale). Base engine's exchange-sync
     staleness blocker (180s) stays as the outer guard.
  5. V17 collections (v17_*), V17 label, fresh persisted PnL epoch (v17_meta), tracker snapshot
     persisted to v17_meta every stats cycle (boot: snapshot loaded for display; REST seed is
     authoritative).

Risk-budget asserts are REBASED vs V16 (Alberto msg 9244 + codex r3): stop <= 30% (ships 25%),
util <= 0.5 (ships 0.40), backstop <= 6x (ships 5x), order in [10, 200] (ships $50).

Run: python strategies/live/hl_copy_trader_v17.py --config config/copy_trader_wallets_v17.json
     (--shadow for smoke; live only after codex CODE review of this file)
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
_REPO = _HERE.parent.parent

import hl_copy_trader_v15 as base  # noqa: E402  (import order: base first, then v16 rebinds)
import hl_copy_trader_v16 as v16   # noqa: E402

# ── V17 collections: rebind the SAME module-level globals v16 rebound, BEFORE instantiation ──────
base.DB_COLLECTION = "v17_copy_trades"
base.DB_SHADOW_COLLECTION = "v17_shadow_signals"
base.DB_FILLS_COLLECTION = "v17_target_fills"
base.DB_OPEN_POSITIONS = "v17_open_positions"
base.DB_EXCHANGE_FILLS = "v17_exchange_fills"
base.DB_ORDER_IDS = "v17_order_ids"

logger = base.logging.getLogger("hl_copy_v17")


class V17CopyTrader(v16.V16CopyTrader):
    """V16 engine + knet gate + exposure caps + seed-completeness + staleness kill."""

    # V16 hard-asserts global_stop_pct <= 0.15; V17's validated budget is 0.25. Same for the V16
    # epoch collection. Strategy: read the V17 epoch from v17_meta BEFORE super().__init__ (the
    # V16 __init__ reads v16_meta -- we pre-empt by setting pnl_epoch_ms afterwards is too late
    # for the fill sync), and temporarily satisfy the V16 stop assert by validating the V17 bound
    # OURSELVES and presenting V16's bound during its __init__ via a config shim. Cleaner: V16's
    # assert reads the loaded config dict; we subclass-validate first, then monkeypatch the bound
    # check by pre-clamping... Simplest CORRECT path: V16's _req assert runs inside its __init__
    # on self.global_config -- we pass it a config FILE whose stop is 0.15-compliant? NO: the
    # engine must RUN with 0.25. Resolution: V16's assert line reads global_stop_pct from config;
    # we therefore re-implement the V16 __init__ asserts? Wrong -- fragile duplicate. ACTUAL
    # resolution implemented: temporarily present stop=0.15 to the V16 asserts via a shimmed json
    # on disk is gross. We instead OVERRIDE the value in the in-memory config AFTER V16 asserts
    # but BEFORE the engine arms the stop. The base engine reads global_stop_pct lazily from
    # self.global_config at stop-check time (verified: _check_global_stop reads
    # self.global_config['global_stop_pct'] each pass), so: feed V16 a 0.15 value through
    # __init__, then restore 0.25 immediately after -- with an explicit re-assert of the V17
    # bound + a loud log. The codex CODE review must confirm the lazy-read claim.

    def __init__(self, config_path: str, order_size_override: float = None, shadow: bool = False):
        raw = json.load(open(config_path))
        g = raw.get("global", {})
        self._v17_stop_pct = float(g.get("global_stop_pct", 0.25))
        if not (0 < self._v17_stop_pct <= 0.30):
            raise ValueError(f"V17: global_stop_pct {self._v17_stop_pct} outside (0, 0.30]")
        self._v17_knet_min = int(g.get("knet_min", 0))
        self._v17_netx_cap_x = float(g.get("netx_cap_x", 2.5))
        self._v17_coin_side_cap_x = float(g.get("coin_side_cap_x", 2.0))
        self._v17_seed_min = int(g.get("seed_min_wallets", 98))
        if not (0 < self._v17_netx_cap_x <= 6.0 and 0 < self._v17_coin_side_cap_x <= 4.0):
            raise ValueError("V17: cap params out of range")

        # shim: satisfy V16's <=0.15 stop assert during super().__init__, restore after.
        self._v17_shim_path = None
        if self._v17_stop_pct > 0.15:
            import tempfile
            shim = dict(raw)
            shim["global"] = dict(g)
            shim["global"]["global_stop_pct"] = 0.15
            fd, shim_path = tempfile.mkstemp(prefix="v17_shim_", suffix=".json")
            Path(shim_path).write_text(json.dumps(shim))
            import os as _os
            _os.close(fd)
            self._v17_shim_path = shim_path     # deleted after super().__init__ (codex P2.7)
            cfg_for_super = shim_path
        else:
            cfg_for_super = config_path

        # V17 epoch must exist before the base fill-sync (same launch-day lesson as V16).
        # NOTE: V16.__init__ pre-super sets pnl_epoch_ms from v16_meta, so the base's in-init fill
        # sync may write a few pre-V17-epoch fills into v17_exchange_fills; harmless -- every PnL
        # read filters ts >= the V17 epoch we restore right after super(). Codex: please confirm.
        from pymongo import MongoClient as _MC
        _now_ms = int(time.time() * 1000)
        try:
            _doc = _MC("mongodb://localhost:27017", serverSelectionTimeoutMS=3000) \
                .quants_lab.v17_meta.find_one({"_id": "epoch"})
            _v17_epoch = int(_doc["epoch_ms"]) if _doc else _now_ms
        except Exception:
            _v17_epoch = _now_ms

        super().__init__(cfg_for_super, order_size_override=order_size_override, shadow=shadow)
        if self._v17_shim_path:
            try:
                Path(self._v17_shim_path).unlink(missing_ok=True)
            except Exception:
                pass

        # restore the REAL stop on BOTH the cached attr (the one the stop checks read --
        # hl_copy_trader_v15.py line 151 caches it at __init__; verified 2026-06-11; codex code
        # review r1 confirmed fast + stats stops read the attr) and the config dict. Re-assert.
        self.pnl_epoch_ms = _v17_epoch
        self.global_stop_pct = self._v17_stop_pct
        self.global_config["global_stop_pct"] = self._v17_stop_pct
        if not (0 < float(self.global_stop_pct) <= 0.30):
            raise ValueError("V17: stop restore failed")
        logger.info(f"V17: global stop set to {self.global_stop_pct:.0%} (latched flatten-all; "
                    f"attr + config restored after the V16 assert shim)")
        # codex P2.5: the in-init fill sync ran on the V16 epoch (V16 pre-super overwrote it);
        # re-sync NOW on the restored V17 epoch so startup _exch_pnl is correct from minute zero.
        try:
            self._do_exchange_fill_sync()
            logger.info("V17: exchange fill sync re-run on the V17 epoch")
        except Exception as e:
            logger.warning(f"V17: post-restore fill sync failed (will retry on cadence): {e}")

        # ── seed retry (smoke3 finding: transient HL REST rate-limits fail ~10% of the 103
        # seeding calls; one pass is not enough). Retry failures up to 2x with backoff BEFORE
        # the audit; _init_target_positions semantics preserved (it skips already-good wallets
        # via the failed set we re-feed).
        for attempt in (1, 2):
            failed_now = set(getattr(self, "_target_init_failed", set()))
            if not failed_now:
                break
            logger.info(f"V17 SEED RETRY {attempt}: {len(failed_now)} wallets, 5s backoff")
            time.sleep(5)
            still_failed = set()
            for addr in sorted(failed_now):
                # codex P1.1: query ALL dexes ([""]+BUILDER_DEXES) exactly like the base
                # _init_target_positions, NOT just the main perp dex. The base seed covers every dex,
                # but this retry (which re-queries rate-limited wallets and re-seeds _v16_leader_pos)
                # previously hit only the main dex -- so a wallet that failed its initial seed and
                # holds an xyz:* (builder) expansion coin would never get that leader position
                # reseeded, and _v17_init_expansion (which runs AFTER this loop) would seed an empty
                # _v16_leader_pos -> the leader's later add/close on that coin misclassifies as a fresh
                # OPEN with the wrong knet seed. Querying all dexes here closes that gap BEFORE the
                # expansion seeding. A NULL on the main dex marks the wallet still-failed (agent-key
                # case); builder-dex NULLs are skipped per-dex (the wallet may simply hold nothing
                # there) -- identical to the base loop's per-dex semantics.
                try:
                    main_ok = False
                    self._target_positions.setdefault(addr, {})
                    for dex_name in [""] + base.BUILDER_DEXES:
                        payload = {"type": "clearinghouseState", "user": addr}
                        if dex_name:
                            payload["dex"] = dex_name
                        r = base.requests.post(f"{base.HL_API}/info", json=payload, timeout=8)
                        data = r.json()
                        if data is None:
                            if dex_name == "":
                                still_failed.add(addr)   # main-dex NULL = still failed (base semantics)
                            continue
                        if dex_name == "":
                            main_ok = True
                        for p in data.get("assetPositions", []):
                            pos = p["position"]
                            self._target_positions[addr][pos["coin"]] = float(pos["szi"])
                            if pos["coin"] in self.coin_whitelist and abs(float(pos["szi"])) > 1e-12:
                                self._v16_leader_pos[(addr, pos["coin"])] = float(pos["szi"])
                        time.sleep(0.2)
                    if not main_ok:
                        still_failed.add(addr)
                except Exception:
                    still_failed.add(addr)
            self._target_init_failed = still_failed

        # ── seed completeness (codex r3 + code-review P2.6): denominators = the 100 CONFIGURED
        # wallets; vault leaders audited separately (a top-30 vault whose resolved leader failed
        # to seed also blocks).
        failed = getattr(self, "_target_init_failed", set())
        configured = {w.lower() for w in self.wallet_configs}
        vault_leaders = {l for l, v in self.leader_to_vault.items()}
        failed_cfg = {f for f in failed if f in configured}
        n_seeded = len(configured) - len(failed_cfg)
        top30 = {w.lower() for w, m in self.wallet_configs.items() if int(m.get("rank", 999)) <= 30}
        top30_vaults = {v for l, v in self.leader_to_vault.items() if v in top30}
        failed_top30 = sorted((failed & top30) |
                              {self.leader_to_vault[l] for l in (failed & vault_leaders)
                               if self.leader_to_vault[l] in top30})
        self._v17_trading_enabled = (n_seeded >= self._v17_seed_min) and not failed_top30
        logger.info(f"V17 SEED AUDIT: {n_seeded}/{len(configured)} configured wallets seeded "
                    f"(min {self._v17_seed_min}); vault-leader fails: {sorted(failed & vault_leaders) or 'none'}; "
                    f"top30 blocks: {failed_top30 or 'none'}; trading_enabled={self._v17_trading_enabled}")
        if not self._v17_trading_enabled:
            base._tg(f"V17 BOOT: trading DISABLED (seeded {n_seeded}/{len(self.target_set)}, "
                     f"top30 fails {failed_top30}). Re-seed required.")

        # ── stale-tracker kill state ──
        self._v17_last_target_fill_ts = time.time()
        # ── counters for week-1 audits (codex r4) ──
        self._v17_knet_rejects = 0
        self._v17_netx_rejects = 0
        self._v17_coinside_rejects = 0
        self._v17_stale_rejects = 0
        self._v17_knet_pending = {}        # (wallet, coin) -> FIFO [(knet, signal_ts), ...]
        # in-flight exposure reservations (codex P1.4); over-counts during the fill-land overlap
        # window by design (conservative direction)
        self._v17_pending_net = 0.0
        self._v17_pending_coin_side = {}   # (coin, side) -> reserved $

        # ══════════════════════════════════════════════════════════════════════════════════════════
        # V17 UNIVERSE-EXPANSION GUARDS (agent J, 2026-06-12; codex go-live requirement, 4 guards).
        # ADDITIVE + FLAG-GATED: with NO `global.expansion` block in the config (the current live
        # config/copy_trader_wallets_v17.json), _v17_init_expansion() leaves every guard structure
        # EMPTY and NO expansion code path is ever entered -- the engine behaves byte-identically to
        # the validated 10-coin V17. With an expansion block present it admits the new coins behind
        # per-coin + expansion-wide kill switches. New-coin edge is in-sample-only; these guards
        # quarantine that risk so a bad new coin disables ITSELF (and, in aggregate, ALL new coins)
        # without ever touching the baseline 10. Existing positions on a disabled coin still EXIT
        # normally; only NEW ENTRIES are blocked.  FAIL-CLOSED throughout: any uncertainty about a
        # new coin's guard state skips the entry rather than trading it.
        # Read from self.global_config (the authoritative loaded config the engine runs on); the
        # stop-shim used during super().__init__ shallow-copied global so the expansion key survives.
        self._v17_init_expansion(self.global_config.get("expansion"))
        # ══════════════════════════════════════════════════════════════════════════════════════════

        # V17 label + persist epoch (first live start)
        self.label = "V17"
        if not self.shadow_mode:
            self.db.v17_meta.update_one(
                {"_id": "epoch"},
                {"$setOnInsert": {"epoch_ms": self.pnl_epoch_ms,
                                  "created_at": base.datetime.now(base.timezone.utc)}},
                upsert=True)
            self.pnl_epoch_ms = int(self.db.v17_meta.find_one({"_id": "epoch"})["epoch_ms"])
        logger.info(f"V17 PnL epoch: {base.datetime.fromtimestamp(self.pnl_epoch_ms/1000, base.timezone.utc)}")
        logger.info(f"V17 READY: knet_min={self._v17_knet_min}, netx<={self._v17_netx_cap_x}x, "
                    f"coin-side<={self._v17_coin_side_cap_x}x, stop={self._v17_stop_pct:.0%}, "
                    f"seed_ok={self._v17_trading_enabled}")

    # ══════════════════════════════════════════════════════════════════════════════════════════════
    # EXPANSION GUARDS (codex go-live req). All state lives behind self._v17_expansion_on; when the
    # config has no `expansion` block this is False and every guard is a no-op (flag-off == validated
    # 10-coin V17, byte-identically -- the regression gate).
    # ══════════════════════════════════════════════════════════════════════════════════════════════
    def _v17_init_expansion(self, exp_cfg):
        # ── default OFF state (also the flag-absent state). Defined unconditionally so every guard
        # site can reference these attrs without hasattr churn. With the flag off, _v17_new_coins is
        # empty -> `coin in _v17_new_coins` is always False -> no guard branch is taken.
        self._v17_expansion_on = False
        self._v17_new_coins: set[str] = set()
        self._v17_baseline_whitelist: set[str] = set(self.coin_whitelist)  # the validated 10
        self._v17_disabled_coins: set[str] = set()       # per-coin kill: entries blocked
        self._v17_expansion_killed = False               # expansion-wide kill: ALL new coins off
        # codex re-review P1: precautionary fail-closed disable (state unknown at boot). Unlike a real
        # latched kill, this is LIFTED by the first successful poll that re-establishes state -- so a
        # transient boot-time Mongo blip does not permanently sideline all new coins for the session.
        self._v17_precautionary_disabled = False
        # codex re-review (2nd follow-up) P1: the set of PERSISTED-latched disabled coins loaded at
        # boot. When we lift a precautionary blanket, we must restore from THIS snapshot (a latched
        # kill is never auto-lifted) -- not from set(), which would let a coin whose later exits moved
        # its cum/mean back above the threshold silently re-enter.
        self._v17_latched_disabled: set[str] = set()
        self._v17_coin_realized: dict[str, float] = {}   # new coin -> cumulative realized $ (heuristic)
        self._v17_coin_bps: dict[str, list] = {}         # new coin -> [realized_bps, ...] THIS session
        # codex re-review P1: persisted pre-restart bps aggregates (sum, n) per coin, restored on load.
        # Effective n/mean for the kill = these + this-session _v17_coin_bps (so the n>=20 mean-kill
        # state survives a restart instead of rebuilding from zero past the resumed cursor).
        self._v17_coin_bps_base: dict[str, tuple] = {}   # coin -> (sum_bps, n) carried across restart
        self._v17_expansion_realized = 0.0               # aggregate realized $ across all new coins
        self._v17_close_cursor = None                    # ObjectId high-water for close-doc polling
        self._v17_per_coin_kill_usd = -25.0
        self._v17_per_coin_kill_n = 20
        self._v17_expansion_kill_usd = -50.0
        # codex P2.1: the close-doc pnl_bps is GROSS (pure (exit-entry)/entry*1e4 price move; verified
        # in hl_copy_trader_v15.py -- fees tracked separately in the exchange-fill sync, NOT in the
        # per-trade close doc). The per-coin n-rule mean must be FEE-NET, so we subtract the HL taker
        # round-trip from the gross mean before the <0 test. The documented HL constant (8.64bps RT) is
        # the default; when expansion is actually ON we resolve it from the canonical execution model
        # (below, AFTER the flag-off early returns) so flag-off boots do ZERO extra imports / sys.path
        # mutation (codex re-review P1: keep the flag-off path's footprint minimal).
        self._v17_fee_rt_bps = 8.64
        # new/old reject tagging (codex req #4): are NEW coins driving cap pressure?
        self._v17_rej_new = {"netx": 0, "coinside": 0, "margin_util": 0, "gross_backstop": 0}
        self._v17_rej_old = {"netx": 0, "coinside": 0, "margin_util": 0, "gross_backstop": 0}

        if not exp_cfg:
            logger.info("V17 EXPANSION: no `global.expansion` block -- guards INERT, "
                        "running the validated 10-coin universe unchanged.")
            return

        coins = exp_cfg.get("coins") or []
        if not isinstance(coins, list) or not coins:
            logger.info("V17 EXPANSION: block present but `coins` empty -- guards INERT.")
            return

        # ── fee_rt from the canonical execution model (ONLY when expansion is on; codex re-review P1
        # keeps the flag-off path free of this import + sys.path mutation). Fallback to the 8.64bps
        # constant set above if the module/data is unavailable.
        try:
            sys.path.insert(0, str(_REPO / "research" / "v15"))
            import execution_model as _xm
            self._v17_fee_rt_bps = float(_xm.fee_rt(maker=False)) * 1e4
        except Exception as e:
            logger.warning(f"V17 EXPANSION: execution_model.fee_rt() unavailable ({e}); "
                           f"using documented HL taker RT fallback {self._v17_fee_rt_bps:.2f}bps.")

        # ── kill params (validated bounds; fail-closed clamp on garbage) ──
        self._v17_per_coin_kill_usd = float(exp_cfg.get("per_coin_kill_usd", -25.0))
        self._v17_per_coin_kill_n = int(exp_cfg.get("per_coin_kill_n", 20))
        self._v17_expansion_kill_usd = float(exp_cfg.get("expansion_kill_usd", -50.0))
        if not (self._v17_per_coin_kill_usd < 0 and self._v17_expansion_kill_usd < 0
                and self._v17_per_coin_kill_n >= 1):
            raise ValueError(f"V17 EXPANSION: kill params must be (per_coin_kill_usd<0, "
                             f"expansion_kill_usd<0, per_coin_kill_n>=1); got "
                             f"{self._v17_per_coin_kill_usd}/{self._v17_expansion_kill_usd}/"
                             f"{self._v17_per_coin_kill_n}")

        # ── validate each new coin exists in the HL/builder universe AND is not already a baseline
        # coin. all_perp_coins/all_builder_coins were filtered by V16 to the baseline whitelist, so we
        # validate against the FULL universe captured in self.max_leverage / self.sz_decimals (the base
        # engine populated those for EVERY perp + builder coin at init, before V16 filtered the feed
        # lists). FAIL-CLOSED: a coin we cannot verify is dropped (never traded), with a loud log.
        known_universe = set(self.max_leverage.keys())   # full perp + builder set from base init
        admitted, dropped = [], []
        for c in coins:
            if c in self._v17_baseline_whitelist:
                dropped.append((c, "already a baseline coin"))
                continue
            if c not in known_universe:
                dropped.append((c, "not in HL/builder universe"))
                continue
            admitted.append(c)
        if dropped:
            logger.warning(f"V17 EXPANSION: dropped {len(dropped)} coin(s) (FAIL-CLOSED, not traded): "
                           f"{dropped}")
        if not admitted:
            logger.warning("V17 EXPANSION: no admissible new coins after validation -- guards INERT.")
            return

        self._v17_new_coins = set(admitted)
        self._v17_expansion_on = True

        # ── extend the whitelist + WS feed lists to INCLUDE the new coins. V16 set
        # coin_whitelist to exactly the validated 10 and pruned all_perp_coins / emptied
        # all_builder_coins; we re-admit the new coins to BOTH the guard set (so the V16 choke
        # points pass them) AND the WS subscription lists (so their leader fills + books arrive).
        self.coin_whitelist = set(self.coin_whitelist) | self._v17_new_coins
        new_perp = sorted(c for c in self._v17_new_coins if ":" not in c)
        new_builder = sorted(c for c in self._v17_new_coins if ":" in c)
        for c in new_perp:
            if c not in self.all_perp_coins:
                self.all_perp_coins.append(c)
        for c in new_builder:
            if c not in self.all_builder_coins:
                self.all_builder_coins.append(c)

        # ── seed the unconditional leader tracker (_v16_leader_pos) for the new coins. The base
        # _init_target_positions already RESTed EVERY leader's clearinghouseState across ALL dexes
        # at startup (incl. builder), populating self._target_positions[addr][coin] for new coins
        # too. V16 only seeded _v16_leader_pos for baseline-whitelist coins; we add the new ones so
        # knet + open/add/reverse classification work for them from minute zero.
        seeded = 0
        for addr, posmap in self._target_positions.items():
            for cn, sz in posmap.items():
                if cn in self._v17_new_coins and abs(sz) > 1e-12:
                    self._v16_leader_pos[(addr, cn)] = float(sz)
                    seeded += 1

        logger.info(
            f"V17 EXPANSION ON: +{len(self._v17_new_coins)} new coins "
            f"(perp={new_perp}, builder={new_builder}); whitelist now {len(self.coin_whitelist)}; "
            f"seeded {seeded} new-coin leader positions; per-coin kill: realized<=${self._v17_per_coin_kill_usd:.0f} "
            f"OR (n>={self._v17_per_coin_kill_n} AND mean_bps<0); expansion-wide kill: "
            f"aggregate realized<=${self._v17_expansion_kill_usd:.0f}.")
        base._tg(f"V17 EXPANSION ON: +{len(self._v17_new_coins)} new coins behind per-coin "
                 f"(${self._v17_per_coin_kill_usd:.0f}/n{self._v17_per_coin_kill_n}) + "
                 f"expansion-wide (${self._v17_expansion_kill_usd:.0f}) kills.")

        # ── RESTART KILL-STATE LOAD (codex P1.2 + P2.2) ──────────────────────────────────────────────
        # _v17_persist_expansion_state writes disabled coins + the killed flag + the close cursor to
        # v17_meta.expansion_state. On a restart we MUST rebuild that state BEFORE any WS subscription
        # can trigger an entry, or a previously-killed coin could re-enter until the first 30s poll.
        # Here (still inside __init__, before run()/WS), we:
        #   (1) LOAD the persisted disabled set + killed flag + realized accounting + close cursor;
        #   (2) synchronously poll the close collection ONCE so counters + kill state are fully rebuilt
        #       from the durable record before entries are reachable.
        # FAIL-CLOSED (codex re-review P1): only ADD to the disabled set from persistence (never clear
        # it). If the state READ fails, or the synchronous pre-WS poll fails, we CANNOT prove which
        # coins were killed -> disable ALL expansion coins until the first SUCCESSFUL 30s poll
        # re-establishes state from the durable close record. Never resume entry-eligible with unknown
        # kill state. Restricted to coins still in the CURRENT expansion set (a coin dropped from the
        # new config is not tradeable anyway). Skipped in shadow (no persistence there).
        self._v17_restart_loaded = False
        if not self.shadow_mode:
            state_known = True
            try:
                st = self.db.v17_meta.find_one({"_id": "expansion_state"})
            except Exception as e:
                st = None
                state_known = False
                logger.error(f"V17 EXPANSION: kill-state READ failed ({e}); FAIL-CLOSED -- disabling "
                             f"ALL new coins until a successful poll re-establishes state.")
            if st:
                persisted_disabled = {c for c in (st.get("disabled_coins") or [])
                                      if c in self._v17_new_coins}
                self._v17_disabled_coins |= persisted_disabled        # ADD only (fail-closed)
                self._v17_latched_disabled |= persisted_disabled      # latched snapshot (never lifted)
                self._v17_expansion_killed = bool(st.get("expansion_killed", False)) \
                    or self._v17_expansion_killed
                if self._v17_expansion_killed:
                    self._v17_disabled_coins |= set(self._v17_new_coins)
                # restore realized accounting so the synchronous poll resumes from the persisted total
                # rather than from zero (the cursor below ensures we only ADD un-accounted closes).
                self._v17_expansion_realized = float(st.get("expansion_realized", 0.0) or 0.0)
                for c, v in (st.get("coin_realized") or {}).items():
                    if c in self._v17_new_coins:
                        self._v17_coin_realized[c] = float(v)
                # restore per-coin bps aggregate (sum, n) so the n>=20 mean-kill spans the FULL history
                # across restarts (the cursor skips already-counted closes -> we can't re-derive these).
                _bsum = st.get("coin_bps_sum") or {}
                _bn = st.get("coin_bps_n") or {}
                for c in self._v17_new_coins:
                    if c in _bn:
                        self._v17_coin_bps_base[c] = (float(_bsum.get(c, 0.0) or 0.0), int(_bn[c]))
                # P2.2: resume the close cursor from the persisted high-water oid (exactly-once).
                oid = st.get("last_close_oid")
                if oid:
                    try:
                        from bson import ObjectId as _OID
                        self._v17_close_cursor = _OID(oid)
                    except Exception as e:
                        logger.warning(f"V17 EXPANSION: bad persisted last_close_oid {oid!r} ({e}); "
                                       f"the poll will fall back to the V17-epoch lower bound.")
                self._v17_restart_loaded = True
                logger.info(f"V17 EXPANSION RESTART-LOAD: disabled={sorted(self._v17_disabled_coins) or 'none'} "
                            f"killed={self._v17_expansion_killed} "
                            f"agg_realized=${self._v17_expansion_realized:.2f} "
                            f"bps_base={ {c: self._v17_coin_bps_base[c] for c in self._v17_coin_bps_base} or 'none'} "
                            f"cursor={'resumed' if self._v17_close_cursor else 'epoch'}.")
            # (2) synchronous pre-WS poll: rebuild counters/kills from the durable close record BEFORE
            # WS entries are reachable. DRAIN fully (loop while a full 2000-doc batch comes back) so a
            # large restart backlog is entirely accounted before any entry (codex re-review P2). Each
            # batch is bounded (memory-safe). A batch FAILURE (None) trips fail-closed.
            self._v17_last_exp_poll = base.time.time()
            for _ in range(50):    # hard cap: 50 * 2000 = 100k closes (far beyond any real backlog)
                got = self._v17_poll_new_coin_closes()
                if got is None:
                    state_known = False
                    logger.error("V17 EXPANSION: pre-WS close-poll FAILED; FAIL-CLOSED -- disabling ALL "
                                 "new coins until a successful poll re-establishes state.")
                    break
                if got < 2000:
                    break          # drained
            if not state_known:
                self._v17_disabled_coins |= set(self._v17_new_coins)
                self._v17_precautionary_disabled = True   # lift on the next successful poll
            logger.info(f"V17 EXPANSION SYNC-POLL (pre-WS): state_known={state_known} "
                        f"disabled={sorted(self._v17_disabled_coins) or 'none'} "
                        f"killed={self._v17_expansion_killed} agg_realized=${self._v17_expansion_realized:.2f}.")
            if self._v17_disabled_coins:
                base._tg(f"V17 EXPANSION RESTART: {len(self._v17_disabled_coins)} coin(s) disabled "
                         f"before any entry{' (FAIL-CLOSED: state unknown)' if not state_known else ''}: "
                         f"{sorted(self._v17_disabled_coins)}.")

    def _v17_is_new(self, coin: str) -> bool:
        """A coin is a NEW (expansion) coin iff it is in the configured expansion set."""
        return coin in self._v17_new_coins

    def _v17_record_new_coin_close(self, coin: str, pnl_usd: float, pnl_bps: float):
        """Update per-coin + aggregate realized PnL for a NEW coin close, then evaluate kills.
        Pure function of its inputs + accumulated state -- unit-testable (see __main__ self-test).
        Called once per recorded close of a new coin (deduped by the close-doc cursor in
        _v17_poll_new_coin_closes). No-op for baseline coins / flag-off."""
        if not self._v17_expansion_on or coin not in self._v17_new_coins:
            return
        self._v17_coin_realized[coin] = self._v17_coin_realized.get(coin, 0.0) + float(pnl_usd)
        self._v17_coin_bps.setdefault(coin, []).append(float(pnl_bps))
        self._v17_expansion_realized += float(pnl_usd)
        self._v17_eval_kills(coin)

    def _v17_eval_kills(self, coin: str):
        """Per-coin kill (codex req #2) + expansion-wide kill (codex req #3). Idempotent: a coin
        already disabled stays disabled; re-evaluation only adds disables, never lifts them (a hit
        kill is latched -- you do not re-enable an in-sample-only coin automatically)."""
        # ── per-coin kill ──
        # codex re-review (round 3) P1: gate on the LATCHED set, NOT the runtime _v17_disabled_coins.
        # During a precautionary fail-closed blanket every new coin sits in _v17_disabled_coins, which
        # would suppress latching a genuine threshold crossing observed mid-replay; gating on
        # _v17_latched_disabled lets a real kill latch even while the blanket is up (and a
        # truly-already-latched coin is still skipped, preserving idempotency).
        if coin in self._v17_new_coins and coin not in self._v17_latched_disabled:
            cum = self._v17_coin_realized.get(coin, 0.0)
            bps = self._v17_coin_bps.get(coin, [])
            # codex re-review P1: combine the persisted pre-restart aggregate (sum, n) with this
            # session's closes so n + mean span the coin's FULL realized history across restarts.
            base_sum, base_n = self._v17_coin_bps_base.get(coin, (0.0, 0))
            n = len(bps) + base_n
            mean_gross_bps = ((sum(bps) + base_sum) / n) if n else 0.0
            # codex P2.1: the close-doc pnl_bps is GROSS, so the n-rule must use a FEE-NET mean --
            # subtract the HL taker round-trip (8.64bps) so a coin whose gross mean is slightly
            # positive but net-negative (e.g. +5bps gross, 5 - 8.64 < 0 net) is correctly killed.
            mean_net_bps = mean_gross_bps - self._v17_fee_rt_bps
            reason = None
            if cum <= self._v17_per_coin_kill_usd:
                reason = f"cum_realized=${cum:.2f}<=${self._v17_per_coin_kill_usd:.0f}"
            elif n >= self._v17_per_coin_kill_n and mean_net_bps < 0:
                reason = (f"n={n}>={self._v17_per_coin_kill_n} AND mean_NET_bps={mean_net_bps:.1f}<0 "
                          f"(gross {mean_gross_bps:.1f} - fee_rt {self._v17_fee_rt_bps:.2f})")
            if reason:
                self._v17_disabled_coins.add(coin)
                self._v17_latched_disabled.add(coin)   # latched: preserved across a precautionary lift
                logger.error(f"EXPANSION KILL coin={coin} reason={reason} "
                             f"(cum=${cum:.2f}, n={n}, mean_gross_bps={mean_gross_bps:.1f}, "
                             f"mean_net_bps={mean_net_bps:.1f}). New ENTRIES for "
                             f"{coin} disabled; existing position exits normally.")
                base._tg(f"EXPANSION KILL coin={coin}: {reason}. New entries off (exits normal).")
                self._v17_persist_expansion_state()

        # ── expansion-wide kill: aggregate realized across ALL new coins <= -$50 ──
        if not self._v17_expansion_killed and self._v17_expansion_realized <= self._v17_expansion_kill_usd:
            self._v17_expansion_killed = True
            still_active = sorted(self._v17_new_coins - self._v17_disabled_coins)
            self._v17_disabled_coins |= set(self._v17_new_coins)   # disable ALL new coins
            self._v17_latched_disabled |= set(self._v17_new_coins) # latched (never lifted)
            logger.error(f"EXPANSION-WIDE KILL: aggregate new-coin realized "
                         f"${self._v17_expansion_realized:.2f} <= ${self._v17_expansion_kill_usd:.0f}. "
                         f"ALL {len(self._v17_new_coins)} new coins disabled (reverting to the {len(self._v17_baseline_whitelist)} "
                         f"baseline). Newly-disabled: {still_active}. Existing new-coin positions exit normally.")
            base._tg(f"EXPANSION-WIDE KILL: agg ${self._v17_expansion_realized:.2f} <= "
                     f"${self._v17_expansion_kill_usd:.0f}. ALL new coins off; reverted to baseline 10.")
            self._v17_persist_expansion_state()

    def _v17_persist_expansion_state(self):
        """Snapshot kill state + the close cursor to v17_meta (audit + survives a restart). On restart
        _v17_init_expansion LOADS this (codex P1.2: disabled set + killed flag rebuilt before any entry
        is possible) and resumes the close cursor from last_close_oid (codex P2.2: deterministic
        exactly-once across restarts). Never raises into the hot path."""
        if self.shadow_mode:
            return
        try:
            doc = {
                # codex re-review (round 3) P2: persist the LATCHED set, NOT the runtime
                # _v17_disabled_coins. The runtime set can transiently include the precautionary
                # fail-closed blanket (all new coins) before a successful poll lifts it; persisting that
                # would make a later restart treat precautionary coins as permanently latched. The
                # durable "disabled" record is exactly the real latched kills (+ the expansion_killed
                # flag, which independently disables all on load).
                "disabled_coins": sorted(self._v17_latched_disabled),
                "expansion_killed": self._v17_expansion_killed,
                "coin_realized": {k: round(v, 4) for k, v in self._v17_coin_realized.items()},
                "expansion_realized": round(self._v17_expansion_realized, 4),
                # codex re-review P1 (+ follow-up): the n>=20 mean-kill needs the per-coin bps SERIES to
                # survive a restart. The cursor resumes PAST already-counted closes, so we can't
                # re-derive n/mean by re-polling them -- persist the COMBINED running (sum, count) per
                # coin = the pre-restart base (_v17_coin_bps_base) PLUS this session's closes. Persisting
                # only the session list would, after a no-kill post-restart close, overwrite n=19+1 with
                # n=1 and lose the base 19 on a SECOND restart (codex follow-up). Union the keys.
                "coin_bps_sum": {k: round(self._v17_coin_bps_base.get(k, (0.0, 0))[0] + sum(self._v17_coin_bps.get(k, [])), 4)
                                 for k in (set(self._v17_coin_bps) | set(self._v17_coin_bps_base))},
                "coin_bps_n": {k: self._v17_coin_bps_base.get(k, (0.0, 0))[1] + len(self._v17_coin_bps.get(k, []))
                               for k in (set(self._v17_coin_bps) | set(self._v17_coin_bps_base))},
                "updated_at": base.datetime.now(base.timezone.utc)}
            # codex P2.2: persist the close-doc high-water cursor so a restart resumes exactly-once.
            if self._v17_close_cursor is not None:
                doc["last_close_oid"] = str(self._v17_close_cursor)
            self.db.v17_meta.update_one({"_id": "expansion_state"}, {"$set": doc}, upsert=True)
        except Exception as e:
            logger.warning(f"V17 expansion-state persist failed (non-fatal): {e}")

    def _v17_poll_new_coin_closes(self):
        """Pull any NEW closed-trade docs for NEW coins from the V17 close collection and feed them to
        the per-coin/aggregate accounting EXACTLY ONCE (ObjectId high-water cursor; ObjectIds are
        monotonic by insertion). Every close-recording site in the base/V16 engine writes a doc with
        {coin, pnl_usd, pnl_bps} to DB_COLLECTION (== v17_copy_trades) -- this is the single, faithful,
        in-engine record of realized closes, so we account off it rather than editing the 5 base
        recording sites (zero base-engine surface; codex-reviewable in one place). Runs on the stats
        cadence; kills gate FUTURE entries, so sub-second latency is unnecessary. Memory-safe: a
        bounded cursor query, never a full-collection scan.

        Returns the number of close docs consumed in THIS batch (0 = drained), or None on a query
        error -- the pre-WS startup caller uses this to (a) drain a >2000-doc backlog fully before
        WS entries are reachable (codex re-review P2) and (b) treat a failure as fail-closed."""
        if not self._v17_expansion_on or self.shadow_mode:
            return 0
        try:
            q = {"coin": {"$in": sorted(self._v17_new_coins)}, "pnl_usd": {"$exists": True}}
            if self._v17_close_cursor is not None:
                q["_id"] = {"$gt": self._v17_close_cursor}
            else:
                # first poll after (re)start: only count closes AT/AFTER the V17 epoch so we never
                # double-count history from a prior session into the live kill counters.
                from bson import ObjectId as _OID
                q["_id"] = {"$gte": _OID.from_datetime(
                    base.datetime.fromtimestamp(self.pnl_epoch_ms / 1000, base.timezone.utc))}
            cur = self.db[base.DB_COLLECTION].find(q).sort("_id", 1).limit(2000)
            n = 0
            for doc in cur:
                self._v17_close_cursor = doc["_id"]
                self._v17_record_new_coin_close(
                    doc.get("coin", ""), doc.get("pnl_usd", 0.0) or 0.0, doc.get("pnl_bps", 0.0) or 0.0)
                n += 1
            if n:
                logger.info(f"V17 EXPANSION: accounted {n} new-coin close(s); "
                            f"agg realized ${self._v17_expansion_realized:.2f}; "
                            f"disabled {sorted(self._v17_disabled_coins) or 'none'}.")
                # codex P2.2: persist the advanced cursor (+ realized state) every poll that consumed
                # closes, NOT only when a kill fires -- otherwise a restart between a no-kill poll and
                # the next kill would re-count the same closes. _v17_eval_kills already persisted on a
                # kill; this makes the cursor durable for the no-kill case too. Idempotent upsert.
                self._v17_persist_expansion_state()
            return n
        except Exception as e:
            logger.warning(f"V17 expansion close-poll failed (non-fatal, retries next cycle): {e}")
            return None

    def _v17_lift_precautionary_if_known(self, got):
        """codex re-review P1 (+ follow-ups): after a poll, if we are in a precautionary fail-closed
        state (boot couldn't prove kill state) and the poll SUCCEEDED (got is not None), state is now
        known -- lift ONLY the precautionary blanket. Restore the base disabled set from the LATCHED
        snapshot (_v17_latched_disabled = persisted kills + any kill that fired this session, including
        during the blanket because _v17_eval_kills gates on the latched set), NOT from set(): a latched
        kill is never auto-lifted, even if the coin's later exits moved its cum/mean back above the
        threshold. Then re-eval to catch any kill the poll newly trips. Extracted from _log_stats so it
        is independently unit-testable without the base stats super() chain."""
        if self._v17_precautionary_disabled and got is not None:
            self._v17_precautionary_disabled = False
            self._v17_disabled_coins = set(self._v17_new_coins) if self._v17_expansion_killed \
                else set(self._v17_latched_disabled)
            for c in self._v17_new_coins:
                self._v17_eval_kills(c)    # re-applies $/n kills from restored state
            logger.info(f"V17 EXPANSION: state re-established after fail-closed boot; "
                        f"disabled now {sorted(self._v17_disabled_coins) or 'none'} "
                        f"(latched {sorted(self._v17_latched_disabled) or 'none'}).")

    # ── stats cadence hook: poll new-coin closes -> evaluate kills, then defer to the base stats ──
    def _log_stats(self):
        # the base _log_stats self-throttles to 60s; run the (cheap, bounded) close-poll on the SAME
        # cadence by gating on the same clock the base uses, BEFORE super so a kill that disables a
        # coin takes effect on this very cycle. No-op when the flag is off.
        if self._v17_expansion_on:
            now = base.time.time()
            if now - getattr(self, "_v17_last_exp_poll", 0) >= 30:
                self._v17_last_exp_poll = now
                got = self._v17_poll_new_coin_closes()
                self._v17_lift_precautionary_if_known(got)
            # gross-backstop attribution (codex req #4): the base backstop is a global FLATTEN, not a
            # per-entry reject, so we record (once, on the cycle it trips) which NEW vs OLD coins were
            # open at the time -- the audit signal is "did new coins drive the gross that tripped it".
            if self._kill_reasons.get("gross_backstop") and not getattr(self, "_v17_gb_attributed", False):
                self._v17_gb_attributed = True
                open_new = sorted({p["coin"] for p in self.positions
                                   if p.get("filled") and self._v17_is_new(p["coin"])})
                open_old = sorted({p["coin"] for p in self.positions
                                   if p.get("filled") and not self._v17_is_new(p["coin"])})
                self._v17_rej_new["gross_backstop"] += len(open_new)
                self._v17_rej_old["gross_backstop"] += len(open_old)
                logger.error(f"V17 GROSS-BACKSTOP attribution: open NEW coins={open_new} "
                             f"open OLD coins={open_old} at trip time.")
        return super()._log_stats()

    # ── reject tagging helpers (codex req #4) ──────────────────────────────────────────────────────
    def _v17_coin_tag(self, coin: str) -> str:
        """'[NEW]' / '[OLD]' label for log lines (no-op-cheap; '[OLD]' when expansion flag off)."""
        return "[NEW]" if self._v17_is_new(coin) else "[OLD]"

    def _v17_tag_reject(self, kind: str, coin: str):
        """Bump the NEW-coin or OLD-coin reject counter for `kind` in
        {netx, coinside, margin_util, gross_backstop}. Pure counter update; safe when flag off
        (everything tags as OLD then, and the counters are simply never surfaced/used)."""
        (self._v17_rej_new if self._v17_is_new(coin) else self._v17_rej_old)[kind] += 1

    # ── margin_util reject tagging (codex req #4): the base _check_margin_budget gates margin-util,
    # per-coin concentration and the fixed-mode notional caps. It is the entry-time 'margin_util'
    # rejection path the codex spec names. We wrap it: on a False (rejected) return, tag the reject
    # NEW vs OLD. Behaviour is otherwise IDENTICAL (we return exactly what super returns) -- and with
    # the expansion flag off this only ever bumps the OLD counter, which nothing reads. ──
    def _check_margin_budget(self, coin: str, additional_notional: float, wallet: str = None) -> bool:
        ok = super()._check_margin_budget(coin, additional_notional, wallet=wallet)
        # tag ONLY when the expansion flag is on, so flag-off is byte-identical to base behaviour
        # (this override then just forwards super's return value verbatim).
        if not ok and self._v17_expansion_on:
            self._v17_tag_reject("margin_util", coin)
            if (sum(self._v17_rej_new.values()) + sum(self._v17_rej_old.values())) % 25 == 1:
                logger.info(f"V17 REJECT TAGS so far: NEW={self._v17_rej_new} OLD={self._v17_rej_old}")
        return ok

    # ── knet from the unconditional tracker (V16 maintains _v16_leader_pos for EVERY target fill) ──
    def _v17_knet(self, coin: str, is_buy: bool, exclude_wallet: str, px: float) -> int:
        k = 0
        for (w2, c2), sz2 in self._v16_leader_pos.items():
            if c2 != coin or w2 == exclude_wallet:
                continue
            if abs(sz2) * px < 1.0:        # dust
                continue
            k += 1 if (sz2 > 0) == is_buy else -1
        return k

    # ── signal path: stamp knet at the leader-fill event (replay semantics), then defer to V16 ──
    # codex code-review r1 fixes: (P1.1) stamp ONLY true 0->nonzero open candidates (adds/closes/
    # reverses polluted the stamp map); dedupe by tid BEFORE stamping. (P1.2) FIFO queue per
    # (wallet, coin) instead of a single slot -- a burst of opens cannot overwrite the stamp the
    # async entry task will consume; base cooldown rejects the extras anyway.
    def _on_hl_trade(self, trade: dict):
        coin = trade.get("coin", "")
        users = trade.get("users", [])
        if coin in self.coin_whitelist and len(users) >= 2:
            buyer, seller = users[0].lower(), users[1].lower()
            w = buyer if buyer in self.target_set else (seller if seller in self.target_set else None)
            if w is not None:
                self._v17_last_target_fill_ts = time.time()
                px = float(trade.get("px", 0) or 0)
                tid = trade.get("tid", "")
                if px > 0 and not (tid and tid in self._seen_tids):
                    wallet = self.leader_to_vault.get(w, w)
                    is_buy = (w == buyer)
                    prev = self._v16_leader_pos.get((wallet, coin), 0.0)
                    if abs(prev) * px < 1.0:        # true OPEN candidate only (P1.1)
                        k = self._v17_knet(coin, is_buy, wallet, px)
                        self._v17_knet_pending.setdefault((wallet, coin), []).append((k, time.time()))
                        if len(self._v17_knet_pending) > 500:   # prune stale queues
                            cut = time.time() - 120
                            self._v17_knet_pending = {
                                kk: [e for e in vv if e[1] > cut]
                                for kk, vv in self._v17_knet_pending.items()}
                            self._v17_knet_pending = {kk: vv for kk, vv in
                                                      self._v17_knet_pending.items() if vv}
        return super()._on_hl_trade(trade)

    # ── order path: gate + caps + seed/staleness kills, then defer to V16 (whitelist) ──
    async def _enter_position(self, coin: str, is_buy: bool, twap_dedup_key=None, wallet: str = None,
                              skip_cooldown: bool = False):
        if not self._v17_trading_enabled:
            self._v17_stale_rejects += 1
            logger.warning(f"V17 ENTRY BLOCKED (trading_disabled/seed): {coin} {wallet}")
            return
        # EXPANSION KILL gate (codex req #2/#3): block NEW ENTRIES on a disabled new coin. A coin is
        # disabled by its own per-coin kill or by the expansion-wide kill (which disables ALL new
        # coins). Existing positions on the coin are NOT touched here -- the exit machinery in
        # _check_exits/_exit_position runs independently and closes them normally. No-op when the
        # expansion flag is off (_v17_disabled_coins is always empty then). FAIL-CLOSED: this gate is
        # the first thing checked, so a disabled coin can never reach sizing/exposure logic.
        if coin in self._v17_disabled_coins:
            logger.warning(f"V17 EXPANSION KILL: ENTRY blocked for disabled new coin {coin} "
                           f"(wallet={wallet}); existing position exits normally.")
            return
        # stale-tracker kill: knet is meaningless if we have not seen target flow recently
        age = time.time() - self._v17_last_target_fill_ts
        if age > 30.0:
            self._v17_stale_rejects += 1
            logger.warning(f"V17 STALE-TRACKER: last target fill {age:.0f}s ago; entry blocked {coin}")
            return

        # knet gate: consume the signal-time stamp (FIFO). Missing/expired stamp = REJECT
        # (codex P1.3: recompute-at-entry-time is a different, unvalidated gate; recovery and
        # non-signal paths must not open NEW risk).
        q = self._v17_knet_pending.get((wallet, coin))
        k = None
        while q:
            cand = q.pop(0)
            if (time.time() - cand[1]) < 60.0:
                k = cand[0]
                break
        if q is not None and not q:
            self._v17_knet_pending.pop((wallet, coin), None)
        if k is None:
            self._v17_stale_rejects += 1
            logger.warning(f"V17 NO-STAMP REJECT: {coin} {'BUY' if is_buy else 'SELL'} wallet={wallet} "
                           f"(no fresh signal-time knet; non-signal entries do not open risk)")
            return
        if k < self._v17_knet_min:
            self._v17_knet_rejects += 1
            if self._v17_knet_rejects % 20 == 1:
                logger.info(f"V17 KNET GATE: rejected {coin} {'BUY' if is_buy else 'SELL'} "
                            f"knet={k} (total rejects {self._v17_knet_rejects})")
            try:
                self.db.v17_gate_log.insert_one({
                    "coin": coin, "side": "BUY" if is_buy else "SELL", "knet": k,
                    "wallet": wallet, "action": "rejected",
                    "ts": base.datetime.now(base.timezone.utc)})
            except Exception:
                pass
            return

        # exposure caps from OUR live filled positions PLUS in-flight reservations (codex P1.4:
        # concurrent entry tasks could all pass the cap before any IOC fill lands in positions).
        eq = max(float(getattr(self, "_equity_cache", 0.0) or 0.0), 1.0)
        side_new = 1 if is_buy else -1
        net = float(self._v17_pending_net)
        coin_side = float(self._v17_pending_coin_side.get((coin, side_new), 0.0))
        for p in self.positions:
            if not p.get("filled"):
                continue
            s = 1 if p.get("side") == "BUY" else -1
            net += s * self.order_size
            if p.get("coin") == coin and s == side_new:
                coin_side += self.order_size
        if abs(net + side_new * self.order_size) > self._v17_netx_cap_x * eq \
                and abs(net + side_new * self.order_size) > abs(net):
            self._v17_netx_rejects += 1
            _tag = ""
            if self._v17_expansion_on:                 # codex req #4: NEW vs OLD cap-pressure audit
                self._v17_tag_reject("netx", coin)
                _tag = self._v17_coin_tag(coin) + " "
            logger.info(f"V17 NETX CAP: rejected {_tag}{coin} (net {net:+.0f} cap "
                        f"{self._v17_netx_cap_x}x${eq:.0f}; total {self._v17_netx_rejects})")
            return
        if coin_side + self.order_size > self._v17_coin_side_cap_x * eq:
            self._v17_coinside_rejects += 1
            _tag = ""
            if self._v17_expansion_on:                 # codex req #4
                self._v17_tag_reject("coinside", coin)
                _tag = self._v17_coin_tag(coin) + " "
            logger.info(f"V17 COIN-SIDE CAP: rejected {_tag}{coin} "
                        f"({coin_side:.0f}+{self.order_size:.0f} > {self._v17_coin_side_cap_x}x${eq:.0f}; "
                        f"total {self._v17_coinside_rejects})")
            return

        # record accepted-entry knet for attribution (week-1 KPI: knet-bucket PnL)
        try:
            self.db.v17_gate_log.insert_one({
                "coin": coin, "side": "BUY" if is_buy else "SELL", "knet": k,
                "wallet": wallet, "action": "accepted",
                "ts": base.datetime.now(base.timezone.utc)})
        except Exception:
            pass
        # reserve in-flight exposure for the duration of the entry attempt (codex P1.4)
        self._v17_pending_net += side_new * self.order_size
        self._v17_pending_coin_side[(coin, side_new)] = \
            self._v17_pending_coin_side.get((coin, side_new), 0.0) + self.order_size
        try:
            return await super()._enter_position(coin, is_buy, twap_dedup_key=twap_dedup_key,
                                                 wallet=wallet, skip_cooldown=skip_cooldown)
        finally:
            self._v17_pending_net -= side_new * self.order_size
            _rem = self._v17_pending_coin_side.get((coin, side_new), 0.0) - self.order_size
            if abs(_rem) < 1e-9:
                self._v17_pending_coin_side.pop((coin, side_new), None)   # codex r2 P2: no clutter
            else:
                self._v17_pending_coin_side[(coin, side_new)] = _rem


def main():
    import argparse
    ap = argparse.ArgumentParser(description="HL Copy Trader V17 -- gated herd copy")
    ap.add_argument("--config", default="config/copy_trader_wallets_v17.json")
    ap.add_argument("--size", type=float, default=None)
    ap.add_argument("--shadow", action="store_true")
    args = ap.parse_args()

    config_path = args.config
    if not Path(config_path).is_absolute():
        config_path = str(_REPO / config_path)
    if not Path(config_path).exists():
        logger.error(f"V17 config not found: {config_path}")
        sys.exit(1)

    trader = V17CopyTrader(config_path, order_size_override=args.size, shadow=args.shadow)
    base.asyncio.run(trader.run())


if __name__ == "__main__":
    main()
