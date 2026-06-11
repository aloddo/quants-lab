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
                try:
                    payload = {"type": "clearinghouseState", "user": addr}
                    r = base.requests.post(f"{base.HL_API}/info", json=payload, timeout=8)
                    data = r.json()
                    if data is None:
                        still_failed.add(addr)
                        continue
                    self._target_positions.setdefault(addr, {})
                    for p in data.get("assetPositions", []):
                        pos = p["position"]
                        self._target_positions[addr][pos["coin"]] = float(pos["szi"])
                        if pos["coin"] in self.coin_whitelist and abs(float(pos["szi"])) > 1e-12:
                            self._v16_leader_pos[(addr, pos["coin"])] = float(pos["szi"])
                    time.sleep(0.5)
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
            logger.info(f"V17 NETX CAP: rejected {coin} (net {net:+.0f} cap "
                        f"{self._v17_netx_cap_x}x${eq:.0f}; total {self._v17_netx_rejects})")
            return
        if coin_side + self.order_size > self._v17_coin_side_cap_x * eq:
            self._v17_coinside_rejects += 1
            logger.info(f"V17 COIN-SIDE CAP: rejected {coin} ({coin_side:.0f}+{self.order_size:.0f} "
                        f"> {self._v17_coin_side_cap_x}x${eq:.0f}; total {self._v17_coinside_rejects})")
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
