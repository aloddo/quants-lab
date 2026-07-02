#!/usr/bin/env python3
"""v25 copy simulation core + roster-level ONE-account portfolio sim (spec-frozen).

Shared sim core (used by BOTH R2 per-wallet scoring and the roster portfolio sim):
- fixed $150/order, FIRST_CLOSE live-parity exit machinery (frozen, gate-b blocker #5;
  parity: strategies/live/hl_copy_trader_v17.py config exit_min_trim_pct =
  full_exit_trim_pct = 0.85 + research/v15/v15_fixed_notional_signals.py lifecycle):
  the copy exit triggers when CUMULATIVE leader reverse flow >= 85% of the accumulated
  copied notional; the close is always FULL (no partial exits exist); leader ADDONs are
  NOT copied but grow the trim denominator; dust threshold $1 (leader residual notional
  < $1 counts as fully closed).
- a leader REVERSAL closes our copy and NEVER opens a new one (canonical lifecycle,
  v15_fixed_notional_signals.py: only a true dust->open transition creates a copy).
  Entries therefore happen ONLY on leader ENTRY actions inside the sim window.
- duplicate-signal coalescing: at most ONE open lot per (wallet, coin); a second entry
  signal for an already-open (wallet, coin) is ignored and counted (live dedup semantics).
  Cross-wallet same-coin stacking is allowed, subject to the account caps.
- 2s repricing (frozen): entry price = close of the FIRST 1m bar closing at
  >= leader_fill_ts + 2000ms, within 60s, else drop-and-count. NEVER a prior bar.
- exit pricing boundary (frozen fallback order, gate-b blocker #5): exit repriced at the
  first mark in [signal+2s, signal+60s]; if none, the first mark >= signal+2s BEFORE the
  window end (counted late); if none before window end, the position falls to terminal
  MTM at the window end (counted unpriced). NO mark beyond the window end is ever read
  (all next_mark calls are capped at end_ms).
- cold-start: only leader journeys whose ENTRY occurs at ts >= sim start are copied
  (pre-window opens and reversal-created legs are never copied).
- terminal MTM at window end with reserved exit costs (exit slip + taker fee at the last
  causal mark <= end).
- Bernoulli p=0.4 entry-dropout stress with event-hash seeding; missed entry => that
  journey's exits are ignored (the lot never existed).

Account caps (portfolio mode only, frozen): initial equity $500; gross notional <= 2.5x
equity ("netx 2.5x", live gross_entry_gate_x semantics); per coin-side notional <= 2x
equity; margin util <= 0.7 with reserve leverage 10x (live margin_reserve_max_lev).

MTM drawdown (frozen, gate-b blocker #6): the equity series starts at the initial $500
and is updated at EVERY simulated event (entry fill, exit fill, day-boundary mark
refresh, terminal close) -- not daily endpoints.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from v25_common import (DROPOUT_P, DUST_USD, EXIT_TRIGGER_FRAC, INITIAL_EQUITY,
                        MAX_COIN_SIDE_X, MAX_GROSS_X, MAX_MARGIN_UTIL, MS_DAY, ORDER_USD,
                        REPRICE_MS, REPRICE_WINDOW_MS, RESERVE_LEV, ExecScenario,
                        MarksIndex, coin_is_spot, event_dropout)

SIM_COLS = ["wallet", "coin", "ts", "action_type", "signed_size", "price", "position_after",
            "journey_id", "opening_journey_id", "closing_journey_id", "is_liquidation"]

COUNTER_KEYS = ["entries", "entries_dropped_no_mark", "entries_dust_skipped",
                "entries_blocked_gross", "entries_blocked_coin_side",
                "entries_blocked_margin", "dup_coalesced", "dropout_dropped",
                "exits_late", "exits_unpriced_to_terminal", "trips_realized",
                "trips_terminal", "reverse_closes", "leader_reduce_ignored_no_lot",
                "journey_mismatch_ignored"]

EPS = 1e-9


@dataclass
class Lot:
    wallet: str
    coin: str
    side: int                    # +1 long, -1 short
    journey_id: int
    size: float                  # coin units, ours (>0)
    entry_px: float
    entry_fill_ts: int
    entry_signal_ts: int
    entry_notional: float
    entry_fee: float
    leader_accumulated: float    # leader's accumulated entry notional (trim denominator)
    leader_reverse: float = 0.0  # CUMULATIVE leader reverse flow (trim numerator)
    exits: list = field(default_factory=list)   # (fill_ts, size, px, fee, late)
    realized: float = 0.0
    fees: float = 0.0

    def notional_at(self, mark: float) -> float:
        return self.size * mark


class CopySim:
    """Event-driven FIRST_CLOSE copy sim over the half-open window [start_ms, end_ms).
    portfolio=True enables the one-account caps + equity/daily accounting;
    portfolio=False is the R2 per-wallet cold-start scorer (no caps, PnL still in $ on
    $150 orders)."""

    def __init__(self, scenario: ExecScenario, marks: MarksIndex, start_ms: int, end_ms: int,
                 portfolio: bool = True, initial_equity: float = INITIAL_EQUITY,
                 order_usd: float = ORDER_USD, dropout_seed: int | None = None,
                 dropout_p: float = DROPOUT_P):
        self.sc = scenario
        self.marks = marks
        self.start_ms = int(start_ms)
        self.end_ms = int(end_ms)
        self.portfolio = portfolio
        self.equity0 = float(initial_equity)
        self.order_usd = float(order_usd)
        self.dropout_seed = dropout_seed
        self.dropout_p = dropout_p
        self.realized = 0.0
        self.lots: dict[tuple, Lot] = {}          # (wallet, coin) -> Lot
        self.copied_journeys: set[tuple] = set()  # (wallet, coin, journey_id) we hold/held
        self.dropped_journeys: set[tuple] = set() # missed entry => exits ignored
        self.trips: list[dict] = []
        self.counters = {k: 0 for k in COUNTER_KEYS}
        self._day_samples: list[tuple] = []       # (day_end_ms, equity)
        self._next_day_ms = ((self.start_ms // MS_DAY) + 1) * MS_DAY
        # event-level MTM DD tracking (frozen): starts at the initial equity
        self._dd_peak = self.equity0
        self._max_dd = 0.0

    # ---- equity / caps / DD ------------------------------------------------------------- #
    def _lot_mark(self, lot: Lot, ts_ms: int) -> float:
        m = self.marks.asof_mark(lot.coin, ts_ms)
        return m if m is not None else lot.entry_px

    def equity(self, ts_ms: int) -> float:
        unreal = 0.0
        for lot in self.lots.values():
            m = self._lot_mark(lot, ts_ms)
            unreal += lot.size * (m - lot.entry_px) * lot.side
        return self.equity0 + self.realized + unreal

    def _dd_update(self, ts_ms: int):
        """Update the event-level MTM drawdown series (EVERY simulated event, frozen)."""
        if not self.portfolio:
            return
        eq = self.equity(ts_ms)
        if eq > self._dd_peak:
            self._dd_peak = eq
        elif self._dd_peak > 0:
            dd = (self._dd_peak - eq) / self._dd_peak
            if dd > self._max_dd:
                self._max_dd = dd

    def _caps_allow(self, coin: str, side: int, ts_ms: int) -> str | None:
        eq = self.equity(ts_ms)
        if eq <= 0:
            return "margin"
        gross = 0.0
        coin_side = 0.0
        for lot in self.lots.values():
            n = lot.notional_at(self._lot_mark(lot, ts_ms))
            gross += n
            if lot.coin == coin and lot.side == side:
                coin_side += n
        if (gross + self.order_usd) / eq > MAX_GROSS_X:
            return "gross"
        if (coin_side + self.order_usd) / eq > MAX_COIN_SIDE_X:
            return "coin_side"
        margin = (gross + self.order_usd) / RESERVE_LEV
        if margin / eq > MAX_MARGIN_UTIL:
            return "margin"
        return None

    # ---- day-boundary equity sampling (mark refresh events) ------------------------------ #
    def _advance_days(self, to_ms: int):
        if not self.portfolio:
            return
        while self._next_day_ms <= min(to_ms, self.end_ms):
            self._day_samples.append((self._next_day_ms, self.equity(self._next_day_ms)))
            self._dd_update(self._next_day_ms)
            self._next_day_ms += MS_DAY

    # ---- events ------------------------------------------------------------------------- #
    def on_entry(self, wallet: str, coin: str, ts: int, journey_id: int, side: int,
                 leader_notional: float):
        self._advance_days(ts)
        key = (wallet, coin)
        jkey = (wallet, coin, journey_id)
        if leader_notional < DUST_USD:
            # live parity: only a true dust->open transition (>= $1) creates a copy
            self.counters["entries_dust_skipped"] += 1
            self.dropped_journeys.add(jkey)
            return
        if key in self.lots:
            self.counters["dup_coalesced"] += 1
            return
        if self.dropout_seed is not None and event_dropout(
                self.dropout_seed, wallet, coin, journey_id, self.dropout_p):
            self.counters["dropout_dropped"] += 1
            self.dropped_journeys.add(jkey)
            return
        if self.portfolio:
            blocked = self._caps_allow(coin, side, ts)
            if blocked is not None:
                self.counters[f"entries_blocked_{blocked}"] += 1
                self.dropped_journeys.add(jkey)
                return
        fill_ts, mark = self.marks.next_mark(coin, ts, REPRICE_MS, REPRICE_WINDOW_MS,
                                             cap_ms=self.end_ms)
        if fill_ts is None:
            self.counters["entries_dropped_no_mark"] += 1
            self.dropped_journeys.add(jkey)
            return
        px = self.sc.entry_px(coin, mark, side > 0)
        size = self.order_usd / px
        fee = self.order_usd * self.sc.fee_oneway(coin)
        self.lots[key] = Lot(wallet=wallet, coin=coin, side=side, journey_id=journey_id,
                             size=size, entry_px=px, entry_fill_ts=fill_ts,
                             entry_signal_ts=ts, entry_notional=self.order_usd, entry_fee=fee,
                             leader_accumulated=max(leader_notional, EPS), fees=fee)
        self.realized -= fee                      # entry fee hits account equity immediately
        self.copied_journeys.add(jkey)
        self.counters["entries"] += 1
        self._dd_update(fill_ts)

    def on_addon(self, wallet: str, coin: str, journey_id: int, leader_notional: float):
        lot = self.lots.get((wallet, coin))
        if lot is not None and lot.journey_id == journey_id:
            lot.leader_accumulated += leader_notional   # grows the trim denominator only

    def _fill_exit(self, lot: Lot, ts: int, close_sz: float) -> bool:
        """Frozen exit-pricing fallback order (gate-b #5): (1) first mark in
        [signal+2s, signal+60s] and <= window end; (2) first mark >= signal+2s and
        <= window end (counted LATE); (3) none -> False, position falls to terminal MTM
        (counted unpriced). A mark beyond end_ms is NEVER read (cap_ms)."""
        fill_ts, mark = self.marks.next_mark(lot.coin, ts, REPRICE_MS, REPRICE_WINDOW_MS,
                                             cap_ms=self.end_ms)
        late = False
        if fill_ts is None:
            fill_ts, mark = self.marks.next_mark(lot.coin, ts, REPRICE_MS, None,
                                                 cap_ms=self.end_ms)
            if fill_ts is None:
                self.counters["exits_unpriced_to_terminal"] += 1
                return False
            late = True
            self.counters["exits_late"] += 1
        px = self.sc.exit_px(lot.coin, mark, lot.side > 0)
        fee = close_sz * px * self.sc.fee_oneway(lot.coin)
        lot.exits.append((fill_ts, close_sz, px, fee, late))
        gain = close_sz * (px - lot.entry_px) * lot.side
        lot.realized += gain
        lot.fees += fee
        lot.size -= close_sz
        self.realized += gain - fee               # account equity realizes fill-by-fill
        self._dd_update(fill_ts)
        return True

    def _emit_trip(self, lot: Lot, terminal: bool):
        # account-level realized already accrued fill-by-fill; this only emits the record
        net = lot.realized - lot.fees
        exit_ts_last = int(lot.exits[-1][0]) if lot.exits else 0
        self.trips.append({
            "wallet": lot.wallet, "coin": lot.coin, "side": int(lot.side),
            "journey_id": int(lot.journey_id),
            "entry_signal_ts": int(lot.entry_signal_ts), "entry_fill_ts": int(lot.entry_fill_ts),
            "entry_px": float(lot.entry_px), "entry_notional": float(lot.entry_notional),
            "exit_fill_ts_last": exit_ts_last,
            "n_exit_fills": int(len(lot.exits)),
            "gross_pnl": float(lot.realized), "fees": float(lot.fees),
            "net_pnl": float(net),
            "net_bps": float(net / lot.entry_notional * 1e4),
            "terminal": bool(terminal),
            "any_late_exit": bool(any(e[4] for e in lot.exits)),
        })
        self.counters["trips_terminal" if terminal else "trips_realized"] += 1

    def on_reduce(self, wallet: str, coin: str, ts: int, reverse_notional: float,
                  leader_after_notional: float, journey_id: int | None = None):
        """Leader reduce flow (TRIM / EXIT / the closing leg of a REVERSE). FIRST_CLOSE
        frozen semantics: accumulate reverse flow; FULL close iff cumulative reverse
        >= 85% of accumulated leader notional OR the leader residual is dust (< $1).
        No partial exits exist."""
        self._advance_days(ts)
        key = (wallet, coin)
        lot = self.lots.get(key)
        if lot is None:
            self.counters["leader_reduce_ignored_no_lot"] += 1
            return
        if journey_id is not None and lot.journey_id != journey_id:
            self.counters["journey_mismatch_ignored"] += 1
            return
        lot.leader_reverse += reverse_notional
        acc = max(lot.leader_accumulated, EPS)
        frac = lot.leader_reverse / acc
        if frac + EPS < EXIT_TRIGGER_FRAC and leader_after_notional >= DUST_USD:
            return                                # threshold not reached; keep holding
        if not self._fill_exit(lot, ts, lot.size):
            return                                # no priceable mark; terminal MTM settles it
        self._emit_trip(lot, terminal=False)
        del self.lots[key]

    def finish(self):
        """Terminal MTM at window end with reserved exit costs (frozen)."""
        self._advance_days(self.end_ms - 1)   # final sample is taken AFTER terminal close
        for key in sorted(self.lots.keys()):
            lot = self.lots[key]
            m = self.marks.asof_mark(lot.coin, self.end_ms)
            if m is None:
                m = lot.entry_px            # fail-closed: flat mark, still pay exit costs
            px = self.sc.exit_px(lot.coin, m, lot.side > 0)
            fee = lot.size * px * self.sc.fee_oneway(lot.coin)
            lot.exits.append((self.end_ms, lot.size, px, fee, False))
            gain = lot.size * (px - lot.entry_px) * lot.side
            lot.realized += gain
            lot.fees += fee
            lot.size = 0.0
            self.realized += gain - fee
            self._emit_trip(lot, terminal=True)
        self.lots.clear()
        if self.portfolio:
            self._dd_update(self.end_ms)
            if not self._day_samples or self._day_samples[-1][0] < self.end_ms:
                self._day_samples.append((self.end_ms, self.equity(self.end_ms)))

    # ---- driver ------------------------------------------------------------------------- #
    def run(self, actions: pd.DataFrame) -> dict:
        """actions: merged (multi-)wallet m02 action rows, ts-sorted ascending. Rows at
        ts >= end_ms are never processed (half-open window). Cold-start: only ENTRY
        actions at ts >= start_ms create copies; a REVERSE never opens (frozen)."""
        a = actions[~actions["coin"].map(coin_is_spot)]
        a = a.sort_values(["ts", "wallet", "coin"], kind="mergesort")
        has_cids = "closing_journey_id" in a.columns
        for r in a.itertuples(index=False):
            ts = int(r.ts)
            if ts >= self.end_ms:
                break
            at = r.action_type
            px = float(r.price) if r.price == r.price else 0.0
            notional = abs(float(r.signed_size)) * px
            after_notional = abs(float(r.position_after)) * px
            jid = int(r.journey_id)
            jkey = (r.wallet, r.coin, jid)
            if at == "ENTRY":
                if ts < self.start_ms:
                    continue                      # cold-start: pre-window open ignored
                side = 1 if float(r.position_after) > 0 else -1
                self.on_entry(r.wallet, r.coin, ts, jid, side, notional)
            elif at == "REVERSE":
                # frozen: a reversal CLOSES our copy and NEVER opens a new one.
                # The closing leg's flow = the leader position crossed to zero.
                closed_notional = abs(float(r.position_after) - float(r.signed_size)) * px
                cjid = None
                if has_cids and r.closing_journey_id == r.closing_journey_id:
                    cjid = int(r.closing_journey_id)
                lot = self.lots.get((r.wallet, r.coin))
                if lot is not None:
                    self.counters["reverse_closes"] += 1
                self.on_reduce(r.wallet, r.coin, ts,
                               max(closed_notional,
                                   lot.leader_accumulated if lot is not None else 0.0),
                               0.0, journey_id=cjid)
            elif at == "ADDON":
                self.on_addon(r.wallet, r.coin, jid, notional)
            elif at in ("TRIM", "EXIT"):
                if jkey in self.dropped_journeys:
                    continue                      # missed entry => exits ignored (frozen)
                self.on_reduce(r.wallet, r.coin, ts, notional, after_notional,
                               journey_id=jid)
        self.finish()
        return self.result()

    def result(self) -> dict:
        trips = pd.DataFrame(self.trips)
        out = {"trips": trips, "counters": dict(self.counters),
               "total_pnl": float(trips["net_pnl"].sum()) if len(trips) else 0.0}
        if self.portfolio:
            days = pd.DataFrame(self._day_samples, columns=["day_end_ms", "equity"])
            days["date"] = pd.to_datetime(days["day_end_ms"], unit="ms").dt.normalize()
            days["daily_pnl"] = days["equity"].diff()
            if len(days):
                days.loc[days.index[0], "daily_pnl"] = days["equity"].iloc[0] - self.equity0
            out["daily"] = days
            out["final_equity"] = float(days["equity"].iloc[-1]) if len(days) else self.equity0
            out["max_mtm_dd_frac"] = float(self._max_dd)
        return out


def simulate_wallet_trips(wdf: pd.DataFrame, scenario: ExecScenario, marks: MarksIndex,
                          start_ms: int, end_ms: int) -> dict:
    """R2 scoring core: fixed-$150 FIRST_CLOSE cold-start copy of ONE wallet, no account
    caps. Returns {trips, counters, total_pnl}. Realized trips only feed the R2 LCB
    (terminal-MTM trips are reported but excluded from the >= 50-trip ranking sample;
    documented resolution -- open-bag risk is handled by the common gate)."""
    sim = CopySim(scenario, marks, start_ms, end_ms, portfolio=False)
    return sim.run(wdf)


def simulate_portfolio(actions: pd.DataFrame, scenario: ExecScenario, marks: MarksIndex,
                       start_ms: int, end_ms: int, dropout_seed: int | None = None) -> dict:
    """Roster-level ONE-account sim (frozen caps). actions = merged roster action rows."""
    sim = CopySim(scenario, marks, start_ms, end_ms, portfolio=True,
                  dropout_seed=dropout_seed)
    return sim.run(actions)
