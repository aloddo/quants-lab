#!/usr/bin/env python3
"""Balanced cohort STEP 4 -- SHIP-GATE netted-subaccount copy SIM (DESIGN CONVERGED, codex r1+r2).

Brain spec: projects/quant/v15/balanced-subaccount-sim-plan.

WHAT THIS IS
  Simulate ONE netted Hyperliquid account that copies N leader wallets at a FIXED $150 budget each,
  per calendar-month OOS fold. Leaders are chosen TRAIN-ONLY via balanced_step3_riskparity.select_fold
  over the prior months. Fills are priced through the CANONICAL execution_model.py (per-coin L2 slip +
  real HL fee + 2000ms latency -- the ONLY allowed fill pricer; no hardcoded slip/fee/latency).

  This is DELTA-DRIVEN netted copy (NOT continuous rebalance-to-target; codex r1->r2 fix): we copy the
  leader's signed-exposure DELTAS. Between leader actions the account HOLDS -- only hourly MTM / stop /
  gross-cap move equity. Opposite legs across leaders NET in the shared book (the whole point of the
  subaccount abstraction).

EVENT MODEL (causal)
  events = union of:
    (a) chosen leaders' m02_actions rows within [month_start, month_end), sorted by (ts, event_order);
    (b) hourly ticks for MTM / latched stop / gross-cap evaluation.
  Per-(leader,coin) we forward-fill the leader's last target exposure fraction (tgt_frac), RESET to 0
  per fold (no carry across the fold boundary; carried pre-fold positions are NOT seeded -- a leader's
  contribution starts flat at fold open, which is the conservative causal choice).

  On a leader L action at ts in coin C:
    new_tgt_frac = 0 if (is_liquidation or position_after==0 or action_type is an exit) else target_exposure_pct
    delta_frac   = new_tgt_frac - last_tgt_frac[(L,C)]
    INTENT IS FROZEN AT ts: the follower size delta in coin units is
        size_delta = 150 * delta_frac / mark_at(C, ts)         # mark observed AT ts (no look-ahead)
    THE FILL PRICE is delayed by LAT_MS (2s copy latency):
        fill_mark  = mark_at(C, ts + LAT_MS)                   # price only is delayed; target NOT recomputed
    Apply size_delta to the account NET position net_pos[C] (true netting across leaders). Cash pays a
    one-way taker fee + one-way slip on the traded notional |size_delta * fill_mark|.

  BETWEEN leader actions: NO trades. Hourly tick only marks equity:
        equity = cash + sum_C net_pos[C] * mark_at(C, h)

ACCOUNT RAILS (account level; live-parity, pre-registered -- not fitted)
  (1) latched equity stop: if equity <= 0.85*start_equity -> DE-RISK flag (we simply stop adding NEW
      gross beyond current, i.e. treat like the gross cap binding); if equity <= 0.75*start_equity ->
      FLATTEN all net positions at the current mark and STOP copying for the rest of the fold (latched).
  (2) aggregate gross cap: block NEW entry deltas (those that INCREASE sum_C |net_pos*mark|) when gross
      already > 4.0*equity. Deltas that REDUCE gross are always allowed.

METRICS per fold
  hourly equity array -> ROE = equity[-1]/start_equity - 1
                         MTM-DD = max peak-to-trough on the hourly path
                         realized beta = OLS slope of diff(hourly equity) on hourly BTC return
  start_equity = 150 * n_leaders.
GATE (report only -- NOT a final PASS/FAIL claim): MTM-DD <= 12% ALL folds AND ROE > 0 in >= 4/5 folds.

MEMORY: m02_actions (4.6GB) is NEVER fully loaded. Per fold we pyarrow-scan ONLY the chosen leaders'
rows (wallet isin filter + column projection) for that month -- a small slice. Marks are page-cached by
leadlag_clean_rank_sim once.
"""
import sys
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

sys.path.insert(0, str(Path(__file__).resolve().parent))

import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
from balanced_step3_riskparity import (
    select_fold, build_legpool, btc_ret_hourly, MONTHS, REGIME, HR,
)

# --- canonical execution params (no hardcoded numbers -- all from execution_model) -------------- #
FEE_OW = fee_rt(maker=False) / 2.0       # one-way taker fee per FILL (fee_rt is round-trip)
LAT_MS = 2000                            # 2s copy latency: fill price delayed, intent frozen at ts

# --- account rails (pre-registered, not fitted to any fold) ------------------------------------ #
STOP_DERISK = 0.85                       # equity/start below this -> stop ADDING new gross
STOP_FLATTEN = 0.75                      # equity/start below this -> flatten + sit out (latched)
GROSS_CAP = 4.0                          # block NEW gross above 4.0x equity

ACTIONS_PATH = "app/data/v15/m02_actions.parquet"
_EXIT_TYPES = {"close", "exit", "reduce_close", "flatten", "liquidation"}


def month_bounds(tm):
    start = int(pd.Timestamp(tm + "-01").value // 1e6)
    nxt = pd.Timestamp(tm + "-01") + pd.offsets.MonthBegin(1)
    end = int(nxt.value // 1e6)
    return start, end


def load_actions_for_leaders(wallets, tm, win=None):
    """Stream ONLY the chosen leaders' m02_actions rows in month tm (wallet isin + ts range + column
    projection). Never loads the full 4.6GB file. Returns a small DataFrame sorted by (ts, event_order).

    win=(start_ms, end_ms) overrides month_bounds(tm) with an explicit [start, end) window. When win is
    None (the default), behavior is byte-identical to the original month-based slice."""
    if not wallets:
        return pd.DataFrame(
            columns=["wallet", "coin", "ts", "event_order", "action_type", "signed_size",
                     "position_after", "mark", "target_exposure_pct", "is_liquidation"])
    start, end = month_bounds(tm) if win is None else (int(win[0]), int(win[1]))
    dset = pads.dataset(ACTIONS_PATH)
    flt = (pc.field("wallet").isin(pa.array(list(wallets)))
           & (pc.field("ts") >= pa.scalar(start))
           & (pc.field("ts") < pa.scalar(end)))
    cols = ["wallet", "coin", "ts", "event_order", "action_type", "signed_size",
            "position_after", "mark", "target_exposure_pct", "is_liquidation"]
    tbl = dset.to_table(columns=cols, filter=flt)
    df = tbl.to_pandas()
    if df.empty:
        return df
    df = df.sort_values(["ts", "event_order"], kind="mergesort").reset_index(drop=True)
    return df


def is_exit_action(action_type, position_after, is_liquidation):
    if bool(is_liquidation):
        return True
    if position_after is not None and abs(float(position_after)) < 1e-12:
        return True
    at = (action_type or "").lower()
    return any(tok in at for tok in _EXIT_TYPES)


def simulate_fold(long_wallets, short_wallets, tm, btc_hourly, n_sleeves, budget,
                  win=None, faithful_copy=False):
    """Delta-driven netted copy sim over month tm. Returns (hourly_equity_array, hourly_grid, btc_ret_arr).

    WINDOW (added 2026-06-24, default-preserving): win=(start_ms, end_ms) runs over an explicit window
    instead of month_bounds(tm). win=None -> byte-identical month behavior.

    FAITHFUL-COPY MODE (added 2026-06-24, default-preserving): faithful_copy=False keeps the original
    BALANCED-SLEEVE behavior (long sleeve copies only target>0, short only target<0; budget keyed
    (wallet, sleeve_is_long)). faithful_copy=True copies each leader's FULL SIGNED target_exposure_pct
    directly (both sides) -- the LIVE V17 exposure-copy semantics. In faithful mode each wallet is ONE
    sleeve; pass it in long_wallets, leave short_wallets empty, key budget on (wallet, True). The sign
    filter is bypassed so a single signed running target per (wallet, coin) tracks the leader exactly.
    All look-ahead guards (intent frozen at ts, fill at ts+LAT_MS) and execution_model pricing are
    IDENTICAL across both modes.

    SLEEVE IDENTITY (codex bug-1 fix): each leader is selected into the LONG sleeve, the SHORT sleeve,
    or BOTH (independent $150 sleeves). A LONG-sleeve leader copies ONLY its long exposure (signed
    target_exposure_pct > 0, and exits from a prior positive target); a SHORT-sleeve leader copies ONLY
    its short exposure (target < 0). When a source action's target has the WRONG sign for the sleeve, the
    sleeve's target for that coin is treated as 0 (sleeve flat in that coin). Running target keyed on
    (wallet, sleeve_is_long, coin). target_exposure_pct is signed upstream (NOT re-signed).

    EVENT MODEL (codex bug-3 fix): action-intent fires at ts (compute frozen size delta + rails), and the
    FILL effect is scheduled at ts+LAT_MS; intents, fills and hourly ticks are processed in timestamp
    order via a min-heap, so the account never holds a position before its fill exists, and an hourly tick
    at h samples only fills whose fill-time <= h.

    GROSS CAP (codex bug-2 fix): the cap clips on PROJECTED post-trade gross (not current gross). An
    entry-side delta is clipped to the available 4x headroom (deploy what fits, not all-or-nothing); the
    per-sleeve COPIED target is advanced only by the fraction actually executed (source intent vs executed
    target kept separate) so a clipped entry cannot desync into a phantom reversal.
    """
    import heapq
    start, end = month_bounds(tm) if win is None else (int(win[0]), int(win[1]))
    grid = list(range((start // HR) * HR, end, HR))
    start_equity = 150.0 * n_sleeves

    wallets = set(long_wallets) | set(short_wallets)
    acts = load_actions_for_leaders(wallets, tm, win=win)

    # account state
    cash = start_equity
    net_pos = defaultdict(float)              # coin -> signed size (shared netted book)
    # per-sleeve targets keyed (wallet, sleeve_is_long, coin); reset per fold.
    src_tgt = defaultdict(float)              # source-intent target fraction (what the leader signals)
    copied_tgt = defaultdict(float)           # target we ACTUALLY copied (advances only by executed amount)
    latched_flat = [False]                    # latched -25% stop hit -> sit out rest of fold

    def gross_with(ts, extra_coin=None, extra_delta=0.0):
        g = 0.0
        for c, sz in net_pos.items():
            s = sz + (extra_delta if c == extra_coin else 0.0)
            if s == 0.0:
                continue
            m = S.mark_at(c, ts)
            if m:
                g += abs(s * m)
        if extra_coin is not None and extra_coin not in net_pos and extra_delta != 0.0:
            m = S.mark_at(extra_coin, ts)
            if m:
                g += abs(extra_delta * m)
        return g

    def equity_at(ts):
        e = cash
        for c, sz in net_pos.items():
            if sz == 0.0:
                continue
            m = S.mark_at(c, ts)
            if m:
                e += sz * m
        return e

    def flatten_all(ts):
        nonlocal cash
        for c, sz in list(net_pos.items()):
            if sz == 0.0:
                continue
            m = S.mark_at(c, ts)
            if not m:
                continue
            notional = abs(sz * m)
            cash += sz * m                       # realize the position into cash
            cash -= notional * (FEE_OW + slip_oneway(c))
            net_pos[c] = 0.0
        latched_flat[0] = True

    # --- build the time-ordered event heap ------------------------------------------------------- #
    # event kinds (priority at equal time): 0=fill (realize prior intent first), 1=action-intent,
    # 2=hourly tick (samples AFTER fills+intents at that exact ms). seq breaks remaining ties stably.
    heap = []  # (effective_ts, kind, seq, payload)
    seq = 0
    for h in grid:
        heap.append((h, 2, seq, h)); seq += 1
    for row in acts.itertuples(index=False):
        ts = int(row.ts)
        wallet = row.wallet
        coin = row.coin
        is_exit = is_exit_action(row.action_type, row.position_after, row.is_liquidation)
        tgt_pct = row.target_exposure_pct
        has_tgt = tgt_pct is not None and not pd.isna(tgt_pct)
        sval = float(tgt_pct) if has_tgt else None
        # expand into per-sleeve action-intents for whichever sleeve(s) this wallet belongs to
        for sleeve_long in (True, False):
            if sleeve_long and wallet not in long_wallets:
                continue
            if (not sleeve_long) and wallet not in short_wallets:
                continue
            heap.append((ts, 1, seq, (wallet, coin, sleeve_long, is_exit, has_tgt, sval)))
            seq += 1
    heapq.heapify(heap)

    eq_path = np.empty(len(grid), dtype=np.float64)
    gi = 0

    while heap:
        eff_ts, kind, _seq, payload = heapq.heappop(heap)

        if kind == 0:
            # FILL effect lands now (price was frozen at intent time inside the payload)
            if latched_flat[0]:
                continue
            coin, size_delta, fill_mark = payload
            if size_delta == 0.0:
                continue
            notional = abs(size_delta * fill_mark)
            net_pos[coin] += size_delta
            cash -= size_delta * fill_mark                # buy => cash down; sell => cash up
            cash -= notional * (FEE_OW + slip_oneway(coin))
            continue

        if kind == 2:
            # hourly tick: evaluate latched stop on book state of all fills so far, then sample equity
            h = payload
            e = equity_at(h)
            if not latched_flat[0] and e <= STOP_FLATTEN * start_equity:
                flatten_all(h)
                e = equity_at(h)
            eq_path[gi] = e
            gi += 1
            continue

        # kind == 1: ACTION INTENT (frozen at eff_ts)
        if latched_flat[0]:
            continue
        ts = eff_ts
        wallet, coin, sleeve_long, is_exit, has_tgt, sval = payload

        # account stop check at this intent ts (latched flatten on current marks)
        eq_now = equity_at(ts)
        if eq_now <= STOP_FLATTEN * start_equity:
            flatten_all(ts)
            continue
        derisk = eq_now <= STOP_DERISK * start_equity

        skey = (wallet, sleeve_long, coin)
        if is_exit or not has_tgt:
            # exit / no-target row -> if we had this sleeve on in this coin, flatten it; else stay flat.
            new_src = 0.0
        elif faithful_copy:
            # FAITHFUL-COPY: copy the leader's FULL SIGNED target directly (both sides), no sign filter.
            new_src = sval
        else:
            # SLEEVE-FILTERED source target: only this sleeve's signed exposure is copyable.
            # wrong sign for this sleeve -> sleeve is flat in this coin (target 0); right sign -> copy it.
            if (sleeve_long and sval > 0.0) or ((not sleeve_long) and sval < 0.0):
                new_src = sval
            else:
                new_src = 0.0
        old_src = src_tgt[skey]
        src_tgt[skey] = new_src
        # the desired copied delta_frac is measured against what we ACTUALLY copied so far (not src),
        # so a previously clipped/blocked entry stays consistent.
        old_cop = copied_tgt[skey]
        desired_frac = new_src - old_cop
        if desired_frac == 0.0:
            continue

        # INTENT FROZEN AT ts -- size computed from mark AT ts
        mk_now = S.mark_at(coin, ts)
        if not mk_now or mk_now <= 0:
            continue
        # WEIGHTED COPY: each sleeve's $ budget is proportional to its step3 risk-parity weight W
        # (normalized so total deployed == the flat-$150 total). This reproduces the exact step3
        # construction that produced |beta|<=0.07; flat-$150 discarded W and was structurally net-long.
        sleeve_budget = budget.get((wallet, sleeve_long), 0.0)
        if sleeve_budget == 0.0:
            continue
        size_delta = sleeve_budget * desired_frac / mk_now
        if size_delta == 0.0:
            continue

        prev = net_pos[coin]
        proposed = prev + size_delta
        increasing = abs(proposed) > abs(prev) + 1e-15

        exec_delta = size_delta
        if increasing:
            if derisk:
                # de-risk: block the entry-side delta entirely (do not advance copied target)
                continue
            # PROJECTED post-trade gross must respect 4x; clip the delta to available headroom.
            cap = GROSS_CAP * max(eq_now, 1e-9)
            proj_gross = gross_with(ts, coin, size_delta)
            if proj_gross > cap:
                cur_gross = gross_with(ts)
                headroom = cap - cur_gross
                if headroom <= 0.0:
                    continue                              # already at/over cap -> no entry room
                # scale the size delta down so projected gross == cap (in this coin's contribution)
                frac_fit = headroom / max(proj_gross - cur_gross, 1e-12)
                frac_fit = max(0.0, min(1.0, frac_fit))
                exec_delta = size_delta * frac_fit
                if exec_delta == 0.0:
                    continue

        # advance the COPIED target only by the fraction we actually executed
        copied_tgt[skey] = old_cop + desired_frac * (exec_delta / size_delta)

        # FILL PRICE delayed by latency (price only; target already frozen at ts). Schedule fill at ts+LAT.
        fill_mark = S.mark_at(coin, ts + LAT_MS)
        if not fill_mark or fill_mark <= 0:
            fill_mark = mk_now                            # fallback: no future mark -> use ts mark
        nonlocal_seq = (eff_ts + LAT_MS, 0, _seq, (coin, exec_delta, fill_mark))
        heapq.heappush(heap, nonlocal_seq)

    # if any grid points remain unsampled (e.g. no events past them), sample on current book
    while gi < len(grid):
        eq_path[gi] = equity_at(grid[gi])
        gi += 1

    br = np.array([btc_hourly.get(h, 0.0) for h in grid], dtype=np.float64)
    return eq_path, np.array(grid, dtype=np.int64), br, start_equity


def fold_metrics(eq, br, start_equity):
    roe = float(eq[-1] / start_equity - 1.0) * 100.0
    pk = np.maximum.accumulate(eq)
    dd = float(((pk - eq) / pk).max()) * 100.0
    # realized beta: regress the account's hourly FRACTIONAL return on BTC's hourly return so beta is
    # unitless and directly comparable to step3 (which normalizes equity to 1.0). Using diff of the
    # dollar equity would yield a $-per-unit slope (~9000x inflated) -- that is the smoke beta=7334 bug.
    eq_norm = eq / start_equity
    pr = np.diff(eq_norm)
    b = br[1:]
    rbeta = float(np.polyfit(b, pr, 1)[0]) if np.std(b) > 1e-9 else 0.0
    return roe, dd, rbeta


def run_one_fold(legs, exph, btc_by_mon, ti, verbose=False):
    tm = MONTHS[ti]
    prior = MONTHS[:ti]
    chL, chS, _ = select_fold(legs, exph, prior, btc_by_mon)
    # sleeve identity carried through the sim (codex bug-1): a leader in the LONG sleeve copies only its
    # long exposure, a leader in the SHORT sleeve only its short. A wallet may be in BOTH sleeves (two
    # independent $150 sleeves); n_sleeves (= len(chL)+len(chS)) drives start_equity, not the dedup count.
    long_wallets = set(w for (w, _W, _b) in chL)
    short_wallets = set(w for (w, _W, _b) in chS)
    n_sleeves = len(chL) + len(chS)
    wallets = long_wallets | short_wallets
    # WEIGHTED-COPY budget per sleeve (wallet, is_long): proportional to the step3 risk-parity weight W,
    # normalized so the TOTAL deployed budget == the flat-$150 total (150*n_sleeves). Keeps the account
    # bankroll + total exposure identical to the flat run; only the long/short distribution changes to the
    # risk-parity 50/50 that creates neutrality. start_equity unchanged (= 150*n_sleeves).
    W_all = sum(_W for (_w, _W, _b) in chL) + sum(_W for (_w, _W, _b) in chS)
    TOTAL_BUDGET = 150.0 * n_sleeves
    budget = {}
    if W_all > 0:
        for (w, _W, _b) in chL:
            budget[(w, True)] = TOTAL_BUDGET * _W / W_all
        for (w, _W, _b) in chS:
            budget[(w, False)] = TOTAL_BUDGET * _W / W_all
    bh = btc_by_mon[tm]
    eq, grid, br, start_equity = simulate_fold(long_wallets, short_wallets, tm, bh, n_sleeves, budget)
    roe, dd, rbeta = fold_metrics(eq, br, start_equity)
    if verbose:
        acts = load_actions_for_leaders(wallets, tm)
        print(f"  [smoke] fold={tm} regime={REGIME[tm]} n_sleeves={n_sleeves} "
              f"(L={len(chL)} S={len(chS)}, {len(wallets)} unique wallets) "
              f"n_actions={len(acts)} start_equity=${start_equity:.0f}", flush=True)
        print(f"  [smoke] ROE={roe:.2f}%  MTM-DD={dd:.2f}%  beta={rbeta:.3f}  end_equity=${eq[-1]:.0f}",
              flush=True)
    return tm, REGIME[tm], roe, dd, rbeta, n_sleeves


def load_legpool():
    """Replicates balanced_step3_riskparity.main() loading preamble (m02_journeys -> legs/exph)."""
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j.dropna(subset=["entry_ts", "exit_ts"])
    j = j[(j.max_position_notional > 10) & (j.exit_ts > j.entry_ts)]
    j["is_long"] = j.side.str.lower().str.contains("long")
    j["mon"] = pd.to_datetime(j.entry_ts, unit="ms").dt.strftime("%Y-%m")
    j = j[j.mon.isin(MONTHS)]
    vc = j.groupby("wallet").size()
    keep = set(vc[vc >= 50].index)
    j = j[j.wallet.isin(keep)]
    print(f"pool: {len(keep)} wallets (>=50 journeys), {len(j)} journeys. Marking once ...", flush=True)
    legs, exph = build_legpool(j)
    print(f"marked {sum(len(v) for v in legs.values())} journey-legs over "
          f"{len({k[0] for k in legs})} wallets", flush=True)
    btc_by_mon = {m: btc_ret_hourly(int(pd.Timestamp(m + '-01').value // 1e6),
                                    int((pd.Timestamp(m + '-01') + pd.offsets.MonthBegin(1)).value // 1e6))
                  for m in MONTHS}
    return legs, exph, btc_by_mon


def main():
    smoke = "--smoke" in sys.argv
    legs, exph, btc_by_mon = load_legpool()

    if smoke:
        print("\n=== SMOKE: first OOS fold only (MONTHS[1]) ===", flush=True)
        run_one_fold(legs, exph, btc_by_mon, 1, verbose=True)
        return

    print("\nBALANCED netted-subaccount copy SIM (delta-driven, $150/leader, "
          "canonical execution). ROE/MTM-DD/realized-beta per OOS fold.")
    header = f"{'fold':>9}{'reg':>6}{'ROE%':>9}{'MTM-DD%':>10}{'beta':>8}{'n_leaders':>11}"
    print(header, flush=True)
    print("-" * len(header), flush=True)
    rows = []
    for ti in range(1, len(MONTHS)):
        tm, reg, roe, dd, rbeta, n = run_one_fold(legs, exph, btc_by_mon, ti)
        rows.append((tm, reg, roe, dd, rbeta, n))
        print(f"{tm:>9}{reg:>6}{roe:>9.2f}{dd:>10.2f}{rbeta:>8.3f}{n:>11}", flush=True)

    print("-" * len(header), flush=True)
    dds = [r[3] for r in rows]
    roes = [r[2] for r in rows]
    n_pos = sum(1 for x in roes if x > 0)
    max_dd = max(dds) if dds else float("nan")
    gate = (max_dd <= 12.0) and (n_pos >= 4)
    verdict = "PASS" if gate else "FAIL"
    print(f"\nGATE (report only -- codex result review pending):", flush=True)
    print(f"  max MTM-DD across folds = {max_dd:.2f}%   (gate: <= 12%)", flush=True)
    print(f"  folds with ROE > 0      = {n_pos}/{len(roes)}   (gate: >= 4/5)", flush=True)
    print(f"  mean ROE                = {np.mean(roes):.2f}%", flush=True)
    print(f"  max |beta|              = {max(abs(r[4]) for r in rows):.3f}", flush=True)
    print(f"  VERDICT: {verdict}", flush=True)


if __name__ == "__main__":
    main()
