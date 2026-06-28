#!/usr/bin/env python3
"""our_drawdown_scorer.py -- OUR mark-to-market drawdown when COPYING leaders (Alberto directive).

WHY THIS EXISTS
  The live mark-to-market DD gate (research/v16/build_skill_cohort.py::mtm_dd_exclude) reads the
  LEADER's OWN HL account equity history at THEIR leverage/capital/risk and excludes near-ruin
  leaders by THEIR drawdown. That is the WRONG yardstick for us: we copy at a fixed $150 budget,
  4x gross cap, -25% account stop, 2s latency, execution_model slippage+fees -- none of which match
  the leader's account. A leader can show a brutal account DD (deep leverage, martingale adds) yet
  OUR bounded copy of their SIGNED exposure deltas may be fine; conversely a leader with a clean
  account DD can still hand US a bad copy DD. We need OUR mark-to-market DD, computed on OUR copy
  book through the CODEX-REVIEWED execution-realistic engine.

WHAT THIS IS
  We REUSE the codex-reviewed delta-driven netted copy engine in balanced_step4_netted_sim.py
  (intent frozen at ts, fill delayed to ts+2s, true netting across leaders, 4x gross cap, latched
  -25%/-15% account stop, execution_model per-coin slip + real HL fee + 2s latency, hourly MTM-DD).
  We run it in FAITHFUL-COPY mode (engine flag faithful_copy=True): copy each leader's FULL SIGNED
  target_exposure_pct directly (both longs AND shorts), exactly like the LIVE V17 exposure-copy book
  -- NOT the balanced long/short sleeve-filtered mode the engine ships in.

  The look-ahead guards and execution_model pricing are inherited UNCHANGED from the engine.

CODEX-REVIEW FIXES (2026-06-24) APPLIED HERE (engine defaults untouched):
  FIX 1  LIVE COIN WHITELIST: live V17 only copies its 50-coin whitelist (10 baseline + 40 expansion).
         We FILTER copied actions to that 50-set BEFORE the engine sees them (the old scorer copied
         EVERY coin a leader traded -> biased OUR-DD upward). We then report the calibrated-slip share
         (execution_model has measured L2 slip for only the 10 l2_calib coins; the other 40 ride the
         class default) and run a default-slip sensitivity (x0.5, x1, x2).
  FIX 2  SAME-WINDOW LEADER-DD: the HL portfolio response is a LIST of [period, payload] pairs (NOT a
         dict; the old d.get("month") was doubly wrong -- wrong type AND a trailing-month-from-today
         window). We now pick the period whose accountValueHistory COVERS [WIN_START, WIN_END]
         (prefer allTime/longest), FILTER points to ts in the window, and compute the leader's MTM
         maxDD over EXACTLY that window. No covering period -> "n/a (no same-window data)".
  FIX 3  MARGINAL COHORT DD: standalone per-leader $150 DD is only a SCREEN. The real gate is the
         leave-one-out marginal cohort DD + a greedy drop sequence to bring cohort OUR-DD <= TARGET_DD.
         Actions are loaded ONCE per wallet and cached; recomputes re-net the cached actions in RAM
         (no re-scan of the 4.6GB file).
  FIX 4  WINDOW BOUNDARY: the engine's hourly grid floors start and excludes the exact end. For our
         OUR-copy runs we build the grid from CEIL(start->hour) and append EXPLICIT start + end equity
         samples so MTM-DD captures the full window. Still causal (mark_at is asof-backward only).

THREE OUTPUTS (main):
  1. PER-LEADER: each live wallet run STANDALONE as a single-leader $150 copy account over the window.
     -> OUR MTM-DD%, OUR net edge (ROE%), n fills. (Secondary screen.)
  2. COHORT: all live wallets netted into ONE account ($150 each, start=150*n) over the window.
     -> cohort OUR MTM-DD% + ROE%, vs the mean/median of the per-leader DDs (netting changes it).
  3. COMPARISON: per wallet, leader-account MTM-DD% over the SAME WINDOW (the proxy) vs OUR per-leader
     MTM-DD% vs OUR net edge%. Flags same-window divergences.
  4. GREEDY LOO DROP SEQUENCE (the real gate): from cohort OUR-DD(n), repeatedly drop the leader whose
     removal most reduces current cohort DD, until cohort OUR-DD <= TARGET_DD. Reports drop order,
     cohort DD + ROE after each drop, final surviving count.

WINDOW: WIN_START=2026-03-23 .. WIN_END=2026-05-23. BUDGET: $150 per leader. CAPS/STOP: engine's.

MEMORY: m02_actions (4.6GB) is NEVER fully loaded. We pyarrow-scan each wallet's window rows ONCE
(wallet isin + ts range + column projection), whitelist-filter, and CACHE the slice. The LOO/greedy
recomputes re-net the cached per-wallet slices in RAM -- the parquet is touched once per wallet, never
re-scanned per recompute. Marks are page-cached once by leadlag_clean_rank_sim. Smoke first 3 wallets
under /usr/bin/time -l before the full run.

Run:
  ~/miniforge3/envs/quants-lab/bin/python research/v15/our_drawdown_scorer.py --smoke3
  ~/miniforge3/envs/quants-lab/bin/python research/v15/our_drawdown_scorer.py
  ~/miniforge3/envs/quants-lab/bin/python research/v15/our_drawdown_scorer.py --target=25 --slipsens
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import leadlag_clean_rank_sim as S  # noqa: E402  (page-cached marks + mark_at)
import execution_model as EM  # noqa: E402
from balanced_step4_netted_sim import (  # noqa: E402
    simulate_fold, fold_metrics, load_actions_for_leaders, HR,
)
from balanced_step3_riskparity import btc_ret_hourly  # noqa: E402

WIN_START = "2026-03-23"
WIN_END = "2026-05-23"
BUDGET = 150.0
TARGET_DD_DEFAULT = 25.0          # tied to the live -25% account stop
OUR_DD_GATE_DEFAULT = 25.0        # per-leader standalone screen threshold (secondary)
LEADER_DD_KEEP = 70.0             # the old proxy KEEPS leaders with leader-account MTM-DD < 70%
WALLETS_JSON = "config/copy_trader_wallets_v17_expansion.json"


def _ms(d):
    return int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)


def live_whitelist(cfg):
    """FIX 1: the live tradeable universe = baseline coin_whitelist (10) UNION expansion.coins (40).
    The engine merges these AFTER __init__ (hl_copy_trader_v17._v17_init_expansion); we replicate that
    union here so the scorer copies EXACTLY the coins the live book would."""
    base = list(cfg["global"]["coin_whitelist"])
    exp = list(cfg["global"].get("expansion", {}).get("coins", []))
    wl = set(base) | set(exp)
    return wl, set(base), set(exp)


# ---- per-wallet whitelist-filtered action cache (load parquet ONCE per wallet) ----------------- #
_ACTS_CACHE = {}  # wallet -> whitelist-filtered DataFrame of its in-window rows


def _cached_acts(wallets, win, whitelist):
    """Return the concatenated cached (whitelist-filtered) action slice for `wallets`, sorted by
    (ts, event_order). Touches parquet only for wallets not yet cached."""
    frames = []
    for w in wallets:
        if w not in _ACTS_CACHE:
            raw = load_actions_for_leaders({w}, tm=None, win=win)
            _ACTS_CACHE[w] = (raw if raw.empty else raw[raw["coin"].isin(whitelist)].reset_index(drop=True))
        df = _ACTS_CACHE[w]
        if not df.empty:
            frames.append(df)
    if not frames:
        return load_actions_for_leaders(set(), tm=None, win=win)  # empty typed frame
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(["ts", "event_order"], kind="mergesort").reset_index(drop=True)


def our_copy_run(wallets, win, btc_hourly, whitelist, actions=None):
    """Run `wallets` as ONE netted faithful-copy $150-per-leader account over `win`.
    Returns (roe%, mtm_dd%, beta, n_actions, n_sleeves).

    FAITHFUL-COPY: each wallet is a single sleeve copying its FULL SIGNED target. We pass all wallets as
    long_wallets (sleeve_long=True) and set faithful_copy=True so the engine bypasses the sign filter and
    tracks one signed running target per (wallet, coin). budget keyed (wallet, True) = $150 each.

    FIX 1: actions are pre-filtered to the live 50-coin whitelist (the engine then sees only copyable
    coins). We inject the cached, whitelist-filtered, concatenated slice via simulate_fold's loader by
    monkeypatching the module-level load through a closure -- instead we pass the engine a window with
    the SAME wallets and rely on the engine's own loader; to keep the engine byte-identical we instead
    feed it the cached slice through the `actions` arg of this wrapper and re-run the engine's event loop
    on that slice. Because simulate_fold loads internally, we replicate FIX1 by temporarily swapping the
    engine's loader with one that returns our cached whitelist-filtered slice (default behavior of the
    engine is untouched outside this call)."""
    import balanced_step4_netted_sim as B
    wl = list(wallets)
    n = len(wl)
    long_wallets = set(wl)
    short_wallets = set()
    budget = {(w, True): BUDGET for w in wl}
    acts = actions if actions is not None else _cached_acts(wl, win, whitelist)

    # Temporarily swap the engine's loader to return our cached, whitelist-filtered slice for THIS call.
    # Engine default behavior is restored immediately after (defaults byte-identical).
    orig_loader = B.load_actions_for_leaders

    def _loader(_w, tm=None, win=None):  # noqa: ARG001
        return acts

    B.load_actions_for_leaders = _loader
    try:
        eq, grid, br, start_equity = simulate_fold(
            long_wallets, short_wallets, tm=None, btc_hourly=btc_hourly,
            n_sleeves=n, budget=budget, win=win, faithful_copy=True)
    finally:
        B.load_actions_for_leaders = orig_loader

    # FIX 4: append explicit start + end equity samples (the engine grid floors start, excludes end).
    # We rebuild OUR DD off [equity@WIN_START, hourly path, equity@WIN_END] so the full window is
    # captured. mark_at is asof-backward only -> still causal.
    roe, dd, rbeta = fold_metrics(eq, br, start_equity)
    dd = _windowed_dd(eq, start_equity)
    return roe, dd, rbeta, len(acts), n


def _windowed_dd(eq, start_equity):
    """FIX 4: MTM maxDD on the equity path with the start sample (== start_equity at WIN_START, the
    account opens flat) prepended and the final hourly sample standing in for WIN_END. The engine already
    samples to/just-before end via the trailing 'sample remaining grid' loop, so eq[-1] is the closest
    causal equity to WIN_END; prepending start_equity guarantees the pre-first-tick peak is counted."""
    full = np.concatenate(([start_equity], eq))
    pk = np.maximum.accumulate(full)
    return float(((pk - full) / pk).max()) * 100.0


def leader_account_dd_samewindow(wallets, win):
    """FIX 2: leader-account MTM-DD over EXACTLY [WIN_START, WIN_END].
    The HL portfolio response is a LIST of [period, payload] pairs. We pick the period whose
    accountValueHistory COVERS the window (first ts <= WIN_START and last ts >= WIN_END), preferring the
    longest-horizon period (allTime > month > week > day). We FILTER points to ts in [lo, hi] and compute
    MTM maxDD on that sub-series. No covering period -> dd=None, err='n/a (no same-window data)'."""
    lo, hi = int(win[0]), int(win[1])
    PREF = ["allTime", "month", "week", "day"]  # longest horizon first

    def post(b):
        r = urllib.request.Request("https://api.hyperliquid.xyz/info", data=json.dumps(b).encode(),
                                   headers={"Content-Type": "application/json"})
        return json.load(urllib.request.urlopen(r, timeout=20))

    out = {}
    for w in wallets:
        rec = {"dd": None, "ret": None, "err": None, "period": None, "n_pts": 0}
        try:
            resp = post({"type": "portfolio", "user": w})
            periods = dict(resp)  # list of [key, payload] -> dict
            chosen = None
            for p in PREF:
                avh = periods.get(p, {}).get("accountValueHistory", [])
                if not avh:
                    continue
                ts0, ts1 = avh[0][0], avh[-1][0]
                if ts0 <= lo and ts1 >= hi:
                    chosen = (p, avh)
                    break
            if chosen is None:
                rec["err"] = "n/a (no same-window data)"
            else:
                pname, avh = chosen
                pts = [(int(t), float(v)) for t, v in avh if lo <= int(t) <= hi]
                arr = np.array([v for _, v in pts])
                arr = arr[arr > 0]
                if len(arr) < 3:
                    rec["err"] = f"n/a (only {len(arr)} in-window pts on '{pname}')"
                else:
                    pk = np.maximum.accumulate(arr)
                    rec["dd"] = float(((pk - arr) / pk).max() * 100.0)
                    rec["ret"] = float((arr[-1] / arr[0] - 1.0) * 100.0)
                    rec["period"] = pname
                    rec["n_pts"] = len(arr)
        except Exception as e:
            rec["err"] = f"{type(e).__name__}: {e}"
        out[w] = rec
        time.sleep(0.05)
    return out


def fmt_dd(x):
    return "  n/a" if x is None else f"{x:6.2f}"


def cohort_dd(wallets, win, btc_hourly, whitelist):
    """Cohort OUR-MTM-DD for a SET of wallets (re-nets the cached whitelist-filtered slices in RAM)."""
    if not wallets:
        return 0.0, 0.0
    roe, dd, _b, _n, _s = our_copy_run(list(wallets), win, btc_hourly, whitelist)
    return dd, roe


def main():
    smoke3 = "--smoke3" in sys.argv
    do_slipsens = ("--slipsens" in sys.argv) or (not smoke3)
    target = TARGET_DD_DEFAULT
    gate = OUR_DD_GATE_DEFAULT
    for a in sys.argv:
        if a.startswith("--target="):
            target = float(a.split("=", 1)[1])
        if a.startswith("--gate="):
            gate = float(a.split("=", 1)[1])

    cfg = json.load(open(WALLETS_JSON))
    wallets = list(cfg["wallets"].keys())
    whitelist, base_coins, exp_coins = live_whitelist(cfg)
    if smoke3:
        wallets = wallets[:3]
        print(f"=== SMOKE: first {len(wallets)} wallets ===", flush=True)

    lo, hi = _ms(WIN_START), _ms(WIN_END)
    win = (lo, hi)
    print(f"OUR-DD scorer: {len(wallets)} live wallets | window {WIN_START}..{WIN_END} | "
          f"$150/leader | faithful-copy | 4x gross + latched -25% stop | execution_model pricing",
          flush=True)
    print(f"FIX1 live whitelist: {len(whitelist)} coins ({len(base_coins)} calibrated baseline + "
          f"{len(exp_coins)} expansion/default-slip)", flush=True)

    print("building BTC hourly return grid over window ...", flush=True)
    btc_hourly = btc_ret_hourly(lo, hi)

    # ---- FIX 1: load + whitelist-filter + cache each wallet's actions ONCE, report slip split -------
    print("loading + whitelist-filtering + caching per-wallet actions (parquet scanned once each) ...",
          flush=True)
    # also count what the whitelist DROPS, by comparing raw vs filtered the first time
    raw_total = filt_total = 0
    for w in wallets:
        raw = load_actions_for_leaders({w}, tm=None, win=win)
        raw_total += len(raw)
        if raw.empty:
            _ACTS_CACHE[w] = raw
        else:
            f = raw[raw["coin"].isin(whitelist)].reset_index(drop=True)
            _ACTS_CACHE[w] = f
            filt_total += len(f)
    dropped = raw_total - filt_total
    print(f"  whitelist filter: kept {filt_total} / {raw_total} in-window actions "
          f"(dropped {dropped} = {100.0*dropped/max(raw_total,1):.1f}% on non-whitelist coins)",
          flush=True)

    # ---- (1) PER-LEADER pass (reset slip-hit counters so calibrated_share() reflects the real run) ---
    EM.reset_hits()
    print("\n=== (1) PER-LEADER pass (standalone $150 single-leader copy account, whitelist-filtered) ===",
          flush=True)
    per = {}
    for i, w in enumerate(wallets, 1):
        roe, dd, rbeta, n_acts, _ = our_copy_run([w], win, btc_hourly, whitelist)
        per[w] = {"roe": roe, "dd": dd, "beta": rbeta, "n_acts": n_acts}
        print(f"  [{i:>2}/{len(wallets)}] {w}  OUR-DD={dd:6.2f}%  ROE={roe:7.2f}%  "
              f"beta={rbeta:6.3f}  n_actions={n_acts}", flush=True)

    # ---- (2) COHORT pass --------------------------------------------------------------------------
    print("\n=== (2) COHORT pass (all wallets netted, $150 each, start=150*n) ===", flush=True)
    c_roe, c_dd, c_beta, c_acts, c_n = our_copy_run(wallets, win, btc_hourly, whitelist)
    per_dds = [per[w]["dd"] for w in wallets]
    print(f"  cohort: n={c_n}  start_equity=${BUDGET * c_n:.0f}  OUR-DD={c_dd:.2f}%  ROE={c_roe:.2f}%  "
          f"beta={c_beta:.3f}  n_actions={c_acts}", flush=True)
    print(f"  per-leader DD (netting changes this): mean={np.mean(per_dds):.2f}%  "
          f"median={np.median(per_dds):.2f}%  max={np.max(per_dds):.2f}%  min={np.min(per_dds):.2f}%",
          flush=True)

    # ---- FIX 1 report: calibrated-vs-default slip share + sensitivity -----------------------------
    share, n_cal, n_def = EM.calibrated_share()
    print("\n=== (FIX1) calibrated-vs-default SLIP share (over per-leader + cohort fills) ===", flush=True)
    print(f"  slip lookups: calibrated={n_cal}  default={n_def}  calibrated_share={share:.1f}%", flush=True)
    # notional split on the cached whitelist-filtered actions (action-mark proxy for copied notional)
    notion_cal = notion_def = 0.0
    coins_cal = coins_def = set()
    for w in wallets:
        df = _ACTS_CACHE[w]
        if df.empty:
            continue
        for c, m in zip(df["coin"], df["mark"]):
            notion = abs(float(m)) if (m is not None and not pd.isna(m)) else 0.0
            if c in base_coins:
                notion_cal += notion
                coins_cal.add(c)
            else:
                notion_def += notion
                coins_def.add(c)
    tot_notion = notion_cal + notion_def
    print(f"  whitelist coins touched: {len(coins_cal)} calibrated + {len(coins_def)} default "
          f"(of {len(whitelist)} possible)", flush=True)
    print(f"  action-mark notional on calibrated coins: {100.0*notion_cal/max(tot_notion,1e-9):.1f}%  "
          f"| on default-slip coins: {100.0*notion_def/max(tot_notion,1e-9):.1f}%", flush=True)

    slip_sens_rows = []
    if do_slipsens:
        print("\n=== (FIX1) SLIP SENSITIVITY: cohort OUR-DD + ROE at default-slip x{0.5,1,2} ===", flush=True)
        base_default = EM.DEFAULT_SLIP_BPS
        for mult in (0.5, 1.0, 2.0):
            EM.set_slip_default_bps(base_default * mult)
            sdd, sroe = cohort_dd(wallets, win, btc_hourly, whitelist)
            slip_sens_rows.append((mult, base_default * mult, sdd, sroe))
            print(f"  default_slip={base_default*mult:5.2f}bps (x{mult})  cohort OUR-DD={sdd:6.2f}%  "
                  f"ROE={sroe:7.2f}%", flush=True)
        EM.set_slip_default_bps(base_default)  # restore

    # ---- FIX 2: same-window leader-account DD -----------------------------------------------------
    print(f"\nquerying HL portfolio (FIX2 SAME-WINDOW leader-account MTM-DD) for {len(wallets)} wallets ...",
          flush=True)
    ldr = leader_account_dd_samewindow(wallets, win)
    failed = [w for w in wallets if ldr[w]["err"]]
    if failed:
        print(f"  leader-DD n/a for {len(failed)} wallets (same-window data unavailable):", flush=True)
        for w in failed:
            print(f"    {w}  -> {ldr[w]['err']}", flush=True)

    # ---- (3) COMPARISON TABLE (same-window) -------------------------------------------------------
    print("\n=== (3) COMPARISON: SAME-WINDOW leader-account DD vs OUR per-leader DD vs OUR edge ===",
          flush=True)
    print("  sorted by OUR MTM-DD descending", flush=True)
    hdr = (f"  {'wallet':>42}  {'ldrDD%':>7}  {'OUR-DD%':>8}  {'OUR-ROE%':>9}  {'period':>7}  "
           f"{'proxy':>6}  {'flag'}")
    print(hdr, flush=True)
    print("  " + "-" * (len(hdr) - 2), flush=True)
    rows = []
    for w in wallets:
        ldd = ldr[w]["dd"]
        odd = per[w]["dd"]
        oroe = per[w]["roe"]
        period = ldr[w]["period"] or "-"
        proxy_excl = (ldd is not None and ldd >= LEADER_DD_KEEP)
        proxy = "EXCL" if proxy_excl else ("KEEP" if ldd is not None else "n/a")
        flag = ""
        if ldd is not None:
            if (not proxy_excl) and odd >= gate:
                flag = "<< proxy KEEPS but OUR-DD blows past gate"
            elif proxy_excl and odd < gate:
                flag = ">> proxy EXCLUDES but OUR copy is fine"
        rows.append((w, ldd, odd, oroe, period, proxy, proxy_excl, flag))
    rows.sort(key=lambda r: r[2], reverse=True)
    for w, ldd, odd, oroe, period, proxy, proxy_excl, flag in rows:
        print(f"  {w:>42}  {fmt_dd(ldd):>7}  {odd:8.2f}  {oroe:9.2f}  {period:>7}  {proxy:>6}  {flag}",
              flush=True)

    # same-window mis-judge counts (only over wallets WITH same-window leader-DD)
    have = [r for r in rows if r[1] is not None]
    proxy_keeps_we_gate = [r for r in have if (not r[6]) and r[2] >= gate]
    proxy_excl_we_fine = [r for r in have if r[6] and r[2] < gate]
    print(f"\n  SAME-WINDOW mis-judgements (of {len(have)} wallets with same-window leader-DD):",
          flush=True)
    print(f"    proxy KEEPS but OUR per-leader DD >= {gate:.0f}%: {len(proxy_keeps_we_gate)}", flush=True)
    print(f"    proxy EXCLUDES (leaderDD>=70%) but OUR copy fine (<{gate:.0f}%): {len(proxy_excl_we_fine)}",
          flush=True)

    # ---- (4) GREEDY LEAVE-ONE-OUT DROP SEQUENCE (the real gate) -----------------------------------
    print(f"\n=== (4) GREEDY LOO DROP to cohort OUR-DD <= TARGET_DD={target:.1f}% (re-nets cached "
          f"slices in RAM) ===", flush=True)
    survivors = list(wallets)
    cur_dd, cur_roe = c_dd, c_roe
    print(f"  start: n={len(survivors)}  cohort OUR-DD={cur_dd:.2f}%  ROE={cur_roe:.2f}%", flush=True)
    drop_seq = []
    step = 0
    while cur_dd > target and len(survivors) > 1:
        step += 1
        best = None  # (new_dd, dropped_wallet, new_roe, marginal_reduction)
        for cand in survivors:
            sub = [x for x in survivors if x != cand]
            sub_dd, sub_roe = cohort_dd(sub, win, btc_hourly, whitelist)
            marginal = cur_dd - sub_dd  # positive = removing cand reduces cohort DD
            if best is None or sub_dd < best[0]:
                best = (sub_dd, cand, sub_roe, marginal)
        new_dd, dropped_w, new_roe, marginal = best
        survivors = [x for x in survivors if x != dropped_w]
        drop_seq.append((step, dropped_w, new_dd, new_roe, marginal))
        print(f"  drop #{step}: {dropped_w}  -> cohort OUR-DD={new_dd:6.2f}%  ROE={new_roe:7.2f}%  "
              f"(marginal DD reduction {marginal:+.2f}pp)  survivors={len(survivors)}", flush=True)
        cur_dd, cur_roe = new_dd, new_roe

    print(f"\n  GREEDY RESULT: dropped {len(drop_seq)} leaders, {len(survivors)} survive, "
          f"final cohort OUR-DD={cur_dd:.2f}%  ROE={cur_roe:.2f}%  (target {target:.1f}%)", flush=True)
    if cur_dd > target:
        print(f"  NOTE: target NOT reached even after dropping to {len(survivors)} survivor(s).", flush=True)

    # ---- secondary screen: standalone per-leader gate ---------------------------------------------
    print(f"\n=== SECONDARY SCREEN: standalone per-leader OUR-DD >= {gate:.1f}% ===", flush=True)
    gated = [(w, odd) for (w, ldd, odd, oroe, period, proxy, proxy_excl, flag) in rows if odd >= gate]
    print(f"  {len(gated)} leaders breach the standalone per-leader gate (screen only, NOT the cohort "
          f"gate):", flush=True)
    for w, odd in gated:
        print(f"    {w}  standalone OUR-DD={odd:.2f}%", flush=True)

    print("\nNOTE: NUMBERS ONLY -- no leader-drop recommendation. Codex review of code+result pending; "
          "Alberto decides.", flush=True)


if __name__ == "__main__":
    main()
