#!/usr/bin/env python
"""
build_skill_cohort.py -- generate the DEPLOYABLE skill-ranked cohort + a final forward re-validation on the
COPYABLE universe (only coins we can faithfully copy = agentC-calibrated, liquid).

Gates baked in (from the deploy-caution handoff):
- copyable: skill computed ONLY on the leader's journeys in calibrated/liquid coins we trade.
- active: wallet traded a copyable coin in the last ACTIVE_DAYS of data.
- enough sample: >= MIN_J copyable journeys.
- skill rank = z(win)+z(sharpe)+z(-maxdd), NO return weight.

Outputs: /tmp/skill_cohort_deploy.json (the 100-wallet dict, engine format) + prints a final OOS
forward re-validation (copyable-restricted) confirming SKILL still beats PnL before deploy.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/build_skill_cohort.py
"""
import json
import os
import numpy as np
import pandas as pd

ASOF = "2026-05-23"          # selection uses data <= asof (live cohort_asof)
# GATE selects the near-ruin DD gate. DEFAULT = "leader_dd" => byte-identical to the historical build
# (calls mtm_dd_exclude, which queries each candidate's LEADER HL account month MTM-DD). Opt-in
# GATE=our_dd => call our_dd_gate.our_dd_exclude instead (marginal-cohort OUR-DD greedy-LOO at OUR
# $150/leader sizing). NOTHING about the default path changes; this is a pure additive branch.
GATE = os.environ.get("GATE", "leader_dd").strip().lower()
OUR_DD_TARGET = float(os.environ.get("OUR_DD_TARGET", "25.0"))   # only used when GATE=our_dd
OUR_DD_WINDOW_DAYS = int(os.environ.get("OUR_DD_WINDOW_DAYS", "60"))  # only used when GATE=our_dd

# ── V17 all-dex / per-coin-cost / set-cover branches (Alberto 2026-06-28: "all coins from all dexes",
#    "as many coins and as few good wallets as possible", selected on consistency + low unrealized DD +
#    copyability -- NOT pnl rank). All three default to the historical path so the legacy build stays
#    byte-identical; opt in via env. Same additive convention as GATE above.
UNIVERSE = os.environ.get("UNIVERSE", "calib").strip().lower()    # calib (10-coin) | all (every coin/dex)
COST = os.environ.get("COST", "flat").strip().lower()            # flat (RT_COST_BPS) | exec (per-coin model)
SELECT = os.environ.get("SELECT", "topk").strip().lower()        # topk | setcover (min wallets, max coins)
SLIP_DEFAULT_BPS = float(os.environ.get("SLIP_DEFAULT_BPS", "4.7"))  # tail one-way slip (COST=exec sensitivity)
WALKFORWARD = os.environ.get("WALKFORWARD", "").strip() in ("1", "true", "yes")  # multi-window OOS test
# DEXES restricts the all-coin universe to chosen dexes. "all" = every dex; otherwise comma list where
# "main" = no-prefix perp coins and a prefix (e.g. "xyz") keeps that builder dex. Alberto 2026-06-28:
# "all coins on main and xyz only" -> DEXES=main,xyz.
DEXES = os.environ.get("DEXES", "all").strip().lower()


def _dex_ok(coin, dexes):
    if dexes == "all":
        return True
    allowed = set(d.strip() for d in dexes.split(","))
    pref = coin.split(":")[0] if ":" in coin else "main"
    return pref in allowed
ACTIVE_DAYS = 14
MIN_J = 40
K = int(os.environ.get("K", "100"))  # max cohort wallets; default 100 preserves historical build
RT_COST_BPS = 11.0
# NOTE (2026-06-28): WALK-FORWARD passed 4/4 adjacent monthly folds, cohort median +67.7bps, calib-10
# anchor median +40.6bps. Single-window causal = +42.6bps. See handoffs/quant-engineer/2026-06-28-1832.
HOLD_MIN_H = 2.0             # LAG-ROBUST filter: median hold >= 2h (copy entry-lag immaterial; the <1h
HOLD_MAX_H = 48.0           # scalpers are lag-sensitive +44bps, the 2-24h band is +107bps -- drop scalps)


def max_dd(r):
    eq = np.cumsum(r)
    return float((np.maximum.accumulate(eq) - eq).max()) if len(eq) else 0.0


def martingale_flags(df):
    """HARD martingale veto (codex 2026-06-05, was dropped at the 06-14 build -> re-added, Phase 3).
    High win-rate + low REALIZED drawdown is the SIGNATURE of averaging-down/martingale: hold losers,
    size up after losses, realize only winners. Closed-trip metrics are blind to the open bag; this veto
    catches the behavioral tell. Per wallet (journeys ordered by entry_ts): size-up-after-loss, holds-
    losers ratio, win/loss magnitude, ~0% realized losses. Returns bool Series (True = martingale = veto)."""
    out = {}
    for w, g in df.sort_values("entry_ts").groupby("wallet"):
        pnl = g["net_realized_pnl"].to_numpy(); ntl = g["max_position_notional"].to_numpy()
        dur = g["duration_h"].to_numpy(); n = len(pnl)
        if n < 10:
            out[w] = False; continue
        win = pnl > 0; loss = pnl < 0; nloss = int(loss.sum())
        nal = ntl[1:][loss[:-1]]; naw = ntl[1:][win[:-1]]
        su = (np.mean(nal) / np.mean(naw)) if (len(nal) and len(naw) and np.mean(naw) > 0) else np.nan
        ha = (np.mean(dur[loss]) / np.mean(dur[win])) if (nloss > 0 and win.any() and np.mean(dur[win]) > 0) else np.nan
        wm = (np.mean(pnl[win]) / np.mean(np.abs(pnl[loss]))) if (nloss > 0 and win.any() and np.mean(np.abs(pnl[loss])) > 0) else np.inf
        lf = nloss / n
        extreme = ((su == su and su > 3.0) or (ha == ha and ha > 5.0) or (lf < 0.02))
        mild = int((su == su and su > 1.3) + (ha == ha and ha > 2.5) + (wm < 0.6) + (lf < 0.05))
        out[w] = bool(extreme or mild >= 2)
    return pd.Series(out)


def skill_scores(df, ret_col="ret"):
    """Per-wallet skill metrics. ret_col selects the return basis: "ret" = legacy (leader gross net of
    leader fees), "net_ret" = OUR copy net (after per-coin execution cost = the COPYABILITY basis)."""
    g = df.groupby("wallet")
    s = g.agg(n=(ret_col, "size"), mean=(ret_col, "mean"), std=(ret_col, "std"),
             win=(ret_col, lambda x: (x > 0).mean()), sum_pnl=("realized_pnl", "sum"),
             liq=("liq_closed", "mean"), hold=("duration_h", "median"))
    s["sharpe"] = s["mean"] / (s["std"] + 1e-9)
    s["maxdd"] = g[ret_col].apply(max_dd)
    return s


def z(x):
    return (x - x.mean()) / (x.std() + 1e-9)


def mtm_dd_exclude(wallets):
    """MARK-TO-MARKET drawdown gate (Phase 3, Alberto 9947 + codex). Closed-trip maxdd is BLIND to open
    bags; this queries each candidate's CURRENT account mark-to-market equity (HL portfolio month history,
    incl unrealized) and excludes the near-ruin tail. Codex drop-19 criterion: MTM maxDD>=70% OR (60-70%
    AND month return<=-40%). Operational filter (current state = who is bag-holding/blown-up NOW); for
    backtesting the rule use v15_m01 historical reconstruction instead. Returns set to exclude."""
    import urllib.request, time
    def post(b):
        r = urllib.request.Request("https://api.hyperliquid.xyz/info", data=json.dumps(b).encode(),
                                   headers={"Content-Type": "application/json"})
        return json.load(urllib.request.urlopen(r, timeout=20))
    excl = set()
    for w in wallets:
        try:
            d = dict(post({"type": "portfolio", "user": w}))
            avh = d.get("month", {}).get("accountValueHistory", [])
            a = np.array([float(v) for _, v in avh]); a = a[a > 0]
            if len(a) < 5:
                continue
            dd = ((np.maximum.accumulate(a) - a) / np.maximum.accumulate(a)).max() * 100
            ret = (a[-1] / a[0] - 1) * 100
            if (dd >= 70) or (60 <= dd < 70 and ret <= -40):
                excl.add(w)
        except Exception:
            pass
        time.sleep(0.05)
    return excl


def build_cost_map(coins):
    """Per-coin ROUND-TRIP execution cost (fraction of notional) via the CANONICAL execution model
    (research/v15/execution_model.py): calibrated coins pay measured L2 slip, every tail coin pays the
    single liquidity-class default. RT = entry slip + exit slip + RT taker fee. Returns {coin: rt_frac}
    plus calibrated-share diagnostics. No hardcoded slippage here -- the model is the single source."""
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parent.parent / "v15"))
    import execution_model as em  # noqa: E402
    em.set_slip_default_bps(SLIP_DEFAULT_BPS)
    # Register the 64 MEASURED half-spreads (real L2, one-way) so the main-perp tail is costed on data,
    # not the flat default. Half-spread is the dominant one-way cost at our $10-300 size (impact ~0); this
    # does NOT override the committed base-10 l2_calib (register_slip_oneway guards base coins). Codex #5.
    MEAS = os.environ.get("MEASURED_SLIP", "/tmp/measured_halfspread.json")
    if not os.path.exists(MEAS):
        raise FileNotFoundError(f"COST=exec requires measured-slip file {MEAS} (codex r2: no silent "
                                f"fallback to tail defaults only). Set MEASURED_SLIP= to override.")
    n_meas = 0
    for c, hs in json.load(open(MEAS)).items():
        try:
            em.register_slip_oneway(c, float(hs)); n_meas += 1
        except Exception:
            pass
    print(f"[COST=exec] registered {n_meas} MEASURED one-way slippages from {os.path.basename(MEAS)}")
    rt_fee = em.fee_rt(maker=False)
    cmap = {}
    for c in coins:
        cmap[c] = float(em.slip_oneway(c)) * 2.0 + rt_fee   # entry + exit slip + RT fee
    share, n_cal, n_tot = em.calibrated_share()
    return cmap, (share, n_cal, n_tot)


def weekly_consistency(df, ret_col="net_ret"):
    """CONSISTENCY axis (Alberto 2026-06-28): fraction of distinct CALENDAR WEEKS in which the wallet's
    summed NET return is positive. Catches the one-lucky-week wallet that per-journey win-rate hides.
    Returns Series wallet -> green-week fraction (NaN-safe; wallets with <MIN_WEEKS weeks get the value
    but are gated on sample elsewhere)."""
    g = df.copy()
    g["wk"] = g["t"].dt.to_period("W")
    wk = g.groupby(["wallet", "wk"])[ret_col].sum().reset_index()
    return wk.groupby("wallet")[ret_col].agg(lambda x: float((x > 0).mean()))


def set_cover_select(s, journeys, k_max=K, min_skill_pct=60.0):
    """SET-COVER selection (Alberto 2026-06-28: 'as many coins and as few good wallets as possible').
    From the skill-eligible pool, greedily pick the SMALLEST wallet set whose union of COPYABLE coins
    (coins the wallet actually traded, net-positive) maximizes coin coverage. Tie-break by skill. Stops
    when a wallet adds no new coin or k_max reached. `s` is the scored/gated frame (index=wallet, has
    'skill'); `journeys` is the per-journey frame restricted to the eligible pool."""
    pool = s[s["skill"] >= np.nanpercentile(s["skill"], min_skill_pct)].copy()
    # copyable coin set per wallet = coins where the wallet's mean NET return > 0 (we'd profit copying it)
    jp = journeys[journeys.wallet.isin(pool.index)]
    cov = {}
    for w, gw in jp.groupby("wallet"):
        good = gw.groupby("coin")["net_ret"].mean()
        cov[w] = set(good[good > 0].index)
    chosen, covered = [], set()
    remaining = dict(cov)
    while remaining and len(chosen) < k_max:
        # pick wallet adding the most NEW coins; tie-break by skill
        best_w, best_gain = None, -1
        for w, cs in remaining.items():
            gain = len(cs - covered)
            if gain > best_gain or (gain == best_gain and best_w is not None
                                    and pool.loc[w, "skill"] > pool.loc[best_w, "skill"]):
                best_w, best_gain = w, gain
        if best_w is None or best_gain <= 0:
            break
        chosen.append(best_w)
        covered |= remaining.pop(best_w)
    return chosen, covered


def coin_class(coin, calib10):
    """3-way coin liquidity class for cost-artifact attribution (codex 2026-06-28 #5): builder-dex
    (xyz:/km:/hyna:/... = thinnest, cost most likely understated), calib (the 10 measured-L2 majors),
    main_tail (main-perp coins on the flat default)."""
    if ":" in coin:
        return "builder"
    return "calib" if coin in calib10 else "main_tail"


def causal_validate(j, asof, ret_col, select, calib10, k=K, window_days=30, use_consistency=True):
    """CAUSAL forward re-validation (codex 2026-06-28 blockers #1/#2/#3/#7). Everything that drives
    selection is computed from the TRAIN slice ONLY (t < cutoff): active status, skill, consistency,
    martingale veto, and -- for SELECT=setcover -- the set_cover_select itself. We then evaluate the
    EXACT selected cohort on the forward slice (cutoff..asof), and split forward edge by coin class so a
    cost-artifact concentrated in under-costed builder-dex coins is visible. Returns a dict of metrics.
    `use_consistency` mirrors the deploy builder's V17 guard (codex 2026-06-28 MED): when the deploy
    cohort build adds the consistency axis (V17 on), the validator must too; on the legacy non-V17 path
    it must NOT, or the validator scores a different skill than deploy ranks on."""
    cutoff = asof - pd.Timedelta(days=window_days)
    tr, fw = j[j.t < cutoff], j[j.t >= cutoff]
    # active computed causally: traded in last ACTIVE_DAYS of TRAIN (no peeking past cutoff)
    tr_last = tr.t.max()
    active_tr = set(tr[tr.t >= tr_last - pd.Timedelta(days=ACTIVE_DAYS)].wallet.unique())
    ts = skill_scores(tr, ret_col)
    ts = ts[(ts.n >= MIN_J) & (ts.index.isin(active_tr)) & (ts.hold >= HOLD_MIN_H) & (ts.hold <= HOLD_MAX_H)].copy()
    # consistency (V17-guarded to mirror deploy) + martingale veto on TRAIN, exactly as deploy does
    if use_consistency:
        cons_tr = weekly_consistency(tr[tr.wallet.isin(ts.index)], ret_col=ret_col)
        ts["consistency"] = ts.index.map(cons_tr).fillna(0.0)
    mart_tr = martingale_flags(tr[tr.wallet.isin(ts.index)])
    ts = ts[~ts.index.map(mart_tr).fillna(False)].copy()
    ts["skill"] = z(ts.win) + z(ts.sharpe) + z(-ts.maxdd) + (z(ts.consistency) if use_consistency else 0.0)
    # SELECT the cohort from TRAIN ONLY (codex 2026-06-28 r2 BLOCKER: no forward info in eligibility).
    # The old code pre-filtered the pool to wallets with >=10 forward journeys before selecting -- that
    # leaked forward-activity. Now selection sees only `ts`/`tr`; forward data is touched ONLY to evaluate.
    if select == "setcover":
        chosen, _cov = set_cover_select(ts, tr[tr.wallet.isin(ts.index)], k_max=k)
        sel_w = list(chosen)
    else:
        sel_w = list(ts.nlargest(k, "skill").index)
    # CAUSAL OUR-DD GATE MIRROR (codex 2026-06-28 HIGH): the live deploy path (main, Phase-3) drops the
    # near-ruin tail via our_dd_exclude. Validation must apply the SAME gate or it validates an ungated
    # cohort that never deploys. We mirror it CAUSALLY here: the gate's asof = `cutoff` (the train/forward
    # boundary). our_dd_exclude windows on [cutoff-window, cutoff) and load_actions_for_leaders filters
    # ts STRICTLY < cutoff (see our_dd_gate.py docstring), while the forward eval slice is t >= cutoff --
    # so the gate and forward windows are DISJOINT at the boundary (no exact-cutoff overlap; codex LOW).
    # Default GATE=leader_dd => this branch is skipped => byte-identical to the prior walk-forward output.
    # We never call the live-API mtm_dd_exclude in validation (it queries the leader's CURRENT account =
    # look-ahead).
    n_sel_pregate = len(sel_w)
    n_gated = 0
    if GATE == "our_dd" and len(sel_w) > 1:
        import sys as _sys
        from pathlib import Path as _Path
        _sys.path.insert(0, str(_Path(__file__).resolve().parent))
        from our_dd_gate import our_dd_exclude  # noqa: E402
        import our_drawdown_scorer as _ODS  # noqa: E402
        # CODEX HIGH (2026-06-28): _ACTS_CACHE is keyed by WALLET ONLY (our_drawdown_scorer.py:117,
        # our_dd_gate.py:108) and is NOT re-filtered by window. In walk_forward each fold calls this
        # gate with a DIFFERENT `cutoff`, but a wallet cached in an earlier (earlier-cutoff) fold would
        # reuse that stale action slice -> the per-fold OUR-DD gate would NOT be evaluated on this
        # fold's [cutoff-window, cutoff) window. Clear the window-bound cache per fold so each fold
        # loads its own slice (mirrors the established our_dd_holdout.py:196 pattern). Marks cache is
        # full-history asof-indexed (not window-bound), so it is left warm.
        _ODS._ACTS_CACHE.clear()  # noqa: E305
        _excl, _ = our_dd_exclude(sel_w, cutoff, window_days=OUR_DD_WINDOW_DAYS,
                                  target_dd=OUR_DD_TARGET)
        sel_w = [w for w in sel_w if w not in _excl]
        n_gated = len(_excl)
    pnl_w = list(ts.nlargest(k, "sum_pnl").index)
    # EVALUATE forward on whatever data exists. Journey-pooled mean (no per-wallet fn threshold -> no
    # selection coupling); plus per-wallet mean on wallets with >=10 fwd journeys for a hit-rate, and
    # we report how many selected wallets had too little forward data (coverage, NOT leakage).
    fwm = fw.groupby("wallet")[ret_col].agg(["mean", "count"]).rename(columns={"mean": "fwd", "count": "fn"})
    sel_pw = fwm.loc[fwm.index.isin(sel_w) & (fwm.fn >= 10)]
    fsel_all = fw[fw.wallet.isin(sel_w)]
    pnl_pool = fw[fw.wallet.isin(pnl_w)]
    out = {
        "n_sel": len(sel_w),
        "n_sel_pregate": n_sel_pregate,                 # cohort size before causal OUR-DD gate
        "n_gated": n_gated,                             # near-ruin wallets dropped by causal OUR-DD gate
        "n_sel_fwd": int(len(sel_pw)),                  # selected wallets with >=10 fwd journeys
        "cohort_fwd_bps": float(fsel_all[ret_col].mean() * 1e4),   # journey-pooled, all fwd data
        "cohort_hit_pct": float((sel_pw["fwd"] > 0).mean() * 100) if len(sel_pw) else float("nan"),
        "pnl_fwd_bps": float(pnl_pool[ret_col].mean() * 1e4),
    }
    # coin-class attribution: forward net edge of the SELECTED cohort, per coin class
    fsel = fsel_all.copy()
    fsel["cls"] = fsel["coin"].map(lambda c: coin_class(c, calib10))
    attr = fsel.groupby("cls")[ret_col].agg(["mean", "size"])
    out["attribution"] = {cls: (float(r["mean"] * 1e4), int(r["size"])) for cls, r in attr.iterrows()}
    return out


def walk_forward(j_full, ret_col, select, calib10, k=K, window_days=30, use_consistency=True):
    """WALK-FORWARD across adjacent 30d forward folds (codex 2026-06-28 r2: the real overfit test --
    one window is not enough). Each fold re-selects the cohort from its OWN train slice (t < asof-30d)
    and evaluates on its own forward [asof-30d, asof]. Reports per-fold cohort net + the slip-invariant
    calib-10 anchor so we see persistence across time, not a single lucky month."""
    tmin, tmax = j_full.t.min(), j_full.t.max()
    # asof = forward-window END; first fold needs >=45d train before its forward starts
    asofs = []
    a = (tmin + pd.Timedelta(days=45 + window_days)).normalize()
    while a <= tmax:
        asofs.append(a)
        a = a + pd.Timedelta(days=window_days)
    rows = []
    for asof in asofs:
        jf = j_full[j_full.t <= asof]
        try:
            cv = causal_validate(jf, asof, ret_col, select, calib10, k=k, window_days=window_days,
                                 use_consistency=use_consistency)
        except Exception as e:
            rows.append((asof, None, str(e)[:40])); continue
        calib_bps = cv["attribution"].get("calib", (float("nan"), 0))[0]
        rows.append((asof, cv, calib_bps))
    print("\n=== WALK-FORWARD (adjacent 30d folds, per-fold train-only re-selection) ===")
    print(f"  {'fwd-end':12s} {'n_sel':>6s} {'cohort':>9s} {'pnl-rank':>9s} {'calib-10':>9s} {'hit':>5s}")
    goods = []
    for asof, cv, extra in rows:
        if cv is None:
            print(f"  {str(asof.date()):12s}  ERR {extra}"); continue
        gtag = f"  [gate {cv['n_sel_pregate']}->{cv['n_sel']}, -{cv['n_gated']}]" if cv.get("n_gated") else ""
        print(f"  {str(asof.date()):12s} {cv['n_sel']:>6d} {cv['cohort_fwd_bps']:>+8.1f}b "
              f"{cv['pnl_fwd_bps']:>+8.1f}b {extra:>+8.1f}b {cv['cohort_hit_pct']:>4.0f}%{gtag}")
        goods.append(cv)
    if goods:
        import statistics as st
        coh = [g["cohort_fwd_bps"] for g in goods]
        cal = [g["attribution"].get("calib", (float("nan"), 0))[0] for g in goods if "calib" in g["attribution"]]
        n_pos = sum(1 for c in coh if c > 0)
        print(f"  --- {len(goods)} folds | cohort median {st.median(coh):+.1f}bps, {n_pos}/{len(coh)} folds positive"
              f" | calib-10 median {st.median(cal):+.1f}bps ---")
    return rows


def main():
    if WALKFORWARD:
        cols = ["wallet", "coin", "entry_ts", "realized_pnl", "net_realized_pnl",
                "max_position_notional", "liq_closed", "duration_h"]
        j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
        j = j[j.max_position_notional > 10].copy()
        if DEXES != "all":
            j = j[j.coin.map(lambda c: _dex_ok(c, DEXES))].copy()
        j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
        j = j[j.ret.between(-1.0, 2.0)].copy()
        j["t"] = pd.to_datetime(j["entry_ts"], unit="ms")
        calib10 = set(json.load(open("app/data/v15/l2_calib_10coin.json")).keys())
        if COST == "exec":
            cmap, _ = build_cost_map(sorted(j.coin.unique()))
            j["rt_cost"] = j["coin"].map(cmap).fillna((SLIP_DEFAULT_BPS * 2 + 8.64) / 1e4)
        else:
            j["rt_cost"] = RT_COST_BPS / 1e4
        j["net_ret"] = j["ret"] - j["rt_cost"]
        walk_forward(j, "net_ret", SELECT if SELECT == "setcover" else "topk", calib10)
        return
    if UNIVERSE == "calib" and not os.path.exists("/tmp/agentC_l2_calib_expanded.json"):
        raise FileNotFoundError("UNIVERSE=calib needs /tmp/agentC_l2_calib_expanded.json "
                                "(codex #8: refuse to silently fall back to all-coin). "
                                "Set UNIVERSE=all to intend the full universe.")
    if UNIVERSE == "calib" and not os.path.exists("/tmp/agentC_l2_calib_expanded.json"):
        raise FileNotFoundError("UNIVERSE=calib needs /tmp/agentC_l2_calib_expanded.json "
                                "(codex #8: refuse to silently fall back to all-coin). "
                                "Set UNIVERSE=all to intend the full universe.")
    calib = set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys()) if UNIVERSE == "calib" else None
    calib10 = set(json.load(open("app/data/v15/l2_calib_10coin.json")).keys())
    cols = ["wallet", "coin", "entry_ts", "realized_pnl", "net_realized_pnl",
            "max_position_notional", "liq_closed", "duration_h"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    if UNIVERSE == "all" or calib is None:
        j = j[j.max_position_notional > 10].copy()
        if DEXES != "all":
            j = j[j.coin.map(lambda c: _dex_ok(c, DEXES))].copy()
        print(f"[UNIVERSE=all DEXES={DEXES}] {j.coin.nunique()} coins, {len(j)} journeys")
    else:
        j = j[(j.max_position_notional > 10) & (j.coin.isin(calib))].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t"] = pd.to_datetime(j["entry_ts"], unit="ms")

    # ── per-coin execution cost -> NET return (the COPYABILITY axis). COST=exec prices each coin through
    #    the canonical execution_model (calibrated precise, tail at default); COST=flat keeps the legacy
    #    flat RT_COST_BPS so the historical path is byte-identical.
    if COST == "exec":
        cmap, (cal_share, n_cal, n_tot) = build_cost_map(sorted(j.coin.unique()))
        j["rt_cost"] = j["coin"].map(cmap).fillna((SLIP_DEFAULT_BPS * 2 + 8.64) / 1e4)
        print(f"[COST=exec] per-coin RT cost via execution_model | calibrated lookups {cal_share:.1f}% "
              f"(calib={n_cal} default={n_tot}) | tail-slip-default {SLIP_DEFAULT_BPS}bps one-way | "
              f"median RT {j['rt_cost'].median()*1e4:.1f}bps max {j['rt_cost'].max()*1e4:.1f}bps")
    else:
        j["rt_cost"] = RT_COST_BPS / 1e4
    j["net_ret"] = j["ret"] - j["rt_cost"]
    asof = pd.Timestamp(ASOF)
    j = j[j.t <= asof]

    # V17 selection basis: when ANY of the new branches is on, score/validate on OUR copy-net return
    # (after per-coin execution cost) so consistency + copyability are measured on what WE earn, and the
    # forward bps is already net (do not subtract the flat cost again). Default path keeps gross + flat.
    V17 = (UNIVERSE == "all") or (COST == "exec") or (SELECT == "setcover")
    RET_COL = "net_ret" if V17 else "ret"
    FWD_COST_BPS = 0.0 if V17 else RT_COST_BPS

    # active filter (deploy-time = full data; the CAUSAL validator below recomputes its own train-only active)
    last = j.t.max()
    active = set(j[j.t >= last - pd.Timedelta(days=ACTIVE_DAYS)].wallet.unique())

    # ---- CAUSAL FORWARD RE-VALIDATION (validates the ACTUAL selected cohort, train-only selection) ----
    cv = causal_validate(j, asof, RET_COL, SELECT if V17 else "topk", calib10, k=K, use_consistency=V17)
    print("=== CAUSAL FORWARD RE-VALIDATION (selected cohort, TRAIN-ONLY selection, journey-pooled net) ===")
    print(f"  {SELECT if V17 else 'topk'} cohort (n={cv['n_sel']} sel, {cv['n_sel_fwd']} w/ >=10 fwd): "
          f"{cv['cohort_fwd_bps']:+.1f}bps (hit {cv['cohort_hit_pct']:.0f}%) vs PnL-rank {cv['pnl_fwd_bps']:+.1f}bps "
          f"-> {'COHORT WINS' if cv['cohort_fwd_bps'] > cv['pnl_fwd_bps'] else 'PnL'}")
    print("  --- coin-class attribution (forward net, selected cohort) [DECISIVE artifact test] ---")
    for cls in ("calib", "main_tail", "builder"):
        if cls in cv["attribution"]:
            bps, n = cv["attribution"][cls]
            print(f"    {cls:9s}: {bps:+.1f}bps  (n={n} journeys)")

    # ---- BUILD the deployable cohort (all data <= asof) ----
    s = skill_scores(j, RET_COL)
    s = s[(s.n >= MIN_J) & (s.index.isin(active)) & (s.hold >= HOLD_MIN_H) & (s.hold <= HOLD_MAX_H)].copy()
    # CONSISTENCY axis (V17): green-calendar-week fraction on NET return, joined onto the scored frame.
    if V17:
        cons = weekly_consistency(j[j.wallet.isin(s.index)], ret_col=RET_COL)
        s["consistency"] = s.index.map(cons).fillna(0.0)
    # HARD MARTINGALE VETO (Phase 3, codex 06-05 rule re-added). Closed-trip skill metrics select FOR
    # martingales (high win + low realized DD); this disqualifies the behavioral tell BEFORE ranking.
    mart = martingale_flags(j[j.wallet.isin(s.index)])
    s["martingale"] = s.index.map(mart).fillna(False)
    n_mart = int(s["martingale"].sum())
    s = s[~s["martingale"]].copy()
    print(f"\n[Phase-3 martingale veto] eligible {len(s) + n_mart} -> vetoed {n_mart} martingales -> clean {len(s)}")
    s["skill"] = z(s.win) + z(s.sharpe) + z(-s.maxdd) + (z(s.consistency) if V17 else 0.0)
    # MARK-TO-MARKET DD GATE (Phase 3): rank by skill, then drop the near-ruin tail by CURRENT account
    # mark-to-market drawdown (incl unrealized -- the metric closed-trip maxdd is blind to). Query only the
    # top candidates (API-bounded), exclude, then take top-K survivors.
    cand = s.nlargest(int(K * 1.8), "skill")
    if GATE == "our_dd":
        # OPT-IN OUR-DD gate (marginal-cohort greedy-LOO at OUR $150/leader sizing). Causal: window
        # [asof-OUR_DD_WINDOW_DAYS, asof], no data past asof. Does NOT run on the default path.
        import sys as _sys
        from pathlib import Path as _Path
        _sys.path.insert(0, str(_Path(__file__).resolve().parent))
        from our_dd_gate import our_dd_exclude  # noqa: E402
        import our_drawdown_scorer as _ODS  # noqa: E402
        # CODEX HIGH follow-up (2026-06-28): _ACTS_CACHE is wallet-keyed + window-bound. If causal_validate
        # / walk_forward ran earlier in THIS process (WALKFORWARD=1), the cache holds slices for the LAST
        # validation cutoff window, not the deploy [ASOF-window, ASOF) window. This is the LIVE deploy
        # cohort -- clear the cache so the deploy gate loads its own ASOF window (same fix as the per-fold
        # clear in causal_validate). Default GATE=leader_dd never reaches this branch.
        _ODS._ACTS_CACHE.clear()
        excl, _diag = our_dd_exclude(list(cand.index), ASOF,
                                     window_days=OUR_DD_WINDOW_DAYS, target_dd=OUR_DD_TARGET)
        print(f"[Phase-3 OUR-DD gate] top-{len(cand)} candidates -> excluded {len(excl)} via greedy-LOO "
              f"to cohort OUR-DD<={OUR_DD_TARGET:.0f}% (window {OUR_DD_WINDOW_DAYS}d, $150/leader)")
    else:
        excl = mtm_dd_exclude(list(cand.index))
        print(f"[Phase-3 mark-to-market DD gate] top-{len(cand)} candidates -> excluded {len(excl)} near-ruin (MTM DD>=70% or 60-70%&down)")
    s = s[~s.index.isin(excl)].copy()
    if SELECT == "setcover":
        # MIN wallets, MAX coin coverage. Greedy set-cover over copyable (net-positive) coins among the
        # skill-eligible pool. Few good wallets that together span the most coins (Alberto 2026-06-28).
        chosen, covered = set_cover_select(s, j[j.wallet.isin(s.index)], k_max=K)
        top = s.loc[chosen].reset_index()
        top = top.sort_values("skill", ascending=False).reset_index(drop=True)
        print(f"\n=== SET-COVER COHORT (min wallets / max coins, copyable+active, data <= {ASOF}) ===")
        print(f"  pool: {len(s)} eligible | selected {len(top)} wallets covering {len(covered)} coins")
    else:
        top = s.nlargest(K, "skill").reset_index()
        # coin coverage of the chosen cohort (net-positive coins per wallet, union)
        jc = j[j.wallet.isin(top["wallet"])]
        covered = set()
        for _w, _g in jc.groupby("wallet"):
            _m = _g.groupby("coin")["net_ret"].mean()
            covered |= set(_m[_m > 0].index)
        print(f"\n=== DEPLOYABLE SKILL COHORT (top {K}, copyable+active, data <= {ASOF}) ===")
        print(f"  pool: {len(s)} eligible | selected {len(top)} wallets covering {len(covered)} coins")
    print(f"  win mean {top.win.mean():.3f} | sharpe mean {top.sharpe.mean():.2f} | "
          f"median journeys {int(top.n.median())} | liq mean {top.liq.mean():.4f} | "
          f"median mean-ret {top['mean'].median()*1e4:+.0f}bps"
          + (f" | consistency mean {top['consistency'].mean():.2f}" if V17 else ""))

    cohort = {}
    for i, r in top.iterrows():
        cohort[r["wallet"]] = {"group": "v17_allcoin_setcover" if SELECT == "setcover" else "v16_skill_decile",
                                "rank": int(i + 1),
                                "skill_win": round(float(r["win"]), 3),
                                "skill_sharpe": round(float(r["sharpe"]), 2),
                                "n_rt": int(r["n"])}
    method = ("skill_rank z(win)+z(sharpe)+z(-maxdd) on copyable coins, active>=14d, n>=40" if not V17
              else f"UNIVERSE={UNIVERSE} COST={COST} SELECT={SELECT} | "
                   f"z(win)+z(sharpe)+z(-maxdd)+z(consistency) on net_ret, active>=14d, n>=40")
    out = {"cohort_asof": f"{ASOF}T23:59:59.000000+00:00", "n": len(cohort),
           "method": method,
           "n_coins_covered": len(covered),
           "wallets": cohort}
    outpath = "/tmp/skill_cohort_v17_allcoin.json" if V17 else "/tmp/skill_cohort_deploy.json"
    json.dump(out, open(outpath, "w"), indent=2)
    cur = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    print(f"  overlap with current live cohort: {len(set(cohort) & cur)}/{len(cohort)}")
    print(f"  saved {outpath} ({len(cohort)} wallets)")


if __name__ == "__main__":
    main()
