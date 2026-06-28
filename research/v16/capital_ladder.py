#!/usr/bin/env python
"""
DEPRECATED / THROWAWAY SCRATCH (Alberto correction 9865, 2026-06-22): this standalone replay bypassed the
BINDING V15 harness (projects/quant/v15/copy-research-infra). It is journey-level (not action-level),
uses flat fees (not execution_model.py), and its self-equity-path gates introduced a capacity-inflation
bug. The CORRECT capital study is research/v15/v15_m09_sim.py swept over b0 (it has min_notional_feasible
+ the capital ledger). NUMBERS HERE ARE NOT AUTHORITATIVE. Capital is parked until the live model proves
itself (n>=200). Kept only as scratch history.

capital_ladder.py -- Alberto 9857 part 2, deliverable #1: the CAPITAL LADDER evidence pack.

Question: is the copy strategy structurally UNDERCAPITALIZED at $500? At each equity level, how much of
the leader edge do we capture, and where does capital stop helping?

Built on the codex-VALIDATED replay core of cap_level_sweep_v2.py (the model that shipped gross 4.0). Same
journey set, same consistent-N fill model, same gross/netx/backstop gate semantics. Only change: SWEEP equity
EQ0 in {500,1k,2.5k,5k,10k} and measure coverage + fraction-of-leader-edge-captured + ROE.

TWO ORDER-SIZE REGIMES (the honest fork the drawing-board glossed):
  A) FIXED $150 order (live config today). Gross cap = gross*eq grows with capital while order stays $150,
     so position CAPACITY = gross*eq/150 grows linearly -> coverage rises until it saturates near 100% of
     leader actions. This isolates "is the gross-gate capacity binding at $500?".
  B) PROPORTIONAL order = 150 * eq/500 (scale the bet with the bankroll). Capacity in POSITIONS is then
     ~constant across the ladder (gross cap and order both scale) -> coverage ~flat -> capital lifts $/mo
     linearly but NOT coverage/diversification. This is the ROE-max regime and the realistic null for
     "more capital = more edge captured".
  The $10 HL min only bites in regime B at high fragmentation; reported as min_skips.

The contrast between A and B is the actual evidence: capital helps COVERAGE only if you DON'T scale order
size proportionally (regime A). If you scale proportionally (B) capital only multiplies $/mo at flat ROE.

netx=2.5, gross=4.0, backstop=5.0 = LIVE config. Run:
  ~/miniforge3/envs/quants-lab/bin/python research/v16/capital_ladder.py
"""
import json
import numpy as np
import pandas as pd
from pymongo import MongoClient

RT = 11.0; NETX = 2.5; GROSS = 4.0; BACKSTOP = 5.0; HL_MIN = 10.0
EQ_GRID = [500.0, 1000.0, 2500.0, 5000.0, 10000.0]
BASE_EQ = 500.0; BASE_N = 150.0


def load_candles(coins):
    db = MongoClient("mongodb://localhost:27017").quants_lab
    out = {}
    for c in coins:
        rows = list(db.hyperliquid_candles_1h.find({"coin": c}, {"timestamp_utc": 1, "close": 1, "_id": 0}).sort("timestamp_utc", 1))
        if len(rows) > 20:
            df = pd.DataFrame(rows); out[c] = (df.timestamp_utc.to_numpy(), df.close.to_numpy())
    return out


def mark(cand, coin, t):
    if coin not in cand:
        return None
    ts, cl = cand[coin]; i = np.searchsorted(ts, t)
    return cl[min(i, len(cl) - 1)] if len(ts) else None


def load_journeys():
    sk = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.wallet.isin(sk)) & (~j.coin.str.startswith("xyz:")) & (j.max_position_notional > 10)].copy()
    j["ret"] = j.net_realized_pnl / j.max_position_notional
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].dropna(subset=["t_en", "t_ex"]).sort_values("t_en").reset_index(drop=True)
    return j


def replay(j, cand, eq0, order_n, gate_fixed=False):
    """Consistent-notional replay. order_n is the per-fill notional at this equity level.
    gate_fixed=True scales the netx/gross/backstop gates off the CONSTANT eq0 instead of the
    model equity path (codex fix: model PnL is ~26x inflated, so an eq-path gate manufactures
    capacity and biases coverage HIGH). gate_fixed=True is the conservative honest bound.
    Returns dict with copies, coverage, edge-capture, $/mo, ROE, maxDD, peak_netx, trips, min_skips."""
    realized = 0.0; open_pos = []; taken = 0; eq_curve = []; peak_netx = 0.0
    backstop_trips = 0; min_skips = 0
    taken_pnl = 0.0; avail_pnl = 0.0  # for fraction-of-leader-edge-captured (per-unit-notional comparable)
    n_avail = 0
    for r in j.itertuples():
        now = r.t_en
        # close matured
        still = []
        for ex, c, sg, epx, p in open_pos:
            if ex <= now: realized += p
            else: still.append((ex, c, sg, epx, p))
        open_pos = still
        # MTM + aggregates
        mtm = 0.0; net_ntl = 0.0; gross_ntl = 0.0; marks = []
        for ex, c, sg, epx, p in open_pos:
            mpx = mark(cand, c, now); upl = sg * (mpx - epx) / epx * order_n if (mpx and epx) else 0.0
            mtm += upl; net_ntl += sg * order_n; gross_ntl += order_n; marks.append(upl)
        eq = eq0 + realized + mtm; eq_curve.append(eq)
        geq = eq0 if gate_fixed else eq   # codex fix: gate off constant capital, not inflated path
        # backstop trim
        if gross_ntl > BACKSTOP * max(geq, 1) and open_pos:
            backstop_trips += 1
            order = np.argsort(marks); keep = list(open_pos)
            target = GROSS * max(geq, 1); g = gross_ntl
            for idx in order:
                if g <= target: break
                ex, c, sg, epx, p = open_pos[idx]
                realized += p; g -= order_n; keep[idx] = None
            open_pos = [pp for pp in keep if pp is not None]
            net_ntl = sum(sg * order_n for _, _, sg, _, _ in open_pos); gross_ntl = g
        # this leader action is "available" edge regardless of whether we take it
        n_avail += 1; avail_pnl += (r.ret - RT / 1e4) * order_n
        # HL $10 min floor
        if order_n < HL_MIN:
            min_skips += 1; continue
        # entry gates (consistent N)
        new_signed = r.sgn * order_n
        if abs(net_ntl + new_signed) > NETX * geq and abs(net_ntl + new_signed) > abs(net_ntl):
            continue
        if gross_ntl + order_n > GROSS * max(geq, 1):
            continue
        epx = mark(cand, r.coin, now)
        pnl = (r.ret - RT / 1e4) * order_n
        open_pos.append((r.t_ex, r.coin, r.sgn, epx if epx else 1, pnl)); taken += 1
        taken_pnl += pnl
        peak_netx = max(peak_netx, abs(net_ntl + new_signed) / max(eq, 1))
    for ex, c, sg, epx, p in open_pos: realized += p
    ec = np.array(eq_curve); peak = np.maximum.accumulate(ec)
    maxdd = ((peak - ec) / peak).max() * 100 if len(ec) else 0
    ndays = (j.t_en.max() - j.t_en.min()) / 86400e3
    usd_mo = realized / ndays * 30.4
    return dict(taken=taken, n_avail=n_avail, coverage=taken / max(n_avail, 1) * 100,
                edge_capture=taken_pnl / avail_pnl * 100 if avail_pnl else 0,
                usd_mo=usd_mo, roe_mo=usd_mo / eq0 * 100, peak_netx=peak_netx,
                maxdd=maxdd, trips=backstop_trips, min_skips=min_skips)


def main():
    j = load_journeys()
    cand = load_candles([c for c in j.coin.value_counts().index if j.coin.value_counts()[c] >= 30])
    j = j[j.coin.isin(cand)].reset_index(drop=True)
    ndays = (j.t_en.max() - j.t_en.min()) / 86400e3
    print(f"journeys {len(j)} over {ndays:.0f}d ({(j.sgn>0).mean()*100:.0f}% long). "
          f"netx={NETX} gross={GROSS} backstop={BACKSTOP}. order regimes: FIXED $150 vs PROPORTIONAL.\n")

    for label, order_fn, gf in [
            ("A) FIXED order $150 (eq-path gates)", lambda eq: BASE_N, False),
            ("A2) FIXED order $150 (FIXED-eq gates -- codex honest bound)", lambda eq: BASE_N, True),
            ("B) PROPORTIONAL order = 150*eq/500", lambda eq: BASE_N * eq / BASE_EQ, False)]:
        print(f"=== REGIME {label} ===")
        print(f"{'eq':>7}{'order':>8}{'copies':>8}{'avail':>7}{'cover%':>8}{'edge%':>8}"
              f"{'$/mo':>9}{'ROE%/mo':>9}{'pkNetx':>8}{'maxDD%':>8}{'trips':>7}{'minskp':>7}")
        base_mo = None
        for eq in EQ_GRID:
            d = replay(j, cand, eq, order_fn(eq), gate_fixed=gf)
            if base_mo is None: base_mo = d["usd_mo"]
            print(f"{eq:>7.0f}{order_fn(eq):>8.0f}{d['taken']:>8}{d['n_avail']:>7}{d['coverage']:>7.0f}%"
                  f"{d['edge_capture']:>7.0f}%{d['usd_mo']:>9.0f}{d['roe_mo']:>8.1f}%{d['peak_netx']:>7.2f}x"
                  f"{d['maxdd']:>8.1f}{d['trips']:>7}{d['min_skips']:>7}")
        print()

    print("READ:")
    print("  Regime A (fixed $150): if coverage + edge% RISE with equity, the gross-gate CAPACITY is the")
    print("    binding constraint at $500 -> strategy is undercapitalized; more capital buys more leader")
    print("    coverage at ~flat per-trade size until coverage saturates. $/mo grows, ROE may compress.")
    print("  Regime B (proportional): coverage ~flat, $/mo scales ~linearly with capital at ~flat ROE ->")
    print("    capital is a MULTIPLIER not a coverage-unlock; the $500 book already expresses the strategy.")
    print("  The TRUE answer to 'undercapitalized?' is which regime matches live. Live runs FIXED $150 ->")
    print("    regime A is the honest read for the current design; regime B is the design-if-we-scale.")


if __name__ == "__main__":
    main()
