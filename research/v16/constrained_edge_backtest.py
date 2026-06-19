#!/usr/bin/env python
"""
constrained_edge_backtest.py -- does the LIVE-CONSTRAINED setup keep the validated edge? (Alberto 9711/9713:
the netx/gross/knet caps were NOT in the +101bps OOS validation; backtest the current setup independently.)

Occupancy replay of the skill cohort's copyable journeys with $150/copy, EVOLVING equity, applying the two
portfolio caps the live engine enforces:
  - NETX  : net directional notional / equity <= 2.5x   (blocks entries that push net exposure over)
  - GROSS : total |notional| / equity        <= 3.5x   (blocks entries that push gross over)
Compares the realized copy-edge WITH caps vs UNCONSTRAINED (every signal copied = the validated baseline).
Reports edge bps, $/mo, fill-rate (how many copies blocked), and which cap binds. (knet/herd gate modeled
separately -- needs signal-time herd data; noted as a follow-up.)

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/constrained_edge_backtest.py
"""
import json
import numpy as np
import pandas as pd

BASE = 150.0
EQUITY0 = 505.0
RT_BPS = 11.0
NETX_CAP = 2.5
GROSS_CAP = 3.5


def main():
    sk = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    calib = set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys())
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "net_realized_pnl", "max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.wallet.isin(sk)) & (j.coin.isin(calib)) & (j.max_position_notional > 10)].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["sgn"] = j.side.str.lower().map(lambda s: 1.0 if "long" in str(s) else -1.0)
    j["t_en"] = j.entry_ts.astype("float64"); j["t_ex"] = j.exit_ts.astype("float64")
    j = j[j.t_ex > j.t_en].dropna(subset=["t_en", "t_ex"]).sort_values("t_en").reset_index(drop=True)
    ndays = (j.t_en.max() - j.t_en.min()) / 86400e3
    print(f"skill-cohort copyable journeys: {len(j)} over {ndays:.0f}d  ({(j.sgn>0).mean()*100:.0f}% long)\n")

    def replay(apply_caps):
        open_pos = []          # (t_ex, signed_notional, pnl)
        realized = []; taken = 0; blocked = {"netx": 0, "gross": 0}
        eq = EQUITY0
        for r in j.itertuples():
            now = r.t_en
            if open_pos:
                keep = []
                for ex, sn, p in open_pos:
                    if ex <= now:
                        realized.append((ex, p)); eq += p
                    else:
                        keep.append((ex, sn, p))
                open_pos = keep
            new_signed = r.sgn * BASE
            if apply_caps:
                net = sum(sn for _, sn, _ in open_pos)
                gross = sum(abs(sn) for _, sn, _ in open_pos)
                if abs(net + new_signed) > NETX_CAP * eq:
                    blocked["netx"] += 1; continue
                if gross + BASE > GROSS_CAP * eq:
                    blocked["gross"] += 1; continue
            pnl = (r.ret - RT_BPS / 1e4) * BASE
            open_pos.append((r.t_ex, new_signed, pnl)); taken += 1
        for ex, sn, p in open_pos:
            realized.append((ex, p))
        rl = pd.DataFrame(realized, columns=["t", "pnl"])
        tot = rl.pnl.sum()
        return dict(taken=taken, blocked=blocked, tot=tot,
                    edge_bps=tot / (taken * BASE) * 1e4 if taken else 0,
                    usd_mo=tot / ndays * 30.4)

    unc = replay(False)
    con = replay(True)
    print(f"{'setup':<24}{'copies':>8}{'edge_bps':>10}{'$/mo':>9}{'blocked':>22}")
    print(f"{'UNCONSTRAINED (validated)':<24}{unc['taken']:>8}{unc['edge_bps']:>10.1f}{unc['usd_mo']:>9.0f}{'--':>22}")
    print(f"{'LIVE CAPS (netx+gross)':<24}{con['taken']:>8}{con['edge_bps']:>10.1f}{con['usd_mo']:>9.0f}"
          f"{('netx '+str(con['blocked']['netx'])+' / gross '+str(con['blocked']['gross'])):>22}")
    fill = con['taken'] / unc['taken'] * 100 if unc['taken'] else 0
    edge_keep = con['edge_bps'] / unc['edge_bps'] * 100 if unc['edge_bps'] else 0
    usd_keep = con['usd_mo'] / unc['usd_mo'] * 100 if unc['usd_mo'] else 0
    print(f"\nFILL-RATE under caps: {fill:.0f}% of signals copied ({unc['taken']-con['taken']} blocked, "
          f"netx={con['blocked']['netx']} gross={con['blocked']['gross']}).")
    print(f"EDGE per-copy retained: {edge_keep:.0f}% | $/mo retained: {usd_keep:.0f}%")
    print(f"\nREAD: if edge_bps holds ~same but $/mo drops -> caps just throttle SIZE (edge intact, fewer copies).")
    print(f"If edge_bps DROPS materially -> the caps are systematically blocking the GOOD copies (e.g., the")
    print(f"net-long adds that recover) -> netx too tight for the long-biased cohort. (knet/herd gate = follow-up.)")


if __name__ == "__main__":
    main()
