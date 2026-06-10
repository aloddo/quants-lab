"""Prototype: closedPnl-based equity reconstruction (avoids synthetic-notional blowup).

equity(t) - equity(anchor) = realized(closedPnl) + funding - fees + external_ledger + (uPnL(t) - uPnL(anchor))

uPnL(x) = sum_coins pos_c(x) * (mark_c(x) - avg_entry_c(x))  -- BOUNDED by position size, not gross notional.
avg_entry tracked via avg-cost accounting through fills (handles adds/reduces/flips).
realized taken from HL's closedPnl (ground truth); fees from fill fee; funding from funding stream;
external_ledger = real deposits/withdrawals/transfers only.

Compare vs the current size*mark + synthetic-cash method on the 3 pure-perp quarantined wallets.
"""
import sys, math
import pandas as pd
import research.v15.v15_m01_equity_reconstruct as m1

S = int(pd.Timestamp('2025-12-01', tz='UTC').timestamp() * 1000)
E = int((pd.Timestamp('2026-05-23', tz='UTC') + pd.Timedelta(days=1)).timestamp() * 1000 - 1)


def avg_cost_state(fills, upto_ms):
    """Return {coin: (pos, avg_entry)} after applying all fills at-or-before upto_ms via avg-cost."""
    st = {}
    for f in fills:
        if f['time'] > upto_ms:
            break
        c = f['coin']; sz = f['signed_sz']; px = f['price']
        pos, entry = st.get(c, (0.0, 0.0))
        if pos == 0 or (pos > 0) == (sz > 0):
            # opening or adding same direction -> weighted avg entry
            newpos = pos + sz
            if abs(newpos) > 1e-12:
                entry = (entry * pos + px * sz) / newpos
            pos = newpos
        else:
            # reducing / closing / flipping
            newpos = pos + sz
            if (newpos > 0) != (pos > 0) and abs(newpos) > 1e-12:
                # flipped through zero -> new basis at this fill price
                entry = px
            pos = newpos
            if abs(pos) < 1e-12:
                pos = 0.0; entry = 0.0
        st[c] = (pos, entry)
    return st


def upnl_at(fills, t_ms):
    st = avg_cost_state(fills, t_ms)
    u = 0.0; unmarkable = 0
    for c, (pos, entry) in st.items():
        if abs(pos) < 1e-9:
            continue
        mk = m1.get_mark(c, t_ms) or m1._last_fill_price(fills, c, t_ms)
        if mk is None:
            unmarkable += 1; continue
        u += pos * (mk - entry)
    return u, unmarkable


def recon_drift_closedpnl(w, fund, led, fills, anchors):
    """Walk anchor->anchor via closedPnl identity; return list of |err|/cv per segment."""
    out = []
    m1._markpx_series.clear()
    for i in range(1, len(anchors)):
        pt, pv = anchors[i - 1]; ct, cv = anchors[i]
        if ct <= pt or abs(cv) < 0.01:
            continue
        realized = sum(float(f.get('raw', f).get('closedPnl', 0) or 0) for f in fills if pt < f['time'] <= ct)
        fees = sum(f.get('fee', 0) for f in fills if pt < f['time'] <= ct)
        fundg = sum(m1.funding_cash_delta(x) for x in fund if pt < int(x['time']) <= ct)
        ledg = sum(m1.ledger_cash_delta(x, w.lower()).cash for x in led if pt < int(x['time']) <= ct)
        u_t, un_t = upnl_at(fills, ct)
        u_a, un_a = upnl_at(fills, pt)
        recon = pv + realized + fundg - fees + ledg + (u_t - u_a)
        out.append(abs(recon - cv) / cv)
    return out


def main():
    wallets = sys.argv[1:] or ['0x4f3577ad', '0x7410a0f7', '0xa4f37e55']
    anchors_df = pd.read_parquet(m1.ANCHOR_PARQUET)
    import glob, os
    have = {os.path.basename(p).split('.')[0] for p in glob.glob(str(m1.S3_BY_WALLET_DIR) + '/*')}
    full = {w[:10]: w for w in have}
    for short in wallets:
        w = full.get(short[:10], short)
        anc = m1.load_wallet_anchor(w, anchors_df)
        fills = m1.load_wallet_fills(w, S, E)
        fund = m1.load_wallet_funding(w, S, E); led = m1.load_wallet_ledger(w, S, E)
        wa = [(int(t), float(v)) for t, v in m1.get_portfolio_all(w) if v > 0.01 and S <= int(t) <= E]
        if len(wa) < 2:
            print(f'{w[:10]}: no anchors'); continue
        # current method
        m1._markpx_series.clear()
        au = m1.reconstruct_wallet((w, anc, S, E, True, False))['audit']
        # closedPnl method
        ds = recon_drift_closedpnl(w, fund, led, fills, wa)
        import numpy as np
        a = np.abs(ds)
        print(f'{w[:10]}: CURRENT median={au["median_inter_anchor_drift_pct"]*100:.2f}% max={au["max_inter_anchor_drift_pct"]*100:.1f}% q={au["quarantined"]}'
              f'  |  CLOSEDPNL median={np.median(a)*100:.2f}% max={a.max()*100:.1f}% n={len(a)}')


if __name__ == '__main__':
    main()


def universe_test(floor):
    import glob, os, numpy as np
    anchors_df = pd.read_parquet(m1.ANCHOR_PARQUET)
    cached = [os.path.basename(p).replace('.json','') for p in glob.glob(str(m1.WHOLE_ANCHOR_CACHE)+'/*.json')]
    have = {os.path.basename(p).split('.')[0] for p in glob.glob(str(m1.S3_BY_WALLET_DIR)+'/*')}
    cand = [w for w in cached if w in have][:60]
    cur_q = cp_q = n = 0
    cur_meds = []; cp_meds = []
    for w in cand:
        try:
            anc = m1.load_wallet_anchor(w, anchors_df)
            if anc is None: continue
            fills = m1.load_wallet_fills(w, S, E)
            if not fills: continue
            fund = m1.load_wallet_funding(w, S, E); led = m1.load_wallet_ledger(w, S, E)
            wa = [(int(t), float(v)) for t, v in m1.get_portfolio_all(w) if v > floor and S <= int(t) <= E]
            if len(wa) < 2: continue
            m1._markpx_series.clear()
            au = m1.reconstruct_wallet((w, anc, S, E, True, False))['audit']
            if au.get('median_inter_anchor_drift_pct') is None: continue
            ds = np.abs(recon_drift_closedpnl(w, fund, led, fills, wa))
            if len(ds) == 0: continue
            n += 1
            cur_q += 1 if au['quarantined'] else 0
            cp_quar = (np.median(ds) > 0.10) or (ds.max() > 0.50)
            cp_q += 1 if cp_quar else 0
            cur_meds.append(au['median_inter_anchor_drift_pct']); cp_meds.append(np.median(ds))
        except Exception: pass
    print(f'floor=${floor}: n={n}')
    print(f'  CURRENT  quarantined={cur_q} ({100*cur_q/max(n,1):.0f}%) median-of-medians={np.median(cur_meds)*100:.2f}%')
    print(f'  CLOSEDPNL quarantined={cp_q} ({100*cp_q/max(n,1):.0f}%) median-of-medians={np.median(cp_meds)*100:.2f}%')
