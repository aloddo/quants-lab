"""
Copyability MARKOUT study (codex spine, W3) -- prereg copy-rebuild/2026-06-28-markout-study-prereg.

Root question: does copyable alpha survive OUR execution (taker, 1.86s late, RT fee+slip)? For every leader
position establishment (journey), simulate our entry at fill+1.86s and measure costed directional markout to
+5m/+15m/+1h/+4h. Rank wallets on a TRAIN half, score OOS on a TEST half. Fast/vectorized over mmap npy cache.
"""
import numpy as np, pandas as pd, urllib.parse as ul
from pathlib import Path
from scipy.stats import spearmanr

import os
CACHE = Path(os.environ.get("MARKOUT_CACHE", "app/data/v15/assetctx_marks"))  # FULL-HISTORY [minute, price]
FEE_RT = 0.000864
SLIP_ONEWAY = 0.00047           # class default; sensitivity reported separately
MS_MIN = 60_000
HORIZONS = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}
LAT_MS = 1860

def px_at(mins, c, ts_arr):
    """realized close of the bar containing each ts (vectorized). nan if out of range."""
    idx = np.searchsorted(mins, ts_arr, side="right") - 1
    ok = idx >= 0
    out = np.full(len(ts_arr), np.nan)
    out[ok] = c[idx[ok]]
    return out

def build_priced(slip_oneway=SLIP_ONEWAY, smoke_coins=None, min_notional=50,
                 min_hold_h=None, max_addon=None, coins_keep=None):
    cols = ["wallet","coin","side","entry_ts","exit_ts","duration_h","n_addon_fills",
            "max_position_notional","net_realized_pnl"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.max_position_notional > min_notional) & j.entry_ts.notna()].copy()
    if min_hold_h is not None:
        j = j[j.duration_h >= min_hold_h]
    if max_addon is not None:
        j = j[j.n_addon_fills <= max_addon]
    if coins_keep is not None:
        j = j[j.coin.isin(coins_keep)]
    j["sign"] = np.where(j.side.astype(str).str.lower().eq("long"), 1.0, -1.0)
    slip_rt = 2 * slip_oneway
    cost_rt = FEE_RT + slip_rt

    coins = sorted(j.coin.unique())
    if smoke_coins:
        coins = coins[:smoke_coins]
    parts = []
    miss = 0
    for coin in coins:
        p = CACHE / f"{coin}.npy"
        if not p.exists():
            p = CACHE / f"{ul.quote(coin, safe='')}.npy"
        if not p.exists():
            miss += 1; continue
        arr = np.load(p, mmap_mode="r")
        mins = np.asarray(arr[0], dtype="int64"); c = np.asarray(arr[1], dtype="float64")  # [minute, price]
        if mins.size == 0: continue
        g = j[j.coin == coin]
        e_ts = g.entry_ts.values.astype("int64")
        # COVERAGE GUARD: only price journeys whose [entry, entry+4h] is within this coin's cached range
        # (else searchsorted returns a stale edge bar -> garbage markout, the ohlc_cache artifact).
        cov = (e_ts >= int(mins[0])) & (e_ts + HORIZONS["4h"] * MS_MIN <= int(mins[-1]))
        g = g[cov]; e_ts = e_ts[cov]
        if len(g) == 0: continue
        entry_px = px_at(mins, c, e_ts + LAT_MS)
        rec = {"wallet": g.wallet.values, "coin": coin, "sign": g.sign.values,
               "duration_h": g.duration_h.values, "n_addon": g.n_addon_fills.values,
               "notional": g.max_position_notional.values, "entry_ts": e_ts,
               "raw_pnl": g.net_realized_pnl.values, "entry_px": entry_px}
        for name, mn in HORIZONS.items():
            fpx = px_at(mins, c, e_ts + mn * MS_MIN)
            ret = (fpx - entry_px) / entry_px * g.sign.values
            rec[f"mk_{name}"] = ret - cost_rt
        parts.append(pd.DataFrame(rec))
    df = pd.concat(parts, ignore_index=True)
    df = df[df.entry_px.notna() & (df.entry_px > 0)]
    print(f"journeys priced: {len(df):,} over {df.coin.nunique()} coins (missing-cache coins: {miss})")
    return df

def analyze(df, label=""):
    print(f"\n##### ANALYSIS {label} #####")
    # ---- pooled costed markout by horizon (is there ANY copyable alpha after costs?) ----
    print("\n=== POOLED costed delayed-entry markout (after RT fee+slip), bps ===")
    for h in HORIZONS:
        m = df[f"mk_{h}"].dropna()
        print(f"  {h:>4}: mean {m.mean()*1e4:+7.1f}  median {m.median()*1e4:+7.1f}  %pos {(m>0).mean():.3f}  n {len(m):,}")

    # ---- per-wallet OOS persistence (rank on TRAIN half, score TEST half) ----
    cut = df.entry_ts.quantile(0.70)
    tr, te = df[df.entry_ts <= cut], df[df.entry_ts > cut]
    PRIMARY = "mk_1h"
    def wal_agg(d):
        gg = d.groupby("wallet").agg(mk=(PRIMARY,"mean"), raw=("raw_pnl","sum"), n=(PRIMARY,"size"))
        return gg
    a_tr, a_te = wal_agg(tr), wal_agg(te)
    a_tr = a_tr[a_tr.n >= 20]
    m = a_tr.join(a_te, rsuffix="_te", how="inner")
    m = m[m.n_te >= 5]
    print(f"\n=== OOS wallet persistence (primary {PRIMARY}), {len(m)} wallets w/ >=20 train & >=5 test ===")
    m["dec"] = pd.qcut(m.mk.rank(method="first"), 10, labels=False)
    dd = m.groupby("dec").agg(train_mk=("mk","mean"), test_mk=("mk_te","mean"),
                              test_mk_med=("mk_te","median"), n_te=("n_te","sum"))
    dd["train_mk"]*=1e4; dd["test_mk"]*=1e4; dd["test_mk_med"]*=1e4
    print(dd.round(1).to_string())
    sp = spearmanr(m.mk, m.mk_te).correlation
    top = dd.loc[9]; bot = dd.loc[0]
    # baselines on TEST: random, rank-by-raw-PnL
    rng = np.random.default_rng(3)
    rand_top = m.assign(r=rng.standard_normal(len(m))).nlargest(max(1,len(m)//10),"r").mk_te.mean()*1e4
    pnl_top = m.nlargest(max(1,len(m)//10),"raw").mk_te.mean()*1e4
    print(f"\n  Spearman(train_mk, test_mk) = {sp:+.4f}")
    print(f"  TOP-decile OOS test markout: mean {top.test_mk:+.1f}bps median {top.test_mk_med:+.1f}bps (n {int(top.n_te)})")
    print(f"  BOT-decile OOS test markout: mean {bot.test_mk:+.1f}bps  (FADE check)")
    print(f"  baselines TOP-decile OOS: random {rand_top:+.1f}bps | rank-by-rawPnL {pnl_top:+.1f}bps")
    g1 = top.test_mk > 0
    g2 = top.test_mk > rand_top and top.test_mk > pnl_top
    g4 = sp > 0
    print(f"\n  GATE1 top-dec OOS markout>0: {g1} | GATE2 beats random&rawPnL: {g2} | GATE4 spearman>0: {g4}")
    print(f"  VERDICT (primary horizon): {'signal of copyable alpha -> deepen (W2 filter + sleeves)' if (g1 and g2 and g4) else 'NO copyable alpha at our execution -> supports REPOINT'}")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "broad"
    if mode == "broad":
        analyze(build_priced(), "BROAD (all journeys, perps universe)")
    elif mode == "subset":
        # W2 style-first: slow holds (>=2h), low churn (<=20 addons), liquid majors
        majors = ["BTC","ETH","SOL","HYPE","XRP","DOGE","SUI","TAO","AVAX","LINK","LTC","BNB",
                  "ARB","OP","APT","SEI","TIA","WLD","PUMP","FARTCOIN","ENA","AAVE","NEAR","INJ",
                  "ADA","DOT","UNI","PEPE","TON","TRX","FIL","ATOM","ZEC","VVV","kPEPE","kBONK"]
        df = build_priced(min_hold_h=2.0, max_addon=20, coins_keep=majors, min_notional=200)
        analyze(df, "SUBSET: slow-hold>=2h, churn<=20, liquid majors, notional>$200")
    elif mode.isdigit():
        analyze(build_priced(smoke_coins=int(mode)), f"SMOKE {mode} coins")
