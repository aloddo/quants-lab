#!/usr/bin/env python
"""Build per-cohort-wallet equity-band artifact for the MID-equity selection tilt.

Output: app/data/v16/v17_cohort_equity_band.parquet  [wallet, eq_med, band]
  band in {minnow(<$10k), mid($10k-100k), pro($100k-1M), whale(>$1M), unknown}

Covers the POST-SWAP 100-wallet v17 expansion cohort (config/copy_trader_wallets_v17_expansion.json).
Equity recovered EXACTLY as research/2026-06-13-mid-equity-coverage-recovery did:
  1. m01_clean_universe.eq_med (as-is, >0)
  2. m02_cohort_slice.source_equity_post -- median per wallet, exclude equity_degraded, >0
  3. m02_actions.parquet -- STREAMED row-group by row-group (pyarrow), in-window-agnostic
     per-wallet median source_equity_post (exclude equity_degraded, >0), for any STILL-missing.
     MEMORY-SAFE: one row group projected to 3 cols at a time; NEVER load the 4.6GB whole.
Wallets with no recoverable positive equity -> band="unknown".

Read-only on all source data. Writes only the output parquet.
"""
import json
import sys

import pandas as pd
import pyarrow.parquet as pq

DATA = "/Users/hermes/quants-lab/app/data"
M01 = f"{DATA}/v15/m01_clean_universe.parquet"
COHORT = f"{DATA}/v16/m02_cohort_slice.parquet"
ACTIONS = f"{DATA}/v15/m02_actions.parquet"
CFG = "config/copy_trader_wallets_v17_expansion.json"
OUT = "app/data/v16/v17_cohort_equity_band.parquet"


def band_of(eq):
    if eq is None or not (eq > 0):
        return "unknown"
    if eq < 10_000:
        return "minnow"
    if eq < 100_000:
        return "mid"
    if eq < 1_000_000:
        return "pro"
    return "whale"


def main():
    cfg = json.load(open(CFG))
    cohort = {w.lower() for w in cfg["wallets"].keys()}
    print(f"[cfg] cohort wallets: {len(cohort)}")

    eq = {}  # wallet -> equity (first available source wins)
    src = {}  # wallet -> provenance

    # ---- Source 1: m01 eq_med (as-is, >0) ----
    m01 = pd.read_parquet(M01, columns=["wallet", "eq_med"])
    m01["wallet"] = m01["wallet"].str.lower()
    m01 = m01[m01.wallet.isin(cohort)]
    for w, v in zip(m01.wallet, m01.eq_med):
        if w not in eq and v is not None and v > 0:
            eq[w] = float(v)
            src[w] = "m01"
    print(f"[m01]    recovered {sum(1 for s in src.values() if s == 'm01')} "
          f"(coverage {len(eq)}/{len(cohort)})")

    # ---- Source 2: m02_cohort_slice median source_equity_post (exclude degraded, >0) ----
    missing = cohort - set(eq)
    if missing:
        cs = pd.read_parquet(COHORT, columns=["wallet", "source_equity_post", "equity_degraded"])
        cs["wallet"] = cs["wallet"].str.lower()
        cs = cs[cs.wallet.isin(missing) & (~cs.equity_degraded.astype(bool))
                & (cs.source_equity_post > 0)]
        med = cs.groupby("wallet")["source_equity_post"].median()
        for w, v in med.items():
            if w not in eq and v > 0:
                eq[w] = float(v)
                src[w] = "m02_cohort_slice"
    print(f"[cohort] recovered {sum(1 for s in src.values() if s == 'm02_cohort_slice')} "
          f"(coverage {len(eq)}/{len(cohort)})")

    # ---- Source 3: STREAMED m02_actions.parquet (still-missing only) ----
    missing = cohort - set(eq)
    if missing:
        pf = pq.ParquetFile(ACTIONS)
        # accumulate per-wallet equity samples in bounded chunks (one row group at a time).
        # store only the values for the few still-missing wallets -> tiny memory footprint.
        samples = {w: [] for w in missing}
        cols = ["wallet", "source_equity_post", "equity_degraded"]
        for rg in range(pf.num_row_groups):
            tbl = pf.read_row_group(rg, columns=cols)
            df = tbl.to_pandas()
            df["wallet"] = df["wallet"].str.lower()
            df = df[df.wallet.isin(missing)
                    & (~df.equity_degraded.astype(bool))
                    & (df.source_equity_post > 0)]
            if len(df):
                for w, v in zip(df.wallet, df.source_equity_post):
                    samples[w].append(float(v))
            del tbl, df
        for w, vals in samples.items():
            if vals:
                s = pd.Series(vals).median()
                if s > 0:
                    eq[w] = float(s)
                    src[w] = "m02_actions"
    print(f"[actions] recovered {sum(1 for s in src.values() if s == 'm02_actions')} "
          f"(coverage {len(eq)}/{len(cohort)})")

    # ---- Assemble output (one row per cohort wallet; unknown for unrecovered) ----
    rows = []
    for w in sorted(cohort):
        e = eq.get(w)
        rows.append({"wallet": w, "eq_med": e, "band": band_of(e)})
    out = pd.DataFrame(rows)
    out.to_parquet(OUT, index=False)

    known = (out.band != "unknown").sum()
    print(f"\n[out] {OUT}")
    print(f"[out] coverage (known band): {known}/{len(out)}")
    print(f"[out] band counts: {out['band'].value_counts().to_dict()}")
    print(f"[out] provenance: {pd.Series(src).value_counts().to_dict()}")


if __name__ == "__main__":
    sys.exit(main())
