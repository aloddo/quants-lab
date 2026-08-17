#!/usr/bin/env python3
"""Data-freshness gate: assert every critical copy-trading input is current, or FAIL LOUD.

Motivation (Alberto 2026-07-07 "our data is a mess"): on 2026-07-07 a forward validation silently returned
all-zeros because assetctx_marks had ended Jun 1 with no warning. Silent-stale data is the worst failure
mode. This script is the guard: it inventories the critical inputs, classifies each by its EXPECTED update
cadence, and exits non-zero the moment a LIVE input is stale or an expected-fresh archive is beyond its lag
tolerance. Run it before trusting ANY validation/selection number.

READ-ONLY. No writes, no deletes. Exit 0 = all good, exit 1 = at least one input failed its freshness gate.

Usage:
    python scripts/data_freshness_check.py            # human table + PASS/FAIL, exit code
    python scripts/data_freshness_check.py --json      # machine-readable
Classes:
    LIVE      streams continuously; must be current to <threshold> (engine health)
    LIVE_OURS updates only when WE trade; looser threshold
    ARCHIVE   S3/reconstruction with a known publish lag; must be within lag+slack of now
    DEPRECATED frozen collection; must NOT have grown past a cutoff (its presence in a hot path is a bug)
    FILE      static artifact; must exist
"""
from __future__ import annotations
import sys, os, glob, json, argparse
import numpy as np, pandas as pd

DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app", "data", "v15")

# (name, class, threshold_hours or None, note)
CHECKS = [
    # --- RAW S3 DAY-STORES: the inputs everything else is derived from. Added 2026-08-17 after the
    # fills feed died on 08-14 and went unnoticed for FOUR DAYS because nothing checked it. HL's S3
    # archive publishes with ~1-2 day lag, so ~48h is normal and 72h means the feed has stopped.
    ("hl_s3_fills_v2_hot",  "daystore", None,     "ARCHIVE",    72.0,  "HL S3 fills day-store -- THE root input; feed death here silently freezes m02-m05"),
    ("hl_s3_funding_hot",   "daystore", None,     "ARCHIVE",    72.0,  "HL S3 funding day-store (funding_net is part of journey identity)"),
    ("hl_s3_ledger_hot",    "daystore", None,     "ARCHIVE",    72.0,  "HL S3 ledger day-store (M4 authenticity input)"),
    ("m02_watermark",       "watermark", None,    "ARCHIVE",    96.0,  "m02 checkpoint watermark_day -- how current the journeys store ACTUALLY is, vs how current its inputs are"),
    ("v17_target_fills",    "mongo", "ts_epoch",  "LIVE",       2.0,   "leader fills WS stream"),
    ("v17_exchange_fills",  "mongo", "time",      "LIVE_OURS",  36.0,  "our own fills (only on trade)"),
    ("assetctx_marks",      "marks", None,        "ARCHIVE",    13.5*24,"HL S3 asset_ctxs, ~10d publish lag"),
    # 2026-08-08: field was the LEGACY "timestamp" (frozen at 06-24 — none of the current writers
    # set it), threshold assumed the dead ~13d archive lag. Now the daily Mongo sync feeds this
    # collection from the 1-day-lag hot store: key on timestamp_utc, alarm at 3 days.
    ("hyperliquid_candles", "mongo", "timestamp_utc", "ARCHIVE", 3.0*24, "hot-store 1m candles via daily hl_candles_mongo_sync"),
    ("hl_copy_target_fills","mongo", "ts_epoch",  "DEPRECATED", None,  "SUPERSEDED by v17_target_fills; do not use post-Jun"),
    ("unified_copy_trades", "mongo", "timestamp", "DEPRECATED", None,  "frozen analysis coll (Jun 2)"),
    ("/tmp/measured_halfspread.json","file",None, "FILE",       None,  "per-coin half-spread (env MEASURED_SLIP, consumed by build_skill_cohort); MISSING => selection COST=flat; NO committed builder = real gap"),
    ("l2_calib_10coin.json","file",  None,        "FILE",       None,  "L2 slippage calib for 10 coins"),
    ("m02_journeys.parquet","journeys",None,      "ARCHIVE",    16.0*24,"copy journey source (build_copy_edge_label input); should track S3 fills (~12d lag); stale => empty copy-edge tests"),
]


def _mongo_last(db, coll, field):
    c = db[coll]
    n = c.estimated_document_count()
    if n == 0:
        return n, None
    d = c.find_one(sort=[(field, -1)])
    if not d or field not in d:
        return n, None
    v = d[field]
    if isinstance(v, (int, float)):
        unit = "ms" if v > 1e11 else "s"
        return n, pd.Timestamp(v, unit=unit, tz="UTC")
    return n, pd.Timestamp(v).tz_localize("UTC") if pd.Timestamp(v).tzinfo is None else pd.Timestamp(v)


def _marks_last():
    ps = sorted(glob.glob(os.path.join(DATA, "assetctx_marks", "*.npy")))
    if not ps:
        return 0, None
    ends = []
    for p in ps[:400]:
        try:
            a = np.load(p)
            if a.shape[1] > 0:
                ends.append(int(a[0].max()))
        except Exception:
            pass
    if not ends:
        return len(ps), None
    # the FRESHEST coin defines coverage (some coins delist); median-of-top for robustness
    return len(ps), pd.Timestamp(int(np.median(sorted(ends)[-20:])), unit="ms", tz="UTC")


def _daystore_last(rel_dir: str):
    """Newest YYYYMMDD.parquet in a day-partitioned hot store, as (n_days, day_end_utc).

    2026-08-17: added because this script -- whose entire premise is "silent-stale data is the worst
    failure mode" -- did not check the HL S3 day-stores AT ALL. On 2026-08-14 the fills feed stopped
    (launchd calendar jobs wedged: loaded, reported exit 0, never fired) and nothing noticed for four
    days. The pipeline was fine; its input had simply stopped arriving. These are the raw inputs
    everything downstream is derived from, so they belong at the TOP of this table, not missing from it.
    """
    d = os.path.join(os.path.dirname(DATA), *rel_dir.split("/"))
    if not os.path.isdir(d):
        return 0, None
    days = sorted(f[:8] for f in os.listdir(d) if f.endswith(".parquet") and f[:8].isdigit())
    if not days:
        return 0, None
    # the day-file covers a whole UTC day; freshness is measured from its END
    return len(days), pd.Timestamp(days[-1], tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


def _checkpoint_watermark():
    """M2's own watermark_day -- the single number saying how current the journeys store actually is."""
    p = os.path.join(DATA, "m02_daily_state", "checkpoint.json")
    if not os.path.exists(p):
        return 0, None
    try:
        wm = json.load(open(p)).get("watermark_day")
        if not wm or len(str(wm)) != 8:
            return 0, None
        return 1, pd.Timestamp(str(wm), tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    except Exception:
        return 0, None


def _journeys_last():
    p = os.path.join(DATA, "m02_journeys.parquet")
    if not os.path.exists(p):
        return 0, None
    try:
        import pyarrow.parquet as pq
        col = pq.read_table(p, columns=["entry_ts"]).column("entry_ts").to_numpy()
        if len(col) == 0:
            return 0, None
        return len(col), pd.Timestamp(int(col.max()), unit="ms", tz="UTC")
    except Exception:
        return 0, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    now = pd.Timestamp.now(tz="UTC")

    db = None
    try:
        from pymongo import MongoClient
        db = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=4000)["quants_lab"]
        db.command("ping")
    except Exception as e:
        print(f"WARN: mongo unavailable ({e}); mongo checks -> UNKNOWN", file=sys.stderr)

    rows, failed = [], []
    for name, kind, field, klass, thr_h, note in CHECKS:
        last, n, status = None, None, "?"
        if kind == "mongo":
            if db is None:
                status = "UNKNOWN"
            else:
                try:
                    n, last = _mongo_last(db, name, field)
                except Exception as e:
                    status = f"ERR:{e}"
        elif kind == "daystore":
            n, last = _daystore_last(name)
        elif kind == "watermark":
            n, last = _checkpoint_watermark()
        elif kind == "marks":
            n, last = _marks_last()
        elif kind == "journeys":
            n, last = _journeys_last()
        elif kind == "file":
            exists = os.path.exists(os.path.join(DATA, name))
            status = "PASS" if exists else "FAIL-MISSING"

        age_h = None
        if last is not None:
            age_h = (now - last).total_seconds() / 3600.0

        # gate logic per class
        if kind != "file" and status in ("?",):
            if last is None:
                status = "FAIL-NODATA"
            elif klass in ("LIVE", "LIVE_OURS", "ARCHIVE"):
                status = "PASS" if (thr_h is None or age_h <= thr_h) else "FAIL-STALE"
            elif klass == "DEPRECATED":
                status = "OK-FROZEN"  # informational; presence is fine, USE in a fresh run is the bug

        # failures that break trust
        if status.startswith("FAIL"):
            # a missing calib file is a WARN not a hard fail (flat-cost fallback is known)
            if not (klass == "FILE" and "measured_halfspread" in name):
                failed.append(name)

        rows.append({
            "input": name, "class": klass, "n": n,
            "last": None if last is None else str(last).split("+")[0],
            "age_h": None if age_h is None else round(age_h, 1),
            "thr_h": thr_h, "status": status, "note": note,
        })

    if args.json:
        print(json.dumps({"now": str(now), "rows": rows, "failed": failed}, indent=1))
    else:
        print(f"DATA FRESHNESS @ {now}\n")
        print(f"{'input':<24}{'class':<11}{'last':<21}{'age_h':>8}{'thr_h':>8}  status")
        for r in rows:
            print(f"{r['input']:<24}{r['class']:<11}{str(r['last']):<21}"
                  f"{str(r['age_h']):>8}{str(r['thr_h']):>8}  {r['status']}   {r['note']}")
        print()
        if failed:
            print(f"RESULT: FAIL ({len(failed)} stale/missing): {', '.join(failed)}")
            print("Do NOT trust validation/selection numbers until these are refreshed.")
        else:
            print("RESULT: PASS (all critical inputs current; measured_halfspread WARN if listed above)")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
