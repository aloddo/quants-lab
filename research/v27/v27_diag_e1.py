#!/usr/bin/env python3
"""v27 DIAGNOSTIC (NOT a ship path, NOT pre-registered) — evaluate the E1 configs the
frozen Stage-1 halving ELIMINATED, over the FULL pooled window (blocks 4-18).

Purpose: answer the single v28 design question — did the 3-block successive-halving throw
away a real full-window edge in H2C1 (by killing the E1 branch on a mid-window regime
flip), or is H2C1 copy net-negative regardless of exit style / selection procedure?

This is DESCRIPTIVE / IN-SAMPLE and CANNOT justify any live deployment: reading a config's
full-window number after the fact and picking it is exactly the selection leak the v27
framework exists to prevent. Any config that looks good here must be re-established in a
fresh v28 pre-registration on unseen data before it can ship.

Reuses the CODE-GATED machinery verbatim: v27_stage1._block_worker, assemble_config_block,
select_round, roster_for_block. Only new code is this driver + config list.
"""
from __future__ import annotations

import json
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "research" / "v27"))

import v27_stage1 as s1  # noqa: E402

# E1 (simple stop) at tight/mid K — the branch the halving killed after round 1.
DIAG_CONFIGS = [c for c in s1.all_configs()
                if c["exit"] == "E1" and c["K"] in (5, 25)]
BLOCKS = list(range(4, 19))                      # full pooled window (same as finalists)


def main():
    procs = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    mem_gb = 2.0
    s1.install_memory_guard(soft_gb=mem_gb * 2, label="v27-diag-parent")
    t0 = time.time()
    print(f"v27 DIAGNOSTIC (in-sample, NOT a ship) — {len(DIAG_CONFIGS)} E1 configs "
          f"x blocks {BLOCKS}", flush=True)

    lcb_df = pd.read_parquet(s1.OUT_DIR / "lcb_table.parquet")
    jidx = s1.load_cell_index(s1.OUT_DIR)
    entity_map = s1.build_entity_map(s1.OUT_DIR)
    tasks = []
    for b in BLOCKS:
        roster = s1.roster_for_block(lcb_df, jidx, entity_map, b, s1.UNION_TOP)
        assert len(roster) >= s1.UNION_TOP, f"union undersupply block {b}: {len(roster)}"
        tasks.append({"block": b, "roster": roster.to_dict("records"),
                      "configs": DIAG_CONFIGS, "out_dir": Path("/tmp/v27_diag")})
    del jidx
    Path("/tmp/v27_diag").mkdir(parents=True, exist_ok=True)

    results = []
    with Pool(processes=min(procs, len(tasks)), initializer=s1._block_worker_init,
              initargs=(mem_gb,)) as pool:
        for res in pool.imap_unordered(s1._block_worker, tasks):
            results.append(res)
            print(f"  block {res['block']} done ({time.time()-t0:.0f}s)", flush=True)

    results.sort(key=lambda r_: r_["block"])
    series = {c["config_id"]: np.concatenate(
        [np.asarray(r_["series"][c["config_id"]]) for r_ in results])
        for c in DIAG_CONFIGS}
    sel = s1.select_round(series, len(DIAG_CONFIGS))
    sign = {c["config_id"]: float(np.mean(
        [np.mean(r_["series"][c["config_id"]]) > 0 for r_ in results]))
        for c in DIAG_CONFIGS}
    rows = []
    for cid in sel["order"]:
        st = sel["stats"][cid]
        tot = sum(r_["counters"].get(cid, {}).get("realized", 0)
                  for r_ in results if r_["counters"].get(cid))
        rows.append({"config_id": cid, "mean_bps_d": st["mean"] * 1e4,
                     "se_bps_d": st["se"] * 1e4, "lcb_adj_bps_d": st["lcb_adj"] * 1e4,
                     "block_sign_consistency": sign[cid], "realized_trips": tot})
    report = {"label": "DIAGNOSTIC in-sample (NOT a ship, NOT pre-registered)",
              "blocks": BLOCKS, "c_star": sel["c_star"], "rows": rows,
              "runtime_s": round(time.time() - t0)}
    out = Path("/tmp/v27_diag/diag_e1_result.json")
    out.write_text(json.dumps(report, indent=1, default=float))
    print(f"\nDIAG DONE -> {out} ({report['runtime_s']}s), c*={sel['c_star']:.3f}",
          flush=True)
    for r in rows:
        print(f"  {r['config_id']:26s} mean={r['mean_bps_d']:+6.2f}bps/d "
              f"se={r['se_bps_d']:5.2f} lcb_adj={r['lcb_adj_bps_d']:+6.2f} "
              f"sign={r['block_sign_consistency']:.2f} trips={r['realized_trips']}",
              flush=True)


if __name__ == "__main__":
    main()
