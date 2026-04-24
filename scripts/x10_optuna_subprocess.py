#!/usr/bin/env python3
"""
X10 Optuna optimization — subprocess-isolated trials.

Each trial runs as a separate OS process to avoid OOM.
Uses pickle to pass params in and results out.
"""
import gc
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import optuna

optuna.logging.set_verbosity(optuna.logging.WARNING)

PYTHON = "/Users/hermes/miniforge3/envs/quants-lab/bin/python"
WORKER = str(Path(__file__).parent / "x10_optuna_worker.py")

TRAIN_START = int(datetime(2026, 1, 20, tzinfo=timezone.utc).timestamp())
TRAIN_END = int(datetime(2026, 3, 20, tzinfo=timezone.utc).timestamp())
TEST_START = TRAIN_END
TEST_END = int(datetime(2026, 4, 21, tzinfo=timezone.utc).timestamp())

TRAIN_PAIRS = ["BTC-USDT", "SOL-USDT", "TAO-USDT"]
ALL_PAIRS = [
    "ADA-USDT", "APT-USDT", "AVAX-USDT", "BCH-USDT", "BNB-USDT", "BTC-USDT",
    "DOGE-USDT", "DOT-USDT", "ETH-USDT", "LINK-USDT", "LTC-USDT", "NEAR-USDT",
    "SEI-USDT", "SOL-USDT", "SUI-USDT", "TAO-USDT", "WLD-USDT", "XRP-USDT",
]


def run_trial_subprocess(pairs, start_ts, end_ts, overrides) -> dict:
    """Run one trial in a subprocess. Returns {pnls: [...], close_types: {...}}."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({
            "pairs": pairs,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "overrides": overrides,
        }, f)
        input_path = f.name

    output_path = input_path + ".out"

    try:
        env = os.environ.copy()
        env.setdefault("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        env.setdefault("MONGO_DATABASE", "quants_lab")

        result = subprocess.run(
            [PYTHON, WORKER, input_path, output_path],
            env=env,
            capture_output=True,
            timeout=300,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode()[-200:]
            return {"pnls": [], "close_types": {}, "error": stderr}

        with open(output_path) as f:
            return json.load(f)
    except subprocess.TimeoutExpired:
        return {"pnls": [], "close_types": {}, "error": "timeout"}
    except Exception as e:
        return {"pnls": [], "close_types": {}, "error": str(e)}
    finally:
        for p in [input_path, output_path]:
            try:
                os.unlink(p)
            except OSError:
                pass


def composite_score(pnls_list, close_types=None):
    pnls = np.array(pnls_list)
    if len(pnls) < 5:
        return -10.0

    mean = np.mean(pnls)
    std = np.std(pnls)
    sharpe = mean / std * np.sqrt(365) if std > 0 else 0

    pos = pnls[pnls > 0].sum()
    neg = -pnls[pnls < 0].sum()
    pf = pos / neg if neg > 0 else 10.0
    wr = (pnls > 0).mean()

    # Penalize TIME_LIMIT dominance
    tl_rate = 0.0
    if close_types:
        total = sum(close_types.values())
        tl_rate = close_types.get("TIME_LIMIT", 0) / total if total > 0 else 0

    score = sharpe
    if pf > 1.5:
        score += 0.5
    if pf > 2.5:
        score += 0.5
    if wr > 0.55:
        score += 0.3
    if tl_rate > 0.5:
        score -= 0.5  # penalize high TIME_LIMIT rate
    if tl_rate < 0.3:
        score += 0.3  # reward clean exits
    if len(pnls) < 15:
        score -= 1.0

    return score


def main():
    print("=" * 70)
    print("X10 OPTUNA (subprocess-isolated)")
    print("=" * 70)
    print(f"Train: 2026-01-20 to 2026-03-20 (60d), Pairs: {TRAIN_PAIRS}")
    print(f"Test:  2026-03-20 to 2026-04-21 (30d), Pairs: all 18")
    print()

    study = optuna.create_study(direction="maximize", study_name="x10_sub")
    n_trials = 40

    for i in range(n_trials):
        trial = study.ask()

        overrides = {
            "z_total_threshold": trial.suggest_float("z_total", 1.0, 3.0, step=0.25),
            "z_imb_threshold": trial.suggest_float("z_imb", 0.25, 1.5, step=0.25),
            "z_window": trial.suggest_int("z_window", 36, 120, step=12),
            "sl_atr_mult": trial.suggest_float("sl_atr", 1.5, 4.0, step=0.5),
            "tp_atr_mult": trial.suggest_float("tp_atr", 1.0, 3.5, step=0.5),
            "trailing_activation_atr": trial.suggest_float("trail_act", 0.75, 2.25, step=0.25),
            "trailing_delta_atr": trial.suggest_float("trail_delta", 0.3, 1.2, step=0.1),
            "time_limit_seconds": trial.suggest_int("time_limit_h", 4, 24, step=2) * 3600,
        }

        result = run_trial_subprocess(TRAIN_PAIRS, TRAIN_START, TRAIN_END, overrides)
        score = composite_score(result["pnls"], result.get("close_types", {}))
        study.tell(trial, score)

        n_trades = len(result["pnls"])
        ct = result.get("close_types", {})
        tp_n = ct.get("TAKE_PROFIT", 0)
        sl_n = ct.get("STOP_LOSS", 0)
        tl_n = ct.get("TIME_LIMIT", 0)

        if (i + 1) % 5 == 0 or i == 0:
            best = study.best_trial
            print(f"  Trial {i+1}/{n_trials}: n={n_trades} TP={tp_n} SL={sl_n} TL={tl_n} "
                  f"score={score:.2f} | best={best.value:.2f}")
            sys.stdout.flush()

    # ── BEST PARAMS ──────────────────────────────────────────
    best = study.best_trial
    bp = best.params
    print(f"\n{'='*70}")
    print(f"BEST TRAIN PARAMS (score={best.value:.2f}):")
    for k, v in sorted(bp.items()):
        print(f"  {k}: {v}")

    best_overrides = {
        "z_total_threshold": bp["z_total"],
        "z_imb_threshold": bp["z_imb"],
        "z_window": bp["z_window"],
        "sl_atr_mult": bp["sl_atr"],
        "tp_atr_mult": bp["tp_atr"],
        "trailing_activation_atr": bp["trail_act"],
        "trailing_delta_atr": bp["trail_delta"],
        "time_limit_seconds": bp["time_limit_h"] * 3600,
    }

    # ── TRAIN RESULT ─────────────────────────────────────────
    print(f"\n--- Train (best params, {TRAIN_PAIRS}) ---")
    train_r = run_trial_subprocess(TRAIN_PAIRS, TRAIN_START, TRAIN_END, best_overrides)
    train_pnls = np.array(train_r["pnls"])
    if len(train_pnls) > 0:
        pos = train_pnls[train_pnls > 0].sum()
        neg = -train_pnls[train_pnls < 0].sum()
        pf = pos / neg if neg > 0 else float('inf')
        print(f"  n={len(train_pnls)}, mean=${np.mean(train_pnls):.4f}, WR={(train_pnls>0).mean():.1%}, "
              f"PF={pf:.2f}, close_types={train_r.get('close_types',{})}")

    # ── TEST (ONE SHOT) ──────────────────────────────────────
    print(f"\n{'='*70}")
    print("TEST (one-shot, all 18 pairs, 30d)")
    print("=" * 70)
    test_r = run_trial_subprocess(ALL_PAIRS, TEST_START, TEST_END, best_overrides)
    test_pnls = np.array(test_r["pnls"])
    if len(test_pnls) > 0:
        pos = test_pnls[test_pnls > 0].sum()
        neg = -test_pnls[test_pnls < 0].sum()
        pf = pos / neg if neg > 0 else float('inf')
        wr = (test_pnls > 0).mean()
        print(f"  n={len(test_pnls)}, mean=${np.mean(test_pnls):.4f}, median=${np.median(test_pnls):.4f}")
        print(f"  WR={wr:.1%}, PF={pf:.2f}, total=${test_pnls.sum():.2f}")
        print(f"  close_types={test_r.get('close_types', {})}")
    else:
        print("  ZERO TEST TRADES")

    # ── DEFAULT COMPARISON ───────────────────────────────────
    print(f"\n--- Default params (test window) ---")
    default_overrides = {
        "z_total_threshold": 2.0, "z_imb_threshold": 0.5, "z_window": 72,
        "sl_atr_mult": 2.5, "tp_atr_mult": 2.0, "trailing_activation_atr": 1.5,
        "trailing_delta_atr": 0.7, "time_limit_seconds": 43200,
    }
    def_r = run_trial_subprocess(ALL_PAIRS, TEST_START, TEST_END, default_overrides)
    def_pnls = np.array(def_r["pnls"])
    if len(def_pnls) > 0:
        pos = def_pnls[def_pnls > 0].sum()
        neg = -def_pnls[def_pnls < 0].sum()
        pf = pos / neg if neg > 0 else float('inf')
        print(f"  n={len(def_pnls)}, mean=${np.mean(def_pnls):.4f}, WR={(def_pnls>0).mean():.1%}, "
              f"PF={pf:.2f}")
        print(f"  close_types={def_r.get('close_types', {})}")

    # ── TOP-10 + BOUNDARY ────────────────────────────────────
    print(f"\n--- Top-10 trials (clustering check) ---")
    top10 = sorted(study.trials, key=lambda t: t.value if t.value is not None else -999, reverse=True)[:10]
    for j, t in enumerate(top10):
        if t.value is not None:
            print(f"  #{j+1} score={t.value:.2f} z_t={t.params['z_total']:.2f} "
                  f"z_i={t.params['z_imb']:.2f} sl={t.params['sl_atr']:.1f} "
                  f"tp={t.params['tp_atr']:.1f} tl={t.params['time_limit_h']}h")

    print(f"\n--- Boundary check ---")
    bounds = {"z_total": (1.0, 3.0), "z_imb": (0.25, 1.5), "sl_atr": (1.5, 4.0),
              "tp_atr": (1.0, 3.5), "trail_act": (0.75, 2.25), "trail_delta": (0.3, 1.2),
              "time_limit_h": (4, 24)}
    for param, (lo, hi) in bounds.items():
        val = bp[param]
        at_bound = val <= lo or val >= hi
        print(f"  {param}: {val} {'⚠ BOUNDARY' if at_bound else '✓'}")

    print(f"\n{'='*70}")
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
