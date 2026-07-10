"""GOLDEN regression test for v15_m05_eligibility trust-audit fixes (2026-07-10).

Locks the STRICT fail-closed behaviour so a wallet cannot slip through eligibility on missing/NaN copyability
data (the loose default is unchanged = byte-identical to the prior pipeline):
- P0#3: unknown (NaN) accessibility FAILS closed in strict mode,
- P0#5: non-finite hold FAILS closed in strict mode,
- P0#4: NaN/absent notional makes accessible_frac_notional unreliable (NaN) -> strict fails it,
- loose mode: NaN accessibility/hold still pass (unchanged).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m05_eligibility as m5
from test_m05 import _good_eqm, _good_jm  # noqa: E402


def test_strict_nan_accessibility_fails_closed():
    jm = _good_jm(); jm["accessible_frac_notional"] = float("nan")   # unknown accessibility
    ok_loose, _ = m5.apply_floors(_good_eqm(), jm, strict=False)
    ok_strict, reasons = m5.apply_floors(_good_eqm(), jm, strict=True)
    assert ok_loose is True                                          # loose: unchanged (does not fire)
    assert ok_strict is False and any("accessible_frac_unknown" in r for r in reasons)


def test_strict_nonfinite_hold_fails_closed():
    jm = _good_jm(); jm["median_hold_s"] = float("nan")
    ok_loose, _ = m5.apply_floors(_good_eqm(), jm, strict=False)
    ok_strict, reasons = m5.apply_floors(_good_eqm(), jm, strict=True)
    assert ok_loose is True
    assert ok_strict is False and any("median_hold_nonfinite" in r for r in reasons)


def test_loose_default_unchanged():
    # a fully-good wallet is eligible in both modes; the strict gates only bite on NaN/unknown.
    assert m5.apply_floors(_good_eqm(), _good_jm(), strict=False)[0] is True
    assert m5.apply_floors(_good_eqm(), _good_jm(), strict=True)[0] is True


def test_strict_present_inf_fails_closed():
    # a PRESENT non-finite (inf) blocking input fails closed in strict; loose is unchanged.
    eqm = _good_eqm(); eqm["max_dd"] = float("inf")
    ok_loose, _ = m5.apply_floors(eqm, _good_jm(), strict=False)   # inf > cap already fails loose here, fine
    ok_strict, reasons = m5.apply_floors(_good_eqm() | {"median_leverage": float("inf")}, _good_jm(), strict=True)
    assert ok_strict is False and any("nonfinite_median_leverage" in r for r in reasons)


def test_strict_accessible_inf_fails_closed():
    # codex r2: a PRESENT inf accessibility must fail closed in strict (it would otherwise pass `< MIN`).
    ok, reasons = m5.apply_floors(_good_eqm(), _good_jm() | {"accessible_frac_notional": float("inf")}, strict=True)
    assert ok is False and any("nonfinite_accessible_frac_notional" in r for r in reasons)


def test_strict_accessible_out_of_range_fails_closed():
    ok, reasons = m5.apply_floors(_good_eqm(), _good_jm() | {"accessible_frac_notional": 1.5}, strict=True)
    assert ok is False and any("accessible_frac_out_of_range" in r for r in reasons)


def test_strict_n_journeys_nonfinite_fails_closed():
    # codex r2: NaN n_journeys would skip the `< MIN_JOURNEYS` gate -> strict fails it.
    ok, reasons = m5.apply_floors(_good_eqm(), _good_jm() | {"n_journeys": float("nan")}, strict=True)
    assert ok is False and any("nonfinite_n_journeys" in r for r in reasons)


def test_run_strict_rejects_lookahead_m04_by_fold():
    # codex r2 P0: a per-fold-SHAPED dict whose as_of_ms != test_start (global/look-ahead) is rejected in strict.
    import pytest
    ts0 = pd.Timestamp("2026-01-01"); tst = pd.Timestamp("2026-02-12")
    folds = pd.DataFrame({"fold_id": [1], "train_start": [ts0], "test_start": [tst]})
    empty_j = pd.DataFrame(columns=["wallet", "entity_id", "coin", "entry_ts", "exit_ts",
                                    "lifecycle_valid", "stream_replay_valid", "net_realized_pnl", "duration_h"])
    wrong_asof = m5._ms(tst) - 86_400_000   # one day early = look-ahead-prone
    m04bf = {1: pd.DataFrame({"entity_id": [1], "primary_wallet": ["0xa"], "copyable": [True],
                             "as_of_ms": [wrong_asof]})}
    with pytest.raises(ValueError, match="as_of_ms"):
        m5.run(folds, empty_j, pd.DataFrame(), None, {1: {"BTC"}}, m04_by_fold=m04bf,
               equity_required=False, strict=True)


def test_run_strict_requires_fold_pure_m04(_folds_and_min_inputs=None):
    import pandas as pd
    folds = pd.DataFrame({"fold_id": [1], "train_start": [pd.Timestamp("2026-01-01")],
                          "test_start": [pd.Timestamp("2026-02-12")]})
    empty_j = pd.DataFrame(columns=["wallet", "entity_id", "coin", "entry_ts", "exit_ts",
                                    "lifecycle_valid", "stream_replay_valid", "net_realized_pnl", "duration_h"])
    m04 = pd.DataFrame({"entity_id": [1], "primary_wallet": ["0xa"], "copyable": [True]})
    import pytest
    with pytest.raises(ValueError, match="fold-pure"):
        m5.run(folds, empty_j, pd.DataFrame(), m04, {1: {"BTC"}}, equity_required=False, strict=True)


def test_p0_4_nan_notional_makes_accessibility_unreliable():
    # 4 tiny BTC journeys + 1 huge EXOTIC journey with NaN notional -> the exotic must NOT vanish from the
    # denominator and inflate accessible_frac toward 1.0; it becomes NaN (unreliable) -> strict fails it.
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    lo, hi = ms("2026-01-01"), ms("2026-02-01")
    jr = pd.DataFrame({
        "coin": ["BTC", "BTC", "BTC", "BTC", "XYZEXOTIC"],
        "entry_ts": [lo + i for i in range(5)],
        "exit_ts": [lo + 3600_000 + i for i in range(5)],
        "max_position_notional": [10.0, 10.0, 10.0, 10.0, float("nan")],  # huge exotic notional is NaN
        "net_realized_pnl": [5.0, 5.0, 5.0, 5.0, 5.0],
        "duration_h": [1.0, 1.0, 1.0, 1.0, 1.0],
        "lifecycle_valid": True, "stream_replay_valid": True,
    })
    jm = m5.journey_metrics(jr, lo, hi, accessible={"BTC"})
    assert not (jm["accessible_frac_notional"] == jm["accessible_frac_notional"])  # NaN, not a fake 1.0
    # and strict fails it closed
    eqm = _good_eqm()
    ok, reasons = m5.apply_floors(eqm, {**_good_jm(), "accessible_frac_notional": jm["accessible_frac_notional"]},
                                  strict=True)
    assert ok is False and any("accessible_frac_unknown" in r for r in reasons)
