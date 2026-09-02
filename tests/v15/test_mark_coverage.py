"""MARK-COVERAGE gate + counters (2026-08-07, Fable-approved plan; findings/quant/
2026-08-07-mongo-candle-freeze-silently-truncated-selection-evidence).

Engine side: every leader action skipped for lack of a valid mark increments
n_actions_unpriced and records the coin (sorted-before-cap determinism); invariant
metadata_uncertain == (n_actions_unpriced > 0).
m06b side: walk_forward_confirm REFUSES a claimed OOS fold with seats>0, actions>0, fills==0
(R2 conjunct — a quiet fold never false-positives), gates partial starvation on
unpriced/actions pct knobs (R3), stamps the override (R4), and degrades on legacy artifacts.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v15_m06b_ranking as M                                    # noqa: E402
import v15_m07_engine as E                                      # noqa: E402
from test_m07 import FakeMarketData, _ohlc_path, _action, T0    # noqa: E402
from test_m06b import (_make_inputs, _write_wf_testdir, _wf_manifest)  # noqa: E402


# --------------------------------------------------------------------------- #
# Engine counters
# --------------------------------------------------------------------------- #
def _run_seat(actions, md):
    adf = pd.DataFrame(actions)
    params = E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity")
    return E.step_subaccount(adf, md, 10_000.0, params, end_ts_ms=T0 + 500 * E.MS_MIN)


def test_unpriced_action_counted_and_skipped():
    """A coin with NO ohlc series: its actions are skipped, counted, and coin recorded; the
    priced coin's actions still fill. Invariant: metadata_uncertain == (n_actions_unpriced > 0)."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", [100.0] * 120))   # only BTC has marks
    acts = [
        _action("BTC", T0 + 1 * E.MS_MIN, 1.0, "ENTRY"),
        _action("NOMARK", T0 + 2 * E.MS_MIN, 1.0, "ENTRY"),
        _action("NOMARK", T0 + 3 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=-1.0),
        _action("BTC", T0 + 4 * E.MS_MIN, 0.0, "EXIT", position_after=0.0, signed_size=-1.0),
    ]
    res = _run_seat(acts, md)
    s = res["summary"]
    assert s["n_actions_unpriced"] == 2
    assert s["n_unpriced_coins"] == 1
    assert s["unpriced_coins"] == ["NOMARK"]
    assert s["metadata_uncertain"] is True
    assert s["metadata_uncertain"] == (s["n_actions_unpriced"] > 0)   # the pinned invariant
    filled_coins = {f["coin"] for f in res["fills"]}
    assert filled_coins == {"BTC"}


def test_all_priced_keeps_flag_false():
    md = FakeMarketData(ohlc=_ohlc_path("BTC", [100.0] * 120))
    res = _run_seat([_action("BTC", T0 + 1 * E.MS_MIN, 1.0, "ENTRY")], md)
    s = res["summary"]
    assert s["n_actions_unpriced"] == 0
    assert s["n_unpriced_coins"] == 0
    assert s["unpriced_coins"] == []
    assert s["metadata_uncertain"] is False
    assert s["metadata_uncertain"] == (s["n_actions_unpriced"] > 0)


def test_unpriced_coins_sorted_before_cap_deterministic():
    """R5: sorted BEFORE the 20-cap; n_unpriced_coins reports the TRUE distinct count; two runs
    produce identical lists."""
    md = FakeMarketData(ohlc=_ohlc_path("BTC", [100.0] * 120))
    coins = [f"Z{i:02d}" for i in range(25)]        # 25 distinct mark-less coins, inserted Z24..Z00
    acts = [_action(c, T0 + (i + 1) * E.MS_MIN, 1.0, "ENTRY")
            for i, c in enumerate(reversed(coins))]
    r1 = _run_seat(list(acts), md)["summary"]
    r2 = _run_seat(list(acts), md)["summary"]
    assert r1["n_unpriced_coins"] == 25
    assert len(r1["unpriced_coins"]) == 20
    assert r1["unpriced_coins"] == sorted(coins)[:20]   # sorted first, THEN capped
    assert r1["unpriced_coins"] == r2["unpriced_coins"]
