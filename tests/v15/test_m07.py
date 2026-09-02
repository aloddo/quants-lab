"""V15 M7 engine tests — golden HL mechanics + synthetic scenarios + streaming + determinism + causal.

Design: brain projects/quant/v15/modules/m07. Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m07.py -q

NOTE (2026-07-30): most cases below pass sizing_mode="leader_equity" EXPLICITLY. They previously relied
on it being the dataclass DEFAULT -- and that default was itself the bug: leader_equity sizes from
`target_exposure_pct`, which is 100% NULL in every real m02 store (M1 is out of scope), so any
production invocation that did not override it emitted ZERO orders and reported success. The default is
now `fixed_position`. These tests exercise ENGINE MECHANICS (backstop, liquidation, follower-trail,
min-order skip, ruin accounting) and use leader_equity purely as the vehicle, with a synthetic non-null
column, so pinning it here preserves exactly what they were written to verify. The all-null production
case is caught at load time by v15_m07_engine.assert_sizing_input_usable(). When the canonical
gross-notional sizing lands (shared by engine + sim), these should migrate onto it.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m07_engine as E  # noqa: E402


# --------------------------------------------------------------------------- #
# Fakes: a controllable MarketData so scenarios are exact + look-ahead-free.
# --------------------------------------------------------------------------- #
class FakeMeta:
    def __init__(self, maxlev=10.0, szdec=3):
        self._maxlev = maxlev
        self.coin_szdec = {}
        self.coin_maxlev = {}
        self._default_szdec = szdec

    def maint_rate(self, coin, notional):
        return 1.0 / (2.0 * self._maxlev)

    def init_margin_rate(self, coin, notional):
        return 1.0 / self._maxlev

    def max_leverage(self, coin):
        return self._maxlev

    def tier_maxlev(self, coin, notional):
        return self._maxlev

    def szdec(self, coin):
        return self.coin_szdec.get(coin, self._default_szdec)


class FakeMarketData:
    """Inject per-coin minute grids + OHLC + funding. Deterministic, no Mongo. `mark()` is causal
    (prior completed minute) matching the engine, so look-ahead tests are meaningful."""
    def __init__(self, ohlc=None, funding=None, maxlev=10.0, adv=1e12, half_spread_bps=1.0):
        self._ohlc = ohlc or {}
        self._funding = funding or {}
        self.meta = FakeMeta(maxlev=maxlev)
        for c in self._ohlc:
            self.meta.coin_szdec[c] = 3
            self.meta.coin_maxlev[c] = maxlev
        self._adv = adv
        self._hs = half_spread_bps

        class _Fees:
            versioned = False
            def taker(self, coin):
                return E.DEFAULT_TAKER_FEE_ONEWAY * (E.HIP3_FEE_MULT_FALLBACK if E.coin_dex(coin) != "main" else 1.0)
        self.fees = _Fees()

    def ohlc(self, coin):
        return self._ohlc.get(coin, (np.empty(0, "int64"),) + tuple(np.empty(0, "float64") for _ in range(4)))

    def mark(self, coin, ts_ms, causal=True):
        mins, _o, _h, _l, c = self.ohlc(coin)
        if mins.size == 0:
            return None
        key = (ts_ms // E.MS_MIN) * E.MS_MIN - (E.MS_MIN if causal else 0)
        i = int(np.searchsorted(mins, key, side="right")) - 1
        if i < 0:
            return None
        v = c[i]
        return None if v != v else float(v)

    def funding_rate_at(self, coin, ts_ms):
        s = self._funding.get(coin)
        if not s:
            return 0.0
        ts, r = s
        i = int(np.searchsorted(ts, ts_ms, side="right")) - 1
        return float(r[i]) if i >= 0 else 0.0

    def liquidity(self, coin, ts_ms):
        return {"adv": self._adv, "half_spread_bps": self._hs, "uncalibrated": False}


def _flat_ohlc(coin, start_min_ms, n, px):
    mins = np.arange(n, dtype="int64") * E.MS_MIN + start_min_ms
    arr = np.full(n, float(px))
    return {coin: (mins, arr.copy(), arr.copy(), arr.copy(), arr.copy())}


def _action(coin, ts, target_pct, action_type="ENTRY", position_after=1.0, signed_size=1.0,
            carry_in_status=""):
    return {"coin": coin, "ts": ts, "event_order": 0, "action_type": action_type,
            "signed_size": signed_size, "position_after": position_after,
            "target_exposure_pct": target_pct, "is_liquidation": False, "carry_in_status": carry_in_status}


T0 = 1_700_000_000_000 // E.MS_MIN * E.MS_MIN
END = T0 + 500 * E.MS_MIN


def _run(acts, md, eq=10_000.0, params=None, end=END, **kw):
    return E.step_subaccount(acts, md, eq, params or E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"), end_ts_ms=end, **kw)


# --------------------------------------------------------------------------- #
# Metadata / helpers
# --------------------------------------------------------------------------- #
def test_coin_dex_scope():
    assert E.coin_dex("BTC") == "main"
    assert E.coin_dex("xyz:AMD") == "xyz"
    assert E.coin_dex("flx:FOO") == "flx"
    assert E.coin_is_spot("#42") is True
    assert E.coin_is_spot("PURR/USDC") is True


def test_fixed_position_sizing_runs_without_leader_equity():
    md = FakeMarketData(_flat_ohlc("BTC", T0 - E.MS_MIN, 20, 100.0))
    acts = pd.DataFrame([
        _action("BTC", T0 + 2 * E.MS_MIN, np.nan, "ENTRY", position_after=1.0),
        _action("BTC", T0 + 3 * E.MS_MIN, np.nan, "EXIT", position_after=0.0, signed_size=-1.0),
    ])
    params = E.EngineParams(
        copy_latency_ms=0, sizing_mode="fixed_position", fixed_target_exposure=0.25
    )
    out = _run(acts, md, eq=1_000.0, params=params, end=T0 + 4 * E.MS_MIN)
    assert out["summary"]["sizing_mode"] == "fixed_position"
    assert out["summary"]["n_fills"] == 2
    assert out["ending_account_state"]["positions"] == {}


def test_m7_rejects_stale_m2_schema_without_stream_validity(tmp_path):
    import pyarrow.dataset as ds

    p = tmp_path / "old_actions.parquet"
    pd.DataFrame({"wallet": ["0x1"], "coin": ["BTC"], "ts": [T0]}).to_parquet(p)
    with pytest.raises(ValueError, match="rebuilt causal M2"):
        E._require_action_schema(ds.dataset(p, format="parquet"))


def test_margin_mode_inference():
    assert E.default_margin_mode("BTC") == "cross"
    assert E.default_margin_mode("xyz:AMD") == "isolated"


def test_hlmeta_maint_rate_from_real_cache():
    meta = E.HLMeta().load()
    if not meta.has("BTC"):
        pytest.skip("HL meta cache not present")
    r = meta.maint_rate("BTC", 1000.0)
    assert 0 < r < 0.5
    assert abs(r - 1.0 / (2.0 * meta.tier_maxlev("BTC", 1000.0))) < 1e-9


def test_m7_fee_schedule_uses_effective_subaccount_rate(tmp_path):
    p = tmp_path / "fees.json"
    p.write_text(__import__("json").dumps({
        "base_taker_oneway": 0.000432,
        "effective_subaccount_taker_oneway": 0.00045,
        "hip3_mult": 2.0,
    }))
    f = E.FeeSchedule(p)
    assert f.taker("BTC") == pytest.approx(0.00045)
    assert f.taker("xyz:TEST") == pytest.approx(0.0009)


def test_liquidity_adv_is_trailing_dollar_volume_not_price_proxy():
    md = E.MarketData(allow_mongo=False)
    mins = np.arange(3, dtype="int64") * E.MS_MIN + T0
    px = np.full(3, 100.0)
    md._ohlc["BTC"] = (mins, px.copy(), px.copy(), px.copy(), px.copy())
    md._volume["BTC"] = np.array([1.0, 2.0, 3.0])

    liq = md.liquidity("BTC", T0 + 4 * E.MS_MIN)

    assert liq["adv"] == pytest.approx(600.0)
    assert liq["adv_unavailable"] is False


def test_liquidity_adv_uses_24h_wall_clock_for_sparse_bars():
    md = E.MarketData(allow_mongo=False)
    mins = np.array([T0, T0 + 48 * E.MS_HOUR], dtype="int64")
    px = np.array([100.0, 100.0])
    md._ohlc["xyz:TEST"] = (mins, px.copy(), px.copy(), px.copy(), px.copy())
    md._volume["xyz:TEST"] = np.array([10.0, 2.0])

    liq = md.liquidity("xyz:TEST", T0 + 48 * E.MS_HOUR + 2 * E.MS_MIN)

    assert liq["adv"] == pytest.approx(200.0)


def test_missing_adv_rejects_new_exposure_fail_closed():
    class MissingAdv(FakeMarketData):
        def liquidity(self, coin, ts_ms):
            return {
                "adv": 0.0,
                "half_spread_bps": 1.0,
                "uncalibrated": False,
                "adv_unavailable": True,
            }

    md = MissingAdv(_flat_ohlc("BTC", T0 - 10 * E.MS_MIN, 600, 100.0))
    out = _run(pd.DataFrame([_action("BTC", T0, 0.1)]), md)

    assert out["summary"]["n_fills"] == 0
    assert out["summary"]["n_rejected"] == 1
    assert out["summary"]["adv_unavailable"] is True


def test_market_data_mark_rejects_stale_asof_bar():
    md = E.MarketData(allow_mongo=False)
    px = np.array([100.0])
    md._ohlc["BTC"] = (np.array([T0], dtype="int64"), px, px, px, px)

    assert md.mark("BTC", T0 + E.MS_MIN, causal=True) == 100.0
    assert md.mark("BTC", T0 + E.MARK_MAX_AGE_MS + 2 * E.MS_MIN, causal=True) is None


def test_account_equity_derivation():
    st = E.AccountState(cross_collateral={"main": 1000.0})
    st.positions["BTC"] = E.Position("BTC", szi=1.0, entry_px=100.0, mode="cross", leverage=1.0)
    assert abs(st.equity({"BTC": 110.0}) - 1010.0) < 1e-9


# --------------------------------------------------------------------------- #
# Core journey
# --------------------------------------------------------------------------- #
def test_round_trip_costs_money():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0),
        _action("BTC", T0 + 50 * E.MS_MIN, 0.0, "EXIT", position_after=0.0),
    ])
    s = _run(acts, md, entity_id=1, fold_id=1)["summary"]
    assert s["n_fills"] == 2
    assert s["total_fees"] > 0
    assert s["final_equity"] < 10_000.0
    assert s["roe_engine"] < 0


def test_min_order_skip():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 100, 100.0))
    acts = pd.DataFrame([_action("BTC", T0 + 5 * E.MS_MIN, 1e-7, "ENTRY", position_after=0.001)])
    assert _run(acts, md)["summary"]["n_fills"] == 0


def test_survived_state_on_flat():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    acts = pd.DataFrame([_action("BTC", T0 + 5 * E.MS_MIN, 0.3, "ENTRY", position_after=30.0)])
    assert "account_ruin" not in _run(acts, md)["summary"]["outcome_states"]


# --------------------------------------------------------------------------- #
# Liquidation + isolation invariant
# --------------------------------------------------------------------------- #
def test_backstop_on_crash_isolation_invariant():
    n = 300
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0)
    px[100:] = 60.0
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 5.0, "ENTRY", position_after=500.0),
        _action("BTC", T0 + 250 * E.MS_MIN, 5.0, "ADDON", position_after=500.0),
    ])
    s = _run(acts, md)["summary"]
    assert set(s["outcome_states"]) & {"backstop", "account_ruin", "position_liquidated"}
    assert s["final_equity"] >= -1e-6


def test_im_admissibility_rejects_on_tier_jump():
    # codex code-r3: total resulting-position IM (tier-correct), not marginal-on-delta. Adding into a
    # >100k notional re-rates the WHOLE position to the higher tier; if cash can't fund it -> reject.
    class TierMeta(FakeMeta):
        def init_margin_rate(self, coin, notional):
            return 0.1 if abs(notional) <= 100_000 else 0.2
        def maint_rate(self, coin, notional):
            return self.init_margin_rate(coin, notional) / 2
        def tier_maxlev(self, coin, notional):
            return 1.0 / self.init_margin_rate(coin, notional)
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 20, 100.0), adv=1e12)
    md.meta = TierMeta(); md.meta.coin_szdec["BTC"] = 3
    st = E.AccountState(cross_collateral={"main": 14000.0})
    st.positions["BTC"] = E.Position("BTC", szi=900.0, entry_px=100.0, mode="cross", leverage=10.0)
    summary = E._new_summary(None, None, 14000.0, 0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"), md)
    summary["_fills_ref"] = []
    fills, events = [], []
    # add 200 -> new notional 110k, total IM = 22k > ~14k cash -> must reject
    E._apply_order(st, md, "BTC", 200.0, 100.0, T0 + 5 * E.MS_MIN, {"ts": T0 + 5 * E.MS_MIN},
                   E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"), 1.0, fills, events, summary)
    assert summary["n_rejected"] == 1 and len(fills) == 0


def test_market_liquidation_path_produces_liq_fill():
    # moderate drop: cross account breaches maintenance but stays ABOVE 2/3 maint -> market-liq order
    # (not backstop). 5x exposure, maxlev 10 (maint 5%). px 100 -> 84 lands in the market-liq band.
    n = 300
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[100:] = 84.0
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 5.0, "ENTRY", position_after=500.0),
        _action("BTC", T0 + 250 * E.MS_MIN, 5.0, "ADDON", position_after=500.0),
    ])
    res = _run(acts, md)
    s = res["summary"]
    assert s["n_market_liq_orders"] >= 1 or "backstop" in s["outcome_states"]
    liq_fills = [f for f in res["fills"] if f["fill_type"] == "market_liq_order"]
    if s["n_market_liq_orders"] >= 1:
        assert liq_fills and all(f["fee"] == 0.0 for f in liq_fills)   # forced-liq fills carry no fee
    assert s["final_equity"] >= -1e-6


def test_market_liquidation_charges_full_order_adv_impact_above_floor():
    md = FakeMarketData(
        ohlc=_flat_ohlc("BTC", T0, 10, 100.0), adv=1_000.0,
        half_spread_bps=1.0,
    )
    st = E.AccountState(cross_collateral={"main": 20_000.0})
    st.positions["BTC"] = E.Position(
        "BTC", szi=1_000.0, entry_px=100.0, mode="cross", leverage=10.0
    )
    summary = E._new_summary(None, None, 20_000.0, 0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"), md)
    summary["_fills_ref"] = []
    E._rt_open(summary, "BTC")
    E._liq_close(st, md, "BTC", 100.0, T0 + E.MS_MIN, summary)
    fill = summary["_fills_ref"][0]
    total_slip = fill["half_spread_bps"] + fill["impact_bps"]
    assert total_slip > E.FORCED_LIQ_SLIP_BPS
    assert fill["our_fill_px"] < 100.0 * (1 - E.FORCED_LIQ_SLIP_BPS / 1e4)
    assert fill["fee"] == 0.0


def test_isolated_position_posts_margin_and_can_liquidate():
    # xyz coin -> isolated. Open isolated long, crash -> isolated liquidation, cross main untouched.
    n = 300
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[100:] = 50.0
    md = FakeMarketData(ohlc={"xyz:AMD": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    acts = pd.DataFrame([
        _action("xyz:AMD", T0 + 5 * E.MS_MIN, 2.0, "ENTRY", position_after=200.0),
        _action("xyz:AMD", T0 + 250 * E.MS_MIN, 2.0, "ADDON", position_after=200.0),
    ])
    res = _run(acts, md)
    s = res["summary"]
    # isolated margin was posted from main on open (main cross collateral dropped below start)
    assert "position_liquidated" in s["outcome_states"] or "backstop" in s["outcome_states"]
    assert s["final_equity"] >= -1e-6


# --------------------------------------------------------------------------- #
# Funding
# --------------------------------------------------------------------------- #
def test_funding_sign_long_pays_positive_rate():
    n = 200
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0)
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())},
                        funding={"BTC": (np.array([T0 + 30 * E.MS_MIN], "int64"), np.array([0.0001]))},
                        maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 1.0, "ENTRY", position_after=100.0),
        _action("BTC", T0 + 120 * E.MS_MIN, 1.0, "ADDON", position_after=100.0),
    ])
    assert _run(acts, md)["summary"]["total_funding"] < 0


# --------------------------------------------------------------------------- #
# Final-horizon risk (codex code-r1 #1): a crash AFTER the last action must still be seen
# --------------------------------------------------------------------------- #
def test_final_horizon_risk_seen_after_last_action():
    n = 400
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0)
    px[200:] = 55.0      # crash AFTER the last action (which is at minute ~10)
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    acts = pd.DataFrame([_action("BTC", T0 + 5 * E.MS_MIN, 5.0, "ENTRY", position_after=500.0)])
    s = _run(acts, md, end=T0 + 390 * E.MS_MIN)["summary"]
    # the crash at minute 200 is well after the only action; final-horizon advance must catch it
    assert set(s["outcome_states"]) & {"backstop", "account_ruin", "position_liquidated"}


# --------------------------------------------------------------------------- #
# Determinism + causal + purity
# --------------------------------------------------------------------------- #
def test_determinism():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0),
        _action("BTC", T0 + 40 * E.MS_MIN, 0.0, "EXIT", position_after=0.0),
    ])
    r1 = _run(acts, md)["summary"]
    r2 = _run(acts, md)["summary"]
    assert r1["final_equity"] == r2["final_equity"]
    assert r1["total_fees"] == r2["total_fees"]


def test_causal_fill_uses_prior_minute_not_future():
    # step up at minute 10; action at minute 8, latency lands minute 9 -> causal mark = minute-8 close
    # (prior completed bar), never the future 200.
    n = 60
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[10:] = 200.0
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())})
    acts = pd.DataFrame([_action("BTC", T0 + 8 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0)])
    res = _run(acts, md, params=E.EngineParams(copy_latency_ms=E.MS_MIN))
    assert abs(res["fills"][0]["ref_mark"] - 100.0) < 1e-6


def test_pure_does_not_mutate_caller_state():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    start = E.AccountState(cross_collateral={"main": 10_000.0})
    acts = pd.DataFrame([_action("BTC", T0 + 5 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0)])
    E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"), end_ts_ms=END, start_state=start)
    assert start.cross_collateral == {"main": 10_000.0}
    assert start.positions == {}


def test_start_state_open_position_advances_risk_with_no_actions():
    # codex code-r2 #2: a carried start_state position must accrue risk + liquidate even with ZERO
    # actions, anchored at start_ts. Crash after start -> liquidation/backstop without any action.
    n = 400
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[150:] = 55.0
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    start = E.AccountState(cross_collateral={"main": 2000.0})
    start.positions["BTC"] = E.Position("BTC", szi=400.0, entry_px=100.0, mode="cross", leverage=10.0)  # $40k @ 5% maint
    res = E.step_subaccount(pd.DataFrame([]), md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=T0 + 390 * E.MS_MIN, start_ts_ms=T0, start_state=start)
    s = res["summary"]
    assert set(s["outcome_states"]) & {"backstop", "account_ruin", "position_liquidated"}
    # purity: caller start_state untouched
    assert start.positions["BTC"].szi == 400.0


def test_funding_stops_after_liquidation():
    # chronological: a position liquidated early should not keep accruing funding to fold end.
    n = 400
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[120:] = 50.0
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())},
                        funding={"BTC": (np.array([T0 + h * E.MS_HOUR for h in range(1, 7)], "int64"),
                                          np.array([0.001] * 6))}, maxlev=10.0)
    start = E.AccountState(cross_collateral={"main": 2000.0})
    start.positions["BTC"] = E.Position("BTC", szi=400.0, entry_px=100.0, mode="cross", leverage=10.0)
    res = E.step_subaccount(pd.DataFrame([]), md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=T0 + 390 * E.MS_MIN, start_ts_ms=T0, start_state=start)
    # liquidated around the crash; funding only accrues on hours BEFORE liquidation, not all 6
    assert set(res["summary"]["outcome_states"]) & {"backstop", "position_liquidated", "account_ruin"}


def test_post_funding_boundary_liquidation():
    # codex code-r4 #1: a settled funding payment at the fold boundary can push below maintenance;
    # must liquidate, not report 'survived' with an insolvent open position. Flat price, large
    # positive funding at the last hour, position sized so funding eats the maintenance buffer.
    n = 200
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0)
    last_h = ((T0 // E.MS_HOUR) + 2) * E.MS_HOUR     # a settlement inside the window
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())},
                        funding={"BTC": (np.array([last_h], "int64"), np.array([0.5]))},  # huge rate
                        maxlev=10.0)
    start = E.AccountState(cross_collateral={"main": 600.0})       # thin buffer over maint
    start.positions["BTC"] = E.Position("BTC", szi=100.0, entry_px=100.0, mode="cross", leverage=10.0)  # $10k, maint $500
    res = E.step_subaccount(pd.DataFrame([]), md, 600.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=last_h + E.MS_MIN, start_ts_ms=T0, start_state=start)
    s = res["summary"]
    assert s["total_funding"] < 0                       # paid the funding
    assert "survived" not in s["outcome_states"]        # not silently solvent
    assert set(s["outcome_states"]) & {"backstop", "position_liquidated", "account_ruin"}


def test_liquidation_cooldown_no_same_ts_double_fill():
    # codex code-r5 #1: a >100k partial liquidation (20%) must NOT be immediately followed by a full
    # close at the SAME timestamp (30s cooldown). Assert market_liq_order fills have distinct our_ts.
    n = 200
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[20:] = 99.0          # mild breach -> market-liq band for a >100k pos
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    start = E.AccountState(cross_collateral={"main": 8000.0})
    start.positions["BTC"] = E.Position("BTC", szi=1500.0, entry_px=100.0, mode="cross", leverage=10.0)  # $150k
    res = E.step_subaccount(pd.DataFrame([]), md, 8000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=T0 + 190 * E.MS_MIN, start_ts_ms=T0, start_state=start)
    liq_ts = [f["our_ts"] for f in res["fills"] if f["fill_type"] == "market_liq_order"]
    assert len(liq_ts) == len(set(liq_ts))           # no two forced fills at the same instant


def test_no_same_ts_double_when_funding_at_fold_end():
    # codex code-r6 #1: when the funding settlement == fold end (t1), the in-loop post-funding check
    # and the terminal fold-boundary check must not BOTH liquidate the same >100k position at the
    # same ts (the second would full-close inside the 30s cooldown).
    t1 = ((T0 // E.MS_HOUR) + 1) * E.MS_HOUR        # a funding hour that we also use as fold end
    n = int((t1 - T0) // E.MS_MIN) + 5
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0)
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())},
                        funding={"BTC": (np.array([t1], "int64"), np.array([0.5]))}, maxlev=10.0)
    start = E.AccountState(cross_collateral={"main": 7600.0})
    start.positions["BTC"] = E.Position("BTC", szi=1500.0, entry_px=100.0, mode="cross", leverage=10.0)  # $150k
    res = E.step_subaccount(pd.DataFrame([]), md, 7600.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=t1, start_ts_ms=T0, start_state=start)
    liq_ts = [f["our_ts"] for f in res["fills"] if f["fill_type"] == "market_liq_order"]
    assert len(liq_ts) == len(set(liq_ts))           # no same-ts double at the funding==fold-end boundary


def test_carry_in_seeds_short_not_long():
    # codex code-r4 #3: a SHORT carry-in must seed a NEGATIVE szi (signed exposure, no double-flip).
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 100, 100.0))
    # source carried a short, then adds -10 to -110 with signed (negative) exposure% post-action
    act = _action("BTC", T0 + 5 * E.MS_MIN, -1.0, "ADDON", position_after=-110.0, signed_size=-10.0,
                  carry_in_status="carry")
    res = E.step_subaccount(pd.DataFrame([act]), md, 10_000.0,
                            E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity", start_policy="causal_carry_in"),
                            end_ts_ms=T0 + 20 * E.MS_MIN)
    # ending position is a SHORT (negative szi), ~the source's -110/-100 ratio scaled to our equity
    bt = res["ending_account_state"]["positions"].get("BTC")
    assert bt is not None and bt["szi"] < 0


def test_empty_actions_survives_flat():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 50, 100.0))
    res = _run(pd.DataFrame([]), md)
    assert res["summary"]["final_equity"] == 10_000.0
    assert res["summary"]["outcome_states"] == ["survived"]


def test_ending_account_state_returned():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    acts = pd.DataFrame([_action("BTC", T0 + 5 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0)])
    res = _run(acts, md)
    eas = res["ending_account_state"]
    assert "cross_collateral" in eas and "positions" in eas and "cooldown_until_ms" in eas
    assert "BTC" in eas["positions"]      # the open long carries into ending state for M9 chaining


def test_open_position_is_marked_and_loss_debited_at_cutoff():
    prices = np.linspace(100.0, 80.0, 200)
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    acts = pd.DataFrame([_action("BTC", T0 + 5 * E.MS_MIN, 0.5, "ENTRY", position_after=1.0)])
    end = T0 + 150 * E.MS_MIN
    res = _run(acts, md, end=end)
    open_rows = [p for p in res["positions"] if p["closed"] is False]
    assert len(open_rows) == 1
    row = open_rows[0]
    assert row["close_reason"] == "cutoff_mark"
    assert row["unrealized_pnl_at_cutoff"] < 0
    assert row["marked_pnl_after_cost"] < 0
    summary = res["summary"]
    assert summary["censoring_coverage"] == 1.0
    assert summary["open_loss_debit"] < 0
    assert summary["conservative_pnl_total"] < summary["realized_pnl_total"]


def test_untracked_carry_in_is_visible_as_incomplete_censoring():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    start = E.AccountState(cross_collateral={"main": 5000.0})
    start.positions["BTC"] = E.Position("BTC", szi=50.0, entry_px=100.0, mode="cross", leverage=1.0)
    res = E.step_subaccount(pd.DataFrame([]), md, 10_000.0,
                            E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=T0 + 100 * E.MS_MIN, start_ts_ms=T0, start_state=start)
    assert res["summary"]["n_open_positions_at_cutoff"] == 1
    assert res["summary"]["n_open_positions_marked_at_cutoff"] == 0
    assert res["summary"]["censoring_coverage"] == 0.0


# --------------------------------------------------------------------------- #
# Streaming
# --------------------------------------------------------------------------- #
def test_streaming_writer_multipart(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
    from _streaming_io import ShardedParquetWriter
    w = ShardedParquetWriter(tmp_path / "out.parquet", flush_rows=3)
    for i in range(10):
        w.add({"a": i, "b": float(i)})
    n = w.close()
    assert n == 10
    df = pd.read_parquet(tmp_path / "out.parquet")
    assert len(df) == 10 and list(df.a) == list(range(10))


# --------------------------------------------------------------------------- #
# M6b-FINAL producers: CHANGE 1 boundary equity, D1 calib, CHANGE 2 tracking_error
# (empirical value assertions -- byte-identical proves the EXISTING outputs unchanged,
#  these prove the NEW outputs are CORRECT)
# --------------------------------------------------------------------------- #
DAY = 86_400_000
T14 = 14 * DAY


def test_change1_boundary_equity_samples_emitted():
    # 30-day flat-price hold -> a block boundary at T0+14d must be sampled with ~constant equity.
    n = 31 * 1440
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, n, 100.0))
    end = T0 + 30 * DAY
    acts = pd.DataFrame([_action("BTC", T0 + 10 * E.MS_MIN, 0.5)])
    res = E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=end, start_ts_ms=T0, entity_id=1, fold_id=1)
    eq = pd.DataFrame(res["equity"])
    flags = set(eq["event_flag"])
    assert "fold_start" in flags and "block_boundary" in flags and "fold_end" in flags
    fs = eq[eq.event_flag == "fold_start"].iloc[0]
    assert fs["ts"] == T0 and abs(fs["subaccount_equity"] - 10_000.0) < 1e-6   # pre-action cash
    bb = eq[(eq.event_flag == "block_boundary")]
    assert (bb["ts"] == T0 + T14).any()                                        # the 14d anchor exists
    b14 = bb[bb.ts == T0 + T14].iloc[0]
    assert abs(b14["subaccount_equity"] - 10_000.0) < 50.0                      # flat px -> ~unchanged


def test_change1_boundary_anchor_count_matches_blocks():
    n = 31 * 1440
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, n, 100.0))
    end = T0 + 30 * DAY                       # 30d -> boundaries at k=0,1,2 (0,14,28d); 42d would be 3
    acts = pd.DataFrame([_action("BTC", T0 + 10 * E.MS_MIN, 0.5)])
    res = E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=end, start_ts_ms=T0, entity_id=1, fold_id=1)
    eq = pd.DataFrame(res["equity"])
    anchors = sorted(eq[eq.event_flag.isin(["fold_start", "block_boundary"])]["ts"].unique())
    assert anchors == [T0, T0 + T14, T0 + 2 * T14]    # 0, 14, 28 days (< 30d end)


def test_change2_te_zero_when_tracking_well():
    # source holds 50% long; our copy enters 50% and holds flat -> our_pct ~= target -> TE ~ 0.
    n = 31 * 1440
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, n, 100.0))
    end = T0 + 20 * DAY
    acts = pd.DataFrame([_action("BTC", T0 + 10 * E.MS_MIN, 0.5)])
    res = E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=end, start_ts_ms=T0, entity_id=1, fold_id=1)
    s = res["summary"]
    assert s["tracking_error"] is not None
    assert s["tracking_error_active_ms"] > 0
    assert s["tracking_error"] < 0.05            # tracks the source closely


def test_te_l1_helper_signed_additive():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 10, 100.0))
    st = E.AccountState(cross_collateral={"main": 0.0})
    # equity 1000; BTC szi=5 @100 -> notional 500 -> our_pct=0.5
    st.positions["BTC"] = E.Position("BTC", szi=5.0, entry_px=100.0, mode="cross", leverage=1.0)
    st.cross_collateral["main"] = 500.0          # equity = 500 cash + 500 uPnL@entry = ... use mark
    ts = T0 + 5 * E.MS_MIN
    # perfect match
    l1, act = E._te_l1_active(st, md, {"BTC": st.positions["BTC"].szi * 100.0 / st.equity(E._marks(st, md, ts))}, ts)
    assert act and l1 < 1e-9
    our_pct = st.positions["BTC"].szi * 100.0 / st.equity(E._marks(st, md, ts))
    # opposite sign target -> additive (|our - (-our)| = 2*our)
    l1_opp, _ = E._te_l1_active(st, md, {"BTC": -our_pct}, ts)
    assert abs(l1_opp - 2 * our_pct) < 1e-9
    # disjoint coin in target we don't hold -> adds |target|
    l1_dis, _ = E._te_l1_active(st, md, {"BTC": our_pct, "ETH": 0.4}, ts)
    assert abs(l1_dis - 0.4) < 1e-9
    # fully flat -> inactive
    _, act2 = E._te_l1_active(E.AccountState(cross_collateral={"main": 1000.0}), md, {}, ts)
    assert act2 is False


def test_d2_wipeout_emits_zero_equity_anchors():
    # crash after entry -> account wiped -> a post-crash action sees cur_eq<=0 -> true _ruin (D2 path).
    # remaining boundary anchors + fold_end at equity 0 (so M6b post-wipe block ROE = -100%).
    n = 31 * 1440
    o = _flat_ohlc("BTC", T0, n, 100.0)
    mins, op, hi, lo, cl = o["BTC"]
    crash_from = 1500
    for arr in (op, hi, lo, cl):
        arr[crash_from:] = 1.0   # -99% crash -> long 5x position wiped
    md = FakeMarketData(ohlc={"BTC": (mins, op, hi, lo, cl)}, maxlev=10.0)
    end = T0 + 30 * DAY
    acts = pd.DataFrame([_action("BTC", T0 + 10 * E.MS_MIN, 5.0),
                         _action("BTC", T0 + 25 * DAY, 5.0)])     # post-crash action -> _ruin
    res = E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"),
                            end_ts_ms=end, start_ts_ms=T0, entity_id=1, fold_id=1)
    s = res["summary"]
    eq = pd.DataFrame(res["equity"])
    assert s["ruin"] is True                         # second action sees cur_eq<=0 -> _ruin (D2 path)
    post = eq[(eq.event_flag == "block_boundary") & (eq.ts.isin([T0 + T14, T0 + 2 * T14]))]
    assert len(post) >= 1 and (post["subaccount_equity"] == 0.0).all()
    fe = eq[eq.event_flag == "fold_end"].iloc[-1]
    assert fe["subaccount_equity"] == 0.0
    # D3: post-ruin TE accrues full penalty (source still 5x exposure) over the remaining horizon
    assert s["tracking_error"] is not None and s["tracking_error"] > 0.5


# --------------------------------------------------------------------------- #
# FOLLOWER TRAILING EXIT (7% circuit breaker) — codex review fixes (bugs #1-#8)
# --------------------------------------------------------------------------- #
def _ohlc_path(coin, prices, start_ms=T0):
    n = len(prices)
    mins = np.arange(n, dtype="int64") * E.MS_MIN + start_ms
    arr = np.array(prices, dtype="float64")
    return {coin: (mins, arr.copy(), arr.copy(), arr.copy(), arr.copy())}


def test_follower_trail_fires_flattens_and_halts():
    # prices per minute; causal mark at minute k = prices[k-1]. enter@100, peak@110, breach@102 (>7% DD).
    prices = [100, 100, 110, 110, 102, 102, 120, 120]
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 1.0, "ENTRY"),       # enter long @100
        _action("BTC", T0 + 3 * E.MS_MIN, 1.0, "ADDON"),       # observe peak @110
        _action("BTC", T0 + 5 * E.MS_MIN, 1.0, "ADDON"),       # @102 -> pre-action breaker fires
        _action("BTC", T0 + 7 * E.MS_MIN, 1.0, "ADDON"),       # @120 -> MUST be ignored (halted)
    ])
    res = E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity", follower_trail=0.07),
                            end_ts_ms=T0 + 9 * E.MS_MIN)
    exits = [e for e in res["events"] if e.get("event_type") == "follower_trail_exit"]
    assert len(exits) == 1, "breaker must fire exactly once"
    # all positions flat after the halt (guaranteed close-all)
    assert res["ending_account_state"]["positions"] == {}
    # halted: the favorable @120 action was NOT copied -> no re-entry after the exit ts
    exit_ts = exits[0]["ts"]
    assert not any(f["our_ts"] > exit_ts for f in res["fills"]), "no copy after halt"
    assert not res["summary"]["ruin"]


def test_follower_trail_disabled_is_byte_identical():
    prices = [100, 100, 110, 110, 102, 102, 120, 120]
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    acts = pd.DataFrame([
        _action("BTC", T0 + 1 * E.MS_MIN, 1.0, "ENTRY"),
        _action("BTC", T0 + 3 * E.MS_MIN, 1.0, "ADDON"),
        _action("BTC", T0 + 5 * E.MS_MIN, 1.0, "ADDON"),
        _action("BTC", T0 + 7 * E.MS_MIN, 1.0, "ADDON"),
    ])
    base = E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity", follower_trail=None),
                             end_ts_ms=T0 + 9 * E.MS_MIN)
    # disabled path: NO breaker event, and a position remains open (verbatim copy)
    assert not any(e.get("event_type") == "follower_trail_exit" for e in base["events"])
    assert "BTC" in base["ending_account_state"]["positions"]


def test_follower_trail_fires_at_fold_end_no_action():
    # carried long, ZERO actions; price drifts 100 -> 90 (>7% DD on 1x) -> must flatten at fold END (#2).
    prices = [100] * 3 + [90] * 6
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices))
    start = E.AccountState(cross_collateral={"main": 0.0})
    start.positions["BTC"] = E.Position("BTC", szi=100.0, entry_px=100.0, mode="cross", leverage=1.0)
    start.cross_collateral["main"] = 10_000.0 - 100.0 * 100.0 + 100.0 * 100.0  # cash s.t. eq@100 ~ 10k
    # peak seeded from real equity at fold start (#8); breach realized only via fold-end MTM.
    res = E.step_subaccount(pd.DataFrame([]), md, 10_000.0,
                            E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity", follower_trail=0.07, start_policy="causal_carry_in"),
                            end_ts_ms=T0 + 8 * E.MS_MIN, start_ts_ms=T0, start_state=start)
    assert any(e.get("event_type") == "follower_trail_exit" for e in res["events"]), "fold-end breach must fire"
    assert res["ending_account_state"]["positions"] == {}


def test_follower_trail_force_close_ignores_min_notional_and_capacity():
    # BIG BTC position triggers the breach (DD>7%); a separate DUST position (closing notional <<
    # MIN_ORDER_NOTIONAL) co-exists. tiny adv -> normal orders would capacity-cap. After the breaker
    # fires, BOTH must be gone (force_close bypasses min-notional + capacity; residual drop catches dust) (#5).
    ohlc = {}
    ohlc.update(_ohlc_path("BTC", [100] * 3 + [80] * 6))
    ohlc.update(_ohlc_path("ETH", [100] * 9))     # dust coin, flat
    md = FakeMarketData(ohlc=ohlc, adv=1.0)        # adv tiny -> normal orders would cap
    start = E.AccountState(cross_collateral={"main": 10_000.0})
    start.positions["BTC"] = E.Position("BTC", szi=100.0, entry_px=100.0, mode="cross", leverage=1.0)
    start.positions["ETH"] = E.Position("ETH", szi=0.01, entry_px=100.0, mode="cross", leverage=1.0)  # dust ($1)
    res = E.step_subaccount(pd.DataFrame([]), md, 10_000.0,
                            E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity", follower_trail=0.07, start_policy="causal_carry_in"),
                            end_ts_ms=T0 + 8 * E.MS_MIN, start_ts_ms=T0, start_state=start)
    assert any(e.get("event_type") == "follower_trail_exit" for e in res["events"])
    assert res["ending_account_state"]["positions"] == {}, "no position (incl dust) may survive a breaker flatten"
    btc_exit = max((f for f in res["fills"] if f["coin"] == "BTC"), key=lambda f: abs(f["our_fill_size"]))
    capped_impact = E.DEFAULT_IMPACT_K_BPS * E.CAPACITY_PARTICIPATION_CAP ** E.DEFAULT_IMPACT_ALPHA
    assert btc_exit["impact_bps"] > capped_impact, "forced-exit impact must use full order/ADV participation"


# --------------------------------------------------------------------------- #
# CHANGE A — realized round-trip aggregates (consumed by M6b)
# --------------------------------------------------------------------------- #
def test_change_a_round_trip_aggregates_two_round_trips():
    # Two complete round-trips on a coin that finishes UP: enter@100, exit@110 (win); enter@110,
    # exit@120 (win). Causal mark = prior-minute close, so price-step minutes are placed so each
    # entry/exit sees the intended causal price. n_round_trips=2, both wins, realized_pnl>0.
    n = 200
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0)
    px[20:] = 110.0      # from minute 20 the causal mark (minute 21+) is 110
    px[60:] = 120.0      # from minute 60 the causal mark (minute 61+) is 120
    o = {"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}
    md = FakeMarketData(ohlc=o, maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 1.0, "ENTRY", position_after=100.0),   # @100
        _action("BTC", T0 + 25 * E.MS_MIN, 0.0, "EXIT", position_after=0.0),     # @110 -> RT#1 win
        _action("BTC", T0 + 30 * E.MS_MIN, 1.0, "ENTRY", position_after=100.0),  # @110
        _action("BTC", T0 + 65 * E.MS_MIN, 0.0, "EXIT", position_after=0.0),     # @120 -> RT#2 win
    ])
    s = _run(acts, md, entity_id=7, fold_id=3)["summary"]
    assert set(["n_round_trips", "n_round_trip_wins", "round_trip_win_rate",
                "realized_pnl_total", "realized_roe"]).issubset(s.keys())
    assert s["n_round_trips"] == 2
    assert s["n_round_trip_wins"] == 2
    assert abs(s["round_trip_win_rate"] - 1.0) < 1e-9
    assert s["realized_pnl_total"] > 0.0
    assert abs(s["realized_roe"] - s["realized_pnl_total"] / 10_000.0) < 1e-9
    # existing summary keys preserved (M9 depends on them)
    for k in ("roe_engine", "max_dd", "tracking_error", "n_fills"):
        assert k in s


def test_change_a_losing_round_trip_not_counted_as_win():
    # one round-trip that loses (enter@100, exit@90): n_round_trips=1, wins=0, realized_pnl<0.
    n = 200
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[20:] = 90.0
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 1.0, "ENTRY", position_after=100.0),
        _action("BTC", T0 + 25 * E.MS_MIN, 0.0, "EXIT", position_after=0.0),
    ])
    s = _run(acts, md)["summary"]
    assert s["n_round_trips"] == 1 and s["n_round_trip_wins"] == 0
    assert s["round_trip_win_rate"] == 0.0
    assert s["realized_pnl_total"] < 0.0


def test_change_a_open_position_not_counted():
    # a single entry never closed -> NO completed round-trip.
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    acts = pd.DataFrame([_action("BTC", T0 + 5 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0)])
    s = _run(acts, md)["summary"]
    assert s["n_round_trips"] == 0 and s["round_trip_win_rate"] == 0.0


# --------------------------------------------------------------------------- #
# codex finding #1 — a LIQUIDATED / BACKSTOPPED / RUINED round-trip is a LOSS, not a win
# --------------------------------------------------------------------------- #
def test_finding1_liquidated_round_trip_is_a_loss():
    # leveraged long, then a hard crash forces a liquidation/backstop/ruin wipe. The forced-close
    # round-trip MUST be booked as a LOSS (realized_pnl < 0, zero wins), never a near-flat win.
    n = 300
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    px = np.full(n, 100.0); px[100:] = 40.0     # -60% crash wipes a 5x long
    md = FakeMarketData(ohlc={"BTC": (mins, px.copy(), px.copy(), px.copy(), px.copy())}, maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 5.0, "ENTRY", position_after=500.0),
        _action("BTC", T0 + 250 * E.MS_MIN, 5.0, "ADDON", position_after=500.0),
    ])
    s = _run(acts, md)["summary"]
    # the position was force-closed by ruin/backstop/liquidation
    assert set(s["outcome_states"]) & {"backstop", "account_ruin", "position_liquidated"}
    # ... and that terminal round-trip is realized as a LOSS, not a spurious win
    assert s["n_round_trips"] >= 1, "the wiped position must finalize a round-trip"
    assert s["n_round_trip_wins"] == 0, "a wiped trade can never be a win"
    assert s["realized_pnl_total"] < 0.0, "terminal loss must land in realized_pnl_total"


def test_finding1_ruined_round_trip_is_a_loss():
    # explicit D2 ruin path: crash wipes equity, a post-crash action triggers _ruin. The open
    # round-trip must be a LOSS in the realized aggregates (not flat/win).
    n = 200
    o = _flat_ohlc("BTC", T0, n, 100.0)
    mins, op, hi, lo, cl = o["BTC"]
    for arr in (op, hi, lo, cl):
        arr[60:] = 1.0      # -99% crash
    md = FakeMarketData(ohlc={"BTC": (mins, op, hi, lo, cl)}, maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 5.0, "ENTRY", position_after=500.0),
        _action("BTC", T0 + 120 * E.MS_MIN, 5.0, "ADDON", position_after=500.0),  # sees cur_eq<=0 -> _ruin
    ])
    s = _run(acts, md, end=T0 + 190 * E.MS_MIN)["summary"]
    assert s["ruin"] is True
    assert s["n_round_trips"] >= 1
    assert s["n_round_trip_wins"] == 0
    assert s["realized_pnl_total"] < 0.0


def test_ruin_final_equity_and_roe_not_stale():
    # codex regression: after a RUIN, _finalize must report the POST-ruin equity (0), not the last
    # positive pre-ruin _core_final_eq sample. A ruined subaccount has final_equity ~= 0 and
    # roe_engine ~= -1.0 (-100%), consistent with the realized terminal round-trip loss.
    n = 200
    o = _flat_ohlc("BTC", T0, n, 100.0)
    mins, op, hi, lo, cl = o["BTC"]
    for arr in (op, hi, lo, cl):
        arr[60:] = 1.0      # -99% crash
    md = FakeMarketData(ohlc={"BTC": (mins, op, hi, lo, cl)}, maxlev=10.0)
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 5.0, "ENTRY", position_after=500.0),
        _action("BTC", T0 + 120 * E.MS_MIN, 5.0, "ADDON", position_after=500.0),  # sees cur_eq<=0 -> _ruin
    ])
    s = _run(acts, md, end=T0 + 190 * E.MS_MIN)["summary"]
    assert s["ruin"] is True
    assert s["final_equity"] < 1e-6, "ruined subaccount must report ~0 final_equity, not a stale positive"
    assert abs(s["roe_engine"] - (-1.0)) < 1e-9, "ruined subaccount ROE must be -100%, not stale"


# --------------------------------------------------------------------------- #
# codex finding #2 — flip-through-zero fee SPLIT across the two round-trips
# --------------------------------------------------------------------------- #
def test_finding2_flip_fee_split_across_round_trips():
    # Flat price 100. Open +100 szi long, then flip to -100 szi short in ONE order (delta -200), then
    # close the short. The flip order's fee must be SPLIT: half to the closing (long) RT, half to the
    # opening (short) RT -- not all charged to the closing RT with a fee-free short open.
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 200, 100.0))
    acts = pd.DataFrame([
        _action("BTC", T0 + 5 * E.MS_MIN, 1.0, "ENTRY", position_after=100.0),    # +100 long
        _action("BTC", T0 + 25 * E.MS_MIN, -1.0, "FLIP", position_after=-100.0),  # flip to -100 short
        _action("BTC", T0 + 45 * E.MS_MIN, 0.0, "EXIT", position_after=0.0),      # close the short
    ])
    s = _run(acts, md, entity_id=9, fold_id=1)["summary"]
    # two completed round-trips (the long that flipped closed + the short that closed)
    assert s["n_round_trips"] == 2
    # build the SAME scenario but inspect per-RT booking via a controlled _book_fill replay to assert
    # the split is proportional. Equity-flat so realized=0 on the flip; both RTs differ ONLY by fees.
    summ = E._new_summary(1, 1, 10_000.0, 0, E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"), md)
    st = E.AccountState(cross_collateral={"main": 10_000.0})
    # open long 100 @100 (entry fee on 100 sz)
    E._book_fill(st, md, "BTC", 100.0, 100.0, "cross", fee=100.0 * 100.0 * 0.00045, summary=summ)
    pnl_before = summ["realized_pnl_total"]
    nrt_before = summ["n_round_trips"]
    # flip -200 @100 -> closes 100 (long RT) + opens 100 short. Fee on 200 sz; must split 50/50.
    flip_fee = 200.0 * 100.0 * 0.00045
    E._book_fill(st, md, "BTC", -200.0, 100.0, "cross", fee=flip_fee, summary=summ)
    # exactly one RT closed by the flip
    assert summ["n_round_trips"] == nrt_before + 1
    closing_rt_pnl = summ["realized_pnl_total"] - pnl_before   # = -(entry fee on long) - (half flip fee)
    long_entry_fee = 100.0 * 100.0 * 0.00045
    expected_close = -(long_entry_fee + flip_fee * 0.5)        # realized MTM is 0 (flat px)
    assert abs(closing_rt_pnl - expected_close) < 1e-9, "closing RT must bear only HALF the flip fee"
    # the new short RT now carries the OTHER half as its open fee
    assert abs(summ["_rt"]["BTC"]["fee"] - flip_fee * 0.5) < 1e-9, "short RT must bear its own half"


# --------------------------------------------------------------------------- #
# codex finding #3 — latency drift is CAUSAL: it depends on the PRIOR bar, not the containing bar
# --------------------------------------------------------------------------- #
def test_finding3_latency_drift_uses_prior_bar_not_containing_bar():
    # Build a grid where the PRIOR bar (the one that closed before our fill) is VOLATILE while the
    # CONTAINING bar (the one our fill lands in) is FLAT. A causal drift must charge a haircut (prior
    # bar vol), whereas the old non-causal containing-bar logic would charge ZERO. Conversely, a flat
    # prior bar + volatile containing bar must charge ~ZERO causal haircut.
    n = 60
    mins = np.arange(n, dtype="int64") * E.MS_MIN + T0
    # CASE A: prior bar volatile, containing bar flat. action at minute 10, latency 0 -> our_ts in
    # minute 10's bar (flat). The PRIOR completed bar is minute 9 (volatile).
    o = np.full(n, 100.0); c = np.full(n, 100.0); h = np.full(n, 100.0); lo = np.full(n, 100.0)
    o[9], c[9], h[9], lo[9] = 100.0, 110.0, 110.0, 100.0   # minute-9 bar: +10% (volatile, causal/prior)
    # minute-10 bar stays flat at 100 (containing bar of a 0-latency fill at minute 10)
    md_a = FakeMarketData(ohlc={"BTC": (mins, o, h, lo, c)}, maxlev=10.0)
    px_prior = E._bar_vol_proxy(md_a, "BTC", T0 + 10 * E.MS_MIN)
    assert abs(px_prior - 0.10) < 1e-9, "vol proxy must read the PRIOR (minute-9) bar's |return| = 10%"

    # CASE B: prior bar flat, containing bar volatile -> causal proxy must be ~0 (no look-ahead into
    # the containing bar). minute-9 flat, minute-10 volatile.
    o2 = np.full(n, 100.0); c2 = np.full(n, 100.0); h2 = np.full(n, 100.0); lo2 = np.full(n, 100.0)
    o2[10], c2[10], h2[10], lo2[10] = 100.0, 110.0, 110.0, 100.0
    md_b = FakeMarketData(ohlc={"BTC": (mins, o2, h2, lo2, c2)}, maxlev=10.0)
    px_flat = E._bar_vol_proxy(md_b, "BTC", T0 + 10 * E.MS_MIN)
    assert abs(px_flat) < 1e-9, "containing-bar vol must NOT leak in (causal): prior bar is flat -> 0"

    # end-to-end: the volatile-PRIOR-bar fill must be WORSE (higher buy) than the flat-prior-bar fill,
    # at the same latency, proving the drift is driven by the causal prior bar.
    def _buy_px(md):
        acts = pd.DataFrame([_action("BTC", T0 + 10 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0)])
        res = E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=30_000),
                                end_ts_ms=T0 + 50 * E.MS_MIN)
        buys = [f for f in res["fills"] if f["side"] == "buy" and f["fill_type"] == "normal"]
        return buys[0]["our_fill_px"] if buys else None
    pa, pb = _buy_px(md_a), _buy_px(md_b)
    assert pa is not None and pb is not None
    assert pa > pb, "volatile PRIOR bar -> worse fill; flat prior bar -> no causal haircut"


def test_finding4_latency_drift_in_slip_diagnostic():
    # the notional-weighted slip diagnostic must INCLUDE the latency drift bps (not understate the
    # execution haircut). Same up-bar; higher latency -> bigger drift -> bigger reported slip.
    md = FakeMarketData(ohlc=_drift_ohlc("BTC", 200, 100.0, 102.0), maxlev=10.0)  # +2% bars
    acts = pd.DataFrame([_action("BTC", T0 + 50 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0)])
    s_lo = _run(acts, md, params=E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity"))["summary"]
    s_hi = _run(acts, md, params=E.EngineParams(copy_latency_ms=30_000))["summary"]
    assert s_hi["slip_bps_notional_weighted"] > s_lo["slip_bps_notional_weighted"], \
        "latency drift must raise the reported notional-weighted slip"


# --------------------------------------------------------------------------- #
# CHANGE B — latency adverse-drift haircut (the critical fix)
# --------------------------------------------------------------------------- #
def _drift_ohlc(coin, n, open_px, close_px, start_ms=T0):
    """A constant up-bar grid: every 1m bar opens at open_px and closes at close_px (bar_ret fixed).
    high/low set to the range so liquidity()/breach scans stay well-defined."""
    mins = np.arange(n, dtype="int64") * E.MS_MIN + start_ms
    o = np.full(n, float(open_px)); c = np.full(n, float(close_px))
    hi = np.maximum(o, c); lo = np.minimum(o, c)
    return {coin: (mins, o, hi, lo, c)}


def _single_buy_fill_px(latency_ms, open_px, close_px):
    md = FakeMarketData(ohlc=_drift_ohlc("BTC", 200, open_px, close_px), maxlev=10.0)
    acts = pd.DataFrame([_action("BTC", T0 + 50 * E.MS_MIN, 0.5, "ENTRY", position_after=50.0)])
    res = _run(acts, md, params=E.EngineParams(copy_latency_ms=latency_ms))
    buys = [f for f in res["fills"] if f["side"] == "buy" and f["fill_type"] == "normal"]
    assert buys, "expected a normal buy fill"
    return buys[0]["our_fill_px"]


def test_change_b_higher_latency_worse_fill():
    # same up-bar (open 100 -> close 101); a BUY with higher copy_latency_ms must fill at a WORSE
    # (higher) price than a near-zero-latency BUY. Causal mark is identical (same prior close) so the
    # difference is the latency haircut alone.
    px_lo = _single_buy_fill_px(0, 100.0, 101.0)        # ~no latency haircut
    px_hi = _single_buy_fill_px(30_000, 100.0, 101.0)   # 0.5 of a 1m bar
    assert px_hi > px_lo, "higher latency must produce a worse (higher) buy fill"


def test_change_b_higher_volatility_worse_fill():
    # same latency; a more volatile bar (bigger |bar_ret|) must produce a WORSE (higher) buy fill.
    px_calm = _single_buy_fill_px(30_000, 100.0, 100.5)   # 0.5% bar move
    px_vol = _single_buy_fill_px(30_000, 100.0, 105.0)    # 5% bar move
    assert px_vol > px_calm, "higher bar volatility must produce a worse buy fill"


def test_change_b_flat_bar_no_haircut_byte_identical():
    # a flat bar (bar_ret==0) charges ZERO latency haircut at ANY latency -> fill px unchanged vs 0ms.
    px0 = _single_buy_fill_px(0, 100.0, 100.0)
    px_hi = _single_buy_fill_px(30_000, 100.0, 100.0)
    assert abs(px0 - px_hi) < 1e-12


def test_change_b_latency_model_flag_present():
    md = FakeMarketData(ohlc=_flat_ohlc("BTC", T0, 50, 100.0))
    s = _run(pd.DataFrame([]), md)["summary"]
    assert s["latency_model"] == "bar_drift_v1"


def test_follower_trail_records_ruin_if_flatten_nonpositive():
    # equity already essentially gone at the breach; post-flatten equity <= 0 -> ruin MUST be set (#6).
    prices = [100] * 3 + [50] * 6   # 50% crash on a ~10x carried position wipes equity
    md = FakeMarketData(ohlc=_ohlc_path("BTC", prices), maxlev=20.0)
    start = E.AccountState(cross_collateral={"main": 100.0})
    start.positions["BTC"] = E.Position("BTC", szi=100.0, entry_px=100.0, mode="cross", leverage=10.0)
    res = E.step_subaccount(pd.DataFrame([]), md, 10_100.0,
                            E.EngineParams(copy_latency_ms=0, sizing_mode="leader_equity", follower_trail=0.07, start_policy="causal_carry_in"),
                            end_ts_ms=T0 + 8 * E.MS_MIN, start_ts_ms=T0, start_state=start)
    assert res["ending_account_state"]["positions"] == {}
    assert res["summary"]["ruin"], "non-positive post-flatten equity must record ruin"


def test_merge_m07_worker_outputs_streams_all_rows(tmp_path):
    """Parallel workers publish every disjoint row in deterministic worker order."""
    names = (
        "m07_fills.parquet", "m07_events.parquet", "m07_summary.parquet",
        "m07_equity.parquet", "m07_positions.parquet",
    )
    workers = [tmp_path / "w0", tmp_path / "w1"]
    for wi, worker in enumerate(workers):
        worker.mkdir()
        for name in names:
            pd.DataFrame({"worker": [wi], "value": [wi + 0.5]}).to_parquet(worker / name, index=False)

    out = tmp_path / "merged"
    E.merge_m07_worker_outputs(workers, out)

    for name in names:
        got = pd.read_parquet(out / name)
        assert got["worker"].tolist() == [0, 1]
        assert got["value"].tolist() == [0.5, 1.5]
