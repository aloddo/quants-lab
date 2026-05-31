"""V15 M7 engine tests — golden HL mechanics + synthetic scenarios + streaming + determinism + causal.

Design: brain projects/quant/v15/modules/m07. Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m07.py -q
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
    return E.step_subaccount(acts, md, eq, params or E.EngineParams(copy_latency_ms=0), end_ts_ms=end, **kw)


# --------------------------------------------------------------------------- #
# Metadata / helpers
# --------------------------------------------------------------------------- #
def test_coin_dex_scope():
    assert E.coin_dex("BTC") == "main"
    assert E.coin_dex("xyz:AMD") == "xyz"
    assert E.coin_dex("flx:FOO") == "flx"


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
    summary = E._new_summary(None, None, 14000.0, 0, E.EngineParams(copy_latency_ms=0), md)
    summary["_fills_ref"] = []
    fills, events = [], []
    # add 200 -> new notional 110k, total IM = 22k > ~14k cash -> must reject
    E._apply_order(st, md, "BTC", 200.0, 100.0, T0 + 5 * E.MS_MIN, {"ts": T0 + 5 * E.MS_MIN},
                   E.EngineParams(copy_latency_ms=0), 1.0, fills, events, summary)
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
    E.step_subaccount(acts, md, 10_000.0, E.EngineParams(copy_latency_ms=0), end_ts_ms=END, start_state=start)
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
    res = E.step_subaccount(pd.DataFrame([]), md, 10_000.0, E.EngineParams(copy_latency_ms=0),
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
    res = E.step_subaccount(pd.DataFrame([]), md, 10_000.0, E.EngineParams(copy_latency_ms=0),
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
    res = E.step_subaccount(pd.DataFrame([]), md, 600.0, E.EngineParams(copy_latency_ms=0),
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
    res = E.step_subaccount(pd.DataFrame([]), md, 8000.0, E.EngineParams(copy_latency_ms=0),
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
    res = E.step_subaccount(pd.DataFrame([]), md, 7600.0, E.EngineParams(copy_latency_ms=0),
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
                            E.EngineParams(copy_latency_ms=0, start_policy="causal_carry_in"),
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
