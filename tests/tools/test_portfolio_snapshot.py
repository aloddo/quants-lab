"""Regression eval: equity = exchange source-of-truth, no recompute, no perp double-count.

Locks the HARD invariant (Alberto correction msg 7126, 2026-05-25, repeated 10000+ times,
Rule 16): HL EQUITY = SPOT USDC ONLY. Perp account values (main/xyz/flx dexes) are LOCKED
MARGIN backing open positions and DOUBLE-COUNT if added to equity. They are NOT equity.

This is the regression test directed 2026-05-25 (equity-regression-eval task, "highest Alberto
escalation, direct financial risk") that was never verifiably shipped. It guards the canonical
tools/portfolio_snapshot.py against the exact recurring bug: an agent re-adding perp acct_value
into HL equity and over-reporting the book by thousands of dollars.

Offline: all exchange calls are mocked. No network, no creds, CI-safe.
Run: pytest tests/tools/test_portfolio_snapshot.py -q
"""
import sys
from pathlib import Path

import pytest

# tools/ is not a package; add it to the path like the other suites do for scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

import portfolio_snapshot as ps  # noqa: E402


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _make_fake_post(spot_balances, perp_payload):
    """Return a fake requests.post that dispatches on the HL query `type`.

    perp_payload deliberately carries a fat marginSummary.accountValue so any test that
    leaks perp value into equity would visibly fail.
    """
    def _post(*args, **kwargs):
        body = kwargs.get("json") or (args[1] if len(args) > 1 else {})
        qtype = body.get("type")
        if qtype == "spotClearinghouseState":
            return _FakeResp({"balances": spot_balances})
        if qtype == "clearinghouseState":
            return _FakeResp(perp_payload)
        raise AssertionError(f"unexpected HL query type: {qtype}")
    return _post


# A perp state with a LARGE accountValue: if any equity path reads it, equity blows up.
_PERP_WITH_FAT_ACCTVALUE = {
    "assetPositions": [{"position": {"coin": "BTC"}}, {"position": {"coin": "ETH"}}],
    "marginSummary": {"accountValue": "5000.00", "totalMarginUsed": "1200.0"},
}


def test_hl_equity_is_spot_usdc_only(monkeypatch):
    """HL equity = sum of spot USDC `total`; non-USDC spot coins and perp acct_value excluded."""
    spot = [
        {"coin": "USDC", "total": "468.14"},
        {"coin": "HYPE", "total": "99.0"},   # non-USDC spot -> NOT equity
        {"coin": "PURR", "total": "12.5"},   # non-USDC spot -> NOT equity
    ]
    monkeypatch.setattr(ps.requests, "post",
                        _make_fake_post(spot, _PERP_WITH_FAT_ACCTVALUE))
    assert ps.hl_spot_usdc() == pytest.approx(468.14)


def test_perp_account_value_never_enters_equity(monkeypatch):
    """The core Rule 16 guard: a fat perp accountValue must NOT change spot-only equity."""
    spot = [{"coin": "USDC", "total": "468.14"}]
    monkeypatch.setattr(ps.requests, "post",
                        _make_fake_post(spot, _PERP_WITH_FAT_ACCTVALUE))
    eq = ps.hl_spot_usdc()
    assert eq == pytest.approx(468.14)
    # explicit: equity must be far below anything that included the $5000 perp value
    assert eq < 1000.0


def test_hl_spot_sums_multiple_usdc_entries(monkeypatch):
    spot = [
        {"coin": "USDC", "total": "100.0"},
        {"coin": "USDC", "total": "23.45"},
        {"coin": "ETH", "total": "1.0"},
    ]
    monkeypatch.setattr(ps.requests, "post",
                        _make_fake_post(spot, _PERP_WITH_FAT_ACCTVALUE))
    assert ps.hl_spot_usdc() == pytest.approx(123.45)


def test_perp_positions_are_counts_only(monkeypatch):
    """Per-dex perp query returns position COUNTS (ints), informational only, never dollars."""
    spot = [{"coin": "USDC", "total": "468.14"}]
    monkeypatch.setattr(ps.requests, "post",
                        _make_fake_post(spot, _PERP_WITH_FAT_ACCTVALUE))
    counts = ps.hl_perp_positions_by_dex()
    assert set(counts.keys()) == {"main", "xyz", "flx"}
    for v in counts.values():
        assert isinstance(v, int)
        assert v == 2  # two assetPositions in the fake perp payload


def test_empty_position_list_returns_zero_not_failure(monkeypatch):
    """A LEGIT zero-position dex (assetPositions present, empty) must return 0, not -1."""
    spot = [{"coin": "USDC", "total": "468.14"}]
    empty_perp = {"assetPositions": [], "marginSummary": {"accountValue": "0.0"}}
    monkeypatch.setattr(ps.requests, "post", _make_fake_post(spot, empty_perp))
    counts = ps.hl_perp_positions_by_dex()
    assert all(v == 0 for v in counts.values())


def test_missing_assetPositions_signals_failure_not_silent_zero(monkeypatch):
    """A transient bad response (no assetPositions key) must surface as -1, never silent 0
    (the 2026-06-22 XYZ=0 glitch). _retry exhausts, then -1."""
    spot = [{"coin": "USDC", "total": "468.14"}]
    bad_perp = {"marginSummary": {"accountValue": "5000.0"}}  # no assetPositions key
    monkeypatch.setattr(ps, "_retry", lambda fn, n=3, sleep=1.0: fn())  # no sleeps
    monkeypatch.setattr(ps.requests, "post", _make_fake_post(spot, bad_perp))
    counts = ps.hl_perp_positions_by_dex()
    assert all(v == -1 for v in counts.values())


def test_bybit_fails_loud_when_creds_missing(monkeypatch):
    """Missing creds must RAISE (fail-loud), never silently return $0 (2026-05-26 bug)."""
    monkeypatch.delenv("BYBIT_API_KEY", raising=False)
    monkeypatch.delenv("BYBIT_API_SECRET", raising=False)
    with pytest.raises(RuntimeError):
        ps.bybit_snapshot()


def test_combined_is_spot_plus_bybit_no_recompute(monkeypatch, capsys):
    """COMBINED line = HL_spot + Bybit_total exactly; no reconstruction, no perp added."""
    monkeypatch.setattr(ps, "hl_spot_usdc", lambda: 468.14)
    monkeypatch.setattr(ps, "hl_perp_positions_by_dex",
                        lambda: {"main": 0, "xyz": 0, "flx": 0})
    monkeypatch.setattr(ps, "bybit_snapshot", lambda: (460.97, 0.0, 0))
    ps.main()
    out = capsys.readouterr().out
    assert "HL_EQ=$468.14" in out
    assert "BYBIT_EQ=$460.97" in out
    assert "COMBINED=$929.11" in out  # 468.14 + 460.97, no recompute


def test_source_never_reads_perp_account_value():
    """Static canary: the module source must not reference accountValue at all in the equity
    path. The ONLY accountValue tokens allowed are in test/comment payloads here, not in
    portfolio_snapshot.py. If someone wires perp acct_value into equity, this fails."""
    src = (Path(ps.__file__)).read_text()
    # The canonical snapshot never reads marginSummary/accountValue for equity.
    assert "accountValue" not in src, (
        "portfolio_snapshot.py references accountValue -> Rule 16 regression risk "
        "(perp value must NEVER enter HL equity)"
    )
    assert "account_value" not in src
