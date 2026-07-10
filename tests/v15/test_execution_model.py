import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import execution_model as em  # noqa: E402


def test_fee_schedule_honors_effective_subaccount_rate(tmp_path, monkeypatch):
    schedule = tmp_path / "fees.json"
    schedule.write_text(json.dumps({
        "base_taker_oneway": 0.00045,
        "base_maker_oneway": 0.00015,
        "referral_discount": 0.04,
        "referral_discount_applies_to_subaccounts": False,
        "effective_subaccount_taker_oneway": 0.00045,
    }))
    monkeypatch.setattr(em, "FEE_SCHEDULE", schedule)
    monkeypatch.setattr(em, "_FEES", None)
    assert em.fee_rt(maker=False) == pytest.approx(0.00090)
    assert em.fee_rt(maker=True) == pytest.approx(0.00030)


def test_fee_schedule_can_apply_discount_when_explicit(tmp_path, monkeypatch):
    schedule = tmp_path / "fees.json"
    schedule.write_text(json.dumps({
        "base_taker_oneway": 0.00045,
        "base_maker_oneway": 0.00015,
        "referral_discount": 0.04,
        "referral_discount_applies_to_subaccounts": True,
    }))
    monkeypatch.setattr(em, "FEE_SCHEDULE", schedule)
    monkeypatch.setattr(em, "_FEES", None)
    assert em.fee_rt(maker=False) == pytest.approx(0.000864)
    assert em.fee_rt(maker=True) == pytest.approx(0.000288)


def test_hip3_fee_uses_market_override_or_multiplier(tmp_path, monkeypatch):
    schedule = tmp_path / "fees.json"
    schedule.write_text(json.dumps({
        "base_taker_oneway": 0.00045,
        "base_maker_oneway": 0.00015,
        "effective_subaccount_taker_oneway": 0.00045,
        "hip3_mult": 2.0,
        "per_market": {"xyz:ABC": 0.0007},
    }))
    monkeypatch.setattr(em, "FEE_SCHEDULE", schedule)
    monkeypatch.setattr(em, "_FEES", None)
    assert em.fee_rt(coin="BTC") == pytest.approx(0.0009)
    assert em.fee_rt(coin="xyz:OTHER") == pytest.approx(0.0018)
    assert em.fee_rt(coin="xyz:ABC") == pytest.approx(0.0014)
    assert em.fee_rt(maker=True, coin="xyz:OTHER") == pytest.approx(0.0006)
