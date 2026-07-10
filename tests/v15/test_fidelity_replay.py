import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
from fidelity_replay import roundtrips  # noqa: E402


def test_roundtrip_reversal_closes_old_and_opens_residual_leg():
    fills = [
        (1000, "BTC", 1.0, 100.0),
        (2000, "BTC", -3.0, 110.0),  # close long 1, open short 2
        (3000, "BTC", 2.0, 100.0),
    ]
    rts = roundtrips(fills)
    assert len(rts) == 2
    assert rts[0][1] == 1 and rts[0][6] == pytest.approx(0.10)
    assert rts[1][1] == -1 and rts[1][6] == pytest.approx((110.0 - 100.0) / 110.0)
