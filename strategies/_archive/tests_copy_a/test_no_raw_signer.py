"""Structural guard (Fable + Codex P0): the ONLY place raw HL signing calls may appear inside the
Copy A package is hl_sdk_adapters.py (the single audited SDK surface). Any Exchange.order /
market_close / update_leverage / bulk_orders call anywhere else in strategies/live/copy_a/ is a
bypass of the RiskBroker admission gate and fails this test.

Run: pytest tests/copy_a/test_no_raw_signer.py -q
"""
import re
from pathlib import Path

PKG = Path(__file__).resolve().parents[2] / "strategies" / "live" / "copy_a"
ALLOWED = {"hl_sdk_adapters.py"}   # the sole audited signer surface
# raw signing / exposure-changing SDK calls
SIGNER = re.compile(r"\.(order|market_close|market_open|bulk_orders|update_leverage)\s*\(")


def test_no_raw_signer_outside_adapter():
    offenders = []
    for py in PKG.glob("*.py"):
        if py.name in ALLOWED:
            continue
        text = py.read_text()
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if SIGNER.search(line):
                offenders.append(f"{py.name}:{i}: {stripped}")
    assert not offenders, (
        "Raw signer calls found outside hl_sdk_adapters.py (RiskBroker gate bypass):\n"
        + "\n".join(offenders)
    )
