"""V13 test infrastructure — pytest fixtures shared across all module tests.

Per Module 12 spec at brain:projects/quant/v13/modules/12-unit-test-contract.
"""
import sys
from pathlib import Path

# V13 moved from scripts/ to research/v13/ in the 2026-05-30 restructure.
# Individual legacy tests still prepend scripts/, so expose the canonical module
# directory centrally until those repetitive imports are cleaned up.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v13"))

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone


@pytest.fixture
def tiny_wallet_universe():
    """5 deterministic test wallets — composed of repeated hex digits for readability."""
    return [
        "0x" + "a" * 40,
        "0x" + "b" * 40,
        "0x" + "c" * 40,
        "0x" + "d" * 40,
        "0x" + "e" * 40,
    ]


@pytest.fixture
def tiny_fills(tiny_wallet_universe):
    """Synthetic in-window fills, all-long BTC, escalating sizes per wallet."""
    rows = []
    base_ts = 1_733_011_200_000  # 2025-12-01 00:00 UTC
    for i, w in enumerate(tiny_wallet_universe):
        rows.append({
            "wallet": w,
            "coin": "BTC",
            "time": base_ts + i * 60_000,
            "side": "B",
            "size": 0.1 * (i + 1),
            "price": 50000 + i * 100,
            "dir": "Open Long",
            "closedPnl": 0.0,
            "fee": 4.32,
            "builderFee": 0.0,
            "deployerFee": 0.0,
            "startPosition": 0.0,
        })
    return pd.DataFrame(rows)


@pytest.fixture
def synthetic_spot_fill(tiny_wallet_universe):
    """One spot fill (@-prefix coin) — should be filtered by Module 02 spot filter."""
    return pd.DataFrame([{
        "wallet": tiny_wallet_universe[0],
        "coin": "@142",  # spot coin format
        "time": 1_733_011_200_000,
        "side": "B",
        "size": 1.0,
        "price": 50.0,
        "dir": "Buy",
        "closedPnl": 0.0,
        "fee": 0.05,
        "builderFee": 0.0,
        "deployerFee": 0.0,
        "startPosition": 0.0,
    }])


@pytest.fixture
def synthetic_xyz_fill(tiny_wallet_universe):
    """One xyz: HIP-3 stock perp fill — should be filtered by Module 01 v8 MAIN-only."""
    return pd.DataFrame([{
        "wallet": tiny_wallet_universe[1],
        "coin": "xyz:NVDA",
        "time": 1_733_011_200_000,
        "side": "B",
        "size": 1.0,
        "price": 500.0,
        "dir": "Open Long",
        "closedPnl": 0.0,
        "fee": 0.22,
        "builderFee": 0.05,
        "deployerFee": 0.0,
        "startPosition": 0.0,
    }])


@pytest.fixture
def synthetic_ledger_events():
    """Representative HL ledger events covering each Module 01 handled type."""
    return [
        {"time": 1_733_100_000_000, "delta": {"type": "deposit", "usdc": 1000.0}},
        {"time": 1_733_100_500_000, "delta": {"type": "withdraw", "usdc": 200.0, "fee": 1.0}},
        # accountClassTransfer: r5 fix — toPerp flag-driven
        {"time": 1_733_101_000_000, "delta": {"type": "accountClassTransfer", "usdc": 500.0, "toPerp": True}},
        {"time": 1_733_101_500_000, "delta": {"type": "accountClassTransfer", "usdc": 300.0, "toPerp": False}},
        # spotTransfer: r5 fix — zero perp impact
        {"time": 1_733_102_000_000, "delta": {"type": "spotTransfer", "token": "USDC", "usdc": 100.0}},
        # send with main dex
        {"time": 1_733_102_500_000, "delta": {"type": "send", "token": "USDC", "user": "0xabc", "destination": "0xdef",
                                              "sourceDex": "main", "destinationDex": "main", "usdcValue": 50.0, "fee": 0.5}},
        # send with xyz dex — r5 #4 fix should EXCLUDE
        {"time": 1_733_103_000_000, "delta": {"type": "send", "token": "USDC", "user": "0xabc", "destination": "0xdef",
                                              "sourceDex": "xyz", "destinationDex": "xyz", "usdcValue": 999.0, "fee": 0.5}},
        # activateDexAbstraction with xyz dex — r7 fix should ignore
        {"time": 1_733_103_500_000, "delta": {"type": "activateDexAbstraction", "token": "USDC", "usdc": 100.0, "dex": "xyz"}},
        # activateDexAbstraction with main — r6 fix: -abs(amount) not amount+fee
        {"time": 1_733_104_000_000, "delta": {"type": "activateDexAbstraction", "token": "USDC", "usdc": 50.0, "dex": "main", "fee": 0.0}},
        # borrowLend with supply operation
        {"time": 1_733_104_500_000, "delta": {"type": "borrowLend", "token": "USDC", "usdc": 200.0, "operation": "supply"}},
        # borrowLend with withdraw operation
        {"time": 1_733_105_000_000, "delta": {"type": "borrowLend", "token": "USDC", "usdc": 150.0, "operation": "withdraw"}},
        # cStakingTransfer — zero perp impact
        {"time": 1_733_105_500_000, "delta": {"type": "cStakingTransfer", "amount": "5.0", "token": "HYPE"}},
    ]


@pytest.fixture
def fixed_anchor():
    """Anchor data shape returned by load_wallet_anchor — MAIN-only fields (v8)."""
    return {
        "cash": 1000.0,                   # MAIN dex cash
        "positions": {"BTC": 0.1},        # MAIN dex positions
        "aggregate_cash": 1500.0,         # informational
        "aggregate_positions": {"BTC": 0.1},
        "aggregate_acct_value": 1500.0,
        "fetched_ms": 1735660800000,
        "dexes_seen": {"main"},
        "perp_acct_value_today": 1000.0,
        "spot_usdc_today_placeholder": 0.0,
        "spot_usdc_today_status": "NOT_FETCHED_USE_perp_acct_value_today_INSTEAD",
    }


# Marker for slow integration tests
def pytest_addoption(parser):
    parser.addoption("--run-slow", action="store_true", default=False, help="run slow integration tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        return
    skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
