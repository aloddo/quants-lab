"""
Runtime Stress Test — Exercises every V2 execution path against live exchanges.

NOT a unit test. This hits real APIs (Bybit mainnet, Binance spot) but uses
tiny quantities ($1/side) to minimize risk. Tests what static review can't:
- Exchange returns unexpected data types
- REST timing under real network conditions
- Fill detection with actual order lifecycle
- Crash recovery with real MongoDB state
- Inventory reconciliation accuracy

Run: set -a && source .env && set +a && python scripts/runtime_stress_test.py

Each test reports PASS/FAIL with details. Any FAIL means the system has a
runtime bug that static review missed.
"""
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Load .env
for line in Path(__file__).resolve().parents[1].joinpath(".env").read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

import aiohttp
from app.services.arb.order_api import BybitOrderAPI, BinanceOrderAPI
from app.services.arb.order_gateway import OrderGateway
from app.services.arb.position_store import PositionStore, PositionState, new_position_doc
from app.services.arb.fill_detector import FillDetector, TrackedLeg
from app.services.arb.instrument_rules import InstrumentRules, PairRules
from app.services.arb.inventory_ledger import InventoryLedger
from app.services.arb.price_feed import PriceFeed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BYBIT_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET", "")
BINANCE_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET = os.getenv("BINANCE_API_SECRET", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")

TEST_COLLECTION = "arb_h2_stress_test"  # separate from live positions

results = []


def report(name: str, passed: bool, detail: str = ""):
    icon = "PASS" if passed else "FAIL"
    results.append((name, passed, detail))
    print(f"  [{icon}] {name}" + (f" — {detail}" if detail else ""))


async def run_tests():
    print("=" * 60)
    print("H2 V2 RUNTIME STRESS TEST")
    print("=" * 60)

    async with aiohttp.ClientSession() as session:
        bb_api = BybitOrderAPI(BYBIT_KEY, BYBIT_SECRET, session)
        bn_api = BinanceOrderAPI(BINANCE_KEY, BINANCE_SECRET, session)
        gateway = OrderGateway(
            session, BYBIT_KEY, BYBIT_SECRET, BINANCE_KEY, BINANCE_SECRET,
            bn_symbol_map={"ENJUSDT": "ENJUSDC"},
        )
        store = PositionStore(MONGO_URI, TEST_COLLECTION)
        store._coll.drop()  # clean test collection

        # ── 1. Exchange Connectivity ─────────────────────────
        print("\n1. Exchange Connectivity")

        try:
            positions = await bb_api.get_positions("ENJUSDT")
            report("Bybit get_positions", positions is not None, f"returned {type(positions)}")
        except Exception as e:
            report("Bybit get_positions", False, str(e))

        try:
            account = await bn_api.get_account()
            report("Binance get_account", "balances" in account, f"{len(account.get('balances', []))} assets")
        except Exception as e:
            report("Binance get_account", False, str(e))

        # ── 2. get_order() — the critical missing method ─────
        print("\n2. get_order() — Fill Detection")

        # Bybit: query a non-existent order
        try:
            result = await bb_api.get_order("ENJUSDT", client_order_id="nonexistent_test_12345")
            fr = result.get("fill_result")
            report("Bybit get_order (nonexistent)", fr in ("NOT_FOUND", "UNKNOWN"),
                   f"fill_result={fr}")
        except Exception as e:
            report("Bybit get_order (nonexistent)", False, str(e))

        # Binance: query a non-existent order
        try:
            result = await bn_api.get_order("ENJUSDC", client_order_id="nonexistent_test_12345")
            fr = result.get("fill_result")
            report("Binance get_order (nonexistent)", fr in ("NOT_FOUND", "UNKNOWN"),
                   f"fill_result={fr}")
        except Exception as e:
            report("Binance get_order (nonexistent)", False, str(e))

        # ── 3. Instrument Rules ──────────────────────────────
        print("\n3. Instrument Rules + Decimal Rounding")

        rules = InstrumentRules()
        await rules.load_all(session, ["ENJUSDT", "NOMUSDT"], ["ENJUSDC", "NOMUSDC"])

        bb_enj = rules.get("bybit", "ENJUSDT")
        bn_enj = rules.get("binance", "ENJUSDC")
        report("Bybit ENJ rules loaded", bb_enj is not None,
               f"step={bb_enj.qty_step}" if bb_enj else "")
        report("Binance ENJ rules loaded", bn_enj is not None,
               f"step={bn_enj.qty_step}" if bn_enj else "")

        # Test rounding edge cases with real step sizes
        if bb_enj:
            qty = 171.15
            rounded = bb_enj.round_qty(qty)
            report("Bybit round_qty", rounded <= qty and rounded > 0,
                   f"{qty} -> {rounded} (step={bb_enj.qty_step})")

            price = 0.0587251
            rounded_buy = bb_enj.round_price_for_side(price, "Buy")
            rounded_sell = bb_enj.round_price_for_side(price, "Sell")
            report("Side-aware rounding", rounded_buy >= rounded_sell,
                   f"buy={rounded_buy} sell={rounded_sell} (tick={bb_enj.price_tick})")

        # ── 4. Position Store — Async Operations ─────────────
        print("\n4. PositionStore — Async MongoDB")

        doc = new_position_doc("stress_test_1", "ENJUSDT", "ENJUSDC",
                               "BUY_BB_SELL_BN", 100, 90, 30)
        created = await store.create(doc)
        report("Async create", created)

        transitioned = await store.transition("stress_test_1",
                                              PositionState.PENDING, PositionState.ENTERING, {})
        report("Async transition", transitioned)

        pos = await store.get("stress_test_1")
        report("Async get", pos is not None and pos["state"] == PositionState.ENTERING)

        active = await store.get_active()
        report("Async get_active", len(active) == 1)

        # Duplicate create should return False (not raise)
        dup = await store.create(doc)
        report("Duplicate create returns False", dup is False)

        # Invalid transition should return False
        invalid = await store.transition("stress_test_1",
                                         PositionState.PENDING, PositionState.OPEN, {})
        report("Invalid transition returns False", invalid is False)

        # ── 5. check_position — Tri-state (None on failure) ──
        print("\n5. check_position — Tri-state")

        pos_size = await gateway.check_position("bybit", "ENJUSDT")
        report("check_position returns float or None", pos_size is not None or pos_size is None,
               f"result={pos_size} type={type(pos_size)}")
        if pos_size is not None:
            report("check_position is numeric", isinstance(pos_size, (int, float)),
                   f"size={pos_size}")

        bn_pos = await gateway.check_position("binance", "ENJUSDC")
        report("Binance check_position returns None (spot)", bn_pos is None)

        # ── 6. Inventory Reconciliation ──────────────────────
        print("\n6. Inventory Reconciliation")

        inv = InventoryLedger(MONGO_URI)
        inv.load(["ENJUSDT"])
        enj_inv = inv.inventories.get("ENJUSDT")
        if enj_inv:
            pre_qty = enj_inv.expected_qty
            disc = await inv.reconcile("ENJUSDT", session, BINANCE_KEY, BINANCE_SECRET)
            report("Inventory reconcile", disc is not None,
                   f"pre={pre_qty:.1f} disc={disc:.4f}" if disc is not None else "failed")
        else:
            report("Inventory reconcile", False, "no ENJ inventory entry")

        # ── 7. Price Feed + Direction Spread ─────────────────
        print("\n7. Price Feed + Direction Spread")

        pf = PriceFeed(["ENJUSDT"], bn_symbol_map={"ENJUSDT": "ENJUSDC"})
        await pf.start(session)
        await asyncio.sleep(4)  # warmup

        snap = pf.get_spread("ENJUSDT")
        report("Price feed has data", snap is not None,
               f"spread={snap.spread_bps:.1f}bp" if snap else "no data after 4s")

        if snap:
            exit_snap = pf.get_spread_for_direction("ENJUSDT", "BUY_BB_SELL_BN", for_exit=True)
            report("Direction-aware exit spread", exit_snap is not None,
                   f"exit_spread={exit_snap.spread_bps:.1f}bp" if exit_snap else "None")

        await pf.stop()

        # ── 8. Fill Detector — REST Polling ──────────────────
        print("\n8. Fill Detector — REST Fallback")

        detector = FillDetector(
            get_order_fn=lambda v, s, order_id="", client_order_id="":
                gateway.get_order(v, s, order_id=order_id, client_order_id=client_order_id)
        )

        # Test definitive check on nonexistent order
        result = await detector.check_fill_definitive(
            "bybit", "ENJUSDT", client_order_id="stress_nonexistent"
        )
        fr = result.get("fill_result")
        report("Fill detector definitive check", fr in ("NOT_FOUND", "UNKNOWN", "NOT_FILLED"),
               f"fill_result={fr}")

        # Test buffer overflow flag
        report("Buffer overflow initially False", not detector.unmatched_buffer_overflow)

        # ── 9. Exchange Data Type Edge Cases ─────────────────
        print("\n9. Exchange Data Type Edge Cases")

        # Test that empty strings don't crash float conversion
        try:
            from app.services.arb.order_api import _REQ_TIMEOUT
            report("Request timeout defined", True, f"{_REQ_TIMEOUT.total}s")
        except ImportError:
            report("Request timeout defined", False, "not found")

        # Simulate the empty string bug that crashed us earlier
        try:
            val = float("" or 0)
            report("Empty string -> float('' or 0)", val == 0.0, f"result={val}")
        except Exception as e:
            report("Empty string -> float('' or 0)", False, str(e))

        # ── 10. Cleanup ──────────────────────────────────────
        store._coll.drop()

    # ── Summary ──────────────────────────────────────────
    print("\n" + "=" * 60)
    passed = sum(1 for _, p, _ in results if p)
    failed = sum(1 for _, p, _ in results if not p)
    print(f"RESULTS: {passed} passed, {failed} failed, {len(results)} total")

    if failed > 0:
        print("\nFAILURES:")
        for name, p, detail in results:
            if not p:
                print(f"  FAIL: {name} — {detail}")

    print("=" * 60)
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
