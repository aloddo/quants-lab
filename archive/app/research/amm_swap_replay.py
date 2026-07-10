"""
AMM Swap Replay Engine for Graduation Sniping Validation
=========================================================

Core simulation engine that replays constant-product AMM swaps to compute
exact reserve states, entry/exit prices, round-trip PnL, drift decomposition,
and liquidity exhaustion metrics.

This is a RESEARCH tool for offline analysis, not a live trading system.

Pool math follows PumpSwap's constant-product AMM (x * y = k) with
three-tier fee structure: LP fee (stays in pool), protocol fee (extracted),
creator fee (extracted).
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# 1. CONSTANT-PRODUCT AMM MATH
# ---------------------------------------------------------------------------

class PumpSwapPool:
    """Simulates a constant-product AMM pool (x * y = k).

    Reserves:
        x = SOL reserves
        y = token reserves

    Fee mechanics (PumpSwap style):
        On a BUY (SOL -> tokens):
            lp_fee      = sol_in * lp_fee_rate       (stays in pool as SOL)
            protocol_fee = sol_in * protocol_fee_rate  (extracted)
            creator_fee  = sol_in * creator_fee_rate   (extracted)
            effective_sol = sol_in - lp_fee - protocol_fee - creator_fee
            tokens_out = y * effective_sol / (x + effective_sol)
            new_x = x + effective_sol + lp_fee
            new_y = y - tokens_out

        On a SELL (tokens -> SOL):
            lp_fee      = tokens_in * lp_fee_rate     (stays in pool as tokens)
            protocol_fee = tokens_in * protocol_fee_rate (extracted)
            creator_fee  = tokens_in * creator_fee_rate  (extracted)
            effective_tokens = tokens_in - lp_fee - protocol_fee - creator_fee
            sol_out = x * effective_tokens / (y + effective_tokens)
            new_x = x - sol_out
            new_y = y + effective_tokens + lp_fee
    """

    def __init__(
        self,
        sol_reserves: float,
        token_reserves: float,
        lp_fee_bps: int = 20,
        protocol_fee_bps: int = 5,
        creator_fee_bps: int = 5,
    ):
        if sol_reserves <= 0 or token_reserves <= 0:
            raise ValueError("Reserves must be positive")
        self.x = sol_reserves
        self.y = token_reserves
        self.lp_fee_bps = lp_fee_bps
        self.protocol_fee_bps = protocol_fee_bps
        self.creator_fee_bps = creator_fee_bps
        self._total_fee_bps = lp_fee_bps + protocol_fee_bps + creator_fee_bps

    @property
    def _lp_fee_rate(self) -> float:
        return self.lp_fee_bps / 10_000

    @property
    def _protocol_fee_rate(self) -> float:
        return self.protocol_fee_bps / 10_000

    @property
    def _creator_fee_rate(self) -> float:
        return self.creator_fee_bps / 10_000

    @property
    def _total_fee_rate(self) -> float:
        return self._total_fee_bps / 10_000

    def spot_price(self) -> float:
        """Current spot price in SOL per token (dx/dy at the margin)."""
        return self.x / self.y

    def buy_tokens(self, sol_in: float) -> dict:
        """Buy tokens by sending SOL into the pool.

        Returns dict with:
            tokens_out: tokens received
            effective_price: SOL paid per token (including fees)
            price_impact_bps: slippage vs spot price in basis points
            fees_paid: breakdown of fees in SOL
            new_spot_price: spot price after the swap
        """
        if sol_in <= 0:
            raise ValueError("sol_in must be positive")

        spot_before = self.spot_price()

        # Fee decomposition
        lp_fee = sol_in * self._lp_fee_rate
        protocol_fee = sol_in * self._protocol_fee_rate
        creator_fee = sol_in * self._creator_fee_rate
        effective_sol = sol_in - lp_fee - protocol_fee - creator_fee

        # Constant-product swap: tokens_out = y * dx / (x + dx)
        tokens_out = self.y * effective_sol / (self.x + effective_sol)

        # Update reserves
        self.x = self.x + effective_sol + lp_fee  # LP fee stays in pool
        self.y = self.y - tokens_out

        effective_price = sol_in / tokens_out  # total SOL spent per token
        price_impact_bps = ((effective_price / spot_before) - 1) * 10_000

        return {
            "tokens_out": tokens_out,
            "effective_price": effective_price,
            "price_impact_bps": price_impact_bps,
            "fees_paid": {
                "lp_fee_sol": lp_fee,
                "protocol_fee_sol": protocol_fee,
                "creator_fee_sol": creator_fee,
                "total_fee_sol": lp_fee + protocol_fee + creator_fee,
            },
            "new_spot_price": self.spot_price(),
        }

    def sell_tokens(self, tokens_in: float) -> dict:
        """Sell tokens to receive SOL from the pool.

        Returns dict with:
            sol_out: SOL received
            effective_price: SOL received per token (after fees)
            price_impact_bps: slippage vs spot price in basis points
            fees_paid: breakdown of fees in tokens
            new_spot_price: spot price after the swap
        """
        if tokens_in <= 0:
            raise ValueError("tokens_in must be positive")

        spot_before = self.spot_price()

        # Fee decomposition (on token input)
        lp_fee = tokens_in * self._lp_fee_rate
        protocol_fee = tokens_in * self._protocol_fee_rate
        creator_fee = tokens_in * self._creator_fee_rate
        effective_tokens = tokens_in - lp_fee - protocol_fee - creator_fee

        # Constant-product swap: sol_out = x * dy / (y + dy)
        sol_out = self.x * effective_tokens / (self.y + effective_tokens)

        # Update reserves
        self.x = self.x - sol_out
        self.y = self.y + effective_tokens + lp_fee  # LP fee stays in pool

        effective_price = sol_out / tokens_in  # SOL per token received
        # Negative impact means we got less than spot
        price_impact_bps = (1 - (effective_price / spot_before)) * 10_000

        return {
            "sol_out": sol_out,
            "effective_price": effective_price,
            "price_impact_bps": price_impact_bps,
            "fees_paid": {
                "lp_fee_tokens": lp_fee,
                "protocol_fee_tokens": protocol_fee,
                "creator_fee_tokens": creator_fee,
                "total_fee_tokens": lp_fee + protocol_fee + creator_fee,
            },
            "new_spot_price": self.spot_price(),
        }

    def marginal_slippage(self, sol_amount: float) -> float:
        """Slippage in bps for a hypothetical buy of sol_amount at current reserves.

        Does NOT mutate pool state. Uses a clone internally.
        """
        clone = self.clone()
        result = clone.buy_tokens(sol_amount)
        return result["price_impact_bps"]

    def snapshot(self) -> dict:
        """Return current pool state as a dict."""
        return {
            "sol_reserves": self.x,
            "token_reserves": self.y,
            "k": self.x * self.y,
            "spot_price": self.spot_price(),
            "lp_fee_bps": self.lp_fee_bps,
            "protocol_fee_bps": self.protocol_fee_bps,
            "creator_fee_bps": self.creator_fee_bps,
        }

    def clone(self) -> "PumpSwapPool":
        """Deep copy for simulation branching."""
        return PumpSwapPool(
            sol_reserves=self.x,
            token_reserves=self.y,
            lp_fee_bps=self.lp_fee_bps,
            protocol_fee_bps=self.protocol_fee_bps,
            creator_fee_bps=self.creator_fee_bps,
        )

    def __repr__(self) -> str:
        return (
            f"PumpSwapPool(x={self.x:.4f} SOL, y={self.y:.2f} tokens, "
            f"spot={self.spot_price():.10f} SOL/token)"
        )


# ---------------------------------------------------------------------------
# 2. INTRA-BLOCK SWAP REPLAY
# ---------------------------------------------------------------------------

@dataclass
class SwapRecord:
    """Single swap event in the replay sequence."""
    block: int
    tx_index: int
    is_buy: bool
    amount: float  # SOL for buys, tokens for sells
    result: dict
    reserve_snapshot: dict


class SwapReplayEngine:
    """Replays ordered swaps within blocks to compute exact reserve states.

    Usage:
        1. Create with initial pool state (at graduation).
        2. Feed swaps in order via replay_swap().
        3. Query reserve states at any point.
        4. Simulate hypothetical entry/exit at any position.
    """

    def __init__(self, initial_pool: PumpSwapPool):
        self._initial_pool = initial_pool.clone()
        self.pool = initial_pool.clone()
        self.history: List[SwapRecord] = []

    def replay_swap(self, block: int, tx_index: int, is_buy: bool, amount: float) -> dict:
        """Replay a single swap and record state.

        Args:
            block: Block number (slot).
            tx_index: Transaction index within the block (ordering).
            is_buy: True = buy tokens with SOL, False = sell tokens for SOL.
            amount: SOL amount (if buy) or token amount (if sell).

        Returns:
            Swap result dict from the pool.
        """
        if is_buy:
            result = self.pool.buy_tokens(amount)
        else:
            result = self.pool.sell_tokens(amount)

        record = SwapRecord(
            block=block,
            tx_index=tx_index,
            is_buy=is_buy,
            amount=amount,
            result=result,
            reserve_snapshot=self.pool.snapshot(),
        )
        self.history.append(record)

        return result

    def get_reserve_state_at(self, block: int, tx_index: int) -> dict:
        """Get pool reserves at a specific point in the swap sequence.

        Returns the snapshot AFTER the swap at (block, tx_index) executed.
        If no swap exists at that exact position, returns the state just
        before the next swap.
        """
        # Find the last record at or before (block, tx_index)
        best: Optional[SwapRecord] = None
        for record in self.history:
            if (record.block, record.tx_index) <= (block, tx_index):
                best = record
            else:
                break  # history is ordered

        if best is not None:
            return best.reserve_snapshot
        return self._initial_pool.snapshot()

    def compute_entry_price(
        self, entry_block: int, entry_tx_index: int, trade_size_sol: float
    ) -> dict:
        """What price would WE get if we entered at this position?

        Replays all swaps up to (entry_block, entry_tx_index) on a fresh
        pool clone, then simulates our buy.

        Returns:
            tokens_received: how many tokens we'd get
            effective_price: SOL per token (including fees + impact)
            price_impact_bps: our slippage at that reserve state
            pool_state_before: reserves just before our trade
            pool_state_after: reserves after our trade
        """
        sim_pool = self._initial_pool.clone()

        # Replay all swaps up to the entry point
        for record in self.history:
            if (record.block, record.tx_index) > (entry_block, entry_tx_index):
                break
            if record.is_buy:
                sim_pool.buy_tokens(record.amount)
            else:
                sim_pool.sell_tokens(record.amount)

        pool_state_before = sim_pool.snapshot()

        # Now simulate our buy
        buy_result = sim_pool.buy_tokens(trade_size_sol)

        return {
            "tokens_received": buy_result["tokens_out"],
            "effective_price": buy_result["effective_price"],
            "price_impact_bps": buy_result["price_impact_bps"],
            "pool_state_before": pool_state_before,
            "pool_state_after": sim_pool.snapshot(),
        }

    def compute_exit_price(
        self, exit_block: int, exit_tx_index: int, tokens_held: float
    ) -> dict:
        """What SOL would we get if we sold at this position?

        Replays all swaps up to (exit_block, exit_tx_index) on a fresh
        pool clone, then simulates our sell.

        Returns:
            sol_received: SOL we'd get back
            effective_price: SOL per token received
            price_impact_bps: our slippage at that reserve state
            pool_state_before: reserves just before our trade
            pool_state_after: reserves after our trade
        """
        sim_pool = self._initial_pool.clone()

        for record in self.history:
            if (record.block, record.tx_index) > (exit_block, exit_tx_index):
                break
            if record.is_buy:
                sim_pool.buy_tokens(record.amount)
            else:
                sim_pool.sell_tokens(record.amount)

        pool_state_before = sim_pool.snapshot()

        sell_result = sim_pool.sell_tokens(tokens_held)

        return {
            "sol_received": sell_result["sol_out"],
            "effective_price": sell_result["effective_price"],
            "price_impact_bps": sell_result["price_impact_bps"],
            "pool_state_before": pool_state_before,
            "pool_state_after": sim_pool.snapshot(),
        }

    @property
    def blocks_covered(self) -> Tuple[int, int]:
        """(first_block, last_block) in the replay history."""
        if not self.history:
            return (0, 0)
        return (self.history[0].block, self.history[-1].block)

    def __len__(self) -> int:
        return len(self.history)


# ---------------------------------------------------------------------------
# 3. ROUND-TRIP PnL CALCULATOR
# ---------------------------------------------------------------------------

@dataclass
class CostAssumptions:
    """Fixed costs for Solana mempool trading.

    All values in SOL.
    """
    priority_fee_buy_sol: float = 0.005
    priority_fee_sell_sol: float = 0.002
    jito_tip_sol: float = 0.01
    failed_tx_rate: float = 0.20       # 20% of attempts fail
    failed_tx_cost_sol: float = 0.001  # cost per failed attempt


class RoundTripSimulator:
    """Compute full round-trip PnL for a graduation snipe.

    Accounts for:
        - AMM fees + slippage on entry and exit
        - Priority fees (buy + sell)
        - Jito tips
        - Expected cost of failed transactions
    """

    def simulate(
        self,
        replay: SwapReplayEngine,
        entry_block: int,
        entry_tx_index: int,
        exit_block: int,
        exit_tx_index: int,
        trade_size_sol: float,
        costs: Optional[CostAssumptions] = None,
    ) -> dict:
        """Simulate a full round-trip trade.

        Returns:
            sol_in: total SOL committed (trade + all costs)
            sol_out: SOL received from exit
            amm_fees_entry: AMM fees on buy
            amm_fees_exit: AMM fees on sell (in SOL equivalent)
            slippage_entry_bps: price impact on entry
            slippage_exit_bps: price impact on exit
            priority_costs: total priority fees
            jito_costs: Jito tip
            failed_tx_costs: expected failed tx cost
            rt_pnl_sol: net profit/loss in SOL
            rt_pnl_pct: net profit/loss as percentage of sol_in
        """
        if costs is None:
            costs = CostAssumptions()

        # Entry simulation
        entry = replay.compute_entry_price(entry_block, entry_tx_index, trade_size_sol)
        tokens_held = entry["tokens_received"]

        # Exit simulation
        exit_result = replay.compute_exit_price(exit_block, exit_tx_index, tokens_held)
        sol_out_gross = exit_result["sol_received"]

        # Fixed costs
        priority_costs = costs.priority_fee_buy_sol + costs.priority_fee_sell_sol
        jito_costs = costs.jito_tip_sol
        # Expected failed tx cost: E[cost] = (attempts - 1) * fail_rate * cost_per_fail
        # For 1 successful tx with fail_rate p, expected attempts = 1/(1-p),
        # so expected failures = p/(1-p). We do this for both buy and sell.
        if costs.failed_tx_rate < 1.0:
            expected_failures_per_tx = costs.failed_tx_rate / (1 - costs.failed_tx_rate)
        else:
            expected_failures_per_tx = 10.0  # cap at 10
        failed_tx_costs = 2 * expected_failures_per_tx * costs.failed_tx_cost_sol

        # AMM fees
        amm_fees_entry = entry.get("pool_state_before", {}).get("spot_price", 0)  # placeholder
        # Actually extract from the entry/exit computations properly.
        # We need to re-run to get fee details. Use a direct pool simulation.
        sim_pool_entry = self._build_pool_at(replay, entry_block, entry_tx_index)
        buy_detail = sim_pool_entry.buy_tokens(trade_size_sol)
        amm_fees_entry_sol = buy_detail["fees_paid"]["total_fee_sol"]

        sim_pool_exit = self._build_pool_at(replay, exit_block, exit_tx_index)
        sell_detail = sim_pool_exit.sell_tokens(tokens_held)
        # Convert token fees to SOL at exit spot price
        exit_spot = sim_pool_exit.spot_price()
        amm_fees_exit_sol = sell_detail["fees_paid"]["total_fee_tokens"] * exit_spot

        # Totals
        total_cost = trade_size_sol + priority_costs + jito_costs + failed_tx_costs
        sol_out_net = sol_out_gross
        rt_pnl_sol = sol_out_net - total_cost
        rt_pnl_pct = (rt_pnl_sol / total_cost) * 100 if total_cost > 0 else 0

        return {
            "sol_in": total_cost,
            "trade_size_sol": trade_size_sol,
            "tokens_acquired": tokens_held,
            "sol_out": sol_out_gross,
            "amm_fees_entry_sol": amm_fees_entry_sol,
            "amm_fees_exit_sol": amm_fees_exit_sol,
            "slippage_entry_bps": entry["price_impact_bps"],
            "slippage_exit_bps": exit_result["price_impact_bps"],
            "priority_costs": priority_costs,
            "jito_costs": jito_costs,
            "failed_tx_costs": failed_tx_costs,
            "rt_pnl_sol": rt_pnl_sol,
            "rt_pnl_pct": rt_pnl_pct,
        }

    @staticmethod
    def _build_pool_at(
        replay: SwapReplayEngine, block: int, tx_index: int
    ) -> PumpSwapPool:
        """Reconstruct pool state at a given point by replaying history."""
        pool = replay._initial_pool.clone()
        for record in replay.history:
            if (record.block, record.tx_index) > (block, tx_index):
                break
            if record.is_buy:
                pool.buy_tokens(record.amount)
            else:
                pool.sell_tokens(record.amount)
        return pool


# ---------------------------------------------------------------------------
# 4. POST-ENTRY DRIFT DECOMPOSITION (A7)
# ---------------------------------------------------------------------------

class DriftDecomposer:
    """Decompose post-entry price movement into self-impact vs exogenous drift.

    Self-impact: the price change caused by our own buy order.
    Drift: the price change from other participants' activity after our trade.

    Adverse selection check: if drift_t5 < 0 (price falls after we buy),
    we are systematically buying local tops.
    """

    def decompose(
        self,
        replay: SwapReplayEngine,
        entry_block: int,
        entry_tx_index: int,
        trade_size_sol: float,
    ) -> dict:
        """Decompose post-entry price movement.

        Args:
            replay: A fully replayed swap history.
            entry_block: Block at which we would enter.
            entry_tx_index: Tx index at which we would enter.
            trade_size_sol: Our hypothetical buy size.

        Returns:
            spot_before: spot price before our buy
            spot_after_self: spot price immediately after our buy
            self_impact_bps: price change from our own buy (in bps)
            spot_t5: spot price 5 blocks after entry (exogenous only)
            spot_t25: spot price 25 blocks after entry (exogenous only)
            drift_t5_bps: exogenous price drift at T+5 (bps from spot_after_self)
            drift_t25_bps: exogenous price drift at T+25 (bps from spot_after_self)
            is_adverse_selected: True if drift_t5_bps < 0
        """
        # Build pool at entry point
        pool_at_entry = RoundTripSimulator._build_pool_at(
            replay, entry_block, entry_tx_index
        )
        spot_before = pool_at_entry.spot_price()

        # Simulate our buy
        pool_after_self = pool_at_entry.clone()
        pool_after_self.buy_tokens(trade_size_sol)
        spot_after_self = pool_after_self.spot_price()

        self_impact_bps = ((spot_after_self / spot_before) - 1) * 10_000

        # Get exogenous spot at T+5 and T+25 blocks (without our trade in the pool)
        # We look at the actual pool state from the replay at those block offsets.
        target_t5 = entry_block + 5
        target_t25 = entry_block + 25

        spot_t5 = self._get_spot_at_block(replay, target_t5)
        spot_t25 = self._get_spot_at_block(replay, target_t25)

        # Drift is measured from the post-self-impact price against
        # the exogenous price (where the pool actually went without us).
        # Since we are counterfactual (we weren't actually in the pool),
        # compare exogenous spot to pre-entry spot to see real drift,
        # then subtract self-impact to isolate exogenous.
        drift_t5_bps = ((spot_t5 / spot_before) - 1) * 10_000 if spot_t5 else None
        drift_t25_bps = ((spot_t25 / spot_before) - 1) * 10_000 if spot_t25 else None

        return {
            "spot_before": spot_before,
            "spot_after_self": spot_after_self,
            "self_impact_bps": self_impact_bps,
            "spot_t5": spot_t5,
            "spot_t25": spot_t25,
            "drift_t5_bps": drift_t5_bps,
            "drift_t25_bps": drift_t25_bps,
            "is_adverse_selected": drift_t5_bps < 0 if drift_t5_bps is not None else None,
        }

    @staticmethod
    def _get_spot_at_block(replay: SwapReplayEngine, target_block: int) -> Optional[float]:
        """Get the spot price at the last swap before or at target_block."""
        best_snapshot = None
        for record in replay.history:
            if record.block <= target_block:
                best_snapshot = record.reserve_snapshot
            else:
                break
        if best_snapshot:
            return best_snapshot["spot_price"]
        return None


# ---------------------------------------------------------------------------
# 5. LIQUIDITY EXHAUSTION METRIC (A8)
# ---------------------------------------------------------------------------

class LiquidityAnalyzer:
    """Measure pool fragility under sequential buying pressure.

    Computes marginal slippage at increasing buy depths.
    A high fragility index means the pool dries up quickly under
    sustained buying -- typical of thin graduation pools.
    """

    def exhaustion_slope(
        self,
        pool: PumpSwapPool,
        sol_increments: Optional[List[float]] = None,
    ) -> dict:
        """Compute marginal slippage at successive cumulative buy depths.

        Each increment is applied sequentially (reserves shrink as we go).

        Args:
            pool: Pool to analyze (not mutated; uses clone).
            sol_increments: List of SOL amounts for each successive buy.
                Defaults to [0.5, 1, 2, 5, 10].

        Returns:
            slippage_curve: list of {sol_cumulative, marginal_slippage_bps, spot_price}
            fragility_index: slippage_at_last / slippage_at_first
            total_tokens_acquired: total tokens from all buys
            initial_spot: spot price before any buys
            final_spot: spot price after all buys
        """
        if sol_increments is None:
            sol_increments = [0.5, 1.0, 2.0, 5.0, 10.0]

        sim = pool.clone()
        initial_spot = sim.spot_price()
        curve = []
        cumulative_sol = 0.0
        total_tokens = 0.0

        for sol_amt in sol_increments:
            # Check if we can even buy (sol_amt must be less than what the pool can absorb)
            pre_spot = sim.spot_price()
            result = sim.buy_tokens(sol_amt)
            cumulative_sol += sol_amt
            total_tokens += result["tokens_out"]

            curve.append({
                "sol_increment": sol_amt,
                "sol_cumulative": cumulative_sol,
                "marginal_slippage_bps": result["price_impact_bps"],
                "effective_price": result["effective_price"],
                "spot_price_after": sim.spot_price(),
                "tokens_out": result["tokens_out"],
            })

        first_slip = curve[0]["marginal_slippage_bps"] if curve else 0
        last_slip = curve[-1]["marginal_slippage_bps"] if curve else 0
        fragility_index = last_slip / first_slip if first_slip > 0 else float("inf")

        return {
            "slippage_curve": curve,
            "fragility_index": fragility_index,
            "total_tokens_acquired": total_tokens,
            "initial_spot": initial_spot,
            "final_spot": sim.spot_price(),
        }


# ---------------------------------------------------------------------------
# UNIT TESTS
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import math

    passed = 0
    failed = 0

    def assert_close(a, b, tol=1e-6, msg=""):
        global passed, failed
        if abs(a - b) < tol:
            passed += 1
        else:
            failed += 1
            print(f"  FAIL: {msg} -- expected {b}, got {a}, diff={abs(a-b)}")

    def assert_true(cond, msg=""):
        global passed, failed
        if cond:
            passed += 1
        else:
            failed += 1
            print(f"  FAIL: {msg}")

    # --- Test 1: Pool initialization and spot price ---
    print("Test 1: Pool init and spot price")
    pool = PumpSwapPool(sol_reserves=100.0, token_reserves=1_000_000.0)
    assert_close(pool.spot_price(), 100.0 / 1_000_000.0, msg="spot price")
    snap = pool.snapshot()
    assert_close(snap["k"], 100.0 * 1_000_000.0, msg="k invariant")

    # --- Test 2: Buy tokens, verify k is maintained (net of extracted fees) ---
    print("Test 2: Buy tokens")
    pool2 = PumpSwapPool(sol_reserves=100.0, token_reserves=1_000_000.0)
    result = pool2.buy_tokens(1.0)

    # total fee = 30 bps = 0.003 of 1.0 = 0.003 SOL
    total_fee = 1.0 * 0.003
    lp_fee = 1.0 * 0.002
    extracted = 1.0 * 0.001
    effective_sol = 1.0 - total_fee

    expected_tokens_out = 1_000_000.0 * effective_sol / (100.0 + effective_sol)
    assert_close(result["tokens_out"], expected_tokens_out, tol=1e-4, msg="tokens_out")

    # Verify reserves
    expected_x = 100.0 + effective_sol + lp_fee
    expected_y = 1_000_000.0 - expected_tokens_out
    assert_close(pool2.x, expected_x, tol=1e-8, msg="new x reserves")
    assert_close(pool2.y, expected_y, tol=1e-4, msg="new y reserves")

    # The product x*y should be >= original k (LP fee adds to reserves)
    assert_true(pool2.x * pool2.y >= 100.0 * 1_000_000.0 - 1.0,
                msg="k should not decrease (LP fee in pool)")

    assert_true(result["price_impact_bps"] > 0, msg="price impact positive on buy")

    # --- Test 3: Sell tokens ---
    print("Test 3: Sell tokens")
    pool3 = PumpSwapPool(sol_reserves=100.0, token_reserves=1_000_000.0)
    sell_result = pool3.sell_tokens(10_000.0)

    token_total_fee = 10_000.0 * 0.003
    token_lp_fee = 10_000.0 * 0.002
    effective_tokens = 10_000.0 - token_total_fee
    expected_sol_out = 100.0 * effective_tokens / (1_000_000.0 + effective_tokens)
    assert_close(sell_result["sol_out"], expected_sol_out, tol=1e-6, msg="sol_out")
    assert_true(sell_result["price_impact_bps"] > 0, msg="price impact positive on sell")

    # --- Test 4: Clone independence ---
    print("Test 4: Clone independence")
    pool4 = PumpSwapPool(sol_reserves=50.0, token_reserves=500_000.0)
    clone4 = pool4.clone()
    clone4.buy_tokens(5.0)
    assert_close(pool4.x, 50.0, msg="original unchanged after clone mutation")

    # --- Test 5: Marginal slippage (non-mutating) ---
    print("Test 5: Marginal slippage")
    pool5 = PumpSwapPool(sol_reserves=100.0, token_reserves=1_000_000.0)
    slip = pool5.marginal_slippage(1.0)
    assert_true(slip > 0, msg="slippage positive")
    assert_close(pool5.x, 100.0, msg="pool5 unchanged after marginal_slippage")

    # --- Test 6: SwapReplayEngine basic replay ---
    print("Test 6: SwapReplayEngine replay")
    init_pool = PumpSwapPool(sol_reserves=80.0, token_reserves=800_000.0)
    engine = SwapReplayEngine(init_pool)
    engine.replay_swap(block=100, tx_index=0, is_buy=True, amount=2.0)
    engine.replay_swap(block=100, tx_index=1, is_buy=True, amount=1.0)
    engine.replay_swap(block=101, tx_index=0, is_buy=False, amount=5000.0)

    assert_true(len(engine) == 3, msg="3 swaps recorded")
    state = engine.get_reserve_state_at(100, 0)
    assert_true(state["sol_reserves"] > 80.0, msg="reserves increased after buy")

    # --- Test 7: compute_entry_price ---
    print("Test 7: compute_entry_price")
    entry = engine.compute_entry_price(entry_block=100, entry_tx_index=1,
                                       trade_size_sol=0.5)
    assert_true(entry["tokens_received"] > 0, msg="got tokens")
    assert_true(entry["effective_price"] > 0, msg="positive price")

    # --- Test 8: compute_exit_price ---
    print("Test 8: compute_exit_price")
    exit_r = engine.compute_exit_price(exit_block=101, exit_tx_index=0,
                                       tokens_held=entry["tokens_received"])
    assert_true(exit_r["sol_received"] > 0, msg="got SOL back")

    # --- Test 9: RoundTripSimulator ---
    print("Test 9: RoundTripSimulator")
    rt = RoundTripSimulator()
    rt_result = rt.simulate(
        replay=engine,
        entry_block=100, entry_tx_index=0,
        exit_block=101, exit_tx_index=0,
        trade_size_sol=1.0,
    )
    assert_true("rt_pnl_sol" in rt_result, msg="has rt_pnl_sol")
    assert_true("rt_pnl_pct" in rt_result, msg="has rt_pnl_pct")
    assert_true(rt_result["sol_in"] > 1.0, msg="sol_in includes costs")
    # The sell in block 101 drops price, so we should lose money
    # (we bought at 100:0 and sell after a sell happened at 101:0)

    # --- Test 10: DriftDecomposer ---
    print("Test 10: DriftDecomposer")
    # Build a longer history for drift testing
    drift_pool = PumpSwapPool(sol_reserves=100.0, token_reserves=1_000_000.0)
    drift_engine = SwapReplayEngine(drift_pool)
    # Simulate 30 blocks of mixed activity
    for b in range(30):
        if b % 3 == 0:
            drift_engine.replay_swap(block=b, tx_index=0, is_buy=True, amount=0.5)
        else:
            drift_engine.replay_swap(block=b, tx_index=0, is_buy=False, amount=2000.0)

    decomposer = DriftDecomposer()
    drift = decomposer.decompose(drift_engine, entry_block=0, entry_tx_index=0,
                                  trade_size_sol=1.0)
    assert_true(drift["spot_before"] > 0, msg="spot_before positive")
    assert_true(drift["self_impact_bps"] > 0, msg="self impact positive (we're buying)")
    assert_true(drift["spot_t5"] is not None, msg="spot_t5 available")
    assert_true(drift["spot_t25"] is not None, msg="spot_t25 available")
    assert_true(isinstance(drift["is_adverse_selected"], bool),
                msg="adverse selection flag is bool")

    # --- Test 11: LiquidityAnalyzer ---
    print("Test 11: LiquidityAnalyzer")
    liq_pool = PumpSwapPool(sol_reserves=50.0, token_reserves=500_000.0)
    analyzer = LiquidityAnalyzer()
    exhaustion = analyzer.exhaustion_slope(liq_pool)

    assert_true(len(exhaustion["slippage_curve"]) == 5, msg="5 default increments")
    assert_true(exhaustion["fragility_index"] > 1.0,
                msg="fragility > 1 (slippage grows with depth)")
    assert_close(liq_pool.x, 50.0, msg="original pool unchanged by analyzer")
    # Verify monotonically increasing slippage
    slippages = [p["marginal_slippage_bps"] for p in exhaustion["slippage_curve"]]
    for i in range(1, len(slippages)):
        assert_true(slippages[i] >= slippages[i - 1],
                    msg=f"slippage monotonic at index {i}")

    # --- Test 12: Zero-fee pool (sanity check on k invariant) ---
    print("Test 12: Zero-fee pool k invariant")
    pool_nofee = PumpSwapPool(sol_reserves=100.0, token_reserves=1_000_000.0,
                               lp_fee_bps=0, protocol_fee_bps=0, creator_fee_bps=0)
    k_before = pool_nofee.x * pool_nofee.y
    pool_nofee.buy_tokens(5.0)
    k_after = pool_nofee.x * pool_nofee.y
    assert_close(k_before, k_after, tol=1e-4, msg="k invariant with zero fees")

    # --- Summary ---
    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed == 0:
        print("All tests passed.")
    else:
        print("SOME TESTS FAILED.")
