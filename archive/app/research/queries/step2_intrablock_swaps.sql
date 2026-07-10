-- Step 2b: Intra-block swap ordering for graduated tokens
-- Framework: Section A1/A2 (exact reserve state reconstruction)
-- For each graduated pool, get ALL swaps in first 300 blocks ordered by tx_index
-- This enables exact replay of reserve transitions per-swap

-- NOTE: Run per-batch (e.g., 100 graduations at a time) due to query size
-- Replace {{GRAD_DATE_START}} and {{GRAD_DATE_END}} with batch dates

WITH batch_grads AS (
    SELECT
        account_mint as token_mint,
        account_pool as pool_address,
        call_block_slot as grad_slot,
        call_block_time as grad_time
    FROM pumpdotfun_solana.pump_call_migrate
    WHERE call_block_date >= DATE '{{GRAD_DATE_START}}'
      AND call_block_date < DATE '{{GRAD_DATE_END}}'
),
-- Buy events with intra-block ordering
buy_events AS (
    SELECT
        g.token_mint,
        g.pool_address,
        g.grad_slot,
        'buy' as swap_type,
        b.evt_block_slot as block_slot,
        b.evt_tx_index as tx_index,
        b.evt_block_time as block_time,
        b.evt_tx_signer as signer,
        CAST(b.quote_amount_in AS double) / 1e9 as sol_in,
        CAST(b.base_amount_out AS double) as tokens_out,
        CAST(b.pool_quote_token_reserves AS double) / 1e9 as post_sol_reserves,
        CAST(b.pool_base_token_reserves AS double) as post_token_reserves,
        CAST(b.lp_fee AS double) / 1e9 as lp_fee_sol,
        CAST(b.protocol_fee AS double) / 1e9 as protocol_fee_sol,
        CAST(COALESCE(b.coin_creator_fee, 0) AS double) / 1e9 as creator_fee_sol,
        b.evt_block_slot - g.grad_slot as blocks_after_grad
    FROM batch_grads g
    JOIN pumpdotfun_solana.pump_amm_evt_buyevent b ON g.pool_address = b.pool
    WHERE b.evt_block_slot >= g.grad_slot
      AND b.evt_block_slot <= g.grad_slot + 300
),
-- Sell events with intra-block ordering
sell_events AS (
    SELECT
        g.token_mint,
        g.pool_address,
        g.grad_slot,
        'sell' as swap_type,
        s.evt_block_slot as block_slot,
        s.evt_tx_index as tx_index,
        s.evt_block_time as block_time,
        s.evt_tx_signer as signer,
        CAST(s.quote_amount_out AS double) / 1e9 as sol_out,
        CAST(s.base_amount_in AS double) as tokens_in,
        CAST(s.pool_quote_token_reserves AS double) / 1e9 as post_sol_reserves,
        CAST(s.pool_base_token_reserves AS double) as post_token_reserves,
        CAST(s.lp_fee AS double) / 1e9 as lp_fee_sol,
        CAST(s.protocol_fee AS double) / 1e9 as protocol_fee_sol,
        CAST(COALESCE(s.coin_creator_fee, 0) AS double) / 1e9 as creator_fee_sol,
        s.evt_block_slot - g.grad_slot as blocks_after_grad
    FROM batch_grads g
    JOIN pumpdotfun_solana.pump_amm_evt_sellevent s ON g.pool_address = s.pool
    WHERE s.evt_block_slot >= g.grad_slot
      AND s.evt_block_slot <= g.grad_slot + 300
)
-- Combine and order by block + tx_index for exact replay
SELECT * FROM buy_events
UNION ALL
SELECT
    token_mint, pool_address, grad_slot, swap_type,
    block_slot, tx_index, block_time, signer,
    sol_out as sol_in,  -- normalize column names
    tokens_in as tokens_out,
    post_sol_reserves, post_token_reserves,
    lp_fee_sol, protocol_fee_sol, creator_fee_sol,
    blocks_after_grad
FROM sell_events
ORDER BY token_mint, block_slot, tx_index
