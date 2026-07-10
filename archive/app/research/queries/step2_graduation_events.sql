-- Step 2a: All PumpSwap-era graduation events with pool init params
-- Framework: Section B2, graduation events table
-- PumpSwap era only (post March 20, 2025)
-- Includes initial reserves from first AMM event for each pool

WITH graduations AS (
    SELECT
        account_mint as token_mint,
        account_pool as pool_address,
        call_tx_id as migrate_tx,
        call_block_slot as grad_slot,
        call_block_time as grad_time,
        call_block_date as grad_date
    FROM pumpdotfun_solana.pump_call_migrate
    WHERE call_block_date >= DATE '2025-03-20'
),
-- Get initial pool reserves from first buy event per pool
first_events AS (
    SELECT
        g.token_mint,
        g.pool_address,
        g.migrate_tx,
        g.grad_slot,
        g.grad_time,
        g.grad_date,
        b.pool_base_token_reserves as init_token_reserves,
        b.pool_quote_token_reserves as init_sol_reserves,
        b.coin_creator_fee_basis_points as creator_fee_bps,
        b.lp_fee_basis_points as lp_fee_bps,
        b.protocol_fee_basis_points as protocol_fee_bps,
        b.evt_block_slot as first_buy_slot,
        b.evt_block_time as first_buy_time,
        b.evt_tx_index as first_buy_tx_index,
        ROW_NUMBER() OVER (
            PARTITION BY g.token_mint
            ORDER BY b.evt_block_time, b.evt_tx_index
        ) as seq
    FROM graduations g
    JOIN pumpdotfun_solana.pump_amm_evt_buyevent b ON g.pool_address = b.pool
    WHERE b.evt_block_time >= g.grad_time
      AND b.evt_block_time <= g.grad_time + INTERVAL '5' MINUTE
)
SELECT
    token_mint,
    pool_address,
    migrate_tx,
    grad_slot,
    grad_time,
    grad_date,
    CAST(init_sol_reserves AS double) / 1e9 as init_sol,
    CAST(init_token_reserves AS double) as init_tokens,
    CAST(creator_fee_bps AS bigint) as creator_fee_bps,
    CAST(lp_fee_bps AS bigint) as lp_fee_bps,
    CAST(protocol_fee_bps AS bigint) as protocol_fee_bps,
    first_buy_slot,
    first_buy_time,
    first_buy_slot - grad_slot as slots_to_first_buy
FROM first_events
WHERE seq = 1
ORDER BY grad_time
