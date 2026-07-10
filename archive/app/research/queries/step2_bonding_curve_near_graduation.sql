-- Step 2c: Near-graduation bonding curve data (for C5 placebo baseline)
-- Framework: Section C5 - tokens reaching 80-95% completion but never graduating
-- Track tokens with high bonding curve activity that did NOT graduate

WITH graduated_mints AS (
    SELECT DISTINCT account_mint as mint
    FROM pumpdotfun_solana.pump_call_migrate
    WHERE call_block_date >= DATE '2025-03-20'
),
-- Tokens with significant bonding curve activity (proxy for near-graduation)
-- High SOL volume = near completion of bonding curve
active_tokens AS (
    SELECT
        t.mint,
        COUNT(*) as trade_count,
        COUNT(DISTINCT t."user") as unique_traders,
        SUM(CAST(COALESCE(t.sol_amount, t.solAmount) AS double) / 1e9) as total_sol_volume,
        MAX(CAST(COALESCE(t.virtual_sol_reserves, t.virtualSolReserves) AS double) / 1e9) as max_sol_reserves,
        MIN(t.evt_block_time) as first_trade,
        MAX(t.evt_block_time) as last_trade
    FROM pumpdotfun_solana.pump_evt_tradeevent t
    WHERE t.evt_block_date >= DATE '2025-11-01'
      AND t.evt_block_date < CURRENT_DATE
    GROUP BY t.mint
    HAVING SUM(CAST(COALESCE(t.sol_amount, t.solAmount) AS double) / 1e9) >= 50
       -- 50+ SOL volume = likely reached 60%+ of bonding curve
)
SELECT
    a.mint,
    a.trade_count,
    a.unique_traders,
    a.total_sol_volume,
    a.max_sol_reserves,
    a.first_trade,
    a.last_trade,
    CASE WHEN g.mint IS NOT NULL THEN true ELSE false END as graduated,
    -- Proxy for bonding curve completion %:
    -- Max SOL reserves / ~85 SOL target
    a.max_sol_reserves / 85.0 * 100 as estimated_completion_pct
FROM active_tokens a
LEFT JOIN graduated_mints g ON a.mint = g.mint
WHERE a.total_sol_volume >= 50
ORDER BY a.total_sol_volume DESC
