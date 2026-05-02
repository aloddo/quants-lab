"""
HL Shitcoin Market Making Engine — V2

Architecture (spec: hl_mm_shitcoin_spec_v1.md):
  0. PairScreener:    scans 230 HL pairs every 15min, ranks by MM profitability
  1. FairValueEngine: three-tier anchor (Bybit direct / sparse / synthetic)
  2. QuoteEngine:     inside-improvement + contrarian quoting + requote logic
  3. StateMachine:    per-pair IDLE/QUOTING_BOTH/QUOTING_ONE_SIDE/INVENTORY_EXIT/HEDGE/PAUSE
  4. InventoryManager: AS reservation price, age-based exits, Bybit hedging
  5. FillTracker:     markout logging at 1/5/15/60s, toxicity scoring
  6. RiskManager:     portfolio-level stops, correlation stops, gap risk
  7. Orchestrator:    asyncio event loop tying it all together

Execution model:
  - Maker-only (ALO) quotes on Hyperliquid
  - Bybit perp for hedging excess inventory
  - Revenue = spread capture - maker fees (1.44bps) - adverse selection
  - Starting capital: $54 HL, $480 Bybit

Dependencies:
  - hyperliquid-python-sdk (v0.23.0)
  - websockets (for Bybit WS)
  - pymongo (for logging + pair rankings)
  - HL wallet private key (env: HL_PRIVATE_KEY)
  - HL account address (env: HL_ADDRESS)
  - Bybit API keys (env: BYBIT_API_KEY, BYBIT_API_SECRET)
"""
