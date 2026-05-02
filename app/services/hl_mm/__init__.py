"""
HL Market Making Engine — Imbalance-Directed Quoting on Hyperliquid

Architecture:
  1. SignalEngine: reads L2 book, computes imbalance z-score
  2. QuoteEngine: manages bid/ask limit orders, skews by signal
  3. InventoryManager: tracks position, triggers hedging
  4. RiskManager: position limits, daily P&L limits, circuit breakers
  5. Orchestrator: ties everything together in the main loop

Execution model:
  - Always quote both sides (bid + ask) as maker (0% fee)
  - Skew quotes toward predicted direction based on L2 imbalance
  - Hedge accumulated inventory on Bybit when thresholds exceeded
  - Revenue = spread capture + directional skew alpha - hedge costs

Dependencies:
  - hyperliquid-python-sdk (v0.23.0)
  - HL wallet private key (env: HL_PRIVATE_KEY)
  - HL account address (env: HL_ADDRESS)
  - Bybit API for hedging (existing credentials)
"""
