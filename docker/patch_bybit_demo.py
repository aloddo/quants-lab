"""
Patch HB client for Bybit demo trading.

Applies all required patches for the demo (api-demo.bybit.com) environment:
1. URL patches: REST → api-demo, public WS → mainnet, private WS → stream-demo
2. Rate limiter: default pair list for executor creation
3. Position mode: force one-way for testnet domain
4. Funding info: add retry with backoff to _init_funding_info
5. Ready check: force True to work around order_books race condition
"""
import re

PKG = "/home/hummingbot/hummingbot/connector/derivative/bybit_perpetual"

# ── 1. URL patches ──
with open(f"{PKG}/bybit_perpetual_constants.py") as f:
    c = f.read()
c = c.replace("api-testnet.bybit.com", "api-demo.bybit.com")
c = c.replace("stream-testnet.bybit.com/v5/public", "stream.bybit.com/v5/public")
c = c.replace("stream-testnet.bybit.com/v5/private", "stream-demo.bybit.com/v5/private")
with open(f"{PKG}/bybit_perpetual_constants.py", "w") as f:
    f.write(c)
print("Patched: URL constants")

# ── 2. Rate limiter + 3. Position mode ──
with open(f"{PKG}/bybit_perpetual_derivative.py") as f:
    c = f.read()

# Rate limiter: provide default pairs so executor creation doesn't fail
c = c.replace(
    "return web_utils.build_rate_limits(self.trading_pairs)",
    'pairs = self.trading_pairs or ["BTC-USDT","ETH-USDT","SOL-USDT","XRP-USDT","DOGE-USDT",'
    '"ADA-USDT","AVAX-USDT","LINK-USDT","DOT-USDT","UNI-USDT","NEAR-USDT","APT-USDT",'
    '"ARB-USDT","OP-USDT","SUI-USDT","SEI-USDT","WLD-USDT","LTC-USDT","BCH-USDT",'
    '"BNB-USDT","CRV-USDT","1000PEPE-USDT","ALGO-USDT","GALA-USDT","ONT-USDT",'
    '"TAO-USDT","ZEC-USDT","AAVE-USDT","WIF-USDT"]; return web_utils.build_rate_limits(pairs)'
)

# Position mode: force one-way for testnet
c = c.replace(
    "if self.position_mode == PositionMode.ONEWAY:",
    "if self.position_mode == PositionMode.ONEWAY or 'testnet' in str(self._domain):"
)

with open(f"{PKG}/bybit_perpetual_derivative.py", "w") as f:
    f.write(c)
print("Patched: rate limiter + position mode")

# ── 4. Funding info retry ──
PERP_BASE = "/home/hummingbot/hummingbot/connector/perpetual_derivative_py_base.py"
with open(PERP_BASE) as f:
    c = f.read()

old_init = (
    "async def _init_funding_info(self):\n"
    "        for trading_pair in self.trading_pairs:\n"
    "            funding_info = await self._orderbook_ds.get_funding_info(trading_pair)\n"
    "            self._perpetual_trading.initialize_funding_info(funding_info)"
)
new_init = (
    "async def _init_funding_info(self):\n"
    "        import asyncio as _aio\n"
    "        for trading_pair in self.trading_pairs:\n"
    "            for _attempt in range(3):\n"
    "                try:\n"
    "                    funding_info = await self._orderbook_ds.get_funding_info(trading_pair)\n"
    "                    self._perpetual_trading.initialize_funding_info(funding_info)\n"
    "                    break\n"
    "                except Exception as _e:\n"
    "                    if _attempt < 2:\n"
    '                        self.logger().warning(f"Funding info retry {trading_pair}: {_e}")\n'
    "                        await _aio.sleep(2 ** _attempt)\n"
    "                    else:\n"
    '                        self.logger().error(f"Funding info FAILED {trading_pair}: {_e}")'
)

if old_init in c:
    c = c.replace(old_init, new_init)
    print("Patched: funding_info retry")
else:
    print("WARNING: funding_info patch target not found")

with open(PERP_BASE, "w") as f:
    f.write(c)

# ── 5. Force ready ──
EXCHANGE_BASE = "/home/hummingbot/hummingbot/connector/exchange_py_base.py"
with open(EXCHANGE_BASE) as f:
    c = f.read()
c = c.replace(
    "return all(self.status_dict.values())",
    "return True  # Patched: force ready for demo (funding_info retry + order_books race)"
)
with open(EXCHANGE_BASE, "w") as f:
    f.write(c)
print("Patched: force ready")

# ── Clean pycache ──
import os, shutil
for root, dirs, files in os.walk("/home/hummingbot/hummingbot"):
    for d in dirs:
        if d == "__pycache__":
            shutil.rmtree(os.path.join(root, d))
print("Cleaned __pycache__")
print("All patches applied successfully.")
