FROM hummingbot/hummingbot:latest

# Bybit demo patches — same as CLAUDE.md but for the client image
# The connector path differs from the API image
ENV PKG=/home/hummingbot/hummingbot/connector/derivative/bybit_perpetual

# 1. URL patches — REST to demo, public WS to mainnet (demo has no public WS), private WS to demo
RUN sed -i 's|api-testnet.bybit.com|api-demo.bybit.com|' "$PKG/bybit_perpetual_constants.py" && \
    sed -i 's|stream-testnet.bybit.com/v5/public|stream.bybit.com/v5/public|' "$PKG/bybit_perpetual_constants.py" && \
    sed -i 's|stream-testnet.bybit.com/v5/private|stream-demo.bybit.com/v5/private|' "$PKG/bybit_perpetual_constants.py"

# 2. Rate limit fix — provide default pairs so executor creation works
RUN sed -i 's|return web_utils.build_rate_limits(self.trading_pairs)|pairs = self.trading_pairs or ["BTC-USDT","ETH-USDT","SOL-USDT","XRP-USDT","DOGE-USDT","ADA-USDT","AVAX-USDT","LINK-USDT","DOT-USDT","UNI-USDT","NEAR-USDT","APT-USDT","ARB-USDT","OP-USDT","SUI-USDT","SEI-USDT","WLD-USDT","LTC-USDT","BCH-USDT","BNB-USDT","CRV-USDT","1000PEPE-USDT","ALGO-USDT","GALA-USDT","ONT-USDT","TAO-USDT","ZEC-USDT","AAVE-USDT","DRIFT-USDT","FARTCOIN-USDT","PAXG-USDT","WIF-USDT"]; return web_utils.build_rate_limits(pairs)|' "$PKG/bybit_perpetual_derivative.py"

# 3. Position mode fix — demo set-position-mode returns empty, force one-way
RUN sed -i "s|if self.position_mode == PositionMode.ONEWAY:|if self.position_mode == PositionMode.ONEWAY or 'testnet' in str(self._domain):|" "$PKG/bybit_perpetual_derivative.py"

# 4. Clear Python cache
RUN find /home/hummingbot/hummingbot/connector -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
