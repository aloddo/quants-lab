#!/usr/bin/env bash
# Re-apply Bybit demo patches to the hummingbot-api Docker container.
# Must be run after every `docker restart hummingbot-api`.
#
# Usage:
#   bash scripts/patch_hb_docker.sh
#   bash scripts/patch_hb_docker.sh --check   # just verify patches are applied
set -euo pipefail

CONTAINER="hummingbot-api"
PKG="/opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector/derivative/bybit_perpetual"

# Pair allowlist — must match pair_historical (verdict=ALLOW)
PAIRS='["BTC-USDT","ETH-USDT","SOL-USDT","XRP-USDT","DOGE-USDT","ADA-USDT","AVAX-USDT","LINK-USDT","DOT-USDT","UNI-USDT","NEAR-USDT","APT-USDT","ARB-USDT","OP-USDT","SUI-USDT","SEI-USDT","WLD-USDT","LTC-USDT","BCH-USDT","BNB-USDT","CRV-USDT","1000PEPE-USDT","ALGO-USDT","GALA-USDT","ONT-USDT","TAO-USDT","ZEC-USDT"]'

check_mode=false
if [[ "${1:-}" == "--check" ]]; then
    check_mode=true
fi

# Verify container is running
if ! docker inspect --format='{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
    echo "ERROR: Container $CONTAINER is not running"
    exit 1
fi

if $check_mode; then
    echo "Checking patches..."
    if docker exec "$CONTAINER" grep -q "api-demo.bybit.com" "$PKG/bybit_perpetual_constants.py" 2>/dev/null; then
        echo "  URL patches: OK"
    else
        echo "  URL patches: MISSING"
        exit 1
    fi
    if docker exec "$CONTAINER" grep -q "stream-demo.bybit.com" "$PKG/bybit_perpetual_constants.py" 2>/dev/null; then
        echo "  Private WS patch: OK"
    else
        echo "  Private WS patch: MISSING"
        exit 1
    fi
    echo "All patches verified."
    exit 0
fi

echo "Applying Bybit demo patches to $CONTAINER..."

# 1. URL patches (REST + WebSocket)
#    Public WS must point to mainnet (demo has no public WS)
echo "  [1/5] REST URL: api-testnet -> api-demo"
docker exec "$CONTAINER" sed -i 's|api-testnet.bybit.com|api-demo.bybit.com|' "$PKG/bybit_perpetual_constants.py"

echo "  [2/5] Public WS: stream-testnet -> stream (mainnet)"
docker exec "$CONTAINER" sed -i 's|stream-testnet.bybit.com/v5/public|stream.bybit.com/v5/public|' "$PKG/bybit_perpetual_constants.py"

echo "  [3/5] Private WS: stream-testnet -> stream-demo"
docker exec "$CONTAINER" sed -i 's|stream-testnet.bybit.com/v5/private|stream-demo.bybit.com/v5/private|' "$PKG/bybit_perpetual_constants.py"

# 2. Rate limit fix — provide default pair list for executor creation
echo "  [4/5] Rate limiter pair allowlist"
docker exec "$CONTAINER" sed -i "s|return web_utils.build_rate_limits(self.trading_pairs)|pairs = self.trading_pairs or $PAIRS; return web_utils.build_rate_limits(pairs)|" "$PKG/bybit_perpetual_derivative.py"

# 3. Position mode fix (demo set-position-mode returns empty, force one-way)
echo "  [5/5] Position mode fix"
docker exec "$CONTAINER" sed -i "s|if self.position_mode == PositionMode.ONEWAY:|if self.position_mode == PositionMode.ONEWAY or 'testnet' in str(self._domain):|" "$PKG/bybit_perpetual_derivative.py"

# Clear caches
echo "  Clearing __pycache__..."
docker exec "$CONTAINER" rm -rf "$PKG/__pycache__"
docker exec "$CONTAINER" find /opt/conda/envs/hummingbot-api/lib/python3.12/site-packages/hummingbot/connector -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

echo "Patches applied successfully."
