"""
Bybit Perpetual Demo Trading connector registration.

Adds 'bybit_perpetual_demo' as a domain to the existing bybit_perpetual connector.
Uses api-demo.bybit.com (real market data, virtual funds — closer to production than testnet).

Usage: Import this module before using the connector.
    import app.connectors.bybit_perpetual_demo  # registers the demo domain
"""
from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.derivative.bybit_perpetual import bybit_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.bybit_perpetual import bybit_perpetual_utils as UTILS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEMO_DOMAIN = "bybit_perpetual_demo"

# ── Patch URL dicts ──────────────────────────────────────
CONSTANTS.REST_URLS[DEMO_DOMAIN] = "https://api-demo.bybit.com/"

CONSTANTS.WSS_LINEAR_PUBLIC_URLS[DEMO_DOMAIN] = "wss://stream-demo.bybit.com/v5/public/linear"
CONSTANTS.WSS_LINEAR_PRIVATE_URLS[DEMO_DOMAIN] = "wss://stream-demo.bybit.com/v5/private"
CONSTANTS.WSS_NON_LINEAR_PUBLIC_URLS[DEMO_DOMAIN] = "wss://stream-demo.bybit.com/v5/public/inverse"
CONSTANTS.WSS_NON_LINEAR_PRIVATE_URLS[DEMO_DOMAIN] = "wss://stream-demo.bybit.com/v5/private"

# ── Patch domain registration ────────────────────────────
if DEMO_DOMAIN not in UTILS.OTHER_DOMAINS:
    UTILS.OTHER_DOMAINS.append(DEMO_DOMAIN)

UTILS.OTHER_DOMAINS_PARAMETER[DEMO_DOMAIN] = DEMO_DOMAIN
UTILS.OTHER_DOMAINS_EXAMPLE_PAIR[DEMO_DOMAIN] = "BTC-USDT"

# Demo uses same fee structure as mainnet
UTILS.OTHER_DOMAINS_DEFAULT_FEES[DEMO_DOMAIN] = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.00055"),
)


# ── Config map for demo credentials ─────────────────────
class BybitPerpetualDemoConfigMap(BaseConnectorConfigMap):
    connector: str = "bybit_perpetual_demo"
    bybit_perpetual_demo_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bybit Demo Trading API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    bybit_perpetual_demo_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bybit Demo Trading API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="bybit_perpetual_demo")


UTILS.OTHER_DOMAINS_KEYS[DEMO_DOMAIN] = BybitPerpetualDemoConfigMap.model_construct()
