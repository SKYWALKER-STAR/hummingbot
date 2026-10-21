from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = False
EXAMPLE_PAIR = "PREDICT_123_1-USDT"
# Actual market fees are estimated by the connector and confirmed from chain logs.
DEFAULT_FEES = TradeFeeSchema(maker_percent_fee_decimal=Decimal("0"),
                             taker_percent_fee_decimal=Decimal("0"))


class PredictConfigMap(BaseConnectorConfigMap):
    connector: str = "predict"
    predict_api_key: SecretStr = Field(default=..., json_schema_extra={
        "prompt": "Enter your Predict API key", "is_secure": True,
        "is_connect_key": True, "prompt_on_new": True})
    predict_private_key: SecretStr = Field(default=..., json_schema_extra={
        "prompt": "Enter your Predict wallet private key", "is_secure": True,
        "is_connect_key": True, "prompt_on_new": True})
    predict_account_address: str = Field(default="", json_schema_extra={
        "prompt": "Predict Account address (empty for EOA)", "is_secure": False,
        "is_connect_key": True, "prompt_on_new": True})
    predict_market_ids: str = Field(default="", json_schema_extra={
        "prompt": "Predict MarketIDs, comma-separated", "is_secure": False,
        "is_connect_key": True, "prompt_on_new": True})
    model_config = ConfigDict(title="predict")


KEYS = PredictConfigMap.model_construct()
