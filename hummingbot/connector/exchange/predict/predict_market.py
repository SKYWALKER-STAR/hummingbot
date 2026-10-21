"""Market identities and full-book conversion, independent of Hummingbot."""
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


def positive_id(value: Any) -> int:
    if isinstance(value, bool) or not str(value).isdigit() or int(value) <= 0:
        raise ValueError(f"Invalid MarketID: {value!r}")
    return int(value)


def decimal_value(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Invalid {field}") from None
    if not result.is_finite() or result < 0:
        raise ValueError(f"Invalid {field}")
    return result


@dataclass(frozen=True)
class PredictOutcome:
    market_id: int
    name: str
    index_set: int
    token_id: str

    @property
    def base_asset(self) -> str:
        # Labels may change and may contain '-' or be duplicated. Asset identity must not.
        return f"PREDICT_{self.market_id}_{self.index_set}"

    @property
    def trading_pair(self) -> str:
        return f"{self.base_asset}-USDT"


@dataclass(frozen=True)
class PredictMarket:
    market_id: int
    title: str
    outcomes: tuple[PredictOutcome, ...]
    decimal_precision: int
    fee_rate_bps: int
    is_neg_risk: bool
    is_yield_bearing: bool
    trading_status: str

    @property
    def tick_size(self) -> Decimal:
        return Decimal(1).scaleb(-self.decimal_precision)

    def outcome(self, *, name=None, token_id=None, index_set=None) -> PredictOutcome:
        if sum(v is not None for v in (name, token_id, index_set)) != 1:
            raise ValueError("Select exactly one of name, token_id, index_set")
        matches = [o for o in self.outcomes if
                   (name is not None and o.name == name) or
                   (token_id is not None and o.token_id == str(token_id)) or
                   (index_set is not None and o.index_set == index_set)]
        if len(matches) != 1:
            raise ValueError("Outcome is missing or ambiguous; use token_id or index_set")
        return matches[0]

    @classmethod
    def parse(cls, data: dict, expected_id=None):
        market_id = positive_id(data["id"])
        if expected_id is not None and market_id != positive_id(expected_id):
            raise ValueError("Market response ID does not match request")
        outcomes = []
        for row in data["outcomes"]:
            name = row["name"]
            token = str(row["onChainId"])
            if not isinstance(name, str) or not name.strip() or not token.isdigit():
                raise ValueError("Missing outcome name or invalid onChainId")
            index = positive_id(row["indexSet"])
            outcomes.append(PredictOutcome(market_id, name, index, token))
        # A single API book describes a binary conditional market. Never manufacture
        # a complementary book for an unsupported multi-outcome schema.
        if len(outcomes) != 2 or {o.index_set for o in outcomes} != {1, 2}:
            raise ValueError("Unsupported market: expected binary indexSet 1 and 2")
        if len({o.token_id for o in outcomes}) != 2:
            raise ValueError("Duplicate outcome token IDs")
        precision = data["decimalPrecision"]
        if isinstance(precision, bool) or not isinstance(precision, int) or not 0 <= precision <= 18:
            raise ValueError("Invalid decimalPrecision")
        fee = decimal_value(data["feeRateBps"], "feeRateBps")
        if fee != int(fee) or fee > 10000:
            raise ValueError("Invalid feeRateBps")
        for flag in ("isNegRisk", "isYieldBearing"):
            if not isinstance(data[flag], bool):
                raise ValueError(f"Invalid {flag}")
        return cls(market_id, data.get("title", data.get("question", "")),
                   tuple(sorted(outcomes, key=lambda o: o.index_set)), precision,
                   int(fee), data["isNegRisk"], data["isYieldBearing"], data["tradingStatus"])

    def book(self, data: dict, outcome: PredictOutcome) -> dict:
        if outcome not in self.outcomes or positive_id(data["marketId"]) != self.market_id:
            raise ValueError("Orderbook/outcome belongs to a different market")
        levels = {}
        for side in ("bids", "asks"):
            parsed = {}
            for raw_price, raw_size in data[side]:
                price = decimal_value(raw_price, "price")
                size = decimal_value(raw_size, "size")
                if price > 1 or price % self.tick_size:
                    raise ValueError("Off-tick or out-of-range orderbook price")
                if size:
                    parsed[price] = parsed.get(price, Decimal(0)) + size
            levels[side] = list(parsed.items())
        if outcome.index_set == 2:
            levels = {"bids": [(1 - p, q) for p, q in levels["asks"]],
                      "asks": [(1 - p, q) for p, q in levels["bids"]]}
        stamp = positive_id(data["updateTimestampMs"])
        return {"trading_pair": outcome.trading_pair, "update_id": stamp,
                "bids": sorted(levels["bids"], reverse=True), "asks": sorted(levels["asks"])}
