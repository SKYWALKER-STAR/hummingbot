"""Pure Decimal pricing and preview lifecycle; no exchange/network dependencies."""
from dataclasses import dataclass, fields
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Optional

D = Decimal


@dataclass(frozen=True)
class QuoteConfig:
    # Spreads and skew are absolute probability-price offsets, not percentages.
    half_spread: D = D("0.01")
    inventory_skew: D = D("0.02")
    order_amount: D = D("10")
    target_inventory: D = D("50")
    max_inventory: D = D("100")
    min_order_amount: D = D("1")
    min_order_notional: D = D("1")
    amount_step: D = D("0.000001")
    min_book_spread: D = D("0")
    min_level_amount: D = D("1")
    min_mid_price: D = D("0.02")
    max_mid_price: D = D("0.98")
    refresh_seconds: D = D("30")
    refresh_price_ticks: D = D("1")
    max_book_age_seconds: D = D("30")
    preview_usdt: D = D("100")
    preview_shares: D = D("0")

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            raise ValueError("Configuration must be a JSON object")
        unknown = set(data) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"Unknown configuration fields: {sorted(unknown)}")
        try:
            values = {key: D(str(value)) for key, value in data.items()}
        except (InvalidOperation, ValueError):
            raise ValueError("Configuration values must be finite decimal numbers") from None
        return cls(**values)

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, D) or not value.is_finite() or value < 0:
                raise ValueError(f"{field.name} must be a finite nonnegative Decimal")
        for name in ("half_spread", "order_amount", "max_inventory", "min_order_amount", "amount_step",
                     "refresh_seconds", "refresh_price_ticks", "max_book_age_seconds"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.target_inventory > self.max_inventory:
            raise ValueError("target_inventory exceeds max_inventory")
        if not 0 <= self.min_mid_price < self.max_mid_price <= 1:
            raise ValueError("Invalid mid-price bounds")
        if self.half_spread >= 1 or self.inventory_skew >= 1:
            raise ValueError("half_spread and inventory_skew must be less than 1")
        if self.refresh_price_ticks != self.refresh_price_ticks.to_integral_value():
            raise ValueError("refresh_price_ticks must be an integer")


@dataclass(frozen=True)
class BookState:
    best_bid: D
    bid_amount: D
    best_ask: D
    ask_amount: D
    tick_size: D
    age_seconds: float
    trading_status: str = "OPEN"
    ready: bool = True


@dataclass(frozen=True)
class Quote:
    side: str
    price: D
    amount: D

    def as_dict(self):
        return {"side": self.side, "price": str(self.price), "amount": str(self.amount),
                "notional": str(self.price * self.amount)}


@dataclass(frozen=True)
class QuotePlan:
    quotes: tuple[Quote, ...] = ()
    reasons: tuple[str, ...] = ()
    mid: Optional[D] = None
    center: Optional[D] = None


def floor_step(value, step):
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_step(value, step):
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def calculate_quotes(config: QuoteConfig, book: BookState, *, inventory: D,
                     available_shares: D, available_usdt: D, fee_rate_bps: D = D(0)) -> QuotePlan:
    """Calculate replacement quotes after old quotes have been removed.

    inventory is settled holdings; available balances exclude OTHER commitments.
    A live caller must finish cancellations and reconcile pending fills first.
    """
    def skip(reason):
        return QuotePlan(reasons=(reason,))

    if not book.ready:
        return skip("connector_not_ready")
    if book.trading_status != "OPEN":
        return skip(f"market_not_open:{book.trading_status}")
    import math
    if not math.isfinite(book.age_seconds) or book.age_seconds < 0 or book.age_seconds > float(config.max_book_age_seconds):
        return skip("stale_orderbook")
    numbers = (book.best_bid, book.best_ask, book.bid_amount, book.ask_amount, book.tick_size,
               inventory, available_shares, available_usdt, fee_rate_bps)
    if any(not value.is_finite() or value < 0 for value in numbers):
        return skip("invalid_book_or_balance")
    if not 0 < book.tick_size < D("0.5") or fee_rate_bps > 10000:
        return skip("invalid_tick_or_fee")
    if not 0 < book.best_bid < book.best_ask < 1:
        return skip("empty_or_crossed_book")
    if book.best_bid % book.tick_size or book.best_ask % book.tick_size:
        return skip("off_tick_book")
    if min(book.bid_amount, book.ask_amount) <= 0 or min(book.bid_amount, book.ask_amount) < config.min_level_amount:
        return skip("insufficient_top_level_depth")
    if book.best_ask - book.best_bid < config.min_book_spread:
        return skip("book_spread_too_small")
    mid = (book.best_bid + book.best_ask) / 2
    if not config.min_mid_price <= mid <= config.max_mid_price:
        return skip("mid_outside_configured_range")
    ratio = max(D(-1), min(D(1), (inventory - config.target_inventory) / config.max_inventory))
    center = mid - config.inventory_skew * ratio
    tick = book.tick_size
    bid = floor_step(max(tick, min(1 - tick, book.best_ask - tick, center - config.half_spread)), tick)
    ask = ceil_step(min(1 - tick, max(tick, book.best_bid + tick, center + config.half_spread)), tick)
    if not 0 < bid < ask < 1 or bid >= book.best_ask or ask <= book.best_bid:
        return skip("no_valid_maker_prices")
    buy_size = floor_step(min(config.order_amount, max(D(0), config.max_inventory - inventory),
                              available_usdt / (bid * (1 + fee_rate_bps / 10000))), config.amount_step)
    sell_size = floor_step(min(config.order_amount, inventory, available_shares), config.amount_step)
    quotes, reasons = [], []
    for side, price, size in (("BUY", bid, buy_size), ("SELL", ask, sell_size)):
        if size < config.min_order_amount or price * size < config.min_order_notional:
            reasons.append(f"{side.lower()}_below_minimum_or_no_budget")
        else:
            quotes.append(Quote(side, price, size))
    return QuotePlan(tuple(quotes), tuple(reasons), mid, center)


class PreviewQuotes:
    """Virtual quote intentions only: no orders, no fills, no P&L simulation."""
    def __init__(self):
        self.active: tuple[Quote, ...] = ()
        self.created_at = 0.0

    def update(self, plan, book, config, now):
        if not plan.quotes:
            action = "WITHDRAW" if self.active else "SKIP"
            self.active = ()
            return action
        previous = {q.side: q for q in self.active}
        target = {q.side: q for q in plan.quotes}
        changed = previous.keys() != target.keys()
        if not changed:
            changed = any(old.amount != target[side].amount or
                          abs(old.price - target[side].price) >= book.tick_size * config.refresh_price_ticks
                          for side, old in previous.items())
        crossing = any(q.price >= book.best_ask if q.side == "BUY" else q.price <= book.best_bid for q in self.active)
        expired = now - self.created_at >= float(config.refresh_seconds)
        if not self.active or changed or crossing or expired:
            action = "REPLACE" if self.active else "QUOTE"
            self.active = plan.quotes
            self.created_at = now
            return action
        return "HOLD"
