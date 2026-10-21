"""Read-only Predict market-making preview. No private key or trading calls."""
import argparse
import asyncio
import json
import logging
import math
import os
import time
from decimal import Decimal
from pathlib import Path


class PredictMarketMakingPreview:
    def __init__(self, connector, market_id, outcome, config, pricing):
        self.connector = connector
        self.market_id = market_id
        self.token_id = outcome.token_id
        self.pair = outcome.trading_pair
        self.config = config
        self.pricing = pricing
        self.preview = pricing.PreviewQuotes()

    def step(self, now=None):
        now = time.monotonic() if now is None else now
        market = self.connector.markets_by_id[self.market_id]
        outcome = market.outcome(token_id=self.token_id)
        age = now - self.connector.book_received_at.get(self.market_id, -float("inf"))
        bid = ask = None
        bid_size = ask_size = Decimal(0)
        ready = self.connector.ready and self.connector.market_data_stream_connected
        try:
            book = self.connector.get_order_book(self.pair)
            bid_row = next(iter(book.bid_entries()), None)
            ask_row = next(iter(book.ask_entries()), None)
            if bid_row is not None:
                bid, bid_size = Decimal(str(bid_row.price)), Decimal(str(bid_row.amount))
            if ask_row is not None:
                ask, ask_size = Decimal(str(ask_row.price)), Decimal(str(ask_row.amount))
        except (KeyError, ValueError):
            ready = False
        state = self.pricing.BookState(
            bid if bid is not None else Decimal(0), bid_size,
            ask if ask is not None else Decimal(0), ask_size, market.tick_size, age,
            trading_status=self.connector.market_status.get(self.market_id, "UNKNOWN"), ready=ready)
        plan = self.pricing.calculate_quotes(
            self.config, state, inventory=self.config.preview_shares,
            available_shares=self.config.preview_shares, available_usdt=self.config.preview_usdt,
            fee_rate_bps=Decimal(market.fee_rate_bps))
        action = self.preview.update(plan, state, self.config, now)
        return {"mode": "READ_ONLY_PREVIEW", "market_id": self.market_id,
                "outcome": outcome.name, "token_id": self.token_id, "trading_pair": self.pair,
                "market_status": state.trading_status, "ready": ready,
                "book_age_seconds": round(age, 3) if math.isfinite(age) else None,
                "best_bid": str(bid) if bid is not None else None,
                "best_ask": str(ask) if ask is not None else None,
                "mid": str(plan.mid) if plan.mid is not None else None,
                "quote_center": str(plan.center) if plan.center is not None else None,
                "balances": {"source": "CONFIGURED_SIMULATION", "usdt": str(self.config.preview_usdt),
                             "shares": str(self.config.preview_shares)},
                "action": action, "reasons": list(plan.reasons),
                "target_quotes": [q.as_dict() for q in plan.quotes],
                "active_preview_quotes": [q.as_dict() for q in self.preview.active]}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", required=True, type=int)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--outcome-name", help="Exact outcome name from the market API")
    selector.add_argument("--token-id", help="Use this if outcome names are ambiguous")
    parser.add_argument("--config", type=Path, help="Optional JSON quotation parameters")
    parser.add_argument("--interval", type=float, default=2, help="Output interval in seconds")
    parser.add_argument("--duration", type=float, default=0, help="Seconds to run; 0 means until Ctrl+C")
    parser.add_argument("--startup-timeout", type=float, default=90)
    args = parser.parse_args(argv)
    if args.market_id <= 0:
        parser.error("--market-id must be positive")
    if any(not math.isfinite(v) for v in (args.interval, args.duration, args.startup_timeout)):
        parser.error("Timing values must be finite")
    if args.interval <= 0 or args.duration < 0 or args.startup_timeout <= 0:
        parser.error("interval/startup-timeout must be positive; duration must be nonnegative")
    return args


async def run(args):
    # Delay runtime imports so --help works without compiled Hummingbot extensions.
    from hummingbot.connector.exchange.predict.predict_exchange import PredictExchange
    from hummingbot.core.clock import Clock
    from hummingbot.core.clock_mode import ClockMode
    from hummingbot.strategy.predict_market_making import quote_engine

    data = json.loads(args.config.read_text(encoding="utf-8"), parse_float=Decimal) if args.config else {}
    config = quote_engine.QuoteConfig.from_dict(data)
    api_key = os.environ.get("PREDICT_API_KEY", "")
    if not api_key:
        raise ValueError("Set PREDICT_API_KEY in the environment first")
    connector = PredictExchange(predict_api_key=api_key, market_ids=[args.market_id], trading_required=False,
                                max_book_age=float(config.max_book_age_seconds))
    try:
        await connector.initialize_markets()
        market = connector.markets_by_id[args.market_id]
        try:
            outcome = market.outcome(name=args.outcome_name, token_id=args.token_id)
        except ValueError as exc:
            choices = [(o.name, o.token_id) for o in market.outcomes]
            raise ValueError(f"Outcome selection failed. Available (name, token_id): {choices}") from exc
        preview = PredictMarketMakingPreview(connector, args.market_id, outcome, config, quote_engine)
        print(json.dumps({"event": "startup", "mode": "READ_ONLY_PREVIEW", "market_id": market.market_id,
                          "title": market.title, "outcome": outcome.name, "token_id": outcome.token_id,
                          "tick_size": str(market.tick_size), "orders_enabled": False,
                          "fills_simulated": False}, ensure_ascii=False), flush=True)
        clock = Clock(ClockMode.REALTIME)
        clock.add_iterator(connector)
        with clock:
            clock_task = asyncio.create_task(clock.run())
            started = time.monotonic()
            became_ready = False
            try:
                while args.duration == 0 or time.monotonic() - started < args.duration:
                    if clock_task.done():
                        await clock_task
                        raise RuntimeError("Hummingbot clock stopped unexpectedly")
                    report = preview.step()
                    print(json.dumps(report, ensure_ascii=False, allow_nan=False), flush=True)
                    became_ready = became_ready or report["ready"]
                    if not became_ready and time.monotonic() - started > args.startup_timeout:
                        raise TimeoutError("Connector/WS not ready before startup timeout; inspect connection logs")
                    await asyncio.sleep(args.interval)
                if not became_ready:
                    raise TimeoutError("Preview duration ended without a ready connector/WS connection")
            finally:
                print(json.dumps({"event": "stop", "action": "WITHDRAW_PREVIEW",
                                  "real_orders_submitted": 0}), flush=True)
                clock_task.cancel()
                await asyncio.gather(clock_task, return_exceptions=True)
    finally:
        # Clock context cancels the connectivity checker; finish network cleanup.
        await asyncio.sleep(0)
        await connector.stop_network()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
