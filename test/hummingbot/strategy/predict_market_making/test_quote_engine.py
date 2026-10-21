import importlib.util
import sys
import unittest
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[4]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


engine = load("predict_quote_engine_test", "hummingbot/strategy/predict_market_making/quote_engine.py")
runner = load("predict_preview_runner_test", "scripts/predict_market_maker.py")


class QuoteTests(unittest.TestCase):
    def setUp(self):
        self.config = engine.QuoteConfig()
        self.book = engine.BookState(D("0.59"), D(100), D("0.61"), D(100), D("0.001"), 1)

    def plan(self, config=None, book=None, inventory="50", shares="50", usdt="100", fee="0"):
        return engine.calculate_quotes(config or self.config, book or self.book, inventory=D(inventory),
                                       available_shares=D(shares), available_usdt=D(usdt), fee_rate_bps=D(fee))

    def test_balanced_inventory_quotes_absolute_spread(self):
        plan = self.plan()
        self.assertEqual(D("0.60"), plan.mid)
        self.assertEqual([D("0.590"), D("0.610")], [q.price for q in plan.quotes])
        self.assertEqual([D(10), D(10)], [q.amount for q in plan.quotes])

    def test_more_inventory_shifts_both_prices_down(self):
        low, high = self.plan(inventory="25", shares="25"), self.plan(inventory="75", shares="75")
        for low_quote, high_quote in zip(low.quotes, high.quotes):
            self.assertGreater(low_quote.price, high_quote.price)

    def test_zero_inventory_has_no_sell(self):
        plan = self.plan(inventory="0", shares="0")
        self.assertEqual(["BUY"], [q.side for q in plan.quotes])
        self.assertIn("sell_below_minimum_or_no_budget", plan.reasons)

    def test_inventory_cap_clips_buy_and_allows_inventory_reduction(self):
        plan = self.plan(inventory="98", shares="98")
        buy = next(q for q in plan.quotes if q.side == "BUY")
        self.assertEqual(D(2), buy.amount)
        for inventory in ("100", "130"):
            self.assertEqual(["SELL"], [q.side for q in self.plan(inventory=inventory, shares=inventory).quotes])

    def test_fee_included_and_sell_limited_to_available_shares(self):
        plan = self.plan(usdt="2", shares="3", fee="100")
        buy, sell = plan.quotes
        self.assertLessEqual(buy.price * buy.amount * D("1.01"), D(2))
        self.assertEqual(D(3), sell.amount)

    def test_minimum_notional_does_not_increase_risk_limit(self):
        config = replace(self.config, min_order_notional=D(5))
        plan = self.plan(config=config, inventory="99", shares="1", usdt="1")
        self.assertEqual((), plan.quotes)

    def test_prices_are_quantized_and_do_not_cross(self):
        config = replace(self.config, half_spread=D("0.0001"), inventory_skew=D("0.9"))
        for inventory in ("0", "50", "100"):
            for quote in self.plan(config=config, inventory=inventory).quotes:
                self.assertEqual(0, quote.price % self.book.tick_size)
                if quote.side == "BUY":
                    self.assertLess(quote.price, self.book.best_ask)
                else:
                    self.assertGreater(quote.price, self.book.best_bid)

    def test_bad_market_states_disable_quotes(self):
        states = (replace(self.book, ready=False), replace(self.book, age_seconds=31),
                  replace(self.book, age_seconds=float("nan")),
                  replace(self.book, trading_status="CANCEL_ONLY"),
                  replace(self.book, best_bid=D("0.62")),
                  replace(self.book, bid_amount=D(0)), replace(self.book, best_bid=D("NaN")),
                  replace(self.book, best_ask=D("0.6101")))
        for state in states:
            with self.subTest(state=state):
                self.assertFalse(self.plan(book=state).quotes)

    def test_invalid_configuration_is_rejected(self):
        for data in ({"half_spread": "NaN"}, {"refresh_seconds": "0"}, {"live": True},
                     {"max_inventory": "-1"}, {"target_inventory": "101"},
                     {"refresh_price_ticks": "1.5"}, {"preview_usdt": True}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                engine.QuoteConfig.from_dict(data)

    def test_preview_holds_refreshes_and_withdraws(self):
        state = engine.PreviewQuotes()
        plan = self.plan()
        self.assertEqual("QUOTE", state.update(plan, self.book, self.config, 0))
        self.assertEqual("HOLD", state.update(plan, self.book, self.config, 1))
        self.assertEqual("REPLACE", state.update(plan, self.book, self.config, 30))
        bad = self.plan(book=replace(self.book, trading_status="CLOSED"))
        self.assertEqual("WITHDRAW", state.update(bad, self.book, self.config, 31))
        self.assertEqual("SKIP", state.update(bad, self.book, self.config, 32))
        self.assertEqual("QUOTE", state.update(plan, self.book, self.config, 33))

    def test_preview_price_threshold_and_new_crossing(self):
        config = replace(self.config, refresh_price_ticks=D(10))
        state = engine.PreviewQuotes()
        state.update(self.plan(), self.book, config, 0)
        shifted = replace(self.book, best_bid=D("0.591"), best_ask=D("0.611"))
        self.assertEqual("HOLD", state.update(self.plan(book=shifted), shifted, config, 1))
        # A wide refresh threshold must not preserve a quote now crossing the book.
        crossing = replace(self.book, best_bid=D("0.58"), best_ask=D("0.59"))
        self.assertEqual("REPLACE", state.update(self.plan(book=crossing), crossing, config, 2))


class RunnerTests(unittest.TestCase):
    def setUp(self):
        outcome = SimpleNamespace(name="Team A", token_id="1231", trading_pair="PREDICT_123_1-USDT")
        market = SimpleNamespace(tick_size=D("0.001"), fee_rate_bps=100, outcome=lambda **kw: outcome)
        row = lambda price: SimpleNamespace(price=price, amount=100)
        book = SimpleNamespace(bid_entries=lambda: iter([row("0.59")]), ask_entries=lambda: iter([row("0.61")]))
        self.connector = SimpleNamespace(markets_by_id={123: market}, book_received_at={123: 100},
                                         ready=True, market_data_stream_connected=True, market_status={123: "OPEN"},
                                         get_order_book=lambda pair: book)
        # There are deliberately no buy/sell/cancel methods on this fake connector.
        self.preview = runner.PredictMarketMakingPreview(
            self.connector, 123, outcome, engine.QuoteConfig(), engine)

    def test_actual_name_and_simulated_balance_are_explicit(self):
        report = self.preview.step(now=101)
        self.assertEqual("Team A", report["outcome"])
        self.assertEqual("CONFIGURED_SIMULATION", report["balances"]["source"])
        self.assertEqual(["BUY"], [q["side"] for q in report["target_quotes"]])
        self.assertEqual("QUOTE", report["action"])

    def test_ws_disconnect_withdraws_even_when_rest_book_exists(self):
        self.preview.step(now=101)
        self.connector.market_data_stream_connected = False
        self.assertEqual("WITHDRAW", self.preview.step(now=102)["action"])

    def test_stale_book_and_market_close_withdraw(self):
        self.preview.step(now=101)
        self.assertEqual("WITHDRAW", self.preview.step(now=132)["action"])
        self.connector.book_received_at[123] = 132
        self.preview.step(now=133)
        self.connector.market_status[123] = "CLOSED"
        self.assertEqual("WITHDRAW", self.preview.step(now=134)["action"])

    def test_example_config_loads(self):
        import json
        path = ROOT / "conf/scripts/predict_market_maker.example.json"
        config = engine.QuoteConfig.from_dict(json.loads(path.read_text(encoding="utf-8")))
        self.assertEqual(D(50), config.preview_shares)


if __name__ == "__main__":
    unittest.main()
