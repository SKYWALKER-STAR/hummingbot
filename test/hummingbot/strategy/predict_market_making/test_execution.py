import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
from decimal import Decimal as D
from enum import Enum
from pathlib import Path
from types import SimpleNamespace as NS

from test_quote_engine import engine, load, runner

execution = load("predict_execution_test", "hummingbot/strategy/predict_market_making/execution.py")


class Side(Enum):
    BUY = 1
    SELL = 2


class SelectionTests(unittest.TestCase):
    def test_strict_threshold_and_invalid_books(self):
        cfg = engine.QuoteConfig()
        a = engine.BookState(D(".59"), D(100), D(".61"), D(100), D(".01"), 0)
        b = replace(a, best_bid=D(".39"), best_ask=D(".41"))
        select = lambda books: execution.select_outcome(books, D(".5"), cfg, engine)
        self.assertEqual("up", select({"up": a, "down": b}))
        self.assertIsNone(select({"up": replace(a, best_bid=D(".49"), best_ask=D(".51"))}))
        for invalid in (replace(a, ready=False), replace(a, age_seconds=31),
                        replace(a, trading_status="CLOSED"), replace(a, best_bid=D(".62"))):
            self.assertIsNone(select({"up": invalid, "down": b}))

    def test_highest_mid_and_stable_tie(self):
        book = engine.BookState(D(".59"), D(100), D(".61"), D(100), D(".01"), 0)
        self.assertEqual("a", execution.select_outcome({"a": book, "b": book}, D(".5"), engine.QuoteConfig(), engine, "a"))

    def test_cli_explicit_live_and_threshold_validation(self):
        args = runner.parse_args(["--market-id", "123", "--auto-select"])
        self.assertFalse(args.live)
        self.assertEqual(D(".5"), args.threshold)
        with self.assertRaises(SystemExit):
            runner.parse_args(["--market-id", "123", "--auto-select", "--live"])
        with self.assertRaises(SystemExit):
            runner.parse_args(["--market-id", "123", "--auto-select", "--threshold", "NaN"])


class StoreTests(unittest.TestCase):
    def test_checkpoint_identity_and_exclusive_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            store = execution.StateStore(path, {"account": "abc"})
            try:
                self.assertEqual({}, store.load())
                store.save({"order": {"exchange_order_id": "hash"}})
                self.assertEqual("hash", store.load()["order"]["exchange_order_id"])
                with self.assertRaises(RuntimeError):
                    execution.StateStore(path, {"account": "abc"})
            finally:
                store.close()
            reopened = execution.StateStore(path, {"account": "wrong"})
            try:
                with self.assertRaises(ValueError):
                    reopened.load()
            finally:
                reopened.close()


class FakeConnector:
    def __init__(self):
        self.outcomes = [NS(name="Up", token_id="1", trading_pair="up", base_asset="UP"),
                         NS(name="Down", token_id="2", trading_pair="down", base_asset="DOWN")]
        self.market = NS(market_id=123, outcomes=self.outcomes, tick_size=D(".01"), fee_rate_bps=0)
        self.ready = self.market_data_stream_connected = True
        self.market_status = {123: "OPEN"}
        self.book_received_at = {123: time.monotonic()}
        self.prices = {"up": (".59", ".61"), "down": (".39", ".41")}
        self.balances = {"USDT": D(100), "UP": D(10), "DOWN": D(0)}
        self._order_tracker = NS(all_updatable_orders={})
        self.events, self.cancelled = [], set()
        self.confirm_cancel = False

    @property
    def tracking_states(self):
        return {key: {"hash": order.exchange_order_id} for key, order in self._order_tracker.all_updatable_orders.items()}

    def outcome_for_pair(self, pair):
        return next(o for o in self.outcomes if o.trading_pair == pair)

    def get_balance(self, asset):
        return self.balances[asset]

    get_available_balance = get_balance

    def get_order_book(self, pair):
        bid, ask = self.prices[pair]
        return NS(bid_entries=lambda: iter([NS(price=bid, amount=100)]),
                  ask_entries=lambda: iter([NS(price=ask, amount=100)]))

    async def _status_polling_loop_fetch_updates(self):
        self.events.append("reconcile")
        if self.confirm_cancel:
            for key in self.cancelled:
                self._order_tracker.all_updatable_orders.pop(key, None)

    async def _create_order(self, trade_type, order_id, trading_pair, amount, order_type, price):
        self.events.append(("submit", trading_pair, trade_type.name, amount, order_type))
        self._order_tracker.all_updatable_orders[order_id] = NS(
            client_order_id=order_id, exchange_order_id="hash" + order_id, trading_pair=trading_pair,
            trade_type=trade_type, price=price, amount=amount, executed_amount_base=D(0))

    async def _execute_cancel(self, pair, order_id):
        self.events.append("cancel")
        self.cancelled.add(order_id)


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.c = FakeConnector()
        self.saved = []
        self.maker = execution.LiveMaker(self.c, self.c.market, engine.QuoteConfig(), engine,
                                         NS(save=lambda value: self.saved.append(value)),
                                         NS(LIMIT_MAKER="post_only"), Side)

    async def test_submits_selected_side_and_real_inventory_sell(self):
        report = await self.maker.step()
        self.assertEqual("Up", report["selected_outcome"])
        self.assertEqual("reconcile", self.c.events[0])
        submitted = [v for v in self.c.events if isinstance(v, tuple)]
        self.assertEqual([("up", "BUY"), ("up", "SELL")], [(v[1], v[2]) for v in submitted])
        self.assertTrue(all(v[4] == "post_only" for v in submitted))
        self.assertEqual("HOLD", (await self.maker.step())["action"])

    async def test_switch_waits_for_cancel_then_sells_old_inventory(self):
        await self.maker.step()
        self.c.prices = {"up": (".39", ".41"), "down": (".59", ".61")}
        self.c.events.clear()
        self.assertEqual("WAIT_CANCEL_CONFIRMATION", (await self.maker.step())["action"])
        self.assertFalse(any(isinstance(v, tuple) for v in self.c.events))
        self.c.confirm_cancel = True
        await self.c._status_polling_loop_fetch_updates()
        await self.maker.step()
        submitted = [v for v in self.c.events if isinstance(v, tuple)]
        self.assertEqual([("up", "SELL"), ("down", "BUY")], [(v[1], v[2]) for v in submitted])

    async def test_aggregate_inventory_cap_and_zero_inventory_sell(self):
        self.c.balances.update(UP=D(0), DOWN=D(99))
        books = execution.read_books(self.c, self.c.market, self.maker.config, engine)
        plan = self.maker.plans(books, "1")
        self.assertFalse(plan["1"].quotes)  # Remaining one share is below minimum notional.
        self.assertEqual(["SELL"], [q.side for q in plan["2"].quotes])

    async def test_no_qualifying_outcome_only_sells_holdings(self):
        self.c.prices = {"up": (".49", ".51"), "down": (".49", ".51")}
        report = await self.maker.step()
        self.assertIsNone(report["selected_outcome"])
        self.assertEqual(["SELL"], [v[2] for v in self.c.events if isinstance(v, tuple)])

    async def test_total_cash_does_not_hide_zero_available_cash(self):
        self.c.balances.update(UP=D(0), DOWN=D(0), USDT=D("61.16"))
        self.c.get_available_balance = lambda asset: D(0)
        report = await self.maker.step()
        self.assertEqual("61.16", report["balances"]["USDT"])
        self.assertEqual("0", report["balances"]["available_USDT"])
        self.assertEqual("1", report["limits"]["min_order_notional"])
        self.assertFalse(report["active_order_ids"])
        self.assertIn("buy_amount_below_minimum:1", report["plans"]["1"]["reasons"])
        self.assertEqual("SKIP", (await self.maker.step())["action"])

    async def test_small_order_notional_is_explained_without_upsizing(self):
        self.maker.config = replace(self.maker.config, order_amount=D(1))
        self.c.balances.update(UP=D(0), DOWN=D(0))
        report = await self.maker.step()
        self.assertIn("buy_notional_below_minimum:1", report["plans"]["1"]["reasons"])
        self.assertFalse(report["active_order_ids"])

    async def test_disconnect_cancels_and_never_replaces(self):
        await self.maker.step()
        self.c.market_data_stream_connected = False
        self.c.events.clear()
        self.assertEqual("WAIT_CANCEL_CONFIRMATION", (await self.maker.step())["action"])
        self.assertIn("cancel", self.c.events)
        self.assertFalse(any(isinstance(v, tuple) for v in self.c.events))

    async def test_order_event_reconciles_before_deciding_to_cancel(self):
        await self.maker.step()
        self.c.ready = self.c.account_reconciled = False
        reconcile = self.c._status_polling_loop_fetch_updates

        async def recovered():
            await reconcile()
            self.c.ready = self.c.account_reconciled = True

        self.c._status_polling_loop_fetch_updates = recovered
        self.c.events.clear()
        report = await self.maker.step()
        self.assertEqual("HOLD", report["action"])
        self.assertEqual("Up", report["selected_outcome"])
        self.assertEqual(["reconcile"], self.c.events)

    async def test_order_event_during_batch_does_not_force_replacement(self):
        submit = self.c._create_order
        reconcile = self.c._status_polling_loop_fetch_updates

        async def submitted(**kwargs):
            await submit(**kwargs)
            self.c.ready = self.c.account_reconciled = False

        async def recovered():
            await reconcile()
            self.c.ready = self.c.account_reconciled = True

        self.c._create_order = submitted
        self.c._status_polling_loop_fetch_updates = recovered
        await self.maker.step()
        self.assertEqual(2, len(self.maker.orders()))
        self.assertFalse(self.maker.replacing)
        self.assertEqual("HOLD", (await self.maker.step())["action"])
        self.assertNotIn("cancel", self.c.events)

    async def test_unresolved_account_still_cancels_after_reconciliation(self):
        await self.maker.step()
        self.c.ready = self.c.account_reconciled = False
        self.c.events.clear()
        self.assertEqual("WAIT_CANCEL_CONFIRMATION", (await self.maker.step())["action"])
        self.assertEqual("reconcile", self.c.events[0])
        self.assertIn("cancel", self.c.events)

    async def test_shutdown_confirms_and_persists(self):
        await self.maker.step()
        self.c.confirm_cancel = True
        await self.maker.stop(timeout=1)
        self.assertFalse(self.maker.orders())
        self.assertEqual({}, self.saved[-1])

    async def test_shutdown_timeout_preserves_unresolved_orders(self):
        await self.maker.step()
        with self.assertRaises(asyncio.TimeoutError):
            await self.maker.stop(timeout=.01)
        self.assertTrue(self.saved[-1])

    async def test_restart_drains_existing_orders_before_new_quotes(self):
        await self.maker.step()
        restarted = execution.LiveMaker(self.c, self.c.market, engine.QuoteConfig(), engine,
                                        NS(save=lambda value: None), NS(LIMIT_MAKER="post_only"), Side)
        self.c.events.clear()
        self.assertEqual("WAIT_CANCEL_CONFIRMATION", (await restarted.step())["action"])
        self.assertFalse(any(isinstance(v, tuple) for v in self.c.events))

    async def test_direction_changed_during_submission_stops_remaining_batch(self):
        submit = self.c._create_order

        async def changed(**kwargs):
            await submit(**kwargs)
            self.c.prices = {"up": (".39", ".41"), "down": (".59", ".61")}

        self.c._create_order = changed
        await self.maker.step()
        self.assertEqual(1, len([v for v in self.c.events if isinstance(v, tuple)]))
        self.assertTrue(self.maker.replacing)

    async def test_partial_fill_triggers_reconciliation_before_replacement(self):
        await self.maker.step()
        next(iter(self.maker.orders().values())).executed_amount_base = D(1)
        self.c.events.clear()
        self.assertEqual("WAIT_CANCEL_CONFIRMATION", (await self.maker.step())["action"])
        self.assertFalse(any(isinstance(v, tuple) for v in self.c.events))
