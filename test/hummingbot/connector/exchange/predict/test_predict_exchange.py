"""Run in a compiled Hummingbot environment; no live credentials or orders."""
import time
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

from test_predict_core import market_data

try:
    from hummingbot.connector.exchange.predict.predict_exchange import PredictExchange
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
    RUNTIME_ERROR = None
except (ImportError, OSError) as exc:
    RUNTIME_ERROR = str(exc)


@unittest.skipIf(RUNTIME_ERROR is not None, f"Compiled Hummingbot environment required: {RUNTIME_ERROR}")
class ExchangeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange = PredictExchange(market_ids=[123], trading_required=False)
        self.exchange.client.request = AsyncMock(return_value={"data": market_data()})
        await self.exchange.initialize_markets()

    async def asyncTearDown(self):
        await self.exchange.client.close()

    def order(self, state=None):
        state = OrderState.OPEN if state is None else state
        order = InFlightOrder(client_order_id="test", exchange_order_id="0xhash",
                              trading_pair="PREDICT_123_1-USDT", order_type=OrderType.LIMIT_MAKER,
                              trade_type=TradeType.BUY, amount=D(10), price=D("0.6"),
                              creation_timestamp=time.time(), initial_state=state)
        self.exchange._order_tracker.start_tracking_order(order)
        return order

    async def test_restart_discards_only_unsubmitted_hashless_checkpoint(self):
        order = self.order(OrderState.PENDING_CREATE)
        state = order.to_json()
        self.exchange._order_tracker.stop_tracking_order(order.client_order_id)
        state["exchange_order_id"] = None
        self.exchange.restore_tracking_states({"test": state})
        self.assertFalse(self.exchange._order_tracker.all_updatable_orders)
        self.assertFalse(self.exchange.account_reconciled)
        state["exchange_order_id"] = "0xhash"
        self.exchange.restore_tracking_states({"test": state})
        self.assertIn("test", self.exchange._order_tracker.all_updatable_orders)
        self.assertIn("0xhash", self.exchange._uncertain_submissions)

    async def test_restart_rejects_ambiguous_hashless_checkpoint(self):
        order = self.order()
        state = order.to_json()
        state["exchange_order_id"] = None
        with self.assertRaises(ValueError):
            self.exchange.restore_tracking_states({"test": state})

    async def test_market_id_loading_and_real_name_selection(self):
        self.assertEqual("PREDICT_123_2-USDT", self.exchange.get_trading_pair(123, outcome_name="Team B"))
        self.assertEqual(2, len(self.exchange.trading_pairs))
        self.exchange.client.request.assert_awaited_once_with("GET", "/v1/markets/123")
        self.exchange.client.request.return_value = {"data": market_data(456, ("Above", "Below"))}
        await self.exchange.add_market(456)
        self.assertEqual(4, len(self.exchange.trading_pairs))
        await self.exchange.remove_market(456)
        self.assertEqual(2, len(self.exchange.trading_pairs))

    async def test_metadata_rename_preserves_asset_and_token_change_is_rejected(self):
        self.exchange.client.request.return_value = {"data": market_data(names=("New A", "New B"))}
        await self.exchange.refresh_market(123)
        self.assertEqual("PREDICT_123_1-USDT", self.exchange.get_trading_pair(123, outcome_name="New A"))
        changed = market_data()
        changed["outcomes"][0]["onChainId"] = "999"
        self.exchange.client.request.return_value = {"data": changed}
        with self.assertRaises(ValueError):
            await self.exchange.refresh_market(123)
        self.assertEqual("IDENTITY_CHANGED", self.exchange.market_status[123])

    async def test_cannot_remove_market_with_pending_order(self):
        self.order()
        with self.assertRaises(ValueError):
            await self.exchange.remove_market(123)

    async def test_matched_but_unsettled_order_stays_tracked(self):
        order = self.order()
        self.exchange.client.request.return_value = {"data": {"status": "FILLED", "amountFilled": str(10 * 10**18)}}
        update = await self.exchange._request_order_status(order)
        self.assertEqual(OrderState.OPEN, update.new_state)
        self.assertIn("0xhash", self.exchange._fill_gaps)
        order.executed_amount_base = D(10)
        update = await self.exchange._request_order_status(order)
        self.assertEqual(OrderState.FILLED, update.new_state)
        self.assertFalse(self.exchange._fill_gaps)

    async def test_reserves_remote_and_local_orders_without_double_counting(self):
        self.order()
        self.exchange._signer = Mock()
        self.exchange._signer.collateral_balance = AsyncMock(return_value=D(100))
        self.exchange._signer.shares_balance = Mock(return_value=D(20))
        self.exchange.client.pages = AsyncMock(return_value=[{
            "status": "OPEN", "order": {"hash": "0xhash", "side": 0, "makerAmount": str(6 * 10**18),
                                        "feeRateBps": "100", "tokenId": "1231"}}])
        await self.exchange._update_balances()
        self.assertEqual(D("93.94"), self.exchange._account_available_balances["USDT"])

    async def test_cancel_noop_does_not_report_success(self):
        order = self.order()
        self.exchange.client.request.side_effect = [{"data": {"id": "42", "status": "OPEN"}},
                                                    {"success": True, "removed": [], "noop": ["42"]}]
        self.assertFalse(await self.exchange._place_cancel("test", order))

    async def test_submission_timeout_keeps_hash_and_blocks_new_orders(self):
        import asyncio
        order = self.order(OrderState.PENDING_CREATE)
        self.exchange.account_reconciled = True
        self.exchange.book_received_at[123] = time.monotonic()
        self.exchange._account_available_balances["USDT"] = D(90)
        self.exchange._update_balances = AsyncMock()
        self.exchange._signer = Mock()
        self.exchange._signer.sign_order.return_value = {"data": {"order": {"hash": "0xhash"}}}
        self.exchange.client.request.side_effect = asyncio.TimeoutError
        self.exchange.client.request.reset_mock()
        with patch.object(PredictExchange, "ready", new_callable=PropertyMock, return_value=True):
            order_hash, _ = await self.exchange._place_order(
                "test", order.trading_pair, order.amount, order.trade_type, order.order_type, order.price)
        self.assertEqual("0xhash", order_hash)
        self.assertIn(order_hash, self.exchange._uncertain_submissions)
        self.assertEqual(1, self.exchange.client.request.await_count)

    async def test_rpc_failure_does_not_mark_account_reconciled(self):
        self.order()
        self.exchange.account_reconciled = True
        self.exchange._all_trade_updates_for_order = AsyncMock(side_effect=IOError("RPC unavailable"))
        with self.assertRaises(IOError):
            await self.exchange._status_polling_loop_fetch_updates()
        self.assertFalse(self.exchange.account_reconciled)


if __name__ == "__main__":
    unittest.main()
