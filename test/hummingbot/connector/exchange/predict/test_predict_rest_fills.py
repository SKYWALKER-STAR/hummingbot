"""Exercise the real status method without the compiled Hummingbot runtime."""
import ast
import time
import unittest
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

from core_loader import ROOT, load


class State(Enum):
    OPEN = 1
    FILLED = 2
    CANCELED = 3
    FAILED = 4
    PARTIALLY_FILLED = 5


path = ROOT / "hummingbot/connector/exchange/predict/predict_exchange.py"
tree = ast.parse(path.read_text(encoding="utf-8"))
cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PredictExchange")
methods = [n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name in
           {"_request_order_status", "_all_trade_updates_for_order"}]
scope = dict(Decimal=Decimal, time=time, OrderState=State, OrderType=NS(LIMIT_MAKER="maker"),
             TradeUpdate=NS, OrderUpdate=NS, AddedToCostTradeFee=NS,
             decimal_value=load("predict_market").decimal_value)
exec(compile(ast.Module(body=methods, type_ignores=[]), str(path), "exec"), scope)


class RESTFillTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.order = NS(get_exchange_order_id=AsyncMock(return_value="hash"),
                        executed_amount_base=Decimal(0), amount=Decimal(10),
                        price=Decimal(".6"), trading_pair="pair", client_order_id="client",
                        order_type="maker")
        self.updates = []

        def apply(update):
            self.updates.append(update)
            self.order.executed_amount_base += update.fill_base_amount

        self.connector = NS(client=NS(request=AsyncMock()), _uncertain_submissions={"hash"},
                            _fill_gaps=set(), _pending_settlements=set(),
                            outcome_for_pair=lambda pair: NS(market_id=1),
                            markets_by_id={1: NS(fee_rate_bps=0)},
                            _order_tracker=NS(process_trade_update=apply), _signer=Mock())

    async def status(self, amount, status="OPEN"):
        self.connector.client.request.return_value = {
            "data": {"amountFilled": str(Decimal(amount) * 10**18), "status": status}}
        return await scope["_request_order_status"](self.connector, self.order)

    async def test_partial_duplicate_and_cancel(self):
        self.assertEqual(State.PARTIALLY_FILLED, (await self.status(3)).new_state)
        await self.status(3)
        self.assertEqual(1, len(self.updates))
        self.assertEqual(State.CANCELED, (await self.status(5, "CANCELLED")).new_state)
        self.assertEqual([Decimal(3), Decimal(2)], [u.fill_base_amount for u in self.updates])

    async def test_full_fill_and_inconsistent_terminal(self):
        self.assertEqual(State.OPEN, (await self.status(0, "FILLED")).new_state)
        self.assertIn("hash", self.connector._fill_gaps)
        self.assertEqual(State.FILLED, (await self.status(10, "FILLED")).new_state)
        self.assertFalse(self.connector._fill_gaps)

    async def test_invalid_quantity_does_not_apply_fill(self):
        with self.assertRaises(ValueError):
            await self.status(11)
        self.assertFalse(self.updates)

    async def test_no_chain_log_query(self):
        self.assertEqual([], await scope["_all_trade_updates_for_order"](self.connector, self.order))
        self.connector._signer.confirmed_fills.assert_not_called()
