import time
import unittest
from dataclasses import replace
from decimal import Decimal as D
from enum import Enum
from types import SimpleNamespace as NS

from hummingbot.strategy.predict_optimal_market_making.config import StrategyConfig
from hummingbot.strategy.predict_optimal_market_making.execution import Maker, plan, reject_unknown_orders
from hummingbot.strategy.predict_optimal_market_making.model import HJBTable, ModelConfig


class Side(Enum):
    BUY = 1
    SELL = 2


class Connector:
    def __init__(self):
        self.ready = self.market_data_stream_connected = self.account_reconciled = True
        self.outcome = NS(token_id='1',base_asset='UP',trading_pair='UP-USDT')
        self.other = NS(token_id='2',base_asset='DOWN',trading_pair='DOWN-USDT')
        self.market = NS(market_id=123,outcomes=[self.outcome,self.other],tick_size=D('.01'),fee_rate_bps=0)
        self.market_status = {123:'OPEN'}
        self.book_received_at = {123:time.monotonic()}
        self._order_tracker = NS(all_updatable_orders={})
        self.balances = {'UP':D(5),'DOWN':D(0),'USDT':D(100)}
        self.prices = (.49,.51)
        self.events = []
        self.confirm = self.reject = False
        self.cancelled = set()

    @property
    def tracking_states(self):
        return {k:{'hash':v.exchange_order_id} for k,v in self._order_tracker.all_updatable_orders.items()}

    def get_balance(self,asset):
        return self.balances[asset]

    get_available_balance = get_balance

    def get_order_book(self,pair):
        return NS(bid_entries=lambda:iter([NS(price=self.prices[0],amount=100)]),
                  ask_entries=lambda:iter([NS(price=self.prices[1],amount=100)]))

    async def _status_polling_loop_fetch_updates(self):
        if self.confirm:
            for key in self.cancelled:
                self._order_tracker.all_updatable_orders.pop(key,None)

    async def _execute_cancel(self,pair,oid):
        self.events.append(('cancel',oid))
        self.cancelled.add(oid)

    async def _create_order(self,**kw):
        self.events.append(('submit',kw))
        if self.reject:
            return
        self._order_tracker.all_updatable_orders[kw['order_id']] = NS(
            client_order_id=kw['order_id'],exchange_order_id='hash'+kw['order_id'],
            trading_pair=kw['trading_pair'],trade_type=kw['trade_type'],price=kw['price'],amount=kw['amount'],
            executed_amount_base=D(0),current_state=NS(name='OPEN'))


def config():
    return StrategyConfig(start_utc='2020-01-01T00:00:00Z',resolution_utc='2040-01-01T00:00:00Z',
                          trading_cutoff_utc='2039-12-31T23:55:00Z',
                          model=ModelConfig(q_min=0,q_max=20,trade_size=5,time_steps=12,price_steps=20,quote_steps=100,a0=10,a1=0))


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.cfg = config()
        cls.table = HJBTable.solve(cls.cfg.model)

    def setUp(self):
        self.c = Connector()
        self.saved = []
        self.recorder = NS(error=None,emit=lambda *a,**k:True,require_healthy=lambda:None)
        self.maker = Maker(self.c,self.c.market,self.c.outcome,self.cfg,self.table,
                           NS(save=lambda x:self.saved.append(x)),self.recorder,NS(LIMIT_MAKER='post_only'),Side)

    def test_budget_inventory_and_tick_prices(self):
        p = plan(self.c,self.c.market,self.c.outcome,self.cfg,self.table)
        self.assertEqual({'BUY','SELL'},{q['side'] for q in p['quotes']})
        self.assertTrue(all(q['price']%D('.01')==0 and q['amount']==D(5) for q in p['quotes']))
        self.c.balances.update(UP=D(0),USDT=D(0))
        self.assertFalse(plan(self.c,self.c.market,self.c.outcome,self.cfg,self.table)['quotes'])
        self.c.balances.update(UP=D(20),USDT=D(100))
        self.assertEqual(['SELL'],[q['side'] for q in plan(self.c,self.c.market,self.c.outcome,self.cfg,self.table)['quotes']])
        self.c.balances['UP'] = D(1)
        self.assertNotIn('SELL',[q['side'] for q in plan(self.c,self.c.market,self.c.outcome,self.cfg,self.table)['quotes']])

    async def test_cancel_before_replacement_and_partial_fill(self):
        self.assertEqual('SUBMITTED',(await self.maker.step())['action'])
        self.assertEqual('HOLD',(await self.maker.step())['action'])
        next(iter(self.maker.orders().values())).executed_amount_base = D(1)
        self.c.events.clear()
        self.assertEqual('WAIT_CANCEL_CONFIRMATION',(await self.maker.step())['action'])
        self.assertFalse(any(e[0]=='submit' for e in self.c.events))
        self.c.confirm = True
        self.assertEqual('SUBMITTED',(await self.maker.step())['action'])

    async def test_disconnect_and_stop_timeout_preserve_states(self):
        await self.maker.step()
        self.c.market_data_stream_connected = False
        self.c.events.clear()
        await self.maker.step()
        self.assertFalse(any(e[0]=='submit' for e in self.c.events))
        with self.assertRaises(TimeoutError):
            await self.maker.stop(timeout=.01)
        self.assertTrue(self.saved[-1])
        self.c.confirm = True
        await self.maker.stop(timeout=1)
        self.assertFalse(self.maker.orders())

    async def test_recording_failure_and_breaker_withdraw(self):
        await self.maker.step()
        self.recorder.error = 'disk_failure'
        self.c.events.clear()
        await self.maker.step()
        self.assertFalse(any(e[0]=='submit' for e in self.c.events))
        self.assertTrue(any(e[0]=='cancel' for e in self.c.events))

    def test_closed_cutoff_and_unknown_open_orders(self):
        self.assertFalse(plan(self.c,self.c.market,self.c.outcome,self.cfg,self.table,now=self.cfg.cutoff)['quotes'])
        self.c.market_status[123] = 'CLOSED'
        self.assertFalse(plan(self.c,self.c.market,self.c.outcome,self.cfg,self.table)['quotes'])
        remote = [{'status':'OPEN','order':{'tokenId':'1','hash':'ABC'}}]
        reject_unknown_orders(remote,{'abc'},{'1','2'})
        with self.assertRaises(RuntimeError):
            reject_unknown_orders(remote,set(),{'1','2'})

    async def test_model_change_forces_cancel(self):
        await self.maker.step()
        self.maker.table = HJBTable.solve(replace(self.cfg.model,a0=11))
        self.assertEqual('WAIT_CANCEL_CONFIRMATION',(await self.maker.step())['action'])
