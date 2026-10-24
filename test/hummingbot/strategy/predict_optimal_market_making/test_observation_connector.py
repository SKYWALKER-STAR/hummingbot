"""Verify strategy-local hooks without importing compiled Hummingbot extensions."""
from dataclasses import dataclass, replace
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch


class BaseExchange:
    def __init__(self):
        self.markets_by_id = {}
        self._order_tracker = NS(all_updatable_orders={})
        self._uncertain_submissions = set()
        self._trading_pairs = []
        self._web_assistants_factory = None

    def _install_market(self,market):
        self.markets_by_id[market.market_id] = market

    async def _place_order(self,*args,**kwargs):
        return 'hash',0


class BaseDataSource:
    def __init__(self,pairs,connector,factory):
        self.connector = connector

    async def _on_order_stream_interruption(self,*args):
        self.interrupted = True


spec = importlib.util.spec_from_file_location('optimal_connector_hooks',Path('hummingbot/strategy/predict_optimal_market_making/connector.py'))
hooks = importlib.util.module_from_spec(spec)
with patch.dict(sys.modules,{
    'hummingbot.connector.exchange.predict.predict_exchange':NS(PredictExchange=BaseExchange),
    'hummingbot.connector.exchange.predict.predict_api_order_book_data_source':NS(PredictAPIOrderBookDataSource=BaseDataSource),
}):
    spec.loader.exec_module(hooks)


@dataclass(frozen=True)
class Market:
    market_id: int = 123
    decimal_precision: int = 2
    fee_rate_bps: int = 0
    is_neg_risk: bool = False
    is_yield_bearing: bool = False


class ObservationConnectorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.events = []
        recorder = NS(require_healthy=lambda:None,emit=lambda kind,**data:self.events.append((kind,data)))
        obs = NS(status=lambda *a:self.events.append(('status',a)),gap=lambda reason:self.events.append(('gap',reason)))
        self.c = hooks.RecordingPredictExchange(recorder=recorder,observations=obs)

    async def test_ack_seeds_exposure_but_uncertainty_does_not(self):
        order = NS(executed_amount_base=0)
        self.c._order_tracker.all_updatable_orders['a'] = order
        await self.c._place_order('a')
        self.assertEqual('status',self.events[-1][0])
        self.c._uncertain_submissions.add('hash')
        self.events.clear()
        await self.c._place_order('a')
        self.assertEqual('submission_uncertain',self.events[-1][0])

    async def test_market_rule_change_halts_submission(self):
        self.c._install_market(Market())
        self.c._install_market(replace(Market(),decimal_precision=3))
        self.assertTrue(self.c.metadata_halt)
        with self.assertRaises(RuntimeError):
            await self.c._place_order('a')

    async def test_interruption_invalidates_exposure_immediately(self):
        ds = self.c._create_order_book_data_source()
        await ds._on_order_stream_interruption()
        self.assertIn(('gap','market_stream_interruption'),self.events)
        self.assertTrue(ds.interrupted)
