import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np

from hummingbot.strategy.predict_optimal_market_making.model import HJBTable, ModelConfig, intensity, simulate


class ModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = ModelConfig(q_min=-10,q_max=10,trade_size=5,time_steps=12,price_steps=20,quote_steps=100,a0=10,a1=5)
        cls.table = HJBTable.solve(cls.c)

    def test_terminal_and_inventory_boundaries(self):
        c,table = self.c,self.table
        p,q = table.prices[:,None],table.inventories[None,:]
        np.testing.assert_allclose(table.values[-1],-c.gamma_terminal*q*q*p*(1-p))
        candidates = np.linspace(.01,.99,99)
        self.assertIsNone(table.quote(.5,.5,c.q_max,'BUY',candidates))
        self.assertIsNone(table.quote(.5,.5,c.q_min,'SELL',candidates))
        with self.assertRaises(ValueError):
            table.value(.5,.5,c.q_max+1)

    def test_complement_symmetry_and_inventory_skew(self):
        table = self.table
        np.testing.assert_allclose(table.values,table.values[:,::-1,::-1],atol=1e-7)
        prices = np.linspace(.01,.99,99)
        b0 = table.quote(.4,.5,0,'BUY',prices)
        a0 = table.quote(.4,.5,0,'SELL',prices)
        self.assertAlmostEqual(b0+a0,1,places=8)
        self.assertLessEqual(table.quote(.4,.5,5,'BUY',prices),b0)
        self.assertLessEqual(table.quote(.4,.5,5,'SELL',prices),a0)

    def test_boundary_intensities_and_myopic_terminal(self):
        c = self.c
        self.assertEqual(0,intensity(c,.5,.5,0.,'BUY'))
        self.assertEqual(0,intensity(c,.5,.5,1.,'SELL'))
        table = HJBTable.solve(replace(c,gamma=0,gamma_terminal=0))
        px = np.linspace(.01,.99,99)
        score = intensity(c,1.,.4,px,'BUY')*(.4-px)
        self.assertEqual(float(px[np.argmax(score)]),table.quote(1.,.4,0,'BUY',px))

    def test_artifact_roundtrip_and_deterministic_simulation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'table.npz'
            self.table.save(path)
            table = HJBTable.load(path)
            self.assertEqual(self.c,table.config)
            np.testing.assert_array_equal(self.table.values,table.values)
        a = simulate(self.table,paths=10,steps=20,seed=1)
        self.assertEqual(a,simulate(self.table,paths=10,steps=20,seed=1))
        self.assertTrue(np.isfinite(a['mean_pnl']))

    def test_refinement_and_validation(self):
        finer = HJBTable.solve(replace(self.c,time_steps=24,price_steps=40,quote_steps=200))
        self.assertLess(abs(finer.value(.5,.5,0)-self.table.value(.5,.5,0)),.2)
        for change in (dict(sigma0=float('nan')),dict(time_steps=2.5),dict(q_max=11),dict(nu=0)):
            with self.assertRaises(ValueError):
                replace(self.c,**change)

    def test_vectorized_interpolation_matches_scalar(self):
        p,q = np.array([.2,.5,.7]),np.array([-5.,0.,5.])
        np.testing.assert_allclose(self.table.value_many(.4,p,q),[self.table.value(.4,a,b) for a,b in zip(p,q)])
        candidates = np.linspace(.01,.99,99)
        for side in ('BUY','SELL'):
            np.testing.assert_allclose(self.table.quote_many(.4,p,q,side,candidates),
                                       [self.table.quote(.4,a,b,side,candidates) for a,b in zip(p,q)])
