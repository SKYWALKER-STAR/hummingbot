import gzip
import json
import queue
import tempfile
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

import numpy as np

from hummingbot.strategy.predict_optimal_market_making.calibration import fit
from hummingbot.strategy.predict_optimal_market_making.config import StrategyConfig
from hummingbot.strategy.predict_optimal_market_making.model import ModelConfig, intensity
from hummingbot.strategy.predict_optimal_market_making.recorder import Observations, Recorder


class DataTests(unittest.TestCase):
    def test_recorder_gzip_sequence_and_overflow(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = Recorder(directory,123)
            recorder.emit('test',value='1')
            recorder.emit('test',value='2')
            recorder.close()
            path = next(Path(directory).glob('raw/date=*/market_id=123/*.gz'))
            with gzip.open(path,'rt',encoding='utf-8') as handle:
                rows = [json.loads(line) for line in handle]
            self.assertEqual([1,2],[r['sequence'] for r in rows])
            self.assertEqual(123,rows[0]['market_id'])
            self.assertEqual(rows[0]['session_id'],rows[1]['session_id'])
        with tempfile.TemporaryDirectory() as directory:
            recorder = Recorder(directory,123)
            with patch.object(recorder.queue,'put_nowait',side_effect=queue.Full):
                self.assertFalse(recorder.emit('test'))
            with self.assertRaises(RuntimeError):
                recorder.require_healthy()
            with self.assertRaises(RuntimeError):
                recorder.close()

    def test_write_failure_is_sticky(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch('hummingbot.strategy.predict_optimal_market_making.recorder.gzip.open',side_effect=OSError):
                recorder = Recorder(directory,123)
                recorder.emit('test')
                recorder.thread.join(timeout=2)
                self.assertIn('recorder_write_failed',recorder.error)
                with self.assertRaises(RuntimeError):
                    recorder.close()

    def test_exposure_baseline_partial_fill_dedup_and_disconnect(self):
        events = []
        obs = Observations(NS(emit=lambda kind,**data:events.append((kind,data))),'1',5)
        obs.mid,obs.book_at = .5,100.
        order = NS(client_order_id='a',exchange_order_id='hash',trading_pair='UP',
                   trade_type=NS(name='BUY'),price='.48',amount='5')
        module = 'hummingbot.strategy.predict_optimal_market_making.recorder.time'
        with patch(module+'.monotonic',return_value=100.):
            obs.status(order,'OPEN',0)
        with patch(module+'.monotonic',return_value=102.):
            obs.status(order,'PARTIALLY_FILLED',2)
        self.assertEqual(.4,obs.exposures[0]['count'])
        with patch(module+'.monotonic',return_value=103.):
            obs.status(order,'PARTIALLY_FILLED',2)
        self.assertEqual(1,len(obs.exposures)) # Remaining 3 is not a full fixed-size order.
        self.assertEqual(0,events[-1][1]['incremental_filled'])
        obs.gap('disconnected')
        with patch(module+'.monotonic',return_value=104.):
            obs.status(order,'CANCELED',3)
        self.assertEqual(1,len(obs.exposures))
        self.assertFalse(obs.active)
        self.assertEqual(1,events[-1][1]['incremental_filled'])
        with patch(module+'.monotonic',return_value=105.):
            obs.status(order,'FILLED',5)
        self.assertIsNone(events[-1][1]['incremental_filled'])

    def test_strict_configuration_and_timezone(self):
        with self.assertRaises(ValueError):
            StrategyConfig('2026-01-01','2026-02-01','2026-01-31')
        with self.assertRaises(ValueError):
            StrategyConfig('2026-01-01T00:00:00Z','2026-02-01T00:00:00Z','2026-01-31T00:00:00Z',order_ttl=60)


class CalibrationTests(unittest.TestCase):
    def samples(self):
        baseline = ModelConfig(a0=500,a1=0,k0=50,k1=50)
        truth = replace(baseline,a0=1200,k0=28,k1=28)
        rows = []
        for i in range(500):
            p = .45+.05*np.sin(i)
            side = 'BUY' if i%2 else 'SELL'
            offset = .005+.005*(i%7)
            price = p-offset if side=='BUY' else p+offset
            y = float(intensity(truth,i/500,p,price,side))/1000
            rows.append(dict(utc=i*2+2,start_utc=i*2+1,seconds=1.,mid=p,price=price,side=side,
                             count=y,filled_volume=y*10,eligible=True))
        return baseline,rows

    def test_fit_holdout_recovers_known_stationary_parameters(self):
        baseline,rows = self.samples()
        candidate,report = fit(baseline,[],rows,0,1002,min_fills=10)
        self.assertIsNotNone(candidate,report)
        self.assertEqual('candidate_validated',report['status'])
        self.assertLess(abs(candidate.k0-28),2)
        self.assertLess(abs(candidate.a0-1200),25)
        self.assertGreater(report['new_validation_score'],report['old_validation_score'])

    def test_sparse_samples_and_bad_holdout_never_promote(self):
        baseline,rows = self.samples()
        candidate,report = fit(baseline,[],rows[:20],0,1002)
        self.assertIsNone(candidate)
        for row in rows[350:]:
            row['count'] = row['filled_volume'] = 0
        candidate,report = fit(baseline,[],rows,0,1002,min_fills=10)
        self.assertIsNone(candidate)

    def test_raw_archive_to_offline_candidate_pipeline(self):
        from scripts.predict_optimal_market_maker import offline_calibration
        baseline,rows = self.samples()
        cfg = StrategyConfig('1970-01-01T00:00:00Z','1970-01-01T00:16:42Z','1970-01-01T00:16:41Z',
            model=replace(baseline,q_min=0,q_max=20,trade_size=5,time_steps=10,price_steps=10,quote_steps=30),
            calibration_min_fills=10)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config_path = root/'config.json'
            config_path.write_text(json.dumps(asdict(cfg)),encoding='utf-8')
            recorder = Recorder(root,123)
            recorder.emit('session_start',mode='LIVE',token_id='1',config=asdict(cfg))
            for row in rows:
                recorder.emit('exposure',**row)
            recorder.close()
            output = root/'result'/'report.json'
            with patch('builtins.print'):
                offline_calibration(NS(config=config_path,data_dir=root,market_id=123,token_id='1',output=output))
            self.assertEqual('candidate_validated',json.loads(output.read_text())['status'])
            self.assertTrue(output.with_suffix('.npz').exists())
            wrong = root/'wrong.json'
            with patch('builtins.print'):
                offline_calibration(NS(config=config_path,data_dir=root,market_id=123,token_id='2',output=wrong))
            self.assertEqual('insufficient_data',json.loads(wrong.read_text())['status'])
