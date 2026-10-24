import gzip
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

from scripts.analyze_predict_reports import Analysis, analyze, documents


def event(kind, data, sequence, session='a'):
    return dict(schema_version=1, session_id=session, sequence=sequence, market_id=123,
                received_utc_ns=1700000000000000000+sequence*1000000000, kind=kind, data=data)


class AnalysisTests(unittest.TestCase):
    def test_archive_to_human_report_and_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root/'raw'/'date=2026-01-01'/'market_id=123'/'events-00-a.jsonl.gz'
            archive.parent.mkdir(parents=True)
            events = [
                event('exposure', dict(side='BUY', seconds=10, filled_volume=2, count=.4, eligible=True), 1),
                event('exposure', dict(side='BUY', seconds=20, filled_volume=3, count=.6, eligible=False), 2),
                event('decision', dict(action='SKIP', plan=dict(reason='stale_book')), 3),
            ]
            with gzip.open(archive, 'wt', encoding='utf-8') as handle:
                for row in events:
                    handle.write(json.dumps(row)+'\n')
            result = analyze([root], root/'analysis')
            exposure = result['exposures'][0]
            self.assertEqual(2, exposure['windows'])
            self.assertEqual(10, exposure['eligible_seconds'])
            self.assertEqual(.2, exposure['filled_shares_per_second'])
            self.assertEqual({'stale_book': 1}, result['decision_reasons'])
            self.assertTrue((root/'analysis'/'summary.md').exists())
            self.assertIn('BUY', (root/'analysis'/'exposure_summary.csv').read_text(encoding='utf-8-sig'))
            self.assertEqual(3, len((root/'analysis'/'preview.jsonl').read_text().splitlines()))

    def test_observed_fills_deduplicate_without_recovery_history(self):
        a = Analysis()
        for sequence, (session, cumulative, incremental) in enumerate([
            ('a',3,None), ('a',5,2), ('a',5,2), ('b',6,None), ('b',7,1)
        ], 1):
            row = event('order_status', dict(order_id='same', token_id='1', side='BUY',
                cumulative_filled=cumulative, incremental_filled=incremental, price='.5', state='OPEN'), sequence, session)
            a.add(row, 'fixture')
        result = a.result()
        self.assertEqual(3, sum(r['observed_filled_shares'] for r in result['orders']))
        self.assertEqual(1.5, sum(r['estimated_notional'] for r in result['orders']))
        self.assertEqual(2, len(result['orders']))

    def test_json_calibration_and_simulation_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root/'report.json'
            report.write_text(json.dumps(dict(status='candidate_validated', windows=100,
                old_validation_score=-20, new_validation_score=-10)), encoding='utf-8')
            simulation = root/'simulation.json'
            simulation.write_text(json.dumps(dict(optimal=dict(mean_pnl=10), myopic=dict(mean_pnl=11))), encoding='utf-8')
            result = analyze([report, simulation], root/'output')
            self.assertEqual(10, result['calibrations'][0]['score_improvement'])
            self.assertEqual(1, len(result['simulations']))
            with self.assertRaisesRegex(ValueError, 'empty'):
                analyze([report], root/'output')

    def test_parquet_payload_expands_in_batches(self):
        row = event('decision', {}, 1)
        row.pop('data')
        row['payload_json'] = json.dumps(dict(action='HOLD', plan={}))
        fake = NS(ParquetFile=lambda path: NS(iter_batches=lambda batch_size: iter([
            NS(to_pylist=lambda: [dict(row)])])))
        with patch.dict('sys.modules', {'pyarrow': NS(parquet=fake), 'pyarrow.parquet': fake}):
            result = list(documents(Path('example.parquet')))
        self.assertEqual('HOLD', result[0]['data']['action'])

    def test_corrupt_archive_and_nonfinite_reports_fail_explicitly(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            broken = root/'bad.jsonl.gz'
            broken.write_bytes(b'not gzip')
            with self.assertRaisesRegex(ValueError, 'bad.jsonl.gz'):
                list(documents(broken))
            report = root/'bad.json'
            report.write_text('{"value": 1e999}', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'Nonfinite'):
                list(documents(report))

    def test_overlapping_inputs_and_duplicate_events_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root/'report.json'
            report.write_text('{"status":"insufficient_data","windows":0}', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'Overlapping'):
                analyze([report, report], root/'out')
        a = Analysis()
        row = event('decision', dict(action='HOLD'), 1)
        a.add(row, 'first')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            a.add(row, 'duplicate')

    def test_market_filter_does_not_attribute_unlabelled_report(self):
        a = Analysis(market_id=124)
        a.add(event('decision', dict(action='HOLD'), 1), 'fixture')
        a.add(dict(status='insufficient_data', windows=0), 'report')
        self.assertEqual(2, a.filtered)
        self.assertFalse(a.events)
