from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS
import unittest

from hummingbot.strategy.predict_optimal_market_making.config import StrategyConfig
from scripts.predict_optimal_market_maker import matches_active_context, parse_args, selected_config


class TokenConfigTests(unittest.TestCase):
    def config(self, **kwargs):
        return StrategyConfig('2026-01-01T00:00:00Z', '2026-02-01T00:00:00Z',
                              '2026-01-31T00:00:00Z', **kwargs)

    def test_large_string_roundtrip_and_command_line_override(self):
        token = '77489318756280456442431142622195838866105581084299227039359231359227028463535'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'live.json'
            path.write_text(json.dumps(asdict(self.config(token_id=token))), encoding='utf-8')
            args = NS(config=path, token_id=None)
            self.assertEqual(token, selected_config(args).token_id)
            self.assertEqual(token, args.token_id)
            args.token_id = '2'
            self.assertEqual('2', selected_config(args).token_id)
            self.assertEqual(token, json.loads(path.read_text())['token_id'])

    def test_old_command_line_only_config_and_missing_identity(self):
        cfg = self.config()
        self.assertEqual('1', cfg.resolve_token_id('1'))
        with self.assertRaisesRegex(ValueError, 'Provide token_id'):
            cfg.resolve_token_id()

    def test_invalid_values_are_rejected_without_numeric_conversion(self):
        for value in (123, True, '', '0', '-1', '1.0', '1e10', ' 12', '１２'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.config(token_id=value)

    def test_run_and_calibrate_accept_omitted_cli_token(self):
        for command in ('run', 'calibrate'):
            arguments = [command, '--market-id', '123', '--config', 'live.json', '--data-dir', 'data']
            if command == 'calibrate':
                arguments.extend(['--output', 'report.json'])
            self.assertIsNone(parse_args(arguments).token_id)
            self.assertEqual('2', parse_args(arguments + ['--token-id', '2']).token_id)

    def test_legacy_context_matches_only_with_same_effective_token_and_settings(self):
        cfg = self.config(token_id='1')
        old = asdict(cfg)
        old.pop('token_id')
        context = dict(config=old, market_id=123, token_id='1')
        self.assertTrue(matches_active_context(context, cfg, 123, '1'))
        self.assertFalse(matches_active_context(context, cfg, 123, '2'))
        self.assertFalse(matches_active_context(context, cfg, 124, '1'))
        self.assertFalse(matches_active_context(context, replace(cfg, refresh_seconds=60), 123, '1'))
        # Explicit effective token in sibling field remains authoritative.
        modern = dict(config=asdict(replace(cfg, token_id='2')), market_id=123, token_id='1')
        self.assertTrue(matches_active_context(modern, cfg, 123, '1'))
