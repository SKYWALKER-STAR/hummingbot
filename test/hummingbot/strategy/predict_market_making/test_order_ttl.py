import contextlib
import io
import unittest

from test_quote_engine import runner


class OrderTTLTests(unittest.TestCase):
    def test_default_is_five_minutes(self):
        self.assertEqual(300, runner.parse_args(['--market-id', '1', '--auto-select']).order_ttl)

    def test_reject_short_expiry(self):
        for ttl in ('0', '60', '120', '179'):
            with self.subTest(ttl=ttl), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                runner.parse_args(['--market-id', '1', '--auto-select', '--order-ttl', ttl])

    def test_allow_margin_and_longer_lifetime(self):
        for ttl in ('180', '300', '1800'):
            self.assertEqual(int(ttl), runner.parse_args(
                ['--market-id', '1', '--auto-select', '--order-ttl', ttl]).order_ttl)
