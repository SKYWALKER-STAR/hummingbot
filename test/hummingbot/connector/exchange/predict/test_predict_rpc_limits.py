import unittest
from unittest.mock import Mock, patch

from core_loader import load

signer = load("predict_signer")


class RPCLogLimitTests(unittest.TestCase):
    def test_range_limit_retries_without_skipping_blocks(self):
        event = Mock()

        def logs(from_block, to_block, argument_filters):
            self.assertEqual({"orderHash": "hash"}, argument_filters)
            if to_block - from_block + 1 > 100:
                raise ValueError({"code": -32005, "message": "limit exceeded"})
            return list(range(from_block, to_block + 1))

        event.get_logs.side_effect = logs
        with patch.object(signer.time, "sleep") as sleep:
            chunks = list(signer.fill_log_chunks(event, 25, 2050, "hash"))
        self.assertEqual(list(range(25, 2051)), [v for chunk in chunks for v in chunk])
        self.assertTrue(sleep.called)

    def test_persistent_limit_is_bounded_and_raised(self):
        event = Mock()
        event.get_logs.side_effect = ValueError("-32005 limit exceeded")
        with patch.object(signer.time, "sleep"), self.assertRaises(ValueError):
            list(signer.fill_log_chunks(event, 0, 2000, "hash"))
        self.assertEqual(12, event.get_logs.call_count)

    def test_other_errors_are_not_retried(self):
        event = Mock()
        event.get_logs.side_effect = ValueError("invalid filter")
        with patch.object(signer.time, "sleep") as sleep, self.assertRaises(ValueError):
            list(signer.fill_log_chunks(event, 0, 2000, "hash"))
        sleep.assert_not_called()
