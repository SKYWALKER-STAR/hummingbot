import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from core_loader import load

module = load("predict_client")


class RateLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_pacing_shared_clients_and_independent_buckets(self):
        now = [100.0]

        async def sleep(delay):
            now[0] += delay

        a = module.PredictClient(api_key="same")
        b = module.PredictClient(api_key="same")
        with patch.object(module.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(module.asyncio, "sleep", side_effect=sleep):
            await a._throttle("GET", "/v1/orders")
            await b._throttle("POST", "/v1/orders")
            self.assertEqual(100, now[0])
            await b._throttle("GET", "/v1/markets")
            self.assertAlmostEqual(100 + 60 / 450, now[0])

    async def test_concurrent_trading_respects_minute_and_burst_limits(self):
        now = [100.0]

        async def sleep(delay):
            now[0] += delay

        client = module.PredictClient()
        sent = []

        async def send():
            await client._throttle("POST", "/v1/orders/remove")
            sent.append(now[0])

        with patch.object(module.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(module.asyncio, "sleep", side_effect=sleep):
            await asyncio.gather(*(send() for _ in range(1001)))
        for period, limit in ((60, 900), (1, 72)):
            self.assertTrue(all(sent[i + limit] - sent[i] >= period - 1e-8
                                for i in range(len(sent) - limit)))

    def test_classification(self):
        bucket = module.PredictClient._request_bucket
        self.assertEqual("general", bucket("GET", "/v1/orders/abc"))
        self.assertEqual("general", bucket("POST", "/v1/auth"))
        self.assertEqual("trading", bucket("POST", "/v1/orders/remove"))
        self.assertEqual("trading", bucket("POST", "/v1/predictions"))
        self.assertEqual("search", bucket("GET", "/v1/search?query=a"))

    async def test_retry_after_and_fallback(self):
        client = module.PredictClient()
        with patch.object(module.time, "monotonic", return_value=100), \
                patch.object(module.time, "time", return_value=0):
            for value, expected in (("10", 110), ("bad", 160), (None, 160),
                                    ("NaN", 160), ("-1", 160),
                                    ("Thu, 01 Jan 1970 00:02:00 GMT", 220)):
                client._limit_state()["blocked_until"] = 0
                client._rate_limited(value)
                self.assertEqual(expected, client._limit_state()["blocked_until"])

    async def test_429_blocks_other_bucket_without_replaying_post(self):
        client = module.PredictClient(api_key="shared")
        response = Mock(status=429, headers={"Retry-After": "30"})
        context = Mock(__aenter__=AsyncMock(return_value=response),
                       __aexit__=AsyncMock(return_value=False))
        client._session = Mock(request=Mock(return_value=context))
        client._log_api_failure = AsyncMock(return_value={})
        now = [100.0]

        async def sleep(delay):
            now[0] += delay

        with patch.object(module.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(module.asyncio, "sleep", side_effect=sleep):
            with self.assertRaises(module.PredictAPIError):
                await client.request("POST", "/v1/orders")
            await module.PredictClient(api_key="shared")._throttle("GET", "/v1/markets")
        self.assertEqual(130, now[0])
        self.assertEqual(1, client._session.request.call_count)
