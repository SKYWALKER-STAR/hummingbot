import asyncio
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from core_loader import load

market_module = load("predict_market")
client_module = load("predict_client")
signer_module = load("predict_signer")
PredictMarket = market_module.PredictMarket


def market_data(mid=123, names=("Team A", "Team B")):
    return {"id": mid, "title": "Match winner", "tradingStatus": "OPEN",
            "decimalPrecision": 3, "feeRateBps": 100, "isNegRisk": False,
            "isYieldBearing": False, "outcomes": [
                {"name": names[1], "indexSet": 2, "onChainId": str(mid * 10 + 2)},
                {"name": names[0], "indexSet": 1, "onChainId": str(mid * 10 + 1)}]}


class MarketTests(unittest.TestCase):
    def test_names_and_order_do_not_determine_direction(self):
        market = PredictMarket.parse(market_data(names=("Down", "Up")), 123)
        self.assertEqual("Down", market.outcomes[0].name)
        self.assertEqual(2, market.outcome(name="Up").index_set)
        book = {"marketId": 123, "updateTimestampMs": 1000,
                "bids": [["0.491", "12"], ["0.490", "13"]],
                "asks": [["0.493", "14"], ["0.492", "15"]]}
        direct = market.book(book, market.outcome(name="Down"))
        inverse = market.book(book, market.outcome(name="Up"))
        self.assertEqual((D("0.491"), D(12)), direct["bids"][0])
        self.assertEqual([(D("0.508"), D(15)), (D("0.507"), D(14))], inverse["bids"])
        self.assertEqual([(D("0.509"), D(12)), (D("0.510"), D(13))], inverse["asks"])

    def test_unicode_duplicate_labels_and_stable_identity(self):
        market = PredictMarket.parse(market_data(names=("队伍-A", "队伍-A")))
        with self.assertRaises(ValueError):
            market.outcome(name="队伍-A")
        self.assertEqual("PREDICT_123_1-USDT", market.outcome(token_id="1231").trading_pair)
        other = PredictMarket.parse(market_data(mid=124))
        self.assertNotEqual(market.outcomes[0].trading_pair, other.outcomes[0].trading_pair)
        renamed = PredictMarket.parse(market_data(names=("Renamed", "Other")))
        self.assertEqual(market.outcomes[0].trading_pair, renamed.outcomes[0].trading_pair)

    def test_rejects_wrong_market_and_unsupported_outcomes(self):
        with self.assertRaises(ValueError):
            PredictMarket.parse(market_data(), 456)
        for modify in (lambda d: d["outcomes"].pop(),
                       lambda d: d["outcomes"][0].update(indexSet=4),
                       lambda d: d["outcomes"][0].update(onChainId="1231"),
                       lambda d: d.update(decimalPrecision=-1),
                       lambda d: d["outcomes"][0].update(name="")):
            data = market_data()
            modify(data)
            with self.assertRaises(ValueError):
                PredictMarket.parse(data)

    def test_book_rejects_bad_prices_and_handles_empty_side(self):
        market = PredictMarket.parse(market_data())
        book = {"marketId": 123, "updateTimestampMs": 1000, "bids": [], "asks": [["0.600", "2"]]}
        self.assertEqual([], market.book(book, market.outcomes[1])["asks"])
        for price in ("NaN", "Infinity", "-0.1", "1.1", "0.6001"):
            book["asks"][0][0] = price
            with self.assertRaises(ValueError):
                market.book(book, market.outcomes[0])

    def test_market_ids_validation(self):
        for value in (True, -1, 0, "../1", 1.5, None):
            with self.assertRaises(ValueError):
                market_module.positive_id(value)
        self.assertEqual(123, market_module.positive_id("123"))


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_auth_refresh_is_serialized(self):
        signer = SimpleNamespace(auth_message=lambda msg: {"message": msg, "signature": "fake"})
        client = client_module.PredictClient(signer=signer)
        client._send = AsyncMock(side_effect=[{"data": {"message": "hello"}}, {"data": {"token": "test-token"}}])
        tokens = await asyncio.gather(*(client.jwt() for _ in range(8)))
        self.assertEqual(["test-token"] * 8, tokens)
        self.assertEqual(2, client._send.await_count)

    async def test_retries_only_explicit_auth_rejection(self):
        client = client_module.PredictClient()
        client.jwt = AsyncMock(side_effect=["old", "new"])
        client._send = AsyncMock(side_effect=[client_module.PredictAPIError(401, "/v1/orders"), {"success": True}])
        await client.request("POST", "/v1/orders", private=True, data={})
        self.assertEqual(2, client._send.await_count)
        client._send = AsyncMock(side_effect=asyncio.TimeoutError)
        with self.assertRaises(asyncio.TimeoutError):
            await client.request("POST", "/v1/orders", data={})
        self.assertEqual(1, client._send.await_count)

    async def test_pagination_and_repeated_cursor(self):
        client = client_module.PredictClient()
        client.request = AsyncMock(side_effect=[{"data": [{"id": 1}], "cursor": "a"},
                                               {"data": [{"id": 2}], "cursor": None}])
        self.assertEqual([{"id": 1}, {"id": 2}], await client.pages("/v1/orders"))
        client.request = AsyncMock(return_value={"data": [], "cursor": "a"})
        with self.assertRaises(ValueError):
            await client.pages("/v1/orders")

    async def test_error_messages_do_not_contain_signed_payload(self):
        error = client_module.PredictAPIError(400, "/v1/orders")
        self.assertEqual("Predict request failed: HTTP 400, /v1/orders", str(error))


class SignerFillTests(unittest.TestCase):
    def test_confirmed_chain_fill_has_stable_id_and_correct_units(self):
        signer = signer_module.PredictSigner.__new__(signer_module.PredictSigner)
        signer.address = "0xabc"
        args = {"maker": "0xABC", "makerAssetId": 0, "takerAssetId": 1231,
                "makerAmountFilled": 6 * 10**18, "takerAmountFilled": 10 * 10**18, "fee": 10**16}
        event = {"args": args, "transactionHash": b"\xaa", "logIndex": 4, "blockNumber": 90}
        get_logs = Mock(return_value=[event])
        w3 = SimpleNamespace(eth=SimpleNamespace(block_number=100,
                                                get_block=lambda n: {"timestamp": n * 10}))
        contract = SimpleNamespace(w3=w3, events=SimpleNamespace(OrderFilled=lambda: SimpleNamespace(get_logs=get_logs)))
        signer._contract = Mock(return_value=contract)
        market = PredictMarket.parse(market_data())
        fills = signer.confirmed_fills(market, market.outcomes[0], "0xhash", 900)
        self.assertEqual("aa:4", fills[0]["id"])
        self.assertEqual(D("0.6"), fills[0]["price"])
        self.assertEqual(D(10), fills[0]["base"])
        self.assertEqual("PREDICT_123_1", fills[0]["fee_asset"])
        self.assertEqual(98, get_logs.call_args.kwargs["to_block"])
        self.assertEqual({"orderHash": "0xhash"}, get_logs.call_args.kwargs["argument_filters"])
        args.update(makerAssetId=1231, takerAssetId=0, makerAmountFilled=10 * 10**18, takerAmountFilled=6 * 10**18)
        self.assertEqual("USDT", signer.confirmed_fills(market, market.outcomes[0], "0xhash", 900)[0]["fee_asset"])

    def test_fill_wrong_asset_is_rejected(self):
        signer = signer_module.PredictSigner.__new__(signer_module.PredictSigner)
        signer.address = "0xabc"
        event = {"args": {"maker": "0xabc", "makerAssetId": 0, "takerAssetId": 999}}
        contract = SimpleNamespace(w3=SimpleNamespace(eth=SimpleNamespace(block_number=2)),
                                   events=SimpleNamespace(OrderFilled=lambda: SimpleNamespace(get_logs=lambda **kw: [event])))
        signer._contract = Mock(return_value=contract)
        market = PredictMarket.parse(market_data())
        with self.assertRaises(ValueError):
            signer.confirmed_fills(market, market.outcomes[0], "hash", 0)


if __name__ == "__main__":
    unittest.main()
