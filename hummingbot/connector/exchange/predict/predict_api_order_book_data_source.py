from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest


class PredictAPIOrderBookDataSource(OrderBookTrackerDataSource):
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 60

    def __init__(self, trading_pairs, connector, api_factory):
        super().__init__(trading_pairs)
        self.connector = connector
        self.api_factory = api_factory
        self._subscribed = set()
        self._request_id = 0
        self._latest = {}

    async def get_last_traded_prices(self, trading_pairs, domain=None):
        return await self.connector.get_last_traded_prices(trading_pairs)

    async def _order_book_snapshot(self, trading_pair):
        outcome = self.connector.outcome_for_pair(trading_pair)
        self.logger().info("Requesting Predict order book snapshot for %s", trading_pair)
        data = await self.connector.fetch_book(outcome.market_id)
        self.logger().info("Received Predict order book snapshot for %s", trading_pair)
        return self._message(data, outcome)

    def _message(self, data, outcome):
        market = self.connector.markets_by_id[outcome.market_id]
        content = market.book(data, outcome)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, content["update_id"] / 1000)

    async def _connected_websocket_assistant(self):
        ws = await self.api_factory.get_ws_assistant()
        await ws.connect(ws_url=self.connector.ws_url, ws_headers=self.connector.client.headers,
                         message_timeout=40)
        self._subscribed.clear()
        return ws

    async def _send_market(self, ws, market_id, method):
        for topic in ("predictOrderbook", "predictTradingStatus", "predictMarketChanged"):
            self._request_id += 1
            await ws.send(WSJSONRequest(payload={"method": method, "requestId": self._request_id,
                                                 "params": [f"{topic}/{market_id}"]}))

    async def _subscribe_channels(self, ws):
        for market_id in sorted({self.connector.outcome_for_pair(p).market_id for p in self._trading_pairs}):
            await self._send_market(ws, market_id, "subscribe")
            self._subscribed.add(market_id)

    async def subscribe_to_trading_pair(self, trading_pair):
        market_id = self.connector.outcome_for_pair(trading_pair).market_id
        if self._ws_assistant is not None and market_id not in self._subscribed:
            await self._send_market(self._ws_assistant, market_id, "subscribe")
            self._subscribed.add(market_id)
        self.add_trading_pair(trading_pair)
        return True  # Reconnect subscribes all desired pairs if currently offline.

    async def unsubscribe_from_trading_pair(self, trading_pair):
        market_id = self.connector.outcome_for_pair(trading_pair).market_id
        others = [p for p in self._trading_pairs if p != trading_pair
                  and self.connector.outcome_for_pair(p).market_id == market_id]
        if not others and market_id in self._subscribed:
            if self._ws_assistant is not None:
                await self._send_market(self._ws_assistant, market_id, "unsubscribe")
            self._subscribed.discard(market_id)
        self.remove_trading_pair(trading_pair)
        return True

    def _channel_originating_message(self, event_message):
        if event_message.get("type") == "M" and event_message.get("topic", "").startswith("predictOrderbook/"):
            return self._snapshot_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message, websocket_assistant):
        topic = event_message.get("topic", "")
        data = event_message.get("data")
        if topic == "heartbeat":
            await websocket_assistant.send(WSJSONRequest(payload={"method": "heartbeat", "data": data}))
        elif event_message.get("type") == "R" and not event_message.get("success"):
            raise IOError("Predict market subscription rejected")
        elif event_message.get("type") == "M" and topic.startswith("predictTradingStatus/"):
            self.connector.market_status[int(data["marketId"])] = data["tradingStatus"]
        elif event_message.get("type") == "M" and topic.startswith("predictMarketChanged/"):
            # Fail closed until refreshed metadata has been validated.
            market_id = int(data["marketId"])
            self.connector.market_status[market_id] = "REFRESHING"
            await self.connector.refresh_market(market_id)

    async def _parse_order_book_snapshot_message(self, raw_message, message_queue):
        data = raw_message["data"]
        market_id = int(data["marketId"])
        if market_id not in self.connector.markets_by_id:
            return
        if raw_message["topic"] != f"predictOrderbook/{market_id}":
            raise ValueError("WS topic/market mismatch")
        timestamp = int(data["updateTimestampMs"])
        known_timestamp = max(self._latest.get(market_id, 0),
                              int(self.connector._books.get(market_id, {}).get("updateTimestampMs", 0)))
        if timestamp < known_timestamp:
            return
        messages = [self._message(data, self.connector.outcome_for_pair(p))
                    for p in tuple(self._trading_pairs)
                    if self.connector.outcome_for_pair(p).market_id == market_id]
        self._latest[market_id] = timestamp
        self.connector.record_book(data)
        for message in messages:
            message_queue.put_nowait(message)

    async def _request_order_book_snapshots(self, output):
        # One REST request per market, not per outcome.
        for market_id in tuple(self.connector.markets_by_id):
            data = await self.connector.fetch_book(market_id)
            await self._parse_order_book_snapshot_message(
                {"topic": f"predictOrderbook/{market_id}", "data": data}, output)

    async def _on_order_stream_interruption(self, websocket_assistant=None):
        self.connector.book_received_at.clear()
        await super()._on_order_stream_interruption(websocket_assistant)
