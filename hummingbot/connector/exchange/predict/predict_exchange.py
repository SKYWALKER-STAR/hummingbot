import asyncio
import re
import time
from decimal import Decimal

import aiohttp
from bidict import bidict

from hummingbot.connector.exchange.predict.predict_api_order_book_data_source import PredictAPIOrderBookDataSource
from hummingbot.connector.exchange.predict.predict_api_user_stream_data_source import PredictAPIUserStreamDataSource
from hummingbot.connector.exchange.predict.predict_client import PredictAPIError, PredictClient
from hummingbot.connector.exchange.predict.predict_market import PredictMarket, decimal_value, positive_id
from hummingbot.connector.exchange.predict.predict_signer import PredictSigner
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class PredictExchange(ExchangePyBase):
    """Binary conditional markets, discovered by ID; outcome labels are metadata.

    exchange_order_id is the signed order hash, which survives HTTP timeouts and
    restarts. Numeric API order IDs are obtained when removing an order.
    """

    LONG_POLL_INTERVAL = 15.0

    def __init__(self, predict_api_key="", predict_private_key="", predict_account_address="",
                 predict_market_ids="", market_ids=None, trading_pairs=None, trading_required=True,
                 domain="predict", balance_asset_limit=None, rate_limits_share_pct=Decimal("100"),
                 ws_url=None, order_ttl=300, max_book_age=30):
        self._domain = domain
        self._trading_required = trading_required
        ids = market_ids if market_ids is not None else [v.strip() for v in predict_market_ids.split(",") if v.strip()]
        self._market_ids = list(dict.fromkeys(positive_id(v) for v in ids))
        self._requested_pairs = list(trading_pairs or [])
        for pair in self._requested_pairs:
            match = re.fullmatch(r"PREDICT_(\d+)_(1|2)-USDT", pair)
            if not match:
                raise ValueError("Predict trading pair must be discovered by MarketID")
            mid = positive_id(match[1])
            if mid not in self._market_ids:
                self._market_ids.append(mid)
        self._trading_pairs = []
        self.markets_by_id = {}
        self.market_status = {}
        self._outcomes = {}
        self._books = {}
        self.book_received_at = {}
        self.account_reconciled = False
        self._account_epoch = 0
        self._network_started = False
        self._market_lock = asyncio.Lock()
        self._trade_lock = asyncio.Lock()
        self._reconcile_lock = asyncio.Lock()
        self._pending_settlements = set()
        self._fill_gaps = set()
        self._uncertain_submissions = set()
        self._signer = None
        if trading_required:
            if not predict_private_key:
                raise ValueError("predict_private_key is required when trading_required=True")
            self._signer = PredictSigner(predict_private_key, domain, predict_account_address)
        self.client = PredictClient(predict_api_key, domain, self._signer)
        # The official docs publish only the mainnet WS endpoint. Testnet must
        # explicitly supply its own endpoint; never mix networks silently.
        self.ws_url = ws_url or ("wss://ws.predict.fun/ws" if domain == "predict" else None)
        if order_ttl <= 0 or max_book_age <= 0:
            raise ValueError("order_ttl and max_book_age must be positive")
        self.order_ttl = order_ttl
        self.max_book_age = max_book_age
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def name(self):
        return self.domain

    @property
    def domain(self):
        return self._domain

    @property
    def authenticator(self):
        return None  # PredictClient supplies API key and wallet JWT, including refresh.

    @property
    def rate_limits_rules(self):
        return []  # All REST consumers share PredictClient's rolling-window limiter.

    @property
    def client_order_id_max_length(self):
        return 64

    @property
    def client_order_id_prefix(self):
        return "HBOT"

    @property
    def trading_rules_request_path(self):
        return "/v1/markets"

    @property
    def trading_pairs_request_path(self):
        return "/v1/markets"

    @property
    def check_network_request_path(self):
        return "/v1/markets"

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_trading_required(self):
        return self._trading_required

    @property
    def is_cancel_request_in_exchange_synchronous(self):
        return False

    @property
    def status_dict(self):
        status = super().status_dict
        status["account_reconciled"] = not self.is_trading_required or self.account_reconciled
        status["no_unresolved_transactions"] = not (self._fill_gaps or self._uncertain_submissions
                                                    or self._pending_settlements)
        return status

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception):
        # A transient 404 must not cause an ambiguous submission to be forgotten.
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception):
        return False

    def _create_web_assistants_factory(self):
        return WebAssistantsFactory(throttler=self._throttler)

    def _create_order_book_data_source(self):
        return PredictAPIOrderBookDataSource(self._trading_pairs, self, self._web_assistants_factory)

    def _create_user_stream_data_source(self):
        return PredictAPIUserStreamDataSource(self, self._web_assistants_factory)

    def _is_user_stream_initialized(self):
        return not self.is_trading_required or self._user_stream_tracker.data_source.connected

    async def _update_time_synchronizer(self, *args, **kwargs):
        # No exchange timestamp authentication; expiration uses the host UTC clock.
        return

    def outcome_for_pair(self, trading_pair):
        return self._outcomes[trading_pair]

    def get_market_outcomes(self, market_id):
        return self.markets_by_id[positive_id(market_id)].outcomes

    def get_trading_pair(self, market_id, *, outcome_name=None, token_id=None, index_set=None):
        return self.markets_by_id[positive_id(market_id)].outcome(
            name=outcome_name, token_id=token_id, index_set=index_set).trading_pair

    async def _read_market(self, market_id):
        response = await self.client.request("GET", f"/v1/markets/{positive_id(market_id)}")
        return PredictMarket.parse(response["data"], market_id)

    def _install_market(self, market):
        old = self.markets_by_id.get(market.market_id)
        if old and {(o.index_set, o.token_id) for o in old.outcomes} != {
                (o.index_set, o.token_id) for o in market.outcomes}:
            self.market_status[market.market_id] = "IDENTITY_CHANGED"
            raise ValueError("Outcome identity changed; reconnect after reconciling orders")
        self.markets_by_id[market.market_id] = market
        self.market_status[market.market_id] = market.trading_status
        for outcome in market.outcomes:
            self._outcomes[outcome.trading_pair] = outcome
            self._trading_rules[outcome.trading_pair] = TradingRule(
                outcome.trading_pair, min_price_increment=market.tick_size,
                min_base_amount_increment=Decimal("0.000000000000000001"),
                min_quote_amount_increment=Decimal("0.000000000000000001"),
                supports_market_orders=False, sell_order_collateral_token=outcome.base_asset)
        self._set_trading_pair_symbol_map(bidict({o.token_id: p for p, o in self._outcomes.items()}))

    async def initialize_markets(self):
        """Resolve configured IDs before constructing the initial tracked books."""
        async with self._market_lock:
            markets = [await self._read_market(mid) for mid in self._market_ids]
            for market in markets:
                self._install_market(market)
            selected = self._requested_pairs or list(self._outcomes)
            for pair in selected:
                if pair not in self._outcomes:
                    raise ValueError(f"Outcome not found: {pair}")
                if pair not in self._trading_pairs:
                    self._trading_pairs.append(pair)
        return {mid: self.get_market_outcomes(mid) for mid in self._market_ids}

    async def add_market(self, market_id):
        """Subscribe both actual outcomes of a MarketID, before or during operation."""
        market_id = positive_id(market_id)
        async with self._market_lock:
            market = await self._read_market(market_id)
            self._install_market(market)
            if market_id not in self._market_ids:
                self._market_ids.append(market_id)
            for outcome in market.outcomes:
                pair = outcome.trading_pair
                if self._network_started:
                    if pair not in self.order_books and not await self.add_trading_pair(pair):
                        raise IOError(f"Failed to initialize orderbook for {pair}; retry add_market")
                elif pair not in self._trading_pairs:
                    self._trading_pairs.append(pair)
            return market.outcomes

    async def remove_market(self, market_id):
        market_id = positive_id(market_id)
        async with self._market_lock:
            market = self.markets_by_id[market_id]
            pairs = {o.trading_pair for o in market.outcomes}
            if any(o.trading_pair in pairs for o in self._order_tracker.all_updatable_orders.values()):
                raise ValueError("Cancel and reconcile market orders before removal")
            if self._signer:
                for outcome in market.outcomes:
                    if await asyncio.to_thread(self._signer.shares_balance, market, outcome):
                        raise ValueError("Market still has inventory; removal refused")
            for pair in pairs:
                if self._network_started and pair in self.order_books:
                    if not await self.remove_trading_pair(pair):
                        raise IOError("Failed to remove market subscription")
                elif pair in self._trading_pairs:
                    self._trading_pairs.remove(pair)
                self._outcomes.pop(pair, None)
                self._trading_rules.pop(pair, None)
            self._market_ids.remove(market_id)
            self.markets_by_id.pop(market_id)
            self.market_status.pop(market_id, None)
            self._books.pop(market_id, None)
            self.book_received_at.pop(market_id, None)
            self._requested_pairs = [p for p in self._requested_pairs if p not in pairs]
            self._set_trading_pair_symbol_map(bidict({o.token_id: p for p, o in self._outcomes.items()}))

    async def refresh_market(self, market_id):
        async with self._market_lock:
            if market_id in self.markets_by_id:
                self._install_market(await self._read_market(market_id))

    async def start_network(self):
        if not self.ws_url:
            raise ValueError("Testnet requires an explicit ws_url for the same network")
        await self.initialize_markets()
        await super().start_network()
        self._network_started = True

    async def stop_network(self):
        self._network_started = False
        self.account_reconciled = False
        await super().stop_network()
        await self.client.close()

    async def _make_network_check_request(self):
        return await self.client.request("GET", "/v1/markets", params={"first": "1"})

    async def _make_trading_pairs_request(self):
        return [await self._read_market(mid) for mid in self._market_ids]

    async def _make_trading_rules_request(self):
        return await self._make_trading_pairs_request()

    async def _format_trading_rules(self, exchange_info_dict):
        for market in exchange_info_dict:
            self._install_market(market)
        return list(self._trading_rules.values())

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info):
        for market in exchange_info:
            self._install_market(market)

    async def _update_trading_rules(self):
        async with self._market_lock:
            for mid in tuple(self._market_ids):
                self._install_market(await self._read_market(mid))

    async def _update_trading_fees(self):
        return  # feeRateBps is refreshed with market metadata.

    def _get_fee(self, base_currency, quote_currency, order_type, order_side, amount,
                 price=Decimal("NaN"), is_maker=None):
        outcome = self._outcomes[f"{base_currency}-{quote_currency}"]
        # Conservative budget estimate. Actual fees come from confirmed chain logs.
        return AddedToCostTradeFee(percent=Decimal(self.markets_by_id[outcome.market_id].fee_rate_bps) / 10000)

    def record_book(self, data):
        mid = int(data["marketId"])
        market = self.markets_by_id[mid]
        for outcome in market.outcomes:
            market.book(data, outcome)  # Validate atomically before exposing it.
        if int(data["updateTimestampMs"]) >= int(self._books.get(mid, {}).get("updateTimestampMs", 0)):
            self._books[mid] = data
            self.book_received_at[mid] = time.monotonic()

    async def fetch_book(self, market_id):
        data = (await self.client.request("GET", f"/v1/markets/{market_id}/orderbook"))["data"]
        self.record_book(data)
        return self._books[market_id]

    async def _get_last_traded_price(self, trading_pair):
        outcome = self.outcome_for_pair(trading_pair)
        data = (await self.client.request("GET", f"/v1/markets/{outcome.market_id}/last-sale"))["data"]
        if data is None:
            return float("nan")
        price = decimal_value(data["priceInCurrency"], "priceInCurrency") / Decimal(10**18)
        if price > 1:
            raise ValueError("Invalid last-sale price")
        # This endpoint uses protocol enums Yes/No even for markets named Up/Down.
        # They identify indexSet, never the market's user-facing outcome names.
        sold_index = {"Yes": 1, "No": 2}[data["outcome"]]
        return float(price if sold_index == outcome.index_set else 1 - price)

    async def _place_order(self, order_id, trading_pair, amount, trade_type, order_type, price, **kwargs):
        async with self._trade_lock:
            if not self.ready or not self.account_reconciled:
                raise ValueError("Predict is not reconciled/ready")
            outcome = self.outcome_for_pair(trading_pair)
            market = self.markets_by_id[outcome.market_id]
            if self.market_status.get(market.market_id) != "OPEN":
                raise ValueError("Market is not open")
            if time.monotonic() - self.book_received_at.get(market.market_id, -float("inf")) > self.max_book_age:
                raise ValueError("Stale market orderbook")
            if order_type not in self.supported_order_types() or not price.is_finite() or not 0 < price < 1:
                raise ValueError("Only valid probability-priced limit orders are supported")
            if price % market.tick_size or not amount.is_finite() or amount <= 0:
                raise ValueError("Invalid order price/quantity")
            await self._update_balances()
            asset = "USDT" if trade_type == TradeType.BUY else outcome.base_asset
            # The current PENDING_CREATE order has already been reserved by _update_balances.
            if self._account_available_balances.get(asset, Decimal(0)) < 0:
                raise ValueError("Insufficient unreserved balance")
            payload = await asyncio.to_thread(self._signer.sign_order, market, outcome, trade_type.name,
                                               amount, price, int(time.time()) + self.order_ttl,
                                               order_type == OrderType.LIMIT_MAKER)
            if not self.ready or self.market_status.get(market.market_id) != "OPEN":
                raise ValueError("Account or market changed while preparing order")
            order_hash = payload["data"]["order"]["hash"]
            tracked = self._order_tracker.all_updatable_orders.get(order_id)
            if tracked:
                tracked.update_exchange_order_id(order_hash)
            try:
                response = await self.client.request("POST", "/v1/orders", data=payload, private=True)
                if response["data"]["orderHash"].lower() != order_hash.lower():
                    raise IOError("Order hash mismatch")
            except PredictAPIError as exc:
                if exc.status < 500 and exc.status != 429:
                    raise
                self._uncertain_submissions.add(order_hash)
            except (aiohttp.ClientError, asyncio.TimeoutError, IOError, KeyError, ValueError):
                # Never resubmit an order with a new salt after an ambiguous response.
                self._uncertain_submissions.add(order_hash)
            return order_hash, time.time()

    def restore_tracking_states(self, saved_states):
        super().restore_tracking_states(saved_states)
        for order in self._order_tracker.all_updatable_orders.values():
            match = re.fullmatch(r"PREDICT_(\d+)_(1|2)-USDT", order.trading_pair)
            if not match:
                raise ValueError("Cannot restore an unknown Predict pair")
            mid = positive_id(match[1])
            if mid not in self._market_ids:
                self._market_ids.append(mid)
            if order.exchange_order_id:
                self._uncertain_submissions.add(order.exchange_order_id)
        self.account_reconciled = False

    async def _place_cancel(self, order_id, tracked_order):
        order_hash = await tracked_order.get_exchange_order_id()
        data = (await self.client.request("GET", f"/v1/orders/{order_hash}", private=True))["data"]
        if data["status"] in ("CANCELLED", "EXPIRED", "INVALIDATED"):
            self._poll_notifier.set()
            return True
        response = await self.client.request("POST", "/v1/orders/remove", private=True,
                                             data={"data": {"ids": [str(data["id"])]}})
        self._poll_notifier.set()
        # A noop (including an already-filled order) isn't confirmation of cancellation.
        return str(data["id"]) in [str(v) for v in response.get("removed", [])]

    async def _update_lost_orders_status(self):
        # Keep the same strict fill-before-terminal ordering for restored lost orders.
        if self._order_tracker.lost_orders:
            await self._status_polling_loop_fetch_updates()

    async def _all_trade_updates_for_order(self, order):
        if not order.exchange_order_id:
            return []
        outcome = self.outcome_for_pair(order.trading_pair)
        rows = await asyncio.to_thread(self._signer.confirmed_fills,
                                       self.markets_by_id[outcome.market_id], outcome,
                                       order.exchange_order_id, order.creation_timestamp)
        return [TradeUpdate(trade_id=row["id"], client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id, trading_pair=order.trading_pair,
                            fill_timestamp=row["timestamp"], fill_price=row["price"],
                            fill_base_amount=row["base"], fill_quote_amount=row["quote"],
                            fee=AddedToCostTradeFee(flat_fees=[TokenAmount(row["fee_asset"], row["fee"])]),
                            is_taker=order.order_type != OrderType.LIMIT_MAKER) for row in rows]

    async def _request_order_status(self, tracked_order):
        order_hash = await tracked_order.get_exchange_order_id()
        data = (await self.client.request("GET", f"/v1/orders/{order_hash}", private=True))["data"]
        self._uncertain_submissions.discard(order_hash)
        filled = decimal_value(data["amountFilled"], "amountFilled") / Decimal(10**18)
        state = {"OPEN": OrderState.OPEN, "FILLED": OrderState.FILLED,
                 "CANCELLED": OrderState.CANCELED, "EXPIRED": OrderState.CANCELED,
                 "INVALIDATED": OrderState.FAILED}[data["status"]]
        if filled > tracked_order.executed_amount_base or (
                state == OrderState.FILLED and tracked_order.executed_amount_base < tracked_order.amount):
            # Matched is not settled: do not release the order's reservation yet.
            self._fill_gaps.add(order_hash)
            state = OrderState.PARTIALLY_FILLED if tracked_order.executed_amount_base else OrderState.OPEN
        else:
            self._fill_gaps.discard(order_hash)
            if filled > 0 and tracked_order.executed_amount_base >= filled:
                self._pending_settlements = {key for key in self._pending_settlements if key[0] != order_hash}
            if state == OrderState.OPEN and filled:
                state = OrderState.PARTIALLY_FILLED
        return OrderUpdate(trading_pair=tracked_order.trading_pair, update_timestamp=time.time(),
                           new_state=state, client_order_id=tracked_order.client_order_id,
                           exchange_order_id=order_hash)

    async def _update_balances(self):
        if not self._signer:
            return
        totals = {"USDT": await self._signer.collateral_balance()}
        for market in tuple(self.markets_by_id.values()):
            for outcome in market.outcomes:
                totals[outcome.base_asset] = await asyncio.to_thread(self._signer.shares_balance, market, outcome)
        available = dict(totals)
        rows = await self.client.pages("/v1/orders", first="100")
        reserved_hashes = set()
        for row in rows:
            if row["status"] != "OPEN":
                continue
            contract = row["order"]
            order_hash = contract.get("hash")
            if not order_hash:
                raise ValueError("Order response lacks hash; cannot reconcile reservations")
            reserved_hashes.add(order_hash)
            side = int(contract["side"])
            asset = "USDT" if side == 0 else next(
                (o.base_asset for o in self._outcomes.values() if o.token_id == str(contract["tokenId"])), None)
            if side not in (0, 1):
                raise ValueError("Unknown order side")
            if asset is not None:
                # Reserve full original amount conservatively, including partial fills.
                value = decimal_value(contract["makerAmount"], "makerAmount") / Decimal(10**18)
                if side == 0:
                    value *= 1 + decimal_value(contract["feeRateBps"], "feeRateBps") / 10000
                available[asset] = available.get(asset, Decimal(0)) - value
        for order in self._order_tracker.all_updatable_orders.values():
            if order.exchange_order_id in reserved_hashes:
                continue
            outcome = self.outcome_for_pair(order.trading_pair)
            asset = "USDT" if order.trade_type == TradeType.BUY else outcome.base_asset
            value = order.amount
            if order.trade_type == TradeType.BUY:
                value *= order.price * (1 + Decimal(self.markets_by_id[outcome.market_id].fee_rate_bps) / 10000)
            available[asset] = available.get(asset, Decimal(0)) - value
        self._account_balances.clear()
        self._account_balances.update(totals)
        self._account_available_balances.clear()
        self._account_available_balances.update(available)

    async def _status_polling_loop_fetch_updates(self):
        async with self._reconcile_lock:
            self.account_reconciled = False
            epoch = self._account_epoch
            # Unlike the generic polling loop, reconcile fills before order terminals
            # and balances. An RPC failure must not be swallowed and release funds.
            for order in list(self._order_tracker.all_updatable_orders.values()):
                if not order.exchange_order_id:
                    continue
                for update in await self._all_trade_updates_for_order(order):
                    self._order_tracker.process_trade_update(update)
                update = await self._request_order_status(order)
                await self._order_tracker.process_order_update(update)
            await self._update_balances()
            self.account_reconciled = epoch == self._account_epoch and not (
                self._fill_gaps or self._uncertain_submissions or self._pending_settlements)

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            self._account_epoch += 1
            order_hash = event.get("orderHash")
            kind = event.get("type")
            if kind == "orderTransactionSubmitted":
                self._pending_settlements.add((order_hash, str(event["settlementId"])))
            elif kind in ("orderTransactionSuccess", "orderTransactionFailed"):
                self._pending_settlements.discard((order_hash, str(event["settlementId"])))
            # WS wakes reconciliation; confirmed chain logs remain the single source
            # of fills, including when notifications arrive twice or out of order.
            self.account_reconciled = False
            self._poll_notifier.set()
