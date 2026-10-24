"""Strategy-local observation subclass. Base Predict connector is unmodified."""
import time
from dataclasses import asdict

from hummingbot.connector.exchange.predict.predict_api_order_book_data_source import PredictAPIOrderBookDataSource
from hummingbot.connector.exchange.predict.predict_exchange import PredictExchange


class RecordingOrderBookDataSource(PredictAPIOrderBookDataSource):
    async def _on_order_stream_interruption(self, websocket_assistant=None):
        self.connector.observations.gap('market_stream_interruption')
        await super()._on_order_stream_interruption(websocket_assistant)

    async def _process_message_for_unknown_channel(self, event_message, websocket_assistant):
        topic = event_message.get('topic','')
        if event_message.get('type')=='M' and topic.startswith('predictTradingStatus/'):
            data = event_message.get('data',{})
            self.connector.recorder.emit('market_status',trading_status=data.get('tradingStatus'))
            if data.get('tradingStatus') != 'OPEN':
                self.connector.observations.gap('market_not_open')
        await super()._process_message_for_unknown_channel(event_message,websocket_assistant)


class RecordingPredictExchange(PredictExchange):
    def __init__(self, *args, recorder, observations, **kwargs):
        self.recorder, self.observations = recorder, observations
        self.metadata_halt = False
        super().__init__(*args, **kwargs)

    def _create_order_book_data_source(self):
        return RecordingOrderBookDataSource(self._trading_pairs,self,self._web_assistants_factory)

    def _install_market(self, market):
        previous = self.markets_by_id.get(market.market_id)
        super()._install_market(market)
        if previous != market:
            self.recorder.emit('market_metadata',market=asdict(market))
        if previous and any(getattr(previous,key)!=getattr(market,key) for key in
                            ('decimal_precision','fee_rate_bps','is_neg_risk','is_yield_bearing')):
            self.metadata_halt = True
            self.observations.gap('market_rules_changed_restart_required')

    def record_book(self, data):
        previous = int(self._books.get(int(data['marketId']), {}).get('updateTimestampMs',0))
        super().record_book(data)
        if int(data['updateTimestampMs']) >= previous:
            self.observations.book(data, self.markets_by_id[int(data['marketId'])])

    async def _place_order(self, *args, **kwargs):
        self.recorder.require_healthy()
        if self.metadata_halt:
            raise RuntimeError('Market rules changed; reconcile and restart')
        result = await super()._place_order(*args, **kwargs)
        order_id = args[0] if args else kwargs['order_id']
        tracked = self._order_tracker.all_updatable_orders.get(order_id)
        if tracked and result[0] not in self._uncertain_submissions:
            # Seed exposure at successful submission acknowledgement, rather than
            # selecting only orders that survive until the first REST poll.
            self.observations.status(tracked,'OPEN',tracked.executed_amount_base)
        else:
            self.recorder.emit('submission_uncertain',order_id=order_id,order_hash=result[0])
        return result

    async def _request_order_status(self, tracked_order):
        try:
            update = await super()._request_order_status(tracked_order)
        except Exception:
            self.observations.gap('order_status_unavailable')
            raise
        if self.outcome_for_pair(tracked_order.trading_pair).token_id == self.observations.token_id:
            self.observations.status(tracked_order, update.new_state.name, tracked_order.executed_amount_base)
        return update

    async def _place_cancel(self, order_id, tracked_order):
        self.recorder.emit('cancel_request', order_id=order_id, order_hash=tracked_order.exchange_order_id)
        started = time.monotonic()
        try:
            result = await super()._place_cancel(order_id,tracked_order)
        except Exception as exc:
            self.recorder.emit('cancel_response', order_id=order_id, success=False,
                               error_type=type(exc).__name__, elapsed=time.monotonic()-started)
            raise
        self.recorder.emit('cancel_response', order_id=order_id, success=result,
                           elapsed=time.monotonic()-started, terminal_confirmation=False)
        return result
