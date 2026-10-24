"""Single outcome, fixed size, long-only HJB execution with cancel-before-replace."""
import asyncio
from decimal import Decimal as D
import time
import uuid

import numpy as np


def reject_unknown_orders(remote, known_hashes, token_ids):
    known = {v.lower() for v in known_hashes}
    for row in remote:
        contract = row['order']
        if row['status']=='OPEN' and str(contract['tokenId']) in token_ids:
            if str(contract.get('hash','')).lower() not in known:
                raise RuntimeError('Unknown open orders in market: restore original state or resolve orders first')


def plan(connector, market, outcome, config, table, now=None, price_only=False, preview=False):
    now = time.time() if now is None else now
    def skip(reason):
        return dict(quotes=[], reason=reason, model_version=table.config.version)
    if getattr(connector,'metadata_halt',False):
        return skip('market_rules_changed_restart_required')
    market = getattr(connector,'markets_by_id',{}).get(market.market_id,market)
    if not config.start <= now < config.cutoff:
        return skip('outside_trading_window')
    if not connector.ready or not connector.market_data_stream_connected:
        return skip('connector_not_ready')
    if connector.market_status.get(market.market_id) != 'OPEN':
        return skip('market_not_open')
    age = time.monotonic()-connector.book_received_at.get(market.market_id,-float('inf'))
    if not 0 <= age <= config.max_book_age_seconds:
        return skip('stale_book')
    try:
        book = connector.get_order_book(outcome.trading_pair)
        bid, ask = next(iter(book.bid_entries()),None), next(iter(book.ask_entries()),None)
        if bid is None or ask is None:
            return skip('empty_book')
        bp, ap, tick = D(str(bid.price)), D(str(ask.price)), market.tick_size
        depth = min(D(str(bid.amount)),D(str(ask.amount)))
        if not 0 < bp < ap < 1 or depth <= 0 or depth < D(str(config.min_level_amount)):
            return skip('invalid_or_shallow_book')
        if tick <= 0 or bp % tick or ap % tick:
            return skip('off_tick_book')
        if 1/tick > config.max_quote_ticks:
            return skip('too_many_quote_ticks')
        p = float((bp+ap)/2)
        if not table.config.p_min <= p <= 1-table.config.p_min:
            return skip('outside_model_price_domain')
        own = D(str(config.preview_shares)) if preview else connector.get_balance(outcome.base_asset)
        total = own if preview else sum((connector.get_balance(o.base_asset) for o in market.outcomes),D(0))
        if not own.is_finite() or not total.is_finite() or own < 0 or own > D(str(table.config.q_max)):
            return skip('inventory_outside_model_domain')
        shares = own if preview else connector.get_available_balance(outcome.base_asset)
        cash = D(str(config.preview_usdt)) if preview else connector.get_available_balance('USDT')
        if not shares.is_finite() or not cash.is_finite():
            return skip('invalid_balance')
        size, quotes, reasons = D(str(table.config.trade_size)), [], []
        t = (now-config.start)/config.horizon
        candidates = [tick*i for i in range(1,int(1/tick)) if tick*i < 1]
        for side in ('BUY','SELL'):
            legal = [v for v in candidates if v < ap] if side == 'BUY' else [v for v in candidates if v > bp]
            px = table.quote(t,p,float(own),side,np.array([float(v) for v in legal]))
            if px is None:
                reasons.append(side.lower()+'_no_positive_model_gain_or_inventory_boundary')
                continue
            # Recover exact Decimal tick from the candidate grid, not binary float arithmetic.
            price = min(legal,key=lambda v:abs(float(v)-px))
            if size < D(str(config.min_order_amount)) or size*price < D(str(config.min_order_notional)):
                reasons.append(side.lower()+'_below_minimum')
                continue
            if side == 'BUY':
                cost = size*price*(1+D(str(market.fee_rate_bps))/10000)
                if total+size > D(str(table.config.q_max)):
                    reasons.append('aggregate_inventory_cap')
                    continue
                if cost > D(str(config.max_cash_per_order)) or (not price_only and cost > max(D(0),cash-D(str(config.cash_reserve)))):
                    reasons.append('buy_budget')
                    continue
            elif not price_only and shares < size:
                reasons.append('insufficient_available_shares')
                continue
            quotes.append(dict(side=side,price=price,amount=size))
        if len(quotes)==2 and quotes[0]['price'] >= quotes[1]['price']:
            return skip('self_crossing_model_quotes')
        return dict(quotes=quotes,reasons=reasons,mid=p,inventory=str(own),normalized_time=t,
                    model_version=table.config.version)
    except (KeyError,ValueError,StopIteration,ArithmeticError):
        return skip('invalid_state_or_model')


class Maker:
    def __init__(self,connector,market,outcome,config,table,store,recorder,order_type,trade_type):
        self.c,self.market,self.outcome,self.config,self.table = connector,market,outcome,config,table
        self.store,self.recorder,self.order_type,self.trade_type = store,recorder,order_type,trade_type
        self.created = 0.
        self.last_cancel = -float('inf')
        self.replacing = True
        self.failures = 0
        self.model_version = table.config.version

    def orders(self):
        return self.c._order_tracker.all_updatable_orders

    async def cancel(self):
        if time.monotonic()-self.last_cancel < 5:
            return
        self.last_cancel = time.monotonic()
        errors = []
        for order in list(self.orders().values()):
            if order.exchange_order_id:
                try:
                    await self.c._execute_cancel(order.trading_pair,order.client_order_id)
                except Exception as exc:
                    errors.append(type(exc).__name__)
        if errors:
            self.recorder.emit('cancel_errors', error_types=errors)

    async def step(self, halt=False):
        c, cfg = self.c,self.config
        await c._status_polling_loop_fetch_updates()
        target = plan(c,self.market,self.outcome,cfg,self.table,price_only=True)
        if halt or self.recorder.error or self.failures >= 3:
            target = dict(quotes=[],reason='halt_or_recorder_failure_or_submit_breaker')
        desired = {q['side']:q for q in target['quotes']}
        orders = self.orders()
        refresh = (time.monotonic()-self.created >= cfg.refresh_seconds or not desired
                   or self.model_version != self.table.config.version)
        for order in orders.values():
            q = desired.get(order.trade_type.name)
            refresh |= (order.trading_pair != self.outcome.trading_pair or q is None
                        or order.executed_amount_base > 0
                        or (q is not None and abs(order.price-q['price']) >= self.market.tick_size*cfg.refresh_ticks))
        if refresh:
            self.replacing = True
        action = 'HOLD'
        if self.replacing:
            await self.cancel()
            if self.orders():
                action = 'WAIT_CANCEL_CONFIRMATION'
            elif desired and not halt and c.ready and c.market_data_stream_connected:
                await c._status_polling_loop_fetch_updates()
                target = plan(c,self.market,self.outcome,cfg,self.table)
                interrupted = False
                for quote in target['quotes']:
                    self.recorder.require_healthy()
                    if not c.account_reconciled:
                        await c._status_polling_loop_fetch_updates()
                    if not c.ready or not c.account_reconciled:
                        interrupted = True
                        break
                    fresh = plan(c,self.market,self.outcome,cfg,self.table)
                    quote = next((q for q in fresh['quotes'] if q['side']==quote['side']),None)
                    if quote is None:
                        interrupted = True
                        break
                    oid = 'pom-'+uuid.uuid4().hex[:24]
                    self.recorder.emit('submit_request',order_id=oid,token_id=self.outcome.token_id,**quote,
                                       model_version=self.table.config.version)
                    await c._create_order(trade_type=getattr(self.trade_type,quote['side']),order_id=oid,
                                          trading_pair=self.outcome.trading_pair,amount=quote['amount'],
                                          order_type=self.order_type.LIMIT_MAKER,price=quote['price'])
                    self.store.save(c.tracking_states)
                    tracked = self.orders().get(oid)
                    self.recorder.emit('submit_result',order_id=oid,tracked=tracked is not None,
                                       order_hash=tracked.exchange_order_id if tracked else None,
                                       state=tracked.current_state.name if tracked else 'NOT_TRACKED')
                    if tracked is None:
                        self.failures += 1
                        interrupted = True
                        break
                self.model_version = self.table.config.version
                self.created = time.monotonic()
                self.last_cancel = -float('inf')
                self.replacing = interrupted or not self.orders()
                action = 'SUBMITTED' if self.orders() else 'SKIP'
            else:
                action = 'WITHDRAWN' if not desired else 'WAIT_READY'
        elif not orders:
            self.replacing = True
        self.store.save(c.tracking_states)
        return dict(mode='LIVE',action=action,ready=c.ready and c.market_data_stream_connected,
                    active_order_ids=list(self.orders()),plan=target,submission_failures=self.failures)

    async def stop(self,timeout=60):
        async def drain():
            self.last_cancel = -float('inf')
            while self.orders():
                await self.cancel()
                await self.c._status_polling_loop_fetch_updates()
                self.store.save(self.c.tracking_states)
                if self.orders():
                    await asyncio.sleep(1)
        try:
            await asyncio.wait_for(drain(),timeout)
        finally:
            self.store.save(self.c.tracking_states)
