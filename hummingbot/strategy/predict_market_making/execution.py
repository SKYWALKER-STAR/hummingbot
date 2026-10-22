"""Selection and live lifecycle. Exchange calls are injected for offline testing."""
import asyncio
import json
import os
import time
import uuid
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path


def select_outcome(books, threshold, config, pricing, current=None):
    """Only healthy books qualify; equality does not satisfy the threshold."""
    candidates = []
    for token, book in books.items():
        plan = pricing.calculate_quotes(config, book, inventory=D(0), available_shares=D(0), available_usdt=D(0))
        if plan.mid is not None and plan.mid > threshold:
            candidates.append((plan.mid, token == current, token))
    return max(candidates)[2] if candidates else None


def read_books(connector, market, config, pricing):
    result = {}
    for outcome in market.outcomes:
        bid = ask = D(0)
        bid_size = ask_size = D(0)
        ready = connector.ready and connector.market_data_stream_connected
        try:
            book = connector.get_order_book(outcome.trading_pair)
            b = next(iter(book.bid_entries()), None)
            a = next(iter(book.ask_entries()), None)
            if b:
                bid, bid_size = D(str(b.price)), D(str(b.amount))
            if a:
                ask, ask_size = D(str(a.price)), D(str(a.amount))
        except (KeyError, ValueError):
            ready = False
        result[outcome.token_id] = pricing.BookState(
            bid, bid_size, ask, ask_size, market.tick_size,
            time.monotonic() - connector.book_received_at.get(market.market_id, -float("inf")),
            connector.market_status.get(market.market_id, "UNKNOWN"), ready)
    return result


class StateStore:
    """Atomic, fsynced checkpoints with an OS lock released on process exit.

    Use the same path for this wallet/market on every launch. No secrets are stored.
    """
    def __init__(self, path, identity):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.identity = identity
        self.lock = self.path.with_suffix(self.path.suffix + ".lock").open("a+b")
        self.lock.seek(0)
        self.lock.write(b"0")
        self.lock.flush()
        self.lock.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise RuntimeError("Another maker is using this state file") from None

    def load(self):
        if not self.path.exists():
            return {}
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if data.get("identity") != self.identity or data.get("version") != 1:
            raise ValueError("State file belongs to a different account/market or version")
        return data["orders"]

    def save(self, orders):
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump({"version": 1, "identity": self.identity, "orders": orders}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
        if os.name != "nt":
            directory = os.open(str(self.path.parent), os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

    def close(self):
        self.lock.close()


class LiveMaker:
    def __init__(self, connector, market, config, pricing, store, order_type, trade_type,
                 threshold=D("0.5"), fixed_token=None):
        self.connector, self.market, self.config, self.pricing = connector, market, config, pricing
        self.store, self.order_type, self.trade_type = store, order_type, trade_type
        self.threshold, self.fixed_token = threshold, fixed_token
        self.selected = None
        self.created = 0
        self.replacing = True  # Restored orders must be removed before the first new quote.
        self.last_cancel = -float("inf")

    def orders(self):
        return self.connector._order_tracker.all_updatable_orders

    async def cancel(self):
        if not self.orders():
            return
        if time.monotonic() - self.last_cancel < 5:
            return
        self.last_cancel = time.monotonic()
        for order in list(self.orders().values()):
            if order.exchange_order_id:
                await self.connector._execute_cancel(order.trading_pair, order.client_order_id)

    def plans(self, books, selected):
        c, cfg = self.connector, self.config
        inventory = {o.token_id: c.get_balance(o.base_asset) for o in self.market.outcomes}
        total = sum(inventory.values(), D(0))
        plans = {}
        for outcome in self.market.outcomes:
            # Across BOTH outcomes, never buy past the configured share cap.
            room = max(D(0), cfg.max_inventory - total)
            own = inventory[outcome.token_id]
            limited = replace(cfg, max_inventory=max(cfg.max_inventory, own))
            plan = self.pricing.calculate_quotes(
                limited, books[outcome.token_id], inventory=own,
                available_shares=max(D(0), c.get_available_balance(outcome.base_asset)),
                available_usdt=max(D(0), c.get_available_balance("USDT")),
                fee_rate_bps=D(self.market.fee_rate_bps))
            quotes = []
            reasons = list(plan.reasons)
            for quote in plan.quotes:
                if quote.side == "BUY":
                    if outcome.token_id != selected:
                        reasons.append("buy_disabled:outcome_not_selected")
                        continue
                    amount = self.pricing.floor_step(min(quote.amount, room), cfg.amount_step)
                    if amount < cfg.min_order_amount or amount * quote.price < cfg.min_order_notional:
                        reasons.append(f"buy_disabled:aggregate_inventory_room={room},clipped_amount={amount}")
                        continue
                    quote = replace(quote, amount=amount)
                quotes.append(quote)
            plans[outcome.token_id] = replace(plan, quotes=tuple(quotes), reasons=tuple(reasons))
        return plans

    async def step(self):
        c = self.connector
        # Order WS events and an in-progress poll temporarily invalidate account
        # readiness. Finish reconciliation before treating that as a withdrawal
        # signal. The connector serializes this with its background poll.
        if not getattr(c, "account_reconciled", True):
            await c._status_polling_loop_fetch_updates()
        books = read_books(c, self.market, self.config, self.pricing)
        selected = self.fixed_token or select_outcome(
            books, self.threshold, self.config, self.pricing, self.selected)
        plans = self.plans(books, selected)
        orders = self.orders()
        refresh = selected != self.selected or time.monotonic() - self.created >= float(self.config.refresh_seconds)
        for order in orders.values():
            token = c.outcome_for_pair(order.trading_pair).token_id
            plan = plans[token]
            # Validate prices without treating our own reserved funds as lost budget.
            price_plan = self.pricing.calculate_quotes(
                self.config, books[token], inventory=c.get_balance(c.outcome_for_pair(order.trading_pair).base_asset),
                available_shares=D("1e30"), available_usdt=D("1e30"), fee_rate_bps=D(self.market.fee_rate_bps))
            target = next((q for q in price_plan.quotes if q.side == order.trade_type.name), None)
            refresh |= (plan.mid is None or target is None or order.executed_amount_base > 0 or
                        (order.trade_type.name == "BUY" and token != selected) or
                        (target is not None and abs(order.price - target.price) >= self.market.tick_size * self.config.refresh_price_ticks) or
                        (order.price >= books[token].best_ask if order.trade_type.name == "BUY"
                         else order.price <= books[token].best_bid))
        if refresh:
            self.replacing = True
        action = "HOLD"
        if self.replacing:
            await self.cancel()
            if self.orders():
                action = "WAIT_CANCEL_CONFIRMATION"
            elif c.ready and c.market_data_stream_connected:
                # This full reconciliation completes before re-reading balances/quotes.
                await c._status_polling_loop_fetch_updates()
                books = read_books(c, self.market, self.config, self.pricing)
                selected = self.fixed_token or select_outcome(books, self.threshold, self.config, self.pricing, self.selected)
                plans = self.plans(books, selected)
                self.selected = selected
                interrupted = False
                for outcome in self.market.outcomes:
                    for quote in plans[outcome.token_id].quotes:
                        if not getattr(c, "account_reconciled", True):
                            await c._status_polling_loop_fetch_updates()
                        if not c.ready or not c.market_data_stream_connected:
                            interrupted = True
                            break
                        fresh_books = read_books(c, self.market, self.config, self.pricing)
                        fresh_selected = self.fixed_token or select_outcome(
                            fresh_books, self.threshold, self.config, self.pricing, selected)
                        if fresh_selected != selected:
                            interrupted = True
                            break
                        fresh_plan = self.plans(fresh_books, selected)[outcome.token_id]
                        quote = next((q for q in fresh_plan.quotes if q.side == quote.side), None)
                        if quote is None:
                            interrupted = True
                            break
                        order_id = "ppm-" + uuid.uuid4().hex[:24]
                        # Await creation so cancellation/stop cannot race a scheduled buy().
                        await c._create_order(
                            trade_type=getattr(self.trade_type, quote.side), order_id=order_id,
                            trading_pair=outcome.trading_pair, amount=quote.amount,
                            order_type=self.order_type.LIMIT_MAKER, price=quote.price)
                        self.store.save(c.tracking_states)
                    if interrupted:
                        break
                self.created = time.monotonic()
                self.last_cancel = -float("inf")
                self.replacing = interrupted
                action = "SUBMITTED" if self.orders() else "SKIP"
            else:
                action = "WAIT_READY"
        elif not orders:
            self.replacing = True
            action = "SKIP"
        self.store.save(c.tracking_states)
        outcome = next((o for o in self.market.outcomes if o.token_id == selected), None)
        return {"mode": "LIVE", "ready": c.ready and c.market_data_stream_connected,
                "fill_source": "REST_CUMULATIVE", "fill_price_and_fee": "ESTIMATED",
                "connector_status": dict(getattr(c, "status_dict", {})),
                "market_data_stream_connected": c.market_data_stream_connected,
                "market_id": self.market.market_id, "selected_outcome": outcome.name if outcome else None,
                "selected_token": selected, "threshold": str(self.threshold), "action": action,
                "active_order_ids": list(self.orders()), "balances": {"source": "ON_CHAIN",
                "USDT": str(c.get_balance("USDT")),
                "available_USDT": str(c.get_available_balance("USDT")),
                "available_shares": {o.token_id: str(c.get_available_balance(o.base_asset)) for o in self.market.outcomes},
                **{o.name + ":" + o.token_id: str(c.get_balance(o.base_asset)) for o in self.market.outcomes}},
                "limits": {name: str(getattr(self.config, name)) for name in
                           ("order_amount", "min_order_amount", "min_order_notional", "amount_step", "max_inventory")},
                "plans": {token: {"mid": str(plan.mid) if plan.mid is not None else None,
                                  "quotes": [q.as_dict() for q in plan.quotes], "reasons": plan.reasons}
                          for token, plan in plans.items()}}

    async def stop(self, timeout=60):
        async def drain():
            self.last_cancel = -float("inf")
            while self.orders():
                await self.cancel()
                await self.connector._status_polling_loop_fetch_updates()
                self.store.save(self.connector.tracking_states)
                if self.orders():
                    await asyncio.sleep(1)
        try:
            await asyncio.wait_for(drain(), timeout)
        finally:
            self.store.save(self.connector.tracking_states)
