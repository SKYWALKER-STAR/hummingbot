"""Bounded, fail-closed, hourly gzip JSONL recorder. No credentials accepted."""
from collections import deque
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import queue
import threading
import time
import uuid


class Recorder:
    def __init__(self, root, market_id, max_queue=50000):
        self.root, self.market_id = Path(root), market_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.session = uuid.uuid4().hex
        self.sequence = 0
        self.error = None
        self.queue = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._write, name='predict-recorder', daemon=True)
        self.thread.start()

    def emit(self, kind, **data):
        self.sequence += 1
        record = dict(schema_version=1, session_id=self.session, sequence=self.sequence,
                      received_utc_ns=time.time_ns(), received_monotonic_ns=time.monotonic_ns(),
                      market_id=self.market_id, kind=kind, data=data)
        if self.error:
            return False
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.error = 'recorder_queue_overflow'
            return False
        return True

    def require_healthy(self):
        if self.error or not self.thread.is_alive():
            raise RuntimeError(self.error or 'recorder_thread_stopped')

    def _write(self):
        handle, bucket, last_flush = None, None, time.monotonic()
        try:
            while not self._stop.is_set() or not self.queue.empty():
                try:
                    record = self.queue.get(timeout=.2)
                except queue.Empty:
                    record = None
                if record is not None:
                    dt = datetime.fromtimestamp(record['received_utc_ns'] / 1e9, timezone.utc)
                    key = (dt.strftime('%Y-%m-%d'), dt.strftime('%H'))
                    if key != bucket:
                        if handle:
                            handle.close()
                        path = self.root / 'raw' / f'date={key[0]}' / f'market_id={self.market_id}'
                        path.mkdir(parents=True, exist_ok=True)
                        # One stream per session, unique filenames; never append to damaged gzip.
                        handle = gzip.open(path / f'events-{key[1]}-{self.session}.jsonl.gz', 'wt', encoding='utf-8')
                        bucket = key
                    handle.write(json.dumps(record, default=str, allow_nan=False, separators=(',', ':')) + '\n')
                    self.queue.task_done()
                if handle and time.monotonic() - last_flush >= 1:
                    handle.flush()
                    os.fsync(handle.fileno())
                    last_flush = time.monotonic()
        except Exception as exc:
            # Do not serialize arbitrary exception messages that might include credentials.
            self.error = 'recorder_write_failed:' + type(exc).__name__
        finally:
            if handle:
                try:
                    handle.close()
                except Exception as exc:
                    self.error = 'recorder_close_failed:' + type(exc).__name__

    def close(self):
        self._stop.set()
        self.thread.join(timeout=15)
        if self.thread.is_alive():
            raise RuntimeError('recorder_shutdown_timeout')
        if self.error:
            raise RuntimeError(self.error)


class Observations:
    """REST-observed exposure windows. No fabricated exchange fill timestamps.

    Counts are fixed-size-equivalent executed volume, not number of fills. Only
    full-size, stable-mid, gap-free windows qualify for approximate calibration.
    The first observation of any order (including recovery) seeds a baseline.
    """
    def __init__(self, recorder, token_id, trade_size, max_book_age=30, limit=200000):
        self.recorder, self.token_id, self.trade_size = recorder, str(token_id), float(trade_size)
        self.max_book_age = max_book_age
        self.books = deque(maxlen=limit)
        self.exposures = deque(maxlen=limit)
        self.active = {}
        self.mid = None
        self.book_at = None
        self.tick = .01
        self.last_sample = None
        self.last_stamp = None

    def book(self, data, market):
        # Raw book is a market-level book. Complement is derived once, not double counted.
        self.recorder.emit('orderbook', source='CONNECTOR_ACCEPTED_SNAPSHOT', payload=data,
                           exchange_time_ms=data.get('updateTimestampMs'))
        outcome = market.outcome(token_id=self.token_id)
        parsed = market.book(data, outcome)
        now = time.monotonic()
        if self.book_at is not None and now-self.book_at > self.max_book_age:
            for row in self.active.values():
                row['healthy'] = False
        self.tick = float(market.tick_size)
        self.mid = None
        if parsed['bids'] and parsed['asks']:
            bid, ask = float(parsed['bids'][0][0]), float(parsed['asks'][0][0])
            if 0 < bid < ask < 1:
                self.mid = (bid + ask) / 2
        self.book_at = now
        stamp = int(data['updateTimestampMs'])
        fresh_update = self.last_stamp is None or stamp > self.last_stamp
        self.last_stamp = stamp
        for row in self.active.values():
            if self.mid is None:
                row['healthy'] = False
            else:
                row['min_mid'] = min(row['min_mid'], self.mid)
                row['max_mid'] = max(row['max_mid'], self.mid)
        if self.mid and fresh_update and (self.last_sample is None or now-self.last_sample >= 5):
            sample = dict(utc=time.time(), mono=now, mid=self.mid)
            self.books.append(sample)
            self.recorder.emit('book_sample',token_id=self.token_id,**sample)
            self.last_sample = now

    def gap(self, reason):
        for row in self.active.values():
            row['healthy'] = False
        self.mid = None
        self.recorder.emit('connection_gap', reason=reason)

    def status(self, order, state, cumulative):
        now = time.monotonic()
        oid = order.client_order_id
        if str(order.trading_pair) == '':
            return
        previous = self.active.pop(oid, None)
        cumulative = float(cumulative)
        data = dict(order_id=oid, order_hash=order.exchange_order_id, token_id=self.token_id,
                    side=order.trade_type.name, price=str(order.price), amount=str(order.amount),
                    cumulative_filled=cumulative, state=state, fill_source='REST_CUMULATIVE',
                    fill_time_source='LOCAL_REST_OBSERVATION', price_and_fee='ESTIMATED')
        if previous:
            delta = max(0., cumulative - previous['cumulative'])
            dt = now - previous['mono']
            healthy = (previous['healthy'] and self.mid is not None and self.book_at is not None
                       and now-self.book_at <= self.max_book_age and 0 < dt <= 60
                       and abs(previous['remaining']-self.trade_size) < 1e-8
                       and previous['max_mid']-previous['min_mid'] <= 2*self.tick
                       and cumulative >= previous['cumulative'])
            row = dict(utc=time.time(), start_utc=previous['utc'], seconds=dt, side=order.trade_type.name,
                       mid=previous['mid'], price=float(order.price), size=previous['remaining'],
                       filled_volume=delta, count=delta/self.trade_size, eligible=healthy,
                       order_id=oid, interval_censored=True)
            self.recorder.emit('exposure', **row)
            if healthy:
                self.exposures.append(row)
            data['incremental_filled'] = delta
        else:
            data['incremental_filled'] = None  # Never treat pre-restart fills as new samples.
        self.recorder.emit('order_status', **data)
        if state in ('OPEN', 'PARTIALLY_FILLED') and self.mid is not None:
            self.active[oid] = dict(mono=now, utc=time.time(), cumulative=cumulative, mid=self.mid,
                                   min_mid=self.mid, max_mid=self.mid,
                                   remaining=float(order.amount)-cumulative,
                                   healthy=self.book_at is not None and now-self.book_at <= self.max_book_age)

    def snapshot(self):
        return list(self.books), list(self.exposures)
