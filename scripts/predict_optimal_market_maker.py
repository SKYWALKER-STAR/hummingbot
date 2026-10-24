"""Independent Predict paper HJB solver, recorder, calibrator and live maker."""
import argparse
import asyncio
from dataclasses import asdict
import gzip
import json
import math
import os
from pathlib import Path
import signal
import time

from hummingbot.strategy.predict_optimal_market_making.calibration import atomic_json, fit
from hummingbot.strategy.predict_optimal_market_making.config import StrategyConfig
from hummingbot.strategy.predict_optimal_market_making.model import HJBTable, ModelConfig, simulate


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command',required=True)
    solve = commands.add_parser('solve',help='Reproduce paper HJB (default: paper Table 1)')
    solve.add_argument('--model',type=Path,help='ModelConfig-only JSON')
    solve.add_argument('--output',required=True,type=Path)
    sim = commands.add_parser('simulate',help='Synthetic Monte Carlo, equal inventory caps')
    sim.add_argument('--table',required=True,type=Path)
    sim.add_argument('--paths',type=int,default=1000)
    sim.add_argument('--steps',type=int,default=1000)
    sim.add_argument('--seed',type=int,default=7)
    sim.add_argument('--output',required=True,type=Path)
    run = commands.add_parser('run',help='Record + calibrate + preview/live simultaneously')
    run.add_argument('--market-id',required=True,type=int)
    run.add_argument('--token-id',required=True)
    run.add_argument('--config',required=True,type=Path)
    run.add_argument('--data-dir',required=True,type=Path)
    run.add_argument('--live',action='store_true')
    run.add_argument('--duration',type=float,default=0.)
    calibration = commands.add_parser('calibrate',help='Offline refit from recorder sessions')
    calibration.add_argument('--config',required=True,type=Path)
    calibration.add_argument('--data-dir',required=True,type=Path)
    calibration.add_argument('--market-id',required=True,type=int)
    calibration.add_argument('--token-id',required=True)
    calibration.add_argument('--output',required=True,type=Path)
    export = commands.add_parser('export',help='Convert raw event types to analysis Parquet (requires pyarrow)')
    export.add_argument('--data-dir',required=True,type=Path)
    export.add_argument('--market-id',required=True,type=int)
    export.add_argument('--output-dir',required=True,type=Path)
    args = parser.parse_args(argv)
    if hasattr(args,'market_id') and args.market_id <= 0:
        parser.error('market-id must be positive')
    if hasattr(args,'token_id') and not args.token_id.isdigit():
        parser.error('token-id must be numeric')
    if hasattr(args,'duration') and (not math.isfinite(args.duration) or args.duration < 0):
        parser.error('duration must be finite and nonnegative')
    return args


def records(root,market_id):
    # Streaming reader; incomplete/corrupt archives fail explicitly, not silently skipped.
    paths = sorted(Path(root).glob(f'raw/date=*/market_id={market_id}/events-*.jsonl.gz'))
    for path in paths:
        with gzip.open(path,'rt',encoding='utf-8') as handle:
            for line in handle:
                yield json.loads(line)


def offline_calibration(args):
    cfg = StrategyConfig.load(args.config)
    books,exposures,sessions = [],[],set()
    # Require matching configuration/context. Do not pool different outcomes or horizons.
    rows = records(args.data_dir,args.market_id)
    for event in rows:
        data = event['data']
        if event['kind']=='session_start':
            if (data.get('token_id')==args.token_id and data.get('config')==asdict(cfg)
                    and data.get('mode')=='LIVE'):
                sessions.add(event['session_id'])
        if event['session_id'] not in sessions:
            continue
        if event['kind']=='book_sample':
            books.append(dict(data,session_id=event['session_id']))
        elif event['kind']=='exposure':
            exposures.append(data)
    candidate,report = fit(cfg.model,books,exposures,cfg.start,cfg.horizon,
                           cfg.calibration_min_windows,cfg.calibration_min_fills)
    if candidate:
        HJBTable.solve(candidate).save(args.output.with_suffix('.npz'))
    atomic_json(args.output,report)
    print(json.dumps(report,default=str))


def export_parquet(args):
    import pyarrow as pa
    import pyarrow.parquet as pq
    args.output_dir.mkdir(parents=True,exist_ok=True)
    batches,counts = {},{}
    def flush(kind):
        rows = batches.get(kind,[])
        if not rows:
            return
        number = counts.get(kind,0)
        directory = args.output_dir / kind
        directory.mkdir(exist_ok=True)
        # Keep payload JSON to avoid inconsistent inferred schemas across batches.
        pq.write_table(pa.Table.from_pylist(rows),directory/f'part-{number:06d}.parquet',compression='zstd')
        counts[kind] = number+1
        batches[kind] = []
    if any(args.output_dir.iterdir()):
        raise ValueError('Export directory must be empty to prevent mixing/overwriting datasets')
    for event in records(args.data_dir,args.market_id):
        kind = event['kind']
        flat = {key:value for key,value in event.items() if key!='data'}
        flat['payload_json'] = json.dumps(event['data'],separators=(',',':'))
        batches.setdefault(kind,[]).append(flat)
        if len(batches[kind]) >= 10000:
            flush(kind)
    for kind in batches:
        flush(kind)
    atomic_json(args.output_dir/'manifest.json',dict(parts=counts,schema_version=1))


def build_candidate(cfg,table,books,exposures,root):
    candidate,report = fit(table.config,books,exposures,cfg.start,cfg.horizon,
                           cfg.calibration_min_windows,cfg.calibration_min_fills)
    result = None
    if candidate:
        try:
            result = HJBTable.solve(candidate)
            result.save(root/'calibration'/f'{candidate.version}.npz')
        except (ArithmeticError,ValueError) as exc:
            report.update(status='rejected_solver_failure',error_type=type(exc).__name__)
    atomic_json(root/'calibration'/f'report-{time.time_ns()}.json',report)
    return result,report


async def run(args):
    # Lazy imports keep paper reproduction and --help independent of Cython/SDKs.
    from hummingbot.core.clock import Clock
    from hummingbot.core.clock_mode import ClockMode
    from hummingbot.core.data_type.common import OrderType, TradeType
    from hummingbot.strategy.predict_market_making.execution import StateStore
    from hummingbot.strategy.predict_optimal_market_making.connector import RecordingPredictExchange
    from hummingbot.strategy.predict_optimal_market_making.execution import Maker, plan, reject_unknown_orders
    from hummingbot.strategy.predict_optimal_market_making.recorder import Observations, Recorder

    cfg = StrategyConfig.load(args.config)
    if time.time() >= cfg.cutoff:
        raise ValueError('Trading cutoff already passed; update explicit market timestamps')
    api_key = os.environ.get('PREDICT_API_KEY','')
    if not api_key:
        raise ValueError('PREDICT_API_KEY required')
    args.data_dir.mkdir(parents=True,exist_ok=True)
    table_path = args.data_dir/'models'/f'{cfg.model.version}.npz'
    table = await asyncio.to_thread(HJBTable.load,table_path) if table_path.exists() else await asyncio.to_thread(HJBTable.solve,cfg.model)
    if table.config != cfg.model:
        raise ValueError('Cached model mismatch')
    if not table_path.exists():
        await asyncio.to_thread(table.save,table_path)
    active_path = args.data_dir/'calibration'/'active.json'
    if cfg.auto_activate_calibration and active_path.exists():
        active = json.loads(active_path.read_text(encoding='utf-8'))
        expected = dict(config=asdict(cfg),market_id=args.market_id,token_id=args.token_id)
        if active.get('context') == expected:
            candidate_config = ModelConfig(**active['model'])
            loaded = await asyncio.to_thread(HJBTable.load,args.data_dir/'calibration'/f'{candidate_config.version}.npz')
            if loaded.config != candidate_config:
                raise ValueError('Active calibration artifact mismatch')
            table = loaded
    recorder = Recorder(args.data_dir,args.market_id)
    observations = Observations(recorder,args.token_id,cfg.model.trade_size,cfg.max_book_age_seconds)
    connector = store = maker = clock_task = calibration_task = None
    stop = asyncio.Event()
    handlers = {}
    for signum in (signal.SIGINT,signal.SIGTERM):
        handlers[signum] = signal.getsignal(signum)
        signal.signal(signum,lambda *_:stop.set())
    try:
        connector = RecordingPredictExchange(
            predict_api_key=api_key,market_ids=[args.market_id],trading_required=args.live,
            predict_private_key=os.environ.get('PREDICT_PRIVATE_KEY','') if args.live else '',
            predict_account_address=os.environ.get('PREDICT_ACCOUNT_ADDRESS','') if args.live else '',
            recorder=recorder,observations=observations,order_ttl=cfg.order_ttl,
            max_book_age=cfg.max_book_age_seconds)
        await connector.initialize_markets()
        market = connector.markets_by_id[args.market_id]
        outcome = market.outcome(token_id=args.token_id)
        recorder.emit('session_start',mode='LIVE' if args.live else 'READ_ONLY',token_id=args.token_id,
                      config=asdict(cfg),model_version=table.config.version,market=asdict(market))
        if args.live:
            address = connector._signer.address.lower()
            store = StateStore(args.data_dir/'state'/f'{address}-{args.market_id}.json',
                               dict(account=address,market_id=args.market_id,domain=connector.domain,
                                    strategy='predict_optimal_market_making',token_id=args.token_id))
            connector.restore_tracking_states(store.load())
            known = {o.exchange_order_id.lower() for o in connector._order_tracker.all_updatable_orders.values() if o.exchange_order_id}
            tokens = {o.token_id for o in market.outcomes}
            remote = await connector.client.pages('/v1/orders',first='100')
            reject_unknown_orders(remote,known,tokens)
            def checkpoint(states):
                store.save(states)
                recorder.require_healthy()
                if stop.is_set() or connector.metadata_halt or not cfg.start <= time.time() < cfg.cutoff:
                    raise RuntimeError('Submission blocked outside trading window or during shutdown')
            connector.before_order_submit = checkpoint
            maker = Maker(connector,market,outcome,cfg,table,store,recorder,OrderType,TradeType)
        clock = Clock(ClockMode.REALTIME)
        clock.add_iterator(connector)
        with clock:
            clock_task = asyncio.create_task(clock.run())
            started,last_calibration = time.monotonic(),time.monotonic()
            became_ready = False
            last_connected = None
            try:
                while not stop.is_set() and (args.duration==0 or time.monotonic()-started < args.duration):
                    if clock_task.done():
                        await clock_task
                        raise RuntimeError('Hummingbot clock stopped')
                    recorder.require_healthy()
                    connected = connector.market_data_stream_connected
                    if connected != last_connected:
                        recorder.emit('connection',connected=connected)
                        if not connected:
                            observations.gap('market_stream_disconnected')
                        last_connected = connected
                    if calibration_task and calibration_task.done():
                        candidate,report = await calibration_task
                        calibration_task = None
                        recorder.emit('calibration',**report)
                        if candidate and cfg.auto_activate_calibration:
                            table = candidate
                            if maker:
                                maker.table = candidate
                            atomic_json(args.data_dir/'calibration'/'active.json',
                                        dict(model=asdict(candidate.config),report=report,activated_utc=time.time(),
                                             context=dict(config=asdict(cfg),market_id=args.market_id,token_id=args.token_id)))
                            recorder.emit('model_activated',version=table.config.version)
                    if calibration_task is None and time.monotonic()-last_calibration >= cfg.calibration_seconds:
                        books,exposures = observations.snapshot()
                        calibration_task = asyncio.create_task(asyncio.to_thread(build_candidate,cfg,table,books,exposures,args.data_dir))
                        last_calibration = time.monotonic()
                    report = await maker.step() if maker else dict(mode='READ_ONLY',
                        ready=connector.ready and connected,
                        plan=plan(connector,market,outcome,cfg,table,preview=True))
                    recorder.emit('decision',**report)
                    print(json.dumps(report,default=str,allow_nan=False),flush=True)
                    became_ready |= report['ready']
                    if not became_ready and time.monotonic()-started > cfg.startup_timeout:
                        raise TimeoutError('Connector startup timeout')
                    if time.time() >= cfg.cutoff:
                        break
                    try:
                        await asyncio.wait_for(stop.wait(),timeout=cfg.interval_seconds)
                    except asyncio.TimeoutError:
                        pass
            finally:
                stop.set()
                if maker:
                    await maker.stop()
                    recorder.emit('shutdown',cancellations_confirmed=True)
    finally:
        # Cleanup continues even when cancellation reconciliation failed.
        stop.set()
        if clock_task:
            clock_task.cancel()
            await asyncio.gather(clock_task,return_exceptions=True)
        try:
            if connector:
                await connector.stop_network()
        finally:
            try:
                if calibration_task:
                    await asyncio.gather(calibration_task,return_exceptions=True)
            finally:
                if store:
                    store.close()
                for signum,handler in handlers.items():
                    signal.signal(signum,handler)
                await asyncio.to_thread(recorder.close)


def main(argv=None):
    args = parse_args(argv)
    if args.command=='solve':
        cfg = ModelConfig(**json.loads(args.model.read_text(encoding='utf-8'))) if args.model else ModelConfig()
        table = HJBTable.solve(cfg)
        table.save(args.output)
        print(json.dumps(dict(model_version=cfg.version,max_iterations=max(table.iterations))))
    elif args.command=='simulate':
        table = HJBTable.load(args.table)
        result = {name:simulate(table,args.paths,args.steps,args.seed,benchmark) for name,benchmark in [('optimal',False),('myopic',True)]}
        atomic_json(args.output,result)
        print(json.dumps(result))
    elif args.command=='run':
        asyncio.run(run(args))
    elif args.command=='calibrate':
        offline_calibration(args)
    else:
        export_parquet(args)


if __name__=='__main__':
    main()
