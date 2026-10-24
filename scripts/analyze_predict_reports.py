"""Read-only analysis of Predict reports, exported Parquet, or raw JSONL archives.

No Hummingbot runtime imports, API keys, network requests, or trading actions.
Only Parquet input requires the optional pyarrow dependency.
"""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import gzip
import json
import math
from pathlib import Path


STATUS = {
    'insufficient_data': '窗口或成交样本不足，继续采集',
    'insufficient_two_sided_fills': '训练集买卖两侧成交不足',
    'insufficient_spread_variation': '挂单距离变化不足，参数难以识别',
    'fit_failed': '参数拟合失败',
    'rejected_validation': '验证改善不足或参数落在搜索边界',
    'rejected_parameter_jump': '参数变化过大',
    'rejected_solver_failure': '候选 HJB 求解失败',
    'candidate_validated': '候选通过当前校准检查，不等于已激活或收益验证通过',
}


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


def utc(ns):
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).isoformat() if ns is not None else None


def json_loads(value):
    def invalid(text):
        raise ValueError('Nonfinite JSON number: ' + text)
    def floating(text):
        result = float(text)
        if not math.isfinite(result):
            raise ValueError('Nonfinite JSON number: ' + text)
        return result
    return json.loads(value, parse_constant=invalid, parse_float=floating)


def input_files(path):
    path = Path(path)
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise ValueError(f'Input does not exist: {path}')
    # An export and raw source are alternative representations. Never count both.
    if (path / 'manifest.json').is_file():
        return sorted(path.glob('*/*.parquet'))
    raw = path / 'raw'
    if raw.is_dir():
        files = sorted(raw.glob('date=*/market_id=*/events-*.jsonl.gz'))
        files += sorted((path / 'calibration').glob('report-*.json'))
        if (path / 'calibration' / 'active.json').exists():
            files.append(path / 'calibration' / 'active.json')
        return files
    return sorted(p for p in path.rglob('*') if p.is_file() and (
        p.suffix in ('.parquet', '.json', '.jsonl') or p.name.endswith('.jsonl.gz'))
        and p.name != 'manifest.json')


def documents(path):
    """Yield event/report objects in bounded batches with source error context."""
    try:
        if path.suffix == '.parquet':
            try:
                import pyarrow.parquet as pq
            except ImportError as exc:
                raise ValueError('Parquet input requires: python -m pip install pyarrow') from exc
            for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
                for row in batch.to_pylist():
                    payload = row.pop('payload_json', None)
                    if payload is None:
                        raise ValueError('Missing payload_json: expected strategy export schema')
                    yield dict(row, data=json_loads(payload))
        elif path.name.endswith('.jsonl.gz') or path.suffix == '.jsonl':
            opener = gzip.open if path.name.endswith('.gz') else open
            with opener(path, 'rt', encoding='utf-8') as handle:
                for line in handle:
                    if line.strip():
                        yield json_loads(line)
        else:
            yield json_loads(path.read_text(encoding='utf-8-sig'))
    except (OSError, EOFError, ValueError) as exc:
        raise ValueError(f'{path}: {exc}') from exc


class Analysis:
    def __init__(self, preview_rows=3, market_id=None):
        self.preview_rows, self.market_id = preview_rows, market_id
        self.events = Counter()
        self.actions, self.reasons = Counter(), Counter()
        self.exposures = defaultdict(lambda: dict(windows=0, eligible_windows=0,
            eligible_seconds=0., eligible_filled_shares=0., equivalent_fills=0.))
        self.orders = {}
        self.previews = defaultdict(list)
        self.calibrations, self.simulations, self.activations = [], [], []
        self.sessions, self.markets = set(), set()
        self.last_sequences = {}
        self.first_ns = self.last_ns = None
        self.ignored = self.filtered = 0

    def calibration(self, data, source):
        old, new = number(data.get('old_validation_score')), number(data.get('new_validation_score'))
        self.calibrations.append(dict(source=source, status=data.get('status'),
            interpretation=STATUS.get(data.get('status'), '未知状态'), windows=data.get('windows'),
            train_fills=data.get('train_fills'), validation_fills=data.get('validation_fills'),
            old_validation_score=old, new_validation_score=new,
            score_improvement=new-old if old is not None and new is not None else None,
            candidate_version=data.get('candidate_version'), volatility_status=data.get('volatility_status'),
            candidate=data.get('candidate'), training_end_utc=data.get('training_end_utc'),
            validation_end_utc=data.get('validation_end_utc')))

    def add(self, obj, source):
        if not isinstance(obj, dict):
            raise ValueError(f'{source}: expected a JSON object')
        if 'kind' not in obj:
            if self.market_id is not None:
                # Standalone reports have no reliable market identity; do not attribute them.
                self.filtered += 1
                return
            if 'status' in obj and ('windows' in obj or 'candidate' in obj):
                self.calibration(obj, source)
            elif 'optimal' in obj and 'myopic' in obj:
                self.simulations.append(dict(source=source, **obj))
            elif 'activated_utc' in obj and 'model' in obj:
                self.activations.append(dict(source=source, **obj))
            else:
                self.ignored += 1
            return
        if self.market_id is not None and str(obj.get('market_id')) != str(self.market_id):
            self.filtered += 1
            return
        kind, data = obj['kind'], obj.get('data')
        if not isinstance(kind, str) or not isinstance(data, dict):
            raise ValueError(f'{source}: malformed event kind/data')
        market, session = str(obj.get('market_id', 'unknown')), str(obj.get('session_id', 'unknown'))
        sequence = obj.get('sequence')
        if sequence is not None:
            if type(sequence) is not int or sequence < 1:
                raise ValueError(f'{source}: invalid event sequence')
            key = (market, session, kind)
            if sequence <= self.last_sequences.get(key, 0):
                raise ValueError(f'{source}: duplicate or out-of-order events; do not combine overlapping exports')
            self.last_sequences[key] = sequence
        self.events[kind] += 1
        self.markets.add(market)
        self.sessions.add((market, session))
        stamp = number(obj.get('received_utc_ns'))
        if stamp is not None:
            self.first_ns = stamp if self.first_ns is None else min(self.first_ns, stamp)
            self.last_ns = stamp if self.last_ns is None else max(self.last_ns, stamp)
        if len(self.previews[kind]) < self.preview_rows:
            self.previews[kind].append(obj)
        if kind == 'calibration':
            self.calibration(data, source + ':' + session)
        elif kind == 'model_activated':
            self.activations.append(dict(source=source, market_id=market, session_id=session, **data))
        elif kind == 'decision':
            self.actions[str(data.get('action', data.get('mode', 'unknown')))] += 1
            plan = data.get('plan') or {}
            reasons = list(plan.get('reasons') or [])
            if plan.get('reason'):
                reasons.append(plan['reason'])
            self.reasons.update(str(reason) for reason in reasons)
        elif kind == 'exposure':
            group = self.exposures[(market, session, str(data.get('side', 'unknown')))]
            group['windows'] += 1
            seconds, volume, count = (number(data.get(k)) for k in ('seconds', 'filled_volume', 'count'))
            if data.get('eligible') is True:
                if any(v is None or v < 0 for v in (seconds, volume, count)) or seconds <= 0:
                    raise ValueError(f'{source}: invalid eligible exposure values')
                group['eligible_windows'] += 1
                group['eligible_seconds'] += seconds
                group['eligible_filled_shares'] += volume
                group['equivalent_fills'] += count
        elif kind == 'order_status':
            self.order_status(data, market, session, stamp, source)

    def order_status(self, data, market, session, stamp, source):
        oid = data.get('order_id')
        cumulative = number(data.get('cumulative_filled'))
        if not oid or cumulative is None or cumulative < 0:
            raise ValueError(f'{source}: invalid order status')
        key = (market, session, str(oid))
        previous = self.orders.get(key)
        if previous and stamp is not None and previous['last_ns'] is not None and stamp < previous['last_ns']:
            raise ValueError(f'{source}: out-of-order order status; analyze each export separately')
        if previous is None:
            initial = number(data.get('incremental_filled'))
            initial = min(cumulative, max(0., initial)) if initial is not None else 0.
            previous = dict(market_id=market, session_id=session, order_id=str(oid),
                token_id=data.get('token_id'), side=data.get('side'), observed_filled_shares=initial,
                cumulative_filled=cumulative, final_observed_state=data.get('state'), last_ns=stamp,
                estimated_notional=0., price_and_fee='ESTIMATED')
            self.orders[key] = previous
            delta = initial
        else:
            if cumulative < previous['cumulative_filled']:
                raise ValueError(f'{source}: cumulative fills regressed for {oid}')
            delta = cumulative-previous['cumulative_filled']
            previous['observed_filled_shares'] += delta
            previous.update(cumulative_filled=cumulative, final_observed_state=data.get('state'), last_ns=stamp)
        price = number(data.get('price'))
        if price is None or not 0 < price < 1:
            raise ValueError(f'{source}: invalid order limit price')
        previous['estimated_notional'] += delta*price

    def result(self):
        exposures = []
        for (market, session, side), value in sorted(self.exposures.items()):
            exposures.append(dict(market_id=market, session_id=session, side=side, **value,
                eligible_fraction=ratio(value['eligible_windows'], value['windows']),
                filled_shares_per_second=ratio(value['eligible_filled_shares'], value['eligible_seconds']),
                equivalent_fills_per_second=ratio(value['equivalent_fills'], value['eligible_seconds'])))
        orders = sorted(self.orders.values(), key=lambda r: (r['market_id'], r['session_id'], r['order_id']))
        return dict(schema_version=1, markets=sorted(self.markets), sessions=len(self.sessions),
            first_received_utc=utc(self.first_ns), last_received_utc=utc(self.last_ns),
            event_counts=dict(self.events), decision_actions=dict(self.actions), decision_reasons=dict(self.reasons),
            exposures=exposures, orders=orders, calibrations=self.calibrations,
            simulations=self.simulations, activations=self.activations,
            ignored_objects=self.ignored, filtered_objects=self.filtered,
            limitations=[
                'Exposure seconds are summed order-seconds, not wall-clock runtime.',
                'Fill times are REST observation times; limit prices and fees are estimates.',
                'Observed fills exclude unknown pre-session/recovery history; sessions stay separate.',
                'No exact realized PnL is inferred from order statuses.',
                'Validation scores from different datasets cannot be directly ranked.',
                'A validated calibration is not evidence of live activation or profitability.',
            ])


def write_csv(path, rows, fields):
    with path.open('w', newline='', encoding='utf-8-sig') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def markdown(result):
    lines = ['# Predict 报告分析', '',
        f"事件总数：{sum(result['event_counts'].values())}；市场：{', '.join(result['markets']) or '无行情事件'}；会话：{result['sessions']}",
        f"接收时间范围（UTC）：{result['first_received_utc']} ～ {result['last_received_utc']}",
        '', '## 事件统计', '', '| 事件 | 记录数 |', '|---|---:|']
    lines += [f'| {kind} | {count} |' for kind, count in sorted(result['event_counts'].items())]
    lines += ['', '## 挂单曝光', '',
        '曝光按市场、会话、买卖方向分别统计，单位为挂单秒，不是程序运行秒。', '',
        '| 市场 | 会话 | 方向 | 窗口/合格 | 合格曝光秒 | 合格成交份额 | 份额/秒 |',
        '|---|---|---|---:|---:|---:|---:|']
    for row in result['exposures']:
        rate = row['filled_shares_per_second']
        lines.append(f"| {row['market_id']} | {row['session_id']} | {row['side']} | {row['windows']}/{row['eligible_windows']} | {row['eligible_seconds']:.3f} | {row['eligible_filled_shares']:.6f} | {rate if rate is not None else 'N/A'} |")
    if not result['exposures']:
        lines += ['', '没有曝光记录；只读行情运行不会产生真实挂单曝光。']
    lines += ['', '## 报价动作与跳过原因', '']
    lines += [f'- 动作 `{key}`：{count}' for key, count in sorted(result['decision_actions'].items())]
    lines += [f'- 原因 `{key}`：{count}' for key, count in sorted(result['decision_reasons'].items())]
    lines += ['', '## 校准结果', '']
    for row in result['calibrations']:
        lines += [f"- `{row['source']}`：`{row['status']}`，{row['interpretation']}；窗口 {row['windows']}，训练/验证等价成交 {row['train_fills']}/{row['validation_fills']}，验证得分改善 {row['score_improvement']}。"]
    lines += ['', f"激活记录数：{len(result['activations'])}。候选通过不代表已经激活；active.json 为历史激活记录，不证明进程当前运行。", '', '## 合成模拟', '']
    metrics = ('mean_pnl', 'std_pnl', 'mean_abs_terminal_inventory', 'var_95', 'es_95')
    for report in result['simulations']:
        lines += [f"来源：`{report['source']}`", '', '| 指标 | optimal | myopic |', '|---|---:|---:|']
        for key in metrics:
            lines.append(f"| {key} | {report['optimal'].get(key)} | {report['myopic'].get(key)} |")
        lines += ['', '这是合成模型结果，不是实际账户收益。', '']
    lines += ['', '## 解释边界', '',
        '- 成交时间来自 REST 观察，成交价和费用存在估算限制，不计算精确实盘 PnL。',
        '- 各会话的已观察成交单独统计，重启前未知历史不补算。',
        '- 不同数据集的校准得分不能直接比较；相同报告在多个来源出现时会分别列出。',
        '- 此脚本不读取密钥、不联网、不改变模型或挂单。', '']
    return '\n'.join(lines)


def analyze(inputs, output, preview_rows=3, market_id=None):
    if output.exists() and any(output.iterdir()):
        raise ValueError('Output directory must be empty; choose a new analysis directory')
    files = []
    for path in inputs:
        files.extend(input_files(path))
    if not files:
        raise ValueError('No supported input files found')
    resolved = [p.resolve() for p in files]
    if len(set(resolved)) != len(resolved):
        raise ValueError('Overlapping input paths: the same file would be counted twice')
    if any(p.suffix == '.parquet' for p in files) and any(p.name.endswith('.jsonl.gz') or p.suffix == '.jsonl' for p in files):
        raise ValueError('Do not combine raw events with Parquet exports; they may contain the same events')
    analysis = Analysis(preview_rows, market_id)
    for path in files:
        for obj in documents(path):
            analysis.add(obj, str(path))
    result = analysis.result()
    if not result['event_counts'] and not result['calibrations'] and not result['simulations'] and not result['activations']:
        raise ValueError('No matching strategy events or reports found')
    result['input_files'] = [str(p) for p in files]
    output.mkdir(parents=True, exist_ok=True)
    (output/'summary.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    (output/'summary.md').write_text(markdown(result), encoding='utf-8')
    write_csv(output/'event_counts.csv', [dict(kind=k, records=v) for k, v in sorted(analysis.events.items())], ['kind', 'records'])
    write_csv(output/'exposure_summary.csv', result['exposures'], ['market_id', 'session_id', 'side', 'windows',
        'eligible_windows', 'eligible_fraction', 'eligible_seconds', 'eligible_filled_shares', 'equivalent_fills',
        'filled_shares_per_second', 'equivalent_fills_per_second'])
    write_csv(output/'orders.csv', result['orders'], ['market_id', 'session_id', 'order_id', 'token_id', 'side',
        'observed_filled_shares', 'cumulative_filled', 'final_observed_state', 'estimated_notional', 'price_and_fee'])
    write_csv(output/'calibrations.csv', result['calibrations'], ['source', 'status', 'interpretation', 'windows',
        'train_fills', 'validation_fills', 'score_improvement', 'candidate_version', 'volatility_status'])
    with (output/'preview.jsonl').open('w', encoding='utf-8') as handle:
        for kind in sorted(analysis.previews):
            for row in analysis.previews[kind]:
                handle.write(json.dumps(row, ensure_ascii=False, allow_nan=False)+'\n')
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, action='append', type=Path,
                        help='Export/data directory or JSON report; repeat to add separate reports')
    parser.add_argument('--output-dir', required=True, type=Path, help='New or empty analysis output directory')
    parser.add_argument('--market-id', type=int, help='Filter event records; unattributed standalone reports are excluded')
    parser.add_argument('--preview-rows', type=int, default=3, help='Expanded example rows per event kind')
    args = parser.parse_args(argv)
    if args.preview_rows < 0 or (args.market_id is not None and args.market_id <= 0):
        parser.error('preview-rows must be nonnegative and market-id must be positive')
    try:
        result = analyze(args.input, args.output_dir, args.preview_rows, args.market_id)
    except (ValueError, OSError) as exc:
        parser.exit(2, f'Analysis failed: {exc}\n')
    print(f"分析完成：{sum(result['event_counts'].values())} 条事件，{len(result['calibrations'])} 份校准结果，{len(result['simulations'])} 份模拟报告。")
    print(f"阅读报告：{args.output_dir / 'summary.md'}")
    print(f"完整结果：{args.output_dir / 'summary.json'}")


if __name__ == '__main__':
    main()
