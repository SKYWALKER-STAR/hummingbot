"""Strict, explicit configuration. UTC timestamps must include timezone offsets."""
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
import json
import math
from pathlib import Path

from .model import ModelConfig


def utc_timestamp(value):
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('Timestamp must include timezone')
    return dt.timestamp()


@dataclass(frozen=True)
class StrategyConfig:
    start_utc: str
    resolution_utc: str
    trading_cutoff_utc: str
    model: ModelConfig = field(default_factory=lambda: ModelConfig(q_min=0, q_max=20, trade_size=5))
    interval_seconds: float = 2.
    refresh_seconds: float = 30.
    refresh_ticks: int = 2
    max_book_age_seconds: float = 30.
    min_level_amount: float = 1.
    min_order_amount: float = 1.
    min_order_notional: float = 1.
    max_cash_per_order: float = 10.
    cash_reserve: float = 5.
    calibration_seconds: float = 900.
    calibration_min_windows: int = 100
    calibration_min_fills: float = 20.
    auto_activate_calibration: bool = False
    preview_usdt: float = 100.
    preview_shares: float = 0.
    startup_timeout: float = 120.
    order_ttl: int = 300
    max_quote_ticks: int = 10000
    token_id: str | None = None

    def __post_init__(self):
        if self.token_id is not None:
            self.validate_token_id(self.token_id)
        if not self.start < self.cutoff <= self.end:
            raise ValueError('Require start < trading_cutoff <= resolution')
        if self.model.q_min != 0:
            raise ValueError('Live/preview model must use q_min=0; paper command supports shorts separately')
        for key, value in asdict(self).items():
            if key in ('model','start_utc','resolution_utc','trading_cutoff_utc','auto_activate_calibration','token_id'):
                continue
            if isinstance(value, bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f'Invalid {key}')
        for key in ('interval_seconds','refresh_seconds','max_book_age_seconds','calibration_seconds',
                    'max_cash_per_order','startup_timeout'):
            if getattr(self,key) <= 0:
                raise ValueError(f'{key} must be positive')
        for key in ('refresh_ticks','calibration_min_windows','order_ttl','max_quote_ticks'):
            if type(getattr(self,key)) is not int or getattr(self,key) < 1:
                raise ValueError(f'{key} must be a positive integer')
        if type(self.auto_activate_calibration) is not bool or self.order_ttl < 180:
            raise ValueError('auto_activate_calibration must be boolean; order_ttl >= 180')
        if not 0 <= self.preview_shares <= self.model.q_max:
            raise ValueError('Preview holdings outside model domain')
        if self.model.trade_size < self.min_order_amount:
            raise ValueError('Fixed trade size below minimum order amount')
        if Decimal(str(self.model.trade_size)) % Decimal('.000001'):
            raise ValueError('trade_size must be a multiple of 0.000001')

    @property
    def start(self):
        return utc_timestamp(self.start_utc)

    @property
    def end(self):
        return utc_timestamp(self.resolution_utc)

    @property
    def cutoff(self):
        return utc_timestamp(self.trading_cutoff_utc)

    @property
    def horizon(self):
        return self.end-self.start

    @staticmethod
    def validate_token_id(value):
        if not isinstance(value, str) or not value.isascii() or not value.isdigit() or int(value) <= 0:
            raise ValueError('token_id must be a positive decimal string (use JSON quotes)')
        return value

    def resolve_token_id(self, command_line=None):
        value = command_line if command_line is not None else self.token_id
        if value is None:
            raise ValueError('Provide token_id in the config file or --token-id on the command line')
        return self.validate_token_id(value)

    def matches_saved_config(self, saved):
        """Compare model/runtime settings; caller must check sibling token identity.

        Old sessions have no token_id inside config. Its source must not affect
        compatibility: identity lives in session_start/context.token_id already.
        """
        if not isinstance(saved, dict):
            return False
        expected = asdict(self)
        expected.pop('token_id', None)
        previous = dict(saved)
        previous.pop('token_id', None)
        return previous == expected

    @classmethod
    def load(cls,path):
        data = json.loads(Path(path).read_text(encoding='utf-8'), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        if not isinstance(data,dict):
            raise ValueError('Configuration must be an object')
        data['model'] = ModelConfig(**data['model']) if 'model' in data else ModelConfig(q_min=0,q_max=20,trade_size=5)
        return cls(**data)
