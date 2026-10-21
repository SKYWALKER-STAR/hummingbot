"""Load dependency-light connector modules without Hummingbot's compiled runtime.

This is only for the standalone unit suite; it does not mock any exchange behavior.
The separate exchange suite imports Hummingbot normally.
"""
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]


def load(name):
    path = ROOT / "hummingbot/connector/exchange/predict" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
