"""
Shared fixtures for Fin_ETF_Auto unit tests (Korean Domestic ETFs).

Pre-import mocks for unavailable/private modules so project modules can be
imported without the real dependencies.
"""
import os
import sys
import json
import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Pre-import mocks — only modules that kept tests actually import transitively
# ---------------------------------------------------------------------------
_MODULES_TO_MOCK = [
    "kis_auth",
    "domestic_stock_functions",
    "FinanceDataReader",
    "yfinance",
    "exchange_calendars",
    "requests",
    "google.genai",
    "google",
    "telegram",
    "telegram.ext",
]

for mod_name in _MODULES_TO_MOCK:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Make project root importable
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_data_dir(tmp_path, monkeypatch):
    """Redirect db_manager file paths to an isolated temp directory."""
    import db_manager

    state_file = str(tmp_path / "portfolio_state.json")
    trade_file = str(tmp_path / "trade_history.json")
    value_file = str(tmp_path / "portfolio_value_history.json")

    monkeypatch.setattr(db_manager, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(db_manager, "STATE_FILE", state_file)
    monkeypatch.setattr(db_manager, "TRADE_HISTORY_FILE", trade_file)
    monkeypatch.setattr(db_manager, "VALUE_HISTORY_FILE", value_file)

    return tmp_path


@pytest.fixture
def sample_close_series():
    """200-day synthetic close-price series with mild uptrend."""
    np.random.seed(42)
    n = 200
    prices = 100 + np.cumsum(np.random.randn(n) * 0.5)
    prices = np.maximum(prices, 1.0)  # keep positive
    idx = pd.bdate_range(end=pd.Timestamp.now().normalize(), periods=n)
    if len(idx) < n:
        idx = pd.bdate_range(end=pd.Timestamp.now().normalize() + pd.Timedelta(days=5), periods=n)
    return pd.Series(prices, index=idx[:n], name="close")


@pytest.fixture
def sample_benchmark_ret(sample_close_series):
    """Daily return series to use as benchmark."""
    return sample_close_series.pct_change().dropna()


@pytest.fixture
def sample_portfolio_state():
    """Pre-built 4-slot state with slot 1 invested in Korean ETFs and slots 2-4 empty."""
    return {
        "slots": {
            "1": {
                "status": "invested",
                "buy_date": "2026-02-20",
                "target_sell_date": "2026-03-20",
                "cash_balance": 50000,
                "holdings": [
                    {
                        "ticker": "069500",
                        "name": "KODEX 200",
                        "shares": 10,
                        "buy_price": 35000,
                        "status": "active",
                    },
                    {
                        "ticker": "233740",
                        "name": "KODEX 코스닥150",
                        "shares": 5,
                        "buy_price": 12000,
                        "status": "active",
                    },
                ],
            },
            "2": {"status": "empty"},
            "3": {"status": "empty"},
            "4": {"status": "empty"},
        }
    }
