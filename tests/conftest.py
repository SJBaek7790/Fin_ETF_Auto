"""
Shared fixtures for Fin_ETF_Auto unit tests.

IMPORTANT: Mocks for unavailable/private modules (config, kis_auth, KIS SDK,
FinanceDataReader, yfinance, exchange_calendars) are registered at import time
so that project modules can be imported without the real dependencies.
"""
import os
import sys
import json
import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Pre-import mocks for modules that are not available in the test environment
# ---------------------------------------------------------------------------
_MODULES_TO_MOCK = [
    "config",
    "kis_auth",
    "order",
    "inquire_present_balance",
    "dailyprice",
    "FinanceDataReader",
    "yfinance",
]

for mod_name in _MODULES_TO_MOCK:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

# Make project root importable
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Also add .local_deps (for exchange_calendars, etc.)
_LOCAL_DEPS = os.path.join(_PROJECT_ROOT, ".local_deps")
if os.path.isdir(_LOCAL_DEPS) and _LOCAL_DEPS not in sys.path:
    sys.path.insert(0, _LOCAL_DEPS)


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
    # bdate_range may return fewer dates on weekends; align sizes
    if len(idx) < n:
        idx = pd.bdate_range(end=pd.Timestamp.now().normalize() + pd.Timedelta(days=5), periods=n)
    return pd.Series(prices, index=idx[:n], name="close")


@pytest.fixture
def sample_benchmark_ret(sample_close_series):
    """Daily return series to use as benchmark."""
    return sample_close_series.pct_change().dropna()


@pytest.fixture
def sample_portfolio_state():
    """Pre-built 4-slot state with slot 1 invested and slots 2-4 empty."""
    return {
        "slots": {
            "1": {
                "status": "invested",
                "buy_date": "2026-02-20",
                "target_sell_date": "2026-03-20",
                "cash_balance": 50.0,
                "holdings": [
                    {
                        "ticker": "SPY",
                        "name": "SPDR S&P 500 ETF",
                        "shares": 10,
                        "buy_price": 500.0,
                        "status": "active",
                    },
                    {
                        "ticker": "QQQ",
                        "name": "Invesco QQQ Trust",
                        "shares": 5,
                        "buy_price": 400.0,
                        "status": "active",
                    },
                ],
            },
            "2": {"status": "empty"},
            "3": {"status": "empty"},
            "4": {"status": "empty"},
        }
    }


@pytest.fixture
def sample_df_report():
    """Small DataFrame mimicking df_final passed to Gemini."""
    return pd.DataFrame(
        {
            "Ticker": ["SPY", "QQQ", "IWM", "GLD", "TLT", "XLF", "XLE"],
            "ETF Name": [
                "SPDR S&P 500",
                "Invesco QQQ",
                "iShares Russell 2000",
                "SPDR Gold",
                "iShares 20+ Year Treasury",
                "Financial Select Sector",
                "Energy Select Sector",
            ],
            "Avg Trading Value (USD)": [5e9, 4e9, 3e9, 2e9, 1.5e9, 1e9, 9e8],
            "RET3M": [12.5, 15.3, 8.1, 6.2, -1.0, 10.2, 7.5],
            "RET3M Score": [80, 100, 50, 35, 0, 65, 45],
            "EXRSI3M": [55, 60, 48, 42, 70, 52, 46],
            "EXRSI3M Score": [40, 30, 55, 70, 0, 45, 60],
            "Composite Score": [60, 65, 52.5, 52.5, 0, 55, 52.5],
        }
    )
