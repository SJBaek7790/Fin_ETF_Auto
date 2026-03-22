"""Tests for common.py — is_us_market_open_today with mocked calendar."""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd


class TestIsUSMarketOpenToday:
    def test_returns_true_on_trading_day(self, monkeypatch):
        """Mocked NYSE calendar says today is a session → True."""
        mock_cal = MagicMock()
        mock_cal.is_session.return_value = True

        with patch("common.xcals.get_calendar", return_value=mock_cal):
            from common import is_us_market_open_today
            assert is_us_market_open_today() is True

    def test_returns_false_on_holiday(self, monkeypatch):
        """Mocked NYSE calendar says today is NOT a session → False."""
        mock_cal = MagicMock()
        mock_cal.is_session.return_value = False

        with patch("common.xcals.get_calendar", return_value=mock_cal):
            from common import is_us_market_open_today
            assert is_us_market_open_today() is False
