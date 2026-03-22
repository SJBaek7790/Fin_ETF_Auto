"""Tests for kis_api — all KIS SDK modules mocked via conftest.py."""
import json
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

import kis_api


class TestGetExchangeCode:
    def test_returns_cached_value(self):
        kis_api._EXCHANGE_MAP = {"SPY": "AMEX", "AAPL": "NASD", "IBM": "NYSE"}
        assert kis_api.get_exchange_code("SPY") == "AMEX"
        assert kis_api.get_exchange_code("AAPL") == "NASD"
        assert kis_api.get_exchange_code("IBM") == "NYSE"

    def test_unknown_ticker_falls_back_to_nasd(self):
        kis_api._EXCHANGE_MAP = {"SPY": "AMEX"}
        assert kis_api.get_exchange_code("UNKNOWN_TICKER") == "NASD"


class TestExecuteOrders:
    def test_buy_returns_false_for_zero_shares(self):
        kis_api.KIS_READY = True
        assert kis_api.execute_kis_buy("SPY", 0, 500.0) is False

    def test_sell_returns_false_for_zero_shares(self):
        kis_api.KIS_READY = True
        assert kis_api.execute_kis_sell("SPY", 0, 500.0) is False

    def test_buy_returns_false_when_not_ready(self):
        kis_api.KIS_READY = False
        assert kis_api.execute_kis_buy("SPY", 10, 500.0) is False

    def test_sell_returns_false_when_not_ready(self):
        kis_api.KIS_READY = False
        assert kis_api.execute_kis_sell("SPY", 10, 500.0) is False


class TestGetAvailableUSD:
    def test_returns_zero_when_not_ready(self):
        kis_api.KIS_READY = False
        assert kis_api.get_available_usd() == 0.0


class TestGetKISHoldings:
    def test_returns_empty_when_not_ready(self):
        kis_api.KIS_READY = False
        assert kis_api.get_kis_holdings() == []
