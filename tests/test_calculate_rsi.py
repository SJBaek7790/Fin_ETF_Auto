"""Tests for etf_screening.calculate_rsi()."""
import numpy as np
import pandas as pd
import pytest

# We import directly; conftest adds project root to sys.path
from etf_screening import calculate_rsi


class TestCalculateRSI:
    """Unit tests for the RSI calculation."""

    def test_all_gains_rsi_near_100(self):
        """A series of only positive changes should yield RSI close to 100."""
        gains = pd.Series([1.0] * 60)
        rsi = calculate_rsi(gains, period=14)
        last_rsi = rsi.iloc[-1]
        assert last_rsi > 95, f"Expected RSI > 95 for all-gains series, got {last_rsi}"

    def test_all_losses_rsi_near_0(self):
        """A series of only negative changes should yield RSI close to 0."""
        losses = pd.Series([-1.0] * 60)
        rsi = calculate_rsi(losses, period=14)
        last_rsi = rsi.iloc[-1]
        assert last_rsi < 5, f"Expected RSI < 5 for all-losses series, got {last_rsi}"

    def test_mixed_series_rsi_in_range(self):
        """A mixed series should produce RSI between 0 and 100."""
        np.random.seed(123)
        mixed = pd.Series(np.random.randn(100))
        rsi = calculate_rsi(mixed, period=14)
        last_rsi = rsi.iloc[-1]
        assert 0 < last_rsi < 100, f"Expected RSI in (0,100), got {last_rsi}"

    def test_short_series_returns_nan(self):
        """If series is shorter than the period, result should be NaN."""
        short = pd.Series([0.5, -0.3, 0.2])
        rsi = calculate_rsi(short, period=14)
        assert rsi.isna().all(), "Expected all NaN for series shorter than period"

    def test_period_60(self):
        """RSI with period=60 (used for EXRSI3M) should still work on long series."""
        np.random.seed(0)
        series = pd.Series(np.random.randn(200))
        rsi = calculate_rsi(series, period=60)
        last_rsi = rsi.iloc[-1]
        assert not np.isnan(last_rsi), "Expected a numeric RSI for 200-length series with period=60"
        assert 0 <= last_rsi <= 100
