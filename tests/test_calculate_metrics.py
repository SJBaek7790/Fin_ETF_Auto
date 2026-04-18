"""Tests for etf_screening.calculate_metrics()."""
import numpy as np
import pandas as pd
import pytest

from screen import calculate_metrics


class TestCalculateMetrics:
    """Unit tests for RET3M and EXRSI3M metric calculation."""

    def _make_data(self, close_series):
        """Helper to build the 'data' dict that calculate_metrics expects."""
        return {"close": close_series}

    def test_returns_none_when_too_short(self, sample_benchmark_ret):
        """If close has < 60 data points, should return None."""
        short = pd.Series(range(1, 50), dtype=float)
        result = calculate_metrics(self._make_data(short), sample_benchmark_ret)
        assert result is None

    def _make_aligned_data(self):
        """Create ETF close series and benchmark returns that share the same index."""
        np.random.seed(42)
        n = 200
        idx = pd.bdate_range(start="2025-06-01", periods=n)
        etf_prices = 100 + np.cumsum(np.random.randn(n) * 0.5)
        etf_prices = np.maximum(etf_prices, 1.0)
        close = pd.Series(etf_prices, index=idx, name="close")

        bm_prices = 100 + np.cumsum(np.random.randn(n) * 0.4)
        bm_prices = np.maximum(bm_prices, 1.0)
        bm_ret = pd.Series(bm_prices, index=idx).pct_change().dropna()
        return close, bm_ret

    def test_returns_dict_with_required_keys(self):
        """With valid data, should return dict with RET3M and EXRSI3M."""
        close, bm_ret = self._make_aligned_data()
        result = calculate_metrics(self._make_data(close), bm_ret)
        assert result is not None
        assert "RET3M" in result
        assert "EXRSI3M" in result

    def test_values_are_rounded(self):
        """RET3M and EXRSI3M should be rounded to 2 decimal places."""
        close, bm_ret = self._make_aligned_data()
        result = calculate_metrics(self._make_data(close), bm_ret)
        if result is None:
            pytest.skip("calculate_metrics returned None for this synthetic data")
        for key in ("RET3M", "EXRSI3M"):
            val = result[key]
            assert val == round(val, 2), f"{key} not rounded: {val}"

    def test_ret3m_positive_for_uptrending(self):
        """A strongly uptrending series should have positive RET3M."""
        prices = pd.Series(np.linspace(100, 200, 200), dtype=float)
        bm_ret = prices.pct_change().dropna()
        result = calculate_metrics({"close": prices}, bm_ret)
        if result is None:
            pytest.skip("calculate_metrics returned None")
        assert result["RET3M"] > 0
