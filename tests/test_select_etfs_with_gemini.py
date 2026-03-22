"""Tests for select_etfs_with_gemini() — Gemini API fully mocked, including retry logic."""
import json
import sys
import pytest
from unittest.mock import patch, MagicMock

from etf_screening import select_etfs_with_gemini, _fallback_top7


def _make_mock_response(selected_list):
    """Create a mock Gemini response with .text returning JSON."""
    mock_resp = MagicMock()
    mock_resp.text = json.dumps(selected_list)
    return mock_resp


class TestSelectETFsWithGemini:
    """Tests for Gemini selection including retry logic."""

    def test_successful_response(self, sample_df_report, monkeypatch):
        """Gemini returns valid tickers → parsed and validated."""
        monkeypatch.setattr("etf_screening.GEMINI_API_KEY", "fake-key")

        payload = [
            {"Ticker": "SPY", "ETF Name": "SPDR S&P 500", "Reason": "test"},
            {"Ticker": "QQQ", "ETF Name": "Invesco QQQ", "Reason": "test"},
        ]

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = _make_mock_response(payload)

        with patch("etf_screening.genai.Client", return_value=mock_client):
            result = select_etfs_with_gemini(sample_df_report)

        assert len(result) == 2
        tickers = {r["Ticker"] for r in result}
        assert tickers == {"SPY", "QQQ"}

    def test_hallucinated_tickers_rejected(self, sample_df_report, monkeypatch):
        """Tickers not in df_report are rejected."""
        monkeypatch.setattr("etf_screening.GEMINI_API_KEY", "fake-key")

        payload = [
            {"Ticker": "SPY", "ETF Name": "SPDR S&P 500", "Reason": "test"},
            {"Ticker": "FAKE_TICKER", "ETF Name": "Hallucinated ETF", "Reason": "test"},
        ]

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = _make_mock_response(payload)

        with patch("etf_screening.genai.Client", return_value=mock_client):
            result = select_etfs_with_gemini(sample_df_report)

        assert len(result) == 1
        assert result[0]["Ticker"] == "SPY"

    def test_retry_succeeds_on_third_attempt(self, sample_df_report, monkeypatch):
        """API fails twice, succeeds on 3rd attempt → returns valid result."""
        monkeypatch.setattr("etf_screening.GEMINI_API_KEY", "fake-key")
        monkeypatch.setattr("etf_screening.time.sleep", lambda x: None)  # skip backoff

        payload = [{"Ticker": "GLD", "ETF Name": "SPDR Gold", "Reason": "test"}]

        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = [
            Exception("API Error 1"),
            Exception("API Error 2"),
            _make_mock_response(payload),
        ]

        with patch("etf_screening.genai.Client", return_value=mock_client):
            result = select_etfs_with_gemini(sample_df_report)

        assert len(result) == 1
        assert result[0]["Ticker"] == "GLD"
        assert mock_client.models.generate_content.call_count == 3

    def test_all_retries_fail_falls_back(self, sample_df_report, monkeypatch):
        """All 3 API attempts fail → falls back to _fallback_top7."""
        monkeypatch.setattr("etf_screening.GEMINI_API_KEY", "fake-key")
        monkeypatch.setattr("etf_screening.time.sleep", lambda x: None)

        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = Exception("Persistent failure")

        with patch("etf_screening.genai.Client", return_value=mock_client):
            result = select_etfs_with_gemini(sample_df_report)

        # Fallback returns top 7
        assert len(result) == 7
        assert mock_client.models.generate_content.call_count == 3

    def test_missing_api_key_falls_back(self, sample_df_report, monkeypatch):
        """No GEMINI_API_KEY → immediate fallback without API call."""
        monkeypatch.setattr("etf_screening.GEMINI_API_KEY", None)
        result = select_etfs_with_gemini(sample_df_report)
        assert len(result) == 7  # fallback top 7

    def test_empty_report_returns_empty(self, monkeypatch):
        """Empty DataFrame → returns empty list."""
        import pandas as pd
        monkeypatch.setattr("etf_screening.GEMINI_API_KEY", "fake-key")
        result = select_etfs_with_gemini(pd.DataFrame())
        assert result == []

    def test_all_hallucinated_falls_back(self, sample_df_report, monkeypatch):
        """If all returned tickers are hallucinated, falls back to top 7."""
        monkeypatch.setattr("etf_screening.GEMINI_API_KEY", "fake-key")

        payload = [{"Ticker": "FAKE1", "ETF Name": "Fake 1", "Reason": "test"}]

        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = _make_mock_response(payload)

        with patch("etf_screening.genai.Client", return_value=mock_client):
            result = select_etfs_with_gemini(sample_df_report)

        assert len(result) == 7  # fallback


class TestFallbackTop7:
    def test_returns_7_entries(self, sample_df_report):
        result = _fallback_top7(sample_df_report)
        assert len(result) == 7

    def test_entries_have_required_keys(self, sample_df_report):
        result = _fallback_top7(sample_df_report)
        for entry in result:
            assert "Ticker" in entry
            assert "ETF Name" in entry
            assert "Reason" in entry
