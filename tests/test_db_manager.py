"""Tests for db_manager — file-system isolated via tmp_data_dir fixture."""
import json
import pytest

import db_manager


class TestInitState:
    def test_creates_4_empty_slots(self, tmp_data_dir):
        db_manager.init_state()
        state = db_manager.get_portfolio_state()
        assert state is not None
        assert len(state["slots"]) == 4
        for key in ("1", "2", "3", "4"):
            assert state["slots"][key]["status"] == "empty"


class TestGetEmptySlot:
    def test_returns_first_empty(self, tmp_data_dir):
        db_manager.init_state()
        slot = db_manager.get_empty_slot()
        assert slot in ("1", "2", "3", "4")

    def test_returns_none_when_all_invested(self, tmp_data_dir, sample_portfolio_state):
        # Fill all slots
        for k in sample_portfolio_state["slots"]:
            sample_portfolio_state["slots"][k] = {
                "status": "invested",
                "holdings": [],
                "cash_balance": 0,
            }
        db_manager._save_state(sample_portfolio_state)
        assert db_manager.get_empty_slot() is None


class TestFillSlot:
    def test_fill_slot_sets_invested(self, tmp_data_dir):
        db_manager.init_state()
        holdings = [
            {"ticker": "069500", "name": "KODEX 200", "shares": 5, "buy_price": 35000, "status": "active"}
        ]
        result = db_manager.fill_slot("1", "2026-04-20", holdings, buy_date="2026-03-22", initial_cash_balance=100000)
        assert result is True

        state = db_manager.get_portfolio_state()
        slot = state["slots"]["1"]
        assert slot["status"] == "invested"
        assert slot["target_sell_date"] == "2026-04-20"
        assert slot["cash_balance"] == 100000
        assert len(slot["holdings"]) == 1
        assert slot["holdings"][0]["ticker"] == "069500"


    def test_fill_invalid_slot_returns_false(self, tmp_data_dir):
        db_manager.init_state()
        result = db_manager.fill_slot("99", "2026-04-20", [])
        assert result is False


class TestClearSlot:
    def test_clear_slot_resets_to_empty(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        result = db_manager.clear_slot("1", returned_cash=5000.0)
        assert result is True

        state = db_manager.get_portfolio_state()
        assert state["slots"]["1"]["status"] == "empty"
        assert state["slots"]["1"]["cash_balance"] == 5000.0

    def test_clear_invalid_slot_returns_false(self, tmp_data_dir):
        db_manager.init_state()
        assert db_manager.clear_slot("99") is False


class TestTriggerStopLoss:
    def test_marks_holding_as_cash(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        result = db_manager.trigger_stop_loss(
            slot_key="1",
            ticker_to_stop="069500",
            sell_reason="MA broken",
            sell_price=34000,
            executed_shares=10,
            sell_date="2026-03-22",
        )
        assert result is True

        state = db_manager.get_portfolio_state()
        kodex_holding = state["slots"]["1"]["holdings"][0]
        assert kodex_holding["status"] == "cash"
        assert kodex_holding["sell_reason"] == "MA broken"
        assert kodex_holding["sell_price"] == 34000

        # Proceeds = 34000 * 10 = 340000, initial cash_balance = 50000
        assert state["slots"]["1"]["cash_balance"] == 390000

    def test_returns_false_for_unknown_ticker(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        result = db_manager.trigger_stop_loss("1", "FAKE", "test", 100, 1)
        assert result is False

    def test_returns_false_for_empty_slot(self, tmp_data_dir):
        db_manager.init_state()
        result = db_manager.trigger_stop_loss("1", "069500", "test", 100, 1)
        assert result is False


class TestGetSlotsToSell:
    def test_slot_past_date_returns_slot(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        # target_sell_date is 2026-03-20, current is 2026-03-22
        slots = db_manager.get_slots_to_sell("2026-03-22")
        assert "1" in slots

    def test_slot_future_date_not_returned(self, tmp_data_dir, sample_portfolio_state):
        sample_portfolio_state["slots"]["1"]["target_sell_date"] = "2026-04-20"
        db_manager._save_state(sample_portfolio_state)
        slots = db_manager.get_slots_to_sell("2026-03-22")
        assert "1" not in slots


class TestReconcileWithKISHoldings:
    def test_adjusts_shortfall(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        # KIS says only 7 shares of KODEX 200 instead of 10
        kis_holdings = [{"ticker": "069500", "shares": 7}, {"ticker": "233740", "shares": 5}]
        alerts = db_manager.reconcile_with_kis_holdings(kis_holdings)

        assert len(alerts) > 0
        state = db_manager.get_portfolio_state()
        kodex_h = [h for h in state["slots"]["1"]["holdings"] if h["ticker"] == "069500"][0]
        assert kodex_h["shares"] == 7  # adjusted down

    def test_no_alerts_when_matching(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        kis_holdings = [{"ticker": "069500", "shares": 10}, {"ticker": "233740", "shares": 5}]
        alerts = db_manager.reconcile_with_kis_holdings(kis_holdings)
        assert alerts == []

    def test_zero_actual_marks_failed_buy(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        # 069500 shows 0 actual shares — 100% shortfall triggers failed_buy
        kis_holdings = [{"ticker": "233740", "shares": 5}]
        alerts = db_manager.reconcile_with_kis_holdings(kis_holdings)
        assert len(alerts) > 0

        state = db_manager.get_portfolio_state()
        kodex_h = [h for h in state["slots"]["1"]["holdings"] if h["ticker"] == "069500"][0]
        assert kodex_h["status"] == "failed_buy"

    def test_large_drop_marks_corporate_action(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        # 069500 shows 4 actual shares instead of 10. (6 / 10 = 0.6 drop >= 0.5), actual > 0
        kis_holdings = [{"ticker": "069500", "shares": 4}, {"ticker": "233740", "shares": 5}]
        alerts = db_manager.reconcile_with_kis_holdings(kis_holdings)
        assert len(alerts) > 0

        state = db_manager.get_portfolio_state()
        kodex_h = [h for h in state["slots"]["1"]["holdings"] if h["ticker"] == "069500"][0]
        assert kodex_h["status"] == "Corporate Action Suspected"

    def test_adjusts_overage(self, tmp_data_dir, sample_portfolio_state):
        db_manager._save_state(sample_portfolio_state)
        # KIS says 12 shares of KODEX 200 instead of 10
        kis_holdings = [{"ticker": "069500", "shares": 12}, {"ticker": "233740", "shares": 5}]
        alerts = db_manager.reconcile_with_kis_holdings(kis_holdings)

        assert len(alerts) > 0
        state = db_manager.get_portfolio_state()
        kodex_h = [h for h in state["slots"]["1"]["holdings"] if h["ticker"] == "069500"][0]
        assert kodex_h["shares"] == 12  # adjusted up


class TestPortfolioValueHistory:
    def test_save_and_load(self, tmp_data_dir):
        db_manager.save_daily_portfolio_value("2026-03-22", 10000.0)
        history = db_manager.load_value_history()
        assert len(history) == 1
        assert history["2026-03-22"] == 10000.0

    def test_update_existing_date(self, tmp_data_dir):
        db_manager.save_daily_portfolio_value("2026-03-22", 10000.0)
        db_manager.save_daily_portfolio_value("2026-03-22", 10500.0)
        history = db_manager.load_value_history()
        assert len(history) == 1
        assert history["2026-03-22"] == 10500.0


class TestCalculatePortfolioMetrics:
    def test_returns_none_for_empty_history(self, tmp_data_dir):
        assert db_manager.calculate_portfolio_metrics() is None

    def test_returns_metrics_dict(self, tmp_data_dir):
        db_manager.save_daily_portfolio_value("2026-01-01", 10000.0)
        db_manager.save_daily_portfolio_value("2026-03-22", 10500.0)
        metrics = db_manager.calculate_portfolio_metrics()
        assert metrics is not None
        assert "total_return_pct" in metrics
        assert "mdd_pct" in metrics
        assert metrics["total_return_pct"] == 5.0
