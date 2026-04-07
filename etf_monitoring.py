"""
ETF Monitoring — Korean Domestic ETFs

Daily monitoring of actively held Korean ETFs for stop-loss and time-stop triggers.
Executes sell orders via the KIS domestic stock API.

Run schedule: Every weekday morning during KRX market hours.
"""

import os
import sys
import json
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from log_config import setup_logging, get_logger
from common import send_telegram_message, get_market_ohlcv_wrapper, is_kr_market_open_today

import kis_api
import db_manager

logger = get_logger(__name__)

def get_price_history(code):
    """Fetches 200 days of close price history for a Korean ETF."""
    try:
        seoul_now = datetime.now(tz=ZoneInfo("Asia/Seoul"))
        end_date = (seoul_now - timedelta(days=1)).strftime("%Y%m%d")
        start_date = (seoul_now - timedelta(days=200)).strftime("%Y%m%d")
        df = get_market_ohlcv_wrapper(start_date, end_date, code)
        
        if df is None or df.empty:
            return None
        
        if 'close' not in df.columns:
            raise ValueError(f"Expected 'close' column missing for {code}. Available columns: {df.columns.tolist()}")
             
        df = df[['close']]
        return df
    except Exception as e:
        logger.error("Error fetching prices for %s via wrapper: %s", code, e)
        return None

def main():
    setup_logging("monitoring")

    if not is_kr_market_open_today():
        logger.info("KRX market is closed today. Skipping monitoring.")
        return

    logger.info("Starting ETF Monitor...")
    
    all_alerts = []
    
    today_str = datetime.now(tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    
    # 0. Daily Reconciliation Job
    logger.info("--- Running Daily Order Reconciliation Job ---")
    if kis_api.KIS_READY:
        kis_holdings = kis_api.get_kis_holdings()
        reconciliation_alerts = db_manager.reconcile_with_kis_holdings(kis_holdings)
        if reconciliation_alerts:
            for alert in reconciliation_alerts:
                logger.warning("Reconciliation: %s", alert)
                all_alerts.append(alert)
            logger.info("Reconciliation complete. DB adjusted based on actual KIS holdings.")
        else:
            logger.info("Reconciliation complete. DB matches actual KIS holdings.")
    else:
        logger.info("KIS API not ready. Skipping Daily Reconciliation Job.")
    
    # 1. Clear slots whose target_sell_date is met
    slots_to_sell = db_manager.get_slots_to_sell(today_str)
    sold_slots_msg = ""
    for s in slots_to_sell:
        state = db_manager.get_portfolio_state()
        slot_proceeds = 0.0
        
        if state:
            slot_data = state.get("slots", {}).get(s, {})
            slot_proceeds += slot_data.get("cash_balance", 0.0)
            
            all_holdings_sold = True
            for h in slot_data.get("holdings", []):
                if h.get("status") == "active":
                    t = h.get("ticker")
                    shares = int(h.get("shares", 0))
                    
                    df_hist_temp = get_price_history(t)
                    if df_hist_temp is None or df_hist_temp.empty:
                        logger.warning("Price data unavailable for %s. Skipping time-stop sell.", t)
                        all_holdings_sold = False
                        continue
                    curr_price = int(round(float(df_hist_temp.iloc[-1]['close'])))
                    
                    if kis_api.KIS_READY:
                        success = kis_api.execute_kis_sell(t, shares, curr_price)
                        if not success:
                            logger.error("API time-stop sell failed for %s. Skipping DB update.", t)
                            all_holdings_sold = False
                            continue
                    else:
                        success = True
                        logger.info("MOCK MODE: Simulated time-stop sell for %s.", t)
                    
                    slot_proceeds += round(shares * curr_price, 0)
                    logger.info("Executed time-stop sell for Slot %s - %s (%d shares). Success: %s", s, t, shares, success)
                    
            if not all_holdings_sold:
                logger.error("Slot %s had failed sells. Aborting clear_slot to prevent desync.", s)
                continue

        db_manager.clear_slot(s, returned_cash=slot_proceeds)
        sold_slots_msg += f"\nSlot {s} 4-week holding period met. Sold to cash (₩{slot_proceeds:,.0f})."
        
    if sold_slots_msg:
        logger.info(sold_slots_msg)
        all_alerts.append(sold_slots_msg)
        
    
    # 2. Monitor active holdings for stop-loss
    active_holdings = db_manager.get_active_holdings_for_monitoring()
    
    if not active_holdings:
        logger.info("No active holdings found in portfolio.")
        send_telegram_message(f"📊 ETF Monitor ({today_str})\nHoldings: 0 | No active positions\n✅ All clear")
        return
        
    logger.info("Found %d active holdings to monitor.", len(active_holdings))
    
    alerts = []
    alert_tickers = []
    
    price_cache = {}
    increments = []
    resets = []
    
    for stock_item in active_holdings:
        code = str(stock_item.get('ticker'))
        name = stock_item.get('name')
        slot_key = stock_item.get('slot')
            
        logger.info("Checking %s (%s) [Slot %s]...", name, code, slot_key)
        time.sleep(0.5)
        
        df = get_price_history(code)
        
        if df is None or df.empty:
            increments.append((slot_key, code, name))
            continue
            
        else:
            resets.append((slot_key, code))
            
        price_cache[code] = df
        
        if len(df) < 120:
            logger.info("Insufficient data for %s", name)
            continue
            
        current_price = df.iloc[-1]['close']
        ma_120 = df['close'].rolling(window=120).mean().iloc[-1]
        
        if len(df) >= 60:
            price_3m_ago = df.iloc[-60]['close']
            momentum_dead = current_price < price_3m_ago
        else:
            momentum_dead = False
            price_3m_ago = 0
            
        ma_broken = current_price < ma_120
        
        logger.info("price:%s, ma_120:%.2f, 3m_ago:%s", current_price, ma_120, price_3m_ago)
        
        if ma_broken or momentum_dead:
            reason = []
            if ma_broken:
                reason.append(f"Price ({current_price}) < 120MA ({ma_120:.2f})")
            if momentum_dead:
                reason.append(f"Price ({current_price}) < 3M ago ({price_3m_ago})")
            
            alerts.append(f"⚠️ {name} ({code}) [Slot {slot_key}]\n- " + "\n- ".join(reason))
            alert_tickers.append((code, slot_key, " | ".join(reason)))
            
    if increments or resets:
        inc_only = [(s, c) for s, c, n in increments]
        batch_results = db_manager.batch_update_none_data_days(inc_only, resets)
        
        for inc_item in increments:
            slot_key, code, name = inc_item
            consecutive_none = next((res['consecutive_none_days'] for res in batch_results if res['slot'] == slot_key and res['ticker'] == code), 1)
            
            logger.warning("Data missing for %s (%s). Consecutive days: %d", name, code, consecutive_none)
            if consecutive_none >= 3:
                alerts.append(f"🚨 EMERGENCY: {name} ({code}) [Slot {slot_key}] has returned NO DATA for {consecutive_none} consecutive days! Possible delisting or liquidation. Manual intervention required!")
    
    if alerts:
        all_alerts.extend(alerts)
        
        # Trigger stop-loss for alerted ETFs
        if alert_tickers:
            logger.info("=== Auto-Sell: Triggering stop loss for %d ETFs ===", len(alert_tickers))
            for ticker, slot_key, reason in alert_tickers:
                
                shares_to_sell = 0
                for h in active_holdings:
                    if h.get('ticker') == ticker and h.get('slot') == slot_key:
                        shares_to_sell = int(h.get('shares', 0))
                        break
                        
                if shares_to_sell == 0:
                    logger.warning("shares_to_sell is 0 for %s in Slot %s. Skipping stop-loss API call.", ticker, slot_key)
                    continue
                        
                # 1. Execute Real Trade via KIS
                sell_success = False
                if kis_api.KIS_READY and shares_to_sell > 0:
                     try:
                         df_hist_temp = price_cache.get(ticker)
                         if df_hist_temp is None or df_hist_temp.empty:
                             df_hist_temp = get_price_history(ticker)
                         if df_hist_temp is None or df_hist_temp.empty:
                             logger.warning("Price data unavailable for %s. Skipping stop-loss sell.", ticker)
                             continue
                         curr_price = int(round(float(df_hist_temp.iloc[-1]['close'])))
                         
                         sell_success = kis_api.execute_kis_sell(ticker, shares_to_sell, curr_price)
                         logger.info("Executed stop-loss limit sell for %s (%d shares). Success: %s", ticker, shares_to_sell, sell_success)
                     except Exception as e:
                         logger.error("Stop-loss KIS API sell error for %s: %s", ticker, e)
                
                # 2. Prevent DB desync if API failed
                if kis_api.KIS_READY and not sell_success:
                    logger.error("API stop-loss sell failed for %s. Skipping DB update.", ticker)
                    continue

                # 3. Get price for record logging
                df_hist = price_cache.get(ticker)
                if df_hist is None or df_hist.empty:
                    df_hist = get_price_history(ticker)
                if df_hist is None or df_hist.empty:
                    logger.warning("Price data unavailable for %s. Skipping DB stop-loss update.", ticker)
                    continue
                execute_price = int(round(float(df_hist.iloc[-1]['close'])))
                
                # 4. Update DB state
                db_manager.trigger_stop_loss(slot_key, ticker, reason, execute_price, shares_to_sell)
    
    # Calculate Total Portfolio Value using KIS API
    total_value = 0
    if kis_api.KIS_READY:
        logger.info("=== Fetching Daily Portfolio Value via KIS API ===")
        total_value = kis_api.get_total_portfolio_value()
        
        if total_value > 0.0:
            today_str = datetime.now(tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
            total_value = round(total_value, 0)
            logger.info("Total Portfolio Value: ₩%s", f"{total_value:,.0f}")
            db_manager.save_daily_portfolio_value(today_str, total_value)
        else:
             logger.warning("Total Portfolio Value fetched as ₩0, check account configuration.")
    else:
        logger.info("KIS API Not Ready: Cannot fetch daily value")
        
    if all_alerts:
        for alert in all_alerts:
            logger.warning("ALERT: %s", alert)
    else:
        logger.info("No alerts to send.")

    # --- FINAL: Send compact summary via Telegram ---
    n_holdings = len(active_holdings)
    n_alerts = len(all_alerts)
    summary_lines = [f"📊 ETF Monitor ({today_str})"]
    parts = [f"Holdings: {n_holdings}", f"Alerts: {n_alerts}"]
    if total_value > 0:
        parts.append(f"₩{total_value:,.0f}")
    summary_lines.append(" | ".join(parts))
    if all_alerts:
        brief = [a.strip().split('\n')[0] for a in all_alerts[:3]]
        summary_lines.extend(brief)
    else:
        summary_lines.append("✅ All clear")
    send_telegram_message("\n".join(summary_lines))
        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        logger.critical("CRITICAL ERROR in etf_monitoring.py: %s", e, exc_info=True)
        try:
            send_telegram_message(f"❌ ETF Monitoring CRASH\n{e}")
        except Exception as tel_e:
            logger.error("Failed to send crash log to Telegram: %s", tel_e)
