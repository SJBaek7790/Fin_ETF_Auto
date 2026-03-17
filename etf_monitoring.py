import os
import sys
import json
import time
import logging
import pandas as pd
from datetime import datetime, timedelta

from log_config import setup_logging, get_logger, get_log_filepath
from common import send_telegram_document_sync, get_market_ohlcv_wrapper, is_us_market_open_today

import kis_api

import db_manager

logger = get_logger(__name__)

def get_price_history(code):
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")
        df = get_market_ohlcv_wrapper(start_date, end_date, code)
        
        if df is None or df.empty:
            return None
        
        # Explicitly check for expected column names instead of guessing by position
        if '종가' in df.columns:
            df = df.rename(columns={'종가': 'close'})
        elif 'Close' in df.columns:
            df = df.rename(columns={'Close': 'close'})
        elif 'close' in df.columns:
            pass # Already named 'close'
        else:
            raise ValueError(f"Expected '종가' or 'Close' column missing for {code}. Available columns: {df.columns.tolist()}")
             
        df = df[['close']]
        return df
    except Exception as e:
        logger.error("Error fetching prices for %s via wrapper: %s", code, e)
        return None

def main():
    setup_logging("monitoring")

    if not is_us_market_open_today():
        logger.info("US market is closed today. Skipping monitoring.")
        return

    logger.info("Starting ETF Monitor...")
    
    all_alerts = []
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    
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
                    curr_price = df_hist_temp.iloc[-1]['close']
                    
                    if kis_api.KIS_READY:
                        success = kis_api.execute_kis_sell(t, shares, curr_price)
                        if not success:
                            logger.error("API time-stop sell failed for %s. Skipping DB update.", t)
                            all_holdings_sold = False
                            continue
                    else:
                        success = True
                        logger.info("MOCK MODE: Simulated time-stop sell for %s.", t)
                    
                    slot_proceeds += round(shares * curr_price, 2)
                    logger.info("Executed time-stop sell for Slot %s - %s (%d shares). Success: %s", s, t, shares, success)
                    
                    db_manager.log_trade(
                        action="SELL",
                        ticker=t,
                        shares=shares,
                        price=curr_price,
                        slot_key=s,
                        name=h.get('name', ''),
                        reason="Target Date Reached",
                        status="target-sell"
                    )
                    
            if not all_holdings_sold:
                logger.error("Slot %s had failed sells. Aborting clear_slot to prevent desync.", s)
                continue

        db_manager.clear_slot(s, returned_cash=slot_proceeds)
        sold_slots_msg += f"\nSlot {s} 4-week holding period met. Sold to cash (${slot_proceeds:,.2f})."
        
    if sold_slots_msg:
        logger.info(sold_slots_msg)
        all_alerts.append(sold_slots_msg)
        
    
    # We only have one DB manager source now for the 4-slot system.
    active_holdings = db_manager.get_active_holdings_for_monitoring()
    
    if not active_holdings:
        logger.info("No active holdings found in portfolio.")
        # Send log file before returning
        log_path = get_log_filepath()
        if log_path:
            send_telegram_document_sync(log_path, caption=f"ETF Monitoring Log ({today_str})")
        return
        
    logger.info("Found %d active holdings to monitor.", len(active_holdings))
    
    alerts = []
    alert_tickers = []
    
    for stock_item in active_holdings:
        code = str(stock_item.get('ticker'))
        name = stock_item.get('name')
        slot_key = stock_item.get('slot')
            
        logger.info("Checking %s (%s) [Slot %s]...", name, code, slot_key)
        time.sleep(0.1)
        
        df = get_price_history(code)
        
        if df is None or df.empty:
            # Handle potential delisting/liquidation
            consecutive_none = db_manager.increment_none_data_days(slot_key, code)
            logger.warning("Data missing for %s (%s). Consecutive days: %d", name, code, consecutive_none)
            if consecutive_none >= 3:
                alerts.append(f"🚨 EMERGENCY: {name} ({code}) [Slot {slot_key}] has returned NO DATA for {consecutive_none} consecutive days! Possible delisting or liquidation. Manual intervention required!")
            continue
            
        else:
            # Reset counter on successful data fetch
            db_manager.reset_none_data_days(slot_key, code)
            
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
    
    if alerts:
        all_alerts.extend(alerts)
        
        # Trigger stop-loss for alerted ETFs using DB manager
        if alert_tickers:
            logger.info("=== Auto-Sell: Triggering stop loss for %d ETFs ===", len(alert_tickers))
            for ticker, slot_key, reason in alert_tickers:
                
                # Fetch accurate shares from db_manager memory
                active_holdings = db_manager.get_active_holdings_for_monitoring()
                shares_to_sell = 0
                for h in active_holdings:
                    if h.get('ticker') == ticker and h.get('slot') == slot_key:
                        shares_to_sell = int(h.get('shares', 0))
                        break
                        
                # 1. Execute Real Trade via KIS
                sell_success = False
                if kis_api.KIS_READY and shares_to_sell > 0:
                     try:
                         # Get current price to calculate limit price
                         df_hist_temp = get_price_history(ticker)
                         if df_hist_temp is None or df_hist_temp.empty:
                             logger.warning("Price data unavailable for %s. Skipping stop-loss sell.", ticker)
                             continue
                         curr_price = df_hist_temp.iloc[-1]['close']
                         
                         sell_success = kis_api.execute_kis_sell(ticker, shares_to_sell, curr_price)
                         logger.info("Executed stop-loss limit sell for %s (%d shares). Success: %s", ticker, shares_to_sell, sell_success)
                     except Exception as e:
                         logger.error("Stop-loss KIS API sell error for %s: %s", ticker, e)
                
                # 2. Prevent DB desync if API failed
                if kis_api.KIS_READY and not sell_success:
                    logger.error("API stop-loss sell failed for %s. Skipping DB update.", ticker)
                    continue

                # 3. Get current price for record logging (fallback to Yahoo if live fails)
                df_hist = get_price_history(ticker)
                if df_hist is None or df_hist.empty:
                    logger.warning("Price data unavailable for %s. Skipping DB stop-loss update.", ticker)
                    continue
                execute_price = df_hist.iloc[-1]['close']
                
                # 4. Update DB state
                db_manager.trigger_stop_loss(slot_key, ticker, reason, execute_price, shares_to_sell)
    
    # Calculate Total Portfolio Value using KIS API Direct Fetch
    if kis_api.KIS_READY:
        logger.info("=== Fetching Daily Portfolio Value via KIS API ===")
        total_value = kis_api.get_total_portfolio_value()
        
        if total_value > 0.0:
            today_str = datetime.now().strftime("%Y-%m-%d")
            total_value = round(total_value, 2)
            logger.info("Total Portfolio Value: $%s", f"{total_value:,.2f}")
            db_manager.save_daily_portfolio_value(today_str, total_value)
        else:
             logger.warning("Total Portfolio Value fetched as $0.00, check account configuration.")
    else:
        logger.info("KIS API Not Ready: Cannot fetch daily value")
        
    if all_alerts:
        for alert in all_alerts:
            logger.warning("ALERT: %s", alert)
    else:
        logger.info("No alerts to send.")

    # --- FINAL: Send log file via Telegram ---
    log_path = get_log_filepath()
    if log_path:
        send_telegram_document_sync(log_path, caption=f"ETF Monitoring Log ({today_str})")
        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        logger.critical("CRITICAL ERROR in etf_monitoring.py: %s", e, exc_info=True)
        # Attempt to send log file even on crash
        try:
            log_path = get_log_filepath()
            if log_path:
                send_telegram_document_sync(log_path, caption="❌ ETF Monitoring CRASH Log")
        except Exception as tel_e:
            logger.error("Failed to send crash log to Telegram: %s", tel_e)
