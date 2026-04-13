"""
ETF Monitoring — Signal Generator (Separate execution)

Generates pending SELL orders for time-stops and stop-losses.
Writes orders to data/pending_orders.json. 
Does NOT execute trades directly.
"""

import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from log_config import setup_logging, get_logger
from common import send_telegram_message, get_market_ohlcv_wrapper, is_kr_market_open_today

import db_manager
import kis_api

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

    logger.info("Starting ETF Monitor (Signal Generation)...")
    
    all_alerts = []
    sell_orders_list = []
    sell_tickers_set = set() # To track what we're already selling
    
    today_str = datetime.now(tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    
    # 0. Daily Reconciliation Job
    logger.info("--- Running Daily Order Reconciliation Job ---")
    if getattr(kis_api, 'KIS_READY', False):
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
    
    # 1. Time-stop detection
    slots_to_sell = db_manager.get_slots_to_sell(today_str)
    state = db_manager.get_portfolio_state()
    
    for s in slots_to_sell:
        if state:
            slot_data = state.get("slots", {}).get(s, {})
            for h in slot_data.get("holdings", []):
                if h.get("status") == "active":
                    t = h.get("ticker")
                    shares = int(h.get("shares", 0))
                    
                    df_hist_temp = get_price_history(t)
                    if df_hist_temp is None or df_hist_temp.empty:
                        logger.warning("Price data unavailable for %s. Skipping time-stop order generation.", t)
                        continue
                        
                    curr_price = int(round(float(df_hist_temp.iloc[-1]['close'])))
                    
                    sell_orders_list.append({
                        "action": "SELL",
                        "ticker": str(t),
                        "slot": str(s),
                        "shares": shares,
                        "reason": "time-stop",
                        "estimated_price": curr_price
                    })
                    sell_tickers_set.add((str(t), str(s)))
                    
                    msg = f"Generated time-stop SELL order for Slot {s} - {t} ({shares} shares) at ~₩{curr_price:,.0f}"
                    logger.info(msg)
                    # We do not clear_slot here, order_placement will do it!
                    all_alerts.append(msg)
    
    # 2. Stop-loss detection
    active_holdings = db_manager.get_active_holdings_for_monitoring()
    
    if not active_holdings:
        logger.info("No active holdings found in portfolio for stop-loss.")
    else:
        logger.info("Found %d active holdings to monitor.", len(active_holdings))
    
    price_cache = {}
    increments = []
    resets = []
    
    for stock_item in active_holdings:
        code = str(stock_item.get('ticker'))
        name = stock_item.get('name')
        slot_key = str(stock_item.get('slot'))
        shares = int(stock_item.get('shares', 0))
        
        if (code, slot_key) in sell_tickers_set:
            logger.info("Skipping %s in Slot %s (already pending time-stop sell).", code, slot_key)
            time.sleep(0.1) # brief pause
            continue
            
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
            
        current_price = int(round(float(df.iloc[-1]['close'])))
        ma_120 = float(df['close'].rolling(window=120).mean().iloc[-1])
        
        if len(df) >= 60:
            price_3m_ago = float(df.iloc[-60]['close'])
            momentum_dead = current_price < price_3m_ago
        else:
            momentum_dead = False
            price_3m_ago = 0.0
            
        ma_broken = current_price < ma_120
        
        logger.info("price:%s, ma_120:%.2f, 3m_ago:%s", current_price, ma_120, price_3m_ago)
        
        if ma_broken or momentum_dead:
            reasons = []
            if ma_broken:
                reasons.append(f"Price ({current_price}) < 120MA ({ma_120:.2f})")
            if momentum_dead:
                reasons.append(f"Price ({current_price}) < 3M ago ({price_3m_ago})")
            
            reason_str = "stop-loss" if ma_broken else "momentum-dead"
            
            msg = f"⚠️ {name} ({code}) [Slot {slot_key}]\n- " + "\n- ".join(reasons)
            alerts_tuple = (code, slot_key, reason_str)
            all_alerts.append(msg)
            
            if shares > 0:
                sell_orders_list.append({
                    "action": "SELL",
                    "ticker": code,
                    "slot": slot_key,
                    "shares": shares,
                    "reason": reason_str,
                    "estimated_price": current_price
                })
                logger.info(f"Generated {reason_str} SELL order for {code} ({shares} shares) in Slot {slot_key}")
            else:
                logger.warning("shares is 0 for %s in Slot %s. Skipping sell order.", code, slot_key)
                
    if increments or resets:
        inc_only = [(s, c) for s, c, n in increments]
        batch_results = db_manager.batch_update_none_data_days(inc_only, resets)
        
        for inc_item in increments:
            slot_key, code, name = inc_item
            consecutive_none = next((res['consecutive_none_days'] for res in batch_results if res['slot'] == slot_key and res['ticker'] == code), 1)
            
            logger.warning("Data missing for %s (%s). Consecutive days: %d", name, code, consecutive_none)
            if consecutive_none >= 3:
                all_alerts.append(f"🚨 EMERGENCY: {name} ({code}) [Slot {slot_key}] has returned NO DATA for {consecutive_none} consecutive days! Manual intervention required!")
    
    # 3. Write Pending Orders
    if sell_orders_list:
        success = db_manager.write_pending_orders(mode="monitoring", orders=sell_orders_list)
        if success:
            logger.info("Successfully wrote %d pending SELL orders to pending_orders.json.", len(sell_orders_list))
        else:
            logger.error("Failed to write pending_orders.json!")
    else:
        logger.info("No SELL signals. Writing empty orders list.")
        db_manager.write_pending_orders(mode="monitoring", orders=[])
        
    if all_alerts:
        for alert in all_alerts:
            logger.warning("ALERT: %s", alert.strip("\n"))
    else:
        logger.info("No alerts to send.")

    # 4. Telegram Notification
    n_holdings = len(active_holdings) if active_holdings else 0
    n_alerts = len(all_alerts)
    
    if n_alerts > 0:
        summary_lines = [f"📊 Monitor Signals ({today_str})", f"Holdings monitored: {n_holdings}", f"SELL Orders generated: {len(sell_orders_list)}"]
        brief = [a.strip().split('\n')[0] for a in all_alerts[:3]]
        summary_lines.extend(brief)
        send_telegram_message("\n".join(summary_lines))
    else:
        logger.info("No alerts. Skipping Telegram notification.")
        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        logger.critical("CRITICAL ERROR in monitor.py: %s", e, exc_info=True)
        try:
            send_telegram_message(f"❌ Monitor CRASH\n{e}")
        except Exception as tel_e:
            logger.error("Failed to send crash log to Telegram: %s", tel_e)
