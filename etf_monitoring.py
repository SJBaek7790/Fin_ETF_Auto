import os
import sys
import json
import time
import pandas as pd
from datetime import datetime, timedelta

from common import send_telegram_message, get_market_ohlcv_wrapper

import kis_api

import db_manager

def get_price_history(code):
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")
        df = get_market_ohlcv_wrapper(start_date, end_date, code)
        
        if df is None or df.empty:
            return None
        
        df.columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'rate'] if len(df.columns) == 7 else df.columns
        if '종가' in df.columns:
            df = df.rename(columns={'종가': 'close'})
        
        if 'close' not in df.columns:
             df.rename(columns={df.columns[3]: 'close'}, inplace=True)
             
        df = df[['close']]
        return df
    except Exception as e:
        print(f"Error fetching prices for {code} via wrapper: {e}")
        return None

def main():
    print("Starting ETF Monitor...")
    
    all_alerts = []
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    
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
                    curr_price = df_hist_temp.iloc[-1]['close'] if df_hist_temp is not None and not df_hist_temp.empty else 1.0
                    
                    if kis_api.KIS_READY:
                        success = kis_api.execute_kis_sell(t, shares, curr_price)
                        if not success:
                            print(f"API time-stop sell failed for {t}. Skipping DB update.")
                            all_holdings_sold = False
                            continue
                    else:
                        success = True
                        print(f"MOCK MODE: Simulated time-stop sell for {t}.")
                    
                    slot_proceeds += round(shares * curr_price, 2)
                    print(f"Executed time-stop sell for Slot {s} - {t} ({shares} shares). Success: {success}")
                    
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
                print(f"Slot {s} had failed sells. Aborting clear_slot to prevent desync.")
                continue

        db_manager.clear_slot(s, returned_cash=slot_proceeds)
        sold_slots_msg += f"\nSlot {s} 4-week holding period met. Sold to cash (${slot_proceeds:,.2f})."
        
    if sold_slots_msg:
        print(sold_slots_msg)
        all_alerts.append(sold_slots_msg)
        
    
    # We only have one DB manager source now for the 4-slot system.
    # We'll treat all tickers in the DB as US markets for now since the screener is US.
    # Future enhancement could store the country code in the db_manager holding dict.
    active_holdings = db_manager.get_active_holdings_for_monitoring()
    
    if not active_holdings:
        print("No active holdings found in portfolio.")
        return
        
    print(f"Found {len(active_holdings)} active holdings to monitor.")
    
    alerts = []
    alert_tickers = []
    
    for stock_item in active_holdings:
        code = str(stock_item.get('ticker'))
        name = stock_item.get('name')
        slot_key = stock_item.get('slot')
            
        print(f"Checking {name} ({code}) [Slot {slot_key}]...")
        time.sleep(0.1)
        
        df = get_price_history(code)
        
        if df is None or df.empty:
            # Handle potential delisting/liquidation
            consecutive_none = db_manager.increment_none_data_days(slot_key, code)
            print(f"Data missing for {name} ({code}). Consecutive days: {consecutive_none}")
            if consecutive_none >= 3:
                alerts.append(f"🚨 EMERGENCY: {name} ({code}) [Slot {slot_key}] has returned NO DATA for {consecutive_none} consecutive days! Possible delisting or liquidation. Manual intervention required!")
            continue
            
        else:
            # Reset counter on successful data fetch
            db_manager.reset_none_data_days(slot_key, code)
            
        if len(df) < 120:
            print(f"Insufficient data for {name}")
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
        
        print(f"price:{current_price}, ma_120:{ma_120:.2f}, 3m_ago:{price_3m_ago}")
        
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
            print(f"\n=== Auto-Sell: Triggering stop loss for {len(alert_tickers)} ETFs ===")
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
                         curr_price = df_hist_temp.iloc[-1]['close'] if df_hist_temp is not None and not df_hist_temp.empty else 1.0
                         
                         sell_success = kis_api.execute_kis_sell(ticker, shares_to_sell, curr_price)
                         print(f"Executed stop-loss limit sell for {ticker} ({shares_to_sell} shares). Success: {sell_success}")
                     except Exception as e:
                         print(f"Stop-loss KIS API sell error for {ticker}: {e}")
                
                # 2. Prevent DB desync if API failed
                if kis_api.KIS_READY and not sell_success:
                    print(f"API stop-loss sell failed for {ticker}. Skipping DB update.")
                    continue

                # 3. Get current price for record logging (fallback to Yahoo if live fails)
                df_hist = get_price_history(ticker)
                execute_price = df_hist.iloc[-1]['close'] if df_hist is not None and not df_hist.empty else 1.0
                
                # 4. Update DB state
                db_manager.trigger_stop_loss(slot_key, ticker, reason, execute_price, shares_to_sell)
    
    # Calculate Total Portfolio Value using KIS API Direct Fetch
    if kis_api.KIS_READY:
        print("\n=== Fetching Daily Portfolio Value via KIS API ===")
        total_value = kis_api.get_total_portfolio_value()
        
        if total_value > 0.0:
            today_str = datetime.now().strftime("%Y-%m-%d")
            total_value = round(total_value, 2)
            print(f"Total Portfolio Value: ${total_value:,.2f}")
            db_manager.save_daily_portfolio_value(today_str, total_value)
        else:
             print("Total Portfolio Value fetched as $0.00, check account configuration.")
    else:
        print("\n=== KIS API Not Ready: Cannot fetch daily value ===")
        
    if all_alerts:
        message = "🚨 ETF Momentum Alert 🚨\n\n" + "\n\n".join(all_alerts)
        print("Sending alert...")
        print(message)
        send_telegram_message(message)
    else:
        print("No alerts to send.")
        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        safe_error = f"<pre>{error_msg}</pre>"
        print(f"CRITICAL ERROR in etf_monitoring.py: {e}")
        try:
            message = f"❌ <b>ETF Monitoring Critical Crash</b>\n{safe_error}"
            send_telegram_message(message)
        except Exception as tel_e:
            print(f"Failed to send crash report to Telegram: {tel_e}")

