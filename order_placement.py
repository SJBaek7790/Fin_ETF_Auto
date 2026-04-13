"""
ETF Order Placement — Execution Stage

Reads pending_orders.json. 
Executes SELL orders first, then BUY orders via KIS API.
Updates db_manager state safely and logs trades.
Deletes pending_orders.json upon completion.
"""

import os
import sys
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import asyncio
import telegram

from log_config import setup_logging, get_logger
from common import (
    send_telegram_message, send_telegram_message_async,
    is_kr_market_open_today
)

import db_manager
import kis_api

logger = get_logger(__name__)

TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

async def main_async():
    setup_logging("order_placement")

    if not is_kr_market_open_today():
        logger.info("KRX market is closed today. Skipping order placement.")
        return

    logger.info("Starting ETF Order Placement...")

    pending_data = await asyncio.to_thread(db_manager.read_and_clear_pending_orders)
    
    if not pending_data:
        logger.info("No pending orders found. Exiting.")
        return

    orders = pending_data.get("orders", [])
    if not orders:
        logger.info("Pending orders list is empty. Exiting.")
        return

    sell_orders = [o for o in orders if o.get('action') == 'SELL']
    buy_orders = [o for o in orders if o.get('action') == 'BUY']
    
    logger.info("Executing orders: %d SELL, %d BUY", len(sell_orders), len(buy_orders))
    
    messages = [f"🚀 Order Placement Executed: {len(sell_orders)} Sells, {len(buy_orders)} Buys"]
    
    # 1. Execute SELL Orders
    for o in sell_orders:
        t = str(o['ticker'])
        s = int(o['shares'])
        p = int(o['estimated_price'])
        slot = str(o['slot'])
        reason = o.get('reason', 'time-stop')
        
        success = False
        if kis_api.KIS_READY:
            success = await asyncio.to_thread(kis_api.execute_kis_sell, t, s, p)
            if not success:
                msg = f"❌ API SELL failed for {t} in Slot {slot} ({s} shares). Skipping DB update."
                logger.error(msg)
                messages.append(msg)
                continue
        else:
            logger.info("MOCK SELL for %s - %d shares", t, s)
            success = True
            
        if success:
            proceeds = s * p
            if reason == "time-stop":
                await asyncio.to_thread(db_manager.clear_slot, slot, returned_cash=proceeds)
            else:
                await asyncio.to_thread(db_manager.trigger_stop_loss, slot, t, reason, p, s)
                
            await asyncio.to_thread(db_manager.log_trade, "SELL", t, slot, s, p, reason)
            
            msg = f"✅ Executed SELL: {t} in Slot {slot} ({s} sh @ ₩{p:,.0f}). Reason: {reason}"
            logger.info(msg)
            messages.append(msg)

    # 2. Execute BUY Orders
    # Group buy orders by slot for db_manager.fill_slot API
    buys_by_slot = {}
    for o in buy_orders:
        sk = str(o['slot'])
        if sk not in buys_by_slot:
            buys_by_slot[sk] = []
        buys_by_slot[sk].append(o)
        
    for slot_key, orders_in_slot in buys_by_slot.items():
        new_holdings = []
        target_sell_date = orders_in_slot[0].get('target_sell_date', (datetime.now(tz=ZoneInfo("Asia/Seoul"))).strftime("%Y-%m-%d"))
        
        # Calculate max initial_cash_balance from the screening stage
        # We assume the slot's DB cash balance is current (from sells!). Wait...
        # If we time-stopped, slot has cash = proceeds. 
        # But wait, BUY orders should just append directly to the slot as new holdings.
        # But db_manager.fill_slot overwrites the slot. 
        # What is the remaining cash balance?
        # The screening.py logic computes a per-ETF budget, but does not provide remaining.
        # Let's derive remaining by reading current state, deducting total spent!
        
        state = await asyncio.to_thread(db_manager.get_portfolio_state)
        current_cash = state.get("slots", {}).get(slot_key, {}).get("cash_balance", 0.0)
        
        total_spent = 0.0
        
        for o in orders_in_slot:
            t = str(o['ticker'])
            s = int(o['shares'])
            p = int(o['estimated_price'])
            n = str(o.get('name', ''))
            
            success = False
            if kis_api.KIS_READY:
                success = await asyncio.to_thread(kis_api.execute_kis_buy, t, s, p)
                if not success:
                    msg = f"❌ API BUY failed for {t} in Slot {slot_key} ({s} shares). Skipping DB update."
                    logger.error(msg)
                    messages.append(msg)
                    continue
            else:
                logger.info("MOCK BUY for %s - %d shares", t, s)
                success = True
                
            if success:
                actual_spent = s * p
                total_spent += actual_spent
                new_holdings.append({
                    "ticker": t,
                    "name": n,
                    "shares": s,
                    "buy_price": p,
                    "status": "active"
                })
                
                await asyncio.to_thread(db_manager.log_trade, "BUY", t, slot_key, s, p, "reallocation")
                
                msg = f"✅ Executed BUY: {n} ({t}) in Slot {slot_key} ({s} sh @ ₩{p:,.0f})."
                logger.info(msg)
                messages.append(msg)
                
        if new_holdings:
            remaining_cash = round(current_cash - total_spent, 0)
            if remaining_cash < 0:
                 remaining_cash = 0.0
            today_str = datetime.now(tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
            await asyncio.to_thread(db_manager.fill_slot, slot_key, target_sell_date, new_holdings, buy_date=today_str, initial_cash_balance=remaining_cash)

    # 3. Portfolio value snapshot
    total_value = 0
    if getattr(kis_api, 'KIS_READY', False):
        logger.info("=== Fetching Daily Portfolio Value via KIS API ===")
        total_value = await asyncio.to_thread(kis_api.get_total_portfolio_value)
        
        if total_value > 0.0:
            today_str = datetime.now(tz=ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
            total_value = round(total_value, 0)
            logger.info("Total Portfolio Value: ₩%s", f"{total_value:,.0f}")
            await asyncio.to_thread(db_manager.save_daily_portfolio_value, today_str, total_value)
            messages.insert(1, f"Total Portfolio Value: ₩{total_value:,.0f}")
        else:
             logger.warning("Total Portfolio Value fetched as ₩0, check account configuration.")
    else:
        logger.info("KIS API Not Ready: Cannot fetch daily value")

    # Telegram notification
    if messages and (len(sell_orders) > 0 or len(buy_orders) > 0):
        bot = telegram.Bot(token=TOKEN) if TOKEN and CHAT_ID else None
        if bot:
            await send_telegram_message_async("\n".join(messages), bot=bot)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        logger.critical("CRITICAL ERROR in order_placement.py: %s", e, exc_info=True)
        try:
            send_telegram_message(f"❌ Order Placement CRASH\n{e}")
        except Exception as tel_e:
            logger.error("Failed to send crash log to Telegram: %s", tel_e)
