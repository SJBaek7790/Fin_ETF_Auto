"""
ETF Screening Wrapper — Korean Domestic ETFs

This is a wrapper script that executes the decoupled screening pipeline:
1. screen.py (Signal generation & selection)
2. order_placement.py (Order execution)

Run schedule: Once weekly.
"""

import os
import subprocess
import sys
import logging
from log_config import setup_logging, get_logger
from common import send_telegram_message

# Configuration
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

setup_logging("screening_wrapper")
logger = get_logger(__name__)

def main():
    logger.info("=== Starting ETF Screening Pipeline (Wrapper) ===")
    
    # 1. Run Signal Generator (screen.py)
    # This generates pending_orders.json and logs selected_etfs_*.json
    logger.info("Step 1: Running screen.py...")
    result_screen = subprocess.run([sys.executable, "screen.py"], check=False)
    if result_screen.returncode != 0:
        logger.error("screen.py failed with exit code %d", result_screen.returncode)
        # We continue to order_placement if there were already pending orders from monitor.py, 
        # but usually screening failure should be investigated.
    
    # 2. Run Order Placement (order_placement.py)
    # This executes both SELLs from monitor.py and BUYs from screen.py
    logger.info("Step 2: Running order_placement.py...")
    result_order = subprocess.run([sys.executable, "order_placement.py"], check=False)
    if result_order.returncode != 0:
        logger.error("order_placement.py failed with exit code %d", result_order.returncode)

    logger.info("=== ETF Screening Pipeline Finished ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical("Unhandled exception in screening wrapper: %s", e, exc_info=True)
        if TOKEN and CHAT_ID:
            try:
                send_telegram_message(f"❌ ETF Screening wrapper CRASH\n{e}")
            except Exception as inner_e:
                logger.error("Failed to send crash log via Telegram: %s", inner_e)
