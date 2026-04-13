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
    import subprocess, sys
    scripts = ["monitor.py"]
    for script in scripts:
        result = subprocess.run([sys.executable, script], check=False)
        if result.returncode != 0:
            logger.error("%s exited with code %d", script, result.returncode)
    subprocess.run([sys.executable, "order_placement.py"], check=False)
        
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
