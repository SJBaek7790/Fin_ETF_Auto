import os
import sys
import json
import logging

# Add .local_deps to sys.path for locally-installed packages (e.g., exchange_calendars)
_LOCAL_DEPS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.local_deps')
if os.path.isdir(_LOCAL_DEPS) and _LOCAL_DEPS not in sys.path:
    sys.path.insert(0, _LOCAL_DEPS)
import requests
import telegram
import asyncio
import pandas as pd
import FinanceDataReader as fdr
import yfinance as yf
import exchange_calendars as xcals
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- TELEGRAM CONSTANTS ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram_message(message):
    """Sends a simple text message via Telegram."""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.warning("Telegram credentials missing.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error("Failed to send Telegram message: %s", e)

async def send_telegram_message_async(message, bot=None):
    """Async version using python-telegram-bot if available, or just wrapping sync."""
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    
    if bot:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')
    else:
        # Fallback to creating a bot instance
        local_bot = telegram.Bot(token=TELEGRAM_TOKEN)
        await local_bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')

async def send_telegram_document_async(file_path, caption=None, bot=None):
    """Sends a local file as a document via Telegram."""
    if not TELEGRAM_TOKEN or not CHAT_ID or not os.path.exists(file_path):
        return
    
    if not bot:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        
    try:
        with open(file_path, 'rb') as f:
            await bot.send_document(chat_id=CHAT_ID, document=f, caption=caption)
    except Exception as e:
        logger.error("Failed to send Telegram document: %s", e)

def send_telegram_document_sync(file_path, caption=None):
    """Sends a local file as a document via Telegram (synchronous)."""
    if not TELEGRAM_TOKEN or not CHAT_ID or not os.path.exists(file_path):
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
        with open(file_path, 'rb') as f:
            requests.post(url, data={"chat_id": CHAT_ID, "caption": caption or ""}, files={"document": f}, timeout=30)
    except Exception as e:
        logger.error("Failed to send Telegram document (sync): %s", e)

# --- FinanceDataReader WRAPPERS ---

_ETF_LISTING_CACHE = None

def _get_etf_listing():
    global _ETF_LISTING_CACHE
    if _ETF_LISTING_CACHE is None:
        try:
            _ETF_LISTING_CACHE = fdr.StockListing('ETF/US')
        except Exception as e:
            logger.error("Error fetching ETF listings for US via fdr: %s", e)
            _ETF_LISTING_CACHE = pd.DataFrame()
    return _ETF_LISTING_CACHE

def get_market_ohlcv_wrapper(start_date, end_date, ticker):
    """Wrapper using FinanceDataReader with yfinance fallback for US ETFs"""
    try:
        start_dt = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
        df = fdr.DataReader(ticker, start_dt, end_dt)
        
        if df is None or df.empty:
            logger.warning("Empty data from fdr for %s, falling back to yfinance", ticker)
            return fetch_ohlcv_yfinance(start_date, end_date, ticker)
            
        # Reconstruct DataFrame with pykrx compatible columns
        df_out = pd.DataFrame({
            '시가': df['Open'],
            '고가': df['High'],
            '저가': df['Low'],
            '종가': df['Close'],
            '거래량': df['Volume'],
            '거래대금': df['Close'] * df['Volume'],
            '등락률': df['Change'] * 100 if 'Change' in df.columns else df['Close'].pct_change() * 100
        })
        return df_out
    except Exception as e:
        logger.warning("Error in get_market_ohlcv_wrapper for %s: %s, falling back to yfinance", ticker, e)
        return fetch_ohlcv_yfinance(start_date, end_date, ticker)

def get_etf_ohlcv_by_date_wrapper(start_date, end_date, ticker):
    """Wrapper using FinanceDataReader with yfinance fallback"""
    return get_market_ohlcv_wrapper(start_date, end_date, ticker)

def get_etf_ticker_list_wrapper(date=None):
    """Wrapper using FinanceDataReader"""
    try:
        df_etf = _get_etf_listing()
        if not df_etf.empty and 'Symbol' in df_etf.columns:
            return df_etf['Symbol'].tolist()
        return []
    except Exception as e:
        logger.error("Error in get_etf_ticker_list_wrapper: %s", e)
        return []

def get_etf_ticker_name_wrapper(ticker):
    """Wrapper using FinanceDataReader"""
    try:
        df_etf = _get_etf_listing()
        if not df_etf.empty and 'Symbol' in df_etf.columns and 'Name' in df_etf.columns:
            match = df_etf[df_etf['Symbol'] == ticker]
            if not match.empty:
                return match.iloc[0]['Name']
        return str(ticker)
    except Exception as e:
        logger.error("Error in get_etf_ticker_name_wrapper for %s: %s", ticker, e)
        return str(ticker)

def fetch_ohlcv_yfinance(start_date_str, end_date_str, ticker):
    """Fallback method to fetch US historical data using yfinance."""
    try:
        start_dt = datetime.strptime(start_date_str, '%Y%m%d').strftime('%Y-%m-%d')
        end_dt = datetime.strptime(end_date_str, '%Y%m%d').strftime('%Y-%m-%d')
        
        df = yf.download(ticker, start=start_dt, end=end_dt, progress=False)
        
        if df is None or df.empty:
            return None
        
        # Handle potential MultiIndex columns in newer yfinance versions
        if isinstance(df.columns, pd.MultiIndex):
            close_series = df['Close'].iloc[:, 0]
            volume_series = df['Volume'].iloc[:, 0]
            open_series = df['Open'].iloc[:, 0]
            high_series = df['High'].iloc[:, 0]
            low_series = df['Low'].iloc[:, 0]
        else:
            close_series = df['Close']
            volume_series = df['Volume']
            open_series = df['Open']
            high_series = df['High']
            low_series = df['Low']
            
        # Reconstruct DataFrame with pykrx compatible columns
        df_out = pd.DataFrame({
            '시가': open_series,
            '고가': high_series,
            '저가': low_series,
            '종가': close_series,
            '거래량': volume_series,
            '거래대금': close_series * volume_series,
            '등락률': close_series.pct_change() * 100
        })
        return df_out
    except Exception as e:
        logger.error("yfinance fallback failed for %s: %s", ticker, e)
        return None


# --- US MARKET CALENDAR ---

def is_us_market_open_today():
    """Returns True if today is a US market trading day (XNYS = NYSE).
    
    Note: Uses the current US Eastern date to check against the NYSE calendar.
    exchange_calendars requires timezone-naive timestamps.
    """
    nyse = xcals.get_calendar("XNYS")
    # Get today's date in US Eastern time, then strip timezone for exchange_calendars
    us_eastern_now = pd.Timestamp.now(tz="America/New_York")
    today = pd.Timestamp(us_eastern_now.date())  # tz-naive date
    return nyse.is_session(today)
