import os
import json
import requests
import telegram
import asyncio
import pandas as pd
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime

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
        print("Telegram credentials missing.")
        return

    # Using requests for synchronous usage (compatibility with non-async parts if needed)
    # But since we have asyncio in screening, we might want async? 
    # Current usages: etf_monitoring (sync), etf_screening (async wrapper).
    # We'll provide a sync version here.
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")

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
        print(f"Failed to send Telegram document: {e}")

# --- FinanceDataReader WRAPPERS ---

_ETF_LISTING_CACHE = None

def _get_etf_listing():
    global _ETF_LISTING_CACHE
    if _ETF_LISTING_CACHE is None:
        try:
            _ETF_LISTING_CACHE = fdr.StockListing('ETF/US')
        except Exception as e:
            print(f"Error fetching ETF listings for US via fdr: {e}")
            _ETF_LISTING_CACHE = pd.DataFrame()
    return _ETF_LISTING_CACHE

def get_market_ohlcv_wrapper(start_date, end_date, ticker):
    """Wrapper using FinanceDataReader with yfinance fallback for US ETFs"""
    try:
        start_dt = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
        df = fdr.DataReader(ticker, start_dt, end_dt)
        
        if df is None or df.empty:
            print(f"Empty data from fdr for {ticker}, falling back to yfinance")
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
        print(f"Error in get_market_ohlcv_wrapper for {ticker}: {e}, falling back to yfinance")
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
        print(f"Error in get_etf_ticker_list_wrapper: {e}")
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
        print(f"Error in get_etf_ticker_name_wrapper for {ticker}: {e}")
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
        print(f"yfinance fallback failed for {ticker}: {e}")
        return None

