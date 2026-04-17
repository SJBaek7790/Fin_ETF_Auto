"""
ETF Screening — Korean Domestic ETFs

Screens Korea-listed ETFs using a momentum + excess RSI composite scoring system,
then uses Gemini AI to select a final elite portfolio of 3 ETFs.
Executes buy orders via the KIS domestic stock API.

Run schedule: Once weekly.
"""

import os
import time
import json
import logging
import pandas as pd
import numpy as np
import asyncio
import telegram
import warnings
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from html import escape
from concurrent.futures import ThreadPoolExecutor, as_completed
from google import genai
from google.genai import types

from log_config import setup_logging, get_logger
from common import (
    send_telegram_message, send_telegram_message_async,
    get_etf_ticker_list_wrapper, get_etf_ohlcv_by_date_wrapper, get_etf_ticker_name_wrapper,
    is_kr_market_open_today
)

import db_manager
import kis_api

warnings.filterwarnings('ignore')

logger = get_logger(__name__)

# --- CONFIG ---
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

# Basic Settings
EXCLUDE_KEYWORDS = [
    '2X', '3X', '-1X', '-2X', '-3X',
    'Ultra', 'Bull', 'Bear', 'Inverse', 'Short', 'VIX', 'ETN',
    'Target', 'Duration',
    # Korean-specific
    '레버리지', '인버스', '곱버스', '선물', '2배', '3배',
    '숏', '베어', '불',
]
MIN_AVG_TRADING_KRW = 1_000_000_000  # ₩1B KRW minimum daily trading value
STARTING_CAPITAL_KRW = 10_000_000     # ₩10M KRW total capital
EPSILON = 1e-8

# Benchmark: KODEX 200 (069500) — Korea's equivalent of SPY
BENCHMARK_TICKER = "069500"

# --- CORE LOGIC ---

def calculate_rsi(returns, period=14):
    """Calculates RSI on a pandas Series of returns or deltas."""
    delta = returns
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_etf_data(ticker, start_str, end_str):
    """Fetches generic ETF data (OHLCV, Name, trading value approximation)."""
    try:
        etf_name = get_etf_ticker_name_wrapper(ticker)
        df_ohlcv = get_etf_ohlcv_by_date_wrapper(start_str, end_str, ticker)

        if df_ohlcv is None or len(df_ohlcv) == 0:
            return None
        
        df_ohlcv = df_ohlcv.sort_index()
        if len(df_ohlcv) < 120:
             return None

        close_prices = df_ohlcv['close'].astype(float)
        trading_values = df_ohlcv['value'].astype(float)

        avg_trading_krw = trading_values.iloc[-20:].mean()

        return {
            'ticker': ticker,
            'name': etf_name,
            'close': close_prices,
            'avg_trading_krw': avg_trading_krw
        }
    except Exception as e:
        return None

def calculate_metrics(data, benchmark_ret):
    """Calculates RET3M and EXRSI3M."""
    close = data['close']
    
    if len(close) < 60:
        return None
        
    ret_3m = ((close.iloc[-1] - close.iloc[-60]) / close.iloc[-60]) * 100
    
    etf_ret = close.pct_change()
    
    df_aligned = pd.DataFrame({
        'ETF': etf_ret,
        'BM': benchmark_ret
    }).dropna()
    
    if len(df_aligned) < 60:
         return None

    df_aligned['Excess'] = df_aligned['ETF'] - df_aligned['BM']
    
    rsi_series = calculate_rsi(df_aligned['Excess'], period=60)
    ex_rsi_3m = rsi_series.iloc[-1]
    
    if len(rsi_series) == 0 or np.isnan(rsi_series.iloc[-1]) or pd.isna(ex_rsi_3m):
        return None

    return {
        'RET3M': round(ret_3m, 2),
        'EXRSI3M': round(ex_rsi_3m, 2)
    }

def process_single_etf(ticker, benchmark_ret, start_str, end_str):
    """Main screening function for a single ETF."""
    stats = {}
    try:
        data = fetch_etf_data(ticker, start_str, end_str)
        if not data:
            stats['filter'] = 'no_data'; return None, stats
        
        name = data['name']
        name_upper = name.upper()
        if any(keyword.upper() in name_upper for keyword in EXCLUDE_KEYWORDS):
            stats['filter'] = 'excluded_keywords'; return None, stats

        if data['avg_trading_krw'] < MIN_AVG_TRADING_KRW:
            stats['filter'] = 'low_trading'; return None, stats
        
        close = data['close']
        sma_120 = close.rolling(window=120).mean().iloc[-1]
        
        price_3m_ago = close.iloc[-60] if len(close) >= 60 else 0
        current_price = close.iloc[-1]
        
        if current_price < sma_120 or current_price < price_3m_ago:
             stats['filter'] = 'failed_momentum'; return None, stats
        
        metrics = calculate_metrics(data, benchmark_ret)
        if not metrics:
             stats['filter'] = 'missing_metrics'; return None, stats
        
        stats['filter'] = 'passed'
        result = {
            'Ticker': ticker, 'ETF Name': name, 
            'Avg Trading Value (KRW)': round(data['avg_trading_krw'], 0),
            **metrics
        }
        return result, stats
        
    except Exception:
        stats['filter'] = 'error'; return None, stats


# --- GEMINI SELECTION ---

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
DATA_DIR = 'data'

def select_etfs_with_gemini(df_report):
    """Uses Gemini to select unique ETFs from the screened report."""
    if df_report.empty:
        logger.warning("No ETFs to select from.")
        return []
    
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set. Falling back to top 3.")
        return _fallback_top3(df_report)

    report_json = df_report.to_json(orient='records', force_ascii=False, indent=2)

    prompt = f"""# Role
You are a Senior Quant Portfolio Manager specializing in the Korean equity market. You are highly skilled in Korean macro trends, GICS sector rotation strategies for Korea-listed ETFs, and have exceptional ability to filter out data noise to capture core trends.

# Task
Analyze the provided momentum top 50 Korea-listed ETF data and select the **'elite universe of exactly 3 ETFs'**.

# Selection Logic & Constraints (Strict Adherence)
1. **Exclude Leverage/Inverse:** Unconditionally exclude funds with keywords like '2X', '3X', 'Ultra', 'Bull', 'Bear', 'Inverse', 'Short', 'VIX', 'ETN', '레버리지', '인버스', '곱버스', '선물', 'TDF', '커버드콜'.
2. **Representation & Deduplication:** If ETFs tracking the same GICS sector or Korean macro theme are duplicated, keep only the 1 with the highest 'Avg Trading Value (KRW)' and market representation, and exclude the rest.
3. **Liquidity & Credit Risk Filtering:** Exclude products with significantly low trading volume, or those with issuer credit risk like ETNs.
4. **Portfolio Diversity:** Ensure the final 3 ETFs are not 100% concentrated in a single theme. Distribute across 2~3 leading sectors/themes (GICS sectors or Korean macro trends). (However, overweighting is permitted if there is an overwhelmingly clear dominant market theme).

# Macro & News Validation
Using Google Search, review the major news and macroeconomic environment from the past 1 week to 1 month for the underlying assets or core sectors of the shortlisted ETFs. Focus on Korea, Asia, and global macro factors affecting the Korean market.
1. Identify Catalysts: Is the current high return (RET3M) justified by fundamental improvements, strong policy support, or robust structural themes?
2. Assess Trend Reversal Risks: Are there emerging headwinds (e.g., BOK rate decisions, KRW weakness, geopolitical risks, China slowdown) that could break the momentum?

# Input Data
{report_json}

# Output Format
The result MUST be output ONLY in the JSON array format below. Do NOT include any additional explanations or greetings outside the markdown code block.
"""

    client = genai.Client(api_key=GEMINI_API_KEY)

    MAX_RETRIES = 3
    response = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model='gemini-3.1-pro-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.HIGH),
                    response_mime_type="application/json",
                    response_schema=types.Schema(
                        type=types.Type.ARRAY,
                        items=types.Schema(
                            type=types.Type.OBJECT,
                            properties={
                                "Ticker": types.Schema(
                                    type=types.Type.STRING,
                                    description="6-digit KRX ticker code (ex: 069500)"
                                ),
                                "ETF Name": types.Schema(
                                    type=types.Type.STRING,
                                    description="Official name of ETF"
                                ),
                                "Reason": types.Schema(
                                    type=types.Type.STRING,
                                    description="Selection Rationale: Based on the latest retrieved news and macro indicators, summarize in a single sentence the macroeconomic justification for the continued trend of this theme and how it contributes to portfolio diversification."
                                )
                            },
                            required=["Ticker", "ETF Name", "Reason"]
                        )
                    )
                )
            )
            break
        except Exception as e:
            logger.warning("Gemini API attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                logger.error("All %d Gemini API attempts failed. Falling back to top 3.", MAX_RETRIES)
                return _fallback_top3(df_report)
            time.sleep(2 ** attempt)

    # --- Parse and validate ---
    try:
        cleaned_text = response.text.strip().removeprefix('```json').removesuffix('```').strip()
        selected = json.loads(cleaned_text)
    except Exception as e:
        logger.error("Gemini response parsing error: %s. Falling back to top 3.", e)
        return _fallback_top3(df_report)

    if isinstance(selected, list):
        valid_tickers = set(df_report['Ticker'].astype(str))
        validated_selected = []
        
        for s in selected:
            ticker = s.get('Ticker', '')
            if ticker in valid_tickers:
                validated_selected.append(s)
            else:
                logger.warning("Gemini hallucinated ticker '%s'. Rejecting from selection.", ticker)
        
        selected = validated_selected

        if not selected:
            logger.warning("Gemini returned no valid ETFs after filtering hallucinations. Falling back.")
            return _fallback_top3(df_report)

        logger.info("Gemini selected ETFs successfully.")
        for s in selected:
            logger.info("  - %s %s: %s", s['Ticker'], s['ETF Name'], s.get('Reason', 'N/A'))
        return selected
    else:
        logger.warning("Gemini returned unexpected format. Falling back.")
        return _fallback_top3(df_report)

def _fallback_top3(df_report):
    """Fallback: pick the top 3 by Composite Score."""
    top3 = df_report.head(3)
    return [
        {'Ticker': str(row['Ticker']), 'ETF Name': row['ETF Name'], 'Reason': 'Fallback: 복합 점수 상위 종목'}
        for _, row in top3.iterrows()
    ]

def save_selected_etfs(selected, date_str):
    """Saves the Gemini-selected ETFs to a dated JSON file in data/ folder."""
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = os.path.join(DATA_DIR, f"selected_etfs_{date_str}_kr.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(selected, f, ensure_ascii=False, indent=2)
    logger.info("Saved selected ETFs to %s", filename)
    return filename


# --- MAIN ---

def main():
    import subprocess, sys
    # NOTE: monitor.py is intentionally NOT listed here.
    # On Fridays, etf_monitoring.py already ran monitor.py + order_placement.py.
    # This wrapper only needs to run the screening signal generator and then
    # re-run order_placement.py with the combined (sell + buy) pending orders.
    result = subprocess.run([sys.executable, "screen.py"], check=False)
    if result.returncode != 0:
        logger.error("screen.py exited with code %d", result.returncode)
    subprocess.run([sys.executable, "order_placement.py"], check=False)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical("Unhandled exception: %s", e, exc_info=True)
        if TOKEN and CHAT_ID:
            try:
                send_telegram_message(f"❌ ETF Screening wrapper CRASH\n{e}")
            except Exception as inner_e:
                logger.error("Failed to send crash log via Telegram: %s", inner_e)
