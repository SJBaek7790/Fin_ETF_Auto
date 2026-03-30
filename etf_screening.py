"""
ETF Screening — Korean Domestic ETFs

Screens Korea-listed ETFs using a momentum + excess RSI composite scoring system,
then uses Gemini AI to select a final elite portfolio of 3 ETFs.
Executes buy orders via the KIS domestic stock API.

Run schedule: Once weekly (e.g. every Thursday during KRX market hours).
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
from html import escape
from concurrent.futures import ThreadPoolExecutor, as_completed
from google import genai
from google.genai import types

from log_config import setup_logging, get_logger, get_log_filepath
from common import (
    send_telegram_document_async,
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

# Dates
end_date = datetime.now()
start_date = end_date - timedelta(days=200)
end_str = end_date.strftime('%Y%m%d')
start_str = start_date.strftime('%Y%m%d')

# Global Filter Stats
filter_stats = {
    'total': 0, 'no_data': 0, 'insufficient_data': 0, 'excluded_keywords': 0,
    'low_trading': 0, 'failed_momentum': 0,
    'missing_metrics': 0, 'passed': 0, 'error': 0
}

# --- CORE LOGIC ---

def calculate_rsi(series, period=14):
    """Calculates RSI on a pandas Series."""
    delta = series
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_etf_data(ticker):
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

def process_single_etf(ticker, benchmark_ret):
    """Main screening function for a single ETF."""
    stats = {}
    try:
        data = fetch_etf_data(ticker)
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
1. **Exclude Leverage/Inverse:** Unconditionally exclude funds with keywords like '2X', '3X', 'Ultra', 'Bull', 'Bear', 'Inverse', 'Short', 'VIX', 'ETN', '레버리지', '인버스', '곱버스', '선물'.
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

async def main():
    setup_logging("screening")
    
    if not is_kr_market_open_today():
        logger.info("KRX market is closed today. Skipping screening.")
        return

    bot = telegram.Bot(token=TOKEN) if TOKEN and CHAT_ID else None
    
    logger.info("Screening Korean ETFs...")
    
    # 0. Fetch Benchmark Data (KODEX 200)
    logger.info("Fetching Benchmark (%s) data...", BENCHMARK_TICKER)
    df_bm = await asyncio.to_thread(get_etf_ohlcv_by_date_wrapper, start_str, end_str, BENCHMARK_TICKER)
    if df_bm is None or df_bm.empty:
        logger.critical("Failed to fetch Benchmark data. Aborting.")
        return

    df_bm = df_bm.sort_index()
    benchmark_ret = df_bm['close'].pct_change().dropna()
    
    etf_tickers = await asyncio.to_thread(get_etf_ticker_list_wrapper, end_str)
    logger.info("Found %d ETF tickers for date %s", len(etf_tickers), end_str)
    results = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_single_etf, t, benchmark_ret.copy()): t for t in etf_tickers}
        for future in as_completed(futures):
            res, stats = future.result()
            filter_stats['total'] += 1
            if stats.get('filter') in filter_stats: 
                filter_stats[stats['filter']] += 1
            if res: results.append(res)
            
    min_max_stats = {}
    df_final = pd.DataFrame()
    df = pd.DataFrame()

    if results:
        df = pd.DataFrame(results)
        
        cols = ['RET3M', 'EXRSI3M']
        for col in cols:
            mn = df[col].min()
            mx = df[col].max()
            min_max_stats[col] = {'min': mn, 'max': mx}
            if col == 'EXRSI3M':
                df[f'S_{col}'] = (mx - df[col]) / (mx - mn) * 100
            else:
                df[f'S_{col}'] = (df[col] - mn) / (mx - mn) * 100
        
        df = df.rename(columns={'S_RET3M': 'RET3M Score', 'S_EXRSI3M': 'EXRSI3M Score'})
        df['Composite Score'] = df[['RET3M Score', 'EXRSI3M Score']].mean(axis=1).round(2)
        df_sorted = df.sort_values('Composite Score', ascending=False).reset_index(drop=True)
        
        output_columns = ['Ticker', 'ETF Name', 'Avg Trading Value (KRW)', 'RET3M', 'RET3M Score', 'EXRSI3M', 'EXRSI3M Score', 'Composite Score']
        df_final = df_sorted[output_columns].head(50)
        
        # --- STEP 1: Gemini selects 3 ETFs ---
        logger.info("=== Gemini ETF Selection ===")
        selected = select_etfs_with_gemini(df_final)
        
        # --- DB MANAGER + KIS ORDER EXECUTION ---
        await asyncio.to_thread(db_manager.init_state)
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        async def get_current_price(ticker):
            data = await asyncio.to_thread(fetch_etf_data, ticker)
            if data and not data['close'].empty:
                return int(float(data['close'].iloc[-1]))
            return None
            
        if selected:
            json_path = save_selected_etfs(selected, end_str)
            
            # Buy into an empty slot
            empty_slot = await asyncio.to_thread(db_manager.get_empty_slot)
            if empty_slot:
                logger.info("=== Buying into Slot %s ===", empty_slot)
                target_sell_date = (datetime.now() + timedelta(days=28)).strftime("%Y-%m-%d")
                
                # Allocation Logic
                state = await asyncio.to_thread(db_manager.get_portfolio_state)
                slot_data = state.get("slots", {}).get(empty_slot, {})
                allocated_krw = slot_data.get("cash_balance", 0.0)
                
                if allocated_krw == 0.0:
                    logger.info("Initial bootstrap: Fetching total KRW to divide among unallocated slots.")
                    if kis_api.KIS_READY:
                        total_krw = await asyncio.to_thread(kis_api.get_available_krw)
                    else:
                        total_krw = STARTING_CAPITAL_KRW
                    
                    unallocated_slots = [k for k, v in state.get("slots", {}).items() if v.get("status") == "empty" and v.get("cash_balance", 0.0) == 0.0]
                    
                    if unallocated_slots:
                        allocated_krw = round(total_krw / len(unallocated_slots), 0)
                        for k in unallocated_slots:
                            if k != empty_slot:
                                state["slots"][k]["cash_balance"] = allocated_krw        
                        await asyncio.to_thread(db_manager._save_state, state)
                
                logger.info("Allocated KRW for Slot %s: ₩%s", empty_slot, f"{allocated_krw:,.0f}")
                
                krw_per_etf = allocated_krw / len(selected)
                
                new_holdings = []
                total_spent = 0.0
                
                for entry in selected:
                    t = str(entry.get('Ticker', entry.get('ticker', '')))
                    n = str(entry.get('ETF Name', entry.get('name', '')))
                    
                    price = await get_current_price(t)
                    if price is None:
                        logger.warning("Price data unavailable for %s. Skipping buy.", t)
                        continue
                    # Apply a 3% cash buffer for price fluctuations
                    shares_to_buy = int((krw_per_etf * 0.97) // price)
                    
                    if shares_to_buy > 0:
                        if kis_api.KIS_READY:
                            success = await asyncio.to_thread(kis_api.execute_kis_buy, t, shares_to_buy, price)
                            if not success:
                                logger.error("API buy failed for %s. Skipping DB update.", t)
                                continue
                        else:
                            success = True
                            logger.info("MOCK MODE: Simulated buy for %s.", t)
                            
                        logger.info("Executed buy for Slot %s - %s (%d shares @ ₩%s). Success: %s",
                                    empty_slot, t, shares_to_buy, f"{price:,}", success)
                        
                        actual_spent = shares_to_buy * price
                        total_spent += actual_spent
                        
                        new_holdings.append({
                            "ticker": t,
                            "name": n,
                            "shares": shares_to_buy,
                            "buy_price": price,
                            "status": "active"
                        })
                
                remaining_cash = round(allocated_krw - total_spent, 0)
                await asyncio.to_thread(db_manager.fill_slot, empty_slot, target_sell_date, new_holdings, today_str, initial_cash_balance=remaining_cash)
                
                logger.info("Bought %d ETFs into Slot %s. (Spent: ₩%s)", len(new_holdings), empty_slot, f"{total_spent:,.0f}")
            else:
                logger.warning("No empty slot available to buy new ETFs!")
    else:
        logger.info("No ETFs passed screening.")

    logger.info("=== Filter Statistics ===")
    for k, v in filter_stats.items():
        logger.info("%s: %s", k, v)

    # --- FINAL: Send log file via Telegram ---
    log_path = get_log_filepath()
    if bot and log_path:
        await send_telegram_document_async(log_path, caption=f"ETF Screening Log ({end_str})", bot=bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical("Unhandled exception: %s", e, exc_info=True)
        if TOKEN and CHAT_ID:
            try:
                log_path = get_log_filepath()
                if log_path:
                    bot = telegram.Bot(token=TOKEN)
                    asyncio.run(send_telegram_document_async(log_path, caption=f"❌ ETF Screening CRASH Log", bot=bot))
            except Exception as inner_e:
                logger.error("Failed to send crash log via Telegram: %s", inner_e)
