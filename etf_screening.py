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
import config
from datetime import datetime, timedelta
from html import escape
from concurrent.futures import ThreadPoolExecutor, as_completed
from google import genai
from google.genai import types

from log_config import setup_logging, get_logger, get_log_filepath
from common import (
    send_telegram_document_async,
    get_etf_ticker_list_wrapper, get_etf_ohlcv_by_date_wrapper, get_etf_ticker_name_wrapper,
    is_us_market_open_today
)

import db_manager
import kis_api

warnings.filterwarnings('ignore')

logger = get_logger(__name__)

# --- CONFIG ---
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

# Basic Settings
EXCLUDE_KEYWORDS = ['2X', '3X', '-1X', '-2X', '-3X', 'Ultra', 'Bull', 'Bear', 'Inverse', 'Short', 'VIX', 'ETN', 'Target', 'Duration']
MIN_AVG_TRADING = 10000000
EPSILON = 1e-8

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
    
    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Calculate EMA (Wilder's Smoothing)
    # com = period - 1 corresponds to Wilder's alpha = 1/period
    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_etf_data(ticker):
    """Fetches generic ETF data (OHLCV, Name, AUM approximation)."""
    try:
        etf_name = get_etf_ticker_name_wrapper(ticker)
        df_ohlcv = get_etf_ohlcv_by_date_wrapper(start_str, end_str, ticker)

        if df_ohlcv is None or len(df_ohlcv) == 0:
            return None
        
        df_ohlcv = df_ohlcv.sort_index()
        # We need enough data for 60-day RSI + some lookback. 120 is safe.
        if len(df_ohlcv) < 120:
             return None

        close_prices = df_ohlcv['종가'].astype(float)
        trading_values = df_ohlcv['거래대금'].astype(float)

        avg_trading_usd = trading_values.iloc[-20:].mean()

        return {
            'ticker': ticker,
            'name': etf_name,
            'close': close_prices,
            'avg_trading_usd': avg_trading_usd
        }
    except Exception as e:
        return None

def calculate_metrics(data, benchmark_ret):
    """Calculates RET3M and EXRSI3M."""
    close = data['close']
    
    # 1. RET3M: ((price_today - price_3m_ago) / price_3m_ago) * 100
    # 3 months approx 60 trading days
    if len(close) < 60:
        return None
        
    ret_3m = ((close.iloc[-1] - close.iloc[-60]) / close.iloc[-60]) * 100
    
    # 2. EXRSI3M: RSI(60) of Excess Returns
    # Align ETF returns with Benchmark returns
    etf_ret = close.pct_change()
    
    # Combine to align dates
    df_aligned = pd.DataFrame({
        'ETF': etf_ret,
        'BM': benchmark_ret
    }).dropna()
    
    if len(df_aligned) < 60:
         return None

    df_aligned['Excess'] = df_aligned['ETF'] - df_aligned['BM']
    
    # Calculate RSI on Excess Returns with period 60
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

        if data['avg_trading_usd'] < MIN_AVG_TRADING:
            stats['filter'] = 'low_trading'; return None, stats
        
        close = data['close']
        sma_120 = close.rolling(window=120).mean().iloc[-1]
        
        price_3m_ago = close.iloc[-60] if len(close) >= 60 else 0
        current_price = close.iloc[-1]
        
        if current_price < sma_120 or current_price < price_3m_ago:
             stats['filter'] = 'failed_momentum'; return None, stats
        
        # Calculate New Metrics
        metrics = calculate_metrics(data, benchmark_ret)
        if not metrics:
             stats['filter'] = 'missing_metrics'; return None, stats
        
        stats['filter'] = 'passed'
        result = {
            'Ticker': ticker, 'ETF Name': name, 
            'Avg Trading Value (USD)': round(data['avg_trading_usd'], 1),
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
        logger.warning("GEMINI_API_KEY not set. Falling back to top 7.")
        return _fallback_top7(df_report)

    report_json = df_report.to_json(orient='records', force_ascii=False, indent=2)

    prompt = f"""# Role
You are a Senior Quant Portfolio Manager at a large Wall Street hedge fund. You are highly skilled in global macro trends, GICS sector rotation strategies, and have exceptional ability to filter out data noise to capture core trends.

# Task
Analyze the provided momentum top 50 ETF data and select the **'elite universe of 5~10 ETFs'**.

# Selection Logic & Constraints (Strict Adherence)
1. **Exclude Leverage/Inverse:** Unconditionally exclude funds with keywords or structures like '2x', '3x', 'Ultra', 'Bull', 'Bear', 'Inverse', 'Short', 'VIX', 'ETN', 'Target', or 'Duration'.
2. **Representation & Deduplication:** If ETFs tracking the same GICS sector or US macro theme are duplicated, keep only the 1 with the highest 'Avg Trading Value (USD)' and market representation, and exclude the rest.
3. **Liquidity & Credit Risk Filtering:** Exclude products with significantly low trading volume, or those with issuer credit risk like ETNs.
4. **Portfolio Diversity:** Ensure the final list is not 100% concentrated in a single theme. Distribute across 2~3 leading sectors/themes (GICS sectors or US macro trends). (However, overweighting is permitted if there is an overwhelmingly clear dominant market theme).

# Macro & News Validation
Using Google Search, review the major news and macroeconomic environment from the past 1 week to 1 month for the underlying assets or core sectors of the shortlisted ETFs.
1. Identify Catalysts: Is the current high return (RET3M) justified by fundamental improvements in the real economy, strong policy support, or robust structural themes? (Exclude news solely reporting on simple price appreciation.)
2. Assess Trend Reversal Risks: Are there emerging macroeconomic headwinds (e.g., sudden interest rate shifts, regulatory risks, geopolitical conflicts) that could abruptly break the current momentum?

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
                    # 1. Enable Google Search as a tool
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    
                    # 2. Set the thinking level to HIGH
                    thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.HIGH),

                    # 3. JSON Output
                    response_mime_type="application/json",
                    response_schema=types.Schema(
                        type=types.Type.ARRAY,
                        items=types.Schema(
                            type=types.Type.OBJECT,
                            properties={
                                "Ticker": types.Schema(
                                    type=types.Type.STRING,
                                    description="Ticker of ETF (ex: SLV)"
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
            break  # Success — exit retry loop
        except Exception as e:
            logger.warning("Gemini API attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                logger.error("All %d Gemini API attempts failed. Falling back to top 7.", MAX_RETRIES)
                return _fallback_top7(df_report)
            time.sleep(2 ** attempt)  # Exponential backoff: 2s, 4s

    # --- Parse and validate the successful response ---
    try:
        cleaned_text = response.text.strip().removeprefix('```json').removesuffix('```').strip()
        selected = json.loads(cleaned_text)
    except Exception as e:
        logger.error("Gemini response parsing error: %s. Falling back to top 7.", e)
        return _fallback_top7(df_report)

    if isinstance(selected, list):
        # Validate against pre-screened universe to prevent LLM hallucinations
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
            return _fallback_top7(df_report)

        logger.info("Gemini selected ETFs successfully.")
        for s in selected:
            logger.info("  - %s %s: %s", s['Ticker'], s['ETF Name'], s.get('Reason', 'N/A'))
        return selected
    else:
        logger.warning("Gemini returned unexpected format (len=%s). Falling back.",
                       len(selected) if isinstance(selected, list) else 'N/A')
        return _fallback_top7(df_report)

def _fallback_top7(df_report):
    """Fallback: pick the top 7 by Composite Score."""
    top7 = df_report.head(7)
    return [
        {'Ticker': str(row['Ticker']), 'ETF Name': row['ETF Name'], 'Reason': 'Fallback: 복합 점수 상위 종목'}
        for _, row in top7.iterrows()
    ]

def save_selected_etfs(selected, date_str):
    """Saves the Gemini-selected ETFs to a dated JSON file in data/ folder."""
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = os.path.join(DATA_DIR, f"selected_etfs_{date_str}_us.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(selected, f, ensure_ascii=False, indent=2)
    logger.info("Saved selected ETFs to %s", filename)
    return filename


import db_manager

# --- MINIMAL FALLBACK NEEDED? NO. DB MANAGER HANDLES HOLDINGS MONITOR ---


# --- MAIN ---

async def main():
    setup_logging("screening")
    
    if not is_us_market_open_today():
        logger.info("US market is closed today. Skipping screening.")
        return

    bot = telegram.Bot(token=TOKEN) if TOKEN and CHAT_ID else None
    
    logger.info("Screening ETFs...")
    
    # 0. Fetch Benchmark Data (SPY)
    logger.info("Fetching Benchmark (SPY) data...")
    df_bm = await asyncio.to_thread(get_etf_ohlcv_by_date_wrapper, start_str, end_str, "SPY")
    if df_bm is None or df_bm.empty:
        logger.critical("Failed to fetch Benchmark data. Aborting.")
        return

    df_bm = df_bm.sort_index()
    benchmark_ret = df_bm['종가'].pct_change().dropna()
    
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
        
        output_columns = ['Ticker', 'ETF Name', 'Avg Trading Value (USD)', 'RET3M', 'RET3M Score', 'EXRSI3M', 'EXRSI3M Score', 'Composite Score']
        df_final = df_sorted[output_columns].head(50)
        
        # --- STEP 1: Gemini selects 5~10 ETFs ---
        logger.info("=== Gemini ETF Selection ===")
        selected = select_etfs_with_gemini(df_final)
        
        # --- NEW DB MANAGER LOGIC ---
        await asyncio.to_thread(db_manager.init_state)
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        async def get_current_price(ticker):
            # Fetches current price using unauthenticated Yahoo Finance as fallback or KIS
            data = await asyncio.to_thread(fetch_etf_data, ticker)
            if data and not data['close'].empty:
                return round(float(data['close'].iloc[-1]), 2)
            return None  # Caller must handle None — never use a fallback price for real orders
            
        if selected:
            # Save dated selection file
            json_path = save_selected_etfs(selected, end_str)
            
            # 2. Buy into an empty slot
            empty_slot = await asyncio.to_thread(db_manager.get_empty_slot)
            if empty_slot:
                logger.info("=== Buying into Slot %s ===", empty_slot)
                target_sell_date = (datetime.now() + timedelta(days=28)).strftime("%Y-%m-%d")
                
                # Allocation Logic
                state = await asyncio.to_thread(db_manager.get_portfolio_state)
                slot_data = state.get("slots", {}).get(empty_slot, {})
                allocated_usd = slot_data.get("cash_balance", 0.0)
                
                if allocated_usd == 0.0:
                    # First run bootstrap: Get total evaluation and divide by number of empty unallocated slots
                    logger.info("Initial bootstrap: Fetching total USD to divide among unallocated slots.")
                    total_usd = await asyncio.to_thread(kis_api.get_available_usd)
                    unallocated_slots = [k for k, v in state.get("slots", {}).items() if v.get("status") == "empty" and v.get("cash_balance", 0.0) == 0.0]
                    
                    if unallocated_slots:
                        allocated_usd = round(total_usd / len(unallocated_slots), 2)
                        # Pre-allocate to other empty slots so they don't recalculate
                        for k in unallocated_slots:
                            if k != empty_slot:
                                state["slots"][k]["cash_balance"] = allocated_usd        
                        await asyncio.to_thread(db_manager._save_state, state) # Save pre-allocations under the hood
                
                logger.info("Allocated USD for Slot %s: $%s", empty_slot, f"{allocated_usd:,.2f}")
                
                usd_per_etf = allocated_usd / len(selected)
                
                new_holdings = []
                total_spent = 0.0
                
                for entry in selected:
                    t = str(entry.get('Ticker', entry.get('ticker', '')))
                    n = str(entry.get('ETF Name', entry.get('name', '')))
                    
                    price = await get_current_price(t)
                    if price is None:
                        logger.warning("Price data unavailable for %s. Skipping buy.", t)
                        continue
                    # Apply a 3% cash buffer to mitigate gap-up opening prices exceeding available funds
                    shares_to_buy = int((usd_per_etf * 0.97) // price)
                    
                    if shares_to_buy > 0:
                        if kis_api.KIS_READY:
                            success = await asyncio.to_thread(kis_api.execute_kis_buy, t, shares_to_buy, price)
                            if not success:
                                logger.error("API buy failed for %s. Skipping DB update.", t)
                                continue
                        else:
                            success = True
                            logger.info("MOCK MODE: Simulated buy for %s.", t)
                            
                        logger.info("Executed buy for Slot %s - %s (%d shares @ $%s). Success: %s",
                                    empty_slot, t, shares_to_buy, price, success)
                        
                        actual_spent = shares_to_buy * price
                        total_spent += actual_spent
                        
                        new_holdings.append({
                            "ticker": t,
                            "name": n,
                            "shares": shares_to_buy,
                            "buy_price": price,
                            "status": "active"
                        })
                
                remaining_cash = round(allocated_usd - total_spent, 2)
                await asyncio.to_thread(db_manager.fill_slot, empty_slot, target_sell_date, new_holdings, today_str, initial_cash_balance=remaining_cash)
                
                logger.info("Bought %d ETFs into Slot %s. (Spent: $%s)", len(selected), empty_slot, round(total_spent, 2))
            else:
                logger.warning("No empty slot available to buy new ETFs!")
    else:
        logger.info("No ETFs passed screening.")

    logger.info("=== Filter Statistics ===")
    for k, v in filter_stats.items():
        logger.info("%s: %s", k, v)

    # Note: Holdings monitoring and stop-loss logic have been moved completely to `etf_monitoring.py`.
    # `etf_screening.py` only handles the selection process and purchasing of top momentum ETFs.

    # --- FINAL: Send log file via Telegram ---
    log_path = get_log_filepath()
    if bot and log_path:
        await send_telegram_document_async(log_path, caption=f"ETF Screening Log ({end_str})", bot=bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical("Unhandled exception: %s", e, exc_info=True)
        # Attempt to send log file even on crash
        if TOKEN and CHAT_ID:
            try:
                log_path = get_log_filepath()
                if log_path:
                    bot = telegram.Bot(token=TOKEN)
                    asyncio.run(send_telegram_document_async(log_path, caption=f"❌ ETF Screening CRASH Log", bot=bot))
            except Exception as inner_e:
                logger.error("Failed to send crash log via Telegram: %s", inner_e)
