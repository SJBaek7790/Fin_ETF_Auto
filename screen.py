"""
ETF Screening — Signal Generator (Separate execution)

Screens Korea-listed ETFs using momentum + excess RSI, uses Gemini AI for selection.
Determines free slots, calculates budget, generates BUY orders, and applies NETTING with pending SELL orders.
Writes final pending orders (SELL + BUY) to data/pending_orders.json. 
Does NOT execute trades directly.
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
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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

warnings.filterwarnings('ignore')
logger = get_logger(__name__)

# --- CONFIG ---
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

EXCLUDE_KEYWORDS = [
    '2X', '3X', '-1X', '-2X', '-3X',
    'Ultra', 'Bull', 'Bear', 'Inverse', 'Short', 'VIX', 'ETN',
    'Target', 'Duration',
    '레버리지', '인버스', '곱버스', '선물', '2배', '3배',
    '숏', '베어', '불',
]
MIN_AVG_TRADING_KRW = 1_000_000_000
STARTING_CAPITAL_KRW = 10_000_000
EPSILON = 1e-8
BENCHMARK_TICKER = "069500"
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
DATA_DIR = 'data'

# --- CORE LOGIC (Verbatim from etf_screening.py except no executing) ---

def calculate_rsi(returns, period=14):
    delta = returns
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(com=period-1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_etf_data(ticker, start_str, end_str):
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
    close = data['close']
    if len(close) < 60:
        return None
    ret_3m = ((close.iloc[-1] - close.iloc[-60]) / close.iloc[-60]) * 100
    etf_ret = close.pct_change()
    df_aligned = pd.DataFrame({'ETF': etf_ret, 'BM': benchmark_ret}).dropna()
    if len(df_aligned) < 60:
         return None
    df_aligned['Excess'] = df_aligned['ETF'] - df_aligned['BM']
    rsi_series = calculate_rsi(df_aligned['Excess'], period=60)
    ex_rsi_3m = rsi_series.iloc[-1]
    if len(rsi_series) == 0 or np.isnan(rsi_series.iloc[-1]) or pd.isna(ex_rsi_3m):
        return None
    return {'RET3M': round(ret_3m, 2), 'EXRSI3M': round(ex_rsi_3m, 2)}

def process_single_etf(ticker, benchmark_ret, start_str, end_str):
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

def select_etfs_with_gemini(df_report):
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
                                "Ticker": types.Schema(type=types.Type.STRING),
                                "ETF Name": types.Schema(type=types.Type.STRING),
                                "Reason": types.Schema(type=types.Type.STRING)
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
                return _fallback_top3(df_report)
            time.sleep(2 ** attempt)

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
                logger.warning("Gemini hallucinated ticker '%s'. Rejecting.", ticker)
        selected = validated_selected
        if not selected:
            return _fallback_top3(df_report)
        return selected
    else:
        return _fallback_top3(df_report)

def _fallback_top3(df_report):
    top3 = df_report.head(3)
    return [{'Ticker': str(row['Ticker']), 'ETF Name': row['ETF Name'], 'Reason': 'Fallback'} for _, row in top3.iterrows()]


async def main():
    setup_logging("screening")
    
    if not is_kr_market_open_today():
        logger.info("KRX market is closed today. Skipping screening.")
        return

    # Read pending orders from monitor.py
    logger.info("Reading pending_orders.json from monitor stage...")
    pending_data = await asyncio.to_thread(db_manager.read_and_clear_pending_orders)
    
    if pending_data and pending_data.get('orders'):
        pending_sells = pending_data['orders']
        mode = pending_data.get('mode', 'screening')
    else:
        pending_sells = []
        mode = "screening"
        
    state = await asyncio.to_thread(db_manager.get_portfolio_state)
    if not state:
        state = {"slots": {str(i): {"status": "empty", "cash_balance": 0.0} for i in range(1, 5)}}

    # Evaluate which slots will be free
    will_be_free_slots = {}  # slot_key -> budget
    sell_orders_by_slot = {}
    for o in pending_sells:
        k = str(o['slot'])
        if k not in sell_orders_by_slot:
            sell_orders_by_slot[k] = []
        sell_orders_by_slot[k].append(o)
        
    today_obj = datetime.now(tz=ZoneInfo("Asia/Seoul"))
    today_str = today_obj.strftime("%Y-%m-%d")

    logger.info("Determining slots available for reallocation...")
    for slot_key, slot_data in state.get('slots', {}).items():
        st = slot_data.get('status', 'empty')
        cash_bal = slot_data.get('cash_balance', 0.0)
        
        if st == 'empty':
            # Bootstrapping for total structural emptiness
            b_val = cash_bal
            if b_val == 0.0:
                 # Check if the entire portfolio has 0 cash balance across all slots
                 total_cash = sum(s.get('cash_balance', 0.0) for s in state.get('slots', {}).values())
                 if total_cash == 0.0:
                     b_val = STARTING_CAPITAL_KRW / 4.0
            will_be_free_slots[slot_key] = b_val
            logger.info("Slot %s is already empty. Usable Budget: ₩%s", slot_key, f"{b_val:,.0f}")
        else:
             # invested slot
             active_holdings = [h for h in slot_data.get('holdings', []) if h.get('status') == 'active']
             active_tickers = {str(h['ticker']): int(h['shares']) for h in active_holdings}
             selling_tickers = {}
             for so in sell_orders_by_slot.get(slot_key, []):
                 t = str(so['ticker'])
                 selling_tickers[t] = int(so.get('shares', 0))
                 
             target_date_str = slot_data.get('target_sell_date')
             is_target_reached = False
             if target_date_str:
                 td_obj = datetime.strptime(target_date_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo("Asia/Seoul"))
                 if today_obj.date() >= td_obj.date():
                     is_target_reached = True
                     
             all_sold = True
             for t, shrs in active_tickers.items():
                 if t not in selling_tickers or selling_tickers[t] < shrs:
                     all_sold = False
                     break
             
             if all_sold or is_target_reached:
                 # Slot will be free
                 proceeds = 0.0
                 for so in sell_orders_by_slot.get(slot_key, []):
                     proceeds += so.get('shares', 0) * so.get('estimated_price', 0.0)
                 
                 budget = cash_bal + proceeds
                 will_be_free_slots[slot_key] = budget
                 logger.info("Slot %s will be freed. Expected budget: ₩%s", slot_key, f"{budget:,.0f}")

    if not will_be_free_slots:
        logger.info("No free slots for reallocation. Screening aborted.")
        # Need to restore the pending sells we popped!
        db_manager.write_pending_orders(mode="screening", orders=pending_sells)
        return
        
    # Standard Screening
    end_date = datetime.now(tz=ZoneInfo("Asia/Seoul"))
    start_date = end_date - timedelta(days=200)
    end_str = end_date.strftime('%Y%m%d')
    start_str = start_date.strftime('%Y%m%d')

    logger.info("Fetching Benchmark (%s) data...", BENCHMARK_TICKER)
    df_bm = await asyncio.to_thread(get_etf_ohlcv_by_date_wrapper, start_str, end_str, BENCHMARK_TICKER)
    if df_bm is None or df_bm.empty:
        logger.critical("Failed to fetch Benchmark data. Aborting.")
        db_manager.write_pending_orders(mode="screening", orders=pending_sells)
        return

    df_bm = df_bm.sort_index()
    benchmark_ret = df_bm['close'].pct_change().dropna()
    
    etf_tickers = await asyncio.to_thread(get_etf_ticker_list_wrapper, end_str)
    results = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_single_etf, t, benchmark_ret.copy(), start_str, end_str): t for t in etf_tickers}
        for future in as_completed(futures):
            res, _ = future.result()
            if res: results.append(res)
            
    df_final = pd.DataFrame()
    if results:
        df = pd.DataFrame(results)
        cols = ['RET3M', 'EXRSI3M']
        for col in cols:
            mn = df[col].min()
            mx = df[col].max()
            rng = mx - mn
            if rng < EPSILON: df[f'S_{col}'] = 50.0
            elif col == 'EXRSI3M': df[f'S_{col}'] = (mx - df[col]) / rng * 100
            else: df[f'S_{col}'] = (df[col] - mn) / rng * 100
        
        df = df.rename(columns={'S_RET3M': 'RET3M Score', 'S_EXRSI3M': 'EXRSI3M Score'})
        df['Composite Score'] = df[['RET3M Score', 'EXRSI3M Score']].mean(axis=1).round(2)
        df_sorted = df.sort_values('Composite Score', ascending=False).reset_index(drop=True)
        df_final = df_sorted[['Ticker', 'ETF Name', 'Avg Trading Value (KRW)', 'RET3M', 'RET3M Score', 'EXRSI3M', 'EXRSI3M Score', 'Composite Score']].head(50)
        
    logger.info("=== Gemini ETF Selection ===")
    selected = select_etfs_with_gemini(df_final)
    
    # Assign new ETFs to free slots
    buy_orders = []
    if selected:
        for sk, budget in will_be_free_slots.items():
            krw_per_etf = budget / len(selected)
            for entry in selected:
                t = str(entry.get('Ticker', entry.get('ticker', '')))
                n = str(entry.get('ETF Name', entry.get('name', '')))
                
                # We need estimated price. We can fetch using fetch_etf_data
                data = await asyncio.to_thread(fetch_etf_data, t, start_str, end_str)
                if data and not data['close'].empty:
                    est_price = int(float(data['close'].iloc[-1]))
                    # Apply 3% cash buffer per the original logic
                    shares_to_buy = int((krw_per_etf * 0.97) // est_price)
                    if shares_to_buy > 0:
                        buy_orders.append({
                            "action": "BUY",
                            "ticker": t,
                            "slot": str(sk),
                            "shares": shares_to_buy,
                            "estimated_price": est_price,
                            "name": n,
                            "budget": krw_per_etf,
                            "target_sell_date": (datetime.now(tz=ZoneInfo("Asia/Seoul")) + timedelta(days=28)).strftime("%Y-%m-%d")
                        })

    # OVERLAP NETTING
    logger.info("=== Overlap Netting ===")
    sells_to_keep = list(pending_sells)
    buys_to_keep = list(buy_orders)
    
    for sk in will_be_free_slots.keys():
        sells_in_slot = [so for so in sells_to_keep if str(so['slot']) == str(sk)]
        buys_in_slot = [bo for bo in buys_to_keep if str(bo['slot']) == str(sk)]
        
        for bo in list(buys_in_slot):  # iterate on copy since we mutate
            matched_sell = next((so for so in sells_in_slot if str(so['ticker']) == str(bo['ticker'])), None)
            if matched_sell:
                s_shares = matched_sell['shares']
                b_shares = bo['shares']
                t = bo['ticker']
                
                if s_shares == b_shares:
                    logger.info("Netting %s in Slot %s: sell %d / buy %d -> net zero! Removing both.", t, sk, s_shares, b_shares)
                    sells_to_keep.remove(matched_sell)
                    buys_to_keep.remove(bo)
                elif s_shares > b_shares:
                    diff = s_shares - b_shares
                    logger.info("Netting %s in Slot %s: sell %d / buy %d -> net SELL %d shares.", t, sk, s_shares, b_shares, diff)
                    matched_sell['shares'] = diff
                    buys_to_keep.remove(bo)
                elif b_shares > s_shares:
                    diff = b_shares - s_shares
                    logger.info("Netting %s in Slot %s: sell %d / buy %d -> net BUY %d shares.", t, sk, s_shares, b_shares, diff)
                    bo['shares'] = diff
                    sells_to_keep.remove(matched_sell)

    # Combine and write
    combined_orders = sells_to_keep + buys_to_keep
    if combined_orders:
        s_count = sum(1 for o in combined_orders if o['action'] == 'SELL')
        b_count = sum(1 for o in combined_orders if o['action'] == 'BUY')
        logger.info("Final pending orders after netting: %d SELLs, %d BUYs.", s_count, b_count)
        db_manager.write_pending_orders(mode="screening", orders=combined_orders)
    else:
        logger.info("No orders remain after netting. Pending orders cleared.")
        db_manager.write_pending_orders(mode="screening", orders=[])

    # Telegram notification
    msg = f"📊 ETF Screening & Netting ({today_str})\n"
    msg += f"Free Slots Reallocated: {len(will_be_free_slots)}\n"
    if selected:
         msg += f"Selected (Gemini): {', '.join([s.get('ETF Name', '?') for s in selected])}\n"
    
    if combined_orders:
        sell_tickers = set([o['ticker'] for o in combined_orders if o['action'] == 'SELL'])
        buy_tickers = set([o['ticker'] for o in combined_orders if o['action'] == 'BUY'])
        
        msg += f"Net Pending BUYs: {len(buy_tickers)}\n"
        msg += f"Net Pending SELLs: {len(sell_tickers)}"
    else:
        msg += "No pending orders."
    
    bot = telegram.Bot(token=TOKEN) if TOKEN and CHAT_ID else None
    if bot:
        await send_telegram_message_async(msg, bot=bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical("Unhandled exception in screen.py: %s", e, exc_info=True)
        try:
            import telegram
            bot = telegram.Bot(token=TOKEN) if TOKEN and CHAT_ID else None
            if bot:
                asyncio.run(send_telegram_message_async(f"❌ Screen CRASH\n{e}", bot=bot))
        except:
             pass
