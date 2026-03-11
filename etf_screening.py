import os
import time
import json
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

from common import (
    send_telegram_message_async, send_telegram_document_async,
    get_etf_ticker_list_wrapper, get_etf_ohlcv_by_date_wrapper, get_etf_ticker_name_wrapper
)

import db_manager
import kis_api

warnings.filterwarnings('ignore')


# --- CONFIG ---
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

# Basic Settings
EXCLUDE_KEYWORDS = ['2X', '3X', '-1X', '-2X', '-3X', 'Ultra', 'Bull', 'Bear', 'Inverse', 'Short', 'VIX', 'ETN', 'Target', 'Duration']
MIN_AUM = 100
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

# --- TELEGRAM ---

async def send_screening_message(filter_stats, bot, gemini_status="Gemini selected ETFs successfully."):
    summary = (
        f"<b>✅ ETF Screening Complete</b>\n"
        f"Total Passed: {filter_stats['passed']} ETFs.\n"
        f"{gemini_status}"
    )
    return summary


# --- GEMINI SELECTION ---

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
DATA_DIR = 'data'

def select_etfs_with_gemini(df_report):
    """Uses Gemini to select unique ETFs from the screened report."""
    if df_report.empty:
        print("No ETFs to select from.")
        return []
    
    if not GEMINI_API_KEY:
        print("GEMINI_API_KEY not set. Falling back to top 7.")
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

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
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
        cleaned_text = response.text.strip().removeprefix('```json').removesuffix('```').strip()
        selected = json.loads(cleaned_text)
        
        if isinstance(selected, list):
            print(f"Gemini selected ETFs successfully.")
            for s in selected:
                print(f"  - {s['Ticker']} {s['ETF Name']}: {s.get('Reason', 'N/A')}")
            return selected
        else:
            print(f"Gemini returned unexpected format (len={len(selected) if isinstance(selected, list) else 'N/A'}). Falling back.")
            return _fallback_top7(df_report)
            
    except Exception as e:
        print(f"Gemini API error: {e}. Falling back to top 7.")
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
    print(f"Saved selected ETFs to {filename}")
    return filename


import db_manager

# --- HOLDINGS MONITOR ---

async def check_holdings_monitor(screened_df, min_max_stats, benchmark_ret, bot):
    """
    Checks current active holdings (from db_manager) against thresholds.
    Returns list of tickers that triggered alerts.
    """
    print("\n--- Starting Holdings Monitor ---")
    
    active_holdings = await asyncio.to_thread(db_manager.get_active_holdings_for_monitoring)
    if not active_holdings:
        print("No active holdings found in portfolio state.")
        return []

    alerts = []
    alert_tickers = []
    
    screened_map = {}
    if not screened_df.empty:
        screened_df_copy = screened_df.copy()
        screened_df_copy['Ticker'] = screened_df_copy['Ticker'].astype(str)
        screened_map = screened_df_copy.set_index('Ticker').to_dict('index')

    for h in active_holdings:
        ticker = str(h.get('ticker'))
        name = h.get('name')
        slot_key = h.get('slot')
        print(f"Checking holding: {name} ({ticker}) in Slot {slot_key}...")
        
        comp_score = None

        # 1. Try Lookup in screened results
        if ticker in screened_map:
            print("  -> Found in screened results (Passed filters).")
            row = screened_map[ticker]
            comp_score = row['Composite Score']
            
        else:
            # 2. Not in screened_df — fetch and calculate
            print("  -> Not in screened results. Fetching data...")
            data = await asyncio.to_thread(fetch_etf_data, ticker)
            if not data:
                print(f"  -> Insufficient data for {name}")
                continue 
            
            metrics = calculate_metrics(data, benchmark_ret)
            if not metrics:
                 print(f"  -> Could not calc metrics for {name}")
                 continue

            # Calculate Composite Score manually if stats exist
            if min_max_stats:
                try:
                    def normalize(val, col):
                        mn, mx = min_max_stats[col]['min'], min_max_stats[col]['max']
                        if mx == mn: return 0
                        if col == 'EXRSI3M':
                            return (mx - val) / (mx - mn) * 100
                        return (val - mn) / (mx - mn) * 100

                    s_ret3m = normalize(metrics['RET3M'], 'RET3M')
                    s_exrsi = normalize(metrics['EXRSI3M'], 'EXRSI3M')
                    
                    comp_score = round(np.mean([s_ret3m, s_exrsi]), 2)
                except Exception as e:
                    print(f"  -> normalization error: {e}")

        # Check Thresholds
        triggered = []
        if comp_score is not None and comp_score < 40:
             triggered.append(f"Composite Score {comp_score} < 40")
             
        if triggered:
            safe_name = escape(name)
            safe_ticker = escape(ticker)
            safe_triggered = [escape(t) for t in triggered]

            alerts.append(f"⚠️ <b>{safe_name}</b> ({safe_ticker}) [Slot {slot_key}]\n   " +"\n   ".join(safe_triggered))
            alert_tickers.append((ticker, slot_key, "Score < 40"))
            print(f"  -> ALERT: {triggered}")
        else:
            print(f"  -> OK (Comp: {comp_score})")

    if alerts:
        print("Holding alerts generated.")
    else:
        print("No alerts for holdings.")
    
    return alert_tickers, alerts

# --- MAIN ---

async def main():
    bot = telegram.Bot(token=TOKEN) if TOKEN and CHAT_ID else None
    
    if bot:
        await send_telegram_message_async("🚀 <b>ETF Screening Started</b>", bot)

    print("Screening ETFs...")
    
    # 0. Fetch Benchmark Data (SPY)
    print("Fetching Benchmark (SPY) data...")
    df_bm = await asyncio.to_thread(get_etf_ohlcv_by_date_wrapper, start_str, end_str, "SPY")
    if df_bm is None or df_bm.empty:
        msg = "CRITICAL: Failed to fetch Benchmark data. Aborting."
        print(msg)
        if bot:
            await send_telegram_message_async(f"❌ <b>ETF Screening Error</b>\n{msg}", bot)
        return

    df_bm = df_bm.sort_index()
    benchmark_ret = df_bm['종가'].pct_change().dropna()
    
    etf_tickers = await asyncio.to_thread(get_etf_ticker_list_wrapper, end_str)
    print(f"Found {len(etf_tickers)} ETF tickers for date {end_str}")
    results = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_single_etf, t, benchmark_ret): t for t in etf_tickers}
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
        print("\n=== Gemini ETF Selection ===")
        selected = select_etfs_with_gemini(df_final)
        
        # --- NEW DB MANAGER LOGIC ---
        await asyncio.to_thread(db_manager.init_state)
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        async def get_current_price(ticker):
            # Fetches current price using unauthenticated Yahoo Finance as fallback or KIS
            data = await asyncio.to_thread(fetch_etf_data, ticker)
            if data and not data['close'].empty:
                return round(float(data['close'].iloc[-1]), 2)
            return 1.0 # Failsafe
            
        sold_slots_msg = "" # Handled by monitor now. Keeps variable here so down-code string additions don't break.
            
            
        gemini_status = "Gemini selected ETFs successfully." if selected else "Gemini failed to select ETFs."
        if sold_slots_msg:
            gemini_status += sold_slots_msg
            
        summary_msg = await send_screening_message(filter_stats, bot, gemini_status)
        
        # --- Fetch Portfolio Metrics from db_manager ---
        metrics = await asyncio.to_thread(db_manager.calculate_portfolio_metrics)
        if metrics:
            metrics_msg = (
                f"\n\n<b>📊 Portfolio Performance</b>\n"
                f"• Total Value: ${metrics.get('current_value', 0):,.2f}\n"
                f"• Cumulative Return: {metrics.get('total_return_pct', 0):.2f}%\n"
                f"• CAGR: {metrics.get('cagr_pct', 0):.2f}%\n"
                f"• MDD: {metrics.get('mdd_pct', 0):.2f}%\n"
                f"• Current Drawdown: {metrics.get('current_dd_pct', 0):.2f}%"
            )
            summary_msg += metrics_msg
        
        if selected:
            # Save dated selection file
            json_path = save_selected_etfs(selected, end_str)
            
            # Send the JSON file via Telegram
            if bot:
                await send_telegram_document_async(json_path, caption=f"US ETF Selection Results ({end_str})", bot=bot)
            
            # 2. Buy into an empty slot
            empty_slot = await asyncio.to_thread(db_manager.get_empty_slot)
            if empty_slot:
                print(f"\n=== Buying into Slot {empty_slot} ===")
                target_sell_date = (datetime.now() + timedelta(days=28)).strftime("%Y-%m-%d")
                
                # Allocation Logic
                state = await asyncio.to_thread(db_manager.get_portfolio_state)
                slot_data = state.get("slots", {}).get(empty_slot, {})
                allocated_usd = slot_data.get("cash_balance", 0.0)
                
                if allocated_usd == 0.0:
                    # First run bootstrap: Get total evaluation and divide by number of empty unallocated slots
                    print("Initial bootstrap: Fetching total USD to divide among unallocated slots.")
                    total_usd = await asyncio.to_thread(kis_api.get_available_usd)
                    unallocated_slots = [k for k, v in state.get("slots", {}).items() if v.get("status") == "empty" and v.get("cash_balance", 0.0) == 0.0]
                    
                    if unallocated_slots:
                        allocated_usd = round(total_usd / len(unallocated_slots), 2)
                        # Pre-allocate to other empty slots so they don't recalculate
                        for k in unallocated_slots:
                            if k != empty_slot:
                                state["slots"][k]["cash_balance"] = allocated_usd        
                        await asyncio.to_thread(db_manager._save_state, state) # Save pre-allocations under the hood
                
                print(f"Allocated USD for Slot {empty_slot}: ${allocated_usd:,.2f}")
                
                usd_per_etf = allocated_usd / len(selected)
                
                new_holdings = []
                total_spent = 0.0
                
                for entry in selected:
                    t = str(entry.get('Ticker', entry.get('ticker', '')))
                    n = str(entry.get('ETF Name', entry.get('name', '')))
                    
                    price = await get_current_price(t)
                    # Apply a 3% cash buffer to mitigate gap-up opening prices exceeding available funds
                    shares_to_buy = int((usd_per_etf * 0.97) // price)
                    
                    if shares_to_buy > 0:
                        if kis_api.KIS_READY:
                            success = await asyncio.to_thread(kis_api.execute_kis_buy, t, shares_to_buy, price)
                            if not success:
                                print(f"API buy failed for {t}. Skipping DB update.")
                                continue
                        else:
                            success = True
                            print(f"MOCK MODE: Simulated buy for {t}.")
                            
                        print(f"Executed buy for Slot {empty_slot} - {t} ({shares_to_buy} shares @ ${price}). Success: {success}")
                        
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
                
                buy_msg = f"\n✅ Bought {len(selected)} ETFs into Slot {empty_slot}. (Spent: ${round(total_spent, 2)})"
                print(buy_msg)
                if bot:
                     await bot.send_message(chat_id=CHAT_ID, text=buy_msg)
            else:
                warn_msg = "\n⚠️ No empty slot available to buy new ETFs!"
                print(warn_msg)
                if bot:
                     await bot.send_message(chat_id=CHAT_ID, text=warn_msg)
            
            # Send Message 2 (list of selected ETFs)
            if bot:
                selected_list_str = "[" + ", ".join([f"{s.get('Ticker', s.get('ticker'))} {s.get('ETF Name', s.get('name'))}" for s in selected]) + "]"
                await bot.send_message(chat_id=CHAT_ID, text=selected_list_str)
    else:
        print("No ETFs passed screening.")
        summary_msg = f"<b>✅ ETF Screening Complete</b>\nTotal Passed: 0 ETFs.\nNo ETFs to select."

    print("\n=== Filter Statistics ===")
    for k, v in filter_stats.items():
        print(f"{k}: {v}")

    # --- STEP 3: Holdings Monitor (from DB Manager) ---
    alert_tickers, alerts = await check_holdings_monitor(df, min_max_stats, benchmark_ret, bot)

    # Append alerts to Message 1 and send
    if bot:
        if alerts:
            summary_msg += "\n" + "\n".join(alerts)
        else:
            summary_msg += "\nNo structural alerts for holdings."
        
        await bot.send_message(chat_id=CHAT_ID, text=summary_msg, parse_mode='HTML')

    # --- STEP 4: Trigger Stop-Loss on alerted ETFs ---
    if alert_tickers:
        print(f"\n=== Auto-Sell: Triggering stop loss for {len(alert_tickers)} ETFs ===")
        # Get active holdings so we can find shares
        active_holdings = await asyncio.to_thread(db_manager.get_active_holdings_for_monitoring)
        
        for ticker, slot_key, reason in alert_tickers:
            shares_to_sell = 0
            for h in active_holdings:
                if h.get('ticker') == ticker and h.get('slot') == slot_key:
                    shares_to_sell = int(h.get('shares', 0))
                    break
                    
            curr_price = await get_current_price(ticker)
            
            if kis_api.KIS_READY and shares_to_sell > 0:
                sell_success = await asyncio.to_thread(kis_api.execute_kis_sell, ticker, shares_to_sell, curr_price)
                if not sell_success:
                    print(f"API stop-loss sell failed for {ticker}. Skipping DB update.")
                    continue
                else:
                    print(f"Executed stop-loss sell for {ticker} ({shares_to_sell} shares).")
            else:
                print(f"MOCK MODE: Simulated stop-loss sell for {ticker}.")

            await asyncio.to_thread(db_manager.trigger_stop_loss, slot_key, ticker, reason, curr_price, shares_to_sell)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Unhandled exception: {e}")
        if TOKEN and CHAT_ID:
            try:
                bot = telegram.Bot(token=TOKEN)
                asyncio.run(send_telegram_message_async(f"❌ <b>ETF Screening Critical Error</b>\n<pre>{escape(str(e))}</pre>", bot))
            except Exception as inner_e:
                print(f"Failed to send error telegram message: {inner_e}")


