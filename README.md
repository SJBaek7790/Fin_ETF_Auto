# Fin_ETF_Auto (Korean Domestic ETFs)

An automated quantitative investment system running a **4-Slot Rotation** and **Stop-Loss/Duration Exit Strategy** for **Korea-listed ETFs** via the **Korea Investment & Securities (KIS) Domestic Stock API**.

This code is meant to run via GitHub Actions.

## System Architecture
This project divides capital (₩10,000,000 KRW) into 4 "slots" and systematically manages them dynamically.

1. **ETF Screening (Weekly)**
   - Uses `etf_screening.py`.
   - Searches for momentum Korean ETFs, scores them based on 3-month return rates (`RET3M`) and 60-day RSI (`EXRSI3M`). RET3M is better when higher, EXRSI3M is better when lower, avoiding overbought ETFs relative to the KODEX 200 benchmark.
   - Shortlists 3 ETFs via **Gemini AI** with macro sentiment validation.
   - Allocates the tracked slot `cash_balance` to buy the shortlisted ETFs. ETFs are bought equally using limit orders via KIS domestic stock API.

2. **Daily Monitoring (Every Weekday)**
   - Uses `etf_monitoring.py`.
   - Iterates through actively held ETF tickers across all slots.
   - If an ETF's price drops below its 120-Day moving average or drops below its price from 3 months prior, the **Stop-Loss** mechanism is triggered.
   - A KIS limit sell order is placed, preserving the generated cash inside that specific slot's tracker until its 4-week cycle clears.
   - Re-evaluates holding periods. If a slot hits its 4-week holding period, the **Time-Stop** mechanism is triggered, and the slot is cleared. Cash is stored in that slot's isolated ledger.

3. **Data Persistence**
   - Stores slot values, active shares, allocations, and isolated cash amounts inside `data/portfolio_state.json`.

## Key Parameters
- **Benchmark**: KODEX 200 (`069500`)
- **Starting Capital**: ₩10,000,000 KRW
- **Slots**: 4
- **Holding Period**: 28 days (4 weeks)
- **Gemini Selection**: 3 ETFs per slot
- **Min Avg Trading Value**: ₩1,000,000,000 KRW