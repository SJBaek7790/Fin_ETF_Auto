# Fin_ETFQualityTrend

An automated quantitative investment system running a **4-Slot Rotation** and **Stop-Loss/Duration Exit Strategy** via the **Korea Investment & Securities (KIS) API**.
This code is meant to run via Github Actions.

## System Architecture
This project divides capital into 4 "slots" and systematically manages them dynamically.

1. **ETF Screening (Every Thursday, 30 minutes before market close)** 
   - Uses `etf_screening.py`.
   - Searches for momentum US ETFs, scores them based on 3-month return rates (`RET3M`) and 60-day RSI (`EXRSI3M`). RET3M is better when higher, EXRSI3M is better when lower, avoiding overbought ETFs than benchmark.
   - Shortlists 5-10 ETFs via **Gemini AI** with macro sentiment.
   - Allocates the exact tracked slot `cash_balance` specifically to buy the shortlisted ETFs. ETFs are bought equally, using Market On Close limit orders via KIS.

2. **Daily Monitoring (Every Weekday Morning, 30 minutes before market open)**
   - Uses `etf_monitoring.py`.
   - Iterates through actively held ETF tickers across all slots.
   - If an ETF's price drops below its 120-Day moving average or drops below its price from 3 months prior, the **Stop-Loss** mechanism is triggered.
   - A KIS "Market On Open" sell order is placed immediately, preserving the generated cash inside that specific slot's tracker until its 4-week cycle clears.
   - Re-evaluates holding periods. If a slot hits its 4-week holding period, the **Time-Stop** mechanism is triggered, and the slot is cleared using Market On Open limit orders via KIS. Cash is stored in that slot's isolated ledger.

3. **Data Persistence**
   - Stores slot values, active shares, allocations, and isolated cash amounts inside `data/portfolio_state.json`.