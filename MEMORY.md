# Fin_ETF_Auto Memory (Korean Domestic ETFs)

## Current Architecture
The project runs a 4-Slot Rotation investment system for **Korea-listed ETFs**.
- `etf_screening.py`: Screens Korean ETFs, scores via RET3M + EXRSI3M, selects 3 via Gemini AI, executes KIS domestic buy orders.
- `etf_monitoring.py`: Daily monitoring for stop-loss (120MA / 3-month momentum) and time-stop (28-day cycle), executes KIS domestic sell orders.
- `kis_api.py`: KIS domestic stock API wrapper using `order_cash()` and `inquire_balance()` from the open-trading-api SDK.
- `common.py`: FinanceDataReader wrappers for Korean ETFs (`ETF/KR`), yfinance fallback with `.KS` suffix, KRX market calendar (`XKRX`).
- `db_manager.py`: JSON-based portfolio state management with 4-slot system, file locking, atomic writes.
- Data persistence via JSON. Python scripts interact with KIS Domestic Stock API.

## Key Parameters
- **Benchmark**: KODEX 200 (`069500`)
- **Starting Capital**: ₩10,000,000 KRW
- **Slots**: 4
- **Holding Period**: 28 days (4 weeks)
- **Gemini Selection**: 3 ETFs per slot
- **Min Avg Trading Value**: ₩1,000,000,000 KRW
- **Exclude Keywords**: 2X, 3X, Inverse, Short, VIX, ETN, 레버리지, 인버스, 곱버스, 선물, etc.

## Completed Tasks (Legacy — US ETF era)
- [x] All previous US ETF tasks (see git history)

## Completed Tasks (Current — Korea ETF pivot)
- [x] Major pivot from US ETFs to Korea-listed ETFs (2026-03-30)
  - Rewrote `kis_api.py` for KIS domestic stock API (`order_cash`, `inquire_balance`)
  - Rewrote `common.py` (ETF/KR listings, yfinance `.KS` fallback, XKRX calendar)
  - Rewrote `etf_screening.py` (KODEX 200 benchmark, ₩10M capital, 3 ETFs, Korean keywords, Gemini prompt for Korean market)
  - Rewrote `etf_monitoring.py` (KRX calendar, KIS domestic sell, KRW currency)
  - Updated `db_manager.py` currency formatting (USD → KRW, round to 0 decimals)
  - Deleted `verify_exchanges.py` (US exchange lookup utility)
  - Deleted `data/kis_master/` (US ticker-exchange mapping cache)
  - Updated all tests to use Korean ETF tickers (069500, 233740)
  - Reset `portfolio_state.json` to fresh empty state
  - Updated README.md, .gitignore, MEMORY.md
- [x] Code review fixes implementation plan (Issue #26 CI persistence, locking fixes, API resilience, RSI docstrings)
- [x] KIS API Audit & Alignment (2026-03-30)
  - Fixed `get_total_portfolio_value()` double-counting cash bug.
  - Fixed swapped `output1` / `output2` reading in `inquire_balance` consumers.
  - Explicitly specified `sll_type="01"` for KIS sell orders.
  - Set default `KIS_MODE` to `prod` in `auth()`.

- [x] GitHub Actions Environment Fixes (2026-04-01)
  - Fixed `FinanceDataReader` pip resolution issue.
  - Corrected monitoring cron schedule to 10:00 AM KST.
  - Supressed Node 20 GitHub Action deprecation warnings (`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`).
  - Set `if-no-files-found: ignore` on log artifacts to suppress upload warnings.

- [x] CI Bug Fixes — Screening Workflow (2026-04-03)
  - Added `pyyaml` to `requirements.txt` (KIS SDK depends on `yaml` module).
  - Fixed `git add data/*.json` → `git add -f data/*.json` in `screening.yml` and `monitoring.yml` (data/*.json is in `.gitignore`).

- [x] Fix Crypto module missing error by adding `pycryptodome` to `requirements.txt`
- [x] Change log format output from `.log` to `.txt` to allow viewing on mobile (via `log_config.py` updates)

- [x] Fix FileNotFoundError for `kis_devlp.yaml` by injecting a cat generator into the GitHub Actions runner workflows using Secrets.

- [x] Bugfix (2026-04-09): Ensure `etf_monitoring.py` always logs daily portfolio value even if there are no active holdings (100% cash portfolio).
- [x] Bugfix (2026-04-09): Suppress normal "All clear" Telegram messages; notify only on alerts or crashes.
- [x] Bugfix (2026-04-09): Refactor `db_manager.py` reconcile logic to aggregate ETF expectations across all slots, preventing database inflation when the same ETF is held in multiple slots.
- [x] Architectural logging: Updated discrepancy handling to deduct shortfalls from the most recently purchased slots first and add overages to the oldest slot tracking that ticker.

## Active Task
(none)
