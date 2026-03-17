# Fin_ETFQualityTrend Memory
## Current Architecture
The project runs a 4-Slot Rotation investment system.
- `etf_screening.py` / `etf_screening_old.py`: Screens ETFs and selects via Gemini
- `etf_monitoring.py`: Daily monitoring for stop-loss
- Data persistence via json. Python scripts interact with KIS API.

## Completed Tasks
- [x] Refactor `etf_screening_old.py` to remove obsolete codes.
- [x] Remove `country` arguments; standardize to US ETFs across the project.
- [x] Extract KIS API functions into `kis_api.py`.
- [x] Move slot clearing logic from screening script to `etf_monitoring.py`.
- [x] Setup architecture mock mapping in `portfolio_state.json`.
- [x] Implement Hybrid Portfolio Approach (`portfolio_state.json` + `trade_history.json`).
- [x] Fix State-Execution Desync bug: `db_manager` updates are now strictly gated by `kis_api` execution success in `etf_monitoring.py` and `etf_screening.py`.
- [x] Implement atomic writes for JSON data files (`portfolio_state.json`, `trade_history.json`, `portfolio_value_history.json`) in `db_manager.py` to prevent file corruption.
- [x] Create a new GitHub repository for `Fin_ETF_Auto` and upload the entire codebase.
- [x] Fix Hardcoded Limit Price Risks: Using Market On Open (MOO) for sales and Limit On Close (LOC) for buys on Real environments.
- [x] Implemented delisted ETF protection: Tracks consecutive `None` data days in `portfolio_state.json` and issues emergency Telegram alerts after 3 days to prevent slot lock-up.
- [x] Removed all mock environment and paper trading conditionals from `kis_api.py`, enforcing permanent real trading execution logic.
- [x] Fixed Gemini LLM JSON parsing crashes in `etf_screening.py` by pre-processing and stripping hallucinatory markdown wrappers (````json ````).
- [x] Fixed Telegram Async/Sync Mixups by wrapping blocking I/O calls inside `etf_screening.py` with `asyncio.to_thread`.
- [x] Implemented 3% cash buffer for new ETF purchases in `etf_screening.py` (`(usd_per_etf * 0.97) // price`) to prevent insufficient USD errors on gap-ups.
- [x] Implemented Order Reconciliation Job: `etf_monitoring.py` now fetches true KIS balances (`get_kis_holdings`) and corrects `portfolio_state.json` downward for failed/partial limit orders, refunding unspent cash to the slot.
- [x] Implemented strict validation filter rejecting Gemini-hallucinated tickers not present in the pre-screened universe in `etf_screening.py`.
- [x] Researched and verified KIS Overseas Master file parsing logic (Standard Library version) for dynamic Exchange Code lookup.
- [x] Implemented Dynamic Exchange Lookup: `kis_api.py` now downloads/caches KIS master files daily to `data/kis_master/us_ticker_exchange_map.json` (12,000+ tickers). `get_exchange_code(ticker)` resolves `NASD`/`NYSE`/`AMEX` dynamically. Falls back to `NASD` for unmapped tickers. Note: KIS classifies NYSE Arca ETFs (SPY, VOO, IWM, GLD, etc.) under `AMEX`.
- [x] Removed all dangerous `else 1.0` / `return 1.0` fallback prices from `etf_monitoring.py` (3 sites) and `etf_screening.py` (1 `get_current_price` + 2 call sites). All replaced with explicit `if price is None: continue` guards that skip the trade entirely.
- [x] Integrated `exchange_calendars` library (NYSE / XNYS) in `common.py::is_us_market_open_today()`. Both `etf_screening.py` and `etf_monitoring.py` now exit early on non-trading days (weekends, US holidays). Package installed to `.local_deps/` with `sys.path` auto-discovery.
- [x] Replaced all `print()` calls with Python `logging` module across the entire codebase. Created `log_config.py` with `TimedRotatingFileHandler` (daily rotation, 30-day retention, `logs/` directory). All modules use `logger = logging.getLogger(__name__)` with level-based filtering (DEBUG/INFO/WARNING/ERROR/CRITICAL). Removed all inline Telegram messages; each entry point (`etf_screening.py`, `etf_monitoring.py`) now sends a single log file via Telegram at end of run. Added `send_telegram_document_sync()` to `common.py` for synchronous log file sends.


## Active Task
(none)
