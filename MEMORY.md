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

- [x] Bugfix (2026-04-10): Fix `etf_screening.py` empty-slot corruption + misleading Telegram message
  - **Root Cause**: When all KIS buy orders failed (likely due to pre-market timing: 07:00 KST, before KRX 08:00 pre-market), `fill_slot()` was still called with empty `new_holdings`, leaving Slot 2 as `"invested"` with zero holdings — a corrupted state.
  - **Fix 1**: Guard `fill_slot()` call — only invoke when `new_holdings` is non-empty. If all buys fail, slot stays `"empty"`.
  - **Fix 2**: Telegram notification now distinguishes three states: successful buy, all-buys-failed, and no-slot-available.
  - **Fix 3**: Added diagnostic logging to `kis_api.py` `execute_kis_buy` to capture actual API response on failure (previously silent `False`).
  - **Fix 4**: Manually repaired `portfolio_state.json` Slot 2 from corrupted `"invested"/empty-holdings` back to `"empty"`.

- [x] Consolidate GitHub Actions Cron Design (2026-04-10)
  - Deleted `.github/workflows/screening.yml`.
  - Updated `.github/workflows/monitoring.yml` to handle both daily monitoring and weekly screening in a single workflow.
  - Added bash condition in `monitoring.yml` to dynamically execute `etf_screening.py` with a 10-minute wait (`sleep 600`) if the day is Friday.

- [x] Bugfix (2026-04-11): Fix outdated unit test assertion in `test_db_manager.py`
  - Updated `test_zero_actual_marks_corporate_action` to assert `failed_buy` instead of `Corporate Action Suspected` to reflect recent core logic patch.
  - Added new `test_large_drop_marks_corporate_action` to preserve the >50% drop logic test case.

- [x] Refactor (2026-04-13): Separated Signal Generation from Order Execution
  - **Architecture**: Decoupled `etf_monitoring.py` and `etf_screening.py` into distinct signal (`monitor.py`, `screen.py`) and execution (`order_placement.py`) stages to make trade generation purely functional and testable without active KIS contexts.
  - **State Transfer**: Components now communicate via atomic `temp` file swaps of `data/pending_orders.json`.
  - **Overlap Netting**: `screen.py` proactively cancels BUY/SELL pairs and trades only the difference for any identical ticker across reallocations to limit unnecessary trading fees.
  - **Backwards Compatibility**: Rewrote `etf_monitoring.py` and `etf_screening.py` as mere `subprocess` wrappers executing the decoupled pipeline stages in correct sequence.

- [x] Sync (2026-04-14): Push decoupled architecture to GitHub
  - Staged new modules: `monitor.py`, `screen.py`, `order_placement.py`.
  - Updated wrappers: `etf_monitoring.py`, `etf_screening.py`.
  - Force-synced `data/portfolio_state.json` to maintain state persistence.

- [x] Bugfix (2026-04-17): Fix `UnboundLocalError: cannot access local variable 'asyncio'` in `screen.py`
  - **Root Cause**: A stray `import asyncio` inside `main()` at line 459 shadowed the module-level import, causing Python to treat `asyncio` as a local variable throughout the entire function scope, crashing every earlier `asyncio.to_thread()` call.
  - **Fix**: Removed the in-function `import asyncio`. Added missing `import telegram` at module level (it was previously imported only inside the `except` block of `__main__`).

- [x] Bugfix (2026-04-17): Fix `monitor.py` double-execution on Fridays
  - **Root Cause**: `etf_screening.py`'s `main()` listed `["monitor.py", "screen.py"]`. On Fridays the workflow runs `etf_monitoring.py` (→ `monitor.py` + `order_placement.py`) first, then `etf_screening.py` (→ `monitor.py` again + `screen.py` + `order_placement.py`). The second `monitor.py` run cleared/overwrote `pending_orders.json`, feeding stale/empty sell data into `screen.py`.
  - **Fix**: `etf_screening.py` now only runs `screen.py` + `order_placement.py`. `monitor.py` is intentionally excluded from the screening wrapper.

- [x] Manual Sync (2026-04-18): Synchronized data files with GitHub via backend tools
  - **Issue**: Local shell environment has restricted network access (`Operation not permitted`), preventing standard `git pull`.
  - **Action**: Verified remote state via backend; manually synchronized `data/portfolio_state.json`, `data/portfolio_value_history.json`, and `data/trade_history.json` with the latest commit on GitHub (`ba539c4`).
  - **Result**: Local state now reflects the latest portfolio updates from GitHub Actions (Slot 3 & 4 reallocation on 2026-04-17).

- [x] Bugfix (2026-04-18): Fix `screen.py` filling multiple empty slots per screening cycle
  - **Root Cause**: The buy-order generation loop at line 379 iterated over **all** entries in `will_be_free_slots`. When both slot 3 and slot 4 were empty, the same 3 Gemini-selected ETFs were duplicated into both slots — producing identical holdings.
  - **Fix**: Added a guard after building `will_be_free_slots` that limits it to **one** slot per cycle (lowest-numbered). This matches the 4-slot / 28-day rotation design where one slot should be filled per weekly run.

- [x] Bugfix (2026-04-18): Restore `selected_etfs_YYYYMMDD_kr.json` generation
  - **Issue**: The `save_selected_etfs` logic was lost during the 2026-04-13 decoupling refactor, causing screening runs to stop logging the Gemini selection results.
  - **Fix**: Re-implemented `save_selected_etfs` in `screen.py` and added an `asyncio.to_thread` call in the `main` loop.
  - **Cleanup**: Purged dead code and redundant logic from `etf_screening.py`, leaving it as a pure pipeline wrapper.

## Active Task
(none)
