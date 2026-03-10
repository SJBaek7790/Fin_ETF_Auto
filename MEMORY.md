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

## Active Task
- None.
