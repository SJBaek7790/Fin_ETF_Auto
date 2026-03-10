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

## Active Task
- Create a new GitHub repository for `Fin_ETF_Auto` and upload the entire codebase.
  - Plan pending user approval: Initialize localized git repository, create `.gitignore`, install `gh` CLI or use manual remote URL, and push to main.
