import os
import json
import logging
import pandas as pd
from datetime import datetime
import fcntl
import contextlib
import threading
import functools

logger = logging.getLogger(__name__)

DATA_DIR = 'data'
STATE_FILE = os.path.join(DATA_DIR, 'portfolio_state.json')
VALUE_HISTORY_FILE = os.path.join(DATA_DIR, 'portfolio_value_history.json')
PORTFOLIO_LOCK_FILE = os.path.join(DATA_DIR, 'portfolio.lock')

_lock_local = threading.local()

@contextlib.contextmanager
def portfolio_lock():
    """
    Acquires an exclusive file lock on the portfolio state.
    Requires UNIX/Linux due to `fcntl` usage. Will not work on Windows.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    if not hasattr(_lock_local, 'lock_count'):
        _lock_local.lock_count = 0
        _lock_local.lock_fd = None

    if _lock_local.lock_count == 0:
        _lock_local.lock_fd = open(PORTFOLIO_LOCK_FILE, 'w')
        fcntl.flock(_lock_local.lock_fd, fcntl.LOCK_EX)
    
    _lock_local.lock_count += 1
    try:
        yield
    finally:
        _lock_local.lock_count -= 1
        if _lock_local.lock_count == 0:
            fcntl.flock(_lock_local.lock_fd, fcntl.LOCK_UN)
            if _lock_local.lock_fd:
                _lock_local.lock_fd.close()
            _lock_local.lock_fd = None

def with_portfolio_lock(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with portfolio_lock():
            return func(*args, **kwargs)
    return wrapper

def init_state():
    """Initializes the portfolio state file if it doesn't exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(STATE_FILE):
        initial_state = {
            "slots": {
                "1": {"status": "empty"},
                "2": {"status": "empty"},
                "3": {"status": "empty"},
                "4": {"status": "empty"}
            }
        }
        _save_state(initial_state)

def _load_state():
    """Loads the portfolio state from JSON."""
    init_state()
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error("Error loading state: %s", e)
        return None

def _save_state(state):
    """Saves the given portfolio state to JSON."""
    try:
        temp_file = STATE_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, STATE_FILE)
        return True
    except Exception as e:
        logger.error("Error saving state: %s", e)
        return False

@with_portfolio_lock
def save_portfolio_state_locked(state):
    """Public wrapper to save state safely with lock."""
    return _save_state(state)



def get_portfolio_state():
    """Returns the current portfolio state map."""
    return _load_state()

def get_empty_slot():
    """Finds the first available empty slot. Returns slot key (e.g. '1') or None."""
    state = _load_state()
    if not state:
        return None
    for key, slot_data in state.get('slots', {}).items():
        if slot_data.get('status') == 'empty':
            return key
    return None

@with_portfolio_lock
def fill_slot(slot_key, target_sell_date, holdings, buy_date=None, initial_cash_balance=0.0):
    """
    Fills an empty slot with selected ETFs.
    holdings format: list of dicts [{'ticker': '069500', 'name': 'KODEX 200', 'shares': 10, 'buy_price': 500.0, 'status': 'active'}, ...]
    """
    state = _load_state()
    if not state or slot_key not in state.get('slots', {}):
        return False
    
    if buy_date is None:
        buy_date = datetime.now().strftime("%Y-%m-%d")

    state['slots'][slot_key] = {
        "status": "invested",
        "buy_date": buy_date,
        "target_sell_date": target_sell_date,
        "holdings": holdings,
        "cash_balance": initial_cash_balance
    }
    _save_state(state)
    return True

@with_portfolio_lock
def clear_slot(slot_key, returned_cash=0.0):
    """
    Sells all holdings in a slot and resets it to 'empty'.
    Preserves generated cash in 'cash_balance'.
    """
    state = _load_state()
    if not state or slot_key not in state.get('slots', {}):
        return False
    
    state['slots'][slot_key] = {"status": "empty", "cash_balance": returned_cash}
    _save_state(state)
    return True

@with_portfolio_lock
def trigger_stop_loss(slot_key, ticker_to_stop, sell_reason, sell_price, executed_shares, sell_date=None):
    """
    Marks a specific holding in a slot as stopped out (cash) and adds execution proceeds to cash_balance.
    """
    state = _load_state()
    if not state or slot_key not in state.get('slots', {}):
        return False
    
    slot = state['slots'][slot_key]
    if not isinstance(slot, dict) or slot.get('status') != 'invested':
        return False
        
    if sell_date is None:
        sell_date = datetime.now().strftime("%Y-%m-%d")

    found = False
    for holding in slot.get('holdings', []):
        if holding.get('ticker') == str(ticker_to_stop) and holding.get('status') == 'active':
            
            proceeds = round(float(sell_price) * float(executed_shares), 0)
            
            holding['status'] = 'cash'
            holding['sell_reason'] = sell_reason
            holding['sell_date'] = sell_date
            holding['sell_price'] = sell_price
            
            # Add proceeds to cash balance
            slot['cash_balance'] = round(slot.get('cash_balance', 0.0) + proceeds, 0)
            
            found = True
            break
            
    if found:
        _save_state(state)
    return found

@with_portfolio_lock
def increment_none_data_days(slot_key, ticker):
    """
    Increments the consecutive missed data day counter for a specific active holding.
    Returns the new counter value.
    """
    state = _load_state()
    if not state or slot_key not in state.get('slots', {}):
        return 0

    slot = state['slots'][slot_key]
    if not isinstance(slot, dict) or slot.get('status') != 'invested':
        return 0

    for holding in slot.get('holdings', []):
        if holding.get('ticker') == str(ticker) and holding.get('status') == 'active':
            current_count = holding.get('consecutive_none_days', 0)
            new_count = current_count + 1
            holding['consecutive_none_days'] = new_count
            _save_state(state)
            return new_count

    return 0

@with_portfolio_lock
def reset_none_data_days(slot_key, ticker):
    """
    Resets the consecutive missed data day counter to zero for a specific active holding.
    """
    state = _load_state()
    if not state or slot_key not in state.get('slots', {}):
        return

    slot = state['slots'][slot_key]
    if not isinstance(slot, dict) or slot.get('status') != 'invested':
        return

    for holding in slot.get('holdings', []):
        if holding.get('ticker') == str(ticker) and holding.get('status') == 'active':
            if holding.get('consecutive_none_days', 0) > 0:
                holding['consecutive_none_days'] = 0
                _save_state(state)
            return

@with_portfolio_lock
def batch_update_none_data_days(increments, resets):
    """
    increments: list of tuples (slot_key, ticker)
    resets: list of tuples (slot_key, ticker)
    Returns list of dicts: [{'slot': slot_key, 'ticker': ticker, 'consecutive_none_days': new_count}] for increments.
    """
    state = _load_state()
    if not state:
        return []
        
    changed = False
    results = []
    
    for slot_key, ticker in resets:
        if slot_key in state.get('slots', {}):
            slot = state['slots'][slot_key]
            if slot.get('status') == 'invested':
                for holding in slot.get('holdings', []):
                    if holding.get('ticker') == str(ticker) and holding.get('status') == 'active':
                        if holding.get('consecutive_none_days', 0) > 0:
                            holding['consecutive_none_days'] = 0
                            changed = True

    for slot_key, ticker in increments:
        if slot_key in state.get('slots', {}):
            slot = state['slots'][slot_key]
            if slot.get('status') == 'invested':
                for holding in slot.get('holdings', []):
                    if holding.get('ticker') == str(ticker) and holding.get('status') == 'active':
                        current_count = holding.get('consecutive_none_days', 0)
                        new_count = current_count + 1
                        holding['consecutive_none_days'] = new_count
                        results.append({
                            'slot': slot_key,
                            'ticker': ticker,
                            'consecutive_none_days': new_count
                        })
                        changed = True

    if changed:
        _save_state(state)
        
    return results

@with_portfolio_lock
def reconcile_with_kis_holdings(kis_holdings):
    """
    Compares the expected active holdings in portfolio_state.json with the actual
    holdings returned from the KIS API. If there is a shortfall (i.e. shares in DB > actual shares),
    it corrects the DB downward and refunds the unspent cash to the slot's cash_balance.
    Returns a list of alert strings describing any actions taken.
    """
    state = _load_state()
    if not state:
        return []
    
    actual_map = {str(k['ticker']): float(k['shares']) for k in kis_holdings}
    alerts = []
    state_changed = False
    
    # 1. Group active holdings by ticker across all slots
    holdings_by_ticker = {}
    for slot_key, slot_data in state.get('slots', {}).items():
        if slot_data.get('status') == 'invested':
            for h in slot_data.get('holdings', []):
                if h.get('status') == 'active':
                    ticker = str(h.get('ticker'))
                    if ticker not in holdings_by_ticker:
                        holdings_by_ticker[ticker] = []
                    holdings_by_ticker[ticker].append({
                        'slot_key': slot_key,
                        'holding': h,
                        'slot_data': slot_data
                    })
                    
    all_known_tickers = set(holdings_by_ticker.keys())
    
    # 2. Reconcile each tracked ticker
    for ticker, h_list in holdings_by_ticker.items():
        actual_total = actual_map.get(ticker, 0.0)
        expected_total = sum(float(item['holding'].get('shares', 0)) for item in h_list)
        
        if expected_total > actual_total:
            # Shortfall: Deduct from newest slots first (sort by buy_date descending)
            h_list.sort(key=lambda x: x['slot_data'].get('buy_date', ''), reverse=True)
            
            shortfall = expected_total - actual_total
            for item in h_list:
                if shortfall <= 0:
                    break
                
                h = item['holding']
                slot_key = item['slot_key']
                slot_data = item['slot_data']
                
                h_shares = float(h.get('shares', 0))
                if h_shares == 0:
                    continue
                    
                deduct = min(h_shares, shortfall)
                buy_price = float(h.get('buy_price', 0.0))
                refund_amount = round(deduct * buy_price, 0)
                
                h['shares'] = h_shares - deduct
                shortfall -= deduct
                
                current_cash = float(slot_data.get('cash_balance', 0.0))
                slot_data['cash_balance'] = round(current_cash + refund_amount, 0)
                state_changed = True
                
                if h['shares'] == 0:
                    h['status'] = 'failed_buy' if actual_total == 0 else 'cash'
                    msg = f"Reconciliation: Buy order for {ticker} in Slot {slot_key} failed or missing. Removed {deduct} shares and refunded ₩{refund_amount:,.0f}."
                else:
                    msg = f"Reconciliation: Partial fill/discrepancy for {ticker} in Slot {slot_key}. Removed {deduct} shares (now {h['shares']}) and refunded ₩{refund_amount:,.0f}."
                
                # Check for > 50% drop (Reverse split?)
                if (deduct / h_shares) >= 0.5 and actual_total > 0:
                    msg = f"🚨 EMERGENCY: Reconciliation: CRITICAL DISCREPANCY for {ticker} in Slot {slot_key}. Deducted {deduct} shares. Suspected Corporate Action/Reverse Split."
                    h['status'] = 'Corporate Action Suspected'
                
                alerts.append(msg)
                
        elif actual_total > expected_total:
            overage = actual_total - expected_total
            # Sort by buy_date ascending to add to oldest slot
            h_list.sort(key=lambda x: x['slot_data'].get('buy_date', ''))
            oldest_holding = h_list[0]['holding']
            oldest_holding['shares'] = float(oldest_holding.get('shares', 0)) + overage
            slot_key = h_list[0]['slot_key']
            msg = f"⚠️ WARNING: Reconciliation: Discrepancy for {ticker}. Expected {expected_total}, found {actual_total}. DB undercounted by {overage}. Adjusted DB actively upward in Slot {slot_key}."
            alerts.append(msg)
            state_changed = True
            
    # 3. Clean up empty slots
    for slot_key, slot_data in state.get('slots', {}).items():
        if slot_data.get('status') == 'invested':
            still_active = [h for h in slot_data.get('holdings', []) if h.get('status') == 'active']
            if not still_active and state_changed:
                slot_data['status'] = 'empty'
                msg = f"Reconciliation: Slot {slot_key} became empty. Reverted to empty status with cash balance ₩{slot_data['cash_balance']:,.0f}."
                alerts.append(msg)
                
    # 4. Check for orphaned / rogue holdings
    for actual_ticker, actual_shares in actual_map.items():
        if actual_ticker not in all_known_tickers and actual_shares > 0:
            msg = f"🔍 ORPHAN ALERT: Found {actual_shares} shares of undocumented ticker {actual_ticker} in KIS account. This is not tracked by any portfolio slot!"
            alerts.append(msg)

    if state_changed:
        _save_state(state)
        
    return alerts

def get_active_holdings_for_monitoring():
    """
    Returns a list of active holdings across all invested slots for daily monitoring.
    Format: [{'slot': '1', 'ticker': 'QQQ', 'name': 'Invesco QQQ', ...}, ...]
    """
    state = _load_state()
    if not state:
        return []
    
    active_holdings = []
    for slot_key, slot_data in state.get('slots', {}).items():
        if slot_data.get('status') == 'invested':
            for holding in slot_data.get('holdings', []):
                if holding.get('status') == 'active':
                    # Add context for which slot it belongs to
                    h_copy = dict(holding)
                    h_copy['slot'] = slot_key
                    active_holdings.append(h_copy)
                    
    return active_holdings

def get_slots_to_sell(current_date=None):
    """
    Returns a list of slot keys whose target_sell_date is <= current_date.
    """
    state = _load_state()
    if not state:
        return []
        
    if current_date is None:
        current_date_obj = datetime.now()
    else:
        current_date_obj = datetime.strptime(current_date, "%Y-%m-%d")
        
    slots_to_sell = []
    for slot_key, slot_data in state.get('slots', {}).items():
        if slot_data.get('status') == 'invested':
            target_date_str = slot_data.get('target_sell_date')
            if target_date_str:
                target_date_obj = datetime.strptime(target_date_str, "%Y-%m-%d")
                if current_date_obj >= target_date_obj:
                    slots_to_sell.append(slot_key)
                    
    return slots_to_sell

def load_value_history():
    """Loads the daily portfolio value history as a dictionary."""
    if not os.path.exists(VALUE_HISTORY_FILE):
        return {}
    try:
        with open(VALUE_HISTORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Migration hook: if data is list of dicts [{"date": "...", "total_value": X}], convert to dict
            if isinstance(data, list):
                new_data = {item['date']: item['total_value'] for item in data}
                return new_data
            return data
    except Exception as e:
        logger.error("Error loading value history: %s", e)
        return {}

def save_daily_portfolio_value(date_str, total_value):
    """Saves the total portfolio value for the given date in O(1) time."""
    history_dict = load_value_history()
    
    history_dict[date_str] = total_value
    
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        temp_file = VALUE_HISTORY_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(history_dict, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, VALUE_HISTORY_FILE)
    except Exception as e:
        logger.error("Error saving value history: %s", e)

def calculate_portfolio_metrics():
    """
    Calculates Total Return, CAGR, peak value, MDD, and current drawdown
    from the daily value history.
    """
    history_dict = load_value_history()
    if not history_dict:
        return None
        
    df = pd.DataFrame(list(history_dict.items()), columns=['date', 'total_value'])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    
    values = df['total_value'].astype(float)
    if len(values) == 0:
        return None
        
    current_value = float(values.iloc[-1])
    initial_value = float(values.iloc[0])
    
    # Cumulative Return
    total_return_pct = ((current_value / initial_value) - 1.0) * 100 if initial_value > 0 else 0.0
    
    # CAGR
    days_elapsed = (df['date'].iloc[-1] - df['date'].iloc[0]).days
    if days_elapsed > 0:
        years = days_elapsed / 365.25
        cagr_pct = ((current_value / initial_value) ** (1 / years) - 1.0) * 100 if initial_value > 0 else 0.0
    else:
        cagr_pct = total_return_pct

    # Drawdown
    peak_value = values.cummax()
    drawdown = (values - peak_value) / peak_value * 100
    mdd_pct = drawdown.min()
    current_dd_pct = drawdown.iloc[-1]
    
    return {
        "current_value": round(current_value, 2),
        "total_return_pct": round(total_return_pct, 2),
        "cagr_pct": round(cagr_pct, 2),
        "peak_value": round(peak_value.max(), 2),
        "mdd_pct": round(mdd_pct, 2),
        "current_dd_pct": round(current_dd_pct, 2)
    }
