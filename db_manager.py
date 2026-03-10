import os
import json
import pandas as pd
from datetime import datetime

DATA_DIR = 'data'
STATE_FILE = os.path.join(DATA_DIR, 'portfolio_state.json')
VALUE_HISTORY_FILE = os.path.join(DATA_DIR, 'portfolio_value_history.json')
TRADE_HISTORY_FILE = os.path.join(DATA_DIR, 'trade_history.json')

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
        print(f"Error loading state: {e}")
        return None

def _save_state(state):
    """Saves the given portfolio state to JSON."""
    try:
        temp_file = STATE_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, STATE_FILE)
    except Exception as e:
        print(f"Error saving state: {e}")

def _load_trade_history():
    """Loads the trade history from JSON."""
    if not os.path.exists(TRADE_HISTORY_FILE):
        return []
    try:
        with open(TRADE_HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading trade history: {e}")
        return []

def _save_trade_history(history):
    """Saves the trade history to JSON."""
    try:
        temp_file = TRADE_HISTORY_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, TRADE_HISTORY_FILE)
    except Exception as e:
        print(f"Error saving trade history: {e}")

def log_trade(action, ticker, shares, price, slot_key, name="", reason="", status=""):
    """
    Logs a trade (BUY, SELL) to the trade history file.
    """
    history = _load_trade_history()
    
    trade_record = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "ticker": ticker,
        "name": name,
        "slot": slot_key,
        "shares": shares,
        "price": price,
        "reason": reason,
        "status": status
    }
    
    history.append(trade_record)
    _save_trade_history(history)
    print(f"Logged {action} trade for {ticker} ({shares} shares @ {price}) in Slot {slot_key}.")

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

def fill_slot(slot_key, target_sell_date, holdings, buy_date=None, initial_cash_balance=0.0):
    """
    Fills an empty slot with selected ETFs.
    holdings format: list of dicts [{'ticker': 'SPY', 'name': 'SPDR...', 'shares': 10, 'buy_price': 500.0, 'status': 'active'}, ...]
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
    
    # Log the BUY trades
    for h in holdings:
        log_trade(
            action="BUY",
            ticker=h.get('ticker'),
            shares=h.get('shares'),
            price=h.get('buy_price'),
            slot_key=slot_key,
            name=h.get('name', ''),
            reason="Initial Allocation",
            status="active"
        )
        
    return True

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

def trigger_stop_loss(slot_key, ticker_to_stop, sell_reason, sell_price, executed_shares, sell_date=None):
    """
    Marks a specific holding in a slot as stopped out (cash) and adds execution proceeds to cash_balance.
    """
    state = _load_state()
    if not state or slot_key not in state.get('slots', {}):
        return False
    
    slot = state['slots'][slot_key]
    if slot.get('status') != 'invested':
        return False
        
    if sell_date is None:
        sell_date = datetime.now().strftime("%Y-%m-%d")

    found = False
    for holding in slot.get('holdings', []):
        if holding.get('ticker') == str(ticker_to_stop) and holding.get('status') == 'active':
            
            proceeds = round(float(sell_price) * float(executed_shares), 2)
            
            holding['status'] = 'cash'
            holding['sell_reason'] = sell_reason
            holding['sell_date'] = sell_date
            holding['sell_price'] = sell_price
            
            # If we sold everything, shares remains 0 (or original if we want to track). Let's represent remaining shares if partial. 
            # Assuming full stop-loss for now:
            
            # Add proceeds to cash balance
            slot['cash_balance'] = round(slot.get('cash_balance', 0.0) + proceeds, 2)
            
            # Log the SELL trade
            log_trade(
                action="SELL",
                ticker=str(ticker_to_stop),
                shares=executed_shares,
                price=sell_price,
                slot_key=slot_key,
                name=holding.get('name', ''),
                reason=sell_reason,
                status="cash"
            )
            
            found = True
            break
            
    if found:
        _save_state(state)
    return found

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
    """Loads the daily portfolio value history."""
    if not os.path.exists(VALUE_HISTORY_FILE):
        return []
    try:
        with open(VALUE_HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading value history: {e}")
        return []

def save_daily_portfolio_value(date_str, total_value):
    """Appends the total portfolio value for the given date."""
    history = load_value_history()
    
    # Update if date exists, else append
    updated = False
    for entry in history:
        if entry.get("date") == date_str:
            entry["total_value"] = total_value
            updated = True
            break
            
    if not updated:
        history.append({"date": date_str, "total_value": total_value})
        
    # Sort by date
    history = sorted(history, key=lambda x: x["date"])
    
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        temp_file = VALUE_HISTORY_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, VALUE_HISTORY_FILE)
    except Exception as e:
        print(f"Error saving value history: {e}")

def calculate_portfolio_metrics():
    """
    Calculates Total Return, CAGR, peak value, MDD, and current drawdown
    from the daily value history.
    """
    history = load_value_history()
    if not history:
        return None
        
    df = pd.DataFrame(history)
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
