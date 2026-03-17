import os
import json
import ssl
import logging
import urllib.request
import zipfile
from datetime import datetime

logger = logging.getLogger(__name__)

import kis_auth as ka

try:
    from order import order
    from inquire_present_balance import inquire_present_balance
    from dailyprice import dailyprice
    
    # Initialize KIS Auth
    kis_mode = os.environ.get("KIS_MODE", "vps")
    ka.auth(kis_mode)
    KIS_READY = True
except ImportError as e:
    logger.warning("Could not load KIS API modules. Make sure open-trading-api is accessible. %s", e)
    KIS_READY = False

# ---------------------------------------------------------------------------
# Dynamic Exchange Code Lookup
# ---------------------------------------------------------------------------
_EXCHANGE_MAP = {}

_MASTER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "kis_master")
_MAP_FILE = os.path.join(_MASTER_DIR, "us_ticker_exchange_map.json")

def _ensure_exchange_mapping():
    """Download KIS master files (daily cache) and build ticker → exchange map."""
    global _EXCHANGE_MAP

    # If already loaded in this process, skip
    if _EXCHANGE_MAP:
        return

    # Check if cached JSON exists and was updated today
    if os.path.exists(_MAP_FILE):
        mtime = datetime.fromtimestamp(os.path.getmtime(_MAP_FILE))
        if mtime.date() == datetime.now().date():
            with open(_MAP_FILE, "r", encoding="utf-8") as f:
                _EXCHANGE_MAP = json.load(f)
            logger.info("[Exchange Map] Loaded %d cached mappings from today.", len(_EXCHANGE_MAP))
            return

    # Download and parse fresh master files
    logger.info("[Exchange Map] Downloading KIS master files...")
    os.makedirs(_MASTER_DIR, exist_ok=True)

    base_url = "https://new.real.download.dws.co.kr/common/master/"
    exchanges = {"nas": "NASD", "nys": "NYSE", "ams": "AMEX"}
    ticker_map = {}

    ssl._create_default_https_context = ssl._create_unverified_context

    for val, excg_cd in exchanges.items():
        zip_name = f"{val}mst.cod.zip"
        txt_name = f"{val}mst.cod"
        zip_path = os.path.join(_MASTER_DIR, zip_name)
        txt_path = os.path.join(_MASTER_DIR, txt_name)
        url = f"{base_url}{zip_name}"

        try:
            urllib.request.urlretrieve(url, zip_path)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(_MASTER_DIR)

            count = 0
            with open(txt_path, "r", encoding="cp949", errors="ignore") as f:
                for line in f:
                    cols = line.strip().split("\t")
                    if len(cols) > 4:
                        symbol = cols[4].strip()
                        if symbol:
                            ticker_map[symbol] = excg_cd
                            count += 1
            logger.info("[Exchange Map] Parsed %d symbols from %s → %s", count, val, excg_cd)
        except Exception as e:
            logger.error("[Exchange Map] Error processing %s: %s", val, e)

    # Persist to JSON
    with open(_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(ticker_map, f)
    logger.info("[Exchange Map] Saved %d total mappings.", len(ticker_map))

    _EXCHANGE_MAP = ticker_map


def get_exchange_code(ticker):
    """Return the KIS exchange code for a US ticker (NASD / NYSE / AMEX).
    Falls back to 'NASD' if the ticker is not found in the master files."""
    _ensure_exchange_mapping()
    code = _EXCHANGE_MAP.get(ticker, "NASD")
    return code

# ---------------------------------------------------------------------------
# Order Execution
# ---------------------------------------------------------------------------

def execute_kis_sell(ticker, shares, current_price):
    if not KIS_READY or shares <= 0: return False
    try:
        env = ka.getTREnv()
        excg = get_exchange_code(ticker)
        
        # Real: MOO (31) Market On Open
        ord_dvsn = "31"
        limit_price = "0"
            
        df = order(
            cano=env.my_acct, acnt_prdt_cd=env.my_prod,
            ovrs_excg_cd=excg, pdno=ticker, ord_qty=str(shares),
            ovrs_ord_unpr=limit_price, ord_dv="sell", ctac_tlno="", mgco_aptm_odno="",
            ord_svr_dvsn_cd="0", ord_dvsn=ord_dvsn, env_dv="real"
        )
        return True if df is not None and not df.empty else False
    except Exception as e:
        logger.error("Sell order error for %s: %s", ticker, e)
        return False

def execute_kis_buy(ticker, shares, current_price):
    if not KIS_READY or shares <= 0: return False
    try:
        env = ka.getTREnv()
        excg = get_exchange_code(ticker)
        
        # Real: LOC (34) Limit On Close at current price
        ord_dvsn = "34"
        limit_price = str(round(current_price, 2))
            
        df = order(
            cano=env.my_acct, acnt_prdt_cd=env.my_prod,
            ovrs_excg_cd=excg, pdno=ticker, ord_qty=str(shares),
            ovrs_ord_unpr=limit_price, ord_dv="buy", ctac_tlno="", mgco_aptm_odno="",
            ord_svr_dvsn_cd="0", ord_dvsn=ord_dvsn, env_dv="real"
        )
        return True if df is not None and not df.empty else False
    except Exception as e:
        logger.error("Buy order error for %s: %s", ticker, e)
        return False

def get_available_usd():
    if not KIS_READY: return 0.0
    try:
        env = ka.getTREnv()
        df1, _, _ = inquire_present_balance(
             cano=env.my_acct, acnt_prdt_cd=env.my_prod,
             wcrc_frcr_dvsn_cd="02", natn_cd="840", tr_mket_cd="00", inqr_dvsn_cd="00",
             env_dv="real"
        )
        if df1 is not None and not df1.empty and "frcr_prsl_tot_amt" in df1.columns:
             return float(df1.iloc[0]["frcr_prsl_tot_amt"])
    except Exception as e:
        logger.error("Error fetching USD balance: %s", e)
    return 0.0

def get_total_portfolio_value():
    if not KIS_READY: return 0.0
    try:
        env = ka.getTREnv()
        df1, _, _ = inquire_present_balance(
             cano=env.my_acct, acnt_prdt_cd=env.my_prod,
             wcrc_frcr_dvsn_cd="02", natn_cd="840", tr_mket_cd="00", inqr_dvsn_cd="00",
             env_dv="real"
        )
        
        usd_cash = 0.0
        holdings_value = 0.0
        if df1 is not None and not df1.empty:
             if "frcr_prsl_tot_amt" in df1.columns:
                  usd_cash = float(df1.iloc[0]["frcr_prsl_tot_amt"])
             if "frcr_evlu_amt_smtl" in df1.columns:
                  holdings_value = float(df1.iloc[0]["frcr_evlu_amt_smtl"])
                  
        return usd_cash + holdings_value
    except Exception as e:
        logger.error("Error fetching Total Portfolio value: %s", e)
    return 0.0

def get_kis_holdings():
    """Returns a list of dictionaries with actual holdings from KIS: [{'ticker': 'SPY', 'shares': 10}, ...]"""
    if not KIS_READY: return []
    try:
        env = ka.getTREnv()
        _, df2, _ = inquire_present_balance(
             cano=env.my_acct, acnt_prdt_cd=env.my_prod,
             wcrc_frcr_dvsn_cd="02", natn_cd="840", tr_mket_cd="00", inqr_dvsn_cd="00",
             env_dv="real"
        )
        holdings = []
        if df2 is not None and not df2.empty:
            for _, row in df2.iterrows():
                if "ovrs_pdno" in row and "ccld_qty_smtl1" in row:
                    ticker = str(row["ovrs_pdno"]).strip()
                    shares = float(row["ccld_qty_smtl1"]) # Shares remaining
                    if shares > 0:
                        holdings.append({'ticker': ticker, 'shares': shares})
        return holdings
    except Exception as e:
        logger.error("Error fetching KIS holdings: %s", e)
    return []
