import os
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
    print(f"Warning: Could not load KIS API modules. Make sure open-trading-api is accessible. {e}")
    KIS_READY = False

def execute_kis_sell(ticker, shares, current_price):
    if not KIS_READY or shares <= 0: return False
    try:
        env = ka.getTREnv()
        # Limit sell (00) at 0.95x current price to ensure fill
        limit_price = str(round(current_price * 0.95, 2))
        df = order(
            cano=env.my_acct, acnt_prdt_cd=env.my_prod,
            ovrs_excg_cd="NASD", pdno=ticker, ord_qty=str(shares),
            ovrs_ord_unpr=limit_price, ord_dv="sell", ctac_tlno="", mgco_aptm_odno="",
            ord_svr_dvsn_cd="0", ord_dvsn="00", env_dv="demo" if ka.isPaperTrading() else "real"
        )
        return True if df is not None and not df.empty else False
    except Exception as e:
        print(f"Sell order error for {ticker}: {e}")
        return False

def execute_kis_buy(ticker, shares, current_price):
    if not KIS_READY or shares <= 0: return False
    try:
        env = ka.getTREnv()
        # Limit buy (00) at 1.05x current price to ensure fill
        limit_price = str(round(current_price * 1.05, 2))
        df = order(
            cano=env.my_acct, acnt_prdt_cd=env.my_prod,
            ovrs_excg_cd="NASD", pdno=ticker, ord_qty=str(shares),
            ovrs_ord_unpr=limit_price, ord_dv="buy", ctac_tlno="", mgco_aptm_odno="",
            ord_svr_dvsn_cd="0", ord_dvsn="00", env_dv="demo" if ka.isPaperTrading() else "real"
        )
        return True if df is not None and not df.empty else False
    except Exception as e:
        print(f"Buy order error for {ticker}: {e}")
        return False

def get_available_usd():
    if not KIS_READY: return 10000.0 # Return mock 10k USD
    try:
        env = ka.getTREnv()
        df1, _, _ = inquire_present_balance(
             cano=env.my_acct, acnt_prdt_cd=env.my_prod,
             wcrc_frcr_dvsn_cd="02", natn_cd="840", tr_mket_cd="00", inqr_dvsn_cd="00",
             env_dv="demo" if ka.isPaperTrading() else "real"
        )
        if df1 is not None and not df1.empty and "frcr_prsl_tot_amt" in df1.columns:
             return float(df1.iloc[0]["frcr_prsl_tot_amt"])
    except Exception as e:
        print(f"Error fetching USD balance: {e}")
    return 10000.0

def get_total_portfolio_value():
    if not KIS_READY: return 0.0
    try:
        env = ka.getTREnv()
        df1, _, _ = inquire_present_balance(
             cano=env.my_acct, acnt_prdt_cd=env.my_prod,
             wcrc_frcr_dvsn_cd="02", natn_cd="840", tr_mket_cd="00", inqr_dvsn_cd="00",
             env_dv="demo" if ka.isPaperTrading() else "real"
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
        print(f"Error fetching Total Portfolio value: {e}")
    return 0.0

