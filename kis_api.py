"""
KIS API Module — Korean Domestic Stock Trading

Handles buy/sell order execution and balance inquiries for Korean domestic
stocks/ETFs via the Korea Investment & Securities (KIS) open-trading-api SDK.

Key functions:
- execute_kis_buy(ticker, shares, price)  → places a domestic limit buy
- execute_kis_sell(ticker, shares, price) → places a domestic limit sell
- get_available_krw()                     → returns available KRW cash
- get_total_portfolio_value()             → returns total portfolio value in KRW
- get_kis_holdings()                      → returns actual holdings from KIS
"""

import os
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# KIS SDK Initialisation
# ---------------------------------------------------------------------------
# The SDK modules (kis_auth, domestic_stock_functions) must be importable.
# They are provided by the KIS open-trading-api repository.
# ---------------------------------------------------------------------------

try:
    import kis_auth as ka
    from domestic_stock_functions import order_cash, inquire_balance

    kis_mode = os.environ.get("KIS_MODE", "prod")
    ka.auth(svr=kis_mode)
    API_ENV_DV = "demo" if kis_mode == "vps" else "real"
    KIS_READY = True
except ImportError as e:
    logger.warning("Could not load KIS API modules. Make sure open-trading-api is accessible. %s", e)
    KIS_READY = False
    API_ENV_DV = "real"


# ---------------------------------------------------------------------------
# Order Execution — Korean Domestic
# ---------------------------------------------------------------------------

def execute_kis_sell(ticker: str, shares: int, current_price: int) -> bool:
    """Place a domestic limit-sell order on KRX.

    Args:
        ticker: 6-digit KRX stock/ETF code (e.g. '069500')
        shares: Number of shares to sell
        current_price: Limit price in KRW (integer)

    Returns:
        True if the order was accepted, False otherwise.
    """
    if not KIS_READY or shares <= 0:
        return False
    try:
        env = ka.getTREnv()
        # ord_dvsn "00" = limit order
        df = order_cash(
            env_dv=API_ENV_DV,
            ord_dv="sell",
            cano=env.my_acct,
            acnt_prdt_cd=env.my_prod,
            pdno=str(ticker),
            ord_dvsn="00",
            ord_qty=str(shares),
            ord_unpr=str(int(current_price)),
            excg_id_dvsn_cd="KRX",
            sll_type="01",
        )
        return df is not None and not df.empty
    except Exception as e:
        logger.error("Sell order error for %s: %s", ticker, e)
        return False


def execute_kis_buy(ticker: str, shares: int, current_price: int) -> bool:
    """Place a domestic limit-buy order on KRX.

    Args:
        ticker: 6-digit KRX stock/ETF code (e.g. '069500')
        shares: Number of shares to buy
        current_price: Limit price in KRW (integer)

    Returns:
        True if the order was accepted, False otherwise.
    """
    if not KIS_READY or shares <= 0:
        return False
    try:
        env = ka.getTREnv()
        # ord_dvsn "00" = limit order
        logger.info("Placing buy order: ticker=%s, shares=%s, price=%s", ticker, shares, current_price)
        df = order_cash(
            env_dv=API_ENV_DV,
            ord_dv="buy",
            cano=env.my_acct,
            acnt_prdt_cd=env.my_prod,
            pdno=str(ticker),
            ord_dvsn="00",
            ord_qty=str(shares),
            ord_unpr=str(int(current_price)),
            excg_id_dvsn_cd="KRX",
        )
        if df is None:
            logger.error("Buy order for %s returned None. API may have rejected the order.", ticker)
            return False
        if df.empty:
            logger.error("Buy order for %s returned empty DataFrame. API may have rejected the order.", ticker)
            return False
        logger.info("Buy order accepted for %s: %s", ticker, df.to_dict())
        return True
    except Exception as e:
        logger.error("Buy order error for %s: %s", ticker, e)
        return False


# ---------------------------------------------------------------------------
# Balance / Holdings Inquiry — Korean Domestic
# ---------------------------------------------------------------------------

def _inquire_balance_raw():
    """Internal helper: call inquire_balance and return (df1, df2)."""
    if not KIS_READY:
        return None, None
    try:
        env = ka.getTREnv()
        df1, df2 = inquire_balance(
            env_dv=API_ENV_DV,
            cano=env.my_acct,
            acnt_prdt_cd=env.my_prod,
            afhr_flpr_yn="N",
            inqr_dvsn="02",
            unpr_dvsn="01",
            fund_sttl_icld_yn="N",
            fncg_amt_auto_rdpt_yn="N",
            prcs_dvsn="00",
        )
        return df1, df2
    except Exception as e:
        logger.error("Error calling inquire_balance: %s", e)
        return None, None


def get_available_krw() -> float:
    """Returns the available KRW cash balance."""
    _, df2 = _inquire_balance_raw()
    if df2 is not None and not df2.empty:
        # dnca_tot_amt = 예수금총금액 (available cash)
        for col in ("dnca_tot_amt", "prvs_rcdl_excc_amt", "nxdy_excc_amt"):
            if col in df2.columns:
                try:
                    return float(df2.iloc[0][col])
                except (ValueError, TypeError):
                    continue
    return 0.0


def get_total_portfolio_value() -> float:
    """Returns total portfolio value (cash + holdings) in KRW.
    
    Uses output2.tot_evlu_amt which is the account-level total evaluation amount
    (주식평가금액 + 예수금). Falls back to nass_amt (순자산금액) if unavailable.
    """
    _, df2 = _inquire_balance_raw()
    if df2 is not None and not df2.empty:
        # tot_evlu_amt = 총평가금액 (includes cash + stock evaluation)
        for col in ("tot_evlu_amt", "nass_amt", "scts_evlu_amt"):
            if col in df2.columns:
                try:
                    val = float(df2.iloc[0][col])
                    if val > 0:
                        return val
                except (ValueError, TypeError):
                    continue
    return 0.0


def get_kis_holdings() -> list[dict]:
    """Returns a list of actual KIS holdings: [{'ticker': '069500', 'shares': 10}, ...]"""
    df1, _ = _inquire_balance_raw()
    holdings = []
    if df1 is not None and not df1.empty:
        for _, row in df1.iterrows():
            # pdno = 상품번호 (종목코드), hldg_qty = 보유수량
            ticker_col = next((c for c in ("pdno", "mksc_shrn_iscd") if c in row.index), None)
            shares_col = "hldg_qty" if "hldg_qty" in row.index else None
            if ticker_col and shares_col:
                ticker = str(row[ticker_col]).strip()
                try:
                    shares = float(row[shares_col])
                except (ValueError, TypeError):
                    shares = 0.0
                if shares > 0:
                    holdings.append({"ticker": ticker, "shares": shares})
    return holdings

