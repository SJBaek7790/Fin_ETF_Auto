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

    kis_mode = os.environ.get("KIS_MODE", "vps")
    ka.auth(kis_mode)
    KIS_READY = True
except ImportError as e:
    logger.warning("Could not load KIS API modules. Make sure open-trading-api is accessible. %s", e)
    KIS_READY = False

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
            env_dv="real",
            ord_dv="sell",
            cano=env.my_acct,
            acnt_prdt_cd=env.my_prod,
            pdno=str(ticker),
            ord_dvsn="00",
            ord_qty=str(shares),
            ord_unpr=str(int(current_price)),
            excg_id_dvsn_cd="KRX",
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
        df = order_cash(
            env_dv="real",
            ord_dv="buy",
            cano=env.my_acct,
            acnt_prdt_cd=env.my_prod,
            pdno=str(ticker),
            ord_dvsn="00",
            ord_qty=str(shares),
            ord_unpr=str(int(current_price)),
            excg_id_dvsn_cd="KRX",
        )
        return df is not None and not df.empty
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
            env_dv="real",
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
    df1, _ = _inquire_balance_raw()
    if df1 is not None and not df1.empty:
        # dnca_tot_amt = 예수금총금액 (available cash)
        for col in ("dnca_tot_amt", "prvs_rcdl_excc_amt", "nxdy_excc_amt"):
            if col in df1.columns:
                try:
                    return float(df1.iloc[0][col])
                except (ValueError, TypeError):
                    continue
    return 0.0


def get_total_portfolio_value() -> float:
    """Returns total portfolio value (cash + holdings) in KRW."""
    df1, _ = _inquire_balance_raw()
    if df1 is not None and not df1.empty:
        cash = 0.0
        holdings_value = 0.0
        if "dnca_tot_amt" in df1.columns:
            try:
                cash = float(df1.iloc[0]["dnca_tot_amt"])
            except (ValueError, TypeError):
                pass
        if "tot_evlu_amt" in df1.columns:
            try:
                holdings_value = float(df1.iloc[0]["tot_evlu_amt"])
            except (ValueError, TypeError):
                pass
        # If tot_evlu_amt already includes cash, just return it
        if holdings_value > 0 and cash > 0:
            # tot_evlu_amt is usually the total (cash + stock eval)
            return holdings_value
        return cash + holdings_value
    return 0.0


def get_kis_holdings() -> list[dict]:
    """Returns a list of actual KIS holdings: [{'ticker': '069500', 'shares': 10}, ...]"""
    _, df2 = _inquire_balance_raw()
    holdings = []
    if df2 is not None and not df2.empty:
        for _, row in df2.iterrows():
            # pdno = 상품번호 (종목코드), hldg_qty = 보유수량
            ticker_col = next((c for c in ("pdno", "mksc_shrn_iscd") if c in row.index), None)
            shares_col = next((c for c in ("hldg_qty", "ccld_qty_smtl1") if c in row.index), None)
            if ticker_col and shares_col:
                ticker = str(row[ticker_col]).strip()
                try:
                    shares = float(row[shares_col])
                except (ValueError, TypeError):
                    shares = 0.0
                if shares > 0:
                    holdings.append({"ticker": ticker, "shares": shares})
    return holdings
