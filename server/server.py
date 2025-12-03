import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import tushare as ts
import pandas as pd
import datetime
import math
import json
from mcp.server.fastmcp import FastMCP

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from strategies.wheel_backtest import run_wheel_backtest, WheelBacktestError

# Load environment variables from repo root
env_path = ROOT_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# Debug logging
import logging
logging.basicConfig(
    filename=str(Path(__file__).parent / 'server.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.info("Server starting...")

# Initialize the MCP server
mcp = FastMCP("Tushare MCP Server")

# Initialize Tushare Pro API
# Best practice: Set TUSHARE_TOKEN in your environment variables
token = os.environ.get("TUSHARE_TOKEN")
if not token:
    print("Warning: TUSHARE_TOKEN environment variable not set.")

pro = ts.pro_api(token) if token else None

@mcp.tool()
def get_stock_basic(
    exchange: str = "", 
    list_status: str = "L", 
    fields: str = "ts_code,symbol,name,area,industry,list_date"
) -> str:
    """
    Get basic stock information from Tushare.
    
    Args:
        exchange: Exchange code (SSE, SZSE, BSE). Empty for all.
        list_status: Listing status (L=Listed, D=Delisted, P=Paused). Default is L.
        fields: Comma-separated list of fields to return.
    """
    if not pro:
        return "Error: Tushare token not configured. Please set TUSHARE_TOKEN environment variable."

    try:
        # Call Tushare API
        df = pro.stock_basic(exchange=exchange, list_status=list_status, fields=fields)
        
        # Convert DataFrame to JSON string for the LLM
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching data from Tushare: {str(e)}"

@mcp.tool()
def get_daily_price(ts_code: str, start_date: str, end_date: str) -> str:
    """
    Get daily stock price data.
    
    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
    """
    if not pro:
        return "Error: Tushare token not configured. Please set TUSHARE_TOKEN environment variable."

    try:
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching daily data: {str(e)}"

@mcp.tool()
def get_index_daily(ts_code: str, start_date: str, end_date: str) -> str:
    """
    Get daily index data (e.g. 000001.SH for SSE).
    
    Args:
        ts_code: Index code
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
    """
    if not pro:
        return "Error: Tushare token not configured."

    try:
        df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching index daily data: {str(e)}"

@mcp.tool()
def get_trade_cal(start_date: str, end_date: str, exchange: str = "SSE") -> str:
    """
    Get trade calendar.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        exchange: Exchange code, default is SSE
    """
    if not pro:
        return "Error: Tushare token not configured. Please set TUSHARE_TOKEN environment variable."

    try:
        df = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching trade calendar: {str(e)}"

@mcp.tool()
def get_daily_basic(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
    """
    Get daily basic indicators (PE, PB, turnover, etc.).
    
    Args:
        ts_code: Stock code (e.g., '000001.SZ')
        trade_date: Trade date (YYYYMMDD)
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.daily_basic(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching daily basic data: {str(e)}"

@mcp.tool()
def get_price_volatility(
    identifier: str,
    window: int = 30,
    frequency: str = "daily",
    annualize: bool = True
) -> str:
    """
    Compute price volatility for a given stock by code or company name.

    Args:
        identifier: Stock code (e.g., '000001.SZ') or exact/partial company name.
        window: Number of most recent periods (days/months/years) to include (default 30).
        frequency: One of 'daily', 'monthly', or 'yearly'.
        annualize: Whether to return annualized volatility (uses 252/12/1 scaling).
    """
    if not pro:
        return "Error: Tushare token not configured."

    identifier = (identifier or "").strip()
    if not identifier:
        return "Error: identifier is required."

    frequency = (frequency or "daily").lower()
    if frequency not in {"daily", "monthly", "yearly"}:
        return "Error: frequency must be one of 'daily', 'monthly', or 'yearly'."

    try:
        ts_code = None
        company_name = None

        if "." in identifier:
            ts_code = identifier.upper()
            df_lookup = pro.stock_basic(ts_code=ts_code, fields='ts_code,name')
            if df_lookup.empty:
                return f"Error: No stock found for code {ts_code}."
            company_name = df_lookup.iloc[0]['name']
        else:
            df_lookup = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
            matches = df_lookup[df_lookup['name'].str.contains(identifier, case=False, na=False)]
            if matches.empty:
                return f"Error: No stock matched name '{identifier}'."
            if len(matches) > 1:
                return "Error: Multiple stocks matched the name. Please provide the exact ts_code."
            ts_code = matches.iloc[0]['ts_code']
            company_name = matches.iloc[0]['name']

        today = datetime.datetime.now()
        if frequency == "daily":
            lookback_days = max(window * 3, 90)
        elif frequency == "monthly":
            lookback_days = max(window * 40, 365)
        else:  # yearly
            lookback_days = max(window * 370, 5 * 365)

        start_date = (today - datetime.timedelta(days=lookback_days)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')

        if frequency == "daily":
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        else:
            df = pro.monthly(ts_code=ts_code, start_date=start_date, end_date=end_date)

        if df.empty:
            return f"Error: No price data found for {ts_code} using frequency '{frequency}'."

        df = df.sort_values('trade_date').reset_index(drop=True)

        if frequency == "yearly":
            df['year'] = df['trade_date'].str[:4]
            df = (
                df.groupby('year', as_index=False)
                .tail(1)
                .sort_values('trade_date')
                .reset_index(drop=True)
            )
            df = df.drop(columns=['year'])

        price_series = df['close']
        returns = price_series.pct_change().dropna()
        if returns.empty:
            return "Error: Not enough price history to compute returns."

        returns_window = returns.tail(window)
        if returns_window.empty:
            returns_window = returns

        period_vol = returns_window.std()
        if pd.isna(period_vol):
            return "Error: Unable to compute volatility from the data available."

        scaling = {
            "daily": math.sqrt(252),
            "monthly": math.sqrt(12),
            "yearly": 1.0
        }
        annualized_vol = period_vol * scaling[frequency] if annualize else None

        window_dates = df.loc[returns_window.index, 'trade_date']

        result = {
            "ts_code": ts_code,
            "name": company_name,
            "frequency": frequency,
            "window_periods": int(len(returns_window)),
            "period_volatility": float(period_vol),
            "annualized_volatility": float(annualized_vol) if annualized_vol is not None else None,
            "mean_period_return": float(returns_window.mean()),
            "data_start": window_dates.iloc[0] if not window_dates.empty else df.iloc[0]['trade_date'],
            "data_end": window_dates.iloc[-1] if not window_dates.empty else df.iloc[-1]['trade_date']
        }

        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        return f"Error computing volatility: {str(e)}"

@mcp.tool()
def get_weekly_price(ts_code: str, start_date: str, end_date: str) -> str:
    """
    Get weekly stock price data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.weekly(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching weekly data: {str(e)}"

@mcp.tool()
def get_monthly_price(ts_code: str, start_date: str, end_date: str) -> str:
    """
    Get monthly stock price data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.monthly(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching monthly data: {str(e)}"

@mcp.tool()
def get_income(ts_code: str, start_date: str = "", end_date: str = "", period: str = "") -> str:
    """
    Get income statement data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
        period: Report period (YYYYMMDD, e.g. 20231231)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date, period=period)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching income data: {str(e)}"

@mcp.tool()
def get_balancesheet(ts_code: str, start_date: str = "", end_date: str = "", period: str = "") -> str:
    """
    Get balance sheet data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
        period: Report period (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.balancesheet(ts_code=ts_code, start_date=start_date, end_date=end_date, period=period)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching balance sheet data: {str(e)}"

@mcp.tool()
def get_cashflow(ts_code: str, start_date: str = "", end_date: str = "", period: str = "") -> str:
    """
    Get cash flow data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
        period: Report period (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.cashflow(ts_code=ts_code, start_date=start_date, end_date=end_date, period=period)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching cash flow data: {str(e)}"

@mcp.tool()
def get_forecast(ts_code: str, start_date: str = "", end_date: str = "") -> str:
    """
    Get performance forecast data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.forecast(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching forecast data: {str(e)}"

@mcp.tool()
def get_top10_holders(ts_code: str, start_date: str = "", end_date: str = "") -> str:
    """
    Get top 10 shareholders data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.top10_holders(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching top 10 holders data: {str(e)}"

@mcp.tool()
def get_fina_indicator(ts_code: str, start_date: str = "", end_date: str = "") -> str:
    """
    Get financial indicators data.
    
    Args:
        ts_code: Stock code
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.fina_indicator(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching financial indicators: {str(e)}"

@mcp.tool()
def get_stock_company(ts_code: str = "", exchange: str = "") -> str:
    """
    Get public company information.
    
    Args:
        ts_code: Stock code
        exchange: Exchange code
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.stock_company(ts_code=ts_code, exchange=exchange)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching company info: {str(e)}"

@mcp.tool()
def get_name_change(ts_code: str = "", start_date: str = "", end_date: str = "") -> str:
    """
    Get stock name change history.
    
    Args:
        ts_code: Stock code
        start_date: Start date
        end_date: End date
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.name_change(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching name change history: {str(e)}"

@mcp.tool()
def get_hs_const(hs_type: str = "SH") -> str:
    """
    Get HSGT (Shanghai-Shenzhen-Hong Kong Stock Connect) constituents.
    
    Args:
        hs_type: Type (SH=Shanghai Connect, SZ=Shenzhen Connect)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.hs_const(hs_type=hs_type)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching HSGT constituents: {str(e)}"

@mcp.tool()
def get_new_share(start_date: str = "", end_date: str = "") -> str:
    """
    Get IPO new share listing info.
    
    Args:
        start_date: Start date
        end_date: End date
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.new_share(start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching new share info: {str(e)}"

@mcp.tool()
def get_adj_factor(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
    """
    Get adjustment factors.
    
    Args:
        ts_code: Stock code
        trade_date: Trade date
        start_date: Start date
        end_date: End date
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.adj_factor(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching adjustment factors: {str(e)}"

@mcp.tool()
def get_suspend_d(ts_code: str = "", suspend_date: str = "", resume_date: str = "", start_date: str = "", end_date: str = "") -> str:
    """
    Get stock suspension/resumption info.
    
    Args:
        ts_code: Stock code
        suspend_date: Suspension date
        resume_date: Resumption date
        start_date: Start date
        end_date: End date
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.suspend_d(ts_code=ts_code, suspend_date=suspend_date, resume_date=resume_date, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching suspension info: {str(e)}"

@mcp.tool()
def get_express(ts_code: str = "", start_date: str = "", end_date: str = "", period: str = "") -> str:
    """
    Get performance express report.
    
    Args:
        ts_code: Stock code
        start_date: Start date
        end_date: End date
        period: Report period
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.express(ts_code=ts_code, start_date=start_date, end_date=end_date, period=period)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching express report: {str(e)}"

@mcp.tool()
def get_dividend(ts_code: str = "", ann_date: str = "", record_date: str = "", ex_date: str = "") -> str:
    """
    Get dividend and bonus share info.
    
    Args:
        ts_code: Stock code
        ann_date: Announcement date
        record_date: Record date
        ex_date: Ex-dividend date
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.dividend(ts_code=ts_code, ann_date=ann_date, record_date=record_date, ex_date=ex_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching dividend info: {str(e)}"

@mcp.tool()
def get_fina_audit(ts_code: str = "", start_date: str = "", end_date: str = "", period: str = "") -> str:
    """
    Get financial audit opinions.
    
    Args:
        ts_code: Stock code
        start_date: Start date
        end_date: End date
        period: Report period
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.fina_audit(ts_code=ts_code, start_date=start_date, end_date=end_date, period=period)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching audit opinions: {str(e)}"

@mcp.tool()
def get_fina_mainbz(ts_code: str = "", period: str = "", type: str = "") -> str:
    """
    Get main business composition.
    
    Args:
        ts_code: Stock code
        period: Report period
        type: Type (P=Product, D=Region)
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.fina_mainbz(ts_code=ts_code, period=period, type=type)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching main business info: {str(e)}"

@mcp.tool()
def get_index_basic(market: str = "", publisher: str = "", category: str = "") -> str:
    """
    Get index basic information.
    
    Args:
        market: Market (MSE, SZSE, CNI, CSI, etc.)
        publisher: Publisher
        category: Index category
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.index_basic(market=market, publisher=publisher, category=category)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching index basic info: {str(e)}"

@mcp.tool()
def get_index_weight(index_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> str:
    """
    Get index constituents and weights.
    
    Args:
        index_code: Index code (e.g. 399300.SZ)
        trade_date: Trade date
        start_date: Start date
        end_date: End date
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.index_weight(index_code=index_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching index weights: {str(e)}"

@mcp.tool()
def get_index_dailybasic(trade_date: str = "", ts_code: str = "", start_date: str = "", end_date: str = "") -> str:
    """
    Get index daily basic indicators (PE, PB, etc.).
    
    Args:
        trade_date: Trade date
        ts_code: Index code
        start_date: Start date
        end_date: End date
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.index_dailybasic(trade_date=trade_date, ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching index daily basic info: {str(e)}"

@mcp.tool()
def get_fund_daily(ts_code: str, start_date: str = "", end_date: str = "", fields: str = "") -> str:
    """
    Get ETF/fund OHLC data using Tushare fund_daily endpoint (doc 127).

    Args:
        ts_code: Fund/ETF code (e.g., '159915.SZ').
        start_date: Start date YYYYMMDD.
        end_date: End date YYYYMMDD.
        fields: Optional comma-separated field list to limit returned columns.
    """
    if not pro:
        return "Error: Tushare token not configured."
    if not ts_code:
        return "Error: ts_code is required."
    try:
        kwargs = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
        if fields:
            kwargs["fields"] = fields
        df = pro.fund_daily(**kwargs)
        if df.empty:
            return f"No fund_daily data returned for {ts_code}."
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching fund daily data: {str(e)}"

@mcp.tool()
def backtest_wheel_strategy(
    underlying: str,
    start_date: str = "20220101",
    end_date: str = datetime.datetime.now().strftime("%Y%m%d"),
    otm_min: float = 0.07,
    otm_max: float = 0.10,
    initial_capital: float = 30000.0
) -> str:
    """
    Run a simple monthly wheel strategy on an ETF using its exchange-traded options.

    Args:
        underlying: ETF/fund ts_code (e.g., '159915.SZ').
        start_date: Backtest start date (YYYYMMDD).
        end_date: Backtest end date (YYYYMMDD).
        otm_min: Lower bound for OTM percentage (e.g., 0.07 for 7%).
        otm_max: Upper bound for OTM percentage (e.g., 0.10 for 10%).
        initial_capital: Starting cash used for tracking return metrics.
    """
    if not pro:
        return "Error: Tushare token not configured."
    if not underlying:
        return "Error: underlying ts_code is required."
    if otm_min < 0 or otm_max <= otm_min:
        return "Error: Invalid OTM range. Ensure 0 <= otm_min < otm_max."

    try:
        result = run_wheel_backtest(
            pro,
            underlying=underlying,
            start_date=start_date,
            end_date=end_date,
            otm_min=otm_min,
            otm_max=otm_max,
            initial_capital=initial_capital,
        )
        result["recent_trades"] = result.get("trades", [])[-12:]
        return json.dumps(result, ensure_ascii=False)
    except WheelBacktestError as exc:
        return f"Error running wheel strategy: {str(exc)}"
    except Exception as exc:  # noqa: BLE001
        return f"Unexpected error running wheel strategy: {str(exc)}"

@mcp.tool()
def get_option_basic(exchange: str = "SSE", fields: str = "ts_code,name,exercise_price,exercise_type,list_date,maturity_date") -> str:
    """
    Get option contract reference information (per Tushare doc 157).

    Args:
        exchange: 'SSE' for 上交所、'SZSE' for 深交所。Empty returns both.
        fields: Comma-separated columns from opt_basic.
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.opt_basic(exchange=exchange, fields=fields)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching option basic data: {str(e)}"

@mcp.tool()
def get_option_daily(ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "", exchange: str = "") -> str:
    """
    Get option daily bar data (per Tushare doc 157).

    Args:
        ts_code: Option code like '10002458.SH'. Optional if querying by date+exchange.
        trade_date: Single trading date (YYYYMMDD).
        start_date: Start date for range queries (YYYYMMDD).
        end_date: End date for range queries (YYYYMMDD).
        exchange: 'SSE' or 'SZSE' when pulling by date.
    """
    if not pro:
        return "Error: Tushare token not configured."
    try:
        df = pro.opt_daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date, exchange=exchange)
        return df.to_json(orient="records", force_ascii=False)
    except Exception as e:
        return f"Error fetching option daily data: {str(e)}"

@mcp.tool()
def scan_market_opportunities(
    consecutive_monthly_drop: int = 0,
    ma_trend: str = "",
    price_below_date: str = ""
) -> str:
    """
    Scan the market for stocks matching specific technical and price criteria.
    This is a composite tool for complex screening.
    
    Args:
        consecutive_monthly_drop: Number of consecutive months with negative returns (close < open). e.g., 3.
        ma_trend: Moving Average trend. 'bear' for MA5 < MA10 < MA20, 'bull' for MA5 > MA10 > MA20.
        price_below_date: Check if current price is lower than the high price on this date (YYYYMMDD).
                          Strictly checks if the stock price has exceeded the high of this date since then.
    """
    if not pro:
        return "Error: Tushare token not configured."
    
    try:
        candidates = []
        
        # Step 1: Monthly Drop Filter
        if consecutive_monthly_drop > 0:
            # Get last N months of month-end dates
            # We look back enough days to cover N months
            cal_start = (datetime.datetime.now() - datetime.timedelta(days=40 * (consecutive_monthly_drop + 2))).strftime('%Y%m%d')
            cal_end = datetime.datetime.now().strftime('%Y%m%d')
            
            # Get monthly calendar to find valid trade_dates for monthly data
            try:
                df_cal = pro.trade_cal(exchange='SSE', start_date=cal_start, end_date=cal_end, is_open='1')
            except Exception as e:
                return f"Error fetching calendar: {e}"

            df_cal['month'] = df_cal['cal_date'].str[:6]
            # Get last trading day of each month
            month_ends = df_cal.groupby('month')['cal_date'].max().sort_values(ascending=False)
            
            # We need the last 'consecutive_monthly_drop' months.
            target_dates = month_ends.head(consecutive_monthly_drop).tolist()
            
            if len(target_dates) < consecutive_monthly_drop:
                 return f"Error: Not enough monthly data. Found {len(target_dates)} months."

            # Fetch monthly data for these dates
            monthly_dfs = []
            for d in target_dates:
                try:
                    # Fetch full market monthly data for this date
                    df_m = pro.monthly(trade_date=d, fields='ts_code,trade_date,close,open')
                    monthly_dfs.append(df_m)
                except Exception as e:
                    print(f"Error fetching monthly for {d}: {e}")
            
            if not monthly_dfs:
                return "Error: Failed to fetch monthly data."
            
            df_monthly = pd.concat(monthly_dfs)
            
            # Filter for drops
            df_monthly['is_drop'] = df_monthly['close'] < df_monthly['open']
            
            # Pivot: index=ts_code, columns=trade_date, values=is_drop
            pivot = df_monthly.pivot_table(index='ts_code', columns='trade_date', values='is_drop')
            
            # We want stocks where all columns are True (drops)
            # Only consider stocks that have data for ALL target dates
            if pivot.shape[1] == consecutive_monthly_drop:
                candidates = pivot[pivot.all(axis=1)].index.tolist()
            else:
                # Filter pivot to only include columns in target_dates (in case extra data)
                valid_cols = [c for c in pivot.columns if c in target_dates]
                pivot = pivot[valid_cols]
                # Drop rows with NaN (missing data for some months)
                pivot = pivot.dropna()
                candidates = pivot[pivot.all(axis=1)].index.tolist()
            
            print(f"DEBUG: Candidates after monthly filter: {len(candidates)}")
        
        if consecutive_monthly_drop > 0 and not candidates:
            return "No stocks found matching monthly drop criteria."

        # Step 2: Price Comparison (Strict: High since Date <= High on Date)
        if price_below_date:
            if not candidates and consecutive_monthly_drop == 0:
                 return "Error: Please provide other criteria to narrow down stocks before checking price history."
            
            # Get all trading dates since price_below_date
            try:
                cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=price_below_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))
                all_dates = cal['cal_date'].tolist()
            except:
                return "Error: Invalid price_below_date or calendar error."

            if not all_dates:
                return "Error: Invalid price_below_date range."
            
            start_d = all_dates[0]
            end_d = all_dates[-1]
            
            final_candidates = []
            chunk_size = 50
            
            for i in range(0, len(candidates), chunk_size):
                chunk = candidates[i:i+chunk_size]
                chunk_str = ",".join(chunk)
                
                try:
                    # Fetch daily data for this chunk for the whole period
                    df_daily = pro.daily(ts_code=chunk_str, start_date=start_d, end_date=end_d, fields='ts_code,trade_date,high')
                    
                    if df_daily.empty:
                        continue
                        
                    # Process each stock in chunk
                    for code in chunk:
                        df_stock = df_daily[df_daily['ts_code'] == code]
                        if df_stock.empty:
                            continue
                        
                        # Check if we have the reference date data
                        rec_ref = df_stock[df_stock['trade_date'] == start_d]
                        if rec_ref.empty:
                            continue
                            
                        ref_high = rec_ref.iloc[0]['high']
                        
                        # Check subsequent highs
                        subsequent = df_stock[df_stock['trade_date'] > start_d]
                        if subsequent.empty:
                            final_candidates.append(code)
                            continue
                            
                        max_sub_high = subsequent['high'].max()
                        
                        if max_sub_high <= ref_high:
                            final_candidates.append(code)
                            
                except Exception as e:
                    print(f"Error processing chunk {i}: {e}")
            
            candidates = final_candidates

        if (consecutive_monthly_drop > 0 or price_below_date) and not candidates:
            return "No stocks found matching price criteria."

        # Step 3: MA Trend
        if ma_trend:
            if not candidates:
                return "No candidates to check MA trend for."
            
            # Fetch recent daily data for MA calculation
            # Need ~30 days for MA20
            cal_ma = pro.trade_cal(exchange='SSE', is_open='1', start_date=(datetime.datetime.now() - datetime.timedelta(days=60)).strftime('%Y%m%d'), end_date=datetime.datetime.now().strftime('%Y%m%d'))
            trade_dates = cal_ma.sort_values('cal_date').iloc[-30:]['cal_date'].tolist()
            start_ma = trade_dates[0]
            end_ma = trade_dates[-1]
            
            final_candidates = []
            chunk_size = 50
            
            for i in range(0, len(candidates), chunk_size):
                chunk = candidates[i:i+chunk_size]
                chunk_str = ",".join(chunk)
                
                try:
                    df_ma = pro.daily(ts_code=chunk_str, start_date=start_ma, end_date=end_ma, fields='ts_code,trade_date,close')
                    
                    for code in chunk:
                        df_s = df_ma[df_ma['ts_code'] == code].sort_values('trade_date')
                        if len(df_s) < 20:
                            continue
                            
                        s = df_s['close']
                        ma5 = s.rolling(5).mean().iloc[-1]
                        ma10 = s.rolling(10).mean().iloc[-1]
                        ma20 = s.rolling(20).mean().iloc[-1]
                        
                        if ma_trend == 'bear':
                            if ma5 < ma10 < ma20:
                                final_candidates.append(code)
                        elif ma_trend == 'bull':
                            if ma5 > ma10 > ma20:
                                final_candidates.append(code)
                except:
                    pass
            
            candidates = final_candidates

        if not candidates:
            return "No stocks found."
            
        # Step 4: Enrich Data (PE, Dividend, Fundamentals)
        # Basic Info
        df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
        df_basic = df_basic[df_basic['ts_code'].isin(candidates)]
        
        # Daily Basic (PE, PB, Dividend)
        cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=(datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y%m%d'), end_date=datetime.datetime.now().strftime('%Y%m%d'))
        latest_date = cal.iloc[-1]['cal_date']
        
        df_daily_basic = pro.daily_basic(trade_date=latest_date, fields='ts_code,pe,pe_ttm,dv_ratio,turnover_rate,close')
        
        # Fundamentals (ROE, Profit Growth)
        fina_data = []
        chunk_size = 50
        for i in range(0, len(candidates), chunk_size):
            chunk = candidates[i:i+chunk_size]
            codes_str = ",".join(chunk)
            try:
                # Query last available report (using a wide range to catch latest)
                # Usually 20240930 or 20240630
                df_fina = pro.fina_indicator(ts_code=codes_str, start_date='20240630', end_date='20251231', fields='ts_code,end_date,roe,profit_yoy')
                if not df_fina.empty:
                    # Sort by date desc and take first per stock
                    df_fina = df_fina.sort_values('end_date', ascending=False).drop_duplicates('ts_code')
                    fina_data.append(df_fina)
            except:
                pass
        
        if fina_data:
            df_fina_all = pd.concat(fina_data)
        else:
            df_fina_all = pd.DataFrame(columns=['ts_code', 'roe', 'profit_yoy'])

        # Merge all
        result = pd.merge(df_basic, df_daily_basic, on='ts_code', how='left')
        result = pd.merge(result, df_fina_all, on='ts_code', how='left')
        
        # Fill NaNs
        result = result.fillna('-')
        
        return result.to_json(orient="records", force_ascii=False)

    except Exception as e:
        return f"Error in market scan: {str(e)}"

if __name__ == "__main__":
    # Run the server using the standard stdio transport (default for MCP)
    mcp.run()
