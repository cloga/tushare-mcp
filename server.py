import os
from pathlib import Path
from dotenv import load_dotenv
import tushare as ts
from mcp.server.fastmcp import FastMCP

# Load environment variables from .env file in the same directory
env_path = Path(__file__).parent / '.env'
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

if __name__ == "__main__":
    # Run the server using the standard stdio transport (default for MCP)
    mcp.run()
