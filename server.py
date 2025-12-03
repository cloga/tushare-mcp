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

if __name__ == "__main__":
    # Run the server using the standard stdio transport (default for MCP)
    mcp.run()
