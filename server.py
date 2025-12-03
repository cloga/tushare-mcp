import os
from pathlib import Path
from dotenv import load_dotenv
import tushare as ts
import pandas as pd
import datetime
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
    """
    if not pro:
        return "Error: Tushare token not configured."
    
    try:
        candidates = []
        
        # Step 1: Monthly Drop Filter
        if consecutive_monthly_drop > 0:
            # Get last N months
            today = datetime.datetime.now()
            # We need to find the last N month-end dates
            # Simplified: Get monthly data for last N+1 months to be safe
            start_date = (today - datetime.timedelta(days=32 * (consecutive_monthly_drop + 1))).strftime('%Y%m%d')
            end_date = today.strftime('%Y%m%d')
            
            # Fetch monthly data for all stocks
            # Note: fetching by date range for all stocks might be heavy, but monthly is okay-ish.
            # Better: fetch by trade_date for the last N months.
            
            # Get month ends
            # Fetch more potential months to handle current incomplete month
            cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date, end_date=end_date)
            cal['cal_date'] = pd.to_datetime(cal['cal_date'])
            cal['ym'] = cal['cal_date'].dt.to_period('M')
            month_ends = cal.groupby('ym')['cal_date'].max().sort_values(ascending=False)
            
            # Take top N+3 candidates to be safe
            candidate_dates = [d.strftime('%Y%m%d') for d in month_ends.head(consecutive_monthly_drop + 3)]
            print(f"DEBUG: Candidate months: {candidate_dates}")
            
            monthly_dfs = []
            valid_dates_count = 0
            
            for d in candidate_dates:
                if valid_dates_count >= consecutive_monthly_drop:
                    break
                    
                try:
                    df = pro.monthly(trade_date=d, fields='ts_code,close,open')
                    if not df.empty:
                        monthly_dfs.append(df)
                        valid_dates_count += 1
                        print(f"DEBUG: Fetched {len(df)} rows for {d}")
                    else:
                        print(f"DEBUG: No data for {d}")
                except Exception as e:
                    print(f"DEBUG: Error fetching {d}: {e}")
                    pass
            
            if len(monthly_dfs) < consecutive_monthly_drop:
                return f"Error: Not enough monthly data found. Needed {consecutive_monthly_drop}, found {len(monthly_dfs)}."
                
            df_monthly = pd.concat(monthly_dfs)
            
            # Filter: For each stock, must appear in all N months and close < open
            # Pivot or Group
            # Let's just count negatives
            df_monthly['is_negative'] = df_monthly['close'] < df_monthly['open']
            
            # Group by ts_code
            stats = df_monthly.groupby('ts_code')['is_negative'].agg(['count', 'sum'])
            
            # Debug
            print(f"DEBUG: Total stocks in monthly data: {len(stats)}")
            print(f"DEBUG: Target count: {consecutive_monthly_drop}")
            
            # count should be N (data exists), sum should be N (all negative)
            # Note: 'sum' of boolean is count of True.
            valid_stocks = stats[(stats['count'] == consecutive_monthly_drop) & (stats['sum'] == consecutive_monthly_drop)].index.tolist()
            candidates = valid_stocks
            print(f"DEBUG: Candidates after monthly filter: {len(candidates)}")
        else:
            # If no monthly filter, start with all listed stocks? Too many.
            # We require at least one filter to start.
            # If only price_below_date is set, we fetch all stocks for that date?
            pass

        if consecutive_monthly_drop > 0 and not candidates:
            return "No stocks found matching monthly drop criteria."

        # Step 2: Price Comparison
        if price_below_date:
            # If candidates list is empty (and monthly filter was not used), we need to initialize it
            # But fetching all stocks is heavy. Let's assume we only run this if candidates exist or we fetch all.
            
            # Fetch reference high prices
            df_ref = pro.daily(trade_date=price_below_date, fields='ts_code,high')
            if df_ref.empty:
                # Try next day
                next_day = (datetime.datetime.strptime(price_below_date, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')
                df_ref = pro.daily(trade_date=next_day, fields='ts_code,high')
            
            df_ref = df_ref.set_index('ts_code')
            
            # Fetch latest prices
            # Get latest trading date
            # Robust logic: find the latest date that actually has daily data
            cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=(datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y%m%d'), end_date=datetime.datetime.now().strftime('%Y%m%d'))
            cal = cal.sort_values('cal_date', ascending=False)
            
            latest_date = ""
            df_latest = pd.DataFrame()
            
            for d in cal['cal_date'].tolist():
                try:
                    df = pro.daily(trade_date=d, fields='ts_code,close')
                    if not df.empty:
                        latest_date = d
                        df_latest = df
                        print(f"DEBUG: Found latest daily data on {latest_date}")
                        break
                except:
                    pass
            
            if df_latest.empty:
                return "Error: Could not find any recent daily data."
                
            df_latest = df_latest.set_index('ts_code')
            
            # Filter
            current_pool = candidates if candidates else df_ref.index.tolist()
            new_candidates = []
            
            for code in current_pool:
                if code in df_ref.index and code in df_latest.index:
                    if df_latest.loc[code, 'close'] < df_ref.loc[code, 'high']:
                        new_candidates.append(code)
            
            candidates = new_candidates

        if (consecutive_monthly_drop > 0 or price_below_date) and not candidates:
            return "No stocks found matching price criteria."

        # Step 3: MA Trend
        if ma_trend:
            # We need history for candidates
            if not candidates:
                return "No candidates to check MA trend for."
                
            # Fetch last 25 days for candidates
            # Optimization: Fetch daily data for ALL stocks for last 25 days is better than 1000 calls if candidates > 50
            # But if candidates is small (<50), loop is better.
            # Let's assume candidates list is moderate.
            
            cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=(datetime.datetime.now() - datetime.timedelta(days=60)).strftime('%Y%m%d'), end_date=datetime.datetime.now().strftime('%Y%m%d'))
            trade_dates = cal.sort_values('cal_date').iloc[-25:]['cal_date'].tolist()
            
            final_candidates = []
            
            # To be safe with API limits, let's fetch by date for the whole market (25 calls) and filter in memory
            # This is safer than N calls where N could be 1000.
            
            daily_data = {}
            for d in trade_dates:
                df = pro.daily(trade_date=d, fields='ts_code,close')
                # Filter for candidates
                df = df[df['ts_code'].isin(candidates)]
                daily_data[d] = df
            
            for code in candidates:
                closes = []
                for d in trade_dates:
                    if d in daily_data:
                        row = daily_data[d][daily_data[d]['ts_code'] == code]
                        if not row.empty:
                            closes.append(row.iloc[0]['close'])
                
                if len(closes) >= 20:
                    s = pd.Series(closes)
                    ma5 = s.rolling(5).mean().iloc[-1]
                    ma10 = s.rolling(10).mean().iloc[-1]
                    ma20 = s.rolling(20).mean().iloc[-1]
                    
                    if ma_trend == 'bear':
                        if ma5 < ma10 < ma20:
                            final_candidates.append(code)
                    elif ma_trend == 'bull':
                        if ma5 > ma10 > ma20:
                            final_candidates.append(code)
            
            candidates = final_candidates

        # Return results with names
        if not candidates:
            return "No stocks found."
            
        # Get names
        df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
        df_basic = df_basic[df_basic['ts_code'].isin(candidates)]
        
        return df_basic.to_json(orient="records", force_ascii=False)

    except Exception as e:
        return f"Error in market scan: {str(e)}"

if __name__ == "__main__":
    # Run the server using the standard stdio transport (default for MCP)
    mcp.run()
