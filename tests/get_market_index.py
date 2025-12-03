import os
from dotenv import load_dotenv
import tushare as ts
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
token = os.environ.get("TUSHARE_TOKEN")

if not token:
    print("Please set a valid TUSHARE_TOKEN in .env file")
    exit(1)

try:
    pro = ts.pro_api(token)

    # Common Indices:
    # 000001.SH - 上证指数 (SSE Composite)
    # 399001.SZ - 深证成指 (SZSE Component)
    # 399006.SZ - 创业板指 (ChiNext)
    indices = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指'
    }

    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
    
    print(f"Fetching index data from {start_date} to {end_date}...\n")

    for ts_code, name in indices.items():
        df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not df.empty:
            # Get the latest record (usually the first one if sorted by date desc, but let's sort to be safe)
            # Tushare usually returns desc date
            latest = df.iloc[0]
            print(f"=== {name} ({ts_code}) ===")
            print(f"Date: {latest['trade_date']}")
            print(f"Close: {latest['close']} ({latest['pct_chg']}%)")
            print(f"Open: {latest['open']}")
            print(f"High: {latest['high']}")
            print(f"Low: {latest['low']}")
            print("-" * 30)
        else:
            print(f"No data found for {name} ({ts_code})")

except Exception as e:
    print(f"An error occurred: {e}")
