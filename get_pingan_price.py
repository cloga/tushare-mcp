import os
from dotenv import load_dotenv
import tushare as ts
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
token = os.environ.get("TUSHARE_TOKEN")

if not token or token == "your_tushare_token_here":
    print("Please set a valid TUSHARE_TOKEN in .env file")
    exit(1)

try:
    pro = ts.pro_api(token)

    # 1. Find Ping An Bank (平安银行)
    # We know it's usually 000001.SZ, but let's search to be sure/demonstrate
    print("Searching for Ping An Bank...")
    df = pro.stock_basic(fields='ts_code,symbol,name')
    stock = df[df['name'] == '平安银行']

    if not stock.empty:
        stock_info = stock.iloc[0]
        ts_code = stock_info['ts_code']
        name = stock_info['name']
        print(f"Found: {name} ({ts_code})")

        # 2. Get recent daily prices
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        
        print(f"Fetching daily prices from {start_date} to {end_date}...")
        df_daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if not df_daily.empty:
            print(df_daily[['trade_date', 'open', 'high', 'low', 'close', 'vol']].to_string(index=False))
        else:
            print("No daily data found for this period (might be weekend or holiday).")
    else:
        print("Ping An Bank not found in stock_basic.")

except Exception as e:
    print(f"An error occurred: {e}")
