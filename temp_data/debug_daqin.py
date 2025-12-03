import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
token = os.environ.get("TUSHARE_TOKEN")
if not token:
    print("Error: TUSHARE_TOKEN not found in .env")
    exit(1)

pro = ts.pro_api(token)

ts_code = '601006.SH'

print(f"Checking data for {ts_code}...")

# 1. Check Daily Data since 20240924
df_daily = pro.daily(ts_code=ts_code, start_date='20240924')
df_daily = df_daily.sort_values('trade_date')

if df_daily.empty:
    print("No daily data found.")
else:
    print(f"Data range: {df_daily['trade_date'].min()} to {df_daily['trade_date'].max()}")
    
    # Check 20240924
    row_924 = df_daily[df_daily['trade_date'] == '20240924']
    if row_924.empty:
        print("20240924 data not found.")
        # Find closest
        print("First 5 rows:")
        print(df_daily.head())
    else:
        high_924 = row_924.iloc[0]['high']
        close_924 = row_924.iloc[0]['close']
        print(f"20240924 High: {high_924}, Close: {close_924}")
        
        # Check subsequent
        subsequent = df_daily[df_daily['trade_date'] > '20240924']
        if not subsequent.empty:
            max_high = subsequent['high'].max()
            max_date = subsequent.loc[subsequent['high'].idxmax(), 'trade_date']
            print(f"Max High since 20240924: {max_high} on {max_date}")
            
            if max_high <= high_924:
                print("PASS: Max high since 924 is <= 924 High")
            else:
                print("FAIL: Max high since 924 is > 924 High")

# 2. Check Monthly Data for last 4 months
# We need to know what "current date" implies for "last 4 months".
# Let's fetch the last 6 months of monthly data.
df_monthly = pro.monthly(ts_code=ts_code, limit=6)
df_monthly = df_monthly.sort_values('trade_date')
print("\nMonthly Data (Last 6 months):")
print(df_monthly[['trade_date', 'open', 'close']])

# Check if last 4 are negative (close < open)
if len(df_monthly) >= 4:
    last_4 = df_monthly.iloc[-4:]
    is_4_neg = all(row['close'] < row['open'] for _, row in last_4.iterrows())
    print(f"Last 4 months consecutive negative? {is_4_neg}")
    for _, row in last_4.iterrows():
        print(f"{row['trade_date']}: Open={row['open']}, Close={row['close']}, Drop={row['close'] < row['open']}")
