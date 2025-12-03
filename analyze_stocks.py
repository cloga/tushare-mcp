import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv
import datetime
import time

# Load environment variables
load_dotenv()
token = os.getenv("TUSHARE_TOKEN")
if not token:
    print("Error: TUSHARE_TOKEN not found in .env")
    exit(1)

pro = ts.pro_api(token)

print("Starting stock analysis...")

# 1. Identify stocks with 3 consecutive monthly negatives (Sep, Oct, Nov 2025)
# We fetch monthly data for the range
print("Fetching monthly data...")

# Get month-end trading dates for Sep, Oct, Nov 2025
# We can query trade_cal for each month end
target_months = ['202509', '202510', '202511']
month_end_dates = []

for m in target_months:
    start = m + '01'
    # Simple hack for end of month: start of next month - 1 day, or just ask trade_cal
    # Let's ask trade_cal for the whole month and take the last one
    if m == '202509': end = '20250930'
    elif m == '202510': end = '20251031'
    elif m == '202511': end = '20251130'
    
    cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start, end_date=end)
    # print(f"DEBUG: Month {m}, Cal rows: {len(cal)}")
    if not cal.empty:
        # print(f"DEBUG: First: {cal.iloc[0]['cal_date']}, Last: {cal.iloc[-1]['cal_date']}")
        # Data seems to be descending (latest first)
        # We want the month end, which is the latest date in the range
        # So we take the first row if it's descending, or check sorting
        cal = cal.sort_values('cal_date', ascending=True)
        month_end_dates.append(cal.iloc[-1]['cal_date'])

print(f"Month end dates: {month_end_dates}")

monthly_dfs = []
for d in month_end_dates:
    try:
        df = pro.monthly(trade_date=d, fields='ts_code,trade_date,open,close')
        monthly_dfs.append(df)
    except Exception as e:
        print(f"Error fetching monthly for {d}: {e}")

# Combine
if not monthly_dfs:
    print("Failed to fetch monthly data")
    exit(1)

df_monthly = pd.concat(monthly_dfs)

# Pivot to get columns by month
# We expect 3 months: 202509, 202510, 202511 (dates will be month-end)
# Let's just group by ts_code
groups = df_monthly.groupby('ts_code')

candidates_step1 = []
for code, group in groups:
    if len(group) < 3:
        continue
    
    # Sort by date
    group = group.sort_values('trade_date')
    
    # Check if we have the specific 3 months (roughly)
    # Or just check the last 3 records if they are consecutive months
    # The query asks for "monthly 3 consecutive negatives".
    # We'll assume the last 3 available months in this window.
    
    last_3 = group.tail(3)
    is_negative = (last_3['close'] < last_3['open']).all()
    
    if is_negative:
        candidates_step1.append(code)

print(f"Stocks with 3 consecutive monthly negatives: {len(candidates_step1)}")

if not candidates_step1:
    print("No stocks found matching monthly criteria.")
    exit()

# 2. Check Price < 2024-09-24 High
print("Fetching reference data (2024-09-24)...")
# 20240924 might not be a trading day? It was a Tuesday.
df_ref = pro.daily(trade_date='20240924', fields='ts_code,high')
if df_ref.empty:
    print("Warning: No data for 20240924. Trying next day.")
    df_ref = pro.daily(trade_date='20240925', fields='ts_code,high')

df_ref = df_ref.set_index('ts_code')

print("Fetching latest daily data...")
# Get latest trading date
# trade_cal returns future dates too. We need the latest date <= today.
today_str = datetime.datetime.now().strftime('%Y%m%d')
cal = pro.trade_cal(exchange='SSE', is_open='1', start_date='20251125', end_date=today_str)
cal = cal.sort_values('cal_date', ascending=True)

# Try dates backwards until we find data
latest_date = ""
df_latest = pd.DataFrame()

for d in reversed(cal['cal_date'].tolist()):
    print(f"Checking data for {d}...")
    df = pro.daily(trade_date=d, fields='ts_code,close')
    if not df.empty:
        latest_date = d
        df_latest = df
        print(f"Found latest data on: {latest_date}")
        break

if df_latest.empty:
    print("Error: Could not find any recent daily data.")
    exit(1)

df_latest = df_latest.set_index('ts_code')

candidates_step2 = []
for code in candidates_step1:
    if code in df_ref.index and code in df_latest.index:
        high_ref = df_ref.loc[code, 'high']
        close_latest = df_latest.loc[code, 'close']
        
        if close_latest < high_ref:
            candidates_step2.append(code)

print(f"Stocks also lower than 2024-09-24 high: {len(candidates_step2)}")

if not candidates_step2:
    print("No stocks found matching price criteria.")
    exit()

# 3. Check MA Bearish (MA5 < MA10 < MA20)
print("Checking MA trends (this may take a moment)...")

final_candidates = []
batch_size = 50
total = len(candidates_step2)

# We need to fetch history. To avoid 1000 API calls, we can fetch by date for all stocks and filter locally?
# But fetching 30 days of ALL stocks is heavy (30 * 5000 rows).
# If candidates_step2 is small (< 100), loop is fine.
# If large, maybe fetch by date is better.
# Let's see the count first. If > 200, we might hit rate limits with loops.
# Tushare limit is usually 120-500 calls/min depending on level.
# We'll try to be efficient.

# Optimization: Fetch daily data for ALL stocks for the last 25 trading days.
# This is ~25 API calls. Much better than looping 500 times.
start_date_ma = (datetime.datetime.strptime(latest_date, '%Y%m%d') - datetime.timedelta(days=40)).strftime('%Y%m%d')

# Get trade calendar for this period
cal_ma = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_date_ma, end_date=latest_date)
trade_dates = cal_ma['cal_date'].tolist()[-25:] # Last 25 trading days

# Fetch data for each date
daily_data_map = {} # date -> df
print(f"Fetching history for last {len(trade_dates)} days...")

for d in trade_dates:
    # We only need close price for the candidates
    # But API returns all. We can filter after.
    try:
        df = pro.daily(trade_date=d, fields='ts_code,close')
        # Filter for our candidates to save memory
        df = df[df['ts_code'].isin(candidates_step2)]
        daily_data_map[d] = df
    except Exception as e:
        print(f"Error fetching {d}: {e}")
    time.sleep(0.1) # slight delay

# Reconstruct history for each candidate
print("Calculating MAs...")
for code in candidates_step2:
    # Build series
    closes = []
    dates = []
    for d in trade_dates:
        if d in daily_data_map:
            row = daily_data_map[d][daily_data_map[d]['ts_code'] == code]
            if not row.empty:
                closes.append(row.iloc[0]['close'])
                dates.append(d)
    
    if len(closes) < 20:
        continue
        
    s = pd.Series(closes)
    ma5 = s.rolling(5).mean().iloc[-1]
    ma10 = s.rolling(10).mean().iloc[-1]
    ma20 = s.rolling(20).mean().iloc[-1]
    
    # Check bearish: MA5 < MA10 < MA20
    if ma5 < ma10 < ma20:
        final_candidates.append(code)

print(f"Final candidates count: {len(final_candidates)}")

# Get names
if final_candidates:
    print("\nResults:")
    # Fetch names in batches or all
    df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
    df_basic = df_basic.set_index('ts_code')
    
    results = []
    for code in final_candidates:
        name = df_basic.loc[code, 'name'] if code in df_basic.index else "Unknown"
        industry = df_basic.loc[code, 'industry'] if code in df_basic.index else "-"
        results.append({'code': code, 'name': name, 'industry': industry})
    
    # Print table
    print(f"{'Code':<10} {'Name':<10} {'Industry':<10}")
    print("-" * 40)
    for r in results:
        print(f"{r['code']:<10} {r['name']:<10} {r['industry']:<10}")
