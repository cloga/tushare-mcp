import os
from dotenv import load_dotenv
import tushare as ts

# Load environment variables
load_dotenv()
token = os.environ.get("TUSHARE_TOKEN")

if not token:
    print("Please set a valid TUSHARE_TOKEN in .env file")
    exit(1)

try:
    pro = ts.pro_api(token)
    
    print("Fetching basic info for Ping An Bank...")
    # Fetch all and filter, or just fetch
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market')
    
    # Filter for Ping An Bank
    stock = df[df['name'] == '平安银行']
    
    if not stock.empty:
        print(stock.to_string(index=False))
    else:
        print("Ping An Bank not found.")

except Exception as e:
    print(f"An error occurred: {e}")
