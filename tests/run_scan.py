from server import scan_market_opportunities
import json

# Call the function directly to test the logic
result = scan_market_opportunities(
    consecutive_monthly_drop=3,
    ma_trend="bear",
    price_below_date="20240924"
)

# The result is a JSON string or an error message
try:
    data = json.loads(result)
    print(f"Found {len(data)} stocks.")
    # Print table
    print(f"{'Code':<10} {'Name':<10} {'Industry':<10}")
    print("-" * 40)
    for item in data:
        print(f"{item['ts_code']:<10} {item['name']:<10} {item['industry']:<10}")
except json.JSONDecodeError:
    print(result)
except Exception as e:
    print(f"Error: {e}")
