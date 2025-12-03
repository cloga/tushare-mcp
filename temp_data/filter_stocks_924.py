import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()
token = os.environ.get("TUSHARE_TOKEN")
if not token:
    print("Error: TUSHARE_TOKEN not found in .env")
    exit(1)

pro = ts.pro_api(token)

# Candidate stocks from the initial scan
candidates = [
    {"ts_code":"000403.SZ","name":"派林生物","industry":"生物制药"},
    {"ts_code":"000503.SZ","name":"国新健康","industry":"软件服务"},
    {"ts_code":"000513.SZ","name":"丽珠集团","industry":"化学制药"},
    {"ts_code":"000553.SZ","name":"安道麦A","industry":"农药化肥"},
    {"ts_code":"001201.SZ","name":"东瑞股份","industry":"农业综合"},
    {"ts_code":"002037.SZ","name":"保利联合","industry":"化工原料"},
    {"ts_code":"002081.SZ","name":"金螳螂","industry":"装修装饰"},
    {"ts_code":"002086.SZ","name":"东方海洋","industry":"渔业"},
    {"ts_code":"002096.SZ","name":"易普力","industry":"化工原料"},
    {"ts_code":"002124.SZ","name":"天邦食品","industry":"农业综合"},
    {"ts_code":"002221.SZ","name":"东华能源","industry":"石油加工"},
    {"ts_code":"002274.SZ","name":"华昌化工","industry":"农药化肥"},
    {"ts_code":"002332.SZ","name":"仙琚制药","industry":"化学制药"},
    {"ts_code":"002480.SZ","name":"新筑股份","industry":"运输设备"},
    {"ts_code":"002628.SZ","name":"成都路桥","industry":"建筑工程"},
    {"ts_code":"300010.SZ","name":"豆神教育","industry":"软件服务"},
    {"ts_code":"300040.SZ","name":"九洲集团","industry":"新型电力"},
    {"ts_code":"300070.SZ","name":"碧水源","industry":"环境保护"},
    {"ts_code":"300169.SZ","name":"天晟新材","industry":"塑料"},
    {"ts_code":"300284.SZ","name":"苏交科","industry":"建筑工程"},
    {"ts_code":"300344.SZ","name":"*ST立方","industry":"软件服务"},
    {"ts_code":"300348.SZ","name":"长亮科技","industry":"软件服务"},
    {"ts_code":"300404.SZ","name":"博济医药","industry":"医疗保健"},
    {"ts_code":"300656.SZ","name":"民德电子","industry":"IT设备"},
    {"ts_code":"300687.SZ","name":"赛意信息","industry":"软件服务"},
    {"ts_code":"300765.SZ","name":"新诺威","industry":"食品"},
    {"ts_code":"300865.SZ","name":"大宏立","industry":"专用机械"},
    {"ts_code":"300869.SZ","name":"康泰医学","industry":"医疗保健"},
    {"ts_code":"300894.SZ","name":"火星人","industry":"家用电器"},
    {"ts_code":"301004.SZ","name":"嘉益股份","industry":"家居用品"},
    {"ts_code":"301024.SZ","name":"霍普股份","industry":"建筑工程"},
    {"ts_code":"301030.SZ","name":"仕净科技","industry":"环境保护"},
    {"ts_code":"301063.SZ","name":"海锅股份","industry":"电气设备"},
    {"ts_code":"301078.SZ","name":"孩子王","industry":"其他商业"},
    {"ts_code":"301079.SZ","name":"邵阳液压","industry":"机械基件"},
    {"ts_code":"301090.SZ","name":"华润材料","industry":"化工原料"},
    {"ts_code":"301281.SZ","name":"科源制药","industry":"化学制药"},
    {"ts_code":"301591.SZ","name":"肯特股份","industry":"塑料"},
    {"ts_code":"301592.SZ","name":"六九一二","industry":"软件服务"},
    {"ts_code":"301602.SZ","name":"超研股份","industry":"医疗保健"},
    {"ts_code":"600048.SH","name":"保利发展","industry":"全国地产"},
    {"ts_code":"600073.SH","name":"光明肉业","industry":"食品"},
    {"ts_code":"600085.SH","name":"同仁堂","industry":"中成药"},
    {"ts_code":"600117.SH","name":"西宁特钢","industry":"特种钢"},
    {"ts_code":"600129.SH","name":"太极集团","industry":"中成药"},
    {"ts_code":"600187.SH","name":"国中水务","industry":"水务"},
    {"ts_code":"600316.SH","name":"洪都航空","industry":"航空"},
    {"ts_code":"600422.SH","name":"昆药集团","industry":"中成药"},
    {"ts_code":"600435.SH","name":"北方导航","industry":"专用机械"},
    {"ts_code":"600529.SH","name":"山东药玻","industry":"医疗保健"},
    {"ts_code":"600567.SH","name":"山鹰国际","industry":"造纸"},
    {"ts_code":"600606.SH","name":"绿地控股","industry":"全国地产"},
    {"ts_code":"600727.SH","name":"鲁北化工","industry":"化工原料"},
    {"ts_code":"600862.SH","name":"中航高科","industry":"航空"},
    {"ts_code":"601006.SH","name":"大秦铁路","industry":"铁路"},
    {"ts_code":"601668.SH","name":"中国建筑","industry":"建筑工程"},
    {"ts_code":"601800.SH","name":"中国交建","industry":"建筑工程"},
    {"ts_code":"601965.SH","name":"中国汽研","industry":"汽车服务"},
    {"ts_code":"603039.SH","name":"泛微网络","industry":"软件服务"},
    {"ts_code":"603117.SH","name":"万林物流","industry":"仓储物流"},
    {"ts_code":"603193.SH","name":"润本股份","industry":"日用化工"},
    {"ts_code":"603229.SH","name":"奥翔药业","industry":"化学制药"},
    {"ts_code":"603300.SH","name":"海南华铁","industry":"多元金融"},
    {"ts_code":"603707.SH","name":"健友股份","industry":"生物制药"},
    {"ts_code":"603879.SH","name":"永悦科技","industry":"化工原料"},
    {"ts_code":"603896.SH","name":"寿仙谷","industry":"中成药"},
    {"ts_code":"688076.SH","name":"ST诺泰","industry":"生物制药"},
    {"ts_code":"688091.SH","name":"上海谊众","industry":"化学制药"},
    {"ts_code":"688150.SH","name":"莱特光电","industry":"元器件"},
    {"ts_code":"688151.SH","name":"华强科技","industry":"医疗保健"},
    {"ts_code":"688170.SH","name":"德龙激光","industry":"专用机械"},
    {"ts_code":"688246.SH","name":"嘉和美康","industry":"软件服务"},
    {"ts_code":"688369.SH","name":"致远互联","industry":"软件服务"},
    {"ts_code":"688393.SH","name":"安必平","industry":"医疗保健"},
    {"ts_code":"688570.SH","name":"天玛智控","industry":"专用机械"},
    {"ts_code":"688793.SH","name":"倍轻松","industry":"家用电器"},
    {"ts_code":"920033.BJ","name":"康普化学","industry":"化工原料"},
    {"ts_code":"920061.BJ","name":"西磁科技","industry":"专用机械"},
    {"ts_code":"920098.BJ","name":"科隆新材","industry":"橡胶"},
    {"ts_code":"920128.BJ","name":"胜业电气","industry":"元器件"},
    {"ts_code":"920245.BJ","name":"威博液压","industry":"机械基件"},
    {"ts_code":"920414.BJ","name":"欧普泰","industry":"电器仪表"},
    {"ts_code":"920427.BJ","name":"华维设计","industry":"建筑工程"},
    {"ts_code":"920429.BJ","name":"康比特","industry":"食品"},
    {"ts_code":"920475.BJ","name":"三友科技","industry":"专用机械"},
    {"ts_code":"920489.BJ","name":"佳先股份","industry":"化工原料"},
    {"ts_code":"920924.BJ","name":"广脉科技","industry":"通信设备"},
    {"ts_code":"920964.BJ","name":"润农节水","industry":"建筑工程"},
    {"ts_code":"920985.BJ","name":"海泰新能","industry":"电气设备"}
]

filtered_stocks = []
print(f"Checking {len(candidates)} stocks...")

# Use a smaller chunk size to avoid hitting data limits for long date ranges
chunk_size = 20
ts_codes = [c['ts_code'] for c in candidates]

for i in range(0, len(ts_codes), chunk_size):
    chunk = ts_codes[i:i+chunk_size]
    codes_str = ",".join(chunk)
    
    try:
        # Get daily data from 20240924 to present
        # Note: 20240924 is the start date
        df = pro.daily(ts_code=codes_str, start_date='20240924', end_date='20251205')
        
        if df.empty:
            continue
            
        # Group by ts_code
        grouped = df.groupby('ts_code')
        
        for code in chunk:
            if code not in grouped.groups:
                continue
                
            stock_df = grouped.get_group(code).sort_values('trade_date')
            
            # Check if 20240924 exists in data
            row_924 = stock_df[stock_df['trade_date'] == '20240924']
            
            if row_924.empty:
                # If 20240924 is not available, try to find the closest date after
                after_924 = stock_df[stock_df['trade_date'] > '20240924']
                if after_924.empty:
                    continue
                # Use the first available date as reference if exact date missing?
                # Strict interpretation: if we can't verify 924 price, we might skip.
                # But let's use the first available date after 924 as the "base" if 924 is missing (e.g. suspension)
                # However, the requirement is "haven't exceeded 924 price".
                # If suspended on 924, the "price on 924" is effectively the pre-suspension price.
                # Let's skip for now to be safe/strict.
                continue
            
            high_924 = row_924.iloc[0]['high']
            close_924 = row_924.iloc[0]['close']
            
            # Check subsequent days
            subsequent = stock_df[stock_df['trade_date'] > '20240924']
            
            if subsequent.empty:
                # No data after 924
                continue
                
            max_high_since = subsequent['high'].max()
            latest_close = subsequent.iloc[-1]['close']
            
            # Condition: High since 924 <= High on 924
            if max_high_since <= high_924:
                # Calculate drawdown from 924 close
                drawdown_pct = (latest_close - close_924) / close_924 * 100
                
                stock_info = next(c for c in candidates if c['ts_code'] == code)
                stock_info['close_924'] = close_924
                stock_info['latest_close'] = latest_close
                stock_info['drawdown_924'] = round(drawdown_pct, 2)
                
                filtered_stocks.append(stock_info)
            else:
                # Debug print for first few failures to verify logic
                if i == 0 and len(filtered_stocks) == 0: 
                     print(f"Debug: {code} rejected. High 924: {high_924}, Max since: {max_high_since}")
                
    except Exception as e:
        print(f"Error processing chunk {i}: {e}")
        time.sleep(1)

print(f"Found {len(filtered_stocks)} stocks matching price criteria.")

# Fetch fundamental data for filtered stocks
if filtered_stocks:
    print("Fetching fundamental data...")
    filtered_codes = [s['ts_code'] for s in filtered_stocks]
    
    # Fetch fina_indicator for these stocks
    # We want the latest available report.
    # We can query a range covering the last few quarters and sort.
    # Since it's Dec 2025, we expect 20250930 data.
    
    # Batch query might be tricky if we want "latest" for each.
    # But we can query a large range and drop duplicates keeping last.
    
    # Let's try fetching 20250930 specifically first.
    # If some stocks haven't published Q3 yet (unlikely in Dec), we might miss them.
    # So we query a range.
    
    # Split into chunks for fina_indicator as well
    chunk_size_fina = 50
    df_fina_all = pd.DataFrame()
    
    for i in range(0, len(filtered_codes), chunk_size_fina):
        chunk = filtered_codes[i:i+chunk_size_fina]
        codes_str = ",".join(chunk)
        try:
            # Fetch data from 20250630 to 20251231 to cover Q2 and Q3
            df_chunk = pro.fina_indicator(ts_code=codes_str, start_date='20250630', end_date='20251231', fields='ts_code,end_date,roe,profit_yoy,q_profit_yoy')
            if not df_chunk.empty:
                df_fina_all = pd.concat([df_fina_all, df_chunk])
        except Exception as e:
            print(f"Error fetching fina data for chunk {i}: {e}")
            time.sleep(1)
            
    if not df_fina_all.empty:
        # Sort by end_date descending and keep first per ts_code
        df_fina_all = df_fina_all.sort_values('end_date', ascending=False).drop_duplicates('ts_code')
        
        # Merge
        for stock in filtered_stocks:
            fina_row = df_fina_all[df_fina_all['ts_code'] == stock['ts_code']]
            if not fina_row.empty:
                stock['roe'] = fina_row.iloc[0]['roe']
                stock['profit_yoy'] = fina_row.iloc[0]['profit_yoy'] # Cumulative YoY
                stock['report_date'] = fina_row.iloc[0]['end_date']
            else:
                stock['roe'] = '-'
                stock['profit_yoy'] = '-'
                stock['report_date'] = '-'
    else:
        for stock in filtered_stocks:
            stock['roe'] = '-'
            stock['profit_yoy'] = '-'
            stock['report_date'] = '-'

# Print result table
print("| 代码 | 名称 | 行业 | 924收盘 | 最新收盘 | 924以来涨跌幅(%) | 报告期 | ROE(%) | 归母净利同比(%) |")
print("|---|---|---|---|---|---|---|---|---|")
for stock in filtered_stocks:
    print(f"| {stock['ts_code']} | {stock['name']} | {stock['industry']} | {stock['close_924']} | {stock['latest_close']} | {stock['drawdown_924']} | {stock.get('report_date','-')} | {stock.get('roe','-')} | {stock.get('profit_yoy','-')} |")

# Generate HTML Report
html_content = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>A股严选：月线四连阴且未破924高点</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f4f4f9; }
        h1 { color: #333; text-align: center; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); border-radius: 8px; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #007bff; color: white; cursor: pointer; position: sticky; top: 0; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        tr:hover { background-color: #f1f1f1; }
        .positive { color: #d9534f; font-weight: bold; }
        .negative { color: #5cb85c; font-weight: bold; }
        .info { margin-bottom: 20px; color: #666; }
    </style>
    <script>
        function sortTable(n) {
            var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
            table = document.getElementById("stockTable");
            switching = true;
            dir = "asc"; 
            while (switching) {
                switching = false;
                rows = table.rows;
                for (i = 1; i < (rows.length - 1); i++) {
                    shouldSwitch = false;
                    x = rows[i].getElementsByTagName("TD")[n];
                    y = rows[i + 1].getElementsByTagName("TD")[n];
                    var xContent = x.innerText.toLowerCase();
                    var yContent = y.innerText.toLowerCase();
                    var xNum = parseFloat(xContent);
                    var yNum = parseFloat(yContent);
                    
                    if (!isNaN(xNum) && !isNaN(yNum)) {
                        if (dir == "asc") {
                            if (xNum > yNum) { shouldSwitch = true; break; }
                        } else if (dir == "desc") {
                            if (xNum < yNum) { shouldSwitch = true; break; }
                        }
                    } else {
                        if (dir == "asc") {
                            if (xContent > yContent) { shouldSwitch = true; break; }
                        } else if (dir == "desc") {
                            if (xContent < yContent) { shouldSwitch = true; break; }
                        }
                    }
                }
                if (shouldSwitch) {
                    rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                    switching = true;
                    switchcount ++;      
                } else {
                    if (switchcount == 0 && dir == "asc") {
                        dir = "desc";
                        switching = true;
                    }
                }
            }
        }
    </script>
</head>
<body>
    <div class="container">
        <h1>A股严选结果</h1>
        <div class="info">
            <p><strong>筛选条件：</strong></p>
            <ul>
                <li>最近四个月月线四连阴</li>
                <li>均线空头排列</li>
                <li>20240924以来股价未曾高于当日高点</li>
            </ul>
            <p>共找到 <strong>""" + str(len(filtered_stocks)) + """</strong> 只符合条件的股票。</p>
        </div>
        <table id="stockTable">
            <thead>
                <tr>
                    <th onclick="sortTable(0)">代码</th>
                    <th onclick="sortTable(1)">名称</th>
                    <th onclick="sortTable(2)">行业</th>
                    <th onclick="sortTable(3)">924收盘</th>
                    <th onclick="sortTable(4)">最新收盘</th>
                    <th onclick="sortTable(5)">924以来回撤(%)</th>
                    <th onclick="sortTable(6)">报告期</th>
                    <th onclick="sortTable(7)">ROE(%)</th>
                    <th onclick="sortTable(8)">归母净利同比(%)</th>
                </tr>
            </thead>
            <tbody>
"""

for stock in filtered_stocks:
    drawdown = stock.get('drawdown_924', 0)
    dd_class = "positive" if drawdown > 0 else "negative"
    
    profit_yoy = stock.get('profit_yoy', '-')
    py_class = ""
    if profit_yoy != '-':
        py_class = "positive" if float(profit_yoy) > 0 else "negative"

    html_content += f"""
            <tr>
                <td>{stock['ts_code']}</td>
                <td>{stock['name']}</td>
                <td>{stock['industry']}</td>
                <td>{stock.get('close_924', '-')}</td>
                <td>{stock.get('latest_close', '-')}</td>
                <td class="{dd_class}">{drawdown}</td>
                <td>{stock.get('report_date', '-')}</td>
                <td>{stock.get('roe', '-')}</td>
                <td class="{py_class}">{profit_yoy}</td>
            </tr>
    """

html_content += """
            </tbody>
        </table>
    </div>
</body>
</html>
"""

output_path = os.path.join('temp_data', 'strict_filter_report.html')
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"HTML report generated at: {output_path}")
