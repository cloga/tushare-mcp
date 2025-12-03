import argparse
import os
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import tushare as ts

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.option_math import estimate_implied_vol

load_dotenv()
TOKEN = os.getenv("TUSHARE_TOKEN")
if not TOKEN:
    raise SystemExit("TUSHARE_TOKEN not set")

pro = ts.pro_api(TOKEN)

parser = argparse.ArgumentParser(description="Monthly wheel strategy backtest for ETF options")
parser.add_argument("--underlying", default=os.getenv("WHEEL_UNDERLYING", "159915.SZ"), help="Target ETF ts_code")
parser.add_argument(
    "--option-keyword",
    default=os.getenv("WHEEL_OPTION_KEYWORD", "创业板ETF"),
    help="Keyword used to filter opt_basic contract names",
)
parser.add_argument("--start-date", default=os.getenv("WHEEL_START_DATE", "20230101"), help="Backtest start date")
parser.add_argument(
    "--end-date",
    default=os.getenv("WHEEL_END_DATE"),
    help="Backtest end date (defaults to today)",
)
parser.add_argument(
    "--otm-min",
    type=float,
    default=float(os.getenv("WHEEL_OTM_MIN", 0.05)),
    help="Lower bound for OTM selection (fraction)",
)
parser.add_argument(
    "--otm-max",
    type=float,
    default=float(os.getenv("WHEEL_OTM_MAX", 0.10)),
    help="Upper bound for OTM selection (fraction)",
)
args = parser.parse_args()

UNDERLYING = args.underlying.upper()
OPTION_KEYWORD = args.option_keyword
START_DATE = args.start_date
END_DATE = args.end_date or datetime.now().strftime("%Y%m%d")
OTM_RANGE = (args.otm_min, args.otm_max)
EXCHANGE = "SSE" if UNDERLYING.endswith(".SH") else "SZSE"
TEMP_DIR = ROOT_DIR / "temp_data"
TEMP_DIR.mkdir(exist_ok=True)

trade_cal = pro.trade_cal(exchange=EXCHANGE, start_date=START_DATE, end_date=END_DATE, is_open="1")
trade_cal = pd.to_datetime(trade_cal["cal_date"])
trade_days = trade_cal.sort_values().tolist()
trade_day_set = set(trade_days)




def nearest_trade_day(date_str, direction="forward"):
    date = datetime.strptime(date_str, "%Y%m%d")
    delta = timedelta(days=1) if direction == "forward" else timedelta(days=-1)
    while True:
        date += delta
        if date < trade_days[0] or date > trade_days[-1]:
            return None
        if date in trade_day_set:
            return date.strftime("%Y%m%d")

def get_price_on_or_before(date_str, price_map):
    cursor = datetime.strptime(date_str, "%Y%m%d")
    while cursor.strftime("%Y%m%d") not in price_map:
        cursor -= timedelta(days=1)
        if cursor < trade_days[0]:
            return None
    return price_map[cursor.strftime("%Y%m%d")]

def load_etf_prices():
    """Load ETF prices, preferring fund_daily per Tushare doc 127."""
    df = pro.fund_daily(ts_code=UNDERLYING, start_date=START_DATE, end_date=END_DATE)
    if df.empty:
        df = pro.daily(ts_code=UNDERLYING, start_date=START_DATE, end_date=END_DATE)
    if df.empty:
        raise SystemExit(
            f"No price data returned for {UNDERLYING}. Check Tushare permissions (fund_daily/doc 127) or date range."
        )
    df = df.sort_values("trade_date")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    if df.empty:
        raise SystemExit("Price data contained no valid close values after cleaning.")
    return df


price_df = load_etf_prices()
price_map = dict(zip(price_df["trade_date"], price_df["close"]))

option_basic = pro.opt_basic(exchange=EXCHANGE)
if option_basic.empty:
    raise SystemExit("No option metadata returned from opt_basic(). Verify token permissions.")
option_basic = option_basic[option_basic["name"].str.contains(OPTION_KEYWORD, na=False)].copy()
if option_basic.empty:
    raise SystemExit(f"No option contracts matched keyword '{OPTION_KEYWORD}'. Adjust OPTION_KEYWORD.")
option_basic["exercise_price"] = pd.to_numeric(option_basic["exercise_price"], errors="coerce")
option_basic["per_unit"] = pd.to_numeric(option_basic["per_unit"], errors="coerce")
option_basic["list_date"] = pd.to_datetime(option_basic["list_date"], errors="coerce")
option_basic["delist_date"] = pd.to_datetime(option_basic["delist_date"], errors="coerce")
option_basic["maturity_date"] = pd.to_datetime(option_basic["maturity_date"], errors="coerce")
option_basic = option_basic.dropna(subset=["exercise_price", "per_unit", "maturity_date"])
if option_basic.empty:
    raise SystemExit("Option contract list empty after cleaning; verify permissions and OPTION_KEYWORD filter.")
contract_unit = option_basic["per_unit"].iloc[0]

month_starts = (
    price_df.assign(ts=pd.to_datetime(price_df["trade_date"]).dt.to_period("M"))
    .groupby("ts")
    .first()["trade_date"].tolist()
)

if not month_starts:
    raise SystemExit("Unable to determine monthly entry points from price data.")

def pick_option(trade_date, spot, option_type):
    date_dt = datetime.strptime(trade_date, "%Y%m%d")
    mask = (
        (option_basic["call_put"] == option_type)
        & (option_basic["list_date"] <= date_dt)
        & ((option_basic["delist_date"].isna()) | (option_basic["delist_date"] >= date_dt))
        & (option_basic["maturity_date"] > date_dt)
    )
    pool = option_basic.loc[mask].copy()
    if pool.empty:
        return None
    if option_type == "P":
        pool["otm"] = (spot - pool["exercise_price"]) / spot
        pool = pool[(pool["otm"] >= OTM_RANGE[0]) & (pool["otm"] <= OTM_RANGE[1])]
    else:
        pool["otm"] = (pool["exercise_price"] - spot) / spot
        pool = pool[(pool["otm"] >= OTM_RANGE[0]) & (pool["otm"] <= OTM_RANGE[1])]
    if pool.empty:
        return None
    pool = pool.sort_values(["maturity_date", "otm"], ascending=[True, True])
    return pool.iloc[0]

cached_option_prices = {}

def get_option_close(ts_code, trade_date):
    cache_key = (ts_code, trade_date)
    if cache_key in cached_option_prices:
        return cached_option_prices[cache_key]
    fields = "ts_code,trade_date,close,implied_vol"
    df = pro.opt_daily(ts_code=ts_code, trade_date=trade_date, fields=fields)
    if df.empty:
        next_day = nearest_trade_day(trade_date, direction="forward")
        if not next_day:
            return None
        df = pro.opt_daily(ts_code=ts_code, trade_date=next_day, fields=fields)
        trade_date = next_day if not df.empty else trade_date
    if df.empty:
        return None
    row = df.iloc[0]
    price = float(row["close"])
    implied_vol = row.get("implied_vol")
    if pd.notna(implied_vol):
        implied_vol = float(implied_vol)
    else:
        implied_vol = None
    cached_option_prices[cache_key] = (price, implied_vol)
    return cached_option_prices[cache_key]

state = {
    "cash": 0.0,
    "shares": 0,
    "max_margin": 0.0,
}

records = []

for month_start in month_starts:
    trade_date = month_start
    if trade_date not in price_map:
        continue
    spot = price_map[trade_date]
    position_type = "PUT" if state["shares"] == 0 else "CALL"
    option_type = "P" if state["shares"] == 0 else "C"
    chosen = pick_option(trade_date, spot, option_type)
    if chosen is None:
        continue
    strike = float(chosen["exercise_price"])
    maturity = chosen["maturity_date"].strftime("%Y%m%d")
    ts_code = chosen["ts_code"]
    opt_data = get_option_close(ts_code, trade_date)
    if opt_data is None:
        continue
    opt_price, implied_vol = opt_data
    if implied_vol is None:
        days_to_expiry = max((datetime.strptime(maturity, "%Y%m%d") - datetime.strptime(trade_date, "%Y%m%d")).days, 0)
        time_years = days_to_expiry / 365 if days_to_expiry > 0 else 0.0
        implied_vol = estimate_implied_vol(opt_price, spot, strike, time_years, option_type)
    premium = opt_price * contract_unit
    state["cash"] += premium
    if option_type == "P":
        state["max_margin"] = max(state["max_margin"], strike * contract_unit)
    expiry_price = get_price_on_or_before(maturity, price_map)
    if expiry_price is None:
        continue
    assigned = False
    if option_type == "P" and expiry_price < strike:
        cost = strike * contract_unit
        state["cash"] -= cost
        state["shares"] += contract_unit
        assigned = True
    elif option_type == "C" and expiry_price > strike:
        proceeds = strike * contract_unit
        state["cash"] += proceeds
        state["shares"] = max(0, state["shares"] - contract_unit)
        assigned = True
    if option_type == "P":
        otm_pct = (spot - strike) / spot
    else:
        otm_pct = (strike - spot) / spot

    # Value the current holdings at the most recent available price (expiry close)
    holding_value = state["shares"] * expiry_price
    portfolio_value = state["cash"] + holding_value

    records.append({
        "month": trade_date[:6],
        "type": position_type,
        "ts_code": ts_code,
        "trade_date": trade_date,
        "maturity": maturity,
        "strike": strike,
        "spot": spot,
        "otm_pct": otm_pct,
        "premium": premium,
        "expiry_price": expiry_price,
        "assigned": assigned,
        "cash_balance": state["cash"],
        "holding_value": holding_value,
        "portfolio_value": portfolio_value,
        "implied_vol": implied_vol,
    })

last_price = price_df.iloc[-1]["close"] if not price_df.empty else 0
ending_value = state["cash"] + state["shares"] * last_price
initial_spot = price_df.iloc[0]["close"] if not price_df.empty else 0
margin = state["max_margin"] if state["max_margin"] else contract_unit * initial_spot
elapsed_days = (datetime.strptime(END_DATE, "%Y%m%d") - datetime.strptime(START_DATE, "%Y%m%d")).days
annualized = None
roi = None
if margin > 0 and elapsed_days > 0:
    roi = ending_value / margin
    if roi > 0:
        annualized = (roi) ** (365 / elapsed_days) - 1

summary = pd.DataFrame(records)
print(f"Wheel Strategy Backtest for {UNDERLYING} ({OPTION_KEYWORD})")
print(f"Periods simulated: {len(summary)}")
print(f"Ending cash value: {ending_value:,.2f}")
print(f"Max margin required: {margin:,.2f}")
if roi is not None:
    print(f"Return on margin: {roi*100:.2f}%")
if annualized is not None:
    print(f"Approx annualized return: {annualized*100:.2f}%")
if summary.empty:
        print("No trades generated.")
else:
        print(summary.tail(10).to_string(index=False))

        # Build HTML report with all trades
        report_path = TEMP_DIR / "wheel_report.html"
        summary_display = summary.copy()
        summary_display["otm_pct"] = (summary_display["otm_pct"] * 100).round(2)
        summary_display["cash_balance"] = summary_display["cash_balance"].round(2)
        summary_display["holding_value"] = summary_display["holding_value"].round(2)
        summary_display["portfolio_value"] = summary_display["portfolio_value"].round(2)
        summary_display["implied_vol"] = summary_display["implied_vol"].apply(
            lambda v: round(v * 100, 2) if pd.notna(v) else None
        )
        summary_display = summary_display.rename(columns={
            "otm_pct": "otm_pct(%)",
            "implied_vol": "implied_vol(%)",
        })

        stats_rows = [
            f"<li>Underlying: {UNDERLYING}</li>",
            f"<li>Backtest window: {START_DATE} -> {END_DATE}</li>",
            f"<li>Trades executed: {len(summary)}</li>",
            f"<li>Ending value: {ending_value:,.2f}</li>",
            (f"<li>Return on margin: {roi*100:.2f}%</li>" if roi is not None else "<li>Return on margin: n/a</li>"),
            (f"<li>Annualized return: {annualized*100:.2f}%</li>" if annualized is not None else "<li>Annualized return: n/a</li>"),
        ]
        stats_html = "\n    ".join(stats_rows)

        html_content = f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
    <meta charset=\"UTF-8\" />
    <title>Wheel Strategy Backtest</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 24px; }}
        table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
        th, td {{ border: 1px solid #ccc; padding: 6px 8px; text-align: center; }}
        th {{ background: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>Wheel Strategy Backtest</h1>
    <ul>
        {stats_html}
    </ul>
    {summary_display.to_html(index=False, escape=False)}
</body>
</html>"""

        report_path.write_text(html_content, encoding="utf-8")
        print(f"Saved full trade log to {report_path}")

        columns = [
            "month",
            "type",
            "ts_code",
            "trade_date",
            "maturity",
            "strike",
            "spot",
            "otm_pct(%)",
            "premium",
            "implied_vol(%)",
            "expiry_price",
            "assigned",
            "cash_balance",
            "holding_value",
            "portfolio_value",
        ]
        rows_html = []
        for record in summary_display.to_dict(orient="records"):
            cells = []
            for col in columns:
                value = record.get(col, "")
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    value = ""
                cells.append(f"<td>{value}</td>")
            row_cells = "".join(cells)
            rows_html.append(f"<tr>{row_cells}</tr>")
        rows_block = "\n".join(rows_html)

        chart_labels = json.dumps(summary_display["trade_date"].tolist(), ensure_ascii=False)
        chart_cash = json.dumps(summary_display["cash_balance"].tolist())
        chart_holding = json.dumps(summary_display["holding_value"].tolist())
        chart_portfolio = json.dumps(summary_display["portfolio_value"].tolist())

        dashboard_path = TEMP_DIR / "wheel_dashboard.html"
        dashboard_html = f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
    <meta charset=\"UTF-8\" />
    <title>Wheel Strategy Interactive Dashboard</title>
    <link rel=\"stylesheet\" href=\"https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css\" />
    <style>
        body {{ font-family: Arial, sans-serif; margin: 24px; background: #fafafa; }}
        .panel {{ background: #fff; padding: 16px; margin-bottom: 24px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        #equityChart {{ max-height: 420px; }}
        table {{ width: 100%; }}
    </style>
</head>
<body>
    <div class=\"panel\">
        <h1>Wheel Strategy Interactive Dashboard</h1>
        <p>Underlying {UNDERLYING} · Window {START_DATE} → {END_DATE}</p>
    </div>
    <div class=\"panel\">
        <canvas id=\"equityChart\"></canvas>
    </div>
    <div class=\"panel\">
        <table id=\"tradesTable\">
            <thead>
                <tr>{''.join(f'<th>{col}</th>' for col in columns)}</tr>
            </thead>
            <tbody>
                {rows_block}
            </tbody>
        </table>
    </div>
    <script src=\"https://code.jquery.com/jquery-3.7.1.min.js\"></script>
    <script src=\"https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js\"></script>
    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>
    <script>
        const labels = {chart_labels};
        const cashSeries = {chart_cash};
        const holdingSeries = {chart_holding};
        const portfolioSeries = {chart_portfolio};

        const ctx = document.getElementById('equityChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels,
                datasets: [
                    {{ label: 'Cash', data: cashSeries, borderColor: '#1f77b4', fill: false }},
                    {{ label: 'Holding', data: holdingSeries, borderColor: '#ff7f0e', fill: false }},
                    {{ label: 'Portfolio', data: portfolioSeries, borderColor: '#2ca02c', fill: false, borderWidth: 2 }}
                ]
            }},
            options: {{
                responsive: true,
                interaction: {{ mode: 'index', intersect: false }},
                plugins: {{
                    tooltip: {{ callbacks: {{ label: (context) => `${{context.dataset.label}}: ${{context.formattedValue}}` }} }}
                }},
                scales: {{
                    y: {{ beginAtZero: false }}
                }}
            }}
        }});

        $(document).ready(function() {{
            $('#tradesTable').DataTable({{
                pageLength: 25,
                order: [[0, 'asc']]
            }});
        }});
    </script>
</body>
</html>"""

        dashboard_path.write_text(dashboard_html, encoding="utf-8")
        print(f"Saved interactive dashboard to {dashboard_path}")
