"""Multi-ETF portfolio backtest with monthly rebalancing."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import tushare as ts
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("TUSHARE_TOKEN")
if not TOKEN:
    raise SystemExit("TUSHARE_TOKEN not configured in environment or .env file.")

pro = ts.pro_api(TOKEN)

ETF_WEIGHTS = [
    ("511090.SH", 0.20),  # 30-year CGB ETF
    ("515080.SH", 0.10),  # CSI Dividend ETF
    ("159980.SZ", 0.03),  # Non-ferrous ETF
    ("159985.SZ", 0.03),  # Soybean meal ETF
    ("501018.SH", 0.04),  # Crude oil LOF
    ("510300.SH", 0.10),  # HS300 ETF
    ("513100.SH", 0.10),  # NASDAQ ETF
    ("159920.SZ", 0.10),  # Hang Seng ETF
    ("518880.SH", 0.10),  # Gold ETF
    ("511260.SH", 0.20),  # 10-year CGB ETF
]
ETF_WEIGHTS = [(code.upper(), weight) for code, weight in ETF_WEIGHTS]
WEIGHT_SUM = sum(weight for _, weight in ETF_WEIGHTS)
if abs(WEIGHT_SUM - 1.0) > 1e-6:
    raise SystemExit(f"Weights sum to {WEIGHT_SUM:.4f}, expected 1.0")

BENCHMARKS = [
    ("000300.SH", "沪深300"),
    ("000905.SH", "中证500"),
    ("399006.SZ", "创业板指"),
]

START_FETCH = "20050101"
END_FETCH = datetime.now().strftime("%Y%m%d")
INITIAL_CAPITAL = 100_000.0
RISK_FREE_RATE = 0.02  # annualized assumption for Sharpe ratio
OUTPUT_DIR = Path(__file__).parent
EQUITY_CSV = OUTPUT_DIR / "portfolio_equity_curve.csv"
REBALANCE_CSV = OUTPUT_DIR / "portfolio_rebalances.csv"
SUMMARY_JSON = OUTPUT_DIR / "portfolio_summary.json"
BENCHMARK_CSV = OUTPUT_DIR / "portfolio_vs_benchmarks.csv"


def load_price_series(ts_code: str) -> pd.DataFrame:
    df = pro.fund_daily(ts_code=ts_code, start_date=START_FETCH, end_date=END_FETCH)
    if df.empty:
        df = pro.daily(ts_code=ts_code, start_date=START_FETCH, end_date=END_FETCH)
    if df.empty:
        raise RuntimeError(f"No price data for {ts_code}")
    df = df.loc[:, ["trade_date", "close"]].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    return df.sort_values("trade_date")


def build_price_panel() -> pd.DataFrame:
    panel = None
    first_dates: Dict[str, pd.Timestamp] = {}
    last_dates: Dict[str, pd.Timestamp] = {}
    for code, _ in ETF_WEIGHTS:
        df = load_price_series(code)
        first_dates[code] = df["trade_date"].iloc[0]
        last_dates[code] = df["trade_date"].iloc[-1]
        df = df.rename(columns={"close": code})
        panel = df if panel is None else panel.merge(df, on="trade_date", how="outer")
    panel = panel.sort_values("trade_date").set_index("trade_date")
    panel = panel.ffill()
    start = max(first_dates.values())
    end = min(last_dates.values())
    panel = panel.loc[(panel.index >= start) & (panel.index <= end)]
    panel = panel.dropna()
    if panel.empty:
        raise RuntimeError("Price panel empty after alignment; check data availability.")
    return panel


def compute_month_starts(prices: pd.DataFrame) -> List[pd.Timestamp]:
    monthly = (
        prices.assign(month=prices.index.to_period("M"))
        .reset_index()
        .sort_values("trade_date")
    )
    firsts = monthly.groupby("month").first()["trade_date"]
    return firsts.tolist()


def load_benchmark_series(ts_code: str, start: datetime, end: datetime) -> pd.Series:
    df = pro.index_daily(ts_code=ts_code, start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"))
    if df.empty:
        raise RuntimeError(f"No index data for {ts_code}")
    df = df.loc[:, ["trade_date", "close"]].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").set_index("trade_date")
    df = df[df.index.to_series().between(start, end)]
    df = df["close"].dropna()
    return df


def compute_metrics(series: pd.Series) -> Dict[str, float | None]:
    returns = series.pct_change().dropna()
    if returns.empty:
        return {
            "total_return": None,
            "annualized_return": None,
            "annualized_vol": None,
            "sharpe": None,
            "max_drawdown": None,
        }
    total_return = series.iloc[-1] / series.iloc[0] - 1
    avg_daily = returns.mean()
    vol_daily = returns.std()
    annualized_return = (1 + total_return) ** (252 / len(returns)) - 1 if len(returns) > 0 else None
    annualized_vol = vol_daily * (252**0.5) if not pd.isna(vol_daily) else None
    sharpe = None
    if annualized_vol and annualized_vol != 0:
        sharpe = ((avg_daily * 252) - RISK_FREE_RATE) / annualized_vol
    cumulative = (1 + returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = cumulative / running_max - 1
    max_drawdown = drawdown.min()
    return {
        "total_return": round(total_return * 100, 2),
        "annualized_return": round(annualized_return * 100, 2) if annualized_return is not None else None,
        "annualized_vol": round(annualized_vol * 100, 2) if annualized_vol is not None else None,
        "sharpe": round(sharpe, 2) if sharpe is not None else None,
        "max_drawdown": round(max_drawdown * 100, 2) if max_drawdown is not None else None,
    }


def run_backtest(prices: pd.DataFrame) -> Dict[str, float]:
    holdings: Dict[str, float] = {code: 0.0 for code, _ in ETF_WEIGHTS}
    cash = INITIAL_CAPITAL
    equity_records = []
    rebalance_records = []

    month_starts = set(compute_month_starts(prices))

    for date, row in prices.iterrows():
        # Trigger rebalance on the first trading day of each month
        if date in month_starts:
            portfolio_value = cash + sum(holdings[code] * row[code] for code, _ in ETF_WEIGHTS)
            allocation = {}
            for code, weight in ETF_WEIGHTS:
                target_value = portfolio_value * weight
                holdings[code] = target_value / row[code]
                allocation[code] = target_value
            cash = 0.0
            rebalance_records.append({
                "date": date.strftime("%Y-%m-%d"),
                **{code: round(allocation[code], 2) for code in allocation},
            })
        equity_value = cash + sum(holdings[code] * row[code] for code, _ in ETF_WEIGHTS)
        equity_records.append({
            "date": date.strftime("%Y-%m-%d"),
            "portfolio_value": equity_value,
        })

    equity_df = pd.DataFrame(equity_records)
    equity_df.to_csv(EQUITY_CSV, index=False)
    pd.DataFrame(rebalance_records).to_csv(REBALANCE_CSV, index=False)
    equity_df["date"] = pd.to_datetime(equity_df["date"])
    portfolio_series = equity_df.set_index("date")["portfolio_value"]

    total_return = (portfolio_series.iloc[-1] / INITIAL_CAPITAL) - 1
    elapsed_days = (prices.index[-1] - prices.index[0]).days
    annualized = (1 + total_return) ** (365 / elapsed_days) - 1 if elapsed_days > 0 else None

    benchmark_curves = pd.DataFrame(index=portfolio_series.index)
    benchmark_curves["Portfolio"] = portfolio_series / portfolio_series.iloc[0]
    benchmark_metrics = {}
    for code, name in BENCHMARKS:
        try:
            series = load_benchmark_series(code, prices.index[0], prices.index[-1])
        except RuntimeError as exc:
            print(f"Warning: {exc}")
            continue
        series = series.reindex(portfolio_series.index).ffill().dropna()
        if series.empty:
            continue
        benchmark_curves[name] = series / series.iloc[0]
        benchmark_metrics[name] = compute_metrics(series)

    benchmark_curves.to_csv(BENCHMARK_CSV)

    summary = {
        "start_date": prices.index[0].strftime("%Y-%m-%d"),
        "end_date": prices.index[-1].strftime("%Y-%m-%d"),
        "initial_capital": INITIAL_CAPITAL,
        "ending_value": round(portfolio_series.iloc[-1], 2),
        "total_return": round(total_return * 100, 2),
        "annualized_return": round(annualized * 100, 2) if annualized is not None else None,
        "num_days": elapsed_days,
        "num_rebalances": len(rebalance_records),
        "portfolio_metrics": compute_metrics(portfolio_series),
        "benchmark_metrics": benchmark_metrics,
    }
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def main() -> None:
    print("Building price panel...")
    prices = build_price_panel()
    print(f"Aligned history: {prices.index[0].date()} -> {prices.index[-1].date()} ({len(prices)} trading days)")
    summary = run_backtest(prices)
    print("Backtest summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    print(f"Equity curve saved to {EQUITY_CSV}")
    print(f"Rebalance log saved to {REBALANCE_CSV}")
    print(f"Summary saved to {SUMMARY_JSON}")


if __name__ == "__main__":
    main()
