"""Reusable wheel strategy backtest logic shared across tools."""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils.option_math import estimate_implied_vol


@dataclass
class EquityPoint:
    date: str
    cash: float
    holding_value: float
    portfolio_value: float


class WheelBacktestError(RuntimeError):
    """Raised when the wheel strategy cannot complete."""


def _infer_exchange(ts_code: str) -> str:
    return "SSE" if ts_code.upper().endswith(".SH") else "SZSE"


def _load_price_history(pro, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df.empty:
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df.empty:
        raise WheelBacktestError(
            f"No price data returned for {ts_code}. Check Tushare permissions or the requested window."
        )
    df = df.sort_values("trade_date").copy()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    if df.empty:
        raise WheelBacktestError("Price data contained no usable close values after cleaning.")
    return df


def _load_option_chain(pro, exchange: str, underlying: str, fallback_name: str) -> pd.DataFrame:
    meta = pro.opt_basic(exchange=exchange)
    if meta.empty:
        raise WheelBacktestError(f"No option metadata returned for exchange {exchange}.")

    code_core = underlying.split(".")[0]
    filtered = pd.DataFrame()

    if "opt_code" in meta.columns:
        filtered = meta[meta["opt_code"].str.contains(code_core, na=False)]

    if filtered.empty:
        keywords = {
            fallback_name,
            fallback_name.replace("ETF", "").strip(),
            fallback_name.replace("ETF", "ETF期权").strip(),
            fallback_name.replace("上证", "").replace("深证", "").strip(),
        }
        for kw in keywords:
            if not kw:
                continue
            candidate = meta[meta["name"].str.contains(kw, case=False, na=False)]
            if not candidate.empty:
                filtered = candidate
                break

    if filtered.empty:
        raise WheelBacktestError(
            "No option contracts matched the underlying. Ensure the ETF has listed options or adjust your keyword."
        )

    filtered = filtered.copy()
    filtered["exercise_price"] = pd.to_numeric(filtered["exercise_price"], errors="coerce")
    filtered["per_unit"] = pd.to_numeric(filtered["per_unit"], errors="coerce")
    filtered["list_date"] = pd.to_datetime(filtered["list_date"], errors="coerce")
    filtered["delist_date"] = pd.to_datetime(filtered["delist_date"], errors="coerce")
    filtered["maturity_date"] = pd.to_datetime(filtered["maturity_date"], errors="coerce")
    filtered = filtered.dropna(subset=["exercise_price", "per_unit", "maturity_date"])
    if filtered.empty:
        raise WheelBacktestError("Option contract metadata incomplete after cleaning.")
    return filtered


def run_wheel_backtest(
    pro,
    *,
    underlying: str,
    start_date: str,
    end_date: str,
    otm_min: float,
    otm_max: float,
    initial_capital: float,
) -> Dict[str, Any]:
    """Execute the wheel strategy backtest and return detailed results."""
    if pro is None:
        raise WheelBacktestError("Tushare client not configured.")
    if not underlying:
        raise WheelBacktestError("underlying is required")
    if otm_min < 0 or otm_max <= otm_min:
        raise WheelBacktestError("Invalid OTM range; ensure 0 <= otm_min < otm_max")

    underlying = underlying.upper()
    exchange = _infer_exchange(underlying)

    price_df = _load_price_history(pro, underlying, start_date, end_date)
    price_map = dict(zip(price_df["trade_date"], price_df["close"]))

    fund_info = pro.fund_basic(ts_code=underlying)
    fallback_name = fund_info.iloc[0]["name"] if not fund_info.empty else underlying.split(".")[0]
    option_basic = _load_option_chain(pro, exchange, underlying, fallback_name)
    contract_unit = float(option_basic["per_unit"].iloc[0])

    cal = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open="1")
    cal = pd.to_datetime(cal["cal_date"])
    trade_days = cal.sort_values().tolist()
    if not trade_days:
        raise WheelBacktestError("No trading days returned for the selected window.")
    day_set = set(trade_days)

    months = (
        price_df.assign(ts=pd.to_datetime(price_df["trade_date"]).dt.to_period("M"))
        .groupby("ts")
        .first()["trade_date"].tolist()
    )
    if not months:
        raise WheelBacktestError("Unable to determine month entry dates from price data.")

    def nearest_trade_day(date_str: str, forward: bool = True) -> Optional[str]:
        current = _dt.datetime.strptime(date_str, "%Y%m%d")
        delta = _dt.timedelta(days=1 if forward else -1)
        while True:
            current += delta
            if current < trade_days[0] or current > trade_days[-1]:
                return None
            if current in day_set:
                return current.strftime("%Y%m%d")

    def price_on_or_before(date_str: str) -> Optional[float]:
        current = _dt.datetime.strptime(date_str, "%Y%m%d")
        while current.strftime("%Y%m%d") not in price_map:
            current -= _dt.timedelta(days=1)
            if current < trade_days[0]:
                return None
        return price_map[current.strftime("%Y%m%d")]

    cache: Dict[tuple[str, str], Tuple[float, Optional[float]]] = {}

    def option_close(ts_code: str, trade_date: str) -> Optional[Tuple[float, Optional[float]]]:
        key = (ts_code, trade_date)
        if key in cache:
            return cache[key]
        fields = "ts_code,trade_date,close,implied_vol"
        df = pro.opt_daily(ts_code=ts_code, trade_date=trade_date, fields=fields)
        if df.empty:
            next_day = nearest_trade_day(trade_date, forward=True)
            if next_day:
                df = pro.opt_daily(ts_code=ts_code, trade_date=next_day, fields=fields)
        if df.empty:
            return None
        row = df.iloc[0]
        price = float(row["close"])
        implied_vol = row.get("implied_vol")
        if pd.notna(implied_vol):
            implied_vol = float(implied_vol)
        else:
            implied_vol = None
        cache[key] = (price, implied_vol)
        return cache[key]

    def pick_option(trade_date: str, spot: float, option_type: str) -> Optional[pd.Series]:
        date_dt = _dt.datetime.strptime(trade_date, "%Y%m%d")
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
        else:
            pool["otm"] = (pool["exercise_price"] - spot) / spot
        pool = pool[(pool["otm"] >= otm_min) & (pool["otm"] <= otm_max)]
        if pool.empty:
            return None
        pool = pool.sort_values(["maturity_date", "otm"], ascending=[True, True])
        return pool.iloc[0]

    state = {"cash": float(initial_capital), "shares": 0, "max_margin": 0.0}
    trades: List[Dict[str, Any]] = []
    equity_curve: List[EquityPoint] = []

    for entry in months:
        if entry not in price_map:
            continue
        spot = price_map[entry]
        option_type = "P" if state["shares"] == 0 else "C"
        chosen = pick_option(entry, spot, option_type)
        if chosen is None:
            continue
        strike = float(chosen["exercise_price"])
        maturity = chosen["maturity_date"].strftime("%Y%m%d")
        ts_code = chosen["ts_code"]
        opt_data = option_close(ts_code, entry)
        if opt_data is None:
            continue
        opt_px, implied_vol = opt_data
        if implied_vol is None:
            days_to_expiry = max(
                (_dt.datetime.strptime(maturity, "%Y%m%d") - _dt.datetime.strptime(entry, "%Y%m%d")).days,
                0,
            )
            time_years = days_to_expiry / 365 if days_to_expiry > 0 else 0.0
            implied_vol = estimate_implied_vol(opt_px, spot, strike, time_years, option_type)
        premium = opt_px * contract_unit
        state["cash"] += premium
        if option_type == "P":
            state["max_margin"] = max(state["max_margin"], strike * contract_unit)

        expiry_price = price_on_or_before(maturity)
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

        holding_value = state["shares"] * spot
        portfolio_value = state["cash"] + holding_value
        equity_curve.append(
            EquityPoint(
                date=entry,
                cash=state["cash"],
                holding_value=holding_value,
                portfolio_value=portfolio_value,
            )
        )

        trades.append(
            {
                "month": entry[:6],
                "side": "PUT" if option_type == "P" else "CALL",
                "option_code": ts_code,
                "entry_date": entry,
                "expiry": maturity,
                "strike": strike,
                "spot": spot,
                "premium": premium,
                "expiry_price": expiry_price,
                "assigned": assigned,
                "cash_balance": state["cash"],
                "holding_value": holding_value,
                "portfolio_value": portfolio_value,
                "implied_vol": implied_vol,
            }
        )

    last_price = float(price_df.iloc[-1]["close"])
    ending_value = state["cash"] + state["shares"] * last_price
    margin = state["max_margin"] if state["max_margin"] else contract_unit * float(price_df.iloc[0]["close"])

    elapsed_days = (
        _dt.datetime.strptime(end_date, "%Y%m%d") - _dt.datetime.strptime(start_date, "%Y%m%d")
    ).days
    roi = None
    annualized = None
    if initial_capital > 0:
        roi = (ending_value - initial_capital) / initial_capital
        if roi is not None and elapsed_days > 0:
            annualized = (1 + roi) ** (365 / elapsed_days) - 1

    result: Dict[str, Any] = {
        "underlying": underlying,
        "start_date": start_date,
        "end_date": end_date,
        "periods": len(trades),
        "ending_value": ending_value,
        "cash": state["cash"],
        "shares": state["shares"],
        "last_price": last_price,
        "max_margin": margin,
        "return_on_capital": roi,
        "annualized_return": annualized,
        "trades": trades,
        "equity_curve": [point.__dict__ for point in equity_curve],
    }
    return result
