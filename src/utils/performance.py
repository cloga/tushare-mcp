"""Reusable performance metrics for portfolio equity curves."""
from __future__ import annotations

import math
from typing import Dict, Optional

import pandas as pd


def compute_performance_metrics(
    series: pd.Series,
    *,
    risk_free_rate: float = 0.02,
) -> Dict[str, Optional[float]]:
    base = {
        "total_return": None,
        "annualized_return": None,
        "annualized_vol": None,
        "sharpe": None,
        "max_drawdown": None,
    }
    if series is None:
        return base
    series = series.dropna()
    if series.empty:
        return base

    returns = series.pct_change().dropna()
    if returns.empty:
        return base

    total_return = series.iloc[-1] / series.iloc[0] - 1
    annualized_return = (1 + total_return) ** (252 / len(returns)) - 1 if len(returns) > 0 else None

    vol_daily = returns.std()
    annualized_vol = vol_daily * math.sqrt(252) if not pd.isna(vol_daily) else None

    sharpe = None
    if annualized_vol and annualized_vol != 0:
        sharpe = ((returns.mean() * 252) - risk_free_rate) / annualized_vol

    cumulative = (1 + returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = cumulative / running_max - 1
    max_drawdown = drawdown.min() if not drawdown.empty else None

    return {
        "total_return": round(total_return * 100, 2),
        "annualized_return": round(annualized_return * 100, 2) if annualized_return is not None else None,
        "annualized_vol": round(annualized_vol * 100, 2) if annualized_vol is not None else None,
        "sharpe": round(sharpe, 2) if sharpe is not None else None,
        "max_drawdown": round(max_drawdown * 100, 2) if max_drawdown is not None else None,
    }
