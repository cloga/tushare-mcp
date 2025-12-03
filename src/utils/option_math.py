"""Option-related math helpers shared across strategies."""
from __future__ import annotations

import math
from typing import Optional


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def black_scholes_price(
    spot: float,
    strike: float,
    time_years: float,
    rate: float,
    sigma: float,
    option_type: str,
) -> float:
    if time_years <= 0 or sigma <= 0 or spot <= 0 or strike <= 0:
        intrinsic = max(0.0, strike - spot if option_type == "P" else spot - strike)
        return intrinsic
    vol_term = sigma * math.sqrt(time_years)
    d1 = (math.log(spot / strike) + (rate + 0.5 * sigma**2) * time_years) / vol_term
    d2 = d1 - vol_term
    if option_type.upper() == "P":
        return strike * math.exp(-rate * time_years) * norm_cdf(-d2) - spot * norm_cdf(-d1)
    return spot * norm_cdf(d1) - strike * math.exp(-rate * time_years) * norm_cdf(d2)


def estimate_implied_vol(
    premium: float,
    spot: float,
    strike: float,
    time_years: float,
    option_type: str,
    rate: float = 0.02,
    iterations: int = 60,
) -> Optional[float]:
    if premium <= 0 or time_years <= 0 or spot <= 0 or strike <= 0:
        return None
    low, high = 1e-4, 5.0
    price_low = black_scholes_price(spot, strike, time_years, rate, low, option_type)
    price_high = black_scholes_price(spot, strike, time_years, rate, high, option_type)
    if premium < price_low or premium > price_high:
        return None
    for _ in range(iterations):
        mid = 0.5 * (low + high)
        price_mid = black_scholes_price(spot, strike, time_years, rate, mid, option_type)
        if abs(price_mid - premium) < 1e-4:
            return mid
        if price_mid > premium:
            high = mid
        else:
            low = mid
    return 0.5 * (low + high)
