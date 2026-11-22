"""Underdog Reversal Strategy (from 5M, ported for 5m & 15m timeframes).

Targets extreme underdog mispricings (shares priced <= MAX_UNDERDOG_PRICE,
typically <= 7¢ / $0.07) offering asymmetric 14:1+ payout potential on mean-reversion:

1. Underdog Price Filter: Only considers sides priced <= max_underdog_price (default 7¢).
2. Strategy D (Volatility Cone Filter):
   Z-distance to strike: |spot - strike| / (spot * sigma_1m * sqrt(t)) <= max_sigma_distance (default 2.0σ).
   Ensures the strike is statistically reachable within normal volatility before expiry.
3. Strategy A (Model Dislocation / EV):
   - Fair continuous-time probability (GBM) >= min_underdog_prob (default 10%).
   - Expected Value: EV = fair_prob - ask_price >= min_underdog_ev (default 2¢).
4. Strategy C (Binance Momentum Reversal Hook):
   - For UP underdog: Binance spot momentum must be positive (> 0), confirming spot price has bounced upward.
   - For DOWN underdog: Binance spot momentum must be negative (< 0), confirming spot price has dropped downward.
   - Prevents catching falling knives before the bounce starts.
5. Strategy B: Convexity Scalper / Early Exit (TP/SL):
   - Take-profit exit at take_profit_cents (default 25¢).
   - Stop-loss exit at stop_loss_pct (default 50%).
"""

import math
from typing import Dict, Any, Optional

UNDERDOG_DEFAULTS: Dict[str, Any] = {
    "enabled": False,
    "max_price": 0.07,             # max ask price (7¢ / $0.07)
    "min_prob": 0.10,              # min continuous fair probability (10%)
    "min_ev": 0.02,                # min expected value (2¢ / $0.02)
    "max_sigma_distance": 2.0,     # max Z-distance to strike (volatility cone)
    "momentum_filter": True,       # require Binance spot momentum bounce
    "momentum_lookback_s": 10.0,   # lookback in seconds for momentum
    "min_book_liquidity_usd": 20.0,
    "tp_sl_enabled": True,         # Strategy B: Convexity Scalper / early exit
    "take_profit_cents": 25.0,     # take-profit exit target (25¢)
    "stop_loss_pct": 50.0          # stop-loss percentage of stake (50%)
}


def merged(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(UNDERDOG_DEFAULTS)
    if isinstance(cfg, dict):
        for k, v in cfg.items():
            if k in out and v is not None:
                out[k] = v
    return out


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("true", "1", "yes", "on"):
            return True
        if t in ("false", "0", "no", "off", ""):
            return False
    return bool(v)


def is_on(cfg: Optional[Dict[str, Any]]) -> bool:
    return _as_bool((cfg or {}).get("enabled", False), False)


def normalize(new_cfg: Any, existing_cfg: Any = None) -> Dict[str, Any]:
    base = merged(existing_cfg if isinstance(existing_cfg, dict) else {})
    if not isinstance(new_cfg, dict):
        return base
    out = dict(base)
    if "enabled" in new_cfg:
        out["enabled"] = _as_bool(new_cfg["enabled"], False)
    if "max_price" in new_cfg:
        out["max_price"] = max(0.01, min(0.50, float(new_cfg["max_price"])))
    if "min_prob" in new_cfg:
        out["min_prob"] = max(0.01, min(0.99, float(new_cfg["min_prob"])))
    if "min_ev" in new_cfg:
        out["min_ev"] = max(0.001, min(0.50, float(new_cfg["min_ev"])))
    if "max_sigma_distance" in new_cfg:
        out["max_sigma_distance"] = max(0.5, min(10.0, float(new_cfg["max_sigma_distance"])))
    if "momentum_filter" in new_cfg:
        out["momentum_filter"] = _as_bool(new_cfg["momentum_filter"], True)
    if "momentum_lookback_s" in new_cfg:
        out["momentum_lookback_s"] = max(1.0, min(60.0, float(new_cfg["momentum_lookback_s"])))
    if "min_book_liquidity_usd" in new_cfg:
        out["min_book_liquidity_usd"] = max(1.0, float(new_cfg["min_book_liquidity_usd"]))
    if "tp_sl_enabled" in new_cfg:
        out["tp_sl_enabled"] = _as_bool(new_cfg["tp_sl_enabled"], True)
    if "take_profit_cents" in new_cfg:
        out["take_profit_cents"] = max(5.0, min(99.0, float(new_cfg["take_profit_cents"])))
    if "stop_loss_pct" in new_cfg:
        out["stop_loss_pct"] = max(5.0, min(100.0, float(new_cfg["stop_loss_pct"])))
    return out


def status_line(cfg: Dict[str, Any]) -> str:
    c = merged(cfg)
    if not c["enabled"]:
        return "UNDERDOG REVERSAL: off (Standard EV Momentum active)"
    tp_str = f"TP {c['take_profit_cents']:.0f}¢ / SL {c['stop_loss_pct']:.0f}%" if c["tp_sl_enabled"] else "TP/SL off"
    mom_str = f"mom hook ({c['momentum_lookback_s']:.0f}s)" if c["momentum_filter"] else "no mom hook"
    return (f"UNDERDOG REVERSAL: on — max price {c['max_price']*100:.0f}¢, min prob {c['min_prob']*100:.0f}%, "
            f"min EV {c['min_ev']*100:.1f}¢, cone ≤{c['max_sigma_distance']:.1f}σ, {mom_str}, {tp_str}")


def calc_z_dist(spot: Optional[float], strike: Optional[float],
                sigma_1m: Optional[float], time_left_min: Optional[float]) -> Optional[float]:
    """Z-score distance: |spot - strike| normalized by expected dollar volatility over remaining time t.
    Works identically for 5m and 15m windows."""
    if not spot or not strike or not sigma_1m or spot <= 0 or strike <= 0 or sigma_1m <= 0:
        return None
    t = max(float(time_left_min or 0.0), 1.0 / 60.0)
    denom = spot * sigma_1m * math.sqrt(t)
    if denom <= 0:
        return None
    return abs(spot - strike) / denom


def _no_trade(reason: str, side=None, prob=None, price=None, ev=None, z_dist=None, momentum=None) -> Dict[str, Any]:
    return {
        "action": "NO_TRADE", "side": side, "phase": "UNDERDOG", "strength": "NONE",
        "reason": reason, "prob": prob, "price": price, "ev": ev,
        "z_dist": z_dist, "momentum": momentum
    }


def decide(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Underdog Reversal Decision: Strategy A + Strategy D + Strategy C.
    Supported on BOTH 5m and 15m timeframes."""
    p_up = inputs.get("mcProbUp")
    price_up = inputs.get("priceUp")       # ask (buy) price for the UP share, 0..1
    price_down = inputs.get("priceDown")   # ask (buy) price for the DOWN share, 0..1

    if p_up is None:
        return _no_trade("missing_model_data")

    p_down = 1.0 - p_up
    max_price = float(inputs.get("maxUnderdogPrice", 0.07))
    min_prob = float(inputs.get("minUnderdogProb", 0.10))
    min_ev = float(inputs.get("minUnderdogEv", 0.02))
    max_sigma_dist = float(inputs.get("maxSigmaDistance", 2.0))
    momentum_filter = bool(inputs.get("momentumFilter", True))

    spot = inputs.get("spotPrice")
    strike = inputs.get("strikePrice")
    sigma_1m = inputs.get("sigma1m")
    time_left_min = inputs.get("timeLeftMin")
    momentum = inputs.get("momentum")  # recent spot price change (positive = up, negative = down)

    z_dist = calc_z_dist(spot, strike, sigma_1m, time_left_min)

    # Filter for candidate underdogs priced <= max_price (e.g. <= 0.07 / 7¢)
    candidates = []

    # Check UP as underdog (market heavily favors DOWN, spot is below strike)
    if price_up is not None and price_up <= max_price:
        ev_up = p_up - price_up
        # UP reversal requires spot to bounce upward toward strike
        mom_up_ok = (momentum is None) or (momentum > 0)
        candidates.append(("UP", p_up, price_up, ev_up, z_dist, mom_up_ok, momentum))

    # Check DOWN as underdog (market heavily favors UP, spot is above strike)
    if price_down is not None and price_down <= max_price:
        ev_down = p_down - price_down
        # DOWN reversal requires spot to drop downward toward strike
        mom_down_ok = (momentum is None) or (momentum < 0)
        candidates.append(("DOWN", p_down, price_down, ev_down, z_dist, mom_down_ok, momentum))

    # If neither side is <= max_price, no underdog entry exists
    if not candidates:
        px_u = f"{price_up*100:.0f}¢" if price_up is not None else "None"
        px_d = f"{price_down*100:.0f}¢" if price_down is not None else "None"
        return _no_trade(f"no_underdog_below_{max_price*100:.0f}¢ (asks: {px_u} UP / {px_d} DOWN)",
                         prob=p_up, z_dist=z_dist, momentum=momentum)

    # Pick the best underdog candidate by highest expected value (EV)
    side, p, price, ev, z, mom_ok, mom = max(candidates, key=lambda c: c[3])

    # ── GATES ──
    # 1. Strategy D: Volatility Cone Gate (Strike must be reachable within max_sigma_dist)
    if z is not None and z > max_sigma_dist:
        return _no_trade(f"{side}_dist_{z:.1f}σ_outside_{max_sigma_dist:.1f}σ_cone",
                         side, p, price, ev, z, mom)

    # 2. Strategy A: Minimum Fair Probability Gate
    if p < min_prob:
        return _no_trade(f"{side}_prob_{p*100:.1f}%_below_{min_prob*100:.1f}%",
                         side, p, price, ev, z, mom)

    # 3. Strategy A: Minimum Expected Value Gate
    if ev < min_ev:
        return _no_trade(f"{side}_ev_{ev*100:.1f}¢_below_{min_ev*100:.1f}¢",
                         side, p, price, ev, z, mom)

    # 4. Strategy C: Binance Momentum Reversal Hook
    if momentum_filter and not mom_ok:
        mom_txt = f"${mom:+.1f}" if mom is not None else "$0"
        return _no_trade(f"{side}_waiting_momentum_bounce ({mom_txt})",
                         side, p, price, ev, z, mom)

    strength = "HIGH_CONVICTION" if p >= 0.20 else "STRONG"
    return {
        "action": "ENTER",
        "side": side,
        "phase": "UNDERDOG",
        "strength": strength,
        "prob": p,
        "price": price,
        "ev": ev,
        "z_dist": z,
        "momentum": mom,
        "reason": "underdog_reversal"
    }
