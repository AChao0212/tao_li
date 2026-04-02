"""
Margin-safe position sizing for hedged funding rate arbitrage.

Calculates the maximum safe position size that survives a given price move
without triggering margin call on EITHER leg.

Key constraints:
  - Spot cross margin: margin level must stay >= SAFE_MARGIN_LEVEL (1.5)
    even if price moves against you by target_move (default 30%)
  - Futures: margin balance must stay above maintenance margin
    even if price moves against you by target_move
  - Since the two legs are on SEPARATE accounts, gains on one leg
    cannot offset losses on the other in real-time.

Math:

NEGATIVE arb (SHORT spot margin + LONG futures):
  Worst case for spot: price goes UP by move%
    - Liability after move = Q * P * (1 + move) + interest
    - Assets = sale_proceeds (Q*P) + collateral_usdt
    - Margin Level = Assets / Liability >= safe_level
    - collateral >= Q*P*(1+move)*safe_level - Q*P
    - collateral >= Q*P * ((1+move)*safe_level - 1)

  Worst case for futures: price goes DOWN by move%
    - Unrealized loss = Q * P * move
    - Initial margin = Q * P / leverage
    - Remaining = Q*P/L - Q*P*move
    - Must > maintenance margin = Q*P*(1-move) * maint_rate
    - Q*P * (1/L - move) > Q*P*(1-move)*maint_rate
    - 1/L > move + (1-move)*maint_rate
    - L < 1 / (move + (1-move)*maint_rate)

POSITIVE arb (BUY spot + SHORT futures):
  Spot: no margin risk (you own the coin)
    - Need Q*P USDT to buy

  Worst case for futures: price goes UP by move%
    - Unrealized loss (short) = Q * P * move
    - Same math as above but for shorts
"""

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

# Binance defaults
CROSS_MARGIN_LIQUIDATION_LEVEL = 1.1
FUTURES_MAINT_RATE_TIER1 = 0.004  # 0.4% for positions < 50k USDT


@dataclass
class MarginSizeResult:
    """Result of position sizing calculation."""
    quantity: float                         # Coin quantity for each leg
    notional_usdt: float                    # Q * price
    spot_collateral_needed: float           # USDT needed in margin account
    futures_collateral_needed: float        # USDT needed in futures account
    total_capital_needed: float             # Total USDT across both accounts
    max_safe_price_move: float              # The target move we sized for
    spot_margin_level_at_max_move: float    # Margin level if worst case hits
    futures_margin_balance_at_max_move: float  # Remaining margin if worst case hits
    estimated_daily_interest: float         # Borrow interest cost per day
    max_leverage: int                       # Safe leverage calculated
    is_feasible: bool
    reason: str


@dataclass
class SizingConfig:
    target_survival_move: float = 0.30      # Survive ±30% price swing
    safe_margin_level: float = 1.5          # Keep spot margin level above this
    futures_maint_rate: float = FUTURES_MAINT_RATE_TIER1
    max_leverage: int = 10                  # Upper bound, actual is calculated
    capital_buffer: float = 0.05            # Keep 5% extra as buffer


def calc_max_safe_leverage(move: float, maint_rate: float = FUTURES_MAINT_RATE_TIER1) -> int:
    """
    Calculate the maximum safe leverage for futures leg.
    Must survive a price move of `move` without liquidation.

    For LONG: price drops by move → loss = Q*P*move
    1/L > move + (1-move)*maint_rate
    L < 1 / (move + (1-move)*maint_rate)
    """
    denominator = move + (1 - move) * maint_rate
    if denominator <= 0:
        return 1
    max_lev = 1 / denominator
    # Floor to integer and be conservative
    return max(1, int(max_lev))


def calc_spot_collateral_ratio(move: float, safe_level: float = 1.5) -> float:
    """
    Calculate collateral needed per unit of notional for spot margin short.

    When shorting Q coins at price P:
    - You sell Q coins for Q*P USDT (this stays in margin account as asset)
    - If price rises to P*(1+move), your liability = Q*P*(1+move)
    - Margin Level = Total Assets / Total Liability >= safe_level
    - (Q*P + collateral) / (Q*P*(1+move)) >= safe_level
    - collateral >= Q*P * ((1+move)*safe_level - 1)

    Returns: collateral_ratio = collateral_needed / notional
    """
    return (1 + move) * safe_level - 1


def calc_safe_size(
    usdt_capital: float,
    price: float,
    direction: str,
    config: SizingConfig | None = None,
    borrow_interest_daily: float = 0.0005,  # 0.05% daily default
) -> MarginSizeResult:
    """
    Calculate the maximum safe hedged position size.

    Args:
        usdt_capital: Total USDT available across all accounts
        price: Current coin price
        direction: "positive" or "negative"
        config: Sizing parameters
        borrow_interest_daily: Daily borrow interest rate for the coin

    Returns:
        MarginSizeResult with all sizing details
    """
    if config is None:
        config = SizingConfig()

    move = config.target_survival_move
    safe_level = config.safe_margin_level
    maint_rate = config.futures_maint_rate

    # Calculate safe leverage
    safe_leverage = calc_max_safe_leverage(move, maint_rate)
    safe_leverage = min(safe_leverage, config.max_leverage)

    if direction == "negative":
        # SHORT spot margin + LONG futures
        # Spot collateral: need extra USDT beyond the sale proceeds
        spot_collateral_ratio = calc_spot_collateral_ratio(move, safe_level)
        # Futures collateral: initial margin
        futures_collateral_ratio = 1.0 / safe_leverage

        # Total capital per unit of notional
        # Note: spot SHORT gives you Q*P from the sale, but you also need collateral
        # Total needed = spot_collateral + futures_margin
        # The Q*P from selling stays in margin account as part of "assets"
        total_ratio = spot_collateral_ratio + futures_collateral_ratio

    elif direction == "positive":
        # BUY spot (full notional) + SHORT futures
        # Spot: need Q*P to buy the coin
        spot_collateral_ratio = 1.0  # Full purchase price
        # Futures: initial margin for short
        futures_collateral_ratio = 1.0 / safe_leverage
        total_ratio = spot_collateral_ratio + futures_collateral_ratio

    else:
        return MarginSizeResult(
            quantity=0, notional_usdt=0,
            spot_collateral_needed=0, futures_collateral_needed=0,
            total_capital_needed=0, max_safe_price_move=move,
            spot_margin_level_at_max_move=0, futures_margin_balance_at_max_move=0,
            estimated_daily_interest=0, max_leverage=1,
            is_feasible=False, reason=f"Unknown direction: {direction}",
        )

    # Apply buffer
    total_ratio_with_buffer = total_ratio * (1 + config.capital_buffer)

    # Max notional
    max_notional = usdt_capital / total_ratio_with_buffer
    quantity = max_notional / price

    # Actual collateral amounts
    spot_collateral = max_notional * spot_collateral_ratio
    futures_collateral = max_notional * futures_collateral_ratio
    total_needed = spot_collateral + futures_collateral

    # Verify: spot margin level at worst case
    if direction == "negative":
        # Assets at worst = sale_proceeds + collateral = max_notional + spot_collateral
        # Liability at worst = max_notional * (1 + move)
        worst_liability = max_notional * (1 + move)
        worst_assets = max_notional + spot_collateral  # sale proceeds + collateral
        spot_ml_worst = worst_assets / worst_liability if worst_liability > 0 else 999
    else:
        spot_ml_worst = 999  # No margin risk on spot for positive direction

    # Verify: futures margin at worst case
    futures_initial_margin = futures_collateral
    if direction == "negative":
        # LONG futures, price drops by move
        futures_loss = max_notional * move
        futures_maint = max_notional * (1 - move) * maint_rate
    else:
        # SHORT futures, price rises by move
        futures_loss = max_notional * move
        futures_maint = max_notional * (1 + move) * maint_rate

    futures_remaining = futures_initial_margin - futures_loss
    feasible = futures_remaining > futures_maint and (spot_ml_worst >= CROSS_MARGIN_LIQUIDATION_LEVEL)

    daily_interest = max_notional * borrow_interest_daily if direction == "negative" else 0

    reason = "OK"
    if not feasible:
        if futures_remaining <= futures_maint:
            reason = f"Futures would be liquidated: remaining={futures_remaining:.2f} < maint={futures_maint:.2f}"
        if spot_ml_worst < CROSS_MARGIN_LIQUIDATION_LEVEL:
            reason = f"Spot margin would liquidate: level={spot_ml_worst:.2f} < {CROSS_MARGIN_LIQUIDATION_LEVEL}"

    if quantity <= 0 or max_notional <= 0:
        feasible = False
        reason = "Insufficient capital"

    return MarginSizeResult(
        quantity=quantity,
        notional_usdt=max_notional,
        spot_collateral_needed=spot_collateral,
        futures_collateral_needed=futures_collateral,
        total_capital_needed=total_needed,
        max_safe_price_move=move,
        spot_margin_level_at_max_move=spot_ml_worst,
        futures_margin_balance_at_max_move=futures_remaining,
        estimated_daily_interest=daily_interest,
        max_leverage=safe_leverage,
        is_feasible=feasible,
        reason=reason,
    )


def calc_position_for_amount(
    target_usdt: float,
    price: float,
    direction: str,
    config: SizingConfig | None = None,
    borrow_interest_daily: float = 0.0005,
) -> MarginSizeResult:
    """
    Calculate position sizing for a specific target notional amount.
    Returns the capital needed (may exceed target if capital-inefficient).

    Use this when you know how much notional you want, and need to know
    the total capital required.
    """
    if config is None:
        config = SizingConfig()

    move = config.target_survival_move
    safe_level = config.safe_margin_level
    maint_rate = config.futures_maint_rate
    safe_leverage = min(calc_max_safe_leverage(move, maint_rate), config.max_leverage)

    if direction == "negative":
        spot_collateral_ratio = calc_spot_collateral_ratio(move, safe_level)
        futures_collateral_ratio = 1.0 / safe_leverage
    else:
        spot_collateral_ratio = 1.0
        futures_collateral_ratio = 1.0 / safe_leverage

    quantity = target_usdt / price
    spot_collateral = target_usdt * spot_collateral_ratio
    futures_collateral = target_usdt * futures_collateral_ratio
    total_needed = (spot_collateral + futures_collateral) * (1 + config.capital_buffer)

    # Verify margins at worst case (same as calc_safe_size)
    if direction == "negative":
        worst_liability = target_usdt * (1 + move)
        worst_assets = target_usdt + spot_collateral
        spot_ml_worst = worst_assets / worst_liability if worst_liability > 0 else 999
        futures_loss = target_usdt * move
        futures_maint = target_usdt * (1 - move) * maint_rate
    else:
        spot_ml_worst = 999
        futures_loss = target_usdt * move
        futures_maint = target_usdt * (1 + move) * maint_rate

    futures_remaining = futures_collateral - futures_loss
    feasible = futures_remaining > futures_maint and spot_ml_worst >= CROSS_MARGIN_LIQUIDATION_LEVEL

    daily_interest = target_usdt * borrow_interest_daily if direction == "negative" else 0

    reason = "OK" if feasible else "Position too large for given capital/params"

    return MarginSizeResult(
        quantity=quantity,
        notional_usdt=target_usdt,
        spot_collateral_needed=spot_collateral,
        futures_collateral_needed=futures_collateral,
        total_capital_needed=total_needed,
        max_safe_price_move=move,
        spot_margin_level_at_max_move=spot_ml_worst,
        futures_margin_balance_at_max_move=futures_remaining,
        estimated_daily_interest=daily_interest,
        max_leverage=safe_leverage,
        is_feasible=feasible,
        reason=reason,
    )
