"""
Risk manager — dual margin monitoring for hedged positions.

Monitors BOTH spot margin account and futures margin independently,
because gains on one leg cannot instantly offset losses on the other.

Safety thresholds:
  - Spot cross margin level: warn at 1.5, close at 1.4 (Binance liquidates at 1.1)
  - Futures margin ratio: close when maint_margin / margin_balance > 0.8
  - Emergency close: instantly unwind both legs if either breaches
"""

import logging
from dataclasses import dataclass

from .client import BinanceClient, BinanceAPIError
from .executor import HedgeExecutor, HedgePosition
from .margin import SizingConfig, calc_position_for_amount

log = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    # Position limits
    max_position_usdt: float = 200.0
    max_total_exposure_usdt: float = 500.0
    max_concurrent_positions: int = 5
    min_usdt_reserve: float = 50.0

    # Margin safety thresholds
    spot_margin_warn_level: float = 1.5     # Warn when spot margin level drops below
    spot_margin_close_level: float = 1.4    # Emergency close below this (Binance liquidates at 1.1)
    futures_margin_close_ratio: float = 0.8 # Close when maint/balance > this

    # Position sizing
    target_survival_move: float = 0.30      # Size to survive ±30% move
    max_leverage: int = 1                   # No leverage — safest for arb

    # Borrow cost guard
    max_borrow_interest_daily: float = 0.002  # Skip if daily interest > 0.2%


@dataclass
class MarginStatus:
    """Current margin health across both accounts."""
    # Spot cross margin
    spot_margin_level: float = 999.0   # Total assets / total liabilities
    spot_total_asset_btc: float = 0.0
    spot_total_liability_btc: float = 0.0
    spot_safe: bool = True

    # Futures
    futures_margin_balance: float = 0.0
    futures_maint_margin: float = 0.0
    futures_margin_ratio: float = 0.0   # maint / balance (0 = safe, 1 = liquidation)
    futures_safe: bool = True

    # Overall
    is_safe: bool = True
    needs_close: bool = False
    warning: str = ""


class RiskManager:
    def __init__(
        self,
        client: BinanceClient,
        executor: HedgeExecutor,
        config: RiskConfig | None = None,
        dry_run: bool = False,
    ):
        self.client = client
        self.executor = executor
        self.config = config or RiskConfig()
        self.dry_run = dry_run

        # Update executor's sizing config to match risk config
        self.executor.sizing_config = SizingConfig(
            target_survival_move=self.config.target_survival_move,
            max_leverage=self.config.max_leverage,
        )

    async def can_open_position(self, symbol: str, usdt_amount: float, direction: str = "negative") -> tuple[bool, str]:
        """Check if a new position is allowed. Returns (allowed, reason)."""
        # Concurrent positions
        open_count = sum(1 for p in self.executor.positions.values() if p.is_open)
        if open_count >= self.config.max_concurrent_positions:
            return False, f"Max concurrent positions reached ({open_count})"

        # Per-position limit
        if usdt_amount > self.config.max_position_usdt:
            return False, f"Amount {usdt_amount} exceeds max {self.config.max_position_usdt}"

        # Total exposure
        total_exposure = sum(
            p.metadata.get("usdt_amount", 0)
            for p in self.executor.positions.values()
            if p.is_open
        )
        if total_exposure + usdt_amount > self.config.max_total_exposure_usdt:
            return False, (
                f"Exposure would be {total_exposure + usdt_amount:.0f}, "
                f"max is {self.config.max_total_exposure_usdt:.0f}"
            )

        # Duplicate check
        if symbol in self.executor.positions and self.executor.positions[symbol].is_open:
            return False, f"Already have position on {symbol}"

        if self.dry_run:
            return True, "OK (dry run)"

        # Check borrow interest for negative direction
        if direction == "negative":
            base_asset = symbol.replace("USDT", "")
            try:
                rate = await self.client.margin_interest_rate(base_asset)
                if rate > self.config.max_borrow_interest_daily:
                    return False, (
                        f"Borrow interest too high: {rate:.4%}/day > "
                        f"max {self.config.max_borrow_interest_daily:.4%}/day"
                    )
            except Exception as e:
                log.warning(f"Cannot check borrow rate for {base_asset}: {e}")

        # Check capital sufficiency
        try:
            price = await self.client.futures_price(symbol)
            sizing = calc_position_for_amount(
                target_usdt=usdt_amount,
                price=price,
                direction=direction,
                config=self.executor.sizing_config,
            )
            if not sizing.is_feasible:
                return False, f"Sizing not feasible: {sizing.reason}"

            # Check we have enough USDT across accounts
            spot_bal = await self.client.spot_balances()
            futures_bal = await self.client.futures_balances()
            total_usdt = spot_bal.get("USDT", 0) + futures_bal.get("USDT", 0)

            needed = sizing.total_capital_needed + self.config.min_usdt_reserve
            if total_usdt < needed:
                return False, (
                    f"Insufficient capital: have {total_usdt:.2f} USDT, "
                    f"need {needed:.2f} (position {sizing.total_capital_needed:.2f} + "
                    f"reserve {self.config.min_usdt_reserve:.2f})"
                )

        except BinanceAPIError as e:
            return False, f"API error: {e}"
        except Exception as e:
            return False, f"Check failed: {e}"

        return True, "OK"

    async def check_margin_status(self) -> MarginStatus:
        """Check margin health on BOTH spot margin and futures accounts."""
        status = MarginStatus()

        # Check spot cross margin
        has_margin_positions = any(
            p.is_open and p.direction == "negative"
            for p in self.executor.positions.values()
        )

        if has_margin_positions:
            try:
                margin_acct = await self.client.margin_account()
                status.spot_margin_level = float(margin_acct.get("marginLevel", 999))
                status.spot_total_asset_btc = float(margin_acct.get("totalAssetOfBtc", 0))
                status.spot_total_liability_btc = float(margin_acct.get("totalLiabilityOfBtc", 0))

                if status.spot_margin_level < self.config.spot_margin_close_level:
                    status.spot_safe = False
                    status.needs_close = True
                    status.warning = (
                        f"SPOT MARGIN CRITICAL: level={status.spot_margin_level:.2f} "
                        f"< {self.config.spot_margin_close_level}"
                    )
                elif status.spot_margin_level < self.config.spot_margin_warn_level:
                    status.warning = (
                        f"Spot margin warning: level={status.spot_margin_level:.2f} "
                        f"< {self.config.spot_margin_warn_level}"
                    )
            except Exception as e:
                log.error(f"Failed to check spot margin: {e}")
                status.spot_safe = False
                status.warning = f"Cannot check spot margin: {e}"

        # Check futures margin
        has_futures_positions = any(
            p.is_open for p in self.executor.positions.values()
        )

        if has_futures_positions:
            try:
                futures_acct = await self.client.futures_account()
                status.futures_margin_balance = float(futures_acct.get("totalMarginBalance", 0))
                status.futures_maint_margin = float(futures_acct.get("totalMaintMargin", 0))

                if status.futures_margin_balance > 0:
                    status.futures_margin_ratio = (
                        status.futures_maint_margin / status.futures_margin_balance
                    )
                else:
                    status.futures_margin_ratio = 0

                if status.futures_margin_ratio > self.config.futures_margin_close_ratio:
                    status.futures_safe = False
                    status.needs_close = True
                    status.warning += (
                        f" | FUTURES MARGIN CRITICAL: ratio={status.futures_margin_ratio:.2f} "
                        f"> {self.config.futures_margin_close_ratio}"
                    )
            except Exception as e:
                log.error(f"Failed to check futures margin: {e}")
                status.futures_safe = False
                status.warning += f" | Cannot check futures margin: {e}"

        status.is_safe = status.spot_safe and status.futures_safe
        return status

    async def emergency_close_all(self, reason: str = "") -> int:
        """Close all open positions immediately. Returns count closed."""
        closed = 0
        log.warning(f"EMERGENCY CLOSE ALL: {reason}")

        for symbol, pos in list(self.executor.positions.items()):
            if pos.is_open:
                try:
                    await self.executor.close_hedge(pos)
                    closed += 1
                    log.warning(f"Emergency closed {symbol}")
                except Exception as e:
                    log.error(f"Failed to emergency close {symbol}: {e}")
                    # Try harder: close futures position directly
                    try:
                        if pos.futures_filled_qty > 0:
                            if pos.direction == "positive":
                                await self.client.futures_market_buy(symbol, pos.futures_filled_qty)
                            else:
                                await self.client.futures_market_sell(symbol, pos.futures_filled_qty)
                            log.warning(f"Force-closed futures leg on {symbol}")
                    except Exception as e2:
                        log.error(f"CRITICAL: Cannot close futures on {symbol}: {e2}")

        return closed

    async def run_safety_check(self) -> bool:
        """
        Periodic safety check. Returns True if all OK.
        Triggers emergency close if margin is unsafe on either side.
        """
        if self.dry_run:
            return True

        # Skip if no open positions
        if not any(p.is_open for p in self.executor.positions.values()):
            return True

        status = await self.check_margin_status()

        if status.warning:
            log.warning(f"Margin status: {status.warning}")

        if status.needs_close:
            log.error(f"MARGIN UNSAFE — triggering emergency close")
            await self.emergency_close_all(reason=status.warning)
            return False

        # Log health
        if any(p.is_open and p.direction == "negative" for p in self.executor.positions.values()):
            log.info(
                f"Margin health: spot_level={status.spot_margin_level:.2f} "
                f"futures_ratio={status.futures_margin_ratio:.4f}"
            )

        return True
