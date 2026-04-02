"""
Hedge executor — manages delta-neutral spot + futures positions.

Supports two directions:
  - Positive carry: BUY spot + SELL futures (collect positive funding)
  - Negative carry: SELL spot (margin borrow) + BUY futures (collect negative funding)

All positions are fully hedged (delta-neutral). No directional exposure.
Position sizing ensures survival of ±30% price swings on BOTH legs independently.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

from .client import BinanceClient, BinanceAPIError
from .margin import MarginSizeResult, SizingConfig, calc_safe_size, calc_position_for_amount
from .types import Order, SymbolInfo

log = logging.getLogger(__name__)


@dataclass
class HedgePosition:
    """Tracks a paired spot + futures hedged position."""
    symbol: str
    direction: str                   # "positive" or "negative"
    quantity: float                  # Target coin quantity
    sizing: MarginSizeResult | None = None

    # Spot/margin leg
    spot_order: Order | None = None
    spot_filled_qty: float = 0.0
    spot_avg_price: float = 0.0

    # Futures leg
    futures_order: Order | None = None
    futures_filled_qty: float = 0.0
    futures_avg_price: float = 0.0

    # Margin borrow tracking (negative direction only)
    borrowed_asset: str = ""
    borrowed_qty: float = 0.0
    borrow_interest_rate: float = 0.0  # daily rate

    # Collateral allocation
    spot_collateral_usdt: float = 0.0
    futures_collateral_usdt: float = 0.0

    # Status
    status: str = "pending"          # pending, open, closing, closed, failed
    open_time: int = 0
    close_time: int = 0
    funding_collected: float = 0.0

    # Close leg orders
    close_spot_order: Order | None = None
    close_futures_order: Order | None = None

    metadata: dict = field(default_factory=dict)

    @property
    def is_open(self) -> bool:
        return self.status == "open"

    @property
    def entry_notional(self) -> float:
        return self.futures_filled_qty * self.futures_avg_price

    @property
    def basis_spread(self) -> float:
        """Price difference between the two legs at entry."""
        if self.spot_avg_price and self.futures_avg_price:
            return abs(self.futures_avg_price - self.spot_avg_price) / self.spot_avg_price
        return 0.0


class HedgeExecutor:
    def __init__(self, client: BinanceClient):
        self.client = client
        self.positions: dict[str, HedgePosition] = {}
        self.sizing_config = SizingConfig()

    async def estimate_slippage(self, symbol: str, usdt_amount: float) -> dict:
        """Estimate slippage from orderbook depth."""
        spot_book, futures_book = await asyncio.gather(
            self.client.spot_orderbook(symbol, limit=20),
            self.client.futures_orderbook(symbol, limit=20),
        )

        def walk_book(levels: list[list[float]], amount_usdt: float) -> float:
            total_cost = 0.0
            total_qty = 0.0
            remaining = amount_usdt
            for price, qty in levels:
                level_value = price * qty
                if level_value >= remaining:
                    total_cost += remaining
                    total_qty += remaining / price
                    break
                total_cost += level_value
                total_qty += qty
                remaining -= level_value
            return total_cost / total_qty if total_qty > 0 else (levels[0][0] if levels else 0)

        spot_ask = walk_book(spot_book["asks"], usdt_amount)
        spot_bid = walk_book(spot_book["bids"], usdt_amount)
        futures_ask = walk_book(futures_book["asks"], usdt_amount)
        futures_bid = walk_book(futures_book["bids"], usdt_amount)
        mid_spot = (spot_ask + spot_bid) / 2
        mid_futures = (futures_ask + futures_bid) / 2

        return {
            "spot_ask": spot_ask, "spot_bid": spot_bid,
            "futures_ask": futures_ask, "futures_bid": futures_bid,
            "spot_spread": (spot_ask - spot_bid) / mid_spot if mid_spot else 0,
            "futures_spread": (futures_ask - futures_bid) / mid_futures if mid_futures else 0,
        }

    async def open_hedge(
        self,
        symbol: str,
        direction: str,
        usdt_amount: float,
    ) -> HedgePosition:
        """
        Open a fully hedged position.

        For positive: BUY spot + SELL futures
        For negative: SELL spot (margin borrow) + BUY futures
        """
        if symbol in self.positions and self.positions[symbol].is_open:
            raise ValueError(f"Already have open position on {symbol}")

        # Get current price and calculate safe sizing
        price = await self.client.futures_price(symbol)

        # Get borrow interest rate for negative direction
        borrow_rate = 0.0
        if direction == "negative":
            base_asset = symbol.replace("USDT", "")
            try:
                borrow_rate = await self.client.margin_interest_rate(base_asset)
            except Exception as e:
                log.warning(f"Cannot get borrow rate for {base_asset}, using default: {e}")
                borrow_rate = 0.001  # 0.1% daily default (conservative)

        # Calculate position sizing
        sizing = calc_position_for_amount(
            target_usdt=usdt_amount,
            price=price,
            direction=direction,
            config=self.sizing_config,
            borrow_interest_daily=borrow_rate,
        )

        if not sizing.is_feasible:
            raise ValueError(f"Position not feasible: {sizing.reason}")

        # Round quantity to exchange precision
        futures_info = self.client.get_futures_info(symbol)
        quantity = sizing.quantity
        if futures_info:
            quantity = futures_info.round_qty(quantity)

        pos = HedgePosition(
            symbol=symbol,
            direction=direction,
            quantity=quantity,
            sizing=sizing,
            spot_collateral_usdt=sizing.spot_collateral_needed,
            futures_collateral_usdt=sizing.futures_collateral_needed,
            metadata={"usdt_amount": usdt_amount, "price_at_entry": price},
        )

        try:
            # Configure futures
            await self.client.futures_set_leverage(symbol, sizing.max_leverage)
            await self.client.futures_set_margin_type(symbol, "CROSSED")

            if direction == "positive":
                await self._open_positive(pos, symbol, quantity)
            else:
                await self._open_negative(pos, symbol, quantity, borrow_rate)

            pos.status = "open"
            pos.open_time = int(time.time() * 1000)
            self.positions[symbol] = pos

            log.info(
                f"OPENED {direction} hedge: {symbol} qty={quantity:.6f} "
                f"spot={pos.spot_avg_price:.4f} futures={pos.futures_avg_price:.4f} "
                f"basis={pos.basis_spread:.4%}"
            )
            return pos

        except Exception as e:
            pos.status = "failed"
            pos.metadata["error"] = str(e)
            log.error(f"Failed to open hedge on {symbol}: {e}")
            await self._unwind_partial(pos)
            raise

    async def _open_positive(self, pos: HedgePosition, symbol: str, quantity: float):
        """Open positive carry: BUY spot + SELL futures simultaneously."""
        spot_order, futures_order = await asyncio.gather(
            self.client.spot_market_buy(symbol, quantity),
            self.client.futures_market_sell(symbol, quantity),
        )
        pos.spot_order = spot_order
        pos.futures_order = futures_order
        pos.spot_filled_qty = spot_order.filled_qty
        pos.spot_avg_price = spot_order.avg_price
        pos.futures_filled_qty = futures_order.filled_qty
        pos.futures_avg_price = futures_order.avg_price

    async def _open_negative(self, pos: HedgePosition, symbol: str, quantity: float, borrow_rate: float):
        """
        Open negative carry: SELL spot (margin borrow) + BUY futures simultaneously.

        Steps:
        1. Transfer USDT to margin account as collateral
        2. Borrow coin + sell it via margin order (sideEffectType=MARGIN_BUY)
        3. Simultaneously buy futures (long)
        """
        base_asset = symbol.replace("USDT", "")

        # Step 1: Transfer collateral to margin account
        collateral = pos.spot_collateral_usdt
        try:
            await self.client.transfer_spot_to_margin("USDT", collateral)
            log.info(f"Transferred {collateral:.2f} USDT to margin account")
        except BinanceAPIError as e:
            # If already in margin account or transfer not needed, continue
            if e.code not in (-3041,):  # -3041 = already transferred
                raise

        # Step 2 + 3: Execute both legs concurrently
        # margin_market_sell with auto_borrow=True will borrow + sell in one call
        margin_order, futures_order = await asyncio.gather(
            self.client.margin_market_sell(symbol, quantity, auto_borrow=True),
            self.client.futures_market_buy(symbol, quantity),
        )

        pos.spot_order = margin_order
        pos.futures_order = futures_order
        pos.spot_filled_qty = margin_order.filled_qty
        pos.spot_avg_price = margin_order.avg_price
        pos.futures_filled_qty = futures_order.filled_qty
        pos.futures_avg_price = futures_order.avg_price
        pos.borrowed_asset = base_asset
        pos.borrowed_qty = margin_order.filled_qty
        pos.borrow_interest_rate = borrow_rate

    async def close_hedge(self, pos: HedgePosition) -> HedgePosition:
        """Close a hedged position, both legs simultaneously."""
        if not pos.is_open:
            raise ValueError(f"Position {pos.symbol} not open (status={pos.status})")

        pos.status = "closing"

        try:
            if pos.direction == "positive":
                await self._close_positive(pos)
            else:
                await self._close_negative(pos)

            pos.status = "closed"
            pos.close_time = int(time.time() * 1000)

            if pos.symbol in self.positions:
                del self.positions[pos.symbol]

            log.info(f"CLOSED {pos.direction} hedge: {pos.symbol}")
            return pos

        except Exception as e:
            log.error(f"Failed to close {pos.symbol}: {e}")
            pos.status = "failed"
            raise

    async def _close_positive(self, pos: HedgePosition):
        """Close positive carry: SELL spot + BUY futures."""
        tasks = []
        if pos.spot_filled_qty > 0:
            tasks.append(self.client.spot_market_sell(pos.symbol, pos.spot_filled_qty))
        if pos.futures_filled_qty > 0:
            tasks.append(self.client.futures_market_buy(pos.symbol, pos.futures_filled_qty))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                log.error(f"Error closing positive leg {i} on {pos.symbol}: {r}")
                raise r
            if i == 0 and pos.spot_filled_qty > 0:
                pos.close_spot_order = r
            else:
                pos.close_futures_order = r

    async def _close_negative(self, pos: HedgePosition):
        """
        Close negative carry: BUY back spot (auto-repay loan) + SELL futures.

        Steps:
        1. Simultaneously:
           a. margin_market_buy with auto_repay=True (buys coin + repays loan)
           b. futures_market_sell (close long)
        2. Transfer remaining USDT from margin back to spot
        """
        tasks = []
        if pos.borrowed_qty > 0:
            # Buy back the borrowed coins + auto repay
            tasks.append(
                self.client.margin_market_buy(pos.symbol, pos.borrowed_qty, auto_repay=True)
            )
        if pos.futures_filled_qty > 0:
            tasks.append(
                self.client.futures_market_sell(pos.symbol, pos.futures_filled_qty)
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                log.error(f"Error closing negative leg {i} on {pos.symbol}: {r}")
                raise r
            if i == 0 and pos.borrowed_qty > 0:
                pos.close_spot_order = r
            else:
                pos.close_futures_order = r

        # Transfer remaining USDT from margin back
        try:
            margin_acct = await self.client.margin_account()
            for asset_info in margin_acct.get("userAssets", []):
                if asset_info["asset"] == "USDT":
                    free = float(asset_info.get("free", 0))
                    if free > 1:  # Leave dust
                        await self.client.transfer_margin_to_spot("USDT", free)
                        log.info(f"Transferred {free:.2f} USDT from margin to spot")
                    break
        except Exception as e:
            log.warning(f"Failed to transfer margin USDT back: {e}")

    async def _unwind_partial(self, pos: HedgePosition):
        """Emergency unwind of partially filled legs."""
        # Unwind spot/margin leg
        if pos.spot_filled_qty > 0:
            try:
                if pos.direction == "positive":
                    log.warning(f"Unwinding partial spot buy on {pos.symbol}")
                    await self.client.spot_market_sell(pos.symbol, pos.spot_filled_qty)
                else:
                    log.warning(f"Unwinding partial margin sell on {pos.symbol}")
                    await self.client.margin_market_buy(
                        pos.symbol, pos.spot_filled_qty, auto_repay=True
                    )
            except Exception as e:
                log.error(f"Failed to unwind spot on {pos.symbol}: {e}")

        # Unwind futures leg
        if pos.futures_filled_qty > 0:
            try:
                log.warning(f"Unwinding partial futures on {pos.symbol}")
                if pos.direction == "positive":
                    await self.client.futures_market_buy(pos.symbol, pos.futures_filled_qty)
                else:
                    await self.client.futures_market_sell(pos.symbol, pos.futures_filled_qty)
            except Exception as e:
                log.error(f"Failed to unwind futures on {pos.symbol}: {e}")

        # Transfer margin USDT back if any was sent
        if pos.direction == "negative" and pos.spot_collateral_usdt > 0:
            try:
                await self.client.transfer_margin_to_spot("USDT", pos.spot_collateral_usdt)
            except Exception:
                pass

    async def get_open_positions_summary(self) -> list[dict]:
        """Get summary of all open positions with current state."""
        summaries = []
        for symbol, pos in self.positions.items():
            if not pos.is_open:
                continue
            try:
                current_price = await self.client.futures_price(symbol)

                # Delta-neutral: PnL comes from funding, not price movement
                # But track unrealized for monitoring
                if pos.direction == "positive":
                    # Long spot + short futures
                    spot_pnl = (current_price - pos.spot_avg_price) * pos.spot_filled_qty
                    futures_pnl = (pos.futures_avg_price - current_price) * pos.futures_filled_qty
                else:
                    # Short spot + long futures
                    spot_pnl = (pos.spot_avg_price - current_price) * pos.spot_filled_qty
                    futures_pnl = (current_price - pos.futures_avg_price) * pos.futures_filled_qty

                net_unrealized = spot_pnl + futures_pnl  # Should be ~0 for delta-neutral
                hold_hours = (time.time() * 1000 - pos.open_time) / 3_600_000
                est_interest = (
                    pos.entry_notional * pos.borrow_interest_rate * hold_hours / 24
                    if pos.direction == "negative" else 0
                )

                summaries.append({
                    "symbol": symbol,
                    "direction": pos.direction,
                    "quantity": pos.futures_filled_qty,
                    "entry_spot": pos.spot_avg_price,
                    "entry_futures": pos.futures_avg_price,
                    "current_price": current_price,
                    "spot_pnl": spot_pnl,
                    "futures_pnl": futures_pnl,
                    "net_unrealized": net_unrealized,
                    "basis_spread": pos.basis_spread,
                    "funding_collected": pos.funding_collected,
                    "est_interest_paid": est_interest,
                    "hold_hours": hold_hours,
                })
            except Exception as e:
                log.error(f"Error getting summary for {symbol}: {e}")
        return summaries
