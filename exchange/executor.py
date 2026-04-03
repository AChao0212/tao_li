"""
Hedge executor — manages delta-neutral spot + futures positions.

Supports two directions:
  - Positive carry: BUY spot + SELL futures (collect positive funding)
  - Negative carry: SELL spot (margin borrow) + BUY futures (collect negative funding)

All positions are fully hedged (delta-neutral). No directional exposure.
Position sizing ensures survival of ±30% price swings on BOTH legs independently.

Safety:
  - Both legs placed concurrently, results verified before marking open
  - If one leg fails, the other is immediately unwound with retry
  - Margin transfers use try/finally to guarantee cleanup
  - All orders verified for fill status after placement
"""

import asyncio
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

# Add parent dir so we can import db
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import save_position, delete_position, load_open_positions

from .client import BinanceClient, BinanceAPIError
from .margin import MarginSizeResult, SizingConfig, calc_safe_size, calc_position_for_amount
from .types import Order, OrderStatus, SymbolInfo

log = logging.getLogger(__name__)

# Timeout for order operations
ORDER_TIMEOUT = 10  # seconds
UNWIND_RETRIES = 3
UNWIND_RETRY_DELAY = 2  # seconds


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

    # Status: pending → open → closing → closed
    #         pending → failed (if open fails)
    #         open → failed_unwind (if close fails critically)
    status: str = "pending"
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
    def needs_attention(self) -> bool:
        return self.status in ("failed", "failed_unwind")

    @property
    def entry_notional(self) -> float:
        return self.futures_filled_qty * self.futures_avg_price

    @property
    def basis_spread(self) -> float:
        """Price difference between the two legs at entry."""
        if self.spot_avg_price and self.futures_avg_price:
            return abs(self.futures_avg_price - self.spot_avg_price) / self.spot_avg_price
        return 0.0


def _pos_to_db(pos: HedgePosition) -> dict:
    """Serialize position for DB storage."""
    return {
        "symbol": pos.symbol,
        "direction": pos.direction,
        "status": pos.status,
        "quantity": pos.quantity,
        "spot_filled_qty": pos.spot_filled_qty,
        "spot_avg_price": pos.spot_avg_price,
        "futures_filled_qty": pos.futures_filled_qty,
        "futures_avg_price": pos.futures_avg_price,
        "borrowed_asset": pos.borrowed_asset,
        "borrowed_qty": pos.borrowed_qty,
        "borrow_interest_rate": pos.borrow_interest_rate,
        "spot_collateral_usdt": pos.spot_collateral_usdt,
        "futures_collateral_usdt": pos.futures_collateral_usdt,
        "open_time": pos.open_time,
        "usdt_amount": pos.metadata.get("usdt_amount", 0),
        "updated_at": int(time.time() * 1000),
    }


def _db_to_pos(row: dict) -> HedgePosition:
    """Restore position from DB row."""
    return HedgePosition(
        symbol=row["symbol"],
        direction=row["direction"],
        status=row["status"],
        quantity=row["quantity"],
        spot_filled_qty=row["spot_filled_qty"],
        spot_avg_price=row["spot_avg_price"],
        futures_filled_qty=row["futures_filled_qty"],
        futures_avg_price=row["futures_avg_price"],
        borrowed_asset=row["borrowed_asset"],
        borrowed_qty=row["borrowed_qty"],
        borrow_interest_rate=row["borrow_interest_rate"],
        spot_collateral_usdt=row["spot_collateral_usdt"],
        futures_collateral_usdt=row["futures_collateral_usdt"],
        open_time=row["open_time"],
        metadata={"usdt_amount": row["usdt_amount"], "recovered": True},
    )


def _verify_order_fill(order: Order, context: str) -> None:
    """Verify a market order was actually filled. Raises if not."""
    if order.filled_qty <= 0:
        raise RuntimeError(
            f"{context}: Order not filled (status={order.status}, "
            f"filled_qty={order.filled_qty})"
        )
    if order.avg_price <= 0:
        raise RuntimeError(
            f"{context}: Order has zero avg_price (status={order.status}, "
            f"filled_qty={order.filled_qty})"
        )


class HedgeExecutor:
    def __init__(self, client: BinanceClient):
        self.client = client
        self.positions: dict[str, HedgePosition] = {}
        self.sizing_config = SizingConfig()
        self._locks: dict[str, asyncio.Lock] = {}

    def recover_positions(self):
        """Load positions from DB that were open when bot last ran."""
        rows = load_open_positions()
        for row in rows:
            pos = _db_to_pos(row)
            self.positions[pos.symbol] = pos
            log.warning(
                f"RECOVERED position: {pos.symbol} {pos.direction} "
                f"status={pos.status} qty={pos.futures_filled_qty}"
            )
        if rows:
            log.warning(f"Recovered {len(rows)} positions from DB — check and close if needed")

    def _get_lock(self, symbol: str) -> asyncio.Lock:
        if symbol not in self._locks:
            self._locks[symbol] = asyncio.Lock()
        return self._locks[symbol]

    async def estimate_slippage(self, symbol: str, usdt_amount: float) -> dict | None:
        """Estimate slippage from orderbook depth. Returns None if data unavailable."""
        try:
            spot_book, futures_book = await asyncio.gather(
                self.client.spot_orderbook(symbol, limit=20),
                self.client.futures_orderbook(symbol, limit=20),
            )
        except Exception as e:
            log.warning(f"Cannot fetch orderbook for {symbol}: {e}")
            return None

        def walk_book(levels: list[list[float]], amount_usdt: float) -> float | None:
            if not levels:
                return None
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
            return total_cost / total_qty if total_qty > 0 else None

        spot_ask = walk_book(spot_book["asks"], usdt_amount)
        spot_bid = walk_book(spot_book["bids"], usdt_amount)
        futures_ask = walk_book(futures_book["asks"], usdt_amount)
        futures_bid = walk_book(futures_book["bids"], usdt_amount)

        if not all([spot_ask, spot_bid, futures_ask, futures_bid]):
            log.warning(f"Thin orderbook for {symbol}, slippage estimate unreliable")
            return None

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

        Uses per-symbol lock to prevent duplicate positions.
        Both legs are placed concurrently and verified before marking open.
        If one leg fails, the other is unwound with retry.
        """
        lock = self._get_lock(symbol)
        async with lock:
            if symbol in self.positions and self.positions[symbol].is_open:
                raise ValueError(f"Already have open position on {symbol}")

            # Get current price and validate
            price = await self.client.futures_price(symbol)
            if price <= 0:
                raise ValueError(f"Invalid price for {symbol}: {price}")

            # Get borrow interest rate for negative direction
            borrow_rate = 0.0
            if direction == "negative":
                # Check if symbol supports margin trading
                if not self.client.is_margin_tradable(symbol):
                    raise ValueError(f"{symbol} not available for margin trading")

                base_asset = symbol.replace("USDT", "")
                try:
                    borrow_rate = await self.client.margin_interest_rate(base_asset)
                except Exception as e:
                    log.warning(f"Cannot get borrow rate for {base_asset}, using default: {e}")
                    borrow_rate = 0.001

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

            if quantity <= 0:
                raise ValueError(f"Quantity rounds to zero for {symbol}")

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

                # Persist to DB for crash recovery
                save_position(_pos_to_db(pos))

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
        results = await asyncio.gather(
            self.client.spot_market_buy(symbol, quantity),
            self.client.futures_market_sell(symbol, quantity),
            return_exceptions=True,
        )

        spot_result, futures_result = results

        # Check for exceptions — record what filled before raising
        spot_ok = not isinstance(spot_result, Exception)
        futures_ok = not isinstance(futures_result, Exception)

        if spot_ok:
            pos.spot_order = spot_result
            pos.spot_filled_qty = spot_result.filled_qty
            pos.spot_avg_price = spot_result.avg_price

        if futures_ok:
            pos.futures_order = futures_result
            pos.futures_filled_qty = futures_result.filled_qty
            pos.futures_avg_price = futures_result.avg_price

        # If one failed, raise (caller will unwind the filled leg)
        if not spot_ok:
            raise RuntimeError(f"Spot order failed: {spot_result}")
        if not futures_ok:
            raise RuntimeError(f"Futures order failed: {futures_result}")

        # Verify both actually filled
        _verify_order_fill(spot_result, f"Spot BUY {symbol}")
        _verify_order_fill(futures_result, f"Futures SELL {symbol}")

    async def _open_negative(self, pos: HedgePosition, symbol: str, quantity: float, borrow_rate: float):
        """
        Open negative carry: SELL spot (margin borrow) + BUY futures.

        Steps:
        1. Transfer USDT to margin account as collateral
        2. Execute both legs concurrently:
           a. margin_market_sell (auto-borrow + sell)
           b. futures_market_buy (long)
        3. Verify both filled, unwind if not
        """
        base_asset = symbol.replace("USDT", "")
        collateral = pos.spot_collateral_usdt
        transferred = False

        # Step 1: Transfer collateral
        try:
            await self.client.transfer_spot_to_margin("USDT", collateral)
            transferred = True
            log.info(f"Transferred {collateral:.2f} USDT to margin account")
        except BinanceAPIError as e:
            if e.code == -3041:
                transferred = True  # Already there
            else:
                raise

        try:
            # Step 2: Execute both legs
            results = await asyncio.gather(
                self.client.margin_market_sell(symbol, quantity, auto_borrow=True),
                self.client.futures_market_buy(symbol, quantity),
                return_exceptions=True,
            )

            margin_result, futures_result = results

            margin_ok = not isinstance(margin_result, Exception)
            futures_ok = not isinstance(futures_result, Exception)

            if margin_ok:
                pos.spot_order = margin_result
                pos.spot_filled_qty = margin_result.filled_qty
                pos.spot_avg_price = margin_result.avg_price
                pos.borrowed_asset = base_asset
                pos.borrowed_qty = margin_result.filled_qty
                pos.borrow_interest_rate = borrow_rate

            if futures_ok:
                pos.futures_order = futures_result
                pos.futures_filled_qty = futures_result.filled_qty
                pos.futures_avg_price = futures_result.avg_price

            if not margin_ok:
                raise RuntimeError(f"Margin sell failed: {margin_result}")
            if not futures_ok:
                raise RuntimeError(f"Futures buy failed: {futures_result}")

            _verify_order_fill(margin_result, f"Margin SELL {symbol}")
            _verify_order_fill(futures_result, f"Futures BUY {symbol}")

        except Exception:
            # If open failed after transfer, try to get collateral back
            if transferred and pos.spot_filled_qty == 0:
                await self._recover_margin_usdt()
            raise

    async def close_hedge(self, pos: HedgePosition) -> HedgePosition:
        """Close a hedged position, both legs simultaneously."""
        if not pos.is_open:
            raise ValueError(f"Position {pos.symbol} not open (status={pos.status})")

        pos.status = "closing"
        errors = []

        try:
            if pos.direction == "positive":
                errors = await self._close_positive(pos)
            else:
                errors = await self._close_negative(pos)

            if errors:
                pos.status = "failed_unwind"
                save_position(_pos_to_db(pos))
                log.error(f"Close {pos.symbol} had errors: {errors}")
            else:
                pos.status = "closed"
                pos.close_time = int(time.time() * 1000)
                if pos.symbol in self.positions:
                    del self.positions[pos.symbol]
                delete_position(pos.symbol)
                log.info(f"CLOSED {pos.direction} hedge: {pos.symbol}")

            return pos

        except Exception as e:
            log.error(f"Failed to close {pos.symbol}: {e}")
            pos.status = "failed_unwind"
            raise

    async def _close_positive(self, pos: HedgePosition) -> list[str]:
        """Close positive carry: SELL spot + BUY futures. Returns list of error messages."""
        errors = []
        tasks = {}

        if pos.spot_filled_qty > 0:
            tasks["spot"] = self.client.spot_market_sell(pos.symbol, pos.spot_filled_qty)
        if pos.futures_filled_qty > 0:
            tasks["futures"] = self.client.futures_market_buy(pos.symbol, pos.futures_filled_qty)

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        keys = list(tasks.keys())

        for key, result in zip(keys, results):
            if isinstance(result, Exception):
                errors.append(f"{key}: {result}")
                log.error(f"Error closing {key} leg on {pos.symbol}: {result}")
            else:
                _try_verify = True
                try:
                    _verify_order_fill(result, f"Close {key} {pos.symbol}")
                except RuntimeError as e:
                    errors.append(str(e))
                    _try_verify = False

                if _try_verify:
                    if key == "spot":
                        pos.close_spot_order = result
                    else:
                        pos.close_futures_order = result

        return errors

    async def _close_negative(self, pos: HedgePosition) -> list[str]:
        """
        Close negative carry: BUY back spot (auto-repay) + SELL futures.
        Always attempts margin cleanup regardless of order success.
        Returns list of error messages.
        """
        errors = []

        # Close both legs
        try:
            tasks = {}
            if pos.borrowed_qty > 0:
                tasks["margin"] = self.client.margin_market_buy(
                    pos.symbol, pos.borrowed_qty, auto_repay=True
                )
            if pos.futures_filled_qty > 0:
                tasks["futures"] = self.client.futures_market_sell(
                    pos.symbol, pos.futures_filled_qty
                )

            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            keys = list(tasks.keys())

            for key, result in zip(keys, results):
                if isinstance(result, Exception):
                    errors.append(f"{key}: {result}")
                    log.error(f"Error closing {key} leg on {pos.symbol}: {result}")
                else:
                    try:
                        _verify_order_fill(result, f"Close {key} {pos.symbol}")
                    except RuntimeError as e:
                        errors.append(str(e))
                        continue

                    if key == "margin":
                        pos.close_spot_order = result
                    else:
                        pos.close_futures_order = result

        finally:
            # ALWAYS attempt to transfer remaining USDT from margin back
            await self._recover_margin_usdt()

        return errors

    async def _recover_margin_usdt(self):
        """Transfer any remaining USDT from margin account back to spot."""
        try:
            margin_acct = await self.client.margin_account()
            for asset_info in margin_acct.get("userAssets", []):
                if asset_info["asset"] == "USDT":
                    free = float(asset_info.get("free", 0))
                    if free > 1:
                        await self.client.transfer_margin_to_spot("USDT", free)
                        log.info(f"Recovered {free:.2f} USDT from margin account")
                    break
        except Exception as e:
            log.warning(f"Failed to recover margin USDT: {e}")

    async def _unwind_partial(self, pos: HedgePosition):
        """
        Emergency unwind of partially filled legs with retry.
        Called when opening a hedge fails mid-way.
        """
        unwind_errors = []

        # Unwind spot/margin leg
        if pos.spot_filled_qty > 0:
            success = False
            for attempt in range(UNWIND_RETRIES):
                try:
                    if pos.direction == "positive":
                        log.warning(f"Unwinding spot buy on {pos.symbol} (attempt {attempt + 1})")
                        order = await self.client.spot_market_sell(pos.symbol, pos.spot_filled_qty)
                    else:
                        log.warning(f"Unwinding margin sell on {pos.symbol} (attempt {attempt + 1})")
                        order = await self.client.margin_market_buy(
                            pos.symbol, pos.spot_filled_qty, auto_repay=True
                        )
                    if order.filled_qty > 0:
                        success = True
                        break
                except Exception as e:
                    log.error(f"Unwind spot attempt {attempt + 1} failed: {e}")
                    if attempt < UNWIND_RETRIES - 1:
                        await asyncio.sleep(UNWIND_RETRY_DELAY)

            if not success:
                msg = f"CRITICAL: Failed to unwind spot leg on {pos.symbol} after {UNWIND_RETRIES} attempts"
                log.error(msg)
                unwind_errors.append(msg)

        # Unwind futures leg
        if pos.futures_filled_qty > 0:
            success = False
            for attempt in range(UNWIND_RETRIES):
                try:
                    log.warning(f"Unwinding futures on {pos.symbol} (attempt {attempt + 1})")
                    if pos.direction == "positive":
                        order = await self.client.futures_market_buy(pos.symbol, pos.futures_filled_qty)
                    else:
                        order = await self.client.futures_market_sell(pos.symbol, pos.futures_filled_qty)
                    if order.filled_qty > 0:
                        success = True
                        break
                except Exception as e:
                    log.error(f"Unwind futures attempt {attempt + 1} failed: {e}")
                    if attempt < UNWIND_RETRIES - 1:
                        await asyncio.sleep(UNWIND_RETRY_DELAY)

            if not success:
                msg = f"CRITICAL: Failed to unwind futures leg on {pos.symbol} after {UNWIND_RETRIES} attempts"
                log.error(msg)
                unwind_errors.append(msg)

        # Transfer margin USDT back
        if pos.direction == "negative":
            await self._recover_margin_usdt()

        if unwind_errors:
            pos.status = "failed_unwind"
            pos.metadata["unwind_errors"] = unwind_errors

    async def get_open_positions_summary(self) -> list[dict]:
        """Get summary of all open positions with current state."""
        summaries = []
        for symbol, pos in self.positions.items():
            if not pos.is_open:
                continue
            try:
                current_price = await self.client.futures_price(symbol)
                if current_price <= 0:
                    continue

                if pos.direction == "positive":
                    spot_pnl = (current_price - pos.spot_avg_price) * pos.spot_filled_qty
                    futures_pnl = (pos.futures_avg_price - current_price) * pos.futures_filled_qty
                else:
                    spot_pnl = (pos.spot_avg_price - current_price) * pos.spot_filled_qty
                    futures_pnl = (current_price - pos.futures_avg_price) * pos.futures_filled_qty

                net_unrealized = spot_pnl + futures_pnl
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
