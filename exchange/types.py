"""
Shared types for exchange interactions.
Designed to be exchange-agnostic for future reuse.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    BOTH = "BOTH"  # One-way mode


@dataclass
class Order:
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: float | None = None       # None for market orders
    order_id: str = ""
    client_order_id: str = ""
    status: OrderStatus = OrderStatus.NEW
    filled_qty: float = 0.0
    avg_price: float = 0.0
    commission: float = 0.0
    timestamp: int = 0
    raw: dict = field(default_factory=dict)  # Full exchange response


@dataclass
class Position:
    symbol: str
    side: PositionSide
    quantity: float               # Absolute quantity
    entry_price: float
    mark_price: float = 0.0
    unrealized_pnl: float = 0.0
    leverage: int = 1
    margin_type: str = "cross"    # "cross" or "isolated"
    raw: dict = field(default_factory=dict)


@dataclass
class SymbolInfo:
    """Exchange trading rules for a symbol."""
    symbol: str
    base_asset: str
    quote_asset: str
    price_precision: int          # Decimal places for price
    qty_precision: int            # Decimal places for quantity
    min_qty: float                # Minimum order quantity
    min_notional: float           # Minimum order value in quote asset
    step_size: float              # Quantity step size
    tick_size: float              # Price step size

    def round_qty(self, qty: float) -> float:
        """Round quantity to valid step size."""
        if self.step_size <= 0:
            return round(qty, self.qty_precision)
        steps = int(qty / self.step_size)
        return round(steps * self.step_size, self.qty_precision)

    def round_price(self, price: float) -> float:
        """Round price to valid tick size."""
        if self.tick_size <= 0:
            return round(price, self.price_precision)
        ticks = int(price / self.tick_size)
        return round(ticks * self.tick_size, self.price_precision)
