"""
Async Binance API client for spot and USDT-M futures.

Handles request signing, rate limiting, and error handling.
Reusable across projects — just instantiate with API keys.

Usage:
    client = BinanceClient(api_key="...", api_secret="...")
    async with client:
        order = await client.spot_market_buy("BTCUSDT", quantity=0.001)
        order = await client.futures_market_sell("BTCUSDT", quantity=0.001)
"""

import asyncio
import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

import aiohttp

from .types import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionSide,
    SymbolInfo,
)

log = logging.getLogger(__name__)

SPOT_BASE = "https://api.binance.com"
FUTURES_BASE = "https://fapi.binance.com"

# Binance error codes that indicate transient failures (retry-able)
RETRYABLE_CODES = {-1001, -1003, -1015, -1021}  # timeout, rate limit, etc.
MAX_RETRIES = 3
RETRY_DELAY = 1.0


class BinanceAPIError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"Binance API error {code}: {msg}")


class BinanceClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self._session: aiohttp.ClientSession | None = None

        if testnet:
            self._spot_base = "https://testnet.binance.vision"
            self._futures_base = "https://testnet.binancefuture.com"
        else:
            self._spot_base = SPOT_BASE
            self._futures_base = FUTURES_BASE

        # Symbol info caches
        self._spot_symbols: dict[str, SymbolInfo] = {}
        self._futures_symbols: dict[str, SymbolInfo] = {}

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers={"X-MBX-APIKEY": self.api_key},
            timeout=aiohttp.ClientTimeout(total=10),
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()
            self._session = None

    # ─── Signing ────────────────────────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        """Add timestamp and HMAC-SHA256 signature to params."""
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    # ─── HTTP helpers ───────────────────────────────────────────────────

    async def _request(
        self, method: str, url: str, params: dict | None = None, signed: bool = False
    ) -> dict:
        """Make an HTTP request with retry logic."""
        if params is None:
            params = {}
        if signed:
            params = self._sign(params)

        for attempt in range(MAX_RETRIES):
            try:
                async with self._session.request(method, url, params=params) as resp:
                    data = await resp.json()

                    if isinstance(data, dict) and "code" in data and data["code"] < 0:
                        code = data["code"]
                        msg = data.get("msg", "")
                        if code in RETRYABLE_CODES and attempt < MAX_RETRIES - 1:
                            log.warning(f"Retryable error {code}: {msg}, attempt {attempt + 1}")
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                            continue
                        raise BinanceAPIError(code, msg)

                    return data

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < MAX_RETRIES - 1:
                    log.warning(f"Request failed: {e}, attempt {attempt + 1}")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                raise

        raise RuntimeError("Max retries exceeded")

    async def _spot_get(self, path: str, params: dict | None = None, signed: bool = False):
        return await self._request("GET", f"{self._spot_base}{path}", params, signed)

    async def _spot_post(self, path: str, params: dict | None = None, signed: bool = True):
        return await self._request("POST", f"{self._spot_base}{path}", params, signed)

    async def _spot_delete(self, path: str, params: dict | None = None, signed: bool = True):
        return await self._request("DELETE", f"{self._spot_base}{path}", params, signed)

    async def _futures_get(self, path: str, params: dict | None = None, signed: bool = False):
        return await self._request("GET", f"{self._futures_base}{path}", params, signed)

    async def _futures_post(self, path: str, params: dict | None = None, signed: bool = True):
        return await self._request("POST", f"{self._futures_base}{path}", params, signed)

    async def _futures_delete(self, path: str, params: dict | None = None, signed: bool = True):
        return await self._request("DELETE", f"{self._futures_base}{path}", params, signed)

    # ─── Symbol info ────────────────────────────────────────────────────

    async def load_spot_symbols(self):
        """Cache spot trading rules."""
        data = await self._spot_get("/api/v3/exchangeInfo")
        for s in data.get("symbols", []):
            info = self._parse_symbol_info(s)
            if info:
                self._spot_symbols[info.symbol] = info
        log.info(f"Loaded {len(self._spot_symbols)} spot symbols")

    async def load_futures_symbols(self):
        """Cache futures trading rules."""
        data = await self._futures_get("/fapi/v1/exchangeInfo")
        for s in data.get("symbols", []):
            info = self._parse_symbol_info(s)
            if info:
                self._futures_symbols[info.symbol] = info
        log.info(f"Loaded {len(self._futures_symbols)} futures symbols")

    def _parse_symbol_info(self, raw: dict) -> SymbolInfo | None:
        symbol = raw.get("symbol", "")
        if not symbol:
            return None

        price_prec = raw.get("pricePrecision", 8)
        qty_prec = raw.get("quantityPrecision", raw.get("baseAssetPrecision", 8))
        min_qty = 0.0
        min_notional = 0.0
        step_size = 0.0
        tick_size = 0.0

        for f in raw.get("filters", []):
            ft = f.get("filterType", "")
            if ft == "LOT_SIZE":
                min_qty = float(f.get("minQty", 0))
                step_size = float(f.get("stepSize", 0))
            elif ft == "PRICE_FILTER":
                tick_size = float(f.get("tickSize", 0))
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                min_notional = float(f.get("minNotional", f.get("notional", 0)))

        return SymbolInfo(
            symbol=symbol,
            base_asset=raw.get("baseAsset", ""),
            quote_asset=raw.get("quoteAsset", ""),
            price_precision=price_prec,
            qty_precision=qty_prec,
            min_qty=min_qty,
            min_notional=min_notional,
            step_size=step_size,
            tick_size=tick_size,
        )

    def get_spot_info(self, symbol: str) -> SymbolInfo | None:
        return self._spot_symbols.get(symbol)

    def get_futures_info(self, symbol: str) -> SymbolInfo | None:
        return self._futures_symbols.get(symbol)

    async def load_margin_symbols(self) -> set[str]:
        """Load symbols available for cross margin trading."""
        data = await self._spot_get("/sapi/v1/margin/allPairs")
        self._margin_symbols = {p["symbol"] for p in data if isinstance(p, dict)}
        log.info(f"Loaded {len(self._margin_symbols)} margin symbols")
        return self._margin_symbols

    def is_margin_tradable(self, symbol: str) -> bool:
        """Check if a symbol supports cross margin trading."""
        return symbol in getattr(self, '_margin_symbols', set())

    # ─── Account ────────────────────────────────────────────────────────

    async def spot_balances(self) -> dict[str, float]:
        """Get spot account balances. Returns {asset: free_balance}."""
        data = await self._spot_get("/api/v3/account", signed=True)
        return {
            b["asset"]: float(b["free"])
            for b in data.get("balances", [])
            if float(b["free"]) > 0
        }

    async def futures_balances(self) -> dict[str, float]:
        """Get futures account balances. Returns {asset: available_balance}."""
        data = await self._futures_get("/fapi/v2/balance", signed=True)
        return {
            b["asset"]: float(b["availableBalance"])
            for b in data
            if float(b["availableBalance"]) > 0
        }

    async def futures_account(self) -> dict:
        """Get full futures account info (positions, margin, etc)."""
        return await self._futures_get("/fapi/v2/account", signed=True)

    async def futures_positions(self) -> list[Position]:
        """Get all open futures positions."""
        data = await self._futures_get("/fapi/v2/account", signed=True)
        positions = []
        for p in data.get("positions", []):
            qty = float(p.get("positionAmt", 0))
            if qty == 0:
                continue
            positions.append(Position(
                symbol=p["symbol"],
                side=PositionSide.LONG if qty > 0 else PositionSide.SHORT,
                quantity=abs(qty),
                entry_price=float(p.get("entryPrice", 0)),
                mark_price=float(p.get("markPrice", 0)),
                unrealized_pnl=float(p.get("unrealizedProfit", 0)),
                leverage=int(p.get("leverage", 1)),
                margin_type=p.get("marginType", "cross"),
                raw=p,
            ))
        return positions

    # ─── Futures config ─────────────────────────────────────────────────

    async def futures_set_leverage(self, symbol: str, leverage: int):
        """Set leverage for a futures symbol."""
        return await self._futures_post("/fapi/v1/leverage", {
            "symbol": symbol,
            "leverage": leverage,
        })

    async def futures_set_margin_type(self, symbol: str, margin_type: str = "CROSSED"):
        """Set margin type. margin_type: ISOLATED or CROSSED."""
        try:
            return await self._futures_post("/fapi/v1/marginType", {
                "symbol": symbol,
                "marginType": margin_type,
            })
        except BinanceAPIError as e:
            # -4046: No need to change margin type (already set)
            if e.code == -4046:
                return
            raise

    # ─── Spot orders ────────────────────────────────────────────────────

    def _parse_order(self, data: dict) -> Order:
        # Calculate avg_price: prefer avgPrice, then compute from fills, then cumulativeQuoteQty/executedQty
        avg_price = float(data.get("avgPrice", 0))
        filled_qty = float(data.get("executedQty", 0))

        if avg_price <= 0 and filled_qty > 0:
            # Try to compute from fills
            fills = data.get("fills", [])
            if fills:
                total_cost = sum(float(f["price"]) * float(f["qty"]) for f in fills)
                total_qty = sum(float(f["qty"]) for f in fills)
                if total_qty > 0:
                    avg_price = total_cost / total_qty

        if avg_price <= 0 and filled_qty > 0:
            # Try cumulativeQuoteQty / executedQty
            cum_quote = float(data.get("cumulativeQuoteQty", 0))
            if cum_quote > 0 and filled_qty > 0:
                avg_price = cum_quote / filled_qty

        return Order(
            symbol=data.get("symbol", ""),
            side=OrderSide(data.get("side", "BUY")),
            order_type=OrderType(data.get("type", "MARKET")),
            quantity=float(data.get("origQty", 0)),
            price=float(data.get("price", 0)) or None,
            order_id=str(data.get("orderId", "")),
            client_order_id=data.get("clientOrderId", ""),
            status=OrderStatus(data.get("status", "NEW")),
            filled_qty=filled_qty,
            avg_price=avg_price,
            commission=sum(
                float(f.get("commission", 0))
                for f in data.get("fills", [])
            ),
            timestamp=data.get("transactTime", data.get("updateTime", 0)),
            raw=data,
        )

    async def spot_market_buy(self, symbol: str, quantity: float) -> Order:
        """Place a spot market buy order."""
        info = self.get_spot_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quantity": str(quantity),
        }
        data = await self._spot_post("/api/v3/order", params)
        order = self._parse_order(data)
        log.info(f"SPOT BUY {symbol} qty={order.filled_qty} avg={order.avg_price}")
        return order

    async def spot_market_sell(self, symbol: str, quantity: float) -> Order:
        """Place a spot market sell order."""
        info = self.get_spot_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(quantity),
        }
        data = await self._spot_post("/api/v3/order", params)
        order = self._parse_order(data)
        log.info(f"SPOT SELL {symbol} qty={order.filled_qty} avg={order.avg_price}")
        return order

    async def spot_limit_buy(self, symbol: str, quantity: float, price: float) -> Order:
        info = self.get_spot_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
            price = info.round_price(price)
        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": str(quantity),
            "price": str(price),
        }
        data = await self._spot_post("/api/v3/order", params)
        return self._parse_order(data)

    async def spot_limit_sell(self, symbol: str, quantity: float, price: float) -> Order:
        info = self.get_spot_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
            price = info.round_price(price)
        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": str(quantity),
            "price": str(price),
        }
        data = await self._spot_post("/api/v3/order", params)
        return self._parse_order(data)

    async def spot_cancel_order(self, symbol: str, order_id: str) -> Order:
        params = {"symbol": symbol, "orderId": order_id}
        data = await self._spot_delete("/api/v3/order", params)
        return self._parse_order(data)

    async def spot_get_order(self, symbol: str, order_id: str) -> Order:
        params = {"symbol": symbol, "orderId": order_id}
        data = await self._spot_get("/api/v3/order", params, signed=True)
        return self._parse_order(data)

    # ─── Futures orders ─────────────────────────────────────────────────

    async def futures_market_buy(self, symbol: str, quantity: float) -> Order:
        """Place a futures market buy (long) order."""
        info = self.get_futures_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quantity": str(quantity),
            "newOrderRespType": "RESULT",  # Get fill info immediately
        }
        data = await self._futures_post("/fapi/v1/order", params)
        order = self._parse_order(data)
        log.info(f"FUTURES BUY {symbol} qty={order.filled_qty} avg={order.avg_price}")
        return order

    async def futures_market_sell(self, symbol: str, quantity: float) -> Order:
        """Place a futures market sell (short) order."""
        info = self.get_futures_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(quantity),
            "newOrderRespType": "RESULT",  # Get fill info immediately
        }
        data = await self._futures_post("/fapi/v1/order", params)
        order = self._parse_order(data)
        log.info(f"FUTURES SELL {symbol} qty={order.filled_qty} avg={order.avg_price}")
        return order

    async def futures_cancel_order(self, symbol: str, order_id: str) -> Order:
        params = {"symbol": symbol, "orderId": order_id}
        data = await self._futures_delete("/fapi/v1/order", params)
        return self._parse_order(data)

    async def futures_get_order(self, symbol: str, order_id: str) -> Order:
        params = {"symbol": symbol, "orderId": order_id}
        data = await self._futures_get("/fapi/v1/order", params, signed=True)
        return self._parse_order(data)

    # ─── Cross Margin account ──────────────────────────────────────────

    async def margin_account(self) -> dict:
        """Get cross margin account details (balances, margin level, liabilities)."""
        return await self._spot_get("/sapi/v1/margin/account", signed=True)

    async def margin_max_borrowable(self, asset: str) -> float:
        """Get max borrowable amount for an asset."""
        data = await self._spot_get("/sapi/v1/margin/maxBorrowable", {"asset": asset}, signed=True)
        return float(data.get("amount", 0))

    async def margin_interest_rate(self, asset: str) -> float:
        """Get current daily borrow interest rate for an asset."""
        data = await self._spot_get(
            "/sapi/v1/margin/interestRateHistory",
            {"asset": asset, "limit": 1},
            signed=True,
        )
        if data and isinstance(data, list) and len(data) > 0:
            return float(data[0].get("dailyInterestRate", 0))
        return 0.0

    async def margin_borrow(self, asset: str, amount: float) -> dict:
        """Borrow an asset from cross margin."""
        data = await self._spot_post("/sapi/v1/margin/loan", {
            "asset": asset,
            "amount": f"{amount:.8f}",
        })
        log.info(f"MARGIN BORROW {asset} amount={amount}")
        return data

    async def margin_repay(self, asset: str, amount: float) -> dict:
        """Repay borrowed asset to cross margin."""
        data = await self._spot_post("/sapi/v1/margin/repay", {
            "asset": asset,
            "amount": f"{amount:.8f}",
        })
        log.info(f"MARGIN REPAY {asset} amount={amount}")
        return data

    async def margin_market_sell(self, symbol: str, quantity: float, auto_borrow: bool = True) -> Order:
        """
        Place a margin market sell order.
        If auto_borrow=True, automatically borrows the coin first (sideEffectType=MARGIN_BUY).
        """
        info = self.get_spot_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(quantity),
            "sideEffectType": "MARGIN_BUY" if auto_borrow else "NO_SIDE_EFFECT",
        }
        data = await self._spot_post("/sapi/v1/margin/order", params)
        order = self._parse_order(data)
        log.info(f"MARGIN SELL {symbol} qty={order.filled_qty} avg={order.avg_price} auto_borrow={auto_borrow}")
        return order

    async def margin_market_buy(self, symbol: str, quantity: float, auto_repay: bool = True) -> Order:
        """
        Place a margin market buy order.
        If auto_repay=True, automatically repays the loan (sideEffectType=AUTO_REPAY).
        """
        info = self.get_spot_info(symbol)
        if info:
            quantity = info.round_qty(quantity)
        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quantity": str(quantity),
            "sideEffectType": "AUTO_REPAY" if auto_repay else "NO_SIDE_EFFECT",
        }
        data = await self._spot_post("/sapi/v1/margin/order", params)
        order = self._parse_order(data)
        log.info(f"MARGIN BUY {symbol} qty={order.filled_qty} avg={order.avg_price} auto_repay={auto_repay}")
        return order

    async def transfer_spot_to_margin(self, asset: str, amount: float) -> dict:
        """Transfer asset from spot to cross margin account."""
        return await self._spot_post("/sapi/v1/margin/transfer", {
            "asset": asset,
            "amount": f"{amount:.8f}",
            "type": "1",  # 1 = spot to margin
        })

    async def transfer_margin_to_spot(self, asset: str, amount: float) -> dict:
        """Transfer asset from cross margin to spot account."""
        return await self._spot_post("/sapi/v1/margin/transfer", {
            "asset": asset,
            "amount": f"{amount:.8f}",
            "type": "2",  # 2 = margin to spot
        })

    async def margin_get_loan(self, asset: str) -> list[dict]:
        """Query outstanding loans for an asset."""
        data = await self._spot_get("/sapi/v1/margin/loan", {"asset": asset, "size": 10}, signed=True)
        return data.get("rows", []) if isinstance(data, dict) else []

    # ─── Market data (public) ───────────────────────────────────────────

    async def spot_price(self, symbol: str) -> float:
        data = await self._spot_get("/api/v3/ticker/price", {"symbol": symbol})
        return float(data["price"])

    async def futures_price(self, symbol: str) -> float:
        data = await self._futures_get("/fapi/v1/ticker/price", {"symbol": symbol})
        return float(data["price"])

    async def futures_funding_rate(self, symbol: str) -> dict:
        data = await self._futures_get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return {
            "symbol": data["symbol"],
            "mark_price": float(data["markPrice"]),
            "index_price": float(data["indexPrice"]),
            "funding_rate": float(data["lastFundingRate"]),
            "next_funding_time": data["nextFundingTime"],
        }

    async def spot_orderbook(self, symbol: str, limit: int = 5) -> dict:
        """Get spot orderbook. Returns {bids: [[price, qty], ...], asks: [...]}."""
        data = await self._spot_get("/api/v3/depth", {"symbol": symbol, "limit": limit})
        return {
            "bids": [[float(p), float(q)] for p, q in data.get("bids", [])],
            "asks": [[float(p), float(q)] for p, q in data.get("asks", [])],
        }

    async def futures_orderbook(self, symbol: str, limit: int = 5) -> dict:
        """Get futures orderbook."""
        data = await self._futures_get("/fapi/v1/depth", {"symbol": symbol, "limit": limit})
        return {
            "bids": [[float(p), float(q)] for p, q in data.get("bids", [])],
            "asks": [[float(p), float(q)] for p, q in data.get("asks", [])],
        }
