"""
Funding rate sniper — enter just before settlement, collect funding, exit immediately.

Flow:
  1. Continuously scan for extreme negative funding rates
  2. When approaching settlement (< ENTRY_SECONDS_BEFORE), open long futures
  3. After settlement, close immediately
  4. Total exposure: ~40 seconds

No margin account needed. Pure futures.
"""

import asyncio
import argparse
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

import aiohttp

from config import BASE_URL
from db import init_db, save_position, delete_position, load_open_positions

LOG_DIR = Path(__file__).parent / "logs"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
SECRET_PATH = Path.home() / ".secret" / "binance.txt"

# Timing
SCAN_INTERVAL = 10              # seconds between scans
ENTRY_SECONDS_BEFORE = 30       # enter this many seconds before settlement
EXIT_SECONDS_AFTER = 10         # exit this many seconds after settlement
SAFETY_CHECK_INTERVAL = 120     # seconds between margin safety checks


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root.addHandler(console)
    fh = TimedRotatingFileHandler(LOG_DIR / "sniper.log", when="midnight", backupCount=30, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root.addHandler(fh)


setup_logging()
log = logging.getLogger("sniper")


def load_api_keys():
    api_key = api_secret = ""
    if SECRET_PATH.exists():
        for line in SECRET_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip("'\"")
                k = k.strip()
                if k == "BINANCE_API_KEY":
                    api_key = v
                elif k == "BINANCE_API_SECRET":
                    api_secret = v
    if not api_key:
        api_key = os.environ.get("BINANCE_API_KEY", "")
    if not api_secret:
        api_secret = os.environ.get("BINANCE_API_SECRET", "")
    return api_key, api_secret


class FundingSniper:
    def __init__(
        self,
        client,  # BinanceClient
        min_rate: float = 0.01,         # minimum |funding rate| to snipe (1%)
        position_usdt: float = 100.0,
        max_positions: int = 3,
        max_exposure: float = 400.0,
        min_volume: float = 1_000_000,
        dry_run: bool = False,
    ):
        self.client = client
        self.min_rate = min_rate
        self.position_usdt = position_usdt
        self.max_positions = max_positions
        self.max_exposure = max_exposure
        self.min_volume = min_volume
        self.dry_run = dry_run

        # Active snipe positions: symbol -> {order, qty, entry_time, direction, funding_time}
        self.positions: dict[str, dict] = {}
        # Symbols we've already sniped this funding period (avoid double entry)
        self.sniped_this_period: dict[str, int] = {}  # symbol -> funding_time

        self._last_safety = 0.0

    async def _get_1m_volatility(self, symbol: str) -> float:
        """Get average 1-minute price change over the last 30 minutes."""
        try:
            data = await self.client._futures_get("/fapi/v1/klines", {
                "symbol": symbol, "interval": "1m", "limit": 30,
            })
            if not data or not isinstance(data, list):
                return 999.0  # Unknown = assume dangerous
            changes = []
            for k in data:
                o, c = float(k[1]), float(k[4])
                if o > 0:
                    changes.append(abs(c - o) / o)
            return sum(changes) / len(changes) if changes else 999.0
        except Exception:
            return 999.0

    async def scan_opportunities(self) -> list[dict]:
        """Find symbols with extreme funding rates approaching settlement."""
        now_ms = int(time.time() * 1000)

        try:
            premium_data = await self.client._futures_get("/fapi/v1/premiumIndex")
            ticker_data = await self.client._futures_get("/fapi/v1/ticker/24hr")
        except Exception as e:
            log.error(f"Scan failed: {e}")
            return []

        volume_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in ticker_data}
        opportunities = []

        for item in premium_data:
            symbol = item.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue

            rate = float(item.get("lastFundingRate", 0))
            mark_price = float(item.get("markPrice", 0))
            next_funding = item.get("nextFundingTime", 0)

            if mark_price <= 0 or not next_funding:
                continue

            volume = volume_map.get(symbol, 0)
            if volume < self.min_volume:
                continue

            abs_rate = abs(rate)
            if abs_rate < self.min_rate:
                continue

            seconds_to_funding = max(0, (next_funding - now_ms) // 1000)

            # Only interested if settlement is within entry window
            if seconds_to_funding > ENTRY_SECONDS_BEFORE + 60:
                continue

            # Skip if already sniped this period
            if self.sniped_this_period.get(symbol) == next_funding:
                continue

            direction = "long" if rate < 0 else "short"
            net_pnl = abs_rate - 0.0008  # 2x taker fee (entry + exit)

            if net_pnl <= 0:
                continue

            # Volatility filter: funding rate must be > 3x the avg 1-minute volatility
            # This ensures we're not gambling on price swings larger than our edge
            vol = await self._get_1m_volatility(symbol)
            if abs_rate < vol * 3:
                log.info(
                    f"[VOL SKIP] {symbol}: rate={abs_rate:.4%} < 3x vol={vol:.4%} "
                    f"(need {vol*3:.4%})"
                )
                continue

            opportunities.append({
                "symbol": symbol,
                "rate": rate,
                "abs_rate": abs_rate,
                "direction": direction,
                "mark_price": mark_price,
                "next_funding": next_funding,
                "seconds_to_funding": seconds_to_funding,
                "volatility_1m": vol,
                "net_pnl": net_pnl,
                "volume": volume,
            })

        opportunities.sort(key=lambda x: x["abs_rate"], reverse=True)
        return opportunities

    async def enter_snipe(self, opp: dict):
        """Enter a snipe position just before funding settlement."""
        symbol = opp["symbol"]
        direction = opp["direction"]
        price = opp["mark_price"]

        # Position sizing
        info = self.client.get_futures_info(symbol)
        raw_qty = self.position_usdt / price
        quantity = info.round_qty(raw_qty) if info else raw_qty

        if quantity <= 0:
            log.warning(f"[SKIP] {symbol}: quantity rounds to 0")
            return

        # Check min notional
        if info and quantity * price < info.min_notional:
            quantity = info.round_qty(info.min_notional / price * 1.05)
            log.info(f"[SIZE] {symbol}: adjusted qty to {quantity} for min_notional")

        log.info(
            f"[ENTER] {symbol} {direction} qty={quantity:.6f} price~{price:.4f} "
            f"rate={opp['rate']:+.4%} net={opp['net_pnl']:+.4%} "
            f"vol_1m={opp.get('volatility_1m', 0):.4%} "
            f"funding_in={opp['seconds_to_funding']}s"
        )

        if self.dry_run:
            self.positions[symbol] = {
                "direction": direction,
                "quantity": quantity,
                "entry_price": price,
                "entry_time": int(time.time() * 1000),
                "funding_time": opp["next_funding"],
                "rate": opp["rate"],
                "dry_run": True,
            }
            self.sniped_this_period[symbol] = opp["next_funding"]
            return

        try:
            await self.client.futures_set_leverage(symbol, 1)
            await self.client.futures_set_margin_type(symbol, "CROSSED")

            if direction == "long":
                order = await self.client.futures_market_buy(symbol, quantity)
            else:
                order = await self.client.futures_market_sell(symbol, quantity)

            if order.filled_qty <= 0 or order.avg_price <= 0:
                log.error(f"[ENTER FAILED] {symbol}: not filled (qty={order.filled_qty})")
                return

            self.positions[symbol] = {
                "direction": direction,
                "quantity": order.filled_qty,
                "entry_price": order.avg_price,
                "entry_time": int(time.time() * 1000),
                "funding_time": opp["next_funding"],
                "rate": opp["rate"],
                "order_id": order.order_id,
            }
            self.sniped_this_period[symbol] = opp["next_funding"]

            # Persist for crash recovery
            save_position({
                "symbol": symbol, "direction": direction, "status": "open",
                "quantity": order.filled_qty,
                "spot_filled_qty": 0, "spot_avg_price": 0,
                "futures_filled_qty": order.filled_qty,
                "futures_avg_price": order.avg_price,
                "borrowed_asset": "", "borrowed_qty": 0, "borrow_interest_rate": 0,
                "spot_collateral_usdt": 0, "futures_collateral_usdt": self.position_usdt,
                "open_time": int(time.time() * 1000),
                "usdt_amount": self.position_usdt,
                "updated_at": int(time.time() * 1000),
            })

            log.info(
                f"[FILLED] {symbol} {direction} qty={order.filled_qty:.6f} "
                f"avg={order.avg_price:.4f}"
            )

        except Exception as e:
            log.error(f"[ENTER FAILED] {symbol}: {e}", exc_info=True)

    async def exit_snipe(self, symbol: str):
        """Exit a snipe position after funding settlement."""
        pos = self.positions.get(symbol)
        if not pos:
            return

        direction = pos["direction"]
        quantity = pos["quantity"]

        log.info(f"[EXIT] {symbol} {direction} qty={quantity:.6f}")

        if pos.get("dry_run"):
            current_price = pos["entry_price"]  # approximate
            funding_pnl = abs(pos["rate"]) * self.position_usdt
            log.info(
                f"[DRY EXIT] {symbol}: funding_collected~${funding_pnl:.4f} "
                f"rate={pos['rate']:+.4%}"
            )
            del self.positions[symbol]
            return

        try:
            if direction == "long":
                order = await self.client.futures_market_sell(symbol, quantity)
            else:
                order = await self.client.futures_market_buy(symbol, quantity)

            if order.filled_qty > 0:
                price_pnl = 0
                if direction == "long":
                    price_pnl = (order.avg_price - pos["entry_price"]) * order.filled_qty
                else:
                    price_pnl = (pos["entry_price"] - order.avg_price) * order.filled_qty

                funding_pnl = abs(pos["rate"]) * pos["entry_price"] * quantity
                total_fees = pos["entry_price"] * quantity * 0.0004 * 2  # taker both sides

                log.info(
                    f"[CLOSED] {symbol}: price_pnl=${price_pnl:+.4f} "
                    f"funding~${funding_pnl:.4f} fees~${total_fees:.4f} "
                    f"net~${price_pnl + funding_pnl - total_fees:+.4f}"
                )

            delete_position(symbol)
            del self.positions[symbol]

        except Exception as e:
            log.error(f"[EXIT FAILED] {symbol}: {e}", exc_info=True)
            # Retry once
            try:
                await asyncio.sleep(2)
                if direction == "long":
                    await self.client.futures_market_sell(symbol, quantity)
                else:
                    await self.client.futures_market_buy(symbol, quantity)
                delete_position(symbol)
                del self.positions[symbol]
                log.info(f"[EXIT RETRY OK] {symbol}")
            except Exception as e2:
                log.error(f"[EXIT RETRY FAILED] {symbol}: {e2} — MANUAL CLOSE NEEDED")

    async def check_exits(self):
        """Check if any positions should be exited (funding has settled)."""
        now_ms = int(time.time() * 1000)
        to_exit = []

        for symbol, pos in list(self.positions.items()):
            funding_time = pos.get("funding_time", 0)
            # Exit if we're past funding time + buffer
            if now_ms > funding_time + EXIT_SECONDS_AFTER * 1000:
                to_exit.append(symbol)

        for symbol in to_exit:
            await self.exit_snipe(symbol)

    async def run(self):
        log.info("=" * 60)
        log.info(f"Funding Sniper starting {'(DRY RUN)' if self.dry_run else '(LIVE)'}")
        log.info(f"Min rate: {self.min_rate:.2%}")
        log.info(f"Position size: ${self.position_usdt}")
        log.info(f"Max positions: {self.max_positions}")
        log.info(f"Entry window: {ENTRY_SECONDS_BEFORE}s before settlement")
        log.info(f"Exit delay: {EXIT_SECONDS_AFTER}s after settlement")
        log.info("=" * 60)

        await self.client.load_futures_symbols()

        if not self.dry_run:
            bal = await self.client.futures_balances()
            log.info(f"Futures USDT: ${bal.get('USDT', 0):.2f}")

            # Recover any positions from crash
            saved = load_open_positions()
            for row in saved:
                sym = row["symbol"]
                log.warning(f"[RECOVER] Found orphaned position: {sym}, closing...")
                try:
                    if row["futures_filled_qty"] > 0:
                        if row["direction"] == "long":
                            await self.client.futures_market_sell(sym, row["futures_filled_qty"])
                        else:
                            await self.client.futures_market_buy(sym, row["futures_filled_qty"])
                    delete_position(sym)
                    log.info(f"[RECOVER] Closed {sym}")
                except Exception as e:
                    log.error(f"[RECOVER FAILED] {sym}: {e}")

        while True:
            try:
                now = time.time()

                # Exit any positions past their funding time
                await self.check_exits()

                # Scan for new opportunities
                opps = await self.scan_opportunities()
                open_count = len(self.positions)
                current_exposure = open_count * self.position_usdt

                for opp in opps:
                    if open_count >= self.max_positions:
                        break
                    if current_exposure + self.position_usdt > self.max_exposure:
                        break
                    if opp["symbol"] in self.positions:
                        continue

                    # Only enter within the window
                    if opp["seconds_to_funding"] <= ENTRY_SECONDS_BEFORE:
                        await self.enter_snipe(opp)
                        open_count += 1
                        current_exposure += self.position_usdt

                if opps and not self.positions:
                    next_funding = min(o["seconds_to_funding"] for o in opps)
                    log.debug(
                        f"[WAIT] {len(opps)} candidates, nearest in {next_funding}s: "
                        f"{opps[0]['symbol']} rate={opps[0]['rate']:+.4%}"
                    )

                # Safety check
                if not self.dry_run and now - self._last_safety > SAFETY_CHECK_INTERVAL:
                    try:
                        acct = await self.client.futures_account()
                        margin_balance = float(acct.get("totalMarginBalance", 0))
                        maint = float(acct.get("totalMaintMargin", 0))
                        if margin_balance > 0 and maint / margin_balance > 0.8:
                            log.error(f"[DANGER] Margin ratio {maint/margin_balance:.2f}, closing all!")
                            for sym in list(self.positions.keys()):
                                await self.exit_snipe(sym)
                    except Exception as e:
                        log.error(f"Safety check failed: {e}")
                    self._last_safety = now

            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"Main loop error: {e}", exc_info=True)

            # Adaptive sleep: faster when positions are open or funding is near
            if self.positions:
                await asyncio.sleep(2)  # Fast poll when in position
            else:
                await asyncio.sleep(SCAN_INTERVAL)

        # Shutdown
        log.info("Shutting down...")
        if not self.dry_run:
            for sym in list(self.positions.keys()):
                await self.exit_snipe(sym)


def main():
    parser = argparse.ArgumentParser(description="Funding rate sniper")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-rate", type=float, default=0.01, help="Min |funding rate| to snipe (default 1%%)")
    parser.add_argument("--position-size", type=float, default=100)
    parser.add_argument("--max-positions", type=int, default=3)
    parser.add_argument("--max-exposure", type=float, default=400)
    args = parser.parse_args()

    api_key, api_secret = load_api_keys()
    if not api_key or not api_secret:
        if args.dry_run:
            api_key = api_secret = "dummy"
        else:
            log.error(f"No API keys. Put them in {SECRET_PATH}")
            return

    from exchange import BinanceClient
    init_db()
    client = BinanceClient(api_key, api_secret)

    sniper = FundingSniper(
        client=client,
        min_rate=args.min_rate,
        position_usdt=args.position_size,
        max_positions=args.max_positions,
        max_exposure=args.max_exposure,
        dry_run=args.dry_run,
    )

    async def run():
        async with client:
            await sniper.run()

    asyncio.run(run())


if __name__ == "__main__":
    main()
