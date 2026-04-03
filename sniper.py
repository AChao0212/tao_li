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
TELEGRAM_PATH = Path.home() / ".secret" / "telegram.txt"

# Timing
SCAN_INTERVAL = 60              # seconds between scans (when idle, far from settlement)
PRESCAN_SECONDS = 120           # start watching candidates this many seconds before settlement
ENTRY_SECONDS_BEFORE = 2        # enter this many seconds before settlement
EXIT_SECONDS_AFTER = 1          # exit this many seconds after settlement
SAFETY_CHECK_INTERVAL = 120     # seconds between margin safety checks
HEARTBEAT_INTERVAL = 300        # log heartbeat every 5 minutes


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


class TelegramNotifier:
    """Send notifications via Telegram bot."""

    def __init__(self):
        self.bot_token = ""
        self.chat_id = ""
        self._load()

    def _load(self):
        if not TELEGRAM_PATH.exists():
            return
        for line in TELEGRAM_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip("'\"")
                k = k.strip()
                if k == "BOT_TOKEN":
                    self.bot_token = v
                elif k == "BOT_CHAT_ID":
                    self.chat_id = v

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def send(self, message: str):
        """Send a message. Non-blocking, never raises."""
        if not self.enabled:
            return
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            async with aiohttp.ClientSession() as session:
                await session.post(url, data={
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                }, timeout=aiohttp.ClientTimeout(total=5))
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")


telegram = TelegramNotifier()


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
        self._last_heartbeat = 0.0

        # Volatility cache: symbol -> (timestamp, value)
        self._vol_cache: dict[str, tuple[float, float]] = {}
        self._vol_cache_ttl = 60  # seconds

        # Background exit tasks
        self._exit_tasks: dict[str, asyncio.Task] = {}
        # Guard against double-exit
        self._exiting: set[str] = set()
        # Symbols already pre-configured (leverage/margin type set)
        self._preconfigured: set[str] = set()

    async def heartbeat(self):
        """Log status with next settlement times and top funding rates."""
        try:
            data = await self.client._futures_get("/fapi/v1/premiumIndex")
            now_ms = int(time.time() * 1000)

            # Find next settlement times and top rates
            funding_times = set()
            top_rates = []
            for item in data:
                sym = item.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                nft = item.get("nextFundingTime", 0)
                rate = float(item.get("lastFundingRate", 0))
                if nft:
                    funding_times.add(nft)
                if abs(rate) >= self.min_rate:
                    secs = max(0, (nft - now_ms) // 1000)
                    top_rates.append((sym, rate, secs))

            top_rates.sort(key=lambda x: abs(x[1]), reverse=True)

            # Next settlement
            next_times = sorted(funding_times)[:3]
            time_strs = []
            for t in next_times:
                secs = max(0, (t - now_ms) // 1000)
                dt = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
                time_strs.append(f"{dt.strftime('%H:%M')}UTC({secs // 60}m)")

            log.info(
                f"[HEARTBEAT] Next settlements: {', '.join(time_strs)} | "
                f"Candidates (rate>={self.min_rate:.1%}): {len(top_rates)}"
            )
            for sym, rate, secs in top_rates[:5]:
                log.info(f"  {sym:<16} rate={rate:+.4%}  settles_in={secs // 60}m{secs % 60}s")

        except Exception as e:
            log.error(f"Heartbeat failed: {e}")

    async def _get_1m_volatility(self, symbol: str) -> float:
        """Get average 1-minute price change over the last 30 minutes. Cached for 60s."""
        now = time.time()
        cached = self._vol_cache.get(symbol)
        if cached and now - cached[0] < self._vol_cache_ttl:
            return cached[1]

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
            vol = sum(changes) / len(changes) if changes else 999.0
            self._vol_cache[symbol] = (now, vol)
            return vol
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

            # Only interested if settlement is approaching (within prescan window)
            if seconds_to_funding > PRESCAN_SECONDS:
                continue

            # Skip if already sniped this period
            if self.sniped_this_period.get(symbol) == next_funding:
                continue

            direction = "long" if rate < 0 else "short"
            net_pnl = abs_rate - 0.0008  # 2x taker fee (entry + exit)

            if net_pnl <= 0:
                continue

            # Volatility filter: with ~5s exposure, we only need rate > estimated 5s move
            # 5s move ≈ avg_1m_vol * sqrt(5/60) ≈ avg_1m_vol * 0.29
            # Require rate > 3x the 5-second estimated move for safety
            vol = await self._get_1m_volatility(symbol)
            vol_5s = vol * 0.29  # Scale 1m vol to 5s
            if abs_rate < vol_5s * 3:
                log.info(
                    f"[VOL SKIP] {symbol}: rate={abs_rate:.4%} < 3x vol_5s={vol_5s:.4%} "
                    f"(vol_1m={vol:.4%})"
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
        """Enter a snipe position just before funding settlement.
        After successful entry, schedules an automatic exit task."""
        symbol = opp["symbol"]
        now_ms = int(time.time() * 1000)

        # Abort if settlement already passed
        if now_ms >= opp["next_funding"]:
            log.info(f"[ABORT] {symbol}: settlement already passed")
            return

        # Re-check funding rate right before entry (it may have changed)
        try:
            fresh = await self.client._futures_get("/fapi/v1/premiumIndex", {"symbol": symbol})
            fresh_rate = float(fresh.get("lastFundingRate", 0))
            fresh_price = float(fresh.get("markPrice", 0))
            if abs(fresh_rate) < self.min_rate:
                log.info(f"[ABORT] {symbol}: rate dropped to {fresh_rate:+.4%} (was {opp['rate']:+.4%})")
                return
            opp["rate"] = fresh_rate
            opp["mark_price"] = fresh_price if fresh_price > 0 else opp["mark_price"]
            opp["direction"] = "long" if fresh_rate < 0 else "short"
        except Exception as e:
            log.warning(f"[RECHECK FAILED] {symbol}: {e}, using cached rate")

        # Abort again after re-check (API call took time)
        now_ms = int(time.time() * 1000)
        if now_ms >= opp["next_funding"]:
            log.info(f"[ABORT] {symbol}: settlement passed during re-check")
            return

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
        await telegram.send(
            f"⚡ <b>SNIPE {direction.upper()}</b> {symbol}\n"
            f"Rate: {opp['rate']:+.4%} | Net: {opp['net_pnl']:+.4%}\n"
            f"Qty: {quantity:.4f} | ~${quantity * price:.2f}"
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
            # Schedule dry exit
            self._schedule_exit(symbol, opp["next_funding"])
            return

        try:
            if symbol not in self._preconfigured:
                await self.client.futures_set_leverage(symbol, 1)
                await self.client.futures_set_margin_type(symbol, "CROSSED")
                self._preconfigured.add(symbol)

            if direction == "long":
                order = await self.client.futures_market_buy(symbol, quantity)
            else:
                order = await self.client.futures_market_sell(symbol, quantity)

            if order.filled_qty <= 0 or order.avg_price <= 0:
                log.error(f"[ENTER FAILED] {symbol}: not filled (qty={order.filled_qty})")
                await telegram.send(f"🚨 <b>ENTER FAILED</b> {symbol}: order not filled")
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

            # Schedule exit immediately after fill
            self._schedule_exit(symbol, opp["next_funding"])

        except Exception as e:
            log.error(f"[ENTER FAILED] {symbol}: {e}", exc_info=True)
            await telegram.send(f"🚨 <b>ENTER FAILED</b> {symbol}: {e}")

    def _schedule_exit(self, symbol: str, funding_time_ms: int):
        """Spawn a background task to exit at funding_time + EXIT_SECONDS_AFTER."""
        async def _auto_exit():
            try:
                # Wait until funding_time + exit delay
                target_ms = funding_time_ms + EXIT_SECONDS_AFTER * 1000
                now_ms = int(time.time() * 1000)
                wait_s = max(0, (target_ms - now_ms) / 1000)
                if wait_s > 0:
                    log.info(f"[EXIT SCHEDULED] {symbol}: will exit in {wait_s:.1f}s")
                    await asyncio.sleep(wait_s)
                await self.exit_snipe(symbol)
            except Exception as e:
                log.error(f"[AUTO EXIT FAILED] {symbol}: {e}", exc_info=True)
            finally:
                self._exit_tasks.pop(symbol, None)

        # Cancel any existing exit task for this symbol
        if symbol in self._exit_tasks:
            self._exit_tasks[symbol].cancel()
        self._exit_tasks[symbol] = asyncio.create_task(_auto_exit())

    async def exit_snipe(self, symbol: str):
        """Exit a snipe position after funding settlement."""
        if symbol in self._exiting:
            return
        pos = self.positions.get(symbol)
        if not pos:
            return
        self._exiting.add(symbol)

        direction = pos["direction"]
        quantity = pos["quantity"]

        log.info(f"[EXIT] {symbol} {direction} qty={quantity:.6f}")

        try:
            if pos.get("dry_run"):
                current_price = pos["entry_price"]  # approximate
                funding_pnl = abs(pos["rate"]) * self.position_usdt
                log.info(
                    f"[DRY EXIT] {symbol}: funding_collected~${funding_pnl:.4f} "
                    f"rate={pos['rate']:+.4%}"
                )
                del self.positions[symbol]
                return

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

                net = price_pnl + funding_pnl - total_fees
                log.info(
                    f"[CLOSED] {symbol}: price_pnl=${price_pnl:+.4f} "
                    f"funding~${funding_pnl:.4f} fees~${total_fees:.4f} "
                    f"net~${net:+.4f}"
                )
                emoji = "✅" if net > 0 else "⚠️"
                await telegram.send(
                    f"{emoji} <b>CLOSED</b> {symbol}\n"
                    f"Price PnL: ${price_pnl:+.4f}\n"
                    f"Funding:   ~${funding_pnl:.4f}\n"
                    f"Fees:      ~${total_fees:.4f}\n"
                    f"<b>Net: ${net:+.4f}</b>"
                )

            delete_position(symbol)
            del self.positions[symbol]

        except Exception as e:
            log.error(f"[EXIT FAILED] {symbol}: {e}", exc_info=True)
            await telegram.send(f"🚨 <b>EXIT FAILED</b> {symbol}: {e}")
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
                await telegram.send(
                    f"🔴 <b>MANUAL CLOSE NEEDED</b> {symbol}\n"
                    f"Exit failed twice: {e2}\n"
                    f"Position still open on Binance!"
                )
        finally:
            self._exiting.discard(symbol)

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

        mode = "DRY RUN" if self.dry_run else "LIVE"
        if not self.dry_run:
            bal = await self.client.futures_balances()
            usdt = bal.get('USDT', 0)
            log.info(f"Futures USDT: ${usdt:.2f}")
            await telegram.send(
                f"🟢 <b>Sniper started ({mode})</b>\n"
                f"Balance: ${usdt:.2f} USDT\n"
                f"Min rate: {self.min_rate:.1%} | Size: ${self.position_usdt}\n"
                f"Window: T-{ENTRY_SECONDS_BEFORE}s → T+{EXIT_SECONDS_AFTER}s"
            )

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

        # Track candidates found during prescan
        candidates: list[dict] = []

        while True:
            try:
                now = time.time()

                # Fallback: exit any positions that missed their scheduled exit
                await self.check_exits()

                # Scan for new opportunities
                opps = await self.scan_opportunities()

                candidates = opps if opps else []

                if opps:
                    nearest = min(o["seconds_to_funding"] for o in opps)

                    if nearest > ENTRY_SECONDS_BEFORE:
                        # Not time yet — log what we're watching
                        symbols = ", ".join(f"{o['symbol']}({o['rate']:+.4%})" for o in opps[:3])
                        log.info(f"[WATCH] {len(opps)} ready, nearest in {nearest}s: {symbols}")

                        # Pre-configure futures while we wait (so entry is faster)
                        if nearest <= ENTRY_SECONDS_BEFORE + 10 and not self.dry_run:
                            for opp in opps[:self.max_positions]:
                                sym = opp["symbol"]
                                if sym not in self.positions and sym not in self._preconfigured:
                                    try:
                                        await self.client.futures_set_leverage(sym, 1)
                                        await self.client.futures_set_margin_type(sym, "CROSSED")
                                        self._preconfigured.add(sym)
                                    except Exception:
                                        pass
                    else:
                        # GO TIME — enter positions in parallel
                        open_count = len(self.positions)
                        current_exposure = open_count * self.position_usdt

                        to_enter = []
                        for opp in opps:
                            if open_count >= self.max_positions:
                                break
                            if current_exposure + self.position_usdt > self.max_exposure:
                                break
                            if opp["symbol"] in self.positions:
                                continue
                            if opp["seconds_to_funding"] <= ENTRY_SECONDS_BEFORE:
                                to_enter.append(opp)
                                open_count += 1
                                current_exposure += self.position_usdt

                        if to_enter:
                            # Enter all positions simultaneously
                            await asyncio.gather(
                                *(self.enter_snipe(opp) for opp in to_enter),
                                return_exceptions=True,
                            )

                # Heartbeat
                if now - self._last_heartbeat > HEARTBEAT_INTERVAL:
                    await self.heartbeat()
                    self._last_heartbeat = now

                # Safety check
                if not self.dry_run and now - self._last_safety > SAFETY_CHECK_INTERVAL:
                    try:
                        acct = await self.client.futures_account()
                        margin_balance = float(acct.get("totalMarginBalance", 0))
                        maint = float(acct.get("totalMaintMargin", 0))
                        if margin_balance > 0 and maint / margin_balance > 0.8:
                            log.error(f"[DANGER] Margin ratio {maint/margin_balance:.2f}, closing all!")
                            await telegram.send(
                                f"🔴 <b>DANGER</b> Margin ratio {maint/margin_balance:.2f}\n"
                                f"Emergency closing all positions!"
                            )
                            for sym in list(self.positions.keys()):
                                await self.exit_snipe(sym)
                    except Exception as e:
                        log.error(f"Safety check failed: {e}")
                    self._last_safety = now

            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"Main loop error: {e}", exc_info=True)

            # Adaptive sleep:
            #   - 1s when candidates are near (<10s to entry)
            #   - 10s when candidates are in prescan window (<120s)
            #   - SCAN_INTERVAL when idle
            #   (exits are handled by scheduled tasks, no need to poll)
            if candidates:
                nearest = min(o["seconds_to_funding"] for o in candidates)
                if nearest < ENTRY_SECONDS_BEFORE + 10:
                    await asyncio.sleep(1)
                elif nearest < PRESCAN_SECONDS:
                    await asyncio.sleep(10)
                else:
                    await asyncio.sleep(SCAN_INTERVAL)
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
