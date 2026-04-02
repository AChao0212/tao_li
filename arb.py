"""
Funding rate arbitrage bot.

Ties together: scanner → risk check → executor → position management.

Usage:
    # Set env vars: BINANCE_API_KEY, BINANCE_API_SECRET
    python arb.py [--dry-run]
"""

import argparse
import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from db import init_db
from exchange import (
    BinanceClient,
    HedgeExecutor,
    RiskConfig,
    RiskManager,
)
from scanner import SignalConfig, scan_once, Signal

SECRET_PATH = Path.home() / ".secret" / "binance.txt"


def load_api_keys(dry_run: bool = False) -> tuple[str, str]:
    """
    Load Binance API keys from ~/.secret/binance.txt.
    Format: BINANCE_API_KEY = 'xxx' / BINANCE_API_SECRET = 'yyy'
    Falls back to env vars, then .env file.
    """
    api_key = ""
    api_secret = ""

    # Try ~/.secret/binance.txt first
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

    # Fallback to env vars
    if not api_key:
        api_key = os.environ.get("BINANCE_API_KEY", "")
    if not api_secret:
        api_secret = os.environ.get("BINANCE_API_SECRET", "")

    if not api_key or not api_secret:
        if dry_run:
            log.warning("No API keys — dry run will use public data only")
            return "dummy", "dummy"
        log.error(f"No API keys found. Put them in {SECRET_PATH}")
        return "", ""

    log.info(f"API keys loaded from {SECRET_PATH}")
    return api_key, api_secret


LOG_DIR = Path(__file__).parent / "logs"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging():
    """Configure logging to both console and rotating file."""
    LOG_DIR.mkdir(exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(console)

    # File handler — one file per day, keeps 30 days
    from logging.handlers import TimedRotatingFileHandler
    file_handler = TimedRotatingFileHandler(
        LOG_DIR / "arb.log",
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)  # More detail in file
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(file_handler)


setup_logging()
log = logging.getLogger("arb")

# How often to run the main loop
SCAN_INTERVAL = 30           # seconds
SAFETY_CHECK_INTERVAL = 60   # seconds
POSITION_CHECK_INTERVAL = 300  # seconds — check if we should exit carry positions


class ArbBot:
    def __init__(
        self,
        client: BinanceClient,
        signal_config: SignalConfig | None = None,
        risk_config: RiskConfig | None = None,
        dry_run: bool = False,
        position_usdt: float = 100.0,
    ):
        self.client = client
        self.executor = HedgeExecutor(client)
        self.risk = RiskManager(client, self.executor, risk_config, dry_run=dry_run)
        self.signal_config = signal_config or SignalConfig()
        self.dry_run = dry_run
        self.position_usdt = position_usdt

        self._last_safety_check = 0.0
        self._last_position_check = 0.0

        # Trade log
        self.trade_log: list[dict] = []

    def _log_trade(self, action: str, symbol: str, details: dict):
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "symbol": symbol,
            **details,
        }
        self.trade_log.append(entry)
        log.info(f"TRADE: {action} {symbol} {details}")

    async def process_signals(self, signals: list[Signal]):
        """Evaluate signals and open positions if risk allows."""
        profitable = [s for s in signals if s.net_expected_pnl > 0]
        if profitable:
            log.info(f"[SCAN] {len(signals)} signals, {len(profitable)} profitable")
            for s in profitable[:5]:
                log.debug(f"  {s}")

        for signal in profitable:
            symbol = signal.symbol

            # Skip if already in position
            if symbol in self.executor.positions and self.executor.positions[symbol].is_open:
                log.debug(f"Skipping {symbol}: already in position")
                continue

            # Risk check
            allowed, reason = await self.risk.can_open_position(symbol, self.position_usdt, signal.direction)
            if not allowed:
                log.info(f"[RISK] Rejected {symbol}: {reason}")
                continue

            if self.dry_run:
                log.info(
                    f"[DRY RUN] Would open {signal.direction} on {symbol}: "
                    f"rate={signal.funding_rate:+.4%}, net={signal.net_expected_pnl:+.4%}, "
                    f"avg8={signal.avg_rate_8h:+.4%}"
                )
                self._log_trade("dry_open", symbol, {
                    "direction": signal.direction,
                    "funding_rate": signal.funding_rate,
                    "net_pnl": signal.net_expected_pnl,
                    "signal_type": signal.signal_type,
                })
                continue

            # Open position
            try:
                log.info(
                    f"[OPEN] {signal.direction} on {symbol}: "
                    f"rate={signal.funding_rate:+.4%}, net={signal.net_expected_pnl:+.4%}, "
                    f"amount=${self.position_usdt}, type={signal.signal_type}"
                )
                pos = await self.executor.open_hedge(
                    symbol=symbol,
                    direction=signal.direction,
                    usdt_amount=self.position_usdt,
                )
                self._log_trade("open", symbol, {
                    "direction": signal.direction,
                    "quantity": pos.futures_filled_qty,
                    "spot_price": pos.spot_avg_price,
                    "futures_price": pos.futures_avg_price,
                    "basis_spread": pos.basis_spread,
                    "funding_rate": signal.funding_rate,
                    "signal_type": signal.signal_type,
                    "leverage": pos.sizing.max_leverage if pos.sizing else 1,
                })
            except Exception as e:
                log.error(f"[OPEN FAILED] {symbol}: {e}", exc_info=True)

    async def check_exit_conditions(self):
        """Check if any open positions should be closed."""
        for symbol, pos in list(self.executor.positions.items()):
            if not pos.is_open:
                continue

            try:
                # Get current funding rate
                fr_data = await self.client.futures_funding_rate(symbol)
                current_rate = fr_data["funding_rate"]
                abs_rate = abs(current_rate)

                should_close = False
                reason = ""

                # For carry positions: close when funding drops below exit threshold
                if pos.direction == "positive" and current_rate < self.signal_config.carry_exit_threshold:
                    should_close = True
                    reason = f"Positive funding dropped to {current_rate:+.4%}"
                elif pos.direction == "negative" and (-current_rate) < self.signal_config.carry_exit_threshold:
                    should_close = True
                    reason = f"Negative funding weakened to {current_rate:+.4%}"

                # For snipe: close after funding settlement (nextFundingTime changed)
                if pos.metadata.get("signal_type") == "snipe":
                    next_funding = fr_data["next_funding_time"]
                    if next_funding > pos.open_time:
                        should_close = True
                        reason = "Snipe: funding settled"

                if not should_close:
                    log.debug(
                        f"[HOLD] {symbol} {pos.direction}: current_rate={current_rate:+.4%}, "
                        f"threshold={self.signal_config.carry_exit_threshold:.4%}"
                    )
                    continue

                if self.dry_run:
                    log.info(f"[DRY CLOSE] {symbol}: {reason}")
                    self._log_trade("dry_close", symbol, {"reason": reason})
                    pos.status = "closed"
                    del self.executor.positions[symbol]
                    continue

                log.info(f"[CLOSE] {symbol}: {reason}")
                await self.executor.close_hedge(pos)
                self._log_trade("close", symbol, {
                    "reason": reason,
                    "hold_hours": (time.time() * 1000 - pos.open_time) / 3_600_000,
                })

            except Exception as e:
                log.error(f"Error checking exit for {symbol}: {e}")

    async def print_status(self):
        """Print current bot status."""
        summaries = await self.executor.get_open_positions_summary()
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")

        open_count = len(summaries)
        total_upnl = sum(s.get("net_unrealized", 0) for s in summaries)

        log.info(f"[STATUS] Positions: {open_count} | uPnL: {total_upnl:+.4f} USDT")
        for s in summaries:
            log.info(
                f"  {s['symbol']:<16} {s['direction']:<10} "
                f"qty={s['quantity']:.4f}  spot={s['entry_spot']:.4f}  "
                f"futures={s['entry_futures']:.4f}  now={s['current_price']:.4f}  "
                f"uPnL={s['net_unrealized']:+.4f}  interest={s['est_interest_paid']:.4f}"
            )

    async def run(self):
        """Main bot loop."""
        log.info("=" * 60)
        log.info(f"Arb bot starting {'(DRY RUN)' if self.dry_run else '(LIVE)'}")
        log.info(f"Position size: ${self.position_usdt} USDT")
        log.info(f"Max positions: {self.risk.config.max_concurrent_positions}")
        log.info(f"Max exposure: ${self.risk.config.max_total_exposure_usdt}")
        log.info(f"Leverage: {self.risk.config.max_leverage}x")
        log.info(f"Carry entry: {self.signal_config.carry_entry_threshold:.4%}")
        log.info(f"Carry exit: {self.signal_config.carry_exit_threshold:.4%}")
        log.info(f"Snipe threshold: {self.signal_config.snipe_threshold:.4%}")
        log.info(f"Round-trip cost: {self.signal_config.round_trip_cost:.4%}")
        log.info(f"Survival move: {self.risk.config.target_survival_move:.0%}")
        log.info("=" * 60)

        # Load symbol info
        await self.client.load_futures_symbols()
        if not self.dry_run:
            await self.client.load_spot_symbols()

            # Recover any positions from previous run
            self.executor.recover_positions()
            if self.executor.positions:
                log.warning("Found orphaned positions — running safety check")
                await self.risk.run_safety_check()

            # Print initial balances
            spot_bal = await self.client.spot_balances()
            futures_bal = await self.client.futures_balances()
            log.info(f"Spot USDT: {spot_bal.get('USDT', 0):.2f}")
            log.info(f"Futures USDT: {futures_bal.get('USDT', 0):.2f}")

        while True:
            try:
                now = time.time()

                # Safety check
                if now - self._last_safety_check >= SAFETY_CHECK_INTERVAL:
                    if not self.dry_run:
                        safe = await self.risk.run_safety_check()
                        if not safe:
                            log.error("Safety check failed! Pausing for 5 minutes.")
                            await asyncio.sleep(300)
                            continue
                    self._last_safety_check = now

                # Scan for signals
                signals = await scan_once(self.signal_config)
                if signals:
                    await self.process_signals(signals)

                # Check exits
                if now - self._last_position_check >= POSITION_CHECK_INTERVAL:
                    await self.check_exit_conditions()
                    self._last_position_check = now

                # Status
                if self.executor.positions:
                    await self.print_status()

            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"Main loop error: {e}", exc_info=True)

            await asyncio.sleep(SCAN_INTERVAL)

        # Shutdown
        log.info("Shutting down...")
        if not self.dry_run and self.executor.positions:
            log.info("Closing all positions...")
            await self.risk.emergency_close_all()


def main():
    parser = argparse.ArgumentParser(description="Funding rate arbitrage bot")
    parser.add_argument("--dry-run", action="store_true", help="Paper trading mode")
    parser.add_argument("--position-size", type=float, default=100, help="USDT per position")
    parser.add_argument("--max-positions", type=int, default=3, help="Max concurrent positions")
    parser.add_argument("--max-exposure", type=float, default=500, help="Max total USDT exposure")
    args = parser.parse_args()

    api_key, api_secret = load_api_keys(args.dry_run)
    if not api_key:
        return

    init_db()

    signal_config = SignalConfig(
        carry_entry_threshold=0.005,
        snipe_threshold=0.01,
        min_volume_usdt=1_000_000,
    )
    risk_config = RiskConfig(
        max_position_usdt=args.position_size,
        max_total_exposure_usdt=args.max_exposure,
        max_concurrent_positions=args.max_positions,
    )

    client = BinanceClient(api_key, api_secret)

    bot = ArbBot(
        client=client,
        signal_config=signal_config,
        risk_config=risk_config,
        dry_run=args.dry_run,
        position_usdt=args.position_size,
    )

    async def run():
        async with client:
            await bot.run()

    asyncio.run(run())


if __name__ == "__main__":
    main()
