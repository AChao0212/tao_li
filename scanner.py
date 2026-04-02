"""
Real-time signal engine for funding rate arbitrage.

Scans all USDT-M perps, ranks by opportunity, and outputs actionable signals.
Two modes:
  - Carry: sustained funding collection over multiple periods
  - Snipe: single-period capture on extreme funding rates
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp

from config import BASE_URL
from db import get_conn

log = logging.getLogger(__name__)

SCAN_INTERVAL = 30  # seconds between scans


@dataclass
class SignalConfig:
    # Fees (maker + BNB discount)
    spot_fee: float = 0.00075
    perp_fee: float = 0.0002
    slippage: float = 0.0002

    # Carry thresholds (applied to abs(funding_rate))
    carry_entry_threshold: float = 0.005   # 0.5%
    carry_exit_threshold: float = 0.001    # 0.1%

    # Snipe threshold
    snipe_threshold: float = 0.01          # 1.0%

    # Snipe timing: enter within this many seconds before funding settlement
    snipe_window_seconds: int = 120        # 2 minutes before settlement

    # Minimum 24h volume (USDT) to consider a symbol
    min_volume_usdt: float = 1_000_000

    @property
    def round_trip_cost(self) -> float:
        return (self.spot_fee + self.perp_fee) * 2 + self.slippage * 4


@dataclass
class Signal:
    symbol: str
    signal_type: str        # "carry_enter", "carry_exit", "snipe"
    direction: str          # "positive" or "negative"
    funding_rate: float
    estimated_rate: float   # predicted next rate
    mark_price: float
    next_funding_time: int  # ms timestamp
    seconds_to_funding: int
    net_expected_pnl: float # expected PnL after costs (single period)
    avg_rate_8h: float      # rolling average funding rate (last 8 periods = ~24-64h)
    timestamp: int

    def __str__(self):
        direction_arrow = "SHORT perp" if self.direction == "positive" else "LONG perp"
        funding_dt = datetime.fromtimestamp(
            self.next_funding_time / 1000, tz=timezone.utc
        ).strftime("%H:%M:%S")
        return (
            f"[{self.signal_type:>12}] {self.symbol:<16} "
            f"rate={self.funding_rate:>+8.4%}  avg8={self.avg_rate_8h:>+8.4%}  "
            f"net={self.net_expected_pnl:>+8.4%}  "
            f"next_funding={funding_dt} ({self.seconds_to_funding:>5}s)  "
            f"=> {direction_arrow}"
        )


def get_avg_funding_rate(symbol: str, periods: int = 8) -> float | None:
    """Get rolling average funding rate for last N periods. Returns None if no data."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT funding_rate FROM funding_rates
           WHERE symbol = ?
           ORDER BY funding_time DESC LIMIT ?""",
        (symbol, periods),
    ).fetchall()
    conn.close()
    if not rows:
        return None
    return sum(r["funding_rate"] for r in rows) / len(rows)


async def fetch_market_data(session: aiohttp.ClientSession) -> tuple[list[dict], dict]:
    """Fetch premiumIndex and 24h ticker data."""
    premium_url = f"{BASE_URL}/fapi/v1/premiumIndex"
    ticker_url = f"{BASE_URL}/fapi/v1/ticker/24hr"

    async with session.get(premium_url) as resp:
        premium_data = await resp.json()
    async with session.get(ticker_url) as resp:
        ticker_data = await resp.json()

    # Build volume lookup
    volume_map = {}
    for t in ticker_data:
        volume_map[t["symbol"]] = float(t.get("quoteVolume", 0))

    return premium_data, volume_map


def generate_signals(
    premium_data: list[dict],
    volume_map: dict[str, float],
    config: SignalConfig,
) -> list[Signal]:
    """Scan all symbols and generate signals."""
    now_ms = int(time.time() * 1000)
    signals = []

    for item in premium_data:
        symbol = item.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue

        next_funding_time = item.get("nextFundingTime", 0)
        if not next_funding_time:
            continue

        funding_rate = float(item.get("lastFundingRate", 0))
        mark_price = float(item.get("markPrice", 0))
        estimated_rate = float(item.get("interestRate", 0))

        # Skip symbols with invalid price data
        if mark_price <= 0:
            continue

        volume = volume_map.get(symbol, 0)
        if volume < config.min_volume_usdt:
            continue

        abs_rate = abs(funding_rate)
        direction = "positive" if funding_rate > 0 else "negative"
        seconds_to_funding = max(0, (next_funding_time - now_ms) // 1000)
        net_pnl = abs_rate - config.round_trip_cost

        avg_rate = get_avg_funding_rate(symbol)
        # If no historical data, use current rate as fallback for avg
        avg_rate_value = avg_rate if avg_rate is not None else funding_rate

        # Snipe signal: extreme rate + close to funding time
        if abs_rate >= config.snipe_threshold and seconds_to_funding <= config.snipe_window_seconds:
            signals.append(Signal(
                symbol=symbol,
                signal_type="snipe",
                direction=direction,
                funding_rate=funding_rate,
                estimated_rate=estimated_rate,
                mark_price=mark_price,
                next_funding_time=next_funding_time,
                seconds_to_funding=seconds_to_funding,
                net_expected_pnl=net_pnl,
                avg_rate_8h=avg_rate_value,
                timestamp=now_ms,
            ))

        # Carry enter signal: sustained high rate
        elif abs_rate >= config.carry_entry_threshold and abs(avg_rate_value) >= config.carry_entry_threshold * 0.5:
            signals.append(Signal(
                symbol=symbol,
                signal_type="carry_enter",
                direction=direction,
                funding_rate=funding_rate,
                estimated_rate=estimated_rate,
                mark_price=mark_price,
                next_funding_time=next_funding_time,
                seconds_to_funding=seconds_to_funding,
                net_expected_pnl=net_pnl,
                avg_rate_8h=avg_rate_value,
                timestamp=now_ms,
            ))

    # Sort by net expected PnL descending
    signals.sort(key=lambda s: s.net_expected_pnl, reverse=True)
    return signals


async def run_scanner(config: SignalConfig | None = None):
    """Main scanner loop."""
    if config is None:
        config = SignalConfig()

    log.info(f"Signal scanner started (interval={SCAN_INTERVAL}s)")
    log.info(f"Config: carry_entry={config.carry_entry_threshold:.4%}, "
             f"snipe={config.snipe_threshold:.4%}, "
             f"min_vol=${config.min_volume_usdt:,.0f}, "
             f"round_trip={config.round_trip_cost:.4%}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                premium_data, volume_map = await fetch_market_data(session)
                signals = generate_signals(premium_data, volume_map, config)

                now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"\n{'='*100}")
                print(f"SCAN @ {now_str}  |  {len(signals)} signals")
                print(f"{'='*100}")

                if signals:
                    for s in signals:
                        print(s)
                else:
                    print("  No signals at current thresholds.")

            except Exception as e:
                log.error(f"Scanner error: {e}")

            await asyncio.sleep(SCAN_INTERVAL)


async def scan_once(config: SignalConfig | None = None) -> list[Signal]:
    """Run a single scan and return signals (for testing)."""
    if config is None:
        config = SignalConfig()

    async with aiohttp.ClientSession() as session:
        premium_data, volume_map = await fetch_market_data(session)
        return generate_signals(premium_data, volume_map, config)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    asyncio.run(run_scanner())
