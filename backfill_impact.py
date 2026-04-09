"""
Backfill historical settlement impact data from Binance.

For each USDT futures symbol, fetches the past N days of funding rates.
For each settlement event with |rate| >= MIN_RATE, fetches aggTrades
around the settlement and computes dump_pct, recovery_pct, and volumes.

Saves to settlement_impact table — same schema the live collector uses.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from db import init_db, save_settlement_impact
from exchange import BinanceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill")

SECRET_PATH = Path.home() / ".secret" / "binance.txt"

DAYS_BACK = 14
MIN_RATE = 0.008  # only events with |rate| >= 0.8%


def load_keys() -> tuple[str, str]:
    key = sec = ""
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
                    key = v
                elif k == "BINANCE_API_SECRET":
                    sec = v
    return key, sec


async def fetch_funding_history(client: BinanceClient, symbol: str, start_time_ms: int) -> list[dict]:
    """Get funding rate history for a symbol from start_time onward."""
    try:
        data = await client._futures_get("/fapi/v1/fundingRate", {
            "symbol": symbol,
            "startTime": start_time_ms,
            "limit": 1000,
        })
        if isinstance(data, list):
            return data
    except Exception as e:
        log.warning(f"funding history failed for {symbol}: {e}")
    return []


async def fetch_settlement_impact(
    client: BinanceClient, symbol: str, funding_time_ms: int, rate: float
) -> dict | None:
    """Fetch aggTrades around a settlement and compute impact metrics."""
    try:
        trades = await client._futures_get("/fapi/v1/aggTrades", {
            "symbol": symbol,
            "startTime": funding_time_ms - 5000,
            "endTime": funding_time_ms + 35000,
            "limit": 1000,
        })

        if not isinstance(trades, list) or len(trades) < 5:
            return None

        sec_data: dict[int, dict] = {}
        for t in trades:
            sec = (t["T"] - funding_time_ms) // 1000
            price = float(t["p"])
            qty = float(t["q"])
            is_sell = t["m"]
            if sec not in sec_data:
                sec_data[sec] = {"price": price, "buy_vol": 0.0, "sell_vol": 0.0}
            sec_data[sec]["price"] = price
            if is_sell:
                sec_data[sec]["sell_vol"] += qty
            else:
                sec_data[sec]["buy_vol"] += qty

        def get_price(*offsets):
            for o in offsets:
                if o in sec_data:
                    return sec_data[o]["price"]
            return None

        p_before = get_price(-2, -1, -3)
        p_at = get_price(0, 1, -1)
        p_3 = get_price(3, 2, 4)
        p_10 = get_price(10, 9, 11, 8, 12)
        p_30 = get_price(30, 29, 28, 31, 27)

        if not (p_before and p_at):
            return None

        sell_vol = sum(d["sell_vol"] for s, d in sec_data.items() if 0 <= s <= 3)
        buy_vol = sum(d["buy_vol"] for s, d in sec_data.items() if 0 <= s <= 3)

        dump_pct = (p_at - p_before) / p_before
        recovery_pct = (p_10 - p_at) / p_at if p_10 else None

        return {
            "symbol": symbol,
            "funding_time": funding_time_ms,
            "funding_rate": rate,
            "price_before": p_before,
            "price_at": p_at,
            "price_after_3s": p_3,
            "price_after_10s": p_10,
            "price_after_30s": p_30,
            "sell_volume_0_3": sell_vol,
            "buy_volume_0_3": buy_vol,
            "dump_pct": dump_pct,
            "recovery_pct": recovery_pct,
        }
    except Exception as e:
        log.warning(f"impact fetch failed for {symbol} @ {funding_time_ms}: {e}")
        return None


async def main():
    init_db()
    key, sec = load_keys()
    if not key:
        log.error("No API keys")
        return

    client = BinanceClient(key, sec)
    async with client:
        # Get all USDT-margined futures symbols
        await client.load_futures_symbols()
        symbols = [s for s in client._futures_symbols.keys() if s.endswith("USDT")]
        log.info(f"Found {len(symbols)} USDT futures symbols")

        start_time_ms = int(time.time() * 1000) - DAYS_BACK * 86400 * 1000

        # Step 1: Fetch funding histories in parallel batches
        log.info(f"Fetching {DAYS_BACK} days of funding history...")
        all_events: list[tuple[str, int, float]] = []  # (symbol, time, rate)

        BATCH = 20
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i : i + BATCH]
            results = await asyncio.gather(
                *[fetch_funding_history(client, s, start_time_ms) for s in batch],
                return_exceptions=True,
            )
            for sym, hist in zip(batch, results):
                if isinstance(hist, Exception) or not hist:
                    continue
                for fr in hist:
                    rate = float(fr.get("fundingRate", 0))
                    ft = fr.get("fundingTime", 0)
                    if abs(rate) >= MIN_RATE and ft:
                        all_events.append((sym, ft, rate))
            log.info(f"  Progress: {min(i + BATCH, len(symbols))}/{len(symbols)} symbols, {len(all_events)} high-rate events so far")

        log.info(f"Found {len(all_events)} high-rate settlement events")

        # Step 2: Fetch impact for each event
        log.info("Fetching aggTrades around each settlement...")
        saved = 0
        skipped = 0
        BATCH2 = 10
        for i in range(0, len(all_events), BATCH2):
            batch = all_events[i : i + BATCH2]
            results = await asyncio.gather(
                *[fetch_settlement_impact(client, sym, ft, rate) for sym, ft, rate in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception) or r is None:
                    skipped += 1
                    continue
                try:
                    save_settlement_impact(r)
                    saved += 1
                except Exception as e:
                    log.warning(f"save failed: {e}")
                    skipped += 1
            if (i + BATCH2) % 100 == 0 or i + BATCH2 >= len(all_events):
                log.info(f"  Progress: {min(i + BATCH2, len(all_events))}/{len(all_events)} events, saved={saved} skipped={skipped}")

        log.info(f"Done. Saved {saved} impact records, skipped {skipped}")


if __name__ == "__main__":
    asyncio.run(main())
