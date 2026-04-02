import asyncio
import logging
import time

import aiohttp

from config import (
    BASE_URL,
    FUNDING_BACKFILL_DAYS,
    REST_POLL_INTERVAL,
    REST_REQUEST_DELAY,
    SYMBOL_REFRESH_INTERVAL,
)
from db import get_latest_funding_time, insert_snapshots, upsert_funding_rates

log = logging.getLogger(__name__)


async def fetch_perpetual_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Get all USDT-M perpetual symbols from Binance."""
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    async with session.get(url) as resp:
        data = await resp.json()
    symbols = [
        s["symbol"]
        for s in data["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    ]
    log.info(f"Found {len(symbols)} USDT-M perpetual symbols")
    return symbols


async def backfill_funding_rates(session: aiohttp.ClientSession, symbols: list[str]):
    """Fetch historical funding rates for all symbols."""
    start_ms = int((time.time() - FUNDING_BACKFILL_DAYS * 86400) * 1000)

    for i, symbol in enumerate(symbols):
        latest = get_latest_funding_time(symbol)
        since = max(start_ms, latest + 1) if latest else start_ms

        rows = []
        cursor = since
        while True:
            url = f"{BASE_URL}/fapi/v1/fundingRate"
            params = {"symbol": symbol, "startTime": cursor, "limit": 1000}
            async with session.get(url, params=params) as resp:
                data = await resp.json()

            if not data:
                break

            for r in data:
                rows.append(
                    {
                        "symbol": r["symbol"],
                        "funding_rate": float(r["fundingRate"]),
                        "funding_time": r["fundingTime"],
                        "mark_price": float(r.get("markPrice", 0)) or None,
                    }
                )
            cursor = data[-1]["fundingTime"] + 1

            if len(data) < 1000:
                break
            await asyncio.sleep(REST_REQUEST_DELAY)

        if rows:
            upsert_funding_rates(rows)

        if (i + 1) % 50 == 0:
            log.info(f"Backfill progress: {i + 1}/{len(symbols)} symbols")
        await asyncio.sleep(REST_REQUEST_DELAY)

    log.info(f"Backfill complete: {len(symbols)} symbols")


async def poll_premium_index(session: aiohttp.ClientSession):
    """Periodically fetch premiumIndex for all symbols."""
    while True:
        try:
            url = f"{BASE_URL}/fapi/v1/premiumIndex"
            async with session.get(url) as resp:
                data = await resp.json()

            now_ms = int(time.time() * 1000)
            rows = []
            for item in data:
                if not item.get("symbol", "").endswith("USDT"):
                    continue
                rows.append(
                    {
                        "symbol": item["symbol"],
                        "timestamp": now_ms,
                        "funding_rate": float(item.get("lastFundingRate", 0)),
                        "mark_price": float(item.get("markPrice", 0)),
                        "index_price": float(item.get("indexPrice", 0)),
                        "next_funding_time": item.get("nextFundingTime"),
                        "estimated_rate": float(
                            item.get("interestRate", 0)
                        ),  # estimated component
                    }
                )

            insert_snapshots(rows)
            log.info(f"REST snapshot: {len(rows)} symbols")

        except Exception as e:
            log.error(f"REST poll error: {e}")

        await asyncio.sleep(REST_POLL_INTERVAL)


async def refresh_symbols_loop() -> list[str]:
    """Periodically refresh the symbol list. Returns initial list."""
    # This is called from main to get the initial list,
    # then the main loop handles periodic refresh.
    async with aiohttp.ClientSession() as session:
        return await fetch_perpetual_symbols(session)
