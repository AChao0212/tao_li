"""
Real-time mark price collector.

Tries WebSocket first. If WS doesn't deliver data (e.g. region-blocked),
falls back to REST polling every REALTIME_POLL_INTERVAL seconds.
"""

import asyncio
import json
import logging
import time

import aiohttp
import websockets

from config import BASE_URL, WS_SNAPSHOT_INTERVAL, WS_URL
from db import insert_snapshots, upsert_funding_rates

log = logging.getLogger(__name__)

REALTIME_POLL_INTERVAL = 5  # seconds, used in REST fallback mode


class MarkPriceTracker:
    """Shared logic for processing mark price updates regardless of source."""

    def __init__(self):
        self.latest: dict[str, dict] = {}
        self.last_funding_time: dict[str, int] = {}
        self.last_flush = time.time()

    def process(self, items: list[dict], key_map: dict):
        """
        Process a list of mark price items.
        key_map translates source keys to our standard keys:
            symbol, funding_rate, mark_price, index_price, next_funding_time
        """
        now_ms = int(time.time() * 1000)
        funding_rows = []

        for item in items:
            symbol = item.get(key_map["symbol"], "")
            if not symbol.endswith("USDT"):
                continue

            funding_rate = float(item.get(key_map["funding_rate"], 0))
            mark_price = float(item.get(key_map["mark_price"], 0))
            index_price = float(item.get(key_map["index_price"], 0))
            next_funding = int(item.get(key_map["next_funding_time"], 0))

            # Detect funding settlement
            prev_funding = self.last_funding_time.get(symbol)
            if prev_funding and next_funding > prev_funding:
                funding_rows.append(
                    {
                        "symbol": symbol,
                        "funding_rate": funding_rate,
                        "funding_time": prev_funding,
                        "mark_price": mark_price,
                    }
                )

            self.last_funding_time[symbol] = next_funding

            self.latest[symbol] = {
                "symbol": symbol,
                "timestamp": now_ms,
                "funding_rate": funding_rate,
                "mark_price": mark_price,
                "index_price": index_price,
                "next_funding_time": next_funding,
                "estimated_rate": funding_rate,
            }

        if funding_rows:
            upsert_funding_rates(funding_rows)
            log.info(f"Detected {len(funding_rows)} funding settlements")

        # Flush snapshots periodically
        now = time.time()
        if now - self.last_flush >= WS_SNAPSHOT_INTERVAL:
            rows = list(self.latest.values())
            insert_snapshots(rows)
            self.latest.clear()
            self.last_flush = now
            log.debug(f"Snapshot flush: {len(rows)} symbols")


# Key mappings for different data sources
WS_KEYS = {
    "symbol": "s",
    "funding_rate": "r",
    "mark_price": "p",
    "index_price": "i",
    "next_funding_time": "T",
}

REST_KEYS = {
    "symbol": "symbol",
    "funding_rate": "lastFundingRate",
    "mark_price": "markPrice",
    "index_price": "indexPrice",
    "next_funding_time": "nextFundingTime",
}


async def _try_websocket(tracker: MarkPriceTracker) -> bool:
    """Try WS stream. Returns False if no data received within 15s."""
    try:
        log.info("Attempting WebSocket connection...")
        async with websockets.connect(WS_URL, ping_interval=30) as ws:
            # Wait for first message with timeout
            msg = await asyncio.wait_for(ws.recv(), timeout=15)
            data = json.loads(msg)
            if isinstance(data, list) and len(data) > 0:
                tracker.process(data, WS_KEYS)
                log.info(f"WebSocket working, got {len(data)} symbols")
                # Continue streaming
                async for msg in ws:
                    data = json.loads(msg)
                    if isinstance(data, list):
                        tracker.process(data, WS_KEYS)
            return True
    except (asyncio.TimeoutError, Exception) as e:
        log.warning(f"WebSocket not available: {e}")
        return False


async def _rest_fallback(tracker: MarkPriceTracker, session: aiohttp.ClientSession):
    """Poll premiumIndex as real-time fallback."""
    log.info(f"Using REST polling fallback (every {REALTIME_POLL_INTERVAL}s)")
    url = f"{BASE_URL}/fapi/v1/premiumIndex"
    while True:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
            tracker.process(data, REST_KEYS)
        except Exception as e:
            log.error(f"REST poll error: {e}")
        await asyncio.sleep(REALTIME_POLL_INTERVAL)


async def stream_mark_prices(session: aiohttp.ClientSession):
    """
    Collect real-time mark prices.
    Tries WebSocket first, falls back to REST polling if WS is unavailable.
    """
    tracker = MarkPriceTracker()

    # Try WebSocket first
    ws_ok = await _try_websocket(tracker)
    if ws_ok:
        # WS exited (disconnected) — retry with backoff
        backoff = 1
        while True:
            await asyncio.sleep(backoff)
            ws_ok = await _try_websocket(tracker)
            if not ws_ok:
                break
            backoff = min(backoff * 2, 60)

    # Fall back to REST polling
    await _rest_fallback(tracker, session)
