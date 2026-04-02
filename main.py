import asyncio
import logging
import signal
import sys

import aiohttp

from collector_rest import backfill_funding_rates, fetch_perpetual_symbols, poll_premium_index
from collector_ws import stream_mark_prices
from db import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tao_li")


async def main():
    init_db()
    log.info("Database initialized")

    async with aiohttp.ClientSession() as session:
        # Fetch symbol list
        symbols = await fetch_perpetual_symbols(session)

        # Backfill historical funding rates
        log.info("Starting backfill...")
        await backfill_funding_rates(session, symbols)

        # Run REST poller and WebSocket stream concurrently
        log.info("Starting live collectors...")
        await asyncio.gather(
            poll_premium_index(session),
            stream_mark_prices(session),
        )


def shutdown(signum, frame):
    log.info("Shutting down...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    asyncio.run(main())
