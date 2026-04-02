from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "funding.db"

# Binance Futures API
BASE_URL = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/ws/!markPrice@arr@3s"

# Collection settings
REST_POLL_INTERVAL = 300        # seconds between premiumIndex snapshots
WS_SNAPSHOT_INTERVAL = 60       # seconds between DB writes from websocket
FUNDING_BACKFILL_DAYS = 30      # how far back to fetch on first run
SYMBOL_REFRESH_INTERVAL = 3600  # seconds between exchangeInfo refreshes

# Rate limiting
REST_REQUEST_DELAY = 0.1        # seconds between paginated REST requests
