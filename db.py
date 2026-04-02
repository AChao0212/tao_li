import sqlite3
from config import DATA_DIR, DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS funding_rates (
            symbol          TEXT    NOT NULL,
            funding_rate    REAL    NOT NULL,
            funding_time    INTEGER NOT NULL,
            mark_price      REAL,
            PRIMARY KEY (symbol, funding_time)
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            symbol              TEXT    NOT NULL,
            timestamp           INTEGER NOT NULL,
            funding_rate        REAL,
            mark_price          REAL,
            index_price         REAL,
            next_funding_time   INTEGER,
            estimated_rate      REAL,
            PRIMARY KEY (symbol, timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_funding_symbol
            ON funding_rates(symbol);
        CREATE INDEX IF NOT EXISTS idx_funding_time
            ON funding_rates(funding_time);
        CREATE INDEX IF NOT EXISTS idx_snapshots_symbol
            ON snapshots(symbol);
    """)
    conn.close()


def upsert_funding_rates(rows: list[dict]):
    if not rows:
        return
    conn = get_conn()
    conn.executemany(
        """INSERT OR IGNORE INTO funding_rates
           (symbol, funding_rate, funding_time, mark_price)
           VALUES (:symbol, :funding_rate, :funding_time, :mark_price)""",
        rows,
    )
    conn.commit()
    conn.close()


def insert_snapshots(rows: list[dict]):
    if not rows:
        return
    conn = get_conn()
    conn.executemany(
        """INSERT OR IGNORE INTO snapshots
           (symbol, timestamp, funding_rate, mark_price, index_price,
            next_funding_time, estimated_rate)
           VALUES (:symbol, :timestamp, :funding_rate, :mark_price,
                   :index_price, :next_funding_time, :estimated_rate)""",
        rows,
    )
    conn.commit()
    conn.close()


def get_latest_funding_time(symbol: str) -> int | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(funding_time) as t FROM funding_rates WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    conn.close()
    return row["t"] if row and row["t"] else None
