# tao_li 套利

Binance funding rate arbitrage system — a fully hedged, delta-neutral strategy that captures funding rate payments on USDT-M perpetual futures.

## Strategy

Perpetual futures charge a **funding rate** every 8 hours to anchor the perp price to spot. When funding is deeply negative (shorts pay longs), we:

1. **Short spot** (cross margin borrow + sell)
2. **Long futures** (perpetual)

Delta-neutral: price movement cancels out. Profit = funding collected - fees - borrow interest.

When funding is positive (longs pay shorts), we reverse: buy spot + short futures.

### Two modes

| Mode | Hold time | Entry condition |
|------|-----------|----------------|
| **Carry** | Hours to days | Sustained funding rate > threshold |
| **Snipe** | ~2 minutes | Extreme funding spike (≥1%) near settlement |

## Architecture

```
tao_li/
├── exchange/              # Reusable Binance client module
│   ├── client.py          # Async API client (spot, futures, cross margin)
│   ├── executor.py        # Hedged order execution (simultaneous legs)
│   ├── margin.py          # Position sizing math (survive ±30% moves)
│   ├── risk.py            # Dual margin monitoring + emergency close
│   └── types.py           # Order, Position, SymbolInfo types
├── scanner.py             # Real-time signal engine (ranks all USDT-M perps)
├── backtester.py          # Historical backtest (carry + snipe, both directions)
├── collector_rest.py      # Funding rate data collection (REST)
├── collector_ws.py        # Real-time data (WebSocket with REST fallback)
├── arb.py                 # Main arbitrage bot
├── db.py                  # SQLite storage
├── config.py              # Constants
└── main.py                # Data collector entry point
```

## Risk Management

The key challenge: even though the overall position is delta-neutral, **each leg has its own margin account**. A price spike can trigger margin call on one side before you can transfer gains from the other.

**Position sizing** ensures both legs survive a ±30% price move independently:
- Spot cross margin level stays ≥ 1.5 (Binance liquidates at 1.1)
- Futures margin stays well above maintenance
- Dual margin monitoring every 60s with automatic emergency close

**Capital efficiency** (1x leverage, 30% survival):
- $500 capital → ~$256 max notional position
- ~51% capital efficiency (safety over returns)

## Usage

### 1. Collect data
```bash
python main.py
```
Backfills 30 days of funding rates for all 500+ USDT-M perps, then streams live data.

### 2. Backtest
```bash
python backtester.py
```

### 3. Run scanner (signal only)
```bash
python scanner.py
```

### 4. Paper trading
```bash
python arb.py --dry-run --position-size 100 --max-positions 3
```

### 5. Live trading
```bash
python arb.py --position-size 100 --max-positions 3 --max-exposure 400
```

API keys are read from `~/.secret/binance.txt`.

## Requirements

- Python 3.10+
- `aiohttp`, `websockets`
- Binance account with spot, margin, and futures enabled

## Disclaimer

This is for educational and personal use. Trading cryptocurrency involves significant risk. Use at your own risk.
