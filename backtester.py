"""
Funding rate arbitrage backtester.

Strategies:
1. Carry — hold spot long + perp short while funding rate is favorable
2. Snipe — enter ~1 period before settlement, collect funding, exit immediately

Uses historical funding_rates from the DB.
"""

import logging
from dataclasses import dataclass, field

from db import get_conn

log = logging.getLogger(__name__)


@dataclass
class TradeResult:
    symbol: str
    strategy: str  # "carry" or "snipe"
    entry_time: int
    exit_time: int
    periods_held: int
    gross_funding_collected: float  # sum of funding rates collected
    total_fees: float
    net_pnl: float  # gross - fees (as rate, not dollar amount)


@dataclass
class BacktestConfig:
    # Fee structure (Binance VIP0 taker)
    spot_taker_fee: float = 0.001       # 0.1%
    perp_taker_fee: float = 0.0004      # 0.04%
    slippage: float = 0.0002            # 0.02% estimated slippage per side

    # Carry strategy params
    carry_entry_threshold: float = 0.0003    # min funding rate to enter
    carry_exit_threshold: float = 0.0001     # exit when rate drops below
    carry_min_periods: int = 1               # minimum hold periods

    # Snipe strategy params
    snipe_entry_threshold: float = 0.001     # min funding rate to snipe (0.1%)

    # Cost per round trip (enter + exit)
    @property
    def round_trip_cost(self) -> float:
        # spot buy + sell + perp open + close + slippage both sides
        return (self.spot_taker_fee + self.perp_taker_fee) * 2 + self.slippage * 4


def load_funding_history(symbol: str | None = None) -> dict[str, list[dict]]:
    """Load funding rate history grouped by symbol."""
    conn = get_conn()
    if symbol:
        rows = conn.execute(
            "SELECT * FROM funding_rates WHERE symbol = ? ORDER BY funding_time",
            (symbol,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM funding_rates ORDER BY symbol, funding_time"
        ).fetchall()
    conn.close()

    history: dict[str, list[dict]] = {}
    for r in rows:
        d = dict(r)
        history.setdefault(d["symbol"], []).append(d)
    return history


def backtest_carry(
    rates: list[dict], config: BacktestConfig, direction: str = "positive"
) -> list[TradeResult]:
    """
    Backtest carry strategy on a single symbol's funding history.

    direction="positive": long spot + short perp, collect positive funding
    direction="negative": short spot + long perp, collect negative funding (longs get paid)
    """
    trades = []
    in_position = False
    entry_time = 0
    collected = 0.0
    periods = 0

    for r in rates:
        rate = r["funding_rate"]
        t = r["funding_time"]

        if direction == "negative":
            # Flip: we profit from negative rates
            rate = -rate

        if not in_position:
            if rate >= config.carry_entry_threshold:
                in_position = True
                entry_time = t
                collected = rate  # collect first funding
                periods = 1
        else:
            if rate >= config.carry_exit_threshold:
                collected += rate
                periods += 1
            else:
                # Exit
                net = collected - config.round_trip_cost
                strategy = f"carry_{direction}"
                trades.append(
                    TradeResult(
                        symbol=r["symbol"],
                        strategy=strategy,
                        entry_time=entry_time,
                        exit_time=t,
                        periods_held=periods,
                        gross_funding_collected=collected,
                        total_fees=config.round_trip_cost,
                        net_pnl=net,
                    )
                )
                in_position = False
                collected = 0.0
                periods = 0

    # Close any open position at end
    if in_position and periods > 0:
        net = collected - config.round_trip_cost
        strategy = f"carry_{direction}"
        trades.append(
            TradeResult(
                symbol=rates[-1]["symbol"],
                strategy=strategy,
                entry_time=entry_time,
                exit_time=rates[-1]["funding_time"],
                periods_held=periods,
                gross_funding_collected=collected,
                total_fees=config.round_trip_cost,
                net_pnl=net,
            )
        )

    return trades


def backtest_snipe(
    rates: list[dict], config: BacktestConfig
) -> list[TradeResult]:
    """
    Backtest snipe strategy: enter just before settlement if rate is extreme,
    collect one funding payment, exit immediately.
    Works both directions — positive or negative funding.
    """
    trades = []
    for r in rates:
        rate = r["funding_rate"]
        abs_rate = abs(rate)
        if abs_rate >= config.snipe_entry_threshold:
            net = abs_rate - config.round_trip_cost
            direction = "snipe_positive" if rate > 0 else "snipe_negative"
            trades.append(
                TradeResult(
                    symbol=r["symbol"],
                    strategy=direction,
                    entry_time=r["funding_time"],
                    exit_time=r["funding_time"],
                    periods_held=1,
                    gross_funding_collected=abs_rate,
                    total_fees=config.round_trip_cost,
                    net_pnl=net,
                )
            )
    return trades


@dataclass
class BacktestSummary:
    symbol: str
    strategy: str
    total_trades: int
    winning_trades: int
    total_net_pnl: float       # sum of net PnL (as rate)
    avg_net_pnl: float
    max_pnl: float
    min_pnl: float
    win_rate: float
    avg_periods_held: float


def summarize(trades: list[TradeResult]) -> BacktestSummary | None:
    if not trades:
        return None
    winners = [t for t in trades if t.net_pnl > 0]
    pnls = [t.net_pnl for t in trades]
    return BacktestSummary(
        symbol=trades[0].symbol,
        strategy=trades[0].strategy,
        total_trades=len(trades),
        winning_trades=len(winners),
        total_net_pnl=sum(pnls),
        avg_net_pnl=sum(pnls) / len(pnls),
        max_pnl=max(pnls),
        min_pnl=min(pnls),
        win_rate=len(winners) / len(trades),
        avg_periods_held=sum(t.periods_held for t in trades) / len(trades),
    )


def run_backtest(config: BacktestConfig | None = None, top_n: int = 20):
    """Run backtest across all symbols, print top opportunities."""
    if config is None:
        config = BacktestConfig()

    history = load_funding_history()
    print(f"Loaded {len(history)} symbols with funding data\n")
    print(f"Config: entry={config.carry_entry_threshold:.4%}, "
          f"exit={config.carry_exit_threshold:.4%}, "
          f"snipe={config.snipe_entry_threshold:.4%}, "
          f"round_trip_cost={config.round_trip_cost:.4%}\n")

    carry_pos_results = []
    carry_neg_results = []
    snipe_results = []

    for symbol, rates in history.items():
        # Positive carry: long spot + short perp
        ct_pos = backtest_carry(rates, config, direction="positive")
        # Negative carry: short spot + long perp (collect from negative funding)
        ct_neg = backtest_carry(rates, config, direction="negative")
        # Snipe: both directions
        st = backtest_snipe(rates, config)

        cs_pos = summarize(ct_pos)
        cs_neg = summarize(ct_neg)
        ss = summarize(st)

        if cs_pos and cs_pos.total_trades > 0:
            carry_pos_results.append(cs_pos)
        if cs_neg and cs_neg.total_trades > 0:
            carry_neg_results.append(cs_neg)
        if ss and ss.total_trades > 0:
            snipe_results.append(ss)

    carry_pos_results.sort(key=lambda x: x.total_net_pnl, reverse=True)
    carry_neg_results.sort(key=lambda x: x.total_net_pnl, reverse=True)
    snipe_results.sort(key=lambda x: x.total_net_pnl, reverse=True)

    def print_table(title, results, show_hold=True):
        print("=" * 80)
        print(f"{title}")
        print("=" * 80)
        if show_hold:
            print(f"{'Symbol':<14} {'Trades':>6} {'WinRate':>8} {'TotPnL':>10} {'AvgPnL':>10} {'AvgHold':>8}")
            print("-" * 60)
            for s in results[:top_n]:
                print(
                    f"{s.symbol:<14} {s.total_trades:>6} {s.win_rate:>7.1%} "
                    f"{s.total_net_pnl:>+10.4%} {s.avg_net_pnl:>+10.4%} {s.avg_periods_held:>7.1f}p"
                )
        else:
            print(f"{'Symbol':<14} {'Trades':>6} {'WinRate':>8} {'TotPnL':>10} {'AvgPnL':>10}")
            print("-" * 52)
            for s in results[:top_n]:
                print(
                    f"{s.symbol:<14} {s.total_trades:>6} {s.win_rate:>7.1%} "
                    f"{s.total_net_pnl:>+10.4%} {s.avg_net_pnl:>+10.4%}"
                )

    print_table("CARRY POSITIVE (long spot + short perp, collect +funding)", carry_pos_results)
    print()
    print_table("CARRY NEGATIVE (short spot + long perp, collect -funding)", carry_neg_results)
    print()
    print_table("SNIPE (both directions, single period)", snipe_results, show_hold=False)

    # Overall stats
    all_pos = sum(s.total_net_pnl for s in carry_pos_results)
    all_neg = sum(s.total_net_pnl for s in carry_neg_results)
    all_snipe = sum(s.total_net_pnl for s in snipe_results)
    print(f"\n{'='*80}")
    print(f"TOTALS")
    print(f"{'='*80}")
    print(f"Carry positive:  {all_pos:>+10.4%}  ({len(carry_pos_results)} symbols)")
    print(f"Carry negative:  {all_neg:>+10.4%}  ({len(carry_neg_results)} symbols)")
    print(f"Snipe:           {all_snipe:>+10.4%}  ({len(snipe_results)} symbols)")
    print(f"Combined:        {all_pos + all_neg + all_snipe:>+10.4%}")


if __name__ == "__main__":
    run_backtest()
