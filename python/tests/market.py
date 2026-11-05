"""Deterministic synthetic market data: trades and L2 book snapshots."""

from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa

LEVELS = 10


def trades(n: int = 200_000, seed: int = 7) -> pa.Table:
    """Trades: ms timestamps in bursts, prices on a 0.01 tick grid (a random walk), sizes in
    lots of 100 shares (heavy tailed), a side flag and trade ids with gaps."""
    rng = np.random.default_rng(seed)
    gaps = np.where(rng.random(n) < 0.35, rng.integers(0, 400, n), 0)
    ts = 1_735_829_400_000 + np.cumsum(gaps)
    ticks = 650_000 + np.cumsum(rng.integers(-3, 4, n))
    lots = 1 + rng.integers(0, 20, n) * rng.integers(0, 20, n) // 7
    ids = 4_000_000_000 + np.cumsum(1 + (rng.random(n) < 0.02) * rng.integers(0, 5, n))
    return pa.table(
        {
            "ts": pa.array(ts, pa.timestamp("ms", tz="America/New_York")),
            "price": pa.array(ticks / 100.0, pa.float64()),
            "size": pa.array(lots * 100, pa.int64()),
            "is_buy": pa.array(rng.random(n) < 0.5),
            "trade_id": pa.array(ids, pa.int64()),
        }
    )


def books(n: int = 20_000, seed: int = 11) -> pa.Table:
    """L2 snapshots every 100 ms: the best LEVELS bid and ask prices (0.01 ticks, a few empty
    ticks skipped) and their sizes (lots of 0.001), as list<double>; between snapshots the mid
    moves a few ticks and about a third of the levels change size."""
    rng = np.random.default_rng(seed)
    mid = 650_000 + np.cumsum(rng.integers(-2, 3, n))
    resting: dict[int, int] = {}

    def lot() -> int:
        r = rng.random()
        if r < 0.1:
            return int(rng.choice([100, 250, 500, 1000]))
        if r < 0.4:
            return int(1 + rng.integers(0, 10))
        return int(1 + rng.integers(0, 40) * rng.integers(0, 8))

    sides: dict[str, list[list[float]]] = {"bid_px": [], "ask_px": [], "bid_sz": [], "ask_sz": []}
    for m in mid.tolist():
        for side, sign, start in (("bid", -1, m), ("ask", 1, m + 1)):
            px = start
            prices: list[float] = []
            sizes: list[float] = []
            while len(prices) < LEVELS:
                size = resting.setdefault(px, 0 if rng.random() < 0.125 else lot())
                if rng.random() < 1 / 3:
                    size = resting[px] = 0 if rng.random() < 0.1 else lot()
                if size:
                    prices.append(px / 100.0)
                    sizes.append(size / 1000.0)
                px += sign
            sides[f"{side}_px"].append(prices)
            sides[f"{side}_sz"].append(sizes)
    ts = 1_735_829_400_000 + 100 * np.arange(n)
    columns: dict[str, pa.Array[Any]] = {"ts": pa.array(ts, pa.timestamp("ms", tz="America/New_York"))}
    for name, values in sides.items():
        columns[name] = pa.array(values, pa.list_(pa.float64()))
    return pa.table(columns)
