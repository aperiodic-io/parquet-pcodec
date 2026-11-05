"""Fixtures of the polars-tparquet tests."""

from __future__ import annotations

import os
import pathlib
from collections.abc import Callable
from typing import Any

import polars as pl
import pytest
import tparquet
from hypothesis import HealthCheck, settings
from tparquet.polars import write_parquet

_slow_ok = [HealthCheck.too_slow, HealthCheck.function_scoped_fixture]
# HYPOTHESIS_PROFILE: "default", "thorough" (CI weekly) or "mutation" (fixed examples)
settings.register_profile("default", max_examples=40, deadline=None, suppress_health_check=_slow_ok)
settings.register_profile("thorough", max_examples=400, deadline=None, suppress_health_check=_slow_ok)
settings.register_profile("mutation", max_examples=10, deadline=None, derandomize=True, suppress_health_check=_slow_ok)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))

REPO = pathlib.Path(__file__).resolve().parents[2]
TESTDATA = pathlib.Path(os.environ.get("TPARQUET_TESTDATA", REPO / "testdata"))
VECTORS = sorted((TESTDATA / "files").glob("*.parquet"))


def ticks(n: int, offset: int = 0) -> pl.DataFrame:
    """A small time series: millisecond timestamps, 0.01-tick prices, a size with nulls, a symbol."""
    i = pl.col("i")
    return (
        pl.DataFrame({"i": pl.int_range(offset, offset + n, eager=True)})
        .select(
            ts=1_700_000_000_000 + i * 250,
            price=(10_000 + (i * 37) % 500) / 100,
            qty=pl.when(i % 7 == 0).then(None).otherwise((i % 50) + 1).cast(pl.Int32),
        )
        .with_columns(sym=pl.Series(["BTC", "ETH", "SOL"] * (n // 3 + 1))[:n])
    )


@pytest.fixture
def write(tmp_path: pathlib.Path) -> Callable[..., pathlib.Path]:
    """Writes a frame as a PCODEC file under tmp_path and returns its path."""

    def write(name: str, df: pl.DataFrame, row_group_size: int | None = None) -> pathlib.Path:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(df, path, row_group_size=row_group_size)
        return path

    return write


class Reads:
    """What every `ParquetFile.stream` call of the test asked for."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def columns(self) -> list[Any]:
        return [c["columns"] for c in self.calls]

    def row_groups(self) -> list[Any]:
        return [c["row_groups"] for c in self.calls]


@pytest.fixture
def reads(monkeypatch: pytest.MonkeyPatch) -> Reads:
    spy = Reads()
    stream = tparquet.ParquetFile.stream

    def recording(self: tparquet.ParquetFile, *args: Any, **kwargs: Any) -> Any:
        names = ["columns", "row_groups", "batch_size", "limit"]
        call = dict(zip(names, args))
        call.update(kwargs)
        spy.calls.append({n: call.get(n) for n in names})
        return stream(self, *args, **kwargs)

    monkeypatch.setattr(tparquet.ParquetFile, "stream", recording)
    return spy
