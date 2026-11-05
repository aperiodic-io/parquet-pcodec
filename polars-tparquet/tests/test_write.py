"""DataFrame.tparquet.write, and the registration of the `tparquet` namespaces."""

from __future__ import annotations

import io
import pathlib
from collections.abc import Callable

import polars as pl
import pytest
import tparquet
from polars.testing import assert_frame_equal

import polars_tparquet as pt
from conftest import ticks

Write = Callable[..., pathlib.Path]


def test_importing_registers_the_namespaces() -> None:
    assert hasattr(pl.DataFrame({"a": [1]}), "tparquet")
    assert hasattr(pl.LazyFrame({"a": [1]}), "tparquet")


def test_write_round_trips_and_returns_the_codecs(tmp_path: pathlib.Path) -> None:
    df = ticks(5000)
    codecs = df.tparquet.write(tmp_path / "t.parquet")
    # Auto: PCODEC where it is smaller (the small cycling qty compresses better with ZSTD)
    assert codecs == {"ts": "PCODEC", "price": "PCODEC", "qty": "ZSTD", "sym": "ZSTD"}
    assert_frame_equal(pt.read_tparquet(tmp_path / "t.parquet"), df)
    assert tparquet.inspect(tmp_path / "t.parquet")["pcodec_version"] == "1"


def test_write_options(tmp_path: pathlib.Path) -> None:
    df = ticks(5000)
    base = df.tparquet.write(tmp_path / "base.parquet", policy="base", base="snappy")
    assert set(base.values()) == {"SNAPPY"}
    forced = df.tparquet.write(tmp_path / "f.parquet", policy="pcodec", columns={"price": "base"}, level=0)
    assert forced == {"ts": "PCODEC", "price": "ZSTD", "qty": "PCODEC", "sym": "ZSTD"}
    df.tparquet.write(tmp_path / "rg.parquet", row_group_size=1000)
    assert tparquet.ParquetFile(tmp_path / "rg.parquet").metadata.num_row_groups == 5
    with pytest.raises(tparquet.PcodecError, match="level"):
        df.tparquet.write(tmp_path / "bad.parquet", level=13)


def test_the_default_level_is_8(tmp_path: pathlib.Path) -> None:
    df = ticks(20_000).with_columns(noisy=(pl.col("ts") * 7919) % 100_003)
    default, eight, nine = (tmp_path / f"{n}.parquet" for n in ("default", "8", "9"))
    df.tparquet.write(default)
    df.tparquet.write(eight, level=8)
    df.tparquet.write(nine, level=9)
    assert default.read_bytes() == eight.read_bytes() != nine.read_bytes()
    df.lazy().tparquet.sink(default)
    df.lazy().tparquet.sink(eight, level=8)
    df.lazy().tparquet.sink(nine, level=9)
    assert default.read_bytes() == eight.read_bytes() != nine.read_bytes()


def test_write_to_a_file_object() -> None:
    buf = io.BytesIO()
    ticks(100).tparquet.write(buf)
    assert_frame_equal(pt.read_tparquet(io.BytesIO(buf.getvalue())), ticks(100))
