"""LazyFrame.tparquet.sink: a query streamed into one file."""

from __future__ import annotations

import io
import pathlib

import polars as pl
import pytest
import tparquet
from polars.testing import assert_frame_equal

import polars_tparquet as pt
from conftest import ticks


def test_sink_streams_the_query_into_one_file(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sources = []
    for i in range(3):
        sources.append(tmp_path / f"in{i}.parquet")
        ticks(2000, offset=2000 * i).tparquet.write(sources[-1], row_group_size=500)
    lf = pt.scan_tparquet(sources).filter(pl.col("qty") > 5).with_columns(notional=pl.col("price") * pl.col("qty"))
    want = lf.collect()

    # sink never materializes the whole result
    def no_collect(*args: object, **kwargs: object) -> None:
        raise AssertionError("sink called LazyFrame.collect")

    monkeypatch.setattr(pl.LazyFrame, "collect", no_collect)
    codecs = lf.tparquet.sink(tmp_path / "out.parquet", chunk_size=1000, row_group_size=1000)
    monkeypatch.undo()

    assert codecs["notional"] == "PCODEC"
    assert_frame_equal(pt.read_tparquet(tmp_path / "out.parquet"), want)
    groups = tparquet.ParquetFile(tmp_path / "out.parquet").metadata.num_row_groups
    assert groups == -(-want.height // 1000)


def test_sink_options_and_file_objects(tmp_path: pathlib.Path) -> None:
    lf = ticks(3000).lazy()
    assert set(lf.tparquet.sink(tmp_path / "b.parquet", policy="base").values()) == {"ZSTD"}
    buf = io.BytesIO()
    lf.tparquet.sink(buf, level=2, columns={"ts": "base"}, row_group_size=1000)
    out = io.BytesIO(buf.getvalue())
    assert_frame_equal(pt.read_tparquet(out), ticks(3000))
    assert tparquet.ParquetFile(io.BytesIO(buf.getvalue())).metadata.num_row_groups == 3


def test_sink_passes_every_option_to_the_writer(tmp_path: pathlib.Path) -> None:
    lf = ticks(3000).lazy()
    codecs = lf.tparquet.sink(tmp_path / "a.parquet", policy="pcodec", columns={"ts": "base"}, base="snappy")
    assert codecs == {"ts": "SNAPPY", "price": "PCODEC", "qty": "PCODEC", "sym": "SNAPPY"}
    with pytest.raises(tparquet.PcodecError, match="level"):
        lf.tparquet.sink(tmp_path / "b.parquet", level=13)


def test_sink_batches_have_chunk_size_rows(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    heights: list[int] = []
    write = tparquet.Writer.write

    def recording(self: tparquet.Writer, data: pl.DataFrame) -> None:
        heights.append(data.height)
        write(self, data)

    monkeypatch.setattr(tparquet.Writer, "write", recording)
    ticks(1000).lazy().tparquet.sink(tmp_path / "a.parquet", chunk_size=300)
    assert sum(heights) == 1000
    assert max(heights) <= 300
    heights.clear()
    ticks(5).lazy().tparquet.sink(tmp_path / "b.parquet", chunk_size=1)
    assert heights == [1, 1, 1, 1, 1]


def test_sink_of_an_empty_result_writes_the_schema(tmp_path: pathlib.Path) -> None:
    lf = ticks(100).lazy().filter(pl.col("ts") < 0)
    lf.tparquet.sink(tmp_path / "empty.parquet")
    out = pt.read_tparquet(tmp_path / "empty.parquet")
    assert out.height == 0
    assert out.schema == ticks(1).schema


def test_a_failing_query_leaves_no_file(tmp_path: pathlib.Path) -> None:
    def boom(df: pl.DataFrame) -> pl.DataFrame:
        raise RuntimeError("boom")

    lf = ticks(100).lazy().map_batches(boom, schema=ticks(1).schema)
    with pytest.raises(RuntimeError, match="boom"):
        lf.tparquet.sink(tmp_path / "out.parquet")
    assert not (tmp_path / "out.parquet").exists()


def test_bad_chunk_size(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="chunk_size must be positive"):
        ticks(10).lazy().tparquet.sink(tmp_path / "x.parquet", chunk_size=0)
