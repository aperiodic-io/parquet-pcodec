"""LazyFrame.tparquet.sink: a query streamed into one file."""

from __future__ import annotations

import io
import os
import pathlib
import re

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


def _boom(df: pl.DataFrame) -> pl.DataFrame:
    raise RuntimeError("boom")


def test_a_sink_over_its_own_input_replaces_it(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "t.parquet"
    ticks(1000).tparquet.write(path, row_group_size=100)
    want = ticks(1000).filter(pl.col("qty") > 10)
    pt.scan_tparquet(path).filter(pl.col("qty") > 10).tparquet.sink(path, chunk_size=64)
    assert_frame_equal(pt.read_tparquet(path), want)
    assert os.listdir(tmp_path) == ["t.parquet"]


def test_a_failing_sink_keeps_the_file_it_would_replace(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "t.parquet"
    ticks(10).tparquet.write(path)
    before = path.read_bytes()
    lf = ticks(100).lazy().map_batches(_boom, schema=ticks(1).schema)
    with pytest.raises(RuntimeError, match="boom"):
        lf.tparquet.sink(path)
    assert path.read_bytes() == before
    assert os.listdir(tmp_path) == ["t.parquet"]


def test_options_the_writer_rejects_leave_no_file(tmp_path: pathlib.Path) -> None:
    # the options are checked before the file is created, the column names only after
    with pytest.raises(tparquet.PcodecError, match=r'^no column named "nope"$'):
        ticks(10).lazy().tparquet.sink(tmp_path / "t.parquet", columns={"nope": "pcodec"})
    assert os.listdir(tmp_path) == []


def test_a_footer_that_cannot_be_written_leaves_no_file(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def disk_full(self: tparquet.Writer) -> dict[str, str]:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(tparquet.Writer, "close", disk_full)
    with pytest.raises(OSError, match="No space left on device"):
        ticks(10).lazy().tparquet.sink(tmp_path / "t.parquet")
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize("existing", [False, True], ids=["new", "existing"])
def test_an_interrupt_while_writing_leaves_the_target_as_it_was(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, existing: bool
) -> None:
    path = tmp_path / "t.parquet"
    if existing:
        ticks(3).tparquet.write(path)
    before = sorted(os.listdir(tmp_path)), path.read_bytes() if existing else None
    written: list[int] = []
    write = tparquet.Writer.write

    def interrupted(self: tparquet.Writer, data: pl.DataFrame) -> None:
        if written:
            raise KeyboardInterrupt
        written.append(data.height)
        write(self, data)

    monkeypatch.setattr(tparquet.Writer, "write", interrupted)
    with pytest.raises(KeyboardInterrupt):
        ticks(100).lazy().tparquet.sink(path, chunk_size=10)
    assert written == [10]
    assert (sorted(os.listdir(tmp_path)), path.read_bytes() if existing else None) == before


def test_the_target_appears_only_once_complete(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []
    write = tparquet.Writer.write

    def recording(self: tparquet.Writer, data: pl.DataFrame) -> None:
        seen.append(os.listdir(tmp_path))
        write(self, data)

    monkeypatch.setattr(tparquet.Writer, "write", recording)
    ticks(30).lazy().tparquet.sink(tmp_path / "out.parquet", chunk_size=10)
    # a hidden file beside the target, which a scan of the directory skips
    assert len(seen) == 3
    assert all(len(names) == 1 and re.fullmatch(r"\.out\.parquet\.[0-9a-f]{32}\.tmp", names[0]) for names in seen)
    assert len({names[0] for names in seen}) == 1
    assert os.listdir(tmp_path) == ["out.parquet"]


def test_a_sink_through_a_symlink_writes_the_file_it_links_to(tmp_path: pathlib.Path) -> None:
    real = tmp_path / "data" / "t.parquet"
    real.parent.mkdir()
    ticks(3).tparquet.write(real)
    link = tmp_path / "link.parquet"
    link.symlink_to(real)
    ticks(10).lazy().tparquet.sink(link)
    assert link.is_symlink()
    assert_frame_equal(pt.read_tparquet(real), ticks(10))
    assert os.listdir(real.parent) == ["t.parquet"]


def test_a_sink_into_a_missing_directory_names_it(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError, match=re.escape(str(tmp_path / "no"))) as e:
        ticks(10).lazy().tparquet.sink(tmp_path / "no" / "t.parquet")
    assert ".tmp" not in str(e.value)
    assert os.listdir(tmp_path) == []


def test_a_sink_onto_a_directory_fails_and_leaves_no_file(tmp_path: pathlib.Path) -> None:
    (tmp_path / "d").mkdir()
    with pytest.raises(IsADirectoryError):
        ticks(10).lazy().tparquet.sink(tmp_path / "d")
    assert os.listdir(tmp_path) == ["d"]
    assert os.listdir(tmp_path / "d") == []
