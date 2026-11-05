"""scan_tparquet: one file, lists, globs and directories, lazily, with the query pushed down."""

from __future__ import annotations

import io
import pathlib
import re
from collections.abc import Callable

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_tparquet as pt
from conftest import Reads, ticks

Write = Callable[..., pathlib.Path]


def test_one_file_as_str_and_as_path(write: Write) -> None:
    df = ticks(100)
    path = write("a.parquet", df)
    assert_frame_equal(pt.scan_tparquet(str(path)).collect(), df)
    assert_frame_equal(pt.scan_tparquet(path).collect(), df)


def test_a_list_is_read_in_order(write: Write) -> None:
    a, b, c = ticks(10), ticks(20, offset=10), ticks(5, offset=30)
    paths = [write("c.parquet", c), write("a.parquet", a), write("b.parquet", b)]
    assert_frame_equal(pt.scan_tparquet(paths).collect(), pl.concat([c, a, b]))
    assert_frame_equal(pt.scan_tparquet(tuple(paths)).collect(), pl.concat([c, a, b]))


def test_a_glob_matches_in_sorted_order(write: Write, tmp_path: pathlib.Path) -> None:
    b = write("d/2024/b.parquet", ticks(10, offset=10))
    a = write("d/2024/a.parquet", ticks(10))
    write("d/2024/notes.txt.parquet.bak", ticks(1))
    deep = write("d/2025/01/c.parquet", ticks(10, offset=20))
    want = pl.concat([pt.read_tparquet(p) for p in (a, b)])
    assert_frame_equal(pt.scan_tparquet(str(tmp_path / "d/2024/*.parquet")).collect(), want)
    everything = pt.scan_tparquet(str(tmp_path / "d/**/*.parquet")).collect()
    assert_frame_equal(everything, pl.concat([want, pt.read_tparquet(deep)]))
    # ? and [] are glob characters too
    assert pt.scan_tparquet(str(tmp_path / "d/2024/?.parquet")).collect().height == 20
    assert pt.scan_tparquet(str(tmp_path / "d/2024/[b].parquet")).collect().height == 10


def test_a_directory_reads_every_parquet_file_below_it(write: Write, tmp_path: pathlib.Path) -> None:
    a = write("d/x/a.parquet", ticks(10))
    b = write("d/b.parquet", ticks(10, offset=10))
    (tmp_path / "d" / "README.md").write_text("not data")
    want = pl.concat([pt.read_tparquet(b), pt.read_tparquet(a)])  # d/b.parquet sorts before d/x/a.parquet
    assert_frame_equal(pt.scan_tparquet(tmp_path / "d").collect(), want)
    assert_frame_equal(pt.scan_tparquet(str(tmp_path / "d")).collect(), want)


def test_nothing_to_read(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"no file matches .*\*\.parquet"):
        pt.scan_tparquet(str(tmp_path / "*.parquet"))
    (tmp_path / "empty").mkdir()
    with pytest.raises(FileNotFoundError, match=r"no \.parquet file in .*empty"):
        pt.scan_tparquet(tmp_path / "empty")
    with pytest.raises(ValueError, match=r"^no files to scan$"):
        pt.scan_tparquet([])
    with pytest.raises(FileNotFoundError):
        pt.scan_tparquet(tmp_path / "missing.parquet")


def test_files_must_share_the_schema(write: Write) -> None:
    a = write("a.parquet", ticks(10))
    b = write("b.parquet", ticks(10).rename({"qty": "size"}))
    with pytest.raises(pl.exceptions.SchemaError, match=r"b\.parquet.*schema differs from .*a\.parquet"):
        pt.scan_tparquet([a, b])
    c = write("c.parquet", ticks(10).with_columns(pl.col("qty").cast(pl.Int64)))
    with pytest.raises(pl.exceptions.SchemaError, match=r"c\.parquet"):
        pt.scan_tparquet([a, c])


def test_file_objects_are_named_in_schema_errors(write: Write) -> None:
    a = write("a.parquet", ticks(10)).read_bytes()
    b = write("b.parquet", ticks(10).rename({"qty": "size"})).read_bytes()
    obj = r"<_io\.BytesIO object at 0x[0-9a-f]+>"
    with pytest.raises(pl.exceptions.SchemaError, match=rf"^{obj}: schema differs from {obj}: "):
        pt.scan_tparquet([io.BytesIO(a), io.BytesIO(b)])


def test_include_file_paths(write: Write) -> None:
    a, b = write("a.parquet", ticks(3)), write("b.parquet", ticks(2, offset=3))
    out = pt.scan_tparquet([a, b], include_file_paths="file").collect()
    assert out.columns == ["ts", "price", "qty", "sym", "file"]
    assert out["file"].dtype == pl.String
    assert out["file"].to_list() == [str(a)] * 3 + [str(b)] * 2
    # the path column can be selected and filtered on by itself
    only = pt.scan_tparquet([a, b], include_file_paths="file").filter(pl.col("file") == str(b)).select("file")
    assert only.collect()["file"].to_list() == [str(b)] * 2
    with pytest.raises(pl.exceptions.DuplicateError, match=rf"^{re.escape(str(a))} already has a column 'sym'$"):
        pt.scan_tparquet(a, include_file_paths="sym")


def test_projection_reaches_every_file(write: Write, reads: Reads) -> None:
    paths = [write("a.parquet", ticks(50)), write("b.parquet", ticks(50, offset=50))]
    out = pt.scan_tparquet(paths).select("price").collect()
    assert out.columns == ["price"]
    assert reads.columns() == [["price"], ["price"]]


def test_predicate_skips_row_groups_in_every_file(write: Write, reads: Reads) -> None:
    a = write("a.parquet", ticks(1000), row_group_size=100)
    b = write("b.parquet", ticks(1000, offset=1000), row_group_size=100)
    cut = 1_700_000_000_000 + 1950 * 250  # in the last row group of b
    out = pt.scan_tparquet([a, b]).filter(pl.col("ts") >= cut).collect()
    assert out.height == 50
    assert reads.row_groups() == [[], [9]]


def test_head_takes_rows_across_files(write: Write) -> None:
    a, b = write("a.parquet", ticks(3)), write("b.parquet", ticks(10, offset=3))
    assert_frame_equal(pt.scan_tparquet([a, b]).head(5).collect(), ticks(5))


def test_the_streaming_engine_gives_the_same_rows(write: Write) -> None:
    paths = [write(f"{i}.parquet", ticks(1000, offset=1000 * i), row_group_size=128) for i in range(3)]
    lf = pt.scan_tparquet(paths).filter(pl.col("qty") > 10).select("ts", "qty")
    assert_frame_equal(lf.collect(engine="streaming"), lf.collect(engine="in-memory"))


def test_batch_size_reaches_the_reader(write: Write, reads: Reads) -> None:
    path = write("a.parquet", ticks(100))
    pt.scan_tparquet(path, batch_size=7).collect()
    assert [c["batch_size"] for c in reads.calls] == [7]
    with pytest.raises(ValueError, match="batch_size must be positive"):
        pt.scan_tparquet(path, batch_size=0)


def test_nothing_is_read_before_collect(write: Write, reads: Reads) -> None:
    lf = pt.scan_tparquet([write("a.parquet", ticks(10)), write("b.parquet", ticks(10))])
    assert lf.collect_schema() == ticks(1).schema
    assert reads.calls == []


def test_file_objects(write: Write) -> None:
    df = ticks(20)
    data = write("a.parquet", df).read_bytes()
    assert_frame_equal(pt.scan_tparquet(io.BytesIO(data)).collect(), df)
    both = pt.scan_tparquet([io.BytesIO(data), io.BytesIO(data)]).collect()
    assert_frame_equal(both, pl.concat([df, df]))
    with pytest.raises(ValueError, match=r"^include_file_paths needs file paths, not file objects$"):
        pt.scan_tparquet(io.BytesIO(data), include_file_paths="file")


def test_a_standard_parquet_file_is_read_too(tmp_path: pathlib.Path) -> None:
    df = ticks(30)
    df.write_parquet(tmp_path / "plain.parquet")  # Polars' own writer: no PCODEC
    assert_frame_equal(pt.scan_tparquet(tmp_path / "plain.parquet").collect(), df)
