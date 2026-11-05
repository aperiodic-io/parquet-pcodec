"""What scan_tparquet reads: the kinds of paths, globs, directories, file objects and lists."""

from __future__ import annotations

import io
import os
import pathlib
import threading
from collections.abc import Callable

import polars as pl
import pytest
import tparquet
from polars.testing import assert_frame_equal

import polars_tparquet as pt
from conftest import ticks

Write = Callable[..., pathlib.Path]


class FsPath:
    """An os.PathLike that is not a pathlib path."""

    def __init__(self, path: str) -> None:
        self._path = path

    def __fspath__(self) -> str:
        return self._path


def test_every_kind_of_path(write: Write) -> None:
    df = ticks(10)
    path = write("a.parquet", df)
    for source in (str(path), path, os.fsencode(path), FsPath(str(path))):
        assert_frame_equal(pt.scan_tparquet(source).collect(), df)
        out = pt.scan_tparquet(source, include_file_paths="file").collect()
        assert out["file"].to_list() == [str(path)] * 10


def test_a_home_relative_path(write: Write, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    write("data/a.parquet", ticks(10))
    write("data/b.parquet", ticks(5, offset=10))
    want = pl.concat([ticks(10), ticks(5, offset=10)])
    for source in ("~/data/a.parquet", pathlib.Path("~/data/a.parquet"), b"~/data/a.parquet"):
        assert_frame_equal(pt.scan_tparquet(source).collect(), ticks(10))
    assert_frame_equal(pt.scan_tparquet("~/data").collect(), want)
    assert_frame_equal(pt.scan_tparquet("~/data/*.parquet").collect(), want)
    out = pt.scan_tparquet(["~/data/b.parquet"], include_file_paths="file").collect()
    assert out["file"].to_list() == [str(tmp_path / "data" / "b.parquet")] * 5


def test_write_and_sink_to_a_home_relative_path(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    ticks(10).tparquet.write("~/w.parquet")  # type: ignore[attr-defined]
    ticks(10).lazy().tparquet.sink(pathlib.Path("~/s.parquet"))  # type: ignore[attr-defined]
    assert sorted(os.listdir(tmp_path)) == ["s.parquet", "w.parquet"]
    assert_frame_equal(pt.read_tparquet([tmp_path / "w.parquet", tmp_path / "s.parquet"]), pl.concat([ticks(10)] * 2))


def test_a_list_mixes_paths_globs_directories_and_file_objects(write: Write, tmp_path: pathlib.Path) -> None:
    parts = [ticks(2), ticks(3, offset=2), ticks(4, offset=5), ticks(5, offset=9), ticks(6, offset=14)]
    a = write("a.parquet", parts[0])
    write("g/b.parquet", parts[1])
    write("g/c.parquet", parts[2])
    write("d/e/f.parquet", parts[3])
    obj = io.BytesIO(write("obj.parquet", parts[4]).read_bytes())
    sources: list[tparquet.Source] = [a, str(tmp_path / "g" / "*.parquet"), tmp_path / "d", obj]
    assert_frame_equal(pt.scan_tparquet(sources).collect(), pl.concat(parts))


def test_a_file_named_twice_is_read_twice(write: Write, tmp_path: pathlib.Path) -> None:
    a = write("a.parquet", ticks(3))
    out = pt.scan_tparquet([a, str(a), str(tmp_path / "*.parquet")], include_file_paths="file").collect()
    assert_frame_equal(out.drop("file"), pl.concat([ticks(3)] * 3))
    assert out["file"].to_list() == [str(a)] * 9


def test_a_path_without_glob_characters_is_one_file(write: Write) -> None:
    path = write("a.parquet", ticks(3))
    assert_frame_equal(pt.scan_tparquet(str(path)).collect(), ticks(3))
    with pytest.raises(FileNotFoundError, match=r"No such file or directory.*b\.parquet"):
        pt.scan_tparquet(str(path.with_name("b.parquet")))


def test_a_file_object_is_read_when_scanned(write: Write) -> None:
    obj = io.BytesIO(write("a.parquet", ticks(10)).read_bytes())
    lf = pt.scan_tparquet(obj)
    obj.close()  # the bytes were taken when scanned
    assert_frame_equal(lf.collect(), ticks(10))
    assert_frame_equal(lf.collect(), ticks(10))
    assert_frame_equal(lf.filter(pl.col("qty") > 3).collect(), ticks(10).filter(pl.col("qty") > 3))


def test_a_file_object_is_read_from_where_it_stands(write: Write) -> None:
    data = write("a.parquet", ticks(10)).read_bytes()
    obj = io.BytesIO(b"header" + data)
    obj.seek(6)
    assert_frame_equal(pt.scan_tparquet(obj).collect(), ticks(10))
    written = io.BytesIO()
    ticks(10).tparquet.write(written)  # type: ignore[attr-defined]
    written.seek(0)
    assert_frame_equal(pt.scan_tparquet(written).collect(), ticks(10))


def test_a_file_object_at_its_end_has_nothing_to_read() -> None:
    written = io.BytesIO()
    ticks(10).tparquet.write(written)  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="Size is 0"):
        pt.scan_tparquet(written)


def test_a_non_seekable_stream(write: Write) -> None:
    data = write("a.parquet", ticks(1000)).read_bytes()
    r, w = os.pipe()

    def feed() -> None:
        with os.fdopen(w, "wb") as f:
            f.write(data)

    feeder = threading.Thread(target=feed)
    feeder.start()
    with os.fdopen(r, "rb") as pipe:
        assert not pipe.seekable()
        lf = pt.scan_tparquet(pipe)
    feeder.join()
    assert_frame_equal(lf.collect(), ticks(1000))


def test_an_object_with_only_read(write: Write) -> None:
    data = write("a.parquet", ticks(10)).read_bytes()

    class Reader:
        def read(self) -> bytes:
            return data

    assert_frame_equal(pt.scan_tparquet(Reader()).collect(), ticks(10))


def test_what_is_not_a_source(write: Write, tmp_path: pathlib.Path) -> None:
    path = write("a.parquet", ticks(3))
    for bad, name in ((3, "int"), (None, "NoneType"), ((p for p in [path]), "generator"), ([[path]], "list")):
        with pytest.raises(TypeError, match=rf"^expected a path or a binary file object, got {name}$"):
            pt.scan_tparquet(bad)  # type: ignore[arg-type]
    with open(path) as text, pytest.raises((TypeError, UnicodeDecodeError)):
        pt.scan_tparquet(text)  # type: ignore[arg-type]


def test_a_missing_file_in_a_list_is_named(write: Write, tmp_path: pathlib.Path) -> None:
    a = write("a.parquet", ticks(3))
    with pytest.raises(FileNotFoundError, match=r"missing\.parquet"):
        pt.scan_tparquet([a, tmp_path / "missing.parquet"])
