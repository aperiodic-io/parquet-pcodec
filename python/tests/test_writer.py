"""The Writer's life (close, abort, use after either) and what a failed write leaves behind."""

from __future__ import annotations

import io
import pathlib
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import tparquet as ppc

TABLE = pa.table({"a": pa.array(range(100), pa.int64()), "b": pa.array([i / 4 for i in range(100)])})


def union_table() -> pa.Table:
    """A table whose union column no Parquet writer takes."""
    u = pa.UnionArray.from_sparse(pa.array([0, 1], pa.int8()), [pa.array([1, 2]), pa.array(["a", "b"])])
    return pa.table({"a": pa.array([1, 2], pa.int64()), "u": u})


# ---------------------------------------------------------------- close and abort


def test_abort_after_close_keeps_the_file(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "w.parquet"
    w = ppc.Writer(path, TABLE.schema)
    w.write(TABLE)
    codecs = w.close()
    w.abort()
    assert ppc.read_table(path).equals(TABLE)
    assert w.closed
    assert w.codecs == codecs


def test_close_then_an_exception_keeps_the_file(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "w.parquet"

    def body() -> None:
        with ppc.Writer(path, TABLE.schema) as w:
            w.write(TABLE)
            w.close()
            raise KeyError("after close")

    with pytest.raises(KeyError):
        body()
    assert ppc.read_table(path).equals(TABLE)


def test_abort_twice(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "w.parquet"
    w = ppc.Writer(path, TABLE.schema)
    w.write(TABLE)
    w.abort()
    assert not path.exists()
    w.abort()
    assert w.closed
    assert w.codecs is None


@pytest.mark.parametrize("end", ["close", "abort"])
def test_a_finished_writer_refuses_everything_but_abort(tmp_path: pathlib.Path, end: str) -> None:
    w = ppc.Writer(tmp_path / "w.parquet", TABLE.schema)
    w.write(TABLE)
    getattr(w, end)()
    for call in [lambda: w.write(TABLE), w.flush, w.close, lambda: w.write(TABLE.to_batches()[0])]:
        with pytest.raises(ppc.PcodecError, match=r"^the writer is closed$"):
            call()
    w.abort()


def test_abort_of_a_file_object_writer_leaves_the_object() -> None:
    buf = io.BytesIO()
    w = ppc.Writer(buf, TABLE.schema, policy="pcodec")
    w.write(TABLE)
    w.abort()
    w.abort()
    assert not buf.closed


def test_abort_of_a_file_removed_meanwhile(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "w.parquet"
    w = ppc.Writer(path, TABLE.schema)
    path.unlink()
    w.abort()
    assert not path.exists()


# ---------------------------------------------------------------- what a failure leaves


def failing_stream(after: int) -> pa.RecordBatchReader:
    def batches() -> Iterator[pa.RecordBatch]:
        yield from TABLE.to_batches(max_chunksize=10)[:after]
        raise RuntimeError("the source broke")

    return pa.RecordBatchReader.from_batches(TABLE.schema, batches())


@pytest.mark.parametrize("after", [0, 1, 3])
def test_a_stream_that_fails_leaves_no_file(tmp_path: pathlib.Path, after: int) -> None:
    path = tmp_path / "w.parquet"
    with pytest.raises(ppc.PcodecError, match="the source broke"):
        ppc.write_table(failing_stream(after), path)
    assert list(tmp_path.iterdir()) == []


def test_a_stream_that_fails_in_a_writer_leaves_it_usable(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "w.parquet"
    with ppc.Writer(path, TABLE.schema) as w:
        with pytest.raises(ppc.PcodecError, match="the source broke"):
            w.write(failing_stream(0))
        w.write(TABLE)
    assert ppc.read_table(path).equals(TABLE)


def existing(tmp_path: pathlib.Path) -> tuple[pathlib.Path, bytes]:
    path = tmp_path / "old.parquet"
    pq.write_table(TABLE, path)
    return path, path.read_bytes()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"columns": {"zz": "pcodec"}}, 'no column named "zz"'),
        ({"columns": {"a.b": "pcodec"}}, 'no column named "a.b"'),
        ({"policy": "nope"}, "unknown policy"),
        ({"level": 13}, "level 13"),
        ({"base": "nope"}, "base codec"),
        ({"row_group_size": -1}, "row_group_size"),
    ],
)
def test_refused_options_leave_an_existing_file_alone(
    tmp_path: pathlib.Path, kwargs: dict[str, Any], message: str
) -> None:
    path, before = existing(tmp_path)
    with pytest.raises(ppc.PcodecError, match=message):
        ppc.write_table(TABLE, path, **kwargs)
    with pytest.raises(ppc.PcodecError, match=message):
        ppc.write_table(TABLE.to_batches()[0], path, **kwargs)
    with pytest.raises(ppc.PcodecError, match=message):
        ppc.Writer(path, TABLE.schema, **kwargs)
    if "row_group_size" not in kwargs:  # transcode keeps the row groups
        source = tmp_path / "source.parquet"
        pq.write_table(TABLE, source)
        with pytest.raises(ppc.PcodecError, match=message):
            ppc.transcode(source, path, **kwargs)
    assert path.read_bytes() == before


def test_a_schema_no_writer_takes_leaves_an_existing_file_alone(tmp_path: pathlib.Path) -> None:
    path, before = existing(tmp_path)
    table = union_table()
    with pytest.raises(ppc.PcodecError):
        ppc.write_table(table, path)
    with pytest.raises(ppc.PcodecError):
        ppc.write_table(table.to_batches()[0], path)
    with pytest.raises(ppc.PcodecError):
        ppc.Writer(path, table.schema)
    assert path.read_bytes() == before


def test_a_transcode_that_fails_leaves_no_file(tmp_path: pathlib.Path) -> None:
    good = tmp_path / "good.parquet"
    ppc.write_table(pa.table({"a": pa.array(range(100_000), pa.int64())}), good)
    data = bytearray(good.read_bytes())
    for i in range(100, 400):  # in the first page, past its header
        data[i] ^= 0x5A
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(bytes(data))
    out = tmp_path / "out.parquet"
    for target in ["pcodec", "standard"]:
        with pytest.raises(ppc.PcodecError):
            ppc.transcode(bad, out, target=target)  # type: ignore[arg-type]
        assert not out.exists()
