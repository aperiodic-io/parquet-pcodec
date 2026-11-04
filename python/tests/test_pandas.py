"""parquet_pcodec.pandas: DataFrames round-trip with their index and dtypes, as with pyarrow."""

from __future__ import annotations

import io
import pathlib

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import parquet_pcodec as ppc
import parquet_pcodec.pandas as ppd


def frame() -> pd.DataFrame:
    n = 500
    idx = pd.date_range("2025-01-02 09:30", periods=n, freq="250ms", tz="America/New_York", name="ts")
    df: pd.DataFrame = pd.DataFrame(
        {
            "price": np.round(100 + np.cumsum(np.where(np.arange(n) % 3 == 0, 0.01, -0.005)), 2),
            "size": (np.arange(n) % 17 + 1) * 100,
            "side": pd.Categorical(["buy", "sell"] * (n // 2), categories=["buy", "sell"]),
            "venue": pd.Categorical(["X", "Y", "Z", "X", "Y"] * (n // 5), categories=["Z", "Y", "X"], ordered=True),
            "f32": np.arange(n, dtype=np.float32) / 4,
            "i8": (np.arange(n) % 100).astype(np.int8),
            "u64": np.arange(n, dtype=np.uint64) * 2**40,
            "flag": np.arange(n) % 2 == 0,
            "name": [f"n{i % 7}" for i in range(n)],
            "maybe": pd.array([None if i % 5 == 0 else i for i in range(n)], dtype="Int64"),
            "utc": pd.date_range("2025-01-01", periods=n, freq="s", tz="UTC"),
            "naive": pd.date_range("2025-01-01", periods=n, freq="min"),
            "delta": pd.to_timedelta(np.arange(n), unit="ms"),
        },
        index=idx,
    )
    return df


def assert_same_as_pyarrow(df: pd.DataFrame, tmp_path: pathlib.Path, **kwargs: object) -> pd.DataFrame:
    """Writes `df` with this package and with pyarrow; both read back equal."""
    ours = tmp_path / "ours.parquet"
    theirs = tmp_path / "theirs.parquet"
    index = kwargs.get("index")
    assert index is None or isinstance(index, bool)
    ppd.to_parquet(df, ours, **kwargs)  # type: ignore[arg-type]
    df.to_parquet(theirs, engine="pyarrow", index=index)
    back = ppd.read_parquet(ours)
    pd.testing.assert_frame_equal(back, pd.read_parquet(theirs, engine="pyarrow"))
    return back


def test_round_trip_keeps_index_and_dtypes(tmp_path: pathlib.Path) -> None:
    df = frame()
    back = assert_same_as_pyarrow(df, tmp_path)
    # no engine stores an index's freq
    pd.testing.assert_frame_equal(back, df, check_freq=False)
    assert isinstance(back.index, pd.DatetimeIndex)
    assert str(back.index.tz) == "America/New_York"
    assert back["venue"].cat.ordered
    assert list(back["venue"].cat.categories) == ["Z", "Y", "X"]
    assert back["maybe"].dtype == "Int64"
    assert back["f32"].dtype == np.float32


def test_categoricals_keep_order_and_unused_categories(tmp_path: pathlib.Path) -> None:
    # the Parquet writer re-encodes dictionaries in order of appearance; the categories are
    # restored on read, as pyarrow's own files keep them
    df = pd.DataFrame(
        {
            "level": pd.Categorical(["mid", "low", None, "mid"], categories=["high", "mid", "low"], ordered=True),
            "code": pd.Categorical([3, 1, 3, 2], categories=[3, 2, 1, 0]),
            "when": pd.Categorical(pd.to_datetime(["2025-01-02", "2025-01-01"] * 2)),
        }
    )
    # pyarrow reads the int and datetime categoricals back as plain columns: compare with df
    path = tmp_path / "c.parquet"
    ppd.to_parquet(df, path)
    back = ppd.read_parquet(path)
    pd.testing.assert_frame_equal(back, df)
    pd.testing.assert_frame_equal(back[["level"]], pd.read_parquet(_pyarrow_copy(df[["level"]], tmp_path)))
    assert list(back["level"].cat.categories) == ["high", "mid", "low"]
    assert list(back["code"].cat.categories) == [3, 2, 1, 0]
    # in several row groups too
    path = tmp_path / "rg.parquet"
    ppd.to_parquet(df, path, row_group_size=1)
    pd.testing.assert_frame_equal(ppd.read_parquet(path), df)
    pd.testing.assert_frame_equal(ppd.read_parquet(path, columns=["code"]), df[["code"]])


def test_files_without_categories_metadata(tmp_path: pathlib.Path) -> None:
    # a file written by pyarrow (no restored categories needed) and plain Arrow dictionaries
    df = pd.DataFrame({"c": pd.Categorical(["b", "a"], categories=["b", "a", "z"])})
    pd.testing.assert_frame_equal(ppd.read_parquet(_pyarrow_copy(df, tmp_path)), df)
    path = tmp_path / "plain.parquet"
    ppc.write_table(pa.table({"c": pa.array(["b", "a"]).dictionary_encode()}), path)
    assert list(ppd.read_parquet(path)["c"].cat.categories) == ["b", "a"]


def test_codecs_and_options(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "o.parquet"
    choice = ppd.to_parquet(frame(), path, policy="pcodec", row_group_size=200)
    assert choice["price"] == "PCODEC"
    assert choice["ts"] == "PCODEC"
    assert choice["name"] == "ZSTD"
    assert ppc.ParquetFile(path).metadata.row_group_rows == [200, 200, 100]


def test_index_false_and_range_index(tmp_path: pathlib.Path) -> None:
    df = frame().reset_index(drop=True)
    back = assert_same_as_pyarrow(df, tmp_path)
    assert isinstance(back.index, pd.RangeIndex)
    pd.testing.assert_frame_equal(back, df)
    dropped = assert_same_as_pyarrow(frame(), tmp_path, index=False)
    assert "ts" not in dropped.columns
    assert isinstance(dropped.index, pd.RangeIndex)
    stored = assert_same_as_pyarrow(df, tmp_path, index=True)
    assert "__index_level_0__" not in stored.columns
    pd.testing.assert_frame_equal(stored, df)


def test_multi_index(tmp_path: pathlib.Path) -> None:
    df = frame().reset_index().set_index(["name", "ts"])
    back = assert_same_as_pyarrow(df, tmp_path)
    pd.testing.assert_frame_equal(back, df, check_freq=False)


def test_column_selection_keeps_the_index(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "c.parquet"
    df = frame()
    ppd.to_parquet(df, path)
    back = ppd.read_parquet(path, columns=["size", "price"])
    pd.testing.assert_frame_equal(back, df[["size", "price"]], check_freq=False)
    pd.testing.assert_frame_equal(back, pd.read_parquet(_pyarrow_copy(df, tmp_path), columns=["size", "price"]))
    # an index column asked for explicitly is not read twice
    multi = df.reset_index().set_index(["name", "ts"])
    ppd.to_parquet(multi, path)
    pd.testing.assert_frame_equal(
        ppd.read_parquet(path, columns=["name", "price"]),
        pd.read_parquet(_pyarrow_copy(multi, tmp_path), columns=["name", "price"]),
    )


def _pyarrow_copy(df: pd.DataFrame, tmp_path: pathlib.Path) -> pathlib.Path:
    p = tmp_path / "pa.parquet"
    df.to_parquet(p, engine="pyarrow")
    return p


def test_to_pandas_kwargs(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "k.parquet"
    ppd.to_parquet(frame(), path)
    back = ppd.read_parquet(path, columns=["side"], categories=[], strings_to_categorical=True)
    assert back["side"].dtype == "category"
    arrow_backed = ppd.read_parquet(path, columns=["size"], types_mapper=pd.ArrowDtype)
    assert isinstance(arrow_backed["size"].dtype, pd.ArrowDtype)


def test_file_objects(tmp_path: pathlib.Path) -> None:
    buf = io.BytesIO()
    df = frame()
    ppd.to_parquet(df, buf)
    pd.testing.assert_frame_equal(ppd.read_parquet(io.BytesIO(buf.getvalue())), df, check_freq=False)


def test_pandas_metadata_is_what_pyarrow_writes(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "md.parquet"
    df = frame()
    ppd.to_parquet(df, path)
    ours = ppc.read_table(path).schema.pandas_metadata
    theirs = pq.read_table(_pyarrow_copy(df, tmp_path)).schema.pandas_metadata
    assert ours is not None
    assert ours["index_columns"] == theirs["index_columns"] == ["ts"]
    assert ours["columns"] == theirs["columns"]


def test_rejects_non_frames(tmp_path: pathlib.Path) -> None:
    with pytest.raises(TypeError, match="DataFrame"):
        ppd.to_parquet({"a": [1]}, tmp_path / "x.parquet")  # type: ignore[arg-type]
