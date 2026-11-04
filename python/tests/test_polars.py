"""parquet_pcodec.polars: scan_parquet (an IO plugin with projection, n_rows, batch_size,
predicate and row-group pruning), read_parquet and write_parquet."""

from __future__ import annotations

import datetime
import io
import pathlib
from typing import Any

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import parquet_pcodec as ppc
import parquet_pcodec.polars as ppl

N = 1000


def frame() -> pl.DataFrame:
    start = datetime.datetime(2025, 1, 2, 14, 30)
    return pl.DataFrame(
        {
            "ts": pl.datetime_range(start, start + datetime.timedelta(milliseconds=250 * (N - 1)), "250ms", eager=True),
            "price": [round(100 + (i % 41) * 0.01, 2) for i in range(N)],
            "size": [(i % 13 + 1) * 100 for i in range(N)],
            "seq": list(range(N)),
            "day": [datetime.date(2025, 1, 1) + datetime.timedelta(days=i // 100) for i in range(N)],
            "sym": pl.Series([f"S{i % 3}" for i in range(N)], dtype=pl.Categorical),
            "book": [[100.0 + j * 0.01 for j in range(i % 4)] for i in range(N)],
            "maybe": [None if i % 4 == 0 else i for i in range(N)],
        }
    ).with_columns(pl.col("ts").dt.replace_time_zone("UTC"))


@pytest.fixture
def path(tmp_path: pathlib.Path) -> pathlib.Path:
    p = tmp_path / "f.parquet"
    ppl.write_parquet(frame(), p, row_group_size=100)
    return p


class Spy:
    """Records the reads scan_parquet makes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls: list[dict[str, Any]] = []
        original = ppc.ParquetFile.stream

        def stream(f: ppc.ParquetFile, *args: Any, **kwargs: Any) -> Any:
            names = ["columns", "row_groups", "batch_size", "limit"]
            call = dict(zip(names, args, strict=False)) | kwargs
            self.calls.append(call)
            return original(f, *args, **kwargs)

        monkeypatch.setattr(ppc.ParquetFile, "stream", stream)


def test_write_returns_codecs_and_round_trips(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "w.parquet"
    choice = ppl.write_parquet(frame(), p, policy="pcodec", level=3)
    assert choice["price"] == "PCODEC"
    assert choice["ts"] == "PCODEC"
    assert choice["book.list.item"] == "PCODEC"
    assert choice["sym"] == "ZSTD"
    assert_frame_equal(ppl.read_parquet(p), frame())


def test_write_lazy_frames(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "lazy.parquet"
    ppl.write_parquet(frame().lazy().filter(pl.col("seq") < 10), p)
    assert_frame_equal(ppl.read_parquet(p), frame().head(10))


def test_write_rejects_other_data(tmp_path: pathlib.Path) -> None:
    with pytest.raises(TypeError, match="DataFrame"):
        ppl.write_parquet({"a": [1]}, tmp_path / "x.parquet")  # type: ignore[arg-type]


def test_read_columns(path: pathlib.Path) -> None:
    assert_frame_equal(ppl.read_parquet(path, columns=["size", "ts"]), frame().select("size", "ts"))


def test_read_and_scan_file_objects(path: pathlib.Path) -> None:
    data = path.read_bytes()
    assert_frame_equal(ppl.read_parquet(io.BytesIO(data)), frame())
    assert_frame_equal(ppl.scan_parquet(io.BytesIO(data)).collect(), frame())


def test_scan_schema_without_reading(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lf = ppl.scan_parquet(path)
    spy = Spy(monkeypatch)
    assert lf.collect_schema() == frame().schema
    assert spy.calls == []


def test_scan_everything(path: pathlib.Path) -> None:
    assert_frame_equal(ppl.scan_parquet(path).collect(), frame())


def test_projection_is_pushed_down(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lf = ppl.scan_parquet(path)
    spy = Spy(monkeypatch)
    out = lf.select("price", "seq").collect()
    assert_frame_equal(out, frame().select("price", "seq"))
    assert len(spy.calls) == 1
    assert sorted(spy.calls[0]["columns"]) == ["price", "seq"]


def test_n_rows_is_pushed_down(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lf = ppl.scan_parquet(path)
    spy = Spy(monkeypatch)
    out = lf.head(7).collect()
    assert_frame_equal(out, frame().head(7))
    assert spy.calls[0]["limit"] == 7
    assert_frame_equal(ppl.scan_parquet(path).head(0).collect(), frame().head(0))


def test_predicate_is_applied(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lf = ppl.scan_parquet(path)
    spy = Spy(monkeypatch)
    lf = lf.filter((pl.col("price") > 100.3) & (pl.col("sym") == "S1")).select("seq")
    want = frame().filter((pl.col("price") > 100.3) & (pl.col("sym") == "S1")).select("seq")
    assert_frame_equal(lf.collect(), want)
    # the predicate's columns are read with the projected ones
    assert sorted(spy.calls[0]["columns"]) == ["price", "seq", "sym"]


def test_predicate_with_n_rows(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lf = ppl.scan_parquet(path)
    spy = Spy(monkeypatch)
    lf = lf.filter(pl.col("seq") % 10 == 3).head(5)
    assert_frame_equal(lf.collect(), frame().filter(pl.col("seq") % 10 == 3).head(5))
    # the limit is on matching rows, so it cannot go to the reader
    assert spy.calls[0]["limit"] is None


def test_batch_size(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lf = ppl.scan_parquet(path, batch_size=123)
    spy = Spy(monkeypatch)
    assert_frame_equal(lf.collect(), frame())
    # the batch size asked for wins over the engine's hint
    assert spy.calls[0]["batch_size"] == 123
    batches = list(ppl.scan_parquet(path, batch_size=123).collect_batches())
    assert_frame_equal(pl.concat(batches), frame())


def test_batch_size_hint_and_default(path: pathlib.Path) -> None:
    f = ppc.ParquetFile(path)
    schema = pl.DataFrame(f.empty()).schema
    source = ppl._source(f, None, schema)
    assert [b.height for b in source(None, None, None, 300)][:2] == [300, 300]
    # batches run across row groups
    assert [b.height for b in source(None, None, None, None)] == [1000]
    small = ppl._source(f, None, schema)
    assert [b.height for b in small(None, None, None, 999)] == [999, 1]
    assert ppl.DEFAULT_BATCH_SIZE == 65536
    capped = ppl._source(f, 250, schema)
    assert [b.height for b in capped(None, None, None, 300)][:2] == [250, 250]


def test_bad_batch_size(path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        ppl.scan_parquet(path, batch_size=0)


@pytest.mark.parametrize(
    ("predicate", "kept"),
    [
        (pl.col("seq") < 150, [0, 1]),
        (pl.col("seq") <= 199, [0, 1]),
        (pl.col("seq") <= 200, [0, 1, 2]),
        (pl.col("seq") > 899, [9]),
        (pl.col("seq") >= 899, [8, 9]),
        (pl.col("seq") == 450, [4]),
        (pl.lit(450) == pl.col("seq"), [4]),
        (pl.lit(850) < pl.col("seq"), [8, 9]),
        (pl.lit(150) >= pl.col("seq"), [0, 1]),
        (pl.lit(150) > pl.col("seq"), [0, 1]),
        (pl.lit(850) <= pl.col("seq"), [8, 9]),
        (pl.col("seq").is_between(250, 349), [2, 3]),
        (pl.col("seq").is_between(250, 300, closed="left"), [2]),
        (pl.col("seq").is_between(199, 300, closed="right"), [2, 3]),
        (pl.col("seq").is_between(199, 300, closed="none"), [2]),
        ((pl.col("seq") > 150) & (pl.col("seq") < 350), [1, 2, 3]),
        ((pl.col("seq") < 50) | (pl.col("seq") > 950), [0, 9]),
        (pl.col("seq") != 5, list(range(10))),
        (pl.col("seq") % 2 == 0, list(range(10))),
        ((pl.col("seq") < 50) | (pl.col("seq") % 2 == 0), list(range(10))),
        (pl.col("seq") == -1, []),
        (pl.col("day") == datetime.date(2025, 1, 4), [3]),
        (pl.col("day") >= datetime.date(2025, 1, 10), [9]),
        (pl.col("ts") < datetime.datetime(2025, 1, 2, 14, 30, 30, tzinfo=datetime.timezone.utc), [0, 1]),
        (pl.col("ts") >= datetime.datetime(2025, 1, 2, 14, 34, tzinfo=datetime.timezone.utc), [9]),
        # a float literal is not used
        (pl.col("seq") < 150.5, list(range(10))),
        # float columns are never pruned: Parquet min/max leave NaN out
        (pl.col("price") > 1000.0, list(range(10))),
        # columns with nulls: min/max still bound the non-null values
        (pl.col("maybe") < 100, [0]),
    ],
    ids=str,
)
def test_row_groups_are_pruned(
    path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, predicate: pl.Expr, kept: list[int]
) -> None:
    lf = ppl.scan_parquet(path)
    spy = Spy(monkeypatch)
    out = lf.filter(predicate).collect()
    assert_frame_equal(out, frame().filter(predicate))
    assert spy.calls[0]["row_groups"] == kept


def test_literal_in_another_unit(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Polars >= 2.1 folds the cast into a literal in the column's unit (then it prunes);
    # older Polars passes the cast (then it does not)
    lit = pl.lit(datetime.datetime(2025, 1, 2, 14, 30, 30, tzinfo=datetime.timezone.utc)).dt.cast_time_unit("ns")
    lf = ppl.scan_parquet(path)
    spy = Spy(monkeypatch)
    out = lf.filter(pl.col("ts") < lit).collect()
    assert_frame_equal(out, frame().filter(pl.col("ts") < lit))
    assert spy.calls[0]["row_groups"] in ([0, 1], list(range(10)))


def test_pruning_off_without_statistics(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = tmp_path / "nostats.parquet"
    pq.write_table(pa.table({"seq": list(range(300))}), p, row_group_size=100, write_statistics=False)
    lf = ppl.scan_parquet(p)
    spy = Spy(monkeypatch)
    out = lf.filter(pl.col("seq") == 5).collect()
    assert out["seq"].to_list() == [5]
    assert spy.calls[0]["row_groups"] == [0, 1, 2]


def test_unsigned_columns_are_not_pruned(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "u.parquet"
    big = 2**63 + 5
    ppl.write_parquet(pl.DataFrame({"u": pl.Series([1, big], dtype=pl.UInt64)}), p, row_group_size=1)
    lf = ppl.scan_parquet(p)
    spy = Spy(monkeypatch)
    assert lf.filter(pl.col("u") > 10).collect()["u"].to_list() == [big]
    assert spy.calls[0]["row_groups"] == [0, 1]


SCHEMA = pl.Schema(
    {
        "i": pl.Int64(),
        "d": pl.Date(),
        "us": pl.Datetime("us"),
        "utc": pl.Datetime("ms", "UTC"),
        "f": pl.Float64(),
        "u": pl.UInt32(),
    }
)


def col(name: str) -> dict[str, Any]:
    return {"Column": name}


def dyn(v: object, kind: str = "Int") -> dict[str, Any]:
    return {"Literal": {"Dyn": {kind: v}}}


def scalar(kind: str, v: object) -> dict[str, Any]:
    return {"Literal": {"Scalar": {kind: v}}}


def binary(left: object, op: str, right: object) -> dict[str, Any]:
    return {"BinaryExpr": {"left": left, "op": op, "right": right}}


@pytest.mark.parametrize(
    ("node", "dtype", "value"),
    [
        (dyn(5), pl.Int64(), 5),
        (dyn(5), pl.Date(), None),
        (dyn(5.0, "Float"), pl.Int64(), None),
        (dyn("5"), pl.Int64(), None),
        (scalar("Int32", 7), pl.Int64(), 7),
        (scalar("Int64", 7), pl.Int16(), 7),
        (scalar("UInt64", 7), pl.Int64(), None),
        (scalar("Int8", "7"), pl.Int64(), None),
        (scalar("Int8", 7), pl.Date(), None),
        (scalar("Date", 3), pl.Date(), 3),
        (scalar("Date", 3), pl.Int64(), None),
        (scalar("Date", "3"), pl.Date(), None),
        (scalar("Datetime", [9, "Microseconds", None]), pl.Datetime("us"), 9),
        (scalar("Datetime", [9, "Milliseconds", None]), pl.Datetime("us"), None),
        (scalar("Datetime", [9, "Nanoseconds", None]), pl.Datetime("ns"), 9),
        (scalar("Datetime", [9, "Milliseconds", "UTC"]), pl.Datetime("ms", "UTC"), 9),
        (scalar("Datetime", [9, "Milliseconds", "UTC"]), pl.Datetime("ms"), None),
        (scalar("Datetime", [9, "Milliseconds", None]), pl.Datetime("ms", "UTC"), None),
        (scalar("Datetime", [9, "Milliseconds", None]), pl.Date(), None),
        (scalar("Datetime", 9), pl.Datetime("ms"), None),
        ({"Literal": {"Scalar": {"Int8": 1, "Int16": 2}}}, pl.Int64(), None),
        ({"Literal": {"Scalar": 5}}, pl.Int64(), None),
        ({"Literal": 5}, pl.Int64(), None),
        ({"Column": "x"}, pl.Int64(), None),
        ("Literal", pl.Int64(), None),
    ],
    ids=str,
)
def test_literals(node: object, dtype: pl.DataType, value: int | None) -> None:
    assert ppl._literal(node, dtype) == value


BOUNDS = {"i": (10, 20), "d": (100, 100)}


@pytest.mark.parametrize(
    ("node", "result"),
    [
        (binary(col("i"), "Eq", dyn(10)), True),
        (binary(col("i"), "Eq", dyn(20)), True),
        (binary(col("i"), "Eq", dyn(9)), False),
        (binary(col("i"), "Eq", dyn(21)), False),
        (binary(col("i"), "Lt", dyn(10)), False),
        (binary(col("i"), "Lt", dyn(11)), True),
        (binary(col("i"), "LtEq", dyn(10)), True),
        (binary(col("i"), "LtEq", dyn(9)), False),
        (binary(col("i"), "Gt", dyn(20)), False),
        (binary(col("i"), "Gt", dyn(19)), True),
        (binary(col("i"), "GtEq", dyn(20)), True),
        (binary(col("i"), "GtEq", dyn(21)), False),
        (binary(dyn(9), "Lt", col("i")), True),
        (binary(dyn(20), "Lt", col("i")), False),
        (binary(dyn(10), "Gt", col("i")), False),
        (binary(dyn(9), "GtEq", col("i")), False),
        (binary(dyn(20), "LtEq", col("i")), True),
        (binary(dyn(21), "LtEq", col("i")), False),
        (binary(binary(col("i"), "Gt", dyn(5)), "And", binary(col("i"), "Lt", dyn(8))), False),
        (binary(binary(col("i"), "Gt", dyn(25)), "And", binary(col("i"), "Lt", dyn(30))), False),
        (binary(binary(col("i"), "Gt", dyn(5)), "And", binary(col("i"), "Lt", dyn(30))), True),
        (binary(binary(col("i"), "Gt", dyn(5)), "LogicalAnd", binary(col("i"), "Lt", dyn(8))), False),
        (binary(binary(col("i"), "Lt", dyn(5)), "Or", binary(col("i"), "Gt", dyn(30))), False),
        (binary(binary(col("i"), "Lt", dyn(15)), "Or", binary(col("i"), "Gt", dyn(30))), True),
        (binary(binary(col("i"), "Lt", dyn(5)), "Or", binary(col("i"), "Gt", dyn(15))), True),
        (binary(binary(col("i"), "Lt", dyn(5)), "LogicalOr", binary(col("i"), "Gt", dyn(30))), False),
        (binary(col("d"), "Eq", scalar("Date", 100)), True),
        (binary(col("d"), "Gt", scalar("Date", 100)), False),
        # a column without bounds is kept
        (binary(col("us"), "Lt", scalar("Datetime", [0, "Microseconds", None])), True),
        (binary(col("utc"), "Lt", scalar("Datetime", [0, "Milliseconds", "UTC"])), True),
    ],
    ids=str,
)
def test_bounds_tests(node: object, result: bool) -> None:
    test = ppl._test(node, SCHEMA)
    assert test is not None
    assert test(BOUNDS) is result


def between(name: str, lo: object, hi: object, closed: str) -> dict[str, Any]:
    return {
        "Function": {
            "input": [col(name), lo, hi],
            "function": {"Boolean": {"IsBetween": {"closed": closed}}},
        }
    }


@pytest.mark.parametrize(
    ("closed", "lo", "hi", "result"),
    [
        ("Both", 20, 30, True),
        ("Both", 0, 10, True),
        ("Both", 21, 30, False),
        ("Left", 20, 30, True),
        ("Left", 0, 10, False),
        ("Right", 20, 30, False),
        ("Right", 0, 10, True),
        ("None", 20, 30, False),
        ("None", 0, 10, False),
        ("None", 0, 11, True),
    ],
)
def test_between(closed: str, lo: int, hi: int, result: bool) -> None:
    test = ppl._test(between("i", dyn(lo), dyn(hi), closed), SCHEMA)
    assert test is not None
    assert test(BOUNDS) is result


@pytest.mark.parametrize(
    "node",
    [
        None,
        "x",
        {},
        binary(col("i"), "NotEq", dyn(1)),
        binary(col("i"), "Plus", dyn(1)),
        binary(col("f"), "Lt", dyn(1)),
        binary(col("u"), "Lt", dyn(1)),
        binary(col("missing"), "Lt", dyn(1)),
        binary(col("i"), "Lt", col("i")),
        binary(dyn(1), "Lt", dyn(2)),
        binary(col("i"), "Lt", dyn(1.5, "Float")),
        binary(dyn(1.5, "Float"), "Lt", col("i")),
        binary(binary(col("i"), "Lt", dyn(5)), "Or", binary(col("f"), "Gt", dyn(30))),
        binary(binary(col("f"), "Lt", dyn(5)), "Or", binary(col("i"), "Gt", dyn(30))),
        binary(binary(col("f"), "Lt", dyn(5)), "And", binary(col("f"), "Gt", dyn(30))),
        {"BinaryExpr": "x"},
        between("f", dyn(1), dyn(2), "Both"),
        between("i", dyn(1), dyn(2.5, "Float"), "Both"),
        between("i", dyn(1.5, "Float"), dyn(2), "Both"),
        {"Function": {"input": [col("i"), dyn(1)], "function": {"Boolean": {"IsBetween": {"closed": "Both"}}}}},
        {"Function": {"input": "x", "function": {"Boolean": {"IsBetween": {"closed": "Both"}}}}},
        {"Function": {"input": [col("i"), dyn(1), dyn(2)], "function": {"Boolean": {"IsIn": {}}}}},
        {"Function": {"input": [col("i"), dyn(1), dyn(2)], "function": {"Boolean": "IsBetween"}}},
        {"Function": {"input": [col("i"), dyn(1), dyn(2)], "function": "Boolean"}},
        {"Function": "x"},
    ],
    ids=str,
)
def test_not_understood(node: object) -> None:
    assert ppl._test(node, SCHEMA) is None


def test_and_with_one_side_understood() -> None:
    known = binary(col("i"), "Gt", dyn(30))
    unknown = binary(col("f"), "Gt", dyn(30))
    for node in [binary(known, "And", unknown), binary(unknown, "And", known)]:
        test = ppl._test(node, SCHEMA)
        assert test is not None
        assert test(BOUNDS) is False
        assert test({"i": (0, 40)}) is True


def test_unserializable_predicates_prune_nothing(path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class Opaque:
        class meta:
            @staticmethod
            def serialize(format: str) -> str:
                raise ValueError("no")

    assert ppl._row_groups(ppc.ParquetFile(path), Opaque(), frame().schema) == list(range(10))  # type: ignore[arg-type]
