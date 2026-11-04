"""Polars DataFrames to and from PCODEC Parquet files (needs Polars; not pyarrow).

`scan_parquet` is a Polars IO plugin: the projection, the row limit, the batch size and the
predicate of a lazy query reach the reader, and the predicate also skips row groups whose
statistics rule it out.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import polars as pl
from polars.io.plugins import register_io_source

from parquet_pcodec._core import ParquetFile, Policy, Sink, Source, write_table

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

__all__ = ["read_parquet", "scan_parquet", "write_parquet"]

#: Polars dtypes whose values are the stored signed integers the statistics bound.
_SIGNED = (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Date, pl.Datetime)
_UNITS = {"Milliseconds": "ms", "Microseconds": "us", "Nanoseconds": "ns"}


def read_parquet(source: Source, columns: Sequence[str] | None = None) -> pl.DataFrame:
    """Reads a Parquet file (PCODEC or not) as a DataFrame; `columns` in the order given."""
    df: pl.DataFrame = pl.DataFrame(ParquetFile(source).stream(columns))
    return df


def write_parquet(
    df: pl.DataFrame | pl.LazyFrame,
    sink: Sink,
    *,
    policy: Policy = "auto",
    columns: Mapping[str, Policy] | None = None,
    level: int = 8,
    base: str = "zstd(3)",
    row_group_size: int | None = None,
) -> dict[str, str]:
    """Writes a DataFrame (or the result of a LazyFrame) with PCODEC where the policy selects
    it; the options are those of `parquet_pcodec.write_table`. Returns the codec of every
    leaf column."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"expected a Polars DataFrame or LazyFrame, got {type(df).__name__}")
    return write_table(df, sink, policy=policy, columns=columns, level=level, base=base, row_group_size=row_group_size)


def _get(node: Any, *keys: str) -> Any:
    """`node[k0][k1]...`, or `None` where a level is not a dict or lacks the key."""
    for key in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def _literal(node: Any, dtype: pl.DataType) -> int | None:
    """The stored integer of a literal compared with a column of `dtype`, or `None` when the
    literal is not one this pruner understands exactly."""
    lit = _get(node, "Literal")
    if not isinstance(lit, dict):
        return None
    dyn = lit.get("Dyn")
    if isinstance(dyn, dict) and isinstance(dyn.get("Int"), int) and dtype.is_integer():
        value: int = dyn["Int"]
        return value
    scalar = lit.get("Scalar")
    if not isinstance(scalar, dict) or len(scalar) != 1:
        return None
    ((kind, v),) = scalar.items()
    if kind == "Date" and isinstance(v, int) and dtype == pl.Date:
        return v
    if kind == "Datetime" and isinstance(v, list) and isinstance(dtype, pl.Datetime):
        value_, unit, tz = v
        if _UNITS.get(unit) == dtype.time_unit and (tz is None) == (dtype.time_zone is None):
            return int(value_)
    if kind in {"Int8", "Int16", "Int32", "Int64"} and isinstance(v, int) and dtype.is_integer():
        return v
    return None


_FLIP = {"Lt": "Gt", "LtEq": "GtEq", "Gt": "Lt", "GtEq": "LtEq", "Eq": "Eq"}


def _compare(name: str, op: str, v: int) -> Callable[[dict[str, tuple[int, int]]], bool]:
    def may(bounds: dict[str, tuple[int, int]]) -> bool:
        if name not in bounds:
            return True
        lo, hi = bounds[name]
        if op == "Eq":
            return lo <= v <= hi
        if op == "Lt":
            return lo < v
        if op == "LtEq":
            return lo <= v
        if op == "Gt":
            return hi > v
        return hi >= v  # GtEq

    return may


Bounds = dict[str, tuple[int, int]]


def _both(a: Callable[[Bounds], bool], b: Callable[[Bounds], bool]) -> Callable[[Bounds], bool]:
    return lambda bounds: a(bounds) and b(bounds)


def _either(a: Callable[[Bounds], bool], b: Callable[[Bounds], bool]) -> Callable[[Bounds], bool]:
    return lambda bounds: a(bounds) or b(bounds)


def _column(node: Any, schema: pl.Schema) -> str | None:
    """The name of a column node of a prunable dtype."""
    name = _get(node, "Column")
    if isinstance(name, str) and isinstance(schema.get(name), _SIGNED):
        return name
    return None


def _test(node: Any, schema: pl.Schema) -> Callable[[dict[str, tuple[int, int]]], bool] | None:
    """A test of row-group bounds for the predicate `node` (serialized Polars expression),
    or `None` when the predicate is not understood (the row group is then kept)."""
    if not isinstance(node, dict):
        return None
    binary = node.get("BinaryExpr")
    if isinstance(binary, dict):
        op, left, right = binary.get("op"), binary.get("left"), binary.get("right")
        if op in {"And", "LogicalAnd"}:
            a, b = _test(left, schema), _test(right, schema)
            if a is None:
                return b
            if b is None:
                return a
            return _both(a, b)
        if op in {"Or", "LogicalOr"}:
            a, b = _test(left, schema), _test(right, schema)
            if a is None or b is None:
                return None
            return _either(a, b)
        if op not in _FLIP:
            return None
        name = _column(left, schema)
        if name is not None:
            v = _literal(right, schema[name])
            return None if v is None else _compare(name, op, v)
        name = _column(right, schema)
        if name is not None:
            v = _literal(left, schema[name])
            return None if v is None else _compare(name, _FLIP[op], v)
        return None
    function = node.get("Function")
    if isinstance(function, dict):
        between = _get(function, "function", "Boolean", "IsBetween")
        inputs = function.get("input")
        if not isinstance(between, dict) or not isinstance(inputs, list) or len(inputs) != 3:
            return None
        name = _column(inputs[0], schema)
        if name is None:
            return None
        lo, hi = _literal(inputs[1], schema[name]), _literal(inputs[2], schema[name])
        if lo is None or hi is None:
            return None
        closed = between.get("closed")
        low = _compare(name, "GtEq" if closed in {"Both", "Left"} else "Gt", lo)
        high = _compare(name, "LtEq" if closed in {"Both", "Right"} else "Lt", hi)
        return _both(low, high)
    return None


def _row_groups(f: ParquetFile, predicate: pl.Expr, schema: pl.Schema) -> list[int]:
    """The row groups whose statistics do not rule `predicate` out."""
    bounds = f.integer_bounds()
    try:
        tree = json.loads(predicate.meta.serialize(format="json"))
    except Exception:  # the serialized form is not a stable Polars API: then prune nothing
        return list(range(len(bounds)))
    test = _test(tree, schema)
    return [i for i, b in enumerate(bounds) if test is None or test(b)]


#: Rows per batch when neither the caller nor the engine asks for a size.
DEFAULT_BATCH_SIZE = 65536


def _source(
    f: ParquetFile, batch_size: int | None, schema: pl.Schema
) -> Callable[[list[str] | None, pl.Expr | None, int | None, int | None], Iterator[pl.DataFrame]]:
    """The IO plugin's generator over `f`, whose Polars schema is `schema`."""

    def source(
        with_columns: list[str] | None, predicate: pl.Expr | None, n_rows: int | None, hint: int | None
    ) -> Iterator[pl.DataFrame]:
        columns = with_columns
        if columns is not None and predicate is not None:
            columns = list(dict.fromkeys([*columns, *predicate.meta.root_names()]))
        row_groups = None if predicate is None else _row_groups(f, predicate, schema)
        # a limit on matching rows cannot go to the reader
        limit = n_rows if predicate is None else None
        left = n_rows
        for batch in f.stream(columns, row_groups, batch_size or hint or DEFAULT_BATCH_SIZE, limit):
            df = pl.DataFrame(batch)
            if predicate is not None:
                df = df.filter(predicate)
            if with_columns is not None:
                df = df.select(with_columns)
            if left is not None:
                df = df.head(left)
                left -= df.height
            yield df
            if left == 0:
                return

    return source


def scan_parquet(source: Source, *, batch_size: int | None = None) -> pl.LazyFrame:
    """A LazyFrame over a Parquet file (PCODEC or not).

    The query's projection, row limit and predicate are pushed to the reader; the predicate
    is applied with Polars and also skips row groups whose integer, date and datetime
    statistics rule it out. Batches have `batch_size` rows, else the size the engine asks for.
    """
    if batch_size is not None and batch_size <= 0:
        raise ValueError(f"batch_size must be positive, got {batch_size}")
    f = ParquetFile(source)
    schema = pl.DataFrame(f.empty()).schema
    return register_io_source(_source(f, batch_size, schema), schema=schema)
