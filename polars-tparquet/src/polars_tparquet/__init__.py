"""Polars plugin for tparquet: Parquet for time series, with the PCODEC codec.

    import polars_tparquet as pt

    lf = pt.scan_tparquet("trades/**/*.parquet")        # lazy; files, lists, globs, directories
    df = pt.read_tparquet("t.parquet", columns=["ts", "price"])
    df.tparquet.write("out.parquet")                     # PCODEC where it is smaller
    lf.filter(...).tparquet.sink("out.parquet")          # streams the query into one file

Reading and writing are tparquet's Rust core; batches reach Polars through the Arrow C
interface. Each file is a Polars IO source, so a query's projection, row limit and predicate
are pushed to the reader, and the predicate also skips row groups by their statistics. Files
that are not PCODEC are read too.
"""

from __future__ import annotations

import glob
import os
import pathlib
from collections.abc import Mapping, Sequence
from typing import Union

import polars as pl
from tparquet import Policy, Sink, Source, Writer
from tparquet.polars import scan_parquet, write_parquet

__all__ = ["DataFrameNamespace", "LazyFrameNamespace", "read_tparquet", "scan_tparquet"]

#: One file, or several: a list of paths and file objects, a glob, or a directory.
Sources = Union[Source, Sequence[Source]]

_GLOB = frozenset("*?[")


def _is_path(source: object) -> bool:
    return isinstance(source, (str, bytes, os.PathLike))


def _expand(source: Source) -> Sequence[Source]:
    """The files one source names: a glob's matches and a directory's .parquet files, sorted."""
    if not _is_path(source):
        return [source]
    name = os.fsdecode(source)  # type: ignore[arg-type]
    if isinstance(source, (str, bytes)) and _GLOB & set(name):
        matches = sorted(glob.glob(name, recursive=True))
        if not matches:
            raise FileNotFoundError(f"no file matches {name}")
        return matches
    if os.path.isdir(name):
        below = sorted(str(p) for p in pathlib.Path(name).rglob("*.parquet") if p.is_file())
        if not below:
            raise FileNotFoundError(f"no .parquet file in {name}")
        return below
    return [source]


def _files(source: Sources) -> list[Source]:
    if isinstance(source, (list, tuple)):
        if not source:
            raise ValueError("no files to scan")
        return [f for s in source for f in _expand(s)]
    return list(_expand(source))  # type: ignore[arg-type]


def _name(source: Source) -> str:
    return os.fsdecode(source) if _is_path(source) else repr(source)  # type: ignore[arg-type]


def scan_tparquet(
    source: Sources, *, batch_size: int | None = None, include_file_paths: str | None = None
) -> pl.LazyFrame:
    """A LazyFrame over Parquet files (PCODEC or not), read in order.

    `source` is a path, a binary file object, a glob (`*`, `?`, `[...]`; `**` matches
    directories too), a directory (every `.parquet` file below it), or a list of these. Globs
    and directories are read in sorted order. All files must have the same schema.

    Each file is a Polars IO source: the projection, row limit and predicate of the query reach
    the reader, and the predicate skips row groups whose statistics rule it out. Batches have
    `batch_size` rows, else the size the engine asks for. `include_file_paths` adds a String
    column of that name holding each row's file path.
    """
    frames: list[pl.LazyFrame] = []
    first: tuple[str, pl.Schema] | None = None
    for f in _files(source):
        lf = scan_parquet(f, batch_size=batch_size)
        schema = lf.collect_schema()
        if first is None:
            first = (_name(f), schema)
        elif schema != first[1]:
            raise pl.exceptions.SchemaError(
                f"{_name(f)}: schema differs from {first[0]}: {dict(schema)} != {dict(first[1])}"
            )
        if include_file_paths is not None:
            if not _is_path(f):
                raise ValueError("include_file_paths needs file paths, not file objects")
            if include_file_paths in schema:
                raise pl.exceptions.DuplicateError(f"{_name(f)} already has a column {include_file_paths!r}")
            lf = lf.with_columns(pl.lit(_name(f)).alias(include_file_paths))
        frames.append(lf)
    return pl.concat(frames)


def read_tparquet(
    source: Sources,
    *,
    columns: Sequence[str] | None = None,
    n_rows: int | None = None,
    batch_size: int | None = None,
    include_file_paths: str | None = None,
) -> pl.DataFrame:
    """Parquet files (PCODEC or not) as one DataFrame: `scan_tparquet`, with `columns` in the
    order given and at most `n_rows` rows, collected."""
    if n_rows is not None and n_rows < 0:
        raise ValueError(f"n_rows must not be negative, got {n_rows}")
    lf = scan_tparquet(source, batch_size=batch_size, include_file_paths=include_file_paths)
    if columns is not None:
        lf = lf.select(columns)
    if n_rows is not None:
        lf = lf.head(n_rows)
    return lf.collect()


@pl.api.register_dataframe_namespace("tparquet")
class DataFrameNamespace:
    """`df.tparquet`: writes the frame as Parquet with the PCODEC codec."""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def write(
        self,
        target: Sink,
        *,
        policy: Policy = "auto",
        columns: Mapping[str, Policy] | None = None,
        level: int = 8,
        # "ZSTD(3)" (a mutant) names the same codec as "zstd(3)": no test can tell them apart
        base: str = "zstd(3)",  # pragma: no mutate
        row_group_size: int | None = None,
    ) -> dict[str, str]:
        """Writes the frame to `target` (a path or a binary file object) and returns the codec of
        every leaf column.

        With `policy="auto"` each INT32/INT64/FLOAT/DOUBLE column gets whichever of PCODEC and the
        base codec is smaller; `"pcodec"` and `"base"` force one. `columns` maps a column name or
        dotted leaf path to its own policy, `level` is the pcodec level (0 to 12), `base` the codec
        of the other columns (`"zstd(3)"`, `"snappy"`, ...) and `row_group_size` the most rows
        per row group.
        """
        return write_parquet(
            self._df, target, policy=policy, columns=columns, level=level, base=base, row_group_size=row_group_size
        )


@pl.api.register_lazyframe_namespace("tparquet")
class LazyFrameNamespace:
    """`lf.tparquet`: streams the query's result into a Parquet file with the PCODEC codec."""

    def __init__(self, lf: pl.LazyFrame) -> None:
        self._lf = lf

    def sink(
        self,
        target: Sink,
        *,
        policy: Policy = "auto",
        columns: Mapping[str, Policy] | None = None,
        level: int = 8,
        # "ZSTD(3)" (a mutant) names the same codec as "zstd(3)": no test can tell them apart
        base: str = "zstd(3)",  # pragma: no mutate
        row_group_size: int | None = None,
        chunk_size: int | None = None,
    ) -> dict[str, str]:
        """Runs the query and writes its result to `target` batch by batch (`chunk_size` rows,
        else the engine's), never holding all of it; returns the codec of every leaf column.

        The options are those of `DataFrame.tparquet.write`; with `policy="auto"` the codecs are
        chosen on the first batch with rows. If the query fails, a file the sink created is
        removed.
        """
        if chunk_size is not None and chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        writer = Writer(
            target,
            self._lf.collect_schema(),
            policy=policy,
            columns=columns,
            level=level,
            base=base,
            row_group_size=row_group_size,
        )
        try:
            for batch in self._lf.collect_batches(chunk_size=chunk_size):
                writer.write(batch)
        except BaseException:
            writer.abort()
            raise
        return writer.close()
