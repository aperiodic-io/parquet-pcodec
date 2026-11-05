"""Property tests: any frame written through the plugin reads back the same, split or not."""

from __future__ import annotations

import pathlib

import polars as pl
from hypothesis import given
from hypothesis import strategies as st
from polars.testing import assert_frame_equal
from tparquet import Policy

import polars_tparquet as pt


@st.composite
def frames(draw: st.DrawFn) -> pl.DataFrame:
    n = draw(st.integers(1, 300))
    i64 = draw(st.lists(st.one_of(st.none(), st.integers(-(2**63), 2**63 - 1)), min_size=n, max_size=n))
    f64 = draw(st.lists(st.one_of(st.none(), st.floats(allow_nan=True, allow_infinity=True)), min_size=n, max_size=n))
    f32 = draw(st.lists(st.floats(width=32), min_size=n, max_size=n))
    s = draw(st.lists(st.one_of(st.none(), st.text(max_size=8)), min_size=n, max_size=n))
    return pl.DataFrame(
        {
            "i64": pl.Series(i64, dtype=pl.Int64),
            "f64": pl.Series(f64, dtype=pl.Float64),
            "f32": pl.Series(f32, dtype=pl.Float32),
            "s": pl.Series(s, dtype=pl.String),
        }
    )


@given(df=frames(), parts=st.integers(1, 4), policy=st.sampled_from(["auto", "pcodec", "base"]))
def test_write_then_scan_any_frame(tmp_path: pathlib.Path, df: pl.DataFrame, parts: int, policy: Policy) -> None:
    size = -(-df.height // parts)
    paths = []
    for k in range(parts):
        part = df.slice(k * size, size)
        if part.height:
            paths.append(tmp_path / f"{k}.parquet")
            part.tparquet.write(paths[-1], policy=policy)
    assert_frame_equal(pt.scan_tparquet(paths).collect(), df, check_exact=True)
    sunk = tmp_path / "sunk.parquet"
    pt.scan_tparquet(paths).tparquet.sink(sunk, policy=policy)
    assert_frame_equal(pt.read_tparquet(sunk), df, check_exact=True)
