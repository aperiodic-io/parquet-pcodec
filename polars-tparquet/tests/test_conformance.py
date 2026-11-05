"""The conformance vectors of testdata/, read through the plugin, equal what Polars' own Parquet
reader reads from a standard (non-PCODEC) copy of the same file: an oracle without our reader."""

from __future__ import annotations

import pathlib

import polars as pl
import pytest
import tparquet
from polars.testing import assert_frame_equal

import polars_tparquet as pt
from conftest import VECTORS


def test_every_vector_is_here() -> None:
    assert len(VECTORS) == 13


@pytest.mark.parametrize("path", VECTORS, ids=lambda p: p.stem)
def test_vector_equals_polars_reading_a_standard_copy(path: pathlib.Path, tmp_path: pathlib.Path) -> None:
    standard = tmp_path / "standard.parquet"
    tparquet.transcode(path, standard, target="standard")
    want = pl.read_parquet(standard)
    assert_frame_equal(pt.read_tparquet(path), want, check_exact=True)
    assert_frame_equal(pt.scan_tparquet(path).collect(engine="streaming"), want, check_exact=True)


def test_polars_alone_refuses_a_pcodec_file() -> None:
    pcodec = next(p for p in VECTORS if p.stem == "primitives_required")
    with pytest.raises(pl.exceptions.ComputeError):
        pl.read_parquet(pcodec)
