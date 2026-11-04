"""Shared fixtures: the conformance vectors and small tables."""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any

import pyarrow as pa
import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
TESTDATA = pathlib.Path(os.environ.get("PARQUET_PCODEC_TESTDATA", REPO / "testdata"))


def manifest() -> list[dict[str, Any]]:
    """The entries of `testdata/files/manifest.json`."""
    files: list[dict[str, Any]] = json.loads((TESTDATA / "files" / "manifest.json").read_text())["files"]
    return files


def vector(name: str) -> pathlib.Path:
    """The conformance file `name`."""
    return TESTDATA / "files" / f"{name}.parquet"


PCODEC_FILES = [f["name"] for f in manifest() if f["pcodec_version"] is not None]


@pytest.fixture
def numbers() -> pa.Table:
    """A small table with an eligible column of each physical type and a string column."""
    n = 2000
    return pa.table(
        {
            "i32": pa.array([i * 7 for i in range(n)], pa.int32()),
            "i64": pa.array([1_700_000_000_000 + i * 250 for i in range(n)], pa.int64()),
            "f32": pa.array([i / 4 for i in range(n)], pa.float32()),
            "f64": pa.array([100 + (i % 37) * 0.01 for i in range(n)], pa.float64()),
            "s": pa.array([f"sym{i % 5}" for i in range(n)], pa.string()),
        }
    )
