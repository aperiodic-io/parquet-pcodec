"""Shared fixtures: the conformance vectors and small tables."""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, settings

# HYPOTHESIS_PROFILE: "default", "thorough" (CI weekly) or "mutation" (fixed examples)
_slow_ok = [HealthCheck.too_slow, HealthCheck.data_too_large]
settings.register_profile("default", max_examples=40, deadline=None, suppress_health_check=_slow_ok)
settings.register_profile("thorough", max_examples=400, deadline=None, suppress_health_check=_slow_ok)
settings.register_profile("mutation", max_examples=10, deadline=None, derandomize=True, suppress_health_check=_slow_ok)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))

REPO = pathlib.Path(__file__).resolve().parents[2]
TESTDATA = pathlib.Path(os.environ.get("TPARQUET_TESTDATA", REPO / "testdata"))


def manifest() -> list[dict[str, Any]]:
    """The entries of `testdata/files/manifest.json`."""
    files: list[dict[str, Any]] = json.loads((TESTDATA / "files" / "manifest.json").read_text())["files"]
    return files


def vector(name: str) -> pathlib.Path:
    """The conformance file `name`."""
    return TESTDATA / "files" / f"{name}.parquet"


TPARQUET_FILES = [f["name"] for f in manifest() if f["pcodec_version"] is not None]


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
