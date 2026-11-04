"""Fixtures of the SQL tests: a DuckDB connection with the built extension loaded.

The extension comes from $PCODEC_DUCKDB_EXTENSION (a `parquet_pcodec.duckdb_extension`, as
`make build` or the cargo test `tests/sql.rs` makes it).
"""

import os
import pathlib
import subprocess

import duckdb
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
TESTDATA = ROOT / "testdata"
FILES = TESTDATA / "files"
EXPECTED = TESTDATA / "expected"


def extension_path() -> str:
    path = os.environ.get("PCODEC_DUCKDB_EXTENSION")
    if not path:
        raise RuntimeError("set PCODEC_DUCKDB_EXTENSION to the built parquet_pcodec.duckdb_extension")
    return path


def connect(**config) -> duckdb.DuckDBPyConnection:
    """A new in-memory database with the extension loaded."""
    con = duckdb.connect(config={"allow_unsigned_extensions": "true", **config})
    con.execute(f"LOAD '{extension_path()}'")
    return con


@pytest.fixture
def con():
    c = connect()
    yield c
    c.close()


@pytest.fixture
def plain():
    """A connection without the extension."""
    c = duckdb.connect()
    yield c
    c.close()


def cli() -> str:
    """The parquet-pcodec CLI ($PCODEC_CLI, built by tests/sql.rs or `make build`)."""
    path = os.environ.get("PCODEC_CLI")
    if not path:
        raise RuntimeError("set PCODEC_CLI to the parquet-pcodec binary")
    return path


def run_cli(*args) -> subprocess.CompletedProcess:
    return subprocess.run([cli(), *map(str, args)], capture_output=True, text=True)


def encode(src, dst, policy: str = "pcodec"):
    """Transcodes a standard Parquet file to PCODEC with the Rust CLI."""
    out = run_cli("encode", src, dst, "--policy", policy)
    assert out.returncode == 0, out.stderr
    return dst
