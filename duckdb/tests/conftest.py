"""Fixtures of the SQL tests: a DuckDB connection with the built extension loaded.

The extension comes from $PCODEC_DUCKDB_EXTENSION (a `parquet_pcodec.duckdb_extension`, as
`make build` or the cargo test `tests/sql.rs` makes it).
"""

import os
import pathlib
import subprocess
import sys
import threading
import time

import duckdb
import pytest


def _watch_memory(limit_mb: int) -> None:
    """Ends the process when its resident memory passes `limit_mb`."""
    page = os.sysconf("SC_PAGE_SIZE")
    while True:
        with open("/proc/self/statm") as f:
            resident = int(f.read().split()[1]) * page
        if resident > limit_mb << 20:
            print(f"conftest: resident memory {resident >> 20} MB is over {limit_mb} MB", file=sys.stderr)
            os._exit(3)
        time.sleep(0.2)


# $PCODEC_DUCKDB_MEMORY_LIMIT_MB bounds the resident memory of the test process (`make
# mutation` sets it). A mutant that makes a scan loop forever produces rows without end, about
# 65 MB/s, which exhausts a CI runner before cargo-mutants' timeout. Over the limit the process
# exits, the tests fail, and the mutant is caught. (An address-space limit does not work:
# DuckDB and the other native libraries reserve far more address space than they use.)
if limit := os.environ.get("PCODEC_DUCKDB_MEMORY_LIMIT_MB"):
    threading.Thread(target=_watch_memory, args=(int(limit),), daemon=True).start()

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
