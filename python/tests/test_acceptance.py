"""Acceptance:

(a) pyarrow, Polars, pandas and DuckDB's own Parquet readers raise on every PCODEC file
    (never return data) and read standard_control;
(b) on deterministic synthetic market data, Auto is at most 0.8 times the bytes of
    `pq.write_table(..., compression="zstd", compression_level=9)`, and round-trips bit for bit;
(c) files written from Python are verified by the Rust CLI against a pyarrow-written copy.

Set TPARQUET_ACCEPTANCE_LOG to a path to record the foreign readers' errors and the
ratios there (JSON).
"""

from __future__ import annotations

import io
import json
import os
import pathlib
import shutil
import subprocess
from collections.abc import Callable, Iterator
from typing import Any

import duckdb
import market
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import tparquet as ppc
from conftest import REPO, TESTDATA, TPARQUET_FILES, manifest, vector
from tparquet import canonical

LOG: dict[str, Any] = {"errors": {}, "ratios": {}}
PYARROW_MAJOR = int(pa.__version__.split(".")[0])


@pytest.fixture(scope="module", autouse=True)
def record() -> Iterator[None]:
    yield
    target = os.environ.get("TPARQUET_ACCEPTANCE_LOG")
    if target:
        pathlib.Path(target).write_text(json.dumps(LOG, indent=2, sort_keys=True) + "\n")


def duckdb_read(path: pathlib.Path) -> list[Any]:
    con = duckdb.connect()
    try:
        return con.sql(f"select * from read_parquet('{path}')").fetchall()
    finally:
        con.close()


READERS: dict[str, Callable[[pathlib.Path], object]] = {
    "pyarrow": pq.read_table,
    "polars": pl.read_parquet,
    "polars-lazy": lambda p: pl.scan_parquet(p).collect(),
    "pandas": pd.read_parquet,
    "duckdb": duckdb_read,
}


@pytest.mark.parametrize("reader", READERS)
@pytest.mark.parametrize("name", TPARQUET_FILES)
def test_a_foreign_readers_fail_loudly(reader: str, name: str) -> None:
    rows = next(f["rows"] for f in manifest() if f["name"] == name)
    if rows == 0 and reader in {"pyarrow", "pandas"} and PYARROW_MAJOR < 26:
        # pyarrow < 26 checks the codec only when it decompresses a page, and `empty` has none:
        # it reads the file as the empty table it is (no wrong data, but no error either)
        out = READERS[reader](vector(name))
        LOG["errors"].setdefault(reader, {})[name] = f"no error: read {len(out)} rows"  # type: ignore[arg-type]
        assert len(out) == 0  # type: ignore[arg-type]
        return
    with pytest.raises(Exception) as e:  # noqa: PT011 - each library raises its own type
        READERS[reader](vector(name))
    message = f"{type(e.value).__module__}.{type(e.value).__name__}: {e.value}"
    LOG["errors"].setdefault(reader, {})[name] = message
    # the message differs between versions (pyarrow 26: "Unsupported compression codec value:
    # 20547", pyarrow 25: "Unexpected end of stream"); it is recorded, and must say something
    assert str(e.value).strip(), message


@pytest.mark.parametrize("reader", READERS)
def test_a_foreign_readers_read_the_standard_control(reader: str) -> None:
    out = READERS[reader](vector("standard_control"))
    expected = (TESTDATA / "expected" / "standard_control.jsonl").read_bytes()
    assert len(out) == expected.count(b"\n")  # type: ignore[arg-type]
    if reader == "pyarrow":
        assert isinstance(out, pa.Table)
        assert canonical.dumps(out).encode() == expected


def test_a_pcodec_files_written_from_python_fail_too(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "p.parquet"
    ppc.write_table(market.trades(5000), path)
    for reader in READERS.values():
        with pytest.raises(Exception):  # noqa: B017, PT011
            reader(path)


def sizes(table: pa.Table) -> tuple[bytes, int]:
    auto = io.BytesIO()
    ppc.write_table(table, auto)
    z9 = io.BytesIO()
    pq.write_table(table, z9, compression="zstd", compression_level=9)
    return auto.getvalue(), len(z9.getvalue())


@pytest.mark.slow
def test_b_auto_is_at_most_0_8_of_zstd9_on_market_data() -> None:
    total_auto = total_z9 = 0
    for name, table in [("trades", market.trades()), ("books", market.books())]:
        auto, z9 = sizes(table)
        # bit for bit
        assert canonical.dumps(ppc.read_table(io.BytesIO(auto))) == canonical.dumps(table)
        LOG["ratios"][name] = {"auto": len(auto), "zstd9": z9, "ratio": round(len(auto) / z9, 3)}
        total_auto += len(auto)
        total_z9 += z9
    LOG["ratios"]["trades+books"] = {"auto": total_auto, "zstd9": total_z9, "ratio": round(total_auto / total_z9, 3)}
    assert LOG["ratios"]["trades"]["auto"] <= 0.8 * LOG["ratios"]["trades"]["zstd9"], LOG["ratios"]
    assert total_auto <= 0.8 * total_z9, LOG["ratios"]
    # the book's size lists alone do not get there: dictionary + ZSTD(9) beats PCODEC on them,
    # so Auto keeps the base codec (ZSTD(3)) for them
    assert LOG["ratios"]["books"]["ratio"] > 0.8


def test_b_small_market_sample_round_trips(tmp_path: pathlib.Path) -> None:
    table = market.trades(3000)
    auto, z9 = sizes(table)
    assert len(auto) <= 0.8 * z9
    assert ppc.read_table(io.BytesIO(auto)).equals(table)
    books = market.books(300)
    auto, _ = sizes(books)
    assert canonical.dumps(ppc.read_table(io.BytesIO(auto))) == canonical.dumps(books)


def cli() -> list[str]:
    exe = os.environ.get("TPARQUET_CLI")
    if exe:
        return [exe]
    cargo = shutil.which("cargo")
    if cargo is None:
        pytest.skip("cargo is not installed and TPARQUET_CLI is not set")
    return [cargo, "run", "-q", "-j", "2", "--manifest-path", str(REPO / "Cargo.toml"), "-p", "tparquet", "--"]


@pytest.mark.slow
@pytest.mark.parametrize("policy", ["auto", "pcodec", "base"])
def test_c_rust_cli_verifies_python_files(tmp_path: pathlib.Path, policy: ppc.Policy) -> None:
    trades = market.trades(20_000)
    books = market.books(2_000)
    for name, table in [("trades", trades), ("books", books)]:
        ours = tmp_path / f"{name}-{policy}.parquet"
        theirs = tmp_path / f"{name}-pyarrow.parquet"
        ppc.write_table(table, ours, policy=policy, row_group_size=7_000)
        pq.write_table(table, theirs)
        out = subprocess.run([*cli(), "verify", str(ours), str(theirs)], capture_output=True, text=True, check=False)
        assert out.returncode == 0, out.stdout + out.stderr
        assert out.stdout.strip() == "identical"
    # and it does tell different files apart
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    ppc.write_table(pa.table({"x": [1.0, 2.0]}), a, policy=policy)
    pq.write_table(pa.table({"x": [1.0, -0.0]}), b)
    out = subprocess.run([*cli(), "verify", str(a), str(b)], capture_output=True, text=True, check=False)
    assert out.returncode == 1
    assert "row 1 differs" in out.stdout
