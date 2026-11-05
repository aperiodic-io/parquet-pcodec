"""Loading the extension."""

import subprocess
import sys

import duckdb
import pytest

from conftest import FILES, extension_path, connect


def test_loads_with_allow_unsigned_extensions(con):
    loaded = con.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() WHERE extension_name = 'tparquet'"
    ).fetchall()
    assert loaded == [(True, "NOT_INSTALLED")] or loaded[0][0] is True
    functions = con.execute(
        "SELECT function_type FROM duckdb_functions() WHERE function_name = 'read_tparquet'"
    ).fetchall()
    assert functions == [("table",)]


def test_unsigned_extension_is_refused_without_the_setting():
    c = duckdb.connect()
    with pytest.raises(duckdb.Error, match="signature"):
        c.execute(f"LOAD '{extension_path()}'")


def test_reads_a_standard_parquet_file(con):
    rows = con.execute(f"SELECT count(*) FROM read_tparquet('{FILES}/standard_control.parquet')").fetchone()
    assert rows == (1500,)


def test_loads_in_a_separate_python_process():
    code = (
        "import duckdb\n"
        "c = duckdb.connect(config={'allow_unsigned_extensions': 'true'})\n"
        f"c.execute(\"LOAD '{extension_path()}'\")\n"
        f"print(c.execute(\"SELECT count(*) FROM read_tparquet('{FILES}/mixed.parquet')\").fetchone()[0])\n"
    )
    out = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip().isdigit()
