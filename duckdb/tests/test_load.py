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


def test_loading_twice_keeps_one_working_extension(con):
    con.execute(f"LOAD '{extension_path()}'")
    con.execute(f"LOAD '{extension_path()}'")
    assert con.execute(f"SELECT count(*) FROM read_tparquet('{FILES}/mixed.parquet')").fetchone() == (1500,)
    assert con.execute(
        "SELECT count(*) FROM duckdb_functions() WHERE function_name = 'read_tparquet'"
    ).fetchone() == (1,)


def test_every_connection_of_the_database_sees_the_functions(con, tmp_path):
    other = con.cursor()
    other.execute(f"COPY (SELECT 1 AS a) TO '{tmp_path}/a.parquet' (FORMAT tparquet, COLUMN_NAMES ['a'])")
    assert con.execute(f"SELECT * FROM read_tparquet('{tmp_path}/a.parquet')").fetchall() == [(1,)]
    other.close()


def test_two_databases_load_it_independently(tmp_path):
    a, b = connect(), connect()
    a.close()
    assert b.execute(f"SELECT count(*) FROM read_tparquet('{FILES}/mixed.parquet')").fetchone() == (1500,)
    b.close()
    # a database without it does not know the function
    plain = duckdb.connect()
    with pytest.raises(duckdb.CatalogException, match="read_tparquet"):
        plain.execute(f"SELECT * FROM read_tparquet('{FILES}/mixed.parquet')")
    plain.close()


def test_a_file_database_reopened_needs_the_extension_again(tmp_path):
    db = str(tmp_path / "x.duckdb")
    c = duckdb.connect(db, config={"allow_unsigned_extensions": "true"})
    c.execute(f"LOAD '{extension_path()}'")
    c.execute(f"CREATE TABLE t AS SELECT * FROM read_tparquet('{FILES}/mixed.parquet')")
    c.close()
    c = duckdb.connect(db, config={"allow_unsigned_extensions": "true"})
    assert c.execute("SELECT count(*) FROM t").fetchone() == (1500,)
    with pytest.raises(duckdb.Error):
        c.execute(f"SELECT * FROM read_tparquet('{FILES}/mixed.parquet')")
    c.close()


def test_the_extension_is_listed_with_its_version(con):
    got = con.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'tparquet'"
    ).fetchall()
    assert got in ([("v0.1.0",)], [("",)], [(None,)])
