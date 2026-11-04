"""pparquet.duckdb: PCODEC files queried from DuckDB-Python through the Arrow stream."""

from __future__ import annotations

import io
import pathlib
import subprocess
import sys
import textwrap
from collections.abc import Iterator

import duckdb
import pyarrow as pa
import pytest

import pparquet as ppc
import pparquet.duckdb as pdd
from conftest import PPARQUET_FILES, vector


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture
def trades(tmp_path: pathlib.Path) -> pathlib.Path:
    n = 1000
    t = pa.table(
        {
            "ts": pa.array([1_735_830_000_000 + i * 250 for i in range(n)], pa.timestamp("ms", tz="UTC")),
            "sym": [f"S{i % 3}" for i in range(n)],
            "price": [round(100 + (i % 41) * 0.01, 2) for i in range(n)],
            "size": pa.array([(i % 13 + 1) * 100 for i in range(n)], pa.int32()),
        }
    )
    path = tmp_path / "trades.parquet"
    ppc.write_table(t, path, policy="pcodec", row_group_size=300)
    return path


def test_register_and_query_repeatedly(con: duckdb.DuckDBPyConnection, trades: pathlib.Path) -> None:
    assert pdd.register(con, "trades", trades) is con
    count, total = con.sql("select count(*), sum(size) from trades").fetchall()[0]
    assert count == 1000
    assert total == sum((i % 13 + 1) * 100 for i in range(1000))
    # a registered file can be scanned any number of times
    for _ in range(3):
        assert con.sql("select count(*) from trades where sym = 'S1'").fetchall()[0] == (333,)
    by_sym = con.sql("select sym, max(price) from trades group by sym order by sym").fetchall()
    assert [s for s, _ in by_sym] == ["S0", "S1", "S2"]


def test_same_rows_as_duckdb_reading_a_standard_copy(
    con: duckdb.DuckDBPyConnection, trades: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    std = tmp_path / "std.parquet"
    ppc.transcode(trades, std, target="standard")
    pdd.register(con, "t", trades)
    ours = con.sql("select * from t order by ts").fetchall()
    theirs = con.sql(f"select * from read_parquet('{std}') order by ts").fetchall()
    assert ours == theirs


@pytest.mark.parametrize("name", PPARQUET_FILES)
def test_conformance_files(con: duckdb.DuckDBPyConnection, name: str, tmp_path: pathlib.Path) -> None:
    # every vector reads in DuckDB as DuckDB reads its standard transcoding
    std = tmp_path / "std.parquet"
    ppc.transcode(vector(name), std, target="standard")
    pdd.register(con, "v", vector(name))
    ours = con.sql("select * from v").fetchall()
    theirs = con.sql(f"select * from read_parquet('{std}')").fetchall()
    assert repr(ours) == repr(theirs)


def test_register_columns_and_join(con: duckdb.DuckDBPyConnection, trades: pathlib.Path) -> None:
    pdd.register(con, "a", trades, columns=["sym", "size"])
    pdd.register(con, "b", trades, columns=["price"])
    assert [d[0] for d in con.sql("select * from a").description] == ["sym", "size"]
    assert con.sql("select count(*) from a, (select max(price) m from b) where size > 1000").fetchall()[0] == (
        sum(1 for i in range(1000) if (i % 13 + 1) * 100 > 1000),
    )


def test_sql_view(con: duckdb.DuckDBPyConnection, trades: pathlib.Path) -> None:
    pdd.sql_view(con, "trades_v", trades)
    assert con.sql("select view_name from duckdb_views() where view_name = 'trades_v'").fetchall()[0] == ("trades_v",)
    con.sql("create temp view big as select * from trades_v where size >= 1200")
    assert con.sql("select count(*) from big").fetchall()[0] == (sum(1 for i in range(1000) if i % 13 + 1 >= 12),)
    # replacing the view points it at another file
    pdd.sql_view(con, "trades_v", trades, columns=["sym"])
    assert [d[0] for d in con.sql("select * from trades_v").description] == ["sym"]


def test_sql_view_names_are_quoted(con: duckdb.DuckDBPyConnection, trades: pathlib.Path) -> None:
    pdd.sql_view(con, 'odd "name"', trades, columns=["size"])
    assert con.sql('select count(*) from "odd ""name"""').fetchall() == [(1000,)]


def test_read_parquet_uses_the_connection_and_columns(trades: pathlib.Path) -> None:
    con = duckdb.connect()
    rel = pdd.read_parquet(trades, connection=con, columns=["size", "sym"])
    assert rel.columns == ["size", "sym"]
    con.close()
    with pytest.raises(duckdb.ConnectionException):
        rel.fetchall()


def test_read_parquet_relation(trades: pathlib.Path, con: duckdb.DuckDBPyConnection) -> None:
    rel = pdd.read_parquet(trades, connection=con)
    assert rel.columns == ["ts", "sym", "price", "size"]
    assert rel.filter("sym = 'S2'").count("*").fetchall()[0] == (333,)
    assert rel.aggregate("count(*)").fetchall()[0] == (1000,)
    default = pdd.read_parquet(trades)
    assert default.aggregate("min(size)").fetchall()[0] == (100,)


def test_file_objects(con: duckdb.DuckDBPyConnection, trades: pathlib.Path) -> None:
    pdd.register(con, "f", io.BytesIO(trades.read_bytes()))
    assert con.sql("select count(*) from f").fetchall()[0] == (1000,)


def test_source_object(trades: pathlib.Path) -> None:
    src = pdd.ArrowSource(trades, columns=["size"], batch_size=128)
    assert pa.schema(src) == pa.schema([pa.field("size", pa.int32())])  # type: ignore[arg-type]
    first = pa.RecordBatchReader.from_stream(src).read_all()
    second = pa.RecordBatchReader.from_stream(src).read_all()
    assert first.equals(second)
    assert first.num_rows == 1000
    assert max(b.num_rows for b in first.to_batches()) == 128
    assert "ArrowSource" in repr(src)
    # the requested schema reaches the stream
    same = pa.RecordBatchReader.from_stream(src, schema=pa.schema([pa.field("size", pa.int32())]))
    assert same.read_all().num_rows == 1000
    with pytest.raises(ppc.PcodecError, match="cast"):
        pa.RecordBatchReader.from_stream(src, schema=pa.schema([pa.field("size", pa.int64())]))


def test_works_without_pyarrow(trades: pathlib.Path) -> None:
    code = textwrap.dedent(
        f"""
        import sys
        sys.modules["pyarrow"] = None  # importing pyarrow now fails
        import duckdb
        import pparquet.duckdb as pdd
        con = duckdb.connect()
        pdd.register(con, "t", {str(trades)!r})
        print(con.sql("select count(*), min(size) from t").fetchall()[0])
        con.close()
        """
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "(1000, 100)"
