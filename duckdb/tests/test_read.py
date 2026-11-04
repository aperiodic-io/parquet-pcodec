"""read_pparquet: types, projection, options, many files, parallelism and errors."""

import datetime
import decimal
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from canonical import lines
from conftest import FILES, connect, encode, run_cli


def rows(con, sql):
    return con.execute(sql).fetchall()


def types_of(con, sql):
    return [(r[0], r[1]) for r in con.execute(f"DESCRIBE {sql}").fetchall()]


# ---------------------------------------------------------------------------------------------
# types


def every_type() -> pa.Table:
    """Every Arrow type a Parquet file can carry that DuckDB can hold exactly, with nulls."""
    n = 5
    tz_ns = pa.array([1_735_689_600_007_000_001, None, -1, 0, 2**62], pa.timestamp("ns", tz="UTC"))
    return pa.table(
        {
            "i8": pa.array([-128, 127, None, 0, 1], pa.int8()),
            "i16": pa.array([-32768, 32767, None, 0, 1], pa.int16()),
            "i32": pa.array([-(2**31), 2**31 - 1, None, 0, 1], pa.int32()),
            "i64": pa.array([-(2**63), 2**63 - 1, None, 0, 1], pa.int64()),
            "u8": pa.array([0, 255, None, 1, 2], pa.uint8()),
            "u16": pa.array([0, 65535, None, 1, 2], pa.uint16()),
            "u32": pa.array([0, 2**32 - 1, None, 1, 2], pa.uint32()),
            "u64": pa.array([0, 2**64 - 1, None, 1, 2], pa.uint64()),
            "f32": pa.array([1.5, float("-inf"), None, -0.0, float("nan")], pa.float32()),
            "f64": pa.array([0.1, float("inf"), None, -0.0, 5e-324], pa.float64()),
            "dec4": pa.array([decimal.Decimal("-99.99"), None, 0, 1, 2], pa.decimal128(4, 2)),
            "dec9": pa.array([decimal.Decimal("-9999999.99"), None, 0, 1, 2], pa.decimal128(9, 2)),
            "dec18": pa.array([decimal.Decimal("12345678901234.5678"), None, 0, 1, 2], pa.decimal128(18, 4)),
            "dec38": pa.array([decimal.Decimal("-" + "9" * 38), None, 0, 1, 2], pa.decimal128(38, 0)),
            "dec256": pa.array([decimal.Decimal("123456789012345678.90"), None, 0, 1, 2], pa.decimal256(20, 2)),
            "s": pa.array(["a", 'quote " and \\ \n', None, "ünïcode ✓", ""], pa.string()),
            "ls": pa.array(["x", None, "y", "z", "long " * 10], pa.large_string()),
            "dict": pa.array(["BTC", "ETH", None, "BTC", "SOL"]).dictionary_encode(),
            "bin": pa.array([b"\x00\xff", None, b"", b"abc", b"\x01" * 20], pa.binary()),
            "lbin": pa.array([b"q", None, b"", b"r", b"s"], pa.large_binary()),
            "fbin": pa.array([b"abcd", None, b"efgh", b"ijkl", b"\x00\x00\x00\x00"], pa.binary(4)),
            "b": pa.array([True, False, None, True, False]),
            "d32": pa.array([20089, -719528, None, 0, 1], pa.date32()),
            "t32ms": pa.array([0, 86_399_999, None, 1, 2], pa.time32("ms")),
            "t64us": pa.array([0, 86_399_999_999, None, 1, 2], pa.time64("us")),
            "t64ns": pa.array([0, 86_399_999_999_999, None, 1, 7], pa.time64("ns")),
            "ts_s": pa.array([0, 1_735_689_600, None, -1, 2], pa.timestamp("s")),
            "ts_ms": pa.array([0, 1_735_689_600_123, None, -1, 2], pa.timestamp("ms")),
            "ts_us": pa.array([0, 1_735_689_600_123_456, None, -1, 2], pa.timestamp("us")),
            "ts_ns": pa.array([0, 1_735_689_600_123_456_789, None, -1, 2], pa.timestamp("ns")),
            "ts_s_tz": pa.array([0, 1_735_689_600, None, -1, 2], pa.timestamp("s", tz="UTC")),
            "ts_ms_tz": pa.array([0, 1_735_689_600_123, None, -1, 2], pa.timestamp("ms", tz="Europe/Paris")),
            "ts_us_tz": pa.array([0, 1_735_689_600_123_456, None, -1, 2], pa.timestamp("us", tz="UTC")),
            "ts_ns_tz": tz_ns,
            "li": pa.array([[1, None], None, [], [2**40], [3, 4, 5]], pa.list_(pa.int64())),
            "lls": pa.array([["a"], None, [None], [], ["b", "c"]], pa.large_list(pa.string())),
            # pyarrow's Parquet writer cannot write a NULL fixed-size list
            "fsl": pa.array([[1, 2], [9, 9], [3, None], [5, 6], [7, 8]], pa.list_(pa.int32(), 2)),
            "nested": pa.array(
                [
                    {"x": 1.5, "tags": [1, 2], "inner": {"y": "u"}},
                    None,
                    {"x": None, "tags": None, "inner": None},
                    {"x": -0.0, "tags": [], "inner": {"y": None}},
                    {"x": 2.5, "tags": [None], "inner": {"y": "v"}},
                ],
                pa.struct(
                    [("x", pa.float64()), ("tags", pa.list_(pa.int32())), ("inner", pa.struct([("y", pa.string())]))]
                ),
            ),
            "lst_struct": pa.array(
                [[{"p": 1.25, "q": 3}], None, [], [{"p": None, "q": None}, {"p": 2.0, "q": 1}], [None]],
                pa.list_(pa.struct([("p", pa.float64()), ("q", pa.int64())])),
            ),
            "m": pa.array([[("k", 1)], None, [], [("a", None), ("b", 2)], [("z", 3)]], pa.map_(pa.string(), pa.int32())),
        }
    ).slice(0, n)


DUCKDB_TYPES = {
    "i8": "TINYINT",
    "i16": "SMALLINT",
    "i32": "INTEGER",
    "i64": "BIGINT",
    "u8": "UTINYINT",
    "u16": "USMALLINT",
    "u32": "UINTEGER",
    "u64": "UBIGINT",
    "f32": "FLOAT",
    "f64": "DOUBLE",
    "dec4": "DECIMAL(4,2)",
    "dec9": "DECIMAL(9,2)",
    "dec18": "DECIMAL(18,4)",
    "dec38": "DECIMAL(38,0)",
    "dec256": "DECIMAL(20,2)",
    "s": "VARCHAR",
    "ls": "VARCHAR",
    "dict": "VARCHAR",
    "bin": "BLOB",
    "lbin": "BLOB",
    "fbin": "BLOB",
    "b": "BOOLEAN",
    "d32": "DATE",
    "t32ms": "TIME",
    "t64us": "TIME",
    "t64ns": "TIME_NS",
    # pyarrow stores seconds as milliseconds (Parquet has no seconds unit)
    "ts_s": "TIMESTAMP_MS",
    "ts_ms": "TIMESTAMP_MS",
    "ts_us": "TIMESTAMP",
    "ts_ns": "TIMESTAMP_NS",
    "ts_s_tz": "TIMESTAMP WITH TIME ZONE",
    "ts_ms_tz": "TIMESTAMP WITH TIME ZONE",
    "ts_us_tz": "TIMESTAMP WITH TIME ZONE",
    "ts_ns_tz": "TIMESTAMPTZ_NS",
    "li": "BIGINT[]",
    "lls": "VARCHAR[]",
    "fsl": "INTEGER[2]",
    "nested": 'STRUCT(x DOUBLE, tags INTEGER[], "inner" STRUCT(y VARCHAR))',
    "lst_struct": "STRUCT(p DOUBLE, q BIGINT)[]",
    "m": "MAP(VARCHAR, INTEGER)",
}


@pytest.fixture(scope="module")
def typed(tmp_path_factory):
    """The every_type table as standard Parquet (pyarrow) and as PCODEC (Rust CLI)."""
    d = tmp_path_factory.mktemp("types")
    standard = d / "standard.parquet"
    pq.write_table(every_type(), standard)
    return standard, encode(standard, d / "pcodec.parquet")


def test_the_pcodec_file_really_uses_pcodec(typed):
    out = run_cli("inspect", "--json", typed[1])
    assert out.returncode == 0, out.stderr
    assert "PCODEC" in out.stdout


@pytest.mark.parametrize("which", [0, 1], ids=["standard", "pcodec"])
def test_every_type_maps_to_the_exact_duckdb_type(con, typed, which):
    got = types_of(con, f"SELECT * FROM read_pparquet('{typed[which]}')")
    assert got == list(DUCKDB_TYPES.items())


@pytest.mark.parametrize("which", [0, 1], ids=["standard", "pcodec"])
def test_every_value_survives_bit_for_bit(con, typed, which):
    schema = pq.read_schema(typed[0])
    want = lines(pq.read_table(typed[0]), schema)
    table = con.sql(
        f"SELECT * EXCLUDE (file_row_number) FROM read_pparquet('{typed[which]}', file_row_number := true) "
        "ORDER BY file_row_number"
    ).to_arrow_table()
    assert lines(table, schema) == want


def test_values_read_as_duckdb_values(con, typed):
    r = rows(
        con,
        f"SELECT u64, i8, dec38::VARCHAR, ts_ns_tz::VARCHAR, t64ns::VARCHAR, ts_ms_tz, s, m, fsl "
        f"FROM read_pparquet('{typed[1]}') LIMIT 2",
    )
    assert r[0][0] == 0 and r[1][0] == 2**64 - 1
    assert r[0][1] == -128
    assert r[0][2] == "-" + "9" * 38
    assert r[0][3] == "2025-01-01 00:00:00.007000001+00"
    assert r[1][4] == "23:59:59.999999999"
    assert r[1][5].astimezone(datetime.timezone.utc) == datetime.datetime(2025, 1, 1, 0, 0, 0, 123000, datetime.timezone.utc)
    assert r[1][6] == 'quote " and \\ \n'
    assert r[0][7] == {"k": 1}
    assert r[0][8] == (1, 2)


def test_half_floats_widen_to_float(con, tmp_path):
    t = pa.table({"h": pa.array([1.5, None, -0.0, 65504.0], pa.float16())})
    pq.write_table(t, tmp_path / "h.parquet")
    assert types_of(con, f"SELECT * FROM read_pparquet('{tmp_path}/h.parquet')") == [("h", "FLOAT")]
    vals = [r[0] for r in rows(con, f"SELECT h FROM read_pparquet('{tmp_path}/h.parquet')")]
    assert vals[0] == 1.5 and vals[1] is None and vals[3] == 65504.0
    assert str(vals[2]) == "-0.0"


def test_unrepresentable_types_are_refused_clearly(con, tmp_path):
    t = pa.table({"wide": pa.array([decimal.Decimal(1)], pa.decimal256(40, 0))})
    pq.write_table(t, tmp_path / "w.parquet")
    with pytest.raises(duckdb.BinderException, match='column "wide": DuckDB decimals have 1 to 38 digits, not 40'):
        con.execute(f"SELECT * FROM read_pparquet('{tmp_path}/w.parquet')")


# ---------------------------------------------------------------------------------------------
# projection, filters, joins


def test_projection_reads_and_returns_only_the_selected_columns(con):
    f = FILES / "mixed.parquet"
    plan = rows(con, f"EXPLAIN SELECT qty, ts FROM read_pparquet('{f}')")[0][1]
    assert "Projections:" in plan and "qty" in plan and "ts" in plan and "symbol" not in plan
    got = rows(con, f"SELECT qty, symbol FROM read_pparquet('{f}') LIMIT 1")
    full = rows(con, f"SELECT * FROM read_pparquet('{f}') LIMIT 1")
    assert got == [(full[0][2], full[0][3])]
    assert rows(con, f"SELECT count(*) FROM read_pparquet('{f}')") == [(1500,)]
    assert rows(con, f"SELECT count(*) FROM read_pparquet('{FILES}/empty.parquet')") == [(0,)]


def test_filters_aggregates_and_joins_with_native_tables(con):
    f = FILES / "mixed.parquet"
    con.execute("CREATE TABLE venues (venue VARCHAR, city VARCHAR)")
    con.execute("INSERT INTO venues VALUES ('XNAS', 'New York'), ('XNYS', 'New York'), ('ARCX', 'Chicago')")
    by_city = rows(
        con,
        f"SELECT city, count(*), sum(qty)::DECIMAL(18,3) FROM read_pparquet('{f}') t "
        "JOIN venues USING (venue) WHERE is_buy AND symbol = 'BTC-USD' GROUP BY city ORDER BY city",
    )
    t = con.sql(f"SELECT * FROM read_pparquet('{f}')").to_arrow_table().to_pylist()
    city = {"XNAS": "New York", "XNYS": "New York", "ARCX": "Chicago"}
    expect = {}
    for r in t:
        if r["is_buy"] and r["symbol"] == "BTC-USD":
            c = city[r["venue"]]
            n, s = expect.get(c, (0, 0.0))
            expect[c] = (n + 1, s + r["qty"])
    assert [(c, n) for c, n, _ in by_city] == [(c, expect[c][0]) for c in sorted(expect)]
    for c, _, s in by_city:
        assert float(s) == pytest.approx(expect[c][1], abs=1e-3)
    # a native table created from the file holds the same rows
    con.execute(f"CREATE TABLE trades AS SELECT * FROM read_pparquet('{f}')")
    assert rows(con, "SELECT count(*), count(note) FROM trades") == [(1500, 1000)]
    assert rows(
        con,
        f"SELECT count(*) FROM trades a JOIN read_pparquet('{f}') b ON a.ts = b.ts AND a.price = b.price "
        "AND a.qty = b.qty AND a.symbol = b.symbol AND a.venue = b.venue",
    )[0][0] >= 1500


# ---------------------------------------------------------------------------------------------
# options and many files


def test_filename_and_file_row_number_columns(con):
    f = FILES / "multiple_row_groups.parquet"
    names = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_pparquet('{f}', filename := true, file_row_number := true)").fetchall()]
    assert names[-2:] == ["filename", "file_row_number"]
    r = rows(con, f"SELECT DISTINCT filename FROM read_pparquet('{f}', filename := true)")
    assert r == [(str(f),)]
    total = rows(con, f"SELECT count(*) FROM read_pparquet('{f}')")[0][0]
    r = rows(
        con,
        f"SELECT min(file_row_number), max(file_row_number), count(DISTINCT file_row_number) "
        f"FROM read_pparquet('{f}', file_row_number := true)",
    )
    assert r == [(0, total - 1, total)]
    # only the extra columns, no file column
    assert rows(con, f"SELECT max(file_row_number) FROM read_pparquet('{f}', file_row_number := true)") == [
        (total - 1,)
    ]
    assert rows(con, f"SELECT count(*) FROM read_pparquet('{f}', filename := false)") == [(total,)]


def copies(tmp_path, n, name="mixed"):
    src = (FILES / f"{name}.parquet").read_bytes()
    paths = []
    for i in range(n):
        p = tmp_path / f"part-{i:03d}.parquet"
        p.write_bytes(src)
        paths.append(p)
    return paths


def test_a_glob_over_many_files(con, tmp_path):
    paths = copies(tmp_path, 40)
    r = rows(con, f"SELECT count(*), count(DISTINCT filename) FROM read_pparquet('{tmp_path}/part-*.parquet', filename := true)")
    assert r == [(40 * 1500, 40)]
    r = rows(con, f"SELECT filename, count(*) FROM read_pparquet('{tmp_path}/part-00?.parquet', filename := true) GROUP BY ALL ORDER BY 1")
    assert r == [(str(p), 1500) for p in paths[:10]]
    one = rows(con, f"SELECT sum(price) FROM read_pparquet('{paths[0]}')")[0][0]
    many = rows(con, f"SELECT sum(price) FROM read_pparquet('{tmp_path}/*.parquet')")[0][0]
    assert many == pytest.approx(40 * one)


def test_a_list_of_files(con, tmp_path):
    a, b = copies(tmp_path, 2)
    r = rows(con, f"SELECT count(*) FROM read_pparquet(['{a}', '{b}', '{FILES}/mixed.parquet'])")
    assert r == [(4500,)]
    r = rows(con, f"SELECT count(*) FROM read_pparquet(['{tmp_path}/*.parquet'])")
    assert r == [(3000,)]


def test_union_by_name(con):
    a, b = FILES / "primitives_optional.parquet", FILES / "pages_v1.parquet"
    with pytest.raises(duckdb.Error, match="the files must have the same schema, or pass union_by_name := true"):
        con.execute(f"SELECT * FROM read_pparquet(['{a}', '{b}'])").fetchall()
    sql = f"SELECT * FROM read_pparquet(['{a}', '{b}'], union_by_name := true, filename := true)"
    cols = [c for c, _ in types_of(con, sql)]
    assert cols == ["i32", "i64", "f32", "f64", "i64_opt", "random", "tag", "filename"]
    r = rows(con, f"SELECT filename, count(i64), count(tag), count(i32) FROM ({sql}) GROUP BY 1 ORDER BY 1")
    assert r == [(str(b), 0, 2500, 2500), (str(a), 2400, 0, 2572)]


def test_union_by_name_type_conflicts(con, tmp_path):
    pq.write_table(pa.table({"x": pa.array([1], pa.int64())}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"x": pa.array([1], pa.int32())}), tmp_path / "b.parquet")
    with pytest.raises(duckdb.BinderException, match='column "x" is BIGINT in .*a.parquet but INTEGER in .*b.parquet'):
        con.execute(f"SELECT * FROM read_pparquet('{tmp_path}/*.parquet', union_by_name := true)")


# ---------------------------------------------------------------------------------------------
# parallelism and memory


def test_threads_read_row_groups_in_parallel_with_the_same_result(tmp_path):
    f = FILES / "multiple_row_groups.parquet"
    paths = copies(tmp_path, 8, "multiple_row_groups")
    results = {}
    for threads in (1, 4):
        c = connect()
        c.execute(f"SET threads = {threads}")
        results[threads] = c.sql(
            f"SELECT * FROM read_pparquet('{tmp_path}/*.parquet', filename := true, file_row_number := true) "
            "ORDER BY filename, file_row_number"
        ).to_arrow_table()
        c.close()
    assert results[1].equals(results[4])
    one = connect().execute(f"SELECT count(*) FROM read_pparquet('{f}')").fetchall()[0][0]
    assert results[1].num_rows == 8 * one


# ---------------------------------------------------------------------------------------------
# errors


@pytest.mark.parametrize(
    "arg, message",
    [
        ("'/no/such/file.parquet'", 'no such file: "/no/such/file.parquet"'),
        ("'/no/such/dir/*.parquet'", 'no files match "/no/such/dir/\\*.parquet"'),
        ("42", "expected a file path, a glob or a list of them, got Int\\(42\\)"),
        ("NULL", "the path is NULL"),
        ("[]", "no files given"),
        ("[1, 2]", "the list of files holds Int\\(1\\), not a path"),
    ],
)
def test_bad_paths_are_explained(con, arg, message):
    with pytest.raises(duckdb.BinderException, match=f"read_pparquet: {message}"):
        con.execute(f"SELECT * FROM read_pparquet({arg})")


def test_a_file_that_is_not_parquet(con, tmp_path):
    p = tmp_path / "x.parquet"
    p.write_text("hello")
    with pytest.raises(duckdb.BinderException, match=f"read_pparquet: cannot read {p} as Parquet: "):
        con.execute(f"SELECT * FROM read_pparquet('{p}')")


def test_a_file_that_breaks_during_the_scan(con, tmp_path):
    a, b = copies(tmp_path, 2)
    stmt = con.execute(f"SELECT count(*) FROM read_pparquet(['{a}', '{b}'])")
    assert stmt.fetchall() == [(3000,)]
    b.write_bytes(b"PAR1 truncated")
    with pytest.raises(duckdb.Error, match=f"read_pparquet: cannot read {b} as Parquet"):
        con.execute(f"SELECT count(*) FROM read_pparquet(['{a}', '{b}'])").fetchall()


def test_a_damaged_pcodec_page(con, tmp_path):
    data = bytearray((FILES / "primitives_required.parquet").read_bytes())
    # flip bytes in the middle of the first column chunk's data
    for i in range(200, 260):
        data[i] ^= 0x5A
    p = tmp_path / "damaged.parquet"
    p.write_bytes(bytes(data))
    with pytest.raises(duckdb.Error, match="read_pparquet: cannot read .*damaged.parquet"):
        con.execute(f"SELECT sum(i32), sum(i64), sum(f32), sum(f64) FROM read_pparquet('{p}')").fetchall()
    # the connection still works
    assert rows(con, "SELECT 1") == [(1,)]


def test_bad_option_values(con):
    with pytest.raises(duckdb.Error):
        con.execute(f"SELECT * FROM read_pparquet('{FILES}/mixed.parquet', filename := 'yes please')")
    with pytest.raises(duckdb.BinderException):
        con.execute(f"SELECT * FROM read_pparquet('{FILES}/mixed.parquet', no_such_option := true)")
