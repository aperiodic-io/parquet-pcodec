"""COPY ... TO ... (FORMAT pcodec_parquet) and COPY ... FROM, read back and checked by the Rust CLI."""

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from canonical import lines
from conftest import FILES, run_cli


def rows(con, sql):
    return con.execute(sql).fetchall()


def names_of(con, sql):
    return [r[0] for r in con.execute(f"DESCRIBE {sql}").fetchall()]


def copy(con, query, path, **options):
    names = names_of(con, query)
    opts = ", ".join(f"{k} {v}" for k, v in options.items())
    cols = ", ".join(f"'{n}'" for n in names)
    con.execute(f"COPY ({query}) TO '{path}' (FORMAT pcodec_parquet, COLUMN_NAMES [{cols}]{', ' + opts if opts else ''})")
    return path


def inspect(path):
    out = run_cli("inspect", "--json", path)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def codecs(path):
    """Leaf path -> set of codecs, from `parquet-pcodec inspect --json`."""
    info = inspect(path)
    return {c["path"]: set(c["codecs"]) for c in info["columns"]}


ALL_TYPES = """
SELECT
    i::TINYINT % 100 AS i8, (i * 300)::SMALLINT AS i16, (i * 70000)::INTEGER AS i32, i * 10000000000 AS i64,
    (i % 200)::UTINYINT AS u8, (i * 60)::USMALLINT AS u16, (i * 4000000)::UINTEGER AS u32,
    (i::UBIGINT * 1000000000000000) AS u64,
    (i * 0.5)::FLOAT AS f32, i * 0.01 AS f64,
    (i * 0.25)::DECIMAL(4,2) AS dec4, (i * 1.5)::DECIMAL(9,2) AS dec9,
    (i * 12345.6789)::DECIMAL(18,4) AS dec18, (i * 1e20)::DECIMAL(38,2) AS dec38,
    'row ' || i || ' ünïcode' AS s, ('\\x' || lpad(to_hex(i), 2, '0'))::BLOB AS bin,
    i % 3 = 0 AS b,
    DATE '2025-01-01' + i::INTEGER AS d, TIME '12:00:00' + INTERVAL (i) SECOND AS t,
    '12:00:00.123456789'::TIME_NS AS tns,
    (TIMESTAMP '2025-01-01' + INTERVAL (i) SECOND)::TIMESTAMP_S AS ts_s,
    (TIMESTAMP '2025-01-01' + INTERVAL (i * 7) MILLISECOND)::TIMESTAMP_MS AS ts_ms,
    TIMESTAMP '2025-01-01' + INTERVAL (i * 7) MICROSECOND AS ts_us,
    ('2025-01-01 00:00:00.000000001'::TIMESTAMP_NS + INTERVAL (i) SECOND) AS ts_ns,
    TIMESTAMPTZ '2025-01-01 00:00:00+00' + INTERVAL (i) MINUTE AS tstz,
    '2025-01-01 00:00:00.123456789+00'::TIMESTAMPTZ_NS AS tstz_ns,
    (['a', 'b', 'c'])[1 + i % 3]::ENUM('a', 'b', 'c') AS e,
    [i, i + 1, NULL] AS li, [] :: VARCHAR[] AS empty_list,
    {'x': i * 1.5, 'tags': [i::INTEGER], 'inner': {'y': 'v' || i}} AS st,
    MAP {'k' || i: i::INTEGER} AS m,
    [i::INTEGER, -i::INTEGER]::INTEGER[2] AS arr
FROM range(0, 50) r(i)
"""


def with_nulls(query):
    """The same columns, with every column NULL in every fifth row."""
    cols = names_of(duckdb.connect(), query)
    picks = ", ".join(f"CASE WHEN rn % 5 = 4 THEN NULL ELSE {c} END AS {c}" for c in cols)
    return f"SELECT {picks} FROM (SELECT *, row_number() OVER () - 1 AS rn FROM ({query}))"


def test_every_writable_type_round_trips(con, tmp_path):
    con.execute(f"CREATE TABLE src AS {with_nulls(ALL_TYPES)}")
    path = copy(con, "SELECT * FROM src", tmp_path / "all.parquet")
    back = f"read_pcodec_parquet('{path}')"
    # seconds come back as milliseconds and enums as strings; every other type is unchanged
    src_types = {r[0]: r[1] for r in con.execute("DESCRIBE src").fetchall()}
    back_types = {r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM {back}").fetchall()}
    changed = {c: (src_types[c], back_types[c]) for c in src_types if src_types[c] != back_types[c]}
    assert changed == {"ts_s": ("TIMESTAMP_S", "TIMESTAMP_MS"), "e": ("ENUM('a', 'b', 'c')", "VARCHAR")}
    for a, b in [("src", back), (back, "src")]:
        diff = rows(con, f"SELECT count(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b})")
        assert diff == [(0,)], (a, b)
    assert rows(con, f"SELECT count(*), count(i64) FROM {back}") == [(50, 40)]


def test_floats_are_written_bit_for_bit(con, tmp_path):
    src = FILES / "float_specials.parquet"
    path = copy(con, f"SELECT * FROM read_pcodec_parquet('{src}')", tmp_path / "f.parquet")
    schema = pq.read_schema(src)
    a = lines(con.sql(f"SELECT * FROM read_pcodec_parquet('{src}')").to_arrow_table(), schema)
    b = lines(con.sql(f"SELECT * FROM read_pcodec_parquet('{path}')").to_arrow_table(), schema)
    assert a == b
    assert a == (FILES.parent / "expected" / "float_specials.jsonl").read_text()


def test_auto_picks_pcodec_for_numeric_columns_and_keeps_strings_on_zstd(con, tmp_path):
    q = (
        "SELECT i AS id, 1735689600000 + i * 37 AS ts, (100 + (i % 97) * 0.01)::DOUBLE AS price, "
        "(i * 0.25)::DECIMAL(18, 2) AS amount, 'sym' || (i % 4) AS sym FROM range(300000) r(i)"
    )
    path = copy(con, q, tmp_path / "auto.parquet", POLICY="'auto'", LEVEL=8)
    c = codecs(path)
    assert c == {"id": {"PCODEC"}, "ts": {"PCODEC"}, "price": {"PCODEC"}, "amount": {"PCODEC"}, "sym": {"ZSTD"}}
    assert inspect(path)["pcodec_version"] == "1"
    assert rows(con, f"SELECT count(*), sum(id), max(price) FROM read_pcodec_parquet('{path}')") == rows(
        con, f"SELECT count(*), sum(id), max(price) FROM ({q})"
    )


def test_order_is_kept_across_vectors_and_row_groups(con, tmp_path):
    q = "SELECT i AS id FROM range(1000000) r(i)"
    path = copy(con, q, tmp_path / "order.parquet", ROW_GROUP_ROWS=100000)
    assert inspect(path)["row_groups"] == 10
    r = rows(
        con,
        f"SELECT count(*) FILTER (WHERE id <> file_row_number) FROM read_pcodec_parquet('{path}', file_row_number := true)",
    )
    assert r == [(0,)]


def test_policies_levels_and_base_codecs(con, tmp_path):
    q = "SELECT i AS a, i * 2 AS b, i::VARCHAR AS c FROM range(10000) r(i)"
    c = codecs(copy(con, q, tmp_path / "base.parquet", POLICY="'base'"))
    assert c == {"a": {"ZSTD"}, "b": {"ZSTD"}, "c": {"ZSTD"}}
    c = codecs(copy(con, q, tmp_path / "pc.parquet", POLICY="'pcodec'", LEVEL=0))
    assert c == {"a": {"PCODEC"}, "b": {"PCODEC"}, "c": {"ZSTD"}}
    c = codecs(copy(con, q, tmp_path / "col.parquet", POLICY="'base'", COLUMN_POLICY="['b=pcodec']"))
    assert c == {"a": {"ZSTD"}, "b": {"PCODEC"}, "c": {"ZSTD"}}
    c = codecs(copy(con, q, tmp_path / "snappy.parquet", POLICY="'base'", COMPRESSION="'snappy'"))
    assert c == {"a": {"SNAPPY"}, "b": {"SNAPPY"}, "c": {"SNAPPY"}}
    c = codecs(copy(con, q, tmp_path / "z9.parquet", POLICY="'pcodec'", COMPRESSION="'zstd'", COMPRESSION_LEVEL=9))
    assert c["c"] == {"ZSTD"}
    for name in ["base", "pc", "col", "snappy", "z9"]:
        assert rows(con, f"SELECT sum(a), sum(b), count(c) FROM read_pcodec_parquet('{tmp_path}/{name}.parquet')") == [
            (49995000, 99990000, 10000)
        ]


def test_an_empty_result_writes_an_empty_file(con, tmp_path):
    path = copy(con, "SELECT 1::BIGINT AS a, 'x' AS b WHERE false", tmp_path / "empty.parquet")
    assert rows(con, f"SELECT count(*) FROM read_pcodec_parquet('{path}')") == [(0,)]
    assert names_of(con, f"SELECT * FROM read_pcodec_parquet('{path}')") == ["a", "b"]


@pytest.mark.parametrize(
    "options, message",
    [
        ("", "FORMAT pcodec_parquet needs COLUMN_NAMES"),
        (", COLUMN_NAMES ['a']", "COLUMN_NAMES has 1 names but the query has 2 columns"),
        (", COLUMN_NAMES ['a', 'a']", 'COLUMN_NAMES: "a" appears twice'),
        (", COLUMN_NAMES ['a', 'b'], POLICY 'fast'", 'unknown policy "fast"'),
        (", COLUMN_NAMES ['a', 'b'], LEVEL 13", "invalid LEVEL 13: expected 0 to 12"),
        (", COLUMN_NAMES ['a', 'b'], COMPRESION 'zstd'", "unknown option COMPRESION for FORMAT pcodec_parquet"),
        (", COLUMN_NAMES ['a', 'b'], COMPRESSION 'lzo'", 'unknown COMPRESSION "lzo"'),
    ],
)
def test_bad_options_are_explained(con, tmp_path, options, message):
    with pytest.raises(duckdb.Error, match=message):
        con.execute(f"COPY (SELECT 1 AS a, 2 AS b) TO '{tmp_path}/x.parquet' (FORMAT pcodec_parquet{options})")


@pytest.mark.parametrize(
    "expr, name",
    [
        ("uuid()", "UUID"),
        ("1::HUGEINT", "HUGEINT"),
        ("1::UHUGEINT", "UHUGEINT"),
        ("INTERVAL 1 DAY", "INTERVAL"),
        ("[INTERVAL 1 DAY]", "INTERVAL"),
        ("union_value(k := 1)", "UNION"),
        ("'101'::BIT", "BIT"),
        ("'12:00:00+01'::TIMETZ", "TIME WITH TIME ZONE"),
        ("1::BIGNUM", "BIGNUM"),
        ("1::VARIANT", "VARIANT"),
        ("'POINT(1 2)'::GEOMETRY", "GEOMETRY"),
        ("NULL", "NULL"),
    ],
)
def test_types_it_cannot_write_are_refused(con, tmp_path, expr, name):
    with pytest.raises(duckdb.Error, match=f'column "v": columns of type {name} cannot be written'):
        con.execute(f"COPY (SELECT {expr} AS v) TO '{tmp_path}/x.parquet' (FORMAT pcodec_parquet, COLUMN_NAMES ['v'])")


def test_an_unwritable_path(con):
    with pytest.raises(duckdb.Error, match="cannot create /no/such/dir/x.parquet"):
        con.execute("COPY (SELECT 1 AS a) TO '/no/such/dir/x.parquet' (FORMAT pcodec_parquet, COLUMN_NAMES ['a'])")


def test_copy_from_into_a_table(con, tmp_path):
    src = FILES / "mixed.parquet"
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_pcodec_parquet('{src}') LIMIT 0")
    con.execute(f"COPY t FROM '{src}' (FORMAT pcodec_parquet)")
    assert rows(con, "SELECT count(*) FROM t") == [(1500,)]
    con.execute(f"COPY t FROM '{src}' (FORMAT pcodec_parquet)")
    assert rows(con, "SELECT count(*) FROM t") == [(3000,)]
    con.execute("CREATE TABLE narrow (ts TIMESTAMPTZ, price DOUBLE)")
    with pytest.raises(duckdb.Error, match="do not match the table columns"):
        con.execute(f"COPY narrow FROM '{src}' (FORMAT pcodec_parquet)")


def test_duckdb_write_then_rust_verify(con, tmp_path):
    """A file DuckDB writes with the extension holds what DuckDB's own Parquet writer writes."""
    q = with_nulls(
        "SELECT i AS id, 100 + (i % 977) * 0.01 AS price, ((i % 50) + 1) * 0.001 AS qty, "
        "(TIMESTAMP '2025-01-01' + INTERVAL (i * 13) MILLISECOND)::TIMESTAMP_MS AS ts, "
        "'s' || (i % 7) AS sym, i % 2 = 0 AS buy, [i, i * 2] AS li, {'a': i::INTEGER, 'b': 'x' || i} AS st, "
        "(i * 0.5)::DECIMAL(10, 2) AS dec, DATE '2025-01-01' + (i % 365)::INTEGER AS d "
        "FROM range(200000) r(i)"
    )
    pcodec = copy(con, q, tmp_path / "pcodec.parquet")
    standard = tmp_path / "standard.parquet"
    con.execute(f"COPY ({q}) TO '{standard}' (FORMAT parquet)")
    out = run_cli("verify", standard, pcodec)
    assert out.returncode == 0, out.stdout + out.stderr
    # and verify notices a difference
    other = copy(con, q.replace("i * 2]", "i * 3]"), tmp_path / "other.parquet")
    assert run_cli("verify", standard, other).returncode == 1


@pytest.mark.parametrize("name", sorted(p.stem for p in FILES.glob("*.parquet")))
def test_vectors_rewritten_by_duckdb_verify_against_duckdbs_own_writer(con, tmp_path, name):
    """Every vector, read and written back by the extension, holds what DuckDB's built-in
    Parquet writer writes for the same rows (`parquet-pcodec verify`), and reads back to the
    vector's canonical rows."""
    src = FILES / f"{name}.parquet"
    q = f"SELECT * EXCLUDE (file_row_number) FROM read_pcodec_parquet('{src}', file_row_number := true) ORDER BY file_row_number"
    out = copy(con, q, tmp_path / f"{name}.parquet")
    standard = tmp_path / f"{name}.standard.parquet"
    con.execute(f"COPY ({q}) TO '{standard}' (FORMAT parquet)")
    r = run_cli("verify", standard, out)
    if name == "float_specials":
        # DuckDB's own writer does not keep NaN payloads: the extension's file has the vector's
        # bits (checked below), DuckDB's has a canonical NaN in their place
        assert r.returncode == 1 and '"f64_opt":"0x7ff0000000000001"' in r.stdout, r.stdout + r.stderr
    else:
        assert r.returncode == 0, r.stdout + r.stderr
    back = con.sql(f"SELECT * FROM read_pcodec_parquet('{out}')").to_arrow_table()
    assert lines(back, pq.read_schema(src)) == (FILES.parent / "expected" / f"{name}.jsonl").read_text()
