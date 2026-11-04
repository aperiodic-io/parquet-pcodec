"""Every conformance vector, read through read_pparquet, gives exactly the expected rows."""

import pyarrow.parquet as pq
import pytest

from canonical import lines
from conftest import EXPECTED, FILES

NAMES = sorted(p.stem for p in FILES.glob("*.parquet"))


def test_every_vector_is_covered():
    assert len(NAMES) == 13
    assert sorted(p.stem for p in EXPECTED.glob("*.jsonl")) == NAMES


@pytest.mark.parametrize("name", NAMES)
def test_canonical_rows_match_byte_for_byte(con, name):
    path = FILES / f"{name}.parquet"
    table = con.sql(
        f"SELECT * EXCLUDE (file_row_number) FROM read_pparquet('{path}', file_row_number := true) "
        "ORDER BY file_row_number"
    ).to_arrow_table()
    got = lines(table, pq.read_schema(path))
    want = (EXPECTED / f"{name}.jsonl").read_text(encoding="utf-8")
    assert got == want


@pytest.mark.parametrize("name", ["multiple_row_groups", "mixed", "pages_v1"])
def test_one_thread_reads_in_file_order(con, name):
    con.execute("SET threads = 1")
    path = FILES / f"{name}.parquet"
    table = con.sql(f"SELECT * FROM read_pparquet('{path}')").to_arrow_table()
    assert lines(table, pq.read_schema(path)) == (EXPECTED / f"{name}.jsonl").read_text(encoding="utf-8")
