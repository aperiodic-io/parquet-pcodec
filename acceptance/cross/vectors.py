"""Cross-language acceptance: vectors written by one implementation, read by the others.

A vector directory holds `files/<name>.parquet` and `expected/<name>.jsonl`, the canonical
rows of `testdata/README.md`. Rust (`testdata/`, `gen-testdata`) and Go (`pparquet-go-gen`)
write their own; this script writes the Python and DuckDB ones and holds the Python and
DuckDB readers. `run.sh` puts them together.

    vectors.py write-python OUT SRC...       transcode every SRC/files/*.parquet to PCODEC
    vectors.py write-duckdb EXT OUT SRC...   rewrite every SRC vector with COPY (FORMAT pparquet)
    vectors.py check-python DIR              read DIR with pparquet
    vectors.py check-duckdb EXT DIR          read DIR with read_pparquet

Every check prints `<n> vectors match`, or names the first differing line and exits 1.
"""

from __future__ import annotations

import pathlib
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator

import pyarrow.parquet as pq

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "duckdb" / "tests"))

# DuckDB's built-in Parquet writer, the oracle of the DuckDB vectors, writes every NaN as the
# canonical quiet NaN; these vectors hold other NaN payloads, which the extension keeps
DUCKDB_ORACLE_LOSES_NAN_PAYLOADS = {"float_specials", "go_special_floats"}


class Mismatch(Exception):
    pass


def vectors(src: pathlib.Path) -> Iterator[tuple[str, pathlib.Path, pathlib.Path]]:
    """(name, file, expected) of every vector in `src`; a file without expected rows fails."""
    files = sorted((src / "files").glob("*.parquet"))
    if not files:
        raise Mismatch(f"no files/*.parquet in {src}")
    for f in files:
        expected = src / "expected" / f"{f.stem}.jsonl"
        if not expected.is_file():
            raise Mismatch(f"{f.stem}: no expected/{f.stem}.jsonl")
        yield f.stem, f, expected


def compare(name: str, got: str, want: str) -> None:
    a, b = got.splitlines(), want.splitlines()
    for i in range(max(len(a), len(b))):
        if a[i : i + 1] != b[i : i + 1]:
            raise Mismatch(f"{name}: line {i + 1} differs from expected/{name}.jsonl")


def check(src: pathlib.Path, read: Callable[[pathlib.Path], str]) -> int:
    n = 0
    for name, f, expected in vectors(src):
        compare(name, read(f), expected.read_text(encoding="utf-8"))
        n += 1
    return n


def joined(lines: Iterator[str]) -> str:
    return "".join(line + "\n" for line in lines)


def python_reader() -> Callable[[pathlib.Path], str]:
    import pparquet as ppc
    from pparquet import canonical

    return lambda f: joined(canonical.lines(ppc.read_table(f)))


def duckdb_connect(extension: str):
    import duckdb

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{extension}'")
    return con


def scan(path: pathlib.Path) -> str:
    return (
        f"SELECT * EXCLUDE (file_row_number) FROM read_pparquet('{path}', file_row_number := true) "
        "ORDER BY file_row_number"
    )


def duckdb_reader(extension: str) -> Callable[[pathlib.Path], str]:
    from canonical import lines  # duckdb/tests/canonical.py

    con = duckdb_connect(extension)
    # canonical.lines gives the whole text, one row per line
    return lambda f: lines(con.sql(scan(f)).to_arrow_table(), pq.read_schema(f))


def fresh(out: pathlib.Path) -> None:
    for sub in ("files", "expected"):
        shutil.rmtree(out / sub, ignore_errors=True)
        (out / sub).mkdir(parents=True)


def write_python(out: pathlib.Path, sources: list[pathlib.Path]) -> int:
    """Every vector transcoded by the Python package with the Pcodec policy; the rows stay
    those of the source, so its expected rows are copied."""
    import pparquet as ppc

    fresh(out)
    n = 0
    for src in sources:
        for name, f, expected in vectors(src):
            ppc.transcode(f, out / "files" / f"{name}.parquet", target="pcodec", policy="pcodec")
            shutil.copy(expected, out / "expected" / f"{name}.jsonl")
            n += 1
    return n


def write_duckdb(extension: str, out: pathlib.Path, sources: list[pathlib.Path]) -> int:
    """Every vector read and written back by the extension. The expected rows are those of
    the same query written by DuckDB's built-in Parquet writer, read by pyarrow: no PCODEC
    code is involved in them."""
    from pparquet import canonical

    con = duckdb_connect(extension)
    fresh(out)
    n = 0
    with tempfile.TemporaryDirectory() as tmp:
        for src in sources:
            for name, f, _ in vectors(src):
                if name in DUCKDB_ORACLE_LOSES_NAN_PAYLOADS:
                    continue
                q = scan(f)
                cols = ", ".join("'" + c.replace("'", "''") + "'" for c in con.sql(q).columns)
                con.execute(
                    f"COPY ({q}) TO '{out / 'files' / f'{name}.parquet'}' "
                    f"(FORMAT pparquet, COLUMN_NAMES [{cols}])"
                )
                standard = pathlib.Path(tmp) / f"{name}.parquet"
                con.execute(f"COPY ({q}) TO '{standard}' (FORMAT parquet)")
                rows = joined(canonical.lines(pq.read_table(standard)))
                (out / "expected" / f"{name}.jsonl").write_text(rows, encoding="utf-8")
                n += 1
    return n


def main(argv: list[str]) -> int:
    try:
        match argv:
            case ["write-python", out, *srcs] if srcs:
                print(f"{write_python(pathlib.Path(out), [pathlib.Path(s) for s in srcs])} vectors written")
            case ["write-duckdb", ext, out, *srcs] if srcs:
                n = write_duckdb(ext, pathlib.Path(out), [pathlib.Path(s) for s in srcs])
                print(f"{n} vectors written")
            case ["check-python", src]:
                print(f"{check(pathlib.Path(src), python_reader())} vectors match")
            case ["check-duckdb", ext, src]:
                print(f"{check(pathlib.Path(src), duckdb_reader(ext))} vectors match")
            case _:
                print(__doc__, file=sys.stderr)
                return 2
    except Mismatch as e:
        print(f"vectors.py: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
