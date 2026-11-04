# pparquet (Python)

Read and write Parquet files that use PCODEC, a Parquet compression codec for numeric columns
(`CompressionCodec` 20547, see `spec/FORMAT.md`). Any Parquet file reads, PCODEC or not.

```python
import pparquet as ppc

choice = ppc.write_table(table, "trades.parquet")      # {"price": "PCODEC", "sym": "ZSTD", ...}
table = ppc.read_table("trades.parquet", columns=["ts", "price"])
stream = ppc.read_batches("trades.parquet")              # __arrow_c_stream__, for any Arrow library
```

- `write_table(data, path, policy="auto", columns=None, level=8, base="zstd(3)", row_group_size=None)`:
  `data` is anything with `__arrow_c_stream__` or `__arrow_c_array__`; `policy` is `"auto"` (PCODEC or
  the base codec per column, whichever is smaller on the first batch), `"pcodec"` or `"base"`, and
  `columns` maps a column name or dotted leaf path to its own policy.
- `Writer(path, schema, ...)`: a streaming context manager with the same options.
- `ParquetFile(path)`: `.schema`, `.metadata`, `.iter_batches()`, `.read()`.
- `transcode`, `inspect`, `verify`; `pparquet.codec` for the page payload alone.
- `pparquet.pandas`: `read_parquet`, `to_parquet` (index and dtypes as with pyarrow).
- `pparquet.polars`: `scan_parquet` (projection, limit, predicate and row-group pruning
  pushed down), `read_parquet`, `write_parquet`.
- `pparquet.duckdb`: `register`, `sql_view`, `read_parquet`.

Paths are `str`, `bytes` or `os.PathLike`, or binary file objects. Errors are `PcodecError` (a
`ValueError`) or the `OSError` of the failed I/O. Readers without PCODEC support (pyarrow, Polars,
DuckDB) fail on PCODEC files instead of returning wrong data; `transcode(..., target="standard")`
writes a copy they read.

The core needs no Python dependency; `read_table` and `ParquetFile.read` return pyarrow objects.

## Development

```
uv sync --python 3.14
uv run --no-sync maturin develop --uv
uv run --no-sync pytest                    # -m "not slow" skips the acceptance tests
uv run --no-sync ruff check src tests && uv run --no-sync mypy --strict
HYPOTHESIS_PROFILE=mutation PPARQUET_TESTDATA=$PWD/../testdata uv run --no-sync python scripts/mutmut_check.py
PPARQUET_PYTHON=$PWD/.venv/bin/python PYO3_PYTHON=$PWD/.venv/bin/python cargo mutants -p pparquet-python
```
