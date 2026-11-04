# pparquet

[pcodec](https://github.com/pcodec/pcodec) as a Parquet compression codec (`CompressionCodec`
20547), with readers and writers for Rust, Go, Python, pandas, Polars and DuckDB.

Numeric columns (`INT32`, `INT64`, `FLOAT`, `DOUBLE`) are compressed with pcodec. Floats that are
short decimals, such as prices and sizes, are stored as exact integers. Each column gets PCODEC or
the base codec (ZSTD by default), whichever is smaller on the first batch. On synthetic trades the
files are 0.27–0.31× the size of pyarrow's ZSTD level 9, bit for bit lossless.

**A PCODEC file is not standard Parquet.** Readers without this codec refuse it with an error
instead of returning wrong data. The acceptance tests check this for pyarrow, pandas, Polars,
DuckDB's `read_parquet` and the upstream arrow-rs `parquet` crate. `pparquet decode` (or `transcode(..., target="standard")`) writes a standard copy.

The format is specified in [`spec/FORMAT.md`](spec/FORMAT.md).

## Packages

| Where | Package | Notes |
|---|---|---|
| Rust | [`rust/pparquet`](rust/pparquet) | writer with automatic codec choice, `transcode`, `inspect`, `verify`, and the `pparquet` CLI |
| Rust, DataFusion, anything on arrow-rs | [`rust/parquet`](rust/parquet) | a small fork of the arrow-rs `parquet` crate 60.1.0 that adds the codec; changes listed in [`FORK.md`](rust/parquet/FORK.md). Use it with `[patch.crates-io] parquet = { path = ... }` |
| Rust, no Parquet | [`rust/core`](rust/core) | the page payload codec alone |
| C | [`rust/capi`](rust/capi) | C ABI over the core ([`pparquet.h`](rust/capi/include/pparquet.h)) |
| Go | [`go/`](go) | arrow-go codec plugin (no fork: `compress.RegisterCodec`) and writer helpers |
| Python, pandas, Polars | [`python/`](python) | pyo3 package: `read_table`, `write_table`, `Writer`; `pparquet.pandas`, `pparquet.polars` (`scan_parquet` with pushdown), `pparquet.duckdb` |
| DuckDB | [`duckdb/`](duckdb) | loadable extension: `read_pparquet(...)`, `COPY ... (FORMAT pparquet)` |

## Quick start

```sh
pparquet encode trades.parquet trades.pcodec.parquet       # standard -> PCODEC (Auto)
pparquet inspect trades.pcodec.parquet                     # codecs, sizes, payload modes
pparquet verify trades.parquet trades.pcodec.parquet       # same data, floats bit for bit
pparquet decode trades.pcodec.parquet trades.standard.parquet
```

```python
import pparquet as ppc

ppc.write_table(table, "trades.parquet")          # {"price": "PCODEC", "sym": "ZSTD", ...}
table = ppc.read_table("trades.parquet", columns=["ts", "price"])
```

```go
import pparquet "github.com/aperiodic-io/pparquet/go" // reading: the import is enough

w, err := pparquet.NewFileWriter(schema, out) // Auto, base codec ZSTD(3)
```

```sql
LOAD 'pparquet.duckdb_extension';
SELECT * FROM read_pparquet('trades/*.parquet');
```

Each package's README has the details: [Python](python/README.md), [Go](go/README.md),
[DuckDB](duckdb/README.md).

## Testing

- **Conformance vectors:** [`testdata/`](testdata/README.md) holds Parquet files with their
  expected rows in canonical JSON, and page payloads with their decoded bytes. Every
  implementation reads them.
- **Cross-language acceptance:** [`acceptance/cross/run.sh`](acceptance/cross/run.sh) has Rust, Go,
  Python and DuckDB each read the files the other three wrote.
- **Mutation testing** of every package: cargo-mutants (Rust, including the fork's changed
  functions), gremlins (Go) and mutmut (Python). Fuzz targets are in `rust/core/fuzz`.

```sh
make check            # Rust: fmt, clippy, tests, C smoke test, fork check, vectors
make -C go test       # Go
make python-test      # Python (uv, Python 3.14)
make -C duckdb sql    # DuckDB extension
make acceptance       # cross-language (needs the Python package and the DuckDB extension built)
```

CI runs all of it on every pull request (`.github/workflows/`). See [`AGENTS.md`](AGENTS.md) for
the rules of the repository.

## License

Apache License 2.0 ([`LICENSE`](LICENSE)). `rust/parquet` is a modified copy of the Apache Arrow
`parquet` crate; see [`NOTICE`](NOTICE).
