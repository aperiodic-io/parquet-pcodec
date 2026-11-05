# tparquet

[![Rust](https://github.com/aperiodic-io/tparquet/actions/workflows/rust.yml/badge.svg)](https://github.com/aperiodic-io/tparquet/actions/workflows/rust.yml)
[![Go](https://github.com/aperiodic-io/tparquet/actions/workflows/go.yml/badge.svg)](https://github.com/aperiodic-io/tparquet/actions/workflows/go.yml)
[![Python](https://github.com/aperiodic-io/tparquet/actions/workflows/python.yml/badge.svg)](https://github.com/aperiodic-io/tparquet/actions/workflows/python.yml)
[![DuckDB](https://github.com/aperiodic-io/tparquet/actions/workflows/duckdb.yml/badge.svg)](https://github.com/aperiodic-io/tparquet/actions/workflows/duckdb.yml)
[![Cross-language](https://github.com/aperiodic-io/tparquet/actions/workflows/acceptance.yml/badge.svg)](https://github.com/aperiodic-io/tparquet/actions/workflows/acceptance.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Parquet for time series.** Trade and tick files at about a third of their ZSTD size
(0.31× in our trades benchmark), lossless, and still Parquet: same schema, row groups, statistics
and page indexes. Read and write them from Python, pandas, Polars, DuckDB, Go and Rust.

tparquet adds one thing to Parquet: a compression codec, PCODEC, that runs
[pcodec](https://github.com/pcodec/pcodec) on numeric columns. Prices and sizes that are short
decimals are stored as exact integers. Everything else in the file stays standard Parquet.

## How much smaller

Deterministic synthetic market data from the acceptance test, against Parquet with ZSTD level 9:

| Data | ZSTD level 9 | tparquet | Size |
|---|---:|---:|---:|
| 50,000 trades: ms timestamps, prices on a 0.01 tick, amounts, side, trade ids | 361 KB | 110 KB | **0.31×** |
| 5,000 L2 book snapshots: lists of bid and ask prices and sizes | 195 KB | 209 KB | 1.07× |
| Both | 556 KB | 319 KB | 0.57× |

Every value comes back bit for bit. On the trades, timestamps, prices and trade ids get PCODEC;
amounts and the side flag keep ZSTD because ZSTD is smaller for them. On the book snapshots
pcodec does not win, so every column keeps ZSTD (at level 3, hence the small gap to level 9).
tparquet makes this choice per column, on the first batch.

If your files compress like these trades, 1 TB of ZSTD Parquet becomes about 310 GB, and a full
scan reads about a third of the bytes. To reproduce the table:

```sh
cargo test --release -p tparquet --test acceptance -- --nocapture
```

Synthetic data only goes so far. The quickest way to find out what tparquet does for your data
is to run it on one of your own files.

## Try it on your own file

```sh
cargo install --path rust/tparquet                          # the tparquet CLI

tparquet encode trades.parquet trades.pcodec.parquet        # standard -> PCODEC, codec chosen per column
tparquet inspect trades.pcodec.parquet                      # codec and size of every column
tparquet verify trades.parquet trades.pcodec.parquet        # same data, floats bit for bit
tparquet decode trades.pcodec.parquet trades.standard.parquet   # and back to standard Parquet
```

Nothing about your original file changes, and `decode` takes you back whenever you want.

## It's still Parquet

PCODEC is set per column chunk in `ColumnMetaData.codec`, exactly like `ZSTD` or `SNAPPY`.
Schema, row groups, encodings, statistics, page indexes, bloom filters and key-value metadata
are untouched, so your files keep their layout, their partitioning and their statistics.
Strings, booleans and any column pcodec doesn't help keep ZSTD.

The trade-off is that a reader without the codec cannot read the file. It fails with an error
and never returns wrong data: the acceptance tests check this for pyarrow, pandas, Polars,
DuckDB's `read_parquet` and the upstream arrow-rs `parquet` crate. When a file has to go to such
a reader, `tparquet decode` (or `transcode(..., target="standard")`) writes a standard copy
and `tparquet verify` confirms it holds the same values.

The codec is specified in [`spec/FORMAT.md`](spec/FORMAT.md), so anyone can implement it.

## Install

The packages are not on PyPI or crates.io yet. Build them from a checkout of this repository
(the Python and Go packages need a Rust toolchain):

```sh
cargo install --path rust/tparquet           # CLI
pip install ./python ./polars-tparquet       # Python, pandas, Polars, DuckDB (Python 3.10+)
make -C duckdb build                         # DuckDB extension: duckdb/build/tparquet.duckdb_extension
cargo build --release -p tparquet-capi       # static library for Go; see go/README.md
```

## Use it

Python, with pyarrow, pandas, Polars or DuckDB:

```python
import tparquet as tpq

tpq.write_table(table, "trades.parquet")          # {"price": "PCODEC", "sym": "ZSTD", ...}
table = tpq.read_table("trades.parquet", columns=["ts", "price"])
```

Polars, lazily over many files:

```python
import polars as pl
import polars_tparquet as pt

lf = pt.scan_tparquet("trades/**/*.parquet")      # projection and predicate pushdown into every file
lf.filter(pl.col("qty") > 0).tparquet.sink("out.parquet")
```

DuckDB:

```sql
LOAD 'tparquet.duckdb_extension';
SELECT symbol, avg(price) FROM read_tparquet('trades/*.parquet') GROUP BY ALL;
COPY (SELECT ts, price, qty FROM ticks) TO 'ticks.parquet' (FORMAT tparquet, COLUMN_NAMES ['ts', 'price', 'qty']);
```

Go, on arrow-go with no fork:

```go
import tparquet "github.com/aperiodic-io/tparquet/go" // reading: the import is enough

w, err := tparquet.NewFileWriter(schema, out) // codec chosen per column, base codec ZSTD(3)
```

Rust:

```rust
use tparquet::{Writer, WriterOptions};

let mut w = Writer::try_new(File::create("trades.parquet")?, schema, WriterOptions::default())?;
w.write(&batch)?;                                  // the first batch picks each column's codec
w.close()?;

let reader = tparquet::read::open("trades.parquet")?.build()?;   // Arrow record batches
```

Each package's README has the details: [Python](python/README.md),
[Polars](polars-tparquet/README.md), [Go](go/README.md), [DuckDB](duckdb/README.md).

## tparquet or Vortex?

[Vortex](https://github.com/vortex-data/vortex) is a new columnar file format built for fast
scans and random access on compressed data. tparquet is a smaller change: your files stay
Parquet and the numeric columns get smaller.

Use tparquet when:

- your data is mostly numeric time series (timestamps, prices, sizes, ids, sensor readings) and
  storage or transfer cost is what you are trying to cut;
- your pipelines, catalogs and partition layouts are built around Parquet and you don't want to
  migrate them to a new format;
- you write from Go, or from Python, Polars, DuckDB or Rust;
- you want a one-command way back to plain Parquet.

Use Vortex when scan speed and point lookups matter more than bytes on disk, when your tables
are mostly strings, or when you need its Spark, Trino or Java integrations. Its default
compressor [leaves out](https://vortex.dev/blog/btrblocks-compressor) heavyweight codecs such as
pcodec to keep random access. A tparquet page, like any compressed Parquet page, is decoded
whole.

Use plain ZSTD Parquet when the files must be read by tools you don't control.

## Why you can trust it

- The format is a short, versioned spec: [`spec/FORMAT.md`](spec/FORMAT.md).
- Every implementation reads the same conformance vectors in [`testdata/`](testdata/README.md):
  Parquet files with their expected rows in canonical JSON, and page payloads with their decoded
  bytes.
- Rust, Go, Python and DuckDB each read the files the other three wrote
  ([`acceptance/cross/run.sh`](acceptance/cross/run.sh)), on every pull request.
- Every package is mutation tested: cargo-mutants for Rust (including the fork's changed
  functions), gremlins for Go and mutmut for Python. A surviving mutant is a missing test.
- Decoders treat payloads as untrusted (no panics, no unbounded allocation) and are fuzzed
  ([`rust/core/fuzz`](rust/core/fuzz)).
- The arrow-rs fork is small: every change is listed in [`FORK.md`](rust/parquet/FORK.md) and
  checked against upstream in CI.

## Packages

| Where | Package | Notes |
|---|---|---|
| Rust | [`rust/tparquet`](rust/tparquet) | writer with automatic codec choice, `transcode`, `inspect`, `verify`, and the `tparquet` CLI |
| Rust, DataFusion, anything on arrow-rs | [`rust/parquet`](rust/parquet) | a small fork of the arrow-rs `parquet` crate 60.1.0 that adds the codec; changes listed in [`FORK.md`](rust/parquet/FORK.md). Use it with `[patch.crates-io] parquet = { path = ... }` |
| Rust, no Parquet | [`rust/core`](rust/core) | the page payload codec alone |
| C | [`rust/capi`](rust/capi) | C ABI over the core ([`tparquet.h`](rust/capi/include/tparquet.h)) |
| Go | [`go/`](go) | arrow-go codec plugin (no fork: `compress.RegisterCodec`) and writer helpers |
| Python, pandas, Polars | [`python/`](python) | pyo3 package: `read_table`, `write_table`, `Writer`; `tparquet.pandas`, `tparquet.polars` (`scan_parquet` with pushdown), `tparquet.duckdb` |
| DuckDB | [`duckdb/`](duckdb) | loadable extension: `read_tparquet(...)`, `COPY ... (FORMAT tparquet)` |
| Polars | [`polars-tparquet/`](polars-tparquet) | Polars plugin `polars-tparquet`: `scan_tparquet` over files, lists, globs and directories (lazy, with pushdown), `read_tparquet`, `df.tparquet.write(...)`, `lf.tparquet.sink(...)` (streamed) |

## Development

```sh
make check            # Rust: fmt, clippy, tests, C smoke test, fork check, vectors
make -C go test       # Go
make python-test      # Python (uv, Python 3.14)
make polars-test      # the Polars plugin
make -C duckdb sql    # DuckDB extension
make acceptance       # cross-language (needs the Python package and the DuckDB extension built)
```

CI runs all of it on every pull request (`.github/workflows/`). See [`AGENTS.md`](AGENTS.md) for
the rules of the repository.

## License

Apache License 2.0 ([`LICENSE`](LICENSE)). `rust/parquet` is a modified copy of the Apache Arrow
`parquet` crate; see [`NOTICE`](NOTICE).
