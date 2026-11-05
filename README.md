# tparquet

**Parquet for time series.** [pcodec](https://github.com/pcodec/pcodec) as a Parquet compression codec (`CompressionCodec`
20547), with readers and writers for Rust, Go, Python, pandas, Polars, DuckDB, Java, Spark, Trino,
DataFusion, C and C++.

Numeric columns (`INT32`, `INT64`, `FLOAT`, `DOUBLE`) are compressed with pcodec. Floats that are
short decimals, such as prices and sizes, are stored as exact integers. Each column gets PCODEC or
the base codec (ZSTD by default), whichever is smaller on the first batch. On synthetic trades the
files are 0.27–0.31× the size of pyarrow's ZSTD level 9, bit for bit lossless.

**A PCODEC file is not standard Parquet.** Readers without this codec refuse it with an error
instead of returning wrong data. The acceptance tests check this for pyarrow, pandas, Polars,
DuckDB's `read_parquet` and the upstream arrow-rs `parquet` crate. `tparquet decode` (or `transcode(..., target="standard")`) writes a standard copy.

The format is specified in [`spec/FORMAT.md`](spec/FORMAT.md).

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
| Java, anything on parquet-java | [`java/`](java) | a small fork of parquet-java 1.17.0 (`parquet-format-structures`, `parquet-common`, `parquet-hadoop`; [`FORK.md`](java/parquet-java/FORK.md)), the codec over JNI ([`tparquet-jni`](java/tparquet-jni), [`rust/jni`](rust/jni)), and [`tparquet`](java/tparquet): `TparquetWriter`, `TparquetReader`, canonical rows |
| Spark | [`java/tparquet-spark`](java/tparquet-spark) | Spark 4.3 reads PCODEC through the parquet-java fork (every reader); the `tparquet` data source writes it ([Java README](java/README.md#spark)) |
| Trino | [`trino/`](trino) | Trino 484 reads tparquet through its Hive, Iceberg, Delta Lake and Hudi connectors: [`install-tparquet.sh`](trino/install-tparquet.sh) puts forks of trino-parquet 484 ([`FORK.md`](trino/trino-parquet/FORK.md)) and parquet-java 1.18.1 ([`FORK.md`](java/parquet-java-1.18/FORK.md)) into a Trino install |
| C++ | [`cpp/`](cpp) | a small fork of Apache Arrow C++ 26.0.1 (a patch over the release, [`FORK.md`](cpp/arrow/FORK.md)) and `tparquet::ReadTable`/`WriteTable`/`FileWriter`; CMake package `tparquet` |
| C | [`rust/cread`](rust/cread) | reader as an Arrow C stream ([`tparquet_read.h`](rust/cread/include/tparquet_read.h)), `libtparquet_read` |
| Polars | [`polars-tparquet/`](polars-tparquet) | Polars plugin `polars-tparquet`: `scan_tparquet` over files, lists, globs and directories (lazy, with pushdown), `read_tparquet`, `df.tparquet.write(...)`, `lf.tparquet.sink(...)` (streamed) |

## Quick start

```sh
tparquet encode trades.parquet trades.pcodec.parquet       # standard -> PCODEC (Auto)
tparquet inspect trades.pcodec.parquet                     # codecs, sizes, payload modes
tparquet verify trades.parquet trades.pcodec.parquet       # same data, floats bit for bit
tparquet decode trades.pcodec.parquet trades.standard.parquet
```

```python
import tparquet as ppc

ppc.write_table(table, "trades.parquet")          # {"price": "PCODEC", "sym": "ZSTD", ...}
table = ppc.read_table("trades.parquet", columns=["ts", "price"])
```

```python
import polars_tparquet as pt

lf = pt.scan_tparquet("trades/**/*.parquet")      # lazy; pushdown into every file
lf.filter(pl.col("qty") > 0).tparquet.sink("out.parquet")
```

```go
import tparquet "github.com/aperiodic-io/tparquet/go" // reading: the import is enough

w, err := tparquet.NewFileWriter(schema, out) // Auto, base codec ZSTD(3)
```

```sql
LOAD 'tparquet.duckdb_extension';
SELECT * FROM read_tparquet('trades/*.parquet');
```

```java
// with the parquet-java fork, any parquet-java reader (and Spark) reads PCODEC files
try (ParquetWriter<Group> w = TparquetWriter.groups(path, schema, TparquetWriter.Options.defaults())) { ... }
```

```scala
df.write.format("tparquet").save("trades")       // Spark: PCODEC, V2 pages, PLAIN numbers
spark.read.parquet("trades")
```

```cpp
ARROW_ASSIGN_OR_RAISE(auto table, tparquet::ReadTable("trades.parquet", {"ts", "price"}));
```

Each package's README has the details: [Python](python/README.md),
[Polars](polars-tparquet/README.md), [Go](go/README.md), [DuckDB](duckdb/README.md),
[Java and Spark](java/README.md), [Trino](trino/README.md), [C and C++](cpp/README.md).

## Testing

- **Conformance vectors:** [`testdata/`](testdata/README.md) holds Parquet files with their
  expected rows in canonical JSON, and page payloads with their decoded bytes. Every
  implementation reads them.
- **Cross-language acceptance:** [`acceptance/cross/run.sh`](acceptance/cross/run.sh) has Rust, Go,
  Python, DuckDB, Java and C++ each read the files all of them wrote.
- **Forks:** every fork (arrow-rs `parquet`, parquet-java 1.17 and 1.18, trino-parquet, Arrow
  C++) is checked against its
  verified upstream release plus its `FORK.patch`; upstream's own suites run against the forks
  where they can, and unmodified upstream readers are shown to refuse PCODEC files.
- **Mutation testing** of every package: cargo-mutants (Rust, including the fork's changed
  functions), gremlins (Go), mutmut (Python), PIT (Java) and a scripted mutation check of the
  Arrow C++ fork. Fuzz targets are in `rust/core/fuzz` and `cpp/fuzz`.

```sh
make check            # Rust: fmt, clippy, tests, C smoke test, fork check, vectors
make -C go test       # Go
make python-test      # Python (uv, Python 3.14)
make polars-test      # the Polars plugin
make -C duckdb sql    # DuckDB extension
make java-test        # Java, the parquet-java fork, Spark (JDK 21, Maven)
make cpp-test         # C++ and the Arrow C++ fork (asan, fuzz-smoke, mutants targets in cpp/)
make trino-test       # Trino: the trino-parquet fork, conformance, queries through the Hive connector
make acceptance       # cross-language (needs the Python package, the DuckDB extension, the Java
                      # packages and the C++ library built)
```

CI runs all of it on every pull request (`.github/workflows/`). See [`AGENTS.md`](AGENTS.md) for
the rules of the repository.

## License

Apache License 2.0 ([`LICENSE`](LICENSE)). `rust/parquet` is a modified copy of the Apache Arrow
`parquet` crate, `java/parquet-java` of Apache Parquet Java modules, and `cpp/arrow` a patch to
Apache Arrow C++; see [`NOTICE`](NOTICE).
