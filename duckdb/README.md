# `tparquet`: a DuckDB extension for Parquet files with the PCODEC codec

DuckDB's own Parquet reader cannot read files whose column chunks use `CompressionCodec`
20547 (`PCODEC`, [`spec/FORMAT.md`](../spec/FORMAT.md)): DuckDB 2.0.0 stops on the footer with

```
TProtocolException: Invalid data
```

(its Thrift parser rejects the unknown codec value; `tests/test_acceptance.py` pins this). This
extension adds a reader and a COPY format that handle PCODEC and standard Parquet alike:

```sql
LOAD 'build/tparquet.duckdb_extension';   -- needs allow_unsigned_extensions

SELECT * FROM read_tparquet('trades/*.parquet');
SELECT symbol, avg(price) FROM read_tparquet(['a.parquet', 'b.parquet'], filename := true) GROUP BY ALL;

COPY (SELECT ts, price, qty FROM ticks) TO 'ticks.parquet'
    (FORMAT tparquet, COLUMN_NAMES ['ts', 'price', 'qty'], POLICY 'auto', LEVEL 8);
COPY ticks FROM 'ticks.parquet' (FORMAT tparquet);
```

It is written in Rust on DuckDB's C extension API (v1, version `v1.5.6`, which DuckDB 2.0.0
serves) with the `duckdb` crate 1.20000.1 (`loadable-extension`, `vtab-arrow`), and the Parquet
fork in [`rust/parquet`](../rust/parquet) through [`rust/tparquet`](../rust/tparquet).
The `duckdb` crate's arrow (`^60`) unifies with the fork's arrow 60.1.0; `make check-arrow`
fails if a second arrow or parquet ever enters the build.

### What comes from the `duckdb` crate

- `duckdb::ffi` (libduckdb-sys' loadable-extension bindings): every C API call goes through
  the function-pointer table DuckDB hands over at `LOAD` (`duckdb_rs_extension_api_init`).
- `duckdb::vtab::arrow::write_arrow_array_to_vector` writes the scan's Arrow arrays into DuckDB
  vectors. `types::normalize` first reshapes the arrays it would not copy exactly: it rounds
  nanosecond times and time-zoned nanosecond timestamps to microseconds, and it does not take
  dictionaries, half floats or 256-bit decimals.
- Not used: the `VTab` trait and `register_table_function` (no local init state per thread, no
  raw registration, and no COPY functions at all), the `#[duckdb_entrypoint_c_api]` macro (it
  hands over a `Connection` without its raw handle, which raw registration needs; `src/lib.rs`
  has the same few lines), `LogicalTypeHandle` (it has no `TIMESTAMPTZ_NS`), and
  `data_chunk_to_arrow` for COPY (no decimals, enums, `TIME_NS` or `TIMESTAMPTZ_NS`):
  `src/chunk.rs` reads DuckDB's vector layout itself.

## Loading

[`INSTALL.md`](../INSTALL.md#duckdb-extension) has the steps for every platform and client.

```sh
make build                      # build/tparquet.duckdb_extension (release)
```

- Python: `duckdb.connect(config={"allow_unsigned_extensions": "true"})`, then
  `con.execute("LOAD 'build/tparquet.duckdb_extension'")`.
- CLI: `duckdb -unsigned`, then `LOAD 'build/tparquet.duckdb_extension';`.

Without `allow_unsigned_extensions` DuckDB refuses the file (it has no signature; see
[Distribution](#distribution)). The CLI path is not covered by the tests: the DuckDB 2.0.0 CLI
binary could not be downloaded in the environment the extension was developed in (no release
asset reachable, no `duckdb-cli` 2.0.0 wheel); the CLI loads extensions through the same engine
code as the Python package.

## Reading: `read_tparquet(path, ...)`

`path` is a file, a glob (`*`, `?`, `[...]`; matches sorted by name) or a list of files and
globs. Named parameters:

| parameter | default | effect |
|---|---|---|
| `filename` | false | adds a `filename` VARCHAR column |
| `file_row_number` | false | adds a `file_row_number` BIGINT column: the row's position in its file, from 0 |
| `union_by_name` | false | combines files by column name; a file without a column gives NULL. The same name must have the same DuckDB type in every file |

Without `union_by_name` every file must have the schema of the first file (the same column
names and DuckDB types, in order), or the scan stops with an error naming both files.

**Projection pushdown.** Only the columns the query uses are read (`EXPLAIN` shows
`Projections:`); `count(*)` reads no column at all. **Filters** are applied by DuckDB after the
scan: the C API does not hand table functions their filters, so there is no row-group skipping
on statistics.

**Parallelism.** The scan's unit of work is a row group. DuckDB's threads take row groups from
a shared queue that walks the files in order (opening each footer once); `max_threads` is the
number of row groups bind can see (row groups of the first file times the number of files).
With one thread rows come in file order; with several, row groups interleave, because C API
table functions cannot report batch indexes for DuckDB to restore the order: use
`ORDER BY filename, file_row_number` when order matters.

**Memory.** Each thread holds one row-group reader and decodes one batch of at most one DuckDB
vector (2048 rows) at a time; pages are read as needed. Reading 400 MB of values in the slow
test keeps the process's peak RSS where it was before the scan (97 MB).

**Files.** Local files only (`std::fs`); DuckDB's virtual file systems (`httpfs`, S3) are not
used.

### Types

The Parquet reader returns Arrow types (from the embedded Arrow schema when the file has one);
each maps to the DuckDB type that holds its values exactly:

| Parquet / Arrow | DuckDB |
|---|---|
| boolean | `BOOLEAN` |
| int8, int16, int32, int64 | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` |
| uint8, uint16, uint32, uint64 | `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT` |
| float16, float32 / float64 | `FLOAT` (half floats widened) / `DOUBLE` |
| decimal32/64/128/256 with at most 38 digits | `DECIMAL(p, s)` |
| string, large string, string view, dictionary of strings | `VARCHAR` |
| binary, large binary, binary view, fixed-size binary | `BLOB` |
| date32, date64 | `DATE` |
| time32 (s, ms), time64 (us) / time64 (ns) | `TIME` / `TIME_NS` |
| timestamp s, ms, us, ns without time zone | `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP`, `TIMESTAMP_NS` |
| timestamp s, ms, us with a time zone / ns with a time zone | `TIMESTAMP WITH TIME ZONE` / `TIMESTAMPTZ_NS` |
| interval, duration | `INTERVAL` |
| list, large list / fixed-size list | `LIST` / `ARRAY` |
| struct, map | `STRUCT`, `MAP` |

DuckDB stores time-zoned timestamps as UTC instants in microseconds (or nanoseconds for
`TIMESTAMPTZ_NS`); the column's own zone (`America/New_York`) is not kept, and DuckDB shows the
instant in its `TimeZone` setting. Decimals of more than 38 digits and Arrow `Null` columns are
refused with the column's name.

## Writing: `COPY ... TO ... (FORMAT tparquet, ...)`

DuckDB 2.0's C API has COPY functions in its stable API (`duckdb_create_copy_function`,
`duckdb_register_copy_function`, stable since C API v1.5.6), so the extension registers the
COPY format `tparquet` with a bind, global init, sink and finalize callback. The file is
written with `tparquet::Writer`, whose `Auto` policy (spec section 6) compares PCODEC and
the base codec per numeric column on the first batch; the writer holds back the first 128Ki rows
so that this comparison sees more than one DuckDB vector.

| option | default | |
|---|---|---|
| `COLUMN_NAMES` | required | the column names, in order: `COLUMN_NAMES ['ts', 'price']` |
| `POLICY` | `'auto'` | `'auto'`, `'pcodec'` or `'base'`, for every `INT32`/`INT64`/`FLOAT`/`DOUBLE` column |
| `LEVEL` | 8 | pcodec level, 0 to 12 |
| `COLUMN_POLICY` | | per column: `['price=pcodec', 'id=base']` |
| `COMPRESSION` | `'zstd'` | codec of the other columns: `zstd`, `snappy`, `gzip`, `brotli`, `lz4`, `uncompressed` |
| `COMPRESSION_LEVEL` | 3 for zstd | level of that codec |
| `ROW_GROUP_ROWS` | 1048576 | rows per row group |

**Why `COLUMN_NAMES`.** The C API gives a COPY function's bind the number and the *types* of the
columns (`duckdb_copy_function_bind_get_column_count/_type`) but not their names, although
DuckDB has them internally (`copy_function-c.cpp` keeps them in its bind info without a getter).
Rather than write `column0, column1, ...` silently, the COPY fails until the names are given.
The other alternatives are worse: the C API has no table in-out functions, and a function
taking a query string would run it on a second connection, which cannot see the caller's
temporary tables or open transaction (and, holding a connection, would keep the database open
forever). DuckDB's C API v2 has `duckdb_v2_copy_to_bind_get_column_name`, but no V2 function
is stable yet: a V2 extension loads only in the exact DuckDB version it was built for.

**`ROW_GROUP_ROWS`, not `ROW_GROUP_SIZE`.** DuckDB keeps `ROW_GROUP_SIZE` and
`ROW_GROUP_SIZE_BYTES` as options of its generic COPY machinery and does not pass them on.

**Order.** The C API sets no parallel execution mode for COPY functions, so DuckDB calls the
sink from one thread at a time, in the query's order; the conversion of DuckDB vectors to Arrow
runs in that thread.

**Types.** Every type of the reading table can be written except `INTERVAL`; in addition
`ENUM` is written as strings, `TIMESTAMP_S` as milliseconds (Parquet has no seconds unit),
`TIMESTAMPTZ` as `timestamp(us, "UTC")`, lists with Parquet's element name `element` and maps
with `key_value`, so the file reads the same with and without its Arrow schema. `UUID`,
`HUGEINT`, `UHUGEINT`, `INTERVAL`, `UNION`, `BIT`, `TIME WITH TIME ZONE`, `BIGNUM`,
`VARIANT` and `GEOMETRY` are refused with the column's name: cast them first.

`COPY t FROM 'f.parquet' (FORMAT tparquet)` reads with `read_tparquet`; the file's
columns must match the table's by position and type (use `INSERT INTO t SELECT ... FROM
read_tparquet(...)` to map or cast).

One more finding: DuckDB's built-in Parquet writer does not keep NaN payloads (the conformance
vector's `0x7ff0000000000001` comes out as `0xfff8000000000000`); `tparquet` keeps them.

## Ratio

On 2,000,000 deterministic market-like ticks (millisecond timestamps with random gaps, a
random-walk price on a 0.01 grid, lot sizes in 0.001 steps, a side flag;
`tests/test_acceptance.py`), `COPY (FORMAT tparquet, POLICY 'auto', LEVEL 8)` writes
4,998,033 bytes, 0.633 of DuckDB's `COPY (FORMAT parquet, COMPRESSION zstd,
COMPRESSION_LEVEL 9)` at 7,894,952 bytes, and the file reads back bit for bit.

## Packaging

`make build` builds the `cdylib` and appends DuckDB's extension metadata
(`src/metadata.rs`, `src/bin/package-extension.rs`): the last 512 bytes of the file are eight
32-byte fields, stored last first (magic `4`, platform such as `linux_amd64`, the C API version
`v1.5.6`, the extension version `v0.1.0`, the ABI type `C_STRUCT`, three empty fields) and a
256-byte signature (zeros: unsigned), behind the header of a Wasm custom section named
`duckdb_signature`, byte for byte what `extension-ci-tools/scripts/append_extension_metadata.py`
writes. With ABI `C_STRUCT` DuckDB checks the C API version, not the DuckDB version, so the file
loads in any DuckDB that serves C API v1.5.6 or later v1.x. The entrypoint is
`tparquet_init_c_api`, so the file must be named `tparquet.duckdb_extension`.

## Tests

```sh
uv sync --python 3.14          # duckdb/.venv: duckdb 2.0.0, pyarrow, pytest (uv.lock pins hashes)
make test                      # unit tests, then every SQL test on the extension cargo built
make sql                       # the SQL tests on the release build
make mutation                  # cargo mutants on this crate
```

- Unit tests (`src/*.rs`): the type mapping both ways, array normalization, path expansion,
  bind and projection slots, the row-group queue (also across threads), COPY options, the
  writer, DuckDB vector layouts converted to Arrow (on vectors laid out in memory), the footer.
- `tests/sql.rs` packages the library this build produced and runs pytest on it, so `cargo
  test`, and every mutant, checks what DuckDB actually loads. It also builds the
  `tparquet` CLI the tests transcode and `verify` with.
- `tests/test_conformance.py`: every `testdata/files/*.parquet` read with
  `read_tparquet`, fetched from DuckDB as Arrow and turned into canonical JSON
  (`tests/canonical.py`: float bits through `Array.view`, time-zoned timestamps back to the
  file's unit) equals `testdata/expected/*.jsonl` byte for byte.
- `tests/test_read.py`, `tests/test_write.py`, `tests/test_acceptance.py`, and the
  sqllogictest-style `tests/sql/*.test` (run by `tests/test_sqllogic.py`, as DuckDB's C++ runner
  does not come with the Python package).

## Distribution

The extension is not submitted anywhere. Notes for doing so:

- **Community extensions.** DuckDB builds, signs and serves community extensions from
  [duckdb/community-extensions](https://github.com/duckdb/community-extensions): a pull request
  adds `extensions/tparquet/description.yml` naming the repository, a commit (`ref`), the
  language (`Rust`), the build system (`cargo`) and the maintainers. Their CI builds with
  [extension-ci-tools](https://github.com/duckdb/extension-ci-tools) for every platform, so the
  repository needs the layout of DuckDB's Rust template
  ([extension-template-rs](https://github.com/duckdb/extension-template-rs)): a top-level
  `Makefile` that includes extension-ci-tools' `makefiles/c_api_extensions/rust.Makefile`
  (with `extension-ci-tools` as a submodule pinned to a commit), `EXTENSION_NAME =
  tparquet`, `USE_UNSTABLE_C_API = 0` and `TARGET_DUCKDB_VERSION = v1.5.6` (the C API
  version), and a crate whose `cdylib` is the extension. Their tooling appends the same footer as
  `package-extension` (and signs it). The workspace's path dependencies (`rust/parquet`,
  `rust/tparquet`) must build from that checkout; the tests here use Python and need a
  `test` target that the community CI can run, or none.
- **Signing.** DuckDB loads a signed extension without `allow_unsigned_extensions` only if the
  signature verifies against one of its built-in keys (core or community); there is no way to
  sign with one's own key for a plain `LOAD`. Signing happens in the community CI. Until then,
  users load the unsigned build with `allow_unsigned_extensions` (or `duckdb -unsigned`), or
  install it from a custom repository (`INSTALL tparquet FROM 'https://...'`), which
  also needs that setting for unsigned files.
- **Versions.** The footer's C API version `v1.5.6` lets the same build load in later DuckDB
  releases that still serve C API v1, on the same platform (one build per platform: `linux_amd64`,
  `linux_arm64`, `osx_amd64`, `osx_arm64`, `windows_amd64`).
