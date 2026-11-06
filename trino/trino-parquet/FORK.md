# Fork of `io.trino:trino-parquet` 484

This directory is Trino's Parquet reader and writer, `io.trino:trino-parquet` 484, as published on
Maven Central, with the PCODEC codec of `spec/FORMAT.md` added. Trino's Hive, Iceberg, Delta Lake
and Hudi connectors read Parquet through this library; with it (and the parquet-java 1.18.1 fork,
[`java/parquet-java-1.18`](../../java/parquet-java-1.18/FORK.md), and `tparquet-jni`) in a
connector's plugin directory, Trino reads PCODEC files (`trino/README.md`). The fork keeps
upstream's coordinates and packages, with the version `484-tparquet`, so that its jar replaces
`trino-parquet-484.jar` one for one.

| | |
|---|---|
| upstream | Trino 484, `io.trino:trino-parquet:484` |
| source | `https://repo.maven.apache.org/maven2/io/trino/trino-parquet/484/trino-parquet-484-sources.jar` |
| sha256 `trino-parquet-484-sources.jar` | `9bf84b7f04d461c03dc757f02094cfd68b835cd89dacc31b3fcca8c2cd272824` |

The published `-sources.jar` is vendored as it is, as the parquet-java forks are: its `.java` files
under `src/main/java`, without the jar's build metadata (`META-INF/MANIFEST.MF`, `META-INF/maven/**`;
it has no other files). `FORK.md` and `FORK.patch` are not part of upstream.

`FORK.patch` is the complete diff from upstream to this tree, the build file and tests the fork adds
included. `scripts/check-fork-java.sh` (or `scripts/check-fork-java.sh trino/trino-parquet`)
downloads the source jar, checks the sha256 above, unpacks it the same way and fails unless the diff
of the git-tracked files equals `FORK.patch`; `scripts/check-fork-java.sh --update trino/trino-parquet`
rewrites it after an intended change, which must also be listed below.

## Changes

Upstream needs these changes, not just the parquet-java fork: Trino decompresses pages itself, in
`ParquetCompressionUtils.decompress`, with an exhaustive `switch` over the thrift
`CompressionCodec`, and compresses them in its own writer (`ParquetCompressor`), so against the
forked enum upstream's sources do not even compile ("the switch expression does not cover all
possible input values"). Those are the only two switches over codecs in trino-parquet; the rest of
it uses the codec only through `CompressionCodecName.fromParquet` (`ParquetMetadata`), as data
(`ColumnChunkMetadata`, the reader's codec metrics) or by equality (`PageReader`,
`ParquetWriteValidation`). The fork's logic is in two new files; the upstream files get one-line
hooks.

| File | Item | Change | Why |
|---|---|---|---|
| `io/trino/parquet/PcodecPages.java` | new | `decompress(dataSourceId, input, uncompressedSize)`: `Pcodec.decompress` (tparquet-jni) to exactly the expected size; a `PcodecException` (a malformed payload, or one of another size) and a negative size become `ParquetCorruptionException` ("Malformed Parquet file. Invalid PCODEC page: ..."); a page with nothing to decode (expected size 0) is not decoded | spec sections 3, 4 and 7; as the reference reader (`rust/parquet`) and the 1.17.0 fork's `PcodecDecompressor` |
| `io/trino/parquet/ParquetCompressionUtils.java` | `decompress` | `case PCODEC -> PcodecPages.decompress(...)`; the empty-input shortcut (an empty slice for any empty input) no longer applies to PCODEC | the switch is exhaustive; a PCODEC payload is never empty, so an empty page that should hold bytes is corrupt, not an empty slice |
| `io/trino/parquet/writer/PcodecColumns.java` | new | `hint(column)`: the column's physical type when its pages are arrays of numbers (INT32, INT64, FLOAT, DOUBLE without definition or repetition levels), else none; `plainValues(codec, column)`; `footerKeyValue(rowGroups)`: `parquet.pcodec.version = "1"` when a column chunk is PCODEC; `PcodecCompressor`, `Pcodec.compress` at the default level (8) with DECIMAL | spec sections 2 and 3, within Trino's writer (see Writing) |
| `io/trino/parquet/writer/ParquetCompressor.java` | `getCompressor` | takes the column; `case PCODEC -> new PcodecCompressor(hint(column))` | the switch is exhaustive; the hint is per column |
| `io/trino/parquet/writer/PrimitiveColumnWriter.java` | constructor | `getCompressor(compressionCodec, columnDescriptor)` | the hint |
| `io/trino/parquet/writer/ParquetWriters.java` | `WriteBuilder.primitive` | `newPlainValuesWriter` where `plainValues(codec, column)` | spec section 3: PLAIN, no dictionary, for the columns PCODEC models |
| `io/trino/parquet/writer/valuewriter/TrinoValuesWriterFactory.java` | new method | `newPlainValuesWriter(bloomFilter)` | as above |
| `io/trino/parquet/writer/ParquetWriter.java` | `FileFooter.createFileMetadata` | the key-value metadata is a list: `writer.time.zone` (upstream) and `parquet.pcodec.version` | spec section 2 |
| `pom.xml` | new | the build (below) | the published pom's parent is not usable as is |
| `src/test/java/**` | new | the fork's tests | see Tests |

### Build

Upstream's pom (`trino-parquet-484.pom`) has `io.trino:trino-root:484` as its parent, which has
airbase as its own. As a parent it does not work for the fork: its dependency management names
Trino's own modules at `${project.version}`, which for the fork is `484-tparquet` (no such Trino
modules exist), and airbase binds its checks (checkstyle, error-prone, modernizer, enforcer,
dependency analysis, license, duplicate classes, ...) to the build. So `pom.xml` lists the
published pom's dependencies, with `tparquet-jni` added, and its parent, `trino/pom.xml`, imports
`trino-root` 484 as a BOM: the same dependency versions as upstream's build, with the forks pinned
over them (`parquet-format-structures` and `parquet-common` `1.18.1-tparquet`). As upstream's,
the classes are Java 25 (class file version 69), compiled with `--add-modules=jdk.incubator.vector`
(trino-parquet's `extraJavaVectorArgs`). Airbase's checks are not run. The published pom's test
dependencies are not needed: the sources jar has no tests, and the fork's tests need JUnit, jqwik,
AssertJ and Jackson only.

The jar (`trino-parquet-484-tparquet.jar`) has exactly upstream's entries (`unzip -Z1` of
`trino-parquet-484.jar`, 532) plus the fork's three classes (`PcodecPages`, `PcodecColumns`,
`PcodecColumns$PcodecCompressor`).

### Writing

Trino's writer has one codec per file and writes `DATA_PAGE` (V1) pages, which start with the
column's levels, so a page is an array of numbers only for an INT32, INT64, FLOAT or DOUBLE column
without levels: a required (NOT NULL) top-level column, which Iceberg tables can have and Trino's
Hive tables do not. Those columns get PLAIN values without a dictionary and their physical type as the
hint (PCO or DECIMAL payloads); the others keep Trino's encodings and their payloads are the codec's
ZSTD fallback, which is valid (spec section 4) but not better than ZSTD. Writing V2 pages, which
would let optional columns be PCO too, is a larger change to Trino's writer and is not made. The
level (8) and DECIMAL are the codec's defaults; Trino's writer options have no place for them.

The connectors choose their codec from their own configuration (`hive.compression-codec`,
`iceberg.compression-codec`, ...), whose enums (in the connectors, not in trino-parquet) do not
have PCODEC, so SQL `INSERT`s do not write PCODEC; the writer is reachable through the
`io.trino.parquet.writer.ParquetWriter` API.

## Tests

- In this module (`make -C trino test`): `TestPcodecDecompression` (every payload vector of
  `testdata/payloads` through `ParquetCompressionUtils.decompress`: the valid ones decode exactly,
  the invalid ones are `ParquetCorruptionException`s with the reference decoder's message; any other
  expected size, a negative size, an empty page, nothing to decode, a slice of a larger array, the
  other codecs unchanged), `TestPcodecDecompressionProperties` (jqwik: random bytes, random bodies
  behind a valid header, damaged and truncated valid payloads give the expected size or a
  `ParquetCorruptionException`, nothing else), `TestPcodecCompressor` (the hint per column, the
  compressor, plain values, the footer key, the other codecs unchanged), `TestPcodecParquetWriter`
  (files written by Trino's `ParquetWriter` with PCODEC: every chunk PCODEC, the footer key, the
  encodings and payload mode of every page, read back by the writer's validation, ZSTD files as
  upstream, malformed and damaged payloads are corruption errors).
- `trino/tests`: Trino's reader on every conformance vector and on larger files, and Trino's query
  engine (the Hive connector) over directories of PCODEC files.
- `trino/acceptance-upstream`: unmodified trino-parquet 484 refuses every PCODEC vector.

## Mutation testing

PIT on the fork's code (`make -C trino mutants`; the classes and methods in `pom.xml`): 31 of 31
mutants killed. Excluded: upstream's other methods of the touched classes, and calls to Guava's
`Preconditions` (upstream's writer invariants, which its API cannot break).
