# Fork of parquet-java (`parquet-format-structures`, `parquet-common`, `parquet-hadoop`)

This directory is three modules of [parquet-java](https://github.com/apache/parquet-java) as
published on Maven Central, with the PCODEC codec of `spec/FORMAT.md` added. The other modules
(`parquet-column`, `parquet-encoding`, `parquet-jackson`, ...) are used unmodified. The fork keeps
upstream's coordinates and packages, with the version `1.17.0-tparquet`, so that it replaces the
upstream jars of an application (Spark, Hive, anything on parquet-java) one for one.

| | |
|---|---|
| upstream | parquet-java 1.17.0 |
| source | `https://repo.maven.apache.org/maven2/org/apache/parquet/<module>/1.17.0/<module>-1.17.0-sources.jar` |
| sha256 `parquet-format-structures-1.17.0-sources.jar` | `bff6057ba3517a3310b11acc95cba2a12b5e0379e89e338751009d8df134d2de` |
| sha256 `parquet-common-1.17.0-sources.jar` | `ed385971f09275d4919f17f9c77256c86e47d0707359e2cdde939a2401a91dcc` |
| sha256 `parquet-hadoop-1.17.0-sources.jar` | `86d3dade33a169701a2d25d4739636cea0fc858abae8027d803217a8763045e8` |

The published `-sources.jar` of each module is vendored as it is: its `.java` files under
`<module>/src/main/java`, its other files (`parquet.thrift`, `META-INF/LICENSE`) under
`<module>/src/main/resources`, without the jar's build metadata (`META-INF/MANIFEST.MF`,
`META-INF/maven/**`). The `parquet-format-structures` sources are the thrift-generated Java that
upstream publishes, so no thrift compiler is needed. `FORK.md` and `FORK.patch` are not part of
upstream.

`FORK.patch` is the complete diff from upstream to this tree, the build files and tests the fork
adds included. `scripts/check-fork-java.sh` downloads the three source jars, checks the sha256
values above, unpacks them the same way and fails unless the diff of the git-tracked files equals
`FORK.patch`; `scripts/check-fork-java.sh --update` rewrites it after an intended change, which
must also be listed below.

## Changes

| File | Item | Change | Why |
|---|---|---|---|
| `parquet-format-structures/.../format/CompressionCodec.java` | thrift enum | `PCODEC(20547)`, and its `findByValue` case | the codec id of spec section 2; without it thrift reads the codec as `null` and refuses the column chunk |
| `parquet-format-structures/.../resources/parquet.thrift` | `enum CompressionCodec` | `PCODEC = 20547;` | the definition the generated enum comes from |
| `parquet-common/.../metadata/CompressionCodecName.java` | enum | `PCODEC("org.apache.parquet.hadoop.codec.PcodecCodec", CompressionCodec.PCODEC, ".pcodec")` | the writer-side codec name (`parquet.compression=PCODEC`, `withCompressionCodec`) and the footer conversion (`fromParquet`, `valueOf`) |
| `parquet-hadoop/.../hadoop/codec/PcodecCodec.java` | new | the Hadoop codec class of `CompressionCodecName.PCODEC`, its configuration keys (`parquet.compression.codec.pcodec.level`, `.decimal`), `compressor(conf)` and `withVersion` (the footer key) | see below why the class refuses streams |
| `parquet-hadoop/.../hadoop/codec/PcodecCompressor.java` | new | `BytesCompressor` over `io.aperiodic.tparquet.Pcodec` (`java/tparquet-jni`), with `forColumn`, which gives a column's pages its physical type as the hint | spec section 3 |
| `parquet-hadoop/.../hadoop/codec/PcodecDecompressor.java` | new | `BytesDecompressor` (heap and `ByteBuffer`) to exactly the expected size; an empty values section is not decoded | spec section 3; as the reference reader (`rust/parquet`, `decode_page`) |
| `parquet-hadoop/.../hadoop/CodecFactory.java` | `createCompressor`, `createDecompressor` | a `PCODEC` branch first | PCODEC is not a Hadoop stream codec |
| `parquet-hadoop/.../hadoop/DirectCodecFactory.java` | `createDecompressor` | `case PCODEC` | direct buffers are decoded in place (`createCompressor` falls through to `CodecFactory`) |
| `parquet-hadoop/.../hadoop/ColumnChunkPageWriteStore.java` | the three constructors that create the page writers | `PcodecCompressor.forColumn(compressor, path)` in place of `compressor` | the type hint; parquet-java has one compressor per file, the hint is per column |
| `parquet-hadoop/.../hadoop/ParquetFileWriter.java` | `end` | the footer's key-value metadata through `PcodecCodec.withVersion` | `parquet.pcodec.version = "1"` when a column chunk uses PCODEC and the key is not there (spec section 2) |
| `*/pom.xml` | new | the build of each module: upstream's dependencies (plus `tparquet-jni` for `parquet-hadoop`), Java 11 bytecode as upstream, and upstream's shading: `libthrift` relocated into `parquet-format-structures`, the jackson and fastutil references of `parquet-hadoop` relocated to `shaded.parquet.*` (fastutil classes in use included, minimized) | the published poms build from thrift files and a parent pom; these build from the vendored sources |
| `*/src/test/java/**/TestPcodec*.java`, `PcodecPages.java` | new | the fork's tests | see Tests |

### Why `PcodecCodec` refuses streams

parquet-java maps every codec name to a Hadoop `CompressionCodec` class and, for a name whose
class is `null`, uses no compression (`NO_OP_COMPRESSOR`). PCODEC needs each page's physical type
to compress and its expected size to decompress, which the Hadoop stream API does not carry, so it
is created by `CodecFactory` instead. Its class is still a real class, whose stream methods throw
`UnsupportedOperationException`: a code path the fork missed fails, instead of writing
uncompressed bytes labelled PCODEC.

### Decompression call sites

`BytesInputDecompressor.decompress(BytesInput, int)` and `decompress(ByteBuffer, int, ByteBuffer,
int)` always get the expected size. Every call site in `parquet-hadoop`: `ColumnChunkPageReadStore`
(data pages V1 and V2, heap and off-heap, and dictionary pages) and `ParquetFileReader`
(dictionary pages). For V2 pages the size is the uncompressed page size minus the level lengths,
and pages with `is_compressed = false` are not decompressed.

### Writing

parquet-java writes one codec for every column of a file, so with `PCODEC` the columns PCODEC does
not model (strings, booleans) get the payload's ZSTD fallback (or RAW), which is valid (spec
section 4). The writer recommendations of spec section 3 are the writer's settings: parquet-java's
V2 writer encodes integers `DELTA_BINARY_PACKED` by default, which is not an array of values, so
those pages fall back to ZSTD too. `java/tparquet` (`TparquetWriter`) sets PLAIN for the numeric
columns. parquet-java's PLAIN writer writes every NaN as the canonical NaN (`doubleToLongBits`,
upstream behaviour); reading keeps NaN payloads.

### Jar contents

The fork's jars have the same entries as upstream's, plus the fork's three classes, except
compiler-synthesized switch-map classes (`Foo$1.class`): upstream's javac emitted 58 of them in
`parquet-format-structures` and one in `parquet-hadoop` (`ColumnMasker$1`) that javac 21 does not
need. `TestPcodecShadedJar` checks the thrift relocation.

## Using the fork

Replace the upstream modules in the dependency tree, e.g. with Maven:

```xml
<dependencyManagement>
  <dependencies>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-format-structures</artifactId><version>1.17.0-tparquet</version></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-common</artifactId><version>1.17.0-tparquet</version></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-hadoop</artifactId><version>1.17.0-tparquet</version></dependency>
  </dependencies>
</dependencyManagement>
```

or, for a binary distribution (Spark's `jars/`), replace the three jars and add `tparquet-jni`.
`java/tparquet-spark` does this for Spark.

## Tests

- `mvn -pl parquet-java/... verify` (`make -C java test`): `TestPcodecCompressionCodec` (the
  thrift enum and a column chunk through thrift), `TestPcodecCompressionCodecName`,
  `TestPcodecCodec` (the Hadoop class, the footer key), `TestPcodecCodecFactory` (heap and direct
  factories, the type hint per physical type, level and decimal from the configuration, buffers,
  errors), `TestPcodecFiles` (files written and read: every page layout, several row groups, the
  direct-buffer read path, the payload mode of every page, dictionary pages, the footer key,
  special floats, a corrupt payload and every sixteenth byte of every payload damaged in turn) and
  `TestPcodecShadedJar`.
- The conformance vectors (`testdata/`) are read through the fork by `java/tparquet`.
- Upstream's own suite, from its published `-tests.jar` files, runs against the fork in
  `parquet-java/upstream-tests` (`make -C java fork-upstream-tests`): once against upstream
  1.17.0 and once against the fork, and `compare.py` requires the same outcome for every test.
  `TestParquetWriterError` is left out of both runs: it writes in a child JVM that gets
  upstream's DEBUG logging configuration and did not finish in 20 minutes here.
- `java/acceptance-upstream`: unmodified parquet-java 1.17.0 refuses every PCODEC vector.

## Mutation testing

PIT on the fork's classes and changed methods (`make -C java fork-mutants`), every mutant killed.
