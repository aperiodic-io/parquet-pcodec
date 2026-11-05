# Fork of parquet-java 1.18.1 (`parquet-format-structures`, `parquet-common`)

This directory is two modules of [parquet-java](https://github.com/apache/parquet-java) 1.18.1 as
published on Maven Central, with the PCODEC codec id of `spec/FORMAT.md` added: exactly the changes
that [`../parquet-java`](../parquet-java/FORK.md) (the 1.17.0 fork) makes to these two modules. It
exists for Trino: Trino 484 has its own Parquet reader (`io.trino:trino-parquet`, forked in
[`trino/trino-parquet`](../../trino/trino-parquet/FORK.md)), built on parquet-java 1.18.1. It reads
footers with the thrift classes of `parquet-format-structures` and maps codecs with
`CompressionCodecName` of `parquet-common`; it does not use `parquet-hadoop`, so that module is not
forked here. The other modules (`parquet-column`, `parquet-encoding`, ...) stay upstream 1.18.1.
The fork keeps upstream's coordinates and packages, with the version `1.18.1-tparquet`, so that it
replaces the upstream jars of a Trino plugin one for one.

The 1.17.0 fork does not take its place: Trino's `parquet-column` 1.18.1 is built against
`parquet-common` 1.18.1, whose classes (`BytesInput` among them) changed since 1.17.0.

| | |
|---|---|
| upstream | parquet-java 1.18.1 |
| source | `https://repo.maven.apache.org/maven2/org/apache/parquet/<module>/1.18.1/<module>-1.18.1-sources.jar` |
| sha256 `parquet-format-structures-1.18.1-sources.jar` | `e3fa98021ee040452c2ba9a4715b5e5a9e36ca6447906b590ca114345f2df99f` |
| sha256 `parquet-common-1.18.1-sources.jar` | `25fc79cbb1414f81537cdfae1353623f9a972ca4dffa3c3837ac5ff9c5a69f35` |

The published `-sources.jar` of each module is vendored as it is, as in `../parquet-java`: its
`.java` files under `<module>/src/main/java`, its other files (`parquet.thrift`,
`META-INF/{LICENSE,NOTICE,DEPENDENCIES}`) under `<module>/src/main/resources`, without the jar's
build metadata (`META-INF/MANIFEST.MF`, `META-INF/maven/**`). The `parquet-format-structures`
sources are the thrift-generated Java (thrift 0.23.0) that upstream publishes, so no thrift
compiler is needed. `FORK.md` and `FORK.patch` are not part of upstream.

`FORK.patch` is the complete diff from upstream to this tree, the build files and tests the fork
adds included. `scripts/check-fork-java.sh` (or `scripts/check-fork-java.sh java/parquet-java-1.18`)
downloads the two source jars, checks the sha256 values above, unpacks them the same way and fails
unless the diff of the git-tracked files equals `FORK.patch`;
`scripts/check-fork-java.sh --update java/parquet-java-1.18` rewrites it after an intended change,
which must also be listed below.

## Changes

| File | Item | Change | Why |
|---|---|---|---|
| `parquet-format-structures/.../format/CompressionCodec.java` | thrift enum | `PCODEC(20547)`, and its `findByValue` case | the codec id of spec section 2; without it thrift reads the codec as `null` and refuses the column chunk ("Required field 'codec' was not present!") |
| `parquet-format-structures/.../resources/parquet.thrift` | `enum CompressionCodec` | `PCODEC = 20547;` | the definition the generated enum comes from |
| `parquet-common/.../metadata/CompressionCodecName.java` | enum | `PCODEC("org.apache.parquet.hadoop.codec.PcodecCodec", CompressionCodec.PCODEC, ".pcodec")` | the footer conversion (`fromParquet`, which Trino's `ParquetMetadata` calls for every column chunk) and the codec name; the Hadoop class is the 1.17.0 fork's, absent from Trino (`getHadoopCompressionCodecClass` is then `null`, which Trino never asks for) |
| `*/pom.xml` | new | the build of each module, in the `java/` reactor: upstream's dependencies (libthrift 0.23.0, javax.annotation-api, slf4j-api), Java 11 bytecode as upstream, and upstream's shading: `libthrift` relocated to `shaded.parquet.org.apache.thrift` in `parquet-format-structures`, `minimizeJar`, the dependency-reduced pom; PIT on the changed enums | the published poms build from thrift files and a parent pom; these build from the vendored sources |
| `*/src/test/java/**/TestPcodec*.java`, `parquet-common/src/test/.../codec/PcodecCodec.java` | new | the fork's tests, the same as the 1.17.0 fork's | see Tests |

These modules are built in the `java/` reactor (JDK 21, Java 11 bytecode, like the 1.17.0 fork
and like upstream) rather than in Trino's (`trino/`, JDK 25): they are parquet-java, with the
parquet-java fork's build rules, checks and mutation testing, and `make -C java install`, which
the Trino build needs for `tparquet-jni` anyway, installs them.

### Jar contents

Compared with upstream's published jars (`unzip -Z1`): `parquet-common-1.18.1-tparquet.jar` has
exactly the entries of `parquet-common-1.18.1.jar` (124). `parquet-format-structures-1.18.1-tparquet.jar`
has the entries of upstream's, the shaded thrift classes included, except 59 compiler-synthesized
switch-map classes (`org/apache/parquet/format/*$1.class`), which upstream's javac emitted and
javac 21 does not need (472 entries against 531), as for the 1.17.0 fork. The classes are Java 11
(class file version 55). `trino/tparquet-trino` checks, on Trino's class path, that the thrift
relocation is in place and that these are the jars Trino's reader loads.

## Using the fork

In a Trino install, `trino/install-tparquet.sh` replaces `parquet-format-structures-1.18.1.jar` and
`parquet-common-1.18.1.jar` in each connector's plugin directory (`trino/README.md`). In a Maven
build:

```xml
<dependencyManagement>
  <dependencies>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-format-structures</artifactId><version>1.18.1-tparquet</version></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-common</artifactId><version>1.18.1-tparquet</version></dependency>
  </dependencies>
</dependencyManagement>
```

## Tests

- `make -C java test`: `TestPcodecCompressionCodec` (the thrift enum: the id of spec section 2,
  the standard codecs' values unchanged, other values still unknown, a PCODEC column chunk through
  thrift's compact protocol, the thrift definition) and `TestPcodecCompressionCodecName` (the
  name, `fromConf`, `fromParquet`, `fromCompressionCodec`, every codec to the footer and back).
- Through Trino: `trino/` reads every conformance vector with these jars on Trino's class path.

## Mutation testing

PIT on the two changed enums (`make -C java mutants`): `CompressionCodec` 10 of 10 mutants killed,
`CompressionCodecName` 14 of 14.
