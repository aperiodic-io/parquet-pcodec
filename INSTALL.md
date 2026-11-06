# Installing tparquet

tparquet is not on PyPI, crates.io, Maven Central or the DuckDB extension repository yet.
Every package builds from a checkout of this repository:

```bash
git clone https://github.com/aperiodic-io/tparquet
```

```bash
cd tparquet
```

Every command below runs from the root of that checkout unless it says otherwise. Find what
you want to do in the first table, set up the [prerequisites](#prerequisites) it lists for
your operating system, then follow its section.

| I want to... | Section | Needs |
|---|---|---|
| try tparquet on my own files from a shell | [The `tparquet` CLI](#the-tparquet-cli) | Rust |
| read and write from Python, pyarrow or pandas | [Python](#python-pyarrow-pandas) | Rust, Python 3.10+ |
| scan, read, write and sink with Polars | [Polars](#polars) | Rust, Python 3.10+ |
| query from DuckDB in Python | [DuckDB from Python](#duckdb-from-python) | Rust, Python 3.10+ |
| query from the DuckDB CLI or any DuckDB client | [DuckDB extension](#duckdb-extension) | Rust |
| read and write from a Rust program | [Rust](#rust) | Rust |
| query with DataFusion | [DataFusion](#datafusion) | Rust |
| read and write from Go | [Go](#go) | Rust, Go 1.25+, a C compiler |
| read and write from Java, or anything on parquet-java | [Java](#java-parquet-java) | Rust, JDK 21, Maven |
| read and write with Spark | [Spark](#spark) | Rust, JDK 21, Maven, Spark 4.3 |
| query with Trino | [Trino](#trino) | Rust, JDK 21 and JDK 25, Maven, Trino 484 |
| read and write from C++ with Arrow C++ | [C++](#c) | Rust, CMake, Ninja, Arrow's build dependencies |
| read from C, as an Arrow C stream | [C](#c-1) | Rust, a C compiler |

Files that tparquet writes can be read only by a reader with the PCODEC codec. A reader
without it fails with an error and never returns wrong data. `tparquet decode` writes a
standard copy for such readers (see [Troubleshooting](#troubleshooting)).

## Platforms

CI builds and tests everything on Linux x86_64 (Ubuntu 24.04). The other platforms are not
tested in CI. The table says what to expect on each.

| | Linux x86_64 | Linux arm64 | macOS (Apple silicon, Intel) | Windows |
|---|---|---|---|---|
| CLI, Rust, DataFusion | tested | builds | builds | builds |
| Python, Polars | tested | wheel built in CI | builds | builds |
| DuckDB extension | tested | builds | builds | manual steps |
| Go | tested | builds | builds | not supported |
| Java, Spark, Trino | tested | native library by hand | native library by hand | native library by hand |
| C++ (Arrow fork) | tested | build scripts may work | not supported by the build scripts | not supported |
| C reader | tested | builds | builds | builds |

*builds*: pure Rust or cgo, with no platform-specific code; expected to work, but no test runs
there. *Native library by hand*: the jar bundles the JNI library for Linux x86_64 only, and
[Java on other platforms](#java-on-other-platforms) says how to supply it. On Windows, WSL 2
with Ubuntu 24.04 gives you the tested Linux setup for every package.

## Prerequisites

### Rust (every package)

Every package needs a Rust toolchain, version 1.88 or later, for the codec core.

Linux and macOS:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Windows: download and run `rustup-init.exe` from <https://rustup.rs>. It also offers to
install the Visual Studio C++ Build Tools, which Rust needs on Windows.

Check it with `cargo --version`. If you already have rustup, `rustup update stable`.

### A C compiler

The Rust build compiles some C (zstd, for example), and Go, the C reader and C++ need one too.

Debian and Ubuntu:

```bash
sudo apt-get install build-essential pkg-config git
```

Fedora and RHEL:

```bash
sudo dnf install gcc gcc-c++ make pkgconf-pkg-config git
```

macOS:

```bash
xcode-select --install
```

Windows: the Visual Studio C++ Build Tools (the rustup installer sets them up).

### Per language

| For | Install |
|---|---|
| Python, Polars | Python 3.10 or later with pip 21.3 or later (`python3 -m pip install -U pip`), or [uv](https://docs.astral.sh/uv/) |
| Go | Go 1.25 or later from <https://go.dev/dl/>, with cgo enabled (the default when a C compiler is on `PATH`) |
| Java, Spark | JDK 21 (for example Eclipse Temurin) and Maven 3.9 or later |
| Trino | JDK 21 and JDK 25, Maven 3.9 or later |
| C++ | the Ubuntu 24.04 packages listed under [C++](#c) |

On macOS, Homebrew has all of them: `brew install python go openjdk@21 maven uv`.

## The `tparquet` CLI

Linux, macOS and Windows:

```bash
cargo install --path rust/tparquet
```

This builds the `tparquet` binary into `~/.cargo/bin` (`%USERPROFILE%\.cargo\bin` on
Windows), which rustup puts on your `PATH`. Check it:

```bash
tparquet --help
```

```bash
tparquet inspect testdata/files/mixed.parquet
```

`tparquet encode`, `decode`, `inspect` and `verify` are shown in the
[README](README.md#try-it-on-your-own-file). To update, pull the repository and run
`cargo install --path rust/tparquet` again. To remove it, `cargo uninstall tparquet`.

## Python (pyarrow, pandas)

The `tparquet` package is a Rust extension module built with maturin; pip downloads maturin by
itself and builds it from source, which takes a few minutes the first time.

Into the active virtual environment, with pip (Linux, macOS, Windows):

```bash
python -m pip install "./python[pandas]"
```

The extras pull in the libraries each module needs: `pyarrow` (for `read_table` and
`ParquetFile.read`), `pandas` (`tparquet.pandas`, with pyarrow), `polars` (`tparquet.polars`)
and `duckdb` (`tparquet.duckdb`). Combine them as you need, `"./python[pandas,polars,duckdb]"`;
without extras the package has no dependencies at all.

With uv, in a project:

```bash
uv add ./path/to/tparquet/python --extra pandas
```

or into an existing environment:

```bash
uv pip install "./python[pandas]"
```

Without a checkout, pip can build it straight from GitHub:

```bash
python -m pip install "tparquet[pandas] @ git+https://github.com/aperiodic-io/tparquet#subdirectory=python"
```

To build a wheel to install elsewhere (on the same OS, CPU architecture and Python 3.10 or
later: the wheel is abi3), run `python -m pip wheel ./python -w dist`. Check the install:

```bash
python -c "import tparquet as tpq; print(tpq.read_table('testdata/files/mixed.parquet').num_rows)"
```

On Linux arm64, macOS and Windows, the build is the same as on Linux x86_64. If pip fails
with `can't find Rust compiler`, the Rust toolchain is not on `PATH` in that shell: open a new
shell after installing rustup, or run `. "$HOME/.cargo/env"`.

## Polars

`polars-tparquet` (`scan_tparquet`, `read_tparquet`, the `tparquet` namespaces) is pure Python
on top of the `tparquet` package and needs Polars 2.0 or later. Install both in one command,
so that pip takes `tparquet` from the checkout (it is not on PyPI, and on its own pip would
look for it there and fail):

```bash
python -m pip install ./python ./polars-tparquet
```

With uv:

```bash
uv pip install ./python ./polars-tparquet
```

From GitHub, without a checkout:

```bash
python -m pip install "tparquet @ git+https://github.com/aperiodic-io/tparquet#subdirectory=python" "polars-tparquet @ git+https://github.com/aperiodic-io/tparquet#subdirectory=polars-tparquet"
```

Check it:

```bash
python -c "import polars_tparquet as pt; print(pt.read_tparquet('testdata/files/mixed.parquet'))"
```

If you only need `scan_parquet`, `read_parquet` and `write_parquet` with Polars 1.20 or later,
`tparquet.polars` in the Python package has them: `python -m pip install "./python[polars]"`.

## DuckDB

There are two ways to use tparquet from DuckDB. In Python, `tparquet.duckdb` reads files
through the Python package and hands them to DuckDB as Arrow. Everywhere else (the DuckDB CLI,
other language clients, and also Python), the loadable extension adds `read_tparquet(...)` and
`COPY ... (FORMAT tparquet)` to DuckDB itself.

### DuckDB from Python

```bash
python -m pip install "./python[duckdb]"
```

```python
import duckdb
import tparquet.duckdb

con = duckdb.connect()
tparquet.duckdb.register(con, "trades", "trades.parquet")
con.sql("SELECT count(*) FROM trades")
```

### DuckDB extension

The extension is built against DuckDB's C extension API v1.5.6 and tested with DuckDB 2.0.0;
the same file loads in later DuckDB releases that still serve C API v1, on the platform it was
built on.

Linux and macOS:

```bash
make -C duckdb build
```

This writes `duckdb/build/tparquet.duckdb_extension`. Keep that file name: DuckDB derives the
entry point from it. Copy the file wherever you like.

Windows (untested; the Makefile assumes a Unix shell), in PowerShell:

```powershell
cargo build --release -p tparquet-duckdb --lib --bin package-extension
```

```powershell
target\release\package-extension.exe target\release\tparquet_duckdb.dll tparquet.duckdb_extension
```

The extension is not signed (DuckDB signs only the extensions it builds itself), so DuckDB
loads it only with unsigned extensions allowed. In the DuckDB CLI:

```bash
duckdb -unsigned
```

```sql
LOAD '/path/to/tparquet.duckdb_extension';
SELECT count(*) FROM read_tparquet('trades/*.parquet');
```

From Python:

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD '/path/to/tparquet.duckdb_extension'")
```

Other DuckDB clients (Go, Java, Node.js, R, ...) set the same `allow_unsigned_extensions`
configuration option when they open the database, then run the same `LOAD`. Each operating
system and architecture needs its own build: build it on each platform you deploy to.

## Rust

The `tparquet` crate (writer with automatic codec choice, reader, `transcode`, `inspect`,
`verify`) is built on a small fork of the arrow-rs `parquet` crate 60.1.0. Depend on both, and
patch `parquet` so that every crate in your build uses the fork:

```toml
[dependencies]
tparquet = { git = "https://github.com/aperiodic-io/tparquet" }
arrow-array = "60.1"
arrow-schema = "60.1"

[patch.crates-io]
parquet = { git = "https://github.com/aperiodic-io/tparquet", version = "=60.1.0" }
```

The repository holds two forks of `parquet` (60.1.0 here, 59.3.2 for DataFusion), so the
patch names the version it wants. With a local checkout instead, use paths:

```toml
[dependencies]
tparquet = { path = "../tparquet/rust/tparquet" }

[patch.crates-io]
parquet = { path = "../tparquet/rust/parquet" }
```

The `[patch.crates-io]` line is required: `tparquet` depends on `parquet = "=60.1.0"` with the
fork's `pcodec` feature, which the crates.io release does not have, so without the patch Cargo
cannot resolve the dependency. With it, any crate in your build that uses `parquet` 60
(including your own code and arrow-rs readers) reads PCODEC files. Your other Arrow crates must
be on arrow 60 as well.

Pin a commit with `rev = "..."` in both git lines for reproducible builds. To use only the page
payload codec, without Parquet, depend on `tparquet-core` from the same repository (no patch
needed).

## DataFusion

The DataFusion connector targets DataFusion 55.2.0, which is on arrow 59, so it uses a
separate fork of `parquet` 59.3.2:

```toml
[dependencies]
datafusion = "=55.2.0"
tparquet-datafusion = { git = "https://github.com/aperiodic-io/tparquet" }

[patch.crates-io]
parquet = { git = "https://github.com/aperiodic-io/tparquet", version = "=59.3.2" }
```

`version = "=59.3.2"` selects the fork in `datafusion/parquet`. DataFusion 55.2 needs Rust 1.94
or later. With a local checkout:

```toml
[dependencies]
datafusion = "=55.2.0"
tparquet-datafusion = { path = "../tparquet/datafusion" }

[patch.crates-io]
parquet = { path = "../tparquet/datafusion/parquet" }
```

Use `tparquet` (arrow 60) or `tparquet-datafusion` (arrow 59) in a Cargo workspace, not
both: each needs its own fork patched in for `parquet`. Usage is in
[`datafusion/README.md`](datafusion/README.md#use).

## Go

The Go package is a cgo binding of the codec's static C library, `libtparquet_capi.a`. Build the
library once, then use the package in one of two ways.

```bash
cargo build --release -p tparquet-capi
```

**With a local checkout** (simplest): point your module at it with a `replace` directive. The
package then links `target/release/libtparquet_capi.a` from the checkout by itself.

```bash
go mod edit -require=github.com/aperiodic-io/tparquet/go@v0.0.0 -replace=github.com/aperiodic-io/tparquet/go=/path/to/tparquet/go
```

```bash
go build ./...
```

**From the module cache** (`go get`), the checkout's `target/` does not exist. Fetch the module,
then pass the library's path and the `tparquet_external` build tag on every build:

```bash
go get github.com/aperiodic-io/tparquet/go@main
```

```bash
CGO_LDFLAGS=/path/to/tparquet/target/release/libtparquet_capi.a go build -tags tparquet_external ./...
```

Build the library from the same commit as the Go module you use. The tag also works for
`go test` and `go run`; set `CGO_LDFLAGS` in your build scripts or CI.

Platform notes:

- Linux: the package adds the system libraries the Rust library needs (`-lgcc_s -lutil -lrt
  -lpthread -lm -ldl`). Linux x86_64 is the tested platform.
- macOS: the same steps; install the Xcode command line tools for cgo.
- Windows: not supported (the package has no Windows link flags). Use WSL 2.
- Cross-compiling: build the library for the target triple
  (`cargo build --release -p tparquet-capi --target aarch64-unknown-linux-gnu`), point
  `CGO_LDFLAGS` at `target/<triple>/release/libtparquet_capi.a`, and set `CC` to a C
  cross-compiler.

## Java (parquet-java)

The JVM packages are a fork of parquet-java 1.17.0 (three jars that keep upstream's
coordinates, version `1.17.0-tparquet`), the codec over JNI (`tparquet-jni`) and the
`tparquet` library (`TparquetWriter`, `TparquetReader`). Build them into your local Maven
repository (`~/.m2`), with JDK 21 as `JAVA_HOME`:

```bash
make -C java install
```

This builds the JNI library with cargo, bundles it in the `tparquet-jni` jar, and installs
every module without running the tests (`make -C java test` runs them). Then, in your
`pom.xml`, pin the fork in place of upstream's parquet-java and add the library:

```xml
<dependencyManagement>
  <dependencies>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-format-structures</artifactId><version>1.17.0-tparquet</version></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-common</artifactId><version>1.17.0-tparquet</version></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-hadoop</artifactId><version>1.17.0-tparquet</version></dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.aperiodic.tparquet</groupId>
    <artifactId>tparquet</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>
  <dependency>
    <!-- parquet-hadoop needs Hadoop's classes; Spark and Hadoop applications already have them -->
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>3.3.6</version>
  </dependency>
</dependencies>
```

Without `hadoop-common` the writer fails with `ClassNotFoundException:
org.apache.hadoop.conf.Configurable`. The modules are built against Hadoop 3.3.

With Gradle, add `mavenLocal()` to the repositories and force the same versions:

```kotlin
repositories { mavenLocal(); mavenCentral() }

dependencies {
    implementation("io.aperiodic.tparquet:tparquet:0.1.0-SNAPSHOT")
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
}

configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.apache.parquet" &&
            requested.name in setOf("parquet-format-structures", "parquet-common", "parquet-hadoop")) {
            useVersion("1.17.0-tparquet")
        }
    }
}
```

The other parquet-java modules (`parquet-column`, `parquet-avro`, ...) stay upstream's 1.17.0.
To publish to a shared repository (Nexus, Artifactory) instead, build the JNI library with
`make -C java jni`, then run `mvn -f java/pom.xml -DskipTests deploy` with your
`distributionManagement` settings.

The modules run on Java 11 and later (the Spark module on 17 and later); JDK 21 is what builds
them.

### Java on other platforms

The `tparquet-jni` jar bundles the JNI library for Linux x86_64 only. On Linux arm64, macOS
and Windows, build the library on that machine:

```bash
cargo build --release -p tparquet-jni
```

This gives `target/release/libtparquet_jni.so` (Linux), `libtparquet_jni.dylib` (macOS) or
`tparquet_jni.dll` (Windows). Either name the file when starting the JVM:

```bash
java -Dtparquet.jni.library=/path/to/libtparquet_jni.dylib -jar app.jar
```

or put its directory on `java.library.path` (`-Djava.library.path=/path/to/dir`). The jar also
looks for the library inside itself at `io/aperiodic/tparquet/native/<os>-<arch>/`, where
`<os>` is `linux`, `darwin` or `windows` and `<arch>` is `x86_64` or `aarch64`, so a jar with
libraries for several platforms works without any option.

On macOS and Windows, `make -C java install` still works (the jar then has no bundled library);
`make` on Windows needs WSL 2 or another Unix shell.

## Spark

Spark 4.3 (Scala 2.13) is built on parquet-java 1.17.0. With the fork's jars in place of its
Parquet jars it reads PCODEC files with every reader, and the `tparquet` data source writes
them. Build the six jars it needs:

```bash
make -C java spark-jars
```

They land in `java/target/spark-jars`. Then, in your Spark distribution (on every node of a
cluster):

```bash
cd "$SPARK_HOME/jars" && mkdir -p ../jars-upstream && mv parquet-hadoop-1.17.0.jar parquet-common-1.17.0.jar parquet-format-structures-1.17.0.jar ../jars-upstream/
```

```bash
cp /path/to/tparquet/java/target/spark-jars/*.jar "$SPARK_HOME/jars/"
```

The three upstream jars must be out of `jars/`: with both versions on the class path, Spark may
load upstream's classes and refuse PCODEC files. To undo, delete the six copied jars and move
the three back from `jars-upstream`.

Check it:

```bash
"$SPARK_HOME/bin/spark-sql" -e 'SELECT count(*) FROM parquet.`/path/to/tparquet/testdata/files/mixed.parquet`'
```

It prints 1500; with upstream's jars still in place it fails on the file's footer.

PySpark from pip (`pip install pyspark==4.3.*`) has its own `jars/` directory inside the
installed package, at `python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))"`;
replace the jars there the same way.

On managed Spark (Databricks, EMR, Dataproc), the Parquet jars are part of the runtime image,
which you usually cannot change; the replacement then needs a custom image or a bootstrap
script that runs the same two steps on every node. Passing the jars with `--jars` or
`spark.jars` is not enough: Spark's own `jars/` come first on the class path, so upstream's
parquet-java classes are the ones loaded.

On platforms other than Linux x86_64, also add `-Dtparquet.jni.library=...` to
`spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` (see
[Java on other platforms](#java-on-other-platforms)).

## Trino

Trino 484 reads tparquet files through its Hive, Iceberg, Delta Lake and Hudi connectors once
their plugin directories hold forks of trino-parquet 484 and parquet-java 1.18.1 and the codec.
Building the jars needs JDK 25 (for Trino) and JDK 21 (for the Java modules). The Makefile
looks for them at Ubuntu's paths; elsewhere, name them:

```bash
make -C trino plugin-jars JAVA_HOME_TRINO=/path/to/jdk-25 JAVA_HOME_JAVA=/path/to/jdk-21
```

The four jars land in `trino/plugin-jars/target/tparquet-jars`. Install them into the Trino
server's installation directory (the one with `plugin/`), on the coordinator and every worker:

```bash
trino/install-tparquet.sh /usr/lib/trino
```

Then restart Trino. The script checks that the install is Trino 484 before it changes anything,
and keeps the upstream jars in `<TRINO_HOME>/tparquet-backup/`. To install on a machine
without the checkout, copy `tparquet-jars/` and the script there and run
`install-tparquet.sh --jars /path/to/tparquet-jars TRINO_HOME`. Run the script again after
upgrading the jars; `trino/install-tparquet.sh --uninstall /usr/lib/trino` restores upstream.

With the official Trino Docker image, run the script in a derived image:

```dockerfile
FROM trinodb/trino:484
COPY tparquet-jars /tmp/tparquet-jars
COPY install-tparquet.sh /tmp/
USER root
RUN /tmp/install-tparquet.sh --jars /tmp/tparquet-jars /usr/lib/trino
USER trino
```

Trino servers run on Linux; the bundled JNI library covers Linux x86_64. For arm64 workers, see
the native library note in [`trino/README.md`](trino/README.md). Trino reads tparquet files; it
does not write them from SQL (see that README, Writing).

## C++

The C++ library (`tparquet::ReadTable`, `WriteTable`, `FileWriter`) is built on a fork of
Apache Arrow C++ 26.0.1, applied as a patch to the release. The build downloads the Arrow
release from dlcdn.apache.org, so it needs network access the first time. On Ubuntu 24.04:

```bash
sudo apt-get install cmake ninja-build ccache thrift-compiler libthrift-dev libzstd-dev libsnappy-dev liblz4-dev libbrotli-dev zlib1g-dev liburiparser-dev rapidjson-dev libgtest-dev libgmock-dev libboost-dev
```

```bash
make -C cpp install JOBS=8
```

This builds the patched Arrow and Parquet into `target/cpp/arrow-release/install` and the
tparquet CMake package into `target/cpp/install` (allow some tens of minutes for Arrow the first
time; `JOBS` sets the parallelism). Use it from CMake:

```cmake
find_package(tparquet 0.1 REQUIRED CONFIG)
target_link_libraries(app PRIVATE tparquet::tparquet)
```

```bash
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH="/path/to/tparquet/target/cpp/install;/path/to/tparquet/target/cpp/arrow-release/install"
```

[`cpp/examples/downstream`](cpp/examples/downstream) is a complete project. The patched Arrow
replaces Arrow 26.0.1 in your build; it must be this one, as an upstream Arrow builds and links
but refuses PCODEC files. When you deploy, ship the patched `libarrow.so` and `libparquet.so`
from `target/cpp/arrow-release/install/lib` with your program (or set `LD_LIBRARY_PATH` to that
directory), in place of any system Arrow.

Other Linux distributions need the same libraries under their own package names. The build
scripts are not written for macOS or Windows; there, use the C reader below, or the Python
package through pyarrow.

## C

`libtparquet_read` reads tparquet files as an Arrow C stream (`tparquet_read.h`), with no Arrow
C++ dependency. Linux, macOS and Windows:

```bash
cargo build --release -p tparquet-cread
```

This gives the static library `target/release/libtparquet_read.a` and the shared
`libtparquet_read.so` (`.dylib` on macOS; `tparquet_read.lib` and `tparquet_read.dll` on
Windows). Link it with your program; on Linux the static library needs the system libraries
after it:

```bash
cc app.c -I rust/cread/include target/release/libtparquet_read.a -lpthread -ldl -lm -o app
```

On macOS, `cc app.c -I rust/cread/include target/release/libtparquet_read.a -o app`. With the
C++ build above, CMake's `tparquet::read` target links it for you. For the codec payload alone
(no Parquet), `cargo build --release -p tparquet-capi` gives `libtparquet_capi` and its header
`rust/capi/include/tparquet.h`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| a reader without tparquet fails on a file (DuckDB's `read_parquet`: `TProtocolException: Invalid data`; upstream Trino: `Required field 'codec' was not present!`; pyarrow, Polars and others: an unknown or unsupported codec) | the file uses PCODEC and the reader does not have the codec. Read it with tparquet's reader for that tool, or write a standard copy: `tparquet decode in.parquet out.parquet` |
| `pip install` fails with `can't find Rust compiler` | Rust is not on `PATH` in that shell: open a new shell after installing rustup, or `. "$HOME/.cargo/env"` |
| `pip install ./polars-tparquet` fails with `ERROR: No matching distribution found for tparquet` | install `./python` in the same command: `pip install ./python ./polars-tparquet` |
| Cargo: ``package `tparquet` depends on `parquet` with feature `pcodec` but `parquet` does not have that feature`` | the `[patch.crates-io] parquet = ...` line is missing from your top-level `Cargo.toml` (Cargo ignores patches in dependencies) |
| Cargo: ``patch for `parquet` ... resolved to more than one candidate`` | add `version = "=60.1.0"` (or `"=59.3.2"` for DataFusion) to the git patch |
| Go: `cannot find .../target/release/libtparquet_capi.a` | from a checkout, build it with `cargo build --release -p tparquet-capi`; from the module cache (the path has `pkg/mod` in it), build with `-tags tparquet_external` and `CGO_LDFLAGS` |
| DuckDB: `extension ... could not be loaded because its signature is either missing or invalid` | start DuckDB with `-unsigned`, or set `allow_unsigned_extensions` |
| DuckDB: `The file was built for the platform ...` | the extension was built on another OS or architecture: build it on the machine that loads it |
| Java: `UnsatisfiedLinkError: no tparquet_jni` | no JNI library for this platform in the jar: see [Java on other platforms](#java-on-other-platforms) |
| Java or Spark fail to read the footer or the codec of a PCODEC file | upstream's parquet-java is on the class path ahead of the fork: check that only the `1.17.0-tparquet` jars of the three modules are there (`mvn dependency:tree`, or Spark's `jars/`) |
| Trino: `already loaded in another classloader` | remove `-Dtparquet.jni.library` from `jvm.config`: each catalog loads its own copy of the bundled library |
