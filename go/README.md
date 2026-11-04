# pparquet for Go

PCODEC ([spec](../spec/FORMAT.md)) for [Apache arrow-go](https://github.com/apache/arrow-go)
v18.8.0: a Parquet compression codec that runs [pcodec](https://github.com/pcodec/pcodec) on
numeric columns. No fork of arrow-go is needed: the package registers the codec with
`compress.RegisterCodec` on import.

```go
import pparquet "github.com/aperiodic-io/pparquet/go"

// Read: importing the package is enough for file.Reader and pqarrow.

// Write with automatic per-column selection (spec section 6):
w, err := pparquet.NewFileWriter(schema, out)        // Auto, base codec ZSTD(3)
err = w.Write(firstBatch)                           // runs the trial, then writes
err = w.Close()

// Or force codecs, and get plain arrow-go writer properties:
plan, err := pparquet.NewPlan(schema, nil,
	pparquet.WithDefaultPolicy(pparquet.Pcodec),
	pparquet.WithPolicy("venue", pparquet.Base),
	pparquet.WithPcodecLevel(8))
props := parquet.NewWriterProperties(plan.Properties()...)
```

See the package documentation (`go doc`) for the policies, how the type hint travels in the
compression level, and the runnable examples.

## Building

The package is a cgo binding of the Rust C ABI in `rust/capi`. It needs a C compiler and the
static library `libpparquet_capi.a`.

**In a checkout of this repository** (or with a `replace` directive pointing at one), build
the library and use the package as is; it links `../target/release/libpparquet_capi.a`:

```sh
cargo build --release -p pparquet-capi   # or: make -C go lib
cd go && go test ./...
```

**From the module cache**, that relative path does not exist. Build the library once from a
checkout of the same version, then link it by path with the `pparquet_external` build tag:

```sh
cargo build --release -p pparquet-capi
export CGO_LDFLAGS=$PWD/target/release/libpparquet_capi.a
go build -tags pparquet_external ./...
```

The Makefile does the same with `make test PPARQUET_LIB_DIR=/path/to/dir`. On Linux the package
adds the system libraries the Rust library needs (`-lgcc_s -lutil -lrt -lpthread -lm -ldl`).
Only linux/amd64 is tested. Linking a second Rust static library into the same binary can
clash on Rust runtime symbols.

## Checks

| | |
|---|---|
| `make test` | unit, integration, property and acceptance tests, conformance vectors |
| `make check` | `go vet`, staticcheck, `go test -race`, 30 s of each fuzz target |
| `make mutation` | gremlins; every viable mutant must be killed (`.gremlins.yaml`) |
| `make vectors OUT=dir` | write the Go-produced conformance vectors (`cmd/pparquet-go-gen`) |

`TestConformanceVectors` checks every `../testdata/files/*.parquet` against
`../testdata/expected/<name>.jsonl` (or `$PPARQUET_TESTDATA`) and skips while there are none.
