# parquet-pcodec for Go

PCODEC ([spec](../spec/FORMAT.md)) for [Apache arrow-go](https://github.com/apache/arrow-go)
v18.8.0: a Parquet compression codec that runs [pcodec](https://github.com/pcodec/pcodec) on
numeric columns. No fork of arrow-go is needed: the package registers the codec with
`compress.RegisterCodec` on import.

```go
import pcodec "github.com/aperiodic-io/parquet-pcodec/go"

// Read: importing the package is enough for file.Reader and pqarrow.

// Write with automatic per-column selection (spec section 6):
w, err := pcodec.NewFileWriter(schema, out)        // Auto, base codec ZSTD(3)
err = w.Write(firstBatch)                           // runs the trial, then writes
err = w.Close()

// Or force codecs, and get plain arrow-go writer properties:
plan, err := pcodec.NewPlan(schema, nil,
	pcodec.WithDefaultPolicy(pcodec.Pcodec),
	pcodec.WithPolicy("venue", pcodec.Base),
	pcodec.WithPcodecLevel(8))
props := parquet.NewWriterProperties(plan.Properties()...)
```

See the package documentation (`go doc`) for the policies, how the type hint travels in the
compression level, and the runnable examples.

## Building

The package is a cgo binding of the Rust C ABI in `rust/capi`. It needs a C compiler and the
static library `libparquet_pcodec.a`.

**In a checkout of this repository** (or with a `replace` directive pointing at one), build
the library and use the package as is; it links `../target/release/libparquet_pcodec.a`:

```sh
cargo build --release -p parquet-pcodec-capi   # or: make -C go lib
cd go && go test ./...
```

**From the module cache**, that relative path does not exist. Build the library once from a
checkout of the same version, then link it by path with the `pcodec_external` build tag:

```sh
cargo build --release -p parquet-pcodec-capi
export CGO_LDFLAGS=$PWD/target/release/libparquet_pcodec.a
go build -tags pcodec_external ./...
```

The Makefile does the same with `make test PCODEC_LIB_DIR=/path/to/dir`. On Linux the package
adds the system libraries the Rust library needs (`-lgcc_s -lutil -lrt -lpthread -lm -ldl`).
Only linux/amd64 is tested. Linking a second Rust static library into the same binary can
clash on Rust runtime symbols.

## Checks

| | |
|---|---|
| `make test` | unit, integration, property and acceptance tests, conformance vectors |
| `make check` | `go vet`, staticcheck, `go test -race`, 30 s of each fuzz target |
| `make mutation` | gremlins; every viable mutant must be killed (`.gremlins.yaml`) |
| `make vectors OUT=dir` | write the Go-produced conformance vectors (`cmd/pcodec-go-gen`) |

`TestConformanceVectors` checks every `../testdata/files/*.parquet` against
`../testdata/expected/<name>.jsonl` (or `$PCODEC_TESTDATA`) and skips while there are none.
