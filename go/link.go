//go:build !pparquet_external

package pparquet

// The default build links the static library that `cargo build --release -p
// pparquet-capi` writes into the repository's target directory. Build with
// `-tags pparquet_external` to supply the library yourself (see link_external.go).
//
// The system libraries the Rust static library needs (cargo rustc -- --print
// native-static-libs) must follow it on the link line, so they are listed after it.

/*
#cgo LDFLAGS: ${SRCDIR}/../target/release/libpparquet_capi.a
#cgo linux LDFLAGS: -lgcc_s -lutil -lrt -lpthread -lm -ldl
#cgo darwin LDFLAGS: -lm
*/
import "C"
