//go:build tparquet_external

package tparquet

// With the tparquet_external build tag the library comes from the environment: set
// CGO_LDFLAGS to the path of libtparquet_capi.a (the Makefile does this from
// TPARQUET_LIB_DIR), for example
//
//	CGO_LDFLAGS=/opt/pcodec/lib/libtparquet_capi.a go build -tags tparquet_external ./...
//
// cgo directives cannot read environment variables, hence the tag. The system libraries
// the Rust static library needs follow it on the link line.

/*
#cgo linux LDFLAGS: -lgcc_s -lutil -lrt -lpthread -lm -ldl
#cgo darwin LDFLAGS: -lm
*/
import "C"
