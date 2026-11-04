//go:build pparquet_external

package pparquet

// With the pparquet_external build tag the library comes from the environment: set
// CGO_LDFLAGS to the path of libpparquet_capi.a (the Makefile does this from
// PPARQUET_LIB_DIR), for example
//
//	CGO_LDFLAGS=/opt/pcodec/lib/libpparquet_capi.a go build -tags pparquet_external ./...
//
// cgo directives cannot read environment variables, hence the tag. The system libraries
// the Rust static library needs follow it on the link line.

/*
#cgo linux LDFLAGS: -lgcc_s -lutil -lrt -lpthread -lm -ldl
#cgo darwin LDFLAGS: -lm
*/
import "C"
