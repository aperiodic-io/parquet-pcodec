//go:build pcodec_external

package pcodec

// With the pcodec_external build tag the library comes from the environment: set
// CGO_LDFLAGS to the path of libparquet_pcodec.a (the Makefile does this from
// PCODEC_LIB_DIR), for example
//
//	CGO_LDFLAGS=/opt/pcodec/lib/libparquet_pcodec.a go build -tags pcodec_external ./...
//
// cgo directives cannot read environment variables, hence the tag. The system libraries
// the Rust static library needs follow it on the link line.

/*
#cgo linux LDFLAGS: -lgcc_s -lutil -lrt -lpthread -lm -ldl
#cgo darwin LDFLAGS: -lm
*/
import "C"
