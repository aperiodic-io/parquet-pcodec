package pparquet_test

import (
	"errors"
	"os"
	"testing"

	"github.com/aperiodic-io/pparquet/go/internal/vectors"
)

// TestConformanceVectors reads every testdata/files/*.parquet of the repository (or of
// $PPARQUET_TESTDATA) and compares it with testdata/expected/<name>.jsonl.
func TestConformanceVectors(t *testing.T) {
	dir := os.Getenv("PPARQUET_TESTDATA")
	if dir == "" {
		dir = "../testdata"
	}
	n, err := vectors.Check(dir)
	if errors.Is(err, vectors.ErrNoVectors) {
		t.Skipf("no conformance vectors in %s (testdata/files/*.parquet); set PPARQUET_TESTDATA to check others", dir)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d vectors match", n)
}
