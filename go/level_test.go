package pparquet_test

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"

	pparquet "github.com/aperiodic-io/pparquet/go"
)

func TestLevelPacking(t *testing.T) {
	cases := []struct {
		typ  parquet.Type
		lvl  int
		want int
	}{
		{parquet.Types.Int32, compress.DefaultCompressionLevel, 0x5043_0100},
		{parquet.Types.Int64, 0, 0x5043_0201},
		{parquet.Types.Double, 12, 0x5043_050d},
		{parquet.Types.Boolean, 8, 0x5043_0009},
		{parquet.Types.FixedLenByteArray, 8, 0x5043_0709},
	}
	for _, tc := range cases {
		if got := pparquet.Level(tc.typ, tc.lvl); got != tc.want {
			t.Errorf("Level(%v, %d) = %#x, want %#x", tc.typ, tc.lvl, got, tc.want)
		}
	}
}

func TestLevelRejectsBadArguments(t *testing.T) {
	cases := []struct {
		typ parquet.Type
		lvl int
		msg string
	}{
		{parquet.Types.Boolean - 1, 8, "pcodec: -1 is not a Parquet physical type"},
		{parquet.Types.Undefined, 8, "pcodec: 8 is not a Parquet physical type"},
		{parquet.Types.Int64, -2, "pcodec: pcodec level -2 is not in 0..12"},
		{parquet.Types.Int64, 13, "pcodec: pcodec level 13 is not in 0..12"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.typ, tc.lvl), func(t *testing.T) {
			defer func() {
				if r := recover(); r != tc.msg {
					t.Fatalf("panic %v, want %q", r, tc.msg)
				}
			}()
			pparquet.Level(tc.typ, tc.lvl)
		})
	}
}

func TestLinkedABIVersion(t *testing.T) {
	if v := pparquet.LinkedABIVersion(); v != 1 {
		t.Fatalf("linked ABI version %d, this binding speaks 1", v)
	}
}
