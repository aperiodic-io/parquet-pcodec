package pcodec_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"

	pcodec "github.com/aperiodic-io/parquet-pcodec/go"
)

// levelSpy records the level arrow-go passes to EncodeLevel.
type levelSpy struct {
	compress.Codec
	levels []int
}

func (s *levelSpy) EncodeLevel(dst, src []byte, level int) []byte {
	s.levels = append(s.levels, level)
	return s.Codec.EncodeLevel(dst, src, level)
}

// TestArrowGoPassesThePackedLevel: arrow-go hands the per-column level to EncodeLevel
// unchanged, for every page, without validating or clamping it for a codec it does not know.
func TestArrowGoPassesThePackedLevel(t *testing.T) {
	real, _ := compress.GetCodec(pcodec.Compression)
	spy := &levelSpy{Codec: real}
	compress.RegisterCodec(pcodec.Compression, spy)
	t.Cleanup(func() { compress.RegisterCodec(pcodec.Compression, real) })

	sc := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := seededRecord(sc, 5000, 3)
	defer rec.Release()
	writeWithProps(t, []arrow.RecordBatch{rec}, append(pcodec.ColumnProperties("x", parquet.Types.Int64, 11),
		parquet.WithDataPageSize(4096))...)
	if len(spy.levels) < 2 {
		t.Fatalf("%d pages encoded", len(spy.levels))
	}
	for _, l := range spy.levels {
		if l != pcodec.Level(parquet.Types.Int64, 11) {
			t.Fatalf("level %#x reached the codec", l)
		}
	}
}

// decodeOnly hides DecodeWithError, as a codec written for the older interface would.
type decodeOnly struct{ compress.Codec }

// TestArrowGoRecoversADecodePanic: had the codec only Decode, arrow-go's compress.Decode
// would still turn its panic into an error.
func TestArrowGoRecoversADecodePanic(t *testing.T) {
	real, _ := compress.GetCodec(pcodec.Compression)
	_, err := compress.Decode(decodeOnly{real}, make([]byte, 8), []byte{9, 0})
	if err == nil || !strings.Contains(err.Error(), "unsupported version 9") {
		t.Fatalf("err = %v", err)
	}
}
