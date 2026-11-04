package pcodec_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"

	pcodec "github.com/aperiodic-io/parquet-pcodec/go"
)

func seedPayloads(f *testing.F) {
	c, _ := compress.GetCodec(pcodec.Compression)
	srcs := [][]byte{nil, ramp(50), float64Bytes(1.25, 3.5, 100.75, 2.0), []byte("not numbers at all")}
	for _, src := range srcs {
		for _, typ := range []parquet.Type{parquet.Types.Int32, parquet.Types.Int64, parquet.Types.Float, parquet.Types.Double} {
			f.Add(c.EncodeLevel(nil, src, pcodec.Level(typ, 8)), uint16(len(src)))
		}
		f.Add(c.Encode(nil, src), uint16(len(src)))
	}
}

// FuzzDecode: any payload and size either decodes to exactly that many bytes or fails; it
// never panics or writes past dst.
func FuzzDecode(f *testing.F) {
	seedPayloads(f)
	c, _ := compress.GetCodec(pcodec.Compression)
	ec := c.(compress.ErrorCodec)
	f.Fuzz(func(t *testing.T, payload []byte, size uint16) {
		buf := make([]byte, int(size)+16)
		for i := range buf {
			buf[i] = 0xa5
		}
		out, err := ec.DecodeWithError(buf[:size:size], payload)
		if err == nil && len(out) != int(size) {
			t.Fatalf("decoded %d bytes into a dst of %d", len(out), size)
		}
		if !bytes.Equal(buf[size:], bytes.Repeat([]byte{0xa5}, 16)) {
			t.Fatal("the decoder wrote past dst")
		}
	})
}

// FuzzRoundTrip: any input with any hint and level decodes back to itself.
func FuzzRoundTrip(f *testing.F) {
	f.Add(ramp(40), uint8(2), uint8(9))
	f.Add(float64Bytes(0.1, 0.2, 0.30000000000000004), uint8(5), uint8(0))
	f.Add([]byte{1, 2, 3}, uint8(1), uint8(13))
	c, _ := compress.GetCodec(pcodec.Compression)
	f.Fuzz(func(t *testing.T, data []byte, typ, lvl uint8) {
		level := pcodec.Level(parquet.Type(typ%8), int(lvl%14)-1)
		enc := c.EncodeLevel(nil, data, level)
		if int64(len(enc)) > c.CompressBound(int64(len(data))) {
			t.Fatalf("payload %d bytes over the bound", len(enc))
		}
		if dec := c.Decode(make([]byte, len(data)), enc); !bytes.Equal(dec, data) {
			t.Fatal("round trip changed the data")
		}
	})
}
