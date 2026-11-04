package tparquet_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"

	tparquet "github.com/aperiodic-io/tparquet/go"
)

func int64Bytes(vs ...int64) []byte {
	b := make([]byte, 8*len(vs))
	for i, v := range vs {
		binary.LittleEndian.PutUint64(b[8*i:], uint64(v))
	}
	return b
}

func float64Bytes(vs ...float64) []byte {
	b := make([]byte, 8*len(vs))
	for i, v := range vs {
		binary.LittleEndian.PutUint64(b[8*i:], math.Float64bits(v))
	}
	return b
}

func ramp(n int) []byte {
	vs := make([]int64, n)
	for i := range vs {
		vs[i] = 1_700_000_000_000 + int64(i)*1000
	}
	return int64Bytes(vs...)
}

func codec(t *testing.T) compress.Codec {
	t.Helper()
	c, err := compress.GetCodec(tparquet.Compression)
	if err != nil {
		t.Fatalf("PCODEC is not registered: %v", err)
	}
	return c
}

func TestCompressionValue(t *testing.T) {
	if tparquet.Compression != compress.Compression(20547) || tparquet.CodecID != 20547 {
		t.Fatalf("Compression = %d, CodecID = %d, want 20547", tparquet.Compression, tparquet.CodecID)
	}
	if tparquet.CodecID != tparquet.LinkedCodecID() {
		t.Fatalf("the linked library says the codec id is %d", tparquet.LinkedCodecID())
	}
}

func TestFooterKeyValue(t *testing.T) {
	if tparquet.FooterKey != "parquet.pcodec.version" || tparquet.FooterValue != "1" {
		t.Fatalf("footer pair %q=%q", tparquet.FooterKey, tparquet.FooterValue)
	}
}

func TestRegisteredOnImport(t *testing.T) {
	c := codec(t)
	if _, ok := c.(compress.ErrorCodec); !ok {
		t.Fatal("the codec should implement compress.ErrorCodec so arrow-go reports decode errors without a panic")
	}
	// arrow-go streams pages only through a StreamingCodec; the payload is not a stream.
	if _, ok := c.(compress.StreamingCodec); ok {
		t.Fatal("the codec must not implement compress.StreamingCodec")
	}
}

func roundTrip(t *testing.T, src []byte, level int) []byte {
	t.Helper()
	c := codec(t)
	enc := c.EncodeLevel(nil, src, level)
	if int64(len(enc)) > c.CompressBound(int64(len(src))) {
		t.Fatalf("payload of %d bytes exceeds CompressBound %d", len(enc), c.CompressBound(int64(len(src))))
	}
	dec := c.Decode(make([]byte, len(src)), enc)
	if !bytes.Equal(dec, src) {
		t.Fatalf("round trip changed the bytes (level %d)", level)
	}
	return enc
}

func TestRoundTripEveryHint(t *testing.T) {
	src := ramp(1000)
	for _, typ := range []parquet.Type{parquet.Types.Boolean, parquet.Types.Int32, parquet.Types.Int64,
		parquet.Types.Int96, parquet.Types.Float, parquet.Types.Double, parquet.Types.ByteArray,
		parquet.Types.FixedLenByteArray} {
		for _, lvl := range []int{compress.DefaultCompressionLevel, 0, 4, 12} {
			roundTrip(t, src, tparquet.Level(typ, lvl))
		}
	}
	for _, lvl := range []int{compress.DefaultCompressionLevel, 0, 3, 9, 22, -7, math.MaxInt32} {
		roundTrip(t, src, lvl)
	}
}

func TestPayloadModes(t *testing.T) {
	src := ramp(1000)
	cases := []struct {
		name  string
		src   []byte
		level int
		mode  byte
	}{
		{"int64 hint uses PCO", src, tparquet.Level(parquet.Types.Int64, 8), 1},
		{"decimal doubles use DECIMAL", float64Bytes(1.25, 3.5, 100.75, 2.0, 7.125, 9.5, 1.5, 3.25), tparquet.Level(parquet.Types.Double, 8), 2},
		{"no hint falls back to ZSTD", src, compress.DefaultCompressionLevel, 3},
		{"a plain level is no hint", src, 8, 3},
		{"a byte-array hint is no hint", src, tparquet.Level(parquet.Types.ByteArray, 8), 3},
		{"empty input is RAW", nil, tparquet.Level(parquet.Types.Int64, 8), 0},
		{"a width that does not divide falls back", src[:len(src)-3], tparquet.Level(parquet.Types.Int64, 8), 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc := roundTrip(t, tc.src, tc.level)
			if enc[0] != 1 || enc[1] != tc.mode {
				t.Fatalf("payload header %v, want version 1 mode %d", enc[:2], tc.mode)
			}
			if tc.mode == 1 && enc[2] != byte(parquet.Types.Int64) {
				t.Fatalf("dtype %d", enc[2])
			}
		})
	}
}

func TestEncodeIsEncodeLevelDefault(t *testing.T) {
	c := codec(t)
	src := ramp(300)
	if !bytes.Equal(c.Encode(nil, src), c.EncodeLevel(nil, src, compress.DefaultCompressionLevel)) {
		t.Fatal("Encode differs from EncodeLevel with the default level")
	}
}

func TestPcodecLevelReachesTheEncoder(t *testing.T) {
	c := codec(t)
	vs := make([]int64, 20000)
	x := int64(1)
	for i := range vs {
		x = x*6364136223846793005 + 1442695040888963407
		vs[i] = x >> 40
	}
	src := int64Bytes(vs...)
	l0 := c.EncodeLevel(nil, src, tparquet.Level(parquet.Types.Int64, 0))
	l12 := c.EncodeLevel(nil, src, tparquet.Level(parquet.Types.Int64, 12))
	def := c.EncodeLevel(nil, src, tparquet.Level(parquet.Types.Int64, compress.DefaultCompressionLevel))
	l8 := c.EncodeLevel(nil, src, tparquet.Level(parquet.Types.Int64, 8))
	if bytes.Equal(l0, l12) {
		t.Fatal("levels 0 and 12 gave the same payload: the level is not passed on")
	}
	if !bytes.Equal(def, l8) {
		t.Fatal("the default level should be pcodec's default, 8")
	}
}

func TestEncodeUsesDstWhenItFits(t *testing.T) {
	c := codec(t)
	src := ramp(100)
	dst := make([]byte, 0, c.CompressBound(int64(len(src))))
	enc := c.EncodeLevel(dst, src, tparquet.Level(parquet.Types.Int64, 8))
	if &enc[0] != &dst[:1][0] {
		t.Fatal("the payload should be written into dst when its capacity is the bound")
	}
	small := make([]byte, 0, 4)
	enc2 := c.EncodeLevel(small, src, tparquet.Level(parquet.Types.Int64, 8))
	if !bytes.Equal(enc, enc2) {
		t.Fatal("a small dst changes the payload")
	}
	if &enc2[0] == &small[:1][0] {
		t.Fatal("a too-small dst must not be written to")
	}
}

func TestCompressBound(t *testing.T) {
	c := codec(t)
	for _, n := range []int64{0, 1, 7, 1 << 20} {
		if got, want := c.CompressBound(n), tparquet.LinkedCompressBound(n); got != want {
			t.Fatalf("CompressBound(%d) = %d, the library says %d", n, got, want)
		}
	}
}

func TestDecodeErrors(t *testing.T) {
	c := codec(t).(compress.ErrorCodec)
	good := c.EncodeLevel(nil, ramp(100), tparquet.Level(parquet.Types.Int64, 8))
	cases := []struct {
		name    string
		payload []byte
		dstLen  int
		msg     string
	}{
		{"empty payload", nil, 8, "truncated"},
		{"bad version", []byte{9, 0}, 0, "unsupported version 9"},
		{"bad mode", []byte{1, 77}, 0, "unknown mode 77"},
		{"wrong size", good, 799, "not a multiple"},
		{"too big", good, 808, "expected 808"},
		{"truncated", good[:len(good)/2], 800, "pcodec payload: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.DecodeWithError(make([]byte, tc.dstLen), tc.payload)
			if err == nil || !strings.Contains(err.Error(), tc.msg) || !strings.HasPrefix(err.Error(), "pcodec payload: ") {
				t.Fatalf("err = %v, want one mentioning %q", err, tc.msg)
			}
			func() {
				defer func() {
					r := recover()
					if e, ok := r.(error); !ok || e.Error() != err.Error() {
						t.Fatalf("Decode panicked with %v, want the error %v", r, err)
					}
				}()
				c.Decode(make([]byte, tc.dstLen), tc.payload)
			}()
		})
	}
}

func TestDecodeFillsExactlyDst(t *testing.T) {
	c := codec(t).(compress.ErrorCodec)
	src := ramp(64)
	enc := c.EncodeLevel(nil, src, tparquet.Level(parquet.Types.Int64, 8))
	dst := make([]byte, len(src), len(src)+100)
	got, err := c.DecodeWithError(dst, enc)
	if err != nil || !bytes.Equal(got, src) || &got[0] != &dst[0] {
		t.Fatalf("err %v; the result should be dst itself", err)
	}
	// An empty payload decodes into a nil dst.
	empty := c.EncodeLevel(nil, nil, tparquet.Level(parquet.Types.Int64, 8))
	got, err = c.DecodeWithError(nil, empty)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty: %v %v", got, err)
	}
}
