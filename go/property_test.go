package pcodec_test

import (
	"bytes"
	"fmt"
	"math"
	mrand "math/rand"
	"math/rand/v2"
	"testing"
	"testing/quick"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	pcodec "github.com/aperiodic-io/parquet-pcodec/go"
)

// TestQuickRoundTripAnyBytes: any bytes, any hint, any level decode to themselves.
func TestQuickRoundTripAnyBytes(t *testing.T) {
	c := codec(t)
	f := func(data []byte, typ uint8, lvl uint8) bool {
		level := pcodec.Level(parquet.Type(typ%8), int(lvl%14)-1)
		enc := c.EncodeLevel(nil, data, level)
		return bytes.Equal(c.Decode(make([]byte, len(data)), enc), data)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 1000, Rand: mrand.New(mrand.NewSource(1))}); err != nil {
		t.Fatal(err)
	}
}

// numbers is a typed column of structured values: what PCODEC is for.
type numbers struct {
	typ  parquet.Type
	data []byte
}

func genNumbers(r *rand.Rand, n int) numbers {
	types := []parquet.Type{parquet.Types.Int32, parquet.Types.Int64, parquet.Types.Float, parquet.Types.Double}
	typ := types[r.IntN(len(types))]
	exp := math.Pow10(r.IntN(6))
	var buf bytes.Buffer
	x := r.Float64() * 1000
	for i := 0; i < n; i++ {
		x += math.Round(r.NormFloat64()*10) / exp
		v := x
		if r.IntN(100) == 0 {
			v = math.Float64frombits(r.Uint64()) // an exception: any bits
		}
		switch typ {
		case parquet.Types.Int32:
			buf.Write(le32(uint32(int32(v))))
		case parquet.Types.Int64:
			buf.Write(le64(uint64(int64(v * exp))))
		case parquet.Types.Float:
			buf.Write(le32(math.Float32bits(float32(v))))
		default:
			buf.Write(le64(math.Float64bits(v)))
		}
	}
	return numbers{typ, buf.Bytes()}
}

func le32(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }
func le64(v uint64) []byte { return append(le32(uint32(v)), le32(uint32(v>>32))...) }

// TestSeededRoundTripTypedColumns: structured numeric pages (random walks on decimal grids,
// with exceptions) round-trip with their real hint, in PCO and DECIMAL mode.
func TestSeededRoundTripTypedColumns(t *testing.T) {
	c := codec(t)
	r := rand.New(rand.NewPCG(7, 7))
	for i := 0; i < 300; i++ {
		col := genNumbers(r, r.IntN(5000))
		for _, lvl := range []int{0, -1} {
			enc := c.EncodeLevel(nil, col.data, pcodec.Level(col.typ, lvl))
			if !bytes.Equal(c.Decode(make([]byte, len(col.data)), enc), col.data) {
				t.Fatalf("case %d (%v, %d values) does not round-trip", i, col.typ, len(col.data))
			}
		}
	}
}

// TestSeededFiles writes random files (schema allTypes, random policies, row counts, row
// group lengths and page versions) with FileWriter and reads them back.
func TestSeededFiles(t *testing.T) {
	paths := leaves(t, allTypes, parquet.WithStoreDecimalAsInteger(true))
	for seed := uint64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			r := rand.New(rand.NewPCG(seed, 3))
			opts := []pcodec.Option{
				pcodec.WithDefaultPolicy(pcodec.Policy(r.IntN(3))),
				pcodec.WithPcodecLevel(r.IntN(14) - 1),
				pcodec.WithArrowWriterProperties(pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())),
				pcodec.WithWriterProperties(
					parquet.WithStoreDecimalAsInteger(true),
					parquet.WithMaxRowGroupLength(int64(100+r.IntN(2000))),
					parquet.WithDataPageSize(int64(512+r.IntN(8192))),
					// arrow-go's dictionary encoder folds NaN payloads (see
					// TestRoundTripEveryTypeAndPageKind); keep float columns plain.
					parquet.WithDictionaryFor("f32", false), parquet.WithDictionaryFor("f64", false),
					parquet.WithDictionaryFor("f64n", false), parquet.WithDictionaryFor("lf64.list.element", false),
					parquet.WithDictionaryFor("st.c.list.element", false)),
			}
			if r.IntN(2) == 0 {
				opts = append(opts, pcodec.WithBaseCodec(compress.Codecs.Snappy, compress.DefaultCompressionLevel))
			}
			for path := range paths {
				if r.IntN(3) == 0 {
					opts = append(opts, pcodec.WithPolicy(path, pcodec.Policy(r.IntN(3))))
				}
			}
			var recs []arrow.RecordBatch
			for i := 0; i < 1+r.IntN(3); i++ {
				recs = append(recs, seededRecord(allTypes, r.IntN(2500), seed*10+uint64(i)))
			}
			defer func() {
				for _, rec := range recs {
					rec.Release()
				}
			}()
			var buf bytes.Buffer
			w, err := pcodec.NewFileWriter(allTypes, &buf, opts...)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range recs {
				if err := w.Write(rec); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			md := fileMetadata(t, buf.Bytes())
			_, marked := footerValue(md, pcodec.FooterKey)
			if marked != w.Plan().UsesPcodec() {
				t.Fatalf("footer key %v, plan uses PCODEC %v", marked, w.Plan().UsesPcodec())
			}
			for path, c := range columnCodecs(t, md) {
				want, _ := w.Plan().Codec(path)
				if (c == "PCODEC") != (want == pcodec.Compression) {
					t.Fatalf("%s written as %s, planned %v", path, c, want)
				}
			}
			assertSameData(t, buf.Bytes(), recs...)
		})
	}
}
