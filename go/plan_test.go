package pcodec_test

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	pcodec "github.com/aperiodic-io/parquet-pcodec/go"
)

func TestColumnProperties(t *testing.T) {
	props := parquet.NewWriterProperties(pcodec.ColumnProperties("a.b", parquet.Types.Double, 5)...)
	if props.CompressionFor("a.b") != pcodec.Compression {
		t.Error("codec")
	}
	if props.CompressionLevelFor("a.b") != pcodec.Level(parquet.Types.Double, 5) {
		t.Error("level")
	}
	if props.EncodingFor("a.b") != parquet.Encodings.Plain {
		t.Error("encoding")
	}
	if props.DictionaryEnabledFor("a.b") {
		t.Error("dictionary")
	}
	if props.CompressionFor("other") != compress.Codecs.Uncompressed || !props.DictionaryEnabledFor("other") {
		t.Error("other columns changed")
	}
}

// mixedSchema has every kind of leaf: eligible numbers at the top, in a struct and in a list,
// and leaves PCODEC does not model.
var mixedSchema = arrow.NewSchema([]arrow.Field{
	{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
	{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "flag", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "s", Type: arrow.StructOf(arrow.Field{Name: "qty", Type: arrow.PrimitiveTypes.Int32})},
	{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
}, nil)

var eligibleLeaves = []string{"ts", "price", "s.qty", "l.list.element"}

func newPlan(t *testing.T, sc *arrow.Schema, sample arrow.RecordBatch, opts ...pcodec.Option) *pcodec.Plan {
	t.Helper()
	p, err := pcodec.NewPlan(sc, sample, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func wantCodecs(t *testing.T, p *pcodec.Plan, want map[string]compress.Compression) {
	t.Helper()
	for path, c := range want {
		got, ok := p.Codec(path)
		if !ok || got != c {
			t.Errorf("Codec(%q) = %v, %v, want %v", path, got, ok, c)
		}
	}
}

func TestPlanPcodecPolicy(t *testing.T) {
	p := newPlan(t, mixedSchema, nil, pcodec.WithDefaultPolicy(pcodec.Pcodec))
	zstd := compress.Codecs.Zstd
	wantCodecs(t, p, map[string]compress.Compression{"ts": pcodec.Compression, "price": pcodec.Compression,
		"s.qty": pcodec.Compression, "l.list.element": pcodec.Compression, "name": zstd, "flag": zstd})
	if got := strings.Join(p.PcodecColumns(), ","); got != strings.Join(eligibleLeaves, ",") {
		t.Errorf("PcodecColumns = %s", got)
	}
	if !p.UsesPcodec() {
		t.Error("UsesPcodec")
	}
	if _, ok := p.Codec("nope"); ok {
		t.Error("Codec of an unknown path")
	}

	props := parquet.NewWriterProperties(p.Properties()...)
	if props.DataPageVersion() != parquet.DataPageV2 {
		t.Error("PCODEC files use data page V2")
	}
	types := map[string]parquet.Type{"ts": parquet.Types.Int64, "price": parquet.Types.Double,
		"s.qty": parquet.Types.Int32, "l.list.element": parquet.Types.Float}
	for path, typ := range types {
		if props.CompressionFor(path) != pcodec.Compression || props.CompressionLevelFor(path) != pcodec.Level(typ, -1) ||
			props.DictionaryEnabledFor(path) || props.EncodingFor(path) != parquet.Encodings.Plain {
			t.Errorf("%s: properties %v level %#x", path, props.CompressionFor(path), props.CompressionLevelFor(path))
		}
	}
	for _, path := range []string{"name", "flag"} {
		if props.CompressionFor(path) != zstd || props.CompressionLevelFor(path) != 3 || !props.DictionaryEnabledFor(path) {
			t.Errorf("%s: base properties %v %d", path, props.CompressionFor(path), props.CompressionLevelFor(path))
		}
	}
}

func TestPlanBasePolicyAndOptions(t *testing.T) {
	p := newPlan(t, mixedSchema, nil,
		pcodec.WithDefaultPolicy(pcodec.Base),
		pcodec.WithPolicy("price", pcodec.Pcodec),
		pcodec.WithPolicy("name", pcodec.Pcodec), // forcing PCODEC on any column is allowed
		pcodec.WithBaseCodec(compress.Codecs.Snappy, 7),
		pcodec.WithPcodecLevel(4),
		pcodec.WithWriterProperties(parquet.WithMaxRowGroupLength(77)))
	snappy := compress.Codecs.Snappy
	wantCodecs(t, p, map[string]compress.Compression{"ts": snappy, "price": pcodec.Compression,
		"name": pcodec.Compression, "s.qty": snappy, "l.list.element": snappy, "flag": snappy})
	props := parquet.NewWriterProperties(p.Properties()...)
	if props.CompressionFor("ts") != snappy || props.CompressionLevelFor("ts") != 7 {
		t.Error("base codec")
	}
	if props.CompressionLevelFor("price") != pcodec.Level(parquet.Types.Double, 4) {
		t.Error("pcodec level")
	}
	if props.CompressionLevelFor("name") != pcodec.Level(parquet.Types.ByteArray, 4) {
		t.Error("the hint of a forced column is its physical type")
	}
	if props.MaxRowGroupLength() != 77 {
		t.Error("the user's writer properties are kept")
	}
}

func TestPlanWithoutPcodecKeepsV1(t *testing.T) {
	p := newPlan(t, mixedSchema, nil, pcodec.WithDefaultPolicy(pcodec.Base))
	if p.UsesPcodec() || len(p.PcodecColumns()) != 0 {
		t.Fatal("no column should use PCODEC")
	}
	if parquet.NewWriterProperties(p.Properties()...).DataPageVersion() != parquet.DataPageV1 {
		t.Fatal("the data page version is left alone without PCODEC columns")
	}
}

func TestPlanAutoWithoutSampleIsBase(t *testing.T) {
	p := newPlan(t, mixedSchema, nil)
	if p.UsesPcodec() {
		t.Fatalf("Auto with no data chose PCODEC for %v", p.PcodecColumns())
	}
}

func TestPlanUsesStoreDecimalAsInteger(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{{Name: "d", Type: &arrow.Decimal128Type{Precision: 9, Scale: 2}}}, nil)
	p := newPlan(t, sc, nil, pcodec.WithDefaultPolicy(pcodec.Pcodec))
	if p.UsesPcodec() {
		t.Fatal("a decimal stored as FIXED_LEN_BYTE_ARRAY is not eligible")
	}
	p = newPlan(t, sc, nil, pcodec.WithDefaultPolicy(pcodec.Pcodec),
		pcodec.WithWriterProperties(parquet.WithStoreDecimalAsInteger(true)))
	if !p.UsesPcodec() {
		t.Fatal("a decimal stored as INT32 is eligible")
	}
	props := parquet.NewWriterProperties(p.Properties()...)
	if props.CompressionLevelFor("d") != pcodec.Level(parquet.Types.Int32, -1) {
		t.Fatal("hint")
	}
}

func TestPlanErrors(t *testing.T) {
	other := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int8}}, nil)
	rec := randomRecord(other, 3)
	defer rec.Release()
	cases := []struct {
		name   string
		sample arrow.RecordBatch
		opts   []pcodec.Option
		msg    string
	}{
		{"unknown column", nil, []pcodec.Option{pcodec.WithPolicy("pricee", pcodec.Pcodec)}, `pcodec: no leaf column "pricee"`},
		{"bad column policy", nil, []pcodec.Option{pcodec.WithPolicy("ts", pcodec.Policy(3))}, "pcodec: invalid policy 3"},
		{"negative policy", nil, []pcodec.Option{pcodec.WithDefaultPolicy(pcodec.Policy(-1))}, "pcodec: invalid policy -1"},
		{"bad default policy", nil, []pcodec.Option{pcodec.WithDefaultPolicy(pcodec.Policy(3))}, "pcodec: invalid policy 3"},
		{"level too high", nil, []pcodec.Option{pcodec.WithPcodecLevel(13)}, "pcodec: pcodec level 13 is not in 0..12"},
		{"level too low", nil, []pcodec.Option{pcodec.WithPcodecLevel(-2)}, "pcodec: pcodec level -2 is not in 0..12"},
		{"sample schema", rec, nil, "pcodec: the sample's schema differs from the file's"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pcodec.NewPlan(mixedSchema, tc.sample, tc.opts...)
			if err == nil || err.Error() != tc.msg {
				t.Fatalf("err = %v, want %q", err, tc.msg)
			}
		})
	}
	// pqarrow's own schema errors are passed on.
	bad := arrow.NewSchema([]arrow.Field{{Name: "u", Type: arrow.DenseUnionOf(nil, nil)}}, nil)
	if _, err := pcodec.NewPlan(bad, nil); err == nil {
		t.Fatal("a union column should be refused")
	}
	// Levels 0 and 12 are valid.
	for _, l := range []int{0, 12, -1} {
		if _, err := pcodec.NewPlan(mixedSchema, nil, pcodec.WithPcodecLevel(l)); err != nil {
			t.Fatalf("level %d: %v", l, err)
		}
	}
}

// autoRecord has a smooth timestamp column (PCODEC wins), a column cycling through 16
// random 64-bit values (dictionary + ZSTD wins) and a constant-width string column.
func autoRecord(n int) arrow.RecordBatch {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
		{Name: "cycle", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	r := rand.New(rand.NewPCG(1, 2))
	var cyc [16]int64
	for i := range cyc {
		cyc[i] = int64(r.Uint64())
	}
	ts := int64(1_700_000_000_000)
	for i := 0; i < n; i++ {
		ts += int64(r.IntN(50))
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts))
		b.Field(1).(*array.Int64Builder).Append(cyc[i%16])
		b.Field(2).(*array.StringBuilder).Append("abc")
	}
	return b.NewRecordBatch()
}

func TestPlanAutoPicksTheSmallerCodec(t *testing.T) {
	rec := autoRecord(20000)
	defer rec.Release()
	p := newPlan(t, rec.Schema(), rec)
	wantCodecs(t, p, map[string]compress.Compression{"ts": pcodec.Compression,
		"cycle": compress.Codecs.Zstd, "name": compress.Codecs.Zstd})
	// A forced policy wins over the trial.
	p = newPlan(t, rec.Schema(), rec, pcodec.WithPolicy("ts", pcodec.Base), pcodec.WithPolicy("cycle", pcodec.Pcodec))
	wantCodecs(t, p, map[string]compress.Compression{"ts": compress.Codecs.Zstd, "cycle": pcodec.Compression})
}

func TestPlanAutoComparesWholeColumnChunks(t *testing.T) {
	// With small row groups the sample is several row groups; the trial sums them.
	rec := autoRecord(20000)
	defer rec.Release()
	p := newPlan(t, rec.Schema(), rec, pcodec.WithWriterProperties(parquet.WithMaxRowGroupLength(3000)))
	wantCodecs(t, p, map[string]compress.Compression{"ts": pcodec.Compression, "cycle": compress.Codecs.Zstd})
}

func TestPlanAutoEmptySampleIsBase(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	rec := b.NewRecordBatch() // no rows: no data to compare
	defer rec.Release()
	if p := newPlan(t, sc, rec); p.UsesPcodec() {
		t.Fatal("an empty sample should keep the base codec")
	}
}

func TestPlanArrowWriterProperties(t *testing.T) {
	// Coercing timestamps changes nothing about the leaves, but the properties must reach
	// pqarrow: with a nanosecond column and no truncation allowed, coercing to ms fails.
	sc := arrow.NewSchema([]arrow.Field{{Name: "t", Type: arrow.FixedWidthTypes.Timestamp_ns}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1))
	rec := b.NewRecordBatch()
	defer rec.Release()
	_, err := pcodec.NewPlan(sc, rec, pcodec.WithArrowWriterProperties(
		pqarrow.NewArrowWriterProperties(pqarrow.WithCoerceTimestamps(arrow.Millisecond))))
	if err == nil {
		t.Fatal("the trial should use the arrow writer properties and fail on lossy coercion")
	}
}

// v2Penalty has a column where PCODEC wins (t), and order-book price lists that V2 pages
// make larger: V2 pages store repetition and definition levels uncompressed.
func v2Penalty(n int) arrow.RecordBatch {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "t", Type: arrow.PrimitiveTypes.Int64},
		{Name: "bids", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)},
		{Name: "asks", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	r := rand.New(rand.NewPCG(12, 0))
	mid := int64(6_500_000)
	for i := 0; i < n; i++ {
		mid += int64(r.IntN(5)) - 2
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		for f, side := range []int64{-1, 1} {
			lb := b.Field(1 + f).(*array.ListBuilder)
			lb.Append(true)
			vb := lb.ValueBuilder().(*array.Float64Builder)
			for k := int64(0); k < 20; k++ {
				p := mid - k
				if side > 0 {
					p = mid + 1 + k
				}
				vb.Append(float64(p) / 100)
			}
		}
	}
	return b.NewRecordBatch()
}

func writeFile(t *testing.T, rec arrow.RecordBatch, opts ...pcodec.Option) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := pcodec.NewFileWriter(rec.Schema(), &buf, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func chunkSize(t *testing.T, data []byte, path string) int64 {
	t.Helper()
	md := fileMetadata(t, data)
	var n int64
	for i := 0; i < md.NumRowGroups(); i++ {
		for j := 0; j < md.RowGroup(i).NumColumns(); j++ {
			cc, err := md.RowGroup(i).ColumnChunk(j)
			if err != nil {
				t.Fatal(err)
			}
			if cc.PathInSchema().String() == path {
				n += cc.TotalCompressedSize()
			}
		}
	}
	return n
}

func TestPlanAutoCountsTheCostOfV2Pages(t *testing.T) {
	rec := v2Penalty(4000)
	defer rec.Release()
	auto := writeFile(t, rec)
	base := writeFile(t, rec, pcodec.WithDefaultPolicy(pcodec.Base))
	pcoT := writeFile(t, rec, pcodec.WithDefaultPolicy(pcodec.Base), pcodec.WithPolicy("t", pcodec.Pcodec))
	// the premise: PCODEC alone is smaller on t, but makes the whole file larger (V2 pages)
	if chunkSize(t, pcoT, "t") >= chunkSize(t, base, "t") {
		t.Fatalf("t: PCODEC %d bytes, base %d", chunkSize(t, pcoT, "t"), chunkSize(t, base, "t"))
	}
	if len(pcoT) <= len(base) {
		t.Fatalf("PCODEC on t: %d bytes, base: %d; V2 pages should cost more than it saves", len(pcoT), len(base))
	}
	for path, c := range columnCodecs(t, fileMetadata(t, auto)) {
		if c != "ZSTD" {
			t.Errorf("Auto: %s is %s, want ZSTD", path, c)
		}
	}
	if len(auto) != len(base) {
		t.Errorf("Auto: %d bytes, base: %d", len(auto), len(base))
	}
	assertSameData(t, auto, rec)
}

func TestPlanAutoKeepsPcodecWhenTheWholeFileIsSmaller(t *testing.T) {
	// the same comparison, where V2 pages cost nothing: no lists, so PCODEC stays on ts
	rec := autoRecord(20000)
	defer rec.Release()
	auto := writeFile(t, rec)
	base := writeFile(t, rec, pcodec.WithDefaultPolicy(pcodec.Base))
	if got := columnCodecs(t, fileMetadata(t, auto))["ts"]; got != "PCODEC" {
		t.Fatalf("ts: %s, want PCODEC", got)
	}
	if len(auto) >= len(base) {
		t.Fatalf("Auto: %d bytes, base: %d", len(auto), len(base))
	}
}
