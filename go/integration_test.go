package tparquet_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	tparquet "github.com/aperiodic-io/tparquet/go"
)

// allTypes has a leaf of every Parquet physical type but INT96 (TestInt96), most logical
// types, nulls, lists, structs and a dictionary column. Timestamp[s] and date64 are left out:
// pqarrow stores them as timestamp[ms] and date32 and reads them back so, whatever the codec.
var allTypes = arrow.NewSchema([]arrow.Field{
	{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
	{Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
	{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
	{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
	{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
	{Name: "u32", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
	{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
	{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
	{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
	{Name: "f64n", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "str", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "bin", Type: arrow.BinaryTypes.Binary},
	{Name: "fsb", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}},
	{Name: "ts_ms", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
	{Name: "ts_us", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "ts_ns", Type: arrow.FixedWidthTypes.Timestamp_ns},
	{Name: "date32", Type: arrow.FixedWidthTypes.Date32},
	{Name: "time32", Type: arrow.FixedWidthTypes.Time32ms},
	{Name: "time64", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
	{Name: "dec9", Type: &arrow.Decimal128Type{Precision: 9, Scale: 2}},
	{Name: "dec18", Type: &arrow.Decimal128Type{Precision: 18, Scale: 4}, Nullable: true},
	{Name: "dec38", Type: &arrow.Decimal128Type{Precision: 38, Scale: 10}},
	{Name: "li64", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
	{Name: "lf64", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Float64, Nullable: true})},
	{Name: "st", Type: arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "c", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
	), Nullable: true},
	{Name: "dict", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}},
}, nil)

// leaves returns the Parquet leaf columns of sc as pqarrow writes it with props.
func leaves(t testing.TB, sc *arrow.Schema, props ...parquet.WriterProperty) map[string]parquet.Type {
	t.Helper()
	pq, err := pqarrow.ToParquet(sc, parquet.NewWriterProperties(props...), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]parquet.Type{}
	for i := 0; i < pq.NumColumns(); i++ {
		out[pq.Column(i).Path()] = pq.Column(i).PhysicalType()
	}
	return out
}

// allPcodec returns properties that write every leaf with PCODEC, with ColumnProperties or,
// with dict, keeping the dictionary (and so PCODEC dictionary pages) and default encodings.
func allPcodec(t testing.TB, sc *arrow.Schema, dict bool, base ...parquet.WriterProperty) []parquet.WriterProperty {
	props := append([]parquet.WriterProperty{}, base...)
	for path, typ := range leaves(t, sc, base...) {
		if dict {
			props = append(props, parquet.WithCompressionFor(path, tparquet.Compression),
				parquet.WithCompressionLevelFor(path, tparquet.Level(typ, 2)))
		} else {
			props = append(props, tparquet.ColumnProperties(path, typ, 2)...)
		}
	}
	return props
}

func TestRoundTripEveryTypeAndPageKind(t *testing.T) {
	recs := []arrow.RecordBatch{seededRecord(allTypes, 1500, 1), seededRecord(allTypes, 700, 2)}
	defer recs[0].Release()
	defer recs[1].Release()
	for _, version := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
		for _, dict := range []bool{false, true} {
			t.Run(fmt.Sprintf("V%d/dict=%v", version+1, dict), func(t *testing.T) {
				data := writeWithProps(t, recs, allPcodec(t, allTypes, dict,
					parquet.WithDataPageVersion(version),
					parquet.WithStoreDecimalAsInteger(true),
					parquet.WithDataPageSize(2048),
					parquet.WithMaxRowGroupLength(1000))...)
				md := fileMetadata(t, data)
				if md.NumRowGroups() != 3 {
					t.Fatalf("%d row groups", md.NumRowGroups())
				}
				for path, c := range columnCodecs(t, md) {
					if c != "PCODEC" {
						t.Fatalf("%s is %s", path, c)
					}
				}
				if !dict {
					assertSameData(t, data, recs...)
					return
				}
				// arrow-go's dictionary encoder folds every NaN payload into one
				// (internal/hashing/hash_funcs.go, parquet/internal/encoding/memo_table.go),
				// whatever the codec, so the reference is the same file written with ZSTD.
				ref := writeWithProps(t, recs, parquet.WithCompression(compress.Codecs.Zstd),
					parquet.WithDataPageVersion(version), parquet.WithStoreDecimalAsInteger(true),
					parquet.WithDataPageSize(2048), parquet.WithMaxRowGroupLength(1000))
				refTbl, err := readTable(t, ref)
				if err != nil {
					t.Fatal(err)
				}
				defer refTbl.Release()
				assertSameTable(t, data, refTbl)
			})
		}
	}
}

func TestInt96(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{{Name: "t", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true}}, nil)
	rec := seededRecord(sc, 3000, 5)
	defer rec.Release()
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompressionFor("t", tparquet.Compression),
		parquet.WithCompressionLevelFor("t", tparquet.Level(parquet.Types.Int96, 8)),
		parquet.WithDataPageVersion(parquet.DataPageV2))
	w, err := pqarrow.NewFileWriter(sc, &buf, props, pqarrow.NewArrowWriterProperties(
		pqarrow.WithDeprecatedInt96Timestamps(true), pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	md := fileMetadata(t, buf.Bytes())
	if md.Schema.Column(0).PhysicalType() != parquet.Types.Int96 || columnCodecs(t, md)["t"] != "PCODEC" {
		t.Fatal("not an INT96 PCODEC column")
	}
	assertSameData(t, buf.Bytes(), rec)
}

func TestPlanWritesMixedFiles(t *testing.T) {
	// Auto on everything: dictionary-encoded base columns sit next to PCODEC columns.
	recs := []arrow.RecordBatch{seededRecord(allTypes, 4000, 3), seededRecord(allTypes, 100, 4)}
	defer recs[0].Release()
	defer recs[1].Release()
	var buf bytes.Buffer
	w, err := tparquet.NewFileWriter(allTypes, &buf,
		tparquet.WithWriterProperties(parquet.WithStoreDecimalAsInteger(true), parquet.WithMaxRowGroupLength(1500)),
		tparquet.WithArrowWriterProperties(pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	md := fileMetadata(t, buf.Bytes())
	codecs := columnCodecs(t, md)
	var pco, base []string
	for path, c := range codecs {
		if c == "PCODEC" {
			pco = append(pco, path)
		} else {
			base = append(base, path)
		}
	}
	if len(pco) == 0 || len(base) == 0 {
		t.Fatalf("want a mix, got PCODEC %v and base %v", pco, base)
	}
	dictPages := 0
	for i := 0; i < md.NumRowGroups(); i++ {
		for j := 0; j < md.RowGroup(i).NumColumns(); j++ {
			cc, _ := md.RowGroup(i).ColumnChunk(j)
			if cc.HasDictionaryPage() {
				dictPages++
				if cc.Compression() == tparquet.Compression {
					t.Fatalf("%s: a planned PCODEC column has a dictionary", cc.PathInSchema())
				}
			}
		}
	}
	if dictPages == 0 {
		t.Fatal("no dictionary-encoded base column")
	}
	assertSameData(t, buf.Bytes(), recs...)
}

// spyCodec records the size of every dst arrow-go decodes into.
type spyCodec struct {
	compress.ErrorCodec
	sizes []int
}

func (s *spyCodec) DecodeWithError(dst, src []byte) ([]byte, error) {
	s.sizes = append(s.sizes, len(dst))
	return s.ErrorCodec.DecodeWithError(dst, src)
}

// TestArrowGoGivesTheExactPageSize checks, for every page kind, that arrow-go hands the
// codec a dst of exactly the decoded size: uncompressed_page_size for V1 data and dictionary
// pages, minus the level bytes for V2 pages.
func TestArrowGoGivesTheExactPageSize(t *testing.T) {
	real, _ := compress.GetCodec(tparquet.Compression)
	spy := &spyCodec{ErrorCodec: real.(compress.ErrorCodec)}
	compress.RegisterCodec(tparquet.Compression, spy)
	t.Cleanup(func() { compress.RegisterCodec(tparquet.Compression, real) })

	rec := seededRecord(allTypes, 3000, 7)
	defer rec.Release()
	kinds := map[string]int{}
	for _, version := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
		data := writeWithProps(t, []arrow.RecordBatch{rec}, allPcodec(t, allTypes, true,
			parquet.WithDataPageVersion(version), parquet.WithDataPageSize(4096))...)
		rdr, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		rg := rdr.RowGroup(0)
		for c := 0; c < rg.NumColumns(); c++ {
			pr, err := rg.GetColumnPageReader(c)
			if err != nil {
				t.Fatal(err)
			}
			cc, err := rg.MetaData().ColumnChunk(c)
			if err != nil {
				t.Fatal(err)
			}
			if cc.HasDictionaryPage() {
				dp, err := pr.GetDictionaryPage()
				if err != nil {
					t.Fatal(err)
				}
				if dp != nil {
					checkLast(t, spy, len(dp.Data()))
					kinds["dictionary (GetDictionaryPage)"]++
				}
			}
			for pr.Next() {
				switch p := pr.Page().(type) {
				case *file.DictionaryPage:
					checkLast(t, spy, len(p.Data()))
					kinds["dictionary"]++
				case *file.DataPageV1:
					checkLast(t, spy, int(p.UncompressedSize()))
					kinds["V1"]++
				case *file.DataPageV2:
					levels := int(p.DefinitionLevelByteLen() + p.RepetitionLevelByteLen())
					checkLast(t, spy, int(p.UncompressedSize())-levels)
					if levels > 0 {
						kinds["V2 with levels"]++
					}
					kinds["V2"]++
				}
			}
			if err := pr.Err(); err != nil {
				t.Fatal(err)
			}
		}
		rdr.Close()
	}
	for _, k := range []string{"dictionary", "dictionary (GetDictionaryPage)", "V1", "V2", "V2 with levels"} {
		if kinds[k] == 0 {
			t.Errorf("no %s page was read", k)
		}
	}
}

func checkLast(t *testing.T, spy *spyCodec, want int) {
	t.Helper()
	if len(spy.sizes) == 0 || spy.sizes[len(spy.sizes)-1] != want {
		t.Fatalf("dst sizes %v, want the last to be %d", spy.sizes[max(0, len(spy.sizes)-3):], want)
	}
}

// pcoPayloadStart is where the first INT64 PCO payload (version 1, mode 1, dtype 2) begins.
func pcoPayloadStart(t *testing.T, data []byte) int {
	t.Helper()
	i := bytes.Index(data, []byte{1, 1, 2})
	if i < 0 {
		t.Fatal("no PCO payload in the file")
	}
	return i
}

func corruptFile(t *testing.T) ([]byte, arrow.RecordBatch) {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := seededRecord(sc, 5000, 11)
	data := writeWithProps(t, []arrow.RecordBatch{rec}, tparquet.ColumnProperties("x", parquet.Types.Int64, 8)...)
	return data, rec
}

func TestCorruptPayloadIsAReadError(t *testing.T) {
	data, rec := corruptFile(t)
	defer rec.Release()
	assertSameData(t, data, rec)

	bad := bytes.Clone(data)
	bad[pcoPayloadStart(t, bad)] = 9
	_, err := readTable(t, bad)
	if err == nil || !strings.Contains(err.Error(), "pcodec payload: unsupported version 9") {
		t.Fatalf("err = %v, want the codec's error", err)
	}

	bad = bytes.Clone(data)
	bad[pcoPayloadStart(t, bad)+2] = 3 // INT96: not a dtype
	if _, err := readTable(t, bad); err == nil || !strings.Contains(err.Error(), "unknown dtype 3") {
		t.Fatalf("err = %v", err)
	}
}

func TestCorruptBytesNeverCrashTheReader(t *testing.T) {
	data, rec := corruptFile(t)
	defer rec.Release()
	start := pcoPayloadStart(t, data)
	errs := 0
	for i := 0; i < 300; i++ {
		bad := bytes.Clone(data)
		pos := start + (i*7919)%(len(data)-start-8) // inside the chunk and the footer
		bad[pos] ^= byte(1 + i%255)
		tbl, err := readTable(t, bad)
		if err != nil {
			errs++
			continue
		}
		tbl.Release() // a flipped pcodec body bit may decode to other values: no checksum
	}
	if errs == 0 {
		t.Fatal("no corruption was detected at all")
	}
}
