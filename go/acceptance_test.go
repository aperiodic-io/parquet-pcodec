package pcodec_test

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	pcodec "github.com/aperiodic-io/parquet-pcodec/go"
)

// marketTrades is deterministic synthetic trade data: millisecond timestamps arriving in
// bursts, prices doing a random walk on a 0.01 tick grid, and sizes in lots of 0.001.
func marketTrades(n int) arrow.RecordBatch {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "size", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	r := rand.New(rand.NewPCG(2024, 6))
	ts := int64(1_700_000_000_000)
	ticks := int64(6_543_210) // 65432.10
	for i := 0; i < n; i++ {
		if r.IntN(3) == 0 {
			ts += int64(r.ExpFloat64() * 40)
		}
		switch k := r.IntN(10); {
		case k < 2:
			ticks--
		case k < 4:
			ticks++
		case k == 4:
			ticks += int64(r.IntN(21) - 10)
		}
		lots := 1 + int64(r.ExpFloat64()*300)
		if r.IntN(4) == 0 {
			lots = (lots/100 + 1) * 100 // round lots
		}
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts))
		b.Field(1).(*array.Float64Builder).Append(float64(ticks) / 100)
		b.Field(2).(*array.Float64Builder).Append(float64(lots) / 1000)
	}
	return b.NewRecordBatch()
}

func TestAcceptanceAutoBeatsZstd9OnMarketData(t *testing.T) {
	if testing.Short() {
		t.Skip("acceptance test: 1M rows")
	}
	rec := marketTrades(1_000_000)
	defer rec.Release()

	var zstd bytes.Buffer
	w, err := pqarrow.NewFileWriter(rec.Schema(), &zstd, parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(9)), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var auto bytes.Buffer
	pw, err := pcodec.NewFileWriter(rec.Schema(), &auto)
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	ratio := float64(auto.Len()) / float64(zstd.Len())
	t.Logf("1M trades: ZSTD(9) %d bytes, Auto %d bytes (PCODEC on %v): %.3f×", zstd.Len(), auto.Len(),
		pw.Plan().PcodecColumns(), ratio)
	for _, path := range []string{"ts", "price", "size"} {
		t.Logf("  %s: ZSTD(9) %d, Auto %d", path, chunkBytes(t, zstd.Bytes(), path), chunkBytes(t, auto.Bytes(), path))
	}
	if ratio > 0.8 {
		t.Fatalf("Auto is %.3f× ZSTD(9), want at most 0.8×", ratio)
	}
	assertSameData(t, auto.Bytes(), rec)
}

func chunkBytes(t *testing.T, data []byte, path string) int64 {
	t.Helper()
	md := fileMetadata(t, data)
	var n int64
	for i := 0; i < md.NumRowGroups(); i++ {
		for j := 0; j < md.RowGroup(i).NumColumns(); j++ {
			cc, _ := md.RowGroup(i).ColumnChunk(j)
			if cc.PathInSchema().String() == path {
				n += cc.TotalCompressedSize()
			}
		}
	}
	return n
}
