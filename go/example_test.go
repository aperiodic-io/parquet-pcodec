package tparquet_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	tparquet "github.com/aperiodic-io/tparquet/go"
)

// Write trades with Auto codec selection, then read them back with plain pqarrow: importing
// the package is all a reader needs.
func Example() {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "venue", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	r := rand.New(rand.NewPCG(1, 2))
	ticks := int64(6_543_210) // prices on a 0.01 grid
	for i := 0; i < 10000; i++ {
		ticks += r.Int64N(5) - 2
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1_700_000_000_000 + int64(i)*25))
		b.Field(1).(*array.Float64Builder).Append(float64(ticks) / 100)
		b.Field(2).(*array.StringBuilder).Append("XNAS")
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	w, err := tparquet.NewFileWriter(sc, &buf) // Auto, base codec ZSTD(3)
	if err != nil {
		panic(err)
	}
	if err := w.Write(rec); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	fmt.Println("PCODEC columns:", w.Plan().PcodecColumns())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		panic(err)
	}
	defer rdr.Close()
	fmt.Println("footer:", *rdr.MetaData().KeyValueMetadata().FindValue(tparquet.FooterKey))
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		panic(err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		panic(err)
	}
	defer tbl.Release()
	fmt.Println("rows:", tbl.NumRows(), "last price:", tbl.Column(1).Data().Chunk(0).(*array.Float64).Value(9999))
	// Output:
	// PCODEC columns: [ts price]
	// footer: 1
	// rows: 10000 last price: 65432.09
}

// Write one column with PCODEC using plain arrow-go writer properties.
func ExampleColumnProperties() {
	props := parquet.NewWriterProperties(append(
		tparquet.ColumnProperties("price", parquet.Types.Double, 8),
		parquet.WithDataPageVersion(parquet.DataPageV2))...)
	fmt.Println(props.CompressionFor("price") == tparquet.Compression, props.DictionaryEnabledFor("price"))
	// Output: true false
}

// The packed level of an INT64 column at pcodec level 8.
func ExampleLevel() {
	fmt.Printf("%#x\n", tparquet.Level(parquet.Types.Int64, 8))
	// Output: 0x50430209
}
