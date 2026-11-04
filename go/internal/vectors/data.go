package vectors

// The data of the Go-produced vectors. Mutation testing skips this file (.gremlins.yaml): a
// mutant here changes test data, which still round-trips, not behaviour.

import (
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"

	pcodec "github.com/aperiodic-io/parquet-pcodec/go"
)

var (
	i32 = parquet.Types.Int32
	i64 = parquet.Types.Int64
	f32 = parquet.Types.Float
	f64 = parquet.Types.Double
	v2  = parquet.WithDataPageVersion(parquet.DataPageV2)
)

func build(sc *arrow.Schema, n int, seed uint64, row func(b *array.RecordBuilder, r *rand.Rand, i int)) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	r := rand.New(rand.NewPCG(seed, 0x5043))
	for i := 0; i < n; i++ {
		row(b, r, i)
	}
	return b.NewRecordBatch()
}

func fields(fs ...arrow.Field) *arrow.Schema { return arrow.NewSchema(fs, nil) }

// all lists the Go-produced vectors.
func all() []vector {
	return []vector{
		{name: "go_ints", recs: ints,
			props: pcodecAll(map[string]parquet.Type{"i32": i32, "i64": i64, "u32": i32, "u64": i64, "i8": i32}, v2)},
		{name: "go_float_decimals", recs: floatDecimals,
			props: pcodecAll(map[string]parquet.Type{"price": f64, "size": f32, "rate": f64}, v2)},
		{name: "go_special_floats", recs: specialFloats,
			props: pcodecAll(map[string]parquet.Type{"f64": f64, "f32": f32}, v2)},
		{name: "go_temporal", recs: temporal,
			props: pcodecAll(map[string]parquet.Type{"ts_ms": i64, "ts_us": i64, "ts_ns": i64, "date": i32,
				"time_ms": i32, "time_us": i64, "dec9": i32, "dec18": i64}, v2, parquet.WithStoreDecimalAsInteger(true))},
		{name: "go_nested", recs: nested,
			props: pcodecAll(map[string]parquet.Type{"l.list.element": i64, "s.a": f64, "s.b.list.element": i32}, v2)},
		{name: "go_v1_pages", recs: v1Pages,
			props: pcodecAll(map[string]parquet.Type{"id": i64, "x": f64}, parquet.WithDataPageSize(4096))},
		{name: "go_dictionary_pages", recs: lowCardinality, props: []parquet.WriterProperty{v2,
			parquet.WithCompressionFor("k", pcodec.Compression), parquet.WithCompressionLevelFor("k", pcodec.Level(i64, -1)),
			parquet.WithCompressionFor("v", pcodec.Compression), parquet.WithCompressionLevelFor("v", pcodec.Level(f64, -1))}},
		{name: "go_fallback", recs: nonNumeric, props: pcodecAll(map[string]parquet.Type{"s": parquet.Types.ByteArray,
			"b": parquet.Types.ByteArray, "flag": parquet.Types.Boolean, "fsb": parquet.Types.FixedLenByteArray})},
		{name: "go_auto_mixed", recs: trades, auto: true, props: []parquet.WriterProperty{parquet.WithMaxRowGroupLength(4000)}},
		{name: "go_empty", recs: empty, props: pcodecAll(map[string]parquet.Type{"x": i64, "y": f64}, v2)},
	}
}

func ints() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
		arrow.Field{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
		arrow.Field{Name: "i8", Type: arrow.PrimitiveTypes.Int8})
	extremes := []int64{math.MinInt64, math.MaxInt64, 0, -1}
	return []arrow.RecordBatch{build(sc, 3000, 1, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		if i%7 == 3 {
			b.Field(0).AppendNull()
		} else {
			b.Field(0).(*array.Int32Builder).Append(int32(r.IntN(2000)) - 1000)
		}
		v := int64(i)*1_000_003 + int64(r.IntN(10))
		if i < len(extremes) {
			v = extremes[i]
		}
		b.Field(1).(*array.Int64Builder).Append(v)
		b.Field(2).(*array.Uint32Builder).Append(math.MaxUint32 - uint32(r.IntN(100)))
		b.Field(3).(*array.Uint64Builder).Append(math.MaxUint64 - uint64(i))
		b.Field(4).(*array.Int8Builder).Append(int8(i))
	})}
}

func floatDecimals() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "size", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		arrow.Field{Name: "rate", Type: arrow.PrimitiveTypes.Float64})
	ticks := int64(1_234_567)
	return []arrow.RecordBatch{build(sc, 5000, 2, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		ticks += int64(r.IntN(5)) - 2
		price := float64(ticks) / 100
		if i%500 == 7 {
			price = math.Pi * float64(i) // an exception to the decimal grid
		}
		b.Field(0).(*array.Float64Builder).Append(price)
		if i%11 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.Float32Builder).Append(float32(r.IntN(400)) / 4)
		}
		b.Field(2).(*array.Float64Builder).Append(float64(r.IntN(100000)) / 1e6)
	})}
}

func specialFloats() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "f32", Type: arrow.PrimitiveTypes.Float32})
	d := []float64{0, math.Copysign(0, -1), math.NaN(), math.Float64frombits(0x7ff8_dead_beef_0001),
		math.Float64frombits(0xfff0_0000_0000_0001), math.Inf(1), math.Inf(-1), math.MaxFloat64,
		-math.MaxFloat64, math.SmallestNonzeroFloat64, 2.2250738585072014e-308, 0.1, 1e300}
	s := []float32{0, float32(math.Copysign(0, -1)), float32(math.NaN()), math.Float32frombits(0x7fc0_0001),
		math.Float32frombits(0xff80_0001), float32(math.Inf(1)), float32(math.Inf(-1)), math.MaxFloat32,
		math.SmallestNonzeroFloat32, 0.1, 16777217}
	return []arrow.RecordBatch{build(sc, 1000, 3, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		if i%13 == 12 {
			b.Field(0).AppendNull()
		} else {
			b.Field(0).(*array.Float64Builder).Append(d[r.IntN(len(d))])
		}
		b.Field(1).(*array.Float32Builder).Append(s[r.IntN(len(s))])
	})}
}

func temporal() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "ts_ms", Type: arrow.FixedWidthTypes.Timestamp_ms},
		arrow.Field{Name: "ts_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		arrow.Field{Name: "ts_ns", Type: arrow.FixedWidthTypes.Timestamp_ns},
		arrow.Field{Name: "date", Type: arrow.FixedWidthTypes.Date32},
		arrow.Field{Name: "time_ms", Type: arrow.FixedWidthTypes.Time32ms},
		arrow.Field{Name: "time_us", Type: arrow.FixedWidthTypes.Time64us},
		arrow.Field{Name: "dec9", Type: &arrow.Decimal128Type{Precision: 9, Scale: 2}},
		arrow.Field{Name: "dec18", Type: &arrow.Decimal128Type{Precision: 18, Scale: 3}, Nullable: true})
	ts := int64(1_700_000_000_000)
	return []arrow.RecordBatch{build(sc, 4000, 4, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		ts += int64(r.IntN(100))
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts))
		if i%9 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(ts*1000 + int64(r.IntN(1000))))
		}
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(ts*1_000_000 + int64(r.IntN(1_000_000))))
		b.Field(3).(*array.Date32Builder).Append(arrow.Date32(19000 + i/100))
		b.Field(4).(*array.Time32Builder).Append(arrow.Time32(ts % 86_400_000))
		b.Field(5).(*array.Time64Builder).Append(arrow.Time64(ts % 86_400_000 * 1000))
		b.Field(6).(*array.Decimal128Builder).Append(decimal128.FromI64(int64(r.IntN(2_000_000)) - 1_000_000))
		if i%5 == 0 {
			b.Field(7).AppendNull()
		} else {
			b.Field(7).(*array.Decimal128Builder).Append(decimal128.FromI64(999_999_999_999_999_999 - int64(i)))
		}
	})}
}

func nested() []arrow.RecordBatch {
	st := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)})
	sc := fields(arrow.Field{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		arrow.Field{Name: "s", Type: st, Nullable: true})
	return []arrow.RecordBatch{build(sc, 2000, 5, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		lb := b.Field(0).(*array.ListBuilder)
		if i%10 == 0 {
			lb.AppendNull()
		} else {
			lb.Append(true)
			for k := 0; k < r.IntN(4); k++ {
				if r.IntN(6) == 0 {
					lb.ValueBuilder().AppendNull()
				} else {
					lb.ValueBuilder().(*array.Int64Builder).Append(int64(i*10 + k))
				}
			}
		}
		sb := b.Field(1).(*array.StructBuilder)
		if i%8 == 0 {
			sb.AppendNull()
			return
		}
		sb.Append(true)
		if i%3 == 0 {
			sb.FieldBuilder(0).AppendNull()
		} else {
			sb.FieldBuilder(0).(*array.Float64Builder).Append(float64(i) / 4)
		}
		bl := sb.FieldBuilder(1).(*array.ListBuilder)
		bl.Append(true)
		for k := 0; k < r.IntN(3); k++ {
			bl.ValueBuilder().(*array.Int32Builder).Append(int32(r.IntN(100)))
		}
	})}
}

func v1Pages() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Float64})
	return []arrow.RecordBatch{build(sc, 6000, 6, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		b.Field(0).(*array.Int64Builder).Append(int64(i) * 3)
		b.Field(1).(*array.Float64Builder).Append(float64(r.IntN(1000)) / 8)
	})}
}

func lowCardinality() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Float64})
	return []arrow.RecordBatch{build(sc, 3000, 7, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		if i%17 == 0 {
			b.Field(0).AppendNull()
		} else {
			b.Field(0).(*array.Int64Builder).Append(int64(r.IntN(20)) * 1_000_000_007)
		}
		b.Field(1).(*array.Float64Builder).Append(float64(r.IntN(8)) * 0.25)
	})}
}

func nonNumeric() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.Binary},
		arrow.Field{Name: "flag", Type: arrow.FixedWidthTypes.Boolean},
		arrow.Field{Name: "fsb", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}})
	return []arrow.RecordBatch{build(sc, 1500, 8, func(b *array.RecordBuilder, r *rand.Rand, i int) {
		if i%6 == 0 {
			b.Field(0).AppendNull()
		} else {
			b.Field(0).(*array.StringBuilder).Append(fmt.Sprintf("sym-%d \"é\" \u2603", r.IntN(50)))
		}
		v := make([]byte, r.IntN(5))
		for k := range v {
			v[k] = byte(r.Uint32())
		}
		b.Field(1).(*array.BinaryBuilder).Append(v)
		b.Field(2).(*array.BooleanBuilder).Append(r.IntN(3) == 0)
		b.Field(3).(*array.FixedSizeBinaryBuilder).Append([]byte{byte(i), byte(i >> 8), 0, 0xff})
	})}
}

func trades() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
		arrow.Field{Name: "symbol", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "size", Type: arrow.PrimitiveTypes.Float64},
		arrow.Field{Name: "side", Type: arrow.PrimitiveTypes.Int8})
	ts, ticks := int64(1_700_000_000_000), int64(6_543_210)
	gen := func(n int, seed uint64) arrow.RecordBatch {
		return build(sc, n, seed, func(b *array.RecordBuilder, r *rand.Rand, i int) {
			ts += int64(r.IntN(30))
			ticks += int64(r.IntN(3)) - 1
			b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts))
			b.Field(1).(*array.StringBuilder).Append([]string{"BTC-USD", "ETH-USD", "SOL-USD"}[r.IntN(3)])
			b.Field(2).(*array.Float64Builder).Append(float64(ticks) / 100)
			b.Field(3).(*array.Float64Builder).Append(float64(1+r.IntN(5000)) / 1000)
			b.Field(4).(*array.Int8Builder).Append(int8(r.IntN(2)))
		})
	}
	return []arrow.RecordBatch{gen(10000, 9), gen(3000, 10)}
}

func empty() []arrow.RecordBatch {
	sc := fields(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: true})
	return []arrow.RecordBatch{build(sc, 0, 11, nil)}
}
