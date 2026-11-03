package pcodec_test

import (
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// randomRecord returns n rows of seeded random data for sc (seed 1).
func randomRecord(sc *arrow.Schema, n int) arrow.RecordBatch {
	return seededRecord(sc, n, 1)
}

func seededRecord(sc *arrow.Schema, n int, seed uint64) arrow.RecordBatch {
	r := rand.New(rand.NewPCG(seed, 99))
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for i := 0; i < n; i++ {
		for j, f := range sc.Fields() {
			appendRandom(b.Field(j), f.Type, f.Nullable, r)
		}
	}
	return b.NewRecordBatch()
}

// specialFloats are values a codec could get wrong: signed zeros, NaN payloads, extremes.
var specialFloats = []float64{0, math.Copysign(0, -1), math.NaN(), math.Float64frombits(0x7ff8_dead_beef_0001),
	math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64, 1.5, -2.25}

var specialFloats32 = []float32{0, float32(math.Copysign(0, -1)), math.Float32frombits(0x7fc0_0001),
	float32(math.Inf(-1)), math.MaxFloat32, math.SmallestNonzeroFloat32, 0.1, -3.75}

// smooth returns a value from a random walk on a coarse grid, so that pcodec has structure
// to find, mixed with occasional arbitrary values.
func smooth(r *rand.Rand) int64 {
	if r.IntN(50) == 0 {
		return int64(r.Uint64())
	}
	return 1_000_000 + int64(r.IntN(1000))*10
}

func appendRandom(b array.Builder, typ arrow.DataType, nullable bool, r *rand.Rand) {
	if nullable && r.IntN(5) == 0 {
		b.AppendNull()
		return
	}
	switch b := b.(type) {
	case *array.Int8Builder:
		b.Append(int8(r.Uint32()))
	case *array.Int16Builder:
		b.Append(int16(r.Uint32()))
	case *array.Int32Builder:
		b.Append(int32(smooth(r)))
	case *array.Int64Builder:
		b.Append(smooth(r))
	case *array.Uint8Builder:
		b.Append(uint8(r.Uint32()))
	case *array.Uint16Builder:
		b.Append(uint16(r.Uint32()))
	case *array.Uint32Builder:
		b.Append(r.Uint32())
	case *array.Uint64Builder:
		b.Append(r.Uint64())
	case *array.Float32Builder:
		if r.IntN(8) == 0 {
			b.Append(specialFloats32[r.IntN(len(specialFloats32))])
		} else {
			b.Append(float32(r.IntN(100000)) / 100)
		}
	case *array.Float64Builder:
		if r.IntN(8) == 0 {
			b.Append(specialFloats[r.IntN(len(specialFloats))])
		} else {
			b.Append(float64(r.IntN(1000000)) / 100)
		}
	case *array.BooleanBuilder:
		b.Append(r.IntN(2) == 0)
	case *array.StringBuilder:
		b.Append(fmt.Sprintf("s%dé\"\\\n", r.IntN(30)))
	case *array.BinaryBuilder:
		v := make([]byte, r.IntN(6))
		for i := range v {
			v[i] = byte(r.Uint32())
		}
		b.Append(v)
	case *array.FixedSizeBinaryBuilder:
		v := make([]byte, typ.(*arrow.FixedSizeBinaryType).ByteWidth)
		for i := range v {
			v[i] = byte(r.Uint32())
		}
		b.Append(v)
	case *array.TimestampBuilder:
		b.Append(arrow.Timestamp(1_700_000_000_000 + smooth(r)))
	case *array.Date32Builder:
		b.Append(arrow.Date32(19000 + r.IntN(1000)))
	case *array.Date64Builder:
		b.Append(arrow.Date64(int64(19000+r.IntN(1000)) * 86_400_000))
	case *array.Time32Builder:
		b.Append(arrow.Time32(r.IntN(86_400_000)))
	case *array.Time64Builder:
		b.Append(arrow.Time64(int64(r.IntN(86_400_000)) * 1000))
	case *array.Decimal128Builder:
		p := typ.(*arrow.Decimal128Type).Precision
		max := int64(math.Pow10(int(min(p, 18))))
		b.Append(decimal128.FromI64(r.Int64N(2*max-1) - max + 1))
	case *array.ListBuilder:
		elem := typ.(*arrow.ListType).ElemField()
		k := r.IntN(4)
		b.Append(true)
		for i := 0; i < k; i++ {
			appendRandom(b.ValueBuilder(), elem.Type, elem.Nullable, r)
		}
	case *array.StructBuilder:
		b.Append(true)
		for i, f := range typ.(*arrow.StructType).Fields() {
			appendRandom(b.FieldBuilder(i), f.Type, f.Nullable, r)
		}
	case *array.BinaryDictionaryBuilder:
		if err := b.AppendString(fmt.Sprintf("d%d", r.IntN(5))); err != nil {
			panic(err)
		}
	default:
		panic(fmt.Sprintf("appendRandom: no generator for %T", b))
	}
}
