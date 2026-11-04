package canonical

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func record(t *testing.T, fields []arrow.Field, fill func(b *array.RecordBuilder)) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer b.Release()
	fill(b)
	return b.NewRecordBatch()
}

func jsonl(t *testing.T, rec arrow.RecordBatch) string {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteRecord(&buf, rec); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestScalars(t *testing.T) {
	rec := record(t, []arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "b", Type: arrow.BinaryTypes.Binary},
		{Name: "ok", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32},
		{Name: "t", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "dec", Type: &arrow.Decimal128Type{Precision: 38, Scale: 2}},
		{Name: "fsb", Type: &arrow.FixedSizeBinaryType{ByteWidth: 2}},
	}, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int8Builder).AppendValues([]int8{-128, 5}, nil)
		b.Field(1).(*array.Uint64Builder).AppendValues([]uint64{math.MaxUint64, 0}, nil)
		b.Field(2).(*array.Int64Builder).AppendValues([]int64{math.MinInt64, 0}, []bool{true, false})
		b.Field(3).(*array.Float64Builder).AppendValues([]float64{1, math.Copysign(0, -1)}, nil)
		b.Field(4).(*array.Float32Builder).AppendValues([]float32{1, float32(math.NaN())}, nil)
		b.Field(5).(*array.StringBuilder).AppendValues([]string{"a\"\\\né<", ""}, nil)
		b.Field(6).(*array.BinaryBuilder).AppendValues([][]byte{{0, 1, 255}, {}}, nil)
		b.Field(7).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
		b.Field(8).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1700000000000000, -1}, nil)
		b.Field(9).(*array.Date32Builder).AppendValues([]arrow.Date32{19000, 0}, nil)
		b.Field(10).(*array.Time64Builder).AppendValues([]arrow.Time64{86399999999, 0}, nil)
		big := decimal128.FromI64(math.MaxInt64).Mul(decimal128.FromI64(1000))
		b.Field(11).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(-12345), big}, nil)
		b.Field(12).(*array.FixedSizeBinaryBuilder).AppendValues([][]byte{{1, 2}, {3, 4}}, nil)
	})
	defer rec.Release()
	want := `{"i8":-128,"u64":18446744073709551615,"i64":-9223372036854775808,"f64":"0x3ff0000000000000","f32":"0x3f800000","s":"a\"\\\né<","b":"AAH/","ok":true,"ts":1700000000000000,"d":19000,"t":86399999999,"dec":-12345,"fsb":"AQI="}
{"i8":5,"u64":0,"i64":null,"f64":"0x8000000000000000","f32":"0x7fc00000","s":"","b":"","ok":false,"ts":-1,"d":0,"t":0,"dec":9223372036854775807000,"fsb":"AwQ="}
`
	got := jsonl(t, rec)
	if ok, diff := Equal([]byte(got), []byte(want)); !ok {
		t.Fatalf("%s\ngot:\n%s", diff, got)
	}
	// The output itself is the canonical spelling: lower-case hex, no spaces.
	if strings.Contains(got, " ") || strings.Contains(got, "0X") {
		t.Fatalf("not canonical: %s", got)
	}
}

func TestNested(t *testing.T) {
	st := arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)})
	rec := record(t, []arrow.Field{
		{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "s", Type: st, Nullable: true},
		{Name: "dict", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}},
		{Name: "ll", Type: arrow.LargeListOf(arrow.BinaryTypes.LargeString)},
	}, func(b *array.RecordBuilder) {
		lb := b.Field(0).(*array.ListBuilder)
		lb.Append(true)
		lb.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2}, []bool{true, false})
		lb.AppendNull()
		lb.Append(true)
		sb := b.Field(1).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int32Builder).AppendNull()
		yl := sb.FieldBuilder(1).(*array.ListBuilder)
		yl.Append(true)
		yl.ValueBuilder().(*array.Float64Builder).Append(2)
		sb.AppendNull()
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int32Builder).Append(7)
		sb.FieldBuilder(1).(*array.ListBuilder).Append(true)
		db := b.Field(2).(*array.BinaryDictionaryBuilder)
		for _, s := range []string{"a", "b", "a"} {
			if err := db.AppendString(s); err != nil {
				t.Fatal(err)
			}
		}
		llb := b.Field(3).(*array.LargeListBuilder)
		for i := 0; i < 3; i++ {
			llb.Append(true)
			llb.ValueBuilder().(*array.LargeStringBuilder).Append("z")
		}
	})
	defer rec.Release()
	want := `{"l":[1,null],"s":{"x":null,"y":["0x4000000000000000"]},"dict":"a","ll":["z"]}
{"l":null,"s":null,"dict":"b","ll":["z"]}
{"l":[],"s":{"x":7,"y":[]},"dict":"a","ll":["z"]}
`
	if got := jsonl(t, rec); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestUnsupportedType(t *testing.T) {
	rec := record(t, []arrow.Field{{Name: "m", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int8)}},
		func(b *array.RecordBuilder) { b.Field(0).(*array.MapBuilder).Append(true) })
	defer rec.Release()
	var buf bytes.Buffer
	if err := WriteRecord(&buf, rec); err == nil || !strings.Contains(err.Error(), "canonical: no JSON form for map") {
		t.Fatalf("err = %v", err)
	}
}

func TestEqual(t *testing.T) {
	cases := []struct {
		a, b string
		ok   bool
		diff string
	}{
		{`{"a":1,"b":"x"}` + "\n", `{"a": 1, "b": "x"}`, true, ""},
		{`{"a":1}`, `{"a":1.0}`, false, "line 1"},
		{`{"a":1}`, `{"a":2}`, false, "line 1"},
		{`{"a":1,"b":2}`, `{"b":2,"a":1}`, false, "line 1"},
		{`{"a":[1,{"c":null}]}`, `{"a":[1,{"c":null}]}`, true, ""},
		{`{"a":"é"}`, `{"a":"é"}`, true, ""},
		{`{"a":true}`, `{"a":"true"}`, false, "line 1"},
		{"{}\n{}\n", "{}\n", false, "2 rows, want 1"},
		{"{}\n{\"a\":1}\n", "{}\n{\"a\":2}\n", false, "line 2"},
		{"{", "{}", false, "line 1"},
		{"{}", "{", false, "line 1"},
		{`{"a":123456789012345678901234567890}`, `{"a":123456789012345678901234567890}`, true, ""},
		{`{"a":123456789012345678901234567890}`, `{"a":123456789012345678901234567891}`, false, "line 1"},
		{"\n{}\n\n", "{}", true, ""},
	}
	for _, tc := range cases {
		ok, diff := Equal([]byte(tc.a), []byte(tc.b))
		if ok != tc.ok || !strings.Contains(diff, tc.diff) {
			t.Errorf("Equal(%q, %q) = %v, %q", tc.a, tc.b, ok, diff)
		}
	}
}

func TestWriteTable(t *testing.T) {
	fields := []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int16}}
	a := record(t, fields, func(b *array.RecordBuilder) { b.Field(0).(*array.Int16Builder).AppendValues([]int16{1, 2}, nil) })
	c := record(t, fields, func(b *array.RecordBuilder) { b.Field(0).(*array.Int16Builder).AppendValues([]int16{3}, nil) })
	defer a.Release()
	defer c.Release()
	tbl := array.NewTableFromRecords(a.Schema(), []arrow.RecordBatch{a, c})
	defer tbl.Release()
	var buf bytes.Buffer
	if err := WriteTable(&buf, tbl); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n" {
		t.Fatalf("got %q", buf.String())
	}

	m := record(t, []arrow.Field{{Name: "m", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int8)}},
		func(b *array.RecordBuilder) { b.Field(0).(*array.MapBuilder).Append(true) })
	defer m.Release()
	mt := array.NewTableFromRecords(m.Schema(), []arrow.RecordBatch{m})
	defer mt.Release()
	if err := WriteTable(&bytes.Buffer{}, mt); err == nil {
		t.Fatal("WriteTable should pass on the record's error")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestWriteErrors(t *testing.T) {
	rec := record(t, []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int16}},
		func(b *array.RecordBuilder) { b.Field(0).(*array.Int16Builder).Append(1) })
	defer rec.Release()
	if err := WriteRecord(failingWriter{}, rec); err == nil || err.Error() != "disk full" {
		t.Fatalf("err = %v", err)
	}
}
