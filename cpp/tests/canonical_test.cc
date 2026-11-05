// The canonical JSON writer on its own (testdata/README.md), value by value.
#include <cstring>
#include <limits>

#include <arrow/util/decimal.h>
#include <arrow/util/float16.h>

#include "common.h"

namespace tparquet::test {
namespace {

template <typename Builder, typename T>
std::shared_ptr<arrow::Array> Make(std::shared_ptr<arrow::DataType> type,
                                   const std::vector<std::optional<T>>& values) {
  Builder b(type, arrow::default_memory_pool());
  for (const auto& v : values) {
    if (v) {
      EXPECT_TRUE(b.Append(*v).ok());
    } else {
      EXPECT_TRUE(b.AppendNull().ok());
    }
  }
  return b.Finish().ValueOrDie();
}

template <typename ArrowType>
std::shared_ptr<arrow::Array> Prim(
    const std::vector<std::optional<typename ArrowType::c_type>>& values,
    std::shared_ptr<arrow::DataType> type = arrow::TypeTraits<ArrowType>::type_singleton()) {
  return Make<arrow::NumericBuilder<ArrowType>>(type, values);
}

/// The canonical JSON of each value of `a`, as the column "v" of a one-column table.
std::vector<std::string> One(const std::shared_ptr<arrow::Array>& a) {
  auto schema = arrow::schema({arrow::field("v", a->type())});
  auto batch = arrow::RecordBatch::Make(schema, a->length(), {a});
  auto lines = CanonicalJsonLines(*batch);
  EXPECT_TRUE(lines.ok()) << lines.status().ToString();
  std::vector<std::string> out;
  for (const auto& l : lines.ValueOr({})) {
    EXPECT_EQ(l.rfind("{\"v\":", 0), 0u) << l;
    EXPECT_EQ(l.back(), '}');
    out.push_back(l.substr(5, l.size() - 6));
  }
  return out;
}

using V = std::vector<std::string>;

TEST(Canonical, IntegersAreNumbers) {
  EXPECT_EQ(One(Prim<arrow::Int8Type>({-8, std::nullopt})), (V{"-8", "null"}));
  EXPECT_EQ(One(Prim<arrow::Int16Type>({-16})), (V{"-16"}));
  EXPECT_EQ(One(Prim<arrow::Int32Type>({std::numeric_limits<int32_t>::min()})), (V{"-2147483648"}));
  EXPECT_EQ(One(Prim<arrow::Int64Type>({std::numeric_limits<int64_t>::min()})),
            (V{"-9223372036854775808"}));
  EXPECT_EQ(One(Prim<arrow::UInt8Type>({255})), (V{"255"}));
  EXPECT_EQ(One(Prim<arrow::UInt16Type>({65535})), (V{"65535"}));
  EXPECT_EQ(One(Prim<arrow::UInt32Type>({4294967295u})), (V{"4294967295"}));
  EXPECT_EQ(One(Prim<arrow::UInt64Type>({std::numeric_limits<uint64_t>::max()})),
            (V{"18446744073709551615"}));
}

double FromBits(uint64_t bits) {
  double d;
  std::memcpy(&d, &bits, 8);
  return d;
}

float FromBits32(uint32_t bits) {
  float f;
  std::memcpy(&f, &bits, 4);
  return f;
}

TEST(Canonical, FloatsAreBits) {
  EXPECT_EQ(One(Prim<arrow::DoubleType>({1.0, -0.0})),
            (V{"\"0x3ff0000000000000\"", "\"0x8000000000000000\""}));
  EXPECT_EQ(One(Prim<arrow::FloatType>({1.0f, FromBits32(1)})),
            (V{"\"0x3f800000\"", "\"0x00000001\""}));
  EXPECT_EQ(One(Prim<arrow::HalfFloatType>({0x3c00, 0x0001})), (V{"\"0x3c00\"", "\"0x0001\""}));
  // NaN payloads, infinities and the top bit are kept
  EXPECT_EQ(One(Prim<arrow::DoubleType>({FromBits(0x7ff8000000000001), FromBits(0xfff0000000000000)})),
            (V{"\"0x7ff8000000000001\"", "\"0xfff0000000000000\""}));
  EXPECT_EQ(One(Prim<arrow::FloatType>({FromBits32(0xffc00001)})), (V{"\"0xffc00001\""}));
}

TEST(Canonical, TemporalAndDecimalAreNumbers) {
  EXPECT_EQ(One(Prim<arrow::Date32Type>({19000})), (V{"19000"}));
  EXPECT_EQ(One(Prim<arrow::Date64Type>({-1})), (V{"-1"}));
  EXPECT_EQ(One(Prim<arrow::Time32Type>({1}, arrow::time32(arrow::TimeUnit::SECOND))), (V{"1"}));
  EXPECT_EQ(One(Prim<arrow::Time32Type>({2}, arrow::time32(arrow::TimeUnit::MILLI))), (V{"2"}));
  EXPECT_EQ(One(Prim<arrow::Time64Type>({3}, arrow::time64(arrow::TimeUnit::MICRO))), (V{"3"}));
  EXPECT_EQ(One(Prim<arrow::Time64Type>({4}, arrow::time64(arrow::TimeUnit::NANO))), (V{"4"}));
  EXPECT_EQ(One(Prim<arrow::TimestampType>({5}, arrow::timestamp(arrow::TimeUnit::SECOND))), (V{"5"}));
  EXPECT_EQ(One(Prim<arrow::TimestampType>({6}, arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"))),
            (V{"6"}));
  EXPECT_EQ(One(Prim<arrow::TimestampType>({7}, arrow::timestamp(arrow::TimeUnit::MICRO,
                                                                 "America/New_York"))),
            (V{"7"}));
  EXPECT_EQ(One(Prim<arrow::TimestampType>({8}, arrow::timestamp(arrow::TimeUnit::NANO))), (V{"8"}));
  EXPECT_EQ(One(Prim<arrow::DurationType>({9}, arrow::duration(arrow::TimeUnit::SECOND))), (V{"9"}));
  EXPECT_EQ(One(Prim<arrow::DurationType>({12}, arrow::duration(arrow::TimeUnit::NANO))), (V{"12"}));

  arrow::Decimal32Builder d32(arrow::decimal32(5, 2));
  ASSERT_OK(d32.Append(arrow::Decimal32(-13)));
  EXPECT_EQ(One(d32.Finish().ValueOrDie()), (V{"-13"}));
  arrow::Decimal64Builder d64(arrow::decimal64(12, 2));
  ASSERT_OK(d64.Append(arrow::Decimal64(14)));
  EXPECT_EQ(One(d64.Finish().ValueOrDie()), (V{"14"}));
  arrow::Decimal128Builder d128(arrow::decimal128(38, 0));
  ASSERT_OK(d128.Append(arrow::Decimal128("170141183460469231731687303715884105727")));
  ASSERT_OK(d128.Append(arrow::Decimal128(-5)));
  EXPECT_EQ(One(d128.Finish().ValueOrDie()), (V{"170141183460469231731687303715884105727", "-5"}));
  arrow::Decimal256Builder d256(arrow::decimal256(76, 3));
  ASSERT_OK(d256.Append(arrow::Decimal256(-15)));
  EXPECT_EQ(One(d256.Finish().ValueOrDie()), (V{"-15"}));
}

TEST(Canonical, StringsAreEscaped) {
  arrow::StringBuilder s;
  ASSERT_OK(s.Append(std::string("a\"b\\c\n\x01\xc3\xa9/", 10)));
  ASSERT_OK(s.Append(std::string("\b\t\n\f\r\x1f\x7f", 7)));
  ASSERT_OK(s.Append(std::string("nul\0x", 5)));
  ASSERT_OK(s.Append("\xe2\x82\xac\xf0\x9f\x98\x80"));  // non-ASCII stays UTF-8
  ASSERT_OK(s.Append(""));
  EXPECT_EQ(One(s.Finish().ValueOrDie()),
            (V{"\"a\\\"b\\\\c\\n\\u0001\xc3\xa9/\"", "\"\\b\\t\\n\\f\\r\\u001f\x7f\"",
               "\"nul\\u0000x\"", "\"\xe2\x82\xac\xf0\x9f\x98\x80\"", "\"\""}));
  arrow::LargeStringBuilder l;
  ASSERT_OK(l.Append("x"));
  EXPECT_EQ(One(l.Finish().ValueOrDie()), (V{"\"x\""}));
  arrow::StringViewBuilder v;
  ASSERT_OK(v.Append("a string longer than twelve bytes"));
  ASSERT_OK(v.Append("short"));
  EXPECT_EQ(One(v.Finish().ValueOrDie()),
            (V{"\"a string longer than twelve bytes\"", "\"short\""}));
}

TEST(Canonical, BinaryIsBase64) {
  arrow::BinaryBuilder b;
  ASSERT_OK(b.Append(std::string("\x00\xff", 2)));
  ASSERT_OK(b.Append(""));
  ASSERT_OK(b.Append("f"));
  ASSERT_OK(b.Append("fo"));
  ASSERT_OK(b.Append("foo"));
  ASSERT_OK(b.Append("foob"));
  ASSERT_OK(b.Append(std::string("\xfb\xff", 2)));  // '+' and '/' (standard alphabet)
  EXPECT_EQ(One(b.Finish().ValueOrDie()),
            (V{"\"AP8=\"", "\"\"", "\"Zg==\"", "\"Zm8=\"", "\"Zm9v\"", "\"Zm9vYg==\"", "\"+/8=\""}));
  arrow::LargeBinaryBuilder lb;
  ASSERT_OK(lb.Append("ab"));
  EXPECT_EQ(One(lb.Finish().ValueOrDie()), (V{"\"YWI=\""}));
  arrow::BinaryViewBuilder bv;
  ASSERT_OK(bv.Append("abc"));
  EXPECT_EQ(One(bv.Finish().ValueOrDie()), (V{"\"YWJj\""}));
  arrow::FixedSizeBinaryBuilder fb(arrow::fixed_size_binary(2));
  ASSERT_OK(fb.Append("\x01\x02"));
  ASSERT_OK(fb.AppendNull());
  EXPECT_EQ(One(fb.Finish().ValueOrDie()), (V{"\"AQI=\"", "null"}));
}

TEST(Canonical, BooleansAndNull) {
  arrow::BooleanBuilder b;
  ASSERT_OK(b.Append(true));
  ASSERT_OK(b.Append(false));
  ASSERT_OK(b.AppendNull());
  EXPECT_EQ(One(b.Finish().ValueOrDie()), (V{"true", "false", "null"}));
  EXPECT_EQ(One(std::make_shared<arrow::NullArray>(2)), (V{"null", "null"}));
}

TEST(Canonical, Lists) {
  arrow::ListBuilder lb(arrow::default_memory_pool(), std::make_shared<arrow::Int32Builder>());
  auto* ints = static_cast<arrow::Int32Builder*>(lb.value_builder());
  ASSERT_OK(lb.Append());
  ASSERT_OK(ints->Append(1));
  ASSERT_OK(ints->AppendNull());
  ASSERT_OK(ints->Append(3));
  ASSERT_OK(lb.AppendNull());
  ASSERT_OK(lb.Append());
  EXPECT_EQ(One(lb.Finish().ValueOrDie()), (V{"[1,null,3]", "null", "[]"}));

  auto values = Prim<arrow::Int64Type>({4, 5, 6, 7});
  auto offsets = Prim<arrow::Int64Type>({0, 2, 4});
  auto large = arrow::LargeListArray::FromArrays(*offsets, *values).ValueOrDie();
  EXPECT_EQ(One(large), (V{"[4,5]", "[6,7]"}));
  auto fixed = arrow::FixedSizeListArray::FromArrays(values, 2).ValueOrDie();
  EXPECT_EQ(One(fixed), (V{"[4,5]", "[6,7]"}));
  // a sliced list starts at its offset
  EXPECT_EQ(One(large->Slice(1)), (V{"[6,7]"}));
  // list views
  auto view_offsets = Prim<arrow::Int32Type>({2, 0});
  auto view_sizes = Prim<arrow::Int32Type>({2, 1});
  auto view = arrow::ListViewArray::FromArrays(*view_offsets, *view_sizes, *values).ValueOrDie();
  EXPECT_EQ(One(view), (V{"[6,7]", "[4]"}));
}

TEST(Canonical, StructsKeepFieldOrder) {
  auto b = Prim<arrow::Int32Type>({1, 2});
  arrow::StringBuilder sb;
  ASSERT_OK(sb.AppendNull());
  ASSERT_OK(sb.Append("q"));
  auto a = sb.Finish().ValueOrDie();
  auto s = arrow::StructArray::Make({b, a}, std::vector<std::string>{"b", "a"}).ValueOrDie();
  EXPECT_EQ(One(s), (V{"{\"b\":1,\"a\":null}", "{\"b\":2,\"a\":\"q\"}"}));
  // a sliced struct reads its children at the struct's offset
  EXPECT_EQ(One(s->Slice(1)), (V{"{\"b\":2,\"a\":\"q\"}"}));
  // a null struct is null, whatever its children hold
  auto validity = arrow::AllocateBitmap(2).ValueOrDie();
  validity->mutable_data()[0] = 0x2;
  auto with_null =
      std::make_shared<arrow::StructArray>(s->type(), 2, s->fields(), validity, 1);
  EXPECT_EQ(One(with_null), (V{"null", "{\"b\":2,\"a\":\"q\"}"}));
}

TEST(Canonical, MapsAreEntryArrays) {
  arrow::MapBuilder mb(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(),
                       std::make_shared<arrow::Int32Builder>());
  auto* keys = static_cast<arrow::StringBuilder*>(mb.key_builder());
  auto* items = static_cast<arrow::Int32Builder*>(mb.item_builder());
  ASSERT_OK(mb.Append());
  ASSERT_OK(keys->Append("k"));
  ASSERT_OK(items->Append(1));
  ASSERT_OK(keys->Append("l"));
  ASSERT_OK(items->AppendNull());
  ASSERT_OK(mb.AppendNull());
  ASSERT_OK(mb.Append());
  EXPECT_EQ(One(mb.Finish().ValueOrDie()),
            (V{"[{\"key\":\"k\",\"value\":1},{\"key\":\"l\",\"value\":null}]", "null", "[]"}));
  // the entry struct's own field names
  auto entries_keys = Prim<arrow::Int32Type>({7});
  auto entries_items = Prim<arrow::Int32Type>({8});
  auto offsets = Prim<arrow::Int32Type>({0, 1});
  auto type = std::make_shared<arrow::MapType>(arrow::field("k", arrow::int32(), false),
                                               arrow::field("v", arrow::int32()));
  auto named = arrow::MapArray::FromArrays(type, offsets, entries_keys, entries_items).ValueOrDie();
  EXPECT_EQ(One(named), (V{"[{\"k\":7,\"v\":8}]"}));
}

TEST(Canonical, DictionariesAreDecoded) {
  arrow::StringBuilder sb;
  ASSERT_OK(sb.Append("x"));
  ASSERT_OK(sb.Append("y"));
  auto dict = sb.Finish().ValueOrDie();
  auto indices = Prim<arrow::Int8Type>({0, 1, std::nullopt, 0});
  auto a = arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int8(), arrow::utf8()), indices,
                                              dict)
               .ValueOrDie();
  EXPECT_EQ(One(a), (V{"\"x\"", "\"y\"", "null", "\"x\""}));
}

TEST(Canonical, RowsAreObjectsInSchemaOrder) {
  auto z = Prim<arrow::Int32Type>({1, 2});
  arrow::StringBuilder sb;
  ASSERT_OK(sb.Append("x"));
  ASSERT_OK(sb.AppendNull());
  auto a = sb.Finish().ValueOrDie();
  auto schema = arrow::schema({arrow::field("z", arrow::int32()), arrow::field("a \"q\"", arrow::utf8())});
  auto batch = arrow::RecordBatch::Make(schema, 2, {z, a});
  ASSERT_OK_AND_ASSIGN(auto lines, CanonicalJsonLines(*batch));
  EXPECT_EQ(lines, (V{"{\"z\":1,\"a \\\"q\\\"\":\"x\"}", "{\"z\":2,\"a \\\"q\\\"\":null}"}));
  // a table with differently chunked columns gives the same rows
  auto z_chunks = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{z->Slice(0, 1), z->Slice(1)});
  auto a_chunks = std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{a});
  auto table = arrow::Table::Make(schema, {z_chunks, a_chunks});
  ASSERT_OK_AND_ASSIGN(auto table_lines, CanonicalJsonLines(*table));
  EXPECT_EQ(table_lines, lines);
  // no rows, no lines
  ASSERT_OK_AND_ASSIGN(auto empty, CanonicalJsonLines(*batch->Slice(0, 0)));
  EXPECT_TRUE(empty.empty());
}

TEST(Canonical, UnsupportedTypesAreErrors) {
  arrow::MonthIntervalBuilder b;
  ASSERT_OK(b.Append(1));
  auto a = b.Finish().ValueOrDie();
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("v", a->type())}), 1, {a});
  auto lines = CanonicalJsonLines(*batch);
  ASSERT_FALSE(lines.ok());
  EXPECT_EQ(lines.status().message(), "canonical JSON does not support month_interval");
}

}  // namespace
}  // namespace tparquet::test
