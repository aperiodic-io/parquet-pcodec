#include "table_gen.h"

#include <cmath>
#include <cstring>
#include <stdexcept>
#include <string>

#include <arrow/util/decimal.h>

namespace tparquet::test {
namespace {

void Check(const arrow::Status& st) {
  if (!st.ok()) throw std::runtime_error(st.ToString());
}

template <typename T>
T Get(arrow::Result<T> r) {
  if (!r.ok()) throw std::runtime_error(r.status().ToString());
  return std::move(r).ValueOrDie();
}

double Bits64(uint64_t b) {
  double d;
  std::memcpy(&d, &b, 8);
  return d;
}

float Bits32(uint32_t b) {
  float f;
  std::memcpy(&f, &b, 4);
  return f;
}

const double kDoubleSpecials[] = {Bits64(0x7ff8000000000001), Bits64(0xfff8000000000000), -0.0, 0.0,
                                  INFINITY, -INFINITY, Bits64(1), Bits64(0x000fffffffffffff),
                                  Bits64(0x7ff0000000000001)};
const float kFloatSpecials[] = {Bits32(0x7fc00001), Bits32(0xffc00000), -0.0f, INFINITY, -INFINITY,
                                Bits32(1), Bits32(0x7f800001)};

/// Appends `rows` values from `value(i)`, null where `null(i)`.
template <typename Builder, typename F, typename N>
std::shared_ptr<arrow::Array> Column(std::unique_ptr<Builder> b, int64_t rows, F value, N null) {
  for (int64_t i = 0; i < rows; ++i) {
    if (null(i)) {
      Check(b->AppendNull());
    } else {
      Check(b->Append(value(i)));
    }
  }
  return Get(b->Finish());
}

template <typename Builder>
std::unique_ptr<Builder> Typed(std::shared_ptr<arrow::DataType> type) {
  return std::make_unique<Builder>(std::move(type), arrow::default_memory_pool());
}

auto Never = [](int64_t) { return false; };

}  // namespace

std::shared_ptr<arrow::Table> EveryType(int64_t rows, uint64_t seed) {
  std::mt19937_64 rng(seed);
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  auto add = [&](const std::string& name, std::shared_ptr<arrow::Array> a, bool nullable = true) {
    fields.push_back(arrow::field(name, a->type(), nullable));
    arrays.push_back(std::move(a));
  };
  auto some_null = [&](int k) { return [k](int64_t i) { return i % k == k - 1; }; };

  add("i32", Column(std::make_unique<arrow::Int32Builder>(), rows,
                    [](int64_t i) { return static_cast<int32_t>(i * 7 - 1000); }, Never), false);
  add("i64", Column(std::make_unique<arrow::Int64Builder>(), rows,
                    [&](int64_t) { return static_cast<int64_t>(rng()); }, some_null(9)));
  add("i8", Column(std::make_unique<arrow::Int8Builder>(), rows,
                   [](int64_t i) { return static_cast<int8_t>(i % 256 - 128); }, some_null(5)));
  add("i16", Column(std::make_unique<arrow::Int16Builder>(), rows,
                    [](int64_t i) { return static_cast<int16_t>(-i); }, Never));
  add("u8", Column(std::make_unique<arrow::UInt8Builder>(), rows,
                   [](int64_t i) { return static_cast<uint8_t>(i); }, Never));
  add("u16", Column(std::make_unique<arrow::UInt16Builder>(), rows,
                    [](int64_t i) { return static_cast<uint16_t>(65535 - i); }, Never));
  add("u32", Column(std::make_unique<arrow::UInt32Builder>(), rows,
                    [](int64_t i) { return static_cast<uint32_t>(4294967295u - i); }, some_null(7)));
  add("u64", Column(std::make_unique<arrow::UInt64Builder>(), rows,
                    [](int64_t i) { return UINT64_MAX - static_cast<uint64_t>(i); }, Never));
  add("f16", Column(std::make_unique<arrow::HalfFloatBuilder>(), rows,
                    [](int64_t i) { return static_cast<uint16_t>(0x3c00 + i % 100); }, some_null(4)));
  add("f32", Column(std::make_unique<arrow::FloatBuilder>(), rows,
                    [](int64_t i) {
                      return i % 10 == 3 ? kFloatSpecials[i % 7] : static_cast<float>(i % 1000) / 4.0f;
                    },
                    some_null(11)));
  add("f64", Column(std::make_unique<arrow::DoubleBuilder>(), rows,
                    [](int64_t i) {
                      return i % 10 == 3 ? kDoubleSpecials[i % 9] : (10000 + i % 300) / 100.0;
                    },
                    Never), false);
  add("f64_bits", Column(std::make_unique<arrow::DoubleBuilder>(), rows,
                         [&](int64_t) { return Bits64(rng()); }, some_null(13)));
  // (no timestamp[s]: Parquet has no seconds unit, and Arrow C++ stores and reads it back as
  // milliseconds, so its stored integers change)
  add("ts_ms_utc",
      Column(Typed<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")), rows,
             [](int64_t i) { return 1735689600000 + i * 1250; }, some_null(17)));
  add("ts_us", Column(Typed<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MICRO)), rows,
                      [&](int64_t i) { return 1735689600000000 + i * 1000 + static_cast<int64_t>(rng() % 50); },
                      Never));
  add("ts_ns_ny",
      Column(Typed<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::NANO, "America/New_York")),
             rows, [](int64_t i) { return 1735689600000000000 + i * 7000001; }, some_null(19)));
  add("date32", Column(std::make_unique<arrow::Date32Builder>(), rows,
                       [](int64_t i) { return static_cast<int32_t>(20089 + i / 100); }, Never));
  add("time32_ms", Column(Typed<arrow::Time32Builder>(arrow::time32(arrow::TimeUnit::MILLI)), rows,
                          [](int64_t i) { return static_cast<int32_t>(i * 1000 % 86400000); }, Never));
  add("time64_us", Column(Typed<arrow::Time64Builder>(arrow::time64(arrow::TimeUnit::MICRO)), rows,
                          [](int64_t i) { return i * 1000003 % 86400000000; }, some_null(23)));
  add("dur_ns", Column(Typed<arrow::DurationBuilder>(arrow::duration(arrow::TimeUnit::NANO)), rows,
                       [](int64_t i) { return -i * 999; }, Never));
  add("dec9_2", Column(Typed<arrow::Decimal128Builder>(arrow::decimal128(9, 2)), rows,
                       [](int64_t i) { return arrow::Decimal128(-50000 + i * 3); }, some_null(6)));
  add("dec18_4", Column(Typed<arrow::Decimal128Builder>(arrow::decimal128(18, 4)), rows,
                        [](int64_t i) { return arrow::Decimal128(1234500 + i * 101); }, Never));
  add("dec38", Column(Typed<arrow::Decimal128Builder>(arrow::decimal128(38, 0)), rows,
                      [](int64_t i) {
                        return arrow::Decimal128("-99999999999999999999999999999999999999") +
                               arrow::Decimal128(i);
                      },
                      Never));
  add("str", Column(std::make_unique<arrow::StringBuilder>(), rows,
                    [](int64_t i) {
                      return "trade #" + std::to_string(i) + " \xc3\xbcn\xc3\xaf" "code \"quoted\"\n";
                    },
                    some_null(8)));
  add("bin", Column(std::make_unique<arrow::BinaryBuilder>(), rows,
                    [](int64_t i) { return std::string(static_cast<size_t>(i % 5), static_cast<char>(i)); },
                    some_null(10)));
  add("fixed", Column(Typed<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(3)), rows,
                      [](int64_t i) { return std::string(3, static_cast<char>(i % 256)); }, Never));
  add("bool", Column(std::make_unique<arrow::BooleanBuilder>(), rows,
                     [](int64_t i) { return i % 3 == 0; }, some_null(12)));

  {  // a dictionary of strings
    arrow::StringDictionaryBuilder b;
    const char* syms[] = {"BTC-USD", "ETH-USD", "SOL-USD"};
    for (int64_t i = 0; i < rows; ++i) {
      if (i % 14 == 13) {
        Check(b.AppendNull());
      } else {
        Check(b.Append(syms[i % 3]));
      }
    }
    add("dict", Get(b.Finish()));
  }
  {  // list<double>: nulls, empty lists, null elements
    arrow::ListBuilder b(arrow::default_memory_pool(), std::make_shared<arrow::DoubleBuilder>());
    auto* v = static_cast<arrow::DoubleBuilder*>(b.value_builder());
    for (int64_t i = 0; i < rows; ++i) {
      if (i % 10 == 0) {
        Check(b.AppendNull());
        continue;
      }
      Check(b.Append());
      for (int64_t j = 0; j < i % 4; ++j) {
        if (j == 2 && i % 3 == 0) {
          Check(v->AppendNull());
        } else {
          Check(v->Append(j == 1 && i % 7 == 0 ? -0.0 : 250.0 + (i + j) / 20.0));
        }
      }
    }
    add("levels", Get(b.Finish()));
  }
  {  // list<int64> without nulls
    arrow::ListBuilder b(arrow::default_memory_pool(), std::make_shared<arrow::Int64Builder>());
    auto* v = static_cast<arrow::Int64Builder*>(b.value_builder());
    for (int64_t i = 0; i < rows; ++i) {
      Check(b.Append());
      for (int64_t j = 0; j < i % 3; ++j) Check(v->Append(i * 1000 + j));
    }
    add("ints", Get(b.Finish()), false);
  }
  {  // struct<id: int64, quote: struct<bid: double, ask: double, size: int32>, tags: list<int32>>
    auto quote_type = arrow::struct_({arrow::field("bid", arrow::float64()),
                                      arrow::field("ask", arrow::float64(), false),
                                      arrow::field("size", arrow::int32())});
    auto tags_type = arrow::list(arrow::int32());
    auto book_type = arrow::struct_({arrow::field("id", arrow::int64(), false),
                                     arrow::field("quote", quote_type), arrow::field("tags", tags_type)});
    auto quote = std::make_shared<arrow::StructBuilder>(
        quote_type, arrow::default_memory_pool(),
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{std::make_shared<arrow::DoubleBuilder>(),
                                                          std::make_shared<arrow::DoubleBuilder>(),
                                                          std::make_shared<arrow::Int32Builder>()});
    auto tags = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                     std::make_shared<arrow::Int32Builder>());
    arrow::StructBuilder book(book_type, arrow::default_memory_pool(),
                              {std::make_shared<arrow::Int64Builder>(), quote, tags});
    auto* id = static_cast<arrow::Int64Builder*>(book.field_builder(0));
    auto* bid = static_cast<arrow::DoubleBuilder*>(quote->field_builder(0));
    auto* ask = static_cast<arrow::DoubleBuilder*>(quote->field_builder(1));
    auto* size = static_cast<arrow::Int32Builder*>(quote->field_builder(2));
    auto* tag = static_cast<arrow::Int32Builder*>(tags->value_builder());
    for (int64_t i = 0; i < rows; ++i) {
      if (i % 15 == 14) {
        Check(book.AppendNull());
        continue;
      }
      Check(book.Append());
      Check(id->Append(i));
      if (i % 6 == 5) {
        Check(quote->AppendNull());
      } else {
        Check(quote->Append());
        if (i % 4 == 1) {
          Check(bid->AppendNull());
        } else {
          Check(bid->Append(1000.0 + i / 8.0));
        }
        Check(ask->Append(1000.5 + i / 8.0));
        Check(size->Append(static_cast<int32_t>(i * 100 % 9000)));
      }
      Check(tags->Append());
      for (int64_t j = 0; j < i % 3; ++j) Check(tag->Append(static_cast<int32_t>(j)));
    }
    add("book", Get(book.Finish()));
  }
  {  // map<string, int64>
    arrow::MapBuilder b(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(),
                        std::make_shared<arrow::Int64Builder>());
    auto* k = static_cast<arrow::StringBuilder*>(b.key_builder());
    auto* v = static_cast<arrow::Int64Builder*>(b.item_builder());
    for (int64_t i = 0; i < rows; ++i) {
      if (i % 9 == 4) {
        Check(b.AppendNull());
        continue;
      }
      Check(b.Append());
      for (int64_t j = 0; j < i % 3; ++j) {
        Check(k->Append("k" + std::to_string(j)));
        if (j == 1) {
          Check(v->AppendNull());
        } else {
          Check(v->Append(i - j));
        }
      }
    }
    add("attrs", Get(b.Finish()));
  }
  return arrow::Table::Make(arrow::schema(fields), arrays, rows);
}

namespace {

std::shared_ptr<arrow::Array> RandomColumn(std::mt19937_64& rng, int64_t rows, int depth) {
  const double null_rate = (rng() % 3 == 0) ? 0.0 : static_cast<double>(rng() % 30) / 100.0;
  std::bernoulli_distribution is_null(null_rate);
  auto null = [&](int64_t) { return is_null(rng); };
  const int kind = static_cast<int>(rng() % (depth > 0 ? 11 : 9));
  switch (kind) {
    case 0:
      return Column(std::make_unique<arrow::Int32Builder>(), rows,
                    [&](int64_t i) { return rng() % 2 ? static_cast<int32_t>(rng()) : static_cast<int32_t>(i % 1000); },
                    null);
    case 1:
      return Column(std::make_unique<arrow::Int64Builder>(), rows,
                    [&](int64_t i) { return rng() % 4 ? 1700000000000 + i * 37 : static_cast<int64_t>(rng()); },
                    null);
    case 2:
      return Column(std::make_unique<arrow::FloatBuilder>(), rows,
                    [&](int64_t i) {
                      return rng() % 3 ? static_cast<float>(i % 400) / 4.0f
                                       : Bits32(static_cast<uint32_t>(rng()));
                    },
                    null);
    case 3:
      return Column(std::make_unique<arrow::DoubleBuilder>(), rows,
                    [&](int64_t) {
                      return rng() % 5 ? static_cast<double>(static_cast<int64_t>(rng() % 200000) - 100000) / 100.0
                                       : Bits64(rng());
                    },
                    null);
    case 4:
      return Column(Typed<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")), rows,
                    [&](int64_t i) { return 1700000000000000 + i * 1000 + static_cast<int64_t>(rng() % 999); },
                    null);
    case 5:
      return Column(Typed<arrow::Decimal128Builder>(arrow::decimal128(18, 4)), rows,
                    [&](int64_t) { return arrow::Decimal128(static_cast<int64_t>(rng() % 20000000) - 10000000); },
                    null);
    case 6:
      return Column(std::make_unique<arrow::StringBuilder>(), rows,
                    [&](int64_t) { return std::string(rng() % 4, static_cast<char>('a' + rng() % 3)); }, null);
    case 7:
      return Column(std::make_unique<arrow::BooleanBuilder>(), rows, [&](int64_t) { return rng() % 2 == 0; },
                    null);
    case 8:
      return Column(std::make_unique<arrow::UInt32Builder>(), rows,
                    [&](int64_t) { return static_cast<uint32_t>(rng()); }, null);
    case 9: {  // a list of a random column
      const int64_t n = rows * 2;
      auto values = RandomColumn(rng, n, depth - 1);
      arrow::Int32Builder offsets;
      int32_t at = 0;
      Check(offsets.Append(0));
      for (int64_t i = 0; i < rows; ++i) {
        at += static_cast<int32_t>(rng() % 3);
        if (at > n) at = static_cast<int32_t>(n);
        Check(offsets.Append(at));
      }
      // the last offset need not reach the end of the values
      auto off = Get(offsets.Finish());
      return Get(arrow::ListArray::FromArrays(*off, *values));
    }
    default: {  // a struct of two random columns
      auto a = RandomColumn(rng, rows, depth - 1);
      auto b = RandomColumn(rng, rows, depth - 1);
      return Get(arrow::StructArray::Make({a, b}, std::vector<std::string>{"a", "b"}));
    }
  }
}

}  // namespace

std::shared_ptr<arrow::Table> RandomTable(std::mt19937_64& rng, int64_t rows, int max_columns) {
  const int n = 1 + static_cast<int>(rng() % static_cast<uint64_t>(max_columns));
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (int c = 0; c < n; ++c) {
    auto a = RandomColumn(rng, rows, 1);
    fields.push_back(arrow::field("c" + std::to_string(c), a->type()));
    arrays.push_back(std::move(a));
  }
  return arrow::Table::Make(arrow::schema(fields), arrays, rows);
}

}  // namespace tparquet::test
