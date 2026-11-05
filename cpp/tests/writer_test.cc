// The C++ writer: round trips of every type the vectors cover through every policy, page
// version and row group size; codecs, encodings, page types and the footer key per column
// chunk; the type hint in the payload bytes; per-column policies, Auto, levels, errors.
#include <cstring>
#include <map>
#include <set>

#include <arrow/c/bridge.h>
#include <arrow/util/compression.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/schema.h>

#include "common.h"
#include "table_gen.h"

#ifdef TPARQUET_HAVE_READ
#include "tparquet_read.h"
#endif

namespace tparquet::test {
namespace {

using arrow::Compression;

bool Eligible(parquet::Type::type t) {
  return t == parquet::Type::INT32 || t == parquet::Type::INT64 || t == parquet::Type::FLOAT ||
         t == parquet::Type::DOUBLE;
}

WriteOptions With(Policy policy) {
  WriteOptions o;
  o.policy = policy;
  return o;
}

/// codec name per leaf path, over every row group
std::map<std::string, std::set<Compression::type>> Codecs(const parquet::FileMetaData& md) {
  std::map<std::string, std::set<Compression::type>> out;
  for (int rg = 0; rg < md.num_row_groups(); ++rg) {
    for (int c = 0; c < md.num_columns(); ++c) {
      out[md.schema()->Column(c)->path()->ToDotString()].insert(
          md.RowGroup(rg)->ColumnChunk(c)->compression());
    }
  }
  return out;
}

/// page types per leaf path
std::map<std::string, std::set<parquet::PageType::type>> PageTypes(std::shared_ptr<arrow::Buffer> file) {
  auto reader = parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(file));
  auto md = reader->metadata();
  std::map<std::string, std::set<parquet::PageType::type>> out;
  for (int rg = 0; rg < md->num_row_groups(); ++rg) {
    for (int c = 0; c < md->num_columns(); ++c) {
      auto pages = reader->RowGroup(rg)->GetColumnPageReader(c);
      while (auto page = pages->NextPage()) {
        out[md->schema()->Column(c)->path()->ToDotString()].insert(page->type());
      }
    }
  }
  return out;
}

#ifdef TPARQUET_HAVE_READ
/// The rows of a file as the Rust reader (rust/cread) reads them.
std::string RustRows(std::shared_ptr<arrow::Buffer> file) {
  TempDir dir;
  const auto path = dir.path / "t.parquet";
  WriteFile(path, file->ToString());
  struct ArrowArrayStream stream;
  if (tpr_open(path.c_str(), nullptr, 0, 0, &stream) != TPR_OK) {
    char msg[512];
    tpr_last_error(msg, sizeof msg);
    return std::string("tpr_open failed: ") + msg;
  }
  auto reader = arrow::ImportRecordBatchReader(&stream).ValueOrDie();
  auto table = reader->ToTable();
  if (!table.ok()) return "read failed: " + table.status().ToString();
  return Jsonl(**table);
}
#endif

TEST(Writer, ParsesPolicies) {
  for (Policy p : {Policy::kAuto, Policy::kPcodec, Policy::kBase}) {
    ASSERT_OK_AND_ASSIGN(auto back, ParsePolicy(PolicyName(p)));
    EXPECT_EQ(back, p);
  }
  EXPECT_STREQ(PolicyName(Policy::kAuto), "auto");
  EXPECT_STREQ(PolicyName(Policy::kPcodec), "pcodec");
  EXPECT_STREQ(PolicyName(Policy::kBase), "base");
  auto bad = ParsePolicy("fast");
  ASSERT_FALSE(bad.ok());
  EXPECT_EQ(bad.status().message(), "unknown policy \"fast\": expected auto, pcodec or base");
}

TEST(Writer, EveryTypeRoundTripsThroughEveryPolicyPageVersionAndRowGroupSize) {
  const auto table = EveryType(2500);
  const std::string want = Jsonl(*table);
  for (Policy policy : {Policy::kPcodec, Policy::kBase, Policy::kAuto}) {
    for (int version : {-1, 1, 2}) {
      for (int64_t group : {int64_t{1} << 20, int64_t{700}}) {
        SCOPED_TRACE(std::string(PolicyName(policy)) + " v" + std::to_string(version) + " rg " +
                     std::to_string(group));
        WriteOptions o = With(policy);
        if (version > 0) {
          o.data_page_version = version == 1 ? parquet::ParquetDataPageVersion::V1
                                             : parquet::ParquetDataPageVersion::V2;
        }
        o.max_row_group_length = group;
        ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table, o));
        ASSERT_OK_AND_ASSIGN(auto back, ReadBuffer(file));
        EXPECT_EQ(FirstDiff(Jsonl(*back), want), "");
        auto md = Metadata(file);
        EXPECT_EQ(md->num_rows(), 2500);
        EXPECT_EQ(md->num_row_groups(), group == 700 ? 4 : 1);
        // the page version: as asked, else V2 exactly when there is a PCODEC column
        bool any_pcodec = false;
        for (const auto& [path, codecs] : Codecs(*md)) any_pcodec |= codecs.count(Compression::PCODEC) > 0;
        EXPECT_EQ(any_pcodec, policy != Policy::kBase);
        const bool v2 = version == 2 || (version < 0 && any_pcodec);
        for (const auto& [path, types] : PageTypes(file)) {
          EXPECT_EQ(types.count(v2 ? parquet::PageType::DATA_PAGE_V2 : parquet::PageType::DATA_PAGE), 1u)
              << path;
          EXPECT_EQ(types.count(v2 ? parquet::PageType::DATA_PAGE : parquet::PageType::DATA_PAGE_V2), 0u)
              << path;
        }
#ifdef TPARQUET_HAVE_READ
        // the Rust reader reads the C++ files the same: the codec id and payloads agree
        EXPECT_EQ(FirstDiff(RustRows(file), want), "") << "the Rust reader";
#endif
      }
    }
  }
}

TEST(Writer, PcodecOnEligibleColumnsOnlyPlainWithoutDictionary) {
  const auto table = EveryType(1000);
  ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table, With(Policy::kPcodec)));
  auto md = Metadata(file);
  int pcodec = 0;
  for (int c = 0; c < md->num_columns(); ++c) {
    const auto* descr = md->schema()->Column(c);
    auto chunk = md->RowGroup(0)->ColumnChunk(c);
    SCOPED_TRACE(descr->path()->ToDotString());
    if (Eligible(descr->physical_type())) {
      pcodec++;
      EXPECT_EQ(chunk->compression(), Compression::PCODEC);
      EXPECT_FALSE(chunk->has_dictionary_page());
      for (auto e : chunk->encodings()) {
        EXPECT_TRUE(e == parquet::Encoding::PLAIN || e == parquet::Encoding::RLE) << e;
      }
    } else {
      EXPECT_EQ(chunk->compression(), Compression::ZSTD);
    }
  }
  // i32 i64 i8 i16 u8 u16 u32 u64 f32 f64 f64_bits, 3 timestamps, date32, time32, time64,
  // dur, dec9_2, dec18_4 (decimals stored as integers), levels, ints, book.id, bid, ask,
  // size, tags, attrs.value
  EXPECT_EQ(pcodec, 28);
  EXPECT_EQ(FooterValue(*md, kPcodecVersionKey), "1");
  // the file says which: the column descriptors of the decimals
  EXPECT_EQ(md->schema()->Column(md->schema()->ColumnIndex("dec18_4"))->physical_type(),
            parquet::Type::INT64);
  EXPECT_EQ(md->schema()->Column(md->schema()->ColumnIndex("dec9_2"))->physical_type(),
            parquet::Type::INT32);
}

TEST(Writer, BaseWritesNoPcodecAndNoFooterKey) {
  const auto table = EveryType(500);
  ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table, With(Policy::kBase)));
  auto md = Metadata(file);
  for (const auto& [path, codecs] : Codecs(*md)) {
    EXPECT_EQ(codecs, std::set<Compression::type>{Compression::ZSTD}) << path;
  }
  EXPECT_EQ(FooterValue(*md, kPcodecVersionKey), "");
  // another base codec
  WriteOptions o = With(Policy::kBase);
  o.base = Compression::SNAPPY;
  ASSERT_OK_AND_ASSIGN(auto snappy, WriteBuffer(*table, o));
  for (const auto& [path, codecs] : Codecs(*Metadata(snappy))) {
    EXPECT_EQ(codecs, std::set<Compression::type>{Compression::SNAPPY}) << path;
  }
}

/// The PCODEC payload of the first data page of leaf `column` (a required column without
/// dictionary, written with V2 pages: the page is its header, then the payload): the suffix
/// of the column chunk that decodes to `size` bytes.
std::string FirstPayload(const std::shared_ptr<arrow::Buffer>& file, int column, int64_t size) {
  auto md = Metadata(file);
  auto chunk = md->RowGroup(0)->ColumnChunk(column);
  const std::string bytes = file->ToString().substr(static_cast<size_t>(chunk->data_page_offset()),
                                                    static_cast<size_t>(chunk->total_compressed_size()));
  auto codec = arrow::util::Codec::Create(Compression::PCODEC).ValueOrDie();
  std::string out(static_cast<size_t>(size), '\0');
  for (size_t h = 1; h < 64 && h + 2 < bytes.size(); ++h) {
    auto n = codec->Decompress(static_cast<int64_t>(bytes.size() - h),
                               reinterpret_cast<const uint8_t*>(bytes.data() + h), size,
                               reinterpret_cast<uint8_t*>(out.data()));
    if (n.ok()) return bytes.substr(h);
  }
  return "";
}

TEST(Writer, ThePhysicalTypeIsTheTypeHint) {
  const int64_t rows = 4000;
  arrow::Int32Builder i32;
  arrow::Int64Builder i64;
  arrow::FloatBuilder f32;
  arrow::DoubleBuilder f64;
  for (int64_t i = 0; i < rows; ++i) {
    ASSERT_OK(i32.Append(static_cast<int32_t>(i * 3 % 1000)));
    ASSERT_OK(i64.Append(1735689600000 + i * 7 + i % 3));
    ASSERT_OK(f32.Append(static_cast<float>(i % 512) * 0.37f));
    ASSERT_OK(f64.Append((10000 + i % 300) / 100.0));
  }
  auto schema = arrow::schema({arrow::field("i32", arrow::int32(), false), arrow::field("i64", arrow::int64(), false),
                               arrow::field("f32", arrow::float32(), false),
                               arrow::field("f64", arrow::float64(), false)});
  auto table = arrow::Table::Make(schema, {i32.Finish().ValueOrDie(), i64.Finish().ValueOrDie(),
                                           f32.Finish().ValueOrDie(), f64.Finish().ValueOrDie()});
  ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table, With(Policy::kPcodec)));
  // version 1; mode PCO (1) for the integers and the float, DECIMAL (2) for the decimal
  // doubles; the dtype is the column's physical type (spec section 4)
  const std::vector<std::tuple<int, int64_t, int, int>> want = {
      {0, 4, 1, 1}, {1, 8, 1, 2}, {2, 4, 1, 4}, {3, 8, 2, 5}};
  for (const auto& [column, width, mode, dtype] : want) {
    const std::string payload = FirstPayload(file, column, rows * width);
    ASSERT_GE(payload.size(), 3u) << "column " << column << ": no payload found";
    EXPECT_EQ(payload[0], 1);
    EXPECT_EQ(static_cast<int>(payload[1]), mode) << "column " << column;
    EXPECT_EQ(static_cast<int>(payload[2]), dtype) << "column " << column;
  }
}

TEST(Writer, LevelsAndBaseLevels) {
  const auto table = EveryType(3000);
  WriteOptions low = With(Policy::kPcodec), high = With(Policy::kPcodec);
  low.pcodec_level = 0;
  high.pcodec_level = 12;
  ASSERT_OK_AND_ASSIGN(auto a, WriteBuffer(*table, low));
  ASSERT_OK_AND_ASSIGN(auto b, WriteBuffer(*table, high));
  EXPECT_NE(a->size(), b->size());
  EXPECT_TRUE(Jsonl(*ReadBuffer(a).ValueOrDie()) == Jsonl(*ReadBuffer(b).ValueOrDie()));
  for (int level : {-1, 13}) {
    WriteOptions o = With(Policy::kPcodec);
    o.pcodec_level = level;
    auto r = WriteBuffer(*table, o);
    ASSERT_FALSE(r.ok()) << level;
    EXPECT_EQ(r.status().message(), "pcodec level " + std::to_string(level) + " not in 0..12");
  }
  // a base codec without levels ignores base_level
  WriteOptions snappy = With(Policy::kBase);
  snappy.base = Compression::SNAPPY;
  snappy.base_level = 7;
  ASSERT_OK(WriteBuffer(*table, snappy).status());
  // the ZSTD level is passed on
  WriteOptions z1 = With(Policy::kBase), z19 = With(Policy::kBase);
  z1.base_level = 1;
  z19.base_level = 19;
  EXPECT_NE(WriteBuffer(*table, z1).ValueOrDie()->size(), WriteBuffer(*table, z19).ValueOrDie()->size());
}

TEST(Writer, PerColumnPoliciesLongestNameWins) {
  const auto table = EveryType(800);
  WriteOptions o = With(Policy::kBase);
  o.columns = {{"f64", Policy::kPcodec}, {"book", Policy::kPcodec}, {"book.quote.bid", Policy::kBase},
               {"str", Policy::kPcodec}};
  ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table, o));
  auto codecs = Codecs(*Metadata(file));
  const std::set<Compression::type> P{Compression::PCODEC}, Z{Compression::ZSTD};
  EXPECT_EQ(codecs["f64"], P);
  EXPECT_EQ(codecs["i64"], Z);
  EXPECT_EQ(codecs["book.id"], P);
  EXPECT_EQ(codecs["book.quote.ask"], P);
  EXPECT_EQ(codecs["book.quote.bid"], Z);
  EXPECT_EQ(codecs["book.tags.list.element"], P);
  EXPECT_EQ(codecs["str"], Z);  // not eligible: always the base codec
  EXPECT_EQ(codecs["levels.list.element"], Z);
  // names that cover no leaf
  for (const char* name : {"nope", "book.quote.bi", "boo"}) {
    WriteOptions bad = With(Policy::kAuto);
    bad.columns = {{name, Policy::kPcodec}};
    auto r = WriteBuffer(*table, bad);
    ASSERT_FALSE(r.ok()) << name;
    EXPECT_EQ(r.status().message(), std::string("no column named \"") + name + "\"");
  }
}

TEST(Writer, AutoKeepsTheSmallerCodecPerColumn) {
  const int64_t rows = 20000;
  std::mt19937_64 rng(7);
  double palette[7];
  for (auto& p : palette) {
    uint64_t bits = rng();
    std::memcpy(&p, &bits, 8);
  }
  arrow::Int64Builder smooth;
  arrow::DoubleBuilder repeats;
  for (int64_t i = 0; i < rows; ++i) {
    ASSERT_OK(smooth.Append(1735689600000000 + i * 1000 + static_cast<int64_t>(rng() % 7)));
    ASSERT_OK(repeats.Append(palette[i % 7]));  // seven arbitrary doubles: a tiny dictionary
  }
  auto schema = arrow::schema({arrow::field("smooth", arrow::int64()), arrow::field("repeats", arrow::float64())});
  auto table = arrow::Table::Make(schema, {smooth.Finish().ValueOrDie(), repeats.Finish().ValueOrDie()});
  ASSERT_OK_AND_ASSIGN(auto sink, arrow::io::BufferOutputStream::Create());
  ASSERT_OK_AND_ASSIGN(auto writer, FileWriter::Open(schema, sink, With(Policy::kAuto)));
  EXPECT_TRUE(writer->codecs().empty()) << "Auto decides on the first batch with rows";
  ASSERT_OK(writer->WriteTable(*table->Slice(0, 0)));
  EXPECT_TRUE(writer->codecs().empty()) << "an empty batch decides nothing";
  ASSERT_OK(writer->WriteTable(*table));
  const auto codecs = writer->codecs();
  ASSERT_EQ(codecs.size(), 2u);
  EXPECT_EQ(codecs[0].path, "smooth");
  EXPECT_EQ(codecs[0].codec, Compression::PCODEC);
  EXPECT_EQ(codecs[1].path, "repeats");
  EXPECT_EQ(codecs[1].codec, Compression::ZSTD);
  ASSERT_OK(writer->Close());
  ASSERT_NE(writer->metadata(), nullptr);
  EXPECT_EQ(writer->metadata()->num_rows(), rows);
  auto file = sink->Finish().ValueOrDie();
  auto in_file = Codecs(*Metadata(file));
  EXPECT_EQ(in_file["smooth"], std::set<Compression::type>{Compression::PCODEC});
  EXPECT_EQ(in_file["repeats"], std::set<Compression::type>{Compression::ZSTD});
  EXPECT_TRUE(Jsonl(*ReadBuffer(file).ValueOrDie()) == Jsonl(*table));
  // forcing either way wins over Auto
  WriteOptions forced = With(Policy::kAuto);
  forced.columns = {{"smooth", Policy::kBase}, {"repeats", Policy::kPcodec}};
  auto forced_codecs = Codecs(*Metadata(WriteBuffer(*table, forced).ValueOrDie()));
  EXPECT_EQ(forced_codecs["smooth"], std::set<Compression::type>{Compression::ZSTD});
  EXPECT_EQ(forced_codecs["repeats"], std::set<Compression::type>{Compression::PCODEC});
}

TEST(Writer, AutoKeepsTheBaseCodecWhenV2PagesCostMoreThanPcodecSaves) {
  // one Auto column that PCODEC makes a little smaller, and optional columns whose definition
  // levels ZSTD compresses away in V1 pages but V2 pages store uncompressed
  const int64_t rows = 50000;
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::mt19937_64 rng(3);
  arrow::Int64Builder small;
  for (int64_t i = 0; i < rows; ++i) ASSERT_OK(small.Append(static_cast<int64_t>(rng() % 50)));
  fields.push_back(arrow::field("small", arrow::int64(), false));
  arrays.push_back(small.Finish().ValueOrDie());
  for (int c = 0; c < 8; ++c) {
    arrow::StringBuilder s;
    for (int64_t i = 0; i < rows; ++i) {
      if ((i * (c + 3)) % 7 < 3) {
        ASSERT_OK(s.AppendNull());
      } else {
        ASSERT_OK(s.Append("x"));
      }
    }
    fields.push_back(arrow::field("s" + std::to_string(c), arrow::utf8()));
    arrays.push_back(s.Finish().ValueOrDie());
  }
  auto table = arrow::Table::Make(arrow::schema(fields), arrays);
  // the per-column choice alone would take PCODEC for "small"...
  ASSERT_OK_AND_ASSIGN(auto pcodec, WriteBuffer(*table, With(Policy::kPcodec)));
  WriteOptions base_v2 = With(Policy::kBase);
  base_v2.data_page_version = parquet::ParquetDataPageVersion::V2;
  ASSERT_OK_AND_ASSIGN(auto base2, WriteBuffer(*table, base_v2));
  EXPECT_LT(Metadata(pcodec)->RowGroup(0)->ColumnChunk(0)->total_compressed_size(),
            Metadata(base2)->RowGroup(0)->ColumnChunk(0)->total_compressed_size());
  // ...but the whole file is smaller with the base codec and V1 pages
  ASSERT_OK_AND_ASSIGN(auto base1, WriteBuffer(*table, With(Policy::kBase)));
  EXPECT_LT(base1->size(), pcodec->size());
  ASSERT_OK_AND_ASSIGN(auto sink, arrow::io::BufferOutputStream::Create());
  ASSERT_OK_AND_ASSIGN(auto writer, FileWriter::Open(table->schema(), sink, With(Policy::kAuto)));
  ASSERT_OK(writer->WriteTable(*table));
  for (const auto& c : writer->codecs()) EXPECT_EQ(c.codec, Compression::ZSTD) << c.path;
  ASSERT_OK(writer->Close());
  auto file = sink->Finish().ValueOrDie();
  EXPECT_EQ(FooterValue(*Metadata(file), kPcodecVersionKey), "");
  for (const auto& [path, types] : PageTypes(file)) {
    EXPECT_EQ(types.count(parquet::PageType::DATA_PAGE), 1u) << path;  // V1 pages
    EXPECT_EQ(types.count(parquet::PageType::DATA_PAGE_V2), 0u) << path;
  }
  // a column forced to PCODEC turns the whole-file comparison off
  WriteOptions forced = With(Policy::kAuto);
  forced.columns = {{"small", Policy::kPcodec}};
  EXPECT_EQ(Codecs(*Metadata(WriteBuffer(*table, forced).ValueOrDie()))["small"],
            std::set<Compression::type>{Compression::PCODEC});
}

TEST(Writer, EmptyTables) {
  const auto table = EveryType(10)->Slice(0, 0);
  for (Policy policy : {Policy::kPcodec, Policy::kBase, Policy::kAuto}) {
    SCOPED_TRACE(PolicyName(policy));
    ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table, With(policy)));
    ASSERT_OK_AND_ASSIGN(auto back, ReadBuffer(file));
    EXPECT_EQ(back->num_rows(), 0);
    EXPECT_TRUE(back->schema()->Equals(*table->schema(), false)) << back->schema()->ToString();
    auto md = Metadata(file);
    EXPECT_EQ(md->num_rows(), 0);
    // Auto never saw rows: nothing is PCODEC
    bool any_pcodec = false;
    for (const auto& [path, codecs] : Codecs(*md)) any_pcodec |= codecs.count(Compression::PCODEC) > 0;
    EXPECT_EQ(any_pcodec, policy == Policy::kPcodec && md->num_row_groups() > 0);
    EXPECT_EQ(FooterValue(*md, kPcodecVersionKey), any_pcodec ? "1" : "");
  }
}

TEST(Writer, FooterKeyIsAddedOnceAndUserMetadataKept) {
  auto table = EveryType(100);
  auto meta = arrow::key_value_metadata({"x", kPcodecVersionKey}, {"y", "1"});
  ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table->ReplaceSchemaMetadata(meta), With(Policy::kPcodec)));
  auto kv = Metadata(file)->key_value_metadata();
  ASSERT_NE(kv, nullptr);
  int found = 0;
  for (int64_t i = 0; i < kv->size(); ++i) found += kv->key(i) == kPcodecVersionKey;
  EXPECT_EQ(found, 1);
  EXPECT_EQ(kv->Get("x").ValueOr(""), "y");
  ASSERT_OK_AND_ASSIGN(auto plain, WriteBuffer(*table, With(Policy::kPcodec)));
  EXPECT_EQ(FooterValue(*Metadata(plain), kPcodecVersionKey), "1");
}

TEST(Writer, StreamingWriterLifecycle) {
  const auto table = EveryType(3000);
  ASSERT_OK_AND_ASSIGN(auto sink, arrow::io::BufferOutputStream::Create());
  WriteOptions o = With(Policy::kPcodec);
  o.max_row_group_length = 1000;
  ASSERT_OK_AND_ASSIGN(auto writer, FileWriter::Open(table->schema(), sink, o));
  EXPECT_EQ(writer->codecs().size(), 36u) << "no Auto column: decided at Open";
  EXPECT_EQ(writer->metadata(), nullptr);
  arrow::TableBatchReader batches(*table);
  batches.set_chunksize(450);
  std::shared_ptr<arrow::RecordBatch> batch, first;
  int n = 0;
  while (batches.ReadNext(&batch).ok() && batch) {
    ASSERT_OK(writer->WriteRecordBatch(*batch));
    if (!first) first = batch;
    n++;
  }
  EXPECT_EQ(n, 7);
  ASSERT_OK(writer->Close());
  EXPECT_FALSE(writer->WriteRecordBatch(*first).ok());
  EXPECT_FALSE(writer->Close().ok());
  auto file = sink->Finish().ValueOrDie();
  EXPECT_EQ(Metadata(file)->num_row_groups(), 3);
  EXPECT_TRUE(Jsonl(*ReadBuffer(file).ValueOrDie()) == Jsonl(*table));
  // a batch of another schema is refused
  ASSERT_OK_AND_ASSIGN(auto sink2, arrow::io::BufferOutputStream::Create());
  ASSERT_OK_AND_ASSIGN(auto w2, FileWriter::Open(table->schema(), sink2, o));
  auto other = arrow::RecordBatch::Make(arrow::schema({arrow::field("z", arrow::int32())}), 0,
                                        {arrow::MakeEmptyArray(arrow::int32()).ValueOrDie()});
  EXPECT_FALSE(w2->WriteRecordBatch(*other).ok());
}

TEST(Writer, WriteTableAndReadTableByPath) {
  TempDir dir;
  const auto path = (dir.path / "t.parquet").string();
  const auto table = EveryType(1200);
  ASSERT_OK(WriteTable(*table, path));
  ASSERT_OK_AND_ASSIGN(auto all, ReadTable(path));
  EXPECT_TRUE(Jsonl(*all) == Jsonl(*table));
  // a projection, in the requested order
  ASSERT_OK_AND_ASSIGN(auto some, ReadTable(path, {"book", "f64", "i32"}));
  ASSERT_EQ(some->num_columns(), 3);
  EXPECT_EQ(some->field(0)->name(), "book");
  EXPECT_EQ(some->field(1)->name(), "f64");
  EXPECT_EQ(some->field(2)->name(), "i32");
  ASSERT_OK_AND_ASSIGN(auto want, table->SelectColumns({table->schema()->GetFieldIndex("book"),
                                                        table->schema()->GetFieldIndex("f64"),
                                                        table->schema()->GetFieldIndex("i32")}));
  EXPECT_TRUE(Jsonl(*some) == Jsonl(*want));
  auto unknown = ReadTable(path, {"i32", "nope"});
  ASSERT_FALSE(unknown.ok());
  EXPECT_EQ(unknown.status().message(), "no column named 'nope'");
  auto twice = ReadTable(path, {"i32", "f64", "i32"});
  ASSERT_FALSE(twice.ok());
  EXPECT_EQ(twice.status().message(), "column 'i32' is requested more than once");
  EXPECT_TRUE(ReadTable((dir.path / "missing.parquet").string()).status().IsIOError());
  // the default is Auto: the smooth columns get PCODEC
  auto codecs = Codecs(*Metadata(std::filesystem::path(path)));
  EXPECT_EQ(codecs["ts_ms_utc"], std::set<Compression::type>{Compression::PCODEC});
}

}  // namespace
}  // namespace tparquet::test
