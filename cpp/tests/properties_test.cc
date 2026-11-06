// Properties: random tables round trip through the writer and both readers, and random
// corruption of a file's PCODEC column chunks gives an error or other rows, never a crash
// (the suite runs under ASan and UBSan in `make -C cpp asan`). TPARQUET_PROPERTY_CASES sets
// the number of cases (default 40).
#include <cstdlib>
#include <random>

#include <arrow/c/bridge.h>
#include <parquet/exception.h>

#include "common.h"
#include "table_gen.h"

#ifdef TPARQUET_HAVE_READ
#include "tparquet_read.h"
#endif

namespace tparquet::test {
namespace {

int Cases() {
  const char* env = std::getenv("TPARQUET_PROPERTY_CASES");
  return env ? std::max(1, std::atoi(env)) : 40;
}

TEST(Properties, RandomTablesRoundTrip) {
  for (int c = 0; c < Cases(); ++c) {
    std::mt19937_64 rng(1000 + c);
    const int64_t rows = static_cast<int64_t>(rng() % 600);
    const auto table = RandomTable(rng, rows, 5);
    WriteOptions o;
    o.policy = static_cast<Policy>(rng() % 3);
    o.pcodec_level = static_cast<int>(rng() % 13);
    o.max_row_group_length = 1 + static_cast<int64_t>(rng() % 300);
    switch (rng() % 3) {
      case 0:
        o.data_page_version = parquet::ParquetDataPageVersion::V1;
        break;
      case 1:
        o.data_page_version = parquet::ParquetDataPageVersion::V2;
        break;
      default:
        break;
    }
    SCOPED_TRACE("case " + std::to_string(c) + ": " + table->schema()->ToString() + " policy " +
                 PolicyName(o.policy));
    ASSERT_OK_AND_ASSIGN(auto file, WriteBuffer(*table, o));
    ASSERT_OK_AND_ASSIGN(auto back, ReadBuffer(file));
    const std::string want = Jsonl(*table);
    EXPECT_EQ(FirstDiff(Jsonl(*back), want), "");
    EXPECT_EQ(Metadata(file)->num_rows(), rows);
#ifdef TPARQUET_HAVE_READ
    TempDir dir;
    const auto path = dir.path / "t.parquet";
    WriteFile(path, file->ToString());
    struct ArrowArrayStream stream;
    ASSERT_EQ(tpr_open(path.c_str(), nullptr, 0, 1 + static_cast<int64_t>(rng() % 100), &stream), TPR_OK);
    ASSERT_OK_AND_ASSIGN(auto reader, arrow::ImportRecordBatchReader(&stream));
    ASSERT_OK_AND_ASSIGN(auto from_rust, reader->ToTable());
    EXPECT_EQ(FirstDiff(Jsonl(*from_rust), want), "") << "the Rust reader";
#endif
  }
}

/// The byte ranges [start, end) of the file's PCODEC column chunks.
std::vector<std::pair<int64_t, int64_t>> PcodecChunks(const std::shared_ptr<parquet::FileMetaData>& md) {
  std::vector<std::pair<int64_t, int64_t>> out;
  for (int rg = 0; rg < md->num_row_groups(); ++rg) {
    for (int c = 0; c < md->num_columns(); ++c) {
      auto chunk = md->RowGroup(rg)->ColumnChunk(c);
      if (chunk->compression() != arrow::Compression::PCODEC || chunk->num_values() == 0) continue;
      const int64_t start = chunk->has_dictionary_page() ? chunk->dictionary_page_offset() : chunk->data_page_offset();
      out.emplace_back(start, start + chunk->total_compressed_size());
    }
  }
  return out;
}

/// Reads every row of `file` with the fork; false on an error (a Status or an exception).
bool ReadsWithoutError(const std::string& file) {
  try {
    auto table = ReadBuffer(arrow::Buffer::FromString(file));
    if (!table.ok()) return false;
    // canonical JSON visits every value, so a garbled array would show under ASan
    return CanonicalJsonLines(**table).ok();
  } catch (const parquet::ParquetException&) {
    return false;
  }
}

TEST(Properties, CorruptPcodecChunksAreErrorsNotCrashes) {
  int cases = 0, errors = 0, payload_errors = 0, payload_cases = 0;
  for (const auto& path : VectorFiles()) {
    const std::string original = ReadFile(path);
    const auto chunks = PcodecChunks(Metadata(path));
    if (chunks.empty()) continue;
    std::mt19937_64 rng(std::hash<std::string>{}(path.filename().string()));
    for (int c = 0; c < std::max(2, Cases() / 4); ++c) {
      std::string bad = original;
      // a few bytes anywhere in a chunk: page headers and payloads
      for (int f = 0; f < 1 + static_cast<int>(rng() % 6); ++f) {
        const auto& [start, end] = chunks[rng() % chunks.size()];
        bad[static_cast<size_t>(start + static_cast<int64_t>(rng() % static_cast<uint64_t>(end - start)))] ^=
            static_cast<char>(1 + rng() % 255);
      }
      errors += !ReadsWithoutError(bad);
      cases++;
      // the second half of every chunk: payload bytes only (headers are at the start)
      bad = original;
      for (const auto& [start, end] : chunks) {
        for (int64_t i = start + (end - start) / 2; i < end; ++i) {
          bad[static_cast<size_t>(i)] = static_cast<char>(rng());
        }
      }
      payload_errors += !ReadsWithoutError(bad);
      payload_cases++;
    }
  }
  EXPECT_GT(cases, 0);
  // flipped bytes are mostly noticed; garbled payloads nearly always (RAW payloads of the
  // right length decode to garbage without an error, as any codec without a checksum would)
  EXPECT_GT(errors, cases / 3);
  EXPECT_GT(payload_errors, payload_cases * 3 / 4);
}

TEST(Properties, TruncatedFilesAreErrors) {
  const std::string original = ReadFile(Testdata() / "files" / "mixed.parquet");
  std::mt19937_64 rng(5);
  for (int c = 0; c < Cases(); ++c) {
    const size_t n = static_cast<size_t>(rng() % original.size());
    EXPECT_FALSE(ReadsWithoutError(original.substr(0, n))) << n;
  }
}

}  // namespace
}  // namespace tparquet::test
