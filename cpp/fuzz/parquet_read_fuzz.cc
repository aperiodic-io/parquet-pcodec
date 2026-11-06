// libFuzzer target over reading whole Parquet files with the fork: tparquet::ReadTable and the
// canonical JSON of every value, then Arrow's own Parquet fuzz entry point (FuzzReader: every
// row group and column, page indexes, bloom filters, statistics). Seeds: the vector files, so
// the mutations hit PCODEC pages, headers and footers. The ASan build of Arrow
// (cpp/arrow/build.sh asan) is built with SanitizerCoverage, so Arrow and Parquet code gives
// coverage feedback; the Rust decoder is coverage-fuzzed in rust/core/fuzz.
#include <cstdint>
#include <memory>

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <parquet/exception.h>

#include "tparquet/tparquet.h"

namespace parquet::fuzzing::internal {
// parquet/arrow/fuzz_internal.h, exported by libparquet but not installed
PARQUET_EXPORT ::arrow::Status FuzzReader(const uint8_t* data, int64_t size);
}  // namespace parquet::fuzzing::internal

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(size));
  try {
    auto table = tparquet::ReadTable(std::make_shared<arrow::io::BufferReader>(buffer));
    if (table.ok()) (void)tparquet::CanonicalJsonLines(**table);
  } catch (const parquet::ParquetException&) {
    // parquet::arrow reports errors as statuses; a few lower layers throw
  }
  (void)parquet::fuzzing::internal::FuzzReader(data, static_cast<int64_t>(size));
  return 0;
}
