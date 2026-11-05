// Helpers shared by the tests.
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include "tparquet/tparquet.h"

#define ASSERT_OK(expr)                                  \
  do {                                                   \
    auto _st = (expr);                                   \
    ASSERT_TRUE(_st.ok()) << #expr << ": " << _st.ToString(); \
  } while (0)

#define TPQ_CONCAT_(a, b) a##b
#define TPQ_CONCAT(a, b) TPQ_CONCAT_(a, b)
#define ASSERT_OK_AND_ASSIGN(lhs, rexpr) \
  ASSERT_OK_AND_ASSIGN_IMPL(TPQ_CONCAT(_res_, __LINE__), lhs, rexpr)
#define ASSERT_OK_AND_ASSIGN_IMPL(res, lhs, rexpr)                       \
  auto res = (rexpr);                                                    \
  ASSERT_TRUE(res.ok()) << #rexpr << ": " << res.status().ToString();    \
  lhs = std::move(res).ValueOrDie()

namespace tparquet::test {

std::filesystem::path Testdata();
std::string ReadFile(const std::filesystem::path& path);
void WriteFile(const std::filesystem::path& path, const std::string& data);
/// Every testdata/files/*.parquet, sorted.
std::vector<std::filesystem::path> VectorFiles(const std::filesystem::path& dir = Testdata());
/// The canonical JSON of `table`, one `\n`-terminated line per row.
std::string Jsonl(const arrow::Table& table);
/// "" if `got` == `want`, else the first line that differs (both sides).
std::string FirstDiff(const std::string& got, const std::string& want);
/// A fresh empty directory under the system temp dir, removed by the destructor.
struct TempDir {
  TempDir();
  ~TempDir();
  std::filesystem::path path;
};
/// The file's metadata.
std::shared_ptr<parquet::FileMetaData> Metadata(const std::filesystem::path& path);
std::shared_ptr<parquet::FileMetaData> Metadata(std::shared_ptr<arrow::Buffer> buffer);
/// The footer's value of `key`, or "" without it.
std::string FooterValue(const parquet::FileMetaData& metadata, const std::string& key);
/// The table in `buffer`, read with the fork's parquet::arrow::FileReader.
arrow::Result<std::shared_ptr<arrow::Table>> ReadBuffer(std::shared_ptr<arrow::Buffer> buffer);
/// `table` written by tparquet::FileWriter into memory.
arrow::Result<std::shared_ptr<arrow::Buffer>> WriteBuffer(const arrow::Table& table,
                                                          const WriteOptions& options);

}  // namespace tparquet::test
