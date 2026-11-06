#include "common.h"

#include <algorithm>
#include <fstream>
#include <random>
#include <sstream>

namespace tparquet::test {

std::filesystem::path Testdata() { return TPARQUET_TESTDATA; }

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot read " + path.string());
  std::ostringstream s;
  s << in.rdbuf();
  return s.str();
}

void WriteFile(const std::filesystem::path& path, const std::string& data) {
  std::ofstream out(path, std::ios::binary);
  out << data;
  if (!out) throw std::runtime_error("cannot write " + path.string());
}

std::vector<std::filesystem::path> VectorFiles(const std::filesystem::path& dir) {
  std::vector<std::filesystem::path> out;
  for (const auto& e : std::filesystem::directory_iterator(dir / "files")) {
    if (e.path().extension() == ".parquet") out.push_back(e.path());
  }
  std::sort(out.begin(), out.end());
  return out;
}

std::string Jsonl(const arrow::Table& table) {
  auto lines = CanonicalJsonLines(table);
  if (!lines.ok()) throw std::runtime_error(lines.status().ToString());
  std::string out;
  for (const auto& l : *lines) out += l + "\n";
  return out;
}

std::string FirstDiff(const std::string& got, const std::string& want) {
  if (got == want) return "";
  std::istringstream a(got), b(want);
  std::string la, lb;
  for (int line = 1;; ++line) {
    const bool ha = static_cast<bool>(std::getline(a, la)), hb = static_cast<bool>(std::getline(b, lb));
    if (!ha && !hb) return "the same lines, other line endings";
    if (!ha || !hb || la != lb) {
      return "line " + std::to_string(line) + "\n  got:  " + (ha ? la : "<none>") + "\n  want: " + (hb ? lb : "<none>");
    }
  }
}

TempDir::TempDir() {
  static std::mt19937_64 rng(std::random_device{}());
  path = std::filesystem::temp_directory_path() / ("tparquet-test-" + std::to_string(rng()));
  std::filesystem::create_directories(path);
}

TempDir::~TempDir() {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
}

std::shared_ptr<parquet::FileMetaData> Metadata(const std::filesystem::path& path) {
  return parquet::ParquetFileReader::OpenFile(path.string())->metadata();
}

std::shared_ptr<parquet::FileMetaData> Metadata(std::shared_ptr<arrow::Buffer> buffer) {
  auto in = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
  return parquet::ParquetFileReader::Open(in)->metadata();
}

std::string FooterValue(const parquet::FileMetaData& metadata, const std::string& key) {
  auto kv = metadata.key_value_metadata();
  if (!kv) return "";
  auto v = kv->Get(key);
  return v.ok() ? *v : "";
}

arrow::Result<std::shared_ptr<arrow::Table>> ReadBuffer(std::shared_ptr<arrow::Buffer> buffer) {
  auto in = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
  ARROW_ASSIGN_OR_RAISE(auto reader, parquet::arrow::OpenFile(in, arrow::default_memory_pool()));
  return reader->ReadTable();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteBuffer(const arrow::Table& table,
                                                          const WriteOptions& options) {
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, FileWriter::Open(table.schema(), sink, options));
  ARROW_RETURN_NOT_OK(writer->WriteTable(table));
  ARROW_RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

}  // namespace tparquet::test
