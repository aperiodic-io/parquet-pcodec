#include "tparquet/tparquet.h"

namespace tparquet {
arrow::Result<Policy> ParsePolicy(const std::string&) { return arrow::Status::NotImplemented("stub"); }
const char* PolicyName(Policy) { return ""; }
struct FileWriter::Impl {};
FileWriter::FileWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
FileWriter::~FileWriter() = default;
arrow::Result<std::unique_ptr<FileWriter>> FileWriter::Open(std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::io::OutputStream>, WriteOptions) {
  return arrow::Status::NotImplemented("stub");
}
arrow::Status FileWriter::WriteRecordBatch(const arrow::RecordBatch&) { return arrow::Status::NotImplemented("stub"); }
arrow::Status FileWriter::WriteTable(const arrow::Table&) { return arrow::Status::NotImplemented("stub"); }
arrow::Status FileWriter::Close() { return arrow::Status::NotImplemented("stub"); }
std::vector<ColumnCodec> FileWriter::codecs() const { return {}; }
std::shared_ptr<parquet::FileMetaData> FileWriter::metadata() const { return nullptr; }
arrow::Status WriteTable(const arrow::Table&, const std::string&, const WriteOptions&) { return arrow::Status::NotImplemented("stub"); }
}  // namespace tparquet
