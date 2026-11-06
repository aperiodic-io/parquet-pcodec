// Reading: parquet::arrow over the fork, with a top-level column projection in request order.
#include <algorithm>
#include <unordered_map>

#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>

#include "tparquet/tparquet.h"

namespace tparquet {
namespace {

void Leaves(const parquet::arrow::SchemaField& field, std::vector<int>* out) {
  if (field.is_leaf()) {
    out->push_back(field.column_index);
  }
  for (const auto& child : field.children) Leaves(child, out);
}

}  // namespace

arrow::Result<std::shared_ptr<arrow::Table>> ReadTable(
    std::shared_ptr<arrow::io::RandomAccessFile> file, const std::vector<std::string>& columns) {
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        parquet::arrow::OpenFile(std::move(file), arrow::default_memory_pool()));
  if (columns.empty()) return reader->ReadTable();

  const auto& fields = reader->manifest().schema_fields;
  std::unordered_map<std::string, int> index;
  for (size_t i = 0; i < fields.size(); ++i) {
    index.emplace(fields[i].field->name(), static_cast<int>(i));  // the first of equal names
  }
  std::vector<int> wanted;
  for (const auto& name : columns) {
    auto it = index.find(name);
    if (it == index.end()) return arrow::Status::Invalid("no column named '", name, "'");
    if (std::find(wanted.begin(), wanted.end(), it->second) != wanted.end()) {
      return arrow::Status::Invalid("column '", name, "' is requested more than once");
    }
    wanted.push_back(it->second);
  }
  std::vector<int> sorted = wanted;
  std::sort(sorted.begin(), sorted.end());
  std::vector<int> leaves;
  for (int i : sorted) Leaves(fields[static_cast<size_t>(i)], &leaves);
  ARROW_ASSIGN_OR_RAISE(auto table, reader->ReadTable(leaves));
  // the table has the requested fields in file order: put them in request order
  std::vector<int> order;
  for (int i : wanted) {
    order.push_back(static_cast<int>(std::lower_bound(sorted.begin(), sorted.end(), i) - sorted.begin()));
  }
  return table->SelectColumns(order);
}

arrow::Result<std::shared_ptr<arrow::Table>> ReadTable(const std::string& path,
                                                       const std::vector<std::string>& columns) {
  ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(path));
  return ReadTable(std::move(file), columns);
}

}  // namespace tparquet
