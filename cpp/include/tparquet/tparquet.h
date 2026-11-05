// tparquet for C++: reading and writing Parquet files with the PCODEC codec (spec/FORMAT.md)
// over the patched Arrow C++ in cpp/arrow (FORK.md).
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/compression.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>

namespace tparquet {

/// Parquet CompressionCodec value of PCODEC (spec section 2).
constexpr int32_t kPcodecCodecId = 20547;
/// Footer key written with every file that has a PCODEC column chunk (spec section 2).
constexpr const char* kPcodecVersionKey = "parquet.pcodec.version";

/// The codec choice for an eligible column (an INT32, INT64, FLOAT or DOUBLE leaf). Other
/// columns always use the base codec.
enum class Policy {
  /// Try both on the first batch with rows and keep the smaller (spec section 6).
  kAuto,
  /// Always PCODEC.
  kPcodec,
  /// Always the base codec.
  kBase,
};

/// "auto", "pcodec" or "base".
arrow::Result<Policy> ParsePolicy(const std::string& name);
const char* PolicyName(Policy policy);

/// Arrow's default writer properties, with decimals of precision up to 18 stored as INT32 or
/// INT64 (so that PCODEC can compress them, and as arrow-rs writes them).
std::shared_ptr<parquet::WriterProperties> DefaultWriterProperties();
/// Arrow's default Arrow writer properties, storing the Arrow schema in the footer (time
/// zones, durations, dictionaries... read back as written).
std::shared_ptr<parquet::ArrowWriterProperties> DefaultArrowWriterProperties();

/// How a writer chooses codecs.
struct WriteOptions {
  /// Policy of the eligible columns that `columns` does not name.
  Policy policy = Policy::kAuto;
  /// Per-column policies: a top-level field name or a dotted leaf path such as
  /// "book.levels.list.element". The longest name that covers a leaf wins; a name that
  /// covers no leaf is an error.
  std::vector<std::pair<std::string, Policy>> columns;
  /// pcodec compression level of the PCODEC columns, 0..12.
  int pcodec_level = 8;
  /// Codec (and level) of every other column.
  arrow::Compression::type base = arrow::Compression::ZSTD;
  int base_level = 3;
  /// Rows per row group at most.
  int64_t max_row_group_length = 1024 * 1024;
  /// Data page version. Unset: V2 when the file has a PCODEC column (spec section 3 writer
  /// recommendations), otherwise V1 (Arrow's default).
  std::optional<parquet::ParquetDataPageVersion> data_page_version;
  /// Everything else (statistics, page size, created_by...). Its compression, encodings and
  /// dictionary settings are kept for the base columns; PCODEC columns always use PLAIN, no
  /// dictionary.
  std::shared_ptr<parquet::WriterProperties> properties = DefaultWriterProperties();
  /// Arrow-specific writer properties.
  std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties = DefaultArrowWriterProperties();
};

/// The codec written for one leaf column.
struct ColumnCodec {
  /// Dotted leaf path.
  std::string path;
  arrow::Compression::type codec;
};

/// Reads the file at `path` (PCODEC or not). `columns`: top-level field names, in the order
/// wanted; empty for every column.
arrow::Result<std::shared_ptr<arrow::Table>> ReadTable(
    const std::string& path, const std::vector<std::string>& columns = {});

/// Reads a Parquet file from `file`.
arrow::Result<std::shared_ptr<arrow::Table>> ReadTable(
    std::shared_ptr<arrow::io::RandomAccessFile> file,
    const std::vector<std::string>& columns = {});

/// Writes `table` to `path`, PCODEC on the eligible columns `options` select.
arrow::Status WriteTable(const arrow::Table& table, const std::string& path,
                         const WriteOptions& options = {});

/// Writes Arrow record batches to Parquet with PCODEC for the eligible columns that the
/// options select. Auto columns are decided on the first batch with rows.
class FileWriter {
 public:
  static arrow::Result<std::unique_ptr<FileWriter>> Open(
      std::shared_ptr<arrow::Schema> schema, std::shared_ptr<arrow::io::OutputStream> sink,
      WriteOptions options = {});
  ~FileWriter();

  arrow::Status WriteRecordBatch(const arrow::RecordBatch& batch);
  arrow::Status WriteTable(const arrow::Table& table);
  /// Finishes the file; the writer cannot be used afterwards.
  arrow::Status Close();

  /// The codec of each leaf column, once chosen (after the first batch with rows, or at
  /// Open when no column is Auto); empty before.
  std::vector<ColumnCodec> codecs() const;
  /// The file's metadata, after Close.
  std::shared_ptr<parquet::FileMetaData> metadata() const;

 private:
  struct Impl;
  explicit FileWriter(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

/// The canonical JSON lines of `table` (testdata/README.md): one compact object per row,
/// floats as the hex of their IEEE bits, binary as base64. Fails for types canonical JSON
/// does not cover (intervals, unions, ...).
arrow::Result<std::vector<std::string>> CanonicalJsonLines(const arrow::Table& table);
arrow::Result<std::vector<std::string>> CanonicalJsonLines(const arrow::RecordBatch& batch);

}  // namespace tparquet
