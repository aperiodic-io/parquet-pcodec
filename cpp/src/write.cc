// Writing: parquet::arrow::FileWriter with PCODEC (PLAIN, no dictionary) on the eligible
// leaves the policy selects, the base codec elsewhere, and spec section 6's Auto choice.
#include <algorithm>
#include <numeric>

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/util/compression.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/schema.h>

#include "tparquet/tparquet.h"

namespace tparquet {
namespace {

using arrow::Status;
using parquet::ParquetDataPageVersion;

struct Leaf {
  std::shared_ptr<parquet::schema::ColumnPath> path;
  /// none when the leaf is not eligible (spec section 3: INT32, INT64, FLOAT, DOUBLE only)
  std::optional<Policy> policy;
};

bool Eligible(parquet::Type::type t) {
  return t == parquet::Type::INT32 || t == parquet::Type::INT64 || t == parquet::Type::FLOAT ||
         t == parquet::Type::DOUBLE;
}

/// Whether the override `name` applies to the leaf `path`: the leaf or one of its ancestors.
bool Covers(const std::string& name, const std::string& path) {
  return path.compare(0, name.size(), name) == 0 &&
         (path.size() == name.size() || path[name.size()] == '.');
}

/// The longest override that covers `path`, else `fallback`.
Policy Resolve(const std::string& path, const std::vector<std::pair<std::string, Policy>>& overrides,
               Policy fallback) {
  size_t best = 0;
  Policy out = fallback;
  for (const auto& [name, policy] : overrides) {
    if (Covers(name, path) && name.size() > best) {
      best = name.size();
      out = policy;
    }
  }
  return out;
}

}  // namespace

std::shared_ptr<parquet::WriterProperties> DefaultWriterProperties() {
  return parquet::WriterProperties::Builder().enable_store_decimal_as_integer()->build();
}

std::shared_ptr<parquet::ArrowWriterProperties> DefaultArrowWriterProperties() {
  return parquet::ArrowWriterProperties::Builder().store_schema()->build();
}

arrow::Result<Policy> ParsePolicy(const std::string& name) {
  for (Policy p : {Policy::kAuto, Policy::kPcodec, Policy::kBase}) {
    if (name == PolicyName(p)) return p;
  }
  return Status::Invalid("unknown policy \"", name, "\": expected auto, pcodec or base");
}

const char* PolicyName(Policy policy) {
  switch (policy) {
    case Policy::kAuto:
      return "auto";
    case Policy::kPcodec:
      return "pcodec";
    case Policy::kBase:
      break;
  }
  return "base";
}

struct FileWriter::Impl {
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<arrow::io::OutputStream> sink;
  WriteOptions options;
  std::vector<Leaf> leaves;
  /// per leaf, once decided: whether it is PCODEC
  std::vector<bool> pcodec;
  std::unique_ptr<parquet::arrow::FileWriter> writer;
  bool closed = false;

  /// The page version of a file with PCODEC on `pcodec` (spec section 3: V2), or as asked.
  ParquetDataPageVersion Version(const std::vector<bool>& pcodec) const {
    const bool any = std::find(pcodec.begin(), pcodec.end(), true) != pcodec.end();
    return options.data_page_version.value_or(any ? ParquetDataPageVersion::V2
                                                  : ParquetDataPageVersion::V1);
  }

  std::shared_ptr<parquet::WriterProperties> Properties(const std::vector<bool>& pcodec,
                                                        ParquetDataPageVersion version) const {
    parquet::WriterProperties::Builder b(*options.properties);
    b.compression(options.base);
    if (arrow::util::Codec::SupportsCompressionLevel(options.base)) {
      b.compression_level(options.base_level);
    }
    b.max_row_group_length(options.max_row_group_length);
    b.data_page_version(version);
    for (size_t i = 0; i < leaves.size(); ++i) {
      if (!pcodec[i]) continue;
      b.compression(leaves[i].path, arrow::Compression::PCODEC);
      b.compression_level(leaves[i].path, options.pcodec_level);
      b.encoding(leaves[i].path, parquet::Encoding::PLAIN);
      b.disable_dictionary(leaves[i].path);
    }
    return b.build();
  }

  /// Compressed bytes per leaf of `sample` written with PCODEC on the `pcodec` leaves.
  arrow::Result<std::vector<int64_t>> ChunkSizes(const arrow::Table& sample, const std::vector<bool>& pcodec,
                                                 ParquetDataPageVersion version) const {
    ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto w, parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), out,
                                                                   Properties(pcodec, version),
                                                                   options.arrow_properties));
    ARROW_RETURN_NOT_OK(w->WriteTable(sample, std::max<int64_t>(sample.num_rows(), 1)));
    ARROW_RETURN_NOT_OK(w->Close());
    std::vector<int64_t> sizes(leaves.size(), 0);
    const auto md = w->metadata();
    for (int rg = 0; rg < md->num_row_groups(); ++rg) {
      for (size_t i = 0; i < leaves.size(); ++i) {
        sizes[i] += md->RowGroup(rg)->ColumnChunk(static_cast<int>(i))->total_compressed_size();
      }
    }
    return sizes;
  }

  /// Chooses the codecs (with `sample` for the Auto leaves, if any) and opens the file.
  ///
  /// Each Auto leaf gets PCODEC when that is smaller than the base codec, both with the page
  /// version PCODEC files get. When only Auto leaves would use PCODEC, the file keeps the base
  /// codec everywhere (and its own page version) if that is smaller in total: V2 pages store
  /// levels uncompressed, which can cost the other columns more than PCODEC saves.
  Status Open(const arrow::Table* sample) {
    std::vector<bool> forced, automatic;
    for (const auto& l : leaves) {
      forced.push_back(l.policy == Policy::kPcodec);
      automatic.push_back(l.policy == Policy::kAuto);
    }
    pcodec = forced;
    if (sample != nullptr) {
      const auto pcodec_version = options.data_page_version.value_or(ParquetDataPageVersion::V2);
      const std::vector<bool> none(leaves.size(), false);
      ARROW_ASSIGN_OR_RAISE(auto base_sizes, ChunkSizes(*sample, none, pcodec_version));
      ARROW_ASSIGN_OR_RAISE(auto pcodec_sizes, ChunkSizes(*sample, automatic, pcodec_version));
      std::vector<bool> choice(leaves.size());
      int64_t choice_total = 0;
      for (size_t i = 0; i < leaves.size(); ++i) {
        choice[i] = forced[i] || (automatic[i] && pcodec_sizes[i] < base_sizes[i]);
        choice_total += choice[i] ? pcodec_sizes[i] : base_sizes[i];
      }
      ARROW_ASSIGN_OR_RAISE(auto plain_sizes, ChunkSizes(*sample, none, Version(none)));
      const int64_t plain_total = std::accumulate(plain_sizes.begin(), plain_sizes.end(), int64_t{0});
      const bool any_forced = std::find(forced.begin(), forced.end(), true) != forced.end();
      if (any_forced || choice_total < plain_total) pcodec = choice;
    }
    ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), sink,
                                                                   Properties(pcodec, Version(pcodec)),
                                                                   options.arrow_properties));
    return Status::OK();
  }

  Status Usable(const arrow::Schema& other) const {
    if (closed) return Status::Invalid("the writer is closed");
    if (!other.Equals(*schema, /*check_metadata=*/false)) {
      return Status::Invalid("the batch's schema differs from the writer's: ", other.ToString());
    }
    return Status::OK();
  }
};

FileWriter::FileWriter(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
FileWriter::~FileWriter() = default;

arrow::Result<std::unique_ptr<FileWriter>> FileWriter::Open(std::shared_ptr<arrow::Schema> schema,
                                                            std::shared_ptr<arrow::io::OutputStream> sink,
                                                            WriteOptions options) {
  if (options.pcodec_level < 0 || options.pcodec_level > 12) {
    return Status::Invalid("pcodec level ", options.pcodec_level, " not in 0..12");
  }
  auto impl = std::make_unique<Impl>();
  impl->schema = std::move(schema);
  impl->sink = std::move(sink);
  impl->options = std::move(options);
  // the leaves as the file will have them (decimals as integers depend on the properties)
  std::shared_ptr<parquet::SchemaDescriptor> descr;
  const std::vector<bool> none;
  ARROW_RETURN_NOT_OK(parquet::arrow::ToParquetSchema(impl->schema.get(), *impl->options.properties,
                                                      *impl->options.arrow_properties, &descr));
  for (int i = 0; i < descr->num_columns(); ++i) {
    const auto* c = descr->Column(i);
    Leaf leaf{c->path(), std::nullopt};
    if (Eligible(c->physical_type())) {
      leaf.policy = Resolve(c->path()->ToDotString(), impl->options.columns, impl->options.policy);
    }
    impl->leaves.push_back(std::move(leaf));
  }
  for (const auto& [name, policy] : impl->options.columns) {
    const bool covers = std::any_of(impl->leaves.begin(), impl->leaves.end(),
                                    [&](const Leaf& l) { return Covers(name, l.path->ToDotString()); });
    if (!covers) return Status::Invalid("no column named \"", name, "\"");
  }
  const bool any_auto = std::any_of(impl->leaves.begin(), impl->leaves.end(),
                                    [](const Leaf& l) { return l.policy == Policy::kAuto; });
  if (!any_auto) ARROW_RETURN_NOT_OK(impl->Open(nullptr));
  return std::unique_ptr<FileWriter>(new FileWriter(std::move(impl)));
}

arrow::Status FileWriter::WriteRecordBatch(const arrow::RecordBatch& batch) {
  ARROW_RETURN_NOT_OK(impl_->Usable(*batch.schema()));
  if (!impl_->writer) {
    if (batch.num_rows() == 0) return Status::OK();  // Auto waits for rows
    const int64_t n = std::min(batch.num_rows(), impl_->options.max_row_group_length);
    ARROW_ASSIGN_OR_RAISE(auto sample, arrow::Table::FromRecordBatches({batch.Slice(0, n)}));
    ARROW_RETURN_NOT_OK(impl_->Open(sample.get()));
  }
  return impl_->writer->WriteRecordBatch(batch);
}

arrow::Status FileWriter::WriteTable(const arrow::Table& table) {
  ARROW_RETURN_NOT_OK(impl_->Usable(*table.schema()));
  if (!impl_->writer) {
    if (table.num_rows() == 0) return Status::OK();  // Auto waits for rows
    auto sample = table.Slice(0, std::min(table.num_rows(), impl_->options.max_row_group_length));
    ARROW_RETURN_NOT_OK(impl_->Open(sample.get()));
  }
  return impl_->writer->WriteTable(table, impl_->options.max_row_group_length);
}

arrow::Status FileWriter::Close() {
  if (impl_->closed) return Status::Invalid("the writer is closed");
  impl_->closed = true;
  if (!impl_->writer) ARROW_RETURN_NOT_OK(impl_->Open(nullptr));  // no rows: no Auto choice
  return impl_->writer->Close();
}

std::vector<ColumnCodec> FileWriter::codecs() const {
  std::vector<ColumnCodec> out;
  if (!impl_->writer) return out;
  for (size_t i = 0; i < impl_->leaves.size(); ++i) {
    out.push_back({impl_->leaves[i].path->ToDotString(),
                   impl_->pcodec[i] ? arrow::Compression::PCODEC : impl_->options.base});
  }
  return out;
}

std::shared_ptr<parquet::FileMetaData> FileWriter::metadata() const {
  return impl_->closed && impl_->writer ? impl_->writer->metadata() : nullptr;
}

arrow::Status WriteTable(const arrow::Table& table, const std::string& path, const WriteOptions& options) {
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::FileOutputStream::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto writer, FileWriter::Open(table.schema(), sink, options));
  ARROW_RETURN_NOT_OK(writer->WriteTable(table));
  ARROW_RETURN_NOT_OK(writer->Close());
  return sink->Close();
}

}  // namespace tparquet
