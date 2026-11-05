// Canonical JSON of Arrow data (testdata/README.md): one compact object per row, floats as
// the hex of their IEEE bits so that comparisons are bit-exact.
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>

#include <arrow/api.h>
#include <arrow/util/base64.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/decimal.h>

#include "tparquet/tparquet.h"

namespace tparquet {
namespace {

using arrow::Status;
using arrow::internal::checked_cast;

void WriteString(std::string_view s, std::string* out) {
  static const char kHex[] = "0123456789abcdef";
  out->push_back('"');
  for (unsigned char c : s) {
    switch (c) {
      case '"':
        out->append("\\\"");
        break;
      case '\\':
        out->append("\\\\");
        break;
      case '\b':
        out->append("\\b");
        break;
      case '\t':
        out->append("\\t");
        break;
      case '\n':
        out->append("\\n");
        break;
      case '\f':
        out->append("\\f");
        break;
      case '\r':
        out->append("\\r");
        break;
      default:
        if (c < 0x20) {
          out->append("\\u00");
          out->push_back(kHex[c >> 4]);
          out->push_back(kHex[c & 0xf]);
        } else {
          out->push_back(static_cast<char>(c));
        }
    }
  }
  out->push_back('"');
}

void WriteBase64(std::string_view b, std::string* out) {
  out->push_back('"');
  out->append(arrow::util::base64_encode(b));
  out->push_back('"');
}

void WriteHex(uint64_t bits, int digits, std::string* out) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "\"0x%0*" PRIx64 "\"", digits, bits);
  out->append(buf);
}

template <typename ArrayType>
void WriteInt(const arrow::Array& a, int64_t i, std::string* out) {
  out->append(std::to_string(checked_cast<const ArrayType&>(a).Value(i)));
}

Status WriteValue(const arrow::Array& a, int64_t i, std::string* out);

Status WriteRow(const arrow::FieldVector& fields, const arrow::ArrayVector& columns, int64_t row,
                std::string* out) {
  out->push_back('{');
  for (size_t c = 0; c < fields.size(); ++c) {
    if (c > 0) out->push_back(',');
    WriteString(fields[c]->name(), out);
    out->push_back(':');
    ARROW_RETURN_NOT_OK(WriteValue(*columns[c], row, out));
  }
  out->push_back('}');
  return Status::OK();
}

Status WriteList(const arrow::Array& values, std::string* out) {
  out->push_back('[');
  for (int64_t j = 0; j < values.length(); ++j) {
    if (j > 0) out->push_back(',');
    ARROW_RETURN_NOT_OK(WriteValue(values, j, out));
  }
  out->push_back(']');
  return Status::OK();
}

Status WriteValue(const arrow::Array& a, int64_t i, std::string* out) {
  if (a.IsNull(i)) {
    out->append("null");
    return Status::OK();
  }
  using T = arrow::Type;
  switch (a.type_id()) {
    case T::BOOL:
      out->append(checked_cast<const arrow::BooleanArray&>(a).Value(i) ? "true" : "false");
      break;
    case T::INT8:
      WriteInt<arrow::Int8Array>(a, i, out);
      break;
    case T::INT16:
      WriteInt<arrow::Int16Array>(a, i, out);
      break;
    case T::INT32:
    case T::DATE32:
    case T::TIME32:
      WriteInt<arrow::Int32Array>(a, i, out);
      break;
    case T::INT64:
    case T::DATE64:
    case T::TIME64:
    case T::TIMESTAMP:
    case T::DURATION:
      WriteInt<arrow::Int64Array>(a, i, out);
      break;
    case T::UINT8:
      WriteInt<arrow::UInt8Array>(a, i, out);
      break;
    case T::UINT16:
      WriteInt<arrow::UInt16Array>(a, i, out);
      break;
    case T::UINT32:
      WriteInt<arrow::UInt32Array>(a, i, out);
      break;
    case T::UINT64:
      WriteInt<arrow::UInt64Array>(a, i, out);
      break;
    case T::HALF_FLOAT:
      WriteHex(checked_cast<const arrow::HalfFloatArray&>(a).Value(i), 4, out);
      break;
    case T::FLOAT: {
      uint32_t bits;
      const float v = checked_cast<const arrow::FloatArray&>(a).Value(i);
      std::memcpy(&bits, &v, 4);
      WriteHex(bits, 8, out);
      break;
    }
    case T::DOUBLE: {
      uint64_t bits;
      const double v = checked_cast<const arrow::DoubleArray&>(a).Value(i);
      std::memcpy(&bits, &v, 8);
      WriteHex(bits, 16, out);
      break;
    }
    case T::DECIMAL32:
      out->append(arrow::Decimal32(checked_cast<const arrow::Decimal32Array&>(a).GetValue(i))
                      .ToIntegerString());
      break;
    case T::DECIMAL64:
      out->append(arrow::Decimal64(checked_cast<const arrow::Decimal64Array&>(a).GetValue(i))
                      .ToIntegerString());
      break;
    case T::DECIMAL128:
      out->append(arrow::Decimal128(checked_cast<const arrow::Decimal128Array&>(a).GetValue(i))
                      .ToIntegerString());
      break;
    case T::DECIMAL256:
      out->append(arrow::Decimal256(checked_cast<const arrow::Decimal256Array&>(a).GetValue(i))
                      .ToIntegerString());
      break;
    case T::STRING:
      WriteString(checked_cast<const arrow::StringArray&>(a).GetView(i), out);
      break;
    case T::LARGE_STRING:
      WriteString(checked_cast<const arrow::LargeStringArray&>(a).GetView(i), out);
      break;
    case T::STRING_VIEW:
      WriteString(checked_cast<const arrow::StringViewArray&>(a).GetView(i), out);
      break;
    case T::BINARY:
      WriteBase64(checked_cast<const arrow::BinaryArray&>(a).GetView(i), out);
      break;
    case T::LARGE_BINARY:
      WriteBase64(checked_cast<const arrow::LargeBinaryArray&>(a).GetView(i), out);
      break;
    case T::BINARY_VIEW:
      WriteBase64(checked_cast<const arrow::BinaryViewArray&>(a).GetView(i), out);
      break;
    case T::FIXED_SIZE_BINARY:
      WriteBase64(checked_cast<const arrow::FixedSizeBinaryArray&>(a).GetView(i), out);
      break;
    case T::LIST:
      return WriteList(*checked_cast<const arrow::ListArray&>(a).value_slice(i), out);
    case T::LARGE_LIST:
      return WriteList(*checked_cast<const arrow::LargeListArray&>(a).value_slice(i), out);
    case T::FIXED_SIZE_LIST:
      return WriteList(*checked_cast<const arrow::FixedSizeListArray&>(a).value_slice(i), out);
    case T::LIST_VIEW:
      return WriteList(*checked_cast<const arrow::ListViewArray&>(a).value_slice(i), out);
    case T::LARGE_LIST_VIEW:
      return WriteList(*checked_cast<const arrow::LargeListViewArray&>(a).value_slice(i), out);
    case T::STRUCT: {
      const auto& s = checked_cast<const arrow::StructArray&>(a);
      arrow::ArrayVector children;
      for (int c = 0; c < s.num_fields(); ++c) children.push_back(s.field(c));
      return WriteRow(s.struct_type()->fields(), children, i, out);
    }
    case T::MAP: {
      const auto& m = checked_cast<const arrow::MapArray&>(a);
      const auto entries = m.value_slice(i);
      const auto& e = checked_cast<const arrow::StructArray&>(*entries);
      arrow::ArrayVector children;
      for (int c = 0; c < e.num_fields(); ++c) children.push_back(e.field(c));
      out->push_back('[');
      for (int64_t j = 0; j < e.length(); ++j) {
        if (j > 0) out->push_back(',');
        ARROW_RETURN_NOT_OK(WriteRow(e.struct_type()->fields(), children, j, out));
      }
      out->push_back(']');
      break;
    }
    case T::DICTIONARY: {
      const auto& d = checked_cast<const arrow::DictionaryArray&>(a);
      return WriteValue(*d.dictionary(), d.GetValueIndex(i), out);
    }
    default:
      return Status::NotImplemented("canonical JSON does not support ", a.type()->ToString());
  }
  return Status::OK();
}

}  // namespace

arrow::Result<std::vector<std::string>> CanonicalJsonLines(const arrow::RecordBatch& batch) {
  std::vector<std::string> lines;
  lines.reserve(static_cast<size_t>(batch.num_rows()));
  for (int64_t row = 0; row < batch.num_rows(); ++row) {
    std::string line;
    ARROW_RETURN_NOT_OK(WriteRow(batch.schema()->fields(), batch.columns(), row, &line));
    lines.push_back(std::move(line));
  }
  return lines;
}

arrow::Result<std::vector<std::string>> CanonicalJsonLines(const arrow::Table& table) {
  std::vector<std::string> lines;
  arrow::TableBatchReader reader(table);
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    ARROW_RETURN_NOT_OK(reader.ReadNext(&batch));
    if (!batch) break;
    ARROW_ASSIGN_OR_RAISE(auto batch_lines, CanonicalJsonLines(*batch));
    for (auto& l : batch_lines) lines.push_back(std::move(l));
  }
  return lines;
}

}  // namespace tparquet
