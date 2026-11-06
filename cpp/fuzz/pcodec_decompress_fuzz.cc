// libFuzzer target over the fork's decompress path: arrow::util::Codec for PCODEC (the
// fork's compression_pcodec.cc over pcp_decompress). Input: a 4-byte little-endian expected
// size (taken modulo 1 MiB), then the payload. Build with `make -C cpp fuzz-build`; seeds are
// the payload vectors (cpp/fuzz/seeds.py).
#include <cstdint>
#include <cstring>
#include <vector>

#include <arrow/util/compression.h>
#include <parquet/types.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  static const auto codec = arrow::util::Codec::Create(arrow::Compression::PCODEC).ValueOrDie();
  static const auto parquet_codec = parquet::GetCodec(arrow::Compression::PCODEC);
  if (size < 4) return 0;
  uint32_t expected;
  std::memcpy(&expected, data, 4);
  expected %= 1u << 20;
  // exactly the expected size, so that ASan sees any write past it
  std::vector<uint8_t> out(expected);
  const auto n = codec->Decompress(static_cast<int64_t>(size - 4), data + 4, expected, out.data());
  if (n.ok() && *n != static_cast<int64_t>(expected)) __builtin_trap();
  // the Parquet layer's codec decodes the same bytes, or fails the same way
  std::vector<uint8_t> again(expected);
  const auto m = parquet_codec->Decompress(static_cast<int64_t>(size - 4), data + 4, expected, again.data());
  if (n.ok() != m.ok() || (n.ok() && out != again)) __builtin_trap();
  return 0;
}
