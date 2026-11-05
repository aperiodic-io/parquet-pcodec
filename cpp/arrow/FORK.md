# Fork of Apache Arrow C++ (Arrow and Parquet)

The C++ side of tparquet is Apache Arrow C++ with the PCODEC codec of `spec/FORMAT.md` added.
The Arrow source tree is too large to vendor (36 MB for `cpp/` alone), so the fork is a patch
over the verified source release:

| | |
|---|---|
| upstream | Apache Arrow 26.0.1, `apache-arrow-26.0.1.tar.gz` |
| source | `https://dlcdn.apache.org/arrow/arrow-26.0.1/apache-arrow-26.0.1.tar.gz` |
| sha512 | `2ee7efc6ae5b1a2d2c4f2daa52110d16d2e64c02f27f341b462459341528bdcdc796c043c4834baa80dbcb4bac0219d1c705741485973e9207656124f83e74dd` |
| patch | `FORK.patch`: the complete diff of `cpp/`, 15 files |

The sha512 is the one Apache publishes next to the tarball
(`apache-arrow-26.0.1.tar.gz.sha512`).

- `prepare.sh [DEST]` downloads the tarball (cached in `target/cpp/dl`), checks the sha512,
  extracts it and applies `FORK.patch` into `target/cpp/arrow-src`.
- `deps.sh` builds the two dependencies Ubuntu 24.04 packages too old (xsimd 14.3.0 and
  simdjson 4.6.11, from Debian's source tarballs, checked by sha256) into `target/cpp/deps`.
  Arrow's own bundled-dependency downloads go to github.com; everything else comes from apt.
- `build.sh release|asan` configures, builds and installs a minimal Arrow (Arrow core, IPC,
  Parquet; ZSTD, Snappy, LZ4, Brotli, zlib; shared libraries) with `-DARROW_WITH_PCODEC=ON`
  into `target/cpp/arrow-<variant>/install`. `asan` is clang with ASan, UBSan and
  SanitizerCoverage.
- `scripts/check-cpp-fork.sh` downloads and verifies the release, checks that this file
  records its sha512, that `FORK.patch` applies cleanly, that the files it changes are exactly
  the ones listed below, and (with the `thrift` compiler installed) that its change to the
  checked-in generated thrift code is what the compiler generates for its change to
  `parquet.thrift`. `scripts/check-cpp-fork.sh --update` rewrites `FORK.patch` from an edited
  `target/cpp/arrow-src`; every change must also be listed below.

## Design

The codec lives in Arrow's codec factory (`arrow::util::Codec`), next to ZSTD and the others,
so that `Codec::Create`, `IsAvailable`, `SupportsCompressionLevel`, `GetCodecAsString` and
`ColumnChunkMetaData::can_decompress` all know it. It calls the C ABI of `rust/capi`
(`pcp_compress`, `pcp_decompress`), linked from `target/release/libtparquet_capi.a` into
`libarrow.so`; Arrow's symbol map keeps the `pcp_` symbols and the Rust runtime local to
`libarrow.so`. Arrow without `-DARROW_WITH_PCODEC=ON` builds as upstream does and reports the
codec as not built. Only shared libraries are supported with PCODEC (a static `libarrow.a`
would need `libtparquet_capi.a` on the link line of every consumer).

The type hint is the column's Parquet physical type, which only Parquet knows: the Parquet
page writer passes it in a `PcodecCodecOptions` (a `CodecOptions` subclass, like
`ZstdCodecOptions`), so data pages and dictionary pages get it.

## Changes

| File | Item | Change | Why |
|---|---|---|---|
| `cpp/src/parquet/parquet.thrift` | `enum CompressionCodec` | `PCODEC = 20547` | the codec id of spec section 2 |
| `cpp/src/generated/parquet_types.h` | `CompressionCodec::type` | `PCODEC = 20547` | the checked-in generated thrift code (checked against the thrift compiler by `check-cpp-fork.sh`) |
| `cpp/src/generated/parquet_types.cpp` | `_kCompressionCodecValues`, `_kCompressionCodecNames`, `_CompressionCodec_VALUES_TO_NAMES` | `PCODEC` entry, 9 values | as the thrift compiler generates it |
| `cpp/src/parquet/thrift_internal.h` | `FromThriftUnsafe(CompressionCodec)`, `ToThrift(Compression)` | `PCODEC` arms | conversion to and from the footer |
| `cpp/src/parquet/thrift_internal.h` | `LoadEnumSafe(CompressionCodec)` | accepts 20547 besides the standard range 0..7 | upstream rejects every value above `LZ4_RAW` with "Unsupported compression codec value" |
| `cpp/src/parquet/types.cc` | `IsCodecSupported` | `PCODEC` | `GetCodec` refuses codecs not listed there |
| `cpp/src/parquet/column_writer.cc` | `SerializedPageWriter::SerializedPageWriter` | for `PCODEC`, a `PcodecCodecOptions` with the column's physical type (and the given level and `decimal` flag) | the type hint (spec section 3) |
| `cpp/src/parquet/metadata.cc` | new `WithPcodecVersion`; `FileMetaDataBuilderImpl::Finish` | adds `parquet.pcodec.version = "1"` to the footer when a column chunk uses PCODEC and the key is not there | spec section 2; `Finish` is where every footer (plain or encrypted) is built |
| `cpp/src/arrow/util/type_fwd.h` | `Compression::type` | `PCODEC`, after `LZ4_HADOOP` | the Arrow codec enum; the existing values keep their numbers |
| `cpp/src/arrow/util/compression.h` | new `PcodecCodecOptions` | `physical_type` (Parquet `Type` value, -1 for none), `decimal` (default true) | the hint and the DECIMAL flag of `pcp_compress` |
| `cpp/src/arrow/util/compression.cc` | `GetCodecAsString`, `GetCompressionType`, `SupportsCompressionLevel`, `Create`, `IsAvailable` | `pcodec` name, levels supported, `MakePcodecCodec` behind `ARROW_WITH_PCODEC` | as for the other codecs |
| `cpp/src/arrow/util/compression_internal.h` | `kPcodecDefaultCompressionLevel`, `MakePcodecCodec` | declarations | as for the other codecs |
| `cpp/src/arrow/util/compression_pcodec.cc` | new: `PcodecCodec::{Decompress, Compress, MaxCompressedLen, MakeCompressor, MakeDecompressor, Init, ...}`, `MakePcodecCodec` | the codec over `pcp_compress`/`pcp_decompress`; `Decompress` must fill `output_buffer_len` exactly, errors are `Status::IOError` (corrupt payload, wrong size) or `Status::Invalid` (arguments, level outside 0..12); no streaming (`NotImplemented`) | PCODEC is a one-shot block codec that needs the expected size (spec section 3) |
| `cpp/src/arrow/util/config.h.cmake` | | `#cmakedefine ARROW_WITH_PCODEC` | the build switch |
| `cpp/cmake_modules/DefineOptions.cmake` | | option `ARROW_WITH_PCODEC` (off) | the build switch |
| `cpp/src/arrow/CMakeLists.txt` | `ARROW_UTIL_SRCS`, the util targets | `compression_pcodec.cc`; include dir `TPARQUET_CAPI_INCLUDE_DIR`, link `TPARQUET_CAPI_LIB` | the C ABI |

### Decompression call sites and the expected size

`Codec::Decompress(input_len, input, output_buffer_len, output)` is the only decompression
entry point a one-shot codec has. For PCODEC, `output_buffer_len` is the exact decoded size:
`pcp_decompress` fails unless the payload decodes to exactly that many bytes. Call sites in
Parquet (`grep -rn 'Decompress(' cpp/src/parquet`):

- `column_reader.cc` `SerializedPageReader::DecompressIfNeeded`: the only one. It passes
  `uncompressed_len - levels_byte_len` (V2 pages: the values section; V1 and dictionary pages:
  the whole page) and skips the call for an empty values section, then checks the returned
  length. Used for data pages V1 and V2 and dictionary pages, by every reader.

Arrow's IPC and `CompressedInputStream` use streaming decompressors (refused with
`NotImplemented`) or only the IPC codecs (LZ4_FRAME, ZSTD).

A corrupt payload is a `Status::IOError` from the codec, which the page reader throws as a
`parquet::ParquetStatusException` (a `parquet::ParquetException`) and `parquet::arrow`
returns as an `arrow::Status`.

## Tests

In `cpp/tests` (the fork's tests are not added to Arrow's own test tree, which needs a much
larger build):

- `codec_test.cc`: every payload vector of `testdata/payloads/manifest.json` through
  `arrow::util::Codec::Create(PCODEC)` and `parquet::GetCodec(PCODEC)` (valid ones decode bit
  for bit and fail with any other size; invalid ones are `IOError`s); names, levels,
  availability, the type hint and `decimal` flag in the payload's mode and dtype bytes, argument
  errors, random payloads.
- `conformance_test.cc`: every `testdata/files` vector through `parquet::arrow::FileReader`
  (whole and batch by batch) gives exactly its expected rows; the footer key, codecs, physical
  types and page types per column agree with `files/manifest.json`; every page decodes through
  the page reader.
- `writer_test.cc` and `properties_test.cc`: the writer side (codec, type hint, footer key) and
  random corruption of PCODEC pages (an error, never a crash; also run under ASan and UBSan).
- `fuzz/`: libFuzzer targets over `Codec::Decompress` and over Parquet reading of mutated
  vector files (`make -C cpp fuzz-smoke`).

Upstream's own tests are not run: they need `ARROW_BUILD_TESTS` (gtest, gmock, Boost and the
`arrow-testing`/`parquet-testing` data from github.com).

## Mutation testing

No C++ mutation tool is installable here (no `mull` or `dextool` package for Ubuntu 24.04 or on
PyPI). `cpp/mutants/run.py` applies hand-picked mutations of the fork's changed functions
(`cpp/mutants/mutants.json`) to `target/cpp/arrow-src` one at a time, rebuilds Arrow
incrementally, runs the C++ tests and requires them to fail; see `cpp/README.md` for the
result.
