# Fork of the arrow-rs `parquet` crate

This directory is the `parquet` crate as published on crates.io, with the PCODEC codec of
`spec/FORMAT.md` added. The crate name stays `parquet`; the root workspace points every
`parquet = "60.1.0"` dependency here with `[patch.crates-io]`.

| | |
|---|---|
| upstream | `parquet` 60.1.0 |
| source | `https://static.crates.io/crates/parquet/parquet-60.1.0.crate` |
| sha256 | `806eaa0f1c0436dcb43bee8e6a82bf2efff90521a60172f8281cb9ef577b43a4` |

The crate is vendored as published (its normalized `Cargo.toml`), without `Cargo.toml.orig`.
`FORK.md` and `FORK.patch` are not part of upstream.

`FORK.patch` is the complete diff from upstream to this tree. `scripts/check-fork.sh`
downloads the upstream crate, checks the sha256 above, and fails unless the diff of the
git-tracked files equals `FORK.patch`; `scripts/check-fork.sh --update` rewrites it after an
intended change, which must also be listed below.

## Changes

| File | Item | Change | Why |
|---|---|---|---|
| `Cargo.toml` | `[features]` | new feature `pcodec = ["dep:parquet-pcodec-core"]`, in `default` | the codec, default on like the other codecs |
| `Cargo.toml` | `[dependencies]`, `[dev-dependencies]` | `parquet-pcodec-core = { path = "../core" }` (optional; and for tests) | the payload encoder and decoder; unit tests build every codec, as upstream does with `cfg(any(feature, test))` |
| `src/basic.rs` | `CompressionCodec` (thrift enum) | `PCODEC = 20547` | the codec id of spec section 2 |
| `src/basic.rs` | `Compression` | variant `PCODEC(PcodecLevel)` | the writer-side codec choice |
| `src/basic.rs` | `From<CompressionCodec> for Compression`, `From<Compression> for CompressionCodec` | `PCODEC` arms | conversion to and from the footer |
| `src/basic.rs` | `FromStr for Compression` | `PCODEC`/`pcodec`, optionally with a level, `PCODEC(level)` | string parsing as for the other codecs; without a level it is the default 8 |
| `src/basic.rs` | `pub use crate::compression::…` | adds `PcodecLevel` | public like `ZstdLevel` (the `compression` module is private unless `experimental`) |
| `src/compression.rs` | `CodecOptions`, `CodecOptionsBuilder` (struct, `Default`, `build`) | field `physical_type: Option<Type>`, setter `CodecOptionsBuilder::set_physical_type` | the PCODEC type hint; other codecs ignore it |
| `src/compression.rs` | `create_codec` | `PCODEC` arm; `pcodec` added to the `cfg_attr` feature lists of its lint expectations | creates `PcodecCodec` |
| `src/compression.rs` | new `mod pcodec_codec`: `PcodecCodec::{new, compress, decompress}` | the codec over `parquet_pcodec_core`; `decompress` without `uncompress_size` is an error | PCODEC needs the expected size (spec section 3) |
| `src/compression.rs` | new `PcodecLevel` (`CompressionLevel`, `try_new`, `compression_level`, `Default`) | levels 0..=12, default 8 | as `ZstdLevel` |
| `src/column/writer/mod.rs` | `GenericColumnWriter::new` | `CodecOptionsBuilder::default().set_physical_type(descr.physical_type())` | data pages and dictionary pages get the type hint |
| `src/file/writer.rs` | `SerializedFileWriter::write_metadata` | calls `with_pcodec_version` on the key-value metadata | spec section 2 |
| `src/file/writer.rs` | new `with_pcodec_version` | adds `parquet.pcodec.version = "1"` when a column chunk uses PCODEC and the key is not there | spec section 2 |
| `tests/arrow_writer/mod.rs` | | `mod pcodec;` | the fork's Arrow tests |
| `tests/arrow_writer/pcodec.rs` | new | Arrow round trips with PCODEC | see Tests |

Unit tests added: `basic::tests::test_pcodec_compression`; `compression::tests::{test_codec_pcodec,
test_codec_pcodec_physical_type, test_codec_pcodec_requires_size,
test_codec_options_physical_type}`; `column::writer::tests::test_column_writer_pcodec_physical_type`;
`file::writer::tests::test_pcodec_version_metadata` (with its helper `pcodec_kv_metadata`).

### Decompression call sites without the expected size

`Codec::decompress` takes `uncompress_size: Option<usize>`. Every call site in the crate:

- `file/serialized_reader.rs` `decode_page`: the only call outside tests. It passes
  `Some(uncompressed_page_size - levels)` and skips the call for an empty values section. Used
  by the sync reader, the async reader, the push decoder and the dictionary reader
  (`file/metadata/dictionary.rs`).
- `benches/compression.rs`: passes `Some`.
- `compression.rs` tests (`test_codec_without_size`) and the module doc example pass `None`;
  they do not use PCODEC.

`PcodecCodec::decompress` returns `PcodecCodec unsupported without uncompress_size` for `None`
(as `LZ4RawCodec` does), tested by `test_codec_pcodec_requires_size`.

## Tests

- `cargo test -p parquet --lib -- pcodec` and `cargo test -p parquet --test arrow_writer -- pcodec`:
  the fork's tests (round trip of every physical type PCODEC models, required and optional,
  V1 and V2 pages, with and without dictionary pages, levels 0, 8 and 12, logical types,
  lists in a struct, NaN payloads and signed zeros bit for bit, the fallback for strings and
  booleans, page types, codec in the metadata, the footer key, string parsing).
- The upstream suite: `cargo test -p parquet --all-features --no-fail-fast`. Without the
  `parquet-testing` and `arrow-testing` data, 333 upstream tests fail the same way before and
  after the fork (they panic in `arrow::util::test_util::parquet_test_data` or load
  `parquet-testing` files); every other test passes. See the report in the pull request for
  the counts.

## Mutation testing

The repository's `.cargo/mutants.toml` excludes `rust/parquet/**`. The fork's own functions are
mutated with:

```
cargo mutants --no-config -p parquet \
  -f rust/parquet/src/basic.rs -f rust/parquet/src/compression.rs \
  -f rust/parquet/src/column/writer/mod.rs -f rust/parquet/src/file/writer.rs \
  --re 'PcodecLevel|PcodecCodec|pcodec_codec|with_pcodec_version|set_physical_type|CodecOptionsBuilder|create_codec|<impl From<CompressionCodec> for Compression>::from|<impl From<Compression> for CompressionCodec>::from|<impl FromStr for Compression>::from_str|GenericColumnWriter<.*>::new|write_metadata' \
  -- --lib -- basic:: compression:: column::writer:: file::writer:: \
  --skip test_byte_stream_split_extended_roundtrip --skip test_int96_interop \
  --skip test_rewrite_missing_column_index --skip test_rewrite_no_page_indexes
```

(the four skipped tests need `parquet-testing`). `make fork-mutants` runs it.
