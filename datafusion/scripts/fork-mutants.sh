#!/usr/bin/env bash
# cargo mutants over the fork's changed functions (parquet/FORK.md), in place (see the
# Makefile's `mutants` for why). Extra arguments go to cargo mutants.
#
# parquet 59.x declares its `compression` module with its `experimental!` macro, which
# cargo-mutants does not expand, so it would never see compression.rs (where most of the fork
# is). For the run, the declaration is spelled out as the macro expands it (the same module,
# public only with the `experimental` feature); parquet/src/lib.rs is restored on exit, and
# scripts/check-fork.sh fails if it were not.
set -euo pipefail
cd "$(dirname "$0")/.."

lib=parquet/src/lib.rs
backup=$(mktemp)
cp "$lib" "$backup"
trap 'cp "$backup" "$lib"; rm -f "$backup"' EXIT
grep -qx 'experimental!(mod compression);' "$lib"
sed -i 's|^experimental!(mod compression);$|#[cfg(feature = "experimental")]\n#[doc(hidden)]\npub mod compression;\n#[cfg(not(feature = "experimental"))]\nmod compression;|' "$lib"

re='PcodecLevel|PcodecCodec|with_pcodec_version|CodecOptionsBuilder::set_physical_type|CodecOptionsBuilder::build|create_codec|<impl From<CompressionCodec> for Compression>::from|<impl From<Compression> for CompressionCodec>::from|<impl FromStr for Compression>::from_str|write_metadata'
status=0
# the fork is rebuilt for every mutant, so it is built without optimization here
CARGO_PROFILE_DEV_PACKAGE_PARQUET_OPT_LEVEL=0 cargo mutants --no-config -p parquet --in-place \
    --build-timeout "${BUILD_TIMEOUT:-1200}" \
    -f parquet/src/basic.rs -f parquet/src/compression.rs \
    -f parquet/src/column/writer/mod.rs -f parquet/src/file/writer.rs \
    --re "$re" "$@" \
    --cargo-arg=--lib -- -- basic:: compression:: column::writer:: file::writer:: \
    --skip test_byte_stream_split_extended_roundtrip \
    --skip test_rewrite_missing_column_index --skip test_rewrite_no_page_indexes \
    || status=$?
# exit 3: some mutants made the tests hang (a detected mutant); accepted only when none was
# missed (see the Makefile's MUTANTS_OK)
if [ "$status" -eq 3 ] && [ ! -s mutants.out/missed.txt ]; then
    status=0
fi
exit "$status"
