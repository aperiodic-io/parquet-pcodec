#!/usr/bin/env bash
# Builds and installs the patched Arrow C++ (Arrow and Parquet only, shared libraries, system
# dependencies) into target/cpp/arrow-VARIANT/install. Incremental: a second run with the
# same patch only reruns ninja.
#
#   cpp/arrow/build.sh [release|asan]       JOBS=3 by default
#
#   release  Release build, the default compiler
#   asan     clang, Debug, AddressSanitizer + UndefinedBehaviorSanitizer (ARROW_USE_ASAN/UBSAN),
#            and SanitizerCoverage (ARROW_USE_COVERAGE) so that libFuzzer targets linked
#            against it get coverage feedback from Arrow and Parquet
set -euo pipefail

variant=${1:-release}
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
src="$root/target/cpp/arrow-src"
out="$root/target/cpp/arrow-$variant"
jobs=${JOBS:-3}
capi_lib="$root/target/release/libtparquet_capi.a"

"$here/prepare.sh" "$src"
deps=$("$here/deps.sh" | tail -1)
[ -f "$capi_lib" ] || cargo build --release -p tparquet-capi --manifest-path "$root/Cargo.toml"

case "$variant" in
    release) flags=(-DCMAKE_BUILD_TYPE=Release) ;;
    asan) flags=(-DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
                 -DARROW_USE_ASAN=ON -DARROW_USE_UBSAN=ON -DARROW_USE_COVERAGE=ON) ;;
    *) echo "build: unknown variant $variant (release|asan)" >&2; exit 2 ;;
esac

if [ ! -f "$out/build/build.ninja" ]; then
    cmake -S "$src/cpp" -B "$out/build" -G Ninja "${flags[@]}" \
        -DCMAKE_INSTALL_PREFIX="$out/install" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_PREFIX_PATH="$deps" \
        -DARROW_DEPENDENCY_SOURCE=SYSTEM \
        -DARROW_BUILD_SHARED=ON -DARROW_BUILD_STATIC=OFF \
        -DARROW_PARQUET=ON -DARROW_IPC=ON \
        -DARROW_COMPUTE=OFF -DARROW_CSV=OFF -DARROW_JSON=OFF -DARROW_DATASET=OFF \
        -DARROW_FILESYSTEM=OFF -DARROW_ACERO=OFF -DARROW_FLIGHT=OFF -DARROW_GANDIVA=OFF \
        -DARROW_JEMALLOC=OFF -DARROW_MIMALLOC=OFF -DARROW_WITH_UTF8PROC=OFF -DARROW_WITH_RE2=OFF \
        -DARROW_WITH_OPENTELEMETRY=OFF -DARROW_WITH_BACKTRACE=OFF \
        -DARROW_BUILD_TESTS=OFF -DARROW_BUILD_BENCHMARKS=OFF -DARROW_BUILD_UTILITIES=OFF \
        -DPARQUET_BUILD_EXECUTABLES=OFF -DPARQUET_BUILD_EXAMPLES=OFF \
        -DPARQUET_REQUIRE_ENCRYPTION=OFF \
        -DARROW_WITH_ZSTD=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_BROTLI=ON \
        -DARROW_WITH_ZLIB=ON -DARROW_WITH_BZ2=OFF \
        -DARROW_WITH_PCODEC=ON \
        -DTPARQUET_CAPI_LIB="$capi_lib" \
        -DTPARQUET_CAPI_INCLUDE_DIR="$root/rust/capi/include"
fi
cmake --build "$out/build" -j "$jobs"
cmake --install "$out/build" > /dev/null
echo "build: Arrow ($variant) installed in $out/install"
