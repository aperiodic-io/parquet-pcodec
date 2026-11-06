# C and C++: the patched Arrow C++ (cpp/arrow), the tparquet C++ library, its tests, the
# libFuzzer targets and the mutation check. Everything builds under target/cpp; the Arrow
# builds are incremental (and ccache'd when ccache is installed), so re-runs are fast.
#
#   make -C cpp build        Arrow (release) + libtparquet_read + the library, CLI and tests
#   make -C cpp test         the tests (gtest), the C smoke test of rust/cread
#   make -C cpp asan         Arrow and the tests with clang, ASan and UBSan; runs the tests
#   make -C cpp fuzz-smoke   both libFuzzer targets for FUZZ_TIME seconds each (default 60)
#   make -C cpp mutants      the mutation check of the fork's changed functions (cpp/mutation)
#   make -C cpp downstream   installs the package and builds cpp/examples/downstream with it
#   make -C cpp check-fork   scripts/check-cpp-fork.sh
ROOT := $(abspath ..)
T := $(ROOT)/target/cpp
JOBS ?= 3
FUZZ_TIME ?= 60
CARGO ?= cargo
READ_LIB := $(ROOT)/target/release/libtparquet_read.so
READ_INC := $(ROOT)/rust/cread/include
COMMON := -G Ninja -DTPARQUET_READ_LIB=$(READ_LIB) -DTPARQUET_READ_INCLUDE_DIR=$(READ_INC)
# ASan: Rust's allocator and the Arrow pools free at exit what they own; leaks of the test
# process itself are still reported
ASAN_ENV := ASAN_OPTIONS=detect_leaks=1:abort_on_error=1:strict_string_checks=1 \
	UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1

.PHONY: arrow arrow-asan rust build test asan fuzz-build fuzz-smoke mutants install downstream \
	check-fork clean

arrow: rust
	JOBS=$(JOBS) arrow/build.sh release

arrow-asan: rust
	JOBS=$(JOBS) arrow/build.sh asan

rust:
	$(CARGO) build --release -j $(JOBS) -p tparquet-capi -p tparquet-cread

build: arrow
	cmake -S . -B $(T)/build-release $(COMMON) -DCMAKE_BUILD_TYPE=RelWithDebInfo \
	  -DCMAKE_PREFIX_PATH=$(T)/arrow-release/install -DCMAKE_INSTALL_PREFIX=$(T)/install
	cmake --build $(T)/build-release -j $(JOBS)

test: build
	$(T)/build-release/tparquet-tests
	$(MAKE) -C ../rust/cread c-test

asan: arrow-asan
	cmake -S . -B $(T)/build-asan $(COMMON) -DCMAKE_BUILD_TYPE=Debug \
	  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DTPARQUET_SANITIZE=ON \
	  -DCMAKE_PREFIX_PATH=$(T)/arrow-asan/install
	cmake --build $(T)/build-asan -j $(JOBS)
	$(ASAN_ENV) TPARQUET_PROPERTY_CASES=200 $(T)/build-asan/tparquet-tests

fuzz-build: arrow-asan
	cmake -S . -B $(T)/build-fuzz $(COMMON) -DCMAKE_BUILD_TYPE=Debug \
	  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DTPARQUET_SANITIZE=ON \
	  -DTPARQUET_FUZZ=ON -DTPARQUET_BUILD_TESTS=OFF -DCMAKE_PREFIX_PATH=$(T)/arrow-asan/install
	cmake --build $(T)/build-fuzz -j $(JOBS)

# seeds: the payload vectors (prefixed with their expected size) and the vector files
fuzz-smoke: fuzz-build
	rm -rf $(T)/fuzz && mkdir -p $(T)/fuzz/payloads $(T)/fuzz/files $(T)/fuzz/artifacts
	python3 fuzz/seeds.py $(ROOT)/testdata $(T)/fuzz/payloads
	cp $(ROOT)/testdata/files/*.parquet $(T)/fuzz/files/
	$(ASAN_ENV) $(T)/build-fuzz/pcodec_decompress_fuzz -max_total_time=$(FUZZ_TIME) \
	  -artifact_prefix=$(T)/fuzz/artifacts/ $(T)/fuzz/payloads
	$(ASAN_ENV) $(T)/build-fuzz/parquet_read_fuzz -max_total_time=$(FUZZ_TIME) -rss_limit_mb=4096 \
	  -artifact_prefix=$(T)/fuzz/artifacts/ $(T)/fuzz/files

mutants: build
	python3 mutation/run.py --root $(ROOT) --jobs $(JOBS)

install: build
	cmake --install $(T)/build-release

downstream: install
	cmake -S examples/downstream -B $(T)/build-downstream -G Ninja \
	  -DCMAKE_PREFIX_PATH="$(T)/install;$(T)/arrow-release/install"
	cmake --build $(T)/build-downstream
	$(T)/build-downstream/downstream $(ROOT)/testdata/files/mixed.parquet $(T)/downstream.parquet
	$(T)/build-downstream/downstream-c $(T)/downstream.parquet

check-fork:
	$(ROOT)/scripts/check-cpp-fork.sh

clean:
	rm -rf $(T)/build-release $(T)/build-asan $(T)/build-fuzz $(T)/build-downstream $(T)/install
