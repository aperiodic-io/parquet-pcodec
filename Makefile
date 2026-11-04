# Local equivalents of .github/workflows/rust.yml. `make check` runs them all but the slow
# ones (fuzz-smoke, mutants, fork-mutants, fork-upstream-tests).
OWN := -p parquet-pcodec-core -p parquet-pcodec-capi -p parquet-pcodec
JOBS ?= 2
CARGO := cargo
UPSTREAM := acceptance/rust-upstream/Cargo.toml
FUZZ_TIME ?= 60

.PHONY: check fmt fmt-check clippy test c-test check-fork testdata testdata-check fuzz-smoke \
	mutants fork-mutants fork-upstream-tests

check: fmt-check clippy test c-test check-fork testdata-check

fmt:
	$(CARGO) fmt $(OWN)
	$(CARGO) fmt --manifest-path $(UPSTREAM)

fmt-check:
	$(CARGO) fmt $(OWN) --check
	$(CARGO) fmt --manifest-path $(UPSTREAM) --check

# --no-deps: the vendored fork keeps upstream's lint configuration
clippy:
	$(CARGO) clippy -j $(JOBS) --no-deps $(OWN) --all-targets -- -D warnings
	$(CARGO) clippy -j $(JOBS) --no-deps --manifest-path $(UPSTREAM) --all-targets -- -D warnings

test:
	$(CARGO) test -j $(JOBS) $(OWN)
	$(CARGO) test -j $(JOBS) -p parquet --lib -- pcodec
	$(CARGO) test -j $(JOBS) -p parquet --test arrow_writer
	$(CARGO) test -j $(JOBS) --manifest-path $(UPSTREAM)

c-test:
	$(MAKE) -C rust/capi c-test

check-fork:
	scripts/check-fork.sh

testdata:
	$(CARGO) run -j $(JOBS) -p parquet-pcodec --bin gen-testdata

# regenerates the vectors and fails if any byte (or file) changed
testdata-check: testdata
	git diff --exit-code -- testdata
	test -z "$$(git status --porcelain -- testdata)"

fuzz-smoke:
	cd rust/core && for t in $$($(CARGO) +nightly fuzz list); do \
		$(CARGO) +nightly fuzz run $$t -- -max_total_time=$(FUZZ_TIME) || exit 1; done

mutants:
	$(CARGO) mutants --workspace -j $(JOBS)

# the fork's changed functions only (rust/parquet/FORK.md)
fork-mutants:
	$(CARGO) mutants --no-config -p parquet -j $(JOBS) \
	  -f rust/parquet/src/basic.rs -f rust/parquet/src/compression.rs \
	  -f rust/parquet/src/column/writer/mod.rs -f rust/parquet/src/file/writer.rs \
	  --re 'PcodecLevel|PcodecCodec|pcodec_codec|with_pcodec_version|set_physical_type|CodecOptionsBuilder|create_codec|<impl From<CompressionCodec> for Compression>::from|<impl From<Compression> for CompressionCodec>::from|<impl FromStr for Compression>::from_str|GenericColumnWriter<.*>::new|write_metadata' \
	  -- --lib -- basic:: compression:: column::writer:: file::writer:: \
	  --skip test_byte_stream_split_extended_roundtrip --skip test_int96_interop \
	  --skip test_rewrite_missing_column_index --skip test_rewrite_no_page_indexes

# the upstream suite of the fork; without parquet-testing/arrow-testing, 333 tests fail
# before and after the fork (see rust/parquet/FORK.md)
fork-upstream-tests:
	$(CARGO) test -j $(JOBS) -p parquet --all-features --no-fail-fast -- --test-threads $(JOBS)
