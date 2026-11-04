# Local equivalents of .github/workflows/rust.yml. `make check` runs them all but the slow
# ones (fuzz-smoke, mutants, fork-mutants, fork-upstream-tests).
OWN := -p parquet-pcodec-core -p parquet-pcodec-capi -p parquet-pcodec
JOBS ?= 2
CARGO := cargo
UPSTREAM := acceptance/rust-upstream/Cargo.toml
FUZZ_TIME ?= 60

.PHONY: check fmt fmt-check clippy test c-test check-fork testdata testdata-check fuzz-smoke \
	mutants fork-mutants fork-upstream-tests python-test python-mutants

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

# --target: cargo-fuzz defaults to the platform it was built for, and prebuilt cargo-fuzz
# binaries (as CI installs) are musl builds, for which nightly ships no sanitizer std
fuzz-smoke:
	cd rust/core && host=$$(rustc +nightly -vV | sed -n 's/^host: //p') && \
	for t in $$($(CARGO) +nightly fuzz list); do \
		$(CARGO) +nightly fuzz run --target $$host $$t -- -max_total_time=$(FUZZ_TIME) || exit 1; done

# python/rust is judged by the Python suite (see python-mutants); the DuckDB extension has its
# own run, `make -C duckdb mutation`: the tests of both need Python
mutants:
	$(CARGO) mutants --workspace -j $(JOBS) --exclude 'python/**' --exclude 'duckdb/**'

PYTHON_VENV := $(CURDIR)/python/.venv/bin/python

# the Python package: `uv sync` (Python 3.14) and `maturin develop`, then pytest
python-test:
	cd python && uv sync --python 3.14 && uv run --no-sync maturin develop --uv && uv run --no-sync pytest

# mutmut on the Python modules; cargo mutants on the pyo3 crate, with pytest as its test
python-mutants:
	cd python && HYPOTHESIS_PROFILE=mutation PARQUET_PCODEC_TESTDATA=$(CURDIR)/testdata \
	  uv run --no-sync python scripts/mutmut_check.py
	HYPOTHESIS_PROFILE=mutation PARQUET_PCODEC_PYTHON=$(PYTHON_VENV) PYO3_PYTHON=$(PYTHON_VENV) \
	  $(CARGO) mutants -p parquet-pcodec-python -j $(JOBS)

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
